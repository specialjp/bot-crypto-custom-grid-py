"""
Hyperliquid martingale/grid bot driven by hyperliquid-python-sdk REST + websocket APIs.

Environment variables:
    HYPERLIQUID_PRIVATE_KEY (hex, required)
    HYPERLIQUID_ACCOUNT_ADDRESS (optional override when trading on behalf of another address)
    HYPERLIQUID_VAULT_ADDRESS (optional vault address for managed subaccounts)
    HYPERLIQUID_MARKET (default: BTC)
    HYPERLIQUID_BASE_ORDER_SIZE (base units, optional)
    HYPERLIQUID_BASE_ORDER_SIZE_USD (default: 100 when base units not provided)
    HYPERLIQUID_PRICE_OFFSET_PCT (default: 0.00015)
    HYPERLIQUID_PRICE_OFFSET_MULTIPLIER (default: 1)
    HYPERLIQUID_PROFIT_PCT (default: 0.0002)
    HYPERLIQUID_SIZE_RATIO (default: 2)
    HYPERLIQUID_ORDER_TIMEOUT_SECONDS (default: 30)

Orders are signed locally via eth-account and submitted through the Exchange REST API.
Websocket subscriptions stream best bid/offer updates and private fills for reactive trading.
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from typing import Any, Optional, Sequence

from dotenv import load_dotenv
from eth_account import Account
from eth_account.signers.local import LocalAccount
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils.constants import MAINNET_API_URL
from hyperliquid.utils.signing import Cloid

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("hyperliquid-grid")


@dataclass(slots=True)
class GridConfig:
    base_url: str
    market: str
    base_order_size: Optional[Decimal] = None
    base_order_size_usd: Optional[Decimal] = Decimal("100")
    price_offset_pct: Decimal = Decimal("0.00015")
    price_offset_multiplier: Decimal = Decimal("1")
    profit_pct: Decimal = Decimal("0.0002")
    size_ratio: Decimal = Decimal("1")
    order_timeout_seconds: float = 10.0


class HyperliquidGridBot:
    BUY_COOLDOWN_WINDOW_SECONDS = 90.0
    BUY_COOLDOWN_STEP_SECONDS = 25.0

    def __init__(self, exchange: Exchange, info: Info, account_address: str, config: GridConfig):
        self.exchange = exchange
        self.info = info
        self.address = account_address.lower()
        self.cfg = config
        if self.cfg.price_offset_multiplier <= 0:
            raise ValueError("HYPERLIQUID_PRICE_OFFSET_MULTIPLIER must be positive.")

        self.market = config.market
        self.market_coin: Optional[str] = None

        self.mid_price = Decimal("0")
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.last_buy_fill_price: Optional[Decimal] = None
        self.level = 0

        self.buy_cloid: Optional[Cloid] = None
        self.sell_cloid: Optional[Cloid] = None
        self.buy_oid: Optional[int] = None
        self.sell_oid: Optional[int] = None
        self.buy_order_ts: Optional[float] = None
        self.sell_order_ts: Optional[float] = None

        self.size_increment = Decimal("0")
        self.price_increment = Decimal("0")
        self._market_meta_loaded = False

        self._lock = asyncio.Lock()
        self._processed_fills: set[str] = set()
        self._pending_buy_task: Optional[asyncio.Task] = None
        self._recent_buy_fill_times: list[float] = []
        self._open_order_cache: list[dict[str, Any]] = []

    async def start(self) -> None:
        async with self._lock:
            await self._ensure_market_metadata()
            await self._restore_remote_state()
            self.mid_price = await self._fetch_mid_price()
            LOGGER.info(
                "Starting Hyperliquid grid on %s mid price %s level=%s pos_qty=%s",
                self.market_coin,
                self.mid_price,
                self.level,
                self.position_qty,
            )

            # If no position exists, start fresh with level 0
            if self.position_qty == Decimal("0"):
                await self._cancel_all()
                self._cancel_pending_buy_task()
                await self._place_buy(level=0)
            else:
                # Has position: ensure take profit is placed
                await self._place_take_profit()
                # Place next buy order at current level
                if self.buy_oid is None and self.buy_cloid is None:
                    await self._place_buy(level=self.level)

    async def on_bbo(self, message: dict[str, Any]) -> None:
        data = message.get("data")
        if not data:
            return
        bbo = data.get("bbo")
        if not isinstance(bbo, Sequence) or len(bbo) != 2:
            return
        bid_level, ask_level = bbo
        bid_px = bid_level.get("px") if isinstance(bid_level, dict) else None
        ask_px = ask_level.get("px") if isinstance(ask_level, dict) else None

        if bid_px is None or ask_px is None:
            return
        try:
            bid = Decimal(str(bid_px))
            ask = Decimal(str(ask_px))
        except (InvalidOperation, TypeError):
            return
        if bid <= 0 or ask <= 0:
            return
        mid = (bid + ask) / Decimal("2")
        async with self._lock:
            self.mid_price = mid

    async def on_user_fills(self, message: dict[str, Any]) -> None:
        data = message.get("data")
        if not data:
            return
        if data.get("isSnapshot"):
            return
        fills = data.get("fills") or []
        async with self._lock:
            for fill in fills:
                if not isinstance(fill, dict):
                    continue
                coin = str(fill.get("coin") or "").upper()
                if not self.market_coin or coin != self.market_coin.upper():
                    continue
                fill_id = str(fill.get("hash") or fill.get("tid") or "")
                if fill_id and fill_id in self._processed_fills:
                    continue
                side_raw = str(fill.get("side") or "").upper()
                if side_raw not in {"A", "B"}:
                    continue
                price_raw = fill.get("px")
                size_raw = fill.get("sz")
                try:
                    price = Decimal(str(price_raw))
                    size = Decimal(str(size_raw))
                except (InvalidOperation, TypeError):
                    continue
                if price <= 0 or size <= 0:
                    continue
                oid = self._safe_int(fill.get("oid"))

                if fill_id:
                    self._processed_fills.add(fill_id)
                    if len(self._processed_fills) > 1000:
                        self._processed_fills = set(list(self._processed_fills)[-500:])

                if side_raw == "B":
                    if oid is not None:
                        self.buy_oid = oid
                    await self._handle_buy_fill(price, size)
                else:
                    if oid is not None:
                        self.sell_oid = oid
                    await self._handle_sell_fill(size)

    async def on_candle(self, _message: dict[str, Any]) -> None:
        """Placeholder for candle data callback. Not currently used."""
        pass

    async def check_stale_orders(self) -> None:
        now = time.time()
        async with self._lock:
            if (
                self.buy_order_ts
                and now - self.buy_order_ts > self.cfg.order_timeout_seconds
                and self.level == 0
            ):
                LOGGER.info("Initial buy order stale; cancelling and restarting")
                await self._cancel_buy_order()
                await self._reset_locked()
                return
            if (
                self.buy_order_ts
                and now - self.buy_order_ts > self.cfg.order_timeout_seconds
                and self.level > 0
            ):
                LOGGER.info(
                    "Buy order stale at level %s; keeping active (position exists)",
                    self.level,
                )
            if (
                self.sell_order_ts
                and now - self.sell_order_ts > self.cfg.order_timeout_seconds
            ):
                LOGGER.info("Take-profit order stale; attempting re-place")
                self._clear_sell_refs()
                await self._place_take_profit()

    async def _handle_buy_fill(self, price: Decimal, qty: Decimal) -> None:
        self.position_qty += qty
        self.position_cost += qty * price
        self.last_buy_fill_price = price
        self.level += 1
        LOGGER.info(
            "Buy filled qty=%s price=%s level=%s pos_qty=%s",
            qty,
            price,
            self.level,
            self.position_qty,
        )

        delay = self._register_buy_cooldown_delay()

        self._clear_buy_refs()
        self._cancel_pending_buy_task()

        # Cancel old take-profit order before placing new one with updated qty and avg price
        await self._cancel_take_profit()
        await self._place_take_profit()
        if self.position_qty <= 0:
            LOGGER.warning("Position qty non-positive after buy fill; resetting")
            await self._reset_locked()
            return

        self._pending_buy_task = asyncio.create_task(
            self._delayed_place_buy(level=self.level, delay=delay)
        )
        LOGGER.info(
            "Scheduled next buy level=%s after %.2fs cooldown",
            self.level,
            delay,
        )

    async def _handle_sell_fill(self, qty: Decimal) -> None:
        self.position_qty -= qty
        LOGGER.info("Sell filled qty=%s remaining=%s", qty, self.position_qty)
        self._clear_sell_refs()
        if self.position_qty <= 0:
            await self._reset_locked()
        else:
            await self._place_take_profit()

    async def _reset_locked(self) -> None:
        LOGGER.info("Resetting cycle")
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.last_buy_fill_price = None
        self.level = 0
        self._cancel_pending_buy_task()
        self._clear_buy_refs()
        self._clear_sell_refs()
        self._processed_fills.clear()
        self._recent_buy_fill_times.clear()
        await self._cancel_all()
        await self._place_buy(level=0)

    async def _ensure_no_active_buy(self) -> bool:
        orders = await self._fetch_open_orders()
        return not self._orders_by_side(orders, {"B"})

    async def _place_buy(self, level: int) -> None:
        if not self.market_coin:
            raise RuntimeError("Market metadata not loaded.")
        if self.mid_price <= 0:
            self.mid_price = await self._fetch_mid_price()
        if not await self._ensure_no_active_buy():
            LOGGER.info("Active buy order detected, skipping new buy placement.")
            return
        offset_multiplier = self.cfg.price_offset_multiplier ** level if level > 0 else Decimal("1")
        adjusted_offset_pct = self.cfg.price_offset_pct * offset_multiplier
        if adjusted_offset_pct >= Decimal("1"):
            raise ValueError(
                f"Buy offset percentage {adjusted_offset_pct} must be less than 1; "
                "lower HYPERLIQUID_PRICE_OFFSET_PCT or HYPERLIQUID_PRICE_OFFSET_MULTIPLIER."
            )
        target_price = self.mid_price * (Decimal("1") - adjusted_offset_pct)
        target_price = self._quantize_price(target_price, is_buy=True)

        if self.last_buy_fill_price is not None and level > 0 and target_price >= self.last_buy_fill_price:
            original_price = target_price
            adjusted_price = self.last_buy_fill_price * (Decimal("1") - adjusted_offset_pct)
            adjusted_price = self._quantize_price(adjusted_price, is_buy=True)
            if adjusted_price >= self.last_buy_fill_price and self.price_increment > 0:
                adjusted_price = self._quantize_price(
                    self.last_buy_fill_price - self.price_increment,
                    is_buy=True,
                )
            target_price = adjusted_price
            LOGGER.info(
                "Adjusted buy price from %s to %s to stay below last fill %s (offset %.6f%%)",
                original_price,
                target_price,
                self.last_buy_fill_price,
                adjusted_offset_pct * Decimal("100"),
            )

        base_qty = self._compute_base_size(level, target_price)

        quantity = self._quantize_size(base_qty)
        if quantity <= 0 or target_price <= 0:
            raise ValueError("Calculated buy order parameters invalid; adjust configuration.")

        cloid = Cloid("0x" + secrets.token_hex(16))
        order_type = {"limit": {"tif": "Gtc"}}
        # print('before place buy order',self.market_coin)

        await asyncio.to_thread(
            self.exchange.order,
            self.market_coin,
            True,
            float(quantity),
            float(target_price),
            order_type,
            False,
        )

        self.buy_cloid = cloid
        self.buy_oid = None
        self.buy_order_ts = time.time()
        order_notional = quantity * target_price
        LOGGER.info(
            "Placed buy level=%s price=%s qty=%s notional=%s offset_pct=%s cloid=%s",
            level,
            target_price,
            quantity,
            order_notional,
            adjusted_offset_pct,
            cloid.to_raw(),
        )
        await self._refresh_open_order_refs()

    async def _place_take_profit(self) -> None:
        if not self.market_coin or self.position_qty <= 0:
            return
        if await self._has_active_sell_order():
            LOGGER.debug("Active take-profit order exists; skipping placement.")
            return
        avg_price = self.position_cost / self.position_qty if self.position_qty > 0 else Decimal("0")
        target_price = avg_price * (Decimal("1") + self.cfg.profit_pct)
        target_price = self._quantize_price(target_price, is_buy=False)
        quantity = self._quantize_size(self.position_qty, round_up=True)
        if quantity <= 0 or target_price <= 0:
            LOGGER.warning("Invalid take profit params qty=%s price=%s", quantity, target_price)
            return

        await self._cancel_take_profit()

        cloid = Cloid("0x" + secrets.token_hex(16))
        order_type = {"limit": {"tif": "Gtc"}}

        try:
            await asyncio.to_thread(
                self.exchange.order,
                self.market_coin,
                False,
                float(quantity),
                float(target_price),
                order_type,
                True,
                cloid,
            )
        except Exception as exc:
            LOGGER.error("Failed to place take profit: %s", exc)
            await self._refresh_open_order_refs()
            return

        self.sell_cloid = cloid
        self.sell_oid = None
        self.sell_order_ts = time.time()
        LOGGER.info(
            "Placed take profit price=%s qty=%s cloid=%s",
            target_price,
            quantity,
            cloid.to_raw(),
        )
        await self._refresh_open_order_refs()

    async def _cancel_all(self) -> None:
        orders = await self._fetch_open_orders()
        if not orders:
            return
        cancel_reqs = []
        for order in orders:
            oid = self._safe_int(order.get("oid"))
            if oid is None:
                continue
            cancel_reqs.append({"coin": order["coin"], "oid": oid})
        if not cancel_reqs:
            return
        LOGGER.info(
            "Cancelling %s Hyperliquid orders on %s",
            len(cancel_reqs),
            self.market_coin,
        )
        try:
            await asyncio.to_thread(self.exchange.bulk_cancel, cancel_reqs)
        except Exception as exc:
            LOGGER.warning("bulk_cancel failed: %s", exc)
        await self._wait_for_orders_cleared({"A", "B"})
        self._clear_buy_refs()
        self._clear_sell_refs()

    async def _cancel_buy_order(self) -> None:
        if self.buy_oid is not None:
            try:
                await asyncio.to_thread(self.exchange.cancel, self.market_coin, self.buy_oid)
            except Exception as exc:
                LOGGER.warning("Failed to cancel buy oid=%s: %s", self.buy_oid, exc)
        elif self.buy_cloid is not None:
            try:
                await asyncio.to_thread(self.exchange.cancel_by_cloid, self.market_coin, self.buy_cloid)
            except Exception as exc:
                LOGGER.warning("Failed to cancel buy cloid=%s: %s", self.buy_cloid.to_raw(), exc)
        self._clear_buy_refs()
        await self._wait_for_orders_cleared({"B"})
        await self._refresh_open_order_refs()

    async def _cancel_take_profit(self) -> None:
        if not self.market_coin:
            return
        if self.sell_oid is not None:
            try:
                await asyncio.to_thread(self.exchange.cancel, self.market_coin, self.sell_oid)
            except Exception as exc:
                LOGGER.warning("Failed to cancel sell oid=%s: %s", self.sell_oid, exc)
        elif self.sell_cloid is not None:
            try:
                await asyncio.to_thread(self.exchange.cancel_by_cloid, self.market_coin, self.sell_cloid)
            except Exception as exc:
                LOGGER.warning("Failed to cancel sell cloid=%s: %s", self.sell_cloid.to_raw(), exc)
        self._clear_sell_refs()
        await self._wait_for_orders_cleared({"A"})
        await self._refresh_open_order_refs()

    async def _delayed_place_buy(self, level: int, delay: float) -> None:
        current_task = asyncio.current_task()
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            async with self._lock:
                if self.buy_oid is not None or self.buy_cloid is not None:
                    LOGGER.debug(
                        "Skipping delayed buy placement; active buy refs oid=%s cloid=%s",
                        self.buy_oid,
                        self.buy_cloid.to_raw() if self.buy_cloid else None,
                    )
                    return
                target_level = self.level
                await self._place_buy(level=target_level)
        except asyncio.CancelledError:
            LOGGER.debug("Delayed buy placement cancelled for level %s", level)
            raise
        except Exception as exc:
            LOGGER.exception("Error placing delayed buy order at level %s: %s", level, exc)
        finally:
            if current_task and self._pending_buy_task is current_task:
                self._pending_buy_task = None

    async def _refresh_open_order_refs(self) -> list[dict[str, Any]]:
        orders = await self._fetch_open_orders()
        buy_oid = None
        sell_oid = None
        buy_cloid = None
        sell_cloid = None
        for order in orders:
            side = str(order.get("side") or "").upper()
            oid = self._safe_int(order.get("oid"))
            cloid_raw = order.get("cloid")
            if side == "B":
                buy_oid = oid
                if isinstance(cloid_raw, str) and cloid_raw:
                    buy_cloid = Cloid(cloid_raw)
            elif side == "A":
                sell_oid = oid
                if isinstance(cloid_raw, str) and cloid_raw:
                    sell_cloid = Cloid(cloid_raw)
        self.buy_oid = buy_oid
        self.sell_oid = sell_oid
        if buy_cloid is not None:
            self.buy_cloid = buy_cloid
        if sell_cloid is not None:
            self.sell_cloid = sell_cloid
        if buy_oid is None and buy_cloid is None:
            self._clear_buy_refs()
        if sell_oid is None and sell_cloid is None:
            self._clear_sell_refs()
        return orders

    async def _fetch_open_orders(self) -> list[dict[str, Any]]:
        if not self.market_coin:
            return []
        try:
            orders = await asyncio.to_thread(self.info.open_orders, self.address)
        except Exception as exc:
            LOGGER.debug("open_orders fetch failed: %s", exc)
            return []
        filtered: list[dict[str, Any]] = []
        for order in orders or []:
            if not isinstance(order, dict):
                continue
            coin = str(order.get("coin") or "").upper()
            if coin != self.market_coin.upper():
                continue
            filtered.append(order)
        self._open_order_cache = filtered
        return filtered

    def _orders_by_side(self, orders: list[dict[str, Any]], sides: set[str]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for order in orders:
            side = str(order.get("side") or "").upper()
            if side in sides:
                result.append(order)
        return result

    async def _wait_for_orders_cleared(self, sides: set[str], attempts: int = 6, delay: float = 0.25) -> None:
        for attempt in range(attempts):
            orders = await self._fetch_open_orders()
            remaining = self._orders_by_side(orders, sides)
            if not remaining:
                return
            await asyncio.sleep(delay)
        LOGGER.warning("Orders still active for sides %s after %s attempts", sides, attempts)

    async def _has_active_sell_order(self) -> bool:
        orders = await self._fetch_open_orders()
        return bool(self._orders_by_side(orders, {"A"}))

    async def _ensure_market_metadata(self) -> None:
        if self._market_meta_loaded:
            return
        canonical_target = _canonical_symbol(self.market)

        matched_coin = None
        for candidate in self.info.name_to_coin.keys():
            if candidate.lower() == self.market.lower():
                matched_coin = candidate
                break
            if _canonical_symbol(candidate) == canonical_target:
                matched_coin = candidate
                break
        if not matched_coin:
            raise RuntimeError(f"Market {self.market} not found in Hyperliquid metadata")

        self.market_coin = matched_coin
        asset_id = self.info.name_to_asset(matched_coin)
        size_decimals = self.info.asset_to_sz_decimals.get(asset_id, 0)

        if size_decimals >= 0:
            self.size_increment = Decimal("1") / (Decimal(10) ** size_decimals)
        try:
            meta = await asyncio.to_thread(self.info.meta)
        except Exception as exc:
            LOGGER.warning("Failed to fetch market meta: %s", exc)
            meta = None

        entry = None
        is_spot = False
        if isinstance(meta, dict):
            universe = meta.get("universe")
            if isinstance(universe, Sequence):
                for candidate in universe:
                    name = str(candidate.get("name") if isinstance(candidate, dict) else "")
                    if name and _canonical_symbol(name) == canonical_target:
                        entry = candidate
                        break
        if isinstance(entry, dict):
            # Check if this is a spot market
            is_spot = False

            # Calculate tick_size (price_increment) from szDecimals
            # Per Hyperliquid API: MAX_DECIMALS - szDecimals
            # where MAX_DECIMALS = 6 for perps, 8 for spot
            if size_decimals >= 0:
                max_decimals = 8 if is_spot else 6
                price_decimals = max_decimals - size_decimals
                if price_decimals >= 0:
                    self.price_increment = Decimal("1") / (Decimal(10) ** price_decimals)
                    LOGGER.info(
                        "Calculated price_increment=%s from szDecimals=%s (max_decimals=%s, %s)",
                        self.price_increment,
                        size_decimals,
                        max_decimals,
                        "spot" if is_spot else "perp"
                    )

        self._market_meta_loaded = True

    async def _fetch_mid_price(self) -> Decimal:
        try:
            response = await asyncio.to_thread(self.info.all_mids)
        except Exception as exc:
            raise RuntimeError(f"Failed to fetch mids: {exc}") from exc
        mids = None
        if isinstance(response, dict):
            mids = response.get("mids") if isinstance(response.get("mids"), dict) else response
        if not isinstance(mids, dict) or not self.market_coin:
            raise RuntimeError("Mid price response malformed")
        value = mids.get(self.market_coin)
        if value is None:
            raise RuntimeError(f"Mid price for {self.market_coin} unavailable")
        try:
            price = Decimal(str(value))
        except (InvalidOperation, TypeError) as exc:
            raise RuntimeError(f"Invalid mid price value {value}") from exc
        if price <= 0:
            raise RuntimeError(f"Non-positive mid price {price}")
        return price

    def _register_buy_cooldown_delay(self) -> float:
        now = time.time()
        window = self.BUY_COOLDOWN_WINDOW_SECONDS
        self._recent_buy_fill_times = [ts for ts in self._recent_buy_fill_times if now - ts < window]
        self._recent_buy_fill_times.append(now)
        delay = self.BUY_COOLDOWN_STEP_SECONDS * len(self._recent_buy_fill_times)
        return delay

    def _cancel_pending_buy_task(self) -> None:
        task = self._pending_buy_task
        if task and not task.done():
            task.cancel()
        self._pending_buy_task = None

    def _quantize_size(self, qty: Decimal, *, round_up: bool = False) -> Decimal:
        increment = self.size_increment
        if increment <= 0:
            return qty
        steps = (qty / increment).quantize(Decimal("1"), rounding=ROUND_UP if round_up else ROUND_DOWN)
        if steps <= 0 and round_up:
            steps = Decimal("1")
        quantized = steps * increment
        if quantized <= 0 and round_up:
            quantized = increment
        return quantized

    def _quantize_price(self, price: Decimal, *, is_buy: bool) -> Decimal:
        increment = self.price_increment
        if increment <= 0:
            return price
        rounding = ROUND_DOWN if is_buy else ROUND_UP
        steps = (price / increment).quantize(Decimal("1"), rounding=rounding)
        if steps <= 0:
            steps = Decimal("1")
        quantized = steps * increment

        # Hyperliquid API constraint: max 5 significant figures (unless integer)
        # Integer prices are always allowed regardless of significant figures
        if quantized % 1 != 0:  # Not an integer
            sig_figs = self._count_significant_figures(quantized)
            if sig_figs > 5:
                # Reduce precision to 5 significant figures
                quantized = self._limit_significant_figures(quantized, 5, is_buy)

        return quantized

    def _count_significant_figures(self, value: Decimal) -> int:
        """Count significant figures in a decimal value."""
        # Convert to string and remove sign
        s = str(abs(value))
        # Remove decimal point
        s = s.replace('.', '')
        # Remove leading zeros
        s = s.lstrip('0')
        # Count remaining digits
        return len(s)

    def _limit_significant_figures(self, value: Decimal, max_sig_figs: int, round_down: bool) -> Decimal:
        """Limit a value to maximum significant figures."""
        if value == 0:
            return Decimal("0")

        # Get string representation
        s = str(abs(value))

        # Check if scientific notation
        if 'E' in s or 'e' in s:
            value = Decimal(value)

        # Find position of first significant digit
        normalized = f"{value:.15e}"  # Use scientific notation
        parts = normalized.split('e')
        mantissa = parts[0].replace('.', '').replace('-', '')
        exponent = int(parts[1]) if len(parts) > 1 else 0

        # Count significant figures
        sig_figs = len(mantissa.lstrip('0'))

        if sig_figs <= max_sig_figs:
            return value

        # Calculate scaling factor
        first_digit_pos = len(str(abs(int(value)))) if abs(value) >= 1 else 0
        if first_digit_pos == 0:
            # Value < 1: count leading zeros after decimal point
            s_dec = str(abs(value))
            if '.' in s_dec:
                after_decimal = s_dec.split('.')[1]
                leading_zeros = len(after_decimal) - len(after_decimal.lstrip('0'))
                scale = 10 ** (-(leading_zeros + max_sig_figs))
            else:
                scale = 1
        else:
            # Value >= 1
            scale = 10 ** (first_digit_pos - max_sig_figs)

        # Round to significant figures
        rounding = ROUND_DOWN if round_down else ROUND_UP
        rounded = (value / Decimal(str(scale))).quantize(Decimal("1"), rounding=rounding) * Decimal(str(scale))

        return rounded

    def _compute_base_size(self, level: int, price: Decimal) -> Decimal:
        if price <= 0:
            raise ValueError("Target price must be positive when computing base size.")

        multiplier = self.cfg.size_ratio ** level if level > 0 else Decimal("1")
        if self.cfg.base_order_size is not None and self.cfg.base_order_size > Decimal("0"):
            return self.cfg.base_order_size * multiplier
        if self.cfg.base_order_size_usd is not None and self.cfg.base_order_size_usd > Decimal("0"):
            usd_notional = self.cfg.base_order_size_usd * multiplier
            return usd_notional / price
        raise ValueError(
            "Configure HYPERLIQUID_BASE_ORDER_SIZE (base units) or HYPERLIQUID_BASE_ORDER_SIZE_USD with a positive value."
        )

    def _clear_buy_refs(self) -> None:
        self.buy_cloid = None
        self.buy_oid = None
        self.buy_order_ts = None

    def _clear_sell_refs(self) -> None:
        self.sell_cloid = None
        self.sell_oid = None
        self.sell_order_ts = None

    async def _restore_remote_state(self) -> None:
        await self._load_remote_position()
        self._infer_level_from_state()
        now = time.time()
        if self.buy_oid is not None or self.buy_cloid is not None:
            self.buy_order_ts = now
        if self.sell_oid is not None or self.sell_cloid is not None:
            self.sell_order_ts = now

    async def _load_remote_position(self) -> None:
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.last_buy_fill_price = None

        if not self.market_coin:
            return

        user_state_fn = getattr(self.info, "user_state", None)
        if user_state_fn is None:
            LOGGER.info("Info.user_state unavailable; skipping remote position restore.")
            return
        try:
            state = await asyncio.to_thread(user_state_fn, self.address)
        except Exception as exc:
            LOGGER.warning("Failed to fetch remote position state: %s", exc)
            return

        qty, avg_px = self._extract_remote_position(state)
        if qty is None or qty == Decimal("0"):
            return
        self.position_qty = qty
        if avg_px is not None and avg_px > 0:
            self.last_buy_fill_price = avg_px
            self.position_cost = qty * avg_px

    def _extract_remote_position(self, state: Any) -> tuple[Optional[Decimal], Optional[Decimal]]:
        if not state:
            return None, None
        target = _canonical_symbol(self.market)
        stack: list[Any] = [state]
        while stack:
            current = stack.pop()
            if isinstance(current, dict):
                # push nested values for traversal
                stack.extend(current.values())
                candidate = current
            elif isinstance(current, list):
                stack.extend(current)
                continue
            else:
                continue

            coin_raw = candidate.get("coin")
            if not coin_raw:
                nested_position = candidate.get("position")
                if isinstance(nested_position, dict):
                    stack.append(nested_position)
                continue
            if _canonical_symbol(str(coin_raw)) != target:
                continue

            # Try different field names for size: 'szi', 'sz', 'size'
            qty = self._safe_decimal(
                candidate.get("szi") or candidate.get("sz") or candidate.get("size")
            )
            if qty is None and isinstance(candidate.get("position"), dict):
                nested = candidate["position"]
                qty = self._safe_decimal(
                    nested.get("szi") or nested.get("sz") or nested.get("size")
                )

            # Try different field names for price: 'entryPx', 'px', 'avgEntryPrice'
            avg_px = self._safe_decimal(
                candidate.get("entryPx") or candidate.get("px") or candidate.get("avgEntryPrice")
            )
            if avg_px is None and isinstance(candidate.get("position"), dict):
                nested = candidate["position"]
                avg_px = self._safe_decimal(
                    nested.get("entryPx") or nested.get("px") or nested.get("avgEntryPrice")
                )

            if qty is not None:
                LOGGER.info(
                    "Loaded remote position for %s: qty=%s avg_px=%s",
                    coin_raw,
                    qty,
                    avg_px
                )
                return qty, avg_px
        return None, None

    def _infer_level_from_state(self) -> None:
        if self.position_qty != Decimal("0"):
            reference_price = self.last_buy_fill_price or self.mid_price
            level_guess = self._match_level_from_position(self.position_qty, reference_price)
            if level_guess is not None:
                self.level = level_guess
                return

        self.level = 1 if self.position_qty > 0 else 0

    def _active_buy_order(self, orders: Sequence[dict[str, Any]]) -> Optional[dict[str, Any]]:
        for order in orders:
            side = str(order.get("side") or "").upper()
            if side == "B":
                return order
        return None

    def _match_level_from_buy_order(self, order: dict[str, Any]) -> Optional[int]:
        qty_raw = order.get("origSz")
        price_raw = order.get("limitPx")
        qty = self._safe_decimal(qty_raw)
        price = self._safe_decimal(price_raw)
        if qty is None or price is None or price <= 0:
            return None

        for level in range(0, 64):
            try:
                theoretical = self._compute_base_size(level, price)
            except ValueError:
                break
            theoretical = self._quantize_size(theoretical)
            if theoretical <= 0:
                break
            if self._approximately_equal(theoretical, qty):
                return level
        return None

    def _match_level_from_position(self, qty: Decimal, reference_price: Optional[Decimal]) -> Optional[int]:
        if qty <= 0:
            return 0
        price = reference_price if reference_price and reference_price > 0 else None
        if price is None or price <= 0:
            price = self.mid_price if self.mid_price > 0 else None
        if price is None or price <= 0:
            return None

        cumulative = Decimal("0")
        for level in range(0, 64):
            try:
                increment = self._compute_base_size(level, price)
            except ValueError:
                break
            increment = self._quantize_size(increment)
            if increment <= 0:
                break
            cumulative += increment
            if cumulative > qty:
                return level
        return None

    def _approximately_equal(self, lhs: Decimal, rhs: Decimal) -> bool:
        tolerance = self.size_increment if self.size_increment > 0 else Decimal("1e-8")
        return (lhs - rhs).copy_abs() <= tolerance

    @staticmethod
    def _safe_decimal(value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            return None

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(str(value), 10)
        except (TypeError, ValueError):
            return None


def _canonical_symbol(symbol: str) -> str:
    cleaned = "".join(ch for ch in (symbol or "") if ch.isalnum()).upper()
    for suffix in ("PERP", "USDC", "USD"):
        if cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)]
    return cleaned


async def main() -> None:
    load_dotenv()

    base_url = MAINNET_API_URL

    private_key = os.environ.get("HYPERLIQUID_PRIVATE_KEY")
    if not private_key:
        raise RuntimeError("Set HYPERLIQUID_PRIVATE_KEY with your signer key.")

    wallet: LocalAccount = Account.from_key(private_key)

    account_address_env = os.environ.get("HYPERLIQUID_ACCOUNT_ADDRESS")
    account_address = account_address_env or wallet.address

    market = os.environ.get("HYPERLIQUID_MARKET", "BTC")

    base_order_size_env = os.environ.get("HYPERLIQUID_BASE_ORDER_SIZE")
    base_order_size = Decimal(base_order_size_env) if base_order_size_env else None

    base_order_size_usd_env = os.environ.get("HYPERLIQUID_BASE_ORDER_SIZE_USD")
    if base_order_size is None:
        base_order_size_usd = Decimal(base_order_size_usd_env or "100")
    else:
        base_order_size_usd = Decimal(base_order_size_usd_env) if base_order_size_usd_env else None

    price_offset_pct = Decimal(os.environ.get("HYPERLIQUID_PRICE_OFFSET_PCT", "0.00015"))
    price_offset_multiplier = Decimal(os.environ.get("HYPERLIQUID_PRICE_OFFSET_MULTIPLIER", "1"))
    profit_pct = Decimal(os.environ.get("HYPERLIQUID_PROFIT_PCT", "0.0002"))
    size_ratio = Decimal(os.environ.get("HYPERLIQUID_SIZE_RATIO", "2"))
    order_timeout = float(os.environ.get("HYPERLIQUID_ORDER_TIMEOUT_SECONDS", "10"))

    exchange = Exchange(
        wallet=wallet,
        base_url=base_url,
        account_address=account_address_env,
    )
    info = Info(base_url)

    cfg = GridConfig(
        base_url=base_url,
        market=market,
        base_order_size=base_order_size,
        base_order_size_usd=base_order_size_usd,
        price_offset_pct=price_offset_pct,
        price_offset_multiplier=price_offset_multiplier,
        profit_pct=profit_pct,
        size_ratio=size_ratio,
        order_timeout_seconds=order_timeout,
    )

    bot = HyperliquidGridBot(exchange, info, account_address, cfg)
    await bot.start()

    loop = asyncio.get_running_loop()

    def bbo_callback(msg: Any) -> None:
        loop.call_soon_threadsafe(lambda: asyncio.create_task(bot.on_bbo(msg)))

    def fills_callback(msg: Any) -> None:
        loop.call_soon_threadsafe(lambda: asyncio.create_task(bot.on_user_fills(msg)))

    # def candle_callback(msg: Any) -> None:
    #     loop.call_soon_threadsafe(lambda: asyncio.create_task(bot.on_candle(msg)))


    if not bot.market_coin:
        raise RuntimeError("Market metadata not initialized.")

    bbo_subscription = {"type": "bbo", "coin": bot.market_coin}
    fills_subscription = {"type": "userFills", "user": account_address.lower()}
    candle_subscription = { "type": "candle", "coin": bot.market_coin, "interval": "1m" }

    info.subscribe(bbo_subscription, bbo_callback)
    info.subscribe(fills_subscription, fills_callback)
    # info.subscribe(candle_subscription, candle_callback)


    LOGGER.info("Hyperliquid grid bot running on %s (%s)", bot.market_coin, base_url)
    try:
        while True:
            await asyncio.sleep(1)
            await bot.check_stale_orders()
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        LOGGER.info("Stopping bot")
    finally:
        bot._cancel_pending_buy_task()
        if info.ws_manager is not None:
            info.disconnect_websocket()


if __name__ == "__main__":
    asyncio.run(main())
