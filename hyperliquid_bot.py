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
    HYPERLIQUID_STOP_LOSS_PCT (optional decimal, e.g. 0.05 for 5% ROE loss)
    HYPERLIQUID_STOP_LOSS_COOLDOWN_MINUTES (default: 5, minutes to pause after stop loss)

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
from decimal import Decimal, DivisionByZero, InvalidOperation, ROUND_DOWN, ROUND_UP
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
    stop_loss_pct: Optional[Decimal] = None
    stop_loss_cooldown_minutes: float = 5.0


@dataclass(slots=True)
class PositionSnapshot:
    qty: Optional[Decimal]
    avg_px: Optional[Decimal]
    margin_used: Optional[Decimal]
    position_value: Optional[Decimal]
    leverage: Optional[Decimal]
    return_on_equity: Optional[Decimal]
    unrealized_pnl: Optional[Decimal]


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
        self.margin_used: Optional[Decimal] = None
        self.current_leverage: Optional[Decimal] = None
        self.current_return_on_equity: Optional[Decimal] = None
        self.stop_loss_price: Optional[Decimal] = None
        self.stop_cloid: Optional[Cloid] = None
        self.stop_oid: Optional[int] = None
        self.stop_order_ts: Optional[float] = None
        self.last_stop_loss_time: Optional[float] = None

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
                await self._place_stop_loss_order()
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
                    is_stop_fill = False
                    avg_price_before_fill: Optional[Decimal] = None
                    if self.position_qty > 0:
                        try:
                            avg_price_before_fill = self.position_cost / self.position_qty
                        except (InvalidOperation, DivisionByZero):
                            avg_price_before_fill = None
                    if oid is not None and self.stop_oid is not None and oid == self.stop_oid:
                        is_stop_fill = True
                    elif oid is not None and self.sell_oid is not None and oid == self.sell_oid:
                        is_stop_fill = False
                    elif (
                        price is not None
                        and avg_price_before_fill is not None
                        and price <= avg_price_before_fill
                    ):
                        is_stop_fill = True
                    if oid is not None:
                        if is_stop_fill:
                            self.stop_oid = oid
                        else:
                            self.sell_oid = oid
                    await self._handle_sell_fill(size, price, is_stop_fill)

    async def on_candle(self, _message: dict[str, Any]) -> None:
        """Placeholder for candle data callback. Not currently used."""
        pass

    def is_in_stop_loss_cooldown(self) -> bool:
        """Check if bot is currently in stop-loss cooldown period."""
        if self.last_stop_loss_time is None:
            return False
        cooldown_seconds = self.cfg.stop_loss_cooldown_minutes * 60
        elapsed = time.time() - self.last_stop_loss_time
        return elapsed < cooldown_seconds

    def get_remaining_cooldown_seconds(self) -> float:
        """Get remaining cooldown time in seconds, or 0 if not in cooldown."""
        if not self.is_in_stop_loss_cooldown():
            return 0.0
        cooldown_seconds = self.cfg.stop_loss_cooldown_minutes * 60
        elapsed = time.time() - self.last_stop_loss_time
        return max(0.0, cooldown_seconds - elapsed)

    async def check_stale_orders(self) -> None:
        now = time.time()
        async with self._lock:
            # Check if cooldown period has ended and we need to resume trading
            if (
                self.last_stop_loss_time is not None
                and self.level == 0
                and self.position_qty == 0
                and self.buy_oid is None
                and self.buy_cloid is None
            ):
                if self.is_in_stop_loss_cooldown():
                    # Still in cooldown, do nothing
                    pass
                else:
                    # Cooldown ended, resume trading
                    LOGGER.info("Stop-loss cooldown period ended. Resuming trading...")
                    self.last_stop_loss_time = None
                    await self._place_buy(level=0)
                    return

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
                await self._place_take_profit()
            if (
                self.stop_order_ts
                and now - self.stop_order_ts > self.cfg.order_timeout_seconds
            ):
                LOGGER.info("Stop-loss order stale; attempting re-place")
                await self._place_stop_loss_order()

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

        # Place TP with retry logic
        tp_placed = False
        for attempt in range(3):
            try:
                await self._place_take_profit()
                tp_placed = True
                break
            except Exception as exc:
                LOGGER.warning("TP placement attempt %s/3 failed: %s", attempt + 1, exc)
                if attempt < 2:
                    await asyncio.sleep(0.5)

        if not tp_placed:
            LOGGER.error("Failed to place take-profit after 3 attempts!")

        if self.position_qty <= 0:
            LOGGER.warning("Position qty non-positive after buy fill; resetting")
            await self._reset_locked()
            return

        # Refresh stop loss state with retry
        stop_placed = False
        for attempt in range(3):
            try:
                await self._refresh_stop_loss_state()
                stop_placed = True
                break
            except Exception as exc:
                LOGGER.warning("Stop loss refresh attempt %s/3 failed: %s", attempt + 1, exc)
                if attempt < 2:
                    await asyncio.sleep(0.5)

        if not stop_placed:
            LOGGER.error("Failed to refresh stop-loss after 3 attempts!")

        self._pending_buy_task = asyncio.create_task(
            self._delayed_place_buy(level=self.level, delay=delay)
        )
        LOGGER.info(
            "Scheduled next buy level=%s after %.2fs cooldown",
            self.level,
            delay,
        )

    async def _handle_sell_fill(
        self,
        qty: Decimal,
        price: Optional[Decimal],
        is_stop_fill: bool,
    ) -> None:
        self.position_qty -= qty
        LOGGER.info(
            "%s fill qty=%s price=%s remaining=%s",
            "Stop-loss" if is_stop_fill else "Take-profit",
            qty,
            price,
            self.position_qty,
        )
        self._clear_sell_refs()
        if self.position_qty <= 0:
            self.margin_used = None
            self.current_leverage = None
            self.current_return_on_equity = None
            self.stop_loss_price = None
            # Record stop loss time if this was a stop loss fill
            if is_stop_fill:
                self.last_stop_loss_time = time.time()
                cooldown_minutes = self.cfg.stop_loss_cooldown_minutes
                LOGGER.warning(
                    "Stop-loss triggered! Bot will pause for %.1f minutes before resuming.",
                    cooldown_minutes,
                )
            await self._reset_locked()
        else:
            await self._refresh_stop_loss_state()
            await self._place_take_profit()

    async def _reset_locked(self) -> None:
        LOGGER.info("Resetting cycle")
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.last_buy_fill_price = None
        self.level = 0
        self.margin_used = None
        self.current_leverage = None
        self.current_return_on_equity = None
        self.stop_loss_price = None
        self._cancel_pending_buy_task()
        self._clear_buy_refs()
        self._clear_sell_refs()
        self._processed_fills.clear()
        self._recent_buy_fill_times.clear()
        await self._cancel_all()

        # Check if in stop-loss cooldown period
        if self.is_in_stop_loss_cooldown():
            remaining = self.get_remaining_cooldown_seconds()
            LOGGER.info(
                "Stop-loss cooldown active. Bot will resume trading in %.1f seconds (%.1f minutes).",
                remaining,
                remaining / 60,
            )
            # Do not place buy order yet - will be handled by check_stale_orders
            return

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
        if await self._has_active_take_profit_order():
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
        self._clear_take_profit_refs()
        await self._refresh_open_order_refs()

    async def _cancel_stop_loss_order(self) -> None:
        if not self.market_coin:
            self._clear_stop_refs()
            return
        cancelled_any = False
        if self.stop_oid is not None:
            try:
                await asyncio.to_thread(self.exchange.cancel, self.market_coin, self.stop_oid)
                cancelled_any = True
            except Exception as exc:
                LOGGER.warning("Failed to cancel stop-loss oid=%s: %s", self.stop_oid, exc)
        elif self.stop_cloid is not None:
            try:
                await asyncio.to_thread(self.exchange.cancel_by_cloid, self.market_coin, self.stop_cloid)
                cancelled_any = True
            except Exception as exc:
                LOGGER.warning("Failed to cancel stop-loss cloid=%s: %s", self.stop_cloid.to_raw(), exc)
        else:
            orders = await self._fetch_open_orders()
            avg_price = None
            if self.position_qty > 0:
                try:
                    avg_price = self.position_cost / self.position_qty
                except (InvalidOperation, DivisionByZero):
                    avg_price = None
            if avg_price is not None:
                for order in orders:
                    side = str(order.get("side") or "").upper()
                    if side != "A":
                        continue
                    order_price = self._safe_decimal(order.get("limitPx") or order.get("px"))
                    if order_price is None or order_price >= avg_price:
                        continue
                    oid = self._safe_int(order.get("oid"))
                    if oid is None:
                        continue
                    try:
                        await asyncio.to_thread(self.exchange.cancel, self.market_coin, oid)
                        cancelled_any = True
                    except Exception as exc:
                        LOGGER.debug("Failed to cancel inferred stop-loss oid=%s: %s", oid, exc)
        self._clear_stop_refs()
        if cancelled_any:
            await self._refresh_open_order_refs()

    async def _place_stop_loss_order(self) -> None:
        pct = self.cfg.stop_loss_pct
        if pct is None or pct <= 0:
            await self._cancel_stop_loss_order()
            return
        if not self.market_coin or self.position_qty <= 0:
            await self._cancel_stop_loss_order()
            return
        self._recompute_stop_loss_price()
        price = self.stop_loss_price
        if price is None or price <= 0:
            LOGGER.debug("Stop-loss price unavailable; cancelling existing stop order.")
            await self._cancel_stop_loss_order()
            return
        quantity = self._quantize_size(self.position_qty, round_up=True)
        if quantity <= 0:
            await self._cancel_stop_loss_order()
            return

        await self._refresh_open_order_refs()

        existing_order: Optional[dict[str, Any]] = None
        for order in self._open_order_cache:
            side = str(order.get("side") or "").upper()
            if side != "A":
                continue
            oid = self._safe_int(order.get("oid"))
            cloid_raw = order.get("cloid")
            if self.stop_oid is not None and oid == self.stop_oid:
                existing_order = order
                break
            if (
                self.stop_cloid is not None
                and isinstance(cloid_raw, str)
                and cloid_raw.lower() == self.stop_cloid.to_raw().lower()
            ):
                existing_order = order
                break

        existing_price: Optional[Decimal] = None
        existing_qty: Optional[Decimal] = None
        if existing_order is not None:
            existing_price = self._safe_decimal(
                existing_order.get("limitPx") or existing_order.get("px")
            )
            existing_qty = self._safe_decimal(
                existing_order.get("origSz") or existing_order.get("sz")
            )

        price_matches = (
            existing_price is not None
            and (existing_price - price).copy_abs() <= (self.price_increment or Decimal("0.00000001"))
        )
        qty_matches = (
            existing_qty is not None
            and self._approximately_equal(existing_qty, quantity)
        )
        if existing_order is not None and price_matches and qty_matches:
            self.stop_order_ts = time.time()
            return

        if existing_order is not None:
            await self._cancel_stop_loss_order()

        avg_entry = None
        if self.position_qty > 0:
            try:
                avg_entry = self.position_cost / self.position_qty
            except (InvalidOperation, DivisionByZero):
                avg_entry = None

        # Cancel any stale stop-like sell orders that might linger (price below entry)
        stale_candidates: list[int] = []
        for order in self._open_order_cache:
            side = str(order.get("side") or "").upper()
            if side != "A":
                continue
            oid = self._safe_int(order.get("oid"))
            if oid is None:
                continue
            if existing_order is not None and order is existing_order:
                continue
            order_price = self._safe_decimal(order.get("limitPx") or order.get("px"))
            if (
                avg_entry is not None
                and order_price is not None
                and order_price < avg_entry
            ):
                stale_candidates.append(oid)

        for oid in stale_candidates:
            try:
                await asyncio.to_thread(self.exchange.cancel, self.market_coin, oid)
            except Exception as exc:
                LOGGER.debug("Failed to cancel stale stop-like order oid=%s: %s", oid, exc)

        if stale_candidates:
            await self._refresh_open_order_refs()

        cloid = Cloid("0x" + secrets.token_hex(16))
        order_type = {"limit": {"tif": "Gtc"}}
        try:
            await asyncio.to_thread(
                self.exchange.order,
                self.market_coin,
                False,
                float(quantity),
                float(price),
                order_type,
                True,
                cloid,
            )
        except Exception as exc:
            LOGGER.error("Failed to place stop-loss order: %s", exc)
            await self._refresh_open_order_refs()
            return

        self.stop_cloid = cloid
        self.stop_oid = None
        self.stop_order_ts = time.time()
        LOGGER.info(
            "Placed stop-loss price=%s qty=%s cloid=%s",
            price,
            quantity,
            cloid.to_raw(),
        )
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
        take_profit_oid = None
        stop_oid = None
        buy_cloid = None
        take_profit_cloid = None
        stop_cloid = None

        avg_price = None
        if self.position_qty > 0:
            try:
                avg_price = self.position_cost / self.position_qty
            except (InvalidOperation, DivisionByZero):
                avg_price = None

        for order in orders:
            side = str(order.get("side") or "").upper()
            oid = self._safe_int(order.get("oid"))
            cloid_raw = order.get("cloid")
            if side == "B":
                buy_oid = oid
                if isinstance(cloid_raw, str) and cloid_raw:
                    buy_cloid = Cloid(cloid_raw)
            elif side == "A":
                order_price = self._safe_decimal(order.get("limitPx") or order.get("px"))
                classification = None
                if self.stop_oid is not None and oid == self.stop_oid:
                    classification = "stop"
                elif self.sell_oid is not None and oid == self.sell_oid:
                    classification = "take"
                elif (
                    isinstance(cloid_raw, str)
                    and cloid_raw
                    and self.stop_cloid is not None
                    and cloid_raw.lower() == self.stop_cloid.to_raw().lower()
                ):
                    classification = "stop"
                elif (
                    isinstance(cloid_raw, str)
                    and cloid_raw
                    and self.sell_cloid is not None
                    and cloid_raw.lower() == self.sell_cloid.to_raw().lower()
                ):
                    classification = "take"
                elif avg_price is not None and order_price is not None:
                    classification = "stop" if order_price < avg_price else "take"
                elif take_profit_oid is None:
                    classification = "take"
                else:
                    classification = "stop"

                if classification == "stop":
                    stop_oid = oid
                    if isinstance(cloid_raw, str) and cloid_raw:
                        stop_cloid = Cloid(cloid_raw)
                else:
                    take_profit_oid = oid
                    if isinstance(cloid_raw, str) and cloid_raw:
                        take_profit_cloid = Cloid(cloid_raw)
        self.buy_oid = buy_oid
        self.sell_oid = take_profit_oid
        self.stop_oid = stop_oid
        if buy_cloid is not None:
            self.buy_cloid = buy_cloid
        if take_profit_cloid is not None:
            self.sell_cloid = take_profit_cloid
        if stop_cloid is not None:
            self.stop_cloid = stop_cloid
        if buy_oid is None and buy_cloid is None:
            self._clear_buy_refs()
        if take_profit_oid is None and take_profit_cloid is None:
            self._clear_take_profit_refs()
        if stop_oid is None and stop_cloid is None:
            self._clear_stop_refs()
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

    async def _has_active_take_profit_order(self) -> bool:
        orders = await self._fetch_open_orders()
        avg_price = None
        if self.position_qty > 0:
            try:
                avg_price = self.position_cost / self.position_qty
            except (InvalidOperation, DivisionByZero):
                avg_price = None
        for order in orders:
            side = str(order.get("side") or "").upper()
            if side != "A":
                continue
            order_price = self._safe_decimal(order.get("limitPx") or order.get("px"))
            if self.sell_cloid is not None:
                cloid_raw = order.get("cloid")
                if isinstance(cloid_raw, str) and cloid_raw.lower() == self.sell_cloid.to_raw().lower():
                    return True
            if avg_price is not None and order_price is not None and order_price >= avg_price:
                return True
        return False

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

    def _recompute_stop_loss_price(self) -> None:
        pct = self.cfg.stop_loss_pct
        if pct is None:
            self.stop_loss_price = None
            return
        if pct <= 0 or self.position_qty <= 0:
            self.stop_loss_price = None
            return
        margin = self.margin_used
        if (
            (margin is None or margin <= 0)
            and self.current_leverage is not None
            and self.current_leverage > 0
        ):
            try:
                margin = self.position_cost / self.current_leverage
            except (InvalidOperation, DivisionByZero):
                margin = None
        if margin is None or margin <= 0:
            self.stop_loss_price = None
            return
        try:
            avg_price = self.position_cost / self.position_qty
        except (InvalidOperation, DivisionByZero):
            self.stop_loss_price = None
            return
        if avg_price <= 0:
            self.stop_loss_price = None
            return
        target_pnl = (Decimal("0") - pct) * margin
        try:
            stop_price = avg_price + (target_pnl / self.position_qty)
        except (InvalidOperation, DivisionByZero):
            self.stop_loss_price = None
            return
        if stop_price <= 0:
            self.stop_loss_price = None
            return
        quantized_price = self._quantize_price(stop_price, is_buy=True)
        if quantized_price <= 0:
            self.stop_loss_price = None
            return
        previous = self.stop_loss_price
        self.stop_loss_price = quantized_price
        if previous != quantized_price:
            LOGGER.info(
                "Calculated stop loss price=%s (target ROE=-%s) avg_price=%s margin=%s leverage=%s",
                quantized_price,
                pct,
                avg_price,
                margin,
                self.current_leverage,
            )

    def _update_position_metrics_from_snapshot(self, snapshot: PositionSnapshot) -> None:
        qty = snapshot.qty
        avg_px = snapshot.avg_px

        computed_avg_px: Optional[Decimal] = None
        if qty is not None and qty > 0:
            if avg_px is not None and avg_px > 0:
                computed_avg_px = avg_px
            else:
                # Derive average entry price from position value and pnl when available
                entry_notional: Optional[Decimal] = None
                if (
                    snapshot.position_value is not None
                    and snapshot.unrealized_pnl is not None
                ):
                    try:
                        entry_notional = snapshot.position_value - snapshot.unrealized_pnl
                    except InvalidOperation:
                        entry_notional = None
                if entry_notional is None and snapshot.position_value is not None:
                    entry_notional = snapshot.position_value
                if entry_notional is not None:
                    try:
                        computed_avg_px = entry_notional / qty
                    except (InvalidOperation, DivisionByZero):
                        computed_avg_px = None

        if (
            qty is not None
            and qty > 0
            and computed_avg_px is not None
            and computed_avg_px > 0
        ):
            self.last_buy_fill_price = computed_avg_px
            self.position_cost = qty * computed_avg_px

        margin_estimate: Optional[Decimal] = None
        if snapshot.margin_used is not None and snapshot.margin_used > 0:
            margin_estimate = snapshot.margin_used
        elif (
            snapshot.position_value is not None
            and snapshot.leverage is not None
            and snapshot.leverage > 0
        ):
            try:
                margin_estimate = snapshot.position_value / snapshot.leverage
            except (InvalidOperation, DivisionByZero):
                margin_estimate = None
        elif (
            snapshot.return_on_equity is not None
            and snapshot.return_on_equity != 0
            and snapshot.unrealized_pnl is not None
        ):
            try:
                margin_estimate = snapshot.unrealized_pnl / snapshot.return_on_equity
            except (InvalidOperation, DivisionByZero):
                margin_estimate = None
        if margin_estimate is not None and margin_estimate < 0:
            margin_estimate = margin_estimate.copy_abs()
        self.margin_used = margin_estimate

        leverage_estimate: Optional[Decimal] = None
        if snapshot.leverage is not None and snapshot.leverage > 0:
            leverage_estimate = snapshot.leverage
        elif (
            snapshot.position_value is not None
            and margin_estimate is not None
            and margin_estimate > 0
        ):
            try:
                leverage_estimate = snapshot.position_value / margin_estimate
            except (InvalidOperation, DivisionByZero):
                leverage_estimate = None
        self.current_leverage = leverage_estimate

        roe_estimate = snapshot.return_on_equity
        if (
            roe_estimate is None
            and snapshot.unrealized_pnl is not None
            and margin_estimate is not None
            and margin_estimate != 0
        ):
            try:
                roe_estimate = snapshot.unrealized_pnl / margin_estimate
            except (InvalidOperation, DivisionByZero):
                roe_estimate = None
        self.current_return_on_equity = roe_estimate
        self._recompute_stop_loss_price()

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

    def _clear_take_profit_refs(self) -> None:
        self.sell_cloid = None
        self.sell_oid = None
        self.sell_order_ts = None

    def _clear_stop_refs(self) -> None:
        self.stop_cloid = None
        self.stop_oid = None
        self.stop_order_ts = None

    def _clear_sell_refs(self) -> None:
        self._clear_take_profit_refs()
        self._clear_stop_refs()

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
        self.margin_used = None
        self.current_leverage = None
        self.current_return_on_equity = None
        self.stop_loss_price = None

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

        snapshot = self._extract_remote_position(state)
        if snapshot is None or snapshot.qty is None or snapshot.qty == Decimal("0"):
            return
        self.position_qty = snapshot.qty
        self._update_position_metrics_from_snapshot(snapshot)

    async def _refresh_stop_loss_state(self) -> None:
        if self.cfg.stop_loss_pct is None:
            return
        if not self.market_coin:
            return
        user_state_fn = getattr(self.info, "user_state", None)
        if user_state_fn is None:
            return
        try:
            state = await asyncio.to_thread(user_state_fn, self.address)
        except Exception as exc:
            LOGGER.debug("Unable to refresh stop loss state: %s", exc)
            return
        snapshot = self._extract_remote_position(state)
        if snapshot is None or snapshot.qty is None or snapshot.qty <= 0:
            self.margin_used = None
            self.current_leverage = None
            self.current_return_on_equity = snapshot.return_on_equity if snapshot else None
            self.stop_loss_price = None
            await self._cancel_stop_loss_order()
            return
        self.position_qty = snapshot.qty
        self._update_position_metrics_from_snapshot(snapshot)
        await self._place_stop_loss_order()

    def _extract_remote_position(self, state: Any) -> Optional[PositionSnapshot]:
        if not state:
            return None
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

            sources: list[dict[str, Any]] = [candidate]
            nested_position = candidate.get("position")
            if isinstance(nested_position, dict):
                sources.append(nested_position)

            def _first_decimal(keys: Sequence[str]) -> Optional[Decimal]:
                for source in sources:
                    for key in keys:
                        if key not in source:
                            continue
                        result = self._safe_decimal(source.get(key))
                        if result is not None:
                            return result
                return None

            qty = _first_decimal(("szi", "sz", "size"))
            avg_px = _first_decimal(("entryPx", "px", "avgEntryPrice"))
            margin_used = _first_decimal(("marginUsed",))
            position_value = _first_decimal(("positionValue", "positionNotional", "notional"))
            return_on_equity = _first_decimal(("returnOnEquity",))
            unrealized_pnl = _first_decimal(("unrealizedPnl", "unrealizedPNL", "unrealizedPnlUsd"))

            leverage_value: Optional[Decimal] = None
            for source in sources:
                leverage_raw = source.get("leverage")
                if isinstance(leverage_raw, dict):
                    leverage_value = self._safe_decimal(leverage_raw.get("value"))
                else:
                    leverage_value = self._safe_decimal(leverage_raw)
                if leverage_value is not None:
                    break
            if (
                leverage_value is None
                and position_value is not None
                and margin_used is not None
                and margin_used != 0
            ):
                try:
                    leverage_value = position_value / margin_used
                except (InvalidOperation, DivisionByZero):
                    leverage_value = None

            snapshot = PositionSnapshot(
                qty=qty,
                avg_px=avg_px,
                margin_used=margin_used,
                position_value=position_value,
                leverage=leverage_value,
                return_on_equity=return_on_equity,
                unrealized_pnl=unrealized_pnl,
            )
            LOGGER.info(
                "Loaded remote position for %s: qty=%s avg_px=%s margin=%s leverage=%s return_on_equity=%s unrealized_pnl=%s",
                coin_raw,
                qty,
                avg_px,
                margin_used,
                leverage_value,
                return_on_equity,
                unrealized_pnl,
            )
            return snapshot
        return None

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
    stop_loss_pct_env = os.environ.get("HYPERLIQUID_STOP_LOSS_PCT")
    stop_loss_pct: Optional[Decimal] = None
    if stop_loss_pct_env:
        stop_loss_pct_candidate = Decimal(stop_loss_pct_env)
        if stop_loss_pct_candidate <= 0:
            raise ValueError("HYPERLIQUID_STOP_LOSS_PCT must be positive when provided.")
        stop_loss_pct = stop_loss_pct_candidate

    stop_loss_cooldown_minutes = float(os.environ.get("HYPERLIQUID_STOP_LOSS_COOLDOWN_MINUTES", "5"))
    if stop_loss_cooldown_minutes < 0:
        raise ValueError("HYPERLIQUID_STOP_LOSS_COOLDOWN_MINUTES must be non-negative.")

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
        stop_loss_pct=stop_loss_pct,
        stop_loss_cooldown_minutes=stop_loss_cooldown_minutes,
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
