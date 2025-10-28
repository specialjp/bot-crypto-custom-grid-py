"""
Paradex martingale/grid bot powered by paradex-py REST and websocket clients.

Environment variables:
    PARADEX_MARKET (default: BTC-USD-PERP)
    PARADEX_BASE_ORDER_SIZE (base units, optional)
    PARADEX_BASE_ORDER_SIZE_USD (default: 100 when base units not provided)
    PARADEX_PRICE_OFFSET_PCT (default: 0.00015)
    PARADEX_PROFIT_PCT (default: 0.00015, take profit percentage)
    PARADEX_PRICE_OFFSET_MULTIPLIER (default: 1.1, geometric multiplier for buy offset per level)
    PARADEX_SIZE_RATIO (default: 1)
    PARADEX_ORDER_TIMEOUT_SECONDS (default: 30)
    PARADEX_L2_PRIVATE_KEY (or L2_PRIVATE_KEY) required for signing orders
    PARADEX_L2_ADDRESS (or L2_ADDRESS) required for subkey authentication

Uses L2-only authentication via `ParadexSubkey`. L1 credentials are not needed.
Runs against the production Paradex environment.
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
from paradex_py import ParadexSubkey
from paradex_py.account.account import ParadexAccount
from paradex_py.api.api_client import ParadexApiClient
from paradex_py.api.ws_client import ParadexWebsocketChannel, ParadexWebsocketClient
from paradex_py.common.order import Order, OrderSide, OrderType
from paradex_py.environment import Environment

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("paradex-grid")


@dataclass(slots=True)
class GridConfig:
    market: str
    base_order_size: Optional[Decimal] = None
    base_order_size_usd: Optional[Decimal] = Decimal("100")
    price_offset_pct: Decimal = Decimal("0.00015")
    price_offset_multiplier: Decimal = Decimal("1")
    profit_pct: Decimal = Decimal("0.0002")
    size_ratio: Decimal = Decimal("2")
    order_timeout_seconds: float = 30.0


class ParadexGridBot:
    BUY_COOLDOWN_WINDOW_SECONDS = 90.0
    BUY_COOLDOWN_STEP_SECONDS = 35.0

    def __init__(self, client: ParadexSubkey, config: GridConfig):
        self.client = client
        self.api: ParadexApiClient = client.api_client
        self.account: ParadexAccount = client.account  # type: ignore[attr-defined]
        self.cfg = config
        if self.cfg.price_offset_multiplier <= 0:
            raise ValueError("PARADEX_PRICE_OFFSET_MULTIPLIER must be positive.")

        self.market = config.market
        self.mid_price = Decimal("0")
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.last_buy_fill_price: Optional[Decimal] = None
        self.level = 0

        self.buy_order_id: Optional[str] = None
        self.sell_order_id: Optional[str] = None
        self.buy_client_id: Optional[str] = None
        self.sell_client_id: Optional[str] = None
        self.buy_order_ts: Optional[float] = None
        self.sell_order_ts: Optional[float] = None
        self.buy_order_size = Decimal("0")
        self.buy_order_filled = Decimal("0")

        self.size_increment = Decimal("0")
        self.price_increment = Decimal("0")
        self.min_notional = Decimal("0")
        self._market_meta_loaded = False

        self._lock = asyncio.Lock()
        self._processed_fills: set[str] = set()  # Track fill IDs to prevent duplicates
        self._pending_buy_task: Optional[asyncio.Task] = None
        self._recent_buy_fill_times: list[float] = []

    async def start(self) -> None:
        async with self._lock:
            await self._ensure_market_metadata()
            self.mid_price = await self._fetch_mid_price()
            LOGGER.info("Starting Paradex grid at mid price %s", self.mid_price)
            await self._cancel_all()
            await self._place_buy(level=0)

    async def on_ticker(self, message: dict[str, Any]) -> None:
        data = self._extract_data(message)
        if not data:
            return
        if isinstance(data, Sequence) and not isinstance(data, (dict, str)):
            data = data[0] if data else None
        if not isinstance(data, dict):
            return
        bid = data.get("bid")
        ask = data.get("ask")
        if not bid or not ask:
            return
        try:
            mid = (Decimal(str(bid)) + Decimal(str(ask))) / Decimal("2")
        except (InvalidOperation, TypeError):
            return
        async with self._lock:
            self.mid_price = mid

    async def on_order(self, message: dict[str, Any]) -> None:
        data = self._extract_data(message)
        if not data:
            return
        entries = data if isinstance(data, list) else [data]
        async with self._lock:
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                status = str(entry.get("status") or "").upper()
                if status not in {"CLOSED", "CANCELED", "CANCELLED"}:
                    continue
                order_id = entry.get("id") or entry.get("order_id") or entry.get("orderId")
                client_id = entry.get("client_id") or entry.get("clientId")
                side = str(entry.get("side") or "").upper()
                if not order_id and not client_id:
                    continue
                remaining = entry.get("remaining_size") or entry.get("remainingSize")
                try:
                    remaining_dec = Decimal(str(remaining)) if remaining is not None else Decimal("0")
                except (InvalidOperation, TypeError):
                    remaining_dec = Decimal("0")

                if side == "BUY" and self._matches_buy(order_id, client_id):
                    if remaining_dec > 0:
                        LOGGER.info("Buy order %s closed without fill; re-placing", order_id)
                        await self._place_buy(level=self.level)
                        self._clear_buy_refs()
                    # If remaining=0, order was fully filled -> on_fill handles it
                elif side == "SELL" and self._matches_sell(order_id, client_id):
                    if remaining_dec > 0 and self.position_qty > 0:
                        LOGGER.info("Take-profit order %s closed without fill; re-placing", order_id)
                        self._clear_sell_refs()
                        await self._place_take_profit()
                    # If remaining=0, order was fully filled -> on_fill handles it

    async def on_fill(self, message: dict[str, Any]) -> None:
        data = self._extract_data(message)
        if not data:
            return
        entries = data if isinstance(data, list) else [data]
        async with self._lock:
            for entry in entries:
                if not isinstance(entry, dict):
                    continue

                # Get fill ID to prevent duplicate processing
                fill_id = entry.get("id") or entry.get("fill_id") or entry.get("trade_id")
                if fill_id and fill_id in self._processed_fills:
                    LOGGER.debug("Skipping duplicate fill id=%s", fill_id)
                    continue

                side = str(entry.get("side") or entry.get("order_side") or "").lower()
                order_id = entry.get("order_id") or entry.get("id")
                client_id = entry.get("client_id") or entry.get("clientId")
                price = entry.get("price") or entry.get("avg_fill_price") or entry.get("avgPrice")
                size = entry.get("size") or entry.get("fill_size") or entry.get("filled_size")
                if not side or not price or not size:
                    continue
                try:
                    price_dec = Decimal(str(price))
                    size_dec = Decimal(str(size))
                except (InvalidOperation, TypeError):
                    continue

                # Mark fill as processed before handling to prevent race conditions
                if fill_id:
                    self._processed_fills.add(fill_id)
                    # Keep set bounded (last 1000 fills)
                    if len(self._processed_fills) > 1000:
                        self._processed_fills = set(list(self._processed_fills)[-500:])

                if side == "buy" and self._matches_buy(order_id, client_id):
                    await self._handle_buy_fill(order_id, price_dec, size_dec)
                elif side == "sell" and self._matches_sell(order_id, client_id):
                    LOGGER.info("Processing sell fill_id=%s price=%s size=%s", fill_id, price_dec, size_dec)
                    await self._handle_sell_fill(size_dec)

    async def check_stale_orders(self) -> None:
        now = time.time()
        async with self._lock:
            # Only cancel buy orders at level 0 (first order that hasn't been matched yet)
            if (
                self.buy_order_id
                and self.buy_order_ts
                and now - self.buy_order_ts > self.cfg.order_timeout_seconds
                and self.level == 0  # Only cancel if this is the first order
            ):
                LOGGER.info("Initial buy order %s stale at level 0; cancelling and restarting", self.buy_order_id)
                await self._safe_cancel(self.buy_order_id)
                self._clear_buy_refs()
                # Always restart from beginning after cancelling level 0 order
                await self._reset_locked()
                return

            # Don't cancel buy orders at level > 0 (already have matched orders)
            # Just log if they're stale
            if (
                self.buy_order_id
                and self.buy_order_ts
                and now - self.buy_order_ts > self.cfg.order_timeout_seconds
                and self.level > 0
            ):
                LOGGER.info("Buy order %s stale at level %s; keeping order active (position already open)",
                           self.buy_order_id, self.level)

    async def _handle_buy_fill(self, order_id: Optional[str], price: Decimal, qty: Decimal) -> None:
        self.position_qty += qty
        self.position_cost += qty * price
        self.last_buy_fill_price = price
        remaining_for_order: Optional[Decimal] = None
        is_primary_order = bool(self.buy_order_id and order_id and order_id == self.buy_order_id)

        if is_primary_order:
            self.buy_order_filled += qty
            remaining_for_order = self.buy_order_size - self.buy_order_filled
            if remaining_for_order < 0:
                remaining_for_order = Decimal("0")

        LOGGER.info(
            "Buy filled qty=%s price=%s pos_qty=%s order_remaining=%s",
            qty,
            price,
            self.position_qty,
            remaining_for_order if remaining_for_order is not None else "n/a",
        )

        if self.position_qty > 0:
            await self._place_take_profit()
        else:
            LOGGER.warning("Buy fill resulted in non-positive position qty=%s; resetting", self.position_qty)
            await self._reset_locked()
            return

        order_complete = False
        if is_primary_order:
            tolerance = self.size_increment if self.size_increment > 0 else Decimal("1e-12")
            remaining_for_order = remaining_for_order or Decimal("0")
            order_complete = remaining_for_order <= tolerance / Decimal("2")

        if not order_complete:
            return

        self.level += 1
        cooldown_delay = self._register_buy_cooldown_delay()
        self._clear_buy_refs()
        self._cancel_pending_buy_task()

        self._pending_buy_task = asyncio.create_task(
            self._delayed_place_buy(level=self.level, delay=cooldown_delay)
        )
        LOGGER.info(
            "Scheduled next buy level=%s after %.2fs cooldown", self.level, cooldown_delay
        )

    async def _handle_sell_fill(self, qty: Decimal) -> None:
        # Calculate the proportional cost to reduce
        if self.position_qty > 0:
            avg_price = self.position_cost / self.position_qty
            cost_reduction = qty * avg_price
            self.position_cost -= cost_reduction
            LOGGER.info("Sell filled qty=%s cost_reduction=%s remaining_cost=%s", qty, cost_reduction, self.position_cost)

        self.position_qty -= qty
        LOGGER.info("Sell filled qty=%s remaining=%s", qty, self.position_qty)

        # Check if position should be cleared:
        # 1. Position is fully closed (qty <= 0)
        # 2. OR current price has moved beyond take-profit level (price check)
        should_clear = self.position_qty <= 0

        # If position still exists, check if price has moved too far beyond TP
        if not should_clear and self.position_qty > 0:
            # Calculate TP price
            avg_price = self.position_cost / self.position_qty if self.position_qty > 0 else Decimal("0")
            tp_price = avg_price * (Decimal("1") + self.cfg.profit_pct)

            # If current mid price is above TP + buffer, assume fills are done
            price_buffer = self.cfg.profit_pct * Decimal("0.5")  # 50% of profit margin as buffer
            if self.mid_price > tp_price * (Decimal("1") + price_buffer):
                LOGGER.warning(
                    "Price %s exceeded TP %s by buffer; clearing remaining position %s",
                    self.mid_price, tp_price, self.position_qty
                )
                should_clear = True

        if should_clear:
            self._clear_sell_refs()
            await self._reset_locked()

    async def _reset_locked(self) -> None:
        LOGGER.info("Resetting cycle")
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.last_buy_fill_price = None
        self.level = 0
        self._cancel_pending_buy_task()
        self._clear_buy_refs()
        self._clear_sell_refs()
        self._processed_fills.clear()  # Clear fill tracking on reset
        self._recent_buy_fill_times.clear()
        await self._cancel_all()
        await self._place_buy(level=0)

    async def _place_buy(self, level: int) -> None:
        if self.mid_price <= 0:
            self.mid_price = await self._fetch_mid_price()
        offset_multiplier = self.cfg.price_offset_multiplier ** level if level > 0 else Decimal("1")
        adjusted_offset_pct = self.cfg.price_offset_pct * offset_multiplier
        if adjusted_offset_pct >= Decimal("1"):
            raise ValueError(
                f"Buy offset percentage {adjusted_offset_pct} must be less than 1; "
                "lower PARADEX_PRICE_OFFSET_PCT or PARADEX_PRICE_OFFSET_MULTIPLIER."
            )
        target_price = self.mid_price * (Decimal("1") - adjusted_offset_pct)
        target_price = self._quantize_price(target_price, OrderSide.Buy)

        # Ensure new buy order is always below last buy fill price
        if self.last_buy_fill_price is not None and level > 0:
            if target_price >= self.last_buy_fill_price:
                # Adjust target price to be below last fill using adjusted_offset_pct
                target_price = self.last_buy_fill_price * (Decimal("1") - adjusted_offset_pct)
                target_price = self._quantize_price(target_price, OrderSide.Buy)
                LOGGER.info(
                    "Adjusted buy price from calculated to %s (below last fill %s with offset %.4f%%)",
                    target_price,
                    self.last_buy_fill_price,
                    adjusted_offset_pct * 100
                )

        base_qty = None
        if self.cfg.base_order_size is not None and self.cfg.base_order_size > Decimal("0"):
            base_qty = self.cfg.base_order_size * (self.cfg.size_ratio ** level)
        elif self.cfg.base_order_size_usd is not None and self.cfg.base_order_size_usd > Decimal("0"):
            usd_notional = self.cfg.base_order_size_usd * (self.cfg.size_ratio ** level)
            base_qty = usd_notional / target_price
        else:
            raise ValueError(
                "Configure PARADEX_BASE_ORDER_SIZE (base units) or PARADEX_BASE_ORDER_SIZE_USD with a positive value."
            )

        quantity = self._quantize_size(base_qty)
        quantity = self._enforce_min_notional(quantity, target_price)
        if quantity <= 0 or target_price <= 0:
            raise ValueError("Calculated buy order parameters invalid; adjust configuration.")

        client_id = self._generate_client_id("buy")
        order = Order(
            market=self.market,
            order_type=OrderType.Limit,
            order_side=OrderSide.Buy,
            size=quantity,
            limit_price=target_price,
            client_id=client_id,
        )
        response = await asyncio.to_thread(self.api.submit_order, order)
        order_id = self._extract_order_id(response)
        self.buy_order_id = order_id
        self.buy_client_id = client_id
        self.buy_order_ts = time.time()
        self.buy_order_size = quantity
        self.buy_order_filled = Decimal("0")
        order_notional = quantity * target_price
        LOGGER.info(
            "Placed buy level=%s price=%s qty=%s notional_usd=%s offset_pct=%s id=%s",
            level,
            target_price,
            quantity,
            order_notional,
            adjusted_offset_pct,
            order_id,
        )

    async def _place_take_profit(self) -> None:
        if self.position_qty <= 0:
            LOGGER.warning("Cannot place take profit with position_qty=%s", self.position_qty)
            return
        avg_price = self.position_cost / self.position_qty
        target_price = avg_price * (Decimal("1") + self.cfg.profit_pct)
        target_price = self._quantize_price(target_price, OrderSide.Sell)
        quantity = self._quantize_size(self.position_qty, round_up=True)
        if quantity <= 0 or target_price <= 0:
            LOGGER.warning("Invalid take profit params qty=%s price=%s", quantity, target_price)
            return
        quantity = self._enforce_min_notional(quantity, target_price, round_up=True)
        if quantity <= 0:
            LOGGER.warning("Take profit quantity too small after min_notional enforcement")
            return

        client_id = self._generate_client_id("sell")
        if self.sell_order_id:
            await self._safe_cancel(self.sell_order_id)

        try:
            order = Order(
                market=self.market,
                order_type=OrderType.Limit,
                order_side=OrderSide.Sell,
                size=quantity,
                limit_price=target_price,
                client_id=client_id,
                reduce_only=True,
            )
            response = await asyncio.to_thread(self.api.submit_order, order)
            order_id = self._extract_order_id(response)
            self.sell_order_id = order_id
            self.sell_client_id = client_id
            self.sell_order_ts = time.time()
            LOGGER.info("Placed take profit price=%s qty=%s pos_qty=%s id=%s", target_price, quantity, self.position_qty, order_id)
        except Exception as exc:
            LOGGER.error("Failed to place take profit order qty=%s pos_qty=%s: %s", quantity, self.position_qty, exc)
            # If reduce-only order fails, likely position mismatch - reset
            LOGGER.warning("Resetting bot due to failed take profit order (possible position mismatch)")
            await self._reset_locked()

    async def _fetch_mid_price(self) -> Decimal:
        book = await asyncio.to_thread(self.api.fetch_bbo, self.market)
        bid = book.get("bid")
        ask = book.get("ask")
        if bid is None or ask is None:
            raise RuntimeError("BBO response missing bid/ask; cannot derive mid price.")
        try:
            bid_price = Decimal(str(bid))
            ask_price = Decimal(str(ask))
        except (InvalidOperation, TypeError) as exc:
            raise RuntimeError(f"Invalid BBO values: {book}") from exc
        if bid_price <= 0 or ask_price <= 0:
            raise RuntimeError(f"Non-positive BBO values: {book}")
        return (bid_price + ask_price) / Decimal("2")

    async def _ensure_market_metadata(self) -> None:
        if self._market_meta_loaded:
            return
        response = await asyncio.to_thread(self.api.fetch_markets, {"market": self.market})
        results = response.get("results") if isinstance(response, dict) else None
        if not results:
            raise RuntimeError(f"Market metadata unavailable for {self.market}")

        entry = None
        for candidate in results:
            symbol = candidate.get("symbol")
            if symbol and str(symbol).upper() == self.market.upper():
                entry = candidate
                break
        if entry is None:
            raise RuntimeError(f"Market {self.market} not found in metadata response")

        self.size_increment = self._safe_decimal(entry.get("order_size_increment")) or Decimal("0")
        self.price_increment = self._safe_decimal(entry.get("price_tick_size")) or Decimal("0")
        self.min_notional = self._safe_decimal(entry.get("min_notional")) or Decimal("0")

        LOGGER.info(
            "Market %s metadata size_increment=%s price_increment=%s min_notional=%s",
            self.market,
            self.size_increment,
            self.price_increment,
            self.min_notional,
        )
        self._market_meta_loaded = True

    async def _cancel_all(self) -> None:
        try:
            await asyncio.to_thread(self.api.cancel_all_orders, {"market": self.market})
        except Exception as exc:  # pragma: no cover - network error handling
            LOGGER.warning("cancel_all_orders failed: %s", exc)

    async def _safe_cancel(self, order_id: Optional[str]) -> None:
        if not order_id:
            return
        try:
            await asyncio.to_thread(self.api.cancel_order, order_id)
        except Exception as exc:  # pragma: no cover - network error handling
            LOGGER.warning("Failed to cancel order %s: %s", order_id, exc)

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

    async def _delayed_place_buy(self, level: int, delay: float) -> None:
        current_task = asyncio.current_task()
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            async with self._lock:
                if self.buy_order_id:
                    LOGGER.debug(
                        "Skipping delayed buy placement; active buy order %s exists", self.buy_order_id
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

    def _extract_order_id(self, response: Any) -> Optional[str]:
        if isinstance(response, dict):
            return (
                response.get("id")
                or response.get("order_id")
                or response.get("orderId")
                or response.get("result", {}).get("id")
            )
        return None

    def _matches_buy(self, order_id: Optional[str], client_id: Optional[str]) -> bool:
        match_by_order = bool(self.buy_order_id and order_id and order_id == self.buy_order_id)
        match_by_client = bool(self.buy_client_id and client_id and client_id == self.buy_client_id)
        return match_by_order or match_by_client

    def _matches_sell(self, order_id: Optional[str], client_id: Optional[str]) -> bool:
        match_by_order = bool(self.sell_order_id and order_id and order_id == self.sell_order_id)
        match_by_client = bool(self.sell_client_id and client_id and client_id == self.sell_client_id)
        return match_by_order or match_by_client

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

    def _quantize_price(self, price: Decimal, side: OrderSide) -> Decimal:
        increment = self.price_increment
        if increment <= 0:
            return price
        rounding = ROUND_DOWN if side == OrderSide.Buy else ROUND_UP
        steps = (price / increment).quantize(Decimal("1"), rounding=rounding)
        if steps <= 0:
            steps = Decimal("1")
        quantized = steps * increment
        return quantized

    def _enforce_min_notional(
        self,
        qty: Decimal,
        price: Decimal,
        *,
        round_up: bool = False,
    ) -> Decimal:
        if self.min_notional <= 0:
            return qty
        notional = qty * price
        if notional >= self.min_notional:
            return qty
        required_qty = self.min_notional / price
        adjusted = self._quantize_size(required_qty, round_up=True)
        return adjusted if round_up or adjusted <= qty else adjusted

    @staticmethod
    def _generate_client_id(prefix: str) -> str:
        return f"{prefix}-{secrets.token_hex(6)}"

    @staticmethod
    def _extract_data(message: dict[str, Any]) -> Any:
        if not isinstance(message, dict):
            return None
        data = message.get("params", {}).get("data")
        if data is None:
            data = message.get("data")
        if data is None:
            data = message.get("result")
        return data

    @staticmethod
    def _safe_decimal(value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError):
            return None

    def _clear_buy_refs(self) -> None:
        self.buy_order_id = None
        self.buy_client_id = None
        self.buy_order_ts = None
        self.buy_order_size = Decimal("0")
        self.buy_order_filled = Decimal("0")

    def _clear_sell_refs(self) -> None:
        self.sell_order_id = None
        self.sell_client_id = None
        self.sell_order_ts = None


async def _subscribe_streams(ws: ParadexWebsocketClient, bot: ParadexGridBot, market: str) -> None:
    async def ticker_callback(_: ParadexWebsocketChannel, msg: dict[str, Any]) -> None:
        await bot.on_ticker(msg)

    async def order_callback(_: ParadexWebsocketChannel, msg: dict[str, Any]) -> None:
        await bot.on_order(msg)

    async def fill_callback(_: ParadexWebsocketChannel, msg: dict[str, Any]) -> None:
        await bot.on_fill(msg)

    await ws.subscribe(ParadexWebsocketChannel.BBO, ticker_callback, params={"market": market})
    await ws.subscribe(ParadexWebsocketChannel.ORDERS, order_callback, params={"market": market})
    await ws.subscribe(ParadexWebsocketChannel.FILLS, fill_callback, params={"market": market})


def _normalize_market(symbol: str) -> str:
    value = (symbol or "BTC-USD-PERP").strip().upper()
    if "_" in value:
        value = value.replace("_", "-")
    if value.endswith("PERP") and "-PERP" not in value:
        value = value.replace("PERP", "-PERP")
    if value.count("-") == 1:
        base, quote = value.split("-")
        return f"{base}-{quote}-PERP"
    return value


async def main() -> None:
    load_dotenv()

    market = _normalize_market(os.environ.get("PARADEX_MARKET", "BTC-USD-PERP"))
    base_order_size_env = os.environ.get("PARADEX_BASE_ORDER_SIZE")
    base_order_size = Decimal(base_order_size_env) if base_order_size_env else None

    base_order_size_usd_env = os.environ.get("PARADEX_BASE_ORDER_SIZE_USD")
    if base_order_size is None:
        base_order_size_usd = Decimal(base_order_size_usd_env or "100")
    else:
        base_order_size_usd = Decimal(base_order_size_usd_env) if base_order_size_usd_env else None
    price_offset_pct = Decimal(os.environ.get("PARADEX_PRICE_OFFSET_PCT", "0.00015"))
    profit_pct = Decimal(os.environ.get("PARADEX_PROFIT_PCT", "0.00015"))
    price_offset_multiplier = Decimal(os.environ.get("PARADEX_PRICE_OFFSET_MULTIPLIER", "1"))
    size_ratio = Decimal(os.environ.get("PARADEX_SIZE_RATIO", "1"))
    order_timeout = float(os.environ.get("PARADEX_ORDER_TIMEOUT_SECONDS", "10"))

    l2_private_key = os.environ.get("PARADEX_L2_PRIVATE_KEY") or os.environ.get("L2_PRIVATE_KEY")
    l2_address = os.environ.get("PARADEX_L2_ADDRESS") or os.environ.get("L2_ADDRESS")

    if not l2_private_key:
        raise RuntimeError("Set PARADEX_L2_PRIVATE_KEY or L2_PRIVATE_KEY for subkey authentication.")
    if not l2_address:
        raise RuntimeError("Set PARADEX_L2_ADDRESS or L2_ADDRESS for subkey authentication.")

    env_literal: Environment = "prod"

    LOGGER.info("Initializing Paradex client with L2 subkey credentials.")
    client = await asyncio.to_thread(
        ParadexSubkey,
        env_literal,
        l2_private_key,
        l2_address,
        LOGGER,
    )

    cfg = GridConfig(
        market=market,
        base_order_size=base_order_size,
        base_order_size_usd=base_order_size_usd,
        price_offset_pct=price_offset_pct,
        price_offset_multiplier=price_offset_multiplier,
        profit_pct=profit_pct,
        size_ratio=size_ratio,
        order_timeout_seconds=order_timeout,
    )
    bot = ParadexGridBot(client, cfg)
    await bot.start()

    ws_client = client.ws_client
    await ws_client.connect()
    await asyncio.sleep(1)
    await _subscribe_streams(ws_client, bot, market)

    LOGGER.info("Paradex grid bot running on %s (%s)", market, env_literal)
    try:
        while True:
            await asyncio.sleep(1)
            await bot.check_stale_orders()
    except asyncio.CancelledError:  # pragma: no cover - runtime shutdown
        pass
    except KeyboardInterrupt:
        LOGGER.info("Stopping bot")


if __name__ == "__main__":
    asyncio.run(main())
