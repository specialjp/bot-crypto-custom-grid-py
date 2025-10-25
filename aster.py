"""
Aster futures martingale/grid bot that mirrors the Paradex cooldown logic.

Environment variables:
    ASTER_API_KEY (required)
    ASTER_API_SECRET (required)
    ASTER_BASE_URL (default: https://fapi.asterdex.com)
    ASTER_STREAM_URL (default: wss://fstream.asterdex.com)
    ASTER_SYMBOL (default: BTCUSDT)
    ASTER_BASE_ORDER_SIZE (base units, optional)
    ASTER_BASE_ORDER_SIZE_USD (default: 100 when base units not provided)
    ASTER_PRICE_OFFSET_PCT (default: 0.00015)
    ASTER_PRICE_OFFSET_MULTIPLIER (default: 1)
    ASTER_PROFIT_PCT (default: 0.0002)
    ASTER_SIZE_RATIO (default: 2)
    ASTER_ORDER_TIMEOUT_SECONDS (default: 30)

The bot uses REST endpoints for order management and the streaming websocket for
book-ticker pricing plus user-data fills. Orders are placed as post-only limit
orders with staggered cooldowns of 5 s / 10 s / 15 s within a 60 second window.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import secrets
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from typing import Any, Coroutine, Optional

from dotenv import load_dotenv

from aster.rest_api import Client as AsterRestClient
from aster.websocket.client.stream import WebsocketClient as AsterWsClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("aster-grid")


@dataclass(slots=True)
class GridConfig:
    symbol: str
    base_order_size: Optional[Decimal] = None
    base_order_size_usd: Optional[Decimal] = Decimal("100")
    price_offset_pct: Decimal = Decimal("0.00015")
    price_offset_multiplier: Decimal = Decimal("1")
    profit_pct: Decimal = Decimal("0.0002")
    size_ratio: Decimal = Decimal("2")
    order_timeout_seconds: float = 30.0


class AsterGridBot:
    BUY_COOLDOWN_WINDOW_SECONDS = 60.0
    BUY_COOLDOWN_STEP_SECONDS = 5.0

    def __init__(self, client: AsterRestClient, config: GridConfig):
        self.client = client
        self.cfg = config
        if self.cfg.price_offset_multiplier <= 0:
            raise ValueError("ASTER_PRICE_OFFSET_MULTIPLIER must be positive.")

        self.symbol = config.symbol.upper()
        self.symbol_stream = self.symbol.lower()

        self.mid_price = Decimal("0")
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.level = 0

        self.buy_order_id: Optional[int] = None
        self.sell_order_id: Optional[int] = None
        self.buy_client_id: Optional[str] = None
        self.sell_client_id: Optional[str] = None
        self.buy_order_ts: Optional[float] = None
        self.sell_order_ts: Optional[float] = None

        self.size_increment = Decimal("0")
        self.price_increment = Decimal("0")
        self.min_base_amount = Decimal("0")
        self.min_quote_amount = Decimal("0")
        self._market_meta_loaded = False

        self._lock = asyncio.Lock()
        self._processed_fills: set[str] = set()
        self._pending_buy_task: Optional[asyncio.Task] = None
        self._recent_buy_fill_times: list[float] = []

    async def start(self) -> None:
        async with self._lock:
            await self._ensure_market_metadata()
            self.mid_price = await self._fetch_mid_price()
            LOGGER.info("Starting Aster grid on %s mid price %s", self.symbol, self.mid_price)
            await self._cancel_all()
            await self._place_buy(level=0)

    async def on_book_ticker(self, message: dict[str, Any]) -> None:
        bid = message.get("b") or message.get("bidPrice")
        ask = message.get("a") or message.get("askPrice")
        if bid is None or ask is None:
            return
        try:
            bid_dec = Decimal(str(bid))
            ask_dec = Decimal(str(ask))
        except (InvalidOperation, TypeError):
            return
        if bid_dec <= 0 or ask_dec <= 0:
            return
        mid = (bid_dec + ask_dec) / Decimal("2")
        async with self._lock:
            self.mid_price = mid

    async def on_user_data(self, message: dict[str, Any]) -> None:
        event_type = str(message.get("e") or "").upper()
        if event_type != "ORDER_TRADE_UPDATE":
            return
        order = message.get("o") or {}
        side = str(order.get("S") or "").upper()
        status = str(order.get("X") or "").upper()
        execution_type = str(order.get("x") or "").upper()
        order_id = self._safe_int(order.get("i") or order.get("orderId"))
        client_id = order.get("c") or order.get("clientOrderId")
        trade_qty = order.get("l") or order.get("lastFilledQty")
        trade_price = order.get("L") or order.get("lastFilledPrice") or order.get("ap")

        async with self._lock:
            if side == "BUY" and self._matches_buy(order_id, client_id):
                if execution_type == "TRADE":
                    qty = self._safe_decimal(trade_qty)
                    price = self._safe_decimal(trade_price) or self.mid_price
                    if qty and qty > 0 and price > 0:
                        fill_id = f"{order_id}-{order.get('T')}"
                        if fill_id in self._processed_fills:
                            return
                        self._processed_fills.add(fill_id)
                        await self._handle_buy_fill(price, qty)
                if status in {"CANCELED", "FILLED", "REJECTED", "EXPIRED"}:
                    self._clear_buy_refs()
            elif side == "SELL" and self._matches_sell(order_id, client_id):
                if execution_type == "TRADE":
                    qty = self._safe_decimal(trade_qty)
                    price = self._safe_decimal(trade_price) or self.mid_price
                    if qty and qty > 0 and price > 0:
                        fill_id = f"{order_id}-{order.get('T')}"
                        if fill_id in self._processed_fills:
                            return
                        self._processed_fills.add(fill_id)
                        await self._handle_sell_fill(qty)
                if status in {"CANCELED", "FILLED", "REJECTED", "EXPIRED"}:
                    self._clear_sell_refs()

    async def check_stale_orders(self) -> None:
        now = time.time()
        async with self._lock:
            if (
                self.buy_order_id is not None
                and self.buy_order_ts
                and now - self.buy_order_ts > self.cfg.order_timeout_seconds
                and self.level == 0
            ):
                LOGGER.info("Initial buy order %s stale; cancelling and restarting", self.buy_order_id)
                await self._cancel_buy_order()
                await self._reset_locked()
                return
            if (
                self.buy_order_id is not None
                and self.buy_order_ts
                and now - self.buy_order_ts > self.cfg.order_timeout_seconds
                and self.level > 0
            ):
                LOGGER.info(
                    "Buy order %s stale at level %s; keeping active (position exists)",
                    self.buy_order_id,
                    self.level,
                )
            if (
                self.sell_order_id is not None
                and self.sell_order_ts
                and now - self.sell_order_ts > self.cfg.order_timeout_seconds
            ):
                LOGGER.info("Take-profit order %s stale; attempting re-place", self.sell_order_id)
                self._clear_sell_refs()
                await self._place_take_profit()

    async def _handle_buy_fill(self, price: Decimal, qty: Decimal) -> None:
        self.position_qty += qty
        self.position_cost += qty * price
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

        await self._place_take_profit()
        if self.position_qty > 0:
            self._pending_buy_task = asyncio.create_task(self._delayed_place_buy(self.level, delay))
            LOGGER.info("Scheduled next buy level=%s after %.2fs cooldown", self.level, delay)
        else:
            LOGGER.warning("Position qty non-positive after buy fill; resetting")
            await self._reset_locked()

    async def _handle_sell_fill(self, qty: Decimal) -> None:
        self.position_qty -= qty
        LOGGER.info("Sell filled qty=%s remaining=%s", qty, self.position_qty)
        if self.position_qty <= 0:
            await self._reset_locked()
        else:
            await self._place_take_profit()

    async def _reset_locked(self) -> None:
        LOGGER.info("Resetting cycle")
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.level = 0
        self._cancel_pending_buy_task()
        self._clear_buy_refs()
        self._clear_sell_refs()
        self._processed_fills.clear()
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
                "lower ASTER_PRICE_OFFSET_PCT or ASTER_PRICE_OFFSET_MULTIPLIER."
            )
        target_price = self.mid_price * (Decimal("1") - adjusted_offset_pct)
        target_price = self._quantize_price(target_price, is_buy=True)

        base_qty = None
        if self.cfg.base_order_size is not None and self.cfg.base_order_size > Decimal("0"):
            base_qty = self.cfg.base_order_size * (self.cfg.size_ratio ** level)
        elif self.cfg.base_order_size_usd is not None and self.cfg.base_order_size_usd > Decimal("0"):
            usd_notional = self.cfg.base_order_size_usd * (self.cfg.size_ratio ** level)
            base_qty = usd_notional / target_price
        else:
            raise ValueError(
                "Configure ASTER_BASE_ORDER_SIZE (base units) or ASTER_BASE_ORDER_SIZE_USD with a positive value."
            )

        quantity = self._quantize_size(base_qty)
        quantity = self._enforce_minimums(quantity, target_price)
        if quantity <= 0 or target_price <= 0:
            raise ValueError("Calculated buy order parameters invalid; adjust configuration.")

        client_id = self._generate_client_id("buy")
        payload = {
            "symbol": self.symbol,
            "side": "BUY",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "price": self._decimal_to_str(target_price),
            "quantity": self._decimal_to_str(quantity),
            "newClientOrderId": client_id,
            "reduceOnly": False,
        }

        response = await asyncio.to_thread(self.client.new_order, **payload)
        order_id = self._safe_int(response.get("orderId") or response.get("orderID"))
        if order_id is None:
            raise RuntimeError(f"Buy order response missing orderId: {response}")

        self.buy_order_id = order_id
        self.buy_client_id = client_id
        self.buy_order_ts = time.time()
        order_notional = quantity * target_price
        LOGGER.info(
            "Placed buy level=%s price=%s qty=%s notional=%s order_id=%s",
            level,
            target_price,
            quantity,
            order_notional,
            order_id,
        )

    async def _place_take_profit(self) -> None:
        if self.position_qty <= 0:
            return
        avg_price = self.position_cost / self.position_qty if self.position_qty > 0 else Decimal("0")
        target_price = avg_price * (Decimal("1") + self.cfg.profit_pct)
        target_price = self._quantize_price(target_price, is_buy=False)
        quantity = self._quantize_size(self.position_qty, round_up=True)
        quantity = self._enforce_minimums(quantity, target_price, round_up=True)
        if quantity <= 0 or target_price <= 0:
            LOGGER.warning("Invalid take profit parameters qty=%s price=%s", quantity, target_price)
            return

        await self._cancel_take_profit()

        client_id = self._generate_client_id("sell")
        payload = {
            "symbol": self.symbol,
            "side": "SELL",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "price": self._decimal_to_str(target_price),
            "quantity": self._decimal_to_str(quantity),
            "newClientOrderId": client_id,
            "reduceOnly": True,
        }

        try:
            response = await asyncio.to_thread(self.client.new_order, **payload)
        except Exception as exc:
            LOGGER.error("Failed to submit take profit order: %s", exc)
            return

        order_id = self._safe_int(response.get("orderId") or response.get("orderID"))
        if order_id is None:
            LOGGER.warning("Take profit response missing orderId: %s", response)
            return

        self.sell_order_id = order_id
        self.sell_client_id = client_id
        self.sell_order_ts = time.time()
        LOGGER.info(
            "Placed take profit price=%s qty=%s order_id=%s",
            target_price,
            quantity,
            order_id,
        )

    async def _cancel_all(self) -> None:
        try:
            await asyncio.to_thread(self.client.cancel_open_orders, self.symbol)
        except Exception as exc:
            LOGGER.warning("cancel_open_orders failed: %s", exc)

    async def _cancel_buy_order(self) -> None:
        if self.buy_order_id is None and not self.buy_client_id:
            return
        params: dict[str, Any] = {"symbol": self.symbol}
        if self.buy_order_id is not None:
            params["orderId"] = self.buy_order_id
        elif self.buy_client_id:
            params["origClientOrderId"] = self.buy_client_id
        try:
            await asyncio.to_thread(self.client.cancel_order, **params)
        except Exception as exc:
            LOGGER.warning("Failed to cancel buy order %s: %s", params, exc)
        self._clear_buy_refs()

    async def _cancel_take_profit(self) -> None:
        if self.sell_order_id is None and not self.sell_client_id:
            return
        params: dict[str, Any] = {"symbol": self.symbol}
        if self.sell_order_id is not None:
            params["orderId"] = self.sell_order_id
        elif self.sell_client_id:
            params["origClientOrderId"] = self.sell_client_id
        try:
            await asyncio.to_thread(self.client.cancel_order, **params)
        except Exception as exc:
            LOGGER.warning("Failed to cancel sell order %s: %s", params, exc)
        self._clear_sell_refs()

    async def _delayed_place_buy(self, level: int, delay: float) -> None:
        current_task = asyncio.current_task()
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            async with self._lock:
                if self.buy_order_id is not None or self.buy_client_id is not None:
                    LOGGER.debug(
                        "Skipping delayed buy placement; active buy refs order_id=%s client_id=%s",
                        self.buy_order_id,
                        self.buy_client_id,
                    )
                    return
                await self._place_buy(level=self.level)
        except asyncio.CancelledError:
            LOGGER.debug("Delayed buy placement cancelled for level %s", level)
            raise
        except Exception as exc:
            LOGGER.exception("Error placing delayed buy order at level %s: %s", level, exc)
        finally:
            if current_task and self._pending_buy_task is current_task:
                self._pending_buy_task = None

    async def _ensure_market_metadata(self) -> None:
        if self._market_meta_loaded:
            return
        response = await asyncio.to_thread(self.client.exchange_info)
        symbols = response.get("symbols") if isinstance(response, dict) else None
        if not symbols:
            raise RuntimeError("Aster exchange metadata unavailable")
        entry = None
        for candidate in symbols:
            if not isinstance(candidate, dict):
                continue
            symbol_name = str(candidate.get("symbol") or "")
            if symbol_name.upper() == self.symbol:
                entry = candidate
                break
        if entry is None:
            raise RuntimeError(f"Symbol {self.symbol} not found in exchange info")

        filters = {f.get("filterType"): f for f in entry.get("filters", []) if isinstance(f, dict)}

        price_filter = filters.get("PRICE_FILTER")
        if price_filter:
            tick = self._safe_decimal(price_filter.get("tickSize"))
            if tick and tick > 0:
                self.price_increment = tick

        lot_filter = filters.get("LOT_SIZE")
        if lot_filter:
            step = self._safe_decimal(lot_filter.get("stepSize"))
            if step and step > 0:
                self.size_increment = step
            min_qty = self._safe_decimal(lot_filter.get("minQty"))
            if min_qty and min_qty > 0:
                self.min_base_amount = min_qty

        min_notional_filter = filters.get("MIN_NOTIONAL")
        if min_notional_filter:
            notional = self._safe_decimal(min_notional_filter.get("notional") or min_notional_filter.get("minNotional"))
            if notional and notional > 0:
                self.min_quote_amount = notional

        self._market_meta_loaded = True

    async def _fetch_mid_price(self) -> Decimal:
        response = await asyncio.to_thread(self.client.book_ticker, self.symbol)
        bid = response.get("bidPrice") or response.get("b")
        ask = response.get("askPrice") or response.get("a")
        if bid is None or ask is None:
            raise RuntimeError("Book ticker response missing bid/ask")
        try:
            bid_dec = Decimal(str(bid))
            ask_dec = Decimal(str(ask))
        except (InvalidOperation, TypeError) as exc:
            raise RuntimeError(f"Invalid book ticker values: {response}") from exc
        if bid_dec <= 0 or ask_dec <= 0:
            raise RuntimeError(f"Non-positive book ticker values: {response}")
        return (bid_dec + ask_dec) / Decimal("2")

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
        return steps * increment

    def _enforce_minimums(self, qty: Decimal, price: Decimal, *, round_up: bool = False) -> Decimal:
        if qty <= 0 or price <= 0:
            return Decimal("0")
        if self.min_base_amount > 0 and qty < self.min_base_amount:
            qty = self._quantize_size(self.min_base_amount, round_up=True)
        notional = qty * price
        if self.min_quote_amount > 0 and notional < self.min_quote_amount:
            required_qty = self.min_quote_amount / price
            qty = self._quantize_size(required_qty, round_up=True)
        return qty

    def _matches_buy(self, order_id: Optional[int], client_id: Optional[str]) -> bool:
        match_by_order = self.buy_order_id is not None and order_id is not None and order_id == self.buy_order_id
        match_by_client = self.buy_client_id is not None and client_id is not None and client_id == self.buy_client_id
        return match_by_order or match_by_client

    def _matches_sell(self, order_id: Optional[int], client_id: Optional[str]) -> bool:
        match_by_order = self.sell_order_id is not None and order_id is not None and order_id == self.sell_order_id
        match_by_client = self.sell_client_id is not None and client_id is not None and client_id == self.sell_client_id
        return match_by_order or match_by_client

    def _clear_buy_refs(self) -> None:
        self.buy_order_id = None
        self.buy_client_id = None
        self.buy_order_ts = None

    def _clear_sell_refs(self) -> None:
        self.sell_order_id = None
        self.sell_client_id = None
        self.sell_order_ts = None

    @staticmethod
    def _generate_client_id(prefix: str) -> str:
        return f"{prefix}-{secrets.token_hex(6)}"

    @staticmethod
    def _decimal_to_str(value: Decimal) -> str:
        return format(value.normalize(), "f")

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


async def _keep_listen_key_alive(client: AsterRestClient, symbol: str, listen_key: str) -> None:
    while True:
        await asyncio.sleep(30 * 60)
        try:
            await asyncio.to_thread(client.renew_listen_key, listen_key)
        except Exception as exc:
            LOGGER.warning("Failed to renew listen key for %s: %s", symbol, exc)


async def main() -> None:
    load_dotenv()

    api_key = os.environ.get("ASTER_API_KEY")
    api_secret = os.environ.get("ASTER_API_SECRET")
    if not api_key or not api_secret:
        raise RuntimeError("Set ASTER_API_KEY and ASTER_API_SECRET for authentication.")

    symbol = os.environ.get("ASTER_SYMBOL", "BTCUSDT").upper()
    base_url = os.environ.get("ASTER_BASE_URL", "https://fapi.asterdex.com")
    stream_url = os.environ.get("ASTER_STREAM_URL", "wss://fstream.asterdex.com")

    base_order_size_env = os.environ.get("ASTER_BASE_ORDER_SIZE")
    base_order_size = Decimal(base_order_size_env) if base_order_size_env else None

    base_order_size_usd_env = os.environ.get("ASTER_BASE_ORDER_SIZE_USD")
    if base_order_size is None:
        base_order_size_usd = Decimal(base_order_size_usd_env or "100")
    else:
        base_order_size_usd = Decimal(base_order_size_usd_env) if base_order_size_usd_env else None

    price_offset_pct = Decimal(os.environ.get("ASTER_PRICE_OFFSET_PCT", "0.00015"))
    price_offset_multiplier = Decimal(os.environ.get("ASTER_PRICE_OFFSET_MULTIPLIER", "1"))
    profit_pct = Decimal(os.environ.get("ASTER_PROFIT_PCT", "0.0002"))
    size_ratio = Decimal(os.environ.get("ASTER_SIZE_RATIO", "2"))
    order_timeout = float(os.environ.get("ASTER_ORDER_TIMEOUT_SECONDS", "30"))

    client = AsterRestClient(key=api_key, secret=api_secret, base_url=base_url)

    cfg = GridConfig(
        symbol=symbol,
        base_order_size=base_order_size,
        base_order_size_usd=base_order_size_usd,
        price_offset_pct=price_offset_pct,
        price_offset_multiplier=price_offset_multiplier,
        profit_pct=profit_pct,
        size_ratio=size_ratio,
        order_timeout_seconds=order_timeout,
    )

    bot = AsterGridBot(client, cfg)
    await bot.start()

    listen_key_resp = await asyncio.to_thread(client.new_listen_key)
    listen_key = listen_key_resp.get("listenKey")
    if not listen_key:
        raise RuntimeError("Failed to obtain Aster listen key")

    ws_client = AsterWsClient(stream_url=stream_url)
    ws_client.start()
    await asyncio.sleep(1)  # give the reactor thread time to spin up

    loop = asyncio.get_running_loop()

    def _schedule_coro(coro: Coroutine[Any, Any, None]) -> None:
        asyncio.run_coroutine_threadsafe(coro, loop)

    ws_client.book_ticker(
        id=1,
        callback=lambda msg: _schedule_coro(bot.on_book_ticker(msg)),
        symbol=bot.symbol_stream,
    )

    ws_client.user_data(
        listen_key,
        id=2,
        callback=lambda msg: _schedule_coro(bot.on_user_data(msg)),
    )

    keep_alive_task = asyncio.create_task(_keep_listen_key_alive(client, symbol, listen_key))

    LOGGER.info("Aster grid bot running on %s", symbol)
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
        keep_alive_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await keep_alive_task
        with contextlib.suppress(Exception):
            ws_client.stop()


if __name__ == "__main__":
    asyncio.run(main())
