"""
GRVT martingale/grid bot powered by pysdk.grvt_ccxt and websocket feeds.

Environment variables:
    GRVT_API_KEY
    GRVT_PRIVATE_KEY
    GRVT_TRADING_ACCOUNT_ID
    GRVT_MARKET (default: BTC_USDT_Perp)
    GRVT_ENV (default: testnet)
    GRVT_BASE_ORDER_SIZE (default: 0.001)
    GRVT_MIN_BASE_QTY (default: 0.001)
    GRVT_PRICE_OFFSET_PCT (default: 0.00015)
    GRVT_SIZE_RATIO (default: 2)
    GRVT_ORDER_TIMEOUT_SECONDS (default: 30)
    GRVT_WS_STREAM_VERSION (default: v1)

Requires the pysdk package that ships the GrvtCcxt/GrvtCcxtWS clients.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_UP
from typing import Any, Optional

from dotenv import load_dotenv
from pysdk.grvt_ccxt import GrvtCcxt
from pysdk.grvt_ccxt_env import GrvtEnv, GrvtWSEndpointType
from pysdk.grvt_ccxt_ws import GrvtCcxtWS
from pysdk.grvt_ccxt_utils import rand_uint32

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("grvt-grid")


@dataclass(slots=True)
class GridConfig:
    instrument: str
    base_order_size: Decimal = Decimal("0.001")
    price_offset_pct: Decimal = Decimal("0.00015")
    size_ratio: Decimal = Decimal("2")
    order_timeout_seconds: float = 30.0
    min_base_qty: Decimal = Decimal("0.001")


class GrvtGridBot:
    def __init__(self, api: GrvtCcxt, config: GridConfig):
        self.api = api
        self.cfg = config

        self.mid_price = Decimal("0")
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.level = 0

        self.buy_order_id: Optional[str] = None
        self.sell_order_id: Optional[str] = None
        self.buy_client_id: Optional[str] = None
        self.sell_client_id: Optional[str] = None
        self.buy_order_ts: Optional[float] = None
        self.sell_order_ts: Optional[float] = None

        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            self.mid_price = await self._fetch_mid_price()
            LOGGER.info("Starting grid at mid price %s", self.mid_price)
            await self._cancel_all()
            # await self._place_buy(level=0)

    async def on_ticker(self, message: dict[str, Any]) -> None:
        data = self._extract_data(message)
        if not data:
            return
        if isinstance(data, list):
            data = data[0]
        bid = data.get("bestBid") or data.get("bid")
        ask = data.get("bestAsk") or data.get("ask")
        if not bid or not ask:
            return
        try:
            mid = (Decimal(str(bid)) + Decimal(str(ask))) / Decimal("2")
        except (InvalidOperation, TypeError) as exc:
            LOGGER.debug("Failed to parse ticker %s: %s", message, exc)
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
                status = (entry.get("status") or entry.get("order_status") or "").lower()
                if status not in {"filled", "closed", "matched"}:
                    continue
                side = (entry.get("side") or "").lower()
                order_id = entry.get("id") or entry.get("order_id") or entry.get("orderId")
                client_id = (
                    entry.get("client_order_id")
                    or entry.get("clientOrderId")
                    or entry.get("metadata", {}).get("client_order_id")
                )
                price = (
                    entry.get("avg_price")
                    or entry.get("avgPrice")
                    or entry.get("price")
                    or entry.get("fill_price")
                )
                qty = (
                    entry.get("filledSize")
                    or entry.get("fill_size")
                    or entry.get("size")
                    or entry.get("quantity")
                    or entry.get("fillQuantity")
                )
                if not side or not qty or not price:
                    continue
                try:
                    price_dec = Decimal(str(price))
                    qty_dec = Decimal(str(qty))
                except (InvalidOperation, TypeError):
                    continue

                if side == "buy" and self._matches_buy(order_id, client_id):
                    await self._handle_buy_fill(price_dec, qty_dec)
                elif side == "sell" and self._matches_sell(order_id, client_id):
                    await self._handle_sell_fill(qty_dec)

    async def check_stale_orders(self) -> None:
        now = time.time()
        async with self._lock:
            if (
                self.buy_order_id
                and self.buy_order_ts
                and now - self.buy_order_ts > self.cfg.order_timeout_seconds
            ):
                LOGGER.info("Buy order %s stale; cancelling and resetting", self.buy_order_id)
                await self._safe_cancel(self.buy_order_id)
                await self._reset_locked()
                return
            if (
                self.sell_order_id
                and self.sell_order_ts
                and now - self.sell_order_ts > self.cfg.order_timeout_seconds
            ):
                LOGGER.info("Sell order %s stale; cancelling and resetting", self.sell_order_id)
                await self._safe_cancel(self.sell_order_id)
                await self._reset_locked()

    async def _handle_buy_fill(self, price: Decimal, qty: Decimal) -> None:
        self.position_qty += qty
        self.position_cost += qty * price
        self.level += 1
        self.buy_order_ts = None
        LOGGER.info("Buy filled qty=%s price=%s level=%s", qty, price, self.level)

        await self._place_take_profit()
        await self._place_buy(level=self.level)

    async def _handle_sell_fill(self, qty: Decimal) -> None:
        self.position_qty -= qty
        self.sell_order_ts = None
        LOGGER.info("Sell filled qty=%s remaining=%s", qty, self.position_qty)
        if self.position_qty <= 0:
            await self._reset_locked()

    async def _reset_locked(self) -> None:
        LOGGER.info("Resetting cycle")
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.level = 0
        self.buy_order_id = None
        self.sell_order_id = None
        self.buy_client_id = None
        self.sell_client_id = None
        self.buy_order_ts = None
        self.sell_order_ts = None
        await self._cancel_all()
        await self._place_buy(level=0)

    async def _place_buy(self, level: int) -> None:
        # if self.mid_price <= 0:
        #     self.mid_price = await self._fetch_mid_price()
        # target_price = self.mid_price * (Decimal("1") - self.cfg.price_offset_pct)
        # base_qty = self.cfg.base_order_size * (self.cfg.size_ratio ** level)
        # quantity = self._quantize_base_qty(base_qty)
        # if quantity <= 0:
        #     raise ValueError("Calculated quantity is non-positive; adjust configuration.")
        # client_order_id = str(rand_uint32())
        # response = await asyncio.to_thread(
        #     self.api.create_order,
        #     symbol=self.cfg.instrument,
        #     order_type="limit",
        #     side="buy",
        #     amount=float(quantity),
        #     price=float(target_price),
        #     params={"client_order_id": client_order_id},
        # )
        # order_id = self._extract_order_id(response)
        # self.buy_order_id = order_id
        # self.buy_client_id = client_order_id
        # self.buy_order_ts = time.time()
        LOGGER.info(
            "Placed buy level=%s price=%s qty=%s id=%s",
            level,
            # target_price,
            # quantity,
            # order_id,
        )

    async def _place_take_profit(self) -> None:
        if self.position_qty <= 0:
            return
        avg_price = self.position_cost / self.position_qty
        target_price = avg_price * (Decimal("1") + self.cfg.price_offset_pct)
        quantity = self._quantize_base_qty(self.position_qty)
        client_order_id = str(rand_uint32())
        if self.sell_order_id:
            await self._safe_cancel(self.sell_order_id)
        response = await asyncio.to_thread(
            self.api.create_order,
            symbol=self.cfg.instrument,
            order_type="limit",
            side="sell",
            amount=float(quantity),
            price=float(target_price),
            params={"client_order_id": client_order_id, "reduce_only": True},
        )
        order_id = self._extract_order_id(response)
        self.sell_order_id = order_id
        self.sell_client_id = client_order_id
        self.sell_order_ts = time.time()
        LOGGER.info("Placed take profit price=%s qty=%s id=%s", target_price, quantity, order_id)

    async def _fetch_mid_price(self) -> Decimal:
        book = await asyncio.to_thread(
            self.api.fetch_order_book,
            self.cfg.instrument,
            limit=10,
        )
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        if not bids or not asks:
            raise RuntimeError("Order book empty; cannot derive mid price.")

        # GRVT returns bids/asks as list of dicts with 'price', 'size', 'num_orders'
        bid_price = Decimal(str(bids[0]['price']))
        ask_price = Decimal(str(asks[0]['price']))
        return (bid_price + ask_price) / Decimal("2")

    async def _cancel_all(self) -> None:
        try:
            await asyncio.to_thread(self.api.cancel_all_orders)
        except Exception as exc:
            LOGGER.warning("cancel_all_orders failed: %s", exc)

    async def _safe_cancel(self, order_id: Optional[str]) -> None:
        if not order_id:
            return
        try:
            await asyncio.to_thread(self.api.cancel_order, id=order_id)
        except Exception as exc:
            LOGGER.warning("Failed to cancel order %s: %s", order_id, exc)

    def _extract_order_id(self, response: Any) -> Optional[str]:
        if isinstance(response, dict):
            return (
                response.get("id")
                or response.get("order_id")
                or response.get("orderId")
                or response.get("info", {}).get("order_id")
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

    def _quantize_base_qty(self, qty: Decimal) -> Decimal:
        if self.cfg.min_base_qty <= 0:
            return qty
        steps = (qty / self.cfg.min_base_qty).quantize(Decimal("1"), rounding=ROUND_UP)
        return max(steps * self.cfg.min_base_qty, self.cfg.min_base_qty)

    @staticmethod
    def _extract_data(message: dict[str, Any]) -> Any:
        if not isinstance(message, dict):
            return None
        return (
            message.get("params", {}).get("data")
            or message.get("params", {}).get("result")
            or message.get("data")
            or message.get("result")
        )


async def _subscribe_streams(ws: GrvtCcxtWS, bot: GrvtGridBot, instrument: str) -> None:
    async def ticker_callback(msg: dict[str, Any]) -> None:
        await bot.on_ticker(msg)

    async def order_callback(msg: dict[str, Any]) -> None:
        await bot.on_order(msg)

    await ws.subscribe(
        stream="ticker.s",
        callback=ticker_callback,
        ws_end_point_type=GrvtWSEndpointType.MARKET_DATA_RPC_FULL,
        params={"instrument": instrument},
    )

    await ws.subscribe(
        stream="fill",
        callback=order_callback,
        ws_end_point_type=GrvtWSEndpointType.TRADE_DATA_RPC_FULL,
        params={"instrument": instrument},
    )


async def main() -> None:
    load_dotenv()

    api_key = os.environ.get("GRVT_API_KEY")
    trading_account_id = os.environ.get("GRVT_TRADING_ACCOUNT_ID")
    private_key = os.environ.get("GRVT_PRIVATE_KEY")
    instrument = os.environ.get("GRVT_MARKET") or "BTC_USDT_Perp"
    env_name = os.environ.get("GRVT_ENV", "testnet")
    order_timeout = float(os.environ.get("ORDER_TIMEOUT_SECONDS", "30"))
    base_order_size = Decimal(os.environ.get("BASE_ORDER_SIZE", "0.001"))
    min_base_qty = Decimal(os.environ.get("MIN_BASE_QTY", "0.001"))
    price_offset_pct = Decimal(os.environ.get("PRICE_OFFSET_PCT", "0.00015"))
    size_ratio = Decimal(os.environ.get("SIZE_RATIO", "2"))

    if not api_key or not trading_account_id or not private_key:
        raise RuntimeError(
            "GRVT_API_KEY, GRVT_TRADING_ACCOUNT_ID, and GRVT_PRIVATE_KEY must be set."
        )

    env = GrvtEnv(env_name)
    params = {
        "api_key": api_key,
        "trading_account_id": trading_account_id,
        "private_key": private_key,
    }
    api = GrvtCcxt(env, LOGGER, parameters=params)

    cfg = GridConfig(
        instrument=instrument,
        base_order_size=base_order_size,
        price_offset_pct=price_offset_pct,
        size_ratio=size_ratio,
        order_timeout_seconds=order_timeout,
        min_base_qty=min_base_qty,
    )
    bot = GrvtGridBot(api, cfg)
    await bot.start()

    loop = asyncio.get_running_loop()
    ws_params = {
        "api_key": api_key,
        "trading_account_id": trading_account_id,
        "private_key": private_key,
        "api_ws_version": os.environ.get("GRVT_WS_STREAM_VERSION", "v1"),
    }
    ws_client = GrvtCcxtWS(env, loop, LOGGER, parameters=ws_params)
    await ws_client.initialize()
    # Give WebSocket connections time to fully establish
    await asyncio.sleep(1)
    await _subscribe_streams(ws_client, bot, instrument)

    LOGGER.info("GRVT grid bot running on %s (%s)", instrument, env_name)
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
