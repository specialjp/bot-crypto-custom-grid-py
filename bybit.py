"""
Single-file Bybit martingale/grid bot that drives the strategy directly from
websocket ticker and order streams.

Environment variables:
    BYBIT_API_KEY
    BYBIT_API_SECRET
    BYBIT_SYMBOL (e.g. BTCUSDT)
    BYBIT_CATEGORY (default: linear)
    BYBIT_TESTNET (default: true)
    BYBIT_BASE_ORDER_SIZE_USD (default: 10)
    BYBIT_PRICE_OFFSET_PCT (default: 0.00015)
    BYBIT_SIZE_RATIO (default: 2)
    BYBIT_ORDER_TIMEOUT_SECONDS (default: 30)
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional

from dotenv import load_dotenv
from pybit.unified_trading import HTTP, WebSocket

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("bybit-grid")


@dataclass(slots=True)
class GridConfig:
    symbol: str
    category: str = "linear"
    base_order_size_usd: Decimal = Decimal("10")
    price_offset_pct: Decimal = Decimal("0.00015")  # 0.015%
    size_ratio: Decimal = Decimal("2")
    order_timeout_seconds: float = 30.0


class BybitGridBot:
    def __init__(self, http: HTTP, config: GridConfig):
        self.http = http
        self.cfg = config
        self.mid_price = Decimal("0")
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.level = 0
        self.buy_order_id: Optional[str] = None
        self.sell_order_id: Optional[str] = None
        self.buy_order_ts: Optional[float] = None
        self.sell_order_ts: Optional[float] = None
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            self.mid_price = self._fetch_mid_price()
            LOGGER.info("Starting grid at mid price %s", self.mid_price)
            self._cancel_all()
            self._place_buy(level=0)

    def on_ticker_message(self, message: dict[str, Any]) -> None:
        data = message.get("data")
        if not data:
            return
        entry = data[0]
        bid = entry.get("bid1Price")
        ask = entry.get("ask1Price")
        if not bid or not ask:
            return
        try:
            mid = (Decimal(bid) + Decimal(ask)) / Decimal("2")
        except Exception as exc:
            LOGGER.debug("Failed to parse ticker %s: %s", message, exc)
            return
        with self._lock:
            self.mid_price = mid

    def on_order_message(self, message: dict[str, Any]) -> None:
        data = message.get("data")
        if not data:
            return
        for entry in data:
            if entry.get("orderStatus") != "Filled":
                continue
            order_id = entry.get("orderId")
            side = entry.get("side", "").lower()
            price = entry.get("avgPrice") or entry.get("price")
            qty = entry.get("cumExecQty")
            if not order_id or not side or not price or not qty:
                continue
            try:
                price_dec = Decimal(price)
                qty_dec = Decimal(qty)
            except Exception as exc:
                LOGGER.debug("Failed to parse fill %s: %s", entry, exc)
                continue
            with self._lock:
                if side == "buy" and order_id == self.buy_order_id:
                    self._handle_buy_fill(price_dec, qty_dec)
                elif side == "sell" and order_id == self.sell_order_id:
                    self._handle_sell_fill(qty_dec)

    def _handle_buy_fill(self, price: Decimal, qty: Decimal) -> None:
        self.position_qty += qty
        self.position_cost += qty * price
        self.level += 1
        LOGGER.info("Buy filled: qty=%s price=%s level=%s", qty, price, self.level)

        self.buy_order_ts = None
        self._place_take_profit()
        self._place_buy(level=self.level)

    def _handle_sell_fill(self, qty: Decimal) -> None:
        self.position_qty -= qty
        self.sell_order_ts = None
        LOGGER.info("Sell filled: qty=%s remaining=%s", qty, self.position_qty)
        if self.position_qty <= 0:
            self._reset()

    def _reset(self) -> None:
        LOGGER.info("Resetting cycle")
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.level = 0
        self.buy_order_id = None
        self.sell_order_id = None
        self.buy_order_ts = None
        self.sell_order_ts = None
        self._cancel_all()
        self._place_buy(level=0)

    def _place_buy(self, level: int) -> None:
        if self.mid_price <= 0:
            self.mid_price = self._fetch_mid_price()
        target_price = self.mid_price * (Decimal("1") - self.cfg.price_offset_pct)
        quote_size = self.cfg.base_order_size_usd * (self.cfg.size_ratio ** level)
        quantity = quote_size / target_price
        order_id = self._place_limit("Buy", target_price, quantity)
        self.buy_order_id = order_id
        self.buy_order_ts = time.time()
        LOGGER.info(
            "Placed buy level %s: price=%s qty=%s order_id=%s", level, target_price, quantity, order_id
        )

    def _place_take_profit(self) -> None:
        if self.position_qty <= 0:
            return
        avg_price = self.position_cost / self.position_qty
        target_price = avg_price * (Decimal("1") + self.cfg.price_offset_pct)
        quantity = self.position_qty
        if self.sell_order_id:
            self._cancel(self.sell_order_id)
        order_id = self._place_limit("Sell", target_price, quantity, reduce_only=True)
        self.sell_order_id = order_id
        self.sell_order_ts = time.time()
        LOGGER.info("Placed take profit: price=%s qty=%s order_id=%s", target_price, quantity, order_id)

    def check_stale_orders(self) -> None:
        now = time.time()
        with self._lock:
            if (
                self.buy_order_id
                and self.buy_order_ts
                and now - self.buy_order_ts > self.cfg.order_timeout_seconds
            ):
                LOGGER.info("Buy order %s stale; cancelling and resetting", self.buy_order_id)
                try:
                    self._cancel(self.buy_order_id)
                except Exception as exc:
                    LOGGER.warning("Failed to cancel stale buy %s: %s", self.buy_order_id, exc)
                self._reset()
                return

            if (
                self.sell_order_id
                and self.sell_order_ts
                and now - self.sell_order_ts > self.cfg.order_timeout_seconds
            ):
                LOGGER.info("Sell order %s stale; cancelling and resetting", self.sell_order_id)
                try:
                    self._cancel(self.sell_order_id)
                except Exception as exc:
                    LOGGER.warning("Failed to cancel stale sell %s: %s", self.sell_order_id, exc)
                self._reset()

    # REST helpers ---------------------------------------------------------

    def _fetch_mid_price(self) -> Decimal:
        response = self.http.get_tickers(category=self.cfg.category, symbol=self.cfg.symbol)
        entries = response.get("result", {}).get("list", [])
        if not entries:
            raise RuntimeError(f"No ticker data for {self.cfg.symbol}")
        data = entries[0]
        bid = Decimal(data["bid1Price"])
        ask = Decimal(data["ask1Price"])
        return (bid + ask) / Decimal("2")

    def _place_limit(
        self,
        side: str,
        price: Decimal,
        quantity: Decimal,
        *,
        reduce_only: bool = False,
    ) -> str:
        result = self.http.place_order(
            category=self.cfg.category,
            symbol=self.cfg.symbol,
            side=side,
            orderType="Limit",
            qty=str(quantity),
            price=str(price),
            timeInForce="PostOnly",
            reduceOnly=reduce_only,
        )
        order_id = result.get("result", {}).get("orderId")
        if not order_id:
            raise RuntimeError(f"Unexpected place_order response: {result}")
        return order_id

    def _cancel(self, order_id: str) -> None:
        self.http.cancel_order(
            category=self.cfg.category,
            symbol=self.cfg.symbol,
            orderId=order_id,
        )

    def _cancel_all(self) -> None:
        self.http.cancel_all_orders(category=self.cfg.category, symbol=self.cfg.symbol)


def main() -> None:
    load_dotenv()

    api_key = os.environ.get("BYBIT_API_KEY")
    api_secret = os.environ.get("BYBIT_API_SECRET")
    symbol = os.environ.get("BYBIT_SYMBOL")

    if not all([api_key, api_secret, symbol]):
        raise RuntimeError("BYBIT_API_KEY, BYBIT_API_SECRET, and BYBIT_SYMBOL must be set.")

    category = os.environ.get("BYBIT_CATEGORY", "linear")
    testnet = os.environ.get("BYBIT_TESTNET", "true").lower() in {"1", "true", "yes"}

    cfg = GridConfig(
        symbol=symbol,
        category=category,
        base_order_size_usd=Decimal(os.environ.get("BYBIT_BASE_ORDER_SIZE_USD", "10")),
        price_offset_pct=Decimal(os.environ.get("BYBIT_PRICE_OFFSET_PCT", "0.00015")),
        size_ratio=Decimal(os.environ.get("BYBIT_SIZE_RATIO", "2")),
        order_timeout_seconds=float(os.environ.get("BYBIT_ORDER_TIMEOUT_SECONDS", "30")),
    )

    http = HTTP(testnet=testnet, api_key=api_key, api_secret=api_secret)
    bot = BybitGridBot(http, cfg)
    bot.start()

    channel_type = category if category in {"linear", "inverse", "spot"} else "linear"
    market_ws = WebSocket(testnet=testnet, channel_type=channel_type)
    market_ws.ticker_stream(symbol=symbol, callback=bot.on_ticker_message)

    order_ws = WebSocket(
        testnet=testnet,
        channel_type="private",
        api_key=api_key,
        api_secret=api_secret,
    )
    order_ws.order_stream(callback=bot.on_order_message)

    LOGGER.info("Bybit bot running on %s", symbol)
    try:
        while True:
            time.sleep(1)
            bot.check_stale_orders()
    except KeyboardInterrupt:
        LOGGER.info("Stopping bot")


if __name__ == "__main__":
    main()
