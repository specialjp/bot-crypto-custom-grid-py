"""
Lighter martingale/grid bot powered by lighter-sdk REST and websocket clients.

Environment variables:
    LIGHTER_BASE_URL (default: https://mainnet.zklighter.elliot.ai)
    LIGHTER_PRIVATE_KEY (hex, required)
    LIGHTER_ACCOUNT_INDEX (default: 0)
    LIGHTER_API_KEY_INDEX (default: 0)
    LIGHTER_MARKET (symbol or market id, default: BTCUSDC-PERP)
    LIGHTER_BASE_ORDER_SIZE (base units, optional)
    LIGHTER_BASE_ORDER_SIZE_USD (default: 100 when base units not provided)
    LIGHTER_PRICE_OFFSET_PCT (default: 0.00015)
    LIGHTER_PROFIT_PCT (default: 0.0002, take profit percentage)
    LIGHTER_PRICE_OFFSET_MULTIPLIER (default: 1.1, geometric multiplier for buy offset per level)
    LIGHTER_SIZE_RATIO (default: 1)
    LIGHTER_ORDER_TIMEOUT_SECONDS (default: 30)

This bot signs orders locally via `SignerClient` and subscribes to order book and
account websocket feeds for updates.
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import time
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from typing import Any, Iterable, Optional, Sequence
from urllib.parse import urlparse

from dotenv import load_dotenv
from lighter import AccountApi, ApiClient, OrderApi, SignerClient
from lighter.configuration import Configuration
from lighter.ws_client import WsClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("lighter-grid")


@dataclass(slots=True)
class GridConfig:
    base_url: str
    market: str
    account_index: int
    api_key_index: int
    base_order_size: Optional[Decimal] = None
    base_order_size_usd: Optional[Decimal] = Decimal("100")
    price_offset_pct: Decimal = Decimal("0.00015")
    price_offset_multiplier: Decimal = Decimal("1")
    profit_pct: Decimal = Decimal("0.0002")
    size_ratio: Decimal = Decimal("2")
    order_timeout_seconds: float = 30.0


class LighterGridBot:
    BUY_COOLDOWN_WINDOW_SECONDS = 60.0
    BUY_COOLDOWN_STEP_SECONDS = 15.0

    def __init__(
        self,
        signer: SignerClient,
        order_api: OrderApi,
        account_api: AccountApi,
        config: GridConfig,
    ):
        self.signer = signer
        self.order_api = order_api
        self.account_api = account_api
        self.cfg = config
        if self.cfg.price_offset_multiplier <= 0:
            raise ValueError("LIGHTER_PRICE_OFFSET_MULTIPLIER must be positive.")

        self.market = config.market
        self.market_id: Optional[int] = None
        self.market_symbol: Optional[str] = None

        self.size_decimals = 0
        self.price_decimals = 0
        self.quote_multiplier = Decimal("1")
        self.size_scale = Decimal("1")
        self.price_scale = Decimal("1")
        self.size_increment = Decimal("0")
        self.price_increment = Decimal("0")
        self.min_base_amount = Decimal("0")
        self.min_quote_amount = Decimal("0")

        self.mid_price = Decimal("0")
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.last_buy_fill_price: Optional[Decimal] = None
        self.level = 0

        self.buy_order_index: Optional[int] = None
        self.sell_order_index: Optional[int] = None
        self.buy_client_index: Optional[int] = None
        self.sell_client_index: Optional[int] = None
        self.buy_order_ts: Optional[float] = None
        self.sell_order_ts: Optional[float] = None

        self._market_meta_loaded = False
        self._lock = asyncio.Lock()
        self._pending_buy_task: Optional[asyncio.Task] = None
        self._recent_buy_fill_times: list[float] = []
        self._filled_order_indexes: deque[int] = deque(maxlen=128)
        self._filled_order_index_set: set[int] = set()
        self._filled_client_indexes: deque[int] = deque(maxlen=128)
        self._filled_client_index_set: set[int] = set()

    @property
    def account_index(self) -> int:
        return self.cfg.account_index

    async def start(self) -> None:
        async with self._lock:
            await self._ensure_market_metadata()
            self.mid_price = await self._fetch_mid_price()
            LOGGER.info(
                "Starting Lighter grid on market %s (id=%s) mid price %s",
                self.market_symbol or self.market,
                self.market_id,
                self.mid_price,
            )
            # await self._cancel_all()
            # self._cancel_pending_buy_task()
            # await self._place_buy(level=0)

    async def on_order_book_update(self, market_id: Any, state: dict[str, Any]) -> None:
        if not self._matches_market(market_id):
            return
        bids = state.get("bids") or []
        asks = state.get("asks") or []
        if not bids or not asks:
            return
        bid_raw = bids[0].get("price")
        ask_raw = asks[0].get("price")
        bid = self._decode_price(bid_raw)
        ask = self._decode_price(ask_raw)
        if bid <= 0 or ask <= 0:
            return
        mid = (bid + ask) / Decimal("2")
        async with self._lock:
            self.mid_price = mid

    async def on_account_market_update(
        self,
        market_id: Any,
        account_id: Any,
        payload: dict[str, Any],
    ) -> None:
        if not payload:
            return

        if market_id is None and isinstance(payload, dict):
            channel = str(payload.get("channel") or "")
            if channel.startswith("account_market"):
                parts = channel.split(":", 2)
                if len(parts) > 1 and parts[1] != "":
                    market_id = parts[1]
                if len(parts) > 2 and parts[2] != "":
                    account_id = parts[2]
            if market_id is None:
                market_id = payload.get("market_id") or payload.get("market")
            if account_id is None:
                account_id = payload.get("account")

        if self._safe_int(account_id) != self.account_index:
            return
        if self.market_id is not None and self._safe_int(market_id) != self.market_id:
            return

        # Extract and process orders directly using is_ask and status fields
        # orders = list(self._extract_orders(payload))
        # if orders:
        #     await self._process_orders(orders)

    async def check_stale_orders(self) -> None:
        now = time.time()
        async with self._lock:
            if (
                self.buy_client_index is not None
                and self.buy_order_ts
                and now - self.buy_order_ts > self.cfg.order_timeout_seconds
                and self.level == 0
            ):
                LOGGER.info(
                    "Initial buy order (client=%s) stale; cancelling and restarting",
                    self.buy_client_index,
                )
                # await self._cancel_all()
                # await self._reset_locked()
                return
            if (
                self.buy_client_index is not None
                and self.buy_order_ts
                and now - self.buy_order_ts > self.cfg.order_timeout_seconds
                and self.level > 0
            ):
                LOGGER.info(
                    "Buy order (client=%s) stale at level %s; keeping active (position exists)",
                    self.buy_client_index,
                    self.level,
                )
    async def _process_orders(self, orders: Sequence[dict[str, Any]]) -> None:
        """Process order updates using is_ask and status fields directly."""
        async with self._lock:
            for order in orders:
                order_idx = self._safe_int(order.get("order_index") or order.get("orderIndex"))
                client_idx = self._safe_int(order.get("client_order_index") or order.get("clientOrderIndex"))
                status = str(order.get("status") or "").lower()
                is_ask = bool(order.get("is_ask") or order.get("isAsk"))
                reduce_only = bool(order.get("reduce_only") or order.get("reduceOnly"))

                if order_idx is None:
                    continue

                # Skip canceled orders - don't track them
                if status.startswith("canceled"):
                    # Only react if this was our active order that got canceled unexpectedly
                    is_buy = not is_ask
                    is_sell = is_ask and reduce_only

                    if is_buy and order_idx == self.buy_order_index:
                        LOGGER.info("Buy order canceled with status=%s; restarting from level %s", status, self.level)
                        self._clear_buy_refs()
                        self._cancel_pending_buy_task()
                        # await self._place_buy(level=self.level)
                    elif is_sell and order_idx == self.sell_order_index:
                        LOGGER.info("Take-profit canceled with status=%s; re-placing if position remains", status)
                        self._clear_sell_refs()
                        if self.position_qty > 0:
                            print('place tp')
                            # await self._place_take_profit()
                    continue  # Skip further processing for canceled orders

                # Determine if this is our BUY or SELL order
                is_buy = not is_ask
                is_sell = is_ask and reduce_only

                # Track order_index and client_index for our orders (only for non-canceled orders)
                if is_buy:
                    if self.buy_order_index is None or self.buy_order_index == order_idx:
                        self.buy_order_index = order_idx
                        if client_idx is not None and self.buy_client_index is None:
                            self.buy_client_index = client_idx
                elif is_sell:
                    if self.sell_order_index is None or self.sell_order_index == order_idx:
                        self.sell_order_index = order_idx
                        if client_idx is not None and self.sell_client_index is None:
                            self.sell_client_index = client_idx

                # Process based on status
                if status == "filled":
                    # Check deduplication
                    if self._was_order_filled(order_idx, client_idx):
                        continue

                    # Extract fill info
                    qty = self._extract_filled_quantity(order)
                    price = self._extract_fill_price(order)

                    # Mark as filled
                    self._mark_order_filled(order_idx, client_idx)

                    if is_buy:
                        if qty > 0 and price > 0:
                            await self._handle_buy_fill(price, qty)
                        else:
                            LOGGER.info("Buy order filled but missing price/qty payload=%s", order)
                            await self._reset_locked()
                            self._clear_buy_refs()
                    elif is_sell:
                        if qty <= 0:
                            qty = self.position_qty
                        if qty > 0:
                            await self._handle_sell_fill(qty)
                        else:
                            self._clear_sell_refs()

    def _extract_filled_quantity(self, order: dict[str, Any]) -> Decimal:
        filled = self._get_decimal_field(
            order, "filled_base_amount", "filledBaseAmount"
        )
        if filled is not None and filled > Decimal("0"):
            return filled
        initial = self._get_decimal_field(
            order, "initial_base_amount", "initialBaseAmount"
        )
        remaining = self._get_decimal_field(
            order, "remaining_base_amount", "remainingBaseAmount"
        )
        if (
            initial is not None
            and remaining is not None
            and initial > Decimal("0")
            and remaining >= Decimal("0")
        ):
            delta = initial - remaining
            if delta > Decimal("0"):
                return delta
        return Decimal("0")

    def _extract_fill_price(self, order: dict[str, Any]) -> Decimal:
        for key in (
            "avg_execution_price",
            "avgExecutionPrice",
            "avg_fill_price",
            "avgFillPrice",
            "price",
        ):
            if key in order:
                price = self._decode_price(order.get(key))
                if price > Decimal("0"):
                    return price
        return Decimal("0")

    def _was_order_filled(
        self, order_idx: Optional[int], client_idx: Optional[int]
    ) -> bool:
        if order_idx is not None and order_idx in self._filled_order_index_set:
            return True
        if client_idx is not None and client_idx in self._filled_client_index_set:
            return True
        return False

    def _mark_order_filled(
        self, order_idx: Optional[int], client_idx: Optional[int]
    ) -> None:
        if order_idx is not None:
            if len(self._filled_order_indexes) >= self._filled_order_indexes.maxlen:
                removed = self._filled_order_indexes.popleft()
                self._filled_order_index_set.discard(removed)
            self._filled_order_indexes.append(order_idx)
            self._filled_order_index_set.add(order_idx)
        if client_idx is not None:
            if len(self._filled_client_indexes) >= self._filled_client_indexes.maxlen:
                removed = self._filled_client_indexes.popleft()
                self._filled_client_index_set.discard(removed)
            self._filled_client_indexes.append(client_idx)
            self._filled_client_index_set.add(client_idx)

    def _get_decimal_field(
        self, payload: dict[str, Any], *keys: str
    ) -> Optional[Decimal]:
        for key in keys:
            if key in payload:
                dec = self._safe_decimal(payload.get(key))
                if dec is not None:
                    return dec
        return None

    async def _handle_buy_fill(self, price: Decimal, qty: Decimal) -> None:
        self.position_qty += qty
        self.position_cost += qty * price
        self.last_buy_fill_price = price
        self.level += 1
        LOGGER.info("Buy filled qty=%s price=%s level=%s", qty, price, self.level)
        cooldown_delay = self._register_buy_cooldown_delay()

        # self._clear_buy_refs()
        # self._cancel_pending_buy_task()

        # await self._place_take_profit()
        # self._pending_buy_task = asyncio.create_task(
        #     self._delayed_place_buy(level=self.level, delay=cooldown_delay)
        # )
        LOGGER.info(
            "Scheduled next buy level=%s after %.2fs cooldown",
            self.level,
            cooldown_delay,
        )

    async def _handle_sell_fill(self, qty: Decimal) -> None:
        # self.position_qty -= qty
        LOGGER.info("Sell filled qty=%s remaining=%s", qty, self.position_qty)
        # self._clear_sell_refs()
        # if self.position_qty <= 0:
        #     await self._reset_locked()
        # else:
        #     await self._place_take_profit()

    async def _reset_locked(self) -> None:
        LOGGER.info("Resetting cycle")
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.last_buy_fill_price = None
        self.level = 0
        # self._cancel_pending_buy_task()
        # self._clear_buy_refs()
        # self._clear_sell_refs()
        # self._recent_buy_fill_times.clear()
        # await self._cancel_all()
        # await self._place_buy(level=0)

    async def _place_buy(self, level: int) -> None:
        if self.market_id is None:
            raise RuntimeError("Market metadata not loaded.")
        if self.mid_price <= 0:
            self.mid_price = await self._fetch_mid_price()

        offset_multiplier = self.cfg.price_offset_multiplier ** level if level > 0 else Decimal("1")
        adjusted_offset_pct = self.cfg.price_offset_pct * offset_multiplier
        if adjusted_offset_pct >= Decimal("1"):
            raise ValueError(
                f"Buy offset percentage {adjusted_offset_pct} must be less than 1; "
                "lower LIGHTER_PRICE_OFFSET_PCT or LIGHTER_PRICE_OFFSET_MULTIPLIER."
            )
        target_price = self.mid_price * (Decimal("1") - adjusted_offset_pct)
        target_price = self._quantize_price(target_price, is_ask=False)

        # Ensure new buy order is always below last buy fill price
        if self.last_buy_fill_price is not None and level > 0:
            if target_price >= self.last_buy_fill_price:
                # Adjust target price to be below last fill using adjusted_offset_pct
                target_price = self.last_buy_fill_price * (Decimal("1") - adjusted_offset_pct)
                target_price = self._quantize_price(target_price, is_ask=False)
                LOGGER.info(
                    "Adjusted buy price from calculated to %s (below last fill %s with offset %.4f%%)",
                    target_price,
                    self.last_buy_fill_price,
                    adjusted_offset_pct * 100
                )

        base_qty = self._compute_base_size(level, target_price)
        quantity = self._quantize_size(base_qty)
        quantity = self._enforce_minimums(quantity, target_price)
        if quantity <= 0 or target_price <= 0:
            raise ValueError("Calculated buy parameters invalid; adjust configuration.")

        price_int = self._encode_price(target_price, is_ask=False)
        size_int = self._encode_size(quantity, round_up=False)
        client_index = self._generate_client_index(prefix=1)

        tx, resp, err = await self._submit_limit_order_with_retry(
            market_index=self.market_id,
            client_index=client_index,
            base_amount=size_int,
            price=price_int,
            is_ask=False,
            time_in_force=self.signer.ORDER_TIME_IN_FORCE_POST_ONLY,
            retries=1,
        )
        if err:
            raise RuntimeError(f"Buy order submission failed: {err}")
        if resp and resp.message:
            LOGGER.info("Buy submission response: %s", resp.message)

        self.buy_client_index = client_index
        self.buy_order_index = None
        self.buy_order_ts = time.time()
        order_notional = quantity * target_price
        LOGGER.info(
            "Placed buy level=%s price=%s qty=%s notional=%s offset_pct=%s client=%s tx=%s",
            level,
            target_price,
            quantity,
            order_notional,
            adjusted_offset_pct,
            client_index,
            resp.tx_hash if resp else None,
        )

    async def _place_take_profit(self) -> None:
        if self.market_id is None or self.position_qty <= 0:
            return
        avg_price = self.position_cost / self.position_qty if self.position_qty > 0 else Decimal("0")
        target_price = avg_price * (Decimal("1") + self.cfg.profit_pct)
        target_price = self._quantize_price(target_price, is_ask=True)
        quantity = self._quantize_size(self.position_qty, round_up=True)
        quantity = self._enforce_minimums(quantity, target_price, round_up=True)
        if quantity <= 0 or target_price <= 0:
            return

        if self.sell_order_index is not None:
            await self._safe_cancel(self.sell_order_index)

        price_int = self._encode_price(target_price, is_ask=True)
        size_int = self._encode_size(quantity, round_up=True)
        client_index = self._generate_client_index(prefix=2)

        tx, resp, err = await self._submit_limit_order_with_retry(
            market_index=self.market_id,
            client_index=client_index,
            base_amount=size_int,
            price=price_int,
            is_ask=True,
            time_in_force=self.signer.ORDER_TIME_IN_FORCE_POST_ONLY,
            reduce_only=True,
            retries=1,
        )
        if err:
            LOGGER.warning("Failed to submit take-profit order: %s", err)
            return
        if resp and resp.message:
            LOGGER.info("Take-profit submission response: %s", resp.message)

        self.sell_client_index = client_index
        self.sell_order_index = None
        self.sell_order_ts = time.time()
        LOGGER.info(
            "Placed take profit price=%s qty=%s client=%s tx=%s",
            target_price,
            quantity,
            client_index,
            resp.tx_hash if resp else None,
        )

    async def _fetch_mid_price(self) -> Decimal:
        if self.market_id is None:
            raise RuntimeError("Market metadata not loaded.")
        book = await self.order_api.order_book_orders(self.market_id, limit=1)
        if not book.bids or not book.asks:
            raise RuntimeError("Order book empty; cannot derive mid price.")
        bid = self._decode_price(book.bids[0].price)
        ask = self._decode_price(book.asks[0].price)
        if bid <= 0 or ask <= 0:
            raise RuntimeError("Invalid order book data for mid price.")
        return (bid + ask) / Decimal("2")

    async def _ensure_market_metadata(self) -> None:
        if self._market_meta_loaded:
            return
        details = await self.order_api.order_book_details()
        entries = details.order_book_details or []
        if not entries:
            raise RuntimeError("No Lighter market metadata available.")
        market_id_candidate = self._safe_int(self.market)
        normalized_target = _normalize_symbol(self.market)

        matched = None
        for entry in entries:
            symbol = entry.symbol or ""
            if market_id_candidate is not None and entry.market_id == market_id_candidate:
                matched = entry
                break
            if normalized_target and _normalize_symbol(symbol) == normalized_target:
                matched = entry
                break
        if matched is None:
            raise RuntimeError(f"Market {self.market} not found in Lighter metadata.")

        self.market_id = int(matched.market_id)
        self.market_symbol = matched.symbol
        self.size_decimals = int(matched.supported_size_decimals)
        self.price_decimals = int(matched.supported_price_decimals)
        self.quote_multiplier = Decimal(str(matched.quote_multiplier or 1))
        self.size_scale = Decimal(10) ** self.size_decimals
        self.price_scale = Decimal(10) ** self.price_decimals
        self.size_increment = Decimal("1") / self.size_scale
        self.price_increment = Decimal("1") / self.price_scale
        self.min_base_amount = self._safe_decimal(matched.min_base_amount) or Decimal("0")
        self.min_quote_amount = self._safe_decimal(matched.min_quote_amount) or Decimal("0")
        LOGGER.info(
            "Market %s (id=%s) metadata size_decimals=%s price_decimals=%s min_base=%s min_quote=%s",
            self.market_symbol,
            self.market_id,
            self.size_decimals,
            self.price_decimals,
            self.min_base_amount,
            self.min_quote_amount,
        )
        self._market_meta_loaded = True

    async def _cancel_all(self) -> None:
        if self.market_id is None:
            return
        await self._cancel_market_orders(self.market_id)

    async def _cancel_market_orders(self, market_id: int) -> None:
        try:
            auth_token, auth_err = self.signer.create_auth_token_with_expiry(
                self.signer.DEFAULT_10_MIN_AUTH_EXPIRY
            )
            if auth_err:
                LOGGER.debug("Auth token generation failed: %s", auth_err)
                auth_token = None
            orders = await self.order_api.account_active_orders(
                account_index=self.account_index,
                market_id=market_id,
                auth=auth_token,
            )
        except Exception as exc:  # pragma: no cover - networking
            LOGGER.warning("Active orders fetch failed for market %s: %s", market_id, exc)
            return

        entries = list(orders.orders) if getattr(orders, "orders", None) else []
        if not entries:
            LOGGER.debug("cancel_all: no active orders for market %s", market_id)
            return

        LOGGER.info("cancel_all: cancelling %s active orders on market %s", len(entries), market_id)
        for entry in entries:
            order_idx = self._safe_int(getattr(entry, "order_index", None))
            entry_market = self._safe_int(getattr(entry, "market_index", None))
            if entry_market is not None and entry_market != market_id:
                continue
            if order_idx is None:
                LOGGER.debug("Skipping order with missing index: %s", entry)
                continue
            await self._safe_cancel(order_idx, market_id)

    async def _safe_cancel(self, order_index: Optional[int], market_id: Optional[int] = None) -> None:
        market = market_id if market_id is not None else self.market_id
        if market is None or order_index is None:
            return
        try:
            _, resp, err = await self.signer.cancel_order(market, order_index)
            if err:
                LOGGER.warning("Failed to cancel order %s: %s", order_index, err)
            elif resp and resp.message:
                LOGGER.info("Cancel order response: %s", resp.message)
        except Exception as exc:  # pragma: no cover - networking
            LOGGER.warning("Cancel order exception for %s: %s", order_index, exc)

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

    async def _submit_limit_order_with_retry(
        self,
        *,
        market_index: int,
        client_index: int,
        base_amount: int,
        price: int,
        is_ask: bool,
        time_in_force: int,
        reduce_only: bool = False,
        retries: int = 1,
        retry_delay: float = 0.2,
    ):
        attempt = 0
        while True:
            tx, resp, err = await self.signer.create_order(
                market_index=market_index,
                client_order_index=client_index,
                base_amount=base_amount,
                price=price,
                is_ask=is_ask,
                order_type=self.signer.ORDER_TYPE_LIMIT,
                time_in_force=time_in_force,
                reduce_only=reduce_only,
            )
            if err:
                err_lower = str(err).lower()
                if "invalid nonce" in err_lower and attempt < retries:
                    attempt += 1
                    LOGGER.warning("create_order returned invalid nonce; retrying (attempt %s)", attempt + 1)
                    await asyncio.sleep(retry_delay)
                    continue
            return tx, resp, err

    async def _delayed_place_buy(self, level: int, delay: float) -> None:
        current_task = asyncio.current_task()
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            async with self._lock:
                if self.buy_client_index is not None or self.buy_order_index is not None:
                    LOGGER.debug(
                        "Skipping delayed buy placement; active buy refs client=%s order=%s",
                        self.buy_client_index,
                        self.buy_order_index,
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
            "Configure LIGHTER_BASE_ORDER_SIZE (base units) or LIGHTER_BASE_ORDER_SIZE_USD with a positive value."
        )

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

    def _quantize_price(self, price: Decimal, *, is_ask: bool) -> Decimal:
        increment = self.price_increment
        if increment <= 0:
            return price
        rounding = ROUND_UP if is_ask else ROUND_DOWN
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

    def _encode_size(self, qty: Decimal, *, round_up: bool) -> int:
        scale = self.size_scale
        if scale <= 0:
            raise RuntimeError("Size scale not initialized.")
        scaled = (qty * scale).quantize(
            Decimal("1"), rounding=ROUND_UP if round_up else ROUND_DOWN
        )
        if scaled <= 0 and round_up:
            scaled = Decimal("1")
        return int(scaled)

    def _encode_price(self, price: Decimal, *, is_ask: bool) -> int:
        scale = self.price_scale
        if scale <= 0:
            raise RuntimeError("Price scale not initialized.")
        scaled = (price * scale).quantize(
            Decimal("1"), rounding=ROUND_UP if is_ask else ROUND_DOWN
        )
        if scaled <= 0:
            scaled = Decimal("1")
        return int(scaled)

    def _decode_price(self, raw: Any) -> Decimal:
        dec = self._safe_decimal(raw)
        if dec is None:
            return Decimal("0")
        raw_str = str(raw)
        if "." in raw_str or self.price_scale == 1:
            return dec
        return dec / self.price_scale

    def _decode_base_amount(self, raw: Any) -> Decimal:
        dec = self._safe_decimal(raw)
        if dec is None:
            return Decimal("0")
        raw_str = str(raw)
        if "." in raw_str or self.size_scale == 1:
            return dec
        return dec / self.size_scale

    def _matches_market(self, market_id: Any) -> bool:
        if self.market_id is None:
            return False
        candidate = self._safe_int(market_id)
        return candidate == self.market_id

    def _clear_buy_refs(self) -> None:
        self.buy_order_index = None
        self.buy_client_index = None
        self.buy_order_ts = None

    def _clear_sell_refs(self) -> None:
        self.sell_order_index = None
        self.sell_client_index = None
        self.sell_order_ts = None

    def _generate_client_index(self, *, prefix: int) -> int:
        base = secrets.randbits(40)
        return (prefix << 40) | base

    @staticmethod
    def _extract_orders(payload: Any) -> Iterable[dict[str, Any]]:
        results: list[dict[str, Any]] = []

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                for key, value in node.items():
                    if key in {
                        "orders",
                        "open_orders",
                        "openOrders",
                        "active_orders",
                        "activeOrders",
                    }:
                        if isinstance(value, list):
                            for item in value:
                                if isinstance(item, dict) and (
                                    "client_order_index" in item
                                    or "clientOrderIndex" in item
                                    or "order_index" in item
                                    or "orderIndex" in item
                                ):
                                    results.append(item)
                        elif isinstance(value, dict):
                            for sub in value.values():
                                if isinstance(sub, list):
                                    for item in sub:
                                        if isinstance(item, dict) and (
                                            "client_order_index" in item
                                            or "clientOrderIndex" in item
                                            or "order_index" in item
                                            or "orderIndex" in item
                                        ):
                                            results.append(item)
                                else:
                                    walk(sub)
                        else:
                            walk(value)
                    else:
                        walk(value)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(payload)
        return results

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
            if isinstance(value, str) and value.strip() == "":
                return None
            return int(str(value), 10)
        except (TypeError, ValueError):
            return None


def _normalize_symbol(symbol: str) -> str:
    if not symbol:
        return ""
    cleaned = symbol.upper()
    for ch in ("-", "_", "/", " "):
        cleaned = cleaned.replace(ch, "")
    return cleaned


async def main() -> None:
    load_dotenv()

    base_url = os.environ.get("LIGHTER_BASE_URL", "https://mainnet.zklighter.elliot.ai")
    private_key = os.environ.get("LIGHTER_PRIVATE_KEY")
    if not private_key:
        raise RuntimeError("Set LIGHTER_PRIVATE_KEY for signing orders.")

    account_index = int(os.environ.get("LIGHTER_ACCOUNT_INDEX", "0"))
    api_key_index = int(os.environ.get("LIGHTER_API_KEY_INDEX", "0"))
    market = os.environ.get("LIGHTER_MARKET", "1")

    base_order_size_env = os.environ.get("LIGHTER_BASE_ORDER_SIZE")
    base_order_size = Decimal(base_order_size_env) if base_order_size_env else None

    base_order_size_usd_env = os.environ.get("LIGHTER_BASE_ORDER_SIZE_USD")
    if base_order_size is None:
        base_order_size_usd = Decimal(base_order_size_usd_env or "100")
    else:
        base_order_size_usd = Decimal(base_order_size_usd_env) if base_order_size_usd_env else None

    price_offset_pct = Decimal(os.environ.get("LIGHTER_PRICE_OFFSET_PCT", "0.00015"))
    price_offset_multiplier = Decimal(os.environ.get("LIGHTER_PRICE_OFFSET_MULTIPLIER", "1"))
    profit_pct = Decimal(os.environ.get("LIGHTER_PROFIT_PCT", "0.0002"))
    size_ratio = Decimal(os.environ.get("LIGHTER_SIZE_RATIO", "1"))
    order_timeout = float(os.environ.get("LIGHTER_ORDER_TIMEOUT_SECONDS", "10"))

    configuration = Configuration(host=base_url)
    api_client = ApiClient(configuration=configuration)
    order_api = OrderApi(api_client)
    account_api = AccountApi(api_client)

    signer = SignerClient(
        url=base_url,
        private_key=private_key,
        api_key_index=api_key_index,
        account_index=account_index,
    )
    error = signer.check_client()
    if error:
        raise RuntimeError(f"Lighter signer verification failed: {error}")

    cfg = GridConfig(
        base_url=base_url,
        market=market,
        account_index=account_index,
        api_key_index=api_key_index,
        base_order_size=base_order_size,
        base_order_size_usd=base_order_size_usd,
        price_offset_pct=price_offset_pct,
        price_offset_multiplier=price_offset_multiplier,
        profit_pct=profit_pct,
        size_ratio=size_ratio,
        order_timeout_seconds=order_timeout,
    )
    bot = LighterGridBot(signer, order_api, account_api, cfg)
    await bot.start()

    parsed = urlparse(base_url)
    ws_host = parsed.netloc or base_url.replace("https://", "").replace("http://", "")
    if not ws_host:
        raise RuntimeError(f"Cannot derive websocket host from {base_url}")

    loop = asyncio.get_running_loop()

    auth_token: Optional[str]
    auth_error: Optional[str]
    try:
        auth_token, auth_error = signer.create_auth_token_with_expiry()
    except Exception as exc:  # pragma: no cover - networking
        LOGGER.warning("Failed to create websocket auth token: %s", exc)
        auth_token, auth_error = None, str(exc)
    if auth_error:
        LOGGER.warning("Websocket auth token error: %s", auth_error)
        auth_token = None

    if auth_error:
        raise RuntimeError(f"Failed to create websocket auth token: {auth_error}")
    if not auth_token:
        raise RuntimeError("Signer did not return an auth token; cannot subscribe to account_market.")

    def order_book_callback(market_id: Any, state: dict[str, Any]) -> None:
        loop.create_task(bot.on_order_book_update(market_id, state))

    def account_market_callback(market_id: Any, account_id: Any, payload: dict[str, Any]) -> None:
        loop.create_task(bot.on_account_market_update(market_id, account_id, payload))

    if bot.market_id is None:
        raise RuntimeError("Market metadata not initialized.")

    account_market_subs: list[Any] = [(bot.market_id, bot.account_index, auth_token)]

    ws_client = WsClient(
        host=ws_host,
        order_book_ids=[bot.market_id],
        account_market=account_market_subs,
        on_order_book_update=order_book_callback,
        on_account_market=account_market_callback,
    )

    ws_task = asyncio.create_task(ws_client.run_async())

    LOGGER.info(
        "Lighter grid bot running on %s (market id %s)",
        bot.market_symbol or bot.market,
        bot.market_id,
    )
    try:
        while True:
            await asyncio.sleep(1)
            await bot.check_stale_orders()
    except asyncio.CancelledError:  # pragma: no cover - runtime shutdown
        pass
    except KeyboardInterrupt:
        LOGGER.info("Stopping bot")
    finally:
        ws_task.cancel()
        with suppress(asyncio.CancelledError):
            await ws_task
        await api_client.close()


if __name__ == "__main__":
    asyncio.run(main())
