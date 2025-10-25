"""
Avantis on-chain martingale/grid bot powered by `avantis-trader-sdk`.

Environment variables:
    AVANTIS_RPC_URL (provider URL, required)
    AVANTIS_L1_RPC_URL (optional, default from SDK)
    AVANTIS_PRIVATE_KEY (hex, required)
    AVANTIS_ACCOUNT_ADDRESS (required)
    AVANTIS_PAIR (e.g. ETH/USD, required)
    AVANTIS_BASE_ORDER_SIZE_USD (default: 100)
    AVANTIS_PRICE_OFFSET_PCT (default: 0.00015)
    AVANTIS_PRICE_OFFSET_MULTIPLIER (default: 1)
    AVANTIS_PROFIT_PCT (default: 0.0002)
    AVANTIS_SIZE_RATIO (default: 2)
    AVANTIS_ORDER_TIMEOUT_SECONDS (default: 30)
    AVANTIS_LEVERAGE (default: 1)
    AVANTIS_SLIPPAGE_PCT (default: 0.0005)

The bot observes Pyth price updates for the requested pair and triggers on-chain
transactions when price targets are reached. Buy fills schedule staggered
cooldowns (5s / 10s / 15s within a 60s window) mirroring the other exchange
implementations. Take-profit execution polls price in the health-check loop and
closes all open Avantis positions once the target is met.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Optional

from dotenv import load_dotenv

from avantis_trader_sdk.client import TraderClient
from avantis_trader_sdk.feed.feed_client import FeedClient
from avantis_trader_sdk.types import TradeInput, TradeInputOrderType, TradeExtendedResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("avantis-grid")


@dataclass(slots=True)
class GridConfig:
    pair: str
    base_order_size_usd: Decimal = Decimal("100")
    price_offset_pct: Decimal = Decimal("0.00015")
    price_offset_multiplier: Decimal = Decimal("1")
    profit_pct: Decimal = Decimal("0.0002")
    size_ratio: Decimal = Decimal("2")
    order_timeout_seconds: float = 30.0
    leverage: Decimal = Decimal("1")
    slippage_pct: Decimal = Decimal("0.0005")


class AvantisGridBot:
    BUY_COOLDOWN_WINDOW_SECONDS = 60.0
    BUY_COOLDOWN_STEP_SECONDS = 5.0

    def __init__(
        self,
        client: TraderClient,
        feed_client: FeedClient,
        account_address: str,
        private_key: str,
        config: GridConfig,
    ) -> None:
        self.client = client
        self.feed_client = feed_client
        self.address = account_address
        self.private_key = private_key
        self.cfg = config
        if self.cfg.price_offset_multiplier <= 0:
            raise ValueError("AVANTIS_PRICE_OFFSET_MULTIPLIER must be positive.")
        if self.cfg.leverage <= 0:
            raise ValueError("AVANTIS_LEVERAGE must be positive.")

        self.pair = config.pair
        self.pair_index: Optional[int] = None

        self.mid_price = Decimal("0")
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.level = 0
        self.take_profit_price: Optional[Decimal] = None

        self._lock = asyncio.Lock()
        self._pending_buy_task: Optional[asyncio.Task] = None
        self._recent_buy_fill_times: list[float] = []
        self._price_feed_task: Optional[asyncio.Task] = None
        self._last_buy_ts: Optional[float] = None

    async def start(self) -> None:
        async with self._lock:
            self.pair_index = await self.client.pairs_cache.get_pair_index(self.pair)
            initial = await self.feed_client.get_latest_price_updates([self.pair])
            if not initial.parsed:
                raise RuntimeError(f"No price data available for {self.pair}")
            self.mid_price = Decimal(str(initial.parsed[0].converted_price))
            LOGGER.info("Starting Avantis grid on %s mid price %s", self.pair, self.mid_price)

        self._price_feed_task = asyncio.create_task(self._run_price_feed())
        await self._place_buy(level=0)

    async def _run_price_feed(self) -> None:
        loop = asyncio.get_running_loop()

        def _handle_price_update(update) -> None:
            coro = self._on_price_feed(update)
            asyncio.run_coroutine_threadsafe(coro, loop)

        self.feed_client.register_price_feed_callback(self.pair, _handle_price_update)
        try:
            await self.feed_client.listen_for_price_updates()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - external dependency
            LOGGER.error("Price feed listener terminated: %s", exc)

    async def _on_price_feed(self, update) -> None:
        price = getattr(update, "converted_price", None)
        if price is None:
            return
        try:
            dec_price = Decimal(str(price))
        except (InvalidOperation, TypeError):
            return
        if dec_price <= 0:
            return
        async with self._lock:
            self.mid_price = dec_price

    async def check_stale_orders(self) -> None:
        async with self._lock:
            if (
                self._last_buy_ts
                and time.time() - self._last_buy_ts > self.cfg.order_timeout_seconds
                and self.position_qty == 0
            ):
                LOGGER.info("Last buy attempt stale without position; reattempting base level")
                await self._place_buy(level=0)
                return

            if self.position_qty > 0 and self.take_profit_price is not None:
                if self.mid_price >= self.take_profit_price:
                    LOGGER.info(
                        "Mid price %s reached take-profit target %s; closing all positions",
                        self.mid_price,
                        self.take_profit_price,
                    )
                    await self._close_all_positions()
                    await self._reset_locked()

    async def _place_buy(self, level: int) -> None:
        async with self._lock:
            if self.mid_price <= 0:
                raise RuntimeError("Mid price unavailable for Avantis buy placement")
            offset_multiplier = self.cfg.price_offset_multiplier ** level if level > 0 else Decimal("1")
            adjusted_offset_pct = self.cfg.price_offset_pct * offset_multiplier
            target_price = self.mid_price * (Decimal("1") - adjusted_offset_pct)
            collateral_usd = self.cfg.base_order_size_usd * (self.cfg.size_ratio ** level)
            await self._submit_buy_transaction(level, target_price, collateral_usd)
            await self._sync_trades()

            if self.position_qty > 0:
                delay = self._register_buy_cooldown_delay()
                self._cancel_pending_buy_task()
                self._pending_buy_task = asyncio.create_task(self._delayed_place_buy(delay))
                LOGGER.info("Scheduled next buy after %.2fs cooldown", delay)
            else:
                LOGGER.warning("No position after buy transaction; will retry base level")
                await self._schedule_reset(level)

    async def _submit_buy_transaction(self, level: int, target_price: Decimal, collateral_usd: Decimal) -> None:
        if self.pair_index is None:
            raise RuntimeError("Pair metadata not initialised")

        price_int = int((target_price * Decimal(10) ** 10).to_integral_value(rounding=ROUND_DOWN))
        collateral_int = int((collateral_usd * Decimal(10) ** 6).to_integral_value(rounding=ROUND_DOWN))
        if price_int <= 0 or collateral_int <= 0:
            raise ValueError("Invalid Avantis order parameters")

        trade_input = TradeInput(
            trader=self.address,
            pair_index=self.pair_index,
            is_long=True,
            leverage=int(self.cfg.leverage),
            open_price=price_int,
            collateral_in_trade=collateral_int,
            open_collateral=collateral_int,
            timestamp=int(time.time()),
        )

        LOGGER.info(
            "Submitting Avantis buy level=%s target_price=%s collateral_usd=%s",
            level,
            target_price,
            collateral_usd,
        )
        tx = await self.client.trade.build_trade_open_tx(
            trade_input,
            TradeInputOrderType.LIMIT,
            float(self.cfg.slippage_pct),
        )
        if not tx.get("gasPrice"):
            tx["gasPrice"] = await self.client.async_web3.eth.gas_price
        if "nonce" not in tx:
            tx["nonce"] = await self.client.get_transaction_count(self.address)

        receipt = await self.client.sign_and_get_receipt(self.private_key, tx)
        LOGGER.info("Buy transaction submitted tx=%s", receipt.transactionHash.hex())
        self._last_buy_ts = time.time()

    async def _delayed_place_buy(self, delay: float) -> None:
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            await self._place_buy(level=self.level)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.exception("Error placing delayed Avantis buy: %s", exc)

    def _register_buy_cooldown_delay(self) -> float:
        now = time.time()
        window = self.BUY_COOLDOWN_WINDOW_SECONDS
        self._recent_buy_fill_times = [ts for ts in self._recent_buy_fill_times if now - ts < window]
        self._recent_buy_fill_times.append(now)
        return self.BUY_COOLDOWN_STEP_SECONDS * len(self._recent_buy_fill_times)

    def _cancel_pending_buy_task(self) -> None:
        task = self._pending_buy_task
        if task and not task.done():
            task.cancel()
        self._pending_buy_task = None

    async def _schedule_reset(self, level: int) -> None:
        await asyncio.sleep(1)
        await self._place_buy(level=level)

    async def _close_all_positions(self) -> None:
        trades = await self.client.trade.get_trades(self.address)
        open_trades, _ = trades
        if not open_trades:
            LOGGER.info("No Avantis positions to close")
            return
        for trade in open_trades:
            await self._close_trade(trade)
            await asyncio.sleep(1)

    async def _close_trade(self, trade: TradeExtendedResponse) -> None:
        trade_index = trade.trade.trade_index
        pair_index = trade.trade.pair_index
        collateral_int = int(
            (Decimal(str(trade.trade.collateral_in_trade)) * Decimal(10) ** 6).to_integral_value(rounding=ROUND_DOWN)
        )
        tx = await self.client.trade.build_trade_close_tx(
            self.address,
            pair_index,
            trade_index,
            collateral_int,
        )
        if not tx.get("gasPrice"):
            tx["gasPrice"] = await self.client.async_web3.eth.gas_price
        if "nonce" not in tx:
            tx["nonce"] = await self.client.get_transaction_count(self.address)
        receipt = await self.client.sign_and_get_receipt(self.private_key, tx)
        LOGGER.info(
            "Closed Avantis trade index=%s tx=%s", trade_index, receipt.transactionHash.hex()
        )

    async def _sync_trades(self) -> None:
        trades, _pending = await self.client.trade.get_trades(self.address)
        position_qty = Decimal("0")
        position_cost = Decimal("0")
        for trade in trades:
            collateral = Decimal(str(trade.trade.collateral_in_trade))
            leverage = Decimal(str(trade.trade.leverage))
            price = Decimal(str(trade.trade.open_price))
            notional = collateral * leverage
            qty = notional / price if price > 0 else Decimal("0")
            position_qty += qty
            position_cost += notional

        if position_qty > 0:
            avg_price = position_cost / position_qty
        else:
            avg_price = Decimal("0")

        self.position_qty = position_qty
        self.position_cost = position_cost
        self.level = len(trades)
        self.take_profit_price = avg_price * (Decimal("1") + self.cfg.profit_pct) if avg_price > 0 else None
        LOGGER.info(
            "Synced Avantis positions level=%s qty=%s avg_price=%s", self.level, self.position_qty, avg_price
        )

    async def _reset_locked(self) -> None:
        self.position_qty = Decimal("0")
        self.position_cost = Decimal("0")
        self.level = 0
        self.take_profit_price = None
        self._recent_buy_fill_times.clear()
        self._cancel_pending_buy_task()
        await self._place_buy(level=0)


async def main() -> None:
    load_dotenv()

    provider_url = os.environ.get("AVANTIS_RPC_URL")
    private_key = os.environ.get("AVANTIS_PRIVATE_KEY")
    account_address = os.environ.get("AVANTIS_ACCOUNT_ADDRESS")
    pair = os.environ.get("AVANTIS_PAIR")
    l1_url = os.environ.get("AVANTIS_L1_RPC_URL")

    if not provider_url or not private_key or not account_address or not pair:
        raise RuntimeError(
            "Set AVANTIS_RPC_URL, AVANTIS_PRIVATE_KEY, AVANTIS_ACCOUNT_ADDRESS, and AVANTIS_PAIR"
        )

    base_order_size_usd = Decimal(os.environ.get("AVANTIS_BASE_ORDER_SIZE_USD", "100"))
    price_offset_pct = Decimal(os.environ.get("AVANTIS_PRICE_OFFSET_PCT", "0.00015"))
    price_offset_multiplier = Decimal(os.environ.get("AVANTIS_PRICE_OFFSET_MULTIPLIER", "1"))
    profit_pct = Decimal(os.environ.get("AVANTIS_PROFIT_PCT", "0.0002"))
    size_ratio = Decimal(os.environ.get("AVANTIS_SIZE_RATIO", "2"))
    order_timeout = float(os.environ.get("AVANTIS_ORDER_TIMEOUT_SECONDS", "30"))
    leverage = Decimal(os.environ.get("AVANTIS_LEVERAGE", "1"))
    slippage_pct = Decimal(os.environ.get("AVANTIS_SLIPPAGE_PCT", "0.0005"))

    client = TraderClient(provider_url, l1_provider_url=l1_url or "https://eth.llamarpc.com")
    feed_client = FeedClient()

    cfg = GridConfig(
        pair=pair,
        base_order_size_usd=base_order_size_usd,
        price_offset_pct=price_offset_pct,
        price_offset_multiplier=price_offset_multiplier,
        profit_pct=profit_pct,
        size_ratio=size_ratio,
        order_timeout_seconds=order_timeout,
        leverage=leverage,
        slippage_pct=slippage_pct,
    )

    bot = AvantisGridBot(client, feed_client, account_address, private_key, cfg)
    await bot.start()

    LOGGER.info("Avantis grid bot running on %s", pair)
    try:
        while True:
            await asyncio.sleep(1)
            await bot.check_stale_orders()
    except asyncio.CancelledError:
        raise
    except KeyboardInterrupt:
        LOGGER.info("Stopping bot")
    finally:
        bot._cancel_pending_buy_task()
        if bot._price_feed_task:
            bot._price_feed_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await bot._price_feed_task


if __name__ == "__main__":
    asyncio.run(main())
