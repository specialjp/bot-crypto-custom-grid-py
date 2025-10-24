# Bybit Grid Bot

`bybit.py`, `grvt.py`, and `paradex.py` are single-file martingale/grid strategies
wired to Bybit, GRVT, and Paradex respectively. Each script:
- pulls the mid price via REST,
- listens to ticker/order websocket streams,
- places staggered bids and a take-profit offer based on fills,
- cancels and restarts if an order sits for more than 30 seconds.

Each exchange reads its own prefixed environment variables (e.g. `BYBIT_*`, `GRVT_*`,
`PARADEX_*`)
so you can run them independently with different parameters.

## Run
1. Install dependencies (uv or pip):
   ```bash
   uv sync
   # or
   pip install -e .
   ```
2. Create a `.env` with your settings (Bybit example):
   ```
   BYBIT_API_KEY=your-key
   BYBIT_API_SECRET=your-secret
   BYBIT_SYMBOL=BTCUSDT
   BYBIT_CATEGORY=linear
   BYBIT_TESTNET=true
   BYBIT_BASE_ORDER_SIZE_USD=10
   BYBIT_PRICE_OFFSET_PCT=0.00015
   BYBIT_SIZE_RATIO=2
   BYBIT_ORDER_TIMEOUT_SECONDS=30
   ```
3. Run the bot:
   ```bash
   python bybit.py
   ```

For GRVT, set the corresponding environment variables and run:
```
GRVT_API_KEY=...
GRVT_TRADING_ACCOUNT_ID=...
GRVT_PRIVATE_KEY=...
GRVT_MARKET=BTC_USDT_Perp
BASE_ORDER_SIZE=0.001
MIN_BASE_QTY=0.001
PRICE_OFFSET_PCT=0.00015
SIZE_RATIO=2
ORDER_TIMEOUT_SECONDS=30
GRVT_REST_URL=https://api.grvt.network          # optional override
GRVT_WS_PUBLIC=wss://ws-public.grvt.network     # optional override
GRVT_WS_PRIVATE=wss://ws-private.grvt.network   # optional override
GRVT_WS_STREAM_VERSION=v1                       # optional override
python grvt.py
```

For Paradex, supply either L1/L2 credentials or an L2 subkey along with the market
settings:
```
PARADEX_ENV=testnet
PARADEX_MARKET=BTC-USD-PERP
PARADEX_L1_ADDRESS=0x...
PARADEX_L1_PRIVATE_KEY=0x...                    # optional if you provide L2 key
PARADEX_L2_PRIVATE_KEY=0x...                   # required
# or, for subkey auth:
# PARADEX_L2_ADDRESS=0x...
PARADEX_BASE_ORDER_SIZE=0.001
PARADEX_PRICE_OFFSET_PCT=0.00015
PARADEX_SIZE_RATIO=2
PARADEX_ORDER_TIMEOUT_SECONDS=30
python paradex.py
```
