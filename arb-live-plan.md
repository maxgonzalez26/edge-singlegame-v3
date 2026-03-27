# Live Arb Execution — Step-by-Step Plan

## Current State
- V3 (port 8902) correctly identifies arb opportunities using team outcomes
- Data: 3-second polling via REST (too slow for live execution)
- No order placement — paper trading only

## Step 1: WebSocket Price Feeds (HIGH PRIORITY)
Replace 3-second polling with real-time WebSocket streams.

### Polymarket CLOB WebSocket
```
Endpoint: wss://ws-subscriptions-clob.polymarket.com/ws/market
Channel: "market"
Subscribe with: asset_ids (token IDs for both outcome tokens)
Key events:
  - best_bid_ask: real-time best bid/ask changes
  - price_change: any price level change
  - last_trade_price: trade executions
```

### Kalshi WebSocket
```
Endpoint: wss://api.elections.kalshi.com/trade-api/ws/v2
Channel: "ticker" (for trade price updates)
         "orderbook_delta" (for bid/ask changes)
Auth: KALSHI-ACCESS-KEY + KALSHI-ACCESS-SIGNATURE + KALSHI-ACCESS-TIMESTAMP
Subscribe with: market_tickers
```

### Implementation
- Use Python `websockets` library (async)
- Run both WS feeds concurrently with `asyncio`
- Update price state on every `best_bid_ask` / `ticker` event
- Recalculate arb on every price update (not every 3 seconds)
- Latency target: <100ms from price change to arb detection

## Step 2: Order Placement APIs

### Polymarket CLOB Order
```
POST https://clob.polymarket.com/order
Auth: API key (Builder API key)
Body: {
  "order": {
    "maker": "<wallet_address>",
    "signer": "<wallet_address>",
    "tokenId": "<token_id>",
    "makerAmount": "<amount_in_usdc_wei>",
    "takerAmount": "<shares>",
    "side": "BUY",
    ...
  },
  "owner": "<api_key_uuid>",
  "orderType": "GTC"  // Good-til-cancelled or IOC (immediate-or-cancel)
}
```
- Requires: Builder API key + wallet private key for signing
- Use IOC (immediate-or-cancel) for arb — fill immediately or cancel
- SDK: `py_clob_client` (Python)

### Kalshi Order
```
POST https://api.elections.kalshi.com/trade-api/v2/orders
Auth: KALSHI-ACCESS-KEY + signature
Body: {
  "ticker": "<market_ticker>",
  "action": "buy",
  "side": "yes" or "no",
  "count": <number_of_contracts>,
  "type": "limit",
  "yes_price" or "no_price": <price_in_cents>,
  "time_in_force": "ioc"
}
```
- Requires: API key (generated in Kalshi settings)
- Use IOC for arb execution
- Price in cents (e.g., 65 = $0.65)

## Step 3: Account Setup

### Polymarket
1. Create account at polymarket.com
2. Deposit USDC (need funds on Polygon network)
3. Generate Builder API key at https://polymarket.com/api-keys
4. Export wallet private key (from MetaMask or Polymarket's embedded wallet)
5. Note: Need `maker` address, `signer` address, and private key

### Kalshi
1. Create account at kalshi.com
2. Deposit USD
3. Generate API key in account settings
4. Note: Need `access_key` and the private key file used for signing

## Step 4: Arb Execution Logic

```
on every price update:
  1. Calculate both arb sides (PM A + KS B, PM B + KS A)
  2. If either side cost < 0.97 (accounting for fees):
     a. Lock the arb (prevent double-execution)
     b. Place IOC order on platform with better price FIRST
     c. Wait for fill confirmation (< 500ms timeout)
     d. If filled, place IOC order on second platform
     e. If second order fails or times out, log "leg risk" event
     f. Unlock arb
  3. Log execution: timing, fills, slippage, profit
```

### Leg Risk Mitigation
- Use IOC (immediate-or-cancel) on both legs
- If leg 2 doesn't fill, leg 1 auto-cancels (IOC)
- Set max position size per game
- Set daily loss limit

## Step 5: Configuration
```python
# Config file: arb_config.json
{
  "polymarket": {
    "api_key": "...",
    "api_secret": "...",
    "private_key": "...",
    "wallet_address": "..."
  },
  "kalshi": {
    "access_key": "...",
    "private_key_path": "~/.kalshi/key.pem"
  },
  "trading": {
    "max_bet": 50.00,
    "min_edge": 0.03,        # minimum 3% edge to trade
    "max_position_per_game": 200.00,
    "daily_loss_limit": 500.00,
    "arb_cooldown_seconds": 10  # don't re-trade same game within 10s
  }
}
```

## Step 6: Monitoring Dashboard
- Extend V3 browser with live execution status
- Show: orders placed, fills received, P&L per trade, latency metrics
- Alert on: failed fills, leg risk events, daily loss limit hit

## Dependencies
```bash
pip install websockets py-clob-client cryptography
```

## Files to Create
- `arb_config.json` — credentials and trading parameters
- `ws_feeds.py` — WebSocket price feed module
- `order_executor.py` — order placement module
- `arb_engine.py` — main arb detection + execution loop
- `singlegame.v4.py` — V4 with live execution integrated

## Risk Warnings
- Start with MAX $5-10 bets to test
- Leg risk: one leg fills, other doesn't → you're exposed
- Both platforms can go down or have API issues
- Price can move between detection and execution
- Fees (2-7%) eat into small arb edges
