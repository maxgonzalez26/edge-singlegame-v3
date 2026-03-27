#!/usr/bin/env python3
"""
test_ws.py — Test WebSocket price feed for Celtics vs Hawks.
Connects to Polymarket CLOB WebSocket (no auth needed) and shows live prices.
Run: python3 test_ws.py
"""

import asyncio
import json
import time
import urllib.request

import websockets

PM_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PM_SLUG = "nba-atl-bos-2026-03-27"


def get_token_ids(slug):
    """Fetch token IDs from Gamma API."""
    url = "https://gamma-api.polymarket.com/markets?slug=" + slug
    req = urllib.request.Request(url, headers={"User-Agent": "EdgeTrader/1.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())
        markets = data if isinstance(data, list) else data.get("markets", [])
        for m in markets:
            tokens = m.get("clobTokenIds", "[]")
            if isinstance(tokens, str):
                tokens = json.loads(tokens)
            outcomes = m.get("outcomes", "[]")
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if len(tokens) == 2:
                return tokens, outcomes
    return None, None


async def test_pm_ws():
    """Connect to Polymarket WS and stream live prices."""
    tokens, outcomes = get_token_ids(PM_SLUG)
    if not tokens:
        print("ERROR: Could not fetch token IDs")
        return

    token_to_name = dict(zip(tokens, outcomes))
    print(f"Game: {' vs '.join(outcomes)}")
    print(f"Token 0: {outcomes[0]} ({tokens[0][:20]}...)")
    print(f"Token 1: {outcomes[1]} ({tokens[1][:20]}...)")
    print(f"\nConnecting to {PM_WS_URL}...")

    async with websockets.connect(PM_WS_URL) as ws:
        # Subscribe
        sub = {
            "assets_ids": tokens,
            "type": "market",
            "custom_feature_enabled": True,
        }
        await ws.send(json.dumps(sub))
        print("Subscribed! Listening for price updates...\n")

        prices = {}
        updates = 0

        async for raw in ws:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            event = data.get("event_type")
            now = time.strftime("%H:%M:%S")

            if event == "book":
                asset = data.get("asset_id", "")
                team = token_to_name.get(asset, asset[:10])
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                bid = float(bids[0]["price"]) if bids else 0
                ask = float(asks[0]["price"]) if asks else 1
                bid_sz = float(bids[0]["size"]) if bids else 0
                ask_sz = float(asks[0]["size"]) if asks else 0
                prices[team] = {"bid": bid, "ask": ask}
                updates += 1
                print(f"[{now}] BOOK  {team:>12s}  bid={bid:.3f} ({bid_sz:.0f})  ask={ask:.3f} ({ask_sz:.0f})")

            elif event == "best_bid_ask":
                asset = data.get("asset_id", "")
                team = token_to_name.get(asset, asset[:10])
                bid = float(data.get("best_bid", 0))
                ask = float(data.get("best_ask", 1))
                prices[team] = {"bid": bid, "ask": ask}
                updates += 1
                print(f"[{now}] BEST  {team:>12s}  bid={bid:.3f}  ask={ask:.3f}")

            elif event == "last_trade_price":
                asset = data.get("asset_id", "")
                team = token_to_name.get(asset, asset[:10])
                price = float(data.get("price", 0))
                side = data.get("side", "")
                size = float(data.get("size", 0))
                print(f"[{now}] TRADE {team:>12s}  {side} {size:.0f} @ {price:.3f}")

            elif event == "price_change":
                for ch in data.get("price_changes", []):
                    asset = ch.get("asset_id", "")
                    team = token_to_name.get(asset, asset[:10])
                    bid = float(ch.get("best_bid", 0))
                    ask = float(ch.get("best_ask", 1))
                    prices[team] = {"bid": bid, "ask": ask}
                    updates += 1

            else:
                print(f"[{now}] {event}: {json.dumps(data)[:100]}")

            # Show summary every 10 updates
            if updates > 0 and updates % 10 == 0:
                if len(prices) >= 2:
                    teams = list(prices.keys())
                    a, b = teams[0], teams[1]
                    pa, pb = prices[a], prices[b]
                    cost1 = pa["ask"] + pb["ask"]
                    cost2 = pb["ask"] + pa["ask"]  # same, just checking
                    arb = 1.0 - cost1
                    arb_str = f"ARB: +${arb:.4f} ({arb/cost1*100:.1f}%)" if arb > 0 else f"no arb (${cost1:.3f})"
                    print(f"\n{'='*60}")
                    print(f"  {a}: bid={pa['bid']:.3f} ask={pa['ask']:.3f}")
                    print(f"  {b}: bid={pb['bid']:.3f} ask={pb['ask']:.3f}")
                    print(f"  Cost: ${cost1:.4f} | {arb_str}")
                    print(f"  Updates: {updates}")
                    print(f"{'='*60}\n")


if __name__ == "__main__":
    print("=" * 60)
    print("  Polymarket WebSocket Test — Celtics vs Hawks")
    print("=" * 60)
    print()
    try:
        asyncio.run(test_pm_ws())
    except KeyboardInterrupt:
        print("\nStopped.")
