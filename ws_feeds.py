#!/usr/bin/env python3
"""
ws_feeds.py — Real-time WebSocket price feeds for Polymarket + Kalshi.

Connects to both platforms via WebSocket and streams best bid/ask updates.
No polling — prices update instantly on every book change.

Usage:
    from ws_feeds import PriceFeed
    feed = PriceFeed(pm_token_ids=["..."], ks_tickers=["..."])
    feed.start()  # blocks, updates feed.prices dict
    # feed.prices["pm"] = {"team_a": {"bid": ..., "ask": ...}, ...}
    # feed.prices["ks"] = {"team_a": {"bid": ..., "ask": ...}, ...}
"""

import asyncio
import json
import logging
import time
from typing import Dict, List, Optional

import websockets

log = logging.getLogger("ws_feeds")

# ---------------------------------------------------------------------------
# Polymarket CLOB WebSocket
# ---------------------------------------------------------------------------
PM_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class PolymarketFeed:
    """Streams real-time best bid/ask from Polymarket CLOB WebSocket."""

    def __init__(self, token_ids: List[str]):
        self.token_ids = token_ids  # [YES_token, NO_token]
        self.prices: Dict[str, dict] = {}  # token_id → {"bid": float, "ask": float, "bid_size": float, "ask_size": float, "ts": float}
        self._ws = None
        self._connected = False
        self.last_update = 0
        self.error = None

    async def connect(self):
        """Connect and subscribe to market channel."""
        while True:
            try:
                async with websockets.connect(PM_WS_URL) as ws:
                    self._ws = ws
                    self._connected = True
                    self.error = None
                    log.info("Polymarket WS connected")

                    # Subscribe to market channel with our token IDs
                    sub_msg = {
                        "assets_ids": self.token_ids,
                        "type": "market",
                        "custom_feature_enabled": True,
                    }
                    await ws.send(json.dumps(sub_msg))
                    log.info(f"Polymarket subscribed: {len(self.token_ids)} tokens")

                    # Listen for messages
                    async for raw in ws:
                        self._handle_message(raw)

            except Exception as e:
                self._connected = False
                self.error = str(e)
                log.warning(f"Polymarket WS error: {e}")
                await asyncio.sleep(3)

    def _handle_message(self, raw: str):
        """Process incoming WebSocket message (may be batched as list)."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        # Messages may arrive as a single dict or a list of dicts
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            return

        for msg in data:
            self._process_event(msg)

    def _process_event(self, data: dict):
        """Process a single event."""
        event_type = data.get("event_type")

        if event_type == "book":
            # Full orderbook snapshot — extract best bid/ask
            asset_id = data.get("asset_id", "")
            bids = data.get("bids", [])
            asks = data.get("asks", [])

            if bids:
                best_bid = float(bids[0]["price"])
                bid_size = float(bids[0]["size"])
            else:
                best_bid = 0.0
                bid_size = 0.0

            if asks:
                best_ask = float(asks[0]["price"])
                ask_size = float(asks[0]["size"])
            else:
                best_ask = 1.0
                ask_size = 0.0

            self.prices[asset_id] = {
                "bid": best_bid,
                "ask": best_ask,
                "bid_size": bid_size,
                "ask_size": ask_size,
                "ts": time.time(),
            }
            self.last_update = time.time()

        elif event_type == "best_bid_ask":
            # Direct best bid/ask update (most efficient)
            asset_id = data.get("asset_id", "")
            self.prices[asset_id] = {
                "bid": float(data.get("best_bid", 0)),
                "ask": float(data.get("best_ask", 1)),
                "bid_size": 0,  # not provided in this event
                "ask_size": 0,
                "ts": time.time(),
            }
            self.last_update = time.time()

        elif event_type == "price_change":
            # Price level change — update best bid/ask from the event
            for change in data.get("price_changes", []):
                asset_id = change.get("asset_id", "")
                if asset_id in self.token_ids:
                    bb = float(change.get("best_bid", 0))
                    ba = float(change.get("best_ask", 1))
                    if asset_id not in self.prices:
                        self.prices[asset_id] = {}
                    self.prices[asset_id].update({
                        "bid": bb,
                        "ask": ba,
                        "ts": time.time(),
                    })
                    self.last_update = time.time()

    @property
    def connected(self):
        return self._connected


# ---------------------------------------------------------------------------
# Kalshi WebSocket
# ---------------------------------------------------------------------------
KS_WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"


class KalshiFeed:
    """Streams real-time ticker updates from Kalshi WebSocket."""

    def __init__(self, tickers: List[str], access_key: str = "", signature: str = "", timestamp: str = ""):
        self.tickers = tickers
        self.access_key = access_key
        self.signature = signature
        self.timestamp = timestamp
        self.prices: Dict[str, dict] = {}  # ticker → {"bid": float, "ask": float, "last": float, "ts": float}
        self._ws = None
        self._connected = False
        self.last_update = 0
        self.error = None

    async def connect(self):
        """Connect with auth headers and subscribe to ticker channel."""
        if not self.access_key:
            log.warning("Kalshi WS: no API key provided, skipping")
            return

        while True:
            try:
                headers = {
                    "KALSHI-ACCESS-KEY": self.access_key,
                    "KALSHI-ACCESS-SIGNATURE": self.signature,
                    "KALSHI-ACCESS-TIMESTAMP": self.timestamp,
                }
                async with websockets.connect(KS_WS_URL, additional_headers=headers) as ws:
                    self._ws = ws
                    self._connected = True
                    self.error = None
                    log.info("Kalshi WS connected")

                    # Subscribe to ticker channel for our markets
                    sub_msg = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["ticker"],
                            "market_tickers": self.tickers,
                        },
                    }
                    await ws.send(json.dumps(sub_msg))
                    log.info(f"Kalshi subscribed: {len(self.tickers)} tickers")

                    async for raw in ws:
                        self._handle_message(raw)

            except Exception as e:
                self._connected = False
                self.error = str(e)
                log.warning(f"Kalshi WS error: {e}")
                await asyncio.sleep(3)

    def _handle_message(self, raw: str):
        """Process incoming WebSocket message."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        msg = data.get("msg", {})
        msg_type = msg.get("type")

        if msg_type == "ticker":
            ticker = msg.get("ticker", "")
            if ticker in self.tickers:
                self.prices[ticker] = {
                    "yes_bid": self._sf(msg.get("yes_bid")),
                    "yes_ask": self._sf(msg.get("yes_ask")),
                    "no_bid": self._sf(msg.get("no_bid")),
                    "no_ask": self._sf(msg.get("no_ask")),
                    "last": self._sf(msg.get("price")),
                    "ts": time.time(),
                }
                self.last_update = time.time()

    @staticmethod
    def _sf(val):
        """Safe float."""
        if val is None:
            return 0.0
        try:
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    @property
    def connected(self):
        return self._connected


# ---------------------------------------------------------------------------
# Combined Price Feed
# ---------------------------------------------------------------------------
class PriceFeed:
    """
    Manages both WebSocket feeds and exposes unified price state.

    Usage:
        feed = PriceFeed(
            pm_token_ids=["token_yes", "token_no"],
            pm_outcome_names=["Hawks", "Celtics"],
            ks_tickers=["KXNBAGAME-26MAR27ATLBOS-BOS"],
            ks_team_code="BOS",
        )
        await feed.start()  # runs forever, prices update in feed.state
    """

    def __init__(self,
                 pm_token_ids: List[str],
                 pm_outcome_names: List[str],
                 ks_tickers: List[str],
                 ks_team_code: str,
                 ks_access_key: str = "",
                 ks_signature: str = "",
                 ks_timestamp: str = ""):

        self.pm_token_ids = pm_token_ids
        self.pm_outcome_names = pm_outcome_names  # ["Hawks", "Celtics"]
        self.ks_tickers = ks_tickers
        self.ks_team_code = ks_team_code

        # Map token IDs to team names
        self._token_to_team = {}
        if len(pm_token_ids) == len(pm_outcome_names):
            for tid, name in zip(pm_token_ids, pm_outcome_names):
                self._token_to_team[tid] = name

        # Feed instances
        self.pm_feed = PolymarketFeed(pm_token_ids)
        self.ks_feed = KalshiFeed(ks_tickers, ks_access_key, ks_signature, ks_timestamp)

        # Unified state — keyed by team name
        self.state = {
            "pm": {},  # "Hawks": {"bid": 0.34, "ask": 0.35}, "Celtics": {...}
            "ks": {},  # "Celtics": {"yes_bid": 0.65, "yes_ask": 0.66, ...}
            "arb_a": None,  # computed arb side A
            "arb_b": None,  # computed arb side B
            "pm_connected": False,
            "ks_connected": False,
            "last_update": 0,
            "updates": 0,
        }

        # Kalshi team mapping
        self._ks_yes_team = ks_team_code  # e.g. "Celtics" or "BOS"
        self._ks_no_team = None  # determined when we know the game

    async def start(self):
        """Run both feeds concurrently."""
        pm_task = asyncio.create_task(self.pm_feed.connect())
        ks_task = asyncio.create_task(self.ks_feed.connect())
        update_task = asyncio.create_task(self._update_loop())

        await asyncio.gather(pm_task, ks_task, update_task, return_exceptions=True)

    async def _update_loop(self):
        """Periodically reconcile feeds into unified state."""
        while True:
            await asyncio.sleep(0.1)  # update state 10x/sec

            self.state["pm_connected"] = self.pm_feed.connected
            self.state["ks_connected"] = self.ks_feed.connected

            # Map PM token prices → team names
            for token_id, team in self._token_to_team.items():
                if token_id in self.pm_feed.prices:
                    p = self.pm_feed.prices[token_id]
                    self.state["pm"][team] = {
                        "bid": p["bid"],
                        "ask": p["ask"],
                        "mid": round((p["bid"] + p["ask"]) / 2, 4),
                    }

            # Map KS ticker prices → team names
            if self.ks_feed.tickers:
                ticker = self.ks_feed.tickers[0]
                if ticker in self.ks_feed.prices:
                    p = self.ks_feed.prices[ticker]
                    self.state["ks"][self._ks_yes_team] = {
                        "bid": p["yes_bid"],
                        "ask": p["yes_ask"],
                        "side": "YES",
                    }
                    # Derive NO side from YES (inverse)
                    yes_ask = p["yes_ask"]
                    if yes_ask > 0:
                        no_ask = round(1.0 - p["yes_bid"], 4)
                        no_bid = round(1.0 - p["yes_ask"], 4)
                        # Determine the NO team from PM outcomes
                        for name in self.pm_outcome_names:
                            if name != self._ks_yes_team:
                                self._ks_no_team = name
                                self.state["ks"][name] = {
                                    "bid": no_bid,
                                    "ask": no_ask,
                                    "side": "NO",
                                }
                                break

            # Compute arb if both feeds have data
            self._compute_arb()
            self.state["last_update"] = max(self.pm_feed.last_update, self.ks_feed.last_update)
            self.state["updates"] += 1

    def _compute_arb(self):
        """Calculate arb from current prices."""
        pm = self.state["pm"]
        ks = self.state["ks"]

        if not pm or not ks or not self._ks_yes_team or not self._ks_no_team:
            self.state["arb_a"] = None
            self.state["arb_b"] = None
            return

        ks_yes = ks.get(self._ks_yes_team, {})
        ks_no = ks.get(self._ks_no_team, {})
        pm_yes = pm.get(self._ks_yes_team, {})
        pm_no = pm.get(self._ks_no_team, {})

        if not all([ks_yes, ks_no, pm_yes, pm_no]):
            self.state["arb_a"] = None
            self.state["arb_b"] = None
            return

        # Side A: PM[NO team] + KS[YES team]
        cost_a = pm_no.get("ask", 1) + ks_yes.get("ask", 1)
        profit_a = round(1.0 - cost_a, 4)

        # Side B: PM[YES team] + KS[NO team]
        cost_b = pm_yes.get("ask", 1) + ks_no.get("ask", 1)
        profit_b = round(1.0 - cost_b, 4)

        self.state["arb_a"] = {
            "pm_team": self._ks_no_team,
            "ks_team": self._ks_yes_team,
            "pm_price": pm_no.get("ask", 0),
            "ks_price": ks_yes.get("ask", 0),
            "cost": round(cost_a, 4),
            "profit": profit_a,
            "roi": round(profit_a / cost_a * 100, 2) if cost_a > 0 else 0,
            "is_arb": profit_a > 0,
        }

        self.state["arb_b"] = {
            "pm_team": self._ks_yes_team,
            "ks_team": self._ks_no_team,
            "pm_price": pm_yes.get("ask", 0),
            "ks_price": ks_no.get("ask", 0),
            "cost": round(cost_b, 4),
            "profit": profit_b,
            "roi": round(profit_b / cost_b * 100, 2) if cost_b > 0 else 0,
            "is_arb": profit_b > 0,
        }
