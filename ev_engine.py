#!/usr/bin/env python3
"""
ev_engine.py — Polymarket Price Oracle → Kalshi Execution Engine

Strategy: Use Polymarket's real-time prices as a "true price" signal.
When Kalshi is mispriced vs PM, bet on Kalshi to capture the edge.

This is +EV betting, not pure arb. You have directional risk but a strong edge.

Usage:
    from ev_engine import EVEngine
    engine = EVEngine(pm_slug="nba-atl-bos-2026-03-27",
                      ks_ticker="KXNBAGAME-26MAR27ATLBOS-BOS",
                      ks_trader=kalshi_trader)
    engine.start()
"""

import json
import logging
import threading
import time
from typing import Dict, Optional

import websockets

log = logging.getLogger("ev_engine")

PM_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class EVEngine:
    """
    Watches Polymarket via WebSocket, compares to Kalshi REST prices,
    and executes IOC orders on Kalshi when edge exists.
    
    Flow:
    1. PM WebSocket sends price updates in real-time (<100ms)
    2. Every 1 second, fetch Kalshi prices via REST
    3. Compare: if Kalshi asks more for a team than PM thinks it's worth → edge
    4. Buy the underpriced outcome on Kalshi with IOC order
    5. Log everything
    """

    def __init__(self,
                 pm_token_ids: list,
                 pm_outcome_names: list,
                 ks_ticker: str,
                 ks_team_code: str,
                 ks_trader,
                 min_edge: float = 0.03,
                 max_bet: float = 20.0):

        self.pm_token_ids = pm_token_ids
        self.pm_outcome_names = pm_outcome_names
        self.ks_ticker = ks_ticker
        self.ks_team_code = ks_team_code
        self.ks_trader = ks_trader
        self.min_edge = min_edge
        self.max_bet = max_bet

        # PM state (updated by WebSocket)
        self.pm_prices: Dict[str, dict] = {}  # team → {"bid":..,"ask":..,"mid":..}
        self.pm_connected = False
        self.pm_last_update = 0

        # Kalshi state (updated by REST polling)
        self.ks_prices: Dict[str, dict] = {}  # team → {"bid":..,"ask":..}
        self.ks_last_update = 0

        # Team mapping
        self._token_to_team = dict(zip(pm_token_ids, pm_outcome_names))
        self._ks_yes_team = None
        self._ks_no_team = None

        # Execution state
        self.running = False
        self.trades = []
        self.total_pnl = 0.0
        self.signals = []  # all detected edges (traded or not)
        self.last_signal_time = 0
        self.cooldown = 10  # seconds between trades on same side

        # Live state for dashboard
        self.state = {
            "pm": {},
            "ks": {},
            "edge_a": None,  # PM team A vs KS team A
            "edge_b": None,  # PM team B vs KS team B
            "best_edge": None,
            "pm_connected": False,
            "last_trade": None,
            "trade_count": 0,
            "total_pnl": 0.0,
            "updates": 0,
        }

    def start(self):
        """Start both PM WebSocket and Kalshi polling."""
        self.running = True

        # PM WebSocket in its own thread
        pm_thread = threading.Thread(target=self._pm_loop, daemon=True)
        pm_thread.start()

        # Kalshi polling + edge detection in main thread
        self._main_loop()

    def stop(self):
        self.running = False

    def _pm_loop(self):
        """Polymarket WebSocket connection loop."""
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._pm_ws())
        except Exception as e:
            log.error(f"PM WS loop error: {e}")

    async def _pm_ws(self):
        """Connect to PM WebSocket and process messages."""
        while self.running:
            try:
                async with websockets.connect(PM_WS_URL) as ws:
                    self.pm_connected = True
                    sub = {
                        "assets_ids": self.pm_token_ids,
                        "type": "market",
                        "custom_feature_enabled": True,
                    }
                    await ws.send(json.dumps(sub))
                    log.info("PM WebSocket connected")

                    async for raw in ws:
                        if not self.running:
                            break
                        self._parse_pm(raw)

            except Exception as e:
                self.pm_connected = False
                log.warning(f"PM WS error: {e}")
                if self.running:
                    import asyncio as _asyncio
                    await _asyncio.sleep(2)

    def _parse_pm(self, raw):
        """Parse PM WebSocket message."""
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except json.JSONDecodeError:
            return

        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            return

        now = time.time()
        for msg in data:
            if not isinstance(msg, dict):
                continue
            ev = msg.get("event_type")
            asset = msg.get("asset_id", "")

            if asset not in self._token_to_team:
                continue

            team = self._token_to_team[asset]

            if ev == "book":
                bids = msg.get("bids", [])
                asks = msg.get("asks", [])
                # Skip placeholder levels (0.01/0.99) — find real prices
                real_bid = 0
                real_ask = 1
                for b in bids:
                    p = float(b["price"])
                    if p > 0.02:
                        real_bid = p
                        break
                for a in asks:
                    p = float(a["price"])
                    if p < 0.98:
                        real_ask = p
                        break
                if real_bid > 0 and real_ask < 1:
                    self.pm_prices[team] = {
                        "bid": real_bid, "ask": real_ask,
                        "mid": round((real_bid + real_ask) / 2, 4),
                    }
                    self.pm_last_update = now

            elif ev == "best_bid_ask":
                bid = float(msg.get("best_bid", 0))
                ask = float(msg.get("best_ask", 1))
                if bid > 0.02 and ask < 0.98:
                    self.pm_prices[team] = {
                        "bid": bid, "ask": ask,
                        "mid": round((bid + ask) / 2, 4),
                    }
                    self.pm_last_update = now

            elif ev == "price_change":
                for ch in msg.get("price_changes", []):
                    if ch.get("asset_id") == asset:
                        bid = float(ch.get("best_bid", 0))
                        ask = float(ch.get("best_ask", 1))
                        if bid > 0.02 and ask < 0.98:
                            self.pm_prices[team] = {
                                "bid": bid, "ask": ask,
                                "mid": round((bid + ask) / 2, 4),
                            }
                            self.pm_last_update = now

    def _fetch_pm_rest(self):
        """Fallback: fetch PM prices via REST midpoint API."""
        for i, token_id in enumerate(self.pm_token_ids):
            team = self.pm_outcome_names[i]
            try:
                url = "https://clob.polymarket.com/midpoint?token_id=" + token_id
                req = __import__("urllib.request", fromlist=["Request"]).Request(url, headers={"User-Agent": "ET"})
                with __import__("urllib.request", fromlist=["urlopen"]).urlopen(req, timeout=5) as resp:
                    data = __import__("json").loads(resp.read())
                    mid = float(data.get("mid", 0))
                    if mid > 0.02 and mid < 0.98:
                        self.pm_prices[team] = {
                            "bid": round(mid - 0.005, 4),
                            "ask": round(mid + 0.005, 4),
                            "mid": round(mid, 4),
                        }
                        self.pm_last_update = time.time()
            except:
                pass

    def _fetch_ks(self):
        """Fetch Kalshi market prices via REST."""
        try:
            market = self.ks_trader.get_market(self.ks_ticker)
            if not market:
                return

            yes_bid = float(market.get("yes_bid_dollars", 0))
            yes_ask = float(market.get("yes_ask_dollars", 0))
            no_bid = float(market.get("no_bid_dollars", 0))
            no_ask = float(market.get("no_ask_dollars", 0))

            # Determine team names from PM outcomes
            ks_code = self.ks_team_code
            yes_team = None
            no_team = None
            for name in self.pm_outcome_names:
                nl = name.lower()
                if ks_code.lower() in nl or any(kw in nl for kw in self._get_keywords(ks_code)):
                    yes_team = name
                else:
                    no_team = name

            if not yes_team:
                yes_team = self.pm_outcome_names[0]
            if not no_team:
                no_team = self.pm_outcome_names[1]

            self._ks_yes_team = yes_team
            self._ks_no_team = no_team

            self.ks_prices = {
                yes_team: {"bid": yes_bid, "ask": yes_ask, "side": "YES"},
                no_team: {"bid": no_bid, "ask": no_ask, "side": "NO"},
            }
            self.ks_last_update = time.time()

        except Exception as e:
            log.warning(f"Kalshi fetch error: {e}")

    def _get_keywords(self, code):
        """Get keywords for a team code."""
        KW = {
            "LAL": ["laker"], "BOS": ["celtic"], "ATL": ["hawk"],
            "BKN": ["net"], "GSW": ["warrior"], "MIA": ["heat"],
            "PHX": ["sun"], "DEN": ["nugget"], "MIL": ["buck"],
            "NYK": ["knick"], "CLE": ["cavalier"], "OKC": ["thunder"],
            "DAL": ["maverick"], "MIN": ["timberwolf"], "HOU": ["rocket"],
            "SAC": ["king"], "IND": ["pacer"], "ORL": ["magic"],
            "PHI": ["76er", "sixer"], "CHI": ["bull"], "TOR": ["raptor"],
            "CHA": ["hornet"], "DET": ["piston"], "WAS": ["wizard"],
            "SAS": ["spur"], "POR": ["blazer"], "MEM": ["grizzlie"],
            "NOP": ["pelican"], "LAC": ["clipper"], "UTA": ["jazz"],
        }
        return KW.get(code.upper(), [code.lower()])

    def _detect_edge(self):
        """
        Compare PM and Kalshi prices to find +EV bets.
        
        Edge exists when:
        - PM says a team is worth X (mid price)
        - Kalshi asks Y for that team
        - Y < X - min_edge → buy on Kalshi (getting it cheaper than PM says it's worth)
        - Y > X + min_edge → sell on Kalshi (or buy the other side)
        
        For simplicity, we only BUY on Kalshi (never sell/short).
        Edge = PM_mid - KS_ask. If positive and > min_edge → buy on Kalshi.
        """
        pm = self.pm_prices
        ks = self.ks_prices

        if not pm or not ks or not self._ks_yes_team or not self._ks_no_team:
            return

        edges = []

        for team in self.pm_outcome_names:
            pm_data = pm.get(team)
            ks_data = ks.get(team)

            if not pm_data or not ks_data:
                continue

            pm_mid = pm_data["mid"]
            ks_ask = ks_data["ask"]
            ks_side = ks_data.get("side", "yes").lower()

            # Edge: PM thinks this team is worth pm_mid, Kalshi sells at ks_ask
            # If ks_ask < pm_mid → Kalshi is underpriced → buy
            edge = pm_mid - ks_ask

            if edge > 0:
                edges.append({
                    "team": team,
                    "side": ks_side,
                    "pm_mid": pm_mid,
                    "ks_ask": ks_ask,
                    "edge": round(edge, 4),
                    "roi": round(edge / ks_ask * 100, 2) if ks_ask > 0 else 0,
                    "action": "BUY",
                })

        # Update state
        self.state["pm"] = {t: {"mid": p["mid"]} for t, p in pm.items()}
        self.state["ks"] = {t: {"ask": p["ask"], "side": p.get("side")} for t, p in ks.items()}
        self.state["pm_connected"] = self.pm_connected
        self.state["updates"] += 1

        if edges:
            best = max(edges, key=lambda e: e["edge"])
            self.state["best_edge"] = best

            # Log signal
            self.signals.append({
                "time": time.strftime("%H:%M:%S"),
                **best,
            })
            if len(self.signals) > 500:
                self.signals = self.signals[-500:]

            # Check if we should trade
            if best["edge"] >= self.min_edge:
                self._maybe_execute(best)

    def _maybe_execute(self, edge: dict):
        """Execute trade if conditions are met."""
        now = time.time()
        team = edge["team"]

        # Cooldown check
        if now - self.last_signal_time < self.cooldown:
            return

        # Check recent trades on this team
        recent = [t for t in self.trades if t["team"] == team and now - t["time_raw"] < 60]
        if len(recent) >= 2:
            return  # max 2 trades per team per minute

        # Calculate bet size
        bet = min(self.max_bet, edge["edge"] * 100)  # scale bet with edge
        bet = max(bet, 1.0)  # minimum $1

        ks_ask_cents = int(edge["ks_ask"] * 100)
        count = max(1, int(bet / (edge["ks_ask"])))  # number of contracts

        log.info(f"EXECUTING: Buy {edge['side'].upper()} {team} on Kalshi @ {ks_ask_cents}¢ x {count} (edge: {edge['edge']:.3f})")

        result = self.ks_trader.place_ioc_order(
            ticker=self.ks_ticker,
            side=edge["side"],
            yes_price=ks_ask_cents,
            count=count,
        )

        trade = {
            "time": time.strftime("%H:%M:%S"),
            "time_raw": now,
            "team": team,
            "side": edge["side"],
            "pm_mid": edge["pm_mid"],
            "ks_ask": edge["ks_ask"],
            "edge": edge["edge"],
            "bet": bet,
            "count": count,
            "result": result,
            "success": result.get("success", False) if isinstance(result, dict) else False,
        }
        self.trades.append(trade)
        self.last_signal_time = now
        self.state["last_trade"] = trade
        self.state["trade_count"] = len(self.trades)

    def _main_loop(self):
        """Main loop: poll Kalshi + PM REST fallback, detect edges, execute."""
        pm_rest_counter = 0
        while self.running:
            try:
                # Fetch Kalshi every second
                self._fetch_ks()

                # Fetch PM via REST every 3 seconds (fallback when WS is down)
                pm_rest_counter += 1
                if pm_rest_counter >= 3:
                    self._fetch_pm_rest()
                    pm_rest_counter = 0

                self._detect_edge()
            except Exception as e:
                log.error(f"Main loop error: {e}")
            time.sleep(1)
