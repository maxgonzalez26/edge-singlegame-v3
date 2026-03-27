#!/usr/bin/env python3
"""
run_ev.py — Run the EV engine for a specific game.
PM WebSocket → compare to Kalshi → execute IOC orders on Kalshi.

Usage:
    python3 run_ev.py                    # default: Celtics vs Hawks
    python3 run_ev.py nba-bkn-lal-2026-03-27 KXNBAGAME-26MAR27BKNLAL-BKN BKN
"""

import json
import logging
import sys
import urllib.request

from order_executor import KalshiTrader
from ev_engine import EVEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("run_ev")

# Load config
with open("research/arb_config.json") as f:
    cfg = json.load(f)


def get_pm_tokens(slug):
    """Fetch PM token IDs and outcome names from Gamma API."""
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


def main():
    # Game config (default: Celtics vs Hawks)
    pm_slug = sys.argv[1] if len(sys.argv) > 1 else "nba-atl-bos-2026-03-27"
    ks_ticker = sys.argv[2] if len(sys.argv) > 2 else "KXNBAGAME-26MAR27ATLBOS-BOS"
    ks_code = sys.argv[3] if len(sys.argv) > 3 else "BOS"

    log.info(f"Game: {pm_slug} / {ks_ticker} / KS side: {ks_code}")

    # Get PM tokens
    tokens, outcomes = get_pm_tokens(pm_slug)
    if not tokens:
        log.error("Could not fetch PM tokens")
        return

    log.info(f"PM outcomes: {outcomes}")
    log.info(f"PM tokens: {tokens[0][:20]}... {tokens[1][:20]}...")

    # Init Kalshi trader
    ks = KalshiTrader(
        access_key=cfg["kalshi"]["access_key"],
        private_key_path=cfg["kalshi"]["private_key_path"],
    )

    # Test Kalshi connection
    market = ks.get_market(ks_ticker)
    if market:
        log.info(f"Kalshi connected: YES ask ${market.get('yes_ask_dollars', '?')}")
    else:
        log.error("Kalshi connection failed")
        return

    # Create and start engine
    trading = cfg.get("trading", {})
    engine = EVEngine(
        pm_token_ids=tokens,
        pm_outcome_names=outcomes,
        ks_ticker=ks_ticker,
        ks_team_code=ks_code,
        ks_trader=ks,
        min_edge=trading.get("min_edge", 0.03),
        max_bet=trading.get("max_bet", 20.0),
    )

    log.info(f"Starting EV engine (min_edge={engine.min_edge}, max_bet=${engine.max_bet})")
    log.info("Watching for Kalshi mispricing vs Polymarket reference prices...")
    log.info("Press Ctrl+C to stop\n")

    try:
        engine.start()
    except KeyboardInterrupt:
        engine.stop()
        log.info(f"\nStopped. Trades: {len(engine.trades)}, Signals: {len(engine.signals)}")
        for t in engine.trades:
            log.info(f"  {t['time']} | {t['team']} | ${t['bet']:.2f} | edge={t['edge']:.3f} | {'OK' if t['success'] else 'FAIL'}")


if __name__ == "__main__":
    main()
