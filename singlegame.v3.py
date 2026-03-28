#!/usr/bin/env python3
"""
singlegame.8902 — NBA Game Browser + Team-Outcome Arb Dashboard
Game finder (Polymarket + Kalshi) + correct arb using actual team outcomes.
NBA only. Filters out finished/expired games.
Port 8902.
"""

import http.server
import socketserver
import json
import logging
import threading
import time
import urllib.request
import os
import re
import asyncio
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("v3")

PORT = 8902
STATE_FILE = "/tmp/singlegame_8902_state.json"
INITIAL_BANKROLL = 1000.00
MAX_BET = 20.00

# NBA team code → full name
TEAM_NAMES = {
    "LAL":"Los Angeles Lakers","LAC":"Los Angeles Clippers","GSW":"Golden State Warriors",
    "PHX":"Phoenix Suns","SAC":"Sacramento Kings","DEN":"Denver Nuggets",
    "MIN":"Minnesota Timberwolves","OKC":"Oklahoma City Thunder","POR":"Portland Trail Blazers",
    "UTA":"Utah Jazz","DAL":"Dallas Mavericks","HOU":"Houston Rockets",
    "SAS":"San Antonio Spurs","MEM":"Memphis Grizzlies","NOP":"New Orleans Pelicans",
    "MIL":"Milwaukee Bucks","CHI":"Chicago Bulls","CLE":"Cleveland Cavaliers",
    "DET":"Detroit Pistons","IND":"Indiana Pacers","BOS":"Boston Celtics",
    "NYK":"New York Knicks","PHI":"Philadelphia 76ers","TOR":"Toronto Raptors",
    "BKN":"Brooklyn Nets","ATL":"Atlanta Hawks","CHA":"Charlotte Hornets",
    "MIA":"Miami Heat","ORL":"Orlando Magic","WAS":"Washington Wizards",
}

KS_TEAM_MAP = {
    "Atlanta":"Atlanta Hawks","Boston":"Boston Celtics","Brooklyn":"Brooklyn Nets",
    "Charlotte":"Charlotte Hornets","Chicago":"Chicago Bulls","Cleveland":"Cleveland Cavaliers",
    "Dallas":"Dallas Mavericks","Denver":"Denver Nuggets","Detroit":"Detroit Pistons",
    "Golden State":"Golden State Warriors","Houston":"Houston Rockets",
    "Indiana":"Indiana Pacers","Los Angeles L":"Los Angeles Lakers",
    "Los Angeles C":"Los Angeles Clippers","Memphis":"Memphis Grizzlies",
    "Miami":"Miami Heat","Milwaukee":"Milwaukee Bucks","Minnesota":"Minnesota Timberwolves",
    "New Orleans":"New Orleans Pelicans","New York":"New York Knicks",
    "Oklahoma City":"Oklahoma City Thunder","Orlando":"Orlando Magic",
    "Philadelphia":"Philadelphia 76ers","Phoenix":"Phoenix Suns",
    "Portland":"Portland Trail Blazers","Sacramento":"Sacramento Kings",
    "San Antonio":"San Antonio Spurs","Toronto":"Toronto Raptors",
    "Utah":"Utah Jazz","Washington":"Washington Wizards",
}

# Reverse: full name → short code for Kalshi matching
NAME_TO_CODE = {v: k for k, v in TEAM_NAMES.items()}

# Keywords to identify a team in a PM outcome name
# code → list of keywords that would appear in a PM outcome string
CODE_KEYWORDS = {
    "LAL": ["laker"],
    "LAC": ["clipper"],
    "GSW": ["warrior"],
    "PHX": ["sun", "phoenix"],
    "SAC": ["king", "sacramento"],
    "DEN": ["nugget", "denver"],
    "MIN": ["timberwolf", "minnesota"],
    "OKC": ["thunder", "oklahoma city"],
    "POR": ["trail blazer", "blazer", "portland"],
    "UTA": ["jazz", "utah"],
    "DAL": ["maverick", "dallas"],
    "HOU": ["rocket", "houston"],
    "SAS": ["spur", "san antonio"],
    "MEM": ["grizzlie", "grizzly", "memphis"],
    "NOP": ["pelican", "new orleans"],
    "MIL": ["buck", "milwaukee"],
    "CHI": ["bull", "chicago"],
    "CLE": ["cavalier", "cleveland"],
    "DET": ["piston", "detroit"],
    "IND": ["pacer", "indiana"],
    "BOS": ["celtic", "boston"],
    "NYK": ["knick", "new york"],
    "PHI": ["76er", "sixer", "philadelphia"],
    "TOR": ["raptor", "toronto"],
    "BKN": ["net", "brooklyn"],
    "ATL": ["hawk", "atlanta"],
    "CHA": ["hornet", "charlotte"],
    "MIA": ["heat", "miami"],
    "ORL": ["magic", "orlando"],
    "WAS": ["wizard", "washington"],
}

# Also handle short 3-letter codes that might appear in PM outcomes
CODE_EXACT = {k: k for k in CODE_KEYWORDS}  # "LAL" → "LAL"

# ---------------------------------------------------------------------------
# Polymarket WebSocket Price Stream
# ---------------------------------------------------------------------------
PM_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class PMPriceStream:
    """
    Connects to Polymarket CLOB WebSocket and caches real-time best bid/ask.
    Runs in its own thread with its own asyncio loop.
    Thread-safe: read pm_stream.prices[token_id] from anywhere.
    """

    def __init__(self):
        self.prices = {}  # token_id → {"bid": float, "ask": float, "ts": float}
        self.token_map = {}  # token_id → team_name
        self.connected = False
        self.error = None
        self.last_update = 0
        self._token_ids = []
        self._thread = None
        self._running = False

    def start(self, token_ids, outcome_names):
        """Start WS connection for given tokens. Kills any existing connection."""
        self.stop()
        self._token_ids = token_ids
        self.token_map = dict(zip(token_ids, outcome_names))
        self.prices = {}
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the WS connection."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        self.connected = False

    def _run_loop(self):
        """Run asyncio event loop in this thread."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._ws_loop())
        except Exception:
            pass
        finally:
            loop.close()

    async def _ws_loop(self):
        """Connect to WS, subscribe, and process messages."""
        while self._running:
            try:
                async with websockets.connect(PM_WS_URL) as ws:
                    self.connected = True
                    self.error = None
                    sub = {
                        "assets_ids": self._token_ids,
                        "type": "market",
                        "custom_feature_enabled": True,
                    }
                    await ws.send(json.dumps(sub))

                    async for raw in ws:
                        if not self._running:
                            break
                        self._parse(raw)

            except Exception as e:
                self.connected = False
                self.error = str(e)
                if self._running:
                    await asyncio.sleep(2)

    def _parse(self, raw):
        """Parse WS message and update prices cache."""
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

            if asset not in self._token_ids:
                continue

            if ev == "book":
                bids = msg.get("bids", [])
                asks = msg.get("asks", [])
                self.prices[asset] = {
                    "bid": float(bids[0]["price"]) if bids else 0,
                    "ask": float(asks[0]["price"]) if asks else 1,
                    "ts": now,
                }
                self.last_update = now

            elif ev == "best_bid_ask":
                self.prices[asset] = {
                    "bid": float(msg.get("best_bid", 0)),
                    "ask": float(msg.get("best_ask", 1)),
                    "ts": now,
                }
                self.last_update = now

            elif ev == "price_change":
                for ch in msg.get("price_changes", []):
                    ch_asset = ch.get("asset_id", "")
                    if ch_asset in self._token_ids:
                        self.prices[ch_asset] = {
                            "bid": float(ch.get("best_bid", 0)),
                            "ask": float(ch.get("best_ask", 1)),
                            "ts": now,
                        }
                        self.last_update = now

    def get_team_prices(self):
        """Return prices dict keyed by team name: {"Hawks": {"bid":..,"ask":..}, ...}"""
        result = {}
        for token_id, team in self.token_map.items():
            if token_id in self.prices:
                p = self.prices[token_id]
                bid = p["bid"]
                ask = p["ask"]
                # Skip placeholder prices (0.01/0.99 is initial book)
                if bid > 0.02 and ask < 0.98:
                    result[team] = {"bid": bid, "ask": ask, "mid": round((bid + ask) / 2, 4)}
        return result


# Global stream instance
pm_stream = PMPriceStream()


def load_state():
    d = {"bankroll":INITIAL_BANKROLL,"trades":[],"total_profit":0.0,"trade_count":0,
         "last_ev":{"a":0.0,"b":0.0},"streak_ev":{"a":0.0,"b":0.0},"streak_count":{"a":0,"b":0},"selected_game":None}
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE) as f:
                s = json.load(f)
            for k in ["bankroll","trades","total_profit","trade_count","last_ev","streak_ev","streak_count","selected_game"]:
                if k in s: d[k] = s[k]
    except: pass
    return d


def save_state():
    try:
        with open(STATE_FILE,"w") as f:
            json.dump({"bankroll":state["bankroll"],"trades":state["trades"][-100:],
                       "total_profit":state["total_profit"],"trade_count":state["trade_count"],
                       "last_ev":state["last_ev"],"streak_ev":state["streak_ev"],
                       "streak_count":state["streak_count"],"selected_game":state["selected_game"]},f)
    except: pass


state = load_state()

live = {"games":[],"pm_data":None,"ks_data":None,"arb":{},"history":[],
        "last_scan":None,"poll_count":0,"error":None,"scan_count":0,
        "pm_outcomes":None,"pm_tokens":None}

PM_TOKENS = {}  # slug → [token0, token1]
PM_OUTCOMES = {}  # slug → [team_a, team_b]


def _e(s): return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
def _esc(s): return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;")
def _f(v):
    if v>=1e6: return "%.1fM"%(v/1e6)
    if v>=1e3: return "%.0fK"%(v/1e3)
    return "%.0f"%v
def _normalize(name): return KS_TEAM_MAP.get(name.strip(), name.strip())


def _is_expired(date_str):
    if not date_str: return False
    try:
        s = date_str.strip()
        if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
            # Date-only: treat as end of day EDT (UTC-4), not UTC
            # So "2026-03-27" expires at 2026-03-28 03:59:59 UTC
            from datetime import timedelta
            dt = datetime.fromisoformat(s).replace(
                hour=23, minute=59, second=59,
                tzinfo=timezone(timedelta(hours=-4))  # EDT
            )
            # Convert to UTC for comparison
            dt = dt.astimezone(timezone.utc)
        else:
            dt = datetime.fromisoformat(s.replace("Z","+00:00"))
        return dt < datetime.now(timezone.utc)
    except:
        return False


# ─── Fetchers ─────────────────────────────────────────────────────────────────
def fetch_pm_games():
    """Scan Polymarket for NBA games using Gamma API. More reliable than web scraping."""
    games = []
    _seen = set()
    TODAY = "2026-03-27"  # only pull today's games

    for order in ["volume24hr", "volume", "liquidity"]:
        for page_offset in [0, 100]:
            url = f"https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=100&offset={page_offset}&order={order}&ascending=false"
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    data = json.loads(resp.read())
                    markets = data if isinstance(data, list) else data.get("data", data.get("markets", []))
                if not markets:
                    break
                for m in markets:
                    slug = m.get("slug", "")
                    if not slug.startswith("nba-"):
                        continue
                    if "-spread-" in slug or "-total-" in slug:
                        continue
                    if slug in _seen:
                        continue
                    # Only today's games
                    if TODAY not in slug:
                        continue
                    is_closed = m.get("closed", False)
                    if is_closed:
                        continue
                    _seen.add(slug)
                    match = re.match(r'nba-([a-z]+)-([a-z]+)-(\d{4}-\d{2}-\d{2})', slug)
                    if match:
                        away_code = match.group(1).upper()
                        home_code = match.group(2).upper()
                        date = match.group(3)
                        away_n = TEAM_NAMES.get(away_code, away_code.title())
                        home_n = TEAM_NAMES.get(home_code, home_code.title())
                        dp = date.split("-")
                        month_names = {"01":"Jan","02":"Feb","03":"Mar","04":"Apr","05":"May","06":"Jun",
                                      "07":"Jul","08":"Aug","09":"Sep","10":"Oct","11":"Nov","12":"Dec"}
                        dd = month_names.get(dp[1] if len(dp)>1 else "", "") + " " + str(int(dp[2])) if len(dp)==3 else date
                        games.append({"title": away_n + " @ " + home_n, "pm_slug": slug, "sport": "NBA", "date_display": dd})
            except Exception as e:
                pass
            time.sleep(0.3)

    log.info(f"PM scan: found {len(games)} NBA games ({TODAY})")
    return games


def fetch_ks_games():
    """Scan Kalshi for NBA games only. Filter to today's games."""
    games = []
    _seen = set()
    base = "https://api.elections.kalshi.com/trade-api/v2"
    series = "KXNBAGAME"
    TODAY = "2026-03-27"
    try:
        url = base + "/events?series_ticker=" + series + "&limit=100"
        req = urllib.request.Request(url, headers={"User-Agent":"EdgeTrader/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            events = json.loads(resp.read()).get("events",[])
            for ev in events[:30]:
                et = ev.get("event_ticker","")
                if not et: continue
                time.sleep(0.5)
                try:
                    mreq = urllib.request.Request(base+"/markets?event_ticker="+et+"&limit=5", headers={"User-Agent":"ET"})
                    with urllib.request.urlopen(mreq, timeout=8) as mresp:
                        for m in json.loads(mresp.read()).get("markets",[]):
                            ya = float(m.get("yes_ask_dollars",0))
                            if 0<ya<1:
                                mt = m.get("title","").replace("Winner?","").strip()
                                pts = mt.split(" at ")
                                if len(pts)==2:
                                    away = _normalize(pts[0]); home = _normalize(pts[1])
                                    display = away + " @ " + home
                                    ticker = m.get("ticker","")
                                    # Extract date from ticker — date is after series prefix
                                    # Format: KXNBAGAME-26MAR27BKNLAL-BKN → date = 26MAR27
                                    dm = re.search(r'KXNBAGAME-(\d{2})([A-Z]{3})(\d{2})', ticker.upper())
                                    dd = ""
                                    game_date = None
                                    if dm:
                                        year = "20" + dm.group(1)
                                        month_code = dm.group(2)
                                        day = dm.group(3)
                                        month_names = {"JAN":"Jan","FEB":"Feb","MAR":"Mar","APR":"Apr","MAY":"May",
                                                      "JUN":"Jun","JUL":"Jul","AUG":"Aug","SEP":"Sep","OCT":"Oct",
                                                      "NOV":"Nov","DEC":"Dec"}
                                        month_num = {"JAN":"01","FEB":"02","MAR":"03","APR":"04","MAY":"05",
                                                     "JUN":"06","JUL":"07","AUG":"08","SEP":"09","OCT":"10",
                                                     "NOV":"11","DEC":"12"}
                                        dd = month_names.get(month_code, "") + " " + str(int(day))
                                        game_date = year + "-" + month_num.get(month_code, "01") + "-" + day
                                    # Only today's games
                                    if game_date != TODAY:
                                        continue
                                    sk = "ks:NBA:"+home+":"+away+":"+dd
                                    if sk not in _seen:
                                        _seen.add(sk)
                                        games.append({"title":display,"ticker":ticker,"sport":"NBA","date_display":dd})
                                    sk = "ks:NBA:"+home+":"+away+":"+dd
                                    if sk not in _seen:
                                        _seen.add(sk)
                                        games.append({"title":display,"ticker":ticker,"sport":"NBA","date_display":dd})
                except Exception as e:
                    if "429" in str(e): time.sleep(3)
    except: pass
    return games


def fetch_pm_detail(slug):
    """
    Get PM prices for both tokens.
    Prefers WebSocket stream (real-time), falls back to CLOB midpoint polling.
    Returns prices keyed by actual team outcome names.
    """
    global PM_TOKENS, PM_OUTCOMES

    # Get token IDs and outcome names from Gamma (only once per slug)
    if slug not in PM_TOKENS:
        url = "https://gamma-api.polymarket.com/markets?slug=" + slug
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"EdgeTrader/1.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read())
                markets = data if isinstance(data, list) else data.get("markets", [])
                for m in markets:
                    tokens = m.get("clobTokenIds","[]")
                    if isinstance(tokens, str): tokens = json.loads(tokens)
                    if len(tokens) == 2:
                        PM_TOKENS[slug] = tokens
                        outcomes_raw = m.get("outcomes","[]")
                        if isinstance(outcomes_raw, str): outcomes_raw = json.loads(outcomes_raw)
                        PM_OUTCOMES[slug] = outcomes_raw
                        break
        except: pass

    if slug not in PM_TOKENS:
        return None, None

    teams = PM_OUTCOMES.get(slug, ["Team_0","Team_1"])

    # Try WebSocket prices first (real-time, no API call)
    ws_prices = pm_stream.get_team_prices()
    if len(ws_prices) >= 2:
        prices = {}
        for team in teams:
            if team in ws_prices:
                wp = ws_prices[team]
                prices[team] = {"price": wp["mid"], "bid": wp["bid"], "ask": wp["ask"]}
        if len(prices) >= 2:
            return {
                "q": " vs. ".join(teams),
                "teams": teams,
                "prices": prices,
                "vol": 0,
                "source": "websocket",
            }, teams

    # Fallback: fetch midpoints for BOTH tokens via REST
    prices = {}
    for i, token_id in enumerate(PM_TOKENS[slug]):
        team = teams[i]
        url = "https://clob.polymarket.com/midpoint?token_id=" + token_id
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"EdgeTrader/1.0"})
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read())
                mid = float(data.get("mid", 0))
                prices[team] = {"price": mid, "bid": round(mid - 0.005, 3), "ask": round(mid + 0.005, 3)}
        except:
            pass

    if len(prices) < 2:
        return None, None

    return {
        "q": " vs. ".join(teams),
        "teams": teams,
        "prices": prices,
        "vol": 0,
        "source": "rest_poll",
    }, teams


def _code_for_outcome(outcome_name, ks_code):
    """
    Check if a PM outcome name corresponds to a KS team code.
    Returns True if the outcome matches this code.
    """
    name_lower = outcome_name.lower().strip()
    # Direct code match (e.g. PM outcome is "BKN")
    if name_lower == ks_code.lower():
        return True
    # Keyword match
    keywords = CODE_KEYWORDS.get(ks_code, [])
    for kw in keywords:
        if kw in name_lower:
            return True
    return False


def fetch_ks_detail(ticker):
    """
    Fetch Kalshi market. Returns prices + team mapping.
    YES = the ticker's team wins. NO = other team wins.
    """
    base = "https://api.elections.kalshi.com/trade-api/v2"
    try:
        req = urllib.request.Request(base+"/markets/"+ticker, headers={"User-Agent":"EdgeTrader/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            m = json.loads(resp.read()).get("market",{})
            raw = m.get("title","").replace("Winner?","").strip()
            pts = raw.split(" at ")

            # TICKER SUFFIX = team that wins on YES. This is the source of truth.
            ks_team_code = ticker.split("-")[-1]  # e.g. "BKN" from "...-BKN"
            ks_team_name = TEAM_NAMES.get(ks_team_code, ks_team_code)

            # Other team = the one in the title that isn't the YES team
            # Title format: "Away at Home"
            away_raw = pts[0].strip() if len(pts) >= 1 else ""
            home_raw = pts[1].strip() if len(pts) >= 2 else ""
            away_name = _normalize(away_raw)
            home_name = _normalize(home_raw)

            # The YES team is identified by code. The NO team is the other one.
            if _code_for_outcome(away_name, ks_team_code) or ks_team_code.lower() in away_name.lower():
                ks_team_display = away_name
                other_team_display = home_name
            elif _code_for_outcome(home_name, ks_team_code) or ks_team_code.lower() in home_name.lower():
                ks_team_display = home_name
                other_team_display = away_name
            else:
                # Fallback: use code name directly
                ks_team_display = ks_team_name
                other_team_display = away_name if home_name == ks_team_name else home_name

            title = away_name + " vs. " + home_name

            return {
                "t": title,
                "ks_ticker": ticker,
                "ks_team_code": ks_team_code,
                "ks_team": ks_team_display,      # team that wins on YES (by code)
                "ks_other": other_team_display,  # team that wins on NO
                "ks_yes_bid": float(m.get("yes_bid_dollars",0)),
                "ks_yes_ask": float(m.get("yes_ask_dollars",0)),
                "ks_no_bid": float(m.get("no_bid_dollars",0)),
                "ks_no_ask": float(m.get("no_ask_dollars",0)),
                "ks_yes_last": float(m.get("last_price_dollars",0)),
                "ks_no_price": round(1.0 - float(m.get("last_price_dollars",0)), 3),
                "vol": float(m.get("volume_fp",0)),
            }
    except: return None


def resolve_team_names(pm_detail, pm_outcome_list, ks):
    """
    Map KS team to PM outcomes using CODE-BASED matching, not fuzzy name matching.
    
    Strategy:
    1. Extract KS ticker code (e.g. "BKN" from "...-BKN")
    2. For each PM outcome, check if it matches this code via keywords
    3. The matching PM outcome = KS YES team, the other = KS NO team
    4. If code matching fails, use positional: KS YES = first PM outcome (away), NO = second (home)
    
    Returns (ks_yes_pm_name, ks_no_pm_name) — the PM outcome names for YES and NO.
    """
    if not ks:
        return None, None

    ks_ticker = ks.get("ks_ticker", "")
    ks_code = ks_ticker.split("-")[-1] if ks_ticker else ""  # e.g. "BKS" from ticker

    # If we have PM outcomes, do code-based matching
    if pm_outcome_list and len(pm_outcome_list) >= 2 and ks_code:
        match_idx = None
        for i, outcome in enumerate(pm_outcome_list):
            if _code_for_outcome(outcome, ks_code):
                match_idx = i
                break

        if match_idx is not None:
            yes_name = pm_outcome_list[match_idx]
            no_name = pm_outcome_list[1 - match_idx]  # the other one
            return yes_name, no_name

        # Fallback: positional mapping
        # Polymarket convention: token 0 = first outcome (usually away), token 1 = second (home)
        # Kalshi convention: YES = home team (from "Away at Home Winner?")
        # So KS YES (home) → PM token 1, KS NO (away) → PM token 0
        return pm_outcome_list[1], pm_outcome_list[0]

    # No PM outcomes available — use KS names directly
    return ks.get("ks_team", ""), ks.get("ks_other", "")


def calc_arb(pm_detail, pm_outcome_list, ks):
    """
    Compute arb using ACTUAL team outcomes, not YES/NO.

    PM: pm_detail["prices"][team_a].ask and pm_detail["prices"][team_b].ask
    KS: ks_yes_ask = price for ks_team wins
        ks_no_ask  = price for ks_other wins

    True arb: bet on OPPOSITE outcomes across platforms.
      Side A: PM[k_other] + KS[yes]  → covers both outcomes
      Side B: PM[k_team] + KS[no]    → covers both outcomes
    """
    if not pm_detail or not ks:
        return {}

    ks_team, ks_other = resolve_team_names(pm_detail, pm_outcome_list, ks)
    if not ks_team or not ks_other:
        return {}

    pm_prices = pm_detail.get("prices", {})

    # Get PM ask prices for each team
    pm_ask_ks_team = pm_prices.get(ks_team, {}).get("ask", 0)
    pm_ask_other = pm_prices.get(ks_other, {}).get("ask", 0)

    ks_yes_ask = ks["ks_yes_ask"]
    ks_no_ask = ks["ks_no_ask"]

    # Side A: Bet PM[k_other team wins] + KS[ks_team wins via YES]
    #   If k_other wins: PM pays $1, KS loses → profit = 1 - cost
    #   If ks_team wins: PM loses, KS pays $1 → profit = 1 - cost
    cost_a = pm_ask_other + ks_yes_ask
    profit_a = round(1.0 - cost_a, 4)
    roi_a = round(profit_a / cost_a * 100, 2) if cost_a > 0 else 0

    # Side B: Bet PM[ks_team wins] + KS[k_other wins via NO]
    #   If ks_team wins: PM pays $1, KS loses → profit = 1 - cost
    #   If k_other wins: PM loses, KS pays $1 → profit = 1 - cost
    cost_b = pm_ask_ks_team + ks_no_ask
    profit_b = round(1.0 - cost_b, 4)
    roi_b = round(profit_b / cost_b * 100, 2) if cost_b > 0 else 0

    return {
        "a": {
            "l": "PM %s + KS %s" % (ks_other, ks_team),
            "det": "PM %s ($%.3f) + KS %s ($%.3f)" % (ks_other, pm_ask_other, ks_team, ks_yes_ask),
            "c": round(cost_a, 4), "p": profit_a, "r": roi_a, "ok": profit_a > 0,
            "pm_team": ks_other, "ks_team": ks_team,
            "_pm_price": pm_ask_other, "_ks_price": ks_yes_ask,
        },
        "b": {
            "l": "PM %s + KS %s (NO)" % (ks_team, ks_other),
            "det": "PM %s ($%.3f) + KS %s ($%.3f)" % (ks_team, pm_ask_ks_team, ks_other, ks_no_ask),
            "c": round(cost_b, 4), "p": profit_b, "r": roi_b, "ok": profit_b > 0,
            "pm_team": ks_team, "ks_team": ks_other,
            "_pm_price": pm_ask_ks_team, "_ks_price": ks_no_ask,
        },
    }


def place_trade(arb):
    trades = []
    pm_data = live.get("pm_data") or {}
    for side in ["a","b"]:
        a = arb.get(side)
        if not a or a["p"]<=0:
            state["last_ev"][side]=0.0; state["streak_ev"][side]=0.0; state["streak_count"][side]=0; continue
        ce = a["p"]; le = state["last_ev"][side]; se = state["streak_ev"][side]; sc = state["streak_count"][side]
        if ce>0:
            if ce>=se and se>0: state["streak_count"][side]+=1
            else: state["streak_ev"][side]=ce; state["streak_count"][side]=1
            sc = state["streak_count"][side]
        else: state["streak_ev"][side]=0.0; state["streak_count"][side]=0; continue
        do = False; tier = ""
        if le==0.0 and ce>0: do=True; tier="INITIAL"
        elif ce>le+0.005 and le>0: do=True; tier="SCALE +%.0fbp"%((ce-le)*1000)
        elif sc>=5 and ce>=se and sc%5==0: do=True; tier="STREAK x%d"%(sc//5)
        if not do or state["bankroll"]<0.01: continue
        if "INITIAL" in tier: bet=min(MAX_BET,state["bankroll"]*0.1)
        elif "SCALE" in tier: bet=min(MAX_BET,state["bankroll"]*0.05)*min((ce-le)/0.01,3.0)
        else: bet=min(MAX_BET,state["bankroll"]*0.08)
        bet=min(bet,MAX_BET,state["bankroll"])
        if bet<0.01: continue
        profit = bet*(a["p"]/a["c"]) if a["c"]>0 else 0

        # Per-leg allocation: equal payout on both sides
        # Stake_A = bet × price_B / (price_A + price_B)
        # Stake_B = bet × price_A / (price_A + price_B)
        # Payout = bet / (price_A + price_B) — same regardless of outcome
        pm_price = a.get("_pm_price", 0)
        ks_price = a.get("_ks_price", 0)
        cost_sum = pm_price + ks_price
        if cost_sum > 0:
            pm_stake = round(bet * ks_price / cost_sum, 2)
            ks_stake = round(bet * pm_price / cost_sum, 2)
            guaranteed_payout = round(bet / cost_sum, 2)
        else:
            pm_stake = round(bet / 2, 2)
            ks_stake = round(bet / 2, 2)
            guaranteed_payout = 0
        leg_detail = a["det"] + " | PM: $%.2f + KS: $%.2f → $%.2f payout" % (pm_stake, ks_stake, guaranteed_payout)

        t={"time":datetime.now().strftime("%H:%M:%S"),"game":(state.get("selected_game") or {}).get("name",""),
           "side":side,"detail":leg_detail,"cost":round(bet,4),"profit":round(profit,4),
           "bankroll_before":round(state["bankroll"],2),"bankroll_after":round(state["bankroll"]+profit,2),
           "roi_pct":round(a["r"],2),"ev":round(ce,4),"ev_increase":round(ce-le if le>0 else ce,4),"tier":tier}
        state["bankroll"]+=profit; state["total_profit"]+=profit; state["trade_count"]+=1
        state["last_ev"][side]=ce; state["trades"].append(t); trades.append(t)
    if len(state["trades"])>300: state["trades"]=state["trades"][-300:]
    if trades: save_state()
    return trades


# ─── Background threads ───────────────────────────────────────────────────────
def game_scanner():
    while True:
        try:
            pm = fetch_pm_games()
            ks = fetch_ks_games()
            live["games"] = pm + ks
            live["last_scan"] = datetime.now().strftime("%H:%M:%S")
            live["scan_count"] = live.get("scan_count",0) + 1
            log.info(f"Scanner: {len(pm)} PM + {len(ks)} KS = {len(pm)+len(ks)} total games")
        except Exception as e:
            live["error"]=str(e)
            log.error(f"Scanner error: {e}")
        time.sleep(30)  # scan every 30 seconds


def game_poller():
    while True:
        sel = state.get("selected_game")
        if sel:
            try:
                pm_detail, pm_outcomes = fetch_pm_detail(sel.get("pm_slug","")) if sel.get("pm_slug") else (None, None)
                ks = fetch_ks_detail(sel.get("ks_ticker","")) if sel.get("ks_ticker") else None
                arb = calc_arb(pm_detail, pm_outcomes, ks)
                live["pm_data"]=pm_detail; live["pm_outcomes"]=pm_outcomes
                live["ks_data"]=ks; live["arb"]=arb
                live["poll_count"]+=1; live["error"]=None
                nt = place_trade(arb)

                # Price feed by team names
                ks_team, ks_other = resolve_team_names(pm_detail, pm_outcomes, ks)
                pm_prices = (pm_detail or {}).get("prices",{})
                py = pm_prices.get(ks_other,{}).get("price") if ks_other else None
                pn = pm_prices.get(ks_team,{}).get("price") if ks_team else None
                ky = (ks or {}).get("ks_yes_ask")
                kn = (ks or {}).get("ks_no_ask")

                live["history"].append({"t":datetime.now().strftime("%H:%M:%S"),
                    "py":py,"pn":pn,"ky":ky,"kn":kn,
                    "team_a":ks_other,"team_b":ks_team,
                    "aa":arb.get("a",{}).get("p"),"ab":arb.get("b",{}).get("p"),"trades":len(nt)})
                if len(live["history"])>500: live["history"]=live["history"][-500:]
                # Show data source in error field (for debugging)
                src = (pm_detail or {}).get("source", "?")
                live["ws_connected"] = pm_stream.connected
                live["data_source"] = src
            except Exception as e: live["error"]=str(e)
        time.sleep(1)  # Kalshi polls every 1s (PM is real-time via WebSocket)


# ─── HTML ─────────────────────────────────────────────────────────────────────
CSS = """<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0a0a0a;color:#e0e0e0;font-family:monospace;padding:20px;font-size:14px}
h1{color:#00ff88;font-size:1.3em}h2.tab{font-size:1.0em;color:#00ff88;margin:18px 0 8px 0;border-bottom:1px solid #222;padding-bottom:4px}
.sub{color:#555;font-size:.78em;margin-bottom:14px}
.bar{display:flex;justify-content:space-between;padding:7px 12px;background:#111;border-radius:6px;margin-bottom:14px;font-size:.78em;color:#888}
.dot{width:8px;height:8px;border-radius:50%;background:#00ff88;display:inline-block;margin-right:5px}
.g{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px}.two-col{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.cd{background:#111;border:1px solid #1a1a1a;border-radius:8px;padding:14px}
.cd h2{font-size:.78em;color:#555;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:10px}
.r{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #161616}.r:last-child{border:none}
.l{color:#777;font-size:.82em}.v{font-size:1.1em;font-weight:700}
.grn{color:#00ff88}.red{color:#ff4444}.blu{color:#44aaff}.wht{color:#fff}.yel{color:#ffcc00}
.ab{border-radius:8px;padding:12px;margin-top:8px;text-align:center}
.ab.ok{background:#001a0d;border:1px solid #004d26}
.ab.nope{background:#1a0000;border:1px solid #4d0000}
.ab .big{font-size:1.6em;font-weight:700}.ab .dt{font-size:.75em;color:#666;margin-top:3px}
.ht{margin-top:12px;background:#111;border:1px solid #1a1a1a;border-radius:8px;padding:14px}
.ht h2{font-size:.78em;color:#555;text-transform:uppercase;letter-spacing:1.5px;margin-bottom:8px}
table{width:100%;font-size:.72em;border-collapse:collapse}
th{text-align:left;color:#444;padding:3px 8px;border-bottom:1px solid #222}
td{padding:4px 8px;border-bottom:1px solid #151515}
tr.trd{background:#0a0a1a}tr.trd:hover{background:#111122}
.sport-cell{color:#888;font-size:.75em;width:50px}.date-cell{color:#888;font-size:.75em;text-align:right}
.game-row{cursor:pointer}.game-row:hover{background:#1a2a1a}
.game-row.selected{background:#0a2a1a;border-left:3px solid #00ff88}
.game-row.hidden{display:none}.game-row.highlight{background:#1a2a0a}
.nd{color:#444;text-align:center;padding:20px}
.sel-bar{background:#111;border:1px solid #00ff88;border-radius:8px;padding:12px 16px;margin-bottom:14px;display:flex;align-items:center;justify-content:space-between}
.sel-label{color:#00ff88;font-weight:700;margin-right:8px}
.go-btn{background:#00ff88;color:#0a0a0a;border:none;border-radius:6px;padding:10px 24px;font-family:monospace;font-weight:700;font-size:.95em;cursor:pointer;text-decoration:none}
.go-btn:hover{background:#00cc6e}
.go-btn.disabled{background:#333;color:#666;cursor:not-allowed;pointer-events:none}
.search-bar{margin-bottom:10px}
.search-bar input{background:#0a0a0a;border:1px solid #333;border-radius:6px;color:#e0e0e0;padding:6px 10px;font-family:monospace;font-size:.85em;width:100%}
.search-bar input:focus{outline:none;border-color:#00ff88}
.search-bar input::placeholder{color:#444}
.back{font-size:.82em;margin-bottom:12px}
a{color:#44aaff;text-decoration:none}a:hover{text-decoration:underline}
</style>"""


def build_browser():
    games = live.get("games",[])
    ls = live.get("last_scan") or "--"
    pm_g = sorted([g for g in games if "pm_slug" in g], key=lambda x: x.get("title",""))
    ks_g = sorted([g for g in games if "ticker" in g], key=lambda x: x.get("title",""))

    pm_r = ""
    for g in pm_g[:100]:
        pm_r += ('<tr class="game-row" data-search="'+_esc((g["title"]).lower())+'" '
                 'onclick="selPM(\''+_esc(g["pm_slug"])+'\',this)">'
                 '<td class="sport-cell">'+g.get("sport","")+'</td>'
                 '<td>'+_e(g["title"])+'</td>'
                 '<td class="date-cell">'+_e(g.get("date_display",""))+'</td></tr>')
    if not pm_r: pm_r = '<tr><td colspan="3" class="nd">Scanning...</td></tr>'

    ks_r = ""
    for g in ks_g[:100]:
        ks_r += ('<tr class="game-row" data-search="'+_esc((g["title"]).lower())+'" '
                 'onclick="selKS(\''+_esc(g["ticker"])+'\',this)">'
                 '<td class="sport-cell">'+g.get("sport","")+'</td>'
                 '<td>'+_e(g["title"])+'</td>'
                 '<td class="date-cell">'+_e(g.get("date_display",""))+'</td></tr>')
    if not ks_r: ks_r = '<tr><td colspan="3" class="nd">Scanning...</td></tr>'

    scan_count = live.get("scan_count",0)
    scanning = " <span class='yel'>Scanning...</span>" if scan_count == 0 else ""

    return ('<!DOCTYPE html><html><head><meta charset="utf-8"><meta http-equiv="refresh" content="15">'
        '<title>NBA Game Scanner</title>'+CSS+'</head><body>'
        '<h1>&#127936; NBA Game Scanner</h1>'
        '<div class="sub">Select one from each column to load arb.'+scanning+'</div>'
        '<div class="bar"><span><span class="dot"></span> '+ls+' | '+str(len(pm_g))+' PM + '+str(len(ks_g))+' KS | Scans: '+str(scan_count)+'</span></div>'
        '<div class="sel-bar"><div>'
        '<span class="sel-label">PM:</span><span id="pmS">None</span> &nbsp; '
        '<span class="sel-label">KS:</span><span id="ksS">None</span></div>'
        '<a id="go" class="go-btn disabled" href="#">Open Arb</a></div>'
        '<div class="search-bar"><input id="sq" placeholder="Search teams..." oninput="flt(this.value)"></div>'
        '<div class="two-col">'
        '<div class="cd"><h2>Polymarket</h2><table><tr><th>Sport</th><th>Game</th><th>Date</th></tr>'+pm_r+'</table></div>'
        '<div class="cd"><h2>Kalshi</h2><table><tr><th>Sport</th><th>Game</th><th>Date</th></tr>'+ks_r+'</table></div>'
        '</div>'
        '<script>var p="",k="";'
        'function selPM(s,e){document.querySelectorAll(".two-col .cd:first-child .game-row").forEach(r=>r.classList.remove("selected"));'
        'e.classList.add("selected");p=s;document.getElementById("pmS").textContent=e.querySelector("td:nth-child(2)").textContent.substring(0,30);ub()}'
        'function selKS(s,e){document.querySelectorAll(".two-col .cd:last-child .game-row").forEach(r=>r.classList.remove("selected"));'
        'e.classList.add("selected");k=s;document.getElementById("ksS").textContent=e.querySelector("td:nth-child(2)").textContent.substring(0,30);ub()}'
        'function ub(){var b=document.getElementById("go");if(p&&k){b.href="/arb?pm="+encodeURIComponent(p)+"&ks="+encodeURIComponent(k);b.classList.remove("disabled")}else{b.href="#";b.classList.add("disabled")}}'
        'function flt(q){q=q.toLowerCase().trim();document.querySelectorAll(".game-row").forEach(function(r){'
        'var t=r.getAttribute("data-search")||"";if(!q){r.classList.remove("hidden","highlight");return}'
        'var m=q.split(" ").every(function(w){return t.indexOf(w)!==-1});'
        'if(m){r.classList.remove("hidden");r.classList.add("highlight")}else{r.classList.add("hidden");r.classList.remove("highlight")}})}'
        '</script></body></html>')


def build_arb():
    pm = live.get("pm_data")
    ks = live.get("ks_data")
    arb = live.get("arb",{})
    h = live.get("history",[])
    trades = state.get("trades",[])
    pm_outcomes = live.get("pm_outcomes")

    ks_team, ks_other = resolve_team_names(pm, pm_outcomes, ks)
    pm_teams = (pm or {}).get("teams",[])
    pm_prices = (pm or {}).get("prices",{})
    game_title = " vs. ".join(pm_teams) if pm_teams else ((ks or {}).get("t","Loading..."))

    # PM card — both team prices
    if pm and pm_teams:
        pm_rows = ""
        for team in pm_teams:
            p = pm_prices.get(team,{})
            pm_rows += '<div class="r"><span class="l">%s</span><span class="v blu">$%.3f</span></div>' % (_e(team), p.get("price",0))
        pm_h = ('<div class="r"><span class="l">Market</span><span class="v" style="font-size:.7em">%s</span></div>'
                % _e((pm.get("q","")[:50]))) + pm_rows + (
            '<div class="r"><span class="l">%s Bid</span><span class="v">$%.3f</span></div>'
            '<div class="r"><span class="l">%s Ask</span><span class="v">$%.3f</span></div>'
            '<div class="r"><span class="l">Volume</span><span class="v">$%s</span></div>'
        ) % (
            _e(ks_other or ""), pm_prices.get(ks_other or "",{}).get("bid",0),
            _e(ks_other or ""), pm_prices.get(ks_other or "",{}).get("ask",0),
            _f(pm.get("vol",0))
        )
    else:
        pm_h = '<div class="nd">Loading...</div>'

    # KS card — YES/NO mapped to team names
    if ks:
        ks_h = (
            '<div class="r"><span class="l">Market</span><span class="v" style="font-size:.7em">%s</span></div>'
            '<div class="r"><span class="l">%s (YES) bid</span><span class="v">$%.3f</span></div>'
            '<div class="r"><span class="l">%s (YES) ask</span><span class="v blu">$%.3f</span></div>'
            '<div class="r"><span class="l">%s (NO) bid</span><span class="v">$%.3f</span></div>'
            '<div class="r"><span class="l">%s (NO) ask</span><span class="v">$%.3f</span></div>'
            '<div class="r"><span class="l">Volume</span><span class="v">$%s</span></div>'
        ) % (
            _e(ks.get("t","")[:50]),
            _e(ks_team or ""), ks.get("ks_yes_bid",0),
            _e(ks_team or ""), ks.get("ks_yes_ask",0),
            _e(ks_other or ""), ks.get("ks_no_bid",0),
            _e(ks_other or ""), ks.get("ks_no_ask",0),
            _f(ks.get("vol",0)),
        )
    else:
        ks_h = '<div class="nd">Loading...</div>'

    # Arb — by team name
    arb_h = ""
    for k in ["a","b"]:
        a = arb.get(k)
        if a:
            c = "grn" if a["ok"] else "red"
            e = "OK" if a["ok"] else "X"
            bx = "ok" if a["ok"] else "nope"
            s1 = "+" if a["p"] >= 0 else ""
            s2 = "+" if a["r"] >= 0 else ""
            arb_h += ('<div class="ab '+bx+'">'
                      '<div class="'+c+' big">'+e+' $'+s1+('%.4f'%a["p"])+' ('+s2+('%.2f'%a["r"])+'&#37;)</div>'
                      '<div class="dt">'+_e(a["det"])+'</div>'
                      '<div class="dt">Cost $'+('%.4f'%a["c"])+' &rarr; $1.00</div>'
                      '</div>')
    if not arb_h:
        arb_h = '<div class="nd">Loading...</div>'

    # Price feed — by team names
    if h:
        team_a = h[-1].get("team_a", ks_other or "Team A")
        team_b = h[-1].get("team_b", ks_team or "Team B")
        rows = ""
        for r in reversed(h[-25:]):
            aa, ab = r.get("aa"), r.get("ab")
            a1 = '<span class="%s">$%+.4f</span>'%("grn" if aa and aa>0 else "red",aa) if aa is not None else "--"
            a2 = '<span class="%s">$%+.4f</span>'%("grn" if ab and ab>0 else "red",ab) if ab is not None else "--"
            trd = r.get("trades",0)
            ts = '<span class="yel">%d</span>'%trd if trd>0 else "0"
            rows += '<tr><td>%s</td><td>$%.3f</td><td>$%.3f</td><td>$%.3f</td><td>$%.3f</td><td>%s</td><td>%s</td><td>%s</td></tr>'%(
                r["t"], r.get("py") or 0, r.get("pn") or 0, r.get("ky") or 0, r.get("kn") or 0, a1, a2, ts)
        feed = ('<table><tr><th>Time</th><th>PM %s</th><th>PM %s</th><th>KS %s</th><th>KS %s</th><th>Arb A</th><th>Arb B</th><th>Trades</th></tr>'
                %(_e(team_a),_e(team_b),_e(team_b),_e(team_a)) + rows + '</table>')
    else:
        feed = '<div class="nd">No data yet.</div>'

    # Paper trading
    br = state["bankroll"]; pnl = br-INITIAL_BANKROLL
    pc = "grn" if pnl>=0 else "red"; ps = "+" if pnl>=0 else ""
    psum = (
        '<div class="r"><span class="l">Starting</span><span class="v">$1,000.00</span></div>'
        '<div class="r"><span class="l">Current</span><span class="v %s">$%.2f</span></div>'
        '<div class="r"><span class="l">P&amp;L</span><span class="v %s">%s$%.2f</span></div>'
        '<div class="r"><span class="l">Return</span><span class="v %s">%s%.2f&#37;</span></div>'
        '<div class="r"><span class="l">Trades</span><span class="v">%d</span></div>'
        '<div class="r"><span class="l">Max Bet</span><span class="v">$%.2f</span></div>'
    )%(pc,br,pc,ps,pnl,pc,ps,(pnl/INITIAL_BANKROLL*100) if INITIAL_BANKROLL>0 else 0,state["trade_count"],MAX_BET)

    if trades:
        trows = ""
        for t in reversed(trades[-25:]):
            sc = "blu" if t["side"]=="a" else "wht"; sl = "A" if t["side"]=="a" else "B"
            tier = t.get("tier",""); tc = "yel" if "SCALE" in tier else "grn"
            trows += ('<tr class="trd"><td>%s</td><td><span class="%s">%s</span></td>'
                      '<td style="font-size:.7em">%s</td><td>$%.2f</td><td class="grn">$%.4f</td>'
                      '<td>$%.2f</td><td>$%.2f</td><td class="grn">+%.2f&#37;</td>'
                      '<td>$%.4f</td><td>+$%.4f</td><td><span class="%s">%s</span></td></tr>')%(
                t["time"],sc,sl,_e(t.get("detail","")[:45]),t["cost"],t["profit"],
                t["bankroll_before"],t["bankroll_after"],t["roi_pct"],
                t.get("ev",0),t.get("ev_increase",0),tc,_e(tier))
        th = ('<table><tr><th>Time</th><th>Side</th><th>Legs</th><th>Cost</th><th>Profit</th>'
              '<th>Before</th><th>After</th><th>Edge</th><th>EV</th><th>EV+</th><th>Tier</th></tr>'
              +trows+'</table>')
    else:
        th = '<div class="nd">No trades yet.</div>'

    err = ' <span class="red">err: '+_e(live.get("error") or "")+'</span>' if live.get("error") else ""

    return ('<!DOCTYPE html><html><head><meta charset="utf-8"><meta http-equiv="refresh" content="3">'
        '<title>Arb: '+_e(game_title)+'</title>'+CSS+'</head><body>'
        '<div class="back"><a href="/">&larr; Game Browser</a></div>'
        '<h1>&#127936; '+_e(game_title)+'</h1>'
        '<div class="sub">Poll #'+str(live.get("poll_count",0))+' | 3s'+err+'</div>'
        '<div class="g">'
        '<div class="cd"><h2>Polymarket</h2>'+pm_h+'</div>'
        '<div class="cd"><h2>Kalshi</h2>'+ks_h+'</div>'
        '<div class="cd" style="border-color:#00ff88"><h2>&#9889; Arbitrage</h2>'+arb_h+'</div>'
        '</div>'
        '<div class="ht"><h2>&#128200; Price Feed</h2>'+feed+'</div>'
        '<h2 class="tab">&#128176; Paper Trading ($1K bankroll / $20 max bet)</h2>'
        '<div class="two-col">'
        '<div class="cd"><h2>Summary</h2>'+psum+'</div>'
        '<div class="cd"><h2>Trade Log</h2>'+th+'</div>'
        '</div>'
        '<script>setTimeout(function(){location.reload()},3000)</script></body></html>')


# ─── WebSocket Startup ─────────────────────────────────────────────────────────
def _start_pm_ws(pm_slug):
    """Fetch token IDs for a slug and start the PM WebSocket stream."""
    global PM_TOKENS, PM_OUTCOMES

    # Get tokens + outcomes from Gamma (cached or fresh)
    if pm_slug not in PM_TOKENS:
        url = "https://gamma-api.polymarket.com/markets?slug=" + pm_slug
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "EdgeTrader/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
                markets = data if isinstance(data, list) else data.get("markets", [])
                for m in markets:
                    tokens = m.get("clobTokenIds", "[]")
                    if isinstance(tokens, str): tokens = json.loads(tokens)
                    outcomes = m.get("outcomes", "[]")
                    if isinstance(outcomes, str): outcomes = json.loads(outcomes)
                    if len(tokens) == 2:
                        PM_TOKENS[pm_slug] = tokens
                        PM_OUTCOMES[pm_slug] = outcomes
                        break
        except:
            pass

    if pm_slug in PM_TOKENS:
        tokens = PM_TOKENS[pm_slug]
        outcomes = PM_OUTCOMES[pm_slug]
        pm_stream.start(tokens, outcomes)


# ─── Handler ──────────────────────────────────────────────────────────────────
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)

        if "pm" in qs and "ks" in qs and parsed.path=="/arb":
            pm_slug = qs["pm"][0].strip()
            ks_ticker = qs["ks"][0].strip()
            # Build display name from the slugs
            pm_parts = re.match(r'nba-([a-z]+)-([a-z]+)', pm_slug)
            name = pm_slug
            if pm_parts:
                away = TEAM_NAMES.get(pm_parts.group(2).upper(), pm_parts.group(2).title())
                home = TEAM_NAMES.get(pm_parts.group(1).upper(), pm_parts.group(1).title())
                name = away + " @ " + home
            state["selected_game"]={"pm_slug":pm_slug,"ks_ticker":ks_ticker,"name":name}
            live["pm_data"]=None; live["pm_outcomes"]=None; live["ks_data"]=None
            live["arb"]={}; live["history"]=[]
            live["poll_count"]=0
            state["last_ev"]={"a":0.0,"b":0.0}; state["streak_ev"]={"a":0.0,"b":0.0}
            state["streak_count"]={"a":0,"b":0}
            # Start PM WebSocket for this game
            _start_pm_ws(pm_slug)
            save_state()
            self.send_response(302); self.send_header("Location","/arb"); self.end_headers(); return

        if parsed.path=="/arb":
            self.send_response(200); self.send_header("Content-Type","text/html; charset=utf-8")
            self.end_headers(); self.wfile.write(build_arb().encode()); return

        # Default: game browser
        self.send_response(200); self.send_header("Content-Type","text/html; charset=utf-8")
        self.end_headers(); self.wfile.write(build_browser().encode())

    def log_message(self,*a): pass


class Srv(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True; allow_reuse_address = True


if __name__=="__main__":
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
    state = load_state()
    threading.Thread(target=game_scanner,daemon=True).start()
    threading.Thread(target=game_poller,daemon=True).start()
    s = Srv(("0.0.0.0",PORT),H)
    print("NBA Game Scanner + Arb: http://localhost:%d"%PORT)
    try: s.serve_forever()
    except KeyboardInterrupt: save_state(); print("\nStopped.")
