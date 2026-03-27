#!/usr/bin/env python3
"""
order_executor.py — Order placement for Polymarket CLOB + Kalshi.
Uses REST API with HMAC/RSA signing. No SDK dependency.

Both platforms use IOC (immediate-or-cancel) orders for arb execution.
"""

import base64
import hashlib
import hmac
import json
import logging
import time
import urllib.request
from typing import Optional

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

log = logging.getLogger("order_executor")


# ---------------------------------------------------------------------------
# Polymarket CLOB Order Placement
# ---------------------------------------------------------------------------
PM_CLOB_BASE = "https://clob.polymarket.com"


class PolymarketTrader:
    """
    Places orders on Polymarket CLOB API.
    Auth: Builder API Key (HMAC-SHA256 signing).
    """

    def __init__(self, api_key: str, api_secret: str, passphrase: str):
        self.api_key = api_key
        # Secret is base64-encoded, decode to bytes for HMAC
        self.api_secret = base64.b64decode(api_secret)
        self.passphrase = passphrase

    def _sign(self, timestamp: str, method: str, path: str, body: str = "") -> str:
        """Generate HMAC-SHA256 signature for Polymarket CLOB API."""
        message = timestamp + method.upper() + path + body
        signature = hmac.new(
            self.api_secret,
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        return base64.b64encode(signature).decode("utf-8")

    def _headers(self, method: str, path: str, body: str = "") -> dict:
        """Build authenticated request headers."""
        timestamp = str(int(time.time()))
        signature = self._sign(timestamp, method, path, body)
        return {
            "Content-Type": "application/json",
            "POLY-ACCESS-KEY": self.api_key,
            "POLY-ACCESS-SIGNATURE": signature,
            "POLY-ACCESS-TIMESTAMP": timestamp,
            "POLY-ACCESS-PASSPHRASE": self.passphrase,
        }

    def get_order_book(self, token_id: str) -> dict:
        """Get order book for a token."""
        path = f"/orderbook/{token_id}"
        url = PM_CLOB_BASE + path
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read())
        except Exception as e:
            log.error(f"PM get_order_book error: {e}")
            return {}

    def place_ioc_order(self, token_id: str, side: str, price: float, size: float) -> dict:
        """
        Place an IOC (immediate-or-cancel) market/limit order.
        
        Args:
            token_id: The CLOB token ID for the outcome
            side: "BUY" or "SELL"
            price: Limit price (0.0 to 1.0)
            size: Number of shares
        
        Returns:
            Order response dict with success, orderID, status, etc.
        """
        path = "/order"

        # Polymarket expects amounts in specific units
        # For BUY: makerAmount = USDC (in base units), takerAmount = shares
        # For SELL: makerAmount = shares, takerAmount = USDC
        price_raw = str(int(price * 100))  # price in cents as string
        size_raw = str(int(size * 100))     # size in shares * 100

        order_payload = {
            "order": {
                "tokenId": token_id,
                "side": side.upper(),
                "price": price_raw,
                "size": size_raw,
            },
            "orderType": "IOC",  # Immediate-or-cancel
        }

        body = json.dumps(order_payload)
        headers = self._headers("POST", path, body)

        url = PM_CLOB_BASE + path
        try:
            req = urllib.request.Request(url, data=body.encode(), headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read())
                log.info(f"PM order result: {result}")
                return result
        except urllib.error.HTTPError as e:
            error_body = e.read().decode() if e.fp else str(e)
            log.error(f"PM order error {e.code}: {error_body}")
            return {"success": False, "error": error_body}
        except Exception as e:
            log.error(f"PM order error: {e}")
            return {"success": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Kalshi Order Placement
# ---------------------------------------------------------------------------
KS_BASE = "https://api.elections.kalshi.com/trade-api/v2"


class KalshiTrader:
    """
    Places orders on Kalshi Trade API.
    Auth: RSA signing with private key file.
    """

    def __init__(self, access_key: str, private_key_path: str):
        self.access_key = access_key
        self.private_key = None
        if private_key_path:
            try:
                with open(private_key_path, "rb") as f:
                    self.private_key = serialization.load_pem_private_key(
                        f.read(), password=None
                    )
            except Exception as e:
                log.error(f"Kalshi key load error: {e}")

    def _sign(self, timestamp: str, method: str, path: str) -> str:
        """Generate RSA-SHA256 signature for Kalshi API."""
        if not self.private_key:
            return ""
        message = timestamp + method.upper() + path
        signature = self.private_key.sign(
            message.encode("utf-8"),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _headers(self, method: str, path: str) -> dict:
        """Build authenticated request headers."""
        timestamp = str(int(time.time() * 1000))  # milliseconds
        signature = self._sign(timestamp, method, path)
        return {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.access_key,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
        }

    def get_market(self, ticker: str) -> dict:
        """Get market details."""
        path = f"/markets/{ticker}"
        url = KS_BASE + path
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read()).get("market", {})
        except Exception as e:
            log.error(f"KS get_market error: {e}")
            return {}

    def place_ioc_order(self, ticker: str, side: str, yes_price: int, count: int) -> dict:
        """
        Place an IOC order on Kalshi.
        
        Args:
            ticker: Market ticker (e.g. "KXNBAGAME-26MAR27ATLBOS-BOS")
            side: "yes" or "no"
            yes_price: Price in cents (e.g. 65 = $0.65)
            count: Number of contracts
        
        Returns:
            Order response dict
        """
        path = "/orders"

        order_payload = {
            "ticker": ticker,
            "action": "buy",
            "side": side.lower(),
            "count": count,
            "type": "limit",
            "yes_price": yes_price if side.lower() == "yes" else None,
            "no_price": yes_price if side.lower() == "no" else None,
            "time_in_force": "ioc",
        }
        # Remove None values
        order_payload = {k: v for k, v in order_payload.items() if v is not None}

        body = json.dumps(order_payload)
        headers = self._headers("POST", path)

        url = KS_BASE + path
        try:
            req = urllib.request.Request(url, data=body.encode(), headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read())
                log.info(f"KS order result: {result}")
                return result
        except urllib.error.HTTPError as e:
            error_body = e.read().decode() if e.fp else str(e)
            log.error(f"KS order error {e.code}: {error_body}")
            return {"success": False, "error": error_body}
        except Exception as e:
            log.error(f"KS order error: {e}")
            return {"success": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Arb Executor — places both legs
# ---------------------------------------------------------------------------
class ArbExecutor:
    """
    Executes arb by placing IOC orders on both platforms simultaneously.
    
    Flow:
    1. Place order on platform A (better price)
    2. Place order on platform B
    3. Both are IOC — fill immediately or cancel
    4. If one doesn't fill, the other auto-cancels (IOC)
    """

    def __init__(self, pm_trader: PolymarketTrader, ks_trader: KalshiTrader,
                 max_bet: float = 20.0, min_edge: float = 0.03):
        self.pm = pm_trader
        self.ks = ks_trader
        self.max_bet = max_bet
        self.min_edge = min_edge
        self.executions = []
        self.total_pnl = 0.0

    def execute_arb(self, arb_side: dict, pm_token_id: str, ks_ticker: str) -> dict:
        """
        Execute one side of an arb.
        
        arb_side from calc_arb: {"pm_price":..,"ks_price":..,"cost":..,"profit":..,"is_arb":..}
        """
        if not arb_side.get("is_arb"):
            return {"executed": False, "reason": "no arb"}

        cost = arb_side["cost"]
        if cost >= 1.0 - self.min_edge:
            return {"executed": False, "reason": f"edge too small: {1-cost:.4f}"}

        # Calculate stakes for equal payout
        pm_price = arb_side["pm_price"]
        ks_price = arb_side["ks_price"]
        cost_sum = pm_price + ks_price

        if cost_sum <= 0:
            return {"executed": False, "reason": "invalid prices"}

        bet = min(self.max_bet, cost_sum and self.max_bet)
        pm_stake = bet * ks_price / cost_sum
        ks_stake = bet * pm_price / cost_sum
        guaranteed_payout = bet / cost_sum

        log.info(f"Executing arb: PM ${pm_stake:.2f} + KS ${ks_stake:.2f} → ${guaranteed_payout:.2f}")

        # Place both orders (IOC — immediate or cancel)
        pm_result = self.pm.place_ioc_order(
            token_id=pm_token_id,
            side="BUY",
            price=pm_price,
            size=pm_stake / pm_price if pm_price > 0 else 0,
        )

        ks_price_cents = int(ks_price * 100)
        ks_count = max(1, int(ks_stake / (ks_price / 100)) if ks_price > 0 else 1)
        ks_result = self.ks.place_ioc_order(
            ticker=ks_ticker,
            side="yes",  # or "no" depending on the arb side
            yes_price=ks_price_cents,
            count=ks_count,
        )

        execution = {
            "time": time.strftime("%H:%M:%S"),
            "pm_result": pm_result,
            "ks_result": ks_result,
            "pm_stake": pm_stake,
            "ks_stake": ks_stake,
            "expected_profit": guaranteed_payout - bet,
            "executed": True,
        }
        self.executions.append(execution)
        return execution
