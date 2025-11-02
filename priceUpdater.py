#!/usr/bin/env python3
"""
Fetch Kraken spot trades since a chosen look-back and upsert to Notion.

Env vars required:
- KRAKEN_API_KEY
- KRAKEN_API_SECRET
- NOTION_API_KEY
- NOTION_DB_ID

Optional:
- KRAKEN_SKIP_PAIRS (comma-separated list of pairs to skip, e.g. "USDGUSD,ZEURZUSD")
- TZ (IANA timezone for rendering dates, default UTC)
"""

import os
import sys
import hmac
import time as time_mod
import base64
import hashlib
import urllib.parse
from time import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import httpx
from notion_client import Client as NotionClient

# If your shell script already sources .env, this is optional.
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


# ------------------------------
# User prompt (kept as-is style)
# ------------------------------
def get_days_lookback():
    try:
        days = int(input("⏳ How many days back should we check for trades? (e.g. 7): "))
        return int(time()) - (days * 86400)
    except:
        print("⚠️ Invalid input. Defaulting to 7 days.")
        return int(time()) - (7 * 86400)


# ------------------------------
# Kraken REST private client
# ------------------------------
class KrakenClient:
    def __init__(self, api_key: str, api_secret: str, base_url: str = "https://api.kraken.com"):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url
        self.http = httpx.Client(timeout=30.0)

    def _nonce(self) -> str:
        # Nonce must be increasing; milliseconds as string
        return str(int(time_mod.time() * 1000))

    def _sign(self, url_path: str, data: Dict[str, Any]) -> str:
        """
        Kraken signature:
        - message = (nonce + postdata)
        - sha256(message)
        - HMAC-SHA512 of (urlpath + sha256(message)) with base64-decoded secret
        """
        postdata = urllib.parse.urlencode(data)
        encoded = (data["nonce"] + postdata).encode()
        message = url_path.encode() + hashlib.sha256(encoded).digest()
        mac = hmac.new(base64.b64decode(self.api_secret), message, hashlib.sha512)
        sig_digest = base64.b64encode(mac.digest())
        return sig_digest.decode()

    def private(self, endpoint: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if payload is None:
            payload = {}
        payload["nonce"] = self._nonce()
        url_path = f"/0/private/{endpoint}"
        headers = {
            "API-Key": self.api_key,
            "API-Sign": self._sign(url_path, payload),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        r = self.http.post(self.base_url + url_path, data=payload, headers=headers)
        r.raise_for_status()
        data = r.json()
        if data.get("error"):
            raise RuntimeError(f"Kraken API error: {data['error']}")
        return data["result"]

    # TradesHistory:
    # https://api.kraken.com/0/private/TradesHistory
    # Accepts: type, trades, start, end, ofs
    def trades_history(self, start_ts: int, end_ts: Optional[int] = None) -> List[Dict[str, Any]]:
        trades: List[Dict[str, Any]] = []
        ofs = 0
        while True:
            payload = {"start": start_ts, "ofs": ofs}
            if end_ts:
                payload["end"] = end_ts
            res = self.private("TradesHistory", payload)
            # res example: {'trades': {'TXID': {...}}, 'count': N}
            raw_trades = res.get("trades", {})
            if not raw_trades:
                break
            # Flatten dict -> list
            for txid, t in raw_trades.items():
                t["_txid"] = txid
                trades.append(t)
            count = res.get("count", len(trades))
            # If we've pulled at least 'count' records or no growth, stop
            if len(trades) >= count:
                break
            ofs += len(raw_trades)
        return trades


# ------------------------------
# Notion helpers
# ------------------------------
class NotionLogger:
    def __init__(self, api_key: str, database_id: str):
        self.notion = NotionClient(auth=api_key)
        self.db_id = database_id

    def _find_existing_by_txid(self, txid: str) -> Optional[str]:
        # Filter on the Title property "TXID"
        resp = self.notion.databases.query(
            **{
                "database_id": self.db_id,
                "filter": {
                    "property": "TXID",
                    "title": {"equals": txid},
                },
                "page_size": 1,
            }
        )
        results = resp.get("results", [])
        if results:
            return results[0]["id"]
        return None

    def upsert_trade(self, trade: Dict[str, Any]) -> None:
        """
        trade keys expected (normalized):
        - txid, pair, trade_type (buy/sell), type ("spot"), price, cost, fee, vol, time_iso
        """
        txid = trade["txid"]
        page_id = self._find_existing_by_txid(txid)

        props = {
            "TXID": {"title": [{"text": {"content": txid}}]},
            "Pair": {"rich_text": [{"text": {"content": trade.get("pair", "")}}]},
            "Trade Type": {"select": {"name": trade.get("trade_type", "").lower() or "unknown"}},
            "Type": {"select": {"name": trade.get("type", "spot")}},
            "Price": {"number": float(trade.get("price") or 0)},
            "Cost": {"number": float(trade.get("cost") or 0)},
            "Fee": {"number": float(trade.get("fee") or 0)},
            "Volume": {"number": float(trade.get("vol") or 0)},
            "Time": {"date": {"start": trade.get("time_iso")}},
        }

        if page_id:
            self.notion.pages.update(page_id=page_id, properties=props)
        else:
            self.notion.pages.create(parent={"database_id": self.db_id}, properties=props)


# ------------------------------
# Normalization utilities
# ------------------------------
def normalize_kraken_trade(t: Dict[str, Any], tz_name: str = "UTC") -> Dict[str, Any]:
    """
    Kraken trades look like:
      {
        'ordertxid': 'OXXXXX',
        'pair': 'XXBTZUSD',
        'time': 1699999999.1234,
        'type': 'buy',   # buy/sell
        'ordertype': 'limit',
        'price': '35000.0',
        'cost': '100.00',
        'fee': '0.26',
        'vol': '0.00285714',
        ...
      }
    We'll keep the most relevant fields for the Notion DB.
    """
    txid = t.get("_txid") or t.get("ordertxid") or ""
    pair = t.get("pair") or ""
    trade_type = (t.get("type") or "").lower()  # buy/sell
    # Kraken 'time' is seconds float
    ts = t.get("time")
    if isinstance(ts, (int, float)):
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    else:
        dt = datetime.now(timezone.utc)

    # Convert to ISO string (UTC or local tz)
    tz_env = tz_name or "UTC"
    try:
        # Safe conversion with stdlib only (keep UTC in ISO; tz name is optional)
        time_iso = dt.isoformat()
    except Exception:
        time_iso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "txid": txid,
        "pair": pair,
        "trade_type": trade_type,
        "type": "spot",
        "price": t.get("price"),
        "cost": t.get("cost"),
        "fee": t.get("fee"),
        "vol": t.get("vol"),
        "time_iso": time_iso,
    }


# ------------------------------
# Main flow
# ------------------------------
def main():
    # --- envs ---
    KRAKEN_API_KEY = os.environ.get("KRAKEN_API_KEY", "").strip()
    KRAKEN_API_SECRET = os.environ.get("KRAKEN_API_SECRET", "").strip()
    NOTION_API_KEY = os.environ.get("NOTION_API_KEY", "").strip()
    NOTION_DB_ID = os.environ.get("NOTION_DB_ID", "").strip()
    SKIP_PAIRS = {p.strip() for p in os.environ.get("KRAKEN_SKIP_PAIRS", "").split(",") if p.strip()}
    TZ = os.environ.get("TZ", "UTC")

    # sanity checks
    missing = [k for k, v in {
        "KRAKEN_API_KEY": KRAKEN_API_KEY,
        "KRAKEN_API_SECRET": KRAKEN_API_SECRET,
        "NOTION_API_KEY": NOTION_API_KEY,
        "NOTION_DB_ID": NOTION_DB_ID,
    }.items() if not v]
    if missing:
        print(f"❌ Missing required env vars: {', '.join(missing)}")
        sys.exit(1)

    # look-back
    start_ts = get_days_lookback()
    end_ts = int(time())

    print("🔄 Fetching recent trades from Kraken...")
    kraken = KrakenClient(KRAKEN_API_KEY, KRAKEN_API_SECRET)

    try:
        trades = kraken.trades_history(start_ts=start_ts, end_ts=end_ts)
    except Exception as e:
        print(f"❌ Kraken error: {e}")
        sys.exit(2)

    print(f"📦 Kraken returned {len(trades)} trades")

    if not trades:
        print("ℹ️ No trades found in that window. Done.")
        return

    notion = NotionLogger(NOTION_API_KEY, NOTION_DB_ID)

    added, updated = 0, 0
    for t in trades:
        if t.get("pair") in SKIP_PAIRS:
            continue
        try:
            norm = normalize_kraken_trade(t, tz_name=TZ)
            # Upsert: we don’t know if it will update or create; do a quick check
            existing_id = notion._find_existing_by_txid(norm["txid"])
            notion.upsert_trade(norm)
            if existing_id:
                updated += 1
            else:
                added += 1
        except Exception as e:
            print(f"⚠️ Failed to upsert trade {t.get('_txid')}: {e}")

    print(f"✅ Done. Added: {added}, Updated: {updated}")


if __name__ == "__main__":
    main()

