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
import json
import hmac
import time as time_mod
import base64
import hashlib
import urllib.parse
from time import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Iterator, Tuple

import httpx
from notion_client import Client as NotionClient

# If your shell script already sources .env, this is optional.
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


# ------------------------------
# General helpers
# ------------------------------
def normalize_pair_key(pair_text: str) -> str:
    return "".join(ch for ch in pair_text.upper() if ch.isalnum())


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

    def asset_pairs(self) -> Dict[str, Any]:
        url_path = f"{self.base_url}/0/public/AssetPairs"
        r = self.http.get(url_path)
        r.raise_for_status()
        data = r.json()
        if data.get("error"):
            raise RuntimeError(f"Kraken API error: {data['error']}")
        return data.get("result", {})

    def ticker(self, pairs: List[str]) -> Dict[str, Any]:
        if not pairs:
            return {}
        url_path = f"{self.base_url}/0/public/Ticker"
        params = {"pair": ",".join(pairs)}
        r = self.http.get(url_path, params=params)
        r.raise_for_status()
        data = r.json()
        if data.get("error"):
            raise RuntimeError(f"Kraken API error: {data['error']}")
        return data.get("result", {})


# ------------------------------
# Notion helpers
# ------------------------------
class NotionLogger:
    def __init__(self, api_key: str, database_id: str):
        self.notion = NotionClient(auth=api_key)
        self.db_id = database_id
        self._db_properties: Dict[str, Dict[str, Any]] = {}
        self._load_db_schema()

    def _load_db_schema(self) -> None:
        try:
            resp = self.notion.databases.retrieve(self.db_id)
        except Exception as exc:
            print(f"⚠️ Unable to load Notion database schema: {exc}")
            self._db_properties = {}
            return
        props = resp.get("properties", {}) if isinstance(resp, dict) else {}
        if isinstance(props, dict):
            self._db_properties = props
        else:
            self._db_properties = {}

    def _property_type(self, name: str) -> Optional[str]:
        prop = self._db_properties.get(name)
        if isinstance(prop, dict):
            return prop.get("type")
        return None

    def _build_equals_filter(self, property_name: str, value: str) -> Optional[Dict[str, Any]]:
        prop_type = self._property_type(property_name)
        if not prop_type:
            return None
        value = value.strip()
        if not value:
            return None
        if prop_type in {"title", "rich_text"}:
            return {"property": property_name, prop_type: {"equals": value}}
        if prop_type == "select":
            return {"property": property_name, "select": {"equals": value}}
        if prop_type == "status":
            return {"property": property_name, "status": {"equals": value}}
        if prop_type == "multi_select":
            return {"property": property_name, "multi_select": {"contains": value}}
        if prop_type == "url":
            return {"property": property_name, "url": {"equals": value}}
        if prop_type == "number":
            try:
                num_value = float(value)
            except ValueError:
                return None
            return {"property": property_name, "number": {"equals": num_value}}
        return None

    def _build_not_empty_filter(self, property_name: str) -> Optional[Dict[str, Any]]:
        prop_type = self._property_type(property_name)
        if not prop_type:
            return None
        if prop_type in {"title", "rich_text", "select", "multi_select", "status", "people", "files", "relation", "date", "url", "email", "phone_number"}:
            return {"property": property_name, prop_type: {"is_not_empty": True}}
        if prop_type == "number":
            return {"property": property_name, "number": {"is_not_empty": True}}
        return None

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

    # ---- price helpers ----
    def _extract_plain_text(self, prop: Optional[Dict[str, Any]]) -> str:
        if not prop:
            return ""
        prop_type = prop.get("type")
        if prop_type in {"title", "rich_text"}:
            items = prop.get(prop_type, [])
            if not isinstance(items, list):
                return ""
            return "".join(item.get("plain_text", "") for item in items)
        if prop_type == "select":
            sel = prop.get("select")
            if isinstance(sel, dict):
                return sel.get("name", "")
        if prop_type == "multi_select":
            sels = prop.get("multi_select", [])
            if isinstance(sels, list):
                return ",".join(sel.get("name", "") for sel in sels if isinstance(sel, dict))
        if prop_type == "url":
            return prop.get("url") or ""
        if prop_type == "number":
            number_val = prop.get("number")
            return "" if number_val is None else str(number_val)
        if prop_type == "formula":
            formula = prop.get("formula", {})
            if isinstance(formula, dict):
                if formula.get("type") == "string":
                    return formula.get("string", "")
                if formula.get("type") == "number":
                    number_val = formula.get("number")
                    return "" if number_val is None else str(number_val)
        return ""

    def _title_property_name(self, properties: Dict[str, Any]) -> Optional[str]:
        for name, prop in properties.items():
            if isinstance(prop, dict) and prop.get("type") == "title":
                return name
        return None

    def iter_price_rows(
        self,
        pair_property: str,
        price_property: str,
        exchange_property: Optional[str] = None,
        exchange_value: Optional[str] = None,
    ) -> Iterator[Tuple[str, str, Dict[str, Any]]]:
        cursor: Optional[str] = None
        base_filters: List[Dict[str, Any]] = []
        if exchange_property and exchange_value:
            exch_filter = self._build_equals_filter(exchange_property, exchange_value)
            if exch_filter:
                base_filters.append(exch_filter)
        pair_not_empty = self._build_not_empty_filter(pair_property)
        if pair_not_empty:
            base_filters.append(pair_not_empty)
        while True:
            query_payload: Dict[str, Any] = {
                "database_id": self.db_id,
                "start_cursor": cursor,
                "page_size": 100,
            }
            if base_filters:
                if len(base_filters) == 1:
                    query_payload["filter"] = base_filters[0]
                else:
                    query_payload["filter"] = {"and": base_filters}
            resp = self.notion.databases.query(
                **query_payload
            )
            results = resp.get("results", [])
            for page in results:
                properties: Dict[str, Any] = page.get("properties", {})
                if exchange_property and exchange_value:
                    exch_prop = properties.get(exchange_property)
                    exch_val = self._extract_plain_text(exch_prop)
                    if exch_val.lower() != exchange_value.lower():
                        continue
                pair_prop = properties.get(pair_property)
                pair_value = self._extract_plain_text(pair_prop)
                if not pair_value:
                    title_name = self._title_property_name(properties)
                    if title_name:
                        pair_value = self._extract_plain_text(properties.get(title_name))
                if not pair_value:
                    continue
                if price_property not in properties:
                    continue
                yield page.get("id"), pair_value, properties
            if not resp.get("has_more"):
                break
            cursor = resp.get("next_cursor")

    def update_price(self, page_id: str, property_name: str, price: float) -> None:
        self.notion.pages.update(
            page_id=page_id,
            properties={property_name: {"number": price}},
        )


# ------------------------------
# Known pair helpers
# ------------------------------
DEFAULT_KNOWN_PAIRS = {
    "BTC": "XBTUSD",
    "BTCUSD": "XBTUSD",
    "XBT": "XBTUSD",
    "ETH": "ETHUSD",
    "ETHUSD": "ETHUSD",
    "ADA": "ADAUSD",
    "ADAUSD": "ADAUSD",
    "DOGE": "XDGUSD",
    "DOGEUSD": "XDGUSD",
    "XDG": "XDGUSD",
    "DOT": "DOTUSD",
    "DOTUSD": "DOTUSD",
    "ATOM": "ATOMUSD",
    "ATOMUSD": "ATOMUSD",
    "SOL": "SOLUSD",
    "SOLUSD": "SOLUSD",
    "LINK": "LINKUSD",
    "LINKUSD": "LINKUSD",
    "MATIC": "MATICUSD",
    "MATICUSD": "MATICUSD",
    "POL": "MATICUSD",
    "LTC": "LTCUSD",
    "LTCUSD": "LTCUSD",
    "BCH": "BCHUSD",
    "BCHUSD": "BCHUSD",
    "AVAX": "AVAXUSD",
    "AVAXUSD": "AVAXUSD",
    "EGLD": "EGLDUSD",
    "EGLDUSD": "EGLDUSD",
}


def load_known_pairs(path_hint: Optional[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for key, value in DEFAULT_KNOWN_PAIRS.items():
        normalized_key = normalize_pair_key(key)
        if not normalized_key:
            continue
        mapping[normalized_key] = value

    candidate_paths: List[str] = []
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if path_hint:
        candidate_paths.append(path_hint)
        if not os.path.isabs(path_hint):
            candidate_paths.append(os.path.join(script_dir, path_hint))
    else:
        candidate_paths.append(os.path.join(script_dir, "known_pairs.json"))

    for candidate in candidate_paths:
        if not candidate:
            continue
        if not os.path.isfile(candidate):
            continue
        try:
            with open(candidate, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception as exc:
            print(f"⚠️ Failed to load known pairs from {candidate}: {exc}")
            continue
        if isinstance(data, dict):
            for key, value in data.items():
                normalized_key = normalize_pair_key(str(key))
                if not normalized_key:
                    continue
                mapping[normalized_key] = str(value)
    return mapping


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
    SKIP_PAIRS = {p.strip().upper() for p in os.environ.get("KRAKEN_SKIP_PAIRS", "").split(",") if p.strip()}
    TZ = os.environ.get("TZ", "UTC")
    NOTION_PAIR_PROPERTY = os.environ.get("NOTION_PAIR_PROPERTY", "Pair")
    NOTION_PRICE_PROPERTY = os.environ.get("NOTION_PRICE_PROPERTY", "Curr Price")
    NOTION_EXCHANGE_PROPERTY = os.environ.get("NOTION_EXCHANGE_PROPERTY", "") or None
    NOTION_EXCHANGE_VALUE = os.environ.get("NOTION_EXCHANGE_VALUE", "") or None

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

    notion = NotionLogger(NOTION_API_KEY, NOTION_DB_ID)

    added, updated = 0, 0
    if not trades:
        print("ℹ️ No trades found in that window.")
    else:
        for t in trades:
            pair_name = (t.get("pair") or "").upper()
            if pair_name in SKIP_PAIRS:
                continue
            try:
                norm = normalize_kraken_trade(t, tz_name=TZ)
                existing_id = notion._find_existing_by_txid(norm["txid"])
                notion.upsert_trade(norm)
                if existing_id:
                    updated += 1
                else:
                    added += 1
            except Exception as e:
                print(f"⚠️ Failed to upsert trade {t.get('_txid')}: {e}")

    if trades:
        print(f"✅ Trade sync complete. Added: {added}, Updated: {updated}")

    # ---- Price refresh ----
    try:
        asset_pairs = kraken.asset_pairs()
    except Exception as e:
        print(f"⚠️ Unable to fetch Kraken asset pairs: {e}")
        return

    pair_lookup: Dict[str, str] = {}
    alt_lookup: Dict[str, Dict[str, Any]] = {}
    for canonical_name, meta in asset_pairs.items():
        if not isinstance(meta, dict):
            continue
        canonical_upper = canonical_name.upper()
        pair_lookup[canonical_upper] = canonical_name
        alt_lookup[canonical_name] = meta

        altname = (meta.get("altname") or "").upper()
        if altname:
            pair_lookup[altname] = canonical_name
        wsname = (meta.get("wsname") or "").upper().replace("/", "")
        if wsname:
            pair_lookup[wsname] = canonical_name
        pairname = (meta.get("name") or "").upper()
        if pairname:
            pair_lookup[pairname] = canonical_name
        base = (meta.get("base") or "").upper().lstrip("X")
        quote = (meta.get("quote") or "").upper().lstrip("Z")
        if base and quote:
            pair_lookup[f"{base}{quote}"] = canonical_name

    raw_known_pairs = load_known_pairs(os.environ.get("KNOWN_PAIRS_FILE"))
    targets: List[Tuple[str, str, str, Dict[str, Any]]] = []
    for page_id, pair_text, props in notion.iter_price_rows(
        pair_property=NOTION_PAIR_PROPERTY,
        price_property=NOTION_PRICE_PROPERTY,
        exchange_property=NOTION_EXCHANGE_PROPERTY,
        exchange_value=NOTION_EXCHANGE_VALUE,
    ):
        normalized_key = normalize_pair_key(pair_text)
        canonical_pair = pair_lookup.get(normalized_key)
        if not canonical_pair and normalized_key in raw_known_pairs:
            mapped_value = raw_known_pairs[normalized_key]
            canonical_pair = pair_lookup.get(normalize_pair_key(mapped_value))
            if not canonical_pair:
                canonical_pair = pair_lookup.get(mapped_value.upper())
            if not canonical_pair and mapped_value.upper() in asset_pairs:
                canonical_pair = mapped_value.upper()
        if not canonical_pair:
            mapped_value = raw_known_pairs.get(normalized_key)
            similar_pairs: List[str] = []
            for canonical, meta in asset_pairs.items():
                canonical_upper = canonical.upper()
                if normalized_key in canonical_upper:
                    similar_pairs.append(canonical)
                    continue
                if isinstance(meta, dict):
                    altname = (meta.get("altname") or "").upper()
                    if altname and normalized_key in altname:
                        similar_pairs.append(canonical)
                        continue
                    wsname = (meta.get("wsname") or "").upper().replace("/", "")
                    if wsname and normalized_key in wsname:
                        similar_pairs.append(canonical)
                        continue
            similar_display = ", ".join(sorted(set(similar_pairs))) or "<none>"
            mapped_display = f", mapped='{mapped_value}'" if mapped_value else ""
            print(
                "⚠️ Skipping Notion page "
                f"{page_id}: unknown pair '{pair_text}' (normalized='{normalized_key}'{mapped_display}). "
                f"Closest Kraken matches: {similar_display}"
            )
            continue
        if SKIP_PAIRS and (
            normalized_key in SKIP_PAIRS
            or canonical_pair.upper() in SKIP_PAIRS
            or (alt_lookup.get(canonical_pair, {}).get("altname", "").upper() in SKIP_PAIRS)
        ):
            continue
        targets.append((page_id, canonical_pair, pair_text, props))

    if not targets:
        print("ℹ️ No Notion rows eligible for price update.")
        return

    unique_pairs = sorted({t[1] for t in targets})
    try:
        ticker_info = kraken.ticker(unique_pairs)
    except Exception as e:
        print(f"❌ Failed to fetch Kraken ticker data: {e}")
        return

    updated_prices = 0
    for page_id, canonical_pair, display_pair, props in targets:
        meta = alt_lookup.get(canonical_pair, {})
        ticker_data = ticker_info.get(canonical_pair)
        if not ticker_data and meta:
            alt_key = meta.get("altname")
            if alt_key:
                ticker_data = ticker_info.get(alt_key)
        if not ticker_data:
            print(f"⚠️ No ticker data returned for pair '{display_pair}' ({canonical_pair})")
            continue
        close_info = ticker_data.get("c") or []
        if not close_info:
            print(f"⚠️ Missing close price for pair '{display_pair}' ({canonical_pair})")
            continue
        try:
            latest_price = float(close_info[0])
        except (ValueError, TypeError):
            print(f"⚠️ Invalid price data for pair '{display_pair}' ({canonical_pair})")
            continue

        current_prop = props.get(NOTION_PRICE_PROPERTY, {})
        current_value = current_prop.get("number") if isinstance(current_prop, dict) else None
        if current_value is not None and abs(current_value - latest_price) < 1e-9:
            continue
        try:
            notion.update_price(page_id, NOTION_PRICE_PROPERTY, latest_price)
            updated_prices += 1
            print(f"💰 Updated {display_pair} price to {latest_price}")
        except Exception as e:
            print(f"⚠️ Failed to update price for page {page_id}: {e}")

    if updated_prices:
        print(f"✅ Price refresh complete. Updated {updated_prices} rows.")
    else:
        print("ℹ️ Prices already up to date.")


if __name__ == "__main__":
    main()

