import os
import json
import time
import hmac
import base64
import requests
from hashlib import sha256
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from datetime import datetime, timezone
from okx import OkxRestClient

# --- Load environment ---
load_dotenv()
WEBHOOK_TOKEN   = os.getenv("WEBHOOK_TOKEN", "change_me")
OKX_API_KEY     = os.getenv("OKX_API_KEY")
OKX_SECRET_KEY  = os.getenv("OKX_SECRET_KEY")
OKX_PASSPHRASE  = os.getenv("OKX_PASSPHRASE")
OKX_ENV         = os.getenv("OKX_ENV", "demo").lower()  # "demo" or "live"
OKX_INST_ID     = os.getenv("OKX_INST_ID", "BTC-USDT")
ORDER_SIZE      = os.getenv("ORDER_SIZE", "0.001")


#--- Helper to convert ORDER_SIZE from USDT to BTC ---
from decimal import Decimal, getcontext
getcontext().prec = 28  # safe precision for money math

OKX_BASE_URL = "https://www.okx.com"

def okx_get_json(path: str, params: dict | None = None) -> dict:
    r = requests.get(OKX_BASE_URL + path, params=params or {}, timeout=10)
    r.raise_for_status()
    return r.json()

def get_ticker_price(inst_id: str) -> dict:
    # returns dict with last, bidPx, askPx (strings)
    data = okx_get_json("/api/v5/market/ticker", {"instId": inst_id})["data"][0]
    return data

def get_instrument_meta(inst_id: str) -> dict:
    # returns dict with lotSz, minSz, tickSz (strings)
    data = okx_get_json("/api/v5/public/instruments", {"instType": "SPOT", "instId": inst_id})["data"][0]
    return data

def floor_to_step(x: Decimal, step: Decimal) -> Decimal:
    # floor x to the nearest multiple of step
    if step == 0:
        return x
    n = (x / step).to_integral_value(rounding="ROUND_FLOOR")
    return n * step

def as_str(d: Decimal) -> str:
    # OKX expects strings without scientific notation
    s = format(d, 'f').rstrip('0').rstrip('.')
    return s if s else "0"


# --- persistent state (SQLite) ---
import sqlite3
from contextlib import contextmanager
from pathlib import Path

TRADING_ENABLED = os.getenv("TRADING_ENABLED", "true").lower() in ("1","true","yes","on")
DB_PATH = Path(__file__).with_name("bot_state.sqlite3")

def _init_db():
    # one-time init + WAL for better concurrency
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS pair_state (
            symbol TEXT NOT NULL,
            tf     TEXT NOT NULL,
            st_12_3 TEXT,
            st_10_1 TEXT,
            last_confluence TEXT NOT NULL DEFAULT 'none',
            updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
            PRIMARY KEY (symbol, tf)
        )
        """)
        conn.commit()

@contextmanager
def db():
    # isolation_level=None = autocommit off; we’ll BEGIN/COMMIT manually
    conn = sqlite3.connect(DB_PATH, timeout=5, isolation_level=None)
    try:
        yield conn
    finally:
        conn.close()

def set_signal_and_maybe_flip(conn, symbol: str, tf: str, indicator: str, signal: str):
    """
    Atomically update one indicator, recompute confluence, and return:
      (confluence_now, last_confluence_before, action)
    where action ∈ {'buy','sell', None}. Only returns 'buy'/'sell' on a *flip*.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        # ensure row
        conn.execute("""
          INSERT OR IGNORE INTO pair_state(symbol, tf, st_12_3, st_10_1, last_confluence)
          VALUES(?, ?, NULL, NULL, 'none')
        """, (symbol, tf))

        if indicator not in ("st_12_3", "st_10_1"):
            raise ValueError(f"Unknown indicator {indicator}")

        # write incoming indicator
        conn.execute(f"""
          UPDATE pair_state
          SET {indicator}=?, updated_at=strftime('%s','now')
          WHERE symbol=? AND tf=?
        """, (signal, symbol, tf))

        # read current state
        cur = conn.execute("""
          SELECT st_12_3, st_10_1, last_confluence
          FROM pair_state WHERE symbol=? AND tf=?
        """, (symbol, tf))
        st1, st2, last = cur.fetchone()

        # compute confluence now
        confluence = "none"
        if st1 == "buy" and st2 == "buy":
            confluence = "buy"
        elif st1 == "sell" and st2 == "sell":
            confluence = "sell"

        # if no confluence or no flip → do nothing
        if confluence == "none" or confluence == last:
            conn.execute("COMMIT")
            return confluence, last, None

        # flip! persist new last_confluence
        conn.execute("""
          UPDATE pair_state
          SET last_confluence=?, updated_at=strftime('%s','now')
          WHERE symbol=? AND tf=?
        """, (confluence, symbol, tf))
        conn.execute("COMMIT")
        return confluence, last, confluence
    except Exception:
        conn.execute("ROLLBACK")
        raise



from datetime import datetime, timezone

from datetime import datetime, timezone

def okx_server_timestamp_iso() -> str:
    r = requests.get("https://www.okx.com/api/v5/public/time", timeout=5)
    r.raise_for_status()
    ms = int(r.json()["data"][0]["ts"])
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)\
                   .isoformat(timespec="milliseconds")\
                   .replace("+00:00", "Z")

# --- Helpers for OKX REST ---
def okx_server_timestamp_ms_str() -> str:
    r = requests.get("https://www.okx.com/api/v5/public/time", timeout=5)
    r.raise_for_status()
    return r.json()["data"][0]["ts"]  # e.g. "1758730616703"


def _okx_sign(ts_iso: str, method: str, path: str, body: dict | None, secret: str) -> str:
    raw = ts_iso + method.upper() + path + (json.dumps(body, separators=(",", ":")) if body else "")
    digest = hmac.new(secret.encode("utf-8"), raw.encode("utf-8"), sha256).digest()
    return base64.b64encode(digest).decode()


def okx_place_order_raw(inst_id: str, side: str, sz: str, td_mode: str = "cash", ord_type: str = "market"):
    base_url = "https://www.okx.com"
    path = "/api/v5/trade/order"
    url = base_url + path

    ts = okx_server_timestamp_iso()  # or your ms→ISO helper
    ts = okx_server_timestamp_iso()  # your working ISO timestamp
    body = {
        "instId": inst_id,
        "tdMode": td_mode,      # "cash" for spot
        "side": side,           # "buy" or "sell"
        "ordType": ord_type,    # "market"
        "sz": str(sz),
        # For spot buys in base units, you may add: "tgtCcy": "base_ccy"
        "tdMode": td_mode,
        "side": side,
        "ordType": ord_type,
    }

    # IMPORTANT: sign the *exact* string we will send
    if side == "buy" and td_mode == "cash":
        # spend sz USDT
        body["tgtCcy"] = "quote_ccy"
        body["sz"] = str(sz)
    else:
        # sell/base or anything else: sz is base size
        body["sz"] = str(sz)

    body_str = json.dumps(body, separators=(",", ":"))
    raw_to_sign = ts + "POST" + path + body_str
    sign = base64.b64encode(hmac.new(OKX_SECRET_KEY.encode("utf-8"),
                                     raw_to_sign.encode("utf-8"),
                                     sha256).digest()).decode()
    raw = ts + "POST" + path + body_str
    sign = base64.b64encode(hmac.new(OKX_SECRET_KEY.encode("utf-8"), raw.encode("utf-8"), sha256).digest()).decode()

    headers = {
        "OK-ACCESS-KEY": OKX_API_KEY,
        "OK-ACCESS-SIGN": sign,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": OKX_PASSPHRASE,
        "Content-Type": "application/json",
        # "x-simulated-trading": "1"  # keep while testing
    }
    if OKX_ENV == "demo":
        headers["x-simulated-trading"] = "1"

    # Send the exact bytes we signed
    # (do NOT use json=body; that changes whitespace and breaks signature)
    resp = requests.post(url, headers=headers, data=body_str, timeout=10)
    return resp.json()
    return requests.post(url, headers=headers, data=body_str, timeout=10).json()


# --- OKX client init (optional) ---
client = OkxRestClient(
    OKX_API_KEY,
    OKX_SECRET_KEY,
    OKX_PASSPHRASE
)
# print("Balance check:", client.account.get_balance())

_init_db()

# --- Flask app ---
app = Flask(__name__)
state = {}

def log(msg: str):
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{ts}] {msg}", flush=True)

def maybe_place_okx_order(direction: str):
    """
    Uses ORDER_SIZE from .env as a USDT notional for BOTH buy and sell on SPOT.
    - BUY: uses tgtCcy='quote_ccy' and sz=ORDER_SIZE (USDT)
    - SELL: converts ORDER_SIZE USDT -> base qty using best bid price; floors to lotSz; checks minSz
    """
    quote_usdt = Decimal(str(ORDER_SIZE))
    side = "buy" if direction == "buy" else "sell"
    log(f"Placing {side.upper()} {OKX_INST_ID} size={ORDER_SIZE} via raw REST")

    try:
        res = okx_place_order_raw(
            OKX_INST_ID,
            side,
            str(ORDER_SIZE),
            td_mode="cash",
            ord_type="market"
        )
        log(f"OKX raw response: {res}")
        if side == "buy":
            log(f"Placing BUY {OKX_INST_ID} spending ≈ {quote_usdt} USDT (market, tgtCcy=quote_ccy)")
            # pass sz as USDT; okx_place_order_raw should set tgtCcy='quote_ccy' for buys
            res = okx_place_order_raw(OKX_INST_ID, "buy", as_str(quote_usdt), td_mode="cash", ord_type="market")
            log(f"OKX response: {res}")
            return res

        # SELL path: convert quote -> base
        meta = get_instrument_meta(OKX_INST_ID)
        lotSz = Decimal(meta["lotSz"])    # e.g., 0.0001
        minSz = Decimal(meta["minSz"])    # e.g., 0.00001

        ticker = get_ticker_price(OKX_INST_ID)
        # Use bid price for conservative conversion
        bid = Decimal(ticker.get("bidPx") or ticker["last"])

        if bid <= 0:
            raise RuntimeError(f"Bad bid price from ticker: {ticker}")

        base_qty_raw = quote_usdt / bid           # BTC amount to sell for ~ORDER_SIZE USDT
        base_qty     = floor_to_step(base_qty_raw, lotSz)

        if base_qty < minSz:
            # not enough base to reach minSz; tell user exactly what’s needed
            needed = minSz * bid
            msg = (f"Computed sell qty {base_qty} < minSz {minSz}. "
                   f"At bid {bid}, need at least ≈ {as_str(needed)} USDT notional "
                   f"or hold more base to sell.")
            log(msg)
            return {"code": "client", "msg": msg}

        log(f"Placing SELL {OKX_INST_ID} size={as_str(base_qty)} (≈{as_str(base_qty*bid)} USDT at bid {bid})")
        res = okx_place_order_raw(OKX_INST_ID, "sell", as_str(base_qty), td_mode="cash", ord_type="market")
        log(f"OKX response: {res}")
        return res

    except Exception as e:
        log(f"OKX error: {e}")
        return {"error": str(e)}
    

@app.route("/health", methods=["GET"])
def health():
    try:
        ts = okx_server_timestamp_ms_str()
        return {"ok": True, "okx_server_ts_ms": ts}, 200
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500


@app.route("/state", methods=["GET"])
def state_dump():
    with db() as conn:
        rows = conn.execute("""
          SELECT symbol, tf, st_12_3, st_10_1, last_confluence, updated_at
          FROM pair_state ORDER BY symbol, tf
        """).fetchall()
    return jsonify([
        {"symbol": r[0], "tf": r[1], "st_12_3": r[2], "st_10_1": r[3], "last_confluence": r[4], "updated_at": r[5]}
        for r in rows
    ])

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=False) or {}
    log(f"Incoming: {data}")

    if data.get("token") != WEBHOOK_TOKEN:
        return jsonify({"status": "error", "msg": "invalid token"}), 403

    indicator = data.get("indicator")
    signal    = data.get("signal")
    symbol    = data.get("symbol", "BTCUSDT")
    tf        = data.get("tf", "NA")

    if indicator not in ("st_12_3", "st_10_1") or signal not in ("buy", "sell"):
        return jsonify({"status": "ignored", "msg": "unknown indicator or signal"}), 200

    with db() as conn:
        confluence, last, action = set_signal_and_maybe_flip(conn, symbol, tf, indicator, signal)
        log(f"For ({symbol}, {tf}), confluence={confluence}, last={last}, action={action}")

        if action in ("buy", "sell"):
            if not TRADING_ENABLED:
                log(f"KILLSWITCH: TRADING_ENABLED=false; would have placed {action.upper()} — skipped")
                return jsonify({"status": "ok", "action": "none", "confluence": confluence, "note": "trading disabled"}), 200

            res = maybe_place_okx_order(action)
            return jsonify({"status": "ok", "action": action, "okx": res}), 200

        return jsonify({"status": "ok", "action": "none", "confluence": confluence}), 200


if __name__ == "__main__":
    app.run(port=8000)