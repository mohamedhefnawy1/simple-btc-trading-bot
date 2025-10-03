import os
import json
import hmac
import base64
import sqlite3
from pathlib import Path
from contextlib import contextmanager
from hashlib import sha256
from decimal import Decimal, getcontext, ROUND_FLOOR
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# --------------------------------------------------------------------------------------
# Env & config
# --------------------------------------------------------------------------------------
load_dotenv()

def env_bool(name: str, default: bool = False) -> bool:
    """Case-insensitive boolean env reader. Accepts 1/true/yes/on."""
    val = os.getenv(name)
    if val is None:
        val = os.getenv(name.lower())
    if val is None:
        return default
    return str(val).strip().lower() in ("1", "true", "yes", "on")

WEBHOOK_TOKEN    = os.getenv("WEBHOOK_TOKEN", "change_me")
OKX_API_KEY      = os.getenv("OKX_API_KEY")
OKX_SECRET_KEY   = os.getenv("OKX_SECRET_KEY")
OKX_PASSPHRASE   = os.getenv("OKX_PASSPHRASE")
OKX_ENV          = os.getenv("OKX_ENV", "demo").lower()   # "demo" or "live"
OKX_INST_ID      = os.getenv("OKX_INST_ID", "BTC-USDT")
ORDER_SIZE       = os.getenv("ORDER_SIZE", "10")          # USDT notional for buys, and for sells we convert -> base qty
TRADING_ENABLED  = env_bool("TRADING_ENABLED", True)

# NEW: toggle whether 15m trades are allowed at all (still require alignment with 60m)
TRADE_15_TF      = env_bool("TRADE_15_TF", False) or env_bool("trade_15_tf", False)

OKX_BASE_URL = "https://www.okx.com"

getcontext().prec = 28  # safe precision for money math

# --------------------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------------------
def log(msg: str):
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{ts}] {msg}", flush=True)

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
    if step == 0:
        return x
    n = (x / step).to_integral_value(rounding=ROUND_FLOOR)
    return n * step

def as_str(d: Decimal) -> str:
    s = format(d, "f").rstrip("0").rstrip(".")
    return s if s else "0"

def okx_server_timestamp_iso() -> str:
    r = requests.get(f"{OKX_BASE_URL}/api/v5/public/time", timeout=5)
    r.raise_for_status()
    ms = int(r.json()["data"][0]["ts"])
    return (
        datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )

def okx_server_timestamp_ms_str() -> str:
    r = requests.get(f"{OKX_BASE_URL}/api/v5/public/time", timeout=5)
    r.raise_for_status()
    return r.json()["data"][0]["ts"]  # "1758730616703"

# --------------------------------------------------------------------------------------
# SQLite state (confluence per (symbol, tf))
# --------------------------------------------------------------------------------------
DB_PATH = Path(__file__).with_name("bot_state.sqlite3")

def _init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pair_state (
                symbol TEXT NOT NULL,
                tf     TEXT NOT NULL,
                st_12_3 TEXT,
                st_10_1 TEXT,
                last_confluence TEXT NOT NULL DEFAULT 'none',
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                PRIMARY KEY (symbol, tf)
            )
            """
        )
        conn.commit()

@contextmanager
def db():
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
        conn.execute(
            """
            INSERT OR IGNORE INTO pair_state(symbol, tf, st_12_3, st_10_1, last_confluence)
            VALUES(?, ?, NULL, NULL, 'none')
            """,
            (symbol, tf),
        )

        if indicator not in ("st_12_3", "st_10_1"):
            raise ValueError(f"Unknown indicator {indicator}")

        conn.execute(
            f"""
            UPDATE pair_state
            SET {indicator}=?, updated_at=strftime('%s','now')
            WHERE symbol=? AND tf=?
            """,
            (signal, symbol, tf),
        )

        cur = conn.execute(
            """
            SELECT st_12_3, st_10_1, last_confluence
            FROM pair_state WHERE symbol=? AND tf=?
            """,
            (symbol, tf),
        )
        st1, st2, last = cur.fetchone()

        confluence = "none"
        if st1 == "buy" and st2 == "buy":
            confluence = "buy"
        elif st1 == "sell" and st2 == "sell":
            confluence = "sell"

        if confluence == "none" or confluence == last:
            conn.execute("COMMIT")
            return confluence, last, None

        conn.execute(
            """
            UPDATE pair_state
            SET last_confluence=?, updated_at=strftime('%s','now')
            WHERE symbol=? AND tf=?
            """,
            (confluence, symbol, tf),
        )
        conn.execute("COMMIT")
        return confluence, last, confluence
    except Exception:
        conn.execute("ROLLBACK")
        raise

def get_last_confluence(conn, symbol: str, tf: str) -> str:
    row = conn.execute(
        "SELECT last_confluence FROM pair_state WHERE symbol=? AND tf=?",
        (symbol, tf),
    ).fetchone()
    return row[0] if row else "none"

# --------------------------------------------------------------------------------------
# OKX REST (signed)
# --------------------------------------------------------------------------------------
def okx_place_order_raw(inst_id: str, side: str, sz: str, td_mode: str = "cash", ord_type: str = "market"):
    base_url = OKX_BASE_URL
    path = "/api/v5/trade/order"
    url = base_url + path

    ts_iso = okx_server_timestamp_iso()

    body = {
        "instId": inst_id,
        "tdMode": td_mode,   # "cash" for spot
        "side": side,        # "buy" or "sell"
        "ordType": ord_type, # "market"
    }

    # BUY (spot): spend sz USDT (quote) via tgtCcy=quote_ccy
    # SELL (spot): sz is in BASE units
    if side == "buy" and td_mode == "cash":
        body["tgtCcy"] = "quote_ccy"
        body["sz"] = str(sz)
    else:
        body["sz"] = str(sz)

    body_str = json.dumps(body, separators=(",", ":"))
    raw_to_sign = ts_iso + "POST" + path + body_str
    signature = base64.b64encode(
        hmac.new(OKX_SECRET_KEY.encode("utf-8"), raw_to_sign.encode("utf-8"), sha256).digest()
    ).decode()

    headers = {
        "OK-ACCESS-KEY": OKX_API_KEY,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": ts_iso,
        "OK-ACCESS-PASSPHRASE": OKX_PASSPHRASE,
        "Content-Type": "application/json",
    }
    if OKX_ENV == "demo":
        headers["x-simulated-trading"] = "1"

    resp = requests.post(url, headers=headers, data=body_str, timeout=10)
    return resp.json()

def maybe_place_okx_order(direction: str):
    """
    Uses ORDER_SIZE from .env as a USDT notional for BOTH buy and sell on SPOT.
    - BUY: uses tgtCcy='quote_ccy' and sz=ORDER_SIZE (USDT)
    - SELL: converts ORDER_SIZE USDT -> base qty using best bid; floors to lotSz; checks minSz
    """
    quote_usdt = Decimal(str(ORDER_SIZE))
    side = "buy" if direction == "buy" else "sell"

    if side == "buy":
        log(f"Placing BUY {OKX_INST_ID} spending ≈ {as_str(quote_usdt)} USDT (market, tgtCcy=quote_ccy)")
        try:
            res = okx_place_order_raw(OKX_INST_ID, "buy", as_str(quote_usdt), td_mode="cash", ord_type="market")
            log(f"OKX response: {res}")
            return res
        except Exception as e:
            log(f"OKX error (BUY): {e}")
            return {"error": str(e)}

    # SELL path
    try:
        meta   = get_instrument_meta(OKX_INST_ID)
        lotSz  = Decimal(meta["lotSz"])
        minSz  = Decimal(meta["minSz"])
        ticker = get_ticker_price(OKX_INST_ID)
        bid    = Decimal(ticker.get("bidPx") or ticker["last"])

        if bid <= 0:
            raise RuntimeError(f"Bad bid price from ticker: {ticker}")

        base_qty_raw = quote_usdt / bid
        base_qty     = floor_to_step(base_qty_raw, lotSz)

        if base_qty < minSz:
            needed = minSz * bid
            msg = (
                f"Computed sell qty {as_str(base_qty)} < minSz {as_str(minSz)}. "
                f"At bid {as_str(bid)}, need at least ≈ {as_str(needed)} USDT notional or hold more base."
            )
            log(msg)
            return {"code": "client", "msg": msg}

        log(f"Placing SELL {OKX_INST_ID} size={as_str(base_qty)} (≈{as_str(base_qty*bid)} USDT at bid {as_str(bid)})")
        res = okx_place_order_raw(OKX_INST_ID, "sell", as_str(base_qty), td_mode="cash", ord_type="market")
        log(f"OKX response: {res}")
        return res
    except Exception as e:
        log(f"OKX error (SELL): {e}")
        return {"error": str(e)}

# --------------------------------------------------------------------------------------
# Flask app
# --------------------------------------------------------------------------------------
_init_db()
app = Flask(__name__)

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
        rows = conn.execute(
            """
            SELECT symbol, tf, st_12_3, st_10_1, last_confluence, updated_at
            FROM pair_state ORDER BY symbol, tf
            """
        ).fetchall()
    return jsonify(
        [
            {
                "symbol": r[0],
                "tf": r[1],
                "st_12_3": r[2],
                "st_10_1": r[3],
                "last_confluence": r[4],
                "updated_at": r[5],
            }
            for r in rows
        ]
    )

def norm_tf(tf_raw: str) -> str:
    """Normalize timeframe to '60' or '15' if it contains those values."""
    s = str(tf_raw).strip().lower()
    if s in ("60", "1h", "60m", "h1"):
        return "60"
    if s in ("15", "15m", "m15"):
        return "15"
    return s  # fallback (won't be tradable unless matches exactly)

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=False) or {}
    log(f"Incoming: {data}")

    if data.get("token") != WEBHOOK_TOKEN:
        return jsonify({"status": "error", "msg": "invalid token"}), 403

    indicator = data.get("indicator")
    signal    = data.get("signal")
    symbol    = data.get("symbol", "BTCUSDT")
    tf        = norm_tf(data.get("tf", "NA"))

    if indicator not in ("st_12_3", "st_10_1") or signal not in ("buy", "sell"):
        return jsonify({"status": "ignored", "msg": "unknown indicator or signal"}), 200

    with db() as conn:
        confluence, last, action = set_signal_and_maybe_flip(conn, symbol, tf, indicator, signal)
        log(f"For ({symbol}, {tf}), confluence={confluence}, last={last}, action={action}")

        # Decide if this action is allowed (60m always allowed; 15m depends on toggle + 60m agreement)
        if action in ("buy", "sell"):
            if tf == "60":
                # 60m always allowed
                if not TRADING_ENABLED:
                    log("KILLSWITCH: TRADING_ENABLED=false; skipping 60m trade")
                    return jsonify({"status": "ok", "action": "none", "confluence": confluence, "note": "trading disabled"}), 200

                res = maybe_place_okx_order(action)
                return jsonify({"status": "ok", "action": action, "okx": res}), 200

            if tf == "15":
                if not TRADE_15_TF:
                    log("15m trade blocked: trade_15_tf=false")
                    return jsonify({"status": "ok", "action": "none", "confluence": confluence, "note": "15m trading disabled"}), 200

                # Require 60m agreement for 15m entries
                c60 = get_last_confluence(conn, symbol, "60")
                if c60 != action:
                    log(f"15m {action} blocked: 60m last_confluence={c60} (requires agreement)")
                    return jsonify({"status": "ok", "action": "none", "confluence": confluence, "note": "needs 60m agreement"}), 200

                if not TRADING_ENABLED:
                    log("KILLSWITCH: TRADING_ENABLED=false; skipping 15m trade")
                    return jsonify({"status": "ok", "action": "none", "confluence": confluence, "note": "trading disabled"}), 200

                res = maybe_place_okx_order(action)
                return jsonify({"status": "ok", "action": action, "okx": res}), 200

        # No flip or no confluence
        return jsonify({"status": "ok", "action": "none", "confluence": confluence}), 200

if __name__ == "__main__":
    # Local testing
    app.run(port=8000)
