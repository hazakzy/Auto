import os
import json
import hmac
import hashlib
import time
import requests
from flask import Flask, request, jsonify
import threading
import traceback

app = Flask(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
API_KEY          = os.environ.get("BYBIT_API_KEY")
API_SECRET       = os.environ.get("BYBIT_API_SECRET")
BASE_URL_PUBLIC  = "https://api.bybit.com"
BASE_URL_PRIVATE = "https://api-demo.bybit.com"

# ─── Risk settings ────────────────────────────────────────────────────────────
TRADE_USDT = 10
LEVERAGE   = 2
SYMBOLS    = ["BTCUSDT"]
INTERVAL   = "15"
EMA_FAST   = 12
EMA_SLOW   = 21

# ─── State ────────────────────────────────────────────────────────────────────
last_signal = {}
bot_status  = {"running": False, "last_scan": None, "error": None}

# ─── Signature ────────────────────────────────────────────────────────────────
def sign(params):
    timestamp   = str(int(time.time() * 1000))
    recv_window = "5000"
    param_str   = timestamp + API_KEY + recv_window + json.dumps(params)
    sig = hmac.new(API_SECRET.encode(), param_str.encode(), hashlib.sha256).hexdigest()
    return {
        "X-BAPI-API-KEY":     API_KEY,
        "X-BAPI-TIMESTAMP":   timestamp,
        "X-BAPI-SIGN":        sig,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type":       "application/json"
    }

def post(endpoint, params):
    headers = sign(params)
    r = requests.post(f"{BASE_URL_PRIVATE}{endpoint}", headers=headers, json=params, timeout=10)
    return r.json()

def get_signed(endpoint, params={}):
    headers = sign(params)
    r = requests.get(f"{BASE_URL_PRIVATE}{endpoint}", headers=headers, params=params, timeout=10)
    return r.json()

def get_public(endpoint, params={}):
    for attempt in range(3):
        try:
            r = requests.get(f"{BASE_URL_PUBLIC}{endpoint}", params=params, timeout=10)
            return r.json()
        except Exception as e:
            print(f"Public API attempt {attempt+1} failed: {e}")
            time.sleep(2)
    return {}

# ─── EMA ──────────────────────────────────────────────────────────────────────
def calc_ema(prices, period):
    k   = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = price * k + ema * (1 - k)
    return ema

def get_candles(symbol):
    for attempt in range(3):
        try:
            data = get_public("/v5/market/kline", {
                "category": "linear",
                "symbol":   symbol,
                "interval": INTERVAL,
                "limit":    150
            })
            candles = data["result"]["list"]
            candles.reverse()
            closes = [float(c[4]) for c in candles]
            return closes
        except Exception as e:
            print(f"Candle fetch attempt {attempt+1} failed for {symbol}: {e}")
            time.sleep(2)
    return []

def check_signal(symbol):
    closes = get_candles(symbol)
    if len(closes) < EMA_SLOW + 5:
        print(f"{symbol} — not enough candles ({len(closes)})")
        return None

    ema_fast_now  = calc_ema(closes,      EMA_FAST)
    ema_slow_now  = calc_ema(closes,      EMA_SLOW)
    ema_fast_prev = calc_ema(closes[:-1], EMA_FAST)
    ema_slow_prev = calc_ema(closes[:-1], EMA_SLOW)

    print(f"{symbol} — EMA12: {round(ema_fast_now,2)} | EMA21: {round(ema_slow_now,2)}")

    if ema_fast_prev < ema_slow_prev and ema_fast_now > ema_slow_now:
        return "buy"
    elif ema_fast_prev > ema_slow_prev and ema_fast_now < ema_slow_now:
        return "sell"
    return None

# ─── Order helpers ────────────────────────────────────────────────────────────
def get_price(symbol):
    data = get_public("/v5/market/tickers", {"category": "linear", "symbol": symbol})
    return float(data["result"]["list"][0]["lastPrice"])

def get_precision(symbol):
    data = get_public("/v5/market/instruments-info", {"category": "linear", "symbol": symbol})
    qty_step = data["result"]["list"][0]["lotSizeFilter"]["qtyStep"]
    decimals = len(qty_step.rstrip("0").split(".")[-1]) if "." in qty_step else 0
    return decimals

def set_leverage(symbol):
    params = {
        "category":     "linear",
        "symbol":       symbol,
        "buyLeverage":  str(LEVERAGE),
        "sellLeverage": str(LEVERAGE)
    }
    result = post("/v5/position/set-leverage", params)
    print(f"Leverage: {result}")

def close_existing(symbol):
    r = get_signed("/v5/position/list", {"category": "linear", "symbol": symbol})
    positions = r.get("result", {}).get("list", [])
    for pos in positions:
        size = float(pos.get("size", 0))
        if size > 0:
            close_side = "Sell" if pos["side"] == "Buy" else "Buy"
            result = post("/v5/order/create", {
                "category":   "linear",
                "symbol":     symbol,
                "side":       close_side,
                "orderType":  "Market",
                "qty":        str(size),
                "reduceOnly": True
            })
            print(f"Closed position {symbol}: {result}")
    time.sleep(0.5)

def place_order(symbol, signal):
    try:
        set_leverage(symbol)
        close_existing(symbol)

        price     = get_price(symbol)
        precision = get_precision(symbol)
        qty       = round((TRADE_USDT * LEVERAGE) / price, precision)
        side      = "Buy" if signal == "buy" else "Sell"

        result = post("/v5/order/create", {
            "category":    "linear",
            "symbol":      symbol,
            "side":        side,
            "orderType":   "Market",
            "qty":         str(qty),
            "timeInForce": "GTC"
        })
        print(f"Order placed {symbol} {side} qty={qty} @ {price}")
        print(f"Response: {result}")

    except Exception as e:
        print(f"Order error {symbol}: {e}")
        traceback.print_exc()

# ─── Bot loop ─────────────────────────────────────────────────────────────────
def run_bot():
    bot_status["running"] = True
    print("=" * 50)
    print("Bot started — GKC EMA 12/21 — 15M BTCUSDT")
    print("=" * 50)

    while True:
        try:
            print(f"\n--- Scan at {time.strftime('%H:%M:%S')} UTC ---")
            bot_status["last_scan"] = time.strftime('%Y-%m-%d %H:%M:%S UTC')

            for symbol in SYMBOLS:
                signal = check_signal(symbol)
                prev   = last_signal.get(symbol)
                print(f"{symbol} — signal: {signal} | prev: {prev}")

                if signal and signal != prev:
                    print(f">>> {symbol} {signal.upper()} — placing order")
                    place_order(symbol, signal)
                    last_signal[symbol] = signal
                else:
                    print(f"{symbol} — holding, no new signal")

            # Sleep to next 15m candle
            now   = time.time()
            sleep = (15 * 60) - (now % (15 * 60)) + 5
            print(f"Next scan in {round(sleep/60, 1)} mins")
            time.sleep(sleep)

        except Exception as e:
            bot_status["error"] = str(e)
            print(f"Bot loop error: {e}")
            traceback.print_exc()
            time.sleep(30)  # wait 30s and retry

# ─── Status page ──────────────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def health():
    return jsonify({
        "status":      "running",
        "bot":         bot_status,
        "symbols":     SYMBOLS,
        "last_signal": last_signal,
        "interval":    f"{INTERVAL}m",
        "leverage":    LEVERAGE,
        "trade_usdt":  TRADE_USDT,
        "mode":        "flip on signal — no SL no TP"
    })

# ─── Start ────────────────────────────────────────────────────────────────────
bot_thread = threading.Thread(target=run_bot, daemon=True)
bot_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
