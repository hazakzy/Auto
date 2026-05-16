import os
import json
import hmac
import hashlib
import time
import requests
from flask import Flask, request, jsonify
import threading

app = Flask(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
API_KEY        = os.environ.get("BYBIT_API_KEY")
API_SECRET     = os.environ.get("BYBIT_API_SECRET")
BASE_URL       = "https://api-demo.bybit.com"

# ─── Risk settings ────────────────────────────────────────────────────────────
TRADE_USDT = 10
LEVERAGE   = 2
SYMBOLS    = ["BTCUSDT"]
INTERVAL   = "15"
EMA_FAST   = 12
EMA_SLOW   = 21

# ─── State tracking ───────────────────────────────────────────────────────────
last_signal = {}

# ─── Bybit signature ──────────────────────────────────────────────────────────
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
    r = requests.post(f"{BASE_URL}{endpoint}", headers=headers, json=params)
    return r.json()

def get_signed(endpoint, params={}):
    headers = sign(params)
    r = requests.get(f"{BASE_URL}{endpoint}", headers=headers, params=params)
    return r.json()

def get_public(endpoint, params={}):
    r = requests.get(f"{BASE_URL}{endpoint}", params=params)
    return r.json()

# ─── EMA calculation ──────────────────────────────────────────────────────────
def calc_ema(prices, period):
    k = 2 / (period + 1)
    ema = prices[0]
    for price in prices[1:]:
        ema = price * k + ema * (1 - k)
    return ema

def get_candles(symbol, interval, limit=100):
    r = get_public("/v5/market/kline", {
        "category": "linear",
        "symbol":   symbol,
        "interval": interval,
        "limit":    limit
    })
    candles = r["result"]["list"]
    candles.reverse()
    closes = [float(c[4]) for c in candles]
    return closes

def check_signal(symbol):
    try:
        closes = get_candles(symbol, INTERVAL, limit=100)
        if len(closes) < EMA_SLOW + 2:
            return None

        ema_fast_now  = calc_ema(closes,      EMA_FAST)
        ema_slow_now  = calc_ema(closes,      EMA_SLOW)
        ema_fast_prev = calc_ema(closes[:-1], EMA_FAST)
        ema_slow_prev = calc_ema(closes[:-1], EMA_SLOW)

        buy_signal  = ema_fast_prev < ema_slow_prev and ema_fast_now > ema_slow_now
        sell_signal = ema_fast_prev > ema_slow_prev and ema_fast_now < ema_slow_now

        if buy_signal:
            return "buy"
        elif sell_signal:
            return "sell"
        return None

    except Exception as e:
        print(f"Error checking signal for {symbol}: {e}")
        return None

# ─── Helpers ──────────────────────────────────────────────────────────────────
def get_price(symbol):
    r = get_public("/v5/market/tickers", {"category": "linear", "symbol": symbol})
    return float(r["result"]["list"][0]["lastPrice"])

def get_precision(symbol):
    r = get_public("/v5/market/instruments-info", {"category": "linear", "symbol": symbol})
    qty_step = r["result"]["list"][0]["lotSizeFilter"]["qtyStep"]
    decimals = len(qty_step.rstrip("0").split(".")[-1]) if "." in qty_step else 0
    return decimals

def set_leverage(symbol, leverage):
    params = {
        "category":     "linear",
        "symbol":       symbol,
        "buyLeverage":  str(leverage),
        "sellLeverage": str(leverage)
    }
    result = post("/v5/position/set-leverage", params)
    print(f"Leverage set: {result}")

def close_existing(symbol):
    params = {"category": "linear", "symbol": symbol}
    r = get_signed("/v5/position/list", params)
    positions = r.get("result", {}).get("list", [])
    for pos in positions:
        size = float(pos.get("size", 0))
        if size > 0:
            side = pos["side"]
            close_side = "Sell" if side == "Buy" else "Buy"
            close_params = {
                "category":   "linear",
                "symbol":     symbol,
                "side":       close_side,
                "orderType":  "Market",
                "qty":        str(size),
                "reduceOnly": True
            }
            result = post("/v5/order/create", close_params)
            print(f"Closed {symbol} {side} position size={size}: {result}")
    time.sleep(0.5)

def place_order(symbol, signal):
    try:
        set_leverage(symbol, LEVERAGE)
        close_existing(symbol)   # close any open trade first

        price     = get_price(symbol)
        precision = get_precision(symbol)
        qty       = round((TRADE_USDT * LEVERAGE) / price, precision)

        bybit_side = "Buy" if signal == "buy" else "Sell"

        # Entry only — no SL, no TP
        params = {
            "category":    "linear",
            "symbol":      symbol,
            "side":        bybit_side,
            "orderType":   "Market",
            "qty":         str(qty),
            "timeInForce": "GTC"
        }

        result = post("/v5/order/create", params)
        print(f"Order placed {symbol} {bybit_side} qty={qty} price={price}")
        print(f"Bybit response: {result}")
        return result

    except Exception as e:
        print(f"Error placing order for {symbol}: {e}")
        return None

# ─── Main loop ────────────────────────────────────────────────────────────────
def run_bot():
    print("Bot started — scanning every 15 minutes")
    while True:
        for symbol in SYMBOLS:
            signal = check_signal(symbol)
            prev   = last_signal.get(symbol)

            if signal and signal != prev:
                print(f"{symbol} — {signal.upper()} signal detected, flipping position")
                place_order(symbol, signal)
                last_signal[symbol] = signal
            else:
                print(f"{symbol} — no new signal (last: {prev})")

        # Sleep until next 15m candle close
        now     = time.time()
        minutes = now % (15 * 60)
        sleep   = (15 * 60) - minutes + 5
        print(f"Sleeping {round(sleep/60, 1)} mins until next candle...")
        time.sleep(sleep)

# ─── Flask status page ────────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def health():
    status = {
        "status":      "running",
        "symbols":     SYMBOLS,
        "last_signal": last_signal,
        "interval":    f"{INTERVAL}m",
        "leverage":    LEVERAGE,
        "trade_usdt":  TRADE_USDT,
        "mode":        "flip on signal — no SL, no TP"
    }
    return jsonify(status)

# ─── Start ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
