import os
import json
import hmac
import hashlib
import time
import requests
from flask import Flask, jsonify
import threading
import traceback

app = Flask(__name__)

API_KEY          = os.environ.get("BYBIT_API_KEY")
API_SECRET       = os.environ.get("BYBIT_API_SECRET")
BASE_URL_PUBLIC  = "https://api.bybit.com"
BASE_URL_PRIVATE = "https://api-demo.bybit.com"

TRADE_USDT = 10
LEVERAGE   = 2
SYMBOLS    = ["BTCUSDT"]
INTERVAL   = "15"
EMA_FAST   = 12
EMA_SLOW   = 21

last_signal = {}
bot_status  = {"last_scan": "never", "last_signal": {}, "error": None}

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

def calc_ema(prices, period):
    k   = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = p * k + ema * (1 - k)
    return ema

def get_candles(symbol):
    try:
        r = requests.get(f"{BASE_URL_PUBLIC}/v5/market/kline", params={
            "category": "linear",
            "symbol":   symbol,
            "interval": INTERVAL,
            "limit":    150
        }, timeout=10)
        candles = r.json()["result"]["list"]
        candles.reverse()
        return [float(c[4]) for c in candles]
    except Exception as e:
        print(f"Candle error: {e}")
        return []

def get_price(symbol):
    r = requests.get(f"{BASE_URL_PUBLIC}/v5/market/tickers", params={
        "category": "linear", "symbol": symbol
    }, timeout=10)
    return float(r.json()["result"]["list"][0]["lastPrice"])

def get_qty_precision(symbol):
    r = requests.get(f"{BASE_URL_PUBLIC}/v5/market/instruments-info", params={
        "category": "linear", "symbol": symbol
    }, timeout=10)
    step = r.json()["result"]["list"][0]["lotSizeFilter"]["qtyStep"]
    return len(step.rstrip("0").split(".")[-1]) if "." in step else 0

def set_leverage(symbol):
    headers = sign({"category": "linear", "symbol": symbol,
                    "buyLeverage": str(LEVERAGE), "sellLeverage": str(LEVERAGE)})
    r = requests.post(f"{BASE_URL_PRIVATE}/v5/position/set-leverage",
        headers=headers,
        json={"category": "linear", "symbol": symbol,
              "buyLeverage": str(LEVERAGE), "sellLeverage": str(LEVERAGE)},
        timeout=10)
    print(f"Leverage: {r.json()}")

def close_position(symbol):
    params = {"category": "linear", "symbol": symbol}
    headers = sign(params)
    r = requests.get(f"{BASE_URL_PRIVATE}/v5/position/list",
        headers=headers, params=params, timeout=10)
    for pos in r.json().get("result", {}).get("list", []):
        size = float(pos.get("size", 0))
        if size > 0:
            close_side = "Sell" if pos["side"] == "Buy" else "Buy"
            body = {"category": "linear", "symbol": symbol,
                    "side": close_side, "orderType": "Market",
                    "qty": str(size), "reduceOnly": True}
            headers2 = sign(body)
            r2 = requests.post(f"{BASE_URL_PRIVATE}/v5/order/create",
                headers=headers2, json=body, timeout=10)
            print(f"Closed: {r2.json()}")
    time.sleep(1)

def place_order(symbol, signal):
    try:
        set_leverage(symbol)
        close_position(symbol)
        price     = get_price(symbol)
        precision = get_qty_precision(symbol)
        qty       = round((TRADE_USDT * LEVERAGE) / price, precision)
        side      = "Buy" if signal == "buy" else "Sell"
        body      = {
            "category":    "linear",
            "symbol":      symbol,
            "side":        side,
            "orderType":   "Market",
            "qty":         str(qty),
            "timeInForce": "GTC"
        }
        headers = sign(body)
        r = requests.post(f"{BASE_URL_PRIVATE}/v5/order/create",
            headers=headers, json=body, timeout=10)
        result = r.json()
        print(f"Order {symbol} {side} qty={qty} @ {price}: {result}")
        return result
    except Exception as e:
        print(f"Order error: {e}")
        traceback.print_exc()
        return {"error": str(e)}

def check_signal(symbol):
    closes = get_candles(symbol)
    if len(closes) < EMA_SLOW + 5:
        return None
    fast_now  = calc_ema(closes,      EMA_FAST)
    slow_now  = calc_ema(closes,      EMA_SLOW)
    fast_prev = calc_ema(closes[:-1], EMA_FAST)
    slow_prev = calc_ema(closes[:-1], EMA_SLOW)
    print(f"{symbol} EMA12={round(fast_now,2)} EMA21={round(slow_now,2)}")
    if fast_prev < slow_prev and fast_now > slow_now:
        return "buy"
    if fast_prev > slow_prev and fast_now < slow_now:
        return "sell"
    return None

def run_bot():
    print("Bot started")
    while True:
        try:
            print(f"\nScan at {time.strftime('%H:%M:%S')} UTC")
            bot_status["last_scan"] = time.strftime('%H:%M:%S UTC')
            for symbol in SYMBOLS:
                signal = check_signal(symbol)
                prev   = last_signal.get(symbol)
                print(f"{symbol} signal={signal} prev={prev}")
                if signal and signal != prev:
                    print(f">>> {symbol} {signal.upper()} firing!")
                    place_order(symbol, signal)
                    last_signal[symbol] = signal
                    bot_status["last_signal"] = last_signal.copy()
            now   = time.time()
            sleep = (15 * 60) - (now % (15 * 60)) + 5
            print(f"Sleeping {round(sleep/60,1)} mins")
            time.sleep(sleep)
        except Exception as e:
            bot_status["error"] = str(e)
            print(f"Loop error: {e}")
            traceback.print_exc()
            time.sleep(30)

@app.route("/")
def index():
    return jsonify({
        "status":     "running",
        "bot":        bot_status,
        "last_signal": last_signal,
        "symbols":    SYMBOLS,
        "interval":   INTERVAL,
        "leverage":   LEVERAGE,
        "trade_usdt": TRADE_USDT
    })

@app.route("/test")
def test():
    try:
        # Test 1 — public price
        price = get_price("BTCUSDT")

        # Test 2 — demo account balance
        params  = {"accountType": "UNIFIED"}
        headers = sign(params)
        r = requests.get(f"{BASE_URL_PRIVATE}/v5/account/wallet-balance",
            headers=headers, params=params, timeout=10)
        balance = r.json()

        return jsonify({
            "btc_price": price,
            "api_keys_set": bool(API_KEY and API_SECRET),
            "demo_balance": balance
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)})

@app.route("/forceorder")
def force_order():
    result = place_order("BTCUSDT", "buy")
    return jsonify({"result": result})

bot_thread = threading.Thread(target=run_bot, daemon=True)
bot_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
