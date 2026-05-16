import os
import json
import hmac
import hashlib
import time
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
API_KEY        = os.environ.get("BYBIT_API_KEY")
API_SECRET     = os.environ.get("BYBIT_API_SECRET")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")
BASE_URL       = "https://api.bybit.com"

# ─── Risk settings ────────────────────────────────────────────────────────────
TRADE_USDT = 50      # USDT per trade
LEVERAGE   = 5       # leverage
SL_PCT     = 0.02    # 2% stop loss
TP_PCT     = 0.05    # 5% take profit

# ─── Bybit signature ──────────────────────────────────────────────────────────
def sign(params):
    timestamp  = str(int(time.time() * 1000))
    recv_window = "5000"
    param_str  = timestamp + API_KEY + recv_window + json.dumps(params)
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

def get(endpoint, params={}):
    r = requests.get(f"{BASE_URL}{endpoint}", params=params)
    return r.json()

# ─── Helpers ──────────────────────────────────────────────────────────────────
def get_price(symbol):
    r = get("/v5/market/tickers", {"category": "linear", "symbol": symbol})
    return float(r["result"]["list"][0]["lastPrice"])

def get_precision(symbol):
    r = get("/v5/market/instruments-info", {"category": "linear", "symbol": symbol})
    qty_step = r["result"]["list"][0]["lotSizeFilter"]["qtyStep"]
    decimals = len(qty_step.rstrip("0").split(".")[-1]) if "." in qty_step else 0
    return decimals

def set_leverage(symbol, leverage):
    params = {
        "category":   "linear",
        "symbol":     symbol,
        "buyLeverage":  str(leverage),
        "sellLeverage": str(leverage)
    }
    result = post("/v5/position/set-leverage", params)
    print(f"Leverage: {result}")

def close_existing(symbol):
    params = {
        "category": "linear",
        "symbol":   symbol
    }
    headers = sign(params)
    r = requests.get(f"{BASE_URL}/v5/position/list", headers=headers, params=params)
    positions = r.json().get("result", {}).get("list", [])
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
            print(f"Closed position: {result}")
    time.sleep(0.5)

def place_order(symbol, side, usdt_amount, leverage, sl_pct, tp_pct):
    set_leverage(symbol, leverage)
    close_existing(symbol)

    price     = get_price(symbol)
    precision = get_precision(symbol)
    qty       = round((usdt_amount * leverage) / price, precision)

    if side == "BUY":
        bybit_side = "Buy"
        sl_price   = round(price * (1 - sl_pct), 2)
        tp_price   = round(price * (1 + tp_pct), 2)
    else:
        bybit_side = "Sell"
        sl_price   = round(price * (1 + sl_pct), 2)
        tp_price   = round(price * (1 - tp_pct), 2)

    # Market entry with SL & TP in one order
    params = {
        "category":    "linear",
        "symbol":      symbol,
        "side":        bybit_side,
        "orderType":   "Market",
        "qty":         str(qty),
        "stopLoss":    str(sl_price),
        "takeProfit":  str(tp_price),
        "slTriggerBy": "LastPrice",
        "tpTriggerBy": "LastPrice",
        "timeInForce": "GTC"
    }

    result = post("/v5/order/create", params)
    print(f"Order placed: {result}")
    return {"entry": price, "sl": sl_price, "tp": tp_price, "qty": qty, "result": result}

# ─── Webhook ──────────────────────────────────────────────────────────────────
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()
    print(f"Received: {data}")

    if data.get("secret") != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    signal = data.get("signal")
    symbol = data.get("symbol", "")

    # Clean up symbol — Bybit uses BTCUSDT format
    symbol = symbol.replace(".P", "").replace("/", "")
    if not symbol.endswith("USDT"):
        symbol = symbol + "USDT"

    if signal == "buy":
        result = place_order(symbol, "BUY",  TRADE_USDT, LEVERAGE, SL_PCT, TP_PCT)
    elif signal == "sell":
        result = place_order(symbol, "SELL", TRADE_USDT, LEVERAGE, SL_PCT, TP_PCT)
    else:
        return jsonify({"error": "Unknown signal"}), 400

    return jsonify({"status": "ok", "result": result})

@app.route("/", methods=["GET"])
def health():
    return "GKC Bybit Bot is running", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
