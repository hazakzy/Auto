import os
import json
import hmac
import hashlib
import time
import requests
from flask import Flask, jsonify
import threading
import traceback
from datetime import datetime, timezone

app = Flask(__name__)

API_KEY          = os.environ.get("BYBIT_API_KEY")
API_SECRET       = os.environ.get("BYBIT_API_SECRET")
BASE_URL_PUBLIC  = "https://api.bybit.com"
BASE_URL_PRIVATE = "https://api-demo.bybit.com"

TRADE_USDT      = 100
LEVERAGE        = 2
SYMBOLS         = ["BTCUSDT"]
INTERVAL        = "15"
EMA_FAST        = 12
EMA_SLOW        = 21
MAX_DAILY_LOSS  = 50    # stop trading if daily loss exceeds $50
MIN_BODY_PCT    = 0.001  # minimum candle body = 0.1% of price

last_signal  = {}
bot_status   = {"last_scan": "never", "error": None}
daily_pnl    = {"date": None, "pnl": 0.0, "trades": 0, "stopped": False}

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

# ─── EMA ──────────────────────────────────────────────────────────────────────
def calc_ema(prices, period):
    k   = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = p * k + ema * (1 - k)
    return ema

# ─── Market data ──────────────────────────────────────────────────────────────
def get_candles(symbol):
    for attempt in range(3):
        try:
            r = requests.get(f"{BASE_URL_PUBLIC}/v5/market/kline", params={
                "category": "linear",
                "symbol":   symbol,
                "interval": INTERVAL,
                "limit":    150
            }, timeout=10)
            candles = r.json()["result"]["list"]
            candles.reverse()
            # Return full candle data: [open, high, low, close]
            return candles
        except Exception as e:
            print(f"Candle fetch attempt {attempt+1} failed: {e}")
            time.sleep(2)
    return []

def get_price(symbol):
    for attempt in range(3):
        try:
            r = requests.get(f"{BASE_URL_PUBLIC}/v5/market/tickers", params={
                "category": "linear", "symbol": symbol
            }, timeout=10)
            return float(r.json()["result"]["list"][0]["lastPrice"])
        except Exception as e:
            print(f"Price fetch attempt {attempt+1} failed: {e}")
            time.sleep(2)
    return None

def get_qty_precision(symbol):
    for attempt in range(3):
        try:
            r = requests.get(f"{BASE_URL_PUBLIC}/v5/market/instruments-info", params={
                "category": "linear", "symbol": symbol
            }, timeout=10)
            step = r.json()["result"]["list"][0]["lotSizeFilter"]["qtyStep"]
            return len(step.rstrip("0").split(".")[-1]) if "." in step else 0
        except Exception as e:
            print(f"Precision fetch attempt {attempt+1} failed: {e}")
            time.sleep(2)
    return 3

# ─── Precise candle close detection ──────────────────────────────────────────
def wait_for_candle_close():
    interval_seconds = int(INTERVAL) * 60
    now              = time.time()
    seconds_into     = now % interval_seconds
    seconds_left     = interval_seconds - seconds_into
    # Only sleep if we're not already within 2 seconds of close
    if seconds_left > 2:
        sleep_time = seconds_left + 2  # +2 sec buffer
        print(f"Waiting {round(sleep_time/60, 2)} mins for candle close...")
        time.sleep(sleep_time)
    else:
        time.sleep(3)

# ─── Daily PnL reset ──────────────────────────────────────────────────────────
def check_daily_reset():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if daily_pnl["date"] != today:
        daily_pnl["date"]    = today
        daily_pnl["pnl"]     = 0.0
        daily_pnl["trades"]  = 0
        daily_pnl["stopped"] = False
        print(f"Daily PnL reset for {today}")

def update_daily_pnl(symbol):
    try:
        params  = {"category": "linear", "symbol": symbol, "limit": 10}
        headers = sign(params)
        r = requests.get(f"{BASE_URL_PRIVATE}/v5/execution/list",
            headers=headers, params=params, timeout=10)
        trades = r.json().get("result", {}).get("list", [])
        today  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for trade in trades:
            trade_time = datetime.fromtimestamp(
                int(trade["execTime"]) / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%d")
            if trade_time == today:
                pnl = float(trade.get("closedPnl", 0))
                if pnl != 0:
                    daily_pnl["pnl"]    += pnl
                    daily_pnl["trades"] += 1
        print(f"Daily PnL updated: ${round(daily_pnl['pnl'], 2)} over {daily_pnl['trades']} trades")
    except Exception as e:
        print(f"PnL update error: {e}")

# ─── Sync state from Bybit ────────────────────────────────────────────────────
def sync_state_from_bybit():
    print("Syncing state from Bybit...")
    for symbol in SYMBOLS:
        try:
            params  = {"category": "linear", "symbol": symbol}
            headers = sign(params)
            r = requests.get(f"{BASE_URL_PRIVATE}/v5/position/list",
                headers=headers, params=params, timeout=10)
            positions = r.json().get("result", {}).get("list", [])
            found = False
            for pos in positions:
                size = float(pos.get("size", 0))
                side = pos.get("side", "")
                if size > 0:
                    last_signal[symbol] = "buy" if side == "Buy" else "sell"
                    print(f"Synced {symbol} → {side} size={size} → last_signal={last_signal[symbol]}")
                    found = True
            if not found:
                print(f"Synced {symbol} → no open position")
        except Exception as e:
            print(f"Sync error for {symbol}: {e}")
    print(f"State after sync: {last_signal}")

# ─── Signal logic ─────────────────────────────────────────────────────────────
def check_signal(symbol):
    candles = get_candles(symbol)
    if len(candles) < EMA_SLOW + 5:
        print(f"{symbol} not enough candles: {len(candles)}")
        return None

    closes = [float(c[4]) for c in candles]
    opens  = [float(c[1]) for c in candles]

    fast_now   = calc_ema(closes,      EMA_FAST)
    slow_now   = calc_ema(closes,      EMA_SLOW)
    fast_prev  = calc_ema(closes[:-1], EMA_FAST)
    slow_prev  = calc_ema(closes[:-1], EMA_SLOW)
    fast_prev2 = calc_ema(closes[:-2], EMA_FAST)
    slow_prev2 = calc_ema(closes[:-2], EMA_SLOW)

    # Candle body filter — confirmation candle must have meaningful body
    confirm_close = closes[-2]
    confirm_open  = opens[-2]
    candle_body   = abs(confirm_close - confirm_open) / confirm_open
    body_ok       = candle_body >= MIN_BODY_PCT

    print(f"{symbol} | "
          f"C2 12={round(fast_prev2,2)} 21={round(slow_prev2,2)} | "
          f"C1 12={round(fast_prev,2)} 21={round(slow_prev,2)} body={round(candle_body*100,3)}% | "
          f"NOW 12={round(fast_now,2)} 21={round(slow_now,2)}")

    if not body_ok:
        print(f"{symbol} — candle body too small ({round(candle_body*100,3)}%) — skipping")
        return None

    # Buy: crossover on C2, confirmed on C1, still holds now
    buy_signal  = (fast_prev2 < slow_prev2 and
                   fast_prev  > slow_prev  and
                   fast_now   > slow_now)

    # Sell: crossunder on C2, confirmed on C1, still holds now
    sell_signal = (fast_prev2 > slow_prev2 and
                   fast_prev  < slow_prev  and
                   fast_now   < slow_now)

    if buy_signal:
        return "buy"
    if sell_signal:
        return "sell"
    return None

# ─── Order helpers ────────────────────────────────────────────────────────────
def set_leverage(symbol):
    body    = {"category": "linear", "symbol": symbol,
               "buyLeverage": str(LEVERAGE), "sellLeverage": str(LEVERAGE)}
    headers = sign(body)
    r = requests.post(f"{BASE_URL_PRIVATE}/v5/position/set-leverage",
        headers=headers, json=body, timeout=10)
    result = r.json()
    if result.get("retCode") not in [0, 110043]:
        print(f"Leverage warning: {result}")

def close_position(symbol):
    params  = {"category": "linear", "symbol": symbol}
    headers = sign(params)
    r = requests.get(f"{BASE_URL_PRIVATE}/v5/position/list",
        headers=headers, params=params, timeout=10)
    closed = False
    for pos in r.json().get("result", {}).get("list", []):
        size = float(pos.get("size", 0))
        if size > 0:
            close_side = "Sell" if pos["side"] == "Buy" else "Buy"
            body    = {"category": "linear", "symbol": symbol,
                       "side": close_side, "orderType": "Market",
                       "qty": str(size), "reduceOnly": True}
            headers = sign(body)
            r2 = requests.post(f"{BASE_URL_PRIVATE}/v5/order/create",
                headers=headers, json=body, timeout=10)
            print(f"Closed {symbol} {pos['side']} size={size}: {r2.json()}")
            closed = True
    if closed:
        time.sleep(1)
        update_daily_pnl(symbol)

def place_order(symbol, signal):
    try:
        set_leverage(symbol)
        close_position(symbol)

        price = get_price(symbol)
        if not price:
            print(f"Could not get price for {symbol}")
            return {"error": "price fetch failed"}

        precision = get_qty_precision(symbol)
        qty       = round((TRADE_USDT * LEVERAGE) / price, precision)
        side      = "Buy" if signal == "buy" else "Sell"

        body    = {"category": "linear", "symbol": symbol,
                   "side": side, "orderType": "Market",
                   "qty": str(qty), "timeInForce": "GTC"}
        headers = sign(body)
        r = requests.post(f"{BASE_URL_PRIVATE}/v5/order/create",
            headers=headers, json=body, timeout=10)
        result = r.json()
        print(f"Order {symbol} {side} qty={qty} @ {price} | {result}")
        return result

    except Exception as e:
        print(f"Order error {symbol}: {e}")
        traceback.print_exc()
        return {"error": str(e)}

def close_all_positions():
    for symbol in SYMBOLS:
        close_position(symbol)
        last_signal.pop(symbol, None)
    print("All positions closed")

# ─── Bot loop ─────────────────────────────────────────────────────────────────
def run_bot():
    print("=" * 55)
    print("GKC Bot — EMA 12/21 — 15M — 2 candle confirm")
    print(f"Max daily loss: ${MAX_DAILY_LOSS} | Min body: {MIN_BODY_PCT*100}%")
    print("=" * 55)

    sync_state_from_bybit()

    while True:
        try:
            # Wait for precise candle close
            wait_for_candle_close()

            # Reset daily PnL if new day
            check_daily_reset()

            print(f"\n--- Scan {time.strftime('%Y-%m-%d %H:%M:%S')} UTC ---")
            bot_status["last_scan"] = time.strftime('%Y-%m-%d %H:%M:%S UTC')
            print(f"Daily PnL: ${round(daily_pnl['pnl'],2)} | "
                  f"Trades today: {daily_pnl['trades']} | "
                  f"Stopped: {daily_pnl['stopped']}")

            # Daily loss limit check
            if daily_pnl["stopped"]:
                print("Daily loss limit reached — skipping all signals today")
                continue

            if daily_pnl["pnl"] <= -MAX_DAILY_LOSS:
                daily_pnl["stopped"] = True
                print(f"Daily loss limit ${MAX_DAILY_LOSS} hit — closing all and stopping for today")
                close_all_positions()
                continue

            for symbol in SYMBOLS:
                signal = check_signal(symbol)
                prev   = last_signal.get(symbol)
                print(f"{symbol} | signal={signal} | prev={prev}")

                if signal and signal != prev:
                    print(f">>> {symbol} {signal.upper()} confirmed — placing order")
                    place_order(symbol, signal)
                    last_signal[symbol] = signal
                    bot_status["last_signal"] = last_signal.copy()
                else:
                    print(f"{symbol} | holding — no new signal")

        except Exception as e:
            bot_status["error"] = str(e)
            print(f"Bot loop error: {e}")
            traceback.print_exc()
            time.sleep(30)

# ─── Routes ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return jsonify({
        "status":      "running",
        "bot":         bot_status,
        "last_signal": last_signal,
        "daily_pnl":   daily_pnl,
        "symbols":     SYMBOLS,
        "interval":    f"{INTERVAL}m",
        "leverage":    LEVERAGE,
        "trade_usdt":  TRADE_USDT,
        "mode":        "flip on signal — 2 candle confirm — no SL no TP"
    })

@app.route("/status")
def status():
    try:
        positions = {}
        for symbol in SYMBOLS:
            params  = {"category": "linear", "symbol": symbol}
            headers = sign(params)
            r = requests.get(f"{BASE_URL_PRIVATE}/v5/position/list",
                headers=headers, params=params, timeout=10)
            for pos in r.json().get("result", {}).get("list", []):
                size = float(pos.get("size", 0))
                if size > 0:
                    positions[symbol] = {
                        "side":           pos["side"],
                        "size":           size,
                        "entry_price":    pos.get("avgPrice", "N/A"),
                        "unrealised_pnl": pos.get("unrealisedPnl", "N/A"),
                        "liq_price":      pos.get("liqPrice", "N/A")
                    }
        return jsonify({
            "open_positions": positions,
            "last_signal":    last_signal,
            "last_scan":      bot_status["last_scan"],
            "daily_pnl":      daily_pnl
        })
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/sync")
def sync():
    try:
        sync_state_from_bybit()
        return jsonify({
            "message":     "Synced successfully",
            "last_signal": last_signal
        })
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/closeall")
def closeall():
    try:
        close_all_positions()
        return jsonify({
            "message":     "All positions closed",
            "last_signal": last_signal
        })
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/test")
def test():
    try:
        r = requests.get(f"{BASE_URL_PUBLIC}/v5/market/tickers", params={
            "category": "linear", "symbol": "BTCUSDT"
        }, timeout=10)
        return jsonify({
            "status_code":  r.status_code,
            "btc_price":    r.json()["result"]["list"][0]["lastPrice"],
            "api_keys_set": bool(API_KEY and API_SECRET)
        })
    except Exception as e:
        return jsonify({"error": str(e)})

# ─── Start ────────────────────────────────────────────────────────────────────
bot_thread = threading.Thread(target=run_bot, daemon=True)
bot_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
