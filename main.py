import os
import json
import hmac
import hashlib
import time
import requests
import threading
import traceback

from flask import Flask, jsonify
from datetime import datetime, timezone

app = Flask(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
API_KEY          = os.environ.get("BYBIT_API_KEY")
API_SECRET       = os.environ.get("BYBIT_API_SECRET")
BASE_URL_PUBLIC  = "https://api.bybit.com"
BASE_URL_PRIVATE = "https://api.bybit.com"

TRADE_USDT     = 20
LEVERAGE       = 10
SYMBOLS         = ["BTCUSDT", "HYPEUSDT"]
INTERVAL       = "60"
EMA_FAST       = 12
EMA_SLOW       = 21
MAX_DAILY_LOSS = 50
RETRACE_PCT    = 0.70   # close if price gives back 70% of peak profit

# ─── STATE ────────────────────────────────────────────────────────────────────
last_signal        = {}
entry_price        = {}   # entry price per symbol
peak_profit        = {}   # peak profit % per symbol
bot_paused         = False
bot_status         = {"last_scan": "never", "error": None}
daily_pnl          = {"date": None, "pnl": 0.0, "trades": 0, "stopped": False}
processed_exec_ids = set()

# ─── SIGNATURE ────────────────────────────────────────────────────────────────
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

def sign_get(params):
    timestamp   = str(int(time.time() * 1000))
    recv_window = "5000"
    query_str   = "&".join(f"{k}={v}" for k, v in params.items())
    param_str   = timestamp + API_KEY + recv_window + query_str
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

# ─── MARKET DATA ──────────────────────────────────────────────────────────────
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
            return [float(c[4]) for c in candles]
        except Exception as e:
            print(f"[ERROR] Candle fetch failed ({attempt+1}/3): {e}")
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
            print(f"[ERROR] Price fetch failed ({attempt+1}/3): {e}")
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
            print(f"[ERROR] Precision fetch failed ({attempt+1}/3): {e}")
            time.sleep(2)
    return 3

# ─── CANDLE WAIT ──────────────────────────────────────────────────────────────
def wait_for_candle_close():
    interval_seconds = int(INTERVAL) * 60
    now          = time.time()
    seconds_left = interval_seconds - (now % interval_seconds)
    sleep_time   = seconds_left + 2
    print(f"[WAIT] Sleeping {round(sleep_time/60, 2)} mins")
    time.sleep(sleep_time)

# ─── DAILY RESET ──────────────────────────────────────────────────────────────
def check_daily_reset():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if daily_pnl["date"] != today:
        daily_pnl["date"]    = today
        daily_pnl["pnl"]     = 0.0
        daily_pnl["trades"]  = 0
        daily_pnl["stopped"] = False
        processed_exec_ids.clear()
        print(f"[RESET] Daily reset for {today}")

# ─── DAILY PNL ────────────────────────────────────────────────────────────────
def update_daily_pnl(symbol):
    try:
        params  = {"category": "linear", "symbol": symbol, "limit": "20"}
        headers = sign_get(params)
        r = requests.get(f"{BASE_URL_PRIVATE}/v5/execution/list",
            headers=headers, params=params, timeout=10)
        trades = r.json().get("result", {}).get("list", [])
        today  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for trade in trades:
            trade_time = datetime.fromtimestamp(
                int(trade["execTime"]) / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%d")
            if trade_time != today:
                continue
            exec_id = trade.get("execId")
            if exec_id in processed_exec_ids:
                continue
            processed_exec_ids.add(exec_id)
            pnl = float(trade.get("closedPnl", 0))
            if pnl != 0:
                daily_pnl["pnl"]    += pnl
                daily_pnl["trades"] += 1
        print(f"[PNL] ${round(daily_pnl['pnl'],2)} | Trades: {daily_pnl['trades']}")
    except Exception as e:
        print(f"[ERROR] PnL update failed: {e}")

# ─── SYNC STATE ───────────────────────────────────────────────────────────────
def sync_state_from_bybit():
    print("[SYNC] Syncing positions from Bybit")
    for symbol in SYMBOLS:
        try:
            params  = {"category": "linear", "symbol": symbol}
            headers = sign_get(params)
            r = requests.get(f"{BASE_URL_PRIVATE}/v5/position/list",
                headers=headers, params=params, timeout=10)
            positions = r.json().get("result", {}).get("list", [])
            found     = False
            for pos in positions:
                size = float(pos.get("size", 0))
                if size > 0:
                    side = pos.get("side")
                    sig  = "buy" if side == "Buy" else "sell"
                    last_signal[symbol] = sig
                    # Restore entry price if not already set
                    if symbol not in entry_price:
                        entry_price[symbol] = float(pos.get("avgPrice", 0))
                        peak_profit[symbol] = 0.0
                        print(f"[SYNC] Restored entry price: {entry_price[symbol]}")
                    print(f"[SYNC] {symbol} → {side} size={size} "
                          f"entry={pos.get('avgPrice')} pnl={pos.get('unrealisedPnl')}")
                    found = True
            if not found:
                last_signal.pop(symbol, None)
                entry_price.pop(symbol, None)
                peak_profit.pop(symbol, None)
                print(f"[SYNC] {symbol} → no open position")
        except Exception as e:
            print(f"[ERROR] Sync failed {symbol}: {e}")
            traceback.print_exc()
    print(f"[SYNC] State: {last_signal}")

# ─── PEAK RETRACE CHECK ───────────────────────────────────────────────────────
def check_peak_retrace(symbol):
    if symbol not in entry_price or symbol not in last_signal:
        return False

    price = get_price(symbol)
    if not price:
        return False

    ep  = entry_price[symbol]
    sig = last_signal[symbol]

    if ep == 0:
        return False

    # Current profit %
    if sig == "buy":
        current_pct = (price - ep) / ep * 100
    else:
        current_pct = (ep - price) / ep * 100

    # Update peak
    if current_pct > peak_profit.get(symbol, 0):
        peak_profit[symbol] = current_pct
        print(f"[PEAK] {symbol} new peak: {round(current_pct, 3)}%")

    peak = peak_profit.get(symbol, 0)

    # Only check retrace if we've been in profit
    if peak <= 0:
        return False

    # Check if current profit has retraced 70% from peak
    retrace_triggered = current_pct <= peak * (1 - RETRACE_PCT)

    print(f"[RETRACE] {symbol} | entry={ep} price={price} | "
          f"current={round(current_pct,3)}% | "
          f"peak={round(peak,3)}% | "
          f"threshold={round(peak*(1-RETRACE_PCT),3)}% | "
          f"triggered={retrace_triggered}")

    return retrace_triggered

# ─── SIGNAL ───────────────────────────────────────────────────────────────────
def check_signal(symbol):
    closes = get_candles(symbol)
    if len(closes) < EMA_SLOW + 5:
        print(f"[ERROR] Not enough candles")
        return None
    fast_now   = calc_ema(closes,      EMA_FAST)
    slow_now   = calc_ema(closes,      EMA_SLOW)
    fast_prev  = calc_ema(closes[:-1], EMA_FAST)
    slow_prev  = calc_ema(closes[:-1], EMA_SLOW)
    fast_prev2 = calc_ema(closes[:-2], EMA_FAST)
    slow_prev2 = calc_ema(closes[:-2], EMA_SLOW)
    print(f"[EMA] {symbol} | "
          f"C2 {round(fast_prev2,2)}/{round(slow_prev2,2)} | "
          f"C1 {round(fast_prev,2)}/{round(slow_prev,2)} | "
          f"NOW {round(fast_now,2)}/{round(slow_now,2)}")
    buy_signal  = (fast_prev2 < slow_prev2 and fast_prev > slow_prev and fast_now > slow_now)
    sell_signal = (fast_prev2 > slow_prev2 and fast_prev < slow_prev and fast_now < slow_now)
    if buy_signal:
        return "buy"
    if sell_signal:
        return "sell"
    return None

# ─── LEVERAGE ─────────────────────────────────────────────────────────────────
def set_leverage(symbol):
    body    = {"category": "linear", "symbol": symbol,
               "buyLeverage": str(LEVERAGE), "sellLeverage": str(LEVERAGE)}
    headers = sign(body)
    r = requests.post(f"{BASE_URL_PRIVATE}/v5/position/set-leverage",
        headers=headers, json=body, timeout=10)
    result = r.json()
    if result.get("retCode") not in [0, 110043]:
        print(f"[WARN] Leverage: {result}")

# ─── CLOSE POSITION ───────────────────────────────────────────────────────────
def close_position(symbol):
    params  = {"category": "linear", "symbol": symbol}
    headers = sign_get(params)
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
            print(f"[CLOSE] {symbol} {pos['side']} size={size}: {r2.json()}")
            closed = True
    if closed:
        time.sleep(1)
        update_daily_pnl(symbol)
        # Reset peak tracking
        entry_price.pop(symbol, None)
        peak_profit.pop(symbol, None)

# ─── PLACE ORDER ──────────────────────────────────────────────────────────────
def place_order(symbol, signal):
    try:
        set_leverage(symbol)
        close_position(symbol)
        price = get_price(symbol)
        if not price:
            print(f"[ERROR] Price fetch failed")
            return
        precision = get_qty_precision(symbol)
        qty       = round((TRADE_USDT * LEVERAGE) / price, precision)
        side      = "Buy" if signal == "buy" else "Sell"
        body      = {"category": "linear", "symbol": symbol,
                     "side": side, "orderType": "Market",
                     "qty": str(qty), "timeInForce": "GTC"}
        headers   = sign(body)
        r = requests.post(f"{BASE_URL_PRIVATE}/v5/order/create",
            headers=headers, json=body, timeout=10)
        result = r.json()
        print(f"[ORDER] {symbol} {side} qty={qty} @ {price} | {result}")
        if result.get("retCode") == 0:
            entry_price[symbol] = price
            peak_profit[symbol] = 0.0
            print(f"[ENTRY] {symbol} entry price set: {price}")
        return result
    except Exception as e:
        print(f"[ERROR] Order failed: {e}")
        traceback.print_exc()

# ─── CLOSE ALL ────────────────────────────────────────────────────────────────
def close_all_positions():
    for symbol in SYMBOLS:
        close_position(symbol)
        last_signal.pop(symbol, None)
    print("[RISK] All positions closed")

# ─── BOT LOOP ─────────────────────────────────────────────────────────────────
def run_bot():
    print("=" * 60)
    print("GKC BOT — BTC EMA REVERSAL SYSTEM")
    print(f"Timeframe: {INTERVAL}m | Leverage: {LEVERAGE}x | Size: ${TRADE_USDT}")
    print(f"Peak retrace exit: {int(RETRACE_PCT*100)}%")
    print("=" * 60)
    sync_state_from_bybit()
    while True:
        try:
            wait_for_candle_close()
            check_daily_reset()

            print(f"\n[SCAN] {time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            bot_status["last_scan"] = time.strftime('%Y-%m-%d %H:%M:%S UTC')
            print(f"[STATUS] PnL=${round(daily_pnl['pnl'],2)} | "
                  f"Trades={daily_pnl['trades']} | "
                  f"Stopped={daily_pnl['stopped']} | "
                  f"Paused={bot_paused}")

            # Pause check
            if bot_paused:
                print("[PAUSED] Bot is paused — skipping signals")
                continue

            # Daily loss check
            if daily_pnl["stopped"]:
                print("[RISK] Daily stop active")
                continue

            if daily_pnl["pnl"] <= -MAX_DAILY_LOSS:
                daily_pnl["stopped"] = True
                print("[RISK] Max daily loss reached — closing all")
                close_all_positions()
                continue

            sync_state_from_bybit()

            for symbol in SYMBOLS:

                # ── Peak retrace check (runs every candle) ──
                if symbol in last_signal:
                    if check_peak_retrace(symbol):
                        print(f"[RETRACE] {symbol} — 70% peak retrace hit — closing position")
                        close_position(symbol)
                        last_signal.pop(symbol, None)
                        continue

                # ── Signal check ──
                signal = check_signal(symbol)
                prev   = last_signal.get(symbol)
                print(f"[SIGNAL] {symbol} | current={signal} | previous={prev}")

                if signal and signal != prev:
                    print(f"[ORDER] {symbol} {signal.upper()} confirmed")
                    place_order(symbol, signal)
                    last_signal[symbol] = signal
                    bot_status["last_signal"] = last_signal.copy()
                else:
                    print(f"[HOLD] {symbol} | no confirmed reversal")

        except Exception as e:
            bot_status["error"] = str(e)
            print(f"[ERROR] Bot loop: {e}")
            traceback.print_exc()
            time.sleep(30)

# ─── ROUTES ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return jsonify({
        "status":      "running",
        "bot":         bot_status,
        "last_signal": last_signal,
        "entry_price": entry_price,
        "peak_profit": peak_profit,
        "daily_pnl":   daily_pnl,
        "symbols":     SYMBOLS,
        "interval":    f"{INTERVAL}m",
        "leverage":    LEVERAGE,
        "trade_usdt":  TRADE_USDT,
        "paused":      bot_paused,
        "mode":        "EMA flip + 70% peak retrace exit"
    })

@app.route("/status")
def status():
    try:
        positions = {}
        for symbol in SYMBOLS:
            params  = {"category": "linear", "symbol": symbol}
            headers = sign_get(params)
            r = requests.get(f"{BASE_URL_PRIVATE}/v5/position/list",
                headers=headers, params=params, timeout=10)
            for pos in r.json().get("result", {}).get("list", []):
                size = float(pos.get("size", 0))
                if size > 0:
                    positions[symbol] = {
                        "side":           pos["side"],
                        "size":           size,
                        "entry_price":    pos.get("avgPrice"),
                        "unrealised_pnl": pos.get("unrealisedPnl"),
                        "liq_price":      pos.get("liqPrice"),
                        "peak_profit":    round(peak_profit.get(symbol, 0), 3),
                    }
        return jsonify({
            "open_positions": positions,
            "last_signal":    last_signal,
            "last_scan":      bot_status["last_scan"],
            "daily_pnl":      daily_pnl,
            "paused":         bot_paused
        })
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/pause")
def pause():
    global bot_paused
    bot_paused = True
    print("[PAUSE] Bot paused by user")
    return jsonify({"message": "Bot paused — no new orders will be placed"})

@app.route("/resume")
def resume():
    global bot_paused
    bot_paused = False
    print("[RESUME] Bot resumed by user")
    return jsonify({"message": "Bot resumed"})

@app.route("/sync")
def sync():
    try:
        sync_state_from_bybit()
        return jsonify({"message": "Synced", "last_signal": last_signal})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/closeall")
def closeall():
    try:
        close_all_positions()
        return jsonify({"message": "All positions closed"})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/debug")
def debug():
    try:
        results = {}
        for symbol in SYMBOLS:
            params  = {"category": "linear", "symbol": symbol}
            headers = sign_get(params)
            r = requests.get(f"{BASE_URL_PRIVATE}/v5/position/list",
                headers=headers, params=params, timeout=10)
            results[symbol] = r.json()
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/test")
def test():
    try:
        r = requests.get(f"{BASE_URL_PUBLIC}/v5/market/tickers", params={
            "category": "linear", "symbol": "BTCUSDT"
        }, timeout=10)
        return jsonify({
            "btc_price":    r.json()["result"]["list"][0]["lastPrice"],
            "api_keys_set": bool(API_KEY and API_SECRET)
        })
    except Exception as e:
        return jsonify({"error": str(e)})

# ─── START ────────────────────────────────────────────────────────────────────
bot_thread = threading.Thread(target=run_bot, daemon=True)
bot_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
