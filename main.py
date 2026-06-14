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

# ── UPDATED: lower minimum so retrace protection activates earlier ──
MIN_PROFIT_TO_TRACK = 5.0   # retrace protection activates after 5% profit

# ── PER-SYMBOL CONFIG ─────────────────────────────────────────────────────────
# early_warning: set True per symbol when you're ready to use HLC3/EMA34 partial close
SYMBOL_CONFIG = {
    "BTCUSDT":  {"trade_usdt": 20, "leverage": 10, "early_warning": False},
    "HYPEUSDT": {"trade_usdt": 20, "leverage": 10, "early_warning": False},
    "SOLUSDT":  {"trade_usdt": 15, "leverage": 10, "early_warning": False},
    "WIFUSDT":  {"trade_usdt": 15, "leverage": 10, "early_warning": False},
}
SYMBOLS = list(SYMBOL_CONFIG.keys())

def get_trade_usdt(symbol):
    return SYMBOL_CONFIG.get(symbol, {}).get("trade_usdt", 20)

def get_leverage(symbol):
    return SYMBOL_CONFIG.get(symbol, {}).get("leverage", 10)

def is_early_warning_on(symbol):
    return SYMBOL_CONFIG.get(symbol, {}).get("early_warning", False)

INTERVAL       = "60"
EMA_FAST       = 12
EMA_SLOW       = 21
EMA_WARN       = 34        # EMA(34) used for HLC3 early warning
MAX_DAILY_LOSS = 50
RETRACE_PCT    = 0.70      # base retrace — overridden by tiered logic below
PARTIAL_PCT    = 0.25      # close 25% on early warning

# ─── STATE ────────────────────────────────────────────────────────────────────
last_signal        = {}
entry_price        = {}
peak_profit        = {}
bot_paused         = False
bot_status         = {"last_scan": "never", "error": None}
daily_pnl          = {"date": None, "pnl": 0.0, "trades": 0, "stopped": False}
processed_exec_ids = set()

# NEW: track which symbols have already had their early warning partial close fired
# prevents the bot firing 25% close every 45s on the same warning
early_warning_fired = set()

# ─── THREAD SAFETY ────────────────────────────────────────────────────────────
# One lock per symbol. Prevents the realtime monitor and the main bot loop from
# acting on the same position simultaneously — e.g. one thread closing while the
# other is placing a new order on the same symbol.
# Different symbols can still process concurrently — no unnecessary blocking.
symbol_locks = {s: threading.Lock() for s in SYMBOL_CONFIG}

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
# UPDATED: now returns (highs, lows, closes) — needed for HLC3 early warning
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
            highs  = [float(c[2]) for c in candles]
            lows   = [float(c[3]) for c in candles]
            closes = [float(c[4]) for c in candles]
            return highs, lows, closes
        except Exception as e:
            print(f"[ERROR] Candle fetch failed ({attempt+1}/3): {e}")
            time.sleep(2)
    return [], [], []

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
        # Acquire lock so realtime monitor can't act on this symbol mid-sync
        lock = symbol_locks.get(symbol)
        with lock:
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
                    early_warning_fired.discard(symbol)
                    print(f"[SYNC] {symbol} → no open position")
            except Exception as e:
                print(f"[ERROR] Sync failed {symbol}: {e}")
                traceback.print_exc()
    print(f"[SYNC] State: {last_signal}")

# ─── TIERED RETRACE THRESHOLD (NEW) ──────────────────────────────────────────
# The bigger your profit, the tighter the trail — protects big winners more
def get_retrace_threshold(peak):
    if peak >= 20:   return 0.35   # up 20%+ → only allow 35% giveback
    elif peak >= 10: return 0.50   # up 10–20% → allow 50% giveback
    elif peak >= 5:  return 0.60   # up 5–10% → allow 60% giveback
    else:            return 0.70   # up 2.5–5% → allow 70% giveback (base)

# ─── PEAK RETRACE CHECK (UPDATED: uses tiered threshold) ─────────────────────
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

    if sig == "buy":
        current_pct = (price - ep) / ep * 100
    else:
        current_pct = (ep - price) / ep * 100

    if current_pct > peak_profit.get(symbol, 0):
        peak_profit[symbol] = current_pct
        print(f"[PEAK] {symbol} new peak: {round(current_pct, 3)}%")

    peak = peak_profit.get(symbol, 0)

    if peak < MIN_PROFIT_TO_TRACK:
        print(f"[RETRACE] {symbol} peak={round(peak,3)}% — waiting for {MIN_PROFIT_TO_TRACK}% minimum")
        return False

    # UPDATED: tiered threshold instead of flat 70%
    threshold         = get_retrace_threshold(peak)
    retrace_triggered = current_pct <= peak * (1 - threshold)

    print(f"[RETRACE] {symbol} | "
          f"entry={ep} price={price} | "
          f"current={round(current_pct,3)}% | "
          f"peak={round(peak,3)}% | "
          f"allowedGiveback={int(threshold*100)}% | "
          f"threshold={round(peak*(1-threshold),3)}% | "
          f"triggered={retrace_triggered}")

    return retrace_triggered

# ─── EMA(34) HLC3 EARLY WARNING (NEW) ────────────────────────────────────────
# Translated from CM_EMA Trend Bars Pine Script by ChrisMoody
# hlc3 = (high + low + close) / 3 — more sensitive than close alone
# Fires 2–4 candles before the 12/21 EMA crossover confirms reversal
def check_early_warning(symbol):
    if symbol not in last_signal:
        return False

    highs, lows, closes = get_candles(symbol)
    if len(closes) < EMA_WARN + 5:
        return False

    hlc3   = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)]
    ema34  = calc_ema(hlc3, EMA_WARN)
    last_h = hlc3[-1]
    sig    = last_signal[symbol]

    if sig == "buy" and last_h < ema34:
        print(f"[WARN] {symbol} HLC3 {round(last_h,4)} < EMA34 {round(ema34,4)} — bearish early warning")
        return True
    if sig == "sell" and last_h >= ema34:
        print(f"[WARN] {symbol} HLC3 {round(last_h,4)} >= EMA34 {round(ema34,4)} — bullish early warning")
        return True

    return False

# ─── PARTIAL CLOSE (NEW) ──────────────────────────────────────────────────────
# Closes a percentage of the open position using reduceOnly — safe, won't open new trades
def partial_close(symbol, pct=PARTIAL_PCT):
    try:
        params  = {"category": "linear", "symbol": symbol}
        headers = sign_get(params)
        r = requests.get(f"{BASE_URL_PRIVATE}/v5/position/list",
            headers=headers, params=params, timeout=10)
        for pos in r.json().get("result", {}).get("list", []):
            size = float(pos.get("size", 0))
            if size > 0:
                precision = get_qty_precision(symbol)
                close_qty = round(size * pct, precision)
                if close_qty <= 0:
                    print(f"[PARTIAL] {symbol} qty too small, skipping")
                    return
                close_side = "Sell" if pos["side"] == "Buy" else "Buy"
                body       = {
                    "category":   "linear",
                    "symbol":     symbol,
                    "side":       close_side,
                    "orderType":  "Market",
                    "qty":        str(close_qty),
                    "reduceOnly": True
                }
                headers2 = sign(body)
                r2 = requests.post(f"{BASE_URL_PRIVATE}/v5/order/create",
                    headers=headers2, json=body, timeout=10)
                print(f"[PARTIAL] {symbol} closed {int(pct*100)}% "
                      f"({close_qty} of {size}) | {r2.json()}")
                time.sleep(1)
                update_daily_pnl(symbol)
    except Exception as e:
        print(f"[ERROR] Partial close failed {symbol}: {e}")
        traceback.print_exc()

# ─── SIGNAL ───────────────────────────────────────────────────────────────────
# UPDATED: unpacks (highs, lows, closes) from get_candles
def check_signal(symbol):
    highs, lows, closes = get_candles(symbol)
    if len(closes) < EMA_SLOW + 5:
        print(f"[ERROR] Not enough candles for {symbol}")
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
    lev     = str(get_leverage(symbol))
    body    = {"category": "linear", "symbol": symbol,
               "buyLeverage": lev, "sellLeverage": lev}
    headers = sign(body)
    r = requests.post(f"{BASE_URL_PRIVATE}/v5/position/set-leverage",
        headers=headers, json=body, timeout=10)
    result = r.json()
    if result.get("retCode") not in [0, 110043]:
        print(f"[WARN] Leverage {symbol}: {result}")

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
        entry_price.pop(symbol, None)
        peak_profit.pop(symbol, None)
        # Reset early warning so next trade starts fresh
        early_warning_fired.discard(symbol)

# ─── PLACE ORDER ──────────────────────────────────────────────────────────────
def place_order(symbol, signal):
    try:
        set_leverage(symbol)
        close_position(symbol)
        price = get_price(symbol)
        if not price:
            print(f"[ERROR] Price fetch failed for {symbol}")
            return
        precision = get_qty_precision(symbol)
        qty       = round((get_trade_usdt(symbol) * get_leverage(symbol)) / price, precision)
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
            # Reset early warning for this new trade
            early_warning_fired.discard(symbol)
            print(f"[ENTRY] {symbol} entry price set: {price}")
        return result
    except Exception as e:
        print(f"[ERROR] Order failed {symbol}: {e}")
        traceback.print_exc()

# ─── CLOSE ALL ────────────────────────────────────────────────────────────────
def close_all_positions():
    for symbol in SYMBOLS:
        lock = symbol_locks.get(symbol)
        with lock:
            close_position(symbol)
            last_signal.pop(symbol, None)
    print("[RISK] All positions closed")

# ─── REAL-TIME MONITOR (NEW) ──────────────────────────────────────────────────
# Runs every 45 seconds in a background thread
# Checks: (1) early warning → 25% partial close, (2) retrace → full close
# This fills the 60-minute blind spot between candle closes
def realtime_retrace_monitor():
    print("[MONITOR] Real-time monitor started — checking every 45s")
    while True:
        try:
            if not bot_paused and not daily_pnl["stopped"]:
                for symbol in list(last_signal.keys()):
                    lock = symbol_locks.get(symbol)
                    if not lock:
                        continue

                    # Acquire lock — blocks if main loop is currently acting on this symbol
                    # Non-blocking try: skip this symbol this cycle rather than pile up
                    if not lock.acquire(blocking=False):
                        print(f"[MONITOR] {symbol} locked by main loop — skipping this cycle")
                        continue

                    try:
                        # ── Early warning: only runs if enabled for this symbol ──
                        if is_early_warning_on(symbol) and symbol not in early_warning_fired:
                            if check_early_warning(symbol):
                                print(f"[EARLY WARNING] {symbol} — closing {int(PARTIAL_PCT*100)}% now")
                                partial_close(symbol, PARTIAL_PCT)
                                early_warning_fired.add(symbol)

                        # ── Peak retrace: close remaining position ──
                        if symbol in last_signal and check_peak_retrace(symbol):
                            print(f"[REALTIME RETRACE] {symbol} — closing full position")
                            close_position(symbol)
                            last_signal.pop(symbol, None)
                    finally:
                        lock.release()

        except Exception as e:
            print(f"[ERROR] Realtime monitor: {e}")
            traceback.print_exc()

        time.sleep(45)

# ─── BOT LOOP ─────────────────────────────────────────────────────────────────
def run_bot():
    print("=" * 60)
    print("GKC BOT — EMA REVERSAL + HLC3 EARLY WARNING SYSTEM")
    print(f"Timeframe: {INTERVAL}m")
    for s, cfg in SYMBOL_CONFIG.items():
        print(f"  {s}: ${cfg['trade_usdt']} x {cfg['leverage']}x leverage")
    print(f"Early warning: EMA({EMA_WARN}) on HLC3 → {int(PARTIAL_PCT*100)}% partial close")
    print(f"Peak retrace: tiered (35–70%) | Min profit: {MIN_PROFIT_TO_TRACK}%")
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

            if bot_paused:
                print("[PAUSED] Bot is paused — skipping signals")
                continue

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
                lock = symbol_locks.get(symbol)
                if not lock:
                    continue

                # Blocking acquire — main loop always waits for the lock.
                # This ensures the realtime monitor finishes any in-progress
                # close before we try to place a new order on the same symbol.
                with lock:

                    # ── Peak retrace check at candle close (backup for realtime) ──
                    if symbol in last_signal:
                        if check_peak_retrace(symbol):
                            print(f"[RETRACE] {symbol} — closing position at candle")
                            close_position(symbol)
                            last_signal.pop(symbol, None)
                            continue

                    # ── EMA 12/21 signal check ──
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
        "status":               "running",
        "bot":                  bot_status,
        "last_signal":          last_signal,
        "entry_price":          entry_price,
        "peak_profit":          peak_profit,
        "daily_pnl":            daily_pnl,
        "symbols":              {s: {**cfg, "early_warning_fired": s in early_warning_fired}
                                for s, cfg in SYMBOL_CONFIG.items()},
        "interval":             f"{INTERVAL}m",
        "paused":               bot_paused,
        "early_warning_fired":  list(early_warning_fired),
        "mode":                 "EMA 12/21 flip + HLC3 EMA34 early warning + tiered retrace"
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
                        "side":               pos["side"],
                        "size":               size,
                        "entry_price":        pos.get("avgPrice"),
                        "unrealised_pnl":     pos.get("unrealisedPnl"),
                        "liq_price":          pos.get("liqPrice"),
                        "peak_profit":        round(peak_profit.get(symbol, 0), 3),
                        "early_warning_fired": symbol in early_warning_fired,
                    }
        return jsonify({
            "open_positions":      positions,
            "last_signal":         last_signal,
            "last_scan":           bot_status["last_scan"],
            "daily_pnl":           daily_pnl,
            "paused":              bot_paused,
            "early_warning_fired": list(early_warning_fired)
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
        # close_all_positions acquires per-symbol locks internally — safe from Flask thread
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

# NEW route — manually reset early warning for a symbol if needed
@app.route("/reset_warning/<symbol>")
def reset_warning(symbol):
    early_warning_fired.discard(symbol.upper())
    return jsonify({"message": f"Early warning reset for {symbol.upper()}"})

# ─── START ────────────────────────────────────────────────────────────────────
bot_thread     = threading.Thread(target=run_bot, daemon=True)
retrace_thread = threading.Thread(target=realtime_retrace_monitor, daemon=True)

bot_thread.start()
retrace_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
