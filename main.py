import os
import json
import hmac
import hashlib
import time
import csv
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
TELEGRAM_TOKEN      = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID", "@cryptoedgelab")
TELEGRAM_PRIVATE_ID = os.environ.get("TELEGRAM_PRIVATE_ID", "5351361684")

MIN_PROFIT_TO_TRACK = 5.0

# ── PER-SYMBOL CONFIG ─────────────────────────────────────────────────────────
SYMBOL_CONFIG = {
    "BTCUSDT":  {"trade_usdt": 20, "leverage": 10, "early_warning": False, "paused": False},
    "HYPEUSDT": {"trade_usdt": 10, "leverage": 10, "early_warning": False, "paused": False},
    "SOLUSDT":  {"trade_usdt": 15, "leverage": 10, "early_warning": False, "paused": False},
    "ETHUSDT":  {"trade_usdt": 15, "leverage": 10, "early_warning": False, "paused": False},
}
SYMBOLS = list(SYMBOL_CONFIG.keys())

def get_trade_usdt(symbol):
    return SYMBOL_CONFIG.get(symbol, {}).get("trade_usdt", 20)

def get_leverage(symbol):
    return SYMBOL_CONFIG.get(symbol, {}).get("leverage", 10)

def is_early_warning_on(symbol):
    return SYMBOL_CONFIG.get(symbol, {}).get("early_warning", False)

def is_symbol_paused(symbol):
    return SYMBOL_CONFIG.get(symbol, {}).get("paused", False)

INTERVAL       = "60"
EMA_FAST       = 12
EMA_SLOW       = 21
EMA_WARN       = 34
MAX_DAILY_LOSS = 25
RETRACE_PCT    = 0.70
PARTIAL_PCT    = 0.25

# ─── FEATURE FLAGS ────────────────────────────────────────────────────────────
ENABLE_ADX_FILTER     = False
ENABLE_TRADE_LOGGING  = False
ENABLE_HARD_STOP_LOSS = False
ENABLE_DUAL_TIMEFRAME = False

ADX_PERIOD       = 14
ADX_THRESHOLD    = 20
STOP_LOSS_PCT    = 4.0
DUAL_TF_INTERVAL = "15"
LOG_FILE         = "/tmp/gkc_trades.csv"

# ─── BOT MODE ─────────────────────────────────────────────────────────────────
# "trading"     → signals + orders + community alerts
# "signal_only" → community alerts only — no orders placed
# "paused"      → completely silent — nothing fires
BOT_MODE = "trading"

# ─── STATE ────────────────────────────────────────────────────────────────────
last_signal          = {}
entry_price          = {}
peak_profit          = {}
bot_status           = {"last_scan": "never", "error": None}
daily_pnl            = {"date": None, "pnl": 0.0, "trades": 0, "stopped": False}
processed_exec_ids   = set()
early_warning_fired  = set()
early_signal_alerted = {}

# ─── THREAD SAFETY ────────────────────────────────────────────────────────────
symbol_locks = {s: threading.Lock() for s in SYMBOL_CONFIG}
mode_lock    = threading.Lock()

def get_mode():
    with mode_lock:
        return BOT_MODE

def set_mode(mode):
    global BOT_MODE
    with mode_lock:
        BOT_MODE = mode

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
def send_telegram(message, private=False):
    if not TELEGRAM_TOKEN:
        return
    try:
        chat_id = TELEGRAM_PRIVATE_ID if private else TELEGRAM_CHAT_ID
        url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data    = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        r       = requests.post(url, json=data, timeout=10)
        if r.status_code != 200:
            print(f"[TELEGRAM] Failed: {r.text}")
    except Exception as e:
        print(f"[TELEGRAM] Error: {e}")

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
def get_candles(symbol, interval=None):
    tf = interval or INTERVAL
    for attempt in range(3):
        try:
            r = requests.get(f"{BASE_URL_PUBLIC}/v5/market/kline", params={
                "category": "linear",
                "symbol":   symbol,
                "interval": tf,
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
        early_signal_alerted.clear()
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

# ─── TIERED RETRACE ───────────────────────────────────────────────────────────
def get_retrace_threshold(peak):
    if peak >= 20:   return 0.35
    elif peak >= 10: return 0.50
    elif peak >= 5:  return 0.60
    else:            return 0.70

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
    threshold         = get_retrace_threshold(peak)
    retrace_triggered = current_pct <= peak * (1 - threshold)
    print(f"[RETRACE] {symbol} | entry={ep} price={price} | "
          f"current={round(current_pct,3)}% | peak={round(peak,3)}% | "
          f"allowedGiveback={int(threshold*100)}% | "
          f"threshold={round(peak*(1-threshold),3)}% | triggered={retrace_triggered}")
    return retrace_triggered

# ─── EMA(34) HLC3 EARLY WARNING ───────────────────────────────────────────────
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
        return True
    if sig == "sell" and last_h >= ema34:
        return True
    return False

# ─── EARLY SIGNAL ALERT ───────────────────────────────────────────────────────
def check_early_signal_alert(symbol, closes):
    if len(closes) < EMA_SLOW + 5:
        return
    fast    = calc_ema(closes, EMA_FAST)
    slow    = calc_ema(closes, EMA_SLOW)
    gap_pct = abs(fast - slow) / slow * 100
    if gap_pct > 0.15:
        early_signal_alerted.pop(symbol, None)
        return
    direction = "BUY 🟢" if fast > slow else "SELL 🔴"
    alert_key = f"{symbol}_{direction}"
    if early_signal_alerted.get(symbol) == alert_key:
        return
    early_signal_alerted[symbol] = alert_key
    current_pos = last_signal.get(symbol, "none")
    print(f"[EARLY SIGNAL] {symbol} EMAs converging — possible {direction} incoming")
    send_telegram(
        f"👀 <b>EARLY SIGNAL ALERT</b>\n"
        f"Symbol: {symbol}\n"
        f"Direction: {direction}\n"
        f"Price: ${round(closes[-1], 4)}\n"
        f"EMA gap: {round(gap_pct, 3)}%\n"
        f"Current position: {current_pos}\n"
        f"⚠️ Not yet confirmed — bot waiting for candle close",
        private=True
    )

# ═══════════════════════════════════════════════════════════════════════════════
# UPGRADE #1 — ADX FILTER (PAUSED)
# IMPORTANT: only filters NEW entries — never blocks exits
# ═══════════════════════════════════════════════════════════════════════════════
def calc_adx(highs, lows, closes, period=ADX_PERIOD):
    if len(closes) < period * 2:
        return 0
    try:
        tr_list, pdm_list, ndm_list = [], [], []
        for i in range(1, len(closes)):
            high, low, prev_close = highs[i], lows[i], closes[i-1]
            tr  = max(high - low, abs(high - prev_close), abs(low - prev_close))
            pdm = max(highs[i] - highs[i-1], 0) if (highs[i] - highs[i-1]) > (lows[i-1] - lows[i]) else 0
            ndm = max(lows[i-1] - lows[i], 0) if (lows[i-1] - lows[i]) > (highs[i] - highs[i-1]) else 0
            tr_list.append(tr)
            pdm_list.append(pdm)
            ndm_list.append(ndm)
        def wilder_smooth(data, p):
            s = sum(data[:p])
            result = [s]
            for v in data[p:]:
                s = s - s / p + v
                result.append(s)
            return result
        atr  = wilder_smooth(tr_list,  period)
        pDM  = wilder_smooth(pdm_list, period)
        nDM  = wilder_smooth(ndm_list, period)
        dx_list = []
        for i in range(len(atr)):
            if atr[i] == 0:
                continue
            pDI = 100 * pDM[i] / atr[i]
            nDI = 100 * nDM[i] / atr[i]
            dx  = 100 * abs(pDI - nDI) / (pDI + nDI) if (pDI + nDI) != 0 else 0
            dx_list.append(dx)
        if not dx_list:
            return 0
        return round(sum(dx_list[-period:]) / period, 2)
    except Exception as e:
        print(f"[ADX] Calc error: {e}")
        return 0

def adx_allows_new_entry(symbol, highs, lows, closes):
    """Only call this for NEW entries — never for exits"""
    if not ENABLE_ADX_FILTER:
        return True
    adx    = calc_adx(highs, lows, closes)
    passes = adx >= ADX_THRESHOLD
    print(f"[ADX] {symbol} ADX={adx} | threshold={ADX_THRESHOLD} | trending={passes}")
    if not passes:
        send_telegram(
            f"⏸️ <b>NEW ENTRY SKIPPED — ADX</b>\n"
            f"Symbol: {symbol}\n"
            f"ADX: {adx} (below {ADX_THRESHOLD})\n"
            f"Market ranging — existing position closed, no new entry",
            private=True
        )
    return passes

# ═══════════════════════════════════════════════════════════════════════════════
# UPGRADE #2 — TRADE LOGGING (PAUSED)
# ═══════════════════════════════════════════════════════════════════════════════
def init_log():
    if not ENABLE_TRADE_LOGGING:
        return
    try:
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp", "symbol", "action", "side",
                    "price", "qty", "peak_profit_pct",
                    "closed_pnl", "daily_pnl", "reason"
                ])
            print(f"[LOG] Trade log created: {LOG_FILE}")
    except Exception as e:
        print(f"[LOG] Init error: {e}")

def log_trade(symbol, action, side, price, qty=0, peak=0, closed_pnl=0, reason=""):
    if not ENABLE_TRADE_LOGGING:
        return
    try:
        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                symbol, action, side, price, qty,
                round(peak, 3), round(closed_pnl, 4),
                round(daily_pnl["pnl"], 4), reason
            ])
    except Exception as e:
        print(f"[LOG] Write error: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# UPGRADE #3 — HARD STOP LOSS (PAUSED)
# ═══════════════════════════════════════════════════════════════════════════════
def check_hard_stop(symbol):
    if not ENABLE_HARD_STOP_LOSS:
        return False
    if symbol not in entry_price or symbol not in last_signal:
        return False
    price = get_price(symbol)
    if not price:
        return False
    ep       = entry_price[symbol]
    sig      = last_signal[symbol]
    if ep == 0:
        return False
    loss_pct = (ep - price) / ep * 100 if sig == "buy" else (price - ep) / ep * 100
    if loss_pct >= STOP_LOSS_PCT:
        print(f"[STOP] {symbol} hard stop hit — loss={round(loss_pct,3)}%")
        send_telegram(
            f"🛑 <b>HARD STOP LOSS</b>\n"
            f"Symbol: {symbol}\n"
            f"Entry: ${ep} | Current: ${price}\n"
            f"Loss: {round(loss_pct, 2)}%\n"
            f"Closing position now",
            private=True
        )
        return True
    return False

# ═══════════════════════════════════════════════════════════════════════════════
# UPGRADE #4 — DUAL TIMEFRAME (PAUSED)
# IMPORTANT: only filters NEW entries — never blocks exits
# ═══════════════════════════════════════════════════════════════════════════════
def dual_tf_allows_new_entry(symbol, signal):
    """Only call this for NEW entries — never for exits"""
    if not ENABLE_DUAL_TIMEFRAME:
        return True
    try:
        _, _, closes_15m = get_candles(symbol, interval=DUAL_TF_INTERVAL)
        if len(closes_15m) < EMA_SLOW + 5:
            return True
        fast_15m = calc_ema(closes_15m, EMA_FAST)
        slow_15m = calc_ema(closes_15m, EMA_SLOW)
        agrees   = fast_15m > slow_15m if signal == "buy" else fast_15m < slow_15m
        print(f"[DUAL TF] {symbol} | 15m {round(fast_15m,4)}/{round(slow_15m,4)} | agrees={agrees}")
        if not agrees:
            send_telegram(
                f"⏸️ <b>NEW ENTRY SKIPPED — DUAL TF</b>\n"
                f"Symbol: {symbol}\n"
                f"60m signal: {signal.upper()}\n"
                f"15m EMA disagrees — existing position closed, no new entry",
                private=True
            )
        return agrees
    except Exception as e:
        print(f"[DUAL TF] Error {symbol}: {e}")
        return True

# ─── PARTIAL CLOSE ────────────────────────────────────────────────────────────
def partial_close(symbol, pct=PARTIAL_PCT):
    try:
        params  = {"category": "linear", "symbol": symbol}
        headers = sign_get(params)
        r = requests.get(f"{BASE_URL_PRIVATE}/v5/position/list",
            headers=headers, params=params, timeout=10)
        for pos in r.json().get("result", {}).get("list", []):
            size = float(pos.get("size", 0))
            if size > 0:
                precision  = get_qty_precision(symbol)
                close_qty  = round(size * pct, precision)
                if close_qty <= 0:
                    return
                close_side = "Sell" if pos["side"] == "Buy" else "Buy"
                body       = {
                    "category": "linear", "symbol": symbol,
                    "side": close_side, "orderType": "Market",
                    "qty": str(close_qty), "reduceOnly": True
                }
                headers2 = sign(body)
                r2 = requests.post(f"{BASE_URL_PRIVATE}/v5/order/create",
                    headers=headers2, json=body, timeout=10)
                print(f"[PARTIAL] {symbol} closed {int(pct*100)}% ({close_qty} of {size}) | {r2.json()}")
                time.sleep(1)
                update_daily_pnl(symbol)
                send_telegram(
                    f"⚠️ <b>EARLY WARNING — PARTIAL CLOSE</b>\n"
                    f"Symbol: {symbol}\n"
                    f"Closed: {int(pct*100)}% ({close_qty} of {size})\n"
                    f"HLC3 crossed EMA34 — possible reversal ahead\n"
                    f"Daily PnL: ${round(daily_pnl['pnl'], 2)}"
                )
    except Exception as e:
        print(f"[ERROR] Partial close failed {symbol}: {e}")
        traceback.print_exc()

# ─── SIGNAL ───────────────────────────────────────────────────────────────────
def check_signal(symbol):
    highs, lows, closes = get_candles(symbol)
    if len(closes) < EMA_SLOW + 5:
        print(f"[ERROR] Not enough candles for {symbol}")
        return None, None, None, None
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
    check_early_signal_alert(symbol, closes)
    buy_signal  = (fast_prev2 < slow_prev2 and fast_prev > slow_prev and fast_now > slow_now)
    sell_signal = (fast_prev2 > slow_prev2 and fast_prev < slow_prev and fast_now < slow_now)
    if buy_signal:
        return "buy", highs, lows, closes
    if sell_signal:
        return "sell", highs, lows, closes
    return None, highs, lows, closes

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
def close_position(symbol, reason="signal"):
    params  = {"category": "linear", "symbol": symbol}
    headers = sign_get(params)
    r = requests.get(f"{BASE_URL_PRIVATE}/v5/position/list",
        headers=headers, params=params, timeout=10)
    closed         = False
    close_qty      = 0
    close_side_str = ""
    for pos in r.json().get("result", {}).get("list", []):
        size = float(pos.get("size", 0))
        if size > 0:
            close_side     = "Sell" if pos["side"] == "Buy" else "Buy"
            close_side_str = close_side
            close_qty      = size
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
        pk = round(peak_profit.get(symbol, 0), 2)
        ep = entry_price.get(symbol, 0)
        log_trade(symbol, "EXIT", close_side_str, get_price(symbol) or 0,
                  close_qty, pk, daily_pnl["pnl"], reason)
        entry_price.pop(symbol, None)
        peak_profit.pop(symbol, None)
        early_warning_fired.discard(symbol)
        early_signal_alerted.pop(symbol, None)
        # Always send close alert regardless of mode or filters
        send_telegram(
            f"🔴 <b>POSITION CLOSED</b>\n"
            f"Symbol: {symbol}\n"
            f"Entry was: ${ep}\n"
            f"Peak profit: {pk}%\n"
            f"Reason: {reason}\n"
            f"Daily PnL: ${round(daily_pnl['pnl'], 2)}"
        )

# ─── PLACE ORDER ──────────────────────────────────────────────────────────────
def place_order(symbol, signal):
    try:
        set_leverage(symbol)
        price = get_price(symbol)
        if not price:
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
            early_warning_fired.discard(symbol)
            early_signal_alerted.pop(symbol, None)
            print(f"[ENTRY] {symbol} entry price set: {price}")
            log_trade(symbol, "ENTRY", side, price, qty, reason="EMA crossover")
            send_telegram(
                f"🟢 <b>NEW TRADE</b>\n"
                f"Symbol: {symbol}\n"
                f"Side: {side}\n"
                f"Entry: ${price}\n"
                f"Daily PnL: ${round(daily_pnl['pnl'], 2)}"
            )
        return result
    except Exception as e:
        print(f"[ERROR] Order failed {symbol}: {e}")
        traceback.print_exc()

# ─── SIGNAL ONLY ALERT ────────────────────────────────────────────────────────
def send_signal_only_alert(symbol, signal, price):
    side = "BUY 🟢" if signal == "buy" else "SELL 🔴"
    send_telegram(
        f"📡 <b>SIGNAL ALERT</b>\n"
        f"Symbol: {symbol}\n"
        f"Direction: {side}\n"
        f"Price: ${price}\n"
        f"⚠️ Monitoring mode — not trading"
    )

# ─── CLOSE ALL ────────────────────────────────────────────────────────────────
def close_all_positions():
    for symbol in SYMBOLS:
        lock = symbol_locks.get(symbol)
        with lock:
            close_position(symbol, reason="close all")
            last_signal.pop(symbol, None)
    print("[RISK] All positions closed")

# ─── REAL-TIME MONITOR ────────────────────────────────────────────────────────
# FIX: retrace and stop loss run in trading mode AND
# when switching from signal_only back to trading
# so open positions are always protected
def realtime_retrace_monitor():
    print("[MONITOR] Real-time monitor started — checking every 45s")
    while True:
        try:
            mode = get_mode()
            # Monitor runs in trading mode only
            # signal_only has no open positions so nothing to protect
            if mode == "trading" and not daily_pnl["stopped"]:
                for symbol in list(last_signal.keys()):
                    if is_symbol_paused(symbol):
                        continue
                    lock = symbol_locks.get(symbol)
                    if not lock:
                        continue
                    if not lock.acquire(blocking=False):
                        print(f"[MONITOR] {symbol} locked — skipping")
                        continue
                    try:
                        if check_hard_stop(symbol):
                            close_position(symbol, reason="hard stop loss")
                            last_signal.pop(symbol, None)
                            continue
                        if is_early_warning_on(symbol) and symbol not in early_warning_fired:
                            if check_early_warning(symbol):
                                partial_close(symbol, PARTIAL_PCT)
                                early_warning_fired.add(symbol)
                        if symbol in last_signal and check_peak_retrace(symbol):
                            send_telegram(
                                f"📉 <b>RETRACE EXIT</b>\n"
                                f"Symbol: {symbol}\n"
                                f"Peak profit: {round(peak_profit.get(symbol,0),2)}%\n"
                                f"Price retraced past threshold"
                            )
                            close_position(symbol, reason="retrace")
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
    print("GKC BOT — EMA REVERSAL SYSTEM")
    print(f"Timeframe: {INTERVAL}m | Min profit: {MIN_PROFIT_TO_TRACK}%")
    print("SYMBOLS:")
    for s, cfg in SYMBOL_CONFIG.items():
        status = "PAUSED" if cfg.get("paused") else f"${cfg['trade_usdt']} x {cfg['leverage']}x"
        print(f"  {s}: {status}")
    print("FEATURE FLAGS:")
    print(f"  ADX Filter:      {'ON' if ENABLE_ADX_FILTER     else 'OFF'}")
    print(f"  Trade Logging:   {'ON' if ENABLE_TRADE_LOGGING  else 'OFF'}")
    print(f"  Hard Stop Loss:  {'ON' if ENABLE_HARD_STOP_LOSS else 'OFF'}")
    print(f"  Dual Timeframe:  {'ON' if ENABLE_DUAL_TIMEFRAME else 'OFF'}")
    print(f"MODE: {BOT_MODE.upper()}")
    print("=" * 60)
    init_log()
    sync_state_from_bybit()
    while True:
        try:
            wait_for_candle_close()
            check_daily_reset()

            mode = get_mode()
            print(f"\n[SCAN] {time.strftime('%Y-%m-%d %H:%M:%S')} UTC | MODE: {mode.upper()}")
            bot_status["last_scan"] = time.strftime('%Y-%m-%d %H:%M:%S UTC')
            bot_status["mode"]      = mode
            print(f"[STATUS] PnL=${round(daily_pnl['pnl'],2)} | "
                  f"Trades={daily_pnl['trades']} | Stopped={daily_pnl['stopped']}")

            # ── FULLY PAUSED ──
            if mode == "paused":
                print("[PAUSED] Bot fully paused — no signals, no orders")
                continue

            if daily_pnl["stopped"]:
                print("[RISK] Daily stop active")
                continue

            if daily_pnl["pnl"] <= -MAX_DAILY_LOSS:
                daily_pnl["stopped"] = True
                print("[RISK] Max daily loss reached — closing all")
                send_telegram(
                    f"🚨 <b>DAILY LOSS LIMIT HIT</b>\n"
                    f"Loss: ${round(daily_pnl['pnl'],2)}\n"
                    f"Limit: -${MAX_DAILY_LOSS}\n"
                    f"All positions closing — bot stopped for today",
                    private=True
                )
                close_all_positions()
                continue

            sync_state_from_bybit()

            for symbol in SYMBOLS:
                if is_symbol_paused(symbol):
                    print(f"[PAUSED] {symbol} — skipping")
                    continue

                lock = symbol_locks.get(symbol)
                if not lock:
                    continue

                with lock:
                    # Retrace backup at candle close — trading mode only
                    if mode == "trading" and symbol in last_signal:
                        if check_peak_retrace(symbol):
                            print(f"[RETRACE] {symbol} — closing at candle")
                            close_position(symbol, reason="retrace at candle")
                            last_signal.pop(symbol, None)
                            continue

                    signal, highs, lows, closes = check_signal(symbol)
                    prev = last_signal.get(symbol)
                    print(f"[SIGNAL] {symbol} | current={signal} | previous={prev} | mode={mode}")

                    if signal and signal != prev:

                        if mode == "trading":
                            # ── FIX #1: Always close existing position on flip
                            # regardless of ADX or dual TF — exits are never filtered
                            if prev and symbol in entry_price:
                                print(f"[EXIT] {symbol} closing {prev} on signal flip")
                                close_position(symbol, reason="signal flip")
                                last_signal.pop(symbol, None)

                            # ── FIX #2: ADX only gates NEW entries, never exits
                            if highs and not adx_allows_new_entry(symbol, highs, lows, closes):
                                print(f"[ADX] {symbol} ranging — position closed, no new entry")
                                continue

                            # ── FIX #3: Dual TF only gates NEW entries, never exits
                            if not dual_tf_allows_new_entry(symbol, signal):
                                print(f"[DUAL TF] {symbol} disagrees — position closed, no new entry")
                                continue

                            print(f"[ORDER] {symbol} {signal.upper()} — placing order")
                            place_order(symbol, signal)
                            last_signal[symbol] = signal
                            bot_status["last_signal"] = last_signal.copy()

                        elif mode == "signal_only":
                            # No positions in signal_only so no exit needed
                            # Just alert community
                            price = get_price(symbol) or 0
                            print(f"[SIGNAL ONLY] {symbol} {signal.upper()} — alerting community")
                            send_signal_only_alert(symbol, signal, price)
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
        "mode":        get_mode(),
        "bot":         bot_status,
        "last_signal": last_signal,
        "entry_price": entry_price,
        "peak_profit": peak_profit,
        "daily_pnl":   daily_pnl,
        "symbols":     {s: {**cfg, "early_warning_fired": s in early_warning_fired}
                        for s, cfg in SYMBOL_CONFIG.items()},
        "interval":    f"{INTERVAL}m",
        "features": {
            "adx_filter":     ENABLE_ADX_FILTER,
            "trade_logging":  ENABLE_TRADE_LOGGING,
            "hard_stop_loss": ENABLE_HARD_STOP_LOSS,
            "dual_timeframe": ENABLE_DUAL_TIMEFRAME,
        }
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
                        "paused":         is_symbol_paused(symbol),
                    }
        return jsonify({
            "mode":           get_mode(),
            "open_positions": positions,
            "last_signal":    last_signal,
            "last_scan":      bot_status["last_scan"],
            "daily_pnl":      daily_pnl,
        })
    except Exception as e:
        return jsonify({"error": str(e)})

# ── FIX #4: switching to signal_only closes all open positions first
# so they're not left unprotected with no retrace monitoring
@app.route("/signalonly")
def mode_signal_only():
    mode = get_mode()
    if mode == "trading" and last_signal:
        print("[MODE] Closing all positions before switching to signal only")
        close_all_positions()
        send_telegram(
            f"📡 <b>SWITCHING TO SIGNAL ONLY</b>\n"
            f"All open positions closed first\n"
            f"Community alerts active — no new orders",
            private=True
        )
    else:
        send_telegram(
            f"📡 <b>BOT MODE: SIGNAL ONLY</b>\n"
            f"Community alerts active — no orders being placed",
            private=True
        )
    set_mode("signal_only")
    print("[MODE] Switched to SIGNAL ONLY")
    return jsonify({"mode": "signal_only", "message": "Signal only — alerts active, no orders"})

@app.route("/trading")
def mode_trading():
    set_mode("trading")
    print("[MODE] Switched to TRADING")
    send_telegram("⚡ <b>BOT MODE: TRADING</b>\nSignals + orders active", private=True)
    return jsonify({"mode": "trading", "message": "Bot now trading"})

@app.route("/pause")
def mode_pause():
    set_mode("paused")
    print("[MODE] Switched to PAUSED")
    send_telegram("⏸️ <b>BOT MODE: PAUSED</b>\nNo signals, no orders", private=True)
    return jsonify({"mode": "paused", "message": "Bot fully paused"})

@app.route("/pause/<symbol>")
def pause_symbol(symbol):
    sym = symbol.upper()
    if sym not in SYMBOL_CONFIG:
        return jsonify({"error": f"{sym} not found"})
    SYMBOL_CONFIG[sym]["paused"] = True
    return jsonify({"message": f"{sym} paused"})

@app.route("/resume/<symbol>")
def resume_symbol(symbol):
    sym = symbol.upper()
    if sym not in SYMBOL_CONFIG:
        return jsonify({"error": f"{sym} not found"})
    SYMBOL_CONFIG[sym]["paused"] = False
    return jsonify({"message": f"{sym} resumed"})

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
            "api_keys_set": bool(API_KEY and API_SECRET),
            "mode":         get_mode()
        })
    except Exception as e:
        return jsonify({"error": str(e)})

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
