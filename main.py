# ╔══════════════════════════════════════════════════════════════════╗
# ║           GKC BOT — VERSION 2.1                                 ║
# ║           Built by Hazak | Hazak Onchain | @cryptoedgelab       ║
# ║           Base strategy by GK                                   ║
# ╚══════════════════════════════════════════════════════════════════╝

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
PARTIAL_PCT    = 0.25

# ╔══════════════════════════════════════════════════════════════════╗
# ║  VERSION 2.1 — FEATURE FLAGS                                    ║
# ║  All OFF by default — enable one at a time to test              ║
# ╚══════════════════════════════════════════════════════════════════╝

# ── V1 upgrades (carried forward) ──
ENABLE_TRADE_LOGGING     = False  # Log all trades to CSV
ENABLE_HARD_STOP_LOSS    = True   # Close on leveraged loss threshold
ENABLE_DUAL_TIMEFRAME    = False  # 15m + 60m EMA agreement
ENABLE_LSMA_FILTER       = False  # Only trade with LSMA400 macro trend
ENABLE_VOLATILITY_FILTER = False  # Legacy percentage range filter (replaced by ATR)

# ── V2.1 upgrades ──
ENABLE_ATR_FILTER          = True  # ★★★★★ ATR-based volatility — replaces range filter
ENABLE_CONSECUTIVE_LOSS    = True  # ★★★★★ Auto-pause symbol after N losing trades
ENABLE_DYNAMIC_SIZING      = False  # ★★★★★ Adjust position size based on ATR volatility
ENABLE_MARKET_REGIME       = False  # ★★★★★ Detect trending vs ranging before entering
ENABLE_PROFIT_LOCKING      = True  # ★★★★★ Lock profit levels (breakeven → 5% → 12%)
ENABLE_VOLUME_FILTER       = False  # ★★★★☆ Require volume > EMA20 volume on crossover
ENABLE_TIME_FILTER         = False  # ★★★★☆ Avoid low-quality trading hours

# ── Feature settings ──
# Hard stop loss
STOP_LOSS_PCT           = 40.0   # leveraged loss % — 40% = 4% price move on 10x

# ATR filter
ATR_PERIOD              = 14     # standard ATR period
ATR_MIN_PCT             = 0.5    # skip entry if ATR < 0.5% of price (too quiet)

# Consecutive loss protection
MAX_CONSECUTIVE_LOSSES  = 3      # auto-pause symbol after 3 losses in a row
COOLDOWN_CANDLES        = 2      # wait 2 candles before re-enabling after cooldown

# Dynamic sizing tiers (ATR % of price)
DYNAMIC_SIZE_HIGH_VOL   = 0.5    # above this → half position
DYNAMIC_SIZE_MED_VOL    = 0.25   # above this → 75% position
# below both → full position

# Market regime
REGIME_LSMA_PERIOD      = 50     # LSMA period for slope calculation
REGIME_SLOPE_MIN        = 0.05   # minimum LSMA slope to confirm trend
REGIME_EMA_DIST_MIN     = 0.15   # minimum EMA 12/21 distance % to confirm trend

# Profit locking levels (peak_profit_pct, lock_at_pct)
PROFIT_LOCK_LEVELS      = [
    (8.0,  0.0),    # at 8% peak → move stop to breakeven
    (15.0, 5.0),    # at 15% peak → lock in 5% minimum profit
    (25.0, 12.0),   # at 25% peak → lock in 12% minimum profit
]

# Volume filter
VOLUME_EMA_PERIOD       = 20     # EMA period for volume baseline

# Time filter (UTC hours to avoid)
TIME_AVOID_HOURS        = [0, 1] # avoid 00:00–02:00 UTC (low liquidity)
TIME_AVOID_WEEKENDS     = False  # set True to skip Saturday/Sunday

# Dual timeframe
DUAL_TF_INTERVAL        = "15"

# LSMA macro
LSMA_PERIOD             = 400

# Trade log
LOG_FILE                = "/tmp/gkc_trades.csv"

# ─── BOT MODE ─────────────────────────────────────────────────────────────────
BOT_MODE = "trading"  # "trading" | "signal_only" | "paused"

# ─── STATE ────────────────────────────────────────────────────────────────────
last_signal          = {}
entry_price          = {}
peak_profit          = {}
locked_profit        = {}    # V2.1 — minimum profit % locked per symbol
bot_status           = {"last_scan": "never", "error": None, "version": "2.1"}
daily_pnl            = {"date": None, "pnl": 0.0, "trades": 0, "stopped": False}
processed_exec_ids   = set()
early_warning_fired  = set()
early_signal_alerted = {}

# V2.1 — per-symbol consecutive loss tracking
consecutive_losses   = {s: 0 for s in SYMBOL_CONFIG}
cooldown_candles     = {s: 0 for s in SYMBOL_CONFIG}

# V2.1 — performance tracking per symbol
performance = {
    s: {
        "trades": 0, "wins": 0, "losses": 0,
        "total_pnl": 0.0,
        "avg_win": 0.0, "avg_loss": 0.0,
        "largest_win": 0.0, "largest_loss": 0.0,
        "win_rate": 0.0,
        "consecutive_losses": 0,
        "on_cooldown": False,
    }
    for s in SYMBOL_CONFIG
}

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

# ─── CORE CALCULATIONS ────────────────────────────────────────────────────────
def calc_ema(prices, period):
    k   = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = p * k + ema * (1 - k)
    return ema

def calc_atr(highs, lows, closes, period=ATR_PERIOD):
    """Wilder's Average True Range"""
    if len(closes) < period + 1:
        return 0
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i]  - closes[i-1])
        )
        trs.append(tr)
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr

def calc_lsma(closes, period):
    """Linear Regression Moving Average — same as ta.linreg in Pine Script"""
    if len(closes) < period:
        return closes[-1]
    y      = closes[-period:]
    n      = period
    x_sum  = n * (n - 1) / 2
    x2_sum = n * (n - 1) * (2 * n - 1) / 6
    xy_sum = sum(i * y[i] for i in range(n))
    y_sum  = sum(y)
    slope  = (n * xy_sum - x_sum * y_sum) / (n * x2_sum - x_sum ** 2)
    intercept = (y_sum - slope * x_sum) / n
    return intercept + slope * (n - 1)

# ─── MARKET DATA ──────────────────────────────────────────────────────────────
def get_candles(symbol, interval=None):
    """Returns (highs, lows, closes, volumes)"""
    tf = interval or INTERVAL
    for attempt in range(3):
        try:
            r = requests.get(f"{BASE_URL_PUBLIC}/v5/market/kline", params={
                "category": "linear",
                "symbol":   symbol,
                "interval": tf,
                "limit":    500
            }, timeout=10)
            candles = r.json()["result"]["list"]
            candles.reverse()
            highs   = [float(c[2]) for c in candles]
            lows    = [float(c[3]) for c in candles]
            closes  = [float(c[4]) for c in candles]
            volumes = [float(c[5]) for c in candles]
            return highs, lows, closes, volumes
        except Exception as e:
            print(f"[ERROR] Candle fetch failed ({attempt+1}/3): {e}")
            time.sleep(2)
    return [], [], [], []

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

# ─── TIMING ───────────────────────────────────────────────────────────────────
def wait_for_candle_close():
    interval_seconds = int(INTERVAL) * 60
    now          = time.time()
    seconds_left = interval_seconds - (now % interval_seconds)
    sleep_time   = seconds_left + 2
    print(f"[WAIT] Sleeping {round(sleep_time/60, 2)} mins")
    time.sleep(sleep_time)

def check_daily_reset():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if daily_pnl["date"] != today:
        daily_pnl["date"]    = today
        daily_pnl["pnl"]     = 0.0
        daily_pnl["trades"]  = 0
        daily_pnl["stopped"] = False
        processed_exec_ids.clear()
        early_signal_alerted.clear()
        # Reset cooldowns daily
        for s in SYMBOLS:
            cooldown_candles[s] = 0
            consecutive_losses[s] = 0
            if SYMBOL_CONFIG[s].get("paused_by_loss"):
                SYMBOL_CONFIG[s]["paused_by_loss"] = False
                SYMBOL_CONFIG[s]["paused"] = False
                print(f"[RESET] {s} cooldown lifted — new day")
        print(f"[RESET] Daily reset for {today}")

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

# ─── SYNC ─────────────────────────────────────────────────────────────────────
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
                            locked_profit[symbol] = 0.0
                            print(f"[SYNC] Restored entry: {entry_price[symbol]}")
                        print(f"[SYNC] {symbol} → {side} size={size} "
                              f"entry={pos.get('avgPrice')} pnl={pos.get('unrealisedPnl')}")
                        found = True
                if not found:
                    last_signal.pop(symbol, None)
                    entry_price.pop(symbol, None)
                    peak_profit.pop(symbol, None)
                    locked_profit.pop(symbol, None)
                    early_warning_fired.discard(symbol)
                    print(f"[SYNC] {symbol} → no open position")
            except Exception as e:
                print(f"[ERROR] Sync failed {symbol}: {e}")
                traceback.print_exc()
    print(f"[SYNC] State: {last_signal}")

# ─── PERFORMANCE TRACKING ─────────────────────────────────────────────────────
def record_trade_result(symbol, pnl):
    """V2.1 — update per-symbol performance stats after every close"""
    p = performance[symbol]
    p["trades"] += 1
    p["total_pnl"] = round(p["total_pnl"] + pnl, 4)
    if pnl > 0:
        p["wins"] += 1
        p["largest_win"] = round(max(p["largest_win"], pnl), 4)
        total_win = p["avg_win"] * (p["wins"] - 1) + pnl
        p["avg_win"] = round(total_win / p["wins"], 4)
        consecutive_losses[symbol] = 0
        p["consecutive_losses"] = 0
    else:
        p["losses"] += 1
        p["largest_loss"] = round(min(p["largest_loss"], pnl), 4)
        total_loss = p["avg_loss"] * (p["losses"] - 1) + pnl
        p["avg_loss"] = round(total_loss / p["losses"], 4)
        consecutive_losses[symbol] += 1
        p["consecutive_losses"] = consecutive_losses[symbol]
    p["win_rate"] = round(p["wins"] / p["trades"] * 100, 1) if p["trades"] > 0 else 0

# ─── TIERED RETRACE ───────────────────────────────────────────────────────────
def get_retrace_threshold(peak):
    if peak >= 20:   return 0.35
    elif peak >= 10: return 0.50
    elif peak >= 5:  return 0.60
    else:            return 0.70

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
    current_pct = (price - ep) / ep * 100 if sig == "buy" else (ep - price) / ep * 100
    if current_pct > peak_profit.get(symbol, 0):
        peak_profit[symbol] = current_pct
        print(f"[PEAK] {symbol} new peak: {round(current_pct, 3)}%")

        # V2.1 — update profit lock when new peak hit
        if ENABLE_PROFIT_LOCKING:
            update_profit_lock(symbol, current_pct)

    peak = peak_profit.get(symbol, 0)
    if peak < MIN_PROFIT_TO_TRACK:
        return False

    # V2.1 — check locked profit floor first
    lock_floor = locked_profit.get(symbol, 0)
    if lock_floor > 0 and current_pct <= lock_floor:
        print(f"[LOCK] {symbol} dropped below locked profit {lock_floor}% — closing")
        return True

    threshold         = get_retrace_threshold(peak)
    retrace_triggered = current_pct <= peak * (1 - threshold)
    print(f"[RETRACE] {symbol} | entry={ep} price={price} | "
          f"current={round(current_pct,3)}% | peak={round(peak,3)}% | "
          f"lock_floor={lock_floor}% | triggered={retrace_triggered}")
    return retrace_triggered

# ═══════════════════════════════════════════════════════════════════════════════
# V2.1 UPGRADE #1 — ATR VOLATILITY FILTER
# More professional than percentage range — accounts for gaps
# Enable: ENABLE_ATR_FILTER = True
# ═══════════════════════════════════════════════════════════════════════════════
def atr_filter_passes(symbol, highs, lows, closes):
    """Skip entry if ATR is too low — market not moving enough"""
    if not ENABLE_ATR_FILTER:
        return True
    atr     = calc_atr(highs, lows, closes)
    atr_pct = (atr / closes[-1]) * 100
    passes  = atr_pct >= ATR_MIN_PCT
    print(f"[ATR] {symbol} ATR={round(atr,4)} ({round(atr_pct,3)}%) | "
          f"min={ATR_MIN_PCT}% | passes={passes}")
    if not passes:
        send_telegram(
            f"⏸️ <b>ENTRY SKIPPED — LOW ATR</b>\n"
            f"Symbol: {symbol}\n"
            f"ATR: {round(atr_pct, 3)}% (min {ATR_MIN_PCT}%)\n"
            f"Market too quiet — waiting for volatility",
            private=True
        )
    return passes

# ═══════════════════════════════════════════════════════════════════════════════
# V2.1 UPGRADE #2 — CONSECUTIVE LOSS PROTECTION
# Auto-pause symbol after N losing trades — resume next candle after cooldown
# Enable: ENABLE_CONSECUTIVE_LOSS = True
# ═══════════════════════════════════════════════════════════════════════════════
def check_consecutive_loss_limit(symbol):
    """Returns True if symbol should be skipped due to consecutive losses"""
    if not ENABLE_CONSECUTIVE_LOSS:
        return False

    # Count down cooldown
    if cooldown_candles.get(symbol, 0) > 0:
        cooldown_candles[symbol] -= 1
        remaining = cooldown_candles[symbol]
        print(f"[COOLDOWN] {symbol} — {remaining} candles remaining")
        if remaining == 0:
            SYMBOL_CONFIG[symbol]["paused_by_loss"] = False
            print(f"[COOLDOWN] {symbol} cooldown ended — resuming")
            send_telegram(
                f"✅ <b>SYMBOL RESUMED</b>\n"
                f"Symbol: {symbol}\n"
                f"Cooldown complete — trading resumed",
                private=True
            )
        return True  # still in cooldown this candle

    # Check if we've hit the limit
    losses = consecutive_losses.get(symbol, 0)
    if losses >= MAX_CONSECUTIVE_LOSSES:
        cooldown_candles[symbol] = COOLDOWN_CANDLES
        SYMBOL_CONFIG[symbol]["paused_by_loss"] = True
        consecutive_losses[symbol] = 0
        print(f"[COOLDOWN] {symbol} — {MAX_CONSECUTIVE_LOSSES} consecutive losses — "
              f"cooling down for {COOLDOWN_CANDLES} candles")
        send_telegram(
            f"⏸️ <b>SYMBOL ON COOLDOWN</b>\n"
            f"Symbol: {symbol}\n"
            f"Reason: {MAX_CONSECUTIVE_LOSSES} consecutive losing trades\n"
            f"Cooling down for {COOLDOWN_CANDLES} candles\n"
            f"Will resume automatically",
            private=True
        )
        return True

    return False

# ═══════════════════════════════════════════════════════════════════════════════
# V2.1 UPGRADE #3 — DYNAMIC POSITION SIZING
# Smaller size in high volatility, larger in low volatility
# Enable: ENABLE_DYNAMIC_SIZING = True
# ═══════════════════════════════════════════════════════════════════════════════
def get_dynamic_trade_usdt(symbol, highs, lows, closes):
    """Adjust position size based on current ATR volatility"""
    base = get_trade_usdt(symbol)
    if not ENABLE_DYNAMIC_SIZING:
        return base
    atr     = calc_atr(highs, lows, closes)
    atr_pct = (atr / closes[-1]) * 100
    if atr_pct > DYNAMIC_SIZE_HIGH_VOL:
        size = round(base * 0.5, 2)
        tier = "HIGH VOL"
    elif atr_pct > DYNAMIC_SIZE_MED_VOL:
        size = round(base * 0.75, 2)
        tier = "MED VOL"
    else:
        size = base
        tier = "LOW VOL"
    print(f"[DYNAMIC SIZE] {symbol} ATR={round(atr_pct,3)}% | "
          f"{tier} | size=${size} (base=${base})")
    return size

# ═══════════════════════════════════════════════════════════════════════════════
# V2.1 UPGRADE #4 — MARKET REGIME DETECTION
# LSMA slope + EMA distance — no ADX needed
# Enable: ENABLE_MARKET_REGIME = True
# ═══════════════════════════════════════════════════════════════════════════════
def market_is_trending(symbol, highs, lows, closes):
    """Returns True if market is trending — safe to trade"""
    if not ENABLE_MARKET_REGIME:
        return True
    if len(closes) < REGIME_LSMA_PERIOD + 10:
        return True

    # LSMA slope — is the trend actually going somewhere?
    lsma_now  = calc_lsma(closes, REGIME_LSMA_PERIOD)
    lsma_prev = calc_lsma(closes[:-5], REGIME_LSMA_PERIOD)
    slope     = abs(lsma_now - lsma_prev) / closes[-1] * 100

    # EMA distance — are fast and slow EMAs separated enough?
    fast     = calc_ema(closes, EMA_FAST)
    slow     = calc_ema(closes, EMA_SLOW)
    ema_dist = abs(fast - slow) / slow * 100

    trending = slope >= REGIME_SLOPE_MIN and ema_dist >= REGIME_EMA_DIST_MIN
    print(f"[REGIME] {symbol} | slope={round(slope,4)}% | "
          f"ema_dist={round(ema_dist,4)}% | trending={trending}")

    if not trending:
        send_telegram(
            f"⏸️ <b>ENTRY SKIPPED — RANGING MARKET</b>\n"
            f"Symbol: {symbol}\n"
            f"LSMA slope: {round(slope,4)}% (min {REGIME_SLOPE_MIN}%)\n"
            f"EMA distance: {round(ema_dist,4)}% (min {REGIME_EMA_DIST_MIN}%)\n"
            f"Market ranging — no entry",
            private=True
        )
    return trending

# ═══════════════════════════════════════════════════════════════════════════════
# V2.1 UPGRADE #5 — PROFIT LOCKING
# Move stop to breakeven at +8%, lock profit at +15% and +25%
# Enable: ENABLE_PROFIT_LOCKING = True
# ═══════════════════════════════════════════════════════════════════════════════
def update_profit_lock(symbol, current_peak):
    """Update the minimum locked profit floor as trade moves in our favour"""
    if not ENABLE_PROFIT_LOCKING:
        return
    current_lock = locked_profit.get(symbol, 0)
    for peak_threshold, lock_at in sorted(PROFIT_LOCK_LEVELS, reverse=True):
        if current_peak >= peak_threshold and lock_at > current_lock:
            locked_profit[symbol] = lock_at
            label = "BREAKEVEN" if lock_at == 0 else f"+{lock_at}%"
            print(f"[LOCK] {symbol} peak={round(current_peak,2)}% — "
                  f"locking profit floor at {label}")
            send_telegram(
                f"🔒 <b>PROFIT LOCKED</b>\n"
                f"Symbol: {symbol}\n"
                f"Peak profit: {round(current_peak,2)}%\n"
                f"Floor set at: {label}\n"
                f"Position won't close below this level",
                private=True
            )
            break

# ═══════════════════════════════════════════════════════════════════════════════
# V2.1 UPGRADE #6 — VOLUME CONFIRMATION
# Only trade when volume is above its EMA — weak crosses ignored
# Enable: ENABLE_VOLUME_FILTER = True
# ═══════════════════════════════════════════════════════════════════════════════
def volume_confirms(symbol, volumes):
    """Returns True if current volume is above volume EMA — strong move"""
    if not ENABLE_VOLUME_FILTER:
        return True
    if len(volumes) < VOLUME_EMA_PERIOD:
        return True
    vol_ema     = calc_ema(volumes, VOLUME_EMA_PERIOD)
    current_vol = volumes[-1]
    passes      = current_vol > vol_ema
    print(f"[VOLUME] {symbol} vol={round(current_vol,2)} | "
          f"ema={round(vol_ema,2)} | confirms={passes}")
    if not passes:
        send_telegram(
            f"⏸️ <b>ENTRY SKIPPED — LOW VOLUME</b>\n"
            f"Symbol: {symbol}\n"
            f"Volume: {round(current_vol,2)}\n"
            f"Volume EMA: {round(vol_ema,2)}\n"
            f"Weak crossover — no entry",
            private=True
        )
    return passes

# ═══════════════════════════════════════════════════════════════════════════════
# V2.1 UPGRADE #7 — TIME FILTER
# Avoid low-quality trading hours and optionally weekends
# Enable: ENABLE_TIME_FILTER = True
# ═══════════════════════════════════════════════════════════════════════════════
def time_allows_entry(symbol):
    """Returns True if current time is suitable for trading"""
    if not ENABLE_TIME_FILTER:
        return True
    now = datetime.now(timezone.utc)
    if now.hour in TIME_AVOID_HOURS:
        print(f"[TIME] {symbol} — blocked hour {now.hour}:00 UTC")
        send_telegram(
            f"⏸️ <b>ENTRY SKIPPED — TIME FILTER</b>\n"
            f"Symbol: {symbol}\n"
            f"Hour: {now.hour}:00 UTC (low liquidity window)",
            private=True
        )
        return False
    if TIME_AVOID_WEEKENDS and now.weekday() >= 5:
        print(f"[TIME] {symbol} — weekend trading disabled")
        return False
    return True

# ─── V1 FILTERS (CARRIED FORWARD) ────────────────────────────────────────────
def check_early_warning(symbol):
    if symbol not in last_signal:
        return False
    highs, lows, closes, _ = get_candles(symbol)
    if len(closes) < EMA_WARN + 5:
        return False
    hlc3   = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)]
    ema34  = calc_ema(hlc3, EMA_WARN)
    last_h = hlc3[-1]
    sig    = last_signal[symbol]
    if sig == "buy"  and last_h < ema34:  return True
    if sig == "sell" and last_h >= ema34: return True
    return False

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
    send_telegram(
        f"👀 <b>EARLY SIGNAL ALERT</b>\n"
        f"Symbol: {symbol}\n"
        f"Direction: {direction}\n"
        f"Price: ${round(closes[-1], 4)}\n"
        f"EMA gap: {round(gap_pct, 3)}%\n"
        f"Current position: {last_signal.get(symbol, 'none')}\n"
        f"⚠️ Not confirmed — bot waiting for candle close",
        private=True
    )

def volatility_filter_passes(symbol, closes):
    if not ENABLE_VOLATILITY_FILTER:
        return True
    recent    = closes[-5:]
    range_pct = (max(recent) - min(recent)) / min(recent) * 100
    passes    = range_pct >= 0.8
    if not passes:
        send_telegram(
            f"⏸️ <b>ENTRY SKIPPED — LOW VOLATILITY</b>\n"
            f"Symbol: {symbol} | Range: {round(range_pct,3)}%",
            private=True
        )
    return passes

def dual_tf_allows_new_entry(symbol, signal):
    if not ENABLE_DUAL_TIMEFRAME:
        return True
    try:
        _, _, closes_15m, _ = get_candles(symbol, interval=DUAL_TF_INTERVAL)
        if len(closes_15m) < EMA_SLOW + 5:
            return True
        fast_15m = calc_ema(closes_15m, EMA_FAST)
        slow_15m = calc_ema(closes_15m, EMA_SLOW)
        agrees   = fast_15m > slow_15m if signal == "buy" else fast_15m < slow_15m
        if not agrees:
            send_telegram(
                f"⏸️ <b>ENTRY SKIPPED — DUAL TF</b>\n"
                f"Symbol: {symbol} | 60m: {signal.upper()} | 15m disagrees",
                private=True
            )
        return agrees
    except Exception as e:
        print(f"[DUAL TF] Error {symbol}: {e}")
        return True

def lsma_macro_confirms(symbol, closes, signal):
    if not ENABLE_LSMA_FILTER:
        return True
    if len(closes) < LSMA_PERIOD:
        return True
    lsma400  = calc_lsma(closes, LSMA_PERIOD)
    price    = closes[-1]
    above    = price > lsma400
    confirms = (signal == "buy" and above) or (signal == "sell" and not above)
    print(f"[LSMA] {symbol} | price={round(price,4)} | "
          f"LSMA400={round(lsma400,4)} | above={above} | confirms={confirms}")
    if not confirms:
        send_telegram(
            f"⏸️ <b>ENTRY SKIPPED — LSMA MACRO</b>\n"
            f"Symbol: {symbol}\n"
            f"{'Price below LSMA400 — no longs' if signal == 'buy' else 'Price above LSMA400 — no shorts'}",
            private=True
        )
    return confirms

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
    lev      = get_leverage(symbol)
    if ep == 0:
        return False
    loss_pct = ((ep - price) / ep * 100 * lev) if sig == "buy" else ((price - ep) / ep * 100 * lev)
    if loss_pct >= STOP_LOSS_PCT:
        raw_loss = round(loss_pct / lev, 2)
        print(f"[STOP] {symbol} hard stop — loss={round(loss_pct,2)}% leveraged")
        send_telegram(
            f"🛑 <b>HARD STOP LOSS</b>\n"
            f"Symbol: {symbol}\n"
            f"Entry: ${ep} | Current: ${price}\n"
            f"Price move: -{raw_loss}%\n"
            f"Leveraged loss ({lev}x): -{round(loss_pct,2)}%\n"
            f"Closing now",
            private=True
        )
        return True
    return False

# ─── TRADE LOGGING ────────────────────────────────────────────────────────────
def init_log():
    if not ENABLE_TRADE_LOGGING:
        return
    try:
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp", "symbol", "action", "side",
                    "price", "qty", "peak_profit_pct", "locked_profit_pct",
                    "closed_pnl", "daily_pnl", "reason"
                ])
            print(f"[LOG] Trade log created: {LOG_FILE}")
    except Exception as e:
        print(f"[LOG] Init error: {e}")

def log_trade(symbol, action, side, price, qty=0, peak=0, locked=0, closed_pnl=0, reason=""):
    if not ENABLE_TRADE_LOGGING:
        return
    try:
        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                symbol, action, side, price, qty,
                round(peak, 3), round(locked, 3),
                round(closed_pnl, 4), round(daily_pnl["pnl"], 4), reason
            ])
    except Exception as e:
        print(f"[LOG] Write error: {e}")

# ─── SIGNAL ───────────────────────────────────────────────────────────────────
def check_signal(symbol):
    highs, lows, closes, volumes = get_candles(symbol)
    if len(closes) < EMA_SLOW + 5:
        print(f"[ERROR] Not enough candles for {symbol}")
        return None, None, None, None, None
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
        return "buy",  highs, lows, closes, volumes
    if sell_signal:
        return "sell", highs, lows, closes, volumes
    return None, highs, lows, closes, volumes

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
                print(f"[PARTIAL] {symbol} closed {int(pct*100)}% ({close_qty}) | {r2.json()}")
                time.sleep(1)
                update_daily_pnl(symbol)
                send_telegram(
                    f"⚠️ <b>EARLY WARNING — PARTIAL CLOSE</b>\n"
                    f"Symbol: {symbol}\n"
                    f"Closed: {int(pct*100)}% ({close_qty} of {size})\n"
                    f"HLC3 crossed EMA34 — possible reversal\n"
                    f"Daily PnL: ${round(daily_pnl['pnl'], 2)}"
                )
    except Exception as e:
        print(f"[ERROR] Partial close failed {symbol}: {e}")
        traceback.print_exc()

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
        pnl_before = daily_pnl["pnl"]
        update_daily_pnl(symbol)
        trade_pnl  = round(daily_pnl["pnl"] - pnl_before, 4)
        pk         = round(peak_profit.get(symbol, 0), 2)
        lk         = round(locked_profit.get(symbol, 0), 2)
        ep         = entry_price.get(symbol, 0)

        # V2.1 — record result for performance tracking and consecutive loss
        record_trade_result(symbol, trade_pnl)
        log_trade(symbol, "EXIT", close_side_str, get_price(symbol) or 0,
                  close_qty, pk, lk, trade_pnl, reason)

        entry_price.pop(symbol, None)
        peak_profit.pop(symbol, None)
        locked_profit.pop(symbol, None)
        early_warning_fired.discard(symbol)
        early_signal_alerted.pop(symbol, None)

        send_telegram(
            f"🔴 <b>POSITION CLOSED</b>\n"
            f"Symbol: {symbol}\n"
            f"Entry: ${ep}\n"
            f"Peak profit: {pk}%\n"
            f"Reason: {reason}\n"
            f"Daily PnL: ${round(daily_pnl['pnl'], 2)}"
        )

# ─── PLACE ORDER ──────────────────────────────────────────────────────────────
def place_order(symbol, signal, highs=None, lows=None, closes=None):
    try:
        set_leverage(symbol)
        price = get_price(symbol)
        if not price:
            return
        precision = get_qty_precision(symbol)

        # V2.1 — dynamic sizing if enabled
        trade_usdt = get_dynamic_trade_usdt(symbol, highs, lows, closes) \
                     if (ENABLE_DYNAMIC_SIZING and highs) else get_trade_usdt(symbol)

        qty  = round((trade_usdt * get_leverage(symbol)) / price, precision)
        side = "Buy" if signal == "buy" else "Sell"
        body = {"category": "linear", "symbol": symbol,
                "side": side, "orderType": "Market",
                "qty": str(qty), "timeInForce": "GTC"}
        headers = sign(body)
        r = requests.post(f"{BASE_URL_PRIVATE}/v5/order/create",
            headers=headers, json=body, timeout=10)
        result = r.json()
        print(f"[ORDER] {symbol} {side} qty={qty} @ {price} | {result}")
        if result.get("retCode") == 0:
            entry_price[symbol]  = price
            peak_profit[symbol]  = 0.0
            locked_profit[symbol] = 0.0
            early_warning_fired.discard(symbol)
            early_signal_alerted.pop(symbol, None)
            print(f"[ENTRY] {symbol} entry set: {price}")
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

def send_signal_only_alert(symbol, signal, price):
    side = "BUY 🟢" if signal == "buy" else "SELL 🔴"
    send_telegram(
        f"📡 <b>SIGNAL ALERT</b>\n"
        f"Symbol: {symbol}\n"
        f"Direction: {side}\n"
        f"Price: ${price}\n"
        f"⚠️ Monitoring mode — not trading"
    )

def close_all_positions():
    for symbol in SYMBOLS:
        lock = symbol_locks.get(symbol)
        with lock:
            close_position(symbol, reason="close all")
            last_signal.pop(symbol, None)
    print("[RISK] All positions closed")

# ─── REAL-TIME MONITOR ────────────────────────────────────────────────────────
def realtime_retrace_monitor():
    print("[MONITOR] Real-time monitor started — checking every 45s")
    while True:
        try:
            mode = get_mode()
            if mode == "trading" and not daily_pnl["stopped"]:
                for symbol in list(last_signal.keys()):
                    if is_symbol_paused(symbol):
                        continue
                    lock = symbol_locks.get(symbol)
                    if not lock:
                        continue
                    if not lock.acquire(blocking=False):
                        continue
                    try:
                        # Hard stop loss
                        if check_hard_stop(symbol):
                            close_position(symbol, reason="hard stop loss")
                            last_signal.pop(symbol, None)
                            continue
                        # Early warning partial close
                        if is_early_warning_on(symbol) and symbol not in early_warning_fired:
                            if check_early_warning(symbol):
                                partial_close(symbol, PARTIAL_PCT)
                                early_warning_fired.add(symbol)
                        # Peak retrace / profit lock
                        if symbol in last_signal and check_peak_retrace(symbol):
                            send_telegram(
                                f"📉 <b>RETRACE EXIT</b>\n"
                                f"Symbol: {symbol}\n"
                                f"Peak: {round(peak_profit.get(symbol,0),2)}%\n"
                                f"Locked floor: {round(locked_profit.get(symbol,0),2)}%"
                            )
                            close_position(symbol, reason="retrace")
                            last_signal.pop(symbol, None)
                    finally:
                        lock.release()
        except Exception as e:
            print(f"[ERROR] Monitor: {e}")
            traceback.print_exc()
        time.sleep(45)

# ─── BOT LOOP ─────────────────────────────────────────────────────────────────
def run_bot():
    print("=" * 62)
    print("  GKC BOT — VERSION 2.1")
    print("  Built by Hazak | @cryptoedgelab")
    print(f"  Timeframe: {INTERVAL}m | Min profit: {MIN_PROFIT_TO_TRACK}%")
    print("  SYMBOLS:")
    for s, cfg in SYMBOL_CONFIG.items():
        status = "PAUSED" if cfg.get("paused") else f"${cfg['trade_usdt']} x {cfg['leverage']}x"
        print(f"    {s}: {status}")
    print("  V1 FLAGS:")
    print(f"    Trade Logging:      {'ON' if ENABLE_TRADE_LOGGING     else 'OFF'}")
    print(f"    Hard Stop Loss:     {'ON' if ENABLE_HARD_STOP_LOSS    else 'OFF'} ({STOP_LOSS_PCT}% lev)")
    print(f"    Dual Timeframe:     {'ON' if ENABLE_DUAL_TIMEFRAME    else 'OFF'}")
    print(f"    LSMA400 Filter:     {'ON' if ENABLE_LSMA_FILTER       else 'OFF'}")
    print(f"    Volatility Filter:  {'ON' if ENABLE_VOLATILITY_FILTER else 'OFF'}")
    print("  V2.1 FLAGS:")
    print(f"    ATR Filter:         {'ON' if ENABLE_ATR_FILTER        else 'OFF'}")
    print(f"    Consecutive Loss:   {'ON' if ENABLE_CONSECUTIVE_LOSS  else 'OFF'}")
    print(f"    Dynamic Sizing:     {'ON' if ENABLE_DYNAMIC_SIZING    else 'OFF'}")
    print(f"    Market Regime:      {'ON' if ENABLE_MARKET_REGIME     else 'OFF'}")
    print(f"    Profit Locking:     {'ON' if ENABLE_PROFIT_LOCKING    else 'OFF'}")
    print(f"    Volume Filter:      {'ON' if ENABLE_VOLUME_FILTER     else 'OFF'}")
    print(f"    Time Filter:        {'ON' if ENABLE_TIME_FILTER       else 'OFF'}")
    print("=" * 62)
    init_log()
    sync_state_from_bybit()

    while True:
        try:
            wait_for_candle_close()
            check_daily_reset()

            mode = get_mode()
            print(f"\n[SCAN] {time.strftime('%Y-%m-%d %H:%M:%S')} UTC | MODE: {mode.upper()} | V2.1")
            bot_status["last_scan"] = time.strftime('%Y-%m-%d %H:%M:%S UTC')
            bot_status["mode"]      = mode
            print(f"[STATUS] PnL=${round(daily_pnl['pnl'],2)} | "
                  f"Trades={daily_pnl['trades']} | Stopped={daily_pnl['stopped']}")

            if mode == "paused":
                print("[PAUSED] Bot fully paused")
                continue

            if daily_pnl["stopped"]:
                print("[RISK] Daily stop active")
                continue

            if daily_pnl["pnl"] <= -MAX_DAILY_LOSS:
                daily_pnl["stopped"] = True
                print("[RISK] Max daily loss reached")
                send_telegram(
                    f"🚨 <b>DAILY LOSS LIMIT HIT</b>\n"
                    f"Loss: ${round(daily_pnl['pnl'],2)}\n"
                    f"Limit: -${MAX_DAILY_LOSS}\n"
                    f"Closing all — bot stopped for today",
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
                    # V2.1 — consecutive loss cooldown check
                    if check_consecutive_loss_limit(symbol):
                        print(f"[COOLDOWN] {symbol} — on cooldown, skipping")
                        continue

                    # Retrace backup at candle close
                    if mode == "trading" and symbol in last_signal:
                        if check_peak_retrace(symbol):
                            print(f"[RETRACE] {symbol} — closing at candle")
                            close_position(symbol, reason="retrace at candle")
                            last_signal.pop(symbol, None)
                            continue

                    # Signal check
                    signal, highs, lows, closes, volumes = check_signal(symbol)
                    prev = last_signal.get(symbol)
                    print(f"[SIGNAL] {symbol} | current={signal} | previous={prev} | mode={mode}")

                    if signal and signal != prev:

                        if mode == "trading":
                            # Always close existing position on flip — no filter
                            if prev and symbol in entry_price:
                                print(f"[EXIT] {symbol} closing {prev} on signal flip")
                                close_position(symbol, reason="signal flip")
                                last_signal.pop(symbol, None)

                            # ── Entry filters — NEW entries only ──

                            # Time filter
                            if not time_allows_entry(symbol):
                                print(f"[TIME] {symbol} — bad hour, no new entry")
                                continue

                            # ATR filter (V2.1)
                            if highs and not atr_filter_passes(symbol, highs, lows, closes):
                                print(f"[ATR] {symbol} too quiet — no new entry")
                                continue

                            # Volatility filter (V1 legacy)
                            if closes and not volatility_filter_passes(symbol, closes):
                                print(f"[VOL] {symbol} flat — no new entry")
                                continue

                            # Market regime (V2.1)
                            if highs and not market_is_trending(symbol, highs, lows, closes):
                                print(f"[REGIME] {symbol} ranging — no new entry")
                                continue

                            # Volume confirmation (V2.1)
                            if volumes and not volume_confirms(symbol, volumes):
                                print(f"[VOLUME] {symbol} weak — no new entry")
                                continue

                            # LSMA macro (V1)
                            if closes and not lsma_macro_confirms(symbol, closes, signal):
                                print(f"[LSMA] {symbol} macro disagrees — no new entry")
                                continue

                            # Dual timeframe (V1)
                            if not dual_tf_allows_new_entry(symbol, signal):
                                print(f"[DUAL TF] {symbol} 15m disagrees — no new entry")
                                continue

                            print(f"[ORDER] {symbol} {signal.upper()} — all filters passed")
                            place_order(symbol, signal, highs, lows, closes)
                            last_signal[symbol] = signal
                            bot_status["last_signal"] = last_signal.copy()

                        elif mode == "signal_only":
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
        "version":     "2.1",
        "status":      "running",
        "mode":        get_mode(),
        "bot":         bot_status,
        "last_signal": last_signal,
        "entry_price": entry_price,
        "peak_profit": peak_profit,
        "locked_profit": locked_profit,
        "daily_pnl":   daily_pnl,
        "symbols":     {s: {**cfg, "early_warning_fired": s in early_warning_fired,
                            "consecutive_losses": consecutive_losses.get(s, 0),
                            "on_cooldown": cooldown_candles.get(s, 0) > 0}
                        for s, cfg in SYMBOL_CONFIG.items()},
        "interval":    f"{INTERVAL}m",
        "v1_features": {
            "trade_logging":     ENABLE_TRADE_LOGGING,
            "hard_stop_loss":    ENABLE_HARD_STOP_LOSS,
            "dual_timeframe":    ENABLE_DUAL_TIMEFRAME,
            "lsma_filter":       ENABLE_LSMA_FILTER,
            "volatility_filter": ENABLE_VOLATILITY_FILTER,
        },
        "v21_features": {
            "atr_filter":        ENABLE_ATR_FILTER,
            "consecutive_loss":  ENABLE_CONSECUTIVE_LOSS,
            "dynamic_sizing":    ENABLE_DYNAMIC_SIZING,
            "market_regime":     ENABLE_MARKET_REGIME,
            "profit_locking":    ENABLE_PROFIT_LOCKING,
            "volume_filter":     ENABLE_VOLUME_FILTER,
            "time_filter":       ENABLE_TIME_FILTER,
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
                        "locked_profit":  round(locked_profit.get(symbol, 0), 3),
                        "paused":         is_symbol_paused(symbol),
                    }
        return jsonify({
            "version":        "2.1",
            "mode":           get_mode(),
            "open_positions": positions,
            "last_signal":    last_signal,
            "last_scan":      bot_status["last_scan"],
            "daily_pnl":      daily_pnl,
        })
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/performance")
def perf():
    """V2.1 — per-symbol win rate dashboard"""
    return jsonify({
        "version":     "2.1",
        "performance": performance,
        "daily_pnl":   daily_pnl,
        "mode":        get_mode(),
    })

@app.route("/trading")
def mode_trading():
    set_mode("trading")
    send_telegram("⚡ <b>BOT MODE: TRADING</b>\nSignals + orders active", private=True)
    return jsonify({"mode": "trading"})

@app.route("/signalonly")
def mode_signal_only():
    mode = get_mode()
    if mode == "trading" and last_signal:
        close_all_positions()
        send_telegram("📡 <b>SWITCHING TO SIGNAL ONLY</b>\nAll positions closed first", private=True)
    else:
        send_telegram("📡 <b>BOT MODE: SIGNAL ONLY</b>\nAlerts active, no orders", private=True)
    set_mode("signal_only")
    return jsonify({"mode": "signal_only"})

@app.route("/pause")
def mode_pause():
    set_mode("paused")
    send_telegram("⏸️ <b>BOT MODE: PAUSED</b>\nNo signals, no orders", private=True)
    return jsonify({"mode": "paused"})

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
    cooldown_candles[sym] = 0
    consecutive_losses[sym] = 0
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
            "version":      "2.1",
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