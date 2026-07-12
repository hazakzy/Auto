# ╔══════════════════════════════════════════════════════════════════╗
# ║           GKC BOT — VERSION 2.2 FINAL                           ║
# ║           Built by Hazak | Hazak Onchain | @cryptoedgelab       ║
# ║           Base strategy by GK                                   ║
# ║                                                                  ║
# ║  V2.2 Core:                                                     ║
# ║  ✅ Persistent JSON state (survives restarts)                   ║
# ║  ✅ Async Telegram queue (never blocks trading)                 ║
# ║  ✅ Safe request wrapper (retry + exponential backoff)          ║
# ║  ✅ Health watchdog with stop events (no duplicate threads)     ║
# ║  ✅ orderLinkId (prevents duplicate orders)                     ║
# ║  ✅ Telegram dedup cache (no spam)                              ║
# ║  ✅ Protected root endpoint                                     ║
# ║  ✅ Exec ID rebuild on restart                                  ║
# ╚══════════════════════════════════════════════════════════════════╝

import os
import json
import hmac
import hashlib
import time
import csv
import queue
import uuid
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
TELEGRAM_PRIVATE_ID = os.environ.get("TELEGRAM_PRIVATE_ID")  # required — no default
BOT_SECRET_TOKEN    = os.environ.get("BOT_SECRET_TOKEN")      # required for control endpoints

MIN_PROFIT_TO_TRACK = 5.0
STATE_FILE          = "/tmp/gkc_state.json"  # persistent state file

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

INTERVAL  = "60"
EMA_FAST  = 12
EMA_SLOW  = 21
EMA_WARN  = 34
MAX_DAILY_LOSS          = 25
MAX_OPEN_POSITIONS      = 4     # max simultaneous open positions across all symbols
DAILY_PROFIT_TARGET     = 0.0   # 0 = disabled. Set e.g. 30.0 to stop new entries per symbol at +$30 daily
SYMBOL_DAILY_PNL        = {s: 0.0 for s in ["BTCUSDT","HYPEUSDT","SOLUSDT","ETHUSDT"]}
PARTIAL_PCT             = 0.25

# ╔══════════════════════════════════════════════════════════════════╗
# ║  FEATURE FLAGS — All OFF by default                             ║
# ║  Enable one at a time to test                                   ║
# ╚══════════════════════════════════════════════════════════════════╝

# ── V1 flags ──
ENABLE_TRADE_LOGGING     = False
ENABLE_HARD_STOP_LOSS    = True    # fixed in V2.2 — measures leveraged loss
ENABLE_DUAL_TIMEFRAME    = False
ENABLE_LSMA_FILTER       = False
ENABLE_VOLATILITY_FILTER = False

# ── V2.1 flags ──
ENABLE_ATR_FILTER        = False
ENABLE_CONSECUTIVE_LOSS  = False
ENABLE_DYNAMIC_SIZING    = False
ENABLE_MARKET_REGIME     = False
ENABLE_PROFIT_LOCKING    = False
ENABLE_VOLUME_FILTER     = False
ENABLE_TIME_FILTER       = False

# ── Feature settings ──
STOP_LOSS_PCT          = 40.0   # leveraged % — 40% = 4% price move on 10x
ATR_PERIOD             = 14
ATR_MIN_PCT            = 0.5
MAX_CONSECUTIVE_LOSSES = 3
COOLDOWN_CANDLES       = 2
DYNAMIC_SIZE_HIGH_VOL  = 0.5
DYNAMIC_SIZE_MED_VOL   = 0.25
REGIME_LSMA_PERIOD     = 50
REGIME_SLOPE_MIN       = 0.05
REGIME_EMA_DIST_MIN    = 0.15
PROFIT_LOCK_LEVELS     = [(8.0, 0.0), (15.0, 5.0), (25.0, 12.0)]
VOLUME_EMA_PERIOD      = 20
TIME_AVOID_HOURS       = [0, 1]
TIME_AVOID_WEEKENDS    = False
DUAL_TF_INTERVAL       = "15"
LSMA_PERIOD            = 400
LOG_FILE               = "/tmp/gkc_trades.csv"

# ─── BOT MODE ─────────────────────────────────────────────────────────────────
BOT_MODE = "trading"

# ─── STATE ────────────────────────────────────────────────────────────────────
last_signal          = {}
entry_price          = {}
peak_profit          = {}
locked_profit        = {}
bot_status           = {"last_scan": "never", "error": None, "version": "2.2"}
daily_pnl            = {"date": None, "pnl": 0.0, "trades": 0, "stopped": False}
processed_exec_ids   = set()
early_warning_fired  = set()
early_signal_alerted = {}
consecutive_losses   = {s: 0 for s in SYMBOL_CONFIG}
cooldown_candles     = {s: 0 for s in SYMBOL_CONFIG}
performance          = {
    s: {
        "trades": 0, "wins": 0, "losses": 0,
        "total_pnl": 0.0, "avg_win": 0.0, "avg_loss": 0.0,
        "largest_win": 0.0, "largest_loss": 0.0,
        "win_rate": 0.0, "consecutive_losses": 0,
    }
    for s in SYMBOL_CONFIG
}

# ─── THREAD SAFETY ────────────────────────────────────────────────────────────
symbol_locks = {s: threading.Lock() for s in SYMBOL_CONFIG}
mode_lock    = threading.Lock()
state_lock   = threading.Lock()

def get_mode():
    with mode_lock:
        return BOT_MODE

def set_mode(mode):
    global BOT_MODE
    with mode_lock:
        BOT_MODE = mode
    save_state()  # FIX: persist mode immediately so restarts restore correct mode

# ╔══════════════════════════════════════════════════════════════════╗
# ║  V2.2 UPGRADE #1 — ASYNC TELEGRAM QUEUE                        ║
# ║  Telegram never blocks trading — messages go into a queue       ║
# ║  A separate thread sends them in order                          ║
# ╚══════════════════════════════════════════════════════════════════╝
telegram_queue    = queue.Queue(maxsize=500)
telegram_last_sent = {}   # dedup cache — {message_key: timestamp}
TELEGRAM_DEDUP_SECS = 60  # suppress identical messages within 60s

def telegram_worker(stop_event=None):
    """Background thread — drains the Telegram queue and sends messages"""
    print("[TELEGRAM] Queue worker started")
    last_cleanup = time.time()
    while True:
        heartbeat("telegram")  # signal watchdog telegram thread is alive
        # FIX: periodic cleanup of dedup cache to prevent memory growth
        if time.time() - last_cleanup > 300:  # every 5 minutes
            now = time.time()
            stale_keys = [k for k, t in list(telegram_last_sent.items())
                          if now - t > 600]
            for k in stale_keys:
                telegram_last_sent.pop(k, None)
            if stale_keys:
                print(f"[TELEGRAM] Cleaned {len(stale_keys)} stale dedup keys")
            last_cleanup = now
        try:
            item = telegram_queue.get(timeout=5)
            if item is None:
                continue
            message, private, attempt = item
            if not TELEGRAM_TOKEN:
                telegram_queue.task_done()
                continue
            chat_id = TELEGRAM_PRIVATE_ID if private else TELEGRAM_CHAT_ID
            url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            data    = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
            try:
                r = requests.post(url, json=data, timeout=10)
                if r.status_code == 429:
                    # Rate limited — requeue with delay
                    retry_after = r.json().get("parameters", {}).get("retry_after", 5)
                    print(f"[TELEGRAM] Rate limited — retrying after {retry_after}s")
                    time.sleep(retry_after)
                    if attempt < 3:
                        telegram_queue.put((message, private, attempt + 1))
                elif r.status_code != 200:
                    print(f"[TELEGRAM] Failed ({r.status_code}): {r.text[:100]}")
            except Exception as e:
                print(f"[TELEGRAM] Send error: {e}")
                if attempt < 3:
                    time.sleep(2 ** attempt)
                    telegram_queue.put((message, private, attempt + 1))
            telegram_queue.task_done()
        except queue.Empty:
            continue
        except Exception as e:
            print(f"[TELEGRAM] Worker error: {e}")

def send_telegram(message, private=False, dedup_key=None):
    """
    Non-blocking — puts message in queue, returns immediately.
    dedup_key: if set, suppresses identical messages within TELEGRAM_DEDUP_SECS
    """
    if dedup_key:
        now  = time.time()
        last = telegram_last_sent.get(dedup_key, 0)
        if now - last < TELEGRAM_DEDUP_SECS:
            return  # suppress duplicate
        telegram_last_sent[dedup_key] = now

    if telegram_queue.full():
        print("[TELEGRAM] Queue full — dropping low-priority message")
        return

    telegram_queue.put((message, private, 0))

# ╔══════════════════════════════════════════════════════════════════╗
# ║  V2.2 UPGRADE #2 — SAFE REQUEST WRAPPER                        ║
# ║  One function for all Bybit API calls                           ║
# ║  Retry + exponential backoff + rate limit handling              ║
# ╚══════════════════════════════════════════════════════════════════╝
def safe_request(method, url, headers=None, params=None, json_body=None,
                 max_retries=3, timeout=10):
    """
    Unified API request with exponential backoff.
    Returns response JSON or None on failure.
    """
    for attempt in range(max_retries):
        try:
            if method == "GET":
                r = requests.get(url, headers=headers, params=params, timeout=timeout)
            else:
                r = requests.post(url, headers=headers, json=json_body, timeout=timeout)

            if r.status_code == 429:
                wait = 2 ** (attempt + 1)
                print(f"[API] Rate limited — waiting {wait}s (attempt {attempt+1})")
                time.sleep(wait)
                continue

            if r.status_code >= 500:
                wait = 2 ** attempt
                print(f"[API] Server error {r.status_code} — waiting {wait}s")
                time.sleep(wait)
                continue

            # FIX: handle HTML responses (Bybit maintenance pages etc)
            try:
                return r.json()
            except (ValueError, requests.exceptions.JSONDecodeError):
                print(f"[API] Non-JSON response from {url}: {r.text[:100]}")
                time.sleep(2 ** attempt)
                continue

        except requests.exceptions.Timeout:
            wait = 2 ** attempt
            print(f"[API] Timeout (attempt {attempt+1}/{max_retries}) — waiting {wait}s")
            time.sleep(wait)
        except requests.exceptions.HTTPError as e:
            wait = 2 ** attempt
            print(f"[API] HTTP error (attempt {attempt+1}): {e} — waiting {wait}s")
            time.sleep(wait)
        except requests.exceptions.ConnectionError as e:
            wait = 2 ** attempt
            print(f"[API] Connection error (attempt {attempt+1}): {e} — waiting {wait}s")
            time.sleep(wait)
        except Exception as e:
            print(f"[API] Unexpected error: {e}")
            time.sleep(2)

    print(f"[API] All {max_retries} attempts failed for {url}")
    return None

# ─── SIGNATURE ────────────────────────────────────────────────────────────────
def _build_signature(param_str):
    return hmac.new(
        API_SECRET.encode(),
        param_str.encode(),
        hashlib.sha256
    ).hexdigest()

def sign_post(params):
    timestamp   = str(int(time.time() * 1000))
    recv_window = "5000"
    param_str   = timestamp + API_KEY + recv_window + json.dumps(params)
    return {
        "X-BAPI-API-KEY":     API_KEY,
        "X-BAPI-TIMESTAMP":   timestamp,
        "X-BAPI-SIGN":        _build_signature(param_str),
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type":       "application/json"
    }

def sign_get(params):
    timestamp   = str(int(time.time() * 1000))
    recv_window = "5000"
    query_str   = "&".join(f"{k}={v}" for k, v in params.items())
    param_str   = timestamp + API_KEY + recv_window + query_str
    return {
        "X-BAPI-API-KEY":     API_KEY,
        "X-BAPI-TIMESTAMP":   timestamp,
        "X-BAPI-SIGN":        _build_signature(param_str),
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type":       "application/json"
    }

# ╔══════════════════════════════════════════════════════════════════╗
# ║  V2.2 UPGRADE #3 — PERSISTENT JSON STATE                       ║
# ║  Saves state every 60s — survives Railway restarts              ║
# ║  Restores peak_profit, locked_profit, entry_price,             ║
# ║  consecutive_losses, cooldown_candles                           ║
# ╚══════════════════════════════════════════════════════════════════╝
def save_state():
    """Save critical bot state to JSON file"""
    with state_lock:
        try:
            state = {
                "last_signal":        last_signal,
                "entry_price":        entry_price,
                "peak_profit":        peak_profit,
                "locked_profit":      locked_profit,
                "daily_pnl":          daily_pnl,
                "consecutive_losses": consecutive_losses,
                "cooldown_candles":   cooldown_candles,
                "performance":        performance,
                "processed_exec_ids": list(processed_exec_ids),
                "symbol_paused":      {s: SYMBOL_CONFIG[s].get("paused", False) for s in SYMBOLS},
                "bot_mode":           get_mode(),
                "saved_at":           datetime.now(timezone.utc).isoformat(),
            }
            tmp = STATE_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(state, f, indent=2)
            os.replace(tmp, STATE_FILE)  # atomic write — no corrupt files
        except Exception as e:
            print(f"[STATE] Save error: {e}")

def load_state():
    """Load state from JSON file on startup"""
    global last_signal, entry_price, peak_profit, locked_profit
    global daily_pnl, consecutive_losses, cooldown_candles, performance
    global BOT_MODE

    if not os.path.exists(STATE_FILE):
        print("[STATE] No saved state found — starting fresh")
        return

    try:
        with open(STATE_FILE) as f:
            state = json.load(f)

        saved_at = state.get("saved_at", "unknown")
        print(f"[STATE] Loading saved state from {saved_at}")

        last_signal        = state.get("last_signal", {})
        entry_price        = state.get("entry_price", {})
        peak_profit        = state.get("peak_profit", {})
        locked_profit      = state.get("locked_profit", {})
        consecutive_losses.update(state.get("consecutive_losses", {}))
        cooldown_candles.update(state.get("cooldown_candles", {}))
        performance.update(state.get("performance", {}))

        # FIX: restore processed exec IDs to prevent double-counting on restart
        saved_exec_ids = state.get("processed_exec_ids", [])
        processed_exec_ids.update(saved_exec_ids)
        print(f"[STATE] Restored {len(saved_exec_ids)} exec IDs from state")

        # Only restore daily PnL if it's from today
        saved_pnl = state.get("daily_pnl", {})
        today     = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if saved_pnl.get("date") == today:
            daily_pnl.update(saved_pnl)
            print(f"[STATE] Restored today's PnL: ${daily_pnl['pnl']}")
        else:
            print("[STATE] Saved PnL is from previous day — resetting")

        # FIX: restore per-symbol pause state
        saved_paused = state.get("symbol_paused", {})
        for s, paused in saved_paused.items():
            if s in SYMBOL_CONFIG:
                SYMBOL_CONFIG[s]["paused"] = paused
        if any(saved_paused.values()):
            print(f"[STATE] Restored paused symbols: "
                  f"{[s for s, p in saved_paused.items() if p]}")

        # Restore mode
        saved_mode = state.get("bot_mode", "trading")
        set_mode(saved_mode)
        print(f"[STATE] Restored mode: {saved_mode}")
        print(f"[STATE] Restored positions: {last_signal}")

    except Exception as e:
        print(f"[STATE] Load error: {e} — starting fresh")

def rebuild_exec_ids():
    """
    FIX: Rebuild processed_exec_ids from today's executions on startup.
    Prevents double-counting executions that were already processed before restart.
    """
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for symbol in SYMBOLS:
            params  = {"category": "linear", "symbol": symbol, "limit": "50"}
            result  = safe_request("GET", f"{BASE_URL_PRIVATE}/v5/execution/list",
                                   headers=sign_get(params), params=params)
            if not result:
                continue
            for trade in result.get("result", {}).get("list", []):
                trade_time = datetime.fromtimestamp(
                    int(trade["execTime"]) / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d")
                if trade_time == today:
                    exec_id = trade.get("execId")
                    if exec_id:
                        processed_exec_ids.add(exec_id)
        print(f"[STATE] Rebuilt exec IDs — {len(processed_exec_ids)} today's executions loaded")
    except Exception as e:
        print(f"[STATE] rebuild_exec_ids error: {e}")

def state_persistence_worker(stop_event=None):
    """Background thread — saves state every 60 seconds"""
    print("[STATE] Persistence worker started — saving every 60s")
    while True:
        heartbeat("state")  # signal watchdog state thread is alive
        if stop_event:
            stop_event.wait(timeout=60)
            if stop_event.is_set():
                print("[STATE] Stop event received — exiting")
                break
        else:
            time.sleep(60)
        save_state()

# ╔══════════════════════════════════════════════════════════════════╗
# ║  V2.2 UPGRADE #4 — HEALTH WATCHDOG                             ║
# ║  Monitors bot and retrace threads every 60s                     ║
# ║  Restarts dead threads + sends private alert                    ║
# ╚══════════════════════════════════════════════════════════════════╝
thread_registry      = {}
thread_last_alive    = {}
thread_last_alerted  = {}   # prevents spam — track last alert time per thread
thread_stop_events   = {}   # FIX: stop events so old threads exit before new ones start

def register_thread(name, fn, daemon=True):
    """Create, register and start a thread with a stop event.
    All worker functions must accept stop_event=None as first arg."""
    stop_event = threading.Event()
    thread_stop_events[name] = stop_event
    # All workers accept stop_event=None — no try/except needed
    t = threading.Thread(target=fn, args=(stop_event,), daemon=daemon, name=name)
    t.start()
    thread_registry[name]     = {"fn": fn, "thread": t, "daemon": daemon}
    thread_last_alive[name]   = time.time()
    thread_last_alerted[name] = 0
    return t

def heartbeat(name):
    """Call this inside long-running thread loops to signal health"""
    thread_last_alive[name] = time.time()

def stop_thread(name, timeout=10):
    """Signal a thread to stop and wait for it to exit"""
    stop_event = thread_stop_events.get(name)
    if stop_event:
        stop_event.set()
    t = thread_registry.get(name, {}).get("thread")
    if t and t.is_alive():
        t.join(timeout=timeout)
        if t.is_alive():
            print(f"[WATCHDOG] Thread '{name}' did not stop in {timeout}s — forcing restart anyway")
    print(f"[WATCHDOG] Thread '{name}' stopped")

def watchdog():
    """Monitors all registered threads — restarts any that die"""
    print("[WATCHDOG] Started — monitoring threads every 60s")
    time.sleep(30)  # give threads time to start
    while True:
        try:
            for name, info in list(thread_registry.items()):
                t = info["thread"]
                now = time.time()

                if not t.is_alive():
                    # FIX: Thread is dead — stop cleanly then restart
                    print(f"[WATCHDOG] ⚠️  Thread '{name}' is dead — restarting")
                    stop_thread(name)
                    new_stop = threading.Event()
                    thread_stop_events[name] = new_stop
                    new_t = threading.Thread(target=info["fn"], args=(new_stop,),
                                             daemon=info["daemon"], name=name)
                    new_t.start()
                    thread_registry[name]["thread"] = new_t
                    thread_last_alive[name]         = now
                    thread_last_alerted[name]       = now
                    send_telegram(
                        f"⚠️ <b>WATCHDOG — THREAD RESTARTED</b>\n"
                        f"Thread: {name} — died and was restarted",
                        private=True,
                        dedup_key=f"watchdog_restart_{name}"
                    )
                    print(f"[WATCHDOG] ✅ Thread '{name}' restarted cleanly")

                else:
                    stale_mins      = (now - thread_last_alive.get(name, now)) / 60
                    last_alert_mins = (now - thread_last_alerted.get(name, 0)) / 60
                    stale_threshold = 75 if name == "bot" else 15

                    if stale_mins > stale_threshold:
                        if stale_mins > stale_threshold * 2:
                            print(f"[WATCHDOG] 🔄 '{name}' stale {round(stale_mins,1)}m — restarting")
                            stop_thread(name)
                            new_stop = threading.Event()
                            thread_stop_events[name] = new_stop
                            new_t = threading.Thread(target=info["fn"], args=(new_stop,),
                                                     daemon=info["daemon"], name=name)
                            new_t.start()
                            thread_registry[name]["thread"] = new_t
                            thread_last_alive[name]         = now
                            thread_last_alerted[name]       = now
                            send_telegram(
                                f"🔄 <b>WATCHDOG — STALE RESTART</b>\n"
                                f"Thread: {name} — stale {round(stale_mins,1)}m",
                                private=True,
                                dedup_key=f"watchdog_stale_{name}"
                            )
                        elif last_alert_mins >= 30:
                            print(f"[WATCHDOG] ⚠️  '{name}' stale {round(stale_mins,1)}m")
                            send_telegram(
                                f"⚠️ <b>WATCHDOG — STALE</b>\n"
                                f"Thread: {name} | {round(stale_mins,1)} mins no heartbeat",
                                private=True,
                                dedup_key=f"watchdog_alert_{name}"
                            )
                            thread_last_alerted[name] = now

        except Exception as e:
            print(f"[WATCHDOG] Error: {e}")
        time.sleep(60)

# ─── CORE CALCULATIONS ────────────────────────────────────────────────────────
def calc_ema(prices, period):
    k   = 2 / (period + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = p * k + ema * (1 - k)
    return ema

def calc_atr(highs, lows, closes, period=ATR_PERIOD):
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
    if len(closes) < period:
        return closes[-1]
    y      = closes[-period:]
    n      = period
    x_sum  = n * (n - 1) / 2
    x2_sum = n * (n - 1) * (2 * n - 1) / 6
    xy_sum = sum(i * y[i] for i in range(n))
    y_sum  = sum(y)
    denom  = n * x2_sum - x_sum ** 2
    if denom == 0:
        return closes[-1]
    slope     = (n * xy_sum - x_sum * y_sum) / denom
    intercept = (y_sum - slope * x_sum) / n
    return intercept + slope * (n - 1)

# ─── MARKET DATA (uses safe_request) ─────────────────────────────────────────
# V2.2: single safe_request call per fetch — no more scattered retry loops

# Per-scan cache — reset every candle
_candle_cache = {}
_price_cache  = {}

def clear_scan_cache():
    """Call at start of each candle scan to reset cache"""
    _candle_cache.clear()
    _price_cache.clear()

def get_candles(symbol, interval=None):
    """Returns (highs, lows, closes, volumes) — cached per scan"""
    tf  = interval or INTERVAL
    key = f"{symbol}_{tf}"
    if key in _candle_cache:
        return _candle_cache[key]
    result = safe_request("GET", f"{BASE_URL_PUBLIC}/v5/market/kline", params={
        "category": "linear", "symbol": symbol, "interval": tf, "limit": 500
    })
    if not result:
        return [], [], [], []
    try:
        candles = result["result"]["list"]
        candles.reverse()
        data = (
            [float(c[2]) for c in candles],
            [float(c[3]) for c in candles],
            [float(c[4]) for c in candles],
            [float(c[5]) for c in candles],
        )
        _candle_cache[key] = data
        return data
    except Exception as e:
        print(f"[ERROR] Candle parse failed: {e}")
        return [], [], [], []

def get_price(symbol):
    """Returns current price — cached per scan"""
    if symbol in _price_cache:
        return _price_cache[symbol]
    result = safe_request("GET", f"{BASE_URL_PUBLIC}/v5/market/tickers", params={
        "category": "linear", "symbol": symbol
    })
    if not result:
        return None
    try:
        price = float(result["result"]["list"][0]["lastPrice"])
        _price_cache[symbol] = price
        return price
    except Exception as e:
        print(f"[ERROR] Price parse failed: {e}")
        return None

def get_qty_precision(symbol):
    result = safe_request("GET", f"{BASE_URL_PUBLIC}/v5/market/instruments-info", params={
        "category": "linear", "symbol": symbol
    })
    if not result:
        return 3
    try:
        step = result["result"]["list"][0]["lotSizeFilter"]["qtyStep"]
        return len(step.rstrip("0").split(".")[-1]) if "." in step else 0
    except:
        return 3

# ─── TIMING ───────────────────────────────────────────────────────────────────
def wait_for_candle_close(stop_event=None):
    """
    FIX: Uses stop_event.wait() instead of time.sleep().
    Wakes immediately if watchdog signals this thread to stop.
    Prevents duplicate bot instances after watchdog restarts.
    """
    interval_seconds = int(INTERVAL) * 60
    now          = time.time()
    seconds_left = interval_seconds - (now % interval_seconds)
    sleep_time   = seconds_left + 2
    print(f"[WAIT] Sleeping {round(sleep_time/60, 2)} mins until next candle")
    if stop_event:
        stop_event.wait(timeout=sleep_time)  # wakes instantly if stop requested
    else:
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
        for s in SYMBOLS:
            cooldown_candles[s]   = 0
            consecutive_losses[s] = 0
            if SYMBOL_CONFIG[s].get("paused_by_loss"):
                SYMBOL_CONFIG[s]["paused_by_loss"] = False
                SYMBOL_CONFIG[s]["paused"]         = False
        print(f"[RESET] Daily reset for {today}")
        save_state()

def update_daily_pnl(symbol):
    params  = {"category": "linear", "symbol": symbol, "limit": "20"}
    result  = safe_request("GET", f"{BASE_URL_PRIVATE}/v5/execution/list",
                           headers=sign_get(params), params=params)
    if not result:
        return
    today  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    trades = result.get("result", {}).get("list", [])
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

# ─── SYNC ─────────────────────────────────────────────────────────────────────
def sync_state_from_bybit():
    print("[SYNC] Syncing positions from Bybit")
    for symbol in SYMBOLS:
        lock = symbol_locks.get(symbol)
        with lock:
            params  = {"category": "linear", "symbol": symbol}
            result  = safe_request("GET", f"{BASE_URL_PRIVATE}/v5/position/list",
                                   headers=sign_get(params), params=params)
            if not result:
                continue
            positions = result.get("result", {}).get("list", [])
            found     = False
            for pos in positions:
                size = float(pos.get("size", 0))
                if size > 0:
                    side = pos.get("side")
                    sig  = "buy" if side == "Buy" else "sell"
                    last_signal[symbol] = sig
                    # FIX: Always trust exchange for entry price
                    # State file may have stale price — exchange is source of truth
                    exchange_entry = float(pos.get("avgPrice", 0))
                    if symbol not in entry_price:
                        peak_profit[symbol]   = 0.0
                        locked_profit[symbol] = 0.0
                    entry_price[symbol] = exchange_entry
                    print(f"[SYNC] Entry price from exchange: {exchange_entry}")
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
    print(f"[SYNC] State: {last_signal}")

# ─── PERFORMANCE ──────────────────────────────────────────────────────────────
def record_trade_result(symbol, pnl):
    p = performance[symbol]
    p["trades"] += 1
    p["total_pnl"] = round(p["total_pnl"] + pnl, 4)
    if pnl > 0:
        p["wins"] += 1
        p["largest_win"] = round(max(p["largest_win"], pnl), 4)
        p["avg_win"] = round(
            (p["avg_win"] * (p["wins"] - 1) + pnl) / p["wins"], 4)
        consecutive_losses[symbol] = 0
        p["consecutive_losses"] = 0
    else:
        p["losses"] += 1
        p["largest_loss"] = round(min(p["largest_loss"], pnl), 4)
        p["avg_loss"] = round(
            (p["avg_loss"] * (p["losses"] - 1) + pnl) / p["losses"], 4)
        consecutive_losses[symbol] += 1
        p["consecutive_losses"] = consecutive_losses[symbol]
    p["win_rate"] = round(p["wins"] / p["trades"] * 100, 1) if p["trades"] > 0 else 0

# ─── RETRACE ──────────────────────────────────────────────────────────────────
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
        if ENABLE_PROFIT_LOCKING:
            update_profit_lock(symbol, current_pct)
    peak = peak_profit.get(symbol, 0)
    if peak < MIN_PROFIT_TO_TRACK:
        return False
    lock_floor = locked_profit.get(symbol, 0)
    if lock_floor > 0 and current_pct <= lock_floor:
        print(f"[LOCK] {symbol} dropped below locked floor {lock_floor}%")
        return True
    threshold         = get_retrace_threshold(peak)
    retrace_triggered = current_pct <= peak * (1 - threshold)
    print(f"[RETRACE] {symbol} | entry={ep} price={price} | "
          f"current={round(current_pct,3)}% | peak={round(peak,3)}% | "
          f"lock={lock_floor}% | triggered={retrace_triggered}")
    return retrace_triggered

# ─── V2.1 FILTERS ─────────────────────────────────────────────────────────────
def atr_filter_passes(symbol, highs, lows, closes):
    if not ENABLE_ATR_FILTER:
        return True
    atr     = calc_atr(highs, lows, closes)
    atr_pct = (atr / closes[-1]) * 100
    passes  = atr_pct >= ATR_MIN_PCT
    print(f"[ATR] {symbol} {round(atr_pct,3)}% | min={ATR_MIN_PCT}% | ok={passes}")
    if not passes:
        send_telegram(f"⏸️ <b>SKIPPED — LOW ATR</b>\n{symbol}: {round(atr_pct,3)}%", private=True)
    return passes

def check_consecutive_loss_limit(symbol):
    if not ENABLE_CONSECUTIVE_LOSS:
        return False
    if cooldown_candles.get(symbol, 0) > 0:
        cooldown_candles[symbol] -= 1
        remaining = cooldown_candles[symbol]
        if remaining == 0:
            SYMBOL_CONFIG[symbol]["paused_by_loss"] = False
            send_telegram(f"✅ <b>{symbol} RESUMED</b>\nCooldown complete", private=True)
        return True
    losses = consecutive_losses.get(symbol, 0)
    if losses >= MAX_CONSECUTIVE_LOSSES:
        cooldown_candles[symbol]          = COOLDOWN_CANDLES
        SYMBOL_CONFIG[symbol]["paused_by_loss"] = True
        consecutive_losses[symbol]        = 0
        send_telegram(
            f"⏸️ <b>{symbol} COOLDOWN</b>\n"
            f"{MAX_CONSECUTIVE_LOSSES} consecutive losses\n"
            f"Pausing {COOLDOWN_CANDLES} candles",
            private=True
        )
        return True
    return False

def get_dynamic_trade_usdt(symbol, highs, lows, closes):
    base = get_trade_usdt(symbol)
    if not ENABLE_DYNAMIC_SIZING:
        return base
    atr     = calc_atr(highs, lows, closes)
    atr_pct = (atr / closes[-1]) * 100
    if atr_pct > DYNAMIC_SIZE_HIGH_VOL:
        size, tier = round(base * 0.5, 2),  "HIGH VOL"
    elif atr_pct > DYNAMIC_SIZE_MED_VOL:
        size, tier = round(base * 0.75, 2), "MED VOL"
    else:
        size, tier = base, "LOW VOL"
    print(f"[SIZING] {symbol} ATR={round(atr_pct,3)}% → {tier} ${size}")
    return size

def market_is_trending(symbol, highs, lows, closes):
    if not ENABLE_MARKET_REGIME:
        return True
    if len(closes) < REGIME_LSMA_PERIOD + 10:
        return True
    lsma_now  = calc_lsma(closes, REGIME_LSMA_PERIOD)
    lsma_prev = calc_lsma(closes[:-5], REGIME_LSMA_PERIOD)
    slope     = abs(lsma_now - lsma_prev) / closes[-1] * 100
    fast      = calc_ema(closes, EMA_FAST)
    slow      = calc_ema(closes, EMA_SLOW)
    ema_dist  = abs(fast - slow) / slow * 100
    trending  = slope >= REGIME_SLOPE_MIN and ema_dist >= REGIME_EMA_DIST_MIN
    print(f"[REGIME] {symbol} slope={round(slope,4)}% ema_dist={round(ema_dist,4)}% trending={trending}")
    if not trending:
        send_telegram(f"⏸️ <b>SKIPPED — RANGING</b>\n{symbol}", private=True)
    return trending

def update_profit_lock(symbol, peak):
    current_lock = locked_profit.get(symbol, 0)
    for peak_threshold, lock_at in sorted(PROFIT_LOCK_LEVELS, reverse=True):
        if peak >= peak_threshold and lock_at > current_lock:
            locked_profit[symbol] = lock_at
            label = "BREAKEVEN" if lock_at == 0 else f"+{lock_at}%"
            print(f"[LOCK] {symbol} peak={round(peak,2)}% → floor {label}")
            send_telegram(
                f"🔒 <b>PROFIT LOCKED — {symbol}</b>\n"
                f"Peak: {round(peak,2)}% | Floor: {label}",
                private=True
            )
            save_state()
            break

def volume_confirms(symbol, volumes):
    if not ENABLE_VOLUME_FILTER or len(volumes) < VOLUME_EMA_PERIOD:
        return True
    vol_ema = calc_ema(volumes, VOLUME_EMA_PERIOD)
    passes  = volumes[-1] > vol_ema
    if not passes:
        send_telegram(f"⏸️ <b>SKIPPED — LOW VOLUME</b>\n{symbol}", private=True)
    return passes

def time_allows_entry(symbol):
    if not ENABLE_TIME_FILTER:
        return True
    now = datetime.now(timezone.utc)
    if now.hour in TIME_AVOID_HOURS:
        print(f"[TIME] {symbol} blocked — hour {now.hour}:00 UTC")
        return False
    if TIME_AVOID_WEEKENDS and now.weekday() >= 5:
        print(f"[TIME] {symbol} blocked — weekend")
        return False
    return True

def volatility_filter_passes(symbol, closes):
    if not ENABLE_VOLATILITY_FILTER or len(closes) < 5:
        return True
    recent    = closes[-5:]
    range_pct = (max(recent) - min(recent)) / min(recent) * 100
    passes    = range_pct >= 0.8
    if not passes:
        send_telegram(f"⏸️ <b>SKIPPED — FLAT</b>\n{symbol}: {round(range_pct,3)}%", private=True)
    return passes

def dual_tf_allows_new_entry(symbol, signal):
    if not ENABLE_DUAL_TIMEFRAME:
        return True
    _, _, closes_15m, _ = get_candles(symbol, interval=DUAL_TF_INTERVAL)
    if len(closes_15m) < EMA_SLOW + 5:
        return True
    fast_15m = calc_ema(closes_15m, EMA_FAST)
    slow_15m = calc_ema(closes_15m, EMA_SLOW)
    agrees   = fast_15m > slow_15m if signal == "buy" else fast_15m < slow_15m
    if not agrees:
        send_telegram(f"⏸️ <b>SKIPPED — DUAL TF</b>\n{symbol}", private=True)
    return agrees

def lsma_macro_confirms(symbol, closes, signal):
    if not ENABLE_LSMA_FILTER or len(closes) < LSMA_PERIOD:
        return True
    lsma400  = calc_lsma(closes, LSMA_PERIOD)
    price    = closes[-1]
    above    = price > lsma400
    confirms = (signal == "buy" and above) or (signal == "sell" and not above)
    print(f"[LSMA] {symbol} price={round(price,4)} lsma={round(lsma400,4)} confirms={confirms}")
    if not confirms:
        send_telegram(f"⏸️ <b>SKIPPED — LSMA MACRO</b>\n{symbol}", private=True)
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
    loss_pct = ((ep - price) / ep * 100 * lev) if sig == "buy" \
               else ((price - ep) / ep * 100 * lev)
    if loss_pct >= STOP_LOSS_PCT:
        raw = round(loss_pct / lev, 2)
        print(f"[STOP] {symbol} hard stop — {round(loss_pct,2)}% leveraged")
        send_telegram(
            f"🛑 <b>HARD STOP LOSS</b>\n"
            f"Symbol: {symbol}\n"
            f"Entry: ${ep} | Now: ${price}\n"
            f"Price move: -{raw}% | Leveraged: -{round(loss_pct,2)}%",
            private=True
        )
        return True
    return False

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
    return (sig == "buy" and last_h < ema34) or (sig == "sell" and last_h >= ema34)

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
        f"👀 <b>EARLY SIGNAL</b>\n"
        f"{symbol} | {direction}\n"
        f"Price: ${round(closes[-1],4)} | Gap: {round(gap_pct,3)}%\n"
        f"⚠️ Not confirmed yet",
        private=True
    )

# ─── TRADE LOGGING ────────────────────────────────────────────────────────────
def init_log():
    if not ENABLE_TRADE_LOGGING:
        return
    try:
        if not os.path.exists(LOG_FILE):
            with open(LOG_FILE, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp", "symbol", "action", "side",
                    "price", "qty", "peak_pct", "locked_pct",
                    "trade_pnl", "daily_pnl", "reason"
                ])
    except Exception as e:
        print(f"[LOG] Init error: {e}")

def log_trade(symbol, action, side, price, qty=0, peak=0, locked=0, pnl=0, reason=""):
    if not ENABLE_TRADE_LOGGING:
        return
    try:
        with open(LOG_FILE, "a", newline="") as f:
            csv.writer(f).writerow([
                datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                symbol, action, side, price, qty,
                round(peak, 3), round(locked, 3),
                round(pnl, 4), round(daily_pnl["pnl"], 4), reason
            ])
    except Exception as e:
        print(f"[LOG] Write error: {e}")

# ─── SIGNAL ───────────────────────────────────────────────────────────────────
def check_signal(symbol):
    highs, lows, closes, volumes = get_candles(symbol)
    if len(closes) < EMA_SLOW + 5:
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
    buy  = fast_prev2 < slow_prev2 and fast_prev > slow_prev and fast_now > slow_now
    sell = fast_prev2 > slow_prev2 and fast_prev < slow_prev and fast_now < slow_now
    if buy:  return "buy",  highs, lows, closes, volumes
    if sell: return "sell", highs, lows, closes, volumes
    return None, highs, lows, closes, volumes

# ─── EXCHANGE ACTIONS ─────────────────────────────────────────────────────────
def set_leverage(symbol):
    lev    = str(get_leverage(symbol))
    body   = {"category": "linear", "symbol": symbol,
              "buyLeverage": lev, "sellLeverage": lev}
    result = safe_request("POST", f"{BASE_URL_PRIVATE}/v5/position/set-leverage",
                          headers=sign_post(body), json_body=body)
    if result and result.get("retCode") not in [0, 110043]:
        print(f"[WARN] Leverage {symbol}: {result}")

def partial_close(symbol, pct=PARTIAL_PCT):
    params  = {"category": "linear", "symbol": symbol}
    result  = safe_request("GET", f"{BASE_URL_PRIVATE}/v5/position/list",
                           headers=sign_get(params), params=params)
    if not result:
        return
    for pos in result.get("result", {}).get("list", []):
        size = float(pos.get("size", 0))
        if size > 0:
            precision = get_qty_precision(symbol)
            close_qty = round(size * pct, precision)
            if close_qty <= 0:
                return
            close_side = "Sell" if pos["side"] == "Buy" else "Buy"
            body = {"category": "linear", "symbol": symbol,
                    "side": close_side, "orderType": "Market",
                    "qty": str(close_qty), "reduceOnly": True}
            safe_request("POST", f"{BASE_URL_PRIVATE}/v5/order/create",
                         headers=sign_post(body), json_body=body)
            print(f"[PARTIAL] {symbol} {int(pct*100)}% ({close_qty})")
            time.sleep(1)
            update_daily_pnl(symbol)
            send_telegram(
                f"⚠️ <b>PARTIAL CLOSE</b>\n"
                f"{symbol}: {int(pct*100)}% closed\n"
                f"HLC3 crossed EMA34"
            )

def close_position(symbol, reason="signal"):
    params = {"category": "linear", "symbol": symbol}
    result = safe_request("GET", f"{BASE_URL_PRIVATE}/v5/position/list",
                          headers=sign_get(params), params=params)
    if not result:
        return
    closed         = False
    close_side_str = ""
    close_qty      = 0
    for pos in result.get("result", {}).get("list", []):
        size = float(pos.get("size", 0))
        if size > 0:
            close_side     = "Sell" if pos["side"] == "Buy" else "Buy"
            close_side_str = close_side
            close_qty      = size
            body = {"category": "linear", "symbol": symbol,
                    "side": close_side, "orderType": "Market",
                    "qty": str(size), "reduceOnly": True}
            safe_request("POST", f"{BASE_URL_PRIVATE}/v5/order/create",
                         headers=sign_post(body), json_body=body)
            print(f"[CLOSE] {symbol} {pos['side']} size={size}")
            closed = True
    if closed:
        time.sleep(1)
        pnl_before = daily_pnl["pnl"]
        update_daily_pnl(symbol)
        trade_pnl  = round(daily_pnl["pnl"] - pnl_before, 4)
        pk         = round(peak_profit.get(symbol, 0), 2)
        lk         = round(locked_profit.get(symbol, 0), 2)
        ep         = entry_price.get(symbol, 0)
        record_trade_result(symbol, trade_pnl)
        log_trade(symbol, "EXIT", close_side_str,
                  get_price(symbol) or 0, close_qty, pk, lk, trade_pnl, reason)
        entry_price.pop(symbol, None)
        peak_profit.pop(symbol, None)
        locked_profit.pop(symbol, None)
        early_warning_fired.discard(symbol)
        early_signal_alerted.pop(symbol, None)
        save_state()
        send_telegram(
            f"🔴 <b>POSITION CLOSED</b>\n"
            f"Symbol: {symbol}\n"
            f"Entry: ${ep} | Peak: {pk}%\n"
            f"Reason: {reason}\n"
            f"Daily PnL: ${round(daily_pnl['pnl'],2)}"
        )

def place_order(symbol, signal, highs=None, lows=None, closes=None):
    set_leverage(symbol)
    price = get_price(symbol)
    if not price:
        print(f"[ERROR] Price unavailable for {symbol}")
        return
    precision  = get_qty_precision(symbol)
    trade_usdt = get_dynamic_trade_usdt(symbol, highs, lows, closes) \
                 if (ENABLE_DYNAMIC_SIZING and highs) else get_trade_usdt(symbol)
    qty           = round((trade_usdt * get_leverage(symbol)) / price, precision)
    side          = "Buy" if signal == "buy" else "Sell"
    order_link_id = f"gkc_{symbol}_{int(time.time())}"  # FIX: unique ID prevents duplicate orders
    body = {"category": "linear", "symbol": symbol,
            "side": side, "orderType": "Market",
            "qty": str(qty), "timeInForce": "GTC",
            "orderLinkId": order_link_id}
    result = safe_request("POST", f"{BASE_URL_PRIVATE}/v5/order/create",
                          headers=sign_post(body), json_body=body)
    if not result:
        print(f"[ERROR] Order failed for {symbol}")
        return
    print(f"[ORDER] {symbol} {side} qty={qty} @ {price} | retCode={result.get('retCode')}")
    if result.get("retCode") == 0:
        # FIX: wait 1s then query exchange for actual fill price
        # ticker price != fill price due to slippage
        time.sleep(1)
        actual_entry = price  # fallback to ticker
        pos_params  = {"category": "linear", "symbol": symbol}
        pos_result  = safe_request("GET", f"{BASE_URL_PRIVATE}/v5/position/list",
                                   headers=sign_get(pos_params), params=pos_params)
        if pos_result:
            for pos in pos_result.get("result", {}).get("list", []):
                if float(pos.get("size", 0)) > 0:
                    actual_entry = float(pos.get("avgPrice", price))
                    break
        print(f"[ENTRY] {symbol} fill price: {actual_entry} (ticker was {price})")
        entry_price[symbol]   = actual_entry
        peak_profit[symbol]   = 0.0
        locked_profit[symbol] = 0.0
        early_warning_fired.discard(symbol)
        early_signal_alerted.pop(symbol, None)
        log_trade(symbol, "ENTRY", side, actual_entry, qty, reason="EMA crossover")
        save_state()
        send_telegram(
            f"🟢 <b>NEW TRADE</b>\n"
            f"Symbol: {symbol}\n"
            f"Side: {side} | Entry: ${actual_entry}\n"
            f"Daily PnL: ${round(daily_pnl['pnl'],2)}"
        )

def close_all_positions():
    for symbol in SYMBOLS:
        lock = symbol_locks.get(symbol)
        with lock:
            close_position(symbol, reason="close all")
            last_signal.pop(symbol, None)
    print("[RISK] All positions closed")

def send_signal_only_alert(symbol, signal, price):
    send_telegram(
        f"📡 <b>SIGNAL ALERT</b>\n"
        f"{symbol} | {'BUY 🟢' if signal == 'buy' else 'SELL 🔴'}\n"
        f"Price: ${price}\n"
        f"⚠️ Monitoring mode — not trading"
    )

# ─── REAL-TIME MONITOR ────────────────────────────────────────────────────────
def realtime_retrace_monitor(stop_event=None):
    """stop_event: threading.Event — set by watchdog to interrupt cleanly"""
    print("[MONITOR] Real-time monitor started — checking every 45s")
    while True:
        if stop_event and stop_event.is_set():
            print("[MONITOR] Stop event received — exiting loop")
            break
        heartbeat("monitor")
        try:
            mode = get_mode()
            if mode == "trading" and not daily_pnl["stopped"]:
                for symbol in list(last_signal.keys()):
                    if is_symbol_paused(symbol):
                        continue
                    lock = symbol_locks.get(symbol)
                    if not lock or not lock.acquire(blocking=False):
                        continue
                    try:
                        if check_hard_stop(symbol):
                            close_position(symbol, reason="hard stop loss")
                            with state_lock:
                                last_signal.pop(symbol, None)
                            continue
                        if is_early_warning_on(symbol) and symbol not in early_warning_fired:
                            if check_early_warning(symbol):
                                partial_close(symbol, PARTIAL_PCT)
                                early_warning_fired.add(symbol)
                        if symbol in last_signal and check_peak_retrace(symbol):
                            send_telegram(
                                f"📉 <b>RETRACE EXIT</b>\n"
                                f"{symbol} | Peak: {round(peak_profit.get(symbol,0),2)}%\n"
                                f"Lock floor: {round(locked_profit.get(symbol,0),2)}%",
                                dedup_key=f"retrace_{symbol}"
                            )
                            close_position(symbol, reason="retrace")
                            with state_lock:
                                last_signal.pop(symbol, None)
                    finally:
                        lock.release()
        except Exception as e:
            print(f"[MONITOR] Error: {e}")
            traceback.print_exc()
        if stop_event:
            stop_event.wait(timeout=45)
        else:
            time.sleep(45)

# ─── BOT LOOP ─────────────────────────────────────────────────────────────────
def run_bot(stop_event=None):
    """stop_event: threading.Event — set by watchdog to interrupt cleanly"""
    print("=" * 62)
    print("  GKC BOT — VERSION 2.2  |  Production Ready")
    print("  Built by Hazak | @cryptoedgelab | Base by GK")
    print(f"  Timeframe: {INTERVAL}m | Daily limit: -${MAX_DAILY_LOSS}")
    print("  SYMBOLS:")
    for s, cfg in SYMBOL_CONFIG.items():
        st = "PAUSED" if cfg.get("paused") else f"${cfg['trade_usdt']} x {cfg['leverage']}x"
        print(f"    {s}: {st}")
    print("  V1 FLAGS:")
    print(f"    Trade Logging:    {'ON' if ENABLE_TRADE_LOGGING     else 'OFF'}")
    print(f"    Hard Stop Loss:   {'ON' if ENABLE_HARD_STOP_LOSS    else 'OFF'} ({STOP_LOSS_PCT}% lev)")
    print(f"    Dual Timeframe:   {'ON' if ENABLE_DUAL_TIMEFRAME    else 'OFF'}")
    print(f"    LSMA400:          {'ON' if ENABLE_LSMA_FILTER       else 'OFF'}")
    print(f"    Volatility:       {'ON' if ENABLE_VOLATILITY_FILTER else 'OFF'}")
    print("  V2.1 FLAGS:")
    print(f"    ATR Filter:       {'ON' if ENABLE_ATR_FILTER        else 'OFF'}")
    print(f"    Consec. Loss:     {'ON' if ENABLE_CONSECUTIVE_LOSS  else 'OFF'}")
    print(f"    Dynamic Sizing:   {'ON' if ENABLE_DYNAMIC_SIZING    else 'OFF'}")
    print(f"    Market Regime:    {'ON' if ENABLE_MARKET_REGIME     else 'OFF'}")
    print(f"    Profit Locking:   {'ON' if ENABLE_PROFIT_LOCKING    else 'OFF'}")
    print(f"    Volume Filter:    {'ON' if ENABLE_VOLUME_FILTER     else 'OFF'}")
    print(f"    Time Filter:      {'ON' if ENABLE_TIME_FILTER       else 'OFF'}")
    print("  V2.2 CORE:")
    print("    ✅ Async Telegram queue (maxsize=500 + dedup)")
    print("    ✅ Safe request wrapper + backoff")
    print("    ✅ Persistent JSON state + exec ID rebuild")
    print("    ✅ Health watchdog (stop events — no duplicate threads)")
    print("    ✅ orderLinkId (no duplicate orders)")
    print("    ✅ Protected endpoints + minimal public root")
    print("=" * 62)

    # Warn if no secret token — don't crash, just alert
    if not BOT_SECRET_TOKEN:
        print("⚠️  WARNING: BOT_SECRET_TOKEN not set — control endpoints are public")
        send_telegram(
            "⚠️ <b>SECURITY WARNING</b>\n"
            "BOT_SECRET_TOKEN is not set\n"
            "Control endpoints are publicly accessible\n"
            "Set BOT_SECRET_TOKEN in Railway env vars",
            private=True
        )

    # Startup API key validation
    if not API_KEY or not API_SECRET:
        print("❌ FATAL: BYBIT_API_KEY or BYBIT_API_SECRET not set — cannot trade")
        send_telegram(
            "❌ <b>STARTUP FAILED</b>\n"
            "BYBIT_API_KEY or BYBIT_API_SECRET missing\n"
            "Bot cannot start — set env vars on Railway",
            private=True
        )
        return

    init_log()
    load_state()
    rebuild_exec_ids()
    sync_state_from_bybit()
    consecutive_failures = 0   # FIX: track repeated failures — break after 10

    while True:
        try:
            heartbeat("bot")
            # FIX: pass stop_event so watchdog can interrupt the sleep
            if stop_event and stop_event.is_set():
                print("[BOT] Stop event received — exiting loop")
                break
            wait_for_candle_close(stop_event)
            if stop_event and stop_event.is_set():
                print("[BOT] Stop event received after wait — exiting loop")
                break
            check_daily_reset()
            clear_scan_cache()

            mode = get_mode()
            print(f"\n[SCAN] {time.strftime('%Y-%m-%d %H:%M:%S')} UTC | "
                  f"MODE: {mode.upper()} | V2.2")
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
                send_telegram(
                    f"🚨 <b>DAILY LOSS LIMIT HIT</b>\n"
                    f"Loss: ${round(daily_pnl['pnl'],2)} | Limit: -${MAX_DAILY_LOSS}\n"
                    f"Closing all — bot stopped for today",
                    private=True
                )
                close_all_positions()
                continue

            for symbol in SYMBOLS:
                heartbeat("bot")  # signal watchdog bot is alive during slow scans
                # FIX: per-symbol try/except — one symbol failing never stops others
                try:
                    if is_symbol_paused(symbol):
                        print(f"[PAUSED] {symbol} — skipping")
                        continue

                    lock = symbol_locks.get(symbol)
                    if not lock:
                        continue

                    with lock:
                        # V2.1 — consecutive loss cooldown
                        if check_consecutive_loss_limit(symbol):
                            print(f"[COOLDOWN] {symbol} — skipping")
                            continue

                        # Retrace at candle close (backup for monitor)
                        if mode == "trading" and symbol in last_signal:
                            if check_peak_retrace(symbol):
                                print(f"[RETRACE] {symbol} — closing at candle")
                                close_position(symbol, reason="retrace at candle")
                                with state_lock:
                                    last_signal.pop(symbol, None)
                                continue

                        signal, highs, lows, closes, volumes = check_signal(symbol)
                        prev = last_signal.get(symbol)
                        print(f"[SIGNAL] {symbol} | current={signal} | prev={prev} | mode={mode}")

                        if signal and signal != prev:

                            if mode == "trading":
                                # Always close existing position first — never filtered
                                if prev and symbol in entry_price:
                                    print(f"[EXIT] {symbol} closing {prev} on flip")
                                    close_position(symbol, reason="signal flip")
                                    with state_lock:
                                        last_signal.pop(symbol, None)

                                # ── Entry filters — NEW entries only ──
                                if DAILY_PROFIT_TARGET > 0:
                                    sym_pnl = SYMBOL_DAILY_PNL.get(symbol, 0)
                                    if sym_pnl >= DAILY_PROFIT_TARGET:
                                        print(f"[PROFIT TARGET] {symbol} — skipping new entry")
                                        continue

                                if not time_allows_entry(symbol):                              continue
                                if highs and not atr_filter_passes(symbol, highs, lows, closes): continue
                                if closes and not volatility_filter_passes(symbol, closes):       continue
                                if highs and not market_is_trending(symbol, highs, lows, closes): continue
                                if volumes and not volume_confirms(symbol, volumes):              continue
                                if closes and not lsma_macro_confirms(symbol, closes, signal):   continue
                                if not dual_tf_allows_new_entry(symbol, signal):                 continue

                                open_count = len([s for s in SYMBOLS if s in last_signal])
                                if open_count >= MAX_OPEN_POSITIONS:
                                    print(f"[MAX POS] {open_count} open — skipping {symbol}")
                                    continue

                                print(f"[ORDER] {symbol} {signal.upper()} — all filters passed")
                                place_order(symbol, signal, highs, lows, closes)
                                with state_lock:
                                    last_signal[symbol] = signal
                                bot_status["last_signal"] = dict(last_signal)
                                save_state()

                            elif mode == "signal_only":
                                price = get_price(symbol) or 0
                                send_signal_only_alert(symbol, signal, price)
                                with state_lock:
                                    last_signal[symbol] = signal
                                bot_status["last_signal"] = dict(last_signal)

                        else:
                            print(f"[HOLD] {symbol} | no confirmed reversal")

                except Exception as sym_err:
                    print(f"[ERROR] {symbol} scan failed: {sym_err}")
                    traceback.print_exc()
                    send_telegram(
                        f"⚠️ <b>SYMBOL ERROR</b>\n"
                        f"{symbol}: {str(sym_err)[:100]}\n"
                        f"Skipping — other symbols continue",
                        private=True,
                        dedup_key=f"sym_error_{symbol}"
                    )

        except Exception as e:
            bot_status["error"] = str(e)
            print(f"[ERROR] Bot loop: {e}")
            traceback.print_exc()
            consecutive_failures += 1
            if consecutive_failures >= 10:
                msg = (
                    f"🚨 <b>BOT STOPPED</b>\n"
                    f"10 consecutive failures\n"
                    f"Last error: {str(e)[:100]}\n"
                    f"Railway will restart automatically"
                )
                send_telegram(msg, private=True)
                print("[FATAL] 10 consecutive failures — stopping bot loop")
                break
            time.sleep(30)
        else:
            consecutive_failures = 0  # reset on successful scan

# ─── ROUTE AUTHENTICATION ─────────────────────────────────────────────────────
# All control endpoints require ?token=BOT_SECRET_TOKEN
# Read-only endpoints (/, /status, /test, /performance) are public
from flask import request as flask_request

def auth_required():
    """Returns True if request is authenticated"""
    if not BOT_SECRET_TOKEN:
        return True  # if no token set, allow all (dev mode)
    token = flask_request.args.get("token") or \
            flask_request.headers.get("Authorization", "").replace("Bearer ", "")
    return token == BOT_SECRET_TOKEN

def auth_error():
    return jsonify({"error": "Unauthorized — provide ?token=YOUR_SECRET"}), 401

# ─── ROUTES ───────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    """Public endpoint — minimal info only. Full dashboard requires token."""
    return jsonify({
        "status":      "running",
        "version":     "2.2",
        "mode":        get_mode(),
        "last_scan":   bot_status.get("last_scan", "never"),
        "symbols":     SYMBOLS,
        "interval":    f"{INTERVAL}m",
    })

@app.route("/dashboard")
def dashboard():
    """Full dashboard — requires auth token"""
    if not auth_required(): return auth_error()
    return jsonify({
        "version":       "2.2",
        "status":        "running",
        "mode":          get_mode(),
        "bot":           dict(bot_status),
        "last_signal":   dict(last_signal),
        "entry_price":   dict(entry_price),
        "peak_profit":   dict(peak_profit),
        "locked_profit": dict(locked_profit),
        "daily_pnl":     dict(daily_pnl),
        "symbols": {
            s: {
                **cfg,
                "early_warning_fired":  s in early_warning_fired,
                "consecutive_losses":   consecutive_losses.get(s, 0),
                "on_cooldown":          cooldown_candles.get(s, 0) > 0,
            }
            for s, cfg in SYMBOL_CONFIG.items()
        },
        "interval": f"{INTERVAL}m",
        "v1_features": {
            "trade_logging":     ENABLE_TRADE_LOGGING,
            "hard_stop_loss":    ENABLE_HARD_STOP_LOSS,
            "dual_timeframe":    ENABLE_DUAL_TIMEFRAME,
            "lsma_filter":       ENABLE_LSMA_FILTER,
            "volatility_filter": ENABLE_VOLATILITY_FILTER,
        },
        "v21_features": {
            "atr_filter":       ENABLE_ATR_FILTER,
            "consecutive_loss": ENABLE_CONSECUTIVE_LOSS,
            "dynamic_sizing":   ENABLE_DYNAMIC_SIZING,
            "market_regime":    ENABLE_MARKET_REGIME,
            "profit_locking":   ENABLE_PROFIT_LOCKING,
            "volume_filter":    ENABLE_VOLUME_FILTER,
            "time_filter":      ENABLE_TIME_FILTER,
        },
        "v22_core": {
            "async_telegram":   True,
            "safe_requests":    True,
            "persistent_state": True,
            "health_watchdog":  True,
        }
    })

@app.route("/status")
def status():
    try:
        positions = {}
        for symbol in SYMBOLS:
            params = {"category": "linear", "symbol": symbol}
            result = safe_request("GET", f"{BASE_URL_PRIVATE}/v5/position/list",
                                  headers=sign_get(params), params=params)
            if not result:
                continue
            for pos in result.get("result", {}).get("list", []):
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
                        "on_cooldown":    cooldown_candles.get(symbol, 0) > 0,
                    }
        return jsonify({
            "version":        "2.2",
            "mode":           get_mode(),
            "open_positions": positions,
            "last_signal":    dict(last_signal),
            "last_scan":      bot_status.get("last_scan", "never"),
            "daily_pnl":      dict(daily_pnl),
        })
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/performance")
def perf():
    return jsonify({
        "version":     "2.2",
        "performance": performance,
        "daily_pnl":   daily_pnl,
        "mode":        get_mode(),
    })

@app.route("/trading")
def mode_trading():
    if not auth_required(): return auth_error()
    set_mode("trading")
    send_telegram("⚡ <b>BOT MODE: TRADING</b>\nV2.2 — signals + orders active", private=True)
    return jsonify({"mode": "trading", "version": "2.2"})

@app.route("/signalonly")
def mode_signal_only():
    if not auth_required(): return auth_error()
    if get_mode() == "trading" and last_signal:
        close_all_positions()
        send_telegram("📡 <b>SIGNAL ONLY</b>\nAll positions closed first", private=True)
    else:
        send_telegram("📡 <b>SIGNAL ONLY</b>\nAlerts active — no orders", private=True)
    set_mode("signal_only")
    return jsonify({"mode": "signal_only"})

@app.route("/pause")
def mode_pause():
    if not auth_required(): return auth_error()
    set_mode("paused")
    send_telegram("⏸️ <b>BOT PAUSED</b>\nNo signals, no orders", private=True)
    return jsonify({"mode": "paused"})

@app.route("/pause/<symbol>")
def pause_symbol(symbol):
    if not auth_required(): return auth_error()
    sym = symbol.upper()
    if sym not in SYMBOL_CONFIG:
        return jsonify({"error": f"{sym} not found"})
    SYMBOL_CONFIG[sym]["paused"] = True
    save_state()
    return jsonify({"message": f"{sym} paused"})

@app.route("/resume/<symbol>")
def resume_symbol(symbol):
    if not auth_required(): return auth_error()
    sym = symbol.upper()
    if sym not in SYMBOL_CONFIG:
        return jsonify({"error": f"{sym} not found"})
    SYMBOL_CONFIG[sym]["paused"] = False
    cooldown_candles[sym]        = 0
    consecutive_losses[sym]      = 0
    save_state()
    return jsonify({"message": f"{sym} resumed"})

@app.route("/sync")
def sync():
    if not auth_required(): return auth_error()
    try:
        sync_state_from_bybit()
        return jsonify({"message": "Synced", "last_signal": last_signal})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/closeall")
def closeall():
    if not auth_required(): return auth_error()
    try:
        close_all_positions()
        return jsonify({"message": "All positions closed"})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/savestate")
def savestate():
    if not auth_required(): return auth_error()
    try:
        save_state()
        return jsonify({"message": "State saved", "file": STATE_FILE})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/debug")
def debug():
    if not auth_required(): return auth_error()
    try:
        results = {}
        for symbol in SYMBOLS:
            params = {"category": "linear", "symbol": symbol}
            results[symbol] = safe_request(
                "GET", f"{BASE_URL_PRIVATE}/v5/position/list",
                headers=sign_get(params), params=params
            )
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route("/test")
def test():
    result = safe_request("GET", f"{BASE_URL_PUBLIC}/v5/market/tickers",
                          params={"category": "linear", "symbol": "BTCUSDT"})
    if not result:
        return jsonify({"error": "API call failed"})
    return jsonify({
        "version":      "2.2",
        "btc_price":    result["result"]["list"][0]["lastPrice"],
        "api_keys_set": bool(API_KEY and API_SECRET),
        "mode":         get_mode(),
        "telegram_queue_size": telegram_queue.qsize(),
    })

@app.route("/reset_warning/<symbol>")
def reset_warning(symbol):
    if not auth_required(): return auth_error()
    early_warning_fired.discard(symbol.upper())
    return jsonify({"message": f"Early warning reset for {symbol.upper()}"})

# ─── START ────────────────────────────────────────────────────────────────────
# Register and start all threads through watchdog registry
telegram_thread   = register_thread("telegram",    telegram_worker)
bot_thread        = register_thread("bot",          run_bot)
monitor_thread    = register_thread("monitor",      realtime_retrace_monitor)
state_thread      = register_thread("state",        state_persistence_worker)
watchdog_thread   = threading.Thread(target=watchdog, daemon=True, name="watchdog")
watchdog_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
