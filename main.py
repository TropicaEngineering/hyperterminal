# runner_live.py
import asyncio
import builtins
import contextlib
import datetime as dt
import json
import math
import os
import sys as _sys
import time
from collections import defaultdict
from pathlib import Path
from random import random

import aiofiles
import aiohttp
import websockets

from dashboard import Metrics, start as start_ui


LAST_TRADE_PX = {}     # coin -> last trade price
LAST_TRADE_DIR = {}    # coin -> "buy" or "sell"

BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = BASE_DIR / "runtime"
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

RUNNER_STDERR_PATH = RUNTIME_DIR / "runner_stderr.log"
CONFIG_DIR = BASE_DIR / "config"
FILTERED_COINS_PATH = CONFIG_DIR / "filtered_coins.json"
PERP_OI_CACHE_PATH = RUNTIME_DIR / "perp_oi_cache.json"

# Keep the HUD in control of stdout; regular logs go to stderr.
_builtin_print = builtins.print


def _stderr_print(*args, **kwargs):
    kwargs.setdefault("file", _sys.stderr)
    return _builtin_print(*args, **kwargs)


builtins.print = _stderr_print
_sys.stderr = open(RUNNER_STDERR_PATH, "a", buffering=1)

# =========================
# CONFIG
# =========================
WS_URL = "wss://api.hyperliquid.xyz/ws"
# Market-data terminal mode; no trade CSV is written.

# Reconnect/backoff
RECONNECT_MIN = 2
RECONNECT_MAX = 15

# Universe refresh (full restart of shards when set changes)
COIN_REFRESH_SECS = 600

# ---- SUB/ACK DIALS (per-connection) ----
MAX_INFLIGHT = 1
SUB_TIMEOUT = 8.0
PER_SUB_DELAY = 0.35
WAVE_SIZE = 3
WAVE_PAUSE = 30
BATCH_SETTLE = 2.0

# Keepalive
PING_INTERVAL_SECS = 15

# Sharding
TARGET_COINS_PER_SOCKET = 10
MAX_SOCKETS = 3
STAGGER_BETWEEN_SOCKETS = 8.0

# Per-symbol retry / temporary blacklist
MAX_ACK_RETRIES_PER_SYMBOL = 3
BLACKLIST_COOLDOWN_SECS = 900


# --- Perp API
async def poll_perp_summary(metrics):
    url = "https://api.hyperliquid.xyz/info"
    payload = {"type": "metaAndAssetCtxs"}
    cache_path = PERP_OI_CACHE_PATH

    last_oi_usd = 0.0

    if os.path.exists(cache_path):
        try:
            data = json.load(open(cache_path))
            last_oi_usd = float(data.get("oi_usd", 0))
        except Exception:
            pass

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as r:
                    data = await r.json()

            if not isinstance(data, list) or len(data) < 2:
                await asyncio.sleep(15)
                continue

            assets = data[1]
            if not assets:
                await asyncio.sleep(15)
                continue

            oi_usd = sum(
                float(a["openInterest"]) * float(a["markPx"])
                for a in assets
                if "openInterest" in a
            )
            funding_avg = sum(float(a.get("funding", 0)) for a in assets) / len(assets)
            coverage = len(assets)
            oi_change_pct = ((oi_usd - last_oi_usd) / last_oi_usd * 100) if last_oi_usd else 0.0

            if int(time.time()) % 900 < 15:
                try:
                    async with aiofiles.open(cache_path, "w") as f:
                        await f.write(json.dumps({"oi_usd": oi_usd}))
                except Exception:
                    pass

            risk = (
                "downside liq ⚠" if funding_avg > 0 else
                "upside squeeze ⚠" if funding_avg < 0 else
                "neutral"
            )

            metrics.perp_summary = {
                "oi_usd": oi_usd,
                "oi_change_pct": round(oi_change_pct, 2),
                "funding": round(funding_avg * 100, 3),
                "coverage": coverage,
                "risk": risk,
            }

        except Exception as e:
            print("perp poll error:", e)

        await asyncio.sleep(15)


# =========================
# COIN LOADING
# =========================
def load_coins_initial():
    try:
        from markets import get_filtered_coins
        names = get_filtered_coins()
        if names:
            names = sorted(set(str(n).upper() for n in names if isinstance(n, str)))
            print(f"[COINS] Loaded {len(names)} via coin_loader.")
            return names
    except Exception as e:
        print(f"[COINS] coin_loader failed: {e}")

    try:
        with open(FILTERED_COINS_PATH) as f:
            names = json.load(f)
        if names:
            names = sorted(set(str(n).upper() for n in names if isinstance(n, str)))
            print(f"[COINS] Loaded {len(names)} from {FILTERED_COINS_PATH.name}.")
            return names
    except Exception as e:
        print(f"[COINS] {FILTERED_COINS_PATH} load failed: {e}")

    print("[COINS] Falling back to tiny default: BTC, ETH, SOL.")
    return ["BTC", "ETH", "SOL"]


def load_coins_refresh():
    try:
        from markets import get_filtered_coins
        names = get_filtered_coins()
        if names:
            return sorted(set(str(n).upper() for n in names if isinstance(n, str)))
    except Exception as e:
        print(f"[COINS] refresh failed: {e}")
    return None


# =========================
# HUD METRICS
# =========================
metrics = Metrics()


# 1m bar accumulator for HUD stats.
BAR_STATE = {}  # coin -> dict(bucket, o, h, l, c, vol_usd, trades)


def _handle_control_msg(msg: dict):
    return


def _note_bar(coin: str, ts_ms: int, px: float, sz: float):
    bucket = int(ts_ms // 60000) * 60000
    vol_usd = float(px) * float(sz or 0.0)
    cur = BAR_STATE.get(coin)

    if cur is None:
        BAR_STATE[coin] = {
            "bucket": bucket,
            "o": px,
            "h": px,
            "l": px,
            "c": px,
            "vol_usd": vol_usd,
            "trades": 1,
        }
        return

    if cur["bucket"] != bucket:
        metrics.note_last_closed_bar(coin, cur.get("vol_usd", 0.0), cur.get("trades", 0))
        BAR_STATE[coin] = {
            "bucket": bucket,
            "o": px,
            "h": px,
            "l": px,
            "c": px,
            "vol_usd": vol_usd,
            "trades": 1,
        }
        return

    cur["h"] = max(cur["h"], px)
    cur["l"] = min(cur["l"], px)
    cur["c"] = px
    cur["vol_usd"] += vol_usd
    cur["trades"] += 1


# =========================
# RUNTIME STATE
# =========================
SUBSCRIBED = defaultdict(set)      # track which coins acked per shard

COIN_STATIC_DENYLIST = set([
    # "KPEPE",
])

TEMP_BLACKLIST = {}                # {symbol: unix_expiry_ts}
SYMBOL_FAILS = {}                  # {symbol: count}


def ms_now() -> int:
    return int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)


def on_trade_msg(trade):
    """Handle one trade dict from WS and feed the HUD."""
    coin = trade.get("coin")
    px = trade.get("px")
    ts = trade.get("time")
    sz = trade.get("sz")

    if coin is None or px is None or ts is None:
        return

    try:
        coin = str(coin).upper()
        if coin.startswith("@"):
            return
        px = float(px)
        ts = int(ts)
        sz = float(sz) if sz is not None else 0.0
    except Exception:
        return

    prev_px = LAST_TRADE_PX.get(coin)
    if prev_px is None:
        side = LAST_TRADE_DIR.get(coin, "buy")
    elif px > prev_px:
        side = "buy"
    elif px < prev_px:
        side = "sell"
    else:
        side = LAST_TRADE_DIR.get(coin, "buy")

    LAST_TRADE_PX[coin] = px
    LAST_TRADE_DIR[coin] = side

    metrics.note_trade(coin, ts, px, sz, side=side)
    _note_bar(coin, ts, px, sz)


def is_blacklisted(symbol: str) -> bool:
    if symbol in COIN_STATIC_DENYLIST:
        return True

    exp = TEMP_BLACKLIST.get(symbol)
    if not exp:
        return False

    if time.time() >= exp:
        TEMP_BLACKLIST.pop(symbol, None)
        SYMBOL_FAILS.pop(symbol, None)
        return False

    return True


def mark_failure(symbol: str, reason: str, shard_name: str, threshold: int = 3):
    SYMBOL_FAILS[symbol] = SYMBOL_FAILS.get(symbol, 0) + 1
    cnt = SYMBOL_FAILS[symbol]

    if cnt >= threshold:
        TEMP_BLACKLIST[symbol] = time.time() + BLACKLIST_COOLDOWN_SECS
        print(f"[{shard_name}] BLK {symbol} for {BLACKLIST_COOLDOWN_SECS}s after {cnt} failures ({reason})")
    else:
        print(f"[{shard_name}] FAIL {symbol} ({reason}) x{cnt} (will retry later)")


# =========================
# SHARD IMPLEMENTATION
# =========================
async def reader_task(ws, q: asyncio.Queue, shard_name: str):
    try:
        while True:
            raw = await ws.recv()
            await q.put(raw)
    except asyncio.CancelledError:
        return
    except Exception as e:
        print(f"[{shard_name}] WARN reader recv error: {e}")
        await q.put(None)


async def keepalive_task(ws, shard_name: str):
    try:
        while True:
            await asyncio.sleep(PING_INTERVAL_SECS)
            await ws.send(json.dumps({"method": "ping"}))
    except asyncio.CancelledError:
        return
    except Exception:
        return


async def drain_and_dispatch(q: asyncio.Queue, shard_name: str, until_ts=None):
    """
    Drain queue for a bit (or until time), dispatch trades,
    and return non-trade/control messages for caller-specific handling.
    """
    others = []
    while True:
        try:
            timeout = 0.05
            if until_ts is not None:
                remaining = max(0.0, until_ts - asyncio.get_event_loop().time())
                timeout = min(timeout, remaining) if remaining > 0 else 0.0
            raw = await asyncio.wait_for(q.get(), timeout=timeout)
        except asyncio.TimeoutError:
            break

        if raw is None:
            others.append(None)
            break

        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue

        ch = msg.get("channel")
        if ch == "trades":
            payload = msg.get("data")
            if isinstance(payload, list):
                for t in payload:
                    if isinstance(t, dict):
                        on_trade_msg(t)
            elif isinstance(payload, dict):
                on_trade_msg(payload)
        else:
            others.append(msg)

    return others


async def subscribe_with_ack(ws, q: asyncio.Queue, shard_name: str, coin: str) -> bool:
    if is_blacklisted(coin):
        print(f"[{shard_name}] SKIP BLK {coin}")
        return False

    try:
        await ws.send(json.dumps({
            "method": "subscribe",
            "subscription": {"type": "trades", "coin": coin},
        }))
    except Exception as e:
        mark_failure(coin, f"senderr: {e}", shard_name, threshold=2)
        raise ConnectionError("socket broke during ws.send()")

    deadline = asyncio.get_event_loop().time() + SUB_TIMEOUT
    loop = asyncio.get_event_loop()

    while loop.time() < deadline:
        others = await drain_and_dispatch(q, shard_name, until_ts=deadline)

        for o in others:
            if isinstance(o, dict):
                _handle_control_msg(o)

        if any(o is None for o in others):
            raise ConnectionError("socket broke while waiting for ACK")

        for msg in others:
            ch = msg.get("channel") if msg else None
            if ch == "subscriptionResponse":
                sub = (msg.get("data") or {}).get("subscription") or {}
                if sub.get("type") == "trades" and str(sub.get("coin")).upper() == coin:
                    print(f"[{shard_name}] SUB_OK {coin}")
                    SUBSCRIBED[shard_name].add(coin)
                    metrics.note_shard_ack(shard_name, coin)
                    return True
            elif ch in {"error", "serverError"}:
                print(f"[{shard_name}] WS_ERR on {coin}: {msg}")

        await asyncio.sleep(0.02)

    mark_failure(coin, "ack-timeout", shard_name, threshold=3)
    return False


async def shard_runner(shard_name: str, coins: list, stop_event: asyncio.Event):
    backoff = RECONNECT_MIN
    toxic_symbols = set()

    while not stop_event.is_set():
        reader = None
        pinger = None
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=None,
                max_queue=None,
            ) as ws:
                backoff = RECONNECT_MIN
                SUBSCRIBED[shard_name].clear()

                q = asyncio.Queue()
                reader = asyncio.create_task(reader_task(ws, q, shard_name))
                pinger = asyncio.create_task(keepalive_task(ws, shard_name))

                await asyncio.sleep(BATCH_SETTLE)

                live_list = [c for c in coins if c not in toxic_symbols]
                next_idx = 0
                total = len(live_list)
                metrics.note_shard_target(shard_name, total)
                print(f"[{shard_name}] Subscribing in waves: total={total} wave={WAVE_SIZE} max_inflight={MAX_INFLIGHT}")

                retry_queue = []
                connection_ok = True

                while next_idx < total and not stop_event.is_set() and connection_ok:
                    wave_end = min(total, next_idx + WAVE_SIZE)
                    wave = live_list[next_idx:wave_end]

                    acked_this_wave = 0
                    for coin in wave:
                        if stop_event.is_set():
                            break
                        try:
                            ok = await subscribe_with_ack(ws, q, shard_name, coin)
                        except ConnectionError as e:
                            print(f"[{shard_name}] subscribe_with_ack aborted on {coin}: {e}")
                            toxic_symbols.add(coin)
                            connection_ok = False
                            break

                        if ok:
                            acked_this_wave += 1
                        else:
                            retry_queue.append(coin)

                        await asyncio.sleep(PER_SUB_DELAY)

                    next_idx = wave_end
                    print(f"[{shard_name}] Wave done: acked={acked_this_wave} / {len(wave)} (total={next_idx}/{total})")

                    if not connection_ok:
                        break

                    if retry_queue and not stop_event.is_set():
                        still = []
                        for coin in retry_queue:
                            try:
                                ok = await subscribe_with_ack(ws, q, shard_name, coin)
                            except ConnectionError as e:
                                print(f"[{shard_name}] retry aborted on {coin}: {e}")
                                toxic_symbols.add(coin)
                                connection_ok = False
                                break
                            if not ok:
                                still.append(coin)
                            await asyncio.sleep(PER_SUB_DELAY)
                        retry_queue = still

                    if not connection_ok:
                        break

                    if next_idx < total:
                        waited = 0.0
                        while waited < WAVE_PAUSE and not stop_event.is_set():
                            others = await drain_and_dispatch(q, shard_name)

                            for o in others:
                                if isinstance(o, dict):
                                    _handle_control_msg(o)

                            if any(o is None for o in others):
                                print(f"[{shard_name}] socket went down during wave pause")
                                connection_ok = False
                                break

                            await asyncio.sleep(0.25)
                            waited += 0.25

                if not connection_ok:
                    continue

                print(f"[{shard_name}] Subscribed (acks) = {len(SUBSCRIBED.get(shard_name, set()))} / {total}")

                last_beat = 0
                while not stop_event.is_set():
                    others = await drain_and_dispatch(q, shard_name)

                    for o in others:
                        if isinstance(o, dict):
                            _handle_control_msg(o)

                    if any(o is None for o in others):
                        break

                    now = int(dt.datetime.now(dt.timezone.utc).timestamp())
                    if now - last_beat > 30:
                        metrics.note_shard_queue(shard_name, q.qsize())
                        metrics.set_open_positions(0)
                        last_beat = now

                    await asyncio.sleep(0.01)

        except Exception as e:
            print(f"[{shard_name}] WARN socket error: {e}. Reconnecting soon …")
            await asyncio.sleep(backoff + random())
            backoff = min(RECONNECT_MAX, backoff * 1.6)

        finally:
            if reader is not None:
                reader.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await reader
            if pinger is not None:
                pinger.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await pinger


# =========================
# ORCHESTRATOR
# =========================
def partition(lst, k):
    """Split list into k nearly equal contiguous slices."""
    n = len(lst)
    if k <= 0:
        return [lst]
    size = max(1, math.ceil(n / k))
    return [lst[i:i + size] for i in range(0, n, size)]


async def run_all():
    coins = load_coins_initial()
    print(f"[BOOT] Coins={len(coins)}")
    if not coins:
        raise SystemExit("[FATAL] No coins to subscribe to.")


    shards_needed = max(1, min(MAX_SOCKETS, math.ceil(len(coins) / TARGET_COINS_PER_SOCKET)))
    slices = partition(coins, shards_needed)
    print(f"[SHARDS] {len(slices)} sockets | " + ", ".join([f"s{i}:{len(s)}" for i, s in enumerate(slices)]))

    start_ui(metrics, hz=1.0)
    asyncio.create_task(poll_perp_summary(metrics))

    stop_event = asyncio.Event()
    tasks = []
    for i, sl in enumerate(slices):
        await asyncio.sleep(STAGGER_BETWEEN_SOCKETS if i > 0 else 0.0)
        tasks.append(asyncio.create_task(shard_runner(f"S{i}", sl, stop_event)))

    async def refresher():
        nonlocal coins, tasks, stop_event
        while True:
            await asyncio.sleep(COIN_REFRESH_SECS)
            new = load_coins_refresh()
            if not new:
                continue
            if set(new) != set(coins):
                print(f"[COINS] Universe changed {len(coins)} → {len(new)}. Restarting shards …")

                stop_event.set()
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

                TEMP_BLACKLIST.clear()
                SYMBOL_FAILS.clear()
                BAR_STATE.clear()
                SUBSCRIBED.clear()

                coins = new
                stop_event = asyncio.Event()
                tasks = []

                shards_needed = max(1, min(MAX_SOCKETS, math.ceil(len(coins) / TARGET_COINS_PER_SOCKET)))
                slices = partition(coins, shards_needed)
                print(f"[SHARDS] {len(slices)} sockets | " + ", ".join([f"s{i}:{len(s)}" for i, s in enumerate(slices)]))

                for i, sl in enumerate(slices):
                    await asyncio.sleep(STAGGER_BETWEEN_SOCKETS if i > 0 else 0.0)
                    tasks.append(asyncio.create_task(shard_runner(f"S{i}", sl, stop_event)))

    ref_task = asyncio.create_task(refresher())

    try:
        while True:
            await asyncio.sleep(5.0)
    except asyncio.CancelledError:
        pass
    finally:
        stop_event.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        ref_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await ref_task


async def graceful_shutdown():
    print("[DONE] Session closed.")


if __name__ == "__main__":
    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        asyncio.run(graceful_shutdown())
