# ui_status.py
import asyncio, time, shutil, sys, atexit
from collections import defaultdict, deque
from threading import Lock
from typing import Optional
from pathlib import Path
import json, os
import io

BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = BASE_DIR / "runtime"
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOT_PATH = RUNTIME_DIR / "hud_snapshot.json"


_ALT_ON = False

def _enter_alt_screen():
    global _ALT_ON
    if _ALT_ON:
        return
    sys.stdout.write("\x1b[?1049h\x1b[?25l")
    sys.stdout.flush()
    _ALT_ON = True


def _exit_alt_screen():
    global _ALT_ON
    if not _ALT_ON:
        return
    sys.stdout.write("\x1b[?25h\x1b[?1049l")
    sys.stdout.flush()
    _ALT_ON = False


atexit.register(_exit_alt_screen)


class Metrics:
    def __init__(self):
        self._lock = Lock()

        self.shard_target = {}
        self.shard_acks = defaultdict(set)
        self.shard_qsize = defaultdict(int)

        self._flow_recent = deque()
        self._flow_ratio = 0.5
        self._flow_buy_5m = 0.0
        self._flow_sell_5m = 0.0

        self._signals_recent = deque()
        self.open_positions = 0

        self._trades_recent = deque()
        self._trades_per_coin = defaultdict(lambda: deque())
        self._last_price = {}
        self._bars_min = 0

        self._hud_hz = 5.0

        self.pnl_open_usd = 0.0
        self.pnl_open_pct = 0.0
        self.exposure_usd = 0.0
        self.realized_today_usd = 0.0
        self.win_wins = 0
        self.win_total = 0

        self.perp_summary = {
            "oi_24h": 0.0,
            "funding": 0.0,
            "coverage": 0,
            "risk": "neutral",
        }

    def note_shard_target(self, shard, n):
        with self._lock:
            self.shard_target[shard] = int(n)

    def note_shard_ack(self, shard, coin):
        with self._lock:
            self.shard_acks[shard].add(coin)

    def note_shard_queue(self, shard, qsize):
        with self._lock:
            self.shard_qsize[shard] = int(qsize)

    def note_signal(self, kind: str):
        now = time.time()
        with self._lock:
            self._signals_recent.append((now, str(kind)))

    def set_open_positions(self, n):
        with self._lock:
            self.open_positions = int(n)

    def note_last_closed_bar(self, coin, vol_usd, trades):
        with self._lock:
            self._bars_min += 1

    def note_trade(self, coin: str, ts_ms: int, px: float, sz: float, side: Optional[str] = None):
        now = time.time()
        usd = (px or 0.0) * (sz or 0.0)

        with self._lock:
            self._trades_recent.append(now)

            dq = self._trades_per_coin[coin]
            dq.append((now, usd))
            self._last_price[coin] = px

            cutoff_60s = now - 60
            while self._trades_recent and self._trades_recent[0] < cutoff_60s:
                self._trades_recent.popleft()

            cutoff_5m = now - 300
            while dq and dq[0][0] < cutoff_5m:
                dq.popleft()

            if side in ("buy", "sell"):
                self._flow_recent.append((now, side, usd))
                while self._flow_recent and self._flow_recent[0][0] < cutoff_5m:
                    self._flow_recent.popleft()

                buy = 0.0
                sell = 0.0
                for _, s, v in self._flow_recent:
                    if s == "buy":
                        buy += v
                    else:
                        sell += v

                total = buy + sell
                self._flow_buy_5m = buy
                self._flow_sell_5m = sell
                self._flow_ratio = (buy / total) if total > 0 else 0.5

    def snapshot(self):
        now = time.time()

        with self._lock:
            tps_all = len(self._trades_recent) / 60.0

            cutoff_5m = now - 300
            while self._signals_recent and self._signals_recent[0][0] < cutoff_5m:
                self._signals_recent.popleft()

            sig_counts = defaultdict(int)
            for _, kind in self._signals_recent:
                sig_counts[kind] += 1

            rows = []
            for coin, dq in self._trades_per_coin.items():
                vol5m = sum(v for _, v in dq)
                tr5m = len(dq)
                tps_c = tr5m / 300.0
                last = float(self._last_price.get(coin, 0.0))
                rows.append((coin, tps_c, vol5m, last, tr5m))

            rows.sort(key=lambda r: (r[1], r[2]), reverse=True)
            top = rows[:12]

            shards = []
            for name, target in self.shard_target.items():
                live = len(self.shard_acks.get(name, set()))
                qsize = self.shard_qsize.get(name, 0)
                shards.append((name, live, target, qsize))
            shards.sort()

            wins = self.win_wins
            total = self.win_total
            winrate = (wins / total * 100.0) if total else 0.0

            return {
                "tps_all": tps_all,
                "signals": dict(sig_counts),
                "open_positions": self.open_positions,
                "bars_min": self._bars_min,
                "top": top,
                "shards": shards,
                "subs_total": sum(live for _, live, _, _ in shards),
                "subs_target": sum(target for _, _, target, _ in shards),
                "pnl_open_usd": self.pnl_open_usd,
                "pnl_open_pct": self.pnl_open_pct,
                "exposure_usd": self.exposure_usd,
                "realized_today_usd": self.realized_today_usd,
                "win_wins": wins,
                "win_total": total,
                "winrate": winrate,
                "hud_hz": self._hud_hz,
                "flow_buy_5m": self._flow_buy_5m,
                "flow_sell_5m": self._flow_sell_5m,
                "flow_ratio": self._flow_ratio,
            }

    def market_snapshot(self):
        with self._lock:
            return {
                "flow_ratio": self._flow_ratio,
                "flow_buy_5m": self._flow_buy_5m,
                "flow_sell_5m": self._flow_sell_5m,
                "tps_all": len(self._trades_recent) / 60.0,
                "open_positions": self.open_positions,
            }


def _term_size():
    size = shutil.get_terminal_size((120, 30))
    return max(36, size.columns), max(10, size.lines)


def _term_inner_width():
    cols, _ = _term_size()
    # Keep one spare column so terminals do not wrap/chop the right edge.
    return max(34, min(158, cols - 3))


def _clip_pad(s: str, width: int) -> str:
    if width <= 0:
        return ""
    s = str(s)
    if len(s) <= width:
        return s + " " * (width - len(s))
    if width <= 1:
        return "…"
    return s[:max(0, width - 1)] + "…"


def _fmt_human(n: float) -> str:
    if n is None:
        return "—"
    n = float(n)
    if abs(n) >= 1_000_000:
        return f"{n / 1_000_000:.1f}m"
    if abs(n) >= 1_000:
        return f"{n / 1_000:.1f}k"
    return f"{n:.0f}"


def _fmt_usd(n: float) -> str:
    if n is None:
        return "$—"
    sign = "-" if n < 0 else ""
    n = abs(float(n))
    if n >= 1_000_000:
        return f"{sign}${n / 1_000_000:.1f}m"
    if n >= 1_000:
        return f"{sign}${n / 1_000:.1f}k"
    return f"{sign}${n:.0f}"


def _bar(frac: float, width: int) -> str:
    width = max(0, int(width))
    frac = max(0.0, min(1.0, float(frac)))
    filled = int(round(frac * width))
    return "█" * filled + "░" * (width - filled)


def _compact_market_row(coin, tps_c, vol5m, last, tr5m, arrow, frac, width):
    prefix = f" {coin:<8} {_fmt_human(vol5m):>8} {last:>10.4f} {arrow} "
    bar_w = max(0, width - len(prefix))
    return prefix + _bar(frac, bar_w)


def _small_market_row(coin, tps_c, vol5m, last, tr5m, arrow):
    return f" {coin:<8} vol {_fmt_human(vol5m):>7} px {last:>10.4f} {arrow}"


def _tiny_market_row(coin, tps_c, vol5m, last, tr5m, arrow):
    return f" {coin:<8} {_fmt_human(vol5m):>7} {arrow}"


def _render_small_screen(metrics: Metrics, snap: dict, term_cols: int, term_rows: int):
    width = max(32, term_cols - 3)

    status = "ok" if (
        snap["subs_total"] == (snap["subs_target"] or 0) and snap["subs_target"] > 0
    ) else "subbing"

    flow_pct = int(round((snap.get("flow_ratio") or 0.5) * 100))
    q_total = sum(q for _, _, _, q in snap["shards"])

    rows = [
        "\x1b[H\x1b[J",
        "HYPERLIQUID TERMINAL",
        "-" * min(width, 48),
        f"status        {status}",
        f"subscriptions {snap['subs_total']}/{snap['subs_target']}",
        f"sockets       {len(snap['shards'])}",
        f"queue         {q_total}",
        f"tps           {snap['tps_all']:.2f}",
        f"flow          {flow_pct}% buying",
        f"buy 5m        ${_fmt_human(snap.get('flow_buy_5m') or 0)}",
        f"sell 5m       ${_fmt_human(snap.get('flow_sell_5m') or 0)}",
        "",
        "Expand terminal for full dashboard.",
        "Recommended: 110 x 30 or larger.",
        "",
        "Ctrl+C to quit",
    ]

    rows = rows[:max(1, term_rows - 1)]
    rows = [row[:max(1, term_cols)] for row in rows]

    sys.stdout.write("\n".join(rows))
    sys.stdout.flush()

def _render(metrics: Metrics):
    buf = io.StringIO()
    write = buf.write

    arrow_up = "^"
    arrow_dn = "v"
    arrow_neu = "-"

    snap = metrics.snapshot()
    term_cols, term_rows = _term_size()

    # Leave spare room so terminal emulators do not wrap on the final column.
    width = max(34, min(150, term_cols - 3))

    if term_cols < 90 or term_rows < 24:
        _render_small_screen(metrics, snap, term_cols, term_rows)
        return

    compact = width < 110
    left = width if compact else max(50, width - 34)
    right = 0 if compact else width - left - 2

    def rule(ch="-"):
        return ch * width

    def row(left_text, right_text=""):
        if right <= 0:
            return _clip_pad(left_text, width) + "\n"
        return (
            _clip_pad(left_text, left)
            + "  "
            + _clip_pad(right_text, right)
            + "\n"
        )

    write("\x1b[H\x1b[J")

    status = "ok" if (
        snap["subs_total"] == (snap["subs_target"] or 0) and snap["subs_target"] > 0
    ) else "subbing"

    q_total = sum(q for _, _, _, q in snap["shards"])
    hdr = (
        f"HyperTerminal v1.0 - {status} - "
        f"subs {snap['subs_total']}/{snap['subs_target']} - "
        f"ws:{len(snap['shards'])} sockets - q {q_total}"
    )

    write(_clip_pad(hdr, width) + "\n")
    write(rule("=") + "\n")

    shard_bits = []
    for name, live, target, qsize in snap["shards"]:
        blen = 8 if compact else 12
        fill = int(blen * (live / max(1, target)))
        bar = "#" * fill + "." * (blen - fill)

        if compact:
            shard_bits.append(f"[{name}] {bar} {live}/{target}")
        else:
            shard_bits.append(f"[{name}] {bar} ({live}/{target}) q:{qsize}")

    shard_line = "  ".join(shard_bits)
    uptime = "UPTIME " + time.strftime("%H:%M:%S", time.gmtime(time.time() % 86400))
    write(row(shard_line, uptime))

    sigs = snap["signals"]
    sig_tot = sum(sigs.values())
    flow_pct = int(round((snap.get("flow_ratio") or 0.5) * 100))

    overview = (
        f"TPS {snap['tps_all']:.2f} | "
        f"signals 5m {sig_tot} | "
        f"pos {snap['open_positions']} | "
        f"bars/min {snap['bars_min']} | "
        f"flow {flow_pct}% buying | "
        f"buy5m ${_fmt_human(snap.get('flow_buy_5m') or 0)} | "
        f"sell5m ${_fmt_human(snap.get('flow_sell_5m') or 0)}"
    )
    write(row(overview, "SYS OK"))
    write(rule("-") + "\n")

    if compact:
        header_left = "Sym       vol5m      last        d   activity"
    else:
        header_left = (
            " Sym          t/s      vol5m         last   tr5m   d  "
            "activity"
        )
    write(row("ACTIVE MARKETS", "PERP RISK" if not compact else ""))
    write(rule("-") + "\n")
    write(row(header_left, "" if compact else "Telemetry"))
    write(rule("-") + "\n")

    perp = getattr(metrics, "perp_summary", None) or {}
    funding = float(perp.get("funding", 0.0) or 0.0)
    coverage = perp.get("coverage", 0)
    risk = perp.get("risk", "neutral")
    oi_usd = float(perp.get("oi_usd", 0.0) or 0.0)
    oi_change_pct = float(perp.get("oi_change_pct", 0.0) or 0.0)

    oi_display = f"${oi_usd / 1_000_000_000:.2f}b"
    if abs(oi_change_pct) >= 0.01:
        oi_display += f" ({oi_change_pct:+.2f}%)"

    pnl_lines = [
        f"OI total  {oi_display}",
        f"Funding   {funding:+.3f}% ({'LONGS PAY' if funding > 0 else 'SHORTS PAY'})",
        f"Coverage  {coverage} perps",
        f"Risk      {risk}",
    ]

    top = snap["top"]
    max_tr5m = max((r[4] for r in top), default=1)

    row_budget = max(4, term_rows - 10)
    rows_to_show = min(max(12, len(pnl_lines)), row_budget, len(top) if top else 12)

    if compact:
        market_width = width
    else:
        market_width = left

    sample_prefix = f" {'XXXX':<9}{0:>5.2f}  {_fmt_human(0):>9}  {0.0:>11.4f}  {0:>5}   {arrow_neu}  "
    act_w_full = max(0, market_width - len(sample_prefix))

    for i in range(rows_to_show):
        if i < len(top):
            coin, tps_c, vol5m, last, tr5m = top[i][:5]
            frac = tr5m / max_tr5m if max_tr5m > 0 else 0.0
            arrow = arrow_up if frac >= 0.55 else arrow_dn if frac <= 0.45 else arrow_neu

            if compact:
                prefix = f"{coin:<8} {_fmt_human(vol5m):>8} {last:>10.4f} {arrow} "
                bar_w = max(0, market_width - len(prefix))
                left_text = prefix + _bar(frac, bar_w)
            else:
                prefix = (
                    f" {coin:<9}{tps_c:>5.2f}  "
                    f"{_fmt_human(vol5m):>9}  {last:>11.4f}  "
                    f"{tr5m:>5}   {arrow}  "
                )
                left_text = prefix + _bar(frac, act_w_full)
        else:
            left_text = ""

        right_text = "" if compact else (pnl_lines[i] if i < len(pnl_lines) else "")
        write(row(left_text, right_text))

    if top and rows_to_show < len(top):
        write(row(f"+{len(top) - rows_to_show} more markets hidden"))

    write(rule("-") + "\n")
    write(_clip_pad("HUD refresh ~2/s - Ctrl+C to quit", width) + "\n")

    try:
        with open(SNAPSHOT_PATH, "w") as f:
            json.dump(snap, f)
    except Exception:
        pass

    frame = buf.getvalue().splitlines()
    frame = [line[:max(1, term_cols - 2)] for line in frame[:max(1, term_rows - 1)]]

    sys.stdout.write("\n".join(frame))
    sys.stdout.flush()

def _render_from_snapshot(snap):
    sys.stdout.write("\x1b[H\x1b[J")
    sys.stdout.write(
        f"Restored HUD snapshot - subs {snap.get('subs_total')}/{snap.get('subs_target')}  "
        f"| signals {sum(snap.get('signals', {}).values())} | "
        f"pos {snap.get('open_positions')} | "
        f"flow {(snap.get('flow_ratio', 0.5)) * 100:.0f}% buying\n"
    )
    sys.stdout.write("(waiting for live metrics to resume...)\n")
    sys.stdout.flush()


async def _ui_loop(metrics: Metrics, hz: float):
    metrics._hud_hz = hz

    if os.path.exists(SNAPSHOT_PATH):
        try:
            snap = json.load(open(SNAPSHOT_PATH))
            _render_from_snapshot(snap)
        except Exception:
            pass

    while True:
        _render(metrics)
        try:
            hz_now = float(getattr(metrics, "_hud_hz", hz))
        except Exception:
            hz_now = hz

        await asyncio.sleep(max(0.05, 1.0 / max(0.5, min(20.0, hz_now))))


def start(metrics: Metrics, hz: float = 2.0):
    _enter_alt_screen()
    asyncio.create_task(_ui_loop(metrics, hz))
