"""
Scheduled market intelligence reports.

Fires at every :00 and :30 mark from 07:00 to 16:30 ET on weekdays.
- 07:00–09:00 ET  → PREMARKET report
- 09:30–16:00 ET  → MARKET report
- 16:00–16:30 ET  → EOD report

If fresh signals exist in the last 30 min → structured Channel B report.
If not → concise MARKET SNAPSHOT with carry-forward context.

Spam control: if a manual batch/aggregated report was already sent in the
same 30-min slot, the scheduled report is skipped unless fresh signals exist.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time as dtime
from typing import Awaitable, Callable, Optional

import pytz

from app.batch import BatchEntry, BatchStore, _analyze
from app.rs_engine import compute_rs, MarketRS

logger = logging.getLogger(__name__)

_ET             = pytz.timezone("America/New_York")
_SCHEDULE_START = dtime(7, 0)
_SCHEDULE_END   = dtime(16, 30)
_MARKET_OPEN    = dtime(9, 30)
_MARKET_CLOSE   = dtime(16, 0)

# NYSE holidays — update annually.  Source: NYSE holiday schedule.
_NYSE_HOLIDAYS: frozenset[str] = frozenset({
    # 2025
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18",
    "2025-05-26", "2025-06-19", "2025-07-04", "2025-09-01",
    "2025-11-27", "2025-12-25",
    # 2026
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
    "2026-11-26", "2026-12-25",
})

_SendFn = Callable[[str, str], Awaitable[None]]   # post_to_b(text, label)


# ── Slot helpers ──────────────────────────────────────────────────────────────

def _now_et() -> datetime:
    return datetime.now(_ET)


def _slot_key(dt: datetime) -> str:
    """Floor to nearest 30-min boundary → '2026-04-14_09:30'."""
    minute = 0 if dt.minute < 30 else 30
    return dt.strftime(f"%Y-%m-%d_%H:{minute:02d}")


def _warn_holiday_coverage() -> None:
    """
    Warn if it is December and the next calendar year has no entries in
    _NYSE_HOLIDAYS.  Safe to call repeatedly — the log rate is controlled
    by the caller.
    """
    now = _now_et()
    if now.month != 12:
        return
    next_year = now.year + 1
    if not any(s.startswith(f"{next_year}-") for s in _NYSE_HOLIDAYS):
        logger.warning(
            "NYSE holiday list is missing %d dates — add them before Jan 1 "
            "(update _NYSE_HOLIDAYS in scheduler.py)",
            next_year,
        )


def _in_schedule(dt: datetime) -> bool:
    if dt.weekday() >= 5:
        return False
    if dt.strftime("%Y-%m-%d") in _NYSE_HOLIDAYS:
        return False
    t = dt.time().replace(second=0, microsecond=0)
    return _SCHEDULE_START <= t <= _SCHEDULE_END


def _report_type(dt: datetime) -> str:
    t = dt.time().replace(second=0, microsecond=0)
    if t < _MARKET_OPEN:
        return "PREMARKET"
    if t >= _MARKET_CLOSE:
        return "EOD"
    return "MARKET"


def _seconds_until_next_slot() -> float:
    """Seconds until the next :00 or :30 boundary (minimum 5s)."""
    now = _now_et()
    if now.minute < 30:
        nxt = now.replace(minute=30, second=0, microsecond=0)
    else:
        nxt = (now.replace(minute=0, second=0, microsecond=0)
               + timedelta(hours=1))
    return max(5.0, (nxt - now).total_seconds())


# ── Signal window — rolling 30-min accumulator ───────────────────────────────

@dataclass
class _Stamped:
    ts: datetime
    entry: BatchEntry


class SignalWindow:
    """
    Holds all BatchEntry objects added in the last 30 minutes.
    Thread-safe for single asyncio event loop use.
    """

    def __init__(self, window_minutes: int = 30) -> None:
        self._window   = timedelta(minutes=window_minutes)
        self._items:   list[_Stamped] = []

    def add(self, entry: BatchEntry) -> None:
        self._items.append(_Stamped(ts=datetime.utcnow(), entry=entry))

    def fresh(self) -> list[BatchEntry]:
        cutoff = datetime.utcnow() - self._window
        self._items = [s for s in self._items if s.ts >= cutoff]
        return [s.entry for s in self._items]

    def clear(self) -> None:
        self._items.clear()


# ── Carry-forward context ─────────────────────────────────────────────────────

@dataclass
class _Context:
    direction: str       = "NEUTRAL"
    leaders:   list[str] = field(default_factory=list)
    laggards:  list[str] = field(default_factory=list)
    last_slot: str       = ""


# ── Snapshot formatter ────────────────────────────────────────────────────────

def _fmt_snapshot(rtype: str, ctx: _Context, slot: str) -> str:
    time_str = slot.split("_")[-1]   # "09:30"
    lines = [
        f"{rtype} SNAPSHOT — {time_str} ET",
        "- No major new flow in last 30 min",
    ]
    if ctx.direction != "NEUTRAL":
        lines.append(f"- Previous dominant bias: {ctx.direction}")
    if ctx.leaders:
        lines.append(f"- Leaders: {', '.join(ctx.leaders)}")
    if ctx.laggards:
        lines.append(f"- Laggards: {', '.join(ctx.laggards)}")

    if ctx.direction == "NEUTRAL" or not ctx.leaders:
        stance = "WAIT"
    elif ctx.direction == "BULLISH" and len(ctx.leaders) >= 2:
        stance = "TREND"
    elif ctx.leaders and ctx.laggards:
        stance = "HEDGE"
    else:
        stance = "WAIT"

    lines.append(f"- Stance: {stance}")
    return "\n".join(lines)


# ── Structured report formatter (from raw BatchEntry list) ───────────────────

def _fmt_p(usd: float) -> str:
    if usd >= 1_000_000:
        return f"${usd / 1_000_000:.1f}M"
    if usd >= 1_000:
        return f"${usd / 1_000:.0f}K"
    return f"${usd:.0f}"


async def _fmt_structured(
    rtype: str,
    entries: list[BatchEntry],
    slot: str,
    market=None,
) -> str:
    """Format a structured report from fresh BatchEntry objects."""
    from app.telegram_handler import format_channel_b_report  # avoid circular at import time
    analysis = _analyze(entries)
    if not analysis:
        return ""
    time_str = slot.split("_")[-1]
    header   = f"{rtype} REPORT — {time_str} ET\n"

    rs_data = None
    if market:
        tickers = list(dict.fromkeys(e.ticker for e in entries))[:5]
        rs_data = await compute_rs(
            market,
            analysis.get("direction", "NEUTRAL"),
            analysis.get("confidence", 0),
            tickers,
        )

    body = format_channel_b_report(analysis, rs_data=rs_data)
    return header + body if body else ""


# ── EOD report formatter ─────────────────────────────────────────────────────

async def _fmt_eod(
    entries: list[BatchEntry],
    ctx: _Context,
    slot: str,
    market=None,
) -> str:
    """
    End-of-day summary.  Always produces output — renders gracefully with
    zero entries, zero actionable contracts, weak/mixed flow, or no Alpaca data.
    """
    date_str = slot.split("_")[0]          # "2026-04-14"
    sep      = "─" * 30

    lines = [f"EOD SUMMARY — {date_str}", sep]

    # ── Session direction from carry-forward context ──────────────────────────
    direction = ctx.direction
    if direction == "BULLISH":
        lines.append("Session bias: BULLISH")
    elif direction == "BEARISH":
        lines.append("Session bias: BEARISH")
    else:
        lines.append("Session bias: NEUTRAL / Mixed")

    if ctx.leaders:
        lines.append(f"Leaders:  {', '.join(ctx.leaders)}")
    if ctx.laggards:
        lines.append(f"Laggards: {', '.join(ctx.laggards)}")

    # ── Last 30-min flow ──────────────────────────────────────────────────────
    lines.append("")
    if entries:
        bulls = [e for e in entries if e.side == "CALL"]
        bears = [e for e in entries if e.side == "PUT"]
        lines.append(f"Last 30-min flow: {len(entries)} signal(s)")
        if bulls:
            lines.append(f"  Calls: {', '.join(e.ticker for e in bulls[:3])}")
        if bears:
            lines.append(f"  Puts:  {', '.join(e.ticker for e in bears[:3])}")
    else:
        lines.append("Last 30-min flow: No new signals")

    # ── Market close RS (best-effort — skipped gracefully on failure) ─────────
    if market:
        try:
            flow_dir = direction if direction in ("BULLISH", "BEARISH") else "NEUTRAL"
            tickers  = list(dict.fromkeys(e.ticker for e in entries))[:5]
            rs_data  = await compute_rs(market, flow_dir, 0, tickers)
            idx      = rs_data.indices if (rs_data and rs_data.data_ok) else None
            if idx and idx.data_ok:
                def _pos(above):
                    if above is True:  return "above VWAP"
                    if above is False: return "below VWAP"
                    return "N/A"
                lines.append("")
                lines.append(
                    f"Close: SPY {_pos(idx.spy_above_vwap)}"
                    f" · QQQ {_pos(idx.qqq_above_vwap)}"
                    f" · IWM {_pos(idx.iwm_above_vwap)}"
                )
                if rs_data.market_state not in (None, "NO_DATA"):
                    lines.append(f"Market state: {rs_data.market_state}")
        except Exception as exc:
            logger.warning("EOD RS fetch failed (skipped): %s", exc)

    # ── Closing stance ────────────────────────────────────────────────────────
    lines.append("")
    if direction == "BULLISH" and ctx.leaders:
        stance = "BULLISH CLOSE — monitor overnight for continuation"
    elif direction == "BEARISH" and ctx.laggards:
        stance = "BEARISH CLOSE — watch for gap-down risk"
    else:
        stance = "NEUTRAL CLOSE — no clear overnight bias"
    lines.append(f"Stance: {stance}")

    return "\n".join(lines)


# ── Scheduler ─────────────────────────────────────────────────────────────────

class Scheduler:
    """
    Fires a scheduled report to Channel B at every :00/:30 mark
    within the 07:00–16:30 ET window on weekdays.
    """

    def __init__(self, window: SignalWindow, send_fn: _SendFn, market=None) -> None:
        self._window   = window
        self._send     = send_fn
        self._market   = market   # Optional MarketDataService — enables RS in scheduled reports
        self._ctx      = _Context()
        self._manual_slots: set[str] = set()   # slots where manual send happened
        self._fired_slots:  set[str] = set()   # slots where scheduled report fired
        self._holiday_warn_date: str = ""       # date of last holiday-coverage warning

    # ── Public API ────────────────────────────────────────────────────────────

    def mark_manual_send(self) -> None:
        """Call whenever a manual batch/aggregated report is sent to Channel B."""
        self._manual_slots.add(_slot_key(_now_et()))

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        logger.info(
            "Scheduler started | window=07:00–16:30 ET | interval=30min | weekdays only"
        )
        _warn_holiday_coverage()   # once at process start
        while True:
            delay = _seconds_until_next_slot()
            next_et = _now_et() + timedelta(seconds=delay)
            logger.info(
                "Scheduler: next slot in %.0fs → %s ET",
                delay, next_et.strftime("%H:%M"),
            )
            await asyncio.sleep(delay)

            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Scheduler tick error (continuing): %s", exc, exc_info=True)

    # ── Tick ──────────────────────────────────────────────────────────────────

    async def _tick(self) -> None:
        now      = _now_et()
        today    = now.strftime("%Y-%m-%d")
        slot     = _slot_key(now)
        rtype    = _report_type(now)

        # Re-check holiday coverage at most once per calendar day
        if today != self._holiday_warn_date:
            _warn_holiday_coverage()
            self._holiday_warn_date = today

        if not _in_schedule(now):
            logger.debug("Scheduler: outside window (%s) — skip", now.strftime("%H:%M"))
            return

        # Dedup: one report per slot regardless of type
        if slot in self._fired_slots:
            logger.debug("Scheduler: slot %s already fired — skip", slot)
            return

        entries = self._window.fresh()

        logger.info(
            "Scheduled report fired | slot=%s | type=%s | fresh_signals=%d",
            slot, rtype, len(entries),
        )

        # ── EOD: always force-send, skip spam control, skip signal threshold ──
        if rtype == "EOD":
            text  = await _fmt_eod(entries, self._ctx, slot, market=self._market)
            label = "SCHEDULED_EOD"
            # text is guaranteed non-empty by _fmt_eod, but guard just in case
            if not text:
                text = _fmt_snapshot(rtype, self._ctx, slot)
            await self._send(text, label)
            self._fired_slots.add(slot)
            self._prune_slots()
            return

        # ── Normal slots: spam control + signal threshold ─────────────────────

        # Spam control: slot was manually covered AND no new signals
        if slot in self._manual_slots and not entries:
            logger.info(
                "Scheduler: slot %s already covered by manual send, no new signals — skip",
                slot,
            )
            self._fired_slots.add(slot)
            return

        MIN_SIGNALS = 1 if rtype == "PREMARKET" else 2
        if len(entries) >= MIN_SIGNALS:
            text = await _fmt_structured(rtype, entries, slot, market=self._market)
            label = f"SCHEDULED_{rtype}"
            if not text:
                # structured format failed — fall back to snapshot
                logger.warning("Scheduled structured format empty — using snapshot | slot=%s", slot)
                text = _fmt_snapshot(rtype, self._ctx, slot)
                label = f"SCHEDULED_{rtype}_SNAPSHOT"
            else:
                self._update_context(entries)
        else:
            text  = _fmt_snapshot(rtype, self._ctx, slot)
            label = f"SCHEDULED_{rtype}_SNAPSHOT"

        if text:
            await self._send(text, label)
            self._fired_slots.add(slot)
            self._prune_slots()
        else:
            logger.warning("Scheduler: empty report for slot %s — not sent", slot)

    # ── Slot pruning ──────────────────────────────────────────────────────────

    def _prune_slots(self) -> None:
        """Discard slot keys older than 12 hours to avoid unbounded growth."""
        cutoff = (_now_et() - timedelta(hours=12)).strftime("%Y-%m-%d_%H:%M")
        self._fired_slots  = {s for s in self._fired_slots  if s >= cutoff}
        self._manual_slots = {s for s in self._manual_slots if s >= cutoff}

    # ── Context update ────────────────────────────────────────────────────────

    def _update_context(self, entries: list[BatchEntry]) -> None:
        bulls = [e for e in entries if e.side == "CALL"]
        bears = [e for e in entries if e.side == "PUT"]
        self._ctx.direction = "BULLISH" if len(bulls) >= len(bears) else "BEARISH"
        self._ctx.leaders   = list({e.ticker for e in sorted(bulls, key=lambda x: x.priority)[:3]})
        self._ctx.laggards  = list({e.ticker for e in sorted(bears, key=lambda x: x.priority)[:3]})
