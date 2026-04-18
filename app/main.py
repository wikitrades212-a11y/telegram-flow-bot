"""
Entry point — python-telegram-bot v20+ (Bot Token mode).

Setup requirements:
  1. Create a bot via @BotFather → get BOT_TOKEN
  2. Add the bot as ADMIN to Channel A (needs "Read Messages")
  3. Add the bot as ADMIN to Channel B (needs "Post Messages")
  4. Set BOT_TOKEN, SOURCE_CHANNEL, DEST_CHANNEL in .env

Flow:
  - Telegram pushes channel_post updates to the bot
  - Every new message from Channel A is parsed into FlowSignal
  - Signals accumulate in BatchStore; on threshold, format_channel_b_report()
    is called and sent as a NEW message to Channel B (no forwarding)
  - Pre-market mode (07:00–09:29 ET): lower thresholds + ticker tracking
  - Forced 8:30 AM pre-market report regardless of flow volume
"""

import asyncio
import hashlib
import logging
import os
import re
import signal
import sys
import uuid
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import pytz as _pytz
from telegram import Update
from telegram.ext import (
    Application, MessageHandler, CommandHandler,
    ContextTypes, filters,
)

import config
from config import validate_env
from app.parser import parse_flow_message
from app.market_data import MarketDataService, _is_trading_session
from app.decision_engine import Decision, DecisionEngine
from app.risk import compute_targets
from app.watcher import Watcher
from app.telegram_handler import (
    format_go,
    format_channel_b_report,
    format_premarket_report,
    format_aggregated_report_b,
    format_batch_report,
    format_stats,
    format_flow_summary,
    format_bias_only,
    format_single_future_plan,
    format_hot_options,
    format_no_flow_snapshot,
    _TECH_TICKERS,
    _INDEX_HEDGE_TICKERS,
)
from app.intel_parser import is_aggregated_report, parse_intel_report
from app.session import current_session
from app.rs_engine import compute_rs
from app.scheduler import Scheduler, SignalWindow, _slot_key
from app.classifier import classify_flow
from app.intel_formatter import format_intel
from app.batch import BatchStore, BatchEntry, _analyze
from app.storage import (
    init_db, was_sent, mark_sent,
    record_signal, update_signal_go, update_price_check,
    get_signal_entry, update_outcome, get_stats_summary,
    record_event,
)
from app.backup import restore_db, backup_db, backup_loop
from app.tradier import fetch_option_quote


# ── Aggregated report → BatchEntry converter ─────────────────────────────────

def _intel_entries_to_batch(report) -> list[BatchEntry]:
    """
    Convert FlowEntry objects from a parsed IntelReport into BatchEntry objects
    so they can be added to sig_window and scored by /options commands.

    FlowEntry lacks score, classification, signal_role, and priority — we derive
    reasonable values from what's available (ticker, side, tag, premium, delta, dte).
    """
    from app.classifier import MARKET_TICKERS, SECTOR_TICKERS

    _TAG_TO_CLS = {
        "HEDGE":       "HEDGE_DIRECTIONAL",
        "POSITIONAL":  None,          # resolved per side below
        "SPEC":        "SPECULATIVE_DIRECTIONAL",
        "SPECULATIVE": "SPECULATIVE_DIRECTIONAL",
        "CONTINUATION":"CONTINUATION_STRONG",
        "SWEEP":       "SPECULATIVE_DIRECTIONAL",
    }

    result: list[BatchEntry] = []
    seen: set[str] = set()

    # Prefer top_overall; fall back to bulls + bears (avoid double-counting)
    all_entries = report.top_overall or (report.top_bulls + report.top_bears)

    for e in all_entries:
        uid = f"{e.ticker}_{e.side}_{e.strike}_{e.dte}"
        if uid in seen:
            continue
        seen.add(uid)

        # Signal role
        if e.ticker in MARKET_TICKERS:
            role = "MARKET_SIGNAL"
        elif e.ticker in SECTOR_TICKERS:
            role = "SECTOR_SIGNAL"
        else:
            role = "SPECULATIVE_PLAY"

        # Classification from tag
        tag_upper = (e.tag or "").upper().strip()
        cls = _TAG_TO_CLS.get(tag_upper)
        if cls is None:
            if tag_upper == "POSITIONAL":
                cls = "POSITIONAL_BULL" if e.side == "CALL" else "POSITIONAL_BEAR"
            elif role == "MARKET_SIGNAL":
                cls = "HEDGE_DIRECTIONAL" if e.side == "PUT" else "POSITIONAL_BULL"
            else:
                cls = "SPECULATIVE_DIRECTIONAL"

        # Priority
        if cls in ("HEDGE_DIRECTIONAL", "POSITIONAL_BULL", "POSITIONAL_BEAR"):
            pri = 1
        elif cls == "SPECULATIVE_DIRECTIONAL":
            pri = 2
        else:
            pri = 3

        # Downgrade SPECULATIVE_PLAY to NOISE at priority 5
        if pri >= 5 and role == "SPECULATIVE_PLAY":
            role = "NOISE"

        direction = "BULLISH" if e.side == "CALL" else "BEARISH"
        delta_val = e.delta if (e.delta and e.delta != 0.0) else None

        result.append(BatchEntry(
            signal_id=uid,
            ticker=e.ticker,
            side=e.side,
            premium_usd=e.premium_usd,
            score=80,            # aggregated entries already passed upstream filters
            classification=cls,
            signal_role=role,
            priority=pri,
            decision="HOLD",
            strike=e.strike,
            iv_pct=e.iv_pct,
            vol_oi_ratio=e.vol_oi_ratio,
            delta=delta_val,
            dte=e.dte,
            direction=direction,
        ))

    return result


# ── Logging ───────────────────────────────────────────────────────────────────

def _setup_logging() -> None:
    fmt = "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s"
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    fh = logging.FileHandler(log_dir / "flow_bot.log", encoding="utf-8")
    fh.setFormatter(logging.Formatter(fmt))
    handlers.append(fh)
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)
_INSTANCE_ID = uuid.uuid4().hex[:8]

# ── HTTP command server ────────────────────────────────────────────────────────
# Populated inside main() once closures over sig_window / market are ready.
_CMD: dict = {}

try:
    from fastapi import FastAPI as _FA, Header as _FH, HTTPException as _FE
    import uvicorn as _UV

    _http_app = _FA(docs_url=None, redoc_url=None)

    @_http_app.get("/health")
    def _http_health():
        return {"ok": True, "service": "sz-flow-bot"}

    @_http_app.get("/commands")
    def _http_list():
        return {"commands": sorted(_CMD)}

    @_http_app.get("/command/{cmd}")
    async def _http_cmd(cmd: str, x_token: str = _FH(default="")):
        if config.COMMAND_TOKEN and x_token != config.COMMAND_TOKEN:
            raise _FE(status_code=401, detail="Unauthorized")
        fn = _CMD.get(cmd)
        if fn is None:
            raise _FE(status_code=404, detail=f"Unknown command: {cmd}")
        try:
            text = await fn()
            return {"ok": True, "text": text, "cmd": cmd}
        except Exception as exc:
            logger.error("HTTP command %s failed: %s", cmd, exc, exc_info=True)
            raise _FE(status_code=500, detail=str(exc))

    _HTTP_OK = True
    logger.info("HTTP command server ready (fastapi+uvicorn found)")
except ImportError:
    _http_app = None
    _HTTP_OK  = False
    logger.warning("fastapi/uvicorn not installed — HTTP command server disabled")
_ET = _pytz.timezone("America/New_York")


# ── Time helpers ──────────────────────────────────────────────────────────────

def _now_et() -> datetime:
    return datetime.now(_ET)


def _is_premarket() -> bool:
    """True between 07:00 and 09:29 ET on weekdays."""
    now = _now_et()
    if now.weekday() >= 5:
        return False
    from datetime import time as dtime
    return dtime(7, 0) <= now.time() < dtime(9, 30)


def _seconds_until_830() -> float:
    """Seconds until next 08:30 ET weekday."""
    now = _now_et()
    target = now.replace(hour=8, minute=30, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    while target.weekday() >= 5:
        target += timedelta(days=1)
    return (target - now).total_seconds()


# ── Channel ID helpers ────────────────────────────────────────────────────────

def _normalize_chat_id(value: str) -> str:
    """Return a comparable string key: numeric IDs stripped of leading zeros,
    @usernames lowercased."""
    v = value.strip()
    if v.startswith("@"):
        return v.lower()
    try:
        return str(int(v))   # normalise -1001234 == -1001234
    except ValueError:
        return v.lower()


def _validate_channel_routing() -> None:
    """
    Fail fast if SOURCE_CHANNEL overlaps with DEST_CHANNEL or INTEL_CHANNEL.
    Logs all three resolved values so misconfigs are obvious in the startup log.
    """
    src   = _normalize_chat_id(config.SOURCE_CHANNEL)
    dest  = _normalize_chat_id(config.DEST_CHANNEL)
    intel = _normalize_chat_id(config.INTEL_CHANNEL) if config.INTEL_CHANNEL else None

    logger.info(
        "Channel routing | SOURCE=%s  DEST=%s  INTEL=%s",
        src, dest, intel or "(disabled)",
    )

    if src == dest:
        raise EnvironmentError(
            f"ROUTING ERROR: SOURCE_CHANNEL and DEST_CHANNEL are the same ({src}). "
            "Bot output would loop back into the source. Fix your .env."
        )
    if intel and src == intel:
        raise EnvironmentError(
            f"ROUTING ERROR: SOURCE_CHANNEL and INTEL_CHANNEL are the same ({src}). "
            "Intel posts would loop back into the source. Fix your .env."
        )
    if intel and intel == dest:
        logger.warning(
            "ROUTING WARNING: INTEL_CHANNEL and DEST_CHANNEL are the same (%s). "
            "Both intel posts and batch reports go to the same channel.", intel
        )


def _is_source_channel(chat) -> bool:
    src = config.SOURCE_CHANNEL.strip()
    if src.startswith("@"):
        return (chat.username or "").lower() == src.lstrip("@").lower()
    try:
        return str(chat.id) == str(src)
    except Exception:
        return False


# ── Duplicate guard ───────────────────────────────────────────────────────────

def _fingerprint(text: str) -> str:
    """Stable hash of normalised text."""
    normalized = re.sub(r"[ \t]+", " ", text.strip())
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


class DuplicateGuard:
    """
    Suppresses duplicate Channel B sends.

    - SCHEDULED_* labels  → slot-keyed: same fingerprint in same 30-min slot = duplicate
    - AGGREGATED_* labels → time-keyed: same fingerprint within cooldown window = duplicate
    - All other labels    → not checked (GO alerts, HOLD, etc. always pass through)
    """

    _AGGREGATED_COOLDOWN = timedelta(minutes=10)

    def __init__(self) -> None:
        # slot_key → last fingerprint sent
        self._slot_fp:   dict[str, str]      = {}
        # fingerprint → timestamp of last send (for aggregated cooldown)
        self._recent_fp: dict[str, datetime] = {}

    def check(self, text: str, label: str) -> tuple[bool, str]:
        """
        Returns (should_send: bool, reason: str).
        reason is one of: SENT | SKIPPED_DUPLICATE | SKIPPED_ALREADY_COVERED
        """
        fp = _fingerprint(text)
        now = _now_et()

        if label.startswith("SCHEDULED_"):
            slot = _slot_key(now)
            if self._slot_fp.get(slot) == fp:
                return False, "SKIPPED_DUPLICATE"
            # Different fingerprint in same slot — check if slot was already covered
            if slot in self._slot_fp:
                return False, "SKIPPED_ALREADY_COVERED"
            return True, "SENT"

        if label.startswith("AGGREGATED_"):
            last_sent = self._recent_fp.get(fp)
            if last_sent and (now - last_sent) < self._AGGREGATED_COOLDOWN:
                return False, "SKIPPED_DUPLICATE"
            return True, "SENT"

        return True, "SENT"

    def record(self, text: str, label: str) -> None:
        """Call after a successful send to update internal state."""
        fp  = _fingerprint(text)
        now = _now_et()
        if label.startswith("SCHEDULED_"):
            self._slot_fp[_slot_key(now)] = fp
            # Prune slots older than 12 h
            cutoff = (now - timedelta(hours=12)).strftime("%Y-%m-%d_%H:%M")
            self._slot_fp = {k: v for k, v in self._slot_fp.items() if k >= cutoff}
        if label.startswith("AGGREGATED_"):
            self._recent_fp[fp] = now
            # Prune fingerprints older than cooldown
            self._recent_fp = {
                k: v for k, v in self._recent_fp.items()
                if (now - v) < self._AGGREGATED_COOLDOWN * 2
            }


# ── Pre-market tracker ────────────────────────────────────────────────────────

class PremarketTracker:
    """Accumulates pre-market signals; resets each day at first use after midnight."""

    def __init__(self) -> None:
        self._batch = BatchStore(trigger_count=999)
        self._ticker_counts: Counter = Counter()
        self._reset_date: str = ""

    def _maybe_reset(self) -> None:
        today = _now_et().strftime("%Y-%m-%d")
        if today != self._reset_date:
            self._batch = BatchStore(trigger_count=999)
            self._ticker_counts.clear()
            self._reset_date = today

    def add(self, sig, classification: str, role: str, priority: int) -> None:
        self._maybe_reset()
        self._batch.add(sig, classification, role, priority, "HOLD")
        self._ticker_counts[sig.ticker] += 1

    def snapshot(self) -> dict:
        self._maybe_reset()
        if self._batch.size() == 0:
            return {}
        return self._batch.analyze_and_reset()

    def peek(self) -> dict:
        """Return current analysis without clearing the accumulator."""
        self._maybe_reset()
        if self._batch.size() == 0:
            return {}
        return self._batch.analyze_peek()

    def overnight_notes(self) -> list[str]:
        notes = []
        for ticker, count in self._ticker_counts.most_common(3):
            if count >= 2:
                notes.append(f"Overnight positioning: {ticker} repeated flow ({count}x)")
        return notes


# ── Price check task ──────────────────────────────────────────────────────────

async def _price_check_task(
    signal_id: str, ticker: str, side: str, market: MarketDataService
) -> None:
    now_et   = _now_et()
    eod_et   = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    eod_secs = int((eod_et - now_et).total_seconds())

    checks = [(300, "price_5m"), (900, "price_15m"), (1800, None), (3600, "price_1h")]
    if 60 < eod_secs < 28800:
        checks.append((eod_secs, "price_eod"))

    elapsed = 0
    for delay, col in checks:
        await asyncio.sleep(delay - elapsed)
        elapsed = delay
        try:
            snap = await market.snapshot(ticker)
            if snap.price is not None:
                if col is not None:
                    update_price_check(signal_id, col, snap.price)
                if delay == 1800:
                    row = get_signal_entry(signal_id)
                    if row:
                        ep = row[0] if row[0] is not None else row[1]
                        if ep and ep > 0:
                            mv = (snap.price - ep) / ep if side == "CALL" else (ep - snap.price) / ep
                            res = "WIN" if mv >= 0.005 else "LOSS" if mv <= -0.005 else "FLAT"
                            update_outcome(signal_id, res, round(mv, 6))
                            logger.info("Outcome | %s | %s | move=%.2f%%", signal_id, res, mv * 100)
        except Exception as exc:
            logger.debug("Price check failed | %s | %s: %s", signal_id, col or "30m", exc)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    _setup_logging()
    validate_env()
    _validate_channel_routing()

    market     = MarketDataService(
        cache_ttl_seconds=config.MARKET_DATA_CACHE_TTL,
        stale_ttl_seconds=config.MARKET_DATA_STALE_TTL,
    )
    engine     = DecisionEngine(market)
    batch      = BatchStore(trigger_count=config.BATCH_SIGNAL_COUNT)
    pm_tracker = PremarketTracker()
    sig_window = SignalWindow(window_minutes=30)
    dedup      = DuplicateGuard()

    application = Application.builder().token(config.BOT_TOKEN).build()

    # ── Send helpers ──────────────────────────────────────────────────────────

    _src_id   = _normalize_chat_id(config.SOURCE_CHANNEL)
    _dest_id  = _normalize_chat_id(config.DEST_CHANNEL)
    _intel_id = _normalize_chat_id(config.INTEL_CHANNEL) if config.INTEL_CHANNEL else None

    # Mutable ref so post_to_b can call scheduler.mark_manual_send() after
    # scheduler is created below.
    _scheduler_ref: list[Scheduler] = []

    async def post_to_b(text: str, label: str = "") -> None:
        """Send a NEW plain-text message to Channel B. Never forwards. Never writes to source."""
        if not text:
            logger.warning("post_to_b called with empty text | label=%s", label)
            return
        if _dest_id == _src_id:
            logger.error(
                "ROUTING GUARD: post_to_b aborted — DEST_CHANNEL equals SOURCE_CHANNEL (%s). "
                "This would create a feedback loop. Check your .env.",
                _dest_id,
            )
            return

        # ── Duplicate suppression ─────────────────────────────────────────────
        should_send, reason = dedup.check(text, label)
        if not should_send:
            logger.info(
                "Channel B send %s | label=%s | fp=%s",
                reason, label or "?", _fingerprint(text),
            )
            return

        logger.info(
            "post_to_b → chat_id=%s | label=%s | chars=%d | status=SENT",
            config.DEST_CHANNEL, label or "?", len(text),
        )
        try:
            sent = await application.bot.send_message(
                chat_id=config.DEST_CHANNEL,
                text=text,
            )
            dedup.record(text, label)
            record_event("CHANNEL_B", text, label=label or None)
            logger.info(
                "Channel B send OK | tg_msg_id=%d | label=%s",
                sent.message_id, label or "?",
            )
            # Mark slot as manually covered for spam control (only for non-scheduled sends)
            if _scheduler_ref and not label.startswith("SCHEDULED_"):
                _scheduler_ref[0].mark_manual_send()
        except Exception as exc:
            logger.error(
                "Channel B send FAILED | dest=%s | label=%s | error: %s",
                config.DEST_CHANNEL, label or "?", exc,
                exc_info=True,
            )

    async def post_to_a(text: str, signal_id: str = "") -> None:
        if not config.INTEL_CHANNEL:
            return
        # Guard: refuse to write intel back into source channel
        if _intel_id == _src_id:
            logger.error(
                "ROUTING GUARD: post_to_a aborted — INTEL_CHANNEL equals SOURCE_CHANNEL (%s). "
                "Check your .env.",
                _intel_id,
            )
            return
        logger.info(
            "post_to_a → chat_id=%s | signal=%s",
            config.INTEL_CHANNEL, signal_id or "?",
        )
        try:
            await application.bot.send_message(
                chat_id=config.INTEL_CHANNEL,
                text=text,
            )
            record_event("CHANNEL_A", text, signal_id=signal_id or None)
        except Exception as exc:
            logger.warning(
                "Channel A (intel) send FAILED | chat_id=%s | signal=%s | error: %s",
                config.INTEL_CHANNEL, signal_id or "?", exc,
            )

    # ── GO callback for watcher ───────────────────────────────────────────────

    async def on_go(sig, dec: Decision) -> None:
        dec = compute_targets(sig, dec)
        update_signal_go(sig.signal_id, dec.price, datetime.utcnow().isoformat())
        await post_to_b(format_go(sig, dec), label="GO")

    watcher = Watcher(engine=engine, on_go=on_go)

    # ── Forced 8:30 AM pre-market report ─────────────────────────────────────

    async def _forced_830_loop() -> None:
        while True:
            delay = _seconds_until_830()
            logger.info("Next forced pre-market report in %.0f seconds", delay)
            await asyncio.sleep(delay)
            if _now_et().weekday() >= 5:
                continue
            logger.info("Firing forced 8:30 AM pre-market bias report")
            try:
                analysis = pm_tracker.snapshot()
                notes    = pm_tracker.overnight_notes()
                report   = format_premarket_report(analysis, overnight_notes=notes)
                await post_to_b(report, label="PREMARKET_8:30")
            except Exception as exc:
                logger.error("Forced 8:30 report failed: %s", exc, exc_info=True)

    # ── /stats command ────────────────────────────────────────────────────────

    async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        args   = context.args or []
        days   = 7
        ticker = None
        cls_f  = None
        i = 0
        while i < len(args):
            a = args[i].lower()
            if a.endswith("d") and a[:-1].isdigit():
                days = int(a[:-1])
            elif a == "ticker" and i + 1 < len(args):
                ticker = args[i + 1].upper(); i += 1
            elif a == "class" and i + 1 < len(args):
                cls_f = args[i + 1].upper(); i += 1
            i += 1
        try:
            summary = get_stats_summary(days=days, ticker=ticker, classification=cls_f)
            await update.message.reply_text(format_stats(summary))
        except Exception as exc:
            logger.error("Stats command failed: %s", exc, exc_info=True)
            await update.message.reply_text("Error running stats. Check logs.")

    application.add_handler(CommandHandler("stats", stats_command))

    # ── On-demand command layer ───────────────────────────────────────────────
    #
    # Coexists with the automatic scan pipeline.  All commands reply to the
    # requesting chat; /report also caches output for /last.
    #
    # Permission model: if ALLOWED_USERS is set, only listed user IDs may
    # trigger commands.  If it is empty, any user who can message the bot
    # is allowed (suitable for private bots).

    _report_cache: dict = {"text": "", "label": "", "ts": None}

    def _is_allowed(update: Update) -> bool:
        if not config.ALLOWED_USERS:
            return True
        user = update.effective_user
        return user is not None and user.id in config.ALLOWED_USERS

    async def _deny(update: Update) -> None:
        await update.message.reply_text("Not authorized.")

    # ── /report — force fresh scan → full structured report ───────────────────

    async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not _is_allowed(update):
            await _deny(update); return
        await update.message.reply_text("Scanning… building fresh report.")
        try:
            entries = sig_window.fresh()
            if entries:
                analysis = _analyze(entries)
                tickers  = list(dict.fromkeys(e.ticker for e in entries))[:5]
                rs_data  = await compute_rs(
                    market,
                    analysis.get("direction", "NEUTRAL"),
                    analysis.get("confidence", 0),
                    tickers,
                )
                report = format_channel_b_report(analysis, rs_data=rs_data)
            else:
                rs_data = await compute_rs(market, "NEUTRAL", 0, [])
                report  = ""

            if not report:
                from datetime import time as dtime
                now = _now_et()
                t, wd = now.time(), now.weekday()
                if wd >= 5 or t < dtime(7, 0) or t > dtime(20, 0):
                    session = "CLOSED"
                elif t < dtime(9, 30):
                    session = "PREMARKET"
                elif t < dtime(16, 0):
                    session = "MARKET"
                else:
                    session = "AFTER HOURS"

                sched = _scheduler_ref[0] if _scheduler_ref else None
                ctx   = sched.context if sched else None

                report = format_no_flow_snapshot(
                    session_label   = session,
                    time_str        = now.strftime("%H:%M"),
                    rs_data         = rs_data,
                    prior_direction = ctx.direction if ctx else "NEUTRAL",
                    prior_leaders   = ctx.leaders   if ctx else [],
                    prior_laggards  = ctx.laggards  if ctx else [],
                )

            _report_cache["text"]  = report
            _report_cache["label"] = "MANUAL_REPORT"
            _report_cache["ts"]    = _now_et()
            await update.message.reply_text(report)
        except Exception as exc:
            logger.error("/report command failed: %s", exc, exc_info=True)
            await update.message.reply_text("Report generation failed. Check logs.")

    # ── /premarket — pre-market bias without clearing the accumulator ──────────

    async def cmd_premarket(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not _is_allowed(update):
            await _deny(update); return
        try:
            analysis = pm_tracker.peek()        # non-destructive; scheduled 8:30 still fires
            notes    = pm_tracker.overnight_notes()
            report   = format_premarket_report(analysis, overnight_notes=notes)
            await update.message.reply_text(report)
        except Exception as exc:
            logger.error("/premarket command failed: %s", exc, exc_info=True)
            await update.message.reply_text("Premarket report failed. Check logs.")

    # ── /flow — options-flow summary of current window ─────────────────────────

    async def cmd_flow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not _is_allowed(update):
            await _deny(update); return
        try:
            entries = sig_window.fresh()
            report  = format_flow_summary(entries)
            await update.message.reply_text(report)
        except Exception as exc:
            logger.error("/flow command failed: %s", exc, exc_info=True)
            await update.message.reply_text("Flow summary failed. Check logs.")

    # ── /bias — market bias only ───────────────────────────────────────────────

    async def cmd_bias(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not _is_allowed(update):
            await _deny(update); return
        try:
            entries  = sig_window.fresh()
            analysis = _analyze(entries) if entries else {}
            tickers  = list(dict.fromkeys(e.ticker for e in entries))[:5] if entries else []
            direction  = analysis.get("direction", "NEUTRAL") if analysis else "NEUTRAL"
            confidence = analysis.get("confidence", 0) if analysis else 0
            rs_data  = await compute_rs(market, direction, confidence, tickers)
            report   = format_bias_only(analysis, rs_data=rs_data)
            await update.message.reply_text(report)
        except Exception as exc:
            logger.error("/bias command failed: %s", exc, exc_info=True)
            await update.message.reply_text("Bias report failed. Check logs.")

    # ── /nq /es /rty /ym — single-future execution plan ───────────────────────

    async def _cmd_single_future(future: str, update: Update) -> None:
        if not _is_allowed(update):
            await _deny(update); return
        try:
            entries  = sig_window.fresh()
            analysis = _analyze(entries) if entries else {}
            direction  = analysis.get("direction", "NEUTRAL") if analysis else "NEUTRAL"
            confidence = analysis.get("confidence", 0) if analysis else 0
            rs_data  = await compute_rs(market, direction, confidence, [])
            report   = format_single_future_plan(future, rs_data, direction, confidence)
            await update.message.reply_text(report)
        except Exception as exc:
            logger.error("/%s command failed: %s", future.lower(), exc, exc_info=True)
            await update.message.reply_text(f"{future.upper()} plan failed. Check logs.")

    async def cmd_nq(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await _cmd_single_future("NQ", update)

    async def cmd_es(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await _cmd_single_future("ES", update)

    async def cmd_rty(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await _cmd_single_future("RTY", update)

    async def cmd_ym(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await _cmd_single_future("YM", update)

    # ── Shared helper: fetch fresh entries + RS direction for filter commands ────
    #
    # Short cache (45 s) prevents redundant sig_window reads when several filter
    # commands are called in quick succession.

    _flow_cache: dict = {
        "entries":    [],
        "direction":  "NEUTRAL",
        "confidence": 0,
        "ts":         None,
    }
    _FLOW_CACHE_TTL = 45   # seconds

    async def _fresh_entries_and_direction() -> tuple[list, str, int]:
        """
        Return (entries, direction, confidence) from the current signal window.
        Result is cached for _FLOW_CACHE_TTL seconds to avoid redundant rescans
        when multiple filter commands are called back-to-back.
        """
        now = _now_et()
        ts  = _flow_cache["ts"]
        if ts is not None and (now - ts).total_seconds() < _FLOW_CACHE_TTL:
            return (
                _flow_cache["entries"],
                _flow_cache["direction"],
                _flow_cache["confidence"],
            )

        entries    = sig_window.fresh()
        analysis   = _analyze(entries) if entries else {}
        direction  = analysis.get("direction", "NEUTRAL") if analysis else "NEUTRAL"
        confidence = analysis.get("confidence", 0) if analysis else 0

        _flow_cache["entries"]    = entries
        _flow_cache["direction"]  = direction
        _flow_cache["confidence"] = confidence
        _flow_cache["ts"]         = now

        return entries, direction, confidence

    # ── /options — all hot contracts, max 2 per ticker ─────────────────────────

    async def cmd_options(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not _is_allowed(update):
            await _deny(update); return
        try:
            entries, direction, _ = await _fresh_entries_and_direction()
            report = format_hot_options(
                entries,
                filter_fn=None,
                label="HOT OPTIONS",
                direction=direction,
                max_per_ticker=2,
            )
            await update.message.reply_text(report)
        except Exception as exc:
            logger.error("/options command failed: %s", exc, exc_info=True)
            await update.message.reply_text("Options scan failed. Check logs.")

    # ── /bulls — bullish (CALL) hot contracts, max 1 per ticker ───────────────

    async def cmd_bulls(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not _is_allowed(update):
            await _deny(update); return
        try:
            entries, direction, _ = await _fresh_entries_and_direction()
            report = format_hot_options(
                entries,
                filter_fn=lambda e: getattr(e, "side", "") == "CALL",
                label="HOT OPTIONS — BULLS",
                direction=direction,
                max_per_ticker=1,
            )
            await update.message.reply_text(report)
        except Exception as exc:
            logger.error("/bulls command failed: %s", exc, exc_info=True)
            await update.message.reply_text("Bulls scan failed. Check logs.")

    # ── /bears — bearish (PUT) hot contracts, max 1 per ticker ────────────────

    async def cmd_bears(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not _is_allowed(update):
            await _deny(update); return
        try:
            entries, direction, _ = await _fresh_entries_and_direction()
            report = format_hot_options(
                entries,
                filter_fn=lambda e: getattr(e, "side", "") == "PUT",
                label="HOT OPTIONS — BEARS",
                direction=direction,
                max_per_ticker=1,
            )
            await update.message.reply_text(report)
        except Exception as exc:
            logger.error("/bears command failed: %s", exc, exc_info=True)
            await update.message.reply_text("Bears scan failed. Check logs.")

    # ── /tech — tech-sector hot contracts, max 1 per ticker ───────────────────

    async def cmd_tech(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not _is_allowed(update):
            await _deny(update); return
        try:
            entries, direction, _ = await _fresh_entries_and_direction()
            report = format_hot_options(
                entries,
                filter_fn=lambda e: getattr(e, "ticker", "") in _TECH_TICKERS,
                label="HOT OPTIONS — TECH",
                direction=direction,
                max_per_ticker=1,
            )
            await update.message.reply_text(report)
        except Exception as exc:
            logger.error("/tech command failed: %s", exc, exc_info=True)
            await update.message.reply_text("Tech scan failed. Check logs.")

    # ── /hedges — index PUT hedges (SPY / QQQ / IWM puts), max 1 per ticker ───

    async def cmd_hedges(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not _is_allowed(update):
            await _deny(update); return
        try:
            entries, direction, _ = await _fresh_entries_and_direction()
            report = format_hot_options(
                entries,
                filter_fn=lambda e: (
                    getattr(e, "ticker", "") in _INDEX_HEDGE_TICKERS
                    and getattr(e, "side", "") == "PUT"
                ),
                label="INDEX HEDGES",
                direction=direction,
                max_per_ticker=1,
            )
            await update.message.reply_text(report)
        except Exception as exc:
            logger.error("/hedges command failed: %s", exc, exc_info=True)
            await update.message.reply_text("Hedges scan failed. Check logs.")

    # ── /techbulls — bullish tech contracts only, max 1 per ticker ────────────

    async def cmd_techbulls(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not _is_allowed(update):
            await _deny(update); return
        try:
            entries, direction, _ = await _fresh_entries_and_direction()
            report = format_hot_options(
                entries,
                filter_fn=lambda e: (
                    getattr(e, "ticker", "") in _TECH_TICKERS
                    and getattr(e, "side", "") == "CALL"
                ),
                label="HOT OPTIONS — TECH BULLS",
                direction=direction,
                max_per_ticker=1,
            )
            await update.message.reply_text(report)
        except Exception as exc:
            logger.error("/techbulls command failed: %s", exc, exc_info=True)
            await update.message.reply_text("Tech bulls scan failed. Check logs.")

    # ── /techbears — bearish tech contracts only, max 1 per ticker ────────────

    async def cmd_techbears(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not _is_allowed(update):
            await _deny(update); return
        try:
            entries, direction, _ = await _fresh_entries_and_direction()
            report = format_hot_options(
                entries,
                filter_fn=lambda e: (
                    getattr(e, "ticker", "") in _TECH_TICKERS
                    and getattr(e, "side", "") == "PUT"
                ),
                label="HOT OPTIONS — TECH BEARS",
                direction=direction,
                max_per_ticker=1,
            )
            await update.message.reply_text(report)
        except Exception as exc:
            logger.error("/techbears command failed: %s", exc, exc_info=True)
            await update.message.reply_text("Tech bears scan failed. Check logs.")

    # ── /last — return most recent cached report ───────────────────────────────

    async def cmd_last(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not _is_allowed(update):
            await _deny(update); return
        text = _report_cache.get("text", "")
        ts   = _report_cache.get("ts")
        if not text:
            await update.message.reply_text("No cached report yet. Run /report first.")
            return
        ts_str = ts.strftime("%Y-%m-%d %H:%M ET") if ts else "unknown time"
        await update.message.reply_text(f"[Cached {ts_str}]\n\n{text}")

    # ── /debug — dump sig_window state so we can diagnose empty /options ─────────

    async def cmd_debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not _is_allowed(update):
            await _deny(update); return
        try:
            from app.telegram_handler import _hot_options_score, _HOT_OPTIONS_MIN_SCORE

            now_et    = _now_et()
            session   = current_session(now_et)
            entries   = sig_window.fresh()
            logger.info("/debug invoked | session=%s | window_size=%d", session, len(entries))
            ana       = _analyze(entries) if entries else {}
            direction = ana.get("direction", "NEUTRAL")

            lines = [
                "DEBUG — sig_window state",
                f"Time (ET):   {now_et.strftime('%Y-%m-%d %H:%M:%S')}",
                f"Session:     {session}",
                f"In window:   {len(entries)} signal(s) (last 30 min)",
                f"Direction:   {direction}",
                f"Score floor: {_HOT_OPTIONS_MIN_SCORE}",
                "",
            ]

            if not entries:
                lines += [
                    "Window is EMPTY — nothing to show for /options.",
                    "",
                    "Signals reach sig_window when:",
                    "  1. Session is RTH (09:30-16:00 ET)",
                    "  2. A message arrives in source channel",
                    "  3. Raw flow alert: parses directly → added to window",
                    "  4. Aggregated report (MARKET BIAS / TOP BULLS): each",
                    "     flow entry is converted and added to window",
                    "",
                    "Window is empty because no source message has arrived yet",
                    "since the bot last restarted. Send /debug again after the",
                    "next report comes in from the source channel.",
                ]
            else:
                lines.append(f"{'#':<3} {'Tkr':<7} {'Side':<5} {'DTE':<4} {'Delta':<7} {'VoI':<5} {'Score':<6} Status")
                lines.append("─" * 58)
                for i, e in enumerate(entries, 1):
                    dte   = getattr(e, "dte", 0)
                    delta = getattr(e, "delta", None)
                    voi   = getattr(e, "vol_oi_ratio", 0)
                    cls   = getattr(e, "classification", "")
                    role  = getattr(e, "signal_role", "")

                    in_pool = cls != "LOTTERY" and role != "NOISE" and 1 <= dte <= 30
                    score   = round(_hot_options_score(e, direction), 1) if in_pool else 0.0
                    passes  = in_pool and score >= _HOT_OPTIONS_MIN_SCORE

                    if not in_pool:
                        if cls == "LOTTERY":       status = "SKIP:LOTTERY"
                        elif role == "NOISE":      status = "SKIP:NOISE"
                        elif not (1 <= dte <= 30): status = f"SKIP:DTE={dte}"
                        else:                      status = "SKIP"
                    elif not passes:
                        status = f"LOW score={score}"
                    else:
                        status = f"OK  score={score}"

                    d_str = f"{delta:+.2f}" if delta is not None else "N/A"
                    lines.append(
                        f"{i:<3} {e.ticker:<7} {e.side:<5} {dte:<4} "
                        f"{d_str:<7} {voi:<5.1f} {score:<6.1f} {status}"
                    )

            await update.message.reply_text("\n".join(lines))
        except Exception as exc:
            logger.error("/debug failed: %s", exc, exc_info=True)
            await update.message.reply_text(f"Debug failed: {exc}")

    # ── HTTP command helpers — return formatted text, no Telegram send ────────

    async def _run_report_text() -> str:
        entries = sig_window.fresh()
        if entries:
            analysis = _analyze(entries)
            tickers  = list(dict.fromkeys(e.ticker for e in entries))[:5]
            rs_data  = await compute_rs(market, analysis.get("direction","NEUTRAL"), analysis.get("confidence",0), tickers)
            report   = format_channel_b_report(analysis, rs_data=rs_data)
        else:
            rs_data = await compute_rs(market, "NEUTRAL", 0, [])
            report  = ""
        if not report:
            from datetime import time as _dtime
            now = _now_et(); t, wd = now.time(), now.weekday()
            session = ("CLOSED" if wd >= 5 or t < _dtime(7,0) or t > _dtime(20,0)
                       else "PREMARKET" if t < _dtime(9,30)
                       else "MARKET" if t < _dtime(16,0) else "AFTER HOURS")
            sched   = _scheduler_ref[0] if _scheduler_ref else None
            ctx     = sched.context if sched else None
            report  = format_no_flow_snapshot(
                session_label=session, time_str=now.strftime("%H:%M"),
                rs_data=rs_data,
                prior_direction=ctx.direction if ctx else "NEUTRAL",
                prior_leaders=ctx.leaders   if ctx else [],
                prior_laggards=ctx.laggards if ctx else [],
            )
        return report

    async def _run_premarket_text() -> str:
        analysis = pm_tracker.peek()
        notes    = pm_tracker.overnight_notes()
        return format_premarket_report(analysis, overnight_notes=notes)

    async def _run_flow_text() -> str:
        return format_flow_summary(sig_window.fresh())

    async def _run_bias_text() -> str:
        entries  = sig_window.fresh()
        analysis = _analyze(entries) if entries else {}
        tickers  = list(dict.fromkeys(e.ticker for e in entries))[:5] if entries else []
        rs_data  = await compute_rs(market, analysis.get("direction","NEUTRAL"), analysis.get("confidence",0), tickers)
        return format_bias_only(analysis, rs_data=rs_data)

    async def _run_options_text() -> str:
        entries, direction, _ = await _fresh_entries_and_direction()
        return format_hot_options(entries, filter_fn=None, label="HOT OPTIONS", direction=direction, max_per_ticker=2)

    async def _run_bulls_text() -> str:
        entries, direction, _ = await _fresh_entries_and_direction()
        return format_hot_options(entries, filter_fn=lambda e: getattr(e,"side","")=="CALL", label="HOT OPTIONS — BULLS", direction=direction, max_per_ticker=1)

    async def _run_bears_text() -> str:
        entries, direction, _ = await _fresh_entries_and_direction()
        return format_hot_options(entries, filter_fn=lambda e: getattr(e,"side","")=="PUT", label="HOT OPTIONS — BEARS", direction=direction, max_per_ticker=1)

    async def _run_tech_text() -> str:
        entries, direction, _ = await _fresh_entries_and_direction()
        return format_hot_options(entries, filter_fn=lambda e: getattr(e,"ticker","") in _TECH_TICKERS, label="HOT OPTIONS — TECH", direction=direction, max_per_ticker=1)

    async def _run_techbulls_text() -> str:
        entries, direction, _ = await _fresh_entries_and_direction()
        return format_hot_options(entries, filter_fn=lambda e: getattr(e,"ticker","") in _TECH_TICKERS and getattr(e,"side","")=="CALL", label="HOT OPTIONS — TECH BULLS", direction=direction, max_per_ticker=1)

    async def _run_techbears_text() -> str:
        entries, direction, _ = await _fresh_entries_and_direction()
        return format_hot_options(entries, filter_fn=lambda e: getattr(e,"ticker","") in _TECH_TICKERS and getattr(e,"side","")=="PUT", label="HOT OPTIONS — TECH BEARS", direction=direction, max_per_ticker=1)

    async def _run_hedges_text() -> str:
        entries, direction, _ = await _fresh_entries_and_direction()
        return format_hot_options(entries, filter_fn=lambda e: getattr(e,"ticker","") in _INDEX_HEDGE_TICKERS and getattr(e,"side","")=="PUT", label="INDEX HEDGES", direction=direction, max_per_ticker=1)

    async def _run_future_text(future: str) -> str:
        entries  = sig_window.fresh()
        analysis = _analyze(entries) if entries else {}
        rs_data  = await compute_rs(market, analysis.get("direction","NEUTRAL"), analysis.get("confidence",0), [])
        return format_single_future_plan(future, rs_data, analysis.get("direction","NEUTRAL"), analysis.get("confidence",0))

    _CMD.update({
        "report":    _run_report_text,
        "premarket": _run_premarket_text,
        "flow":      _run_flow_text,
        "bias":      _run_bias_text,
        "options":   _run_options_text,
        "bulls":     _run_bulls_text,
        "bears":     _run_bears_text,
        "tech":      _run_tech_text,
        "techbulls": _run_techbulls_text,
        "techbears": _run_techbears_text,
        "hedges":    _run_hedges_text,
        "nq":  lambda: _run_future_text("NQ"),
        "es":  lambda: _run_future_text("ES"),
        "rty": lambda: _run_future_text("RTY"),
        "ym":  lambda: _run_future_text("YM"),
    })

    # ── Register manual command handlers ──────────────────────────────────────

    for _cmd, _fn in [
        # Filtered flow commands
        ("options",    cmd_options),
        ("bulls",      cmd_bulls),
        ("bears",      cmd_bears),
        ("tech",       cmd_tech),
        ("techbulls",  cmd_techbulls),
        ("techbears",  cmd_techbears),
        ("hedges",     cmd_hedges),
        # Full reports
        ("report",     cmd_report),
        ("premarket",  cmd_premarket),
        ("flow",       cmd_flow),
        ("bias",       cmd_bias),
        # Futures
        ("nq",         cmd_nq),
        ("es",         cmd_es),
        ("rty",        cmd_rty),
        ("ym",         cmd_ym),
        # Cache
        ("last",       cmd_last),
        # Diagnostics
        ("debug",      cmd_debug),
    ]:
        application.add_handler(CommandHandler(_cmd, _fn))

    logger.info(
        "Manual command handlers registered: "
        "/options /bulls /bears /tech /techbulls /techbears /hedges "
        "/report /premarket /flow /bias "
        "/nq /es /rty /ym /last /debug | allowed_users=%s",
        config.ALLOWED_USERS or "all",
    )

    # ── Channel A handler ─────────────────────────────────────────────────────

    async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.channel_post
        if not message or not message.text:
            return

        if not _is_source_channel(message.chat):
            return

        text = message.text

        # ══ PATH A: Aggregated intel report ═══════════════════════════════════
        if is_aggregated_report(text):
            logger.info(
                "Parser path: aggregated_report | detection=MATCHED | msg_id=%s",
                message.message_id,
            )
            report = parse_intel_report(text, msg_id=message.message_id)

            if report is None:
                logger.warning(
                    "Parser path: AGGREGATED_RAW_FALLBACK | reason=parse_returned_None "
                    "| msg_id=%s — sending whitespace-cleaned raw text",
                    message.message_id,
                )
                fallback = "\n".join(l.strip() for l in text.splitlines() if l.strip())
                await post_to_b(fallback, label="AGGREGATED_RAW_FALLBACK")
                return

            # ── Feed aggregated flow entries into sig_window ──────────────────
            # PATH A never added entries to sig_window, so /options always saw
            # an empty window. Fix: convert FlowEntry → BatchEntry and add them.
            _agg_entries = _intel_entries_to_batch(report)
            for _be in _agg_entries:
                sig_window.add(_be)
            if _agg_entries:
                logger.info(
                    "sig_window: added %d entries from aggregated report | msg_id=%s",
                    len(_agg_entries), message.message_id,
                )

            # Fetch RS data in parallel with any remaining work
            flow_tickers = list(dict.fromkeys(
                e.ticker for e in (report.top_overall + report.top_bulls + report.top_bears)
            ))[:5]
            rs_data = await compute_rs(market, report.direction, report.confidence, flow_tickers)

            formatted = format_aggregated_report_b(report, rs_data=rs_data)

            if not formatted:
                logger.warning(
                    "Parser path: AGGREGATED_RAW_FALLBACK | reason=formatter_returned_empty "
                    "| msg_id=%s | parsed direction=%s bulls=%d bears=%d — sending raw",
                    message.message_id, report.direction,
                    len(report.top_bulls), len(report.top_bears),
                )
                await post_to_b(text.strip(), label="AGGREGATED_RAW_FALLBACK")
                return

            logger.info(
                "Parser path: AGGREGATED_PARSED | msg_id=%s | direction=%s "
                "| confidence=%d | top_overall=%d | bulls=%d | bears=%d | chars=%d "
                "| rs_state=%s",
                message.message_id, report.direction, report.confidence,
                len(report.top_overall), len(report.top_bulls), len(report.top_bears),
                len(formatted), rs_data.market_state,
            )
            await post_to_b(formatted, label="AGGREGATED_PARSED")
            return

        # ══ PATH B: Raw single-flow signal ════════════════════════════════════
        sig = parse_flow_message(text, message_id=message.message_id)
        if sig is None:
            logger.info("Parser path: ignored | msg_id=%s | no pattern matched", message.message_id)
            return

        logger.info("Parser path: raw_signal | msg_id=%s | signal=%s", message.message_id, sig.signal_id)

        # ── Pre-market branch (07:00–09:29 ET) ───────────────────────────────
        if _is_premarket():
            if (sig.premium_usd < config.PREMARKET_MIN_PREMIUM
                    or sig.vol_oi_ratio < config.PREMARKET_MIN_VOL_OI):
                logger.debug(
                    "Pre-market filter: skip %s | premium=%.0f vol_oi=%.1f",
                    sig.signal_id, sig.premium_usd, sig.vol_oi_ratio,
                )
                return
            cls, role, pri = classify_flow(sig)
            pm_tracker.add(sig, cls, role, pri)
            logger.info(
                "Pre-market signal tracked | %s | cls=%s | premium=%.0f | vol_oi=%.1f",
                sig.signal_id, cls, sig.premium_usd, sig.vol_oi_ratio,
            )
            return

        # ── Outside trading hours — drop ──────────────────────────────────────
        if not _is_trading_session():
            logger.debug("Signal ignored — outside trading hours")
            return

        await fetch_option_quote(sig)
        sig.lock_signal_premium()   # lock mid/last as premium_at_signal — never updated again

        # ── Classify → Channel A (INTEL_CHANNEL) ─────────────────────────────
        cls, role, pri = classify_flow(sig)
        asyncio.create_task(
            post_to_a(format_intel(sig, cls, role, pri), signal_id=sig.signal_id)
        )

        logger.info(
            "Signal received: %s | cls=%s | role=%s | p%d | score=%d | dte=%d | vol/oi=%.1f",
            sig.signal_id, cls, role, pri, sig.score, sig.dte, sig.vol_oi_ratio,
        )

        # ── Evaluate ──────────────────────────────────────────────────────────
        try:
            decision = await engine.evaluate(sig)
        except Exception as exc:
            logger.error("Engine error for %s: %s", sig.signal_id, exc, exc_info=True)
            batch.add(sig, cls, role, pri, "ERROR")
            return

        batch.add(sig, cls, role, pri, decision.verdict)
        sig_window.add(batch._entries[-1])   # feed latest entry to rolling window

        if decision.verdict == "KILL":
            logger.info("Decision: KILL | signal=%s | reason=%s", sig.signal_id, decision.reason)

        elif decision.verdict == "GO":
            if not was_sent(sig.signal_id, "GO"):
                decision = compute_targets(sig, decision)
                record_signal(sig, decision.price, state="GO", classification=cls)
                update_signal_go(sig.signal_id, decision.price, datetime.utcnow().isoformat())
                asyncio.create_task(_price_check_task(sig.signal_id, sig.ticker, sig.side, market))
                await post_to_b(format_go(sig, decision), label="GO")
                mark_sent(sig.signal_id, "GO")

        elif decision.verdict == "HOLD":
            logger.info("Decision: HOLD | signal=%s | reason=%s", sig.signal_id, decision.reason)
            if not was_sent(sig.signal_id, "HOLD"):
                record_signal(sig, decision.price, state="HOLD", classification=cls)
                asyncio.create_task(_price_check_task(sig.signal_id, sig.ticker, sig.side, market))
                mark_sent(sig.signal_id, "HOLD")
            watcher.add(sig)

        # ── Batch fire → Channel B structured report ──────────────────────────
        if batch.should_post():
            analysis = batch.analyze_and_reset()
            # Fetch RS data for batch report
            batch_tickers = list(dict.fromkeys(
                e.ticker for e in analysis.get("entries", [])
            ))[:5]
            rs_data  = await compute_rs(market, analysis.get("direction", "NEUTRAL"), analysis.get("confidence", 0), batch_tickers)
            formatted = format_channel_b_report(analysis, rs_data=rs_data)
            if formatted:
                await post_to_b(formatted, label="BATCH")
                logger.info(
                    "Batch report sent | state=%s | mode=%s | signals=%d | rs_state=%s",
                    analysis.get("state"), analysis.get("mode"), analysis.get("total"),
                    rs_data.market_state,
                )
            else:
                logger.warning("format_channel_b_report returned empty string — NOT sent")

    application.add_handler(
        MessageHandler(filters.UpdateType.CHANNEL_POSTS, handle_channel_post)
    )

    # ── Start ─────────────────────────────────────────────────────────────────

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig_num in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig_num, stop_event.set)

    async with application:
        if config.BACKUP_CHAT_ID:
            await restore_db(application.bot, config.BACKUP_CHAT_ID, config.DB_PATH)

        init_db()

        logger.info("Startup delay (8s) — waiting for previous instance to release polling...")
        await asyncio.sleep(8)

        await application.start()
        await application.updater.start_polling(
            allowed_updates=["channel_post", "message"],
            drop_pending_updates=True,
        )
        logger.info(
            "Bot started | instance=%s | SOURCE=%s | DEST=%s | INTEL=%s",
            _INSTANCE_ID,
            config.SOURCE_CHANNEL,
            config.DEST_CHANNEL,
            config.INTEL_CHANNEL or "(disabled)",
        )

        try:
            spy_snap, qqq_snap = await asyncio.gather(
                market.snapshot("SPY"), market.snapshot("QQQ")
            )
            if spy_snap.fetch_ok and spy_snap.price:
                logger.info(
                    "Alpaca OK | SPY price=%.2f vwap=%.2f pm_high=%.2f pm_low=%.2f",
                    spy_snap.price,
                    spy_snap.vwap or 0,
                    spy_snap.pm_high or 0,
                    spy_snap.pm_low or 0,
                )
            else:
                logger.error(
                    "Alpaca FAILED for SPY — fetch_ok=%s price=%s. "
                    "Check ALPACA_API_KEY and ALPACA_API_SECRET in .env / Railway vars.",
                    spy_snap.fetch_ok, spy_snap.price,
                )
            if qqq_snap.fetch_ok and qqq_snap.price:
                logger.info(
                    "Alpaca OK | QQQ price=%.2f vwap=%.2f",
                    qqq_snap.price, qqq_snap.vwap or 0,
                )
            else:
                logger.error(
                    "Alpaca FAILED for QQQ — fetch_ok=%s price=%s.",
                    qqq_snap.fetch_ok, qqq_snap.price,
                )
            if spy_snap.fetch_ok and qqq_snap.fetch_ok:
                logger.info("Alpaca market data confirmed — decision engine will use live prices")
            else:
                logger.error(
                    "Alpaca not returning data — GO decisions will not fire. "
                    "Signals will stay HOLD until Alpaca is fixed."
                )
        except Exception as exc:
            logger.error("Alpaca warm cache failed: %s — check credentials", exc)

        scheduler = Scheduler(window=sig_window, send_fn=post_to_b, market=market)
        _scheduler_ref.append(scheduler)

        watcher_task   = asyncio.create_task(watcher.run())
        pm_task        = asyncio.create_task(_forced_830_loop())
        scheduler_task = asyncio.create_task(scheduler.run())
        backup_task    = asyncio.create_task(
            backup_loop(application.bot, config.BACKUP_CHAT_ID, config.DB_PATH)
        )

        _uv_srv = None
        if _HTTP_OK:
            _port   = int(os.environ.get("PORT", 8080))
            _uv_cfg = _UV.Config(_http_app, host="0.0.0.0", port=_port, log_level="warning")
            _uv_srv = _UV.Server(_uv_cfg)
            asyncio.create_task(_uv_srv.serve())
            logger.info("HTTP command server listening on port %d", _port)

        await stop_event.wait()

        logger.info("Shutdown signal received")
        watcher.stop()
        if _uv_srv:
            _uv_srv.should_exit = True
        for task in (watcher_task, pm_task, scheduler_task, backup_task):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        if config.BACKUP_CHAT_ID:
            await backup_db(application.bot, config.BACKUP_CHAT_ID, config.DB_PATH)

        await application.updater.stop()
        await application.stop()

    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
