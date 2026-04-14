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
)
from app.intel_parser import is_aggregated_report, parse_intel_report
from app.scheduler import Scheduler, SignalWindow, _slot_key
from app.classifier import classify_flow
from app.intel_formatter import format_intel
from app.batch import BatchStore
from app.storage import (
    init_db, was_sent, mark_sent,
    record_signal, update_signal_go, update_price_check,
    get_signal_entry, update_outcome, get_stats_summary,
)
from app.backup import restore_db, backup_db, backup_loop
from app.tradier import fetch_option_quote


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

            formatted = format_aggregated_report_b(report)

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
                "| confidence=%d | top_overall=%d | bulls=%d | bears=%d | chars=%d",
                message.message_id, report.direction, report.confidence,
                len(report.top_overall), len(report.top_bulls), len(report.top_bears),
                len(formatted),
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
            analysis  = batch.analyze_and_reset()
            formatted = format_channel_b_report(analysis)
            if formatted:
                await post_to_b(formatted, label="BATCH")
                logger.info(
                    "Batch report sent | state=%s | mode=%s | signals=%d",
                    analysis.get("state"), analysis.get("mode"), analysis.get("total"),
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

        scheduler = Scheduler(window=sig_window, send_fn=post_to_b)
        _scheduler_ref.append(scheduler)

        watcher_task   = asyncio.create_task(watcher.run())
        pm_task        = asyncio.create_task(_forced_830_loop())
        scheduler_task = asyncio.create_task(scheduler.run())
        backup_task    = asyncio.create_task(
            backup_loop(application.bot, config.BACKUP_CHAT_ID, config.DB_PATH)
        )

        await stop_event.wait()

        logger.info("Shutdown signal received")
        watcher.stop()
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
