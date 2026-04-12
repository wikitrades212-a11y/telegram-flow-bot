"""
Entry point — bot-token mode via python-telegram-bot v20+.

Setup requirements:
  1. Create a bot via @BotFather → get BOT_TOKEN
  2. Add the bot as an ADMIN to Channel A (needs "Read Messages" / all message access)
  3. Add the bot as an ADMIN to Channel B (needs "Post Messages")
  4. Set SOURCE_CHANNEL and DEST_CHANNEL in .env

Flow:
  - Telegram delivers channel_post updates to the bot (long polling)
  - Every new message from Channel A is parsed and evaluated
  - HOLD signals are posted to Channel B and added to the watchlist
  - GO signals are posted with entry/stop/target
  - Background watcher re-evaluates HOLD signals every ~20 seconds
"""

import asyncio
import logging
import signal
import sys
import uuid
from pathlib import Path

_INSTANCE_ID = uuid.uuid4().hex[:8]

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, MessageHandler, ContextTypes, filters

import config
from config import validate_env
from app.parser import parse_flow_message
from app.market_data import MarketDataService
from app.decision_engine import Decision, DecisionEngine
from app.risk import compute_targets
from app.watcher import Watcher
from app.telegram_handler import format_hold, format_go
from app.storage import init_db, was_sent, mark_sent
from app.backup import restore_db, backup_db, backup_loop


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
    logging.getLogger("urllib3").setLevel(logging.WARNING)


logger = logging.getLogger(__name__)


# ── Channel matching helper ───────────────────────────────────────────────────

def _is_source_channel(chat) -> bool:
    src = config.SOURCE_CHANNEL
    if src.startswith("@"):
        return (chat.username or "").lower() == src.lstrip("@").lower()
    try:
        return str(chat.id) == str(src)
    except Exception:
        return False


# ── Main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    _setup_logging()
    validate_env()

    market = MarketDataService(
        cache_ttl_seconds=config.MARKET_DATA_CACHE_TTL,
        stale_ttl_seconds=config.MARKET_DATA_STALE_TTL,
    )
    engine = DecisionEngine(market)

    application = Application.builder().token(config.BOT_TOKEN).build()

    # ── Shared send helper ────────────────────────────────────────────────────

    async def post_to_b(text: str, signal_id: str = "", verdict: str = "") -> None:
        logger.info(
            "Sending to Channel B | dest=%s | verdict=%s | signal=%s",
            config.DEST_CHANNEL, verdict or "?", signal_id or "?",
        )
        try:
            sent = await application.bot.send_message(
                chat_id=config.DEST_CHANNEL,
                text=text,
                parse_mode=ParseMode.HTML,
            )
            logger.info(
                "Channel B send OK | tg_msg_id=%d | verdict=%s | signal=%s",
                sent.message_id, verdict or "?", signal_id or "?",
            )
        except Exception as exc:
            logger.error(
                "Channel B send FAILED | dest=%s | verdict=%s | signal=%s | error: %s",
                config.DEST_CHANNEL, verdict or "?", signal_id or "?", exc,
                exc_info=True,
            )

    # ── GO callback for background watcher ───────────────────────────────────

    async def on_go(sig, dec: Decision) -> None:
        dec = compute_targets(sig, dec)
        await post_to_b(format_go(sig, dec), signal_id=sig.signal_id, verdict="GO")

    watcher = Watcher(engine=engine, on_go=on_go)

    # ── Channel A handler ─────────────────────────────────────────────────────

    async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        message = update.channel_post
        if not message or not message.text:
            return

        if not _is_source_channel(message.chat):
            return

        sig = parse_flow_message(message.text, message_id=message.message_id)
        if sig is None:
            return

        logger.info(
            "Signal received: %s | score=%d | dte=%d | vol/oi=%.1f",
            sig.signal_id, sig.score, sig.dte, sig.vol_oi_ratio,
        )

        try:
            decision = await engine.evaluate(sig)
        except Exception as exc:
            logger.error("Engine error for %s: %s", sig.signal_id, exc, exc_info=True)
            return

        if decision.verdict == "KILL":
            logger.info("Decision: KILL | signal=%s | reason=%s", sig.signal_id, decision.reason)
            return

        if decision.verdict == "GO":
            if not was_sent(sig.signal_id, "GO"):
                decision = compute_targets(sig, decision)
                await post_to_b(format_go(sig, decision), signal_id=sig.signal_id, verdict="GO")
                mark_sent(sig.signal_id, "GO")
            return

        if decision.verdict == "HOLD":
            logger.info("Decision: HOLD | signal=%s | reason=%s", sig.signal_id, decision.reason)
            if not was_sent(sig.signal_id, "HOLD"):
                await post_to_b(format_hold(sig, decision), signal_id=sig.signal_id, verdict="HOLD")
                mark_sent(sig.signal_id, "HOLD")
            watcher.add(sig)

    application.add_handler(
        MessageHandler(filters.UpdateType.CHANNEL_POSTS, handle_channel_post)
    )

    # ── Start and run ─────────────────────────────────────────────────────────

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig_num in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig_num, stop_event.set)

    async with application:
        # Restore DB from Telegram backup before init_db creates a blank one
        if config.BACKUP_CHAT_ID:
            await restore_db(application.bot, config.BACKUP_CHAT_ID, config.DB_PATH)

        init_db()

        await application.start()
        await application.updater.start_polling(
            allowed_updates=["channel_post"],
            drop_pending_updates=True,
        )
        logger.info(
            "Bot started | instance=%s | source=%s | dest=%s",
            _INSTANCE_ID, config.SOURCE_CHANNEL, config.DEST_CHANNEL,
        )

        logger.info("Warming market data cache (SPY, QQQ)...")
        try:
            await asyncio.gather(market.snapshot("SPY"), market.snapshot("QQQ"))
            logger.info("Market data cache warm")
        except Exception as exc:
            logger.warning("Warm cache failed (non-fatal): %s", exc)

        watcher_task = asyncio.create_task(watcher.run())
        backup_task = asyncio.create_task(
            backup_loop(application.bot, config.BACKUP_CHAT_ID, config.DB_PATH)
        )

        await stop_event.wait()

        logger.info("Shutdown signal received")
        watcher.stop()
        watcher_task.cancel()
        backup_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass
        try:
            await backup_task
        except asyncio.CancelledError:
            pass

        # Final backup on clean shutdown
        if config.BACKUP_CHAT_ID:
            await backup_db(application.bot, config.BACKUP_CHAT_ID, config.DB_PATH)

        await application.updater.stop()
        await application.stop()

    logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
