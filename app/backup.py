"""
Telegram-backed SQLite persistence.

Uses a private bot-accessible channel (BACKUP_CHAT_ID) as a free file store.
The bot uploads signals.db as a document and pins the latest backup so
restore can find it via get_chat().pinned_message on the next startup.

Usage:
  restore_db(bot, backup_chat_id, db_path)  — call BEFORE init_db() at startup
  backup_db(bot, backup_chat_id, db_path)   — call after significant writes
  backup_loop(...)                           — long-running periodic task
"""

import asyncio
import logging
from pathlib import Path

from telegram import Bot
from telegram.error import TelegramError

logger = logging.getLogger(__name__)


async def restore_db(bot: Bot, backup_chat_id: str, db_path: str) -> bool:
    if not backup_chat_id:
        return False

    local = Path(db_path)
    if local.exists():
        logger.info("[backup] Local DB exists at %s — skipping restore", db_path)
        return False

    try:
        chat = await bot.get_chat(backup_chat_id)
        if not chat.pinned_message or not chat.pinned_message.document:
            logger.info("[backup] No pinned backup found in chat %s", backup_chat_id)
            return False

        doc = chat.pinned_message.document
        if not doc.file_name or not doc.file_name.endswith(".db"):
            logger.info("[backup] Pinned message is not a .db file — skipping restore")
            return False

        local.parent.mkdir(parents=True, exist_ok=True)
        tg_file = await bot.get_file(doc.file_id)
        await tg_file.download_to_drive(db_path)
        logger.info(
            "[backup] Restored DB from Telegram | chat=%s | file_id=%s | size=%s bytes",
            backup_chat_id, doc.file_id, doc.file_size,
        )
        return True

    except TelegramError as exc:
        logger.warning("[backup] Restore failed (non-fatal): %s", exc)
        return False
    except Exception as exc:
        logger.warning("[backup] Restore error (non-fatal): %s", exc, exc_info=True)
        return False


async def backup_db(bot: Bot, backup_chat_id: str, db_path: str) -> bool:
    if not backup_chat_id:
        return False

    local = Path(db_path)
    if not local.exists():
        logger.debug("[backup] DB file not found at %s — skipping backup", db_path)
        return False

    try:
        try:
            chat = await bot.get_chat(backup_chat_id)
            if chat.pinned_message:
                await bot.unpin_chat_message(
                    chat_id=backup_chat_id,
                    message_id=chat.pinned_message.message_id,
                )
        except TelegramError:
            pass

        with open(local, "rb") as fh:
            sent = await bot.send_document(
                chat_id=backup_chat_id,
                document=fh,
                filename=local.name,
                caption="signals.db backup",
            )

        await bot.pin_chat_message(
            chat_id=backup_chat_id,
            message_id=sent.message_id,
            disable_notification=True,
        )

        logger.info(
            "[backup] DB backed up | chat=%s | tg_msg_id=%d | size=%d bytes",
            backup_chat_id, sent.message_id, local.stat().st_size,
        )
        return True

    except TelegramError as exc:
        logger.warning("[backup] Backup failed (non-fatal): %s", exc)
        return False
    except Exception as exc:
        logger.warning("[backup] Backup error (non-fatal): %s", exc, exc_info=True)
        return False


async def backup_loop(
    bot: Bot,
    backup_chat_id: str,
    db_path: str,
    interval_seconds: int = 300,
) -> None:
    if not backup_chat_id:
        logger.info("[backup] BACKUP_CHAT_ID not set — periodic backup disabled")
        return

    logger.info(
        "[backup] Periodic backup started | chat=%s | interval=%ds",
        backup_chat_id, interval_seconds,
    )
    while True:
        await asyncio.sleep(interval_seconds)
        await backup_db(bot, backup_chat_id, db_path)
