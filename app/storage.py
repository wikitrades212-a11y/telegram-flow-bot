"""
SQLite-backed deduplication store.

Tracks which (signal_id, verdict) pairs have already been sent to Channel B
so the bot never double-posts even across restarts.
"""

import sqlite3
import logging
from datetime import datetime
from pathlib import Path

import config

logger = logging.getLogger(__name__)

_DB = Path(config.DB_PATH)


def init_db() -> None:
    _DB.parent.mkdir(parents=True, exist_ok=True)
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sent_signals (
                signal_id  TEXT NOT NULL,
                verdict    TEXT NOT NULL,
                sent_at    TEXT NOT NULL,
                PRIMARY KEY (signal_id, verdict)
            )
        """)
    logger.info("Storage initialised at %s", _DB)


def _connect() -> sqlite3.Connection:
    return sqlite3.connect(_DB, check_same_thread=False)


def was_sent(signal_id: str, verdict: str) -> bool:
    with _connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM sent_signals WHERE signal_id = ? AND verdict = ?",
            (signal_id, verdict),
        ).fetchone()
    return row is not None


def mark_sent(signal_id: str, verdict: str) -> None:
    with _connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO sent_signals (signal_id, verdict, sent_at) VALUES (?, ?, ?)",
            (signal_id, verdict, datetime.utcnow().isoformat()),
        )
    logger.debug("Marked sent: %s / %s", signal_id, verdict)


def purge_old(days: int = 30) -> None:
    """Remove records older than `days` to keep the DB small."""
    cutoff = datetime.utcnow().isoformat()[:10]  # YYYY-MM-DD
    with _connect() as conn:
        conn.execute(
            "DELETE FROM sent_signals WHERE sent_at < ?",
            (cutoff,),
        )
