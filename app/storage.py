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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                signal_id        TEXT PRIMARY KEY,
                ticker           TEXT NOT NULL,
                side             TEXT NOT NULL,
                strike           REAL,
                expiration       TEXT,
                premium_usd      REAL,
                delta            REAL,
                score            INTEGER,
                conviction       TEXT,
                state            TEXT NOT NULL DEFAULT 'HOLD',
                timestamp_signal TEXT NOT NULL,
                timestamp_go     TEXT,
                price_at_signal  REAL,
                price_at_go      REAL,
                price_5m         REAL,
                price_15m        REAL,
                price_1h         REAL,
                price_eod        REAL
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


def record_signal(sig, price, state: str = "HOLD") -> None:
    """Insert a new signal row. INSERT OR IGNORE — safe to call on duplicates."""
    with _connect() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO signals
                (signal_id, ticker, side, strike, expiration,
                 premium_usd, delta, score, conviction,
                 state, timestamp_signal, price_at_signal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sig.signal_id, sig.ticker, sig.side, sig.strike,
            sig.expiration.isoformat(), sig.premium_usd, sig.delta,
            sig.score, sig.conviction, state,
            datetime.utcnow().isoformat(), price,
        ))


def update_signal_go(signal_id: str, price, ts: str) -> None:
    """Mark a signal as GO and record the go price and timestamp."""
    with _connect() as conn:
        conn.execute(
            "UPDATE signals SET state='GO', price_at_go=?, timestamp_go=? WHERE signal_id=?",
            (price, ts, signal_id),
        )


def update_price_check(signal_id: str, column: str, price: float) -> None:
    """Store a delayed price check. column must be one of the allowed names."""
    allowed = {"price_5m", "price_15m", "price_1h", "price_eod"}
    if column not in allowed:
        return
    with _connect() as conn:
        conn.execute(
            f"UPDATE signals SET {column}=? WHERE signal_id=?",
            (price, signal_id),
        )


def purge_old(days: int = 30) -> None:
    """Remove records older than `days` to keep the DB small."""
    cutoff = datetime.utcnow().isoformat()[:10]  # YYYY-MM-DD
    with _connect() as conn:
        conn.execute(
            "DELETE FROM sent_signals WHERE sent_at < ?",
            (cutoff,),
        )
