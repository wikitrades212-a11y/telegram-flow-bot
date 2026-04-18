"""
SQLite-backed deduplication store.

Tracks which (signal_id, verdict) pairs have already been sent to Channel B
so the bot never double-posts even across restarts.
"""

import sqlite3
import logging
import threading
import warnings
from datetime import datetime, timedelta
from pathlib import Path

import config

logger = logging.getLogger(__name__)

_DB = Path(config.DB_PATH)

if not config.DB_PATH or not Path(config.DB_PATH).is_absolute():
    warnings.warn(
        "DB_PATH is not set to an absolute path — using ephemeral path "
        f"'{config.DB_PATH}'. Signal history will be lost on redeploy. "
        "Set DB_PATH=/data/signals.db and mount a Railway persistent volume at /data.",
        RuntimeWarning,
        stacklevel=2,
    )


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
                price_eod        REAL,
                result_30m       TEXT,
                move_30m         REAL,
                classification   TEXT
            )
        """)
        # Migrate existing DB — ignored if columns already exist
        for _col in ("result_30m TEXT", "move_30m REAL", "classification TEXT",
                     "premium_at_signal REAL"):
            try:
                conn.execute(f"ALTER TABLE signals ADD COLUMN {_col}")
            except sqlite3.OperationalError:
                pass

        # Flow interpreter event log
        conn.execute("""
            CREATE TABLE IF NOT EXISTS flow_events (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                label      TEXT,
                signal_id  TEXT,
                ticker     TEXT,
                content    TEXT NOT NULL,
                timestamp  TEXT NOT NULL
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


def _push_to_dashboard(payload: dict) -> None:
    """
    Fire-and-forget POST to the local dashboard ingest endpoint.
    Runs in a background thread — never blocks record_signal().
    Silently ignored if DASHBOARD_INGEST_URL is unset or the server is offline.
    """
    url = config.__dict__.get("DASHBOARD_INGEST_URL", "") or ""
    if not url:
        return
    try:
        import httpx
        httpx.post(url, json=payload, timeout=2.0)
    except Exception:
        pass   # local server offline — ignore


def record_signal(sig, price, state: str = "HOLD", classification: str = None) -> None:
    """Insert a new signal row. INSERT OR IGNORE — safe to call on duplicates."""
    ts = datetime.utcnow().isoformat()
    prem = getattr(sig, "premium_at_signal", None)
    with _connect() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO signals
                (signal_id, ticker, side, strike, expiration,
                 premium_usd, delta, score, conviction,
                 state, timestamp_signal, price_at_signal, classification,
                 premium_at_signal)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sig.signal_id, sig.ticker, sig.side, sig.strike,
            sig.expiration.isoformat(), sig.premium_usd, sig.delta,
            sig.score, sig.conviction, state,
            ts, price, classification, prem,
        ))

    # Push copy to local dashboard (non-blocking, best-effort)
    threading.Thread(
        target=_push_to_dashboard,
        args=({
            "signal_id":         sig.signal_id,
            "ticker":            sig.ticker,
            "side":              sig.side,
            "strike":            sig.strike,
            "expiration":        sig.expiration.isoformat(),
            "premium_usd":       sig.premium_usd,
            "delta":             sig.delta,
            "score":             sig.score,
            "conviction":        sig.conviction,
            "state":             state,
            "timestamp_signal":  ts,
            "price_at_signal":   price,
            "classification":    classification,
            "premium_at_signal": prem,
        },),
        daemon=True,
    ).start()


def _push_event_to_dashboard(payload: dict) -> None:
    base_url = config.__dict__.get("DASHBOARD_INGEST_URL", "") or ""
    if not base_url:
        return
    url = base_url.rsplit("/api/", 1)[0] + "/api/ingest-event"
    try:
        import httpx
        httpx.post(url, json=payload, timeout=2.0)
    except Exception:
        pass


def record_event(
    event_type: str,
    content: str,
    label: str = None,
    signal_id: str = None,
    ticker: str = None,
) -> None:
    """Log a Channel A or B formatted message to flow_events."""
    ts = datetime.utcnow().isoformat()
    with _connect() as conn:
        conn.execute("""
            INSERT INTO flow_events (event_type, label, signal_id, ticker, content, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (event_type, label, signal_id, ticker, content, ts))
    threading.Thread(
        target=_push_event_to_dashboard,
        args=({
            "event_type": event_type,
            "label":      label,
            "signal_id":  signal_id,
            "ticker":     ticker,
            "content":    content,
            "timestamp":  ts,
        },),
        daemon=True,
    ).start()


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


def get_signal_entry(signal_id: str):
    """Return (price_at_go, price_at_signal) for outcome computation, or None."""
    with _connect() as conn:
        row = conn.execute(
            "SELECT price_at_go, price_at_signal FROM signals WHERE signal_id=?",
            (signal_id,),
        ).fetchone()
    return row


def update_outcome(signal_id: str, result: str, move: float) -> None:
    """Store 30-minute outcome result and move."""
    with _connect() as conn:
        conn.execute(
            "UPDATE signals SET result_30m=?, move_30m=? WHERE signal_id=?",
            (result, move, signal_id),
        )


def get_stats_summary(
    days: int = 7,
    ticker: str = None,
    classification: str = None,
) -> dict:
    """
    Query performance metrics from the signals table.

    Filters: last `days` days, optional ticker and classification.
    Only rows where result_30m IS NOT NULL are counted in outcome stats.
    Read-only — no writes.
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()

    filters = "timestamp_signal >= ?"
    params: list = [cutoff]

    if ticker:
        filters += " AND ticker = ?"
        params.append(ticker.upper())
    if classification:
        filters += " AND classification = ?"
        params.append(classification.upper())

    with _connect() as conn:
        # Overall signal counts
        row = conn.execute(f"""
            SELECT
                COUNT(*),
                SUM(CASE WHEN state='GO'   THEN 1 ELSE 0 END),
                SUM(CASE WHEN state='HOLD' THEN 1 ELSE 0 END),
                SUM(CASE WHEN state='KILL' THEN 1 ELSE 0 END)
            FROM signals WHERE {filters}
        """, params).fetchone()
        total, go_c, hold_c, kill_c = row

        # Outcome stats (result_30m not null only)
        row = conn.execute(f"""
            SELECT
                COUNT(*),
                SUM(CASE WHEN result_30m='WIN'  THEN 1 ELSE 0 END),
                SUM(CASE WHEN result_30m='LOSS' THEN 1 ELSE 0 END),
                SUM(CASE WHEN result_30m='FLAT' THEN 1 ELSE 0 END),
                AVG(move_30m)
            FROM signals WHERE {filters} AND result_30m IS NOT NULL
        """, params).fetchone()
        n_res, wins, losses, flats, avg_move = row

        # By classification
        cls_rows = conn.execute(f"""
            SELECT
                classification,
                COUNT(*),
                SUM(CASE WHEN result_30m='WIN' THEN 1 ELSE 0 END)
            FROM signals
            WHERE {filters} AND result_30m IS NOT NULL AND classification IS NOT NULL
            GROUP BY classification
            ORDER BY COUNT(*) DESC
        """, params).fetchall()

        # Top tickers by signal count
        ticker_rows = conn.execute(f"""
            SELECT
                ticker,
                COUNT(*),
                SUM(CASE WHEN result_30m='WIN' THEN 1 ELSE 0 END)
            FROM signals
            WHERE {filters} AND result_30m IS NOT NULL
            GROUP BY ticker
            ORDER BY COUNT(*) DESC
            LIMIT 5
        """, params).fetchall()

    n_res  = n_res  or 0
    wins   = wins   or 0
    losses = losses or 0
    flats  = flats  or 0

    return {
        "days":           days,
        "ticker_filter":  ticker,
        "class_filter":   classification,
        "total":          total  or 0,
        "go":             go_c   or 0,
        "hold":           hold_c or 0,
        "kill":           kill_c or 0,
        "n_results":      n_res,
        "wins":           wins,
        "losses":         losses,
        "flats":          flats,
        "win_rate":       round(wins / n_res * 100) if n_res > 0 else 0,
        "avg_move":       avg_move,
        "by_classification": [
            {
                "cls":      r[0],
                "n":        r[1],
                "wins":     r[2] or 0,
                "win_rate": round((r[2] or 0) / r[1] * 100) if r[1] > 0 else 0,
            }
            for r in cls_rows
        ],
        "top_tickers": [
            {
                "ticker":   r[0],
                "n":        r[1],
                "wins":     r[2] or 0,
                "win_rate": round((r[2] or 0) / r[1] * 100) if r[1] > 0 else 0,
            }
            for r in ticker_rows
        ],
    }


def purge_old(days: int = 30) -> None:
    """Remove records older than `days` to keep the DB small."""
    cutoff = datetime.utcnow().isoformat()[:10]  # YYYY-MM-DD
    with _connect() as conn:
        conn.execute(
            "DELETE FROM sent_signals WHERE sent_at < ?",
            (cutoff,),
        )
