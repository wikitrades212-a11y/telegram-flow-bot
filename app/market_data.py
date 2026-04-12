"""
Fetch live market data via yfinance.

Provides for any ticker:
  - current price
  - session VWAP (from 09:30 ET)
  - premarket high / premarket low (04:00–09:29 ET)

CandleBuffer tracks the last N *closed* 1-minute candle CLOSE prices per
ticker. "Closed" = every row except the most recent one in the 1m DataFrame
(the last row may still be forming intrabar).

All network calls are run in a thread-pool executor to keep the asyncio
event loop non-blocking.
"""

import asyncio
import logging
from datetime import datetime, time as dtime
from typing import Optional

import pytz
import yfinance as yf
import pandas as pd

logger = logging.getLogger(__name__)

ET = pytz.timezone("America/New_York")
_MARKET_OPEN = dtime(9, 30)
_PREMARKET_START = dtime(4, 0)
_PREMARKET_END = dtime(9, 30)

_CANDLE_BUFFER_SIZE = 10   # keep last 10 closed candles — more than enough for 2-candle checks


# ── Low-level fetchers (run in executor) ─────────────────────────────────────

def _fetch_1m(ticker: str) -> Optional[pd.DataFrame]:
    """Download today's 1-minute bars including pre/post market."""
    try:
        df = yf.Ticker(ticker).history(period="1d", interval="1m", prepost=True)
        if df.empty:
            return None
        if df.index.tzinfo is None:
            df.index = df.index.tz_localize("UTC").tz_convert(ET)
        else:
            df.index = df.index.tz_convert(ET)
        return df
    except Exception as exc:
        logger.error("yfinance fetch error for %s: %s", ticker, exc)
        return None


def _vwap_from_df(df: pd.DataFrame) -> Optional[float]:
    """Cumulative session VWAP anchored at 09:30 ET. Falls back to all bars if pre-market only."""
    try:
        session = df[df.index.time >= _MARKET_OPEN].copy()
        if session.empty:
            session = df.copy()
        if session["Volume"].sum() == 0:
            return None
        tp = (session["High"] + session["Low"] + session["Close"]) / 3
        vwap = (tp * session["Volume"]).cumsum() / session["Volume"].cumsum()
        return round(float(vwap.iloc[-1]), 4)
    except Exception as exc:
        logger.error("VWAP computation error: %s", exc)
        return None


def _premarket_levels_from_df(df: pd.DataFrame) -> tuple[Optional[float], Optional[float]]:
    """Return (pm_high, pm_low) for the 04:00–09:29 ET window."""
    try:
        pm = df[
            (df.index.time >= _PREMARKET_START) & (df.index.time < _PREMARKET_END)
        ]
        if pm.empty:
            return None, None
        return round(float(pm["High"].max()), 4), round(float(pm["Low"].min()), 4)
    except Exception as exc:
        logger.error("Premarket levels error: %s", exc)
        return None, None


def _current_price_from_df(df: pd.DataFrame) -> Optional[float]:
    try:
        return round(float(df["Close"].iloc[-1]), 4)
    except Exception:
        return None


def _closed_candle_closes_from_df(df: pd.DataFrame, n: int) -> list[float]:
    """
    Return up to the last n closed candle CLOSE prices from a 1m DataFrame.

    The final row of a live 1m DataFrame is the currently-forming (open) candle.
    Slicing df.iloc[:-1] gives only confirmed closed bars.
    We further restrict to session bars (>= 09:30 ET) so pre-market noise doesn't
    pollute the VWAP confirmation check; if session is empty we fall back to all
    closed bars (e.g. during pre-market itself).
    """
    if df is None or len(df) < 2:
        return []
    closed = df.iloc[:-1]                              # drop the open (live) candle
    session = closed[closed.index.time >= _MARKET_OPEN]
    if session.empty:
        session = closed
    closes = [round(float(v), 4) for v in session["Close"].tolist()]
    return closes[-n:]


# ── Candle buffer ─────────────────────────────────────────────────────────────

class CandleBuffer:
    """
    Stores the last N *closed* 1-minute candle CLOSE prices per ticker.

    Updated automatically by MarketDataService every time a fresh DataFrame
    is fetched. Read by the decision engine to enforce the 2-candle-close rule.
    """

    def __init__(self, n: int = _CANDLE_BUFFER_SIZE):
        self._n = n
        # ticker → [oldest, ..., newest]  (max n entries)
        self._data: dict[str, list[float]] = {}

    def update_from_df(self, ticker: str, df: pd.DataFrame) -> None:
        closes = _closed_candle_closes_from_df(df, self._n)
        if closes:
            self._data[ticker] = closes
            logger.debug("CandleBuffer[%s] updated: %s", ticker, closes[-3:])

    def last_closes(self, ticker: str, n: int) -> list[float]:
        """
        Return the last n closed candle closes for ticker.
        Returns [] (empty) if fewer than n closes are available — callers
        must treat an empty result as 'not enough data → no trigger'.
        """
        buf = self._data.get(ticker, [])
        if len(buf) < n:
            return []
        return buf[-n:]

    def __repr__(self) -> str:
        return f"CandleBuffer({list(self._data.keys())})"


# ── Snapshot type ─────────────────────────────────────────────────────────────

class Snapshot:
    __slots__ = ("ticker", "price", "vwap", "pm_high", "pm_low", "fetched_at", "fetch_ok")

    def __init__(self, ticker: str, price, vwap, pm_high, pm_low, fetch_ok: bool = True):
        self.ticker = ticker
        self.price: Optional[float] = price
        self.vwap: Optional[float] = vwap
        self.pm_high: Optional[float] = pm_high
        self.pm_low: Optional[float] = pm_low
        self.fetched_at: float = datetime.utcnow().timestamp()
        self.fetch_ok: bool = fetch_ok   # False when yfinance failed or returned empty

    def above_vwap(self) -> Optional[bool]:
        if self.price is None or self.vwap is None:
            return None
        return self.price > self.vwap

    def below_vwap(self) -> Optional[bool]:
        if self.price is None or self.vwap is None:
            return None
        return self.price < self.vwap

    def __repr__(self) -> str:
        return (
            f"Snapshot({self.ticker} price={self.price} vwap={self.vwap} "
            f"pm_high={self.pm_high} pm_low={self.pm_low})"
        )


# ── Service ───────────────────────────────────────────────────────────────────

class MarketDataService:
    """TTL-cached async market data service with integrated candle buffer.

    On yfinance failure the service tries three layers in order:
      1. Fresh fetch  — returned if within cache TTL
      2. Stale cache  — last successful snapshot, served up to STALE_TTL seconds
      3. Empty stub   — Snapshot with fetch_ok=False, signals data unavailable
    """

    def __init__(self, cache_ttl_seconds: int = 120, stale_ttl_seconds: int = 300):
        self._ttl = cache_ttl_seconds
        self._stale_ttl = stale_ttl_seconds
        self._cache: dict[str, tuple[float, Snapshot]] = {}
        self._last_good: dict[str, tuple[float, Snapshot]] = {}   # last successful fetch
        # Per-ticker lock — prevents multiple coroutines firing yfinance for the
        # same ticker simultaneously (thundering-herd on cache miss / startup).
        self._locks: dict[str, asyncio.Lock] = {}
        self.candles = CandleBuffer()

    def _lock(self, ticker: str) -> asyncio.Lock:
        """Return (creating if needed) the per-ticker fetch lock."""
        return self._locks.setdefault(ticker, asyncio.Lock())

    async def snapshot(self, ticker: str) -> Snapshot:
        """Return a Snapshot for ticker, falling back to stale data on failure.

        Uses double-checked locking so concurrent callers for the same ticker
        (e.g. three watcher entries all needing SPY at the same moment) share
        a single yfinance call instead of each issuing their own.
        """
        now = datetime.utcnow().timestamp()

        # Fast path — cache hit (no lock needed for a dict read)
        cached = self._cache.get(ticker)
        if cached and (now - cached[0]) < self._ttl:
            return cached[1]

        async with self._lock(ticker):
            # Re-check inside the lock — another coroutine may have fetched while
            # this one was waiting to acquire it.
            now = datetime.utcnow().timestamp()
            cached = self._cache.get(ticker)
            if cached and (now - cached[0]) < self._ttl:
                return cached[1]

            # Layer 1 — live fetch
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(None, _fetch_1m, ticker)

            if df is not None:
                price = _current_price_from_df(df)
                vwap = _vwap_from_df(df)
                pm_high, pm_low = _premarket_levels_from_df(df)
                snap = Snapshot(ticker, price, vwap, pm_high, pm_low, fetch_ok=True)
                self.candles.update_from_df(ticker, df)
                logger.info("Fetched market data: %s", snap)
                self._cache[ticker] = (now, snap)
                self._last_good[ticker] = (now, snap)
                return snap

            # Layer 2 — serve stale last-good data if recent enough
            last_good = self._last_good.get(ticker)
            if last_good and (now - last_good[0]) < self._stale_ttl:
                age = int(now - last_good[0])
                logger.warning(
                    "yfinance fetch failed for %s — serving stale snapshot (%ds old)", ticker, age
                )
                return last_good[1]   # fetch_ok=True — real data, just aged

            # Layer 3 — no usable data at all
            logger.error("yfinance fetch failed for %s — no usable stale data", ticker)
            snap = Snapshot(ticker, None, None, None, None, fetch_ok=False)
            # Cache the failure for TTL/4 to avoid hammering the API on every tick
            self._cache[ticker] = (now - self._ttl * 0.75, snap)
            return snap

    async def context(self, ticker: str) -> dict[str, Snapshot]:
        """Fetch SPY, QQQ, and target ticker in parallel. Never raises."""
        results = await asyncio.gather(
            self.snapshot("SPY"),
            self.snapshot("QQQ"),
            self.snapshot(ticker),
            return_exceptions=True,
        )

        def _safe(result, sym: str) -> Snapshot:
            if isinstance(result, Exception):
                logger.error("snapshot(%s) raised unexpectedly: %s", sym, result)
                return Snapshot(sym, None, None, None, None, fetch_ok=False)
            return result

        return {
            "SPY":  _safe(results[0], "SPY"),
            "QQQ":  _safe(results[1], "QQQ"),
            ticker: _safe(results[2], ticker),
        }

    def invalidate(self, ticker: str):
        self._cache.pop(ticker, None)
