"""
Fetch live market data via Alpaca Markets Data API v2.

Provides for any ticker:
  - current price  (last 1-minute bar close)
  - session VWAP   (cumulative from 09:30 ET, falls back to all bars pre-open)
  - premarket high / premarket low (04:00–09:29 ET bars)

CandleBuffer tracks the last N *closed* 1-minute candle CLOSE prices per
ticker. "Closed" = every bar except the most recent one (which may still
be forming intrabar).

All network I/O uses httpx async client — no thread-pool executor needed.
"""

import asyncio
import logging
from datetime import datetime, time as dtime
from typing import Optional

import httpx
import pytz

import config

logger = logging.getLogger(__name__)

ET = pytz.timezone("America/New_York")
_MARKET_OPEN     = dtime(9, 30)
_PREMARKET_START = dtime(4, 0)
_PREMARKET_END   = dtime(9, 30)
_ALPACA_DATA_URL = "https://data.alpaca.markets"
_CANDLE_BUFFER_SIZE = 10   # keep last 10 closed candles — enough for 2-candle checks


def _is_trading_session() -> bool:
    """True during hours when equity bars are expected: Mon–Fri 04:00–20:00 ET."""
    now = datetime.now(ET)
    if now.weekday() >= 5:          # Saturday=5, Sunday=6
        return False
    return dtime(4, 0) <= now.time() <= dtime(20, 0)


# ── Internal bar type ─────────────────────────────────────────────────────────

class _Bar:
    __slots__ = ("time_et", "open", "high", "low", "close", "volume")

    def __init__(self, time_et: dtime, open_: float, high: float,
                 low: float, close: float, volume: float):
        self.time_et = time_et
        self.open    = open_
        self.high    = high
        self.low     = low
        self.close   = close
        self.volume  = volume


def _parse_bars(raw: list[dict]) -> list[_Bar]:
    """Convert Alpaca JSON bar list to _Bar objects with ET timestamps."""
    bars = []
    for b in raw:
        ts_utc = datetime.fromisoformat(b["t"].replace("Z", "+00:00"))
        ts_et  = ts_utc.astimezone(ET)
        bars.append(_Bar(
            time_et=ts_et.time(),
            open_=b["o"],
            high=b["h"],
            low=b["l"],
            close=b["c"],
            volume=b["v"],
        ))
    return bars


# ── Alpaca fetch ──────────────────────────────────────────────────────────────

async def _fetch_bars_alpaca(ticker: str) -> Optional[list[_Bar]]:
    """
    Fetch today's 1-minute bars from Alpaca Data API v2 (premarket through now).
    Returns None on any network or auth error — callers fall back to stale cache.
    """
    now_et   = datetime.now(ET)
    start_et = now_et.replace(hour=4, minute=0, second=0, microsecond=0)

    headers = {
        "APCA-API-KEY-ID":     config.ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": config.ALPACA_API_SECRET,
    }
    params = {
        "timeframe": "1Min",
        "start":     start_et.isoformat(),
        "feed":      config.ALPACA_FEED,
        "limit":     1000,
        "sort":      "asc",
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{_ALPACA_DATA_URL}/v2/stocks/{ticker}/bars",
                headers=headers,
                params=params,
            )
            resp.raise_for_status()
            raw = resp.json().get("bars") or []

        if not raw:
            if _is_trading_session():
                logger.warning("Alpaca: no bars returned for %s", ticker)
            else:
                logger.debug("Alpaca: no bars for %s (market closed)", ticker)
            return None

        return _parse_bars(raw)

    except httpx.HTTPStatusError as exc:
        logger.error(
            "Alpaca HTTP %s for %s: %s",
            exc.response.status_code, ticker, exc.response.text[:200],
        )
        return None
    except Exception as exc:
        logger.error("Alpaca fetch error for %s: %s", ticker, exc)
        return None


# ── Derived calculations ──────────────────────────────────────────────────────

def _current_price(bars: list[_Bar]) -> Optional[float]:
    return round(bars[-1].close, 4) if bars else None


def _vwap(bars: list[_Bar]) -> Optional[float]:
    """Cumulative session VWAP anchored at 09:30 ET. Falls back to all bars pre-open."""
    session = [b for b in bars if b.time_et >= _MARKET_OPEN]
    if not session:
        session = bars
    if not session:
        return None
    total_vol = sum(b.volume for b in session)
    if total_vol == 0:
        return None
    tp_vol = sum(((b.high + b.low + b.close) / 3) * b.volume for b in session)
    return round(tp_vol / total_vol, 4)


def _premarket_levels(bars: list[_Bar]) -> tuple[Optional[float], Optional[float]]:
    """Return (pm_high, pm_low) for the 04:00–09:29 ET window."""
    pm = [b for b in bars if _PREMARKET_START <= b.time_et < _PREMARKET_END]
    if not pm:
        return None, None
    return round(max(b.high for b in pm), 4), round(min(b.low for b in pm), 4)


def _closed_candle_closes(bars: list[_Bar], n: int) -> list[float]:
    """
    Return up to the last n closed candle CLOSE prices.

    Drop the last bar (may still be forming), restrict to session bars
    (>= 09:30 ET) to avoid pre-market noise in the VWAP confirmation check.
    Falls back to all closed bars during pre-market.
    """
    if len(bars) < 2:
        return []
    closed  = bars[:-1]
    session = [b for b in closed if b.time_et >= _MARKET_OPEN]
    source  = session if session else closed
    return [round(b.close, 4) for b in source][-n:]


# ── Candle buffer ─────────────────────────────────────────────────────────────

class CandleBuffer:
    """
    Stores the last N *closed* 1-minute candle CLOSE prices per ticker.

    Updated by MarketDataService on every successful fetch.
    Read by the decision engine to enforce the 2-candle-close trigger rule.
    """

    def __init__(self, n: int = _CANDLE_BUFFER_SIZE):
        self._n = n
        self._data: dict[str, list[float]] = {}

    def update(self, ticker: str, bars: list[_Bar]) -> None:
        closes = _closed_candle_closes(bars, self._n)
        if closes:
            self._data[ticker] = closes
            logger.debug("CandleBuffer[%s] updated: %s", ticker, closes[-3:])

    def last_closes(self, ticker: str, n: int) -> list[float]:
        """
        Return the last n closed candle closes for ticker.
        Returns [] if fewer than n closes are available — callers must treat
        an empty result as 'not enough data → no trigger'.
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
        self.ticker   = ticker
        self.price:    Optional[float] = price
        self.vwap:     Optional[float] = vwap
        self.pm_high:  Optional[float] = pm_high
        self.pm_low:   Optional[float] = pm_low
        self.fetched_at: float = datetime.utcnow().timestamp()
        self.fetch_ok: bool = fetch_ok

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

    On Alpaca failure the service tries three layers in order:
      1. Fresh fetch  — returned if within cache TTL
      2. Stale cache  — last successful snapshot, served up to STALE_TTL seconds
      3. Empty stub   — Snapshot with fetch_ok=False, signals data unavailable
    """

    def __init__(self, cache_ttl_seconds: int = 120, stale_ttl_seconds: int = 300):
        self._ttl        = cache_ttl_seconds
        self._stale_ttl  = stale_ttl_seconds
        self._cache:     dict[str, tuple[float, Snapshot]] = {}
        self._last_good: dict[str, tuple[float, Snapshot]] = {}
        # Per-ticker lock — prevents concurrent fetches for the same ticker.
        self._locks: dict[str, asyncio.Lock] = {}
        self.candles = CandleBuffer()

    def _lock(self, ticker: str) -> asyncio.Lock:
        return self._locks.setdefault(ticker, asyncio.Lock())

    async def snapshot(self, ticker: str) -> Snapshot:
        """Return a Snapshot for ticker, falling back to stale data on failure.

        Uses double-checked locking so concurrent callers for the same ticker
        share a single Alpaca request instead of each issuing their own.
        """
        now = datetime.utcnow().timestamp()

        # Fast path — cache hit (no lock needed for a dict read)
        cached = self._cache.get(ticker)
        if cached and (now - cached[0]) < self._ttl:
            return cached[1]

        async with self._lock(ticker):
            # Re-check inside the lock — another coroutine may have fetched
            # while this one was waiting to acquire it.
            now    = datetime.utcnow().timestamp()
            cached = self._cache.get(ticker)
            if cached and (now - cached[0]) < self._ttl:
                return cached[1]

            # Layer 1 — live fetch
            bars = await _fetch_bars_alpaca(ticker)

            if bars is not None:
                price              = _current_price(bars)
                vwap               = _vwap(bars)
                pm_high, pm_low    = _premarket_levels(bars)
                snap               = Snapshot(ticker, price, vwap, pm_high, pm_low, fetch_ok=True)
                self.candles.update(ticker, bars)
                logger.info("Fetched market data: %s", snap)
                self._cache[ticker]     = (now, snap)
                self._last_good[ticker] = (now, snap)
                return snap

            # Layer 2 — serve stale last-good data if recent enough
            last_good = self._last_good.get(ticker)
            if last_good and (now - last_good[0]) < self._stale_ttl:
                age = int(now - last_good[0])
                logger.warning(
                    "Alpaca fetch failed for %s — serving stale snapshot (%ds old)",
                    ticker, age,
                )
                return last_good[1]

            # Layer 3 — no usable data at all
            if _is_trading_session():
                logger.error("Alpaca fetch failed for %s — no usable stale data", ticker)
            else:
                logger.debug("Alpaca: no data for %s (market closed)", ticker)
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

    def invalidate(self, ticker: str) -> None:
        self._cache.pop(ticker, None)
