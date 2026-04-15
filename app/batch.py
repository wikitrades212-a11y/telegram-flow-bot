"""
Batch accumulator for Channel B market intelligence reports.

Collects classified signals until the trigger count is reached,
then analyzes them to produce a market state report.

Market states: TREND | ROTATION | CHOP | DISTRIBUTION | TRAP | BULLISH WITH HEDGING

Trade modes:
  BEARISH → FAILED BOUNCE SHORT
  BULLISH → DIP BUY
  CHOP    → PAIR TRADE

v2: adds hedge detection (hedge_detector), leaders/drags extraction, and
    macro_override flag — all consumed by bot_data.BotDataBlock.
"""

import logging
from dataclasses import dataclass
from typing import Optional
from app.hedge_detector import is_hedging, classify_hedge

logger = logging.getLogger(__name__)


@dataclass
class BatchEntry:
    signal_id: str
    ticker: str
    side: str
    premium_usd: float
    score: int
    classification: str
    signal_role: str
    priority: int
    decision: str   # "HOLD" | "GO" | "KILL" | "ERROR"
    # Extra fields for Channel B formatted report
    strike: float = 0.0
    iv_pct: float = 0.0
    vol_oi_ratio: float = 0.0
    delta: Optional[float] = None         # None = not reported (not zero delta)
    dte: int = 0
    direction: str = "NEUTRAL"
    option_price: Optional[float] = None  # per-contract mid/last at time of callout


class BatchStore:
    def __init__(self, trigger_count: int = 3):
        self._trigger  = trigger_count
        self._entries: list[BatchEntry] = []

    def add(
        self,
        sig,
        classification: str,
        signal_role: str,
        priority: int,
        decision: str,
    ) -> None:
        self._entries.append(BatchEntry(
            signal_id=sig.signal_id,
            ticker=sig.ticker,
            side=sig.side,
            premium_usd=sig.premium_usd,
            score=sig.score,
            classification=classification,
            signal_role=signal_role,
            priority=priority,
            decision=decision,
            strike=getattr(sig, "strike", 0.0),
            iv_pct=getattr(sig, "iv_pct", 0.0),
            vol_oi_ratio=getattr(sig, "vol_oi_ratio", 0.0),
            delta=getattr(sig, "delta", None),        # preserve None — do not coerce to 0.0
            dte=getattr(sig, "dte", 0),
            direction=getattr(sig, "direction", "NEUTRAL"),
            option_price=(
                getattr(sig, "option_mid", None)
                or getattr(sig, "option_last", None)
            ),
        ))
        logger.debug(
            "Batch add | %s | cls=%s | role=%s | p%d | decision=%s | size=%d",
            sig.signal_id, classification, signal_role, priority, decision, len(self._entries),
        )

    def should_post(self) -> bool:
        return len(self._entries) >= self._trigger

    def analyze_and_reset(self) -> dict:
        entries = list(self._entries)
        self._entries.clear()
        logger.info("Batch firing | %d signals", len(entries))
        return _analyze(entries)

    def analyze_peek(self) -> dict:
        """Analyze current entries without clearing."""
        return _analyze(list(self._entries))

    def size(self) -> int:
        return len(self._entries)


# ── Analysis ──────────────────────────────────────────────────────────────────

def _fmt_premium(usd: float) -> str:
    if usd >= 1_000_000:
        return f"${usd / 1_000_000:.1f}M"
    if usd >= 1_000:
        return f"${usd / 1_000:.0f}K"
    return f"${usd:.0f}"


def _analyze(entries: list[BatchEntry]) -> dict:
    if not entries:
        return {}

    total    = len(entries)
    bull     = [e for e in entries if e.side == "CALL"]
    bear     = [e for e in entries if e.side == "PUT"]
    bull_pct = round(len(bull) / total * 100)
    bear_pct = 100 - bull_pct

    hedge_count = sum(1 for e in entries if e.classification == "HEDGE_DIRECTIONAL")
    positional  = [e for e in entries if "POSITIONAL" in e.classification]
    mkt_signals = [e for e in entries if e.signal_role == "MARKET_SIGNAL"]

    # ── Bias ──────────────────────────────────────────────────────────────────
    diff       = abs(bull_pct - bear_pct)
    confidence = min(100, diff + hedge_count * 10)
    direction  = "BULLISH" if bull_pct >= bear_pct else "BEARISH"

    # Hedge detection — use the dedicated detector (not just classification label)
    hedging = is_hedging(entries, direction)

    if hedge_count >= 2 or hedging:
        subtype = "HEDGED"
    elif positional:
        subtype = "POSITIONAL"
    else:
        subtype = "SPECULATIVE"

    # ── Market state ──────────────────────────────────────────────────────────
    if confidence < 20:
        state = "CHOP"
    elif (hedge_count >= 2 or hedging) and bull_pct > 55:
        state = "BULLISH WITH HEDGING"
    elif (hedge_count >= 2 or hedging) and bear_pct > 55:
        state = "DISTRIBUTION"
    elif bull_pct >= 70 or bear_pct >= 70:
        state = "TREND"
    elif 40 <= bull_pct <= 60 and not mkt_signals:
        state = "ROTATION"
    else:
        state = "CHOP"

    # ── Trade mode ────────────────────────────────────────────────────────────
    if direction == "BEARISH" and state in ("TREND", "DISTRIBUTION", "BULLISH WITH HEDGING"):
        mode        = "BEARISH"
        trade_logic = "FAILED BOUNCE SHORT\nBounce → resistance → stall → leaders weaken → short"
    elif direction == "BULLISH" and state in ("TREND", "BULLISH WITH HEDGING"):
        mode        = "BULLISH"
        trade_logic = "DIP BUY\nPullback → support → hold → leaders strong → long"
    else:
        mode        = "CHOP"
        trade_logic = "PAIR TRADE\nLong strong sector / short weak sector"

    # ── Drivers: priority 1–2, market/sector first ────────────────────────────
    drivers = [e for e in entries
               if e.priority <= 2 and e.signal_role in ("MARKET_SIGNAL", "SECTOR_SIGNAL")]
    if not drivers:
        drivers = sorted([e for e in entries if e.priority <= 2], key=lambda x: x.priority)

    # ── Trade candidates ──────────────────────────────────────────────────────
    # Ignore GAMMA_VOL and LOTTERY per spec
    actionable = [e for e in entries
                  if e.classification not in ("GAMMA_VOL", "LOTTERY")
                  and e.decision != "KILL"]

    high_conviction = [e for e in actionable if e.priority <= 2]
    speculative     = [e for e in actionable if e.priority == 3]

    # ── Noise ─────────────────────────────────────────────────────────────────
    noise = [e for e in entries
             if e.signal_role == "NOISE"
             or e.classification in ("GAMMA_VOL", "LOTTERY")
             or e.priority >= 4]

    # ── Sectors ───────────────────────────────────────────────────────────────
    bull_sec  = sorted({e.ticker for e in bull if e.signal_role == "SECTOR_SIGNAL"})
    bear_sec  = sorted({e.ticker for e in bear if e.signal_role == "SECTOR_SIGNAL"})
    # Tickers appearing on both sides are neutral
    neutral_sec = sorted(set(bull_sec) & set(bear_sec))
    bull_sec    = [t for t in bull_sec if t not in neutral_sec]
    bear_sec    = [t for t in bear_sec if t not in neutral_sec]

    # ── Tags ──────────────────────────────────────────────────────────────────
    tags: list[str] = []
    if hedge_count >= 2 or hedging:
        tags.append("HEDGE_CLUSTER")
    if confidence < 20:
        tags.append("LOW_CONVICTION")
    if len({e.ticker for e in mkt_signals}) >= 2:
        tags.append("BROAD_MARKET")
    if len(entries) >= 5:
        tags.append("HIGH_VOLUME_SESSION")

    # ── Leaders / Drags (for BOT_DATA block) ─────────────────────────────────
    # Leaders: non-index tickers whose flow ALIGNS with overall direction
    # Drags:   non-index tickers whose flow OPPOSES overall direction
    from app.classifier import MARKET_TICKERS  # local import avoids circular dep
    _aligned_side  = "CALL" if direction == "BULLISH" else "PUT"
    _opposed_side  = "PUT"  if direction == "BULLISH" else "CALL"

    leaders_set: list[str] = []
    drags_set: list[str]   = []
    seen_leaders: set[str] = set()
    seen_drags:   set[str] = set()

    for e in sorted(entries, key=lambda x: x.premium_usd, reverse=True):
        if e.ticker in MARKET_TICKERS:
            continue
        if e.side == _aligned_side and e.ticker not in seen_leaders:
            leaders_set.append(e.ticker)
            seen_leaders.add(e.ticker)
        elif e.side == _opposed_side and e.ticker not in seen_drags:
            drags_set.append(e.ticker)
            seen_drags.add(e.ticker)

    leaders = leaders_set[:5]
    drags   = drags_set[:5]

    # ── Macro override — market/index signals dominate the batch ─────────────
    # True when ≥2 distinct market-tier tickers appear in the batch
    macro_override = len({e.ticker for e in mkt_signals}) >= 2

    return {
        "total":           total,
        "state":           state,
        "mode":            mode,
        "trade_logic":     trade_logic,
        "direction":       direction,
        "subtype":         subtype,
        "bull_pct":        bull_pct,
        "bear_pct":        bear_pct,
        "confidence":      confidence,
        "hedging":         hedging,
        "macro_override":  macro_override,
        "leaders":         leaders,
        "drags":           drags,
        "drivers":         drivers,
        "high_conviction": high_conviction,
        "speculative":     speculative,
        "noise":           noise,
        "sectors_strong":  bull_sec if direction == "BULLISH" else bear_sec,
        "sectors_weak":    bear_sec if direction == "BULLISH" else bull_sec,
        "sectors_neutral": neutral_sec,
        "tags":            tags,
        "entries":         entries,
    }
