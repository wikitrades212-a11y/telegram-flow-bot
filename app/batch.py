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

    from app.hedge_detector import classify_hedge, HEDGE_TYPE_HEDGE, HEDGE_TYPE_PROBABLE
    from app.classifier import MARKET_TICKERS

    total    = len(entries)
    bull     = [e for e in entries if e.side == "CALL"]
    bear     = [e for e in entries if e.side == "PUT"]
    bull_pct = round(len(bull) / total * 100)
    bear_pct = 100 - bull_pct

    # ── Classify entries as SPEC vs HEDGE ─────────────────────────────────────
    # Bootstrap direction from raw flow, then use it for hedge classification.
    raw_direction = "BULLISH" if bull_pct >= bear_pct else "BEARISH"

    hedge_entries: list[BatchEntry] = []
    spec_entries:  list[BatchEntry] = []
    for e in entries:
        hr = classify_hedge(
            side=e.side,
            delta=e.delta,
            vol_oi_ratio=e.vol_oi_ratio,
            premium_usd=e.premium_usd,
            market_direction=raw_direction,
        )
        if hr.hedge_type in (HEDGE_TYPE_HEDGE, HEDGE_TYPE_PROBABLE):
            hedge_entries.append(e)
        else:
            spec_entries.append(e)

    # ── Bias from SPEC flow only (hedges excluded) ────────────────────────────
    spec_total    = len(spec_entries)
    spec_bull_pct = round(sum(1 for e in spec_entries if e.side == "CALL") / spec_total * 100) if spec_total else bull_pct
    spec_bear_pct = 100 - spec_bull_pct

    spec_diff = abs(spec_bull_pct - spec_bear_pct)
    if spec_diff < 10:
        direction = "NEUTRAL"
    elif spec_bull_pct >= spec_bear_pct:
        direction = "BULLISH"
    else:
        direction = "BEARISH"

    # bias_confidence: how lopsided spec flow is — hedges do NOT inflate this
    bias_confidence = min(100, spec_diff * 2)

    hedge_count = len(hedge_entries)
    positional  = [e for e in entries if "POSITIONAL" in e.classification]
    mkt_signals = [e for e in entries if e.signal_role == "MARKET_SIGNAL"]

    hedging = is_hedging(entries, direction)

    if hedge_count >= 2 or hedging:
        subtype = "HEDGED"
    elif positional:
        subtype = "POSITIONAL"
    else:
        subtype = "SPECULATIVE"

    # ── Market state ──────────────────────────────────────────────────────────
    if bias_confidence < 20:
        state = "CHOP"
    elif (hedge_count >= 2 or hedging) and spec_bull_pct > 55:
        state = "BULLISH WITH HEDGING"
    elif (hedge_count >= 2 or hedging) and spec_bear_pct > 55:
        state = "DISTRIBUTION"
    elif spec_bull_pct >= 70 or spec_bear_pct >= 70:
        state = "TREND"
    elif 40 <= spec_bull_pct <= 60 and not mkt_signals:
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

    # ── Drivers ───────────────────────────────────────────────────────────────
    drivers = [e for e in entries
               if e.priority <= 2 and e.signal_role in ("MARKET_SIGNAL", "SECTOR_SIGNAL")]
    if not drivers:
        drivers = sorted([e for e in entries if e.priority <= 2], key=lambda x: x.priority)

    # ── Trade candidates ──────────────────────────────────────────────────────
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
    bull_sec    = sorted({e.ticker for e in bull  if e.signal_role == "SECTOR_SIGNAL"})
    bear_sec    = sorted({e.ticker for e in bear  if e.signal_role == "SECTOR_SIGNAL"})
    neutral_sec = sorted(set(bull_sec) & set(bear_sec))
    bull_sec    = [t for t in bull_sec if t not in neutral_sec]
    bear_sec    = [t for t in bear_sec if t not in neutral_sec]

    # ── Tags ──────────────────────────────────────────────────────────────────
    tags: list[str] = []
    if hedge_count >= 2 or hedging:
        tags.append("HEDGE_CLUSTER")
    if bias_confidence < 20:
        tags.append("LOW_CONVICTION")
    if len({e.ticker for e in mkt_signals}) >= 2:
        tags.append("BROAD_MARKET")
    if len(entries) >= 5:
        tags.append("HIGH_VOLUME_SESSION")

    # ── Leaders / Drags — single source of truth ──────────────────────────────
    # A ticker that appears on BOTH sides → MIXED (excluded from leaders & drags).
    # A ticker can never be in both leaders and drags.
    _aligned_side = "CALL" if direction == "BULLISH" else "PUT"
    _opposed_side = "PUT"  if direction == "BULLISH" else "CALL"

    ticker_sides: dict[str, set] = {}
    for e in entries:
        if e.ticker in MARKET_TICKERS:
            continue
        ticker_sides.setdefault(e.ticker, set()).add(e.side)

    mixed_tickers = sorted(t for t, sides in ticker_sides.items() if len(sides) == 2)

    leaders_list: list[str] = []
    drags_list:   list[str] = []
    seen_all:     set[str]  = set()

    for e in sorted(entries, key=lambda x: x.premium_usd, reverse=True):
        if e.ticker in MARKET_TICKERS or e.ticker in mixed_tickers:
            continue
        if e.ticker in seen_all:
            continue
        seen_all.add(e.ticker)
        if e.side == _aligned_side:
            leaders_list.append(e.ticker)
        elif e.side == _opposed_side:
            drags_list.append(e.ticker)

    leaders = leaders_list[:5]
    drags   = drags_list[:5]
    mixed   = mixed_tickers[:3]

    # ── Macro override ─────────────────────────────────────────────────────────
    macro_override = len({e.ticker for e in mkt_signals}) >= 2

    return {
        "total":            total,
        "state":            state,
        "mode":             mode,
        "trade_logic":      trade_logic,
        "direction":        direction,
        "subtype":          subtype,
        "bull_pct":         bull_pct,
        "bear_pct":         bear_pct,
        "spec_bull_pct":    spec_bull_pct,
        "spec_bear_pct":    spec_bear_pct,
        "bias_confidence":  bias_confidence,
        "confidence":       bias_confidence,   # backward-compat alias
        "hedging":          hedging,
        "hedge_entries":    hedge_entries,
        "spec_entries":     spec_entries,
        "macro_override":   macro_override,
        "leaders":          leaders,
        "drags":            drags,
        "mixed":            mixed,
        "drivers":          drivers,
        "high_conviction":  high_conviction,
        "speculative":      speculative,
        "noise":            noise,
        "sectors_strong":   bull_sec if direction == "BULLISH" else bear_sec,
        "sectors_weak":     bear_sec if direction == "BULLISH" else bull_sec,
        "sectors_neutral":  neutral_sec,
        "tags":             tags,
        "entries":          entries,
    }
