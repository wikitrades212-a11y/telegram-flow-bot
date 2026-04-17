"""
Format outbound messages for Channel A and Channel B.

Channel A (format_intel) — per-flow YAML, see intel_formatter.py.
Channel B templates:
  format_hold()                  — legacy; kept for compatibility
  format_go()                    — real-time GO alert (still posted immediately)
  format_channel_b_report()      — structured batch report from raw signals
  format_aggregated_report_b()   — Channel B output from aggregated intel report
  format_premarket_report()      — forced 8:30 AM pre-market bias report
  format_batch_report()          — old report (kept for /testsignal fallback)
  format_stats()                 — /stats command reply

All messages use plain text (no HTML parse_mode) unless explicitly noted.
Channel B send_message uses NO parse_mode — plain UTF-8.
"""

import re
from typing import TYPE_CHECKING, Optional

from app.parser import FlowSignal
from app.decision_engine import Decision
from app.session import current_session, baseline_data_quality, degrade_data_quality
from app.bot_data import build_bot_data, render_bot_data

if TYPE_CHECKING:
    from app.rs_engine import MarketRS, IndexRS


# ── Shared helpers ────────────────────────────────────────────────────────────

def _fmt(value, suffix: str = "", fmt: str = ".2f") -> str:
    if value is None:
        return "N/A"
    return f"{value:{fmt}}{suffix}"


def _fmt_p(usd: float) -> str:
    if usd >= 1_000_000:
        return f"${usd / 1_000_000:.1f}M"
    if usd >= 1_000:
        return f"${usd / 1_000:.0f}K"
    return f"${usd:.0f}"


_MARKET_TICKERS = {"SPY", "QQQ", "IWM", "SPX", "NDX", "DIA"}

# Tech tickers for /tech filter — ETFs + mega-cap + semis + cloud/cyber
_TECH_TICKERS: frozenset[str] = frozenset({
    # Tech-heavy ETFs / indices
    "QQQ", "XLK", "SMH", "SOXX", "ARKK",
    # Mega-cap tech
    "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "META", "AMZN", "TSLA",
    "AMD", "INTC", "QCOM", "CRM", "ORCL", "AVGO", "AMAT", "MU",
    "LRCX", "KLAC", "NFLX", "ADBE", "NOW", "INTU", "SNPS", "CDNS",
    "TXN", "ON", "MRVL", "MPWR",
    # Cloud / cybersecurity
    "PANW", "CRWD", "ZS", "FTNT", "NET", "DDOG", "SNOW", "OKTA",
    "PLTR", "COIN", "HOOD",
})

# Tickers qualifying as index hedges for /hedges filter (PUT side only)
_INDEX_HEDGE_TICKERS: frozenset[str] = frozenset({
    "SPY", "QQQ", "IWM", "SPX", "NDX", "DIA",
    "SPXU", "SPXS", "SQQQ", "PSQ", "QID",
})


def _contract_score(e, direction: str) -> float:
    """
    Composite score for actionability. Higher = more tradeable.
    Criteria: premium, vol/oi, ATM-ness (delta 0.4-0.6), DTE 3-10, bias alignment.
    """
    _raw_delta = getattr(e, "delta", None)
    delta_abs  = abs(_raw_delta) if _raw_delta is not None else 0.0
    vol_oi     = getattr(e, "vol_oi_ratio", 0) or 0
    premium    = getattr(e, "premium_usd", 0) or 0
    dte        = getattr(e, "dte", 0) or 0
    side       = getattr(e, "side", "")

    score = 0.0

    # Premium (log scale so $22M doesn't totally dominate)
    import math
    if premium > 0:
        score += min(40, math.log10(max(1, premium)) * 6)

    # Vol/OI — aggressive flow signal
    score += min(20, vol_oi * 2)

    # ATM-ness — prefer delta 0.40–0.60; None delta gets a neutral partial credit
    if _raw_delta is None:
        score += 8   # reported flow without delta — assume tradeable, no bonus
    elif 0.40 <= delta_abs <= 0.60:
        score += 20
    elif 0.30 <= delta_abs < 0.40 or 0.60 < delta_abs <= 0.70:
        score += 8

    # DTE — prefer 3–10
    if 3 <= dte <= 10:
        score += 15
    elif 1 <= dte < 3:
        score += 5
    elif 11 <= dte <= 21:
        score += 8

    # Bias alignment
    aligned = (direction == "BULLISH" and side == "CALL") or \
              (direction == "BEARISH" and side == "PUT")
    contra  = (direction == "BULLISH" and side == "PUT") or \
              (direction == "BEARISH" and side == "CALL")
    if aligned:
        score += 15
    elif contra and (_raw_delta is None or delta_abs >= 0.35):
        score += 5   # meaningful hedge — keep but score lower

    return score


def _contract_description(e, direction: str) -> str:
    """Short plain-English description of why this contract is notable."""
    delta_abs = abs(getattr(e, "delta", 0) or 0)
    vol_oi    = getattr(e, "vol_oi_ratio", 0) or 0
    premium   = getattr(e, "premium_usd", 0) or 0
    side      = getattr(e, "side", "")
    ticker    = getattr(e, "ticker", "")

    parts = []

    if ticker in _MARKET_TICKERS:
        parts.append("index play")
    if premium >= 10_000_000:
        parts.append("institutional size")
    elif premium >= 1_000_000:
        parts.append("large premium")
    if vol_oi >= 10:
        parts.append("extreme vol/oi")
    elif vol_oi >= 5:
        parts.append("high vol/oi")

    aligned = (direction == "BULLISH" and side == "CALL") or \
              (direction == "BEARISH" and side == "PUT")
    if not aligned and delta_abs >= 0.35:
        parts.append("hedge")
    elif 0.40 <= delta_abs <= 0.60:
        parts.append("conviction delta")

    return ", ".join(parts) if parts else "strong flow"


def _top_actionable_contracts(entries, direction: str, top_n: int = 5) -> list:
    """
    Return top_n contracts ranked by actionability score.
    Filters to bias-aligned + meaningful hedges only.
    Deduplicates by (ticker, strike, side).
    """
    seen: set[tuple] = set()
    candidates = []

    for e in entries:
        key = (getattr(e, "ticker", ""), getattr(e, "strike", 0), getattr(e, "side", ""))
        if key in seen:
            continue
        seen.add(key)

        # Exclude contracts outside tradeable DTE range
        dte = getattr(e, "dte", 0) or 0
        if dte < 1 or dte > 21:
            continue

        # Exclude pure lottery / noise
        classification = getattr(e, "classification", "")
        if classification == "LOTTERY":
            continue

        score = _contract_score(e, direction)
        if score > 0:
            candidates.append((score, e))

    candidates.sort(key=lambda x: x[0], reverse=True)
    return [e for _, e in candidates[:top_n]]


def _fmt_actionable_section(entries, direction: str) -> str:
    """Build the TOP ACTIONABLE CONTRACTS section string."""
    contracts = _top_actionable_contracts(entries, direction)
    if not contracts:
        return ""

    lines = ["", "TOP ACTIONABLE CONTRACTS"]
    for e in contracts:
        delta_abs = abs(getattr(e, "delta", 0) or 0)
        delta_str = f"{getattr(e, 'delta', 0):+.2f}" if getattr(e, "delta", None) is not None else "N/A"
        dte       = getattr(e, "dte", 0)
        ticker    = getattr(e, "ticker", "")
        strike    = getattr(e, "strike", 0)
        side      = getattr(e, "side", "")
        desc      = _contract_description(e, direction)
        lines.append(
            f"• {ticker} {strike:.0f}{side[0]} "
            f"(Δ {delta_str}, DTE {dte}) — {desc}"
        )
    return "\n".join(lines)


def _tag_for(entry) -> str:
    """Derive a short label tag from classification and direction."""
    cls = entry.classification
    tag_map = {
        "HEDGE_DIRECTIONAL":      "HEDGE",
        "POSITIONAL_BULL":        "POSITIONAL",
        "POSITIONAL_BEAR":        "POSITIONAL",
        "SPECULATIVE_DIRECTIONAL":"SPEC",
        "CONTINUATION_STRONG":    "CONTINUATION",
        "CONTINUATION_WEAK":      "CONTINUATION",
        "GAMMA_VOL":              "GAMMA",
        "LOTTERY":                "LOTTERY",
    }
    return tag_map.get(cls, cls)


# ── Shared spread thresholds (one place — used by all regime/execution logic) ─

class _T:
    """
    Spread thresholds shared across regime tag, RTY/YM decisions,
    confidence scoring, and execution plan.  Edit here; all callers follow.
    """
    # QQQ vs IWM: when QQQ outpaces IWM by this much, the tape is tech-concentrated
    TECH_VS_SMALL_CONCENTRATION: float = 0.50
    # QQQ vs SPY: proxy for tech dominance vs broad market
    TECH_VS_BROAD_DOMINANCE:     float = 0.50
    # SPY vs IWM: small caps materially lagging broad market (defensive risk-off signal)
    BROAD_VS_SMALL_LAG:          float = 0.25
    # QQQ leading selloff harder than small caps (tech-led bear signal)
    TECH_LEADS_DOWN:             float = 0.40
    # Single-instrument VWAP proximity — "barely above/below" zone
    VWAP_PROXIMITY:              float = 0.10
    # QQQ/SPY divergence above which we penalise broad confidence
    CONFIDENCE_DIV_PENALTY:      float = 0.80


# ── Alignment helper ─────────────────────────────────────────────────────────

def _compute_alignment(direction: str, indices: Optional["IndexRS"]) -> str:
    """ALIGNED when both SPY and QQQ VWAP positions confirm the flow direction."""
    if not indices or not indices.data_ok:
        return "UNKNOWN"
    if direction == "BULLISH":
        if indices.spy_above_vwap is True and indices.qqq_above_vwap is True:
            return "ALIGNED"
    elif direction == "BEARISH":
        if indices.spy_above_vwap is False and indices.qqq_above_vwap is False:
            return "ALIGNED"
    return "NOT ALIGNED"


# ── Simplified regime label ───────────────────────────────────────────────────

def _simplified_regime(detailed_tag: str, hedging: bool) -> str:
    """
    Map the detailed regime tag to the simplified four-way label.
    TRENDING / ROTATIONAL / HEDGED / CHOP
    """
    if detailed_tag in (
        "BROAD TREND UP", "NARROW TECH-LED UP",
        "BROAD TREND DOWN", "NARROW TECH-LED DOWN",
        "DEFENSIVE RISK-OFF",
    ):
        return "HEDGED" if hedging else "TRENDING"
    if detailed_tag == "ROTATIONAL CHOP":
        return "ROTATIONAL"
    return "CHOP"   # MIXED / UNTRADEABLE, NO_DATA, CHOP


# ── Regime persistence tracker ───────────────────────────────────────────────

class RegimeTracker:
    """
    Prevents noisy regime flips by requiring _REQUIRED_CYCLES consecutive
    cycles to agree on a new tag before the displayed regime is updated.

    Thread-safe for a single asyncio event loop.
    """
    _REQUIRED_CYCLES: int = 2

    def __init__(self) -> None:
        self._displayed:        str = ""
        self._candidate:        str = ""
        self._candidate_count:  int = 0

    def update(self, raw: str) -> tuple[str, bool, str]:
        """
        Feed a freshly-derived raw regime.
        Returns (displayed_regime, changed: bool, old_regime).
        `changed` is True only when the stable displayed regime flips.
        """
        # Bootstrap on first call
        if not self._displayed:
            self._displayed       = raw
            self._candidate       = raw
            self._candidate_count = 1
            return self._displayed, False, ""

        # Raw matches current stable display → no change
        if raw == self._displayed:
            self._candidate       = raw
            self._candidate_count = 1
            return self._displayed, False, ""

        # Raw differs — accumulate candidate cycles
        if raw == self._candidate:
            self._candidate_count += 1
        else:
            self._candidate       = raw
            self._candidate_count = 1

        if self._candidate_count >= self._REQUIRED_CYCLES:
            old                   = self._displayed
            self._displayed       = raw
            self._candidate_count = 0
            return self._displayed, True, old

        # Not enough cycles yet — keep showing the stable regime
        return self._displayed, False, ""


# Module-level singleton — shared across all formatter calls in this process
_regime_tracker = RegimeTracker()


# ── Market Regime Tag ─────────────────────────────────────────────────────────

def _derive_regime_tag(
    direction: str,
    confidence: int,
    market_state: str,
    indices: Optional["IndexRS"],
) -> str:
    """
    Derive one of seven regime labels from flow + VWAP + breadth + concentration.

    ROTATIONAL CHOP vs MIXED / UNTRADEABLE distinction:
      ROTATIONAL CHOP   — indices are moving with visible rotation between
                          sectors or size-factors; there IS movement, but no
                          clean directional edge for index futures.
                          Requires: market_state == ROTATIONAL (SPY/QQQ on
                          opposite VWAP sides) AND confidence >= 25.
      MIXED/UNTRADEABLE — stand-down condition.  No data, CHOP state,
                          confidence < 20, or a directional flow that cannot
                          be confirmed by ANY index VWAP reading.

    Priority:
      1. No data / CHOP / confidence < 20         → MIXED / UNTRADEABLE
      2. market_state == ROTATIONAL, conf 20–24   → MIXED / UNTRADEABLE
         market_state == ROTATIONAL, conf >= 25   → ROTATIONAL CHOP
      3. NEUTRAL direction with some index spread  → ROTATIONAL CHOP
         NEUTRAL direction with no index spread    → MIXED / UNTRADEABLE
      4. BULLISH — check concentration / breadth
      5. BEARISH — check concentration / breadth
      6. Fallback                                  → MIXED / UNTRADEABLE
    """
    if not indices or not indices.data_ok:
        return "MIXED / UNTRADEABLE"
    if confidence < 20 or market_state in ("CHOP", "NO_DATA"):
        return "MIXED / UNTRADEABLE"

    spy_up  = indices.spy_above_vwap
    qqq_up  = indices.qqq_above_vwap
    iwm_up  = indices.iwm_above_vwap
    spy_pct = indices.spy_pct_vs_vwap or 0.0
    qqq_pct = indices.qqq_pct_vs_vwap or 0.0
    iwm_pct = indices.iwm_pct_vs_vwap or 0.0

    tech_vs_small = qqq_pct - iwm_pct
    tech_vs_broad = qqq_pct - spy_pct
    broad_vs_small = spy_pct - iwm_pct

    # ── ROTATIONAL state ──────────────────────────────────────────────────────
    if market_state == "ROTATIONAL":
        # True rotation requires visible index divergence (one up, one down)
        indices_diverge = (spy_up is True and qqq_up is False) or \
                          (spy_up is False and qqq_up is True)
        if indices_diverge and confidence >= 25:
            return "ROTATIONAL CHOP"
        return "MIXED / UNTRADEABLE"

    # ── NEUTRAL flow ──────────────────────────────────────────────────────────
    if direction == "NEUTRAL":
        # Rotational if indices are visibly split; otherwise no edge
        indices_diverge = (spy_up is True and qqq_up is False) or \
                          (spy_up is False and qqq_up is True)
        if indices_diverge and confidence >= 25:
            return "ROTATIONAL CHOP"
        return "MIXED / UNTRADEABLE"

    # ── BULLISH flow ──────────────────────────────────────────────────────────
    if direction == "BULLISH":
        if not (spy_up and qqq_up):
            # SPY or QQQ below VWAP despite bullish flow — no clean read
            return "MIXED / UNTRADEABLE"
        # Concentration check: tech outrunning small caps beyond threshold
        if tech_vs_small > _T.TECH_VS_SMALL_CONCENTRATION or iwm_up is False:
            return "NARROW TECH-LED UP"
        if iwm_up is True:
            return "BROAD TREND UP"
        # IWM data absent: fall back on tech-vs-broad spread
        if tech_vs_broad <= _T.TECH_VS_BROAD_DOMINANCE:
            return "BROAD TREND UP"
        return "NARROW TECH-LED UP"

    # ── BEARISH flow ──────────────────────────────────────────────────────────
    if direction == "BEARISH":
        if not (spy_up is False and qqq_up is False):
            # Only QQQ below while SPY still above: Nasdaq leading the selling
            if qqq_up is False and spy_up is True:
                return "NARROW TECH-LED DOWN"
            return "MIXED / UNTRADEABLE"

        # Both SPY and QQQ below VWAP — determine character of the selloff
        # NARROW TECH-LED DOWN: QQQ leading the decline significantly harder
        #   than small caps (Nasdaq is the epicentre of selling)
        if tech_vs_small < -_T.TECH_LEADS_DOWN:
            return "NARROW TECH-LED DOWN"

        # DEFENSIVE RISK-OFF: small caps underperforming broad market —
        #   investors shedding risk/cyclical exposure beyond just tech
        if iwm_up is False and broad_vs_small > _T.BROAD_VS_SMALL_LAG:
            return "DEFENSIVE RISK-OFF"

        # BROAD TREND DOWN: relatively even weakness across all indices
        if iwm_up is False:
            return "BROAD TREND DOWN"

        # IWM still above VWAP while SPY/QQQ sell — selective, not broad
        if confidence >= 25:
            return "ROTATIONAL CHOP"
        return "MIXED / UNTRADEABLE"

    return "MIXED / UNTRADEABLE"


def _fmt_regime_block(
    direction: str,
    confidence: int,
    market_state: str,
    indices: Optional["IndexRS"],
) -> str:
    """
    Returns the MARKET REGIME block, optionally preceded by a REGIME CHANGE
    notice when the displayed regime flips from the previous cycle.
    """
    raw_tag = _derive_regime_tag(direction, confidence, market_state, indices)
    displayed, changed, old = _regime_tracker.update(raw_tag)
    parts: list[str] = []
    if changed and old:
        parts.append(f"REGIME CHANGE:\n{old} → {displayed}")
        parts.append("")
    parts.append(f"MARKET REGIME:\n{displayed}")
    return "\n".join(parts)


# ── Per-instrument decision helpers ──────────────────────────────────────────

def _nq_decision(
    indices: "IndexRS", market_state: str, confidence: int
) -> tuple[str, Optional[str]]:
    """Returns (action, reason_or_None). action = LONG | SHORT | NO TRADE."""
    if indices is None or not indices.data_ok or indices.qqq_above_vwap is None:
        return "NO TRADE", "QQQ data unavailable"
    if market_state in ("CHOP", "NO_DATA"):
        return "NO TRADE", "CHOP — no directional edge in tech"
    if market_state == "ROTATIONAL":
        return "NO TRADE", "Rotation — QQQ leadership is unclear"

    qqq_pct = indices.qqq_pct_vs_vwap or 0.0

    if market_state == "TREND_UP":
        if not indices.qqq_above_vwap:
            return "NO TRADE", "QQQ below VWAP — tech not confirming upside"
        if abs(qqq_pct) < _T.VWAP_PROXIMITY and confidence < 50:
            return "NO TRADE", (
                f"QQQ barely above VWAP ({qqq_pct:+.2f}%) with low confidence"
                " — wait for clear hold"
            )
        return "LONG", None

    if market_state == "TREND_DOWN":
        if indices.qqq_above_vwap:
            return "NO TRADE", "QQQ still above VWAP — tech not confirming downside"
        return "SHORT", None

    return "NO TRADE", "Insufficient alignment"


def _es_decision(
    indices: "IndexRS", market_state: str, confidence: int
) -> tuple[str, Optional[str]]:
    if indices is None or not indices.data_ok or indices.spy_above_vwap is None:
        return "NO TRADE", "SPY data unavailable"
    if market_state in ("CHOP", "NO_DATA"):
        return "NO TRADE", "CHOP — conflicting breadth, no broad edge"
    if market_state == "ROTATIONAL":
        return "NO TRADE", "Rotation — broad sectors are not aligned"

    spy_pct = indices.spy_pct_vs_vwap or 0.0

    if market_state == "TREND_UP":
        if not indices.spy_above_vwap:
            return "NO TRADE", "SPY below VWAP — broad market not confirming"
        if abs(spy_pct) < _T.VWAP_PROXIMITY and confidence < 50:
            return "NO TRADE", (
                f"SPY near VWAP ({spy_pct:+.2f}%) with low confidence"
                " — wait for clear hold"
            )
        return "LONG", None

    if market_state == "TREND_DOWN":
        if indices.spy_above_vwap:
            return "NO TRADE", "SPY still above VWAP — downside not confirmed"
        return "SHORT", None

    return "NO TRADE", "Insufficient alignment"


def _rty_decision(
    indices: "IndexRS", market_state: str
) -> tuple[str, str]:
    """
    Stricter RTY logic.
    LONG  requires IWM + SPY both above VWAP and small caps not lagging.
    SHORT requires IWM + SPY both below VWAP.
    NO TRADE when tech is carrying the tape or breadth is insufficient.
    """
    if indices is None or not indices.data_ok:
        return "NO TRADE", "IWM data unavailable"
    if market_state in ("CHOP", "NO_DATA"):
        return "NO TRADE", "CHOP — no broad participation edge"
    if market_state == "ROTATIONAL":
        return "NO TRADE", "Rotation — breadth is unclear, IWM confirmation needed"

    iwm_up  = indices.iwm_above_vwap
    spy_up  = indices.spy_above_vwap
    qqq_pct = indices.qqq_pct_vs_vwap or 0.0
    spy_pct = indices.spy_pct_vs_vwap or 0.0
    iwm_pct = indices.iwm_pct_vs_vwap or 0.0

    tech_vs_small  = qqq_pct - iwm_pct   # +ve = tech leading small caps
    broad_vs_small = spy_pct - iwm_pct   # +ve = small caps lagging broad market

    if market_state == "TREND_UP":
        if iwm_up is not True:
            return "NO TRADE", "IWM below VWAP — small caps not confirming upside"
        if spy_up is not True:
            return "NO TRADE", "SPY not above VWAP — broad market not confirmed for RTY"
        if tech_vs_small > _T.TECH_VS_SMALL_CONCENTRATION:
            return "NO TRADE", (
                f"Tech concentrated — QQQ {qqq_pct:+.2f}% vs VWAP, "
                f"IWM only {iwm_pct:+.2f}% — small caps not participating"
            )
        if broad_vs_small > _T.BROAD_VS_SMALL_LAG:
            return "NO TRADE", (
                f"Small caps lagging SPY by {broad_vs_small:.2f}%"
                " — breadth not broad enough for RTY long"
            )
        return "LONG", ""

    if market_state == "TREND_DOWN":
        if iwm_up is not False:
            return "NO TRADE", "IWM still above VWAP — small caps not confirming downside"
        if spy_up is not False:
            return "NO TRADE", "SPY still above VWAP — wait for broad market confirmation"
        return "SHORT", ""

    return "NO TRADE", "Insufficient confirmation"


def _ym_decision(
    indices: "IndexRS", market_state: str
) -> tuple[str, str]:
    """
    Strict YM logic — does NOT automatically inherit ES direction.
    YM requires broad non-tech participation, not just SPY being up.
    YM defaults to NO TRADE when tech is driving the tape.
    """
    if indices is None or not indices.data_ok:
        return "NO TRADE", "No market data for Dow proxy"
    if market_state in ("CHOP", "NO_DATA"):
        return "NO TRADE", "CHOP — no broad non-tech participation edge"
    if market_state == "ROTATIONAL":
        return "NO TRADE", "Rotation — no clear cyclical/value direction for Dow"

    spy_up  = indices.spy_above_vwap
    qqq_pct = indices.qqq_pct_vs_vwap or 0.0
    spy_pct = indices.spy_pct_vs_vwap or 0.0

    tech_dominance = qqq_pct - spy_pct   # QQQ running harder than SPY

    if market_state == "TREND_UP":
        if spy_up is not True:
            return "NO TRADE", "SPY not above VWAP — Dow proxy not confirming"
        if tech_dominance > _T.TECH_VS_BROAD_DOMINANCE:
            return "NO TRADE", (
                f"Tape driven by tech — QQQ {qqq_pct:+.2f}% vs SPY {spy_pct:+.2f}%"
                " vs VWAP. No broad Dow/value participation confirmed"
            )
        return "LONG", ""

    if market_state == "TREND_DOWN":
        if spy_up is not False:
            return "NO TRADE", "SPY still above VWAP — no clear Dow downside signal"
        if tech_dominance < -_T.TECH_VS_BROAD_DOMINANCE:
            return "NO TRADE", (
                "Nasdaq leading lower harder than the broad tape —"
                " Dow (value/cyclicals) may not follow at the same pace"
            )
        return "SHORT", ""

    return "NO TRADE", "No clear Dow-style participation signal"


# ── Rule-based confidence scoring ─────────────────────────────────────────────

def _compute_structured_confidence(
    flow_direction: str,
    bull_pct: int,
    bear_pct: int,
    rs_data: Optional["MarketRS"],
    all_entries: list,
) -> int:
    """
    Rule-based confidence score.

    Components:
      +20  clear broad flow direction (≥70% one side)
      +15  one side dominant (60–69%)
      +8   slight lean (55–59%)
      +20  SPY + QQQ VWAP both align with flow
      +10  one index VWAP confirms flow
      +15  IWM confirms (breadth)
      +15  multiple flow-ticker RS readings confirm direction
      +8   single ticker RS confirms
      +10  no conflicting hedge flow
      -10  multiple hedge entries conflict with direction
      -10  mixed ticker RS (leaders and laggards in same session)
      -10  QQQ/SPY diverge > _T.CONFIDENCE_DIV_PENALTY (concentrated tape)
    Clamp: 0–100
    """
    score = 0
    dominant_pct = max(bull_pct, bear_pct)

    if dominant_pct >= 70:
        score += 20
    elif dominant_pct >= 60:
        score += 15
    elif dominant_pct >= 55:
        score += 8

    if rs_data and rs_data.indices.data_ok:
        idx = rs_data.indices

        if flow_direction == "BULLISH":
            spy_ok = idx.spy_above_vwap is True
            qqq_ok = idx.qqq_above_vwap is True
        elif flow_direction == "BEARISH":
            spy_ok = idx.spy_above_vwap is False
            qqq_ok = idx.qqq_above_vwap is False
        else:
            spy_ok = qqq_ok = False

        if spy_ok and qqq_ok:
            score += 20
        elif spy_ok or qqq_ok:
            score += 10
        elif idx.spy_above_vwap is not None and idx.qqq_above_vwap is not None:
            score -= 10  # both indices explicitly conflict

        # IWM breadth
        if flow_direction == "BULLISH" and idx.iwm_above_vwap is True:
            score += 15
        elif flow_direction == "BEARISH" and idx.iwm_above_vwap is False:
            score += 15
        elif idx.iwm_above_vwap is not None:
            score -= 10  # IWM explicitly conflicts

        # Ticker RS confirmation
        if rs_data.tickers:
            if flow_direction == "BULLISH":
                confirming  = sum(1 for t in rs_data.tickers.values() if t.classification == "STRONG")
                conflicting = sum(1 for t in rs_data.tickers.values() if t.classification == "WEAK")
            elif flow_direction == "BEARISH":
                confirming  = sum(1 for t in rs_data.tickers.values() if t.classification == "WEAK")
                conflicting = sum(1 for t in rs_data.tickers.values() if t.classification == "STRONG")
            else:
                confirming = conflicting = 0

            if confirming >= 2:
                score += 15
            elif confirming == 1:
                score += 8
            if conflicting >= 2:
                score -= 10  # key leaders explicitly disagree

        # Hedge flow conflicts
        hedge_count = sum(
            1 for e in all_entries if getattr(e, "classification", "") == "HEDGE_DIRECTIONAL"
        )
        if hedge_count == 0:
            score += 10
        elif hedge_count >= 2:
            score -= 10

        # QQQ/SPY divergence penalty — concentrated tape reduces broad confidence
        spy_pct_val = idx.spy_pct_vs_vwap or 0.0
        qqq_pct_val = idx.qqq_pct_vs_vwap or 0.0
        if abs(qqq_pct_val - spy_pct_val) > _T.CONFIDENCE_DIV_PENALTY:
            score -= 10

    return max(0, min(100, score))


# ── Driver list ───────────────────────────────────────────────────────────────

def _fmt_driver_list(
    entries_bull: list,
    entries_bear: list,
    rs_data: Optional["MarketRS"],
) -> str:
    """
    Build DRIVERS block.

    Driving higher: bullish-flow tickers, promoted if their RS is STRONG.
    Dragging lower: bearish-flow tickers, promoted if their RS is WEAK.
    RS-only tickers (no matching flow entry) are appended when relevant.
    """
    def _tickers(entries: list) -> list[str]:
        seen: list[str] = []
        for e in entries:
            t = getattr(e, "ticker", "")
            if t and t not in seen:
                seen.append(t)
        return seen[:4]

    up_tickers   = _tickers(entries_bull)
    down_tickers = _tickers(entries_bear)

    # Augment from RS data (STRONG → driving higher, WEAK → dragging lower)
    if rs_data and rs_data.tickers:
        for t, rs in rs_data.tickers.items():
            if rs.classification == "STRONG" and t not in up_tickers:
                up_tickers.append(t)
            elif rs.classification == "WEAK" and t not in down_tickers:
                down_tickers.append(t)
        up_tickers   = up_tickers[:4]
        down_tickers = down_tickers[:4]

    if not up_tickers and not down_tickers:
        return ""

    lines = ["", "DRIVERS:"]
    lines.append(f"- Driving higher: {', '.join(up_tickers)  if up_tickers   else 'None'}")
    lines.append(f"- Dragging lower: {', '.join(down_tickers) if down_tickers else 'None'}")
    return "\n".join(lines)


# ── Futures conviction rank ────────────────────────────────────────────────────

def _fmt_conviction_rank(
    market_state: str,
    indices: Optional["IndexRS"],
    direction: str,
    confidence: int,
    rs_data: Optional["MarketRS"],
) -> str:
    """
    Rank tradeable futures instruments by quality of alignment between
    flow, VWAP, breadth, and RS.  Output after EXECUTION PLAN.

    Scoring (each instrument independently):
      up to 30 pts  — VWAP distance of reference index
      20 pts        — both SPY + QQQ aligned
      15 pts        — IWM breadth confirms direction
      up to 20 pts  — flow confidence contribution
      up to 15 pts  — RS ticker confirmations
      −5  pts       — RTY / YM higher-bar penalty
    Only instruments with a LONG or SHORT decision are eligible.
    """
    if not indices or not indices.data_ok:
        return ""

    decisions = {
        "NQ":  _nq_decision(indices, market_state, confidence)[0],
        "ES":  _es_decision(indices, market_state, confidence)[0],
        "RTY": _rty_decision(indices, market_state)[0],
        "YM":  _ym_decision(indices, market_state)[0],
    }

    def _score(inst: str, action: str) -> float:
        if action == "NO TRADE":
            return -1.0
        s = 0.0

        # VWAP distance of the primary reference index
        ref_pct = {
            "NQ":  abs(indices.qqq_pct_vs_vwap or 0.0),
            "ES":  abs(indices.spy_pct_vs_vwap  or 0.0),
            "RTY": abs(indices.iwm_pct_vs_vwap  or 0.0),
            "YM":  abs(indices.spy_pct_vs_vwap  or 0.0),
        }.get(inst, 0.0)
        s += min(30.0, ref_pct * 20.0)

        # Both major indices aligned
        if indices.spy_above_vwap is not None and indices.qqq_above_vwap is not None:
            if indices.spy_above_vwap == indices.qqq_above_vwap:
                s += 20.0

        # IWM breadth confirmation
        iwm = indices.iwm_above_vwap
        if iwm is not None:
            confirms = (action == "LONG" and iwm) or (action == "SHORT" and not iwm)
            if confirms:
                s += 15.0

        # Flow confidence contribution (linear, max 20 pts)
        s += confidence * 0.20

        # RS ticker confirmation
        if rs_data and rs_data.tickers:
            aligned = sum(
                1 for t in rs_data.tickers.values()
                if (action == "LONG"  and t.classification == "STRONG") or
                   (action == "SHORT" and t.classification == "WEAK")
            )
            s += min(15.0, aligned * 7.0)

        # Higher-bar penalty for RTY / YM (require additional confirmation)
        if inst in ("RTY", "YM"):
            s -= 5.0

        return s

    ranked = sorted(
        [(inst, act, _score(inst, act)) for inst, act in decisions.items()],
        key=lambda x: x[2], reverse=True,
    )
    eligible = [(inst, act) for inst, act, sc in ranked if sc >= 0]

    primary   = eligible[0][0] if len(eligible) >= 1 else "NONE"
    secondary = eligible[1][0] if len(eligible) >= 2 else "NONE"

    return f"\nBEST EXPRESSION:\n- Primary: {primary}\n- Secondary: {secondary}"


# ── Execution plan ─────────────────────────────────────────────────────────────

def _fmt_execution_plan(
    market_state: str,
    indices: "IndexRS",
    direction: str = "NEUTRAL",
    confidence: int = 0,
) -> str:
    """
    Build EXECUTION PLAN block for NQ, ES, RTY, YM.
    Each instrument gets its own independent decision. Returns "" if no data.
    """
    if indices is None or not indices.data_ok:
        return ""

    def _p(v: Optional[float]) -> str:
        return f"${v:.2f}" if v is not None else "N/A"

    def _render(future: str, action: str, reason: Optional[str],
                trigger: str, stop: str) -> list[str]:
        if action == "NO TRADE":
            return [f"{future}: NO TRADE — {reason}"]
        rows = [f"{future}: {action}", f"  Trigger: {trigger}", f"  Stop: {stop}"]
        return rows

    lines = ["", "EXECUTION PLAN"]

    # NQ
    nq_act, nq_why = _nq_decision(indices, market_state, confidence)
    if nq_act == "LONG":
        lines += _render("NQ", "LONG", None,
                         f"QQQ holds above {_p(indices.qqq_vwap)} VWAP",
                         f"QQQ loses {_p(indices.qqq_pm_low)} (PM low)")
    elif nq_act == "SHORT":
        lines += _render("NQ", "SHORT", None,
                         f"QQQ fails to reclaim {_p(indices.qqq_vwap)} VWAP",
                         f"QQQ reclaims {_p(indices.qqq_vwap)}")
    else:
        lines.append(f"NQ: NO TRADE — {nq_why}")

    # ES
    es_act, es_why = _es_decision(indices, market_state, confidence)
    if es_act == "LONG":
        lines += _render("ES", "LONG", None,
                         f"SPY holds above {_p(indices.spy_vwap)} VWAP",
                         f"SPY loses {_p(indices.spy_pm_low)} (PM low)")
    elif es_act == "SHORT":
        lines += _render("ES", "SHORT", None,
                         f"SPY fails to reclaim {_p(indices.spy_vwap)} VWAP",
                         f"SPY reclaims {_p(indices.spy_vwap)}")
    else:
        lines.append(f"ES: NO TRADE — {es_why}")

    # RTY
    rty_act, rty_why = _rty_decision(indices, market_state)
    if rty_act == "LONG":
        lines += _render("RTY", "LONG", None,
                         f"IWM holds above {_p(indices.iwm_vwap)} VWAP with SPY breadth confirming",
                         f"IWM loses {_p(indices.iwm_pm_low)} (PM low)")
    elif rty_act == "SHORT":
        lines += _render("RTY", "SHORT", None,
                         f"IWM fails to reclaim {_p(indices.iwm_vwap)} VWAP",
                         f"IWM reclaims {_p(indices.iwm_vwap)}")
    else:
        lines.append(f"RTY: NO TRADE — {rty_why}")

    # YM
    ym_act, ym_why = _ym_decision(indices, market_state)
    if ym_act == "LONG":
        lines += _render("YM", "LONG", None,
                         f"SPY VWAP hold confirms broad non-tech participation",
                         f"SPY loses {_p(indices.spy_pm_low)}")
    elif ym_act == "SHORT":
        lines += _render("YM", "SHORT", None,
                         f"SPY fails {_p(indices.spy_vwap)} with broad cyclical weakness",
                         f"SPY reclaims {_p(indices.spy_vwap)}")
    else:
        lines.append(f"YM: NO TRADE — {ym_why}")

    return "\n".join(lines)


# ── Final Verdict ─────────────────────────────────────────────────────────────

def _fmt_final_verdict(
    market_state: str,
    direction: str,
    bias_confidence: int,
    execution_confidence: int,
    simplified_regime: str,
    alignment: str,
    rs_data: Optional["MarketRS"],
    bull_pct: int,
    bear_pct: int,
    entries_all: list,
    leaders: list,
    drags: list,
) -> str:
    """
    Build FINAL VERDICT block.
    Uses pre-computed bias_confidence, execution_confidence, simplified_regime,
    and alignment — never recomputes them independently.
    """
    state_label = {
        "TREND_UP":   "TREND UP",
        "TREND_DOWN": "TREND DOWN",
        "ROTATIONAL": "ROTATIONAL",
        "CHOP":       "CHOP",
        "NO_DATA":    "UNKNOWN",
    }.get(market_state, market_state)

    # Execution confidence band
    if execution_confidence <= 20:
        exec_band = "no edge"
    elif execution_confidence <= 40:
        exec_band = "weak"
    elif execution_confidence <= 60:
        exec_band = "moderate"
    elif execution_confidence <= 80:
        exec_band = "strong"
    else:
        exec_band = "high"

    # ── Tradability verdict ───────────────────────────────────────────────────
    if execution_confidence < 25:
        tradable     = "NO TRADE"
        trade_reason = f"Execution confidence too low ({execution_confidence}/100) — wait for price/flow alignment"
    elif simplified_regime == "CHOP":
        tradable     = "NO TRADE"
        trade_reason = "No edge — wait for SPY, QQQ, IWM to align above or below VWAP"
    elif simplified_regime == "HEDGED":
        tradable     = "HEDGE / WAIT"
        trade_reason = "Heavy hedge flow suppressing directional conviction — no clean index futures trade"
    elif simplified_regime == "ROTATIONAL":
        tradable     = "PAIR TRADE ONLY"
        trade_reason = "Rotation active — relative strength plays only, avoid outright index futures"
    elif alignment == "NOT ALIGNED":
        tradable     = "WAIT — MISALIGNED"
        trade_reason = f"{direction} flow but price not confirming — wait for VWAP alignment"
    else:
        tradable     = "YES"
        trade_reason = f"{direction} flow + price aligned | regime: {simplified_regime}"

    lines = ["", "FINAL VERDICT"]
    lines.append(
        f"Regime: {simplified_regime} | Bias Conf: {bias_confidence}/100 | "
        f"Exec Conf: {execution_confidence}/100 ({exec_band}) | Alignment: {alignment}"
    )
    lines.append(f"TRADABLE: {tradable}")
    lines.append(f"WHY: {trade_reason}")

    # ── Supporting evidence ───────────────────────────────────────────────────
    lines.append("EVIDENCE:")

    if direction == "NEUTRAL":
        lines.append("- Flow is split — no dominant directional bias")
    else:
        dom_pct = bull_pct if direction == "BULLISH" else bear_pct
        lines.append(f"- {direction} spec flow dominant ({dom_pct}%)")

    if rs_data and rs_data.indices.data_ok:
        idx = rs_data.indices

        def _vwap_state(name: str, above: Optional[bool], pct: Optional[float]) -> str:
            if above is None:
                return f"{name}: no data"
            pos  = "above" if above else "below"
            pstr = f" ({pct:+.2f}%)" if pct is not None else ""
            return f"{name} is {pos} VWAP{pstr}"

        lines.append(f"- {_vwap_state('SPY', idx.spy_above_vwap, idx.spy_pct_vs_vwap)}")
        lines.append(f"- {_vwap_state('QQQ', idx.qqq_above_vwap, idx.qqq_pct_vs_vwap)}")
        lines.append(f"- {_vwap_state('IWM', idx.iwm_above_vwap, idx.iwm_pct_vs_vwap)}")

        spy_pct_val   = idx.spy_pct_vs_vwap or 0.0
        qqq_pct_val   = idx.qqq_pct_vs_vwap or 0.0
        iwm_pct_val   = idx.iwm_pct_vs_vwap or 0.0
        tech_vs_broad = qqq_pct_val - spy_pct_val
        tech_vs_small = qqq_pct_val - iwm_pct_val

        if tech_vs_small > 0.60:
            lines.append(f"- Tape concentrated — tech outpacing small caps by {tech_vs_small:.2f}%")
        elif tech_vs_broad > 0.50:
            lines.append(f"- Move tilted toward tech — QQQ leading SPY by {tech_vs_broad:.2f}%")
        elif abs(tech_vs_small) < 0.20 and abs(tech_vs_broad) < 0.20:
            lines.append("- Broad participation — tech, large-cap, and small caps moving together")
        else:
            lines.append("- Mixed participation across indices")

    # Use the pre-computed leaders/drags — same values as shown in report header
    if leaders:
        lines.append(f"- Leaders: {', '.join(leaders)}")
    if drags:
        lines.append(f"- Drags: {', '.join(drags)}")

    # ── Compact futures summary ───────────────────────────────────────────────
    if rs_data and rs_data.indices.data_ok:
        idx = rs_data.indices

        def _vp(v: Optional[float]) -> str:
            return f"${v:.2f}" if v is not None else "?"

        nq_act,  _ = _nq_decision(idx, market_state, execution_confidence)
        es_act,  _ = _es_decision(idx, market_state, execution_confidence)
        rty_act, _ = _rty_decision(idx, market_state)
        ym_act,  _ = _ym_decision(idx, market_state)

        def _compact_line(future: str, action: str, ref: str, vwap: Optional[float]) -> str:
            if action == "LONG":
                return f"{future} → LONG | {ref} above {_vp(vwap)}"
            if action == "SHORT":
                return f"{future} → SHORT | {ref} below {_vp(vwap)}"
            return f"{future} → NO TRADE"

        lines.append(_compact_line("NQ",  nq_act,  "QQQ", idx.qqq_vwap))
        lines.append(_compact_line("ES",  es_act,  "SPY", idx.spy_vwap))
        lines.append(_compact_line("RTY", rty_act, "IWM", idx.iwm_vwap))
        lines.append(_compact_line("YM",  ym_act,  "SPY", idx.spy_vwap))

    # ── DO NOT ────────────────────────────────────────────────────────────────
    if simplified_regime == "TRENDING":
        if direction == "BULLISH":
            do_not = "Short into VWAP strength. Chase breakouts without a pullback entry."
        else:
            do_not = "Buy weakness without a VWAP reclaim. Catch falling knives."
    elif simplified_regime == "ROTATIONAL":
        do_not = "Take outright directional futures positions. Use sector pairs only."
    elif simplified_regime == "HEDGED":
        do_not = "Trade directionally into heavy hedge flow — wait for hedge unwind confirmation."
    else:
        do_not = "Force trades. No edge — wait for VWAP alignment across SPY, QQQ, and IWM."
    lines.append(f"DO NOT: {do_not}")

    # ── KEY READ ──────────────────────────────────────────────────────────────
    if rs_data and rs_data.indices.data_ok:
        idx = rs_data.indices
        spy_pct_val   = idx.spy_pct_vs_vwap or 0.0
        qqq_pct_val   = idx.qqq_pct_vs_vwap or 0.0
        tech_vs_small = qqq_pct_val - (idx.iwm_pct_vs_vwap or 0.0)

        if simplified_regime == "TRENDING":
            if direction == "BULLISH":
                if tech_vs_small > 0.60:
                    key_read = (
                        "Tech may bounce, but broad participation is weak — "
                        "favor NQ longs on VWAP hold, avoid forcing RTY or YM."
                    )
                elif drags:
                    key_read = (
                        f"Bias long NQ and ES while QQQ and SPY hold VWAP. "
                        f"Watch {drags[0]} for rotation or reversal signal."
                    )
                else:
                    key_read = (
                        "Broad market aligned — bias long NQ and ES. "
                        "RTY and YM add if IWM and non-tech breadth hold."
                    )
            else:
                if tech_vs_small < -0.30:
                    key_read = (
                        "Tech leading lower — NQ short is the primary trade. "
                        "Confirm RTY and YM independently before adding."
                    )
                else:
                    key_read = (
                        "Broad selling pressure is active — fade bounces on NQ and ES. "
                        "RTY short adds only if IWM confirms VWAP rejection."
                    )
        elif simplified_regime == "ROTATIONAL":
            key_read = (
                "Sector rotation active — avoid index futures, "
                "target relative strength plays. "
                + (f"Leaders: {', '.join(leaders)}. " if leaders else "")
                + (f"Fading: {', '.join(drags)}." if drags else "")
            )
        elif simplified_regime == "HEDGED":
            key_read = (
                "Hedge flow suppressing directional conviction — "
                "wait for hedge unwind or price confirmation before committing."
            )
        else:
            key_read = (
                "No clear edge — reduce size. "
                "Wait for SPY, QQQ, and IWM to align above or below VWAP before committing."
            )
    else:
        if simplified_regime == "TRENDING":
            if direction == "BULLISH":
                key_read = "Bias long NQ until QQQ loses VWAP. Confirm breadth with IWM before adding RTY/YM."
            else:
                key_read = "Selling pressure dominant — fade bounces, protect gains."
        elif simplified_regime == "ROTATIONAL":
            key_read = "Sector rotation active — target relative strength, avoid index plays."
        elif simplified_regime == "HEDGED":
            key_read = "Hedge flow present — wait for unwind or price confirmation."
        else:
            key_read = "No clear edge — reduce size, wait for index VWAP alignment."

    lines.append(f"KEY READ: {key_read}")

    return "\n".join(lines)


# ── BOT_DATA block assembly helper ───────────────────────────────────────────

def _get_primary_secondary_futures(
    market_state: str,
    indices: Optional["IndexRS"],
    direction: str,
    confidence: int,
    rs_data: Optional["MarketRS"],
) -> tuple[str, str]:
    """
    Return (primary, secondary) futures instrument names.
    Reuses the same scoring logic as _fmt_conviction_rank but returns
    the names directly instead of a formatted string.
    """
    if not indices or not indices.data_ok:
        return "NONE", "NONE"

    decisions = {
        "NQ":  _nq_decision(indices, market_state, confidence)[0],
        "ES":  _es_decision(indices, market_state, confidence)[0],
        "RTY": _rty_decision(indices, market_state)[0],
        "YM":  _ym_decision(indices, market_state)[0],
    }

    def _score(inst: str, action: str) -> float:
        if action == "NO TRADE":
            return -1.0
        import math as _math
        s = 0.0
        ref_pct = {
            "NQ":  abs(indices.qqq_pct_vs_vwap or 0.0),
            "ES":  abs(indices.spy_pct_vs_vwap  or 0.0),
            "RTY": abs(indices.iwm_pct_vs_vwap  or 0.0),
            "YM":  abs(indices.spy_pct_vs_vwap  or 0.0),
        }.get(inst, 0.0)
        s += min(30.0, ref_pct * 20.0)
        if indices.spy_above_vwap is not None and indices.qqq_above_vwap is not None:
            if indices.spy_above_vwap == indices.qqq_above_vwap:
                s += 20.0
        iwm = indices.iwm_above_vwap
        if iwm is not None:
            confirms = (action == "LONG" and iwm) or (action == "SHORT" and not iwm)
            if confirms:
                s += 15.0
        s += confidence * 0.20
        if rs_data and rs_data.tickers:
            aligned = sum(
                1 for t in rs_data.tickers.values()
                if (action == "LONG"  and t.classification == "STRONG") or
                   (action == "SHORT" and t.classification == "WEAK")
            )
            s += min(15.0, aligned * 7.0)
        if inst in ("RTY", "YM"):
            s -= 5.0
        return s

    ranked = sorted(
        [(inst, act, _score(inst, act)) for inst, act in decisions.items()],
        key=lambda x: x[2], reverse=True,
    )
    eligible = [inst for inst, act, sc in ranked if sc >= 0]
    primary   = eligible[0] if len(eligible) >= 1 else "NONE"
    secondary = eligible[1] if len(eligible) >= 2 else "NONE"
    return primary, secondary


def _append_bot_data_block(
    human_text: str,
    *,
    direction: str,
    hedging: bool,
    macro_override: bool,
    bias_confidence: int,
    execution_confidence: int,
    alignment: str,
    simplified_regime: str,
    primary: str,
    secondary: str,
    leaders: list,
    drags: list,
    session: str,
    data_quality: str,
    rs_data: Optional["MarketRS"] = None,
) -> str:
    """
    Build and append the [BOT_DATA]...[/BOT_DATA] block to a human-readable
    report string.  Never raises — returns original text on any error.
    """
    try:
        indices = rs_data.indices if (rs_data and rs_data.data_ok) else None

        block = build_bot_data(
            bias=direction,
            hedging=hedging,
            bias_confidence=bias_confidence,
            execution_confidence=execution_confidence,
            alignment=alignment,
            regime_raw=simplified_regime,
            primary_futures=primary,
            secondary_futures=secondary,
            leaders=list(leaders),
            drags=list(drags),
            session=session,
            data_quality=data_quality,
            macro_override=macro_override,
            qqq_vwap=getattr(indices, "qqq_vwap", None) if indices else None,
            qqq_price=getattr(indices, "qqq_price", None) if indices else None,
            qqq_pm_high=None,
            qqq_pm_low=getattr(indices, "qqq_pm_low", None) if indices else None,
            spy_vwap=getattr(indices, "spy_vwap", None) if indices else None,
            spy_price=getattr(indices, "spy_price", None) if indices else None,
            spy_pm_high=None,
            spy_pm_low=getattr(indices, "spy_pm_low", None) if indices else None,
        )
        return human_text + "\n\n" + render_bot_data(block)
    except Exception:
        return human_text


# ── Channel B: new structured report ─────────────────────────────────────────

def format_channel_b_report(analysis: dict, rs_data: Optional["MarketRS"] = None) -> str:
    """
    Build the Channel B structured output.

    All confidence, regime, alignment, leaders, and drags values are computed
    ONCE here and threaded to every section including BOT_DATA — no section
    recomputes them independently.
    """
    if not analysis:
        return ""

    direction       = analysis["direction"]
    subtype         = analysis["subtype"]
    bull_pct        = analysis["bull_pct"]
    bear_pct        = analysis["bear_pct"]
    bias_confidence = analysis["bias_confidence"]
    state           = analysis["state"]
    mode            = analysis["mode"]
    entries         = analysis.get("entries", [])
    hedging         = analysis.get("hedging", False)
    # Single source of truth for leaders/drags/mixed
    leaders         = analysis.get("leaders", [])
    drags           = analysis.get("drags", [])
    mixed           = analysis.get("mixed", [])
    hedge_entries   = analysis.get("hedge_entries", [])
    spec_entries    = analysis.get("spec_entries", entries)
    spec_bull_pct   = analysis.get("spec_bull_pct", bull_pct)
    spec_bear_pct   = analysis.get("spec_bear_pct", bear_pct)

    # ── Compute execution_confidence ONCE ────────────────────────────────────
    _LOW_CONFIDENCE = 20
    effective_direction = "NEUTRAL" if bias_confidence < _LOW_CONFIDENCE else direction
    execution_confidence = (
        _compute_structured_confidence(effective_direction, bull_pct, bear_pct, rs_data, entries)
        if rs_data and rs_data.data_ok
        else bias_confidence
    )

    # ── Regime + alignment computed ONCE ─────────────────────────────────────
    ms             = rs_data.market_state if (rs_data and rs_data.data_ok) else "NO_DATA"
    regime_indices = rs_data.indices      if (rs_data and rs_data.data_ok) else None
    detailed_tag   = _derive_regime_tag(effective_direction, execution_confidence, ms, regime_indices)
    simp_regime    = _simplified_regime(detailed_tag, hedging)
    alignment      = _compute_alignment(effective_direction, regime_indices)

    # ── Bias header ──────────────────────────────────────────────────────────
    if effective_direction == "BULLISH":
        bias_emoji = "🟢"
    elif effective_direction == "BEARISH":
        bias_emoji = "🔴"
    else:
        bias_emoji = "⚪"

    conf_note = " ⚠️ LOW CONFIDENCE" if bias_confidence < _LOW_CONFIDENCE else ""
    lines = [
        f"{bias_emoji} MARKET BIAS: {effective_direction}{conf_note}",
        f"BIAS CONFIDENCE: {bias_confidence}/100 | EXECUTION CONFIDENCE: {execution_confidence}/100",
        f"REGIME: {simp_regime} | ALIGNMENT: {alignment}",
        "",
    ]

    # ── Leaders / Drags / Mixed ───────────────────────────────────────────────
    lines.append(f"LEADERS: {', '.join(leaders) if leaders else 'None'}")
    lines.append(f"DRAGS:   {', '.join(drags)   if drags   else 'None'}")
    if mixed:
        lines.append(f"MIXED:   {', '.join(mixed)} (flow on both sides)")
    lines.append("")

    # ── Flow Summary ─────────────────────────────────────────────────────────
    spec_count  = len(spec_entries)
    hedge_count = len(hedge_entries)
    lines.append("FLOW SUMMARY:")
    lines.append(
        f"- SPEC FLOW:  {spec_count} signal(s) | "
        f"{spec_bull_pct}% call / {spec_bear_pct}% put"
    )
    if hedge_count:
        hedge_tickers = list(dict.fromkeys(e.ticker for e in hedge_entries))
        lines.append(
            f"- HEDGE FLOW: {hedge_count} signal(s) — "
            f"{', '.join(hedge_tickers[:4])} "
            f"(suppresses execution confidence)"
        )
    else:
        lines.append("- HEDGE FLOW: none detected")
    lines.append("")

    # ── Price Context ─────────────────────────────────────────────────────────
    if regime_indices and regime_indices.data_ok:
        idx = regime_indices
        def _vwap_line(name, price, vwap, above, pct):
            if price is None:
                return f"- {name}: no data"
            pos  = "ABOVE" if above else "BELOW"
            pstr = f" ({pct:+.2f}%)" if pct is not None else ""
            return f"- {name}: ${price:.2f} — {pos} VWAP (${vwap:.2f}){pstr}" if vwap else f"- {name}: ${price:.2f} — VWAP N/A"
        lines.append("PRICE CONTEXT:")
        lines.append(_vwap_line("SPY", idx.spy_price, idx.spy_vwap, idx.spy_above_vwap, idx.spy_pct_vs_vwap))
        lines.append(_vwap_line("QQQ", idx.qqq_price, idx.qqq_vwap, idx.qqq_above_vwap, idx.qqq_pct_vs_vwap))
        lines.append(_vwap_line("IWM", idx.iwm_price, idx.iwm_vwap, idx.iwm_above_vwap, idx.iwm_pct_vs_vwap))
        lines.append("")

    # ── Top Overall Flow ──────────────────────────────────────────────────────
    actionable = [e for e in entries if e.decision != "KILL"]
    top_all    = sorted(actionable, key=lambda e: (e.priority, -e.score))[:5]

    if top_all:
        lines.append("TOP FLOW")
        for i, e in enumerate(top_all, 1):
            tag       = _tag_for(e)
            delta_str = f"{e.delta:+.2f}" if e.delta else "N/A"
            lines.append(
                f"{i}. {e.ticker} ${e.strike:.0f}{e.side[0]} "
                f"| {_fmt_p(e.premium_usd)} IV:{e.iv_pct:.1f}% "
                f"| Vol/OI {e.vol_oi_ratio:.1f}x "
                f"| Δ {delta_str} | DTE {e.dte} | {tag}"
            )
        lines.append("")

    # ── Top Bulls (SPEC CALL flow only — label as leaders if applicable) ──────
    bulls = sorted([e for e in actionable if e.side == "CALL"], key=lambda e: (e.priority, -e.score))[:3]
    if bulls:
        lines.append("TOP BULLS")
        for e in bulls:
            delta_str  = f"{e.delta:+.2f}" if e.delta else "N/A"
            leader_tag = " ★" if e.ticker in leaders else ""
            lines.append(
                f"• {e.ticker}{leader_tag} ${e.strike:.0f}C "
                f"| {_fmt_p(e.premium_usd)} IV:{e.iv_pct:.1f}% "
                f"| Vol/OI {e.vol_oi_ratio:.1f}x "
                f"| Δ {delta_str} | DTE {e.dte}"
            )
        lines.append("")

    # ── Top Bears (SPEC PUT flow only — label as drags if applicable) ─────────
    bears = sorted([e for e in actionable if e.side == "PUT"], key=lambda e: (e.priority, -e.score))[:3]
    if bears:
        lines.append("TOP BEARS")
        for e in bears:
            delta_str = f"{e.delta:+.2f}" if e.delta else "N/A"
            drag_tag  = " ★" if e.ticker in drags else ""
            lines.append(
                f"• {e.ticker}{drag_tag} ${e.strike:.0f}P "
                f"| {_fmt_p(e.premium_usd)} IV:{e.iv_pct:.1f}% "
                f"| Vol/OI {e.vol_oi_ratio:.1f}x "
                f"| Δ {delta_str} | DTE {e.dte}"
            )
        lines.append("")

    # ── Game Plan ─────────────────────────────────────────────────────────────
    lines.append("GAME PLAN")
    if simp_regime == "HEDGED":
        gp_primary   = "Hedge or wait — do not force directional index trades"
        gp_secondary = "Watch for hedge unwind signal before committing"
        gp_execution = "No new index futures positions until alignment confirmed"
    elif simp_regime == "ROTATIONAL":
        s_strong = analysis.get("sectors_strong", [])
        s_weak   = analysis.get("sectors_weak", [])
        gp_primary   = f"Long {s_strong[0] if s_strong else 'strong sector'}"
        gp_secondary = f"Short {s_weak[0] if s_weak else 'weak sector'}"
        gp_execution = "Pair trade — size small, wide stops, fade extremes"
    elif simp_regime == "CHOP":
        gp_primary   = "Stand aside — no edge"
        gp_secondary = "Wait for SPY/QQQ/IWM VWAP alignment"
        gp_execution = "Do not force trades"
    elif mode == "BULLISH":
        gp_primary   = "Buy dips into VWAP on leading names"
        gp_secondary = "Avoid shorts — trend favors longs"
        gp_execution = "Scale in on first 15m confirmation, stop below PM low"
    elif mode == "BEARISH":
        gp_primary   = "Fade bounces into resistance on weak names"
        gp_secondary = "Hedge core longs with index puts"
        gp_execution = "Enter on failed bounce candle, stop above PM high"
    else:
        gp_primary   = "Reduce size"
        gp_secondary = "Wait for alignment"
        gp_execution = "No conviction trade"

    lines.append(f"▸ Primary:   {gp_primary}")
    lines.append(f"▸ Secondary: {gp_secondary}")
    lines.append(f"▸ Execution: {gp_execution}")

    # ── Top Actionable Contracts ──────────────────────────────────────────────
    actionable_section = _fmt_actionable_section(entries, effective_direction)
    if actionable_section:
        lines.append(actionable_section)

    # ── Regime block (with change detection) ─────────────────────────────────
    lines.append("")
    lines.append(_fmt_regime_block(effective_direction, execution_confidence, ms, regime_indices))

    # ── Execution Plan ────────────────────────────────────────────────────────
    if rs_data and rs_data.data_ok:
        exec_plan = _fmt_execution_plan(
            rs_data.market_state, rs_data.indices,
            direction=effective_direction, confidence=execution_confidence,
        )
        if exec_plan:
            lines.append(exec_plan)
        conviction_block = _fmt_conviction_rank(
            rs_data.market_state, rs_data.indices,
            effective_direction, execution_confidence, rs_data,
        )
        if conviction_block:
            lines.append(conviction_block)

    # ── Final Verdict — uses the same pre-computed values, no recompute ───────
    bulls_list = sorted([e for e in actionable if e.side == "CALL"], key=lambda e: e.priority)
    bears_list = sorted([e for e in actionable if e.side == "PUT"],  key=lambda e: e.priority)
    lines.append(_fmt_final_verdict(
        ms,
        effective_direction,
        bias_confidence,
        execution_confidence,
        simp_regime,
        alignment,
        rs_data,
        bull_pct,
        bear_pct,
        actionable,
        leaders,
        drags,
    ))

    human_text = "\n".join(lines)

    # ── BOT_DATA block — identical values, zero drift ─────────────────────────
    session      = current_session()
    dq_base      = baseline_data_quality(session)
    alpaca_ok    = bool(rs_data and rs_data.data_ok)
    data_quality = degrade_data_quality(dq_base, alpaca_ok=alpaca_ok, tradier_ok=True)

    primary, secondary = _get_primary_secondary_futures(
        ms,
        rs_data.indices if (rs_data and rs_data.data_ok) else None,
        effective_direction,
        execution_confidence,
        rs_data,
    )

    return _append_bot_data_block(
        human_text,
        direction=effective_direction,
        hedging=hedging,
        macro_override=analysis.get("macro_override", False),
        bias_confidence=bias_confidence,
        execution_confidence=execution_confidence,
        alignment=alignment,
        simplified_regime=simp_regime,
        primary=primary,
        secondary=secondary,
        leaders=leaders,
        drags=drags,
        session=session,
        data_quality=data_quality,
        rs_data=rs_data,
    )


# ── Pre-market forced report ──────────────────────────────────────────────────

def format_premarket_report(
    analysis: dict | None,
    overnight_notes: list[str] | None = None,
) -> str:
    """
    Forced 8:30 AM pre-market bias report.

    Output:
        PRE-MARKET BIAS REPORT
        - No major unusual flow detected   (or flow summary)
        - Overnight positioning: ...
        - ...

        Bias: NEUTRAL → WAIT OPEN   (or derived bias)
    """
    lines = ["PRE-MARKET BIAS REPORT"]

    if not analysis or analysis.get("total", 0) == 0:
        lines.append("- No major unusual flow detected")
    else:
        direction  = analysis["direction"]
        bull_pct   = analysis["bull_pct"]
        bear_pct   = analysis["bear_pct"]
        confidence = analysis["confidence"]
        entries    = analysis.get("entries", [])

        lines.append(
            f"- Flow detected: {direction} ({bull_pct}% bull / {bear_pct}% bear)"
            f" | Confidence: {confidence}/100"
        )

        top = sorted(entries, key=lambda e: (e.priority, -e.score))[:3]
        for e in top:
            lines.append(
                f"- {e.ticker} {e.side}: {_fmt_p(e.premium_usd)} "
                f"Vol/OI {e.vol_oi_ratio:.1f}x DTE {e.dte}"
            )

    # Overnight / positioning notes
    if overnight_notes:
        for note in overnight_notes:
            lines.append(f"- {note}")
    else:
        lines.append("- Overnight positioning: no data")

    lines.append("")

    # Bias conclusion
    if not analysis or analysis.get("total", 0) == 0:
        lines.append("Bias: NEUTRAL → WAIT OPEN")
        direction  = "NEUTRAL"
        confidence = 0
    else:
        direction  = analysis["direction"]
        confidence = analysis["confidence"]
        if confidence < 30:
            lines.append("Bias: NEUTRAL → WAIT OPEN")
        else:
            lines.append(f"Bias: {direction} → WATCH OPEN CONFIRMATION")

    human_text = "\n".join(lines)

    # ── BOT_DATA block (premarket — DATA_QUALITY always MEDIUM or LOW) ────────
    session      = current_session()
    data_quality = degrade_data_quality("MEDIUM", alpaca_ok=True, tradier_ok=False)
    bias_confidence = confidence
    execution_confidence = min(100, max(0, confidence - 10))

    return _append_bot_data_block(
        human_text,
        direction=direction,
        hedging=analysis.get("hedging", False) if analysis else False,
        macro_override=analysis.get("macro_override", False) if analysis else False,
        bias_confidence=bias_confidence,
        execution_confidence=execution_confidence,
        alignment="UNKNOWN",
        simplified_regime="CHOP",
        primary="NONE",
        secondary="NONE",
        leaders=analysis.get("leaders", []) if analysis else [],
        drags=analysis.get("drags", []) if analysis else [],
        session=session,
        data_quality=data_quality,
    )


# ── Channel B: legacy batch report (kept for /testsignal) ────────────────────

def format_batch_report(analysis: dict) -> str:
    """Legacy Channel B market intelligence batch post — used by /testsignal."""
    if not analysis:
        return ""

    n          = analysis["total"]
    state      = analysis["state"]
    mode       = analysis["mode"]
    logic      = analysis["trade_logic"]
    direction  = analysis["direction"]
    bull_pct   = analysis["bull_pct"]
    bear_pct   = analysis["bear_pct"]
    confidence = analysis["confidence"]
    subtype    = analysis["subtype"]
    tags       = analysis.get("tags", [])

    lines = [
        f"MARKET INTELLIGENCE  ({n} signals)",
        "",
        f"TRUE MARKET STATE:  {state}",
        "",
        "BIAS:",
        f"  Direction:   {direction} ({subtype})",
        f"  Bull / Bear: {bull_pct}% / {bear_pct}%",
        f"  Confidence:  {confidence}%",
    ]

    if confidence < 20:
        lines.append("  ⚠️  Confidence <20% — bias ignored")

    if tags:
        lines.append(f"  Tags:        {' · '.join(tags)}")

    drivers = analysis.get("drivers", [])
    if drivers:
        lines += ["", "REAL DRIVERS:"]
        for e in drivers:
            lines.append(f"  • {e.ticker} {e.side} — {e.classification} ({_fmt_p(e.premium_usd)}, p{e.priority})")

    high = analysis.get("high_conviction", [])
    spec = analysis.get("speculative", [])
    if high or spec:
        lines += ["", "TRADE CANDIDATES:"]
        if high:
            lines.append("  High Conviction:")
            for e in high:
                go_tag = " ✅ GO" if e.decision == "GO" else ""
                lines.append(f"    • {e.ticker} {e.side} — {e.classification}{go_tag}")
        if spec:
            lines.append("  Speculative:")
            for e in spec:
                lines.append(f"    • {e.ticker} {e.side} — {e.classification}")

    lines += [
        "",
        f"EXECUTION MODE:  {mode}",
        logic,
    ]

    strong  = analysis.get("sectors_strong", [])
    weak    = analysis.get("sectors_weak", [])
    neutral = analysis.get("sectors_neutral", [])
    if strong or weak:
        lines += ["", "SECTORS:"]
        if strong:
            lines.append(f"  Strong:  {', '.join(strong)}")
        if weak:
            lines.append(f"  Weak:    {', '.join(weak)}")
        if neutral:
            lines.append(f"  Neutral: {', '.join(neutral)}")

    noise = analysis.get("noise", [])
    if noise:
        counts: dict[str, int] = {}
        for e in noise:
            counts[e.classification] = counts.get(e.classification, 0) + 1
        lines += ["", "AVOID / NOISE:"]
        for cls, cnt in sorted(counts.items()):
            lines.append(f"  • {cls}: {cnt}")

    lines += ["", "Not financial advice."]
    return "\n".join(lines)


# ── Stats ─────────────────────────────────────────────────────────────────────

def format_stats(s: dict) -> str:
    days    = s["days"]
    t_note  = f" · {s['ticker_filter']}"  if s.get("ticker_filter")  else ""
    c_note  = f" · {s['class_filter']}"   if s.get("class_filter")   else ""
    avg_m   = s["avg_move"]
    avg_str = f"{avg_m * 100:+.2f}%" if avg_m is not None else "N/A"

    lines = [
        f"STATS ({days}D){t_note}{c_note}",
        "",
        f"TOTAL SIGNALS: {s['total']}",
        f"GO / HOLD / KILL: {s['go']} / {s['hold']} / {s['kill']}",
    ]

    if s["n_results"] == 0:
        lines += ["", "No completed outcomes yet."]
    else:
        lines += [
            "",
            "RESULTS:",
            f"WIN / LOSS / FLAT: {s['wins']} / {s['losses']} / {s['flats']}",
            f"WIN RATE: {s['win_rate']}%",
            f"AVG MOVE (30m): {avg_str}",
        ]

    if s.get("by_classification"):
        lines += ["", "BY CLASSIFICATION:"]
        for c in s["by_classification"]:
            lines.append(f"• {c['cls']} → {c['win_rate']}% (n={c['n']})")

    if s.get("top_tickers"):
        lines += ["", "TOP TICKERS:"]
        for t in s["top_tickers"]:
            lines.append(f"• {t['ticker']} → {t['win_rate']}% (n={t['n']})")

    return "\n".join(lines)


# ── Aggregated intel report → Channel B ──────────────────────────────────────

def format_aggregated_report_b(report, rs_data: Optional["MarketRS"] = None) -> str:
    """
    Re-format a parsed IntelReport into the standard Channel B output.

    Preserves the intelligence from the upstream aggregated report but
    normalises it into our clean Channel B structure.
    """
    if not report:
        return ""

    _LOW_CONFIDENCE = 20
    effective_direction = (
        "NEUTRAL" if report.confidence < _LOW_CONFIDENCE else report.direction
    )
    if effective_direction == "BULLISH":
        bias_emoji = "🟢"
    elif effective_direction == "BEARISH":
        bias_emoji = "🔴"
    else:
        bias_emoji = "⚪"

    context = report.context or report.direction
    conf_note = " ⚠️ LOW CONFIDENCE" if report.confidence < _LOW_CONFIDENCE else ""

    lines = [
        f"{bias_emoji} MARKET BIAS: {effective_direction} WITH {context}{conf_note}",
        f"Bear {report.bear_pct}% vs Bull {report.bull_pct}% | Confidence: {report.confidence}/100",
        "",
    ]

    def _entry_line(e) -> str:
        delta_str = f"{e.delta:+.2f}" if e.delta else "N/A"
        tag = e.tag.strip() if e.tag else ""
        return (
            f"{e.ticker} ${e.strike:.0f}{e.side[0]} "
            f"| {_fmt_p(e.premium_usd)} IV:{e.iv_pct:.0f}% "
            f"| Vol/OI {e.vol_oi_ratio:.1f}x "
            f"| Δ {delta_str} "
            f"| DTE {e.dte}"
            + (f" | {tag}" if tag else "")
        )

    # Top Overall Flow
    if report.top_overall:
        lines.append("Top Overall Flow")
        for i, e in enumerate(report.top_overall, 1):
            lines.append(f"{i}. {_entry_line(e)}")
        lines.append("")

    # Top Bulls
    if report.top_bulls:
        lines.append("Top Bulls")
        for e in report.top_bulls:
            lines.append(f"• {_entry_line(e)}")
        lines.append("")

    # Top Bears
    if report.top_bears:
        lines.append("Top Bears")
        for e in report.top_bears:
            lines.append(f"• {_entry_line(e)}")
        lines.append("")

    # Market Structure
    if report.market_structure:
        lines.append("Market Structure")
        for bullet in report.market_structure:
            # Strip leading emoji/bullet and re-add clean bullet
            clean = re.sub(r"^[•·📉📈\-\s]+", "", bullet).strip()
            if clean:
                lines.append(f"• {clean}")
        lines.append("")

    # Sector Leadership
    if report.sector_leadership:
        lines.append("Sector Leadership")
        for bullet in report.sector_leadership:
            clean = re.sub(r"^[•·📉📈\-\s]+", "", bullet).strip()
            if clean:
                lines.append(f"• {clean}")
        lines.append("")

    # Game Plan
    if report.game_plan:
        lines.append("Game Plan")
        for bullet in report.game_plan:
            clean = bullet.strip()
            if clean.startswith("▸"):
                lines.append(clean)
            elif re.match(r"^(Primary|Secondary|Execution)", clean, re.IGNORECASE):
                lines.append(f"▸ {clean}")
            elif clean.startswith("—") or clean.startswith("-"):
                lines.append(f"  {clean}")
            else:
                lines.append(f"  {clean}")
        lines.append("")

    # Quick Read (headline only — first quoted line or first bullet)
    if report.quick_read:
        lines.append("Quick Read")
        for bullet in report.quick_read[:4]:   # cap at 4 lines
            lines.append(f"• {bullet.strip().lstrip('•· ')}")
        lines.append("")

    # Top Actionable Contracts — combine all flow entries
    all_entries = report.top_overall or (report.top_bulls + report.top_bears)
    actionable_section = _fmt_actionable_section(all_entries, report.direction)
    if actionable_section:
        lines.append(actionable_section)

    # ── Market Regime Tag (with change detection + persistence) ──────────────
    ms             = rs_data.market_state if (rs_data and rs_data.data_ok) else "NO_DATA"
    regime_indices = rs_data.indices      if (rs_data and rs_data.data_ok) else None
    lines.append("")
    lines.append(_fmt_regime_block(effective_direction, report.confidence, ms, regime_indices))

    # ── Driver List ───────────────────────────────────────────────────────────
    driver_block = _fmt_driver_list(report.top_bulls, report.top_bears, rs_data)
    if driver_block:
        lines.append(driver_block)

    # ── Execution Plan (RS-powered) ───────────────────────────────────────────
    if rs_data and rs_data.data_ok:
        exec_plan = _fmt_execution_plan(
            rs_data.market_state, rs_data.indices,
            direction=effective_direction, confidence=report.confidence,
        )
        if exec_plan:
            lines.append(exec_plan)
        conviction = _fmt_conviction_rank(
            rs_data.market_state, rs_data.indices,
            effective_direction, report.confidence, rs_data,
        )
        if conviction:
            lines.append(conviction)

    # ── Compute values for Final Verdict + BOT_DATA ───────────────────────────
    bias_confidence      = report.confidence
    indices              = rs_data.indices if (rs_data and rs_data.data_ok) else None
    alignment            = _compute_alignment(effective_direction, indices)
    simp_regime          = _simplified_regime(ms, hedging=False)
    execution_confidence = _compute_structured_confidence(
        effective_direction, report.bull_pct, report.bear_pct, rs_data, all_entries
    )

    # Extract leaders/drags from parsed flow entries
    _aligned  = "CALL" if effective_direction == "BULLISH" else "PUT"
    _opposed  = "PUT"  if effective_direction == "BULLISH" else "CALL"
    leaders = list(dict.fromkeys(
        e.ticker for e in (report.top_bulls if effective_direction == "BULLISH" else report.top_bears)
        if getattr(e, "side", "") == _aligned
    ))[:5]
    drags = list(dict.fromkeys(
        e.ticker for e in (report.top_bears if effective_direction == "BULLISH" else report.top_bulls)
        if getattr(e, "side", "") == _opposed
    ))[:5]

    # ── Final Verdict ─────────────────────────────────────────────────────────
    lines.append(_fmt_final_verdict(
        ms,
        effective_direction,
        bias_confidence,
        execution_confidence,
        simp_regime,
        alignment,
        rs_data,
        report.bull_pct,
        report.bear_pct,
        all_entries,
        leaders,
        drags,
    ))

    human_text = "\n".join(lines)

    # ── BOT_DATA block (machine-readable) ─────────────────────────────────────
    session = current_session()
    dq_base = baseline_data_quality(session)
    alpaca_ok  = bool(rs_data and rs_data.data_ok)
    data_quality = degrade_data_quality(dq_base, alpaca_ok=alpaca_ok, tradier_ok=True)

    primary, secondary = _get_primary_secondary_futures(
        ms,
        indices,
        effective_direction,
        bias_confidence,
        rs_data,
    )

    return _append_bot_data_block(
        human_text,
        direction=effective_direction,
        hedging=False,   # IntelReport doesn't track hedging — safe default
        macro_override=False,
        bias_confidence=bias_confidence,
        execution_confidence=execution_confidence,
        alignment=alignment,
        simplified_regime=simp_regime,
        primary=primary,
        secondary=secondary,
        leaders=leaders,
        drags=drags,
        session=session,
        data_quality=data_quality,
        rs_data=rs_data,
    )


# ── On-demand command formatters ─────────────────────────────────────────────

def _hot_options_score(e, direction: str) -> float:
    """
    Composite urgency + quality score for /options ranking.
    Weights: Vol/OI (primary urgency), premium (size), delta quality,
    DTE urgency, classification bonus, institutional size, bias alignment.

    Hedge bonus is capped when there is no broader bearish alignment to
    prevent every index put from auto-ranking above directional plays.
    """
    import math

    _raw_delta = getattr(e, "delta", None)
    delta_abs  = abs(_raw_delta) if _raw_delta is not None else 0.0
    vol_oi     = getattr(e, "vol_oi_ratio", 0) or 0
    premium    = getattr(e, "premium_usd", 0) or 0
    dte        = getattr(e, "dte", 0) or 0
    side       = getattr(e, "side", "")
    cls        = getattr(e, "classification", "")
    ticker     = getattr(e, "ticker", "")

    score = 0.0

    # Premium — log scale so a $22M print doesn't fully dominate
    if premium > 0:
        score += min(30, math.log10(max(1, premium)) * 4.5)

    # Vol/OI — primary signal of informed / unusual activity
    if vol_oi >= 20:
        score += 32
    elif vol_oi >= 10:
        score += 26
    elif vol_oi >= 5:
        score += 18
    elif vol_oi >= 2:
        score += 8

    # Delta quality — prefer near-ATM (0.40–0.65).
    # None means delta was not reported in the flow alert — give a neutral 8 pts
    # so these valid signals are not silently buried below the floor.
    if _raw_delta is None:
        score += 8    # unknown delta: assume tradeable, no bonus, no penalty
    elif 0.40 <= delta_abs <= 0.65:
        score += 20
    elif 0.30 <= delta_abs < 0.40 or 0.65 < delta_abs <= 0.75:
        score += 10
    elif delta_abs > 0.75:
        score += 4   # deep ITM: less levered, lower urgency
    # delta_abs == 0.0 and raw_delta is not None → genuinely zero delta → 0 pts

    # DTE urgency — tighter expiries command attention
    if 1 <= dte <= 3:
        score += 20
    elif 4 <= dte <= 7:
        score += 15
    elif 8 <= dte <= 14:
        score += 10
    elif 15 <= dte <= 21:
        score += 4

    # Classification bonus
    # HEDGE_DIRECTIONAL: full bonus only when broader context confirms it
    # (bearish direction, or non-index hedge used for sector protection).
    # Pure index puts without directional confirmation get a reduced bonus so
    # they don't auto-rank above aligned directional plays.
    if cls == "HEDGE_DIRECTIONAL":
        is_index_put = ticker in _INDEX_HEDGE_TICKERS and side == "PUT"
        if is_index_put and direction != "BEARISH":
            score += 9    # hedge pressure exists but no thesis alignment — cap it
        else:
            score += 22   # directionally confirmed hedge or non-index directional hedge
    elif cls in ("POSITIONAL_BULL", "POSITIONAL_BEAR"):
        score += 16
    elif cls == "SPECULATIVE_DIRECTIONAL":
        score += 12
    elif cls == "CONTINUATION_STRONG":
        score += 8
    elif cls == "CONTINUATION_WEAK":
        score += 4

    # Institutional size bonus
    if premium >= 10_000_000:
        score += 16
    elif premium >= 1_000_000:
        score += 10
    elif premium >= 500_000:
        score += 5

    # Bias alignment bonus — separate from classification so hedges don't
    # double-dip when direction happens to match their PUT side
    is_index_hedge_put = ticker in _INDEX_HEDGE_TICKERS and side == "PUT"
    aligned = (direction == "BULLISH" and side == "CALL") or \
              (direction == "BEARISH" and side == "PUT" and not is_index_hedge_put)
    if aligned:
        score += 10

    return score


def _action_tag(e, direction: str) -> str:
    """
    Derive a concise per-contract action tag for display in /options output.

    Priority order:
      HEDGE      — HEDGE_DIRECTIONAL classification, or any index-ticker PUT
      SWEEP      — vol/oi >= 10 AND dte <= 7 (rapid informed order)
      POSITIONAL — POSITIONAL_BULL / POSITIONAL_BEAR
      MOMO       — SPECULATIVE_DIRECTIONAL or CONTINUATION_STRONG with directional edge
      LOWER QUALITY — passed the score floor but none of the above
    """
    cls    = getattr(e, "classification", "")
    side   = getattr(e, "side", "")
    vol_oi = getattr(e, "vol_oi_ratio", 0) or 0
    dte    = getattr(e, "dte", 0) or 0
    ticker = getattr(e, "ticker", "")

    is_index_put = ticker in _INDEX_HEDGE_TICKERS and side == "PUT"
    if cls == "HEDGE_DIRECTIONAL" or is_index_put:
        return "HEDGE"

    if vol_oi >= 10 and dte <= 7:
        return "SWEEP"

    if cls in ("POSITIONAL_BULL", "POSITIONAL_BEAR"):
        return "POSITIONAL"

    aligned = (direction == "BULLISH" and side == "CALL") or \
              (direction == "BEARISH" and side == "PUT")
    if cls in ("SPECULATIVE_DIRECTIONAL", "CONTINUATION_STRONG") and (vol_oi >= 5 or aligned):
        return "MOMO"

    return "LOWER QUALITY"


def _hot_options_quick_take(entries: list, direction: str) -> list[str]:
    """Generate 2–3 concise quick-take bullets for the /options footer."""
    if not entries:
        return ["No notable contracts in current window."]

    bullets: list[str] = []

    calls = [e for e in entries if getattr(e, "side", "") == "CALL"]
    puts  = [e for e in entries if getattr(e, "side", "") == "PUT"]

    # Bias bullet
    if len(calls) > len(puts) * 1.5:
        bullets.append(f"CALL-dominant flow — {len(calls)}C vs {len(puts)}P in filtered set")
    elif len(puts) > len(calls) * 1.5:
        bullets.append(f"PUT-dominant flow — {len(puts)}P vs {len(calls)}C in filtered set")
    else:
        bullets.append(f"Mixed flow — {len(calls)} calls / {len(puts)} puts")

    # Institutional bullet
    big = [e for e in entries if getattr(e, "premium_usd", 0) >= 1_000_000]
    if big:
        tickers = list(dict.fromkeys(e.ticker for e in big))[:3]
        bullets.append(f"Institutional size (>$1M): {', '.join(tickers)}")

    # Vol/OI spike bullet
    extreme = [e for e in entries if getattr(e, "vol_oi_ratio", 0) >= 10]
    high    = [e for e in entries if getattr(e, "vol_oi_ratio", 0) >= 5]
    if extreme:
        tickers = list(dict.fromkeys(e.ticker for e in extreme))[:3]
        bullets.append(f"Extreme Vol/OI (>10x) — possible informed flow: {', '.join(tickers)}")
    elif high:
        tickers = list(dict.fromkeys(e.ticker for e in high))[:3]
        bullets.append(f"High Vol/OI (>5x): {', '.join(tickers)}")

    # Urgency bullet
    urgent = [e for e in entries if 1 <= getattr(e, "dte", 99) <= 3]
    if urgent:
        tickers = list(dict.fromkeys(e.ticker for e in urgent))[:3]
        bullets.append(
            f"Expiry urgency (DTE 1–3): {', '.join(tickers)} — watch for acceleration"
        )

    return bullets[:3]


# Minimum composite score a contract must reach to appear in any /options output.
# Set to 45 — signals have already cleared hard filters (score, conviction, vol/oi,
# premium, DTE). The floor only needs to exclude the weakest combinaton of valid
# attributes, not re-apply the hard filters.
_HOT_OPTIONS_MIN_SCORE: float = 45.0


def format_hot_options(
    entries: list,
    filter_fn=None,
    label: str = "HOT OPTIONS",
    direction: str = "NEUTRAL",
    max_n: int = 8,
    max_per_ticker: int = 2,
) -> str:
    """
    Ranked hot / unusual options contracts for /options /bulls /bears /tech /hedges.

    min_score:      contracts below _HOT_OPTIONS_MIN_SCORE are silently dropped.
    max_per_ticker: de-duplication cap — highest-scoring contract(s) per ticker kept.

    Output format:
        HOT OPTIONS

        1. {ticker strike side} | ${premium} | Vol/OI {ratio} | Δ {delta} | DTE {dte} | {bias} [{tag}]
        2. ...

        Quick Take:
        - {bullet 1}
        - {bullet 2}
        - {bullet 3}

    filter_fn: optional callable(entry) -> bool applied before scoring.
    Noise and lottery are always excluded before the caller's filter.
    """
    # Base exclusions — noise and pure lottery before any filter
    pool = [
        e for e in entries
        if getattr(e, "classification", "") != "LOTTERY"
        and getattr(e, "signal_role", "") != "NOISE"
        and 1 <= getattr(e, "dte", 0) <= 30
    ]

    # Caller-supplied filter (bulls, bears, tech, hedges)
    if filter_fn is not None:
        pool = [e for e in pool if filter_fn(e)]

    if not pool:
        total = len(entries)
        reason = (
            f"0 of {total} recent signal(s) passed filters "
            f"(all were LOTTERY / NOISE / DTE out of range)."
            if total else "No signals in the last 30 minutes."
        )
        return f"{label}\n\nNo hot options right now. {reason}"

    # Score every candidate
    scored_all = sorted(
        [(round(_hot_options_score(e, direction), 1), e) for e in pool],
        key=lambda x: x[0],
        reverse=True,
    )

    # Apply minimum score floor
    passed = [(s, e) for s, e in scored_all if s >= _HOT_OPTIONS_MIN_SCORE]

    if not passed:
        best = scored_all[0][0] if scored_all else 0
        return (
            f"{label}\n\n"
            f"No hot options right now. "
            f"{len(pool)} signal(s) scanned, best score {best:.0f} "
            f"(floor {_HOT_OPTIONS_MIN_SCORE:.0f}). "
            f"Flow may be too mixed or low-conviction."
        )
    scored_all = passed

    # De-duplicate by ticker — keep the top max_per_ticker per ticker by score
    ticker_count: dict[str, int] = {}
    top: list = []
    for s, e in scored_all:
        ticker = getattr(e, "ticker", "")
        if ticker_count.get(ticker, 0) < max_per_ticker:
            top.append((s, e))
            ticker_count[ticker] = ticker_count.get(ticker, 0) + 1
        if len(top) >= max_n:
            break

    lines = [label, ""]

    for i, (_, e) in enumerate(top, 1):
        ticker    = getattr(e, "ticker", "?")
        strike    = getattr(e, "strike", 0)
        side      = getattr(e, "side", "?")
        premium   = getattr(e, "premium_usd", 0)
        vol_oi    = getattr(e, "vol_oi_ratio", 0)
        delta_v   = getattr(e, "delta", None)
        dte       = getattr(e, "dte", 0)
        opt_price = getattr(e, "option_price", None)

        strike_str    = f" {strike:.0f}" if strike else ""
        delta_str     = f"{delta_v:+.2f}" if delta_v is not None else "N/A"
        opt_price_str = f" @${opt_price:.2f}" if opt_price else ""
        bias          = "BULLISH" if side == "CALL" else "BEARISH"
        tag           = _action_tag(e, direction)

        lines.append(
            f"{i}. {ticker}{strike_str} {side}{opt_price_str} | "
            f"{_fmt_p(premium)} | "
            f"Vol/OI {vol_oi:.1f}x | "
            f"\u0394 {delta_str} | "
            f"DTE {dte} | "
            f"{bias} [{tag}]"
        )

    lines.append("")
    lines.append("Quick Take:")
    for bullet in _hot_options_quick_take([e for _, e in top], direction):
        lines.append(f"- {bullet}")

    return "\n".join(lines)


def format_flow_summary(entries: list, max_entries: int = 10) -> str:
    """
    Concise summary of recent flow signals for /flow command.
    Uses the same Channel B formatting conventions.
    """
    if not entries:
        return "FLOW SUMMARY\n- No signals in current window."

    total    = len(entries)
    bulls    = [e for e in entries if e.side == "CALL"]
    bears    = [e for e in entries if e.side == "PUT"]
    bull_pct = round(len(bulls) / total * 100)
    bear_pct = 100 - bull_pct

    lines = [
        f"FLOW SUMMARY — {total} signal(s) in window",
        f"Calls {len(bulls)} ({bull_pct}%) · Puts {len(bears)} ({bear_pct}%)",
        "",
        "TOP BY PREMIUM",
    ]

    top = sorted(entries, key=lambda e: getattr(e, "premium_usd", 0), reverse=True)[:max_entries]
    for e in top:
        tag     = _tag_for(e)
        ticker  = getattr(e, "ticker", "?")
        strike  = getattr(e, "strike", 0)
        side    = getattr(e, "side", "?")
        prem    = getattr(e, "premium_usd", 0)
        vol_oi  = getattr(e, "vol_oi_ratio", 0)
        dte     = getattr(e, "dte", 0)
        strike_str = f" {strike:.0f}" if strike else ""
        lines.append(
            f"• {ticker}{strike_str}{side[0] if side else ''}  "
            f"{_fmt_p(prem)}  Vol/OI {vol_oi:.1f}x  DTE {dte}  [{tag}]"
        )

    return "\n".join(lines)


def format_bias_only(analysis: dict, rs_data: Optional["MarketRS"] = None) -> str:
    """
    Market bias extract for /bias command.
    Includes direction, confidence, regime, and VWAP state.
    """
    lines = ["MARKET BIAS"]

    if not analysis:
        lines.append("Flow: No signals in current window")
    else:
        direction  = analysis["direction"]
        bull_pct   = analysis["bull_pct"]
        bear_pct   = analysis["bear_pct"]
        confidence = analysis["confidence"]
        state      = analysis["state"]
        lines.append(f"Direction: {direction}")
        lines.append(f"Bull {bull_pct}% / Bear {bear_pct}% | Confidence: {confidence}/100")
        lines.append(f"Flow state: {state}")

    if rs_data and rs_data.data_ok:
        idx = rs_data.indices

        def _pos(v: Optional[bool]) -> str:
            if v is True:  return "above"
            if v is False: return "below"
            return "N/A"

        lines.append(f"Market State: {rs_data.market_state}")
        if idx.data_ok:
            lines.append(
                f"SPY {_pos(idx.spy_above_vwap)} VWAP"
                f" · QQQ {_pos(idx.qqq_above_vwap)} VWAP"
                f" · IWM {_pos(idx.iwm_above_vwap)} VWAP"
            )
            if analysis:
                direction  = analysis["direction"]
                confidence = analysis["confidence"]
                regime = _derive_regime_tag(direction, confidence, rs_data.market_state, idx)
                if regime:
                    lines.append(f"Regime: {regime}")
    else:
        lines.append("Market data: unavailable")

    return "\n".join(lines)


def _interpret_no_flow_stance(market_state: str, idx: "IndexRS") -> str:
    """Plain-English market read for the no-flow snapshot."""
    spy_up    = idx.spy_above_vwap
    qqq_up    = idx.qqq_above_vwap
    iwm_up    = idx.iwm_above_vwap
    all_above = spy_up is True  and qqq_up is True  and iwm_up is True
    all_below = spy_up is False and qqq_up is False and iwm_up is False

    if market_state == "TREND_UP":
        return "Read: Uptrend intact — no new flow pressure. Watch for VWAP hold."
    if market_state == "TREND_DOWN":
        return "Read: Downtrend intact — no counter-flow. Monitor VWAP rejection."
    if market_state == "ROTATIONAL":
        return "Read: Rotation active — sector divergence, avoid index-futures entries."
    # CHOP or unknown
    if all_above:
        return "Read: Bullish lean — indices holding VWAP. No directional edge yet."
    if all_below:
        return "Read: Bearish lean — indices below VWAP. Flow confirmation needed."
    return "Read: Mixed positioning. No edge — stand aside."


def format_no_flow_snapshot(
    session_label: str,
    time_str: str,
    rs_data: Optional["MarketRS"] = None,
    prior_direction: str = "NEUTRAL",
    prior_leaders: Optional[list] = None,
    prior_laggards: Optional[list] = None,
) -> str:
    """
    Improved snapshot for when there are no signals in the window.
    Used by cmd_report (with live RS data) and _fmt_snapshot in scheduler
    (carry-forward context only, no RS).

    Output example:
        AFTER HOURS SNAPSHOT — 17:30 ET
        - No signals in last 30 min
        SPY +0.12% · QQQ +0.31% · IWM -0.08%
        State: CHOP | Regime: MIXED / UNTRADEABLE
        Prior bias: BULLISH | Leaders: NVDA, META

        Read: Bullish lean — indices holding VWAP. No directional edge yet.
    """
    lines = [f"{session_label} SNAPSHOT — {time_str} ET", "- No signals in last 30 min"]

    if rs_data and rs_data.data_ok and rs_data.indices.data_ok:
        idx = rs_data.indices

        def _vwap_str(pct: Optional[float], above: Optional[bool]) -> str:
            if pct is not None:
                return f"{'+' if pct >= 0 else ''}{pct:.2f}%"
            if above is True:  return "above VWAP"
            if above is False: return "below VWAP"
            return "N/A"

        lines.append(
            f"SPY {_vwap_str(idx.spy_pct_vs_vwap, idx.spy_above_vwap)}"
            f" · QQQ {_vwap_str(idx.qqq_pct_vs_vwap, idx.qqq_above_vwap)}"
            f" · IWM {_vwap_str(idx.iwm_pct_vs_vwap, idx.iwm_above_vwap)}"
        )

        regime = _derive_regime_tag("NEUTRAL", 0, rs_data.market_state, idx)
        lines.append(f"State: {rs_data.market_state} | Regime: {regime}")

        carry_parts: list[str] = []
        if prior_direction and prior_direction != "NEUTRAL":
            carry_parts.append(f"Prior bias: {prior_direction}")
        if prior_leaders:
            carry_parts.append(f"Leaders: {', '.join(prior_leaders)}")
        if prior_laggards:
            carry_parts.append(f"Laggards: {', '.join(prior_laggards)}")
        if carry_parts:
            lines.append(" | ".join(carry_parts))

        lines.append("")
        lines.append(_interpret_no_flow_stance(rs_data.market_state, idx))

    else:
        # No RS data — carry-forward context + generic read
        carry_parts = []
        if prior_direction and prior_direction != "NEUTRAL":
            carry_parts.append(f"Prior bias: {prior_direction}")
        if prior_leaders:
            carry_parts.append(f"Leaders: {', '.join(prior_leaders)}")
        if prior_laggards:
            carry_parts.append(f"Laggards: {', '.join(prior_laggards)}")
        if carry_parts:
            lines.append(" | ".join(carry_parts))
        lines.append("Read: No clear market structure. Wait for flow confirmation.")

    return "\n".join(lines)


def format_single_future_plan(
    future: str,
    rs_data: Optional["MarketRS"],
    direction: str = "NEUTRAL",
    confidence: int = 0,
) -> str:
    """
    Concise execution plan for a single index future.
    Used by /nq /es /rty /ym commands.
    """
    fu = future.upper()

    if rs_data is None or not rs_data.data_ok:
        return f"{fu} PLAN\n- No market data available."

    idx = rs_data.indices
    if idx is None or not idx.data_ok:
        return f"{fu} PLAN\n- Index data unavailable."

    market_state = rs_data.market_state

    def _p(v: Optional[float]) -> str:
        return f"${v:.2f}" if v is not None else "N/A"

    if fu == "NQ":
        act, why   = _nq_decision(idx, market_state, confidence)
        ref_ticker = "QQQ"
        ref_vwap   = idx.qqq_vwap
        ref_pm_low = idx.qqq_pm_low
        ref_above  = idx.qqq_above_vwap
        ref_pct    = idx.qqq_pct_vs_vwap
    elif fu == "ES":
        act, why   = _es_decision(idx, market_state, confidence)
        ref_ticker = "SPY"
        ref_vwap   = idx.spy_vwap
        ref_pm_low = idx.spy_pm_low
        ref_above  = idx.spy_above_vwap
        ref_pct    = idx.spy_pct_vs_vwap
    elif fu == "RTY":
        act, why   = _rty_decision(idx, market_state)
        ref_ticker = "IWM"
        ref_vwap   = idx.iwm_vwap
        ref_pm_low = idx.iwm_pm_low
        ref_above  = idx.iwm_above_vwap
        ref_pct    = idx.iwm_pct_vs_vwap
    elif fu == "YM":
        act, why   = _ym_decision(idx, market_state)
        ref_ticker = "SPY"
        ref_vwap   = idx.spy_vwap
        ref_pm_low = idx.spy_pm_low
        ref_above  = idx.spy_above_vwap
        ref_pct    = idx.spy_pct_vs_vwap
    else:
        return f"Unknown future: {fu}"

    vwap_pos = "above" if ref_above else ("below" if ref_above is False else "N/A")
    pct_str  = f" ({ref_pct:+.2f}%)" if ref_pct is not None else ""

    lines = [
        f"{fu} EXECUTION PLAN",
        f"{ref_ticker}: {vwap_pos} VWAP {_p(ref_vwap)}{pct_str}",
        f"State: {market_state}",
        "",
    ]

    if act == "NO TRADE":
        lines.append("Decision: NO TRADE")
        lines.append(f"Reason: {why}")
    elif act == "LONG":
        lines.append("Decision: LONG")
        if fu == "RTY":
            lines.append(
                f"Trigger: {ref_ticker} holds above {_p(ref_vwap)} VWAP "
                "with SPY breadth confirming"
            )
        elif fu == "YM":
            lines.append("Trigger: SPY VWAP hold confirms broad non-tech participation")
        else:
            lines.append(f"Trigger: {ref_ticker} holds above {_p(ref_vwap)} VWAP")
        lines.append(f"Stop: {ref_ticker} loses {_p(ref_pm_low)} (PM low)")
    elif act == "SHORT":
        lines.append("Decision: SHORT")
        lines.append(f"Trigger: {ref_ticker} fails to reclaim {_p(ref_vwap)} VWAP")
        lines.append(f"Stop: {ref_ticker} reclaims {_p(ref_vwap)}")

    return "\n".join(lines)


# ── Legacy single-signal formatters ──────────────────────────────────────────

def format_hold(sig: FlowSignal, dec: Decision) -> str:
    return (
        f"{sig.ticker} {sig.side} FLOW\n"
        f"Score: {sig.score} ({sig.conviction})\n"
        f"Decision: HOLD\n"
        f"\n"
        f"Reason: Awaiting confirmation\n"
        f"\n"
        f"Not financial advice."
    )


def format_go(sig: FlowSignal, dec: Decision) -> str:
    from datetime import datetime
    import pytz
    _ET = pytz.timezone("America/New_York")
    ts_et = datetime.now(_ET).strftime("%-I:%M %p ET")

    trigger_line = f"Trigger: {dec.trigger_reason}\n\n" if dec.trigger_reason else ""
    direction = "BULLISH" if sig.side == "CALL" else "BEARISH"
    exp_str = sig.expiration.strftime("%-m/%-d")
    contract_label = f"{sig.ticker} {sig.strike:.0f}{sig.side[0]} {exp_str}"

    opt_prem = sig.premium_at_signal
    prem_line = f"Premium at signal: ${opt_prem:.2f}" if opt_prem else "Premium at signal: N/A"

    return (
        f"{direction} — {contract_label} | {prem_line} | {ts_et}\n"
        f"\n"
        f"Score: {sig.score} ({sig.conviction})\n"
        f"Decision: GO\n"
        f"\n"
        f"Entry: {_fmt(dec.entry)}\n"
        f"Stop: {_fmt(dec.stop)}\n"
        f"Target: {_fmt(dec.target)} (2R)\n"
        f"\n"
        f"{trigger_line}"
        f"Not financial advice."
    )
