"""
Parse pre-formatted aggregated intelligence report messages from Channel A.

Detection is intentionally loose — uses substring checks, not strict regex,
so formatting variations (emoji spacing, missing sections, different bullets)
do not cause a miss.

Parsing is tolerant — partial results are returned instead of None when a
section is present but slightly malformed.
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

_PREMIUM_MULT = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class FlowEntry:
    ticker: str
    strike: float
    side: str           # "CALL" | "PUT"
    premium_usd: float
    iv_pct: float
    vol_oi_ratio: float
    delta: float
    dte: int
    tag: str = ""


@dataclass
class IntelReport:
    direction: str            # "BULLISH" | "BEARISH" | "NEUTRAL"
    context: str
    bear_pct: int
    bull_pct: int
    confidence: int
    top_overall:       list[FlowEntry] = field(default_factory=list)
    top_bears:         list[FlowEntry] = field(default_factory=list)
    top_bulls:         list[FlowEntry] = field(default_factory=list)
    market_structure:  list[str]       = field(default_factory=list)
    sector_leadership: list[str]       = field(default_factory=list)
    game_plan:         list[str]       = field(default_factory=list)
    quick_read:        list[str]       = field(default_factory=list)


# ── Detection ─────────────────────────────────────────────────────────────────

def is_aggregated_report(text: str) -> bool:
    """
    Robust detection using substring checks only.
    True if message contains MARKET BIAS + at least one flow section.
    """
    upper = text.upper()
    has_bias   = "MARKET BIAS" in upper
    has_flow   = "TOP OVERALL FLOW" in upper
    has_sides  = ("TOP BULLS" in upper) or ("TOP BEARS" in upper)
    return has_bias and (has_flow or has_sides)


# ── Premium parser ────────────────────────────────────────────────────────────

def _parse_premium(raw: str) -> float:
    raw = raw.replace(",", "").strip()
    if not raw:
        return 0.0
    suffix = raw[-1].upper() if raw[-1].upper() in _PREMIUM_MULT else ""
    try:
        val = float(raw[:-1] if suffix else raw)
        return val * _PREMIUM_MULT.get(suffix, 1)
    except ValueError:
        return 0.0


# ── Entry line parser ─────────────────────────────────────────────────────────
#
# Handles both formats:
#   1. 🟢 META 660C AGGR  | $22.37M IV:37%  | Vol/OI 2.7x  | Δ 0.57  | DTE 3  | TAG
#   • META $660C | $22.4M IV:37% | Vol/OI 2.7x | Δ +0.57 | DTE 3 | TAG
#
_ENTRY_RE = re.compile(
    r"(?:(?:\d+)[.)]\s*|[•·\-]\s*)"    # "1." / "1)" / "•" / "-" prefix
    r"(?:[🟢🔴✅⚠️]\s*)*"               # zero or more leading emoji
    r"([A-Z]{1,6})"                      # ticker
    r"\s+\$?(\d+(?:\.\d+)?)(C|P)"        # optional $ + strike + C/P
    r"(?:\s+\w+)?"                        # optional label (AGGR, etc.)
    r"\s*\|\s*\$([0-9,.]+[KMBkmb]?)"     # | $premium
    r"(?:\s+IV:(\d+(?:\.\d+)?)%)?"       # IV (optional)
    r"\s*\|\s*Vol/OI\s+([\d.]+)x"        # | Vol/OI Nx
    r"\s*\|\s*[Δδ]\s*([+-]?[\d.]+)"      # | Δ delta
    r"\s*\|\s*DTE\s+(\d+)"               # | DTE N
    r"(?:\s*\|\s*(.+?))?(?:\s*\|.*)?$",  # | tag  (stop at next pipe)
    re.IGNORECASE,
)


def _parse_entry(line: str) -> Optional[FlowEntry]:
    # Strip leading whitespace and common decorators before matching
    clean = line.strip()
    m = _ENTRY_RE.search(clean)
    if not m:
        return None
    ticker, strike, cp, premium_raw, iv_raw, vol_oi, delta, dte, tag = m.groups()
    try:
        return FlowEntry(
            ticker=ticker.upper(),
            strike=float(strike),
            side="CALL" if cp.upper() == "C" else "PUT",
            premium_usd=_parse_premium(premium_raw or ""),
            iv_pct=float(iv_raw) if iv_raw else 0.0,
            vol_oi_ratio=float(vol_oi),
            delta=float(delta),
            dte=int(dte),
            tag=(tag or "").strip().rstrip("|").strip(),
        )
    except (ValueError, TypeError):
        return None


# ── Section splitter ──────────────────────────────────────────────────────────

# Section header keywords (case-insensitive substring match)
_SECTION_KEYS = [
    "top overall flow",
    "top bears",
    "top bulls",
    "market structure",
    "sector leadership",
    "sector",
    "game plan",
    "quick read",
]


def _section_header(line: str) -> Optional[str]:
    """Return the canonical section key if line is a section header, else None."""
    low = line.strip().lower()
    # Strip leading emoji / punctuation for matching
    clean = re.sub(r"^[^a-z]+", "", low).strip()
    for key in _SECTION_KEYS:
        if clean.startswith(key):
            return key
    return None


def _split_sections(lines: list[str]) -> dict[str, list[str]]:
    """Split lines into {section_key: [content lines]} dict."""
    sections: dict[str, list[str]] = {}
    current_key: Optional[str] = None

    for line in lines:
        key = _section_header(line)
        if key:
            current_key = key
            sections.setdefault(current_key, [])
        elif current_key is not None:
            stripped = line.strip()
            if stripped:
                sections[current_key].append(stripped)

    return sections


# ── Main parser ───────────────────────────────────────────────────────────────

def parse_intel_report(text: str, msg_id: int = 0) -> Optional[IntelReport]:
    """
    Parse an aggregated intelligence report.

    Returns a (possibly partial) IntelReport.
    Only returns None if the bias line cannot be found at all.
    """
    logger.info("Aggregated parser triggered | msg_id=%s", msg_id or "?")

    lines = text.strip().splitlines()

    # ── Bias line — scan first 5 lines ───────────────────────────────────────
    direction = "NEUTRAL"
    context   = ""
    for line in lines[:5]:
        m = re.search(r"MARKET\s+BIAS\s*[:\-]\s*(BEARISH|BULLISH|NEUTRAL)", line, re.IGNORECASE)
        if m:
            direction = m.group(1).upper()
            after = line[m.end():].strip()
            ctx_m = re.search(r"WITH\s+(.+)", after, re.IGNORECASE)
            context = ctx_m.group(1).strip() if ctx_m else after
            break
    else:
        logger.warning("Aggregated parser: no MARKET BIAS line found | msg_id=%s", msg_id or "?")
        return None

    # ── Bear / Bull / Confidence ──────────────────────────────────────────────
    bear_pct = bull_pct = confidence = 0
    for line in lines[:8]:
        bb = re.search(r"Bear\s+(\d+)%.*?Bull\s+(\d+)%", line, re.IGNORECASE)
        if bb:
            bear_pct = int(bb.group(1))
            bull_pct = int(bb.group(2))
        cf = re.search(r"Confidence\s*[:\-]?\s*(\d+)", line, re.IGNORECASE)
        if cf:
            confidence = int(cf.group(1))

    report = IntelReport(
        direction=direction,
        context=context,
        bear_pct=bear_pct,
        bull_pct=bull_pct,
        confidence=confidence,
    )

    # ── Split into sections ───────────────────────────────────────────────────
    sections = _split_sections(lines)

    def _parse_entries(key: str) -> list[FlowEntry]:
        entries = []
        for line in sections.get(key, []):
            e = _parse_entry(line)
            if e:
                entries.append(e)
            else:
                logger.debug("Entry parse miss | section=%s | line=%r", key, line[:80])
        return entries

    report.top_overall       = _parse_entries("top overall flow")
    report.top_bulls         = _parse_entries("top bulls")
    report.top_bears         = _parse_entries("top bears")
    report.market_structure  = sections.get("market structure", [])
    report.sector_leadership = sections.get("sector leadership", sections.get("sector", []))
    report.game_plan         = sections.get("game plan", [])
    report.quick_read        = sections.get("quick read", [])

    logger.info(
        "Aggregated parser done | msg_id=%s | direction=%s | confidence=%d "
        "| top_overall=%d | bulls=%d | bears=%d | game_plan_lines=%d",
        msg_id or "?", direction, confidence,
        len(report.top_overall), len(report.top_bulls), len(report.top_bears),
        len(report.game_plan),
    )

    return report
