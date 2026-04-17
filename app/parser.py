"""
Parse raw Channel A flow alert messages into FlowSignal dataclasses.

Expected format:
    🔴 GOOGL $318P  13 Apr [ATM]
    💰 $653K  ·  Vol 2,974 / OI 139 (21.4x)
    Δ -0.53  IV 19.6%  DTE 3
    Score 100  ·  Conviction A (88)
    📈 BEARISH AGGRESSIVE
"""

import re
import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)

# Matches "13 Apr", "3 Apr", "13 Apr 2025"
_EXP_RE = re.compile(r"(\d{1,2})\s+([A-Za-z]{3})(?:\s+(\d{4}))?")
_PREMIUM_RE = re.compile(r"\$([\d,]+(?:\.\d+)?)(K|M|B)?", re.IGNORECASE)
_VOL_RE = re.compile(r"Vol\s+([\d,]+)", re.IGNORECASE)
_OI_RE = re.compile(r"/\s*OI\s+([\d,]+)", re.IGNORECASE)
_VOIRATIO_RE = re.compile(r"\(([\d.]+)x\)")
_DELTA_RE = re.compile(r"Δ\s*([-\d.]+)")
_IV_RE = re.compile(r"IV\s+([\d.]+)%", re.IGNORECASE)
_DTE_RE = re.compile(r"DTE\s+(\d+)", re.IGNORECASE)
_SCORE_RE = re.compile(r"Score\s+(\d+)", re.IGNORECASE)
_CONV_RE = re.compile(r"Conviction\s+([A-Z])", re.IGNORECASE)


@dataclass
class FlowSignal:
    raw_message: str
    ticker: str
    side: str           # "CALL" | "PUT"
    strike: float
    expiration: date
    premium_usd: float
    volume: int
    open_interest: int
    vol_oi_ratio: float
    iv_pct: float
    dte: int
    score: int
    conviction: str     # "A" | "B" | ...
    direction: str      # "BULLISH" | "BEARISH"
    delta: Optional[float] = None           # None if absent from message
    message_id: Optional[int] = None
    option_last: Optional[float] = None     # contract last price
    option_bid: Optional[float] = None
    option_ask: Optional[float] = None
    option_mid: Optional[float] = None     # (bid + ask) / 2 — preferred over last
    option_quote_time: Optional[str] = None
    # Locked signal-time premium — set once after quote fetch, never updated from live chain.
    # Fallback order: mid → mark → last. Source of truth for all alerts and DB records.
    premium_at_signal: Optional[float] = None
    signal_id: str = field(init=False)

    def __post_init__(self):
        self.signal_id = f"{self.ticker}_{self.side}_{self.strike}_{self.expiration.isoformat()}"

    def lock_signal_premium(self) -> None:
        """Call immediately after option quote is fetched. Locks premium_at_signal once."""
        if self.premium_at_signal is not None:
            return  # already locked — never overwrite
        self.premium_at_signal = self.option_mid or self.option_last or None


def _parse_premium(raw: str) -> float:
    m = _PREMIUM_RE.search(raw)
    if not m:
        return 0.0
    val = float(m.group(1).replace(",", ""))
    mult_map = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
    mult = mult_map.get((m.group(2) or "").upper(), 1)
    return val * mult


def _parse_expiration(raw: str) -> Optional[date]:
    m = _EXP_RE.search(raw)
    if not m:
        return None
    day = int(m.group(1))
    mon = m.group(2).capitalize()
    year = int(m.group(3)) if m.group(3) else datetime.now().year
    try:
        exp = datetime.strptime(f"{day} {mon} {year}", "%d %b %Y").date()
        # Roll to next year if already past
        if exp < date.today():
            exp = datetime.strptime(f"{day} {mon} {year + 1}", "%d %b %Y").date()
        return exp
    except ValueError:
        logger.warning("Could not parse expiration from: %s", raw)
        return None


def _int_clean(s: str) -> int:
    return int(s.replace(",", ""))


def parse_flow_message(text: str, message_id: int = None) -> Optional[FlowSignal]:
    """Return FlowSignal or None if the message doesn't match the expected format."""
    if not text:
        return None

    lines = [ln.strip() for ln in text.strip().splitlines() if ln.strip()]
    if len(lines) < 4:
        return None

    # ── Line 0: ticker, strike, side, expiry ────────────────────────────────
    # Pattern: optional emoji + TICKER $<strike>C/P  <day> <month>
    line0 = lines[0]
    ticker_match = re.search(
        r"([A-Z]{1,6})\s+\$?([\d.]+)(C|P)\b",
        line0,
        re.IGNORECASE,
    )
    if not ticker_match:
        logger.debug("Line 0 did not match ticker pattern: %s", line0)
        return None

    ticker = ticker_match.group(1).upper()
    strike = float(ticker_match.group(2))
    side = "CALL" if ticker_match.group(3).upper() == "C" else "PUT"

    expiration = _parse_expiration(line0)
    if expiration is None:
        logger.debug("Could not parse expiration from: %s", line0)
        return None

    # ── Line 1: premium, vol, OI, ratio ─────────────────────────────────────
    line1 = lines[1]
    premium = _parse_premium(line1)

    vol_m = _VOL_RE.search(line1)
    oi_m = _OI_RE.search(line1)
    ratio_m = _VOIRATIO_RE.search(line1)

    if not vol_m or not oi_m:
        logger.debug("Line 1 missing Vol/OI: %s", line1)
        return None

    volume = _int_clean(vol_m.group(1))
    oi = _int_clean(oi_m.group(1))
    vol_oi = float(ratio_m.group(1)) if ratio_m else (round(volume / oi, 2) if oi > 0 else 0.0)

    # ── Line 2: delta, IV, DTE ───────────────────────────────────────────────
    line2 = lines[2]
    delta_m = _DELTA_RE.search(line2)
    iv_m = _IV_RE.search(line2)
    dte_m = _DTE_RE.search(line2)

    delta = float(delta_m.group(1)) if delta_m else None
    iv = float(iv_m.group(1)) if iv_m else 0.0
    dte = int(dte_m.group(1)) if dte_m else 0

    # ── Line 3: score, conviction ────────────────────────────────────────────
    line3 = lines[3]
    score_m = _SCORE_RE.search(line3)
    conv_m = _CONV_RE.search(line3)

    if not score_m or not conv_m:
        logger.debug("Line 3 missing Score/Conviction: %s", line3)
        return None

    score = int(score_m.group(1))
    conviction = conv_m.group(1).upper()

    # ── Line 4 (optional): direction ─────────────────────────────────────────
    direction = "BULLISH"
    if len(lines) > 4:
        tail = lines[4].upper()
        if "BEARISH" in tail:
            direction = "BEARISH"

    try:
        return FlowSignal(
            raw_message=text,
            ticker=ticker,
            side=side,
            strike=strike,
            expiration=expiration,
            premium_usd=premium,
            volume=volume,
            open_interest=oi,
            vol_oi_ratio=vol_oi,
            delta=delta,
            iv_pct=iv,
            dte=dte,
            score=score,
            conviction=conviction,
            direction=direction,
            message_id=message_id,
        )
    except Exception as exc:
        logger.error("FlowSignal construction failed: %s", exc, exc_info=True)
        return None
