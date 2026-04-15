"""
Session handler — detect current market session and enforce per-session rules.

Sessions (all times Eastern):
  PREMARKET   04:00 – 09:29
  RTH         09:30 – 15:59
  AFTER_HOURS 16:00 – 19:59
  CLOSED      all other times

Rules:
  PREMARKET   — options flow signals disabled; price data only; DATA_QUALITY LOW/MEDIUM
  RTH         — full engine active; DATA_QUALITY HIGH (degrades on data failure)
  AFTER_HOURS — flow signals disabled; price data only; DATA_QUALITY LOW
  CLOSED      — no signals
"""

from datetime import datetime, time as dtime
from typing import Optional
import pytz

ET = pytz.timezone("America/New_York")

SESSION_PREMARKET   = "PREMARKET"
SESSION_RTH         = "RTH"
SESSION_AFTER_HOURS = "AFTER_HOURS"
SESSION_CLOSED      = "CLOSED"

_PREMARKET_START   = dtime(4, 0)
_RTH_START         = dtime(9, 30)
_RTH_END           = dtime(16, 0)
_AFTER_HOURS_END   = dtime(20, 0)


def current_session(now_et: Optional[datetime] = None) -> str:
    """Return the session label for the given (or current) ET timestamp."""
    if now_et is None:
        now_et = datetime.now(ET)
    t = now_et.time()
    wd = now_et.weekday()   # Monday=0, Sunday=6

    if wd >= 5:  # weekend
        return SESSION_CLOSED

    if _PREMARKET_START <= t < _RTH_START:
        return SESSION_PREMARKET
    if _RTH_START <= t < _RTH_END:
        return SESSION_RTH
    if _RTH_END <= t < _AFTER_HOURS_END:
        return SESSION_AFTER_HOURS
    return SESSION_CLOSED


def flow_signals_enabled(session: str) -> bool:
    """True only during RTH — options flow data is meaningful only then."""
    return session == SESSION_RTH


def baseline_data_quality(session: str) -> str:
    """
    Baseline DATA_QUALITY before any runtime degradation checks.

    RTH         → HIGH   (Alpaca + Tradier fully reliable)
    PREMARKET   → MEDIUM (price data only; flow absent)
    AFTER_HOURS → LOW    (thin tape; flow absent)
    CLOSED      → LOW
    """
    if session == SESSION_RTH:
        return "HIGH"
    if session == SESSION_PREMARKET:
        return "MEDIUM"
    return "LOW"


def degrade_data_quality(baseline: str, *, alpaca_ok: bool, tradier_ok: bool) -> str:
    """
    Apply runtime degradation on top of the session baseline.

    HIGH → MEDIUM if either data source is unavailable.
    MEDIUM → LOW  if Alpaca (price) is unavailable.
    LOW stays LOW.
    """
    if baseline == "HIGH":
        if not alpaca_ok or not tradier_ok:
            return "MEDIUM"
        return "HIGH"
    if baseline == "MEDIUM":
        if not alpaca_ok:
            return "LOW"
        return "MEDIUM"
    return "LOW"


def signals_actionable(data_quality: str) -> bool:
    """Signals must NOT be actionable when data quality is LOW."""
    return data_quality != "LOW"
