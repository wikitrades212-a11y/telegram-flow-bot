"""
Hedge flow detector.

Distinguishes HEDGE flow from DIRECTIONAL flow based on three criteria:

  1. Delta band — hedges cluster around ±0.40–0.60 (near-money protection)
  2. Vol/OI ratio — hedges are urgent; elevated Vol/OI (≥ 2.0) indicates fresh
     position-opening rather than roll activity
  3. Counter-trend — the contract side is OPPOSITE to the current market
     direction (i.e. PUTs while trend is bullish, or CALLs while bearish)

All three conditions together → HEDGE.
Two out of three → PROBABLE_HEDGE (treated as HEDGE for the BOT_DATA block).
One or none → DIRECTIONAL.

Public API
----------
classify_hedge(sig, market_direction) -> HedgeResult
is_hedging(signals, market_direction) -> bool   — True if ≥1 hedge in the batch
"""

from dataclasses import dataclass
from typing import Optional

# Thresholds — tuned for equity options
_DELTA_LOW  = 0.35   # softer lower bound for "near-money"
_DELTA_HIGH = 0.65   # upper bound before it becomes deep ITM (speculation)
_MIN_VOL_OI = 2.0    # minimum urgency to be a hedge (not a roll)
_MIN_PREMIUM = 100_000  # hedges carry meaningful size


HEDGE_TYPE_HEDGE         = "HEDGE"
HEDGE_TYPE_PROBABLE      = "PROBABLE_HEDGE"
HEDGE_TYPE_DIRECTIONAL   = "DIRECTIONAL"


@dataclass
class HedgeResult:
    hedge_type: str          # HEDGE | PROBABLE_HEDGE | DIRECTIONAL
    delta_flag: bool         # delta in the protection band
    counter_trend_flag: bool # contract opposes current market direction
    vol_oi_flag: bool        # elevated Vol/OI
    score: int               # 0–3 criteria met


def classify_hedge(
    side: str,                      # "CALL" | "PUT"
    delta: Optional[float],
    vol_oi_ratio: float,
    premium_usd: float,
    market_direction: str,          # "BULLISH" | "BEARISH" | "NEUTRAL"
) -> HedgeResult:
    """
    Classify a single flow signal as HEDGE, PROBABLE_HEDGE, or DIRECTIONAL.

    Parameters
    ----------
    side             : "CALL" or "PUT"
    delta            : absolute delta value (pass abs(delta) or None)
    vol_oi_ratio     : Vol / OI ratio
    premium_usd      : total premium in USD
    market_direction : overall market bias at the time of classification
    """
    delta_abs = abs(delta) if delta is not None else None

    # ── Criterion 1 — delta in near-money protection band ──────────────────────
    delta_flag = (
        delta_abs is not None
        and _DELTA_LOW <= delta_abs <= _DELTA_HIGH
    )

    # ── Criterion 2 — elevated Vol/OI (urgency / fresh open interest) ──────────
    vol_oi_flag = vol_oi_ratio >= _MIN_VOL_OI

    # ── Criterion 3 — counter-trend direction ──────────────────────────────────
    # PUTs while market is BULLISH, or CALLs while market is BEARISH
    if market_direction == "BULLISH":
        counter_trend_flag = (side == "PUT")
    elif market_direction == "BEARISH":
        counter_trend_flag = (side == "CALL")
    else:
        # NEUTRAL market — cannot determine counter-trend
        counter_trend_flag = False

    score = sum([delta_flag, vol_oi_flag, counter_trend_flag])

    # Premium guard — small trades are speculation, not hedges
    if premium_usd < _MIN_PREMIUM:
        return HedgeResult(
            hedge_type=HEDGE_TYPE_DIRECTIONAL,
            delta_flag=delta_flag,
            counter_trend_flag=counter_trend_flag,
            vol_oi_flag=vol_oi_flag,
            score=0,
        )

    # Counter-trend direction is a hard prerequisite — you cannot hedge
    # by buying contracts that already align with the market direction.
    if not counter_trend_flag:
        return HedgeResult(
            hedge_type=HEDGE_TYPE_DIRECTIONAL,
            delta_flag=delta_flag,
            counter_trend_flag=False,
            vol_oi_flag=vol_oi_flag,
            score=score,
        )

    if score == 3:
        hedge_type = HEDGE_TYPE_HEDGE
    elif score == 2:
        hedge_type = HEDGE_TYPE_PROBABLE
    else:
        hedge_type = HEDGE_TYPE_DIRECTIONAL

    return HedgeResult(
        hedge_type=hedge_type,
        delta_flag=delta_flag,
        counter_trend_flag=counter_trend_flag,
        vol_oi_flag=vol_oi_flag,
        score=score,
    )


def is_hedging(entries: list, market_direction: str) -> bool:
    """
    Return True if the batch contains at least one confirmed HEDGE or
    PROBABLE_HEDGE flow signal.

    Parameters
    ----------
    entries          : list of BatchEntry (or any object with side, delta,
                       vol_oi_ratio, premium_usd attributes)
    market_direction : "BULLISH" | "BEARISH" | "NEUTRAL"
    """
    for e in entries:
        result = classify_hedge(
            side=getattr(e, "side", ""),
            delta=getattr(e, "delta", None),
            vol_oi_ratio=getattr(e, "vol_oi_ratio", 0.0),
            premium_usd=getattr(e, "premium_usd", 0.0),
            market_direction=market_direction,
        )
        if result.hedge_type in (HEDGE_TYPE_HEDGE, HEDGE_TYPE_PROBABLE):
            return True
    return False
