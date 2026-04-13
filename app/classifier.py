"""
Flow classification: derives classification, signal_role, and priority
from FlowSignal fields.

Classification labels:
  HEDGE_DIRECTIONAL     — large near-money put/call, short DTE, big premium
  SPECULATIVE_DIRECTIONAL — high vol/OI, short DTE, OTM
  POSITIONAL_BULL/BEAR  — large premium, longer DTE, conviction A
  CONTINUATION_STRONG   — top score + A conviction
  CONTINUATION_WEAK     — passing score + A conviction
  GAMMA_VOL             — near-expiry, elevated IV
  LOTTERY               — deep OTM, low premium, short DTE

Signal roles:
  MARKET_SIGNAL   — SPY, QQQ, major indices
  SECTOR_SIGNAL   — sector ETFs
  SPECULATIVE_PLAY — individual stocks with unusual activity
  NOISE           — priority 5 + not a market/sector instrument
"""

MARKET_TICKERS = {
    "SPY","QQQ","IWM","SPX","NDX","VIX","DIA","TLT",
    "SQQQ","TQQQ","UVXY","VIXY","SPXU","SPXS","QID","PSQ",
}

SECTOR_TICKERS = {
    "XLF","XLE","XLY","XLK","XLV","XLI","XLB","XLU","XLRE","XLP",
    "GLD","SLV","USO","GDX","SMH","SOXX","ARKK","HYG","LQD","EEM",
    "KRE","XBI","IBB","ITB","XRT","KBE",
}


def iv_bucket(iv_pct: float) -> str:
    if iv_pct < 20:
        return "LOW"
    if iv_pct < 40:
        return "MID"
    if iv_pct < 70:
        return "HIGH"
    return "EXTREME"


def aggression_label(vol_oi: float) -> str:
    if vol_oi >= 10:
        return "EXTREME"
    if vol_oi >= 5:
        return "HIGH"
    if vol_oi >= 2:
        return "MEDIUM"
    return "LOW"


def participant_label(premium: float) -> str:
    if premium >= 1_000_000:
        return "INSTITUTION"
    if premium >= 200_000:
        return "FUND"
    return "RETAIL"


def classify_flow(sig) -> tuple[str, str, int]:
    """
    Returns (classification, signal_role, priority).

    Rules are ordered by specificity — first match wins.
    """
    delta   = abs(sig.delta) if sig.delta is not None else 0.0
    premium = sig.premium_usd

    # ── Signal role ───────────────────────────────────────────────────────────
    if sig.ticker in MARKET_TICKERS:
        role = "MARKET_SIGNAL"
    elif sig.ticker in SECTOR_TICKERS:
        role = "SECTOR_SIGNAL"
    else:
        role = "SPECULATIVE_PLAY"

    # ── Classification (first match wins) ─────────────────────────────────────
    if delta >= 0.40 and premium >= 500_000 and sig.dte <= 7:
        cls, pri = "HEDGE_DIRECTIONAL", 1

    elif premium >= 500_000 and sig.dte >= 7 and sig.conviction == "A":
        cls = "POSITIONAL_BULL" if sig.side == "CALL" else "POSITIONAL_BEAR"
        pri = 2

    elif sig.vol_oi_ratio >= 5.0 and sig.dte <= 14 and sig.score >= 85:
        cls, pri = "SPECULATIVE_DIRECTIONAL", 2

    elif sig.vol_oi_ratio >= 5.0 and sig.dte <= 14:
        cls, pri = "SPECULATIVE_DIRECTIONAL", 3

    elif sig.score >= 90 and sig.conviction == "A":
        cls, pri = "CONTINUATION_STRONG", 3

    elif sig.score >= 75 and sig.conviction == "A":
        cls, pri = "CONTINUATION_WEAK", 4

    elif sig.dte <= 3 and sig.iv_pct >= 50:
        cls, pri = "GAMMA_VOL", 4

    else:
        cls, pri = "LOTTERY", 5

    # Downgrade SPECULATIVE_PLAY → NOISE when priority is 5
    if pri == 5 and role == "SPECULATIVE_PLAY":
        role = "NOISE"

    return cls, role, pri
