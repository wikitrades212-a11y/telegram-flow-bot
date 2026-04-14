"""
Relative Strength engine and market state interpretation.

RS = pct_vs_vwap(ticker) - pct_vs_vwap(index)
where pct_vs_vwap = (price - vwap) / vwap * 100

Provides:
  1. Per-ticker RS classification: STRONG | NEUTRAL | WEAK | NO_DATA
  2. Market state: TREND_UP | TREND_DOWN | ROTATIONAL | CHOP | NO_DATA
  3. Input data for per-futures execution plan (NQ, ES, RTY, YM)
"""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

_RS_STRONG_THRESHOLD = 0.5    # % above index VWAP deviation
_RS_WEAK_THRESHOLD   = -0.5   # % below index VWAP deviation


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class TickerRS:
    ticker: str
    price: Optional[float]
    vwap: Optional[float]
    pct_vs_vwap: Optional[float]   # (price - vwap) / vwap * 100
    rs_vs_spy: Optional[float]
    rs_vs_qqq: Optional[float]
    classification: str             # STRONG | NEUTRAL | WEAK | NO_DATA
    data_ok: bool


@dataclass
class IndexRS:
    spy_above_vwap:  Optional[bool]
    qqq_above_vwap:  Optional[bool]
    iwm_above_vwap:  Optional[bool]
    spy_pct_vs_vwap: Optional[float]
    qqq_pct_vs_vwap: Optional[float]
    iwm_pct_vs_vwap: Optional[float]
    spy_price:  Optional[float]
    qqq_price:  Optional[float]
    iwm_price:  Optional[float]
    spy_vwap:   Optional[float]
    qqq_vwap:   Optional[float]
    iwm_vwap:   Optional[float]
    spy_pm_low: Optional[float]
    qqq_pm_low: Optional[float]
    iwm_pm_low: Optional[float]
    data_ok: bool = False


@dataclass
class MarketRS:
    indices: IndexRS
    tickers: dict = field(default_factory=dict)  # str → TickerRS
    market_state: str = "NO_DATA"               # TREND_UP | TREND_DOWN | ROTATIONAL | CHOP | NO_DATA
    data_ok: bool = False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pct_vs_vwap(price: Optional[float], vwap: Optional[float]) -> Optional[float]:
    if price is None or vwap is None or vwap == 0:
        return None
    return round((price - vwap) / vwap * 100, 3)


def _classify_rs(rs: Optional[float]) -> str:
    if rs is None:
        return "NO_DATA"
    if rs > _RS_STRONG_THRESHOLD:
        return "STRONG"
    if rs < _RS_WEAK_THRESHOLD:
        return "WEAK"
    return "NEUTRAL"


def _empty_indices() -> IndexRS:
    return IndexRS(
        spy_above_vwap=None,  qqq_above_vwap=None,  iwm_above_vwap=None,
        spy_pct_vs_vwap=None, qqq_pct_vs_vwap=None, iwm_pct_vs_vwap=None,
        spy_price=None,  qqq_price=None,  iwm_price=None,
        spy_vwap=None,   qqq_vwap=None,   iwm_vwap=None,
        spy_pm_low=None, qqq_pm_low=None, iwm_pm_low=None,
        data_ok=False,
    )


# ── Market state ──────────────────────────────────────────────────────────────

def derive_market_state(
    flow_direction: str,   # BULLISH | BEARISH | NEUTRAL
    flow_confidence: int,
    indices: IndexRS,
) -> str:
    if not indices.data_ok:
        return "NO_DATA"
    if flow_confidence < 20:
        return "CHOP"

    spy_up = indices.spy_above_vwap
    qqq_up = indices.qqq_above_vwap

    if spy_up is None and qqq_up is None:
        return "NO_DATA"

    indices_bullish = (spy_up is True)  and (qqq_up is True)
    indices_bearish = (spy_up is False) and (qqq_up is False)

    if not (indices_bullish or indices_bearish):
        return "ROTATIONAL"

    if flow_direction == "BULLISH" and indices_bullish:
        return "TREND_UP"
    if flow_direction == "BEARISH" and indices_bearish:
        return "TREND_DOWN"
    if flow_direction == "NEUTRAL":
        return "CHOP"

    # Flow direction opposes index VWAP alignment → divergence
    return "ROTATIONAL"


# ── Main compute function ─────────────────────────────────────────────────────

async def compute_rs(
    market,                             # MarketDataService instance
    flow_direction: str,
    flow_confidence: int,
    ticker_list: Optional[list] = None,
) -> MarketRS:
    """
    Fetch SPY, QQQ, IWM and optional individual tickers in parallel.
    Returns MarketRS. Never raises — data_ok=False on any failure.
    """
    base = ["SPY", "QQQ", "IWM"]
    extras = [t for t in (ticker_list or []) if t not in base][:5]  # cap extra fetches
    all_tickers = base + extras

    try:
        results = await asyncio.gather(
            *[market.snapshot(t) for t in all_tickers],
            return_exceptions=True,
        )
    except Exception as exc:
        logger.error("RS compute_rs gather failed: %s", exc)
        return MarketRS(indices=_empty_indices(), data_ok=False)

    snap_map: dict = {}
    for ticker, result in zip(all_tickers, results):
        if isinstance(result, Exception):
            logger.warning("RS snapshot failed for %s: %s", ticker, result)
        elif getattr(result, "fetch_ok", False):
            snap_map[ticker] = result

    def _val(t: str, attr: str):
        s = snap_map.get(t)
        return getattr(s, attr, None) if s else None

    spy_p, spy_v = _val("SPY", "price"), _val("SPY", "vwap")
    qqq_p, qqq_v = _val("QQQ", "price"), _val("QQQ", "vwap")
    iwm_p, iwm_v = _val("IWM", "price"), _val("IWM", "vwap")

    spy_pct = _pct_vs_vwap(spy_p, spy_v)
    qqq_pct = _pct_vs_vwap(qqq_p, qqq_v)
    iwm_pct = _pct_vs_vwap(iwm_p, iwm_v)

    def _above(p, v):
        if p is None or v is None:
            return None
        return p > v

    indices_ok = spy_p is not None or qqq_p is not None

    indices = IndexRS(
        spy_above_vwap  = _above(spy_p, spy_v),
        qqq_above_vwap  = _above(qqq_p, qqq_v),
        iwm_above_vwap  = _above(iwm_p, iwm_v),
        spy_pct_vs_vwap = spy_pct,
        qqq_pct_vs_vwap = qqq_pct,
        iwm_pct_vs_vwap = iwm_pct,
        spy_price  = spy_p,  qqq_price = qqq_p,  iwm_price = iwm_p,
        spy_vwap   = spy_v,  qqq_vwap  = qqq_v,  iwm_vwap  = iwm_v,
        spy_pm_low = _val("SPY", "pm_low"),
        qqq_pm_low = _val("QQQ", "pm_low"),
        iwm_pm_low = _val("IWM", "pm_low"),
        data_ok    = indices_ok,
    )

    # Per-ticker RS
    ticker_rs: dict = {}
    for t in extras:
        snap = snap_map.get(t)
        if not snap:
            ticker_rs[t] = TickerRS(t, None, None, None, None, None, "NO_DATA", False)
            continue
        pct    = _pct_vs_vwap(snap.price, snap.vwap)
        rs_spy = round(pct - spy_pct, 3) if (pct is not None and spy_pct is not None) else None
        rs_qqq = round(pct - qqq_pct, 3) if (pct is not None and qqq_pct is not None) else None
        vals   = [v for v in [rs_spy, rs_qqq] if v is not None]
        rs_avg = sum(vals) / len(vals) if vals else None
        ticker_rs[t] = TickerRS(
            ticker=t,
            price=snap.price,
            vwap=snap.vwap,
            pct_vs_vwap=pct,
            rs_vs_spy=rs_spy,
            rs_vs_qqq=rs_qqq,
            classification=_classify_rs(rs_avg),
            data_ok=True,
        )

    market_state = derive_market_state(flow_direction, flow_confidence, indices)

    logger.info(
        "RS computed | state=%s | SPY_vwap_pos=%s | QQQ_vwap_pos=%s | IWM_vwap_pos=%s",
        market_state,
        "ABOVE" if indices.spy_above_vwap else ("BELOW" if indices.spy_above_vwap is False else "N/A"),
        "ABOVE" if indices.qqq_above_vwap else ("BELOW" if indices.qqq_above_vwap is False else "N/A"),
        "ABOVE" if indices.iwm_above_vwap else ("BELOW" if indices.iwm_above_vwap is False else "N/A"),
    )

    return MarketRS(
        indices=indices,
        tickers=ticker_rs,
        market_state=market_state,
        data_ok=indices_ok,
    )
