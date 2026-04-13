"""
Decision engine: hard filter → market context → price trigger.

Possible verdicts:
  KILL  — does not pass hard filters; never posted
  HOLD  — passes filters but no trigger yet; posted once, then watched
  GO    — trigger confirmed; posted with entry/stop/target
"""

import logging
from dataclasses import dataclass
from typing import Optional
from app.parser import FlowSignal
from app.market_data import MarketDataService, Snapshot, CandleBuffer
import config

logger = logging.getLogger(__name__)


@dataclass
class Decision:
    signal_id: str
    verdict: str                        # "KILL" | "HOLD" | "GO"
    reason: str
    trigger_reason: Optional[str] = None  # e.g. "PM High Break", "VWAP Reclaim (2 Close Confirm)"
    entry: Optional[float] = None
    stop: Optional[float] = None
    target: Optional[float] = None
    # Market levels at decision time
    vwap: Optional[float] = None
    pm_high: Optional[float] = None
    pm_low: Optional[float] = None
    price: Optional[float] = None


# ── Hard filter ───────────────────────────────────────────────────────────────

def _hard_filter(sig: FlowSignal) -> Optional[str]:
    """Return a kill reason string, or None if the signal passes."""
    if sig.score < config.MIN_SCORE:
        return f"score {sig.score} < {config.MIN_SCORE}"
    if sig.conviction != config.REQUIRED_CONVICTION:
        return f"conviction '{sig.conviction}' != '{config.REQUIRED_CONVICTION}'"
    if sig.vol_oi_ratio < config.MIN_VOL_OI:
        return f"vol/oi {sig.vol_oi_ratio:.1f} < {config.MIN_VOL_OI}"
    if sig.dte > config.MAX_DTE:
        return f"dte {sig.dte} > {config.MAX_DTE}"
    if sig.premium_usd < 50_000:
        return f"premium ${sig.premium_usd:,.0f} below minimum"
    if sig.delta is not None and abs(sig.delta) < 0.10:
        return f"delta {sig.delta:.2f} too far OTM"
    if sig.side == "CALL" and sig.delta is not None and sig.delta < 0:
        return f"delta {sig.delta:.2f} contradicts CALL side"
    if sig.side == "PUT" and sig.delta is not None and sig.delta > 0:
        return f"delta {sig.delta:.2f} contradicts PUT side"
    return None


# ── Market alignment ──────────────────────────────────────────────────────────

def _market_aligned(sig: FlowSignal, ctx: dict[str, Snapshot]) -> Optional[bool]:
    """
    CALL: SPY OR QQQ OR ticker price > VWAP
    PUT:  SPY OR QQQ OR ticker price < VWAP

    Returns:
      True  — at least one instrument is aligned with flow direction
      False — data is available but no instrument is aligned
      None  — no usable market data (all fetches failed / rate-limited)
    """
    any_data = False
    for sym in ("SPY", "QQQ", sig.ticker):
        snap = ctx.get(sym)
        if snap is None or not snap.fetch_ok:
            continue
        if snap.price is None or snap.vwap is None:
            continue
        any_data = True
        result = snap.above_vwap() if sig.side == "CALL" else snap.below_vwap()
        if result:
            return True

    if not any_data:
        return None   # distinguish from False: no data, not misaligned
    return False


# ── Price trigger ─────────────────────────────────────────────────────────────

def _check_price_trigger(
    sig: FlowSignal,
    snap: Snapshot,
    candles: CandleBuffer,
) -> tuple[bool, str]:
    """
    Evaluate exact trigger rules. Returns (triggered, reason_label).

    CALL FLOW
    ---------
    Rule 1: price strictly breaks above premarket high  → "PM High Break"
    Rule 2: last 2 *closed* 1-minute candle CLOSEs are both above VWAP
            → "VWAP Reclaim (2 Close Confirm)"

    PUT FLOW
    --------
    Rule 1: price strictly breaks below premarket low   → "PM Low Break"
    Rule 2: last 2 *closed* 1-minute candle CLOSEs are both below VWAP
            → "VWAP Reject (2 Close Confirm)"

    No intrabar triggers — candle close confirmation is evaluated strictly on
    closed bar CLOSE prices supplied by CandleBuffer, never on the live price.
    """
    price = snap.price

    if sig.side == "CALL":
        # Rule 1 — PM High breakout (uses live price, not candle close,
        # because a print above PM High is itself a confirmed breakout tick)
        if price is not None and snap.pm_high is not None and price > snap.pm_high:
            return True, "PM High Break"

        # Rule 2 — VWAP reclaim: 2 consecutive closed candles above VWAP
        if snap.vwap is not None:
            last2 = candles.last_closes(sig.ticker, 2)
            if len(last2) == 2 and all(c > snap.vwap for c in last2):
                return True, "VWAP Reclaim (2 Close Confirm)"

    else:  # PUT
        # Rule 1 — PM Low breakdown
        if price is not None and snap.pm_low is not None and price < snap.pm_low:
            return True, "PM Low Break"

        # Rule 2 — VWAP rejection: 2 consecutive closed candles below VWAP
        if snap.vwap is not None:
            last2 = candles.last_closes(sig.ticker, 2)
            if len(last2) == 2 and all(c < snap.vwap for c in last2):
                return True, "VWAP Reject (2 Close Confirm)"

    return False, ""


# ── Engine ────────────────────────────────────────────────────────────────────

class DecisionEngine:
    def __init__(self, market: MarketDataService):
        self.market = market

    async def evaluate(self, sig: FlowSignal) -> Decision:
        # 1. Hard filter
        kill_reason = _hard_filter(sig)
        if kill_reason:
            logger.info("KILL [%s]: %s", sig.signal_id, kill_reason)
            return Decision(signal_id=sig.signal_id, verdict="KILL", reason=kill_reason)

        # TEST_MODE — bypass all market data, alignment, and trigger checks
        if config.TEST_MODE:
            logger.info("TEST_MODE [%s]: hard filter passed — returning HOLD bypass", sig.signal_id)
            return Decision(
                signal_id=sig.signal_id,
                verdict="HOLD",
                reason="test mode bypass",
            )

        # 2. Fetch market context (also refreshes candle buffer as a side-effect)
        try:
            ctx = await self.market.context(sig.ticker)
        except Exception as exc:
            logger.error("Market data error for %s: %s", sig.ticker, exc, exc_info=True)
            return Decision(
                signal_id=sig.signal_id,
                verdict="HOLD",
                reason="market data unavailable — retrying",
            )

        ticker_snap: Snapshot = ctx.get(sig.ticker, Snapshot(sig.ticker, None, None, None, None, fetch_ok=False))

        # 3. Market alignment check
        aligned = _market_aligned(sig, ctx)

        if aligned is None:
            logger.warning(
                "HOLD [%s]: market data unavailable (SPY/QQQ/%s all failed — rate-limited?)",
                sig.signal_id, sig.ticker,
            )
            return Decision(
                signal_id=sig.signal_id,
                verdict="HOLD",
                reason="market data unavailable",
                vwap=ticker_snap.vwap,
                pm_high=ticker_snap.pm_high,
                pm_low=ticker_snap.pm_low,
                price=ticker_snap.price,
            )

        if not aligned:
            logger.info(
                "HOLD [%s]: market not aligned — %s flow but no SPY/QQQ/%s above/below VWAP",
                sig.signal_id, sig.side, sig.ticker,
            )
            return Decision(
                signal_id=sig.signal_id,
                verdict="HOLD",
                reason="market not aligned with flow direction",
                vwap=ticker_snap.vwap,
                pm_high=ticker_snap.pm_high,
                pm_low=ticker_snap.pm_low,
                price=ticker_snap.price,
            )

        # 4. Price trigger (candle-close confirmed)
        triggered, trigger_reason = _check_price_trigger(sig, ticker_snap, self.market.candles)
        if triggered:
            logger.info("GO  [%s]: %s (price=%.4f)", sig.signal_id, trigger_reason, ticker_snap.price or 0)
            return Decision(
                signal_id=sig.signal_id,
                verdict="GO",
                reason="price trigger confirmed",
                trigger_reason=trigger_reason,
                entry=ticker_snap.price,
                vwap=ticker_snap.vwap,
                pm_high=ticker_snap.pm_high,
                pm_low=ticker_snap.pm_low,
                price=ticker_snap.price,
            )

        logger.info(
            "HOLD [%s]: aligned but no trigger yet (price=%.4f vwap=%.4f pm_high=%.4f pm_low=%.4f)",
            sig.signal_id,
            ticker_snap.price or 0,
            ticker_snap.vwap or 0,
            ticker_snap.pm_high or 0,
            ticker_snap.pm_low or 0,
        )
        return Decision(
            signal_id=sig.signal_id,
            verdict="HOLD",
            reason="awaiting price trigger",
            vwap=ticker_snap.vwap,
            pm_high=ticker_snap.pm_high,
            pm_low=ticker_snap.pm_low,
            price=ticker_snap.price,
        )
