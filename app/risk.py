"""
Compute entry / stop / target (2R) for GO decisions.

Entry  = trigger price (current price at the moment GO fires)
Stop   = premarket low  (for CALL)  |  premarket high (for PUT)
         Fallback: 1% of entry if premarket level unavailable
Target = entry + 2 * risk  (for CALL)
         entry - 2 * risk  (for PUT)
"""

import logging
from app.parser import FlowSignal
from app.decision_engine import Decision

logger = logging.getLogger(__name__)

_FALLBACK_RISK_PCT = 0.01    # 1 % of entry price


def compute_targets(sig: FlowSignal, decision: Decision) -> Decision:
    """Mutates and returns the Decision with stop/target filled in."""
    if decision.verdict != "GO" or decision.entry is None:
        return decision

    entry = decision.entry

    if sig.side == "CALL":
        stop_anchor = decision.pm_low
    else:
        stop_anchor = decision.pm_high

    if stop_anchor is None:
        # Fallback: 1 % below / above entry
        logger.warning(
            "[%s] No premarket level available for stop — using 1%% fallback", sig.signal_id
        )
        stop_anchor = entry * (1 - _FALLBACK_RISK_PCT) if sig.side == "CALL" else entry * (1 + _FALLBACK_RISK_PCT)

    risk = abs(entry - stop_anchor)
    if risk < 0.001:
        risk = entry * _FALLBACK_RISK_PCT    # avoid zero-risk degenerate case

    if sig.side == "CALL":
        decision.stop = round(stop_anchor, 2)
        decision.target = round(entry + 2 * risk, 2)
    else:
        decision.stop = round(stop_anchor, 2)
        decision.target = round(entry - 2 * risk, 2)

    logger.debug(
        "[%s] Risk params — entry=%.2f stop=%.2f target=%.2f (risk=%.2f)",
        sig.signal_id, entry, decision.stop, decision.target, risk,
    )
    return decision
