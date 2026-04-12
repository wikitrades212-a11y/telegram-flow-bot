"""
Unit tests for price trigger logic (_check_price_trigger).

All 6 required scenarios. No live API calls — all data is injected directly.

Scenarios
---------
1. PM High breakout → GO
2. PM Low breakdown → GO
3. VWAP reclaim with 1 candle above VWAP → HOLD (not enough confirmation)
4. VWAP reclaim with 2 candles above VWAP → GO
5. VWAP rejection with 1 candle below VWAP → HOLD (not enough confirmation)
6. VWAP rejection with 2 candles below VWAP → GO
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app.market_data import Snapshot, CandleBuffer
from app.decision_engine import _check_price_trigger


# ── Test helpers ──────────────────────────────────────────────────────────────

class _Sig:
    """Minimal signal stub — only .side and .ticker are used by _check_price_trigger."""
    def __init__(self, side: str, ticker: str = "TSLA"):
        self.side = side
        self.ticker = ticker


def _snap(ticker, price, vwap, pm_high=None, pm_low=None) -> Snapshot:
    return Snapshot(ticker, price, vwap, pm_high, pm_low)


def _buf(ticker: str, closes: list[float]) -> CandleBuffer:
    """Build a CandleBuffer with pre-loaded close data for ticker."""
    buf = CandleBuffer(n=10)
    buf._data[ticker] = closes
    return buf


# ── 1. PM High breakout → GO ──────────────────────────────────────────────────

def test_pm_high_break_triggers_go():
    """Price strictly above premarket high fires GO immediately."""
    sig = _Sig("CALL")
    # price=259 > pm_high=258 → Rule 1 fires
    snap = _snap("TSLA", price=259.0, vwap=255.0, pm_high=258.0, pm_low=248.0)
    buf = _buf("TSLA", [252.0, 253.0])    # candle data irrelevant for this path

    triggered, reason = _check_price_trigger(sig, snap, buf)

    assert triggered is True, "PM High breakout should trigger GO"
    assert reason == "PM High Break"


# ── 2. PM Low breakdown → GO ──────────────────────────────────────────────────

def test_pm_low_break_triggers_go():
    """Price strictly below premarket low fires GO immediately."""
    sig = _Sig("PUT")
    # price=249 < pm_low=250 → Rule 1 fires
    snap = _snap("TSLA", price=249.0, vwap=255.0, pm_high=262.0, pm_low=250.0)
    buf = _buf("TSLA", [256.0, 257.0])

    triggered, reason = _check_price_trigger(sig, snap, buf)

    assert triggered is True, "PM Low breakdown should trigger GO"
    assert reason == "PM Low Break"


# ── 3. VWAP reclaim — 1 closed candle above → HOLD ───────────────────────────

def test_vwap_reclaim_one_candle_is_hold():
    """
    Only the most recent closed candle is above VWAP.
    The candle before it is below VWAP. Two consecutive closes required — HOLD.
    """
    sig = _Sig("CALL")
    # price < pm_high → Rule 1 does NOT fire
    snap = _snap("TSLA", price=256.0, vwap=255.0, pm_high=265.0, pm_low=248.0)
    # Last 2 closes: [253.0, 256.5] — only 256.5 > 255; 253.0 < 255
    buf = _buf("TSLA", [250.0, 253.0, 256.5])

    triggered, reason = _check_price_trigger(sig, snap, buf)

    assert triggered is False, "1 candle above VWAP should NOT trigger GO"
    assert reason == ""


# ── 4. VWAP reclaim — 2 closed candles above → GO ────────────────────────────

def test_vwap_reclaim_two_candles_triggers_go():
    """Both of the last 2 closed candles have CLOSE > VWAP → GO."""
    sig = _Sig("CALL")
    # price < pm_high → Rule 1 does NOT fire
    snap = _snap("TSLA", price=257.0, vwap=255.0, pm_high=265.0, pm_low=248.0)
    # Last 2 closes: [256.5, 257.0] — both > 255
    buf = _buf("TSLA", [252.0, 256.5, 257.0])

    triggered, reason = _check_price_trigger(sig, snap, buf)

    assert triggered is True, "2 consecutive closes above VWAP should trigger GO"
    assert reason == "VWAP Reclaim (2 Close Confirm)"


# ── 5. VWAP rejection — 1 closed candle below → HOLD ─────────────────────────

def test_vwap_reject_one_candle_is_hold():
    """
    Only the most recent closed candle is below VWAP.
    The candle before it is above VWAP. Two consecutive closes required — HOLD.
    """
    sig = _Sig("PUT")
    # price > pm_low → Rule 1 does NOT fire
    snap = _snap("TSLA", price=253.0, vwap=255.0, pm_high=262.0, pm_low=245.0)
    # Last 2 closes: [257.0, 253.5] — only 253.5 < 255; 257.0 > 255
    buf = _buf("TSLA", [259.0, 257.0, 253.5])

    triggered, reason = _check_price_trigger(sig, snap, buf)

    assert triggered is False, "1 candle below VWAP should NOT trigger GO"
    assert reason == ""


# ── 6. VWAP rejection — 2 closed candles below → GO ──────────────────────────

def test_vwap_reject_two_candles_triggers_go():
    """Both of the last 2 closed candles have CLOSE < VWAP → GO."""
    sig = _Sig("PUT")
    # price > pm_low → Rule 1 does NOT fire
    snap = _snap("TSLA", price=253.0, vwap=255.0, pm_high=262.0, pm_low=245.0)
    # Last 2 closes: [254.5, 253.0] — both < 255
    buf = _buf("TSLA", [258.0, 254.5, 253.0])

    triggered, reason = _check_price_trigger(sig, snap, buf)

    assert triggered is True, "2 consecutive closes below VWAP should trigger GO"
    assert reason == "VWAP Reject (2 Close Confirm)"


# ── Edge cases ────────────────────────────────────────────────────────────────

def test_insufficient_candle_data_does_not_trigger():
    """Buffer has fewer than 2 closes — VWAP rule must not fire."""
    sig = _Sig("CALL")
    snap = _snap("TSLA", price=256.0, vwap=255.0, pm_high=265.0, pm_low=248.0)
    buf = _buf("TSLA", [256.5])   # only 1 close — not enough for 2-candle rule

    triggered, _ = _check_price_trigger(sig, snap, buf)

    assert triggered is False, "Should not trigger with only 1 close in buffer"


def test_price_at_pm_high_does_not_trigger():
    """Price exactly AT premarket high is not a breakout (must be strictly above)."""
    sig = _Sig("CALL")
    snap = _snap("TSLA", price=258.0, vwap=255.0, pm_high=258.0, pm_low=248.0)
    buf = _buf("TSLA", [252.0, 253.0])

    triggered, _ = _check_price_trigger(sig, snap, buf)

    assert triggered is False, "Price == PM High is not a break; must be strictly above"


def test_price_at_pm_low_does_not_trigger():
    """Price exactly AT premarket low is not a breakdown (must be strictly below)."""
    sig = _Sig("PUT")
    snap = _snap("TSLA", price=250.0, vwap=255.0, pm_high=262.0, pm_low=250.0)
    buf = _buf("TSLA", [256.0, 257.0])

    triggered, _ = _check_price_trigger(sig, snap, buf)

    assert triggered is False, "Price == PM Low is not a break; must be strictly below"


# ── Runner ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_pm_high_break_triggers_go,
        test_pm_low_break_triggers_go,
        test_vwap_reclaim_one_candle_is_hold,
        test_vwap_reclaim_two_candles_triggers_go,
        test_vwap_reject_one_candle_is_hold,
        test_vwap_reject_two_candles_triggers_go,
        test_insufficient_candle_data_does_not_trigger,
        test_price_at_pm_high_does_not_trigger,
        test_price_at_pm_low_does_not_trigger,
    ]
    for t in tests:
        t()
        print(f"  {t.__name__}  PASSED")
    print(f"\n{len(tests)}/{len(tests)} tests passed.")
