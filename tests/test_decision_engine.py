"""
Unit tests for decision_engine.py — market data is mocked.
No network calls, no Telegram credentials needed.
"""
import sys, os, asyncio
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from unittest.mock import AsyncMock, MagicMock
from datetime import date

import config
# Pin TEST_MODE off so these tests exercise the full evaluation path
# regardless of what is set in .env during local development.
config.TEST_MODE = False

from app.parser import FlowSignal
from app.decision_engine import DecisionEngine, _hard_filter
from app.market_data import Snapshot


def _make_signal(**overrides):
    defaults = dict(
        raw_message="",
        ticker="TSLA",
        side="CALL",
        strike=250.0,
        expiration=date.today(),
        premium_usd=500_000,
        volume=3000,
        open_interest=200,
        vol_oi_ratio=15.0,
        delta=0.45,
        iv_pct=30.0,
        dte=7,
        score=90,
        conviction="A",
        direction="BULLISH",
    )
    defaults.update(overrides)
    return FlowSignal(**defaults)


def _snap(ticker, price, vwap, pm_high=None, pm_low=None):
    return Snapshot(ticker, price, vwap, pm_high, pm_low)


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ── Hard filter tests ─────────────────────────────────────────────────────────

def test_hard_filter_pass():
    assert _hard_filter(_make_signal()) is None

def test_hard_filter_low_score():
    reason = _hard_filter(_make_signal(score=74))
    assert reason is not None and "score" in reason

def test_hard_filter_bad_conviction():
    reason = _hard_filter(_make_signal(conviction="B"))
    assert reason is not None and "conviction" in reason

def test_hard_filter_low_voiratio():
    reason = _hard_filter(_make_signal(vol_oi_ratio=4.9))
    assert reason is not None and "vol/oi" in reason

def test_hard_filter_dte_too_high():
    reason = _hard_filter(_make_signal(dte=15))
    assert reason is not None and "dte" in reason


# ── Engine integration tests ──────────────────────────────────────────────────

def _mock_market(ticker, price, vwap, pm_high=None, pm_low=None):
    market = MagicMock()
    spy = _snap("SPY", price * 0.99, price * 0.98)   # slightly below vwap for neutral
    qqq = _snap("QQQ", price * 0.99, price * 0.98)
    target = _snap(ticker, price, vwap, pm_high, pm_low)
    market.context = AsyncMock(return_value={"SPY": spy, "QQQ": qqq, ticker: target})
    return market


def test_engine_kill():
    sig = _make_signal(score=50)
    engine = DecisionEngine(_mock_market("TSLA", 260, 255))
    dec = run(engine.evaluate(sig))
    assert dec.verdict == "KILL"


def test_engine_go_call_above_pm_high():
    sig = _make_signal(side="CALL")
    # price=260 > pm_high=255 → immediate GO
    market = _mock_market("TSLA", 260, 255, pm_high=255, pm_low=245)
    # Override SPY/QQQ to be above VWAP too
    spy = _snap("SPY", 510, 500)
    qqq = _snap("QQQ", 450, 440)
    tsla = _snap("TSLA", 260, 255, pm_high=255, pm_low=245)
    market.context = AsyncMock(return_value={"SPY": spy, "QQQ": qqq, "TSLA": tsla})
    engine = DecisionEngine(market)
    dec = run(engine.evaluate(sig))
    assert dec.verdict == "GO"
    assert dec.entry == 260


def test_engine_hold_not_aligned():
    sig = _make_signal(side="CALL")
    # All prices below VWAP → not aligned for CALL
    spy = _snap("SPY", 490, 510)
    qqq = _snap("QQQ", 430, 450)
    tsla = _snap("TSLA", 240, 260, pm_high=265, pm_low=235)
    market = MagicMock()
    market.context = AsyncMock(return_value={"SPY": spy, "QQQ": qqq, "TSLA": tsla})
    engine = DecisionEngine(market)
    dec = run(engine.evaluate(sig))
    assert dec.verdict == "HOLD"
    assert "aligned" in dec.reason


if __name__ == "__main__":
    test_hard_filter_pass();       print("hard_filter_pass         PASSED")
    test_hard_filter_low_score();  print("hard_filter_low_score    PASSED")
    test_hard_filter_bad_conviction(); print("hard_filter_conviction   PASSED")
    test_hard_filter_low_voiratio();   print("hard_filter_voiratio     PASSED")
    test_hard_filter_dte_too_high();   print("hard_filter_dte          PASSED")
    test_engine_kill();            print("engine_kill              PASSED")
    test_engine_go_call_above_pm_high(); print("engine_go_call_pm_high   PASSED")
    test_engine_hold_not_aligned(); print("engine_hold_not_aligned  PASSED")
    print("\nAll tests passed.")
