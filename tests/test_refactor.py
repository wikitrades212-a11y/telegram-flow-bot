"""
Integration tests for the v2 refactor:
  - session.py
  - hedge_detector.py
  - bot_data.py
  - batch.py (hedging + leaders/drags)
  - telegram_handler.py (BOT_DATA block injection)

Run: python3 tests/test_refactor.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import datetime
import pytz

from app.session import (
    current_session, baseline_data_quality, degrade_data_quality,
    flow_signals_enabled, signals_actionable,
    SESSION_PREMARKET, SESSION_RTH, SESSION_AFTER_HOURS, SESSION_CLOSED,
)
from app.hedge_detector import classify_hedge, is_hedging, HEDGE_TYPE_HEDGE, HEDGE_TYPE_DIRECTIONAL, HEDGE_TYPE_PROBABLE
from app.bot_data import build_bot_data, render_bot_data, query_bias, query_leaders, query_hedge, query_playbook
from app.batch import BatchStore, BatchEntry

ET = pytz.timezone("America/New_York")

_passed = []
_failed = []


def _test(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        _passed.append(name)
        print(f"  PASS  {name}")
    else:
        _failed.append(name)
        print(f"  FAIL  {name}" + (f" — {detail}" if detail else ""))


# ─────────────────────────────────────────────────────────────────────────────
# 1. Session detection
# ─────────────────────────────────────────────────────────────────────────────

print("\n── Session detection ──")

_test("premarket at 05:30",
      current_session(datetime(2026, 4, 15, 5, 30, tzinfo=ET)) == SESSION_PREMARKET)

_test("RTH at 10:00",
      current_session(datetime(2026, 4, 15, 10, 0, tzinfo=ET)) == SESSION_RTH)

_test("RTH edge 09:30",
      current_session(datetime(2026, 4, 15, 9, 30, tzinfo=ET)) == SESSION_RTH)

_test("premarket edge 09:29",
      current_session(datetime(2026, 4, 15, 9, 29, tzinfo=ET)) == SESSION_PREMARKET)

_test("after-hours at 17:00",
      current_session(datetime(2026, 4, 15, 17, 0, tzinfo=ET)) == SESSION_AFTER_HOURS)

_test("closed on Saturday",
      current_session(datetime(2026, 4, 12, 10, 0, tzinfo=ET)) == SESSION_CLOSED)

_test("closed at midnight",
      current_session(datetime(2026, 4, 15, 0, 30, tzinfo=ET)) == SESSION_CLOSED)

_test("flow_signals_enabled RTH only",
      flow_signals_enabled(SESSION_RTH) and
      not flow_signals_enabled(SESSION_PREMARKET) and
      not flow_signals_enabled(SESSION_AFTER_HOURS))

_test("baseline quality RTH=HIGH PM=MEDIUM AH=LOW",
      baseline_data_quality(SESSION_RTH) == "HIGH" and
      baseline_data_quality(SESSION_PREMARKET) == "MEDIUM" and
      baseline_data_quality(SESSION_AFTER_HOURS) == "LOW")

_test("degrade HIGH→MEDIUM when alpaca fails",
      degrade_data_quality("HIGH", alpaca_ok=False, tradier_ok=True) == "MEDIUM")

_test("degrade HIGH→MEDIUM when tradier fails",
      degrade_data_quality("HIGH", alpaca_ok=True, tradier_ok=False) == "MEDIUM")

_test("degrade HIGH stays HIGH when both ok",
      degrade_data_quality("HIGH", alpaca_ok=True, tradier_ok=True) == "HIGH")

_test("degrade MEDIUM→LOW when alpaca fails",
      degrade_data_quality("MEDIUM", alpaca_ok=False, tradier_ok=True) == "LOW")

_test("signals_actionable LOW=False HIGH=True",
      not signals_actionable("LOW") and signals_actionable("HIGH") and signals_actionable("MEDIUM"))


# ─────────────────────────────────────────────────────────────────────────────
# 2. Hedge detector
# ─────────────────────────────────────────────────────────────────────────────

print("\n── Hedge detector ──")

# Classic hedge: PUT while bullish, near-money delta, elevated vol/oi, big premium
r = classify_hedge("PUT", delta=-0.50, vol_oi_ratio=3.5, premium_usd=500_000, market_direction="BULLISH")
_test("full hedge (all 3 criteria)", r.hedge_type == HEDGE_TYPE_HEDGE, f"got {r.hedge_type}")
_test("full hedge score==3", r.score == 3)

# Directional CALL aligned with BULLISH — counter_trend=False → score ≤ 2
r2 = classify_hedge("CALL", delta=0.52, vol_oi_ratio=8.0, premium_usd=300_000, market_direction="BULLISH")
_test("directional call not HEDGE", r2.hedge_type != HEDGE_TYPE_HEDGE)

# Small premium → always DIRECTIONAL regardless of other flags
r3 = classify_hedge("PUT", delta=-0.48, vol_oi_ratio=5.0, premium_usd=5_000, market_direction="BULLISH")
_test("small premium → DIRECTIONAL", r3.hedge_type == HEDGE_TYPE_DIRECTIONAL)

# NEUTRAL market direction — counter_trend always False
r4 = classify_hedge("PUT", delta=-0.50, vol_oi_ratio=3.5, premium_usd=500_000, market_direction="NEUTRAL")
_test("neutral market: counter_trend=False", not r4.counter_trend_flag)
_test("neutral market: score ≤ 2 → not HEDGE", r4.hedge_type in (HEDGE_TYPE_PROBABLE, HEDGE_TYPE_DIRECTIONAL))

# is_hedging batch
class _FakeEntry:
    def __init__(self, side, delta, voi, premium):
        self.side, self.delta, self.vol_oi_ratio, self.premium_usd = side, delta, voi, premium

_test("is_hedging detects PUT hedge in BULLISH batch",
      is_hedging([_FakeEntry("PUT", -0.50, 3.5, 500_000),
                  _FakeEntry("CALL", 0.55, 6.0, 200_000)], "BULLISH"))

_test("is_hedging returns False for purely directional batch",
      not is_hedging([_FakeEntry("CALL", 0.55, 6.0, 200_000),
                      _FakeEntry("CALL", 0.48, 4.0, 150_000)], "BULLISH"))


# ─────────────────────────────────────────────────────────────────────────────
# 3. BotDataBlock
# ─────────────────────────────────────────────────────────────────────────────

print("\n── BotDataBlock ──")

block = build_bot_data(
    bias="BULLISH",
    hedging=True,
    confidence=75,
    regime_raw="BROAD TREND UP",
    primary_futures="NQ",
    secondary_futures="ES",
    leaders=["NVDA", "AMD", "MSFT"],
    drags=["TSLA", "AMZN"],
    session="RTH",
    data_quality="HIGH",
    macro_override=True,
    qqq_vwap=629.83,
    qqq_price=631.20,
    qqq_pm_high=630.50,
    spy_vwap=695.73,
    spy_price=696.40,
)
rendered = render_bot_data(block)

_test("BOT_DATA opens with [BOT_DATA]",  rendered.startswith("[BOT_DATA]"))
_test("BOT_DATA closes with [/BOT_DATA]", rendered.strip().endswith("[/BOT_DATA]"))
_test("BIAS=BULLISH present",      "BIAS=BULLISH" in rendered)
_test("HEDGING=TRUE present",      "HEDGING=TRUE" in rendered)
_test("MACRO_OVERRIDE=TRUE",       "MACRO_OVERRIDE=TRUE" in rendered)
_test("CONFIDENCE=75",             "CONFIDENCE=75" in rendered)
_test("REGIME=TREND_UP",           "REGIME=TREND_UP" in rendered)
_test("PRIMARY=NQ",                "PRIMARY=NQ" in rendered)
_test("SECONDARY=ES",              "SECONDARY=ES" in rendered)
_test("LEADERS=NVDA,AMD,MSFT",     "LEADERS=NVDA,AMD,MSFT" in rendered)
_test("DRAGS=TSLA,AMZN",           "DRAGS=TSLA,AMZN" in rendered)
_test("SESSION=RTH",               "SESSION=RTH" in rendered)
_test("DATA_QUALITY=HIGH",         "DATA_QUALITY=HIGH" in rendered)
_test("QQQ_VWAP numeric",          "QQQ_VWAP=629.83" in rendered)
_test("QQQ_STOP numeric (derived)", "QQQ_STOP=" in rendered and "N/A" not in rendered.split("QQQ_STOP=")[1].split("\n")[0])
_test("SPY_VWAP numeric",          "SPY_VWAP=695.73" in rendered)
_test("PLAYBOOK present and non-empty", "PLAYBOOK=" in rendered)
_test("ABOVE_PM_HIGH tag detected", block.qqq_vwap_tag == "ABOVE_PM_HIGH")

# Exactly 16 key=value lines between the tags
inner_lines = [l for l in rendered.splitlines() if "=" in l and not l.startswith("[")]
_test("exactly 16 key=value fields", len(inner_lines) == 16, f"got {len(inner_lines)}")

# No commentary / extra text inside block
_test("no lowercase inside block",
      all(line == line.upper() or "." in line for line in inner_lines))

# LOW data quality → no actionable
block_low = build_bot_data(
    bias="BEARISH", hedging=False, confidence=50,
    regime_raw="NO_DATA", primary_futures="NONE", secondary_futures="NONE",
    leaders=[], drags=[], session="PREMARKET", data_quality="LOW",
)
_test("LOW data_quality → PLAYBOOK=NO_TRADE", block_low.playbook == "NO_TRADE")

# Query helpers
_test("query_bias includes hedging note", "with active hedging" in query_bias(block))
_test("query_leaders returns tickers",    "NVDA" in query_leaders(block))
_test("query_hedge TRUE message",         "TRUE" in query_hedge(block))
_test("query_playbook includes regime",   "TREND_UP" in query_playbook(block))


# ─────────────────────────────────────────────────────────────────────────────
# 4. Batch: hedging + leaders/drags
# ─────────────────────────────────────────────────────────────────────────────

print("\n── Batch analysis (hedging + leaders/drags) ──")


def _make_entry(ticker, side, premium, cls, role, pri, delta=0.0, voi=2.0):
    class _Sig:
        pass
    s = _Sig()
    s.signal_id = f"{ticker}_{side}"
    s.ticker = ticker; s.side = side; s.premium_usd = premium
    s.score = 85; s.strike = 500.0; s.iv_pct = 35.0
    s.vol_oi_ratio = voi; s.delta = delta; s.dte = 5
    s.direction = "BULLISH" if side == "CALL" else "BEARISH"
    return s


store = BatchStore(trigger_count=3)
store.add(_make_entry("QQQ", "CALL", 800_000, "POSITIONAL_BULL", "MARKET_SIGNAL", 1, 0.55, 4.5), "POSITIONAL_BULL", "MARKET_SIGNAL", 1, "HOLD")
store.add(_make_entry("NVDA","CALL", 350_000, "SPECULATIVE_DIRECTIONAL","SPECULATIVE_PLAY", 2, 0.48, 6.1), "SPECULATIVE_DIRECTIONAL","SPECULATIVE_PLAY",2,"GO")
store.add(_make_entry("SPY", "PUT",  250_000, "HEDGE_DIRECTIONAL","MARKET_SIGNAL", 1, -0.45, 3.2), "HEDGE_DIRECTIONAL","MARKET_SIGNAL",1,"HOLD")

analysis = store.analyze_peek()

_test("analysis has 'hedging' key",     "hedging" in analysis)
_test("hedging=True (SPY PUT detected)", analysis["hedging"] is True)
_test("analysis has 'leaders' key",     "leaders" in analysis)
_test("NVDA in leaders",                "NVDA" in analysis["leaders"])
_test("analysis has 'drags' key",       "drags" in analysis)
_test("analysis has 'macro_override'",  "macro_override" in analysis)
_test("macro_override=True (QQQ+SPY)",  analysis["macro_override"] is True)


# ─────────────────────────────────────────────────────────────────────────────
# 5. format_channel_b_report injects BOT_DATA
# ─────────────────────────────────────────────────────────────────────────────

print("\n── format_channel_b_report BOT_DATA injection ──")

from app.telegram_handler import format_channel_b_report, format_premarket_report

report = format_channel_b_report(analysis)

_test("report contains [BOT_DATA]",   "[BOT_DATA]" in report)
_test("report contains [/BOT_DATA]",  "[/BOT_DATA]" in report)
_test("HEDGING=TRUE in report",       "HEDGING=TRUE" in report)
_test("SESSION= present in report",   "SESSION=" in report)
_test("DATA_QUALITY= present",        "DATA_QUALITY=" in report)
_test("PLAYBOOK= present",            "PLAYBOOK=" in report)

# Human section still present
_test("human section: MARKET BIAS present", "MARKET BIAS" in report)
_test("human section: Game Plan present",   "Game Plan" in report)

pm_report = format_premarket_report(None)
_test("premarket report has BOT_DATA",    "[BOT_DATA]" in pm_report)
_test("premarket DATA_QUALITY=MEDIUM",    "DATA_QUALITY=MEDIUM" in pm_report)
_test("premarket human section present",  "PRE-MARKET BIAS REPORT" in pm_report)


# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────

print(f"\n{'─'*50}")
print(f"Results: {len(_passed)} passed, {len(_failed)} failed")
if _failed:
    print("FAILED tests:")
    for f in _failed:
        print(f"  • {f}")
    sys.exit(1)
else:
    print("All tests passed.")
