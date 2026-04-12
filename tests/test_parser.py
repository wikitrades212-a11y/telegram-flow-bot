"""Tests for parser.py — no external deps required."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from app.parser import parse_flow_message

SAMPLE = """🔴 GOOGL $318P  13 Apr [ATM]
💰 $653K  ·  Vol 2,974 / OI 139 (21.4x)
Δ -0.53  IV 19.6%  DTE 3
Score 100  ·  Conviction A (88)
📈 BEARISH AGGRESSIVE"""

def test_full_parse():
    sig = parse_flow_message(SAMPLE, message_id=42)
    assert sig is not None,            "Should parse successfully"
    assert sig.ticker == "GOOGL"
    assert sig.side == "PUT"
    assert sig.strike == 318.0
    assert sig.premium_usd == 653_000
    assert sig.volume == 2974
    assert sig.open_interest == 139
    assert abs(sig.vol_oi_ratio - 21.4) < 0.01
    assert abs(sig.delta - (-0.53)) < 0.001
    assert abs(sig.iv_pct - 19.6) < 0.01
    assert sig.dte == 3
    assert sig.score == 100
    assert sig.conviction == "A"
    assert sig.direction == "BEARISH"
    assert sig.message_id == 42
    assert "GOOGL_PUT_318.0" in sig.signal_id

def test_call_parse():
    msg = """🟢 AAPL $200C  20 Jun [OTM]
💰 $1.2M  ·  Vol 5,000 / OI 250 (20.0x)
Δ 0.42  IV 22.1%  DTE 10
Score 88  ·  Conviction A (91)
📈 BULLISH AGGRESSIVE"""
    sig = parse_flow_message(msg)
    assert sig is not None
    assert sig.side == "CALL"
    assert sig.ticker == "AAPL"
    assert sig.premium_usd == 1_200_000
    assert sig.direction == "BULLISH"

def test_bad_message_returns_none():
    assert parse_flow_message("Hello world") is None
    assert parse_flow_message("") is None
    assert parse_flow_message(None) is None

def test_signal_id_uniqueness():
    sig1 = parse_flow_message(SAMPLE)
    sig2 = parse_flow_message(SAMPLE)
    assert sig1.signal_id == sig2.signal_id

if __name__ == "__main__":
    test_full_parse(); print("test_full_parse  PASSED")
    test_call_parse(); print("test_call_parse  PASSED")
    test_bad_message_returns_none(); print("test_bad_message PASSED")
    test_signal_id_uniqueness(); print("test_signal_id   PASSED")
    print("\nAll tests passed.")
