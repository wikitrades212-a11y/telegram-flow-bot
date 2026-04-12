"""
Format outbound messages for Channel B.

Two templates:
  format_hold() → posted once when a signal is placed on the watchlist
  format_go()   → posted when the trigger is confirmed

All messages use HTML parse_mode.
"""

from app.parser import FlowSignal
from app.decision_engine import Decision


def _fmt(value, suffix: str = "", fmt: str = ".2f") -> str:
    if value is None:
        return "N/A"
    return f"{value:{fmt}}{suffix}"


def format_hold(sig: FlowSignal, dec: Decision) -> str:
    label = f"{sig.ticker} {sig.side} FLOW"

    return (
        f"<b>{label}</b>\n"
        f"Score: {sig.score} ({sig.conviction})\n"
        f"Decision: HOLD\n"
        f"\n"
        f"Reason: Awaiting confirmation\n"
        f"\n"
        f"<i>Not financial advice.</i>"
    )


def format_go(sig: FlowSignal, dec: Decision) -> str:
    label = f"{sig.ticker} {sig.side} FLOW"

    trigger_line = f"Trigger: {dec.trigger_reason}\n\n" if dec.trigger_reason else ""

    return (
        f"<b>{label}</b>\n"
        f"Score: {sig.score} ({sig.conviction})\n"
        f"Decision: GO\n"
        f"\n"
        f"Entry: {_fmt(dec.entry)}\n"
        f"Stop: {_fmt(dec.stop)}\n"
        f"Target: {_fmt(dec.target)} (2R)\n"
        f"\n"
        f"{trigger_line}"
        f"<i>Not financial advice.</i>"
    )
