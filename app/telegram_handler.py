"""
Format outbound messages for Channel A and Channel B.

Channel A (format_intel) — per-flow YAML, see intel_formatter.py.
Channel B templates:
  format_hold()         — legacy; kept for compatibility
  format_go()           — real-time GO alert (still posted immediately)
  format_batch_report() — market intelligence batch post (N signals)

All messages use HTML parse_mode.
"""

from app.parser import FlowSignal
from app.decision_engine import Decision


def _fmt(value, suffix: str = "", fmt: str = ".2f") -> str:
    if value is None:
        return "N/A"
    return f"{value:{fmt}}{suffix}"


def format_stats(s: dict) -> str:
    """Format /stats query result for Telegram."""
    days    = s["days"]
    t_note  = f" · {s['ticker_filter']}"       if s.get("ticker_filter")  else ""
    c_note  = f" · {s['class_filter']}"        if s.get("class_filter")   else ""
    avg_m   = s["avg_move"]
    avg_str = f"{avg_m * 100:+.2f}%" if avg_m is not None else "N/A"

    lines = [
        f"🧠 <b>STATS ({days}D){t_note}{c_note}</b>",
        "",
        f"TOTAL SIGNALS: {s['total']}",
        f"GO / HOLD / KILL: {s['go']} / {s['hold']} / {s['kill']}",
    ]

    if s["n_results"] == 0:
        lines += ["", "<i>No completed outcomes yet.</i>"]
    else:
        lines += [
            "",
            "RESULTS:",
            f"WIN / LOSS / FLAT: {s['wins']} / {s['losses']} / {s['flats']}",
            f"WIN RATE: {s['win_rate']}%",
            f"AVG MOVE (30m): {avg_str}",
        ]

    if s.get("by_classification"):
        lines += ["", "BY CLASSIFICATION:"]
        for c in s["by_classification"]:
            lines.append(f"• {c['cls']} → {c['win_rate']}% (n={c['n']})")

    if s.get("top_tickers"):
        lines += ["", "TOP TICKERS:"]
        for t in s["top_tickers"]:
            lines.append(f"• {t['ticker']} → {t['win_rate']}% (n={t['n']})")

    return "\n".join(lines)


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


def _fmt_p(usd: float) -> str:
    if usd >= 1_000_000:
        return f"${usd / 1_000_000:.1f}M"
    if usd >= 1_000:
        return f"${usd / 1_000:.0f}K"
    return f"${usd:.0f}"


def format_batch_report(analysis: dict) -> str:
    """Format Channel B market intelligence batch post."""
    if not analysis:
        return ""

    n          = analysis["total"]
    state      = analysis["state"]
    mode       = analysis["mode"]
    logic      = analysis["trade_logic"]
    direction  = analysis["direction"]
    bull_pct   = analysis["bull_pct"]
    bear_pct   = analysis["bear_pct"]
    confidence = analysis["confidence"]
    subtype    = analysis["subtype"]
    tags       = analysis.get("tags", [])

    lines = [
        f"🧠 <b>MARKET INTELLIGENCE</b>  <i>({n} signals)</i>",
        "",
        f"<b>TRUE MARKET STATE:</b>  {state}",
        "",
        "<b>BIAS:</b>",
        f"  Direction:   {direction} ({subtype})",
        f"  Bull / Bear: {bull_pct}% / {bear_pct}%",
        f"  Confidence:  {confidence}%",
    ]

    if confidence < 20:
        lines.append("  ⚠️  Confidence &lt;20% — bias ignored")

    if tags:
        lines.append(f"  Tags:        {' · '.join(tags)}")

    # Real Drivers
    drivers = analysis.get("drivers", [])
    if drivers:
        lines += ["", "<b>REAL DRIVERS:</b>"]
        for e in drivers:
            lines.append(
                f"  • {e.ticker} {e.side} — {e.classification}"
                f" ({_fmt_p(e.premium_usd)}, p{e.priority})"
            )

    # Trade Candidates
    high = analysis.get("high_conviction", [])
    spec = analysis.get("speculative", [])
    if high or spec:
        lines += ["", "<b>TRADE CANDIDATES:</b>"]
        if high:
            lines.append("  <i>High Conviction:</i>")
            for e in high:
                go_tag = " ✅ GO" if e.decision == "GO" else ""
                lines.append(f"    • {e.ticker} {e.side} — {e.classification}{go_tag}")
        if spec:
            lines.append("  <i>Speculative:</i>")
            for e in spec:
                lines.append(f"    • {e.ticker} {e.side} — {e.classification}")

    # Execution
    lines += [
        "",
        f"<b>EXECUTION MODE:</b>  {mode}",
        f"<pre>{logic}</pre>",
    ]

    # Sectors
    strong  = analysis.get("sectors_strong", [])
    weak    = analysis.get("sectors_weak", [])
    neutral = analysis.get("sectors_neutral", [])
    if strong or weak:
        lines += ["", "<b>SECTORS:</b>"]
        if strong:
            lines.append(f"  Strong:  {', '.join(strong)}")
        if weak:
            lines.append(f"  Weak:    {', '.join(weak)}")
        if neutral:
            lines.append(f"  Neutral: {', '.join(neutral)}")

    # Avoid / Noise
    noise = analysis.get("noise", [])
    if noise:
        counts: dict[str, int] = {}
        for e in noise:
            counts[e.classification] = counts.get(e.classification, 0) + 1
        lines += ["", "<b>AVOID / NOISE:</b>"]
        for cls, cnt in sorted(counts.items()):
            lines.append(f"  • {cls}: {cnt}")

    lines += ["", "<i>Not financial advice.</i>"]
    return "\n".join(lines)


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
