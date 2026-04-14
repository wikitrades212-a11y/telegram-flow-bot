"""
Format outbound messages for Channel A and Channel B.

Channel A (format_intel) — per-flow YAML, see intel_formatter.py.
Channel B templates:
  format_hold()                  — legacy; kept for compatibility
  format_go()                    — real-time GO alert (still posted immediately)
  format_channel_b_report()      — structured batch report from raw signals
  format_aggregated_report_b()   — Channel B output from aggregated intel report
  format_premarket_report()      — forced 8:30 AM pre-market bias report
  format_batch_report()          — old report (kept for /testsignal fallback)
  format_stats()                 — /stats command reply

All messages use plain text (no HTML parse_mode) unless explicitly noted.
Channel B send_message uses NO parse_mode — plain UTF-8.
"""

import re

from app.parser import FlowSignal
from app.decision_engine import Decision


# ── Shared helpers ────────────────────────────────────────────────────────────

def _fmt(value, suffix: str = "", fmt: str = ".2f") -> str:
    if value is None:
        return "N/A"
    return f"{value:{fmt}}{suffix}"


def _fmt_p(usd: float) -> str:
    if usd >= 1_000_000:
        return f"${usd / 1_000_000:.1f}M"
    if usd >= 1_000:
        return f"${usd / 1_000:.0f}K"
    return f"${usd:.0f}"


_MARKET_TICKERS = {"SPY", "QQQ", "IWM", "SPX", "NDX", "DIA"}


def _contract_score(e, direction: str) -> float:
    """
    Composite score for actionability. Higher = more tradeable.
    Criteria: premium, vol/oi, ATM-ness (delta 0.4-0.6), DTE 3-10, bias alignment.
    """
    delta_abs = abs(getattr(e, "delta", 0) or 0)
    vol_oi    = getattr(e, "vol_oi_ratio", 0) or 0
    premium   = getattr(e, "premium_usd", 0) or 0
    dte       = getattr(e, "dte", 0) or 0
    side      = getattr(e, "side", "")

    score = 0.0

    # Premium (log scale so $22M doesn't totally dominate)
    import math
    if premium > 0:
        score += min(40, math.log10(max(1, premium)) * 6)

    # Vol/OI — aggressive flow signal
    score += min(20, vol_oi * 2)

    # ATM-ness — prefer delta 0.40–0.60
    if 0.40 <= delta_abs <= 0.60:
        score += 20
    elif 0.30 <= delta_abs < 0.40 or 0.60 < delta_abs <= 0.70:
        score += 8

    # DTE — prefer 3–10
    if 3 <= dte <= 10:
        score += 15
    elif 1 <= dte < 3:
        score += 5
    elif 11 <= dte <= 21:
        score += 8

    # Bias alignment
    aligned = (direction == "BULLISH" and side == "CALL") or \
              (direction == "BEARISH" and side == "PUT")
    contra  = (direction == "BULLISH" and side == "PUT") or \
              (direction == "BEARISH" and side == "CALL")
    if aligned:
        score += 15
    elif contra and delta_abs >= 0.35:
        score += 5   # meaningful hedge — keep but score lower

    return score


def _contract_description(e, direction: str) -> str:
    """Short plain-English description of why this contract is notable."""
    delta_abs = abs(getattr(e, "delta", 0) or 0)
    vol_oi    = getattr(e, "vol_oi_ratio", 0) or 0
    premium   = getattr(e, "premium_usd", 0) or 0
    side      = getattr(e, "side", "")
    ticker    = getattr(e, "ticker", "")

    parts = []

    if ticker in _MARKET_TICKERS:
        parts.append("index play")
    if premium >= 10_000_000:
        parts.append("institutional size")
    elif premium >= 1_000_000:
        parts.append("large premium")
    if vol_oi >= 10:
        parts.append("extreme vol/oi")
    elif vol_oi >= 5:
        parts.append("high vol/oi")

    aligned = (direction == "BULLISH" and side == "CALL") or \
              (direction == "BEARISH" and side == "PUT")
    if not aligned and delta_abs >= 0.35:
        parts.append("hedge")
    elif 0.40 <= delta_abs <= 0.60:
        parts.append("conviction delta")

    return ", ".join(parts) if parts else "strong flow"


def _top_actionable_contracts(entries, direction: str, top_n: int = 5) -> list:
    """
    Return top_n contracts ranked by actionability score.
    Filters to bias-aligned + meaningful hedges only.
    Deduplicates by (ticker, strike, side).
    """
    seen: set[tuple] = set()
    candidates = []

    for e in entries:
        key = (getattr(e, "ticker", ""), getattr(e, "strike", 0), getattr(e, "side", ""))
        if key in seen:
            continue
        seen.add(key)

        # Exclude contracts outside tradeable DTE range
        dte = getattr(e, "dte", 0) or 0
        if dte < 1 or dte > 21:
            continue

        # Exclude pure lottery / noise
        classification = getattr(e, "classification", "")
        if classification == "LOTTERY":
            continue

        score = _contract_score(e, direction)
        if score > 0:
            candidates.append((score, e))

    candidates.sort(key=lambda x: x[0], reverse=True)
    return [e for _, e in candidates[:top_n]]


def _fmt_actionable_section(entries, direction: str) -> str:
    """Build the TOP ACTIONABLE CONTRACTS section string."""
    contracts = _top_actionable_contracts(entries, direction)
    if not contracts:
        return ""

    lines = ["", "TOP ACTIONABLE CONTRACTS"]
    for e in contracts:
        delta_abs = abs(getattr(e, "delta", 0) or 0)
        delta_str = f"{getattr(e, 'delta', 0):+.2f}" if getattr(e, "delta", None) is not None else "N/A"
        dte       = getattr(e, "dte", 0)
        ticker    = getattr(e, "ticker", "")
        strike    = getattr(e, "strike", 0)
        side      = getattr(e, "side", "")
        desc      = _contract_description(e, direction)
        lines.append(
            f"• {ticker} {strike:.0f}{side[0]} "
            f"(Δ {delta_str}, DTE {dte}) — {desc}"
        )
    return "\n".join(lines)


def _tag_for(entry) -> str:
    """Derive a short label tag from classification and direction."""
    cls = entry.classification
    tag_map = {
        "HEDGE_DIRECTIONAL":      "HEDGE",
        "POSITIONAL_BULL":        "POSITIONAL",
        "POSITIONAL_BEAR":        "POSITIONAL",
        "SPECULATIVE_DIRECTIONAL":"SPEC",
        "CONTINUATION_STRONG":    "CONTINUATION",
        "CONTINUATION_WEAK":      "CONTINUATION",
        "GAMMA_VOL":              "GAMMA",
        "LOTTERY":                "LOTTERY",
    }
    return tag_map.get(cls, cls)


# ── Channel B: new structured report ─────────────────────────────────────────

def format_channel_b_report(analysis: dict) -> str:
    """
    Build the new Channel B structured output.

    Format:
        🟢/🔴 MARKET BIAS: {bias} WITH {context}
        Bear {bear%} vs Bull {bull%} | Confidence: {confidence}/100

        Top Overall Flow
        1. {ticker strike type} | ${premium} IV:{iv}% | Vol/OI {ratio} | Δ {delta} | DTE {dte} | {tag}

        Top Bulls
        • ...

        Top Bears
        • ...

        Market Structure
        • First to pop: ...
        • Lagging shorts: ...
        • Likely to catch bid: ...

        Game Plan
        ▸ Primary: ...
        ▸ Secondary: ...
        ▸ Execution: ...
    """
    if not analysis:
        return ""

    direction  = analysis["direction"]       # "BULLISH" | "BEARISH"
    subtype    = analysis["subtype"]         # "HEDGED" | "POSITIONAL" | "SPECULATIVE"
    bull_pct   = analysis["bull_pct"]
    bear_pct   = analysis["bear_pct"]
    confidence = analysis["confidence"]
    state      = analysis["state"]
    mode       = analysis["mode"]
    entries    = analysis.get("entries", [])

    # ── Bias line ─────────────────────────────────────────────────────────────
    _LOW_CONFIDENCE = 20
    effective_direction = "NEUTRAL" if confidence < _LOW_CONFIDENCE else direction
    if effective_direction == "BULLISH":
        bias_emoji = "🟢"
    elif effective_direction == "BEARISH":
        bias_emoji = "🔴"
    else:
        bias_emoji = "⚪"
    context    = f"{subtype} | {state}"
    conf_note  = " ⚠️ LOW CONFIDENCE" if confidence < _LOW_CONFIDENCE else ""
    lines = [
        f"{bias_emoji} MARKET BIAS: {effective_direction} WITH {context}{conf_note}",
        f"Bear {bear_pct}% vs Bull {bull_pct}% | Confidence: {confidence}/100",
        "",
    ]

    # ── Top Overall Flow (top 5 by priority, then score) ─────────────────────
    actionable = [e for e in entries if e.decision != "KILL"]
    top_all = sorted(actionable, key=lambda e: (e.priority, -e.score))[:5]

    if top_all:
        lines.append("Top Overall Flow")
        for i, e in enumerate(top_all, 1):
            tag = _tag_for(e)
            delta_str = f"{e.delta:+.2f}" if e.delta else "N/A"
            lines.append(
                f"{i}. {e.ticker} ${e.strike:.0f}{e.side[0]} "
                f"| {_fmt_p(e.premium_usd)} IV:{e.iv_pct:.1f}% "
                f"| Vol/OI {e.vol_oi_ratio:.1f}x "
                f"| Δ {delta_str} "
                f"| DTE {e.dte} "
                f"| {tag}"
            )
        lines.append("")

    # ── Top Bulls ─────────────────────────────────────────────────────────────
    bulls = sorted(
        [e for e in actionable if e.side == "CALL"],
        key=lambda e: (e.priority, -e.score),
    )[:3]
    if bulls:
        lines.append("Top Bulls")
        for e in bulls:
            delta_str = f"{e.delta:+.2f}" if e.delta else "N/A"
            lines.append(
                f"• {e.ticker} ${e.strike:.0f}C "
                f"| {_fmt_p(e.premium_usd)} IV:{e.iv_pct:.1f}% "
                f"| Vol/OI {e.vol_oi_ratio:.1f}x "
                f"| Δ {delta_str} | DTE {e.dte}"
            )
        lines.append("")

    # ── Top Bears ─────────────────────────────────────────────────────────────
    bears = sorted(
        [e for e in actionable if e.side == "PUT"],
        key=lambda e: (e.priority, -e.score),
    )[:3]
    if bears:
        lines.append("Top Bears")
        for e in bears:
            delta_str = f"{e.delta:+.2f}" if e.delta else "N/A"
            lines.append(
                f"• {e.ticker} ${e.strike:.0f}P "
                f"| {_fmt_p(e.premium_usd)} IV:{e.iv_pct:.1f}% "
                f"| Vol/OI {e.vol_oi_ratio:.1f}x "
                f"| Δ {delta_str} | DTE {e.dte}"
            )
        lines.append("")

    # ── Market Structure ──────────────────────────────────────────────────────
    lines.append("Market Structure")

    # First to pop: highest-priority bull
    first_pop = bulls[0].ticker if bulls else (bears[0].ticker if bears else "N/A")
    lines.append(f"• First to pop: {first_pop}")

    # Lagging shorts: bears with higher DTE (positional hedges)
    lagging = [e.ticker for e in bears if e.dte >= 7][:2]
    lines.append(f"• Lagging shorts: {', '.join(lagging) if lagging else 'None'}")

    # Likely to catch bid: speculative calls
    spec_tickers = [
        e.ticker for e in actionable
        if e.side == "CALL" and e.classification in ("SPECULATIVE_DIRECTIONAL", "CONTINUATION_STRONG")
    ][:2]
    lines.append(f"• Likely to catch bid: {', '.join(spec_tickers) if spec_tickers else 'N/A'}")
    lines.append("")

    # ── Game Plan ─────────────────────────────────────────────────────────────
    lines.append("Game Plan")

    if mode == "BULLISH":
        primary   = "Buy dips into VWAP on leading names"
        secondary = "Avoid shorts — trend favors longs"
        execution = "Scale in on first 15m confirmation, stop below PM low"
    elif mode == "BEARISH":
        primary   = "Fade bounces into resistance on weak names"
        secondary = "Hedge core longs with index puts"
        execution = "Enter on failed bounce candle, stop above PM high"
    else:  # CHOP / PAIR TRADE
        primary   = f"Long {analysis.get('sectors_strong', ['N/A'])[0] if analysis.get('sectors_strong') else 'N/A'}"
        secondary = f"Short {analysis.get('sectors_weak', ['N/A'])[0] if analysis.get('sectors_weak') else 'N/A'}"
        execution = "Pair trade — size small, wide stops, fade extremes"

    lines.append(f"▸ Primary: {primary}")
    lines.append(f"▸ Secondary: {secondary}")
    lines.append(f"▸ Execution: {execution}")

    # ── Top Actionable Contracts ──────────────────────────────────────────────
    actionable_section = _fmt_actionable_section(entries, direction)
    if actionable_section:
        lines.append(actionable_section)

    return "\n".join(lines)


# ── Pre-market forced report ──────────────────────────────────────────────────

def format_premarket_report(
    analysis: dict | None,
    overnight_notes: list[str] | None = None,
) -> str:
    """
    Forced 8:30 AM pre-market bias report.

    Output:
        PRE-MARKET BIAS REPORT
        - No major unusual flow detected   (or flow summary)
        - Overnight positioning: ...
        - ...

        Bias: NEUTRAL → WAIT OPEN   (or derived bias)
    """
    lines = ["PRE-MARKET BIAS REPORT"]

    if not analysis or analysis.get("total", 0) == 0:
        lines.append("- No major unusual flow detected")
    else:
        direction  = analysis["direction"]
        bull_pct   = analysis["bull_pct"]
        bear_pct   = analysis["bear_pct"]
        confidence = analysis["confidence"]
        entries    = analysis.get("entries", [])

        lines.append(
            f"- Flow detected: {direction} ({bull_pct}% bull / {bear_pct}% bear)"
            f" | Confidence: {confidence}/100"
        )

        top = sorted(entries, key=lambda e: (e.priority, -e.score))[:3]
        for e in top:
            lines.append(
                f"- {e.ticker} {e.side}: {_fmt_p(e.premium_usd)} "
                f"Vol/OI {e.vol_oi_ratio:.1f}x DTE {e.dte}"
            )

    # Overnight / positioning notes
    if overnight_notes:
        for note in overnight_notes:
            lines.append(f"- {note}")
    else:
        lines.append("- Overnight positioning: no data")

    lines.append("")

    # Bias conclusion
    if not analysis or analysis.get("total", 0) == 0:
        lines.append("Bias: NEUTRAL → WAIT OPEN")
    else:
        direction  = analysis["direction"]
        confidence = analysis["confidence"]
        if confidence < 30:
            lines.append("Bias: NEUTRAL → WAIT OPEN")
        else:
            lines.append(f"Bias: {direction} → WATCH OPEN CONFIRMATION")

    return "\n".join(lines)


# ── Channel B: legacy batch report (kept for /testsignal) ────────────────────

def format_batch_report(analysis: dict) -> str:
    """Legacy Channel B market intelligence batch post — used by /testsignal."""
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
        f"MARKET INTELLIGENCE  ({n} signals)",
        "",
        f"TRUE MARKET STATE:  {state}",
        "",
        "BIAS:",
        f"  Direction:   {direction} ({subtype})",
        f"  Bull / Bear: {bull_pct}% / {bear_pct}%",
        f"  Confidence:  {confidence}%",
    ]

    if confidence < 20:
        lines.append("  ⚠️  Confidence <20% — bias ignored")

    if tags:
        lines.append(f"  Tags:        {' · '.join(tags)}")

    drivers = analysis.get("drivers", [])
    if drivers:
        lines += ["", "REAL DRIVERS:"]
        for e in drivers:
            lines.append(f"  • {e.ticker} {e.side} — {e.classification} ({_fmt_p(e.premium_usd)}, p{e.priority})")

    high = analysis.get("high_conviction", [])
    spec = analysis.get("speculative", [])
    if high or spec:
        lines += ["", "TRADE CANDIDATES:"]
        if high:
            lines.append("  High Conviction:")
            for e in high:
                go_tag = " ✅ GO" if e.decision == "GO" else ""
                lines.append(f"    • {e.ticker} {e.side} — {e.classification}{go_tag}")
        if spec:
            lines.append("  Speculative:")
            for e in spec:
                lines.append(f"    • {e.ticker} {e.side} — {e.classification}")

    lines += [
        "",
        f"EXECUTION MODE:  {mode}",
        logic,
    ]

    strong  = analysis.get("sectors_strong", [])
    weak    = analysis.get("sectors_weak", [])
    neutral = analysis.get("sectors_neutral", [])
    if strong or weak:
        lines += ["", "SECTORS:"]
        if strong:
            lines.append(f"  Strong:  {', '.join(strong)}")
        if weak:
            lines.append(f"  Weak:    {', '.join(weak)}")
        if neutral:
            lines.append(f"  Neutral: {', '.join(neutral)}")

    noise = analysis.get("noise", [])
    if noise:
        counts: dict[str, int] = {}
        for e in noise:
            counts[e.classification] = counts.get(e.classification, 0) + 1
        lines += ["", "AVOID / NOISE:"]
        for cls, cnt in sorted(counts.items()):
            lines.append(f"  • {cls}: {cnt}")

    lines += ["", "Not financial advice."]
    return "\n".join(lines)


# ── Stats ─────────────────────────────────────────────────────────────────────

def format_stats(s: dict) -> str:
    days    = s["days"]
    t_note  = f" · {s['ticker_filter']}"  if s.get("ticker_filter")  else ""
    c_note  = f" · {s['class_filter']}"   if s.get("class_filter")   else ""
    avg_m   = s["avg_move"]
    avg_str = f"{avg_m * 100:+.2f}%" if avg_m is not None else "N/A"

    lines = [
        f"STATS ({days}D){t_note}{c_note}",
        "",
        f"TOTAL SIGNALS: {s['total']}",
        f"GO / HOLD / KILL: {s['go']} / {s['hold']} / {s['kill']}",
    ]

    if s["n_results"] == 0:
        lines += ["", "No completed outcomes yet."]
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


# ── Aggregated intel report → Channel B ──────────────────────────────────────

def format_aggregated_report_b(report) -> str:
    """
    Re-format a parsed IntelReport into the standard Channel B output.

    Preserves the intelligence from the upstream aggregated report but
    normalises it into our clean Channel B structure.
    """
    from app.intel_parser import IntelReport  # local import to avoid circular
    if not report:
        return ""

    _LOW_CONFIDENCE = 20
    effective_direction = (
        "NEUTRAL" if report.confidence < _LOW_CONFIDENCE else report.direction
    )
    if effective_direction == "BULLISH":
        bias_emoji = "🟢"
    elif effective_direction == "BEARISH":
        bias_emoji = "🔴"
    else:
        bias_emoji = "⚪"

    context = report.context or report.direction
    conf_note = " ⚠️ LOW CONFIDENCE" if report.confidence < _LOW_CONFIDENCE else ""

    lines = [
        f"{bias_emoji} MARKET BIAS: {effective_direction} WITH {context}{conf_note}",
        f"Bear {report.bear_pct}% vs Bull {report.bull_pct}% | Confidence: {report.confidence}/100",
        "",
    ]

    def _entry_line(e) -> str:
        delta_str = f"{e.delta:+.2f}" if e.delta else "N/A"
        tag = e.tag.strip() if e.tag else ""
        return (
            f"{e.ticker} ${e.strike:.0f}{e.side[0]} "
            f"| {_fmt_p(e.premium_usd)} IV:{e.iv_pct:.0f}% "
            f"| Vol/OI {e.vol_oi_ratio:.1f}x "
            f"| Δ {delta_str} "
            f"| DTE {e.dte}"
            + (f" | {tag}" if tag else "")
        )

    # Top Overall Flow
    if report.top_overall:
        lines.append("Top Overall Flow")
        for i, e in enumerate(report.top_overall, 1):
            lines.append(f"{i}. {_entry_line(e)}")
        lines.append("")

    # Top Bulls
    if report.top_bulls:
        lines.append("Top Bulls")
        for e in report.top_bulls:
            lines.append(f"• {_entry_line(e)}")
        lines.append("")

    # Top Bears
    if report.top_bears:
        lines.append("Top Bears")
        for e in report.top_bears:
            lines.append(f"• {_entry_line(e)}")
        lines.append("")

    # Market Structure
    if report.market_structure:
        lines.append("Market Structure")
        for bullet in report.market_structure:
            # Strip leading emoji/bullet and re-add clean bullet
            clean = re.sub(r"^[•·📉📈\-\s]+", "", bullet).strip()
            if clean:
                lines.append(f"• {clean}")
        lines.append("")

    # Sector Leadership
    if report.sector_leadership:
        lines.append("Sector Leadership")
        for bullet in report.sector_leadership:
            clean = re.sub(r"^[•·📉📈\-\s]+", "", bullet).strip()
            if clean:
                lines.append(f"• {clean}")
        lines.append("")

    # Game Plan
    if report.game_plan:
        lines.append("Game Plan")
        for bullet in report.game_plan:
            clean = bullet.strip()
            if clean.startswith("▸"):
                lines.append(clean)
            elif re.match(r"^(Primary|Secondary|Execution)", clean, re.IGNORECASE):
                lines.append(f"▸ {clean}")
            elif clean.startswith("—") or clean.startswith("-"):
                lines.append(f"  {clean}")
            else:
                lines.append(f"  {clean}")
        lines.append("")

    # Quick Read (headline only — first quoted line or first bullet)
    if report.quick_read:
        lines.append("Quick Read")
        for bullet in report.quick_read[:4]:   # cap at 4 lines
            lines.append(f"• {bullet.strip().lstrip('•· ')}")
        lines.append("")

    # Top Actionable Contracts — combine all flow entries
    all_entries = report.top_overall or (report.top_bulls + report.top_bears)
    actionable_section = _fmt_actionable_section(all_entries, report.direction)
    if actionable_section:
        lines.append(actionable_section)

    return "\n".join(lines)


# ── Legacy single-signal formatters ──────────────────────────────────────────

def format_hold(sig: FlowSignal, dec: Decision) -> str:
    return (
        f"{sig.ticker} {sig.side} FLOW\n"
        f"Score: {sig.score} ({sig.conviction})\n"
        f"Decision: HOLD\n"
        f"\n"
        f"Reason: Awaiting confirmation\n"
        f"\n"
        f"Not financial advice."
    )


def format_go(sig: FlowSignal, dec: Decision) -> str:
    trigger_line = f"Trigger: {dec.trigger_reason}\n\n" if dec.trigger_reason else ""
    return (
        f"{sig.ticker} {sig.side} FLOW\n"
        f"Score: {sig.score} ({sig.conviction})\n"
        f"Decision: GO\n"
        f"\n"
        f"Entry: {_fmt(dec.entry)}\n"
        f"Stop: {_fmt(dec.stop)}\n"
        f"Target: {_fmt(dec.target)} (2R)\n"
        f"\n"
        f"{trigger_line}"
        f"Not financial advice."
    )
