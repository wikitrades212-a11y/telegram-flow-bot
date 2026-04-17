"""
Format a single FlowSignal as a structured YAML block for Channel A (INTEL_CHANNEL).

Channel A is the structured intelligence layer — machine-readable first,
human-readable second. No trade plan or execution logic here.
"""

from app.parser import FlowSignal
from app.classifier import iv_bucket, aggression_label, participant_label


def _fmt_premium(usd: float) -> str:
    if usd >= 1_000_000:
        return f"${usd / 1_000_000:.1f}M"
    if usd >= 1_000:
        return f"${usd / 1_000:.0f}K"
    return f"${usd:.0f}"


def format_intel(
    sig: FlowSignal,
    classification: str,
    signal_role: str,
    priority: int,
) -> str:
    """Return HTML-formatted Channel A post for a single flow."""
    contract = f"${sig.strike}{sig.side[0]} {sig.expiration.strftime('%-d %b')}"
    delta_str = f"{sig.delta:.2f}" if sig.delta is not None else "N/A"

    opt_prem = getattr(sig, "premium_at_signal", None)
    opt_prem_str = f"${opt_prem:.2f}" if opt_prem else "N/A"
    direction = "BULLISH" if sig.side == "CALL" else "BEARISH"

    lines = [
        "📊 <b>FLOW INTELLIGENCE</b>",
        "<pre>",
        "FLOW:",
        f"  ticker:             {sig.ticker}",
        f"  contract:           {contract}",
        f"  direction:          {direction}",
        f"  type:               {sig.side}",
        f"  notional:           {_fmt_premium(sig.premium_usd)}",
        f"  premium_at_signal:  {opt_prem_str}",
        f"  iv:                 {sig.iv_pct:.1f}%",
        f"  iv_bucket:          {iv_bucket(sig.iv_pct)}",
        f"  vol_oi:             {sig.vol_oi_ratio:.1f}x",
        f"  delta:              {delta_str}",
        f"  dte:                {sig.dte}",
        f"  aggression:         {aggression_label(sig.vol_oi_ratio)}",
        f"  participant:        {participant_label(sig.premium_usd)}",
        f"  classification:     {classification}",
        f"  signal_role:        {signal_role}",
        f"  priority:           {priority}",
        "</pre>",
    ]
    return "\n".join(lines)
