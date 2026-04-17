"""
Structured BOT_DATA block generator.

Every report that goes to Channel B MUST include this block at the end so
the command bot can parse it without NLP.

Block format (strict — no variations allowed):

    [BOT_DATA]
    BIAS=BULLISH
    HEDGING=TRUE
    MACRO_OVERRIDE=FALSE
    CONFIDENCE=75
    REGIME=TREND_UP
    PRIMARY=NQ
    SECONDARY=ES
    LEADERS=NVDA,AMD,MSFT
    DRAGS=TSLA,AMZN
    QQQ_VWAP=629.83
    QQQ_STOP=626.95
    SPY_VWAP=695.73
    SPY_STOP=693.05
    PLAYBOOK=BUY_DIPS_BIG_TECH
    SESSION=RTH
    DATA_QUALITY=HIGH
    [/BOT_DATA]

Rules enforced here:
  - ALL keys always present (never omitted)
  - Values are uppercase where applicable
  - Exactly one CONFIDENCE value
  - No commentary inside the block
  - DATA_QUALITY=LOW blocks actionable signal emission upstream

Public API
----------
BotDataBlock            — dataclass representing every field
build_bot_data(...)     — construct a BotDataBlock from engine outputs
render_bot_data(block)  — serialize to the [BOT_DATA]...[/BOT_DATA] string
"""

from dataclasses import dataclass, field
from typing import Optional


# ── Canonical playbook labels ─────────────────────────────────────────────────

PLAYBOOK_BUY_DIPS_BIG_TECH   = "BUY_DIPS_BIG_TECH"
PLAYBOOK_BUY_DIPS_BROAD      = "BUY_DIPS_BROAD"
PLAYBOOK_FAILED_BOUNCE_SHORT = "FAILED_BOUNCE_SHORT"
PLAYBOOK_PAIR_TRADE          = "PAIR_TRADE"
PLAYBOOK_WAIT                = "WAIT"
PLAYBOOK_PREMARKET_WATCH     = "PREMARKET_WATCH"
PLAYBOOK_AFTER_HOURS_WATCH   = "AFTER_HOURS_WATCH"
PLAYBOOK_NO_TRADE            = "NO_TRADE"


# ── Regime normalizer ─────────────────────────────────────────────────────────
# Maps the human-readable regime labels from telegram_handler to BOT_DATA keys.

_REGIME_MAP: dict[str, str] = {
    "BROAD TREND UP":        "TREND_UP",
    "NARROW TECH-LED UP":    "TREND_UP_TECH",
    "BROAD TREND DOWN":      "TREND_DOWN",
    "NARROW TECH-LED DOWN":  "TREND_DOWN_TECH",
    "DEFENSIVE RISK-OFF":    "RISK_OFF",
    "ROTATIONAL CHOP":       "ROTATIONAL",
    "MIXED / UNTRADEABLE":   "MIXED",
    # Simplified regime labels from _simplified_regime()
    "TRENDING":              "TRENDING",
    "ROTATIONAL":            "ROTATIONAL",
    "HEDGED":                "HEDGED",
    "CHOP":                  "CHOP",
    # Already-normalized values pass through
    "TREND_UP":              "TREND_UP",
    "TREND_UP_TECH":         "TREND_UP_TECH",
    "TREND_DOWN":            "TREND_DOWN",
    "TREND_DOWN_TECH":       "TREND_DOWN_TECH",
    "RISK_OFF":              "RISK_OFF",
    "MIXED":                 "MIXED",
    "NO_DATA":               "NO_DATA",
}


def normalize_regime(raw: str) -> str:
    return _REGIME_MAP.get(raw.strip(), "NO_DATA")


# ── BotDataBlock dataclass ────────────────────────────────────────────────────

@dataclass
class BotDataBlock:
    # ── Core bias ──────────────────────────────────────────────────────────────
    bias: str                          # BULLISH | BEARISH | NEUTRAL
    hedging: bool                      # True = hedge flow detected in batch
    macro_override: bool               # True = macro/index flow overrides ticker flow
    bias_confidence: int               # 0–100, spec-flow directional strength
    execution_confidence: int          # 0–100, tradability (price + flow alignment)
    alignment: str                     # ALIGNED | NOT ALIGNED | UNKNOWN

    # ── Regime + futures ───────────────────────────────────────────────────────
    regime: str                        # normalized regime key
    primary: str                       # primary futures instrument (NQ | ES | RTY | YM | NONE)
    secondary: str                     # secondary (or NONE)

    # ── Stock leadership ───────────────────────────────────────────────────────
    leaders: list[str] = field(default_factory=list)    # aligned leaders (up to 5)
    drags:   list[str] = field(default_factory=list)    # lagging/counter tickers

    # ── VWAP levels ────────────────────────────────────────────────────────────
    qqq_vwap:  Optional[float] = None
    qqq_stop:  Optional[float] = None   # VWAP − buffer (used as soft stop)
    spy_vwap:  Optional[float] = None
    spy_stop:  Optional[float] = None

    # ── Execution context ──────────────────────────────────────────────────────
    playbook:     str = PLAYBOOK_WAIT
    session:      str = "RTH"          # RTH | PREMARKET | AFTER_HOURS | CLOSED
    data_quality: str = "HIGH"         # HIGH | MEDIUM | LOW

    # ── Optional VWAP tags ────────────────────────────────────────────────────
    qqq_vwap_tag: str = ""             # ABOVE_PM_HIGH | BELOW_PM_LOW | ""
    spy_vwap_tag: str = ""


# ── Stop calculation ──────────────────────────────────────────────────────────

_STOP_BUFFER_PCT = 0.004   # 0.4% below VWAP as soft stop


def _vwap_stop(vwap: Optional[float], bias: str) -> Optional[float]:
    """
    Derive a simple VWAP-based stop level.
    BULLISH: stop = vwap * (1 - buffer)
    BEARISH: stop = vwap * (1 + buffer)
    """
    if vwap is None:
        return None
    if bias == "BULLISH":
        return round(vwap * (1 - _STOP_BUFFER_PCT), 2)
    if bias == "BEARISH":
        return round(vwap * (1 + _STOP_BUFFER_PCT), 2)
    return round(vwap, 2)


# ── VWAP position tag ─────────────────────────────────────────────────────────

def _vwap_tag(price: Optional[float], pm_high: Optional[float],
              pm_low: Optional[float]) -> str:
    if price is None:
        return ""
    if pm_high is not None and price > pm_high:
        return "ABOVE_PM_HIGH"
    if pm_low is not None and price < pm_low:
        return "BELOW_PM_LOW"
    return ""


# ── Playbook derivation ───────────────────────────────────────────────────────

def _derive_playbook(
    bias: str,
    regime: str,
    hedging: bool,
    session: str,
    data_quality: str,
    leaders: list[str],
    execution_confidence: int = 50,
) -> str:
    if data_quality == "LOW":
        return PLAYBOOK_NO_TRADE
    if session == "PREMARKET":
        return PLAYBOOK_PREMARKET_WATCH
    if session == "AFTER_HOURS":
        return PLAYBOOK_AFTER_HOURS_WATCH
    if session == "CLOSED":
        return PLAYBOOK_NO_TRADE
    if execution_confidence < 25:
        return PLAYBOOK_NO_TRADE
    if regime in ("MIXED", "NO_DATA", "CHOP"):
        return PLAYBOOK_WAIT
    if regime == "HEDGED":
        return PLAYBOOK_WAIT
    if regime == "ROTATIONAL":
        return PLAYBOOK_PAIR_TRADE
    if bias == "BULLISH":
        tech_tickers = {"QQQ", "XLK", "NVDA", "AMD", "MSFT", "AAPL", "META", "GOOGL"}
        has_tech_leaders = bool(set(leaders) & tech_tickers)
        if regime in ("TREND_UP_TECH",) or has_tech_leaders:
            return PLAYBOOK_BUY_DIPS_BIG_TECH
        return PLAYBOOK_BUY_DIPS_BROAD
    if bias == "BEARISH":
        return PLAYBOOK_FAILED_BOUNCE_SHORT
    return PLAYBOOK_PAIR_TRADE


# ── Builder ───────────────────────────────────────────────────────────────────

def build_bot_data(
    *,
    bias: str,
    hedging: bool,
    bias_confidence: int,
    execution_confidence: int,
    alignment: str = "UNKNOWN",
    regime_raw: str,
    primary_futures: str,
    secondary_futures: str,
    leaders: list[str],
    drags: list[str],
    session: str,
    data_quality: str,
    macro_override: bool = False,
    qqq_vwap: Optional[float] = None,
    qqq_price: Optional[float] = None,
    qqq_pm_high: Optional[float] = None,
    qqq_pm_low: Optional[float] = None,
    spy_vwap: Optional[float] = None,
    spy_price: Optional[float] = None,
    spy_pm_high: Optional[float] = None,
    spy_pm_low: Optional[float] = None,
) -> BotDataBlock:
    """
    Construct a BotDataBlock from engine outputs.

    All inputs are keyword-only to prevent positional mistakes.
    """
    regime = normalize_regime(regime_raw)

    qqq_stop = _vwap_stop(qqq_vwap, bias)
    spy_stop = _vwap_stop(spy_vwap, bias)

    qqq_tag = _vwap_tag(qqq_price, qqq_pm_high, qqq_pm_low)
    spy_tag = _vwap_tag(spy_price, spy_pm_high, spy_pm_low)

    playbook = _derive_playbook(
        bias=bias,
        regime=regime,
        hedging=hedging,
        session=session,
        data_quality=data_quality,
        leaders=leaders,
        execution_confidence=execution_confidence,
    )

    return BotDataBlock(
        bias=bias.upper(),
        hedging=hedging,
        macro_override=macro_override,
        bias_confidence=max(0, min(100, bias_confidence)),
        execution_confidence=max(0, min(100, execution_confidence)),
        alignment=alignment.upper(),
        regime=regime,
        primary=primary_futures.upper() if primary_futures else "NONE",
        secondary=secondary_futures.upper() if secondary_futures else "NONE",
        leaders=leaders[:5],
        drags=drags[:5],
        qqq_vwap=round(qqq_vwap, 2) if qqq_vwap else None,
        qqq_stop=qqq_stop,
        spy_vwap=round(spy_vwap, 2) if spy_vwap else None,
        spy_stop=spy_stop,
        playbook=playbook,
        session=session,
        data_quality=data_quality,
        qqq_vwap_tag=qqq_tag,
        spy_vwap_tag=spy_tag,
    )


# ── Renderer ──────────────────────────────────────────────────────────────────

def _fmt_float(v: Optional[float]) -> str:
    return f"{v:.2f}" if v is not None else "N/A"


def _fmt_bool(v: bool) -> str:
    return "TRUE" if v else "FALSE"


def _fmt_list(items: list[str]) -> str:
    return ",".join(items) if items else "NONE"


def render_bot_data(block: BotDataBlock) -> str:
    """
    Serialize a BotDataBlock to the canonical [BOT_DATA]...[/BOT_DATA] string.

    Key contract:
      - Every key is always emitted (no optional omissions)
      - Values are uppercase
      - No extra whitespace or commentary
    """
    lines = [
        "[BOT_DATA]",
        f"BIAS={block.bias}",
        f"HEDGING={_fmt_bool(block.hedging)}",
        f"MACRO_OVERRIDE={_fmt_bool(block.macro_override)}",
        f"BIAS_CONFIDENCE={block.bias_confidence}",
        f"EXECUTION_CONFIDENCE={block.execution_confidence}",
        f"ALIGNMENT={block.alignment}",
        f"REGIME={block.regime}",
        f"PRIMARY={block.primary}",
        f"SECONDARY={block.secondary}",
        f"LEADERS={_fmt_list(block.leaders)}",
        f"DRAGS={_fmt_list(block.drags)}",
        f"QQQ_VWAP={_fmt_float(block.qqq_vwap)}",
        f"QQQ_STOP={_fmt_float(block.qqq_stop)}",
        f"SPY_VWAP={_fmt_float(block.spy_vwap)}",
        f"SPY_STOP={_fmt_float(block.spy_stop)}",
        f"PLAYBOOK={block.playbook}",
        f"SESSION={block.session}",
        f"DATA_QUALITY={block.data_quality}",
        "[/BOT_DATA]",
    ]
    return "\n".join(lines)


# ── Command-bot query helpers ─────────────────────────────────────────────────
# Each function mirrors a /command the QRE bot handles.

def query_bias(block: BotDataBlock) -> str:
    hedging_note = " (with active hedging)" if block.hedging else ""
    return (
        f"BIAS: {block.bias}{hedging_note} | "
        f"Bias Conf: {block.bias_confidence}% | "
        f"Exec Conf: {block.execution_confidence}% | "
        f"Alignment: {block.alignment}"
    )


def query_leaders(block: BotDataBlock) -> str:
    if not block.leaders:
        return "LEADERS: none identified"
    return f"LEADERS: {', '.join(block.leaders)}"


def query_triggers(block: BotDataBlock) -> str:
    lines = []
    if block.qqq_vwap:
        lines.append(f"QQQ VWAP: {block.qqq_vwap:.2f} | Stop: {_fmt_float(block.qqq_stop)}")
        if block.qqq_vwap_tag:
            lines.append(f"  Tag: {block.qqq_vwap_tag}")
    if block.spy_vwap:
        lines.append(f"SPY VWAP: {block.spy_vwap:.2f} | Stop: {_fmt_float(block.spy_stop)}")
        if block.spy_vwap_tag:
            lines.append(f"  Tag: {block.spy_vwap_tag}")
    return "\n".join(lines) if lines else "TRIGGERS: no VWAP data"


def query_playbook(block: BotDataBlock) -> str:
    return f"PLAYBOOK: {block.playbook} | Regime: {block.regime} | Session: {block.session}"


def query_hedge(block: BotDataBlock) -> str:
    if block.hedging:
        return "HEDGING: TRUE — counter-trend protection flow detected in batch"
    return "HEDGING: FALSE — flow is directional, no significant hedge activity"
