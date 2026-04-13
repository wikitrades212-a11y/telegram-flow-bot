"""
Fetch option contract quotes from Tradier at signal arrival time.

Populates FlowSignal.option_bid/ask/last/mid/quote_time in-place.
Silent on failure — bot proceeds normally with fields as None.
"""

import logging
from typing import Optional

import httpx

import config
from app.parser import FlowSignal

logger = logging.getLogger(__name__)

_TRADIER_URL = "https://api.tradier.com/v1/markets/options/quotes"


def _occ_symbol(sig: FlowSignal) -> str:
    """
    Build OCC option symbol from FlowSignal fields.
    Format: {TICKER}{YY}{MM}{DD}{C|P}{strike*1000 zero-padded to 8 digits}
    Example: GOOGL PUT $318 exp 2026-04-18 → GOOGL260418P00318000
    """
    exp = sig.expiration
    cp  = "C" if sig.side == "CALL" else "P"
    return (
        f"{sig.ticker}"
        f"{str(exp.year)[2:]}{exp.month:02d}{exp.day:02d}"
        f"{cp}"
        f"{int(round(sig.strike * 1000)):08d}"
    )


def _f(val) -> Optional[float]:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


async def fetch_option_quote(sig: FlowSignal) -> None:
    """
    Fetch Tradier option quote and populate sig.option_* fields in-place.

    Prefers option_mid (bid+ask)/2 when both sides exist; falls back to last.
    Returns immediately (no-op) if TRADIER_TOKEN is not configured.
    Any network or parse error is logged at WARNING and silently ignored.
    """
    if not config.TRADIER_TOKEN:
        return

    symbol = _occ_symbol(sig)

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                _TRADIER_URL,
                headers={
                    "Authorization": f"Bearer {config.TRADIER_TOKEN}",
                    "Accept": "application/json",
                },
                params={"symbols": symbol, "greeks": "false"},
            )
            resp.raise_for_status()
            quote = resp.json().get("quotes", {}).get("quote")

        if not quote:
            logger.warning("Tradier: no quote returned for %s", symbol)
            return

        bid  = _f(quote.get("bid"))
        ask  = _f(quote.get("ask"))
        last = _f(quote.get("last"))

        sig.option_bid        = bid
        sig.option_ask        = ask
        sig.option_last       = last
        sig.option_quote_time = quote.get("trade_date") or quote.get("quote_date")

        if bid is not None and ask is not None:
            sig.option_mid = round((bid + ask) / 2, 4)
        elif last is not None:
            sig.option_mid = last

        logger.info(
            "Tradier quote | %s | bid=%s ask=%s mid=%s last=%s",
            symbol, bid, ask, sig.option_mid, last,
        )

    except Exception as exc:
        logger.warning("Tradier quote fetch failed for %s: %s", symbol, exc)
