import os
from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            "Copy .env.example to .env and fill in your credentials."
        )
    return val


def validate_env() -> None:
    """Call once at startup. Raises EnvironmentError on missing required vars."""
    _require("BOT_TOKEN")
    _require("SOURCE_CHANNEL")
    _require("DEST_CHANNEL")


# ── Telegram bot ──────────────────────────────────────────────────────────────
# Bot token from @BotFather. The bot must be admin in both SOURCE and DEST channels.
BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")

# Channel A: source of raw flow alerts — @username or numeric ID (-100...)
SOURCE_CHANNEL: str = os.environ.get("SOURCE_CHANNEL", "")
# Channel B: where interpreted decisions are posted — numeric ID (-100xxxx)
DEST_CHANNEL: str = os.environ.get("DEST_CHANNEL", "")

# ── Test mode ─────────────────────────────────────────────────────────────────
TEST_MODE: bool = os.environ.get("TEST_MODE", "false").strip().lower() == "true"

# ── Hard filters — regular session ───────────────────────────────────────────
MIN_SCORE: int = 75
REQUIRED_CONVICTION: str = "A"
MIN_VOL_OI: float = 5.0
MAX_DTE: int = 14

# ── Pre-market filters (07:00–09:29 ET) ──────────────────────────────────────
PREMARKET_MIN_PREMIUM: float = 100_000   # $100K
PREMARKET_MIN_VOL_OI:  float = 1.2

# ── Timing ───────────────────────────────────────────────────────────────────
WATCH_INTERVAL_SECONDS: int = 20
SIGNAL_EXPIRY_MINUTES: int = 90

# ── Alpaca market data ────────────────────────────────────────────────────────
ALPACA_API_KEY:    str = os.environ.get("ALPACA_API_KEY", "")
ALPACA_API_SECRET: str = os.environ.get("ALPACA_API_SECRET", "")
ALPACA_FEED:       str = os.environ.get("ALPACA_FEED", "sip")

# ── Market data caching ───────────────────────────────────────────────────────
MARKET_DATA_CACHE_TTL: int = 120
MARKET_DATA_STALE_TTL: int = 300
MARKET_OPEN_ET: tuple[int, int] = (9, 30)
PREMARKET_START_ET: tuple[int, int] = (4, 0)

# ── Paths ─────────────────────────────────────────────────────────────────────
DB_PATH: str = os.environ.get("DB_PATH", "data/signals.db")

# ── Backup ────────────────────────────────────────────────────────────────────
BACKUP_CHAT_ID: str = os.environ.get("BACKUP_CHAT_ID", "")

# ── Intelligence layer ────────────────────────────────────────────────────────
INTEL_CHANNEL: str = os.environ.get("INTEL_CHANNEL", "")

# Number of signals that trigger a Channel B batch report.
BATCH_SIGNAL_COUNT: int = int(os.environ.get("BATCH_SIGNAL_COUNT", "3"))

# ── Tradier ───────────────────────────────────────────────────────────────────
TRADIER_TOKEN: str = os.environ.get("TRADIER_TOKEN", "")
