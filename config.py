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


# ── Telegram ─────────────────────────────────────────────────────────────────
# Bot token from @BotFather. The bot must be admin in both SOURCE and DEST channels.
BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")

# Channel A: source of raw flow alerts — @username or numeric ID (-100...)
SOURCE_CHANNEL: str = os.environ.get("SOURCE_CHANNEL", "")
# Channel B: where interpreted decisions are posted
DEST_CHANNEL: str = os.environ.get("DEST_CHANNEL", "")

# ── Test mode ────────────────────────────────────────────────────────────────
# When true: hard filter runs normally, then a HOLD is immediately posted to
# Channel B without fetching market data, checking alignment, or waiting for
# a price trigger. Use only for local pipeline validation.
TEST_MODE: bool = os.environ.get("TEST_MODE", "false").strip().lower() == "true"

# ── Hard filters ─────────────────────────────────────────────────────────────
MIN_SCORE: int = 75
REQUIRED_CONVICTION: str = "A"
MIN_VOL_OI: float = 5.0
MAX_DTE: int = 14

# ── Timing ───────────────────────────────────────────────────────────────────
WATCH_INTERVAL_SECONDS: int = 20        # watcher poll cadence
SIGNAL_EXPIRY_MINUTES: int = 90         # watchlist TTL

# ── Market data ───────────────────────────────────────────────────────────────
MARKET_DATA_CACHE_TTL: int = 120        # seconds before a re-fetch is attempted
MARKET_DATA_STALE_TTL: int = 300        # seconds to serve last-known-good data on rate-limit failure
MARKET_OPEN_ET: tuple[int, int] = (9, 30)
PREMARKET_START_ET: tuple[int, int] = (4, 0)

# ── Paths ─────────────────────────────────────────────────────────────────────
# Override with DB_PATH=/app/data/signals.db on Railway (mounted volume).
DB_PATH: str = os.environ.get("DB_PATH", "data/signals.db")

# ── Backup ────────────────────────────────────────────────────────────────────
# Private channel/group where the bot uploads signals.db for persistence across
# Railway redeploys. Set to a numeric chat ID (e.g. -1001234567890) or leave
# blank to disable. The bot must be admin with "Pin Messages" permission.
BACKUP_CHAT_ID: str = os.environ.get("BACKUP_CHAT_ID", "")
