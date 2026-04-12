"""
Watchlist manager.

HOW IT WORKS
- add(signal)     : put a HOLD signal under observation
- run()           : async loop — checks every WATCH_INTERVAL seconds
- on_go callback  : called with (FlowSignal, Decision) when a trigger fires
- Signals expire after SIGNAL_EXPIRY_MINUTES minutes
- Duplicate GO events are blocked by storage.was_sent / mark_sent
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Awaitable, Callable

from app.parser import FlowSignal
from app.decision_engine import Decision, DecisionEngine
from app.risk import compute_targets
from app.storage import was_sent, mark_sent
import config

logger = logging.getLogger(__name__)

_GoCallback = Callable[[FlowSignal, Decision], Awaitable[None]]


@dataclass
class _WatchEntry:
    signal: FlowSignal
    added_at: datetime = field(default_factory=datetime.utcnow)


class Watcher:
    def __init__(self, engine: DecisionEngine, on_go: _GoCallback):
        self.engine = engine
        self.on_go = on_go
        self._watch: dict[str, _WatchEntry] = {}
        self._running = False

    # ── Public API ──────────────────────────────────────────────────────────

    def add(self, sig: FlowSignal) -> None:
        if sig.signal_id in self._watch:
            logger.debug("Already watching %s — skipping", sig.signal_id)
            return
        self._watch[sig.signal_id] = _WatchEntry(signal=sig)
        logger.info("Watch added: %s  (watchlist size=%d)", sig.signal_id, len(self._watch))

    def size(self) -> int:
        return len(self._watch)

    # ── Loop ────────────────────────────────────────────────────────────────

    async def run(self) -> None:
        self._running = True
        logger.info("Watcher loop started (interval=%ds ttl=%dmin)",
                    config.WATCH_INTERVAL_SECONDS, config.SIGNAL_EXPIRY_MINUTES)
        while self._running:
            await self._tick()
            await asyncio.sleep(config.WATCH_INTERVAL_SECONDS)

    def stop(self) -> None:
        self._running = False
        logger.info("Watcher loop stopped")

    # ── Internal ────────────────────────────────────────────────────────────

    async def _tick(self) -> None:
        if not self._watch:
            return

        now = datetime.utcnow()
        expiry = timedelta(minutes=config.SIGNAL_EXPIRY_MINUTES)
        to_remove: list[str] = []

        for sid, entry in list(self._watch.items()):
            if now - entry.added_at > expiry:
                logger.info("Expired from watchlist: %s", sid)
                to_remove.append(sid)
                continue

            try:
                decision = await self.engine.evaluate(entry.signal)
            except Exception as exc:
                logger.error("Engine error for %s: %s", sid, exc, exc_info=True)
                continue

            if decision.verdict == "GO":
                if not was_sent(sid, "GO"):
                    decision = compute_targets(entry.signal, decision)
                    try:
                        await self.on_go(entry.signal, decision)
                        mark_sent(sid, "GO")
                    except Exception as exc:
                        logger.error("on_go callback failed for %s: %s", sid, exc, exc_info=True)
                        continue     # leave in watch — will retry next tick
                to_remove.append(sid)

        for sid in to_remove:
            self._watch.pop(sid, None)
