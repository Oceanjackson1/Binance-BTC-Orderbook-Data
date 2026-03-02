"""
TimescaleWriter — periodically snapshots every collector's local order book
and writes the result to TimescaleDB.

This writer is OPTIONAL.  If TIMESCALE_DSN is not set in the environment
the writer is never started and the system runs Parquet-only.

Tables written (created automatically if they don't exist):
  orderbook_snapshots  — best bid/ask, spread, depth metrics every ~1 s
  collector_health     — restart count and live status every ~10 s
"""

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from src.collector.base_collector import BaseCollector

logger = logging.getLogger(__name__)

_SNAPSHOT_INTERVAL = 1      # seconds between orderbook_snapshots rows
_HEALTH_INTERVAL = 10       # seconds between collector_health rows


class TimescaleWriter:
    def __init__(self, dsn: str, collectors: "List[BaseCollector]") -> None:
        self.dsn = dsn
        self.collectors = collectors
        self._pool = None

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        try:
            import asyncpg
        except ImportError:
            logger.error("asyncpg not installed — TimescaleWriter disabled.")
            return

        try:
            self._pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=3)
        except Exception:
            logger.exception("Cannot connect to TimescaleDB — TimescaleWriter disabled.")
            return

        logger.info("TimescaleWriter connected to TimescaleDB.")
        await self._setup_tables()

        tick = 0
        while True:
            try:
                await asyncio.sleep(_SNAPSHOT_INTERVAL)
                tick += 1

                await self._write_snapshots()

                if tick % _HEALTH_INTERVAL == 0:
                    await self._write_health()

            except asyncio.CancelledError:
                logger.info("TimescaleWriter cancelled.")
                if self._pool:
                    await self._pool.close()
                raise
            except Exception:
                logger.exception("TimescaleWriter error (continuing).")

    # ------------------------------------------------------------------
    # Table setup
    # ------------------------------------------------------------------

    async def _setup_tables(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                    ts              TIMESTAMPTZ NOT NULL,
                    market          TEXT        NOT NULL,
                    symbol          TEXT        NOT NULL,
                    best_bid        NUMERIC,
                    best_ask        NUMERIC,
                    spread          NUMERIC,
                    mid_price       NUMERIC,
                    bid_depth_10    NUMERIC,
                    ask_depth_10    NUMERIC,
                    last_update_id  BIGINT
                );
                SELECT create_hypertable(
                    'orderbook_snapshots', 'ts', if_not_exists => TRUE
                );
                CREATE INDEX IF NOT EXISTS idx_obs_market_symbol
                    ON orderbook_snapshots (market, symbol, ts DESC);

                CREATE TABLE IF NOT EXISTS collector_health (
                    ts              TIMESTAMPTZ NOT NULL,
                    market          TEXT        NOT NULL,
                    symbol          TEXT        NOT NULL,
                    restarts        INT         NOT NULL DEFAULT 0,
                    last_update_id  BIGINT,
                    is_live         BOOLEAN     NOT NULL DEFAULT FALSE
                );
                SELECT create_hypertable(
                    'collector_health', 'ts', if_not_exists => TRUE
                );
                """
            )

    # ------------------------------------------------------------------
    # Writers
    # ------------------------------------------------------------------

    async def _write_snapshots(self) -> None:
        now = datetime.now(timezone.utc)
        rows = []

        for collector in self.collectors:
            book = collector.book
            if not book.initialized:
                continue

            bb = book.best_bid()
            ba = book.best_ask()
            spread = book.spread()
            mid = book.mid_price()

            bid_depth = sum(q for _, q in book.depth(10, "bid"))
            ask_depth = sum(q for _, q in book.depth(10, "ask"))

            rows.append(
                (
                    now,
                    collector.MARKET,
                    collector.symbol,
                    float(bb[0]) if bb else None,
                    float(ba[0]) if ba else None,
                    float(spread) if spread else None,
                    float(mid) if mid else None,
                    float(bid_depth),
                    float(ask_depth),
                    book.last_update_id,
                )
            )

        if not rows:
            return

        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO orderbook_snapshots
                    (ts, market, symbol, best_bid, best_ask, spread,
                     mid_price, bid_depth_10, ask_depth_10, last_update_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                """,
                rows,
            )

    async def _write_health(self) -> None:
        now = datetime.now(timezone.utc)
        rows = [
            (
                now,
                c.MARKET,
                c.symbol,
                c.restart_count,
                c.book.last_update_id if c.book.initialized else None,
                c.is_live,
            )
            for c in self.collectors
        ]

        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO collector_health
                    (ts, market, symbol, restarts, last_update_id, is_live)
                VALUES ($1,$2,$3,$4,$5,$6)
                """,
                rows,
            )
