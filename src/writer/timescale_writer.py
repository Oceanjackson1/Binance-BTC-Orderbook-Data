"""
TimescaleWriter stores raw order book data and collector health in TimescaleDB.

Tables written:
  orderbook_events_raw         raw diff events from Binance
  orderbook_full_snapshots_raw full-book snapshots and checkpoints
  orderbook_snapshots          1-second top-of-book summary
  collector_health             collector restart/live status
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from src.collector.base_collector import BaseCollector

logger = logging.getLogger(__name__)

_SUMMARY_INTERVAL = 1
_HEALTH_INTERVAL = 10
_EVENT_FLUSH_SIZE = 1_000
_EVENT_FLUSH_INTERVAL = 2
_SNAPSHOT_FLUSH_SIZE = 4
_SNAPSHOT_FLUSH_INTERVAL = 5


class TimescaleWriter:
    def __init__(
        self,
        dsn: str,
        collectors: "List[BaseCollector]",
        event_queue: asyncio.Queue,
    ) -> None:
        self.dsn = dsn
        self.collectors = collectors
        self.event_queue = event_queue
        self._pool = None
        self._event_rows: list[tuple] = []
        self._snapshot_rows: list[tuple] = []
        self._last_event_flush = 0.0
        self._last_snapshot_flush = 0.0

    async def run(self) -> None:
        try:
            import asyncpg
        except ImportError:
            logger.error("asyncpg not installed — TimescaleWriter disabled.")
            return

        try:
            self._pool = await asyncpg.create_pool(
                self.dsn,
                min_size=1,
                max_size=5,
                init=_init_connection,
            )
        except Exception:
            logger.exception("Cannot connect to TimescaleDB — TimescaleWriter disabled.")
            return

        logger.info("TimescaleWriter connected to TimescaleDB.")
        await self._setup_tables()

        self._last_event_flush = asyncio.get_running_loop().time()
        self._last_snapshot_flush = self._last_event_flush

        raw_task = asyncio.create_task(self._consume_raw_events(), name="timescale-raw")
        summary_task = asyncio.create_task(
            self._write_periodic_summaries(),
            name="timescale-summary",
        )

        try:
            await asyncio.gather(raw_task, summary_task)
        except asyncio.CancelledError:
            raw_task.cancel()
            summary_task.cancel()
            await asyncio.gather(raw_task, summary_task, return_exceptions=True)
            raise
        finally:
            await self._flush_all()
            if self._pool:
                await self._pool.close()

    async def _setup_tables(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                CREATE EXTENSION IF NOT EXISTS timescaledb;

                CREATE TABLE IF NOT EXISTS orderbook_events_raw (
                    exchange_time         TIMESTAMPTZ NOT NULL,
                    exchange_time_ms      BIGINT      NOT NULL,
                    transaction_time_ms   BIGINT,
                    receive_time_ns       BIGINT      NOT NULL,
                    market                TEXT        NOT NULL,
                    symbol                TEXT        NOT NULL,
                    first_update_id       BIGINT      NOT NULL,
                    last_update_id        BIGINT      NOT NULL,
                    prev_final_update_id  BIGINT,
                    bids                  JSONB       NOT NULL,
                    asks                  JSONB       NOT NULL
                );
                SELECT create_hypertable(
                    'orderbook_events_raw', 'exchange_time', if_not_exists => TRUE
                );
                CREATE INDEX IF NOT EXISTS idx_orderbook_events_raw_symbol_time
                    ON orderbook_events_raw (market, symbol, exchange_time DESC);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_orderbook_events_raw_unique
                    ON orderbook_events_raw (market, symbol, exchange_time, last_update_id);

                CREATE TABLE IF NOT EXISTS orderbook_full_snapshots_raw (
                    snapshot_time       TIMESTAMPTZ NOT NULL,
                    snapshot_time_ns    BIGINT      NOT NULL,
                    market              TEXT        NOT NULL,
                    symbol              TEXT        NOT NULL,
                    snapshot_type       TEXT        NOT NULL,
                    last_update_id      BIGINT      NOT NULL,
                    bid_count           INT         NOT NULL,
                    ask_count           INT         NOT NULL,
                    bids                JSONB       NOT NULL,
                    asks                JSONB       NOT NULL
                );
                SELECT create_hypertable(
                    'orderbook_full_snapshots_raw', 'snapshot_time', if_not_exists => TRUE
                );
                CREATE INDEX IF NOT EXISTS idx_orderbook_full_snapshots_symbol_time
                    ON orderbook_full_snapshots_raw (market, symbol, snapshot_time DESC);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_orderbook_full_snapshots_unique
                    ON orderbook_full_snapshots_raw (
                        market, symbol, snapshot_time, last_update_id, snapshot_type
                    );

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

    async def _consume_raw_events(self) -> None:
        while True:
            try:
                event = await asyncio.wait_for(self.event_queue.get(), timeout=1)
                event_type = event.get("event_type", "diff")
                if event_type.startswith("snapshot_"):
                    self._snapshot_rows.append(_snapshot_row(event))
                else:
                    self._event_rows.append(_event_row(event))
                await self._maybe_flush()
            except asyncio.TimeoutError:
                await self._flush_stale()
            except asyncio.CancelledError:
                logger.info("Timescale raw writer cancelled.")
                raise
            except Exception:
                logger.exception("Timescale raw writer error (continuing).")

    async def _write_periodic_summaries(self) -> None:
        tick = 0
        while True:
            try:
                await asyncio.sleep(_SUMMARY_INTERVAL)
                tick += 1
                await self._write_top_of_book()
                if tick % _HEALTH_INTERVAL == 0:
                    await self._write_health()
            except asyncio.CancelledError:
                logger.info("Timescale summary writer cancelled.")
                raise
            except Exception:
                logger.exception("Timescale summary writer error (continuing).")

    async def _maybe_flush(self) -> None:
        now = asyncio.get_running_loop().time()
        if len(self._event_rows) >= _EVENT_FLUSH_SIZE:
            await self._flush_events()
        elif self._event_rows and now - self._last_event_flush >= _EVENT_FLUSH_INTERVAL:
            await self._flush_events()

        if len(self._snapshot_rows) >= _SNAPSHOT_FLUSH_SIZE:
            await self._flush_snapshots()
        elif self._snapshot_rows and now - self._last_snapshot_flush >= _SNAPSHOT_FLUSH_INTERVAL:
            await self._flush_snapshots()

    async def _flush_stale(self) -> None:
        now = asyncio.get_running_loop().time()
        if self._event_rows and now - self._last_event_flush >= _EVENT_FLUSH_INTERVAL:
            await self._flush_events()
        if self._snapshot_rows and now - self._last_snapshot_flush >= _SNAPSHOT_FLUSH_INTERVAL:
            await self._flush_snapshots()

    async def _flush_all(self) -> None:
        await self._flush_events()
        await self._flush_snapshots()

    async def _flush_events(self) -> None:
        if not self._event_rows:
            return

        rows = self._event_rows
        self._event_rows = []

        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(
                    """
                    INSERT INTO orderbook_events_raw (
                        exchange_time, exchange_time_ms, transaction_time_ms,
                        receive_time_ns, market, symbol, first_update_id,
                        last_update_id, prev_final_update_id, bids, asks
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    ON CONFLICT DO NOTHING
                    """,
                    rows,
                )
        except Exception:
            self._event_rows = rows + self._event_rows
            raise
        self._last_event_flush = asyncio.get_running_loop().time()

    async def _flush_snapshots(self) -> None:
        if not self._snapshot_rows:
            return

        rows = self._snapshot_rows
        self._snapshot_rows = []

        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(
                    """
                    INSERT INTO orderbook_full_snapshots_raw (
                        snapshot_time, snapshot_time_ns, market, symbol,
                        snapshot_type, last_update_id, bid_count, ask_count, bids, asks
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT DO NOTHING
                    """,
                    rows,
                )
        except Exception:
            self._snapshot_rows = rows + self._snapshot_rows
            raise
        self._last_snapshot_flush = asyncio.get_running_loop().time()

    async def _write_top_of_book(self) -> None:
        now = datetime.now(timezone.utc)
        rows = []

        for collector in self.collectors:
            book = collector.book
            if not book.initialized:
                continue

            best_bid = book.best_bid()
            best_ask = book.best_ask()
            spread = book.spread()
            mid_price = book.mid_price()

            bid_depth = sum(qty for _, qty in book.depth(10, "bid"))
            ask_depth = sum(qty for _, qty in book.depth(10, "ask"))

            rows.append(
                (
                    now,
                    collector.MARKET,
                    collector.symbol,
                    best_bid[0] if best_bid else None,
                    best_ask[0] if best_ask else None,
                    spread,
                    mid_price,
                    bid_depth,
                    ask_depth,
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
                collector.MARKET,
                collector.symbol,
                collector.restart_count,
                collector.book.last_update_id if collector.book.initialized else None,
                collector.is_live,
            )
            for collector in self.collectors
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


async def _init_connection(conn) -> None:
    await conn.set_type_codec(
        "json",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
        format="text",
    )


def _event_row(event: dict) -> tuple:
    return (
        _ms_to_datetime(event["exchange_time_ms"]),
        event["exchange_time_ms"],
        event.get("transaction_time_ms"),
        event["receive_time_ns"],
        event["market"],
        event["symbol"],
        event["first_update_id"],
        event["last_update_id"],
        event.get("prev_final_update_id"),
        event.get("bids", []),
        event.get("asks", []),
    )


def _snapshot_row(event: dict) -> tuple:
    return (
        _ns_to_datetime(event["snapshot_time_ns"]),
        event["snapshot_time_ns"],
        event["market"],
        event["symbol"],
        event["event_type"].replace("snapshot_", ""),
        event["last_update_id"],
        event["bid_count"],
        event["ask_count"],
        event.get("bids", []),
        event.get("asks", []),
    )


def _ms_to_datetime(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def _ns_to_datetime(timestamp_ns: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ns / 1_000_000_000, tz=timezone.utc)
