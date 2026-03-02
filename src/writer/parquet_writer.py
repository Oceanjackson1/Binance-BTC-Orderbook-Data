"""
ParquetWriter — consumes events from the shared queue and persists them to
Parquet files partitioned by market / symbol.

Two file types are written to <DATA_DIR>/<market>/<symbol>/:

  events_<ts>Z.parquet
      Raw diff events (one row per depthUpdate message).
      Buffered and flushed every FLUSH_SIZE events or FLUSH_INTERVAL seconds.
      Schema: exchange_time_ms, receive_time_ns, update IDs, bids[], asks[].

  snapshots_<ts>Z.parquet
      Full order book state (ALL price levels, not just changes).
      Written immediately on arrival — no buffering.
      Types: "snapshot_initial"   — emitted once right after each sync
             "snapshot_checkpoint" — emitted every hour during live streaming
      Schema: snapshot_time_ns, last_update_id, bid/ask counts, bids[], asks[].

Backtesting replay pattern:
  1. Find the latest snapshots_*.parquet whose last_update_id <= your target time.
  2. Seed the local book from that snapshot.
  3. Apply all diff events from events_*.parquet with exchange_time_ms >= snapshot.

DuckDB query across all event files for a symbol:
  SELECT * FROM 'data/spot/BTCUSDT/events_*.parquet'
  ORDER BY exchange_time_ms
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

# Diff event schema — prices / quantities stay as strings to preserve precision.
_EVENTS_SCHEMA = pa.schema(
    [
        ("exchange_time_ms", pa.int64()),           # Binance event time (Unix ms)
        ("transaction_time_ms", pa.int64()),        # futures only; null for spot
        ("receive_time_ns", pa.int64()),            # local wall-clock (Unix ns)
        ("market", pa.string()),
        ("symbol", pa.string()),
        ("first_update_id", pa.int64()),
        ("last_update_id", pa.int64()),
        ("prev_final_update_id", pa.int64()),       # futures pu; null for spot
        # list<list<string>>: each entry is [price_str, qty_str]
        ("bids", pa.list_(pa.list_(pa.string()))),
        ("asks", pa.list_(pa.list_(pa.string()))),
    ]
)

# Full book snapshot schema
_SNAPSHOTS_SCHEMA = pa.schema(
    [
        ("snapshot_time_ns", pa.int64()),           # local wall-clock (Unix ns)
        ("market", pa.string()),
        ("symbol", pa.string()),
        ("snapshot_type", pa.string()),             # "initial" | "checkpoint"
        ("last_update_id", pa.int64()),
        ("bid_count", pa.int32()),
        ("ask_count", pa.int32()),
        # ALL current price levels at the time of the snapshot
        ("bids", pa.list_(pa.list_(pa.string()))),
        ("asks", pa.list_(pa.list_(pa.string()))),
    ]
)

_FLUSH_SIZE = 10_000        # diff events per file
_FLUSH_INTERVAL = 5 * 60   # seconds


class ParquetWriter:
    def __init__(self, data_dir: Path, event_queue: asyncio.Queue) -> None:
        self.data_dir = Path(data_dir)
        self.event_queue = event_queue

        # Per-(market, symbol) diff-event buffers
        self._buffers: Dict[tuple, List[dict]] = {}
        self._last_flush: Dict[tuple, float] = {}

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        logger.info("ParquetWriter started  data_dir=%s", self.data_dir)
        while True:
            try:
                event = await asyncio.wait_for(self.event_queue.get(), timeout=30)

                event_type = event.get("event_type", "diff")

                if event_type.startswith("snapshot_"):
                    # Snapshots are written immediately — no buffering.
                    await asyncio.to_thread(self._write_snapshot, event)
                else:
                    # Diff events are buffered for batch writes.
                    self._buffer(event)
                    await self._maybe_flush_all()

            except asyncio.TimeoutError:
                await self._flush_stale()
            except asyncio.CancelledError:
                logger.info("ParquetWriter cancelled — flushing remaining buffers.")
                await self._flush_all()
                raise

    # ------------------------------------------------------------------
    # Diff event buffering
    # ------------------------------------------------------------------

    def _buffer(self, event: dict) -> None:
        key = (event["market"], event["symbol"])
        if key not in self._buffers:
            self._buffers[key] = []
            self._last_flush[key] = time.monotonic()
        self._buffers[key].append(event)

    async def _maybe_flush_all(self) -> None:
        for key, buf in list(self._buffers.items()):
            if len(buf) >= _FLUSH_SIZE:
                await asyncio.to_thread(self._flush_events, key)

    async def _flush_stale(self) -> None:
        now = time.monotonic()
        for key, buf in list(self._buffers.items()):
            if buf and now - self._last_flush[key] >= _FLUSH_INTERVAL:
                await asyncio.to_thread(self._flush_events, key)

    async def _flush_all(self) -> None:
        for key, buf in list(self._buffers.items()):
            if buf:
                await asyncio.to_thread(self._flush_events, key)

    # ------------------------------------------------------------------
    # Diff event Parquet write
    # ------------------------------------------------------------------

    def _flush_events(self, key: tuple) -> None:
        market, symbol = key
        events = self._buffers.get(key)
        if not events:
            return

        out_dir = self._ensure_dir(market, symbol)
        ts_str = _ts_str()
        tmp_path = out_dir / f"events_{ts_str}.parquet.tmp"
        out_path = out_dir / f"events_{ts_str}.parquet"

        try:
            table = self._build_events_table(events)
            pq.write_table(table, tmp_path, compression="snappy")
            tmp_path.rename(out_path)
            logger.info("events  %d rows → %s", len(events), out_path)
        except Exception:
            logger.exception("Failed to write events file %s", out_path)
            tmp_path.unlink(missing_ok=True)
            return

        self._buffers[key] = []
        self._last_flush[key] = time.monotonic()

    @staticmethod
    def _build_events_table(events: List[dict]) -> pa.Table:
        def col(name, default=None):
            return [e.get(name, default) for e in events]

        return pa.table(
            {
                "exchange_time_ms":     pa.array(col("exchange_time_ms"),     type=pa.int64()),
                "transaction_time_ms":  pa.array(col("transaction_time_ms"),  type=pa.int64()),
                "receive_time_ns":      pa.array(col("receive_time_ns"),       type=pa.int64()),
                "market":               pa.array(col("market"),                type=pa.string()),
                "symbol":               pa.array(col("symbol"),                type=pa.string()),
                "first_update_id":      pa.array(col("first_update_id"),       type=pa.int64()),
                "last_update_id":       pa.array(col("last_update_id"),        type=pa.int64()),
                "prev_final_update_id": pa.array(col("prev_final_update_id"),  type=pa.int64()),
                "bids":                 pa.array(col("bids", []),              type=pa.list_(pa.list_(pa.string()))),
                "asks":                 pa.array(col("asks", []),              type=pa.list_(pa.list_(pa.string()))),
            },
            schema=_EVENTS_SCHEMA,
        )

    # ------------------------------------------------------------------
    # Snapshot Parquet write (immediate, no buffering)
    # ------------------------------------------------------------------

    def _write_snapshot(self, event: dict) -> None:
        market = event["market"]
        symbol = event["symbol"]
        # Strip the "snapshot_" prefix to store just "initial" | "checkpoint"
        snap_type = event["event_type"].replace("snapshot_", "")

        out_dir = self._ensure_dir(market, symbol)
        ts_str = _ts_str()
        tmp_path = out_dir / f"snapshots_{ts_str}.parquet.tmp"
        out_path = out_dir / f"snapshots_{ts_str}.parquet"

        try:
            table = pa.table(
                {
                    "snapshot_time_ns": pa.array([event["snapshot_time_ns"]], type=pa.int64()),
                    "market":           pa.array([market],                    type=pa.string()),
                    "symbol":           pa.array([symbol],                    type=pa.string()),
                    "snapshot_type":    pa.array([snap_type],                 type=pa.string()),
                    "last_update_id":   pa.array([event["last_update_id"]],   type=pa.int64()),
                    "bid_count":        pa.array([event["bid_count"]],        type=pa.int32()),
                    "ask_count":        pa.array([event["ask_count"]],        type=pa.int32()),
                    "bids":             pa.array([event["bids"]],             type=pa.list_(pa.list_(pa.string()))),
                    "asks":             pa.array([event["asks"]],             type=pa.list_(pa.list_(pa.string()))),
                },
                schema=_SNAPSHOTS_SCHEMA,
            )
            pq.write_table(table, tmp_path, compression="snappy")
            tmp_path.rename(out_path)
            logger.info(
                "snapshot (%s)  bids=%d asks=%d lid=%d → %s",
                snap_type,
                event["bid_count"],
                event["ask_count"],
                event["last_update_id"],
                out_path,
            )
        except Exception:
            logger.exception("Failed to write snapshot file %s", out_path)
            tmp_path.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ensure_dir(self, market: str, symbol: str) -> Path:
        out_dir = self.data_dir / market / symbol
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir


def _ts_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "Z"
