"""
BaseCollector — implements Binance's 7-step order book synchronisation algorithm.

Subclasses override:
  - MARKET, REST_LIMIT  (class-level constants)
  - get_ws_url()
  - get_rest_url()
  - is_bridging_event()   (spot vs futures bridging condition)
  - check_continuity()    (spot uses U == prev_u+1; futures uses pu == prev_u)

The output of each collector is a stream of raw diff-event dicts placed on
`event_queue`.  The ParquetWriter and TimescaleWriter consume from there.
"""

import asyncio
import json
import logging
import time
from typing import Optional

import aiohttp
import websockets

from src.orderbook.local_book import LocalOrderBook

logger = logging.getLogger(__name__)

# Maximum number of sync attempts before giving up on a single connection
_MAX_SYNC_RETRIES = 15
# Seconds before 24 h forced disconnect at which we proactively reconnect
_RECONNECT_BEFORE_24H = 10 * 60  # 10 minutes
# How often to emit a full-book checkpoint during live streaming
_CHECKPOINT_INTERVAL = 3600  # 1 hour


class BaseCollector:
    MARKET: str = ""
    REST_LIMIT: int = 5000

    def __init__(self, symbol: str, event_queue: asyncio.Queue) -> None:
        self.symbol = symbol.upper()
        self.event_queue = event_queue
        self.book = LocalOrderBook(symbol=self.symbol, market=self.MARKET)
        self.restart_count: int = 0
        self.is_live: bool = False
        self._log = logging.getLogger(f"{self.MARKET}.{self.symbol}")

    # ------------------------------------------------------------------
    # Subclass API
    # ------------------------------------------------------------------

    def get_ws_url(self) -> str:
        raise NotImplementedError

    def get_rest_url(self) -> str:
        raise NotImplementedError

    def get_rest_params(self) -> dict:
        return {"symbol": self.symbol, "limit": self.REST_LIMIT}

    def get_first_update_id(self, event: dict) -> int:
        return event["U"]

    def get_final_update_id(self, event: dict) -> int:
        return event["u"]

    def is_bridging_event(self, event: dict, snap_lid: int) -> bool:
        """
        Spot rule (Binance docs):
          first event must satisfy U <= lastUpdateId+1 AND u >= lastUpdateId+1
        """
        U = self.get_first_update_id(event)
        u = self.get_final_update_id(event)
        return U <= snap_lid + 1 and u >= snap_lid + 1

    def check_continuity(self, event: dict, prev_final_id: int) -> bool:
        """
        Spot rule: every new event's U must equal previous event's u + 1.
        Futures overrides this to use the `pu` field instead.
        """
        return self.get_first_update_id(event) == prev_final_id + 1

    # ------------------------------------------------------------------
    # Main run loop (exponential back-off reconnect)
    # ------------------------------------------------------------------

    async def run(self) -> None:
        backoff = 1
        max_backoff = 60

        while True:
            try:
                await self._run_once()
                backoff = 1  # reset on clean exit (proactive 24 h reconnect)
            except asyncio.CancelledError:
                self._log.info("Collector cancelled — shutting down.")
                raise
            except Exception as exc:
                self.is_live = False
                self.book.reset()
                self.restart_count += 1
                self._log.error(
                    "Unhandled error (restart #%d): %r. Reconnecting in %ds.",
                    self.restart_count,
                    exc,
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    # ------------------------------------------------------------------
    # Single connection lifecycle
    # ------------------------------------------------------------------

    async def _run_once(self) -> None:
        ws_url = self.get_ws_url()
        self._log.info("Connecting → %s", ws_url)
        self.is_live = False

        # Internal queue: WebSocket receiver → sync / live processor
        internal_queue: asyncio.Queue = asyncio.Queue()

        async with websockets.connect(
            ws_url,
            ping_interval=20,
            ping_timeout=60,
            max_size=10 * 1024 * 1024,  # 10 MB per frame
        ) as ws:
            recv_task = asyncio.create_task(
                self._recv_loop(ws, internal_queue),
                name=f"recv-{self.MARKET}-{self.symbol}",
            )
            try:
                # --- Phase 1: synchronise ---
                await self._sync(internal_queue)

                # --- Phase 2: live processing ---
                self.is_live = True
                self._log.info(
                    "LIVE  lastUpdateId=%d  restarts=%d",
                    self.book.last_update_id,
                    self.restart_count,
                )

                # Emit the initial full-book snapshot immediately after sync.
                # This is the backtesting seed: replay = this snapshot + all
                # subsequent diff events from the Parquet event files.
                await self._emit_snapshot("initial")

                connection_open = time.monotonic()
                last_checkpoint = time.monotonic()

                while True:
                    # Proactive reconnect ~10 min before 24 h forced close
                    if time.monotonic() - connection_open > 86400 - _RECONNECT_BEFORE_24H:
                        self._log.info("Approaching 24 h limit — proactive reconnect.")
                        break

                    try:
                        event = await asyncio.wait_for(internal_queue.get(), timeout=30)
                    except asyncio.TimeoutError:
                        self._log.warning("No events for 30 s — stale connection, reconnecting.")
                        break

                    await self._apply_live_event(event)

                    # Hourly checkpoint: full book state written to Parquet.
                    # Allows backtesting to start from any checkpoint rather
                    # than always replaying from the very first snapshot.
                    if time.monotonic() - last_checkpoint >= _CHECKPOINT_INTERVAL:
                        await self._emit_snapshot("checkpoint")
                        last_checkpoint = time.monotonic()

            finally:
                self.is_live = False
                recv_task.cancel()
                try:
                    await recv_task
                except (asyncio.CancelledError, Exception):
                    pass

    # ------------------------------------------------------------------
    # WebSocket receiver
    # ------------------------------------------------------------------

    async def _recv_loop(self, ws, queue: asyncio.Queue) -> None:
        try:
            async for raw in ws:
                event = json.loads(raw)
                await queue.put(event)
        except websockets.ConnectionClosed as exc:
            self._log.warning("WebSocket closed: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Synchronisation (Steps 1–7 from Binance docs)
    # ------------------------------------------------------------------

    async def _sync(self, internal_queue: asyncio.Queue) -> None:
        """
        Buffer events → fetch REST snapshot → locate bridging event →
        apply events from bridging point forward.

        Retries up to _MAX_SYNC_RETRIES times before raising RuntimeError.
        """
        for attempt in range(1, _MAX_SYNC_RETRIES + 1):
            # Step 1: Let the stream settle before reading (gives the WS time
            # to deliver the first batch of events before we call REST).
            await asyncio.sleep(0.25)

            # Step 2: Drain whatever has accumulated so far
            buffer = self._drain(internal_queue)

            # Step 3: Fetch REST snapshot (more events arrive during this call)
            self._log.info("Fetching REST snapshot (attempt %d)…", attempt)
            try:
                snapshot = await self._fetch_snapshot()
            except Exception as exc:
                self._log.warning("REST snapshot failed: %r", exc)
                await asyncio.sleep(2)
                continue

            snap_lid: int = snapshot["lastUpdateId"]

            # Drain events that arrived while we were fetching
            buffer.extend(self._drain(internal_queue))
            self._log.debug(
                "snap_lid=%d  buffered=%d events", snap_lid, len(buffer)
            )

            # Step 4-5: Drop events fully covered by the snapshot (u <= snap_lid)
            buffer = [e for e in buffer if self.get_final_update_id(e) > snap_lid]

            if not buffer:
                # The snapshot is more recent than every buffered event.
                # DO NOT refetch the snapshot — just wait for the stream to
                # produce at least one event beyond snap_lid, then proceed
                # with the same snapshot.  Refetching only makes things worse
                # because each new snapshot will be even more recent.
                self._log.info(
                    "All buffered events pre-date snapshot (snap_lid=%d). "
                    "Waiting for stream to advance…",
                    snap_lid,
                )
                while True:
                    try:
                        new_event = await asyncio.wait_for(
                            internal_queue.get(), timeout=5.0
                        )
                    except asyncio.TimeoutError:
                        self._log.warning(
                            "Timeout waiting for post-snapshot event. "
                            "Refetching snapshot…"
                        )
                        break  # break inner while → outer for loop refetches
                    if self.get_final_update_id(new_event) > snap_lid:
                        buffer = [new_event]
                        buffer.extend(self._drain(internal_queue))
                        break  # break inner while → fall through to bridging
                # If we timed out, buffer is still empty → continue outer loop
                if not buffer:
                    continue

            # Step 6: Find the bridging event
            bridging_idx: Optional[int] = None
            for i, e in enumerate(buffer):
                if self.is_bridging_event(e, snap_lid):
                    bridging_idx = i
                    break

            if bridging_idx is None:
                first_U = self.get_first_update_id(buffer[0])
                if first_U > snap_lid + 1:
                    # Stream has jumped past the snapshot — need a fresher one
                    self._log.warning(
                        "Snapshot too old: snap_lid=%d, first_buffered_U=%d. Refetching…",
                        snap_lid,
                        first_U,
                    )
                    continue
                # Edge case: bridging event not yet received
                self._log.info("Bridging event not yet in buffer, waiting…")
                await asyncio.sleep(0.25)
                continue

            # Step 7: Initialise book from snapshot
            self.book.init_from_snapshot(snapshot)

            # Apply events starting from the bridging event
            prev_u = snap_lid
            gap_detected = False
            for i, e in enumerate(buffer[bridging_idx:]):
                u_val = self.get_final_update_id(e)
                # Strict continuity check for all events after the bridging one
                if i > 0 and not self.check_continuity(e, prev_u):
                    self._log.error(
                        "Gap during initial sync: prev_u=%d  U=%d  pu=%s",
                        prev_u,
                        self.get_first_update_id(e),
                        e.get("pu"),
                    )
                    gap_detected = True
                    break
                self.book.apply_bids_asks(e.get("b", []), e.get("a", []))
                self.book.last_update_id = u_val
                prev_u = u_val

            if gap_detected:
                self.book.reset()
                continue

            self._log.info(
                "Sync OK — applied %d events from buffer, lastUpdateId=%d",
                len(buffer) - bridging_idx,
                self.book.last_update_id,
            )
            return  # success

        raise RuntimeError(
            f"[{self.MARKET}:{self.symbol}] Sync failed after {_MAX_SYNC_RETRIES} attempts"
        )

    # ------------------------------------------------------------------
    # Live event processing (Step 8–9)
    # ------------------------------------------------------------------

    async def _apply_live_event(self, event: dict) -> None:
        """
        Validate continuity, update the local book, and push the raw event
        onto the shared output queue for writers to consume.
        """
        if not self.check_continuity(event, self.book.last_update_id):
            raise RuntimeError(
                f"Gap detected in live stream: "
                f"prev_u={self.book.last_update_id}  "
                f"U={self.get_first_update_id(event)}  "
                f"pu={event.get('pu')}"
            )

        receive_time_ns: int = time.time_ns()

        self.book.apply_bids_asks(event.get("b", []), event.get("a", []))
        self.book.last_update_id = self.get_final_update_id(event)

        await self.event_queue.put(
            {
                "market": self.MARKET,
                "symbol": self.symbol,
                "exchange_time_ms": event.get("E"),        # Binance event time (ms)
                "receive_time_ns": receive_time_ns,         # local wall-clock (ns)
                "first_update_id": self.get_first_update_id(event),
                "last_update_id": self.get_final_update_id(event),
                "bids": event.get("b", []),
                "asks": event.get("a", []),
            }
        )

    # ------------------------------------------------------------------
    # Snapshot emission
    # ------------------------------------------------------------------

    async def _emit_snapshot(self, snapshot_type: str) -> None:
        """
        Serialize the entire current local book to the output queue.

        snapshot_type:
          "initial"    — emitted once immediately after a successful sync.
                         This is the required seed for backtesting replay.
          "checkpoint" — emitted every _CHECKPOINT_INTERVAL seconds during
                         live streaming.  Allows replay to start mid-day
                         without walking the full event history.

        The ParquetWriter routes these to separate snapshot files
        (snapshots_*.parquet) so they are never mixed with diff events.
        """
        bids, asks = self.book.to_snapshot_lists()
        self._log.info(
            "Emitting %s snapshot  bids=%d  asks=%d  lid=%d",
            snapshot_type,
            len(bids),
            len(asks),
            self.book.last_update_id,
        )
        await self.event_queue.put(
            {
                "event_type": f"snapshot_{snapshot_type}",  # "snapshot_initial" | "snapshot_checkpoint"
                "market": self.MARKET,
                "symbol": self.symbol,
                "snapshot_time_ns": time.time_ns(),
                "last_update_id": self.book.last_update_id,
                "bid_count": len(bids),
                "ask_count": len(asks),
                "bids": bids,
                "asks": asks,
            }
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _drain(queue: asyncio.Queue) -> list:
        """Non-blocking drain of all currently available items."""
        items = []
        while not queue.empty():
            try:
                items.append(queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return items

    async def _fetch_snapshot(self) -> dict:
        url = self.get_rest_url()
        params = self.get_rest_params()
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                resp.raise_for_status()
                return await resp.json()
