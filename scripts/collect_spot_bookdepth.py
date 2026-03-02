#!/usr/bin/env python3
"""
Binance Spot bookDepth-equivalent Collector.

Maintains a local order book via WebSocket diff depth stream + REST snapshots,
then periodically aggregates bid/ask volume by percentage distance from mid price
(±1%, ±2%, ±3%, ±4%, ±5%) — matching the format of Binance Futures bookDepth.

Since Binance does NOT publish historical spot bookDepth on data.binance.vision,
this script collects spot bookDepth-equivalent data in real-time.

Output: CSV files saved to ~/Desktop/BTC-Historical-Data/spot/BTCUSDT/bookDepth/

Usage:
    python3 scripts/collect_spot_bookdepth.py
    python3 scripts/collect_spot_bookdepth.py --symbol BTCUSDT --interval 30
    python3 scripts/collect_spot_bookdepth.py --duration 3600   # run for 1 hour
"""

import argparse
import asyncio
import csv
import json
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("spot_bookdepth")

PERCENTAGES = [1, 2, 3, 4, 5]
REST_LIMIT = 5000
MAX_SYNC_RETRIES = 10


class SpotBookDepthCollector:
    """Collect spot bookDepth-equivalent data from Binance."""

    def __init__(
        self,
        symbol: str,
        output_dir: Path,
        snapshot_interval: int = 30,
        duration: Optional[int] = None,
    ):
        self.symbol = symbol.upper()
        self.output_dir = output_dir
        self.snapshot_interval = snapshot_interval
        self.duration = duration
        self.bids: dict[str, str] = {}  # price_str -> qty_str
        self.asks: dict[str, str] = {}
        self.last_update_id: int = 0
        self.is_synced: bool = False
        self._stop = False
        self._total_snapshots = 0
        self._current_file: Optional[Path] = None
        self._current_writer = None
        self._current_fh = None
        self._current_date: Optional[str] = None

    def _get_ws_url(self) -> str:
        return f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@depth@100ms"

    def _get_rest_url(self) -> str:
        return "https://api.binance.com/api/v3/depth"

    async def _fetch_rest_snapshot(self) -> dict:
        """Fetch order book snapshot from REST API."""
        async with aiohttp.ClientSession() as session:
            params = {"symbol": self.symbol, "limit": REST_LIMIT}
            async with session.get(
                self._get_rest_url(),
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                resp.raise_for_status()
                return await resp.json()

    def _apply_snapshot(self, snapshot: dict):
        """Initialize local order book from REST snapshot."""
        self.bids.clear()
        self.asks.clear()
        for price, qty in snapshot["bids"]:
            if float(qty) > 0:
                self.bids[price] = qty
        for price, qty in snapshot["asks"]:
            if float(qty) > 0:
                self.asks[price] = qty
        self.last_update_id = snapshot["lastUpdateId"]

    def _apply_event(self, event: dict):
        """Apply a depth update event to the local order book."""
        for price, qty in event.get("b", []):
            if float(qty) == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
        for price, qty in event.get("a", []):
            if float(qty) == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty
        self.last_update_id = event["u"]

    def _compute_bookdepth(self) -> list[dict]:
        """Compute bookDepth-equivalent aggregated data from local order book."""
        if not self.bids or not self.asks:
            return []

        bid_prices = sorted(
            [(float(p), float(q)) for p, q in self.bids.items()], reverse=True
        )
        ask_prices = sorted(
            [(float(p), float(q)) for p, q in self.asks.items()]
        )

        best_bid = bid_prices[0][0]
        best_ask = ask_prices[0][0]
        mid = (best_bid + best_ask) / 2

        now = datetime.now(timezone.utc)
        ts_str = now.strftime("%Y-%m-%d %H:%M:%S")
        rows = []

        for pct in PERCENTAGES:
            # Bid side: cumulative depth within pct% below mid
            bid_threshold = mid * (1 - pct / 100)
            bid_depth = 0.0
            bid_notional = 0.0
            for price, qty in bid_prices:
                if price >= bid_threshold:
                    bid_depth += qty
                    bid_notional += price * qty
                else:
                    break

            rows.append(
                {
                    "timestamp": ts_str,
                    "percentage": -pct,
                    "depth": f"{bid_depth:.8f}",
                    "notional": f"{bid_notional:.8f}",
                }
            )

        for pct in PERCENTAGES:
            # Ask side: cumulative depth within pct% above mid
            ask_threshold = mid * (1 + pct / 100)
            ask_depth = 0.0
            ask_notional = 0.0
            for price, qty in ask_prices:
                if price <= ask_threshold:
                    ask_depth += qty
                    ask_notional += price * qty
                else:
                    break

            rows.append(
                {
                    "timestamp": ts_str,
                    "percentage": pct,
                    "depth": f"{ask_depth:.8f}",
                    "notional": f"{ask_notional:.8f}",
                }
            )

        # Sort: -5,-4,-3,-2,-1, 1,2,3,4,5
        rows.sort(key=lambda r: (0 if r["percentage"] < 0 else 1, abs(r["percentage"])))
        # Re-sort to match futures bookDepth order: -5,-4,-3,-2,-1,1,2,3,4,5
        neg_rows = [r for r in rows if r["percentage"] < 0]
        pos_rows = [r for r in rows if r["percentage"] > 0]
        neg_rows.sort(key=lambda r: r["percentage"])  # -5,-4,-3,-2,-1
        pos_rows.sort(key=lambda r: r["percentage"])  # 1,2,3,4,5
        return neg_rows + pos_rows

    def _ensure_csv_file(self, date_str: str):
        """Open/rotate CSV file for the given date."""
        if self._current_date == date_str:
            return

        if self._current_fh:
            self._current_fh.close()

        self.output_dir.mkdir(parents=True, exist_ok=True)
        filepath = self.output_dir / f"{self.symbol}-bookDepth-{date_str}.csv"
        file_exists = filepath.exists()

        self._current_fh = filepath.open("a", newline="")
        self._current_writer = csv.writer(self._current_fh)
        if not file_exists:
            self._current_writer.writerow(
                ["timestamp", "percentage", "depth", "notional"]
            )
        self._current_file = filepath
        self._current_date = date_str
        log.info("Writing to %s", filepath.name)

    def _write_snapshot(self, rows: list[dict]):
        """Write a bookDepth snapshot to CSV."""
        date_str = rows[0]["timestamp"][:10]  # YYYY-MM-DD
        self._ensure_csv_file(date_str)
        for row in rows:
            self._current_writer.writerow(
                [row["timestamp"], row["percentage"], row["depth"], row["notional"]]
            )
        self._current_fh.flush()
        self._total_snapshots += 1

    async def run(self):
        """Main collection loop."""
        log.info("=" * 60)
        log.info("Binance Spot bookDepth Collector")
        log.info("=" * 60)
        log.info("Symbol: %s", self.symbol)
        log.info("Snapshot interval: %ds", self.snapshot_interval)
        log.info("Percentages: +/-%s%%", ", +/-".join(str(p) for p in PERCENTAGES))
        log.info("Output: %s", self.output_dir)
        if self.duration:
            log.info("Duration: %ds", self.duration)
        log.info("=" * 60)

        start_time = time.monotonic()

        while not self._stop:
            try:
                await self._run_once(start_time)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error("Connection error: %s. Reconnecting in 5s...", e)
                await asyncio.sleep(5)

        if self._current_fh:
            self._current_fh.close()

        log.info("\n" + "=" * 60)
        log.info("COLLECTION COMPLETE")
        log.info("Total snapshots: %d", self._total_snapshots)
        log.info("Total rows: %d", self._total_snapshots * 10)
        log.info("Output: %s", self.output_dir)
        log.info("=" * 60)

    async def _run_once(self, start_time: float):
        """Single WebSocket connection lifecycle."""
        ws_url = self._get_ws_url()
        log.info("Connecting to %s", ws_url)
        self.is_synced = False
        buffer: list[dict] = []

        async with websockets.connect(
            ws_url,
            ping_interval=20,
            ping_timeout=60,
            max_size=10 * 1024 * 1024,
        ) as ws:
            # Phase 1: Sync
            await self._sync(ws, buffer)
            log.info(
                "SYNCED — local book: %d bids, %d asks, lastUpdateId=%d",
                len(self.bids),
                len(self.asks),
                self.last_update_id,
            )

            # Take first snapshot immediately
            rows = self._compute_bookdepth()
            if rows:
                self._write_snapshot(rows)
                log.info("Snapshot #%d written (10 rows)", self._total_snapshots)

            last_snapshot = time.monotonic()
            connection_open = time.monotonic()

            # Phase 2: Live streaming + periodic snapshots
            while not self._stop:
                # Check duration limit
                if self.duration and (time.monotonic() - start_time) >= self.duration:
                    log.info("Duration limit reached.")
                    self._stop = True
                    break

                # Proactive reconnect before 24h limit
                if time.monotonic() - connection_open > 82800:  # 23h
                    log.info("Approaching 24h limit, reconnecting...")
                    break

                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=30)
                except asyncio.TimeoutError:
                    log.warning("No data for 30s, reconnecting...")
                    break

                event = json.loads(raw)

                # Continuity check
                U = event["U"]
                u = event["u"]
                if U != self.last_update_id + 1:
                    if U > self.last_update_id + 1:
                        log.warning(
                            "Gap detected: expected U=%d, got U=%d. Re-syncing...",
                            self.last_update_id + 1,
                            U,
                        )
                        break
                    # U < last_update_id + 1: overlapping event, skip
                    continue

                self._apply_event(event)

                # Periodic snapshot
                now = time.monotonic()
                if now - last_snapshot >= self.snapshot_interval:
                    rows = self._compute_bookdepth()
                    if rows:
                        self._write_snapshot(rows)
                        if self._total_snapshots % 10 == 0:
                            log.info(
                                "Snapshot #%d — bids=%d, asks=%d",
                                self._total_snapshots,
                                len(self.bids),
                                len(self.asks),
                            )
                    last_snapshot = now

    async def _sync(self, ws, buffer: list):
        """Synchronize local order book with Binance."""
        for attempt in range(1, MAX_SYNC_RETRIES + 1):
            # Buffer incoming events
            await asyncio.sleep(0.3)
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=0.1)
                    buffer.append(json.loads(raw))
                except asyncio.TimeoutError:
                    break

            # Fetch REST snapshot
            log.info("Fetching REST snapshot (attempt %d)...", attempt)
            try:
                snapshot = await self._fetch_rest_snapshot()
            except Exception as e:
                log.warning("REST snapshot failed: %s", e)
                await asyncio.sleep(2)
                continue

            snap_lid = snapshot["lastUpdateId"]
            self._apply_snapshot(snapshot)

            # Drain more events
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=0.1)
                    buffer.append(json.loads(raw))
                except asyncio.TimeoutError:
                    break

            # Drop events fully before snapshot
            buffer = [e for e in buffer if e["u"] > snap_lid]

            if not buffer:
                log.info("Waiting for post-snapshot events...")
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    event = json.loads(raw)
                    if event["u"] > snap_lid:
                        buffer = [event]
                except asyncio.TimeoutError:
                    continue

            if not buffer:
                continue

            # Find bridging event: U <= snap_lid+1 AND u >= snap_lid+1
            bridging_idx = None
            for i, e in enumerate(buffer):
                if e["U"] <= snap_lid + 1 and e["u"] >= snap_lid + 1:
                    bridging_idx = i
                    break

            if bridging_idx is None:
                if buffer[0]["U"] > snap_lid + 1:
                    log.warning("Snapshot too old, refetching...")
                    buffer.clear()
                    continue
                continue

            # Apply events from bridging point
            for e in buffer[bridging_idx:]:
                self._apply_event(e)

            self.is_synced = True
            buffer.clear()
            return

        raise RuntimeError(f"Sync failed after {MAX_SYNC_RETRIES} attempts")


def main():
    parser = argparse.ArgumentParser(
        description="Collect Binance Spot bookDepth-equivalent data"
    )
    parser.add_argument("--symbol", default="BTCUSDT", help="Trading pair")
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Snapshot interval in seconds (default: 30)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Duration in seconds (default: run forever)",
    )
    parser.add_argument(
        "--output",
        default=str(
            Path.home() / "Desktop" / "BTC-Historical-Data" / "spot" / "BTCUSDT" / "bookDepth"
        ),
        help="Output directory",
    )
    args = parser.parse_args()

    collector = SpotBookDepthCollector(
        symbol=args.symbol,
        output_dir=Path(args.output),
        snapshot_interval=args.interval,
        duration=args.duration,
    )

    loop = asyncio.new_event_loop()

    def handle_signal(*_):
        collector._stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        loop.run_until_complete(collector.run())
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        loop.close()


if __name__ == "__main__":
    main()
