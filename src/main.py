"""
Entry point for the Binance BTC order book collector.

Usage:
    python -m src.main

Configuration:
    config/symbols.yaml  — which symbols to collect
    .env                 — DATA_DIR, TIMESCALE_DSN, LOG_LEVEL

The process runs forever, reconnecting automatically on any failure.
Ctrl-C triggers a graceful shutdown that flushes remaining Parquet buffers.
"""

import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

from src.collector.spot_collector import SpotCollector
from src.collector.futures_collector import FuturesCollector
from src.event_fanout import EventFanout
from src.writer.parquet_writer import ParquetWriter
from src.writer.timescale_writer import TimescaleWriter


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )


async def main() -> None:
    load_dotenv()

    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    log = logging.getLogger("main")

    # --- Load symbol config ---
    config_path = Path("config/symbols.yaml")
    with config_path.open() as f:
        config = yaml.safe_load(f)

    spot_symbols: list = config.get("spot", [])
    futures_symbols: list = config.get("futures", [])

    log.info(
        "Starting collector  spot=%s  futures=%s",
        spot_symbols,
        futures_symbols,
    )

    # Collectors publish into one queue, then EventFanout duplicates every
    # message into a dedicated queue per writer.
    ingest_queue: asyncio.Queue = asyncio.Queue(maxsize=50_000)
    parquet_queue: asyncio.Queue = asyncio.Queue(maxsize=50_000)
    db_queue: asyncio.Queue = asyncio.Queue(maxsize=50_000)

    # --- Build collectors ---
    collectors = []
    for sym in spot_symbols:
        collectors.append(SpotCollector(symbol=sym, event_queue=ingest_queue))
    for sym in futures_symbols:
        collectors.append(FuturesCollector(symbol=sym, event_queue=ingest_queue))

    # --- Build writers ---
    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    parquet_writer = ParquetWriter(data_dir=data_dir, event_queue=parquet_queue)

    timescale_dsn = os.getenv("TIMESCALE_DSN")
    timescale_writer = (
        TimescaleWriter(
            dsn=timescale_dsn,
            collectors=collectors,
            event_queue=db_queue,
        )
        if timescale_dsn
        else None
    )

    fanout_targets = [parquet_queue]
    if timescale_writer:
        fanout_targets.append(db_queue)
    else:
        log.info("TIMESCALE_DSN not set — running Parquet-only mode.")

    event_fanout = EventFanout(
        source_queue=ingest_queue,
        target_queues=fanout_targets,
    )

    # --- Create asyncio tasks ---
    collector_tasks = []
    service_tasks = []

    for collector in collectors:
        collector_tasks.append(
            asyncio.create_task(
                collector.run(),
                name=f"collector-{collector.MARKET}-{collector.symbol}",
            )
        )

    service_tasks.append(
        asyncio.create_task(event_fanout.run(), name="event-fanout")
    )
    service_tasks.append(
        asyncio.create_task(parquet_writer.run(), name="parquet-writer")
    )

    if timescale_writer:
        service_tasks.append(
            asyncio.create_task(timescale_writer.run(), name="timescale-writer")
        )

    # --- Graceful shutdown on SIGINT / SIGTERM ---
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _signal_handler():
        log.info("Shutdown signal received — cancelling tasks…")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # Wait until shutdown is requested or a task raises unexpectedly
    done_task = asyncio.create_task(shutdown_event.wait(), name="shutdown-watcher")
    all_tasks = collector_tasks + service_tasks + [done_task]

    try:
        done, pending = await asyncio.wait(
            all_tasks, return_when=asyncio.FIRST_COMPLETED
        )

        # Log any tasks that finished unexpectedly (before shutdown)
        for t in done:
            if t is not done_task and not t.cancelled():
                exc = t.exception()
                if exc:
                    log.error("Task %s raised: %r", t.get_name(), exc)

    finally:
        log.info("Stopping collectors…")
        for t in collector_tasks:
            t.cancel()
        done_task.cancel()
        await asyncio.gather(*collector_tasks, return_exceptions=True)

        log.info("Waiting for queues to drain…")
        await _wait_for_queues_to_drain(
            queues=[ingest_queue, parquet_queue, db_queue],
            timeout=10,
        )

        log.info("Cancelling service tasks…")
        for t in service_tasks:
            t.cancel()
        await asyncio.gather(*service_tasks, done_task, return_exceptions=True)
        log.info("All tasks cancelled — exiting.")


async def _wait_for_queues_to_drain(
    queues: list[asyncio.Queue],
    timeout: float,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if all(queue.empty() for queue in queues):
            return
        await asyncio.sleep(0.25)


if __name__ == "__main__":
    asyncio.run(main())
