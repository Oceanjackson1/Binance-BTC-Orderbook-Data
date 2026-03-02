#!/usr/bin/env python3
"""
Download historical BTC data from Binance public data portal (data.binance.vision).

Available data for the requested period:
  - Futures: bookDepth (aggregated depth), aggTrades, trades, metrics
  - Spot: aggTrades, trades

Output directory: ~/Desktop/BTC-Historical-Data/

Usage:
    python scripts/download_historical.py
    python scripts/download_historical.py --start 2026-01-01 --end 2026-03-01
"""

import argparse
import asyncio
import hashlib
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("downloader")

BASE_URL = "https://data.binance.vision/data"

# Available data types (verified via HTTP HEAD)
DOWNLOAD_TASKS = [
    # (market, category, data_type, symbol, file_template)
    ("futures", "futures/um", "bookDepth", "BTCUSDT", "{sym}-bookDepth-{d}.zip"),
    ("futures", "futures/um", "aggTrades", "BTCUSDT", "{sym}-aggTrades-{d}.zip"),
    ("futures", "futures/um", "trades", "BTCUSDT", "{sym}-trades-{d}.zip"),
    ("futures", "futures/um", "metrics", "BTCUSDT", "{sym}-metrics-{d}.zip"),
    ("spot", "spot", "aggTrades", "BTCUSDT", "{sym}-aggTrades-{d}.zip"),
    ("spot", "spot", "trades", "BTCUSDT", "{sym}-trades-{d}.zip"),
]

MAX_CONCURRENT = 8


def daterange(start: date, end: date):
    """Yield dates from start (inclusive) to end (exclusive)."""
    d = start
    while d < end:
        yield d
        d += timedelta(days=1)


async def download_file(
    session: aiohttp.ClientSession,
    url: str,
    dest: Path,
    semaphore: asyncio.Semaphore,
) -> bool:
    """Download a single file. Returns True on success."""
    if dest.exists():
        log.debug("SKIP (exists): %s", dest.name)
        return True

    async with semaphore:
        try:
            async with session.get(url) as resp:
                if resp.status == 404:
                    return False
                if resp.status != 200:
                    log.warning("HTTP %d: %s", resp.status, url)
                    return False

                dest.parent.mkdir(parents=True, exist_ok=True)
                content = await resp.read()
                dest.write_bytes(content)
                size_mb = len(content) / (1024 * 1024)
                log.info("OK  %s (%.1f MB)", dest.name, size_mb)
                return True
        except Exception as e:
            log.warning("FAIL %s: %s", dest.name, e)
            return False


async def download_checksum(
    session: aiohttp.ClientSession,
    url: str,
    dest: Path,
    semaphore: asyncio.Semaphore,
) -> bool:
    """Download the CHECKSUM file for verification."""
    checksum_url = url + ".CHECKSUM"
    checksum_dest = Path(str(dest) + ".CHECKSUM")

    if checksum_dest.exists():
        return True

    async with semaphore:
        try:
            async with session.get(checksum_url) as resp:
                if resp.status == 200:
                    checksum_dest.write_bytes(await resp.read())
                    return True
        except Exception:
            pass
    return False


async def run_downloads(
    output_dir: Path,
    start: date,
    end: date,
) -> dict:
    """Download all available data types for the date range."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    stats = {"success": 0, "skipped": 0, "failed": 0}

    dates = list(daterange(start, end))
    total_days = len(dates)

    log.info("=" * 60)
    log.info("Binance Historical BTC Data Downloader")
    log.info("=" * 60)
    log.info("Period: %s to %s (%d days)", start, end, total_days)
    log.info("Output: %s", output_dir)
    log.info("Data types: %d", len(DOWNLOAD_TASKS))
    log.info("Total files to check: %d", total_days * len(DOWNLOAD_TASKS))
    log.info("=" * 60)

    timeout = aiohttp.ClientTimeout(total=300)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for task_idx, (market, category, dtype, sym, tpl) in enumerate(DOWNLOAD_TASKS):
            log.info(
                "\n[%d/%d] Downloading %s/%s %s...",
                task_idx + 1,
                len(DOWNLOAD_TASKS),
                market,
                sym,
                dtype,
            )

            dest_dir = output_dir / market / sym / dtype
            dest_dir.mkdir(parents=True, exist_ok=True)

            # Process dates in batches
            batch_size = 10
            for batch_start in range(0, total_days, batch_size):
                batch_dates = dates[batch_start : batch_start + batch_size]
                tasks = []

                for d in batch_dates:
                    date_str = d.strftime("%Y-%m-%d")
                    filename = tpl.format(sym=sym, d=date_str)
                    url = f"{BASE_URL}/{category}/daily/{dtype}/{sym}/{filename}"
                    dest = dest_dir / filename

                    if dest.exists():
                        stats["skipped"] += 1
                        continue

                    tasks.append(download_file(session, url, dest, semaphore))
                    tasks.append(
                        download_checksum(session, url, dest, semaphore)
                    )

                if tasks:
                    results = await asyncio.gather(*tasks)
                    # Count data file results (every other one)
                    for i in range(0, len(results), 2):
                        if results[i]:
                            stats["success"] += 1
                        else:
                            stats["failed"] += 1

                progress = min(batch_start + batch_size, total_days)
                log.info(
                    "  Progress: %d/%d days", progress, total_days
                )

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Download historical BTC data from Binance"
    )
    parser.add_argument(
        "--start",
        default="2026-01-01",
        help="Start date (inclusive), default: 2026-01-01",
    )
    parser.add_argument(
        "--end",
        default="2026-03-01",
        help="End date (exclusive), default: 2026-03-01",
    )
    parser.add_argument(
        "--output",
        default=str(Path.home() / "Desktop" / "BTC-Historical-Data"),
        help="Output directory",
    )
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    output_dir = Path(args.output)

    stats = asyncio.run(run_downloads(output_dir, start, end))

    log.info("\n" + "=" * 60)
    log.info("DOWNLOAD COMPLETE")
    log.info("=" * 60)
    log.info("  Success:  %d files", stats["success"])
    log.info("  Skipped:  %d files (already exist)", stats["skipped"])
    log.info("  Failed:   %d files (not available)", stats["failed"])
    log.info("  Output:   %s", output_dir)
    log.info("=" * 60)

    # Show directory sizes
    for market_dir in sorted(output_dir.iterdir()):
        if market_dir.is_dir():
            total_size = sum(f.stat().st_size for f in market_dir.rglob("*.zip"))
            log.info(
                "  %s: %.1f MB",
                market_dir.name,
                total_size / (1024 * 1024),
            )


if __name__ == "__main__":
    main()
