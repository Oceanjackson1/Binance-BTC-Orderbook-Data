#!/usr/bin/env python3
"""
Download BTC Order Book historical depth data from CoinGlass API.

Data type: Aggregated bid/ask volume at ±X% from mid price (30m default, configurable).
Available for both Binance Spot and Futures, plus cross-exchange aggregated.

Output: CSV files saved to ~/Desktop/BTC-Historical-Data/coinglass/

Usage:
    python scripts/download_coinglass_orderbook.py --api-key YOUR_KEY
    python scripts/download_coinglass_orderbook.py --api-key YOUR_KEY --interval 30m
    python scripts/download_coinglass_orderbook.py --api-key YOUR_KEY --start 2026-01-01 --end 2026-03-01
"""

import argparse
import asyncio
import csv
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("coinglass")

API_BASE = "https://open-api-v4.coinglass.com/api"
MAX_LIMIT = 4500
RATE_LIMIT_DELAY = 0.8  # seconds between requests (80 req/min limit)

# Range levels: ±1%, ±2%, ±3%, ±5%, ±10% from mid price
RANGES = [1, 2, 3, 5, 10]

# Download tasks: (name, endpoint, extra_params, response_fields)
TASKS = [
    # Binance Futures per-exchange
    (
        "futures_binance",
        "/futures/orderbook/ask-bids-history",
        {"exchange": "Binance", "symbol": "BTCUSDT"},
        ["bids_usd", "bids_quantity", "asks_usd", "asks_quantity"],
    ),
    # Binance Spot per-exchange
    (
        "spot_binance",
        "/spot/orderbook/ask-bids-history",
        {"exchange": "Binance", "symbol": "BTCUSDT"},
        ["bids_usd", "bids_quantity", "asks_usd", "asks_quantity"],
    ),
    # Cross-exchange aggregated Futures
    (
        "futures_aggregated",
        "/futures/orderbook/aggregated-ask-bids-history",
        {"symbol": "BTC", "exchange_list": "Binance,OKX,Bybit,Bitget,Gate,dYdX,Hyperliquid"},
        [
            "aggregated_bids_usd",
            "aggregated_bids_quantity",
            "aggregated_asks_usd",
            "aggregated_asks_quantity",
        ],
    ),
    # Cross-exchange aggregated Spot
    (
        "spot_aggregated",
        "/spot/orderbook/aggregated-ask-bids-history",
        {"symbol": "BTC", "exchange_list": "Binance,OKX,Bybit,Coinbase,Bitfinex,Gate"},
        [
            "aggregated_bids_usd",
            "aggregated_bids_quantity",
            "aggregated_asks_usd",
            "aggregated_asks_quantity",
        ],
    ),
]


async def fetch_data(
    session: aiohttp.ClientSession,
    api_key: str,
    endpoint: str,
    params: dict,
) -> list:
    """Fetch data from a single CoinGlass API endpoint."""
    headers = {"accept": "application/json", "CG-API-KEY": api_key}
    url = f"{API_BASE}{endpoint}"

    async with session.get(url, headers=headers, params=params) as resp:
        data = await resp.json()

    if data.get("code") != "0":
        log.warning("API error: %s (endpoint=%s, params=%s)", data.get("msg"), endpoint, params)
        return []

    return data.get("data", [])


def filter_by_time(records: list, start_ms: int, end_ms: int) -> list:
    """Filter records to only include those within the requested time range."""
    return [r for r in records if start_ms <= r["time"] < end_ms]


def write_csv(filepath: Path, records: list, fields: list, range_val: int):
    """Write records to CSV with readable timestamps."""
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with filepath.open("w", newline="") as f:
        writer = csv.writer(f)
        # Header
        writer.writerow(["timestamp_utc", "timestamp_ms", "range_pct"] + fields)
        # Data
        for rec in records:
            ts = datetime.fromtimestamp(rec["time"] / 1000, tz=timezone.utc)
            row = [
                ts.strftime("%Y-%m-%d %H:%M:%S"),
                rec["time"],
                range_val,
            ]
            for field in fields:
                row.append(rec.get(field, ""))
            writer.writerow(row)


async def download_all(
    api_key: str,
    output_dir: Path,
    start_dt: datetime,
    end_dt: datetime,
    interval: str = "30m",
):
    """Download all order book depth data from CoinGlass."""
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    log.info("=" * 60)
    log.info("CoinGlass Order Book Depth Downloader")
    log.info("=" * 60)
    log.info("Period: %s to %s", start_dt.date(), end_dt.date())
    log.info("Interval: %s", interval)
    log.info("Ranges: ±%s%%", ", ±".join(str(r) for r in RANGES))
    log.info("Data types: %d × %d ranges = %d API calls", len(TASKS), len(RANGES), len(TASKS) * len(RANGES))
    log.info("Output: %s", output_dir)
    log.info("=" * 60)

    timeout = aiohttp.ClientTimeout(total=60)
    total_records = 0
    total_files = 0

    async with aiohttp.ClientSession(timeout=timeout) as session:
        for task_name, endpoint, extra_params, fields in TASKS:
            log.info("\n--- %s ---", task_name)

            for range_val in RANGES:
                params = {
                    **extra_params,
                    "interval": interval,
                    "limit": MAX_LIMIT,
                    "range": range_val,
                }

                log.info("  Fetching %s range=±%d%% ...", task_name, range_val)
                records = await fetch_data(session, api_key, endpoint, params)

                if not records:
                    log.warning("  No data returned for %s range=%d", task_name, range_val)
                    await asyncio.sleep(RATE_LIMIT_DELAY)
                    continue

                # Filter to requested time range
                filtered = filter_by_time(records, start_ms, end_ms)
                log.info(
                    "  Got %d records total, %d in requested range",
                    len(records),
                    len(filtered),
                )

                if filtered:
                    filename = f"{task_name}_range{range_val}pct_{interval}.csv"
                    filepath = output_dir / task_name / filename
                    write_csv(filepath, filtered, fields, range_val)
                    total_records += len(filtered)
                    total_files += 1
                    log.info("  Saved → %s", filepath)

                await asyncio.sleep(RATE_LIMIT_DELAY)

    # Also create a combined file per task with all ranges
    log.info("\n--- Creating combined files ---")
    for task_name, _, _, fields in TASKS:
        task_dir = output_dir / task_name
        if not task_dir.exists():
            continue

        combined_path = output_dir / f"{task_name}_all_ranges_{interval}.csv"
        all_rows = []

        for range_val in RANGES:
            csv_file = task_dir / f"{task_name}_range{range_val}pct_{interval}.csv"
            if csv_file.exists():
                with csv_file.open() as f:
                    reader = csv.reader(f)
                    header = next(reader)
                    for row in reader:
                        all_rows.append(row)

        if all_rows:
            # Sort by timestamp then range
            all_rows.sort(key=lambda r: (r[1], int(r[2])))
            with combined_path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(header)
                writer.writerows(all_rows)
            log.info("  Combined → %s (%d rows)", combined_path.name, len(all_rows))

    log.info("\n" + "=" * 60)
    log.info("DOWNLOAD COMPLETE")
    log.info("=" * 60)
    log.info("  Total records: %d", total_records)
    log.info("  Total files:   %d", total_files)
    log.info("  Output:        %s", output_dir)
    log.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Download BTC order book depth history from CoinGlass"
    )
    parser.add_argument("--start", default="2026-01-01", help="Start date (inclusive)")
    parser.add_argument("--end", default="2026-03-01", help="End date (exclusive)")
    parser.add_argument("--interval", default="30m", help="Data interval: 30m, 1h, 2h, 4h, 12h, 1d")
    parser.add_argument(
        "--output",
        default=str(Path.home() / "Desktop" / "BTC-Historical-Data" / "coinglass"),
        help="Output directory",
    )
    parser.add_argument(
        "--api-key",
        required=True,
        help="CoinGlass API key (get from https://www.coinglass.com/account)",
    )
    args = parser.parse_args()

    start_dt = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    output_dir = Path(args.output)

    asyncio.run(download_all(args.api_key, output_dir, start_dt, end_dt, args.interval))


if __name__ == "__main__":
    main()
