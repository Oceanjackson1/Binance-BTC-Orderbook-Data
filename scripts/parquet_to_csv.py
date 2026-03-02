"""
Convert order book Parquet files to CSV.

Snapshots  → one row per price level  (complete order book ladder)
Events     → one row per price level change  (change log)

Both formats are "exploded": nested bids/asks lists are flattened so every
CSV row represents a single price level, making the files compatible with
Excel, pandas, DuckDB, and any other tool without special handling.

Usage:
    python scripts/parquet_to_csv.py --data-dir /path/to/data
    python scripts/parquet_to_csv.py --data-dir /Users/ocean/Desktop/btc-orderbook-sample
"""

import argparse
import csv
import logging
import sys
from pathlib import Path

import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s — %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("csv-export")

# ─────────────────────────────────────────────────────────────────────────────
# Snapshot conversion
# Parquet row (1 snapshot) → N CSV rows (one per price level)
# ─────────────────────────────────────────────────────────────────────────────

SNAPSHOT_CSV_HEADER = [
    "snapshot_time_ns",
    "market",
    "symbol",
    "snapshot_type",
    "last_update_id",
    "side",        # "bid" | "ask"
    "price",
    "quantity",
]


def convert_snapshot(parquet_path: Path) -> Path:
    csv_path = parquet_path.with_suffix(".csv")
    table = pq.read_table(parquet_path)
    rows = table.to_pydict()
    n_snapshots = table.num_rows

    written = 0
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(SNAPSHOT_CSV_HEADER)

        for i in range(n_snapshots):
            base = [
                rows["snapshot_time_ns"][i],
                rows["market"][i],
                rows["symbol"][i],
                rows["snapshot_type"][i],
                rows["last_update_id"][i],
            ]
            # Bids — highest price first (already sorted by collector)
            for price, qty in rows["bids"][i]:
                writer.writerow(base + ["bid", price, qty])
                written += 1
            # Asks — lowest price first
            for price, qty in rows["asks"][i]:
                writer.writerow(base + ["ask", price, qty])
                written += 1

    log.info(
        "snapshot  %s  →  %s  (%d price levels)",
        parquet_path.name, csv_path.name, written,
    )
    return csv_path


# ─────────────────────────────────────────────────────────────────────────────
# Event conversion
# Parquet row (1 diff event) → M CSV rows (one per changed price level)
# ─────────────────────────────────────────────────────────────────────────────

EVENTS_CSV_HEADER = [
    "exchange_time_ms",
    "transaction_time_ms",   # futures only; empty for spot
    "receive_time_ns",
    "market",
    "symbol",
    "first_update_id",
    "last_update_id",
    "prev_final_update_id",  # futures only; empty for spot
    "side",                  # "bid" | "ask"
    "price",
    "quantity",              # "0" means level removed
]


def convert_events(parquet_path: Path) -> Path:
    csv_path = parquet_path.with_suffix(".csv")
    table = pq.read_table(parquet_path)
    rows = table.to_pydict()
    n_events = table.num_rows

    written = 0
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(EVENTS_CSV_HEADER)

        for i in range(n_events):
            base = [
                rows["exchange_time_ms"][i],
                rows["transaction_time_ms"][i] or "",   # null → empty
                rows["receive_time_ns"][i],
                rows["market"][i],
                rows["symbol"][i],
                rows["first_update_id"][i],
                rows["last_update_id"][i],
                rows["prev_final_update_id"][i] or "",  # null → empty
            ]
            for price, qty in rows["bids"][i]:
                writer.writerow(base + ["bid", price, qty])
                written += 1
            for price, qty in rows["asks"][i]:
                writer.writerow(base + ["ask", price, qty])
                written += 1

    log.info(
        "events    %s  →  %s  (%d level-change rows)",
        parquet_path.name, csv_path.name, written,
    )
    return csv_path


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def run(data_dir: Path) -> None:
    snap_files  = sorted(data_dir.rglob("snapshots_*.parquet"))
    event_files = sorted(data_dir.rglob("events_*.parquet"))

    if not snap_files and not event_files:
        log.error("No Parquet files found under %s", data_dir)
        sys.exit(1)

    log.info("Found %d snapshot file(s) and %d event file(s)", len(snap_files), len(event_files))
    log.info("")

    total_snap_rows = 0
    total_event_rows = 0

    for f in snap_files:
        csv_path = convert_snapshot(f)
        total_snap_rows += sum(1 for _ in csv_path.open()) - 1  # subtract header

    for f in event_files:
        csv_path = convert_events(f)
        total_event_rows += sum(1 for _ in csv_path.open()) - 1

    log.info("")
    log.info("Done.")
    log.info("  Snapshot CSV rows : %d  (one row = one price level)", total_snap_rows)
    log.info("  Events CSV rows   : %d  (one row = one price-level change)", total_event_rows)
    log.info("")
    log.info("Files written alongside Parquet files in: %s", data_dir.resolve())


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Root data directory (e.g. /Users/ocean/Desktop/btc-orderbook-sample)",
    )
    args = p.parse_args()
    run(args.data_dir)
