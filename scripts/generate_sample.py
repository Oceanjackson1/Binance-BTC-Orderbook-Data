"""
Generate sample order book data by running the collectors for a fixed duration,
then validate the output files.

Usage:
    python scripts/generate_sample.py [--duration SECONDS] [--out-dir PATH]

Defaults:
    duration = 90 seconds
    out-dir  = data/sample
"""

import argparse
import asyncio
import logging
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pyarrow.parquet as pq

# ── Path setup ─────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.collector.spot_collector import SpotCollector
from src.collector.futures_collector import FuturesCollector
from src.writer.parquet_writer import ParquetWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("sample")

SEPARATOR = "─" * 70


# ═══════════════════════════════════════════════════════════════════════════
# Collection
# ═══════════════════════════════════════════════════════════════════════════

async def collect(duration: int, data_dir: Path) -> list:
    """Run collectors for `duration` seconds, return collector list."""
    log.info(SEPARATOR)
    log.info("COLLECTION PHASE  duration=%ds  out=%s", duration, data_dir)
    log.info(SEPARATOR)

    event_queue: asyncio.Queue = asyncio.Queue(maxsize=50_000)

    collectors = [
        SpotCollector("BTCUSDT", event_queue),
        FuturesCollector("BTCUSDT", event_queue),
    ]
    writer = ParquetWriter(data_dir=data_dir, event_queue=event_queue)

    tasks = [asyncio.create_task(c.run()) for c in collectors]
    tasks.append(asyncio.create_task(writer.run()))

    log.info("Collectors and writer started.  Waiting %d seconds…", duration)
    start = time.monotonic()

    # Progress ticker
    async def ticker():
        while True:
            await asyncio.sleep(10)
            elapsed = time.monotonic() - start
            live = [c for c in collectors if c.is_live]
            log.info(
                "  elapsed=%.0fs  live_collectors=%d/%d  "
                "queue_depth=%d",
                elapsed, len(live), len(collectors), event_queue.qsize(),
            )

    ticker_task = asyncio.create_task(ticker())
    await asyncio.sleep(duration)

    log.info("Duration reached — shutting down collectors.")
    ticker_task.cancel()
    for t in tasks:
        t.cancel()

    # wait for writer's CancelledError handler to flush remaining buffers
    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("All tasks finished.")
    return collectors


# ═══════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════

class ValidationError(Exception):
    pass


def _check(condition: bool, msg: str) -> None:
    if not condition:
        raise ValidationError(msg)


def validate_snapshots(snap_files: list[Path]) -> dict:
    """Validate all snapshot Parquet files. Returns stats dict."""
    results = {}
    for f in snap_files:
        table = pq.read_table(f)
        rows = table.to_pydict()

        _check(table.num_rows == 1, f"{f.name}: expected 1 row, got {table.num_rows}")

        snap_type = rows["snapshot_type"][0]
        market    = rows["market"][0]
        symbol    = rows["symbol"][0]
        lid       = rows["last_update_id"][0]
        bid_count = rows["bid_count"][0]
        ask_count = rows["ask_count"][0]
        bids      = rows["bids"][0]
        asks      = rows["asks"][0]

        _check(snap_type in ("initial", "checkpoint"),
               f"{f.name}: unknown snapshot_type '{snap_type}'")
        _check(bid_count == len(bids),
               f"{f.name}: bid_count={bid_count} but len(bids)={len(bids)}")
        _check(ask_count == len(asks),
               f"{f.name}: ask_count={ask_count} but len(asks)={len(asks)}")
        _check(bid_count > 0, f"{f.name}: snapshot has no bids")
        _check(ask_count > 0, f"{f.name}: snapshot has no asks")

        # Parse prices as Decimal — float is intentionally avoided
        bid_prices = []
        ask_prices = []
        for entry in bids:
            _check(len(entry) == 2, f"{f.name}: bid entry must be [price, qty], got {entry}")
            p, q = Decimal(entry[0]), Decimal(entry[1])
            _check(q > 0, f"{f.name}: snapshot bid has zero qty at {p}")
            bid_prices.append(p)
        for entry in asks:
            _check(len(entry) == 2, f"{f.name}: ask entry must be [price, qty], got {entry}")
            p, q = Decimal(entry[0]), Decimal(entry[1])
            _check(q > 0, f"{f.name}: snapshot ask has zero qty at {p}")
            ask_prices.append(p)

        # Bids must be descending, asks must be ascending
        _check(bid_prices == sorted(bid_prices, reverse=True),
               f"{f.name}: bid levels are not sorted highest-first")
        _check(ask_prices == sorted(ask_prices),
               f"{f.name}: ask levels are not sorted lowest-first")

        best_bid = bid_prices[0]
        best_ask = ask_prices[0]

        # Sanity: no crossed book
        _check(best_bid < best_ask,
               f"{f.name}: CROSSED BOOK — best_bid={best_bid} >= best_ask={best_ask}")

        # Sanity: BTC price in plausible range
        _check(10_000 < float(best_bid) < 500_000,
               f"{f.name}: suspicious best_bid={best_bid} (expected 10k–500k USD)")

        spread = best_ask - best_bid

        results[f.name] = {
            "market": market, "symbol": symbol, "type": snap_type,
            "last_update_id": lid,
            "bid_levels": bid_count, "ask_levels": ask_count,
            "best_bid": best_bid, "best_ask": best_ask, "spread": spread,
        }

    return results


def validate_events(event_files: list[Path], market: str) -> dict:
    """Validate diff event Parquet files. Returns stats dict."""
    if not event_files:
        return {}

    all_rows = []
    for f in event_files:
        table = pq.read_table(f)
        rows = table.to_pydict()
        n = table.num_rows
        _check(n > 0, f"{f.name}: empty event file")

        # Schema checks
        for col in ("exchange_time_ms", "receive_time_ns",
                    "first_update_id", "last_update_id", "bids", "asks"):
            _check(col in rows, f"{f.name}: missing column '{col}'")

        # Receive time must be >= exchange time (accounting for unit difference)
        for i in range(n):
            et_ms = rows["exchange_time_ms"][i]
            rt_ns = rows["receive_time_ns"][i]
            _check(rt_ns >= et_ms * 1_000_000 - 5_000_000_000,
                   f"{f.name} row {i}: receive_time precedes exchange_time by >5s")

        # Update IDs must be strictly increasing within the file
        first_ids = rows["first_update_id"]
        last_ids  = rows["last_update_id"]
        for i in range(n):
            _check(first_ids[i] <= last_ids[i],
                   f"{f.name} row {i}: first_update_id > last_update_id")

        for i in range(1, n):
            _check(last_ids[i] > last_ids[i - 1],
                   f"{f.name} rows {i-1}→{i}: last_update_id not increasing")

        # Continuity check
        if market == "spot":
            for i in range(1, n):
                _check(
                    first_ids[i] == last_ids[i - 1] + 1,
                    f"{f.name} row {i}: spot gap — "
                    f"expected first_update_id={last_ids[i-1]+1}, "
                    f"got {first_ids[i]}",
                )
        else:  # futures — use pu field
            pu_ids = rows["prev_final_update_id"]
            for i in range(1, n):
                _check(
                    pu_ids[i] == last_ids[i - 1],
                    f"{f.name} row {i}: futures gap — "
                    f"expected pu={last_ids[i-1]}, got pu={pu_ids[i]}",
                )

        # Spot-check a sample of bid/ask entries
        for i in range(min(5, n)):
            for entry in rows["bids"][i]:
                _check(len(entry) == 2,
                       f"{f.name} row {i}: malformed bid entry {entry}")
                Decimal(entry[0])  # raises InvalidOperation if not a number
                Decimal(entry[1])

        all_rows.append({
            "file": f.name,
            "row_count": n,
            "first_exchange_time_ms": rows["exchange_time_ms"][0],
            "last_exchange_time_ms":  rows["exchange_time_ms"][-1],
            "first_update_id": first_ids[0],
            "last_update_id":  last_ids[-1],
        })

    total_events = sum(r["row_count"] for r in all_rows)
    time_span_ms = (
        max(r["last_exchange_time_ms"] for r in all_rows)
        - min(r["first_exchange_time_ms"] for r in all_rows)
    )
    return {
        "files": len(event_files),
        "total_events": total_events,
        "time_span_seconds": round(time_span_ms / 1000, 1),
        "events_per_second": round(total_events / max(time_span_ms / 1000, 1), 1),
        "per_file": all_rows,
    }


def validate_reconstruction(snap_files: list[Path], event_files: list[Path]) -> dict:
    """
    Smoke-test: seed a LocalOrderBook from the first snapshot, replay all
    events, verify the book remains valid throughout.
    """
    if not snap_files or not event_files:
        return {"skipped": True, "reason": "no files to validate"}

    from src.orderbook.local_book import LocalOrderBook

    # Pick first snapshot
    snap_table = pq.read_table(snap_files[0]).to_pydict()
    book = LocalOrderBook("BTCUSDT", "spot")
    book.init_from_snapshot({
        "lastUpdateId": snap_table["last_update_id"][0],
        "bids": snap_table["bids"][0],
        "asks": snap_table["asks"][0],
    })

    crossings = 0
    events_applied = 0
    last_mid = None

    for f in event_files:
        rows = pq.read_table(f).to_pydict()
        n = len(rows["exchange_time_ms"])
        for i in range(n):
            if rows["last_update_id"][i] <= book.last_update_id:
                continue  # covered by snapshot

            book.apply_bids_asks(rows["bids"][i], rows["asks"][i])
            book.last_update_id = rows["last_update_id"][i]
            events_applied += 1

            bb = book.best_bid()
            ba = book.best_ask()
            if bb and ba:
                if bb[0] >= ba[0]:
                    crossings += 1
                last_mid = float((bb[0] + ba[0]) / 2)

    return {
        "events_applied": events_applied,
        "crossed_book_count": crossings,
        "final_best_bid": float(book.best_bid()[0]) if book.best_bid() else None,
        "final_best_ask": float(book.best_ask()[0]) if book.best_ask() else None,
        "final_mid_price": last_mid,
        "final_bid_levels": len(book.bids),
        "final_ask_levels": len(book.asks),
    }


# ═══════════════════════════════════════════════════════════════════════════
# Report
# ═══════════════════════════════════════════════════════════════════════════

def print_report(data_dir: Path) -> bool:
    """Run all validations and print a structured report. Returns True if all pass."""
    log.info("")
    log.info(SEPARATOR)
    log.info("VALIDATION REPORT")
    log.info(SEPARATOR)

    all_passed = True
    markets = ["spot", "futures"]

    for market in markets:
        market_dir = data_dir / market / "BTCUSDT"
        if not market_dir.exists():
            log.warning("[%s] Directory not found: %s", market.upper(), market_dir)
            all_passed = False
            continue

        snap_files  = sorted(market_dir.glob("snapshots_*.parquet"))
        event_files = sorted(market_dir.glob("events_*.parquet"))

        log.info("")
        log.info("── %s/BTCUSDT ─────────────────────────────────────────", market.upper())
        log.info("   snapshot files : %d", len(snap_files))
        log.info("   event files    : %d", len(event_files))

        # ── Snapshot validation ──────────────────────────────────────────
        if not snap_files:
            log.error("   [FAIL] No snapshot files found — collector may not have gone LIVE")
            all_passed = False
        else:
            try:
                snap_stats = validate_snapshots(snap_files)
                for fname, s in snap_stats.items():
                    log.info("")
                    log.info("   Snapshot: %s", fname)
                    log.info("     type            : %s", s["type"])
                    log.info("     last_update_id  : %d", s["last_update_id"])
                    log.info("     bid levels      : %d", s["bid_levels"])
                    log.info("     ask levels      : %d", s["ask_levels"])
                    log.info("     best bid        : %s", s["best_bid"])
                    log.info("     best ask        : %s", s["best_ask"])
                    log.info("     spread          : %s", s["spread"])
                log.info("   [PASS] All snapshot checks passed")
            except ValidationError as e:
                log.error("   [FAIL] Snapshot validation failed: %s", e)
                all_passed = False

        # ── Event validation ─────────────────────────────────────────────
        if not event_files:
            log.warning("   [WARN] No event files found (normal if runtime < 5 min and "
                        "< 10,000 events — try longer duration)")
        else:
            try:
                ev_stats = validate_events(event_files, market)
                log.info("")
                log.info("   Event files summary:")
                log.info("     total events    : %d", ev_stats["total_events"])
                log.info("     time span       : %.1fs", ev_stats["time_span_seconds"])
                log.info("     avg event rate  : %.1f events/s", ev_stats["events_per_second"])
                for pf in ev_stats["per_file"]:
                    log.info("     %s: %d rows", pf["file"], pf["row_count"])
                log.info("   [PASS] All event checks passed (schema + continuity + timestamps)")
            except ValidationError as e:
                log.error("   [FAIL] Event validation failed: %s", e)
                all_passed = False

        # ── Reconstruction smoke-test ─────────────────────────────────────
        if snap_files and event_files:
            log.info("")
            log.info("   Running book reconstruction smoke-test…")
            try:
                r = validate_reconstruction(snap_files, event_files)
                if r.get("skipped"):
                    log.info("   [SKIP] %s", r["reason"])
                else:
                    log.info("     events applied    : %d", r["events_applied"])
                    log.info("     crossed book count: %d", r["crossed_book_count"])
                    log.info("     final bid levels  : %d", r["final_bid_levels"])
                    log.info("     final ask levels  : %d", r["final_ask_levels"])
                    log.info("     final best bid    : %s", r["final_best_bid"])
                    log.info("     final best ask    : %s", r["final_best_ask"])
                    log.info("     final mid price   : %s", r["final_mid_price"])
                    if r["crossed_book_count"] > 0:
                        log.error("   [FAIL] Book crossed %d time(s) during replay",
                                  r["crossed_book_count"])
                        all_passed = False
                    else:
                        log.info("   [PASS] Book reconstruction: no crossings, prices valid")
            except Exception as exc:
                log.error("   [FAIL] Reconstruction error: %s", exc)
                all_passed = False

    log.info("")
    log.info(SEPARATOR)
    if all_passed:
        log.info("OVERALL RESULT: ALL CHECKS PASSED ✓")
    else:
        log.info("OVERALL RESULT: SOME CHECKS FAILED — see errors above")
    log.info(SEPARATOR)
    log.info("")
    log.info("Data directory: %s", data_dir.resolve())
    log.info("Query with DuckDB:")
    log.info("  python3 -c \"import duckdb; print(duckdb.query(\\\"")
    log.info("    SELECT market, COUNT(*) as events")
    log.info("    FROM '%s/*/BTCUSDT/events_*.parquet'", data_dir)
    log.info("    GROUP BY 1\\\").df())\"")

    return all_passed


# ═══════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--duration", type=int, default=90,
                   help="Collection duration in seconds (default: 90)")
    p.add_argument("--out-dir", type=Path, default=ROOT / "data" / "sample",
                   help="Output directory for Parquet files")
    return p.parse_args()


async def async_main(duration: int, data_dir: Path) -> bool:
    await collect(duration, data_dir)
    return print_report(data_dir)


if __name__ == "__main__":
    args = parse_args()
    ok = asyncio.run(async_main(args.duration, args.out_dir))
    sys.exit(0 if ok else 1)
