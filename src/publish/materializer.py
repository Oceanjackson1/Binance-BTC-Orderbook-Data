from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

from src.publish.common import (
    SUPPORTED_DATASETS,
    bucket_end,
    bucket_start,
    market_slug,
    parse_timeframes,
    timeframe_seconds,
    utc_now,
    write_json_atomic,
)


logger = logging.getLogger(__name__)


def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )


class PublishMaterializer:
    def __init__(self) -> None:
        self.dsn = _require_env("TIMESCALE_DSN")
        self.agent_id = os.getenv("AGENT_ID", "binance-orderbook").strip() or "binance-orderbook"
        self.data_dir = Path(os.getenv("DATA_DIR", "/app/data")).resolve()
        self.publish_dir = Path(os.getenv("PUBLISH_DIR", "/app/publish")).resolve()
        self.raw_dir = self.publish_dir / "raw"
        self.curated_dir = self.publish_dir / "curated"
        self.meta_dir = self.publish_dir / "meta"
        self.interval_seconds = _env_int("MATERIALIZE_INTERVAL_SECONDS", 60)
        self.lookback_days = _env_int("PUBLISH_LOOKBACK_DAYS", 2)
        self.stable_seconds = _env_int("PUBLISH_STABLE_SECONDS", 120)
        self.timeframes = parse_timeframes(os.getenv("PUBLISH_TIMEFRAMES", "5m,15m,1h,4h"))
        self._pool: asyncpg.Pool | None = None

    async def run(self) -> None:
        self.publish_dir.mkdir(parents=True, exist_ok=True)
        self._pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=4)
        logger.info(
            "Publish materializer started. agent_id=%s publish_dir=%s interval=%ss",
            self.agent_id,
            self.publish_dir,
            self.interval_seconds,
        )
        try:
            while True:
                try:
                    await self.materialize_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Materialize cycle failed")
                await asyncio.sleep(self.interval_seconds)
        finally:
            await self._pool.close()

    async def materialize_once(self) -> None:
        started_at = utc_now()
        markets = await self._fetch_markets()
        self._sync_raw_files(started_at.timestamp())

        for offset in range(self.lookback_days):
            target_dt = (started_at - timedelta(days=offset)).date()
            day_start = datetime.combine(target_dt, time.min, tzinfo=timezone.utc)
            day_end = day_start + timedelta(days=1)

            snapshot_rows = await self._fetch_snapshot_rows(day_start, day_end)
            health_rows = await self._fetch_health_rows(day_start, day_end)

            snapshot_groups = _group_rows(snapshot_rows)
            health_groups = _group_rows(health_rows)
            for market_name, symbol in markets:
                slug = market_slug(market_name, symbol)
                market_rows = snapshot_groups.get((market_name, symbol), [])
                health_market_rows = health_groups.get((market_name, symbol), [])
                for timeframe in self.timeframes:
                    book_rows = _build_book_snapshot_rows(market_name, symbol, timeframe, market_rows)
                    price_rows = _build_price_change_rows(market_name, symbol, timeframe, market_rows)
                    recovery_rows = _build_recovery_rows(
                        market_name,
                        symbol,
                        timeframe,
                        health_market_rows,
                    )
                    self._write_curated_file("book_snapshots", target_dt, timeframe, slug, book_rows)
                    self._write_curated_file("price_changes", target_dt, timeframe, slug, price_rows)
                    self._write_curated_file("recovery_events", target_dt, timeframe, slug, recovery_rows)

        files_payload = self._build_files_payload()
        write_json_atomic(self.meta_dir / "files.json", files_payload)
        write_json_atomic(self.meta_dir / "markets.json", self._build_markets_payload(markets, files_payload))
        self._write_keysets(files_payload)
        write_json_atomic(
            self.meta_dir / "status.json",
            {
                "agent_id": self.agent_id,
                "generated_at": utc_now().isoformat(),
                "publish_dir": str(self.publish_dir),
                "market_count": len(markets),
                "datasets": list(SUPPORTED_DATASETS),
                "timeframes": self.timeframes,
            },
        )

    async def _fetch_markets(self) -> list[tuple[str, str]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT market, symbol
                FROM (
                    SELECT market, symbol FROM orderbook_snapshots
                    UNION
                    SELECT market, symbol FROM collector_health
                    UNION
                    SELECT market, symbol FROM orderbook_full_snapshots_raw
                ) t
                ORDER BY market, symbol
                """
            )
        return [(row["market"], row["symbol"]) for row in rows]

    async def _fetch_snapshot_rows(self, day_start: datetime, day_end: datetime) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ts, market, symbol, best_bid::float8 AS best_bid,
                       best_ask::float8 AS best_ask, spread::float8 AS spread,
                       mid_price::float8 AS mid_price,
                       bid_depth_10::float8 AS bid_depth_10,
                       ask_depth_10::float8 AS ask_depth_10,
                       last_update_id
                FROM orderbook_snapshots
                WHERE ts >= $1 AND ts < $2
                ORDER BY market, symbol, ts
                """,
                day_start,
                day_end,
            )
        return [dict(row) for row in rows]

    async def _fetch_health_rows(self, day_start: datetime, day_end: datetime) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT ts, market, symbol, restarts, last_update_id, is_live
                FROM collector_health
                WHERE ts >= $1 AND ts < $2
                ORDER BY market, symbol, ts
                """,
                day_start,
                day_end,
            )
        return [dict(row) for row in rows]

    def _sync_raw_files(self, now_ts: float) -> None:
        if not self.data_dir.exists():
            return
        for source_path in sorted(self.data_dir.rglob("*.parquet")):
            if source_path.name.endswith(".tmp"):
                continue
            if now_ts - source_path.stat().st_mtime < self.stable_seconds:
                continue
            relative_path = source_path.relative_to(self.data_dir)
            target_path = self.raw_dir / relative_path
            if _same_file_version(source_path, target_path):
                continue
            target_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = target_path.with_suffix(target_path.suffix + ".tmp")
            shutil.copy2(source_path, tmp_path)
            tmp_path.replace(target_path)

    def _write_curated_file(
        self,
        dataset: str,
        dt_value: date,
        timeframe: str,
        slug: str,
        rows: list[dict],
    ) -> None:
        path = (
            self.curated_dir
            / dataset
            / f"dt={dt_value.isoformat()}"
            / f"timeframe={timeframe}"
            / f"market_slug={slug}.json"
        )
        if not rows:
            if path.exists():
                path.unlink()
                _prune_empty_dirs(path.parent, self.curated_dir)
            return
        payload = {
            "dataset": dataset,
            "dt": dt_value.isoformat(),
            "timeframe": timeframe,
            "market_slug": slug,
            "generated_at": utc_now().isoformat(),
            "total_rows": len(rows),
            "rows": rows,
        }
        write_json_atomic(path, payload)

    def _build_files_payload(self) -> dict:
        curated_items = _scan_curated_files(self.curated_dir)
        raw_items = _scan_raw_files(self.raw_dir)
        items = raw_items + curated_items
        items.sort(
            key=lambda item: (
                item.get("layer", ""),
                item.get("dataset", ""),
                item.get("dt", ""),
                item.get("timeframe", ""),
                item.get("market_slug", ""),
                item.get("relative_path", ""),
            )
        )
        return {
            "agent_id": self.agent_id,
            "generated_at": utc_now().isoformat(),
            "items": items,
        }

    def _build_markets_payload(self, markets: list[tuple[str, str]], files_payload: dict) -> dict:
        by_slug: dict[str, dict] = {}
        curated_items = [item for item in files_payload["items"] if item.get("layer") == "curated"]
        for market_name, symbol in markets:
            slug = market_slug(market_name, symbol)
            by_slug[slug] = {
                "market_slug": slug,
                "market": market_name,
                "symbol": symbol,
                "agent_id": self.agent_id,
                "datasets": [],
                "timeframes": [],
            }

        for item in curated_items:
            slug = item["market_slug"]
            market_entry = by_slug.setdefault(
                slug,
                {
                    "market_slug": slug,
                    "market": slug.split("-", 1)[0],
                    "symbol": slug.split("-", 1)[1].upper() if "-" in slug else slug.upper(),
                    "agent_id": self.agent_id,
                    "datasets": [],
                    "timeframes": [],
                },
            )
            if item["dataset"] not in market_entry["datasets"]:
                market_entry["datasets"].append(item["dataset"])
            if item["timeframe"] not in market_entry["timeframes"]:
                market_entry["timeframes"].append(item["timeframe"])

        items = sorted(by_slug.values(), key=lambda item: item["market_slug"])
        for item in items:
            item["datasets"].sort()
            item["timeframes"].sort(key=timeframe_seconds)
        return {
            "agent_id": self.agent_id,
            "generated_at": utc_now().isoformat(),
            "items": items,
        }

    def _write_keysets(self, files_payload: dict) -> None:
        keysets_root = self.meta_dir / "keysets"
        keysets_root.mkdir(parents=True, exist_ok=True)

        curated_items = [item for item in files_payload["items"] if item.get("layer") == "curated"]
        grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for item in curated_items:
            grouped[(item["dt"], item["timeframe"])].append(item)

        index_items = []
        expected_manifest_paths: set[Path] = set()
        for (dt_value, timeframe), items in sorted(grouped.items()):
            manifest_path = keysets_root / f"dt={dt_value}" / f"timeframe={timeframe}" / "manifest.json"
            expected_manifest_paths.add(manifest_path)
            payload = {
                "agent_id": self.agent_id,
                "dt": dt_value,
                "timeframe": timeframe,
                "generated_at": utc_now().isoformat(),
                "datasets": sorted({item["dataset"] for item in items}),
                "market_slugs": sorted({item["market_slug"] for item in items}),
                "files": items,
            }
            write_json_atomic(manifest_path, payload)
            index_items.append(
                {
                    "dt": dt_value,
                    "timeframe": timeframe,
                    "manifest_path": manifest_path.relative_to(self.publish_dir).as_posix(),
                    "datasets": payload["datasets"],
                    "market_slugs": payload["market_slugs"],
                    "file_count": len(items),
                }
            )

        for path in keysets_root.rglob("manifest.json"):
            if path not in expected_manifest_paths:
                path.unlink()
                _prune_empty_dirs(path.parent, keysets_root)

        write_json_atomic(
            keysets_root / "index.json",
            {
                "agent_id": self.agent_id,
                "generated_at": utc_now().isoformat(),
                "items": index_items,
            },
        )


def _group_rows(rows: list[dict]) -> dict[tuple[str, str], list[dict]]:
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        grouped[(row["market"], row["symbol"])].append(row)
    return grouped


def _build_book_snapshot_rows(
    market_name: str,
    symbol: str,
    timeframe: str,
    rows: list[dict],
) -> list[dict]:
    if not rows:
        return []
    buckets: dict[datetime, dict] = {}
    slug = market_slug(market_name, symbol)
    for row in rows:
        start = bucket_start(row["ts"], timeframe)
        buckets[start] = row

    items = []
    for start in sorted(buckets):
        row = buckets[start]
        items.append(
            {
                "bucket_start": start.isoformat(),
                "bucket_end": bucket_end(start, timeframe).isoformat(),
                "snapshot_ts": row["ts"].isoformat(),
                "market": market_name,
                "symbol": symbol,
                "market_slug": slug,
                "best_bid": row["best_bid"],
                "best_ask": row["best_ask"],
                "spread": row["spread"],
                "mid_price": row["mid_price"],
                "bid_depth_10": row["bid_depth_10"],
                "ask_depth_10": row["ask_depth_10"],
                "last_update_id": row["last_update_id"],
            }
        )
    return items


def _build_price_change_rows(
    market_name: str,
    symbol: str,
    timeframe: str,
    rows: list[dict],
) -> list[dict]:
    if not rows:
        return []
    grouped: dict[datetime, list[dict]] = defaultdict(list)
    slug = market_slug(market_name, symbol)
    for row in rows:
        grouped[bucket_start(row["ts"], timeframe)].append(row)

    items = []
    for start in sorted(grouped):
        bucket_rows = grouped[start]
        first = bucket_rows[0]
        last = bucket_rows[-1]
        mid_prices = [row["mid_price"] for row in bucket_rows if row["mid_price"] is not None]
        spreads = [row["spread"] for row in bucket_rows if row["spread"] is not None]
        open_price = first["mid_price"]
        close_price = last["mid_price"]
        change = None
        change_bps = None
        if open_price not in (None, 0) and close_price is not None:
            change = close_price - open_price
            change_bps = (change / open_price) * 10_000
        items.append(
            {
                "bucket_start": start.isoformat(),
                "bucket_end": bucket_end(start, timeframe).isoformat(),
                "market": market_name,
                "symbol": symbol,
                "market_slug": slug,
                "first_snapshot_ts": first["ts"].isoformat(),
                "last_snapshot_ts": last["ts"].isoformat(),
                "mid_price_open": open_price,
                "mid_price_close": close_price,
                "mid_price_high": max(mid_prices) if mid_prices else None,
                "mid_price_low": min(mid_prices) if mid_prices else None,
                "mid_price_change": change,
                "mid_price_change_bps": change_bps,
                "spread_avg": (sum(spreads) / len(spreads)) if spreads else None,
                "sample_count": len(bucket_rows),
            }
        )
    return items


def _build_recovery_rows(
    market_name: str,
    symbol: str,
    timeframe: str,
    rows: list[dict],
) -> list[dict]:
    if not rows:
        return []
    slug = market_slug(market_name, symbol)
    items = []
    previous = rows[0]
    items.append(
        {
            "ts": previous["ts"].isoformat(),
            "bucket_start": bucket_start(previous["ts"], timeframe).isoformat(),
            "bucket_end": bucket_end(previous["ts"], timeframe).isoformat(),
            "market": market_name,
            "symbol": symbol,
            "market_slug": slug,
            "event_type": "initial_state",
            "restarts": previous["restarts"],
            "previous_restarts": None,
            "is_live": previous["is_live"],
            "previous_is_live": None,
            "last_update_id": previous["last_update_id"],
        }
    )

    for row in rows[1:]:
        start = bucket_start(row["ts"], timeframe)
        end = bucket_end(row["ts"], timeframe)
        if row["restarts"] != previous["restarts"]:
            items.append(
                {
                    "ts": row["ts"].isoformat(),
                    "bucket_start": start.isoformat(),
                    "bucket_end": end.isoformat(),
                    "market": market_name,
                    "symbol": symbol,
                    "market_slug": slug,
                    "event_type": "restart_increment",
                    "restarts": row["restarts"],
                    "previous_restarts": previous["restarts"],
                    "restart_delta": row["restarts"] - previous["restarts"],
                    "is_live": row["is_live"],
                    "previous_is_live": previous["is_live"],
                    "last_update_id": row["last_update_id"],
                }
            )
        if row["is_live"] != previous["is_live"]:
            items.append(
                {
                    "ts": row["ts"].isoformat(),
                    "bucket_start": start.isoformat(),
                    "bucket_end": end.isoformat(),
                    "market": market_name,
                    "symbol": symbol,
                    "market_slug": slug,
                    "event_type": "liveness_change",
                    "restarts": row["restarts"],
                    "previous_restarts": previous["restarts"],
                    "is_live": row["is_live"],
                    "previous_is_live": previous["is_live"],
                    "last_update_id": row["last_update_id"],
                }
            )
        previous = row
    return items


def _scan_curated_files(curated_dir: Path) -> list[dict]:
    if not curated_dir.exists():
        return []
    items = []
    for path in sorted(curated_dir.rglob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = payload.get("rows", [])
        stat = path.stat()
        items.append(
            {
                "layer": "curated",
                "dataset": payload.get("dataset"),
                "dt": payload.get("dt"),
                "timeframe": payload.get("timeframe"),
                "market_slug": payload.get("market_slug"),
                "row_count": len(rows),
                "size_bytes": stat.st_size,
                "updated_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                "relative_path": path.relative_to(curated_dir.parent).as_posix(),
            }
        )
    return items


def _scan_raw_files(raw_dir: Path) -> list[dict]:
    if not raw_dir.exists():
        return []
    items = []
    for path in sorted(raw_dir.rglob("*")):
        if not path.is_file() or path.name.endswith(".tmp"):
            continue
        relative = path.relative_to(raw_dir)
        parts = relative.parts
        market_name = parts[0] if len(parts) >= 1 else "unknown"
        symbol = parts[1] if len(parts) >= 2 else "unknown"
        dataset = "snapshots_parquet" if path.name.startswith("snapshots_") else "events_parquet"
        stat = path.stat()
        items.append(
            {
                "layer": "raw",
                "dataset": dataset,
                "market_slug": market_slug(market_name, symbol),
                "size_bytes": stat.st_size,
                "updated_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                "relative_path": path.relative_to(raw_dir.parent).as_posix(),
            }
        )
    return items


def _same_file_version(source_path: Path, target_path: Path) -> bool:
    if not target_path.exists():
        return False
    source_stat = source_path.stat()
    target_stat = target_path.stat()
    return (
        source_stat.st_size == target_stat.st_size
        and source_stat.st_mtime_ns == target_stat.st_mtime_ns
    )


def _prune_empty_dirs(start: Path, stop_at: Path) -> None:
    current = start
    while current != stop_at and current.exists():
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer for {name}: {raw}") from exc


async def main() -> None:
    load_dotenv()
    setup_logging()
    materializer = PublishMaterializer()
    await materializer.run()


if __name__ == "__main__":
    asyncio.run(main())
