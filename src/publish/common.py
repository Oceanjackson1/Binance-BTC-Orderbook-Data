from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable


SUPPORTED_DATASETS = ("trades", "price_changes", "book_snapshots", "recovery_events")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_timeframes(raw: str) -> list[str]:
    items = [item.strip() for item in raw.split(",") if item.strip()]
    return items or ["5m", "15m", "1h", "4h"]


def timeframe_seconds(timeframe: str) -> int:
    if timeframe.endswith("m"):
        return int(timeframe[:-1]) * 60
    if timeframe.endswith("h"):
        return int(timeframe[:-1]) * 3600
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def bucket_start(ts: datetime, timeframe: str) -> datetime:
    seconds = timeframe_seconds(timeframe)
    epoch = int(ts.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % seconds), tz=timezone.utc)


def bucket_end(ts: datetime, timeframe: str) -> datetime:
    return bucket_start(ts, timeframe) + timedelta(seconds=timeframe_seconds(timeframe))


def market_slug(market: str, symbol: str) -> str:
    return f"{market.lower()}-{symbol.lower()}"


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable")


def write_json_atomic(path: Path, payload) -> None:
    ensure_parent(path)
    if path.exists():
        try:
            existing_payload = json.loads(path.read_text(encoding="utf-8"))
            if _normalize_payload(existing_payload) == _normalize_payload(payload):
                return
        except (OSError, json.JSONDecodeError):
            pass
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        indent=2,
        sort_keys=False,
        default=json_default,
    ) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == encoded:
        return
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(encoded, encoding="utf-8")
    tmp_path.replace(path)


def select_columns(rows: Iterable[dict], columns: list[str] | None) -> list[dict]:
    if not columns:
        return list(rows)
    selected = []
    for row in rows:
        selected.append({column: row[column] for column in columns if column in row})
    return selected


def _normalize_payload(payload):
    if isinstance(payload, dict):
        return {
            key: _normalize_payload(value)
            for key, value in payload.items()
            if key != "generated_at"
        }
    if isinstance(payload, list):
        return [_normalize_payload(item) for item in payload]
    return payload
