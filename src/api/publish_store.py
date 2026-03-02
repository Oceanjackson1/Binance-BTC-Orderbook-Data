from __future__ import annotations

import json
import os
from pathlib import Path

from fastapi import HTTPException

from src.publish.common import SUPPORTED_DATASETS, select_columns, timeframe_seconds


class PublishStore:
    def __init__(self, root: str | None = None) -> None:
        self.root = Path(root or os.getenv("PUBLISH_DIR", "./publish")).resolve()
        self.meta_dir = self.root / "meta"

    def status(self) -> dict:
        status_path = self.meta_dir / "status.json"
        if status_path.exists():
            return self._load_json(status_path)
        return {"agent_id": os.getenv("AGENT_ID", "binance-orderbook"), "status": "starting"}

    def keysets_index(self) -> dict:
        return self._load_json(self.meta_dir / "keysets" / "index.json")

    def keyset_manifest(self, dt_value: str, timeframe: str) -> dict:
        return self._load_json(
            self.meta_dir / "keysets" / f"dt={dt_value}" / f"timeframe={timeframe}" / "manifest.json"
        )

    def markets(self) -> dict:
        return self._load_json(self.meta_dir / "markets.json")

    def files(
        self,
        *,
        dataset: str | None = None,
        dt_value: str | None = None,
        timeframe: str | None = None,
        market_slug: str | None = None,
        layer: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        payload = self._load_json(self.meta_dir / "files.json")
        items = payload.get("items", [])
        filtered = [
            item
            for item in items
            if (dataset is None or item.get("dataset") == dataset)
            and (dt_value is None or item.get("dt") == dt_value)
            and (timeframe is None or item.get("timeframe") == timeframe)
            and (market_slug is None or item.get("market_slug") == market_slug)
            and (layer is None or item.get("layer") == layer)
        ]
        return {
            "agent_id": payload.get("agent_id"),
            "generated_at": payload.get("generated_at"),
            "total_items": len(filtered),
            "returned_items": len(filtered[offset : offset + limit]),
            "items": filtered[offset : offset + limit],
        }

    def curated(
        self,
        dataset: str,
        *,
        dt_value: str | None,
        timeframe: str | None,
        market_slug: str | None,
        limit: int,
        offset: int,
        columns: list[str] | None,
    ) -> dict:
        if dataset not in SUPPORTED_DATASETS:
            raise HTTPException(status_code=404, detail="dataset not supported")

        files_payload = self._load_json(self.meta_dir / "files.json")
        items = [
            item
            for item in files_payload.get("items", [])
            if item.get("layer") == "curated" and item.get("dataset") == dataset
        ]

        resolved_dt, resolved_timeframe = self._resolve_partition(items, dt_value, timeframe)
        filtered_items = [
            item
            for item in items
            if item.get("dt") == resolved_dt
            and item.get("timeframe") == resolved_timeframe
            and (market_slug is None or item.get("market_slug") == market_slug)
        ]

        rows = []
        for item in filtered_items:
            payload = self._load_json(self.root / item["relative_path"])
            rows.extend(payload.get("rows", []))

        rows.sort(key=_row_sort_key, reverse=True)
        total_rows = len(rows)
        sliced_rows = rows[offset : offset + limit]
        return {
            "dataset": dataset,
            "dt": resolved_dt,
            "timeframe": resolved_timeframe,
            "market_slug": market_slug,
            "file_count": len(filtered_items),
            "total_rows": total_rows,
            "returned_rows": len(sliced_rows),
            "rows": select_columns(sliced_rows, columns),
        }

    def _resolve_partition(
        self,
        items: list[dict],
        dt_value: str | None,
        timeframe: str | None,
    ) -> tuple[str | None, str | None]:
        available = sorted(
            {
                (item.get("dt"), item.get("timeframe"))
                for item in items
                if item.get("dt") and item.get("timeframe")
            },
            key=lambda item: (item[0], -timeframe_seconds(item[1])),
        )
        if dt_value and timeframe:
            return dt_value, timeframe
        if not available:
            return dt_value, timeframe
        target_dt = dt_value or available[-1][0]
        timeframes = [tf for dt_item, tf in available if dt_item == target_dt]
        resolved_timeframe = timeframe or min(timeframes, key=timeframe_seconds)
        return target_dt, resolved_timeframe

    @staticmethod
    def _load_json(path: Path) -> dict:
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"resource not found: {path.name}")
        return json.loads(path.read_text(encoding="utf-8"))


def _row_sort_key(row: dict) -> tuple:
    for key in ("bucket_start", "ts", "snapshot_ts", "first_snapshot_ts"):
        if key in row:
            return (row.get("market_slug", ""), row[key])
    return (row.get("market_slug", ""), "")
