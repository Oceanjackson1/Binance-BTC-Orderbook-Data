#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from qcloud_cos import CosConfig, CosS3Client
except ImportError:
    CosConfig = None
    CosS3Client = None


logger = logging.getLogger(__name__)


def configure_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.warning("State file unreadable, starting fresh: %s", path, exc_info=True)
        return {}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def iter_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and not path.name.endswith(".tmp")
    )


def file_version(path: Path) -> dict[str, int]:
    stat = path.stat()
    return {"size": int(stat.st_size), "mtime_ns": int(stat.st_mtime_ns)}


def build_object_key(prefix: str, relative_path: Path) -> str:
    clean_prefix = prefix.strip("/")
    rel = relative_path.as_posix()
    return rel if not clean_prefix else f"{clean_prefix}/{rel}"


def is_stable(path: Path, stable_seconds: float, now: float) -> bool:
    return now - path.stat().st_mtime >= stable_seconds


class BackupManager:
    def __init__(self) -> None:
        self.timescale_dsn = require_env("TIMESCALE_DSN")
        self.publish_dir = Path(os.getenv("PUBLISH_DIR", "/app/publish")).resolve()
        self.backup_dir = Path(os.getenv("BACKUP_DIR", "/app/backups/db")).resolve()
        self.state_dir = Path(os.getenv("BACKUP_STATE_DIR", "/app/backups/state")).resolve()
        self.state_file = self.state_dir / "backup_state.json"
        self.publish_state_file = self.state_dir / "cos_publish_state.json"
        self.db_state_file = self.state_dir / "cos_db_state.json"

        self.sync_interval = env_int("TENCENT_COS_SYNC_INTERVAL_SECONDS", 300)
        self.stable_seconds = env_int("TENCENT_COS_STABLE_SECONDS", 120)
        self.db_backup_interval = env_int("DB_BACKUP_INTERVAL_SECONDS", 3600)
        self.db_backup_retention_count = env_int("DB_BACKUP_RETENTION_COUNT", 48)

        self.cos_bucket = os.getenv("TENCENT_COS_BUCKET", "").strip()
        self.cos_region = os.getenv("TENCENT_COS_REGION", "").strip()
        self.cos_prefix = os.getenv("TENCENT_COS_PREFIX", "binance-orderbook").strip("/")
        self.cos_secret_id = os.getenv("TENCENTCLOUD_SECRET_ID", "").strip()
        self.cos_secret_key = os.getenv("TENCENTCLOUD_SECRET_KEY", "").strip()
        self._cos_missing_logged = False

        self.state = load_state(self.state_file)
        self._client = self._build_cos_client()

    def run(self) -> None:
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "Backup manager started. publish_dir=%s backup_dir=%s sync_interval=%ss db_backup_interval=%ss",
            self.publish_dir,
            self.backup_dir,
            self.sync_interval,
            self.db_backup_interval,
        )

        while True:
            cycle_started = time.time()

            try:
                self._maybe_create_db_backup(cycle_started)
                self._prune_local_backups()
                self._sync_to_cos()
            except KeyboardInterrupt:
                logger.info("Stopping backup manager.")
                break
            except Exception:
                logger.exception("Backup cycle failed")

            elapsed = time.time() - cycle_started
            time.sleep(max(1, self.sync_interval - int(elapsed)))

    def _maybe_create_db_backup(self, now: float) -> None:
        last_backup_ts = float(self.state.get("last_db_backup_ts", 0))
        if now - last_backup_ts < self.db_backup_interval:
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        tmp_path = self.backup_dir / f"orderbook_{timestamp}.dump.tmp"
        final_path = self.backup_dir / f"orderbook_{timestamp}.dump"

        cmd = [
            "pg_dump",
            f"--dbname={self.timescale_dsn}",
            "--format=custom",
            "--no-owner",
            "--no-privileges",
            f"--file={tmp_path}",
        ]

        logger.info("Creating DB backup: %s", final_path)
        subprocess.run(cmd, check=True)
        tmp_path.replace(final_path)

        self.state["last_db_backup_ts"] = now
        self.state["last_db_backup_file"] = str(final_path)
        save_state(self.state_file, self.state)

    def _prune_local_backups(self) -> None:
        dumps = sorted(self.backup_dir.glob("orderbook_*.dump"))
        excess = len(dumps) - self.db_backup_retention_count
        if excess <= 0:
            return

        for path in dumps[:excess]:
            logger.info("Removing expired local backup: %s", path)
            path.unlink(missing_ok=True)

    def _sync_to_cos(self) -> None:
        if not self._client:
            if not self._cos_missing_logged:
                logger.warning("COS credentials not configured — remote sync disabled.")
                self._cos_missing_logged = True
            return

        publish_state = load_state(self.publish_state_file)
        db_state = load_state(self.db_state_file)

        uploaded_publish, skipped_publish, already_publish = self._sync_dir(
            source_dir=self.publish_dir,
            state=publish_state,
            prefix_suffix="",
        )
        uploaded_db, skipped_db, already_db = self._sync_dir(
            source_dir=self.backup_dir,
            state=db_state,
            prefix_suffix="db",
        )

        save_state(self.publish_state_file, publish_state)
        save_state(self.db_state_file, db_state)

        logger.info(
            "COS sync complete. publish(uploaded=%d skipped=%d already=%d) db(uploaded=%d skipped=%d already=%d)",
            uploaded_publish,
            skipped_publish,
            already_publish,
            uploaded_db,
            skipped_db,
            already_db,
        )

    def _sync_dir(
        self,
        source_dir: Path,
        state: dict[str, dict[str, int]],
        prefix_suffix: str,
    ) -> tuple[int, int, int]:
        if not source_dir.exists():
            return 0, 0, 0

        now = time.time()
        uploaded = 0
        skipped_active = 0
        already_synced = 0
        prefix = "/".join(part for part in [self.cos_prefix, prefix_suffix] if part)

        for path in iter_files(source_dir):
            relative_path = path.relative_to(source_dir)
            object_key = build_object_key(prefix, relative_path)
            version = file_version(path)

            if not is_stable(path, self.stable_seconds, now):
                skipped_active += 1
                continue

            if state.get(object_key) == version:
                already_synced += 1
                continue

            logger.info("Uploading %s -> cos://%s/%s", path, self.cos_bucket, object_key)
            with path.open("rb") as fh:
                self._client.put_object(
                    Bucket=self.cos_bucket,
                    Body=fh,
                    Key=object_key,
                )
            state[object_key] = version
            uploaded += 1

        return uploaded, skipped_active, already_synced

    def _build_cos_client(self):
        if not all([self.cos_bucket, self.cos_region, self.cos_secret_id, self.cos_secret_key]):
            return None
        if CosConfig is None or CosS3Client is None:
            raise RuntimeError("cos-python-sdk-v5 is not installed.")

        return CosS3Client(
            CosConfig(
                Region=self.cos_region,
                SecretId=self.cos_secret_id,
                SecretKey=self.cos_secret_key,
                Scheme="https",
            )
        )


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer for {name}: {raw}") from exc
    if value <= 0:
        raise RuntimeError(f"{name} must be > 0")
    return value


if __name__ == "__main__":
    configure_logging()
    BackupManager().run()
