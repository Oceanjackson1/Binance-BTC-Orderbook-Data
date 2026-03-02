import json
import os
from contextlib import asynccontextmanager
from datetime import datetime

import asyncpg
from fastapi import FastAPI, HTTPException, Query

from src.api.publish_store import PublishStore
from src.api.security import ApiSecurity


async def _init_connection(conn) -> None:
    await conn.set_type_codec(
        "json",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
        format="text",
    )


def _get_dsn() -> str | None:
    return os.getenv("ORDERBOOK_API_DSN") or os.getenv("TIMESCALE_DSN")


@asynccontextmanager
async def lifespan(app: FastAPI):
    dsn = _get_dsn()
    app.state.pool = None
    if dsn:
        app.state.pool = await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=5,
            init=_init_connection,
        )
    app.state.store = PublishStore()
    try:
        yield
    finally:
        if app.state.pool:
            await app.state.pool.close()


app = FastAPI(
    title="Binance Orderbook Public API",
    version="2.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

security = ApiSecurity()


@app.middleware("http")
async def security_middleware(request, call_next):
    return await security.enforce(request, call_next)


@app.get("/health")
async def health() -> dict:
    if app.state.pool:
        async with app.state.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
    status = app.state.store.status()
    return {
        "status": "ok",
        "agent_id": status.get("agent_id"),
        "generated_at": status.get("generated_at"),
    }


@app.get("/v1/keysets/index")
async def keysets_index() -> dict:
    return app.state.store.keysets_index()


@app.get("/v1/keysets/{dt_value}/{timeframe}")
async def keyset_manifest(dt_value: str, timeframe: str) -> dict:
    return app.state.store.keyset_manifest(dt_value, timeframe)


@app.get("/v1/meta/markets")
async def meta_markets() -> dict:
    return app.state.store.markets()


@app.get("/v1/meta/files")
async def meta_files(
    dataset: str | None = None,
    dt: str | None = None,
    timeframe: str | None = None,
    market_slug: str | None = None,
    layer: str | None = None,
    limit: int = Query(default=200, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return app.state.store.files(
        dataset=dataset,
        dt_value=dt,
        timeframe=timeframe,
        market_slug=market_slug,
        layer=layer,
        limit=limit,
        offset=offset,
    )


@app.get("/v1/curated/{dataset}")
async def curated_dataset(
    dataset: str,
    dt: str | None = None,
    timeframe: str | None = None,
    market_slug: str | None = None,
    limit: int = Query(default=500, ge=1, le=10_000),
    offset: int = Query(default=0, ge=0),
    columns: str | None = None,
) -> dict:
    selected_columns = [column.strip() for column in columns.split(",")] if columns else None
    return app.state.store.curated(
        dataset,
        dt_value=dt,
        timeframe=timeframe,
        market_slug=market_slug,
        limit=limit,
        offset=offset,
        columns=selected_columns,
    )


@app.get("/v1/collector-health")
async def collector_health(
    market: str | None = None,
    symbol: str | None = None,
) -> dict:
    if not app.state.pool:
        raise HTTPException(status_code=503, detail="database unavailable")
    query = """
        SELECT DISTINCT ON (market, symbol)
            ts, market, symbol, restarts, last_update_id, is_live
        FROM collector_health
        WHERE ($1::text IS NULL OR market = $1)
          AND ($2::text IS NULL OR symbol = $2)
        ORDER BY market, symbol, ts DESC
    """
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(query, market, symbol)
    return {"items": [_record_to_dict(row) for row in rows]}


@app.get("/v1/orderbook/latest-summary")
async def latest_summary(market: str, symbol: str) -> dict:
    slug = f"{market.lower()}-{symbol.lower()}"
    payload = app.state.store.curated(
        "book_snapshots",
        dt_value=None,
        timeframe=None,
        market_slug=slug,
        limit=1,
        offset=0,
        columns=None,
    )
    if not payload["rows"]:
        raise HTTPException(status_code=404, detail="summary not found")
    return payload["rows"][0]


@app.get("/v1/orderbook/latest-full-snapshot")
async def latest_full_snapshot(
    market: str,
    symbol: str,
    snapshot_type: str | None = None,
) -> dict:
    if not app.state.pool:
        raise HTTPException(status_code=503, detail="database unavailable")
    query = """
        SELECT snapshot_time, snapshot_time_ns, market, symbol, snapshot_type,
               last_update_id, bid_count, ask_count, bids, asks
        FROM orderbook_full_snapshots_raw
        WHERE market = $1
          AND symbol = $2
          AND ($3::text IS NULL OR snapshot_type = $3)
        ORDER BY snapshot_time DESC
        LIMIT 1
    """
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow(query, market, symbol, snapshot_type)
    if not row:
        raise HTTPException(status_code=404, detail="full snapshot not found")
    return _record_to_dict(row)


@app.get("/v1/orderbook/snapshots")
async def full_snapshots(
    market: str,
    symbol: str,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
) -> dict:
    if not app.state.pool:
        raise HTTPException(status_code=503, detail="database unavailable")
    query = """
        SELECT snapshot_time, snapshot_time_ns, market, symbol, snapshot_type,
               last_update_id, bid_count, ask_count, bids, asks
        FROM orderbook_full_snapshots_raw
        WHERE market = $1
          AND symbol = $2
          AND ($3::timestamptz IS NULL OR snapshot_time >= $3)
          AND ($4::timestamptz IS NULL OR snapshot_time <= $4)
        ORDER BY snapshot_time DESC
        LIMIT $5
    """
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(query, market, symbol, start_time, end_time, limit)
    return {"items": [_record_to_dict(row) for row in rows]}


@app.get("/v1/orderbook/events")
async def raw_events(
    market: str,
    symbol: str,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int = Query(default=1000, ge=1, le=10000),
) -> dict:
    if not app.state.pool:
        raise HTTPException(status_code=503, detail="database unavailable")
    query = """
        SELECT exchange_time, exchange_time_ms, transaction_time_ms,
               receive_time_ns, market, symbol, first_update_id,
               last_update_id, prev_final_update_id, bids, asks
        FROM orderbook_events_raw
        WHERE market = $1
          AND symbol = $2
          AND ($3::timestamptz IS NULL OR exchange_time >= $3)
          AND ($4::timestamptz IS NULL OR exchange_time <= $4)
        ORDER BY exchange_time DESC
        LIMIT $5
    """
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(query, market, symbol, start_time, end_time, limit)
    return {"items": [_record_to_dict(row) for row in rows]}


def _record_to_dict(row: asyncpg.Record) -> dict:
    item = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            item[key] = value.isoformat()
        else:
            item[key] = value
    return item
