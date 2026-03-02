CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS orderbook_events_raw (
    exchange_time         TIMESTAMPTZ NOT NULL,
    exchange_time_ms      BIGINT      NOT NULL,
    transaction_time_ms   BIGINT,
    receive_time_ns       BIGINT      NOT NULL,
    market                TEXT        NOT NULL,
    symbol                TEXT        NOT NULL,
    first_update_id       BIGINT      NOT NULL,
    last_update_id        BIGINT      NOT NULL,
    prev_final_update_id  BIGINT,
    bids                  JSONB       NOT NULL,
    asks                  JSONB       NOT NULL
);

SELECT create_hypertable('orderbook_events_raw', 'exchange_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_orderbook_events_raw_symbol_time
    ON orderbook_events_raw (market, symbol, exchange_time DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_orderbook_events_raw_unique
    ON orderbook_events_raw (market, symbol, exchange_time, last_update_id);

CREATE TABLE IF NOT EXISTS orderbook_full_snapshots_raw (
    snapshot_time       TIMESTAMPTZ NOT NULL,
    snapshot_time_ns    BIGINT      NOT NULL,
    market              TEXT        NOT NULL,
    symbol              TEXT        NOT NULL,
    snapshot_type       TEXT        NOT NULL,
    last_update_id      BIGINT      NOT NULL,
    bid_count           INT         NOT NULL,
    ask_count           INT         NOT NULL,
    bids                JSONB       NOT NULL,
    asks                JSONB       NOT NULL
);

SELECT create_hypertable('orderbook_full_snapshots_raw', 'snapshot_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_orderbook_full_snapshots_symbol_time
    ON orderbook_full_snapshots_raw (market, symbol, snapshot_time DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_orderbook_full_snapshots_unique
    ON orderbook_full_snapshots_raw (
        market, symbol, snapshot_time, last_update_id, snapshot_type
    );

CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    ts              TIMESTAMPTZ NOT NULL,
    market          TEXT        NOT NULL,
    symbol          TEXT        NOT NULL,
    best_bid        NUMERIC,
    best_ask        NUMERIC,
    spread          NUMERIC,
    mid_price       NUMERIC,
    bid_depth_10    NUMERIC,
    ask_depth_10    NUMERIC,
    last_update_id  BIGINT
);

SELECT create_hypertable('orderbook_snapshots', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_obs_market_symbol
    ON orderbook_snapshots (market, symbol, ts DESC);

CREATE TABLE IF NOT EXISTS collector_health (
    ts              TIMESTAMPTZ NOT NULL,
    market          TEXT        NOT NULL,
    symbol          TEXT        NOT NULL,
    restarts        INT         NOT NULL DEFAULT 0,
    last_update_id  BIGINT,
    is_live         BOOLEAN     NOT NULL DEFAULT FALSE
);

SELECT create_hypertable('collector_health', 'ts', if_not_exists => TRUE);
