-- Raw periodic order book snapshots (best bid/ask + depth metrics)
-- Written every ~1 second by the TimescaleDB writer.
CREATE TABLE IF NOT EXISTS orderbook_snapshots (
    ts              TIMESTAMPTZ     NOT NULL,
    market          TEXT            NOT NULL,   -- 'spot' | 'futures'
    symbol          TEXT            NOT NULL,   -- e.g. 'BTCUSDT'
    best_bid        NUMERIC,
    best_ask        NUMERIC,
    spread          NUMERIC,
    mid_price       NUMERIC,
    bid_depth_10    NUMERIC,                    -- total qty across top-10 bid levels
    ask_depth_10    NUMERIC,
    last_update_id  BIGINT
);

SELECT create_hypertable('orderbook_snapshots', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_obs_market_symbol
    ON orderbook_snapshots (market, symbol, ts DESC);

-- Collector health metrics
CREATE TABLE IF NOT EXISTS collector_health (
    ts              TIMESTAMPTZ     NOT NULL,
    market          TEXT            NOT NULL,
    symbol          TEXT            NOT NULL,
    restarts        INT             NOT NULL DEFAULT 0,
    last_update_id  BIGINT,
    is_live         BOOLEAN         NOT NULL DEFAULT FALSE
);

SELECT create_hypertable('collector_health', 'ts', if_not_exists => TRUE);
