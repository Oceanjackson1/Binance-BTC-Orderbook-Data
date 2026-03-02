# Tencent Cloud Deployment

## Services

- `timescaledb`: stores raw full snapshots, raw diff events, top-of-book summaries, collector health
- `collector`: records Binance `spot.BTCUSDT` and `futures.BTCUSDT` full order books
- `materialize`: builds `publish/raw`, `publish/curated`, `publish/meta`
- `api`: serves the public read-only API from `publish/`
- `backup`: rotates local DB dumps and syncs `publish/` plus DB dumps to COS
- `nginx` on host: terminates TLS and proxies `/<agent_id>/health` and `/<agent_id>/v1/*`

## Public API contract

Internal API binds to `127.0.0.1:18080`.
Public HTTPS is exposed through Nginx:

- `GET /<agent_id>/health`
- `GET /<agent_id>/v1/keysets/index`
- `GET /<agent_id>/v1/keysets/{dt}/{timeframe}`
- `GET /<agent_id>/v1/meta/markets`
- `GET /<agent_id>/v1/meta/files`
- `GET /<agent_id>/v1/curated/{dataset}`

Supported curated datasets:

- `book_snapshots`
- `price_changes`
- `recovery_events`
- `trades` returns an empty result for this agent because no trade feed is recorded

All `/<agent_id>/v1/*` endpoints require:

```text
Authorization: Bearer <ORDERBOOK_API_TOKEN>
```

`/health` stays unauthenticated.

## Publish layout

```text
/app/publish
  raw/
  curated/
  meta/
```

COS prefix should follow the multi-agent standard:

```text
agents/<agent_id>/raw/...
agents/<agent_id>/curated/...
agents/<agent_id>/meta/...
agents/<agent_id>/db/...
```

## `.env`

Minimum settings:

```bash
POSTGRES_PASSWORD=<strong-password>
TIMESCALE_DSN=postgresql://postgres:${POSTGRES_PASSWORD}@localhost:5432/orderbook
ORDERBOOK_API_DSN=postgresql://postgres:${POSTGRES_PASSWORD}@localhost:5432/orderbook
AGENT_ID=binance-orderbook
DATA_DIR=./data
PUBLISH_DIR=./publish
PUBLISH_TIMEFRAMES=5m,15m,1h,4h
PUBLISH_LOOKBACK_DAYS=2
PUBLISH_STABLE_SECONDS=120
MATERIALIZE_INTERVAL_SECONDS=60
API_PORT=18080
ORDERBOOK_API_TOKEN=<strong-random-token>
API_RATE_LIMIT_PER_MINUTE=240
ALLOW_PRIVATE_NETWORK_WITHOUT_AUTH=false
PRIVATE_ACCESS_CIDRS=127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16
TENCENT_COS_BUCKET=polymarket-ai-agent-1328550492
TENCENT_COS_REGION=ap-tokyo
TENCENT_COS_PREFIX=agents/binance-orderbook
TENCENTCLOUD_SECRET_ID=<secret-id>
TENCENTCLOUD_SECRET_KEY=<secret-key>
TENCENT_COS_SYNC_INTERVAL_SECONDS=30
TENCENT_COS_STABLE_SECONDS=10
DB_BACKUP_INTERVAL_SECONDS=3600
DB_BACKUP_RETENTION_COUNT=48
```

## Deploy

```bash
mkdir -p /opt/binance-orderbook
cd /opt/binance-orderbook
git clone <your-repo-url> app
cd app
cp .env.example .env
vi .env
docker compose up -d --build
```

## Install HTTPS reverse proxy

```bash
cd /opt/binance-orderbook/app
chmod +x deploy/install_nginx_proxy.sh
AGENT_ID=binance-orderbook \
SERVER_NAME=_ \
CERT_CN=<public-domain-or-public-ip> \
LOCAL_API_PORT=18080 \
sudo ./deploy/install_nginx_proxy.sh
```

This generates a self-signed certificate by default. Replace it with a real certificate before long-term external use.

## Feilian private-access mode

If users should only need Feilian login and no API token:

```bash
ALLOW_PRIVATE_NETWORK_WITHOUT_AUTH=true
PRIVATE_ACCESS_CIDRS=127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16
```

In this mode:

- private-network clients can call `https://10.193.48.144/binance-orderbook/v1/...` without Bearer token
- non-private clients still need `Authorization: Bearer <ORDERBOOK_API_TOKEN>`

## Validate

```bash
docker compose ps
docker compose logs -f collector
docker compose logs -f materialize
docker compose logs -f backup
curl -k https://127.0.0.1/binance-orderbook/health
curl -k -H "Authorization: Bearer $ORDERBOOK_API_TOKEN" \
  "https://127.0.0.1/binance-orderbook/v1/keysets/index"
curl -k -H "Authorization: Bearer $ORDERBOOK_API_TOKEN" \
  "https://127.0.0.1/binance-orderbook/v1/curated/book_snapshots?market_slug=spot-btcusdt&timeframe=5m"
```
