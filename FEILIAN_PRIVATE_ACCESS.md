# Feilian Private Access

This mode is for users who should only need Feilian login to fetch data.

## Target behavior

Users connect through Feilian and access the server's private IP:

```text
https://10.193.48.144/binance-orderbook/health
https://10.193.48.144/binance-orderbook/v1/keysets/index
https://10.193.48.144/binance-orderbook/v1/meta/markets
https://10.193.48.144/binance-orderbook/v1/curated/book_snapshots?market_slug=spot-btcusdt&timeframe=5m
```

In this mode they do **not** need:

- Tencent Cloud SSH key
- Tencent Cloud API key
- API Bearer token

## How it works

The API now supports dual access policy:

- client IP inside trusted private CIDRs: no token required
- client IP outside trusted private CIDRs: Bearer token still required

Trusted private access is determined from the reverse-proxy `X-Real-IP` set by Nginx.

## Required `.env` settings

```bash
ALLOW_PRIVATE_NETWORK_WITHOUT_AUTH=true
PRIVATE_ACCESS_CIDRS=127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16
```

If Feilian uses a narrower dedicated CIDR, replace the defaults with the actual Feilian source ranges.

Example:

```bash
ALLOW_PRIVATE_NETWORK_WITHOUT_AUTH=true
PRIVATE_ACCESS_CIDRS=10.200.0.0/16
```

## Restart

```bash
cd /opt/binance-orderbook/app
docker compose up -d --build api
```

## Verification

From a host that reaches the server through Feilian private networking:

```bash
curl -k https://10.193.48.144/binance-orderbook/health
curl -k https://10.193.48.144/binance-orderbook/v1/keysets/index
curl -k "https://10.193.48.144/binance-orderbook/v1/curated/book_snapshots?market_slug=spot-btcusdt&timeframe=5m&limit=5"
```

Expected result:

- `/health` works anonymously
- `/v1/*` also works without token when source IP is in trusted private CIDRs

## Security notes

- Keep Tencent Cloud public `443` closed if this service is meant to stay private.
- If you later open public `443`, public users will still need Bearer token unless they appear from trusted private CIDRs.
- Prefer using the exact Feilian source CIDRs instead of all RFC1918 ranges once you know them.
