import asyncio
import ipaddress
import os
import time
from collections import defaultdict, deque
from typing import Deque

from fastapi import Request
from starlette.responses import JSONResponse


class ApiSecurity:
    def __init__(self) -> None:
        token = os.getenv("ORDERBOOK_API_TOKEN") or os.getenv("ORDERBOOK_API_KEY")
        if not token:
            raise RuntimeError("ORDERBOOK_API_TOKEN or ORDERBOOK_API_KEY must be set.")

        self.token = token
        self.rate_limit_per_minute = int(os.getenv("API_RATE_LIMIT_PER_MINUTE", "240"))
        self.allow_private_network_without_auth = (
            os.getenv("ALLOW_PRIVATE_NETWORK_WITHOUT_AUTH", "false").strip().lower()
            in {"1", "true", "yes", "on"}
        )
        self.trusted_private_networks = _parse_networks(
            os.getenv(
                "PRIVATE_ACCESS_CIDRS",
                "127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16",
            )
        )
        self._requests: dict[str, Deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def enforce(self, request: Request, call_next):
        if request.url.path == "/health":
            return await call_next(request)

        if not self._is_private_access(request):
            supplied_token = _extract_bearer_token(request) or request.headers.get("x-api-key")
            if supplied_token != self.token:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "invalid bearer token"},
                )

        if self.rate_limit_per_minute > 0:
            retry_after = await self._check_rate_limit(request)
            if retry_after is not None:
                return JSONResponse(
                    status_code=429,
                    content={"detail": "rate limit exceeded"},
                    headers={"Retry-After": str(retry_after)},
                )

        return await call_next(request)

    async def _check_rate_limit(self, request: Request) -> int | None:
        now = time.monotonic()
        client_id = _client_ip(request) or "unknown"

        async with self._lock:
            bucket = self._requests[client_id]
            while bucket and now - bucket[0] >= 60:
                bucket.popleft()

            if len(bucket) >= self.rate_limit_per_minute:
                return max(1, int(60 - (now - bucket[0])))

            bucket.append(now)

        return None

    def _is_private_access(self, request: Request) -> bool:
        if not self.allow_private_network_without_auth:
            return False

        client_ip = _client_ip(request)
        if not client_ip:
            return False

        try:
            ip = ipaddress.ip_address(client_ip)
        except ValueError:
            return False

        return any(ip in network for network in self.trusted_private_networks)


def _parse_networks(raw: str) -> list[ipaddress._BaseNetwork]:
    networks = []
    for item in raw.split(","):
        cidr = item.strip()
        if not cidr:
            continue
        networks.append(ipaddress.ip_network(cidr, strict=False))
    return networks


def _client_ip(request: Request) -> str:
    x_real_ip = request.headers.get("x-real-ip")
    if x_real_ip:
        return x_real_ip.strip()

    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[-1].strip()

    if request.client and request.client.host:
        return request.client.host

    return ""


def _extract_bearer_token(request: Request) -> str | None:
    header = request.headers.get("authorization", "")
    if not header:
        return None
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token.strip()
