"""
Spot order book collector.

Endpoints:
  REST:  https://api.binance.com/api/v3/depth  (max 5000 levels)
  WS:    wss://stream.binance.com:9443/ws/<symbol>@depth@100ms
"""

import asyncio

from src.collector.base_collector import BaseCollector


class SpotCollector(BaseCollector):
    MARKET = "spot"
    REST_LIMIT = 5000

    def __init__(self, symbol: str, event_queue: asyncio.Queue) -> None:
        super().__init__(symbol=symbol, event_queue=event_queue)

    def get_ws_url(self) -> str:
        return f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@depth@100ms"

    def get_rest_url(self) -> str:
        return "https://api.binance.com/api/v3/depth"

    # Spot uses the default bridging condition and continuity check
    # from BaseCollector — no overrides needed.
