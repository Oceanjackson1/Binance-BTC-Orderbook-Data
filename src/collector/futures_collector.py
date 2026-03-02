"""
USD-M Perpetual Futures order book collector.

Endpoints:
  REST:  https://fapi.binance.com/fapi/v1/depth  (max 1000 levels)
  WS:    wss://fstream.binance.com/ws/<symbol>@depth@100ms

Key differences from spot:
  - REST snapshot cap is 1000 (vs 5000 for spot)
  - Each WS event carries a `pu` field (previous final update ID)
  - Bridging condition: U <= snap_lid AND u >= snap_lid
    (spot uses snap_lid+1 on both sides)
  - Continuity check uses event['pu'] == prev_u
    (spot checks U == prev_u + 1)
  - Events also carry a transaction time `T` (separate from event time `E`)
"""

import asyncio

from src.collector.base_collector import BaseCollector


class FuturesCollector(BaseCollector):
    MARKET = "futures"
    REST_LIMIT = 1000

    def __init__(self, symbol: str, event_queue: asyncio.Queue) -> None:
        super().__init__(symbol=symbol, event_queue=event_queue)

    def get_ws_url(self) -> str:
        return f"wss://fstream.binance.com/ws/{self.symbol.lower()}@depth@100ms"

    def get_rest_url(self) -> str:
        return "https://fapi.binance.com/fapi/v1/depth"

    def is_bridging_event(self, event: dict, snap_lid: int) -> bool:
        """
        Futures bridging rule (Binance docs):
          U <= lastUpdateId AND u >= lastUpdateId
        """
        U = self.get_first_update_id(event)
        u = self.get_final_update_id(event)
        return U <= snap_lid and u >= snap_lid

    def check_continuity(self, event: dict, prev_final_id: int) -> bool:
        """
        Futures continuity rule:
          event['pu'] must equal the previous event's 'u'.
        """
        return event.get("pu") == prev_final_id

    async def _apply_live_event(self, event: dict) -> None:
        """
        Extends the base implementation to also record the futures-specific
        transaction time `T` in the output payload.
        """
        import time

        if not self.check_continuity(event, self.book.last_update_id):
            raise RuntimeError(
                f"Gap detected in futures live stream: "
                f"prev_u={self.book.last_update_id}  "
                f"U={self.get_first_update_id(event)}  "
                f"pu={event.get('pu')}"
            )

        receive_time_ns: int = time.time_ns()

        self.book.apply_bids_asks(event.get("b", []), event.get("a", []))
        self.book.last_update_id = self.get_final_update_id(event)

        await self.event_queue.put(
            {
                "market": self.MARKET,
                "symbol": self.symbol,
                "exchange_time_ms": event.get("E"),
                "transaction_time_ms": event.get("T"),   # futures-only field
                "receive_time_ns": receive_time_ns,
                "first_update_id": self.get_first_update_id(event),
                "last_update_id": self.get_final_update_id(event),
                "prev_final_update_id": event.get("pu"),  # futures-only field
                "bids": event.get("b", []),
                "asks": event.get("a", []),
            }
        )
