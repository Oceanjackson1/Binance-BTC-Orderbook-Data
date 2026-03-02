"""
Local order book maintained in memory.

Structure:
  - bids: SortedDict keyed by price descending (highest first)
  - asks: SortedDict keyed by price ascending  (lowest first)

All prices and quantities are kept as Decimal to avoid float precision loss.
"""
from decimal import Decimal
from typing import List, Optional, Tuple

from sortedcontainers import SortedDict


class LocalOrderBook:
    def __init__(self, symbol: str, market: str) -> None:
        self.symbol = symbol
        self.market = market

        # Negate key so highest bid comes first in iteration
        self.bids: SortedDict = SortedDict(lambda x: -x)
        self.asks: SortedDict = SortedDict()

        self.last_update_id: int = 0
        self.initialized: bool = False

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def init_from_snapshot(self, snapshot: dict) -> None:
        """Seed the book from a REST depth snapshot response."""
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = snapshot["lastUpdateId"]

        for price_str, qty_str in snapshot["bids"]:
            qty = Decimal(qty_str)
            if qty > 0:
                self.bids[Decimal(price_str)] = qty

        for price_str, qty_str in snapshot["asks"]:
            qty = Decimal(qty_str)
            if qty > 0:
                self.asks[Decimal(price_str)] = qty

        self.initialized = True

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = 0
        self.initialized = False

    # ------------------------------------------------------------------
    # Applying diff events
    # ------------------------------------------------------------------

    def apply_bids_asks(
        self,
        bids: List[List[str]],
        asks: List[List[str]],
    ) -> None:
        """
        Apply a list of bid and ask changes from a depthUpdate event.

        Quantity == "0" means the price level must be removed.
        All quantities are absolute (not deltas).
        """
        for price_str, qty_str in bids:
            price = Decimal(price_str)
            qty = Decimal(qty_str)
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty

        for price_str, qty_str in asks:
            price = Decimal(price_str)
            qty = Decimal(qty_str)
            if qty == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    def best_bid(self) -> Optional[Tuple[Decimal, Decimal]]:
        if not self.bids:
            return None
        price = self.bids.keys()[0]
        return price, self.bids[price]

    def best_ask(self) -> Optional[Tuple[Decimal, Decimal]]:
        if not self.asks:
            return None
        price = self.asks.keys()[0]
        return price, self.asks[price]

    def mid_price(self) -> Optional[Decimal]:
        bb = self.best_bid()
        ba = self.best_ask()
        if bb and ba:
            return (bb[0] + ba[0]) / 2
        return None

    def spread(self) -> Optional[Decimal]:
        bb = self.best_bid()
        ba = self.best_ask()
        if bb and ba:
            return ba[0] - bb[0]
        return None

    def depth(
        self,
        n: int = 10,
        side: str = "bid",
    ) -> List[Tuple[Decimal, Decimal]]:
        """Return the top-n levels on the given side as [(price, qty), ...]."""
        if side == "bid":
            keys = list(self.bids.keys())[:n]
            return [(p, self.bids[p]) for p in keys]
        else:
            keys = list(self.asks.keys())[:n]
            return [(p, self.asks[p]) for p in keys]

    def to_snapshot_lists(self) -> tuple:
        """
        Return (bids, asks) as [[price_str, qty_str], ...] for serialisation.

        bids: highest price first (SortedDict negated-key order)
        asks: lowest  price first (SortedDict natural order)
        """
        bids = [[str(p), str(q)] for p, q in self.bids.items()]
        asks = [[str(p), str(q)] for p, q in self.asks.items()]
        return bids, asks

    def __repr__(self) -> str:
        bb = self.best_bid()
        ba = self.best_ask()
        return (
            f"<LocalOrderBook {self.market}:{self.symbol} "
            f"bid={bb[0] if bb else 'N/A'} "
            f"ask={ba[0] if ba else 'N/A'} "
            f"lid={self.last_update_id}>"
        )
