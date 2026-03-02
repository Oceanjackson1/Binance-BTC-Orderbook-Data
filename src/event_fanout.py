"""
Fan out collector events to multiple downstream writer queues.
"""

import asyncio
import logging
from typing import Sequence

logger = logging.getLogger(__name__)


class EventFanout:
    def __init__(
        self,
        source_queue: asyncio.Queue,
        target_queues: Sequence[asyncio.Queue],
    ) -> None:
        self.source_queue = source_queue
        self.target_queues = list(target_queues)

    async def run(self) -> None:
        logger.info("EventFanout started  targets=%d", len(self.target_queues))
        while True:
            try:
                event = await self.source_queue.get()
                for queue in self.target_queues:
                    await queue.put(event.copy())
            except asyncio.CancelledError:
                logger.info("EventFanout cancelled.")
                raise
