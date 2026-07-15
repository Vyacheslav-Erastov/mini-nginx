import asyncio
from contextlib import asynccontextmanager
import itertools

from proxy.config import Config


class UpstreamPool:
    def __init__(self, config: Config):
        if not config.upstreams:
            raise ValueError("At least one upstream must be configured")
        self._round_robin = itertools.cycle(config.upstreams)
        self._semaphores = {
            up: asyncio.Semaphore(config.limits.max_conns_per_upstream)
            for up in config.upstreams
        }

    @asynccontextmanager
    async def get_upstream(self):
        upstream = next(self._round_robin)
        semaphore = self._semaphores[upstream]

        async with semaphore:
            yield upstream
