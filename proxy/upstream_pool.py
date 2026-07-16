import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
import itertools

from proxy.config import Config, Upstream
from proxy.timeouts import TimeoutPolicy


@dataclass
class UpstreamConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    reusable: bool = False

    @property
    def closed(self):
        return self.writer.is_closing() or self.reader.at_eof()


@dataclass
class UpstreamState:
    upstream: Upstream
    semaphore: asyncio.Semaphore
    idle_connections: list[UpstreamConnection]


class UpstreamPool:
    def __init__(self, config: Config, timeout_policy: TimeoutPolicy):
        if not config.upstreams:
            raise ValueError("At least one upstream must be configured")
        self._round_robin = itertools.cycle(config.upstreams)
        self._states = {
            upstream: UpstreamState(
                upstream=upstream,
                semaphore=asyncio.Semaphore(config.limits.max_conns_per_upstream),
                idle_connections=[],
            )
            for upstream in config.upstreams
        }
        self.timeout_policy = timeout_policy

    @asynccontextmanager
    async def get_connection(self):
        upstream = next(self._round_robin)
        state = self._states[upstream]

        async with state.semaphore:
            connection = None

            while state.idle_connections:
                candidate = state.idle_connections.pop()

                if not candidate.closed:
                    connection = candidate
                    break
                else:
                    await self._close_connection(candidate)

            if connection is None:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(upstream.host, upstream.port),
                    timeout=self.timeout_policy.connect,
                )
                connection = UpstreamConnection(reader, writer)

            connection.reusable = False
            
            try:
                yield upstream, connection
            finally:
                if not connection.closed and connection.reusable:
                    state.idle_connections.append(connection)
                else:
                    await self._close_connection(connection)

    async def _close_connection(self, connection: UpstreamConnection) -> None:
        connection.writer.close()

        try:
            await connection.writer.wait_closed()
        except ConnectionError, OSError:
            pass
