import asyncio
from asyncio.streams import StreamReader, StreamWriter

from proxy.client_handler import ClientHandler
from proxy.config import Config
from proxy.metrics import Metrics
from proxy.timeouts import TimeoutPolicy
from proxy.upstream_pool import UpstreamPool
from proxy.utils import logs


logger = logs.get_logger(__name__)


class ProxyServer:
    def __init__(self, config: Config):
        self.config = config
        self.server: asyncio.Server | None = None
        self.timeout_policy = TimeoutPolicy.from_config(config.timeouts)
        self.client_semaphore = asyncio.Semaphore(config.limits.max_client_conns)
        upstream_pool = UpstreamPool(config)
        self.metrics = Metrics()
        self.client_handler = ClientHandler(
            config=config,
            timeout_policy=self.timeout_policy,
            upstream_pool=upstream_pool,
            metrics=self.metrics,
        )

    async def start(self):
        self.server = await asyncio.start_server(
            self._accept_client, self.config.listen_host, self.config.listen_port
        )
        address = self.server.sockets[0].getsockname()
        logger.info("start_proxy", address=address)
        async with self.server:
            await self.server.serve_forever()

    async def _accept_client(
        self, client_reader: StreamReader, client_writer: StreamWriter
    ):
        try:
            address = client_writer.get_extra_info("peername")

            self.metrics.client_connected()

            logger.info(
                "client_connected",
                client_address=address,
                active_connections=(self.metrics.client_connections_active),
            )

            async with self.client_semaphore:
                await self.client_handler.handle_client(client_reader, client_writer)

        except asyncio.CancelledError:
            logger.info(
                "client_cancelled",
                client_address=address,
            )
            raise
        except Exception:
            logger.exception(
                "client_error",
                client_address=address,
            )
        finally:
            logger.info(
                "client_disconnected",
                client_address=address,
                active_connections=(self.metrics.client_connections_active),
            )
