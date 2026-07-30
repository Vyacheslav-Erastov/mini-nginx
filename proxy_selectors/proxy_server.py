from __future__ import annotations

import selectors
import socket
from time import monotonic

from proxy_selectors.client_handler import SelectorClientHandler
from proxy_selectors.metrics import Metrics
from proxy_selectors.timeouts import TimeoutPolicy
from proxy_selectors.upstream_pool import SelectorUpstreamPool
from proxy_selectors.utils import logs
from proxy_selectors.config import Config


logger = logs.get_logger(__name__)


class SelectorProxyServer:
    def __init__(self, config: Config):
        self.config = config
        self.timeout_policy = TimeoutPolicy.from_config(config.timeouts)
        self.metrics = Metrics()
        self.upstream_pool = SelectorUpstreamPool(
            config=config,
            timeout_policy=self.timeout_policy,
        )

        self.server_socket: socket.socket | None = None
        self.selector: selectors.BaseSelector | None = None
        self.handlers: set[SelectorClientHandler] = set()
        self.stopping = False

    def start(self) -> None:
        if self.server_socket is not None:
            raise RuntimeError("Proxy server is already running")

        self.stopping = False

        with selectors.DefaultSelector() as selector:
            self.selector = selector

            with socket.create_server(
                (
                    self.config.listen_host,
                    self.config.listen_port,
                ),
                reuse_port=True,
            ) as server_socket:
                self.server_socket = server_socket
                server_socket.setblocking(False)
                selector.register(
                    server_socket,
                    selectors.EVENT_READ,
                    data=None,
                )

                logger.info(
                    "proxy_started",
                    address=server_socket.getsockname(),
                )

                try:
                    self._serve_forever()
                finally:
                    self._close_handlers()
                    self.upstream_pool.close()
                    self.server_socket = None
                    self.selector = None
                    logger.info("proxy_stopped")

    def stop(self) -> None:
        self.stopping = True

    def _serve_forever(self) -> None:
        if self.selector is None:
            raise RuntimeError("Selector is not initialized")

        while not self.stopping:
            timeout = self._get_select_timeout()

            try:
                events = self.selector.select(timeout)
            except OSError:
                if self.stopping:
                    break
                raise

            for key, mask in events:
                if key.data is None:
                    self._accept_clients(key.fileobj)
                else:
                    handler: SelectorClientHandler = key.data
                    handler.handle_event(key.fileobj, mask)

            now = monotonic()

            for handler in tuple(self.handlers):
                handler.check_timeouts(now)

    def _accept_clients(self, server_socket: socket.socket) -> None:
        while True:
            try:
                client_socket, client_address = server_socket.accept()
            except BlockingIOError:
                return

            if len(self.handlers) >= self.config.limits.max_client_conns:
                logger.warning(
                    "client_limit_reached",
                    client_address=client_address,
                )
                client_socket.close()
                continue

            handler = SelectorClientHandler(
                selector=self.selector,
                client_socket=client_socket,
                client_address=client_address,
                upstream_pool=self.upstream_pool,
                timeout_policy=self.timeout_policy,
                metrics=self.metrics,
                on_close=self._remove_handler,
            )
            self.handlers.add(handler)

            try:
                handler.start()
            except Exception:
                logger.exception(
                    "accept_client_failed",
                    client_address=client_address,
                )
                handler.close()

    def _get_select_timeout(self) -> float:
        nearest_deadline = None

        for handler in self.handlers:
            deadline = handler.next_deadline()

            if deadline is None:
                continue

            if nearest_deadline is None or deadline < nearest_deadline:
                nearest_deadline = deadline

        if nearest_deadline is None:
            return 1.0

        return min(
            1.0,
            max(0.0, nearest_deadline - monotonic()),
        )

    def _remove_handler(
        self,
        handler: SelectorClientHandler,
    ) -> None:
        self.handlers.discard(handler)

    def _close_handlers(self) -> None:
        for handler in tuple(self.handlers):
            handler.close()
