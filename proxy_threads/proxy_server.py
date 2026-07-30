import socket
import threading
from concurrent.futures import ThreadPoolExecutor

from proxy_threads.client_handler import ClientHandler
from proxy_threads.config import Config
from proxy_threads.metrics import Metrics
from proxy_threads.timeouts import TimeoutPolicy
from proxy_threads.upstream_pool import UpstreamPool
from proxy_threads.utils import logs


logger = logs.get_logger(__name__)


class ProxyServer:
    def __init__(self, config: Config):
        self.config = config

        self.server_socket: socket.socket | None = None
        self._stop_event = threading.Event()

        self.timeout_policy = TimeoutPolicy.from_config(config.timeouts)

        self.client_semaphore = threading.BoundedSemaphore(
            config.limits.max_client_conns
        )

        self.executor = ThreadPoolExecutor(
            max_workers=config.limits.max_client_conns,
            thread_name_prefix="proxy-client",
        )

        upstream_pool = UpstreamPool(
            config,
            timeout_policy=self.timeout_policy,
        )

        self.metrics = Metrics()

        self.client_handler = ClientHandler(
            config=config,
            timeout_policy=self.timeout_policy,
            upstream_pool=upstream_pool,
            metrics=self.metrics,
        )

        self._client_sockets: set[socket.socket] = set()
        self._client_sockets_lock = threading.Lock()

    def start(self) -> None:
        if self.server_socket is not None:
            raise RuntimeError("Proxy server is already running")

        self._stop_event.clear()

        self.server_socket = socket.create_server(
            address=(
                self.config.listen_host,
                self.config.listen_port,
            ),
            reuse_port=True
        )

        self.server_socket.settimeout(1.0)

        address = self.server_socket.getsockname()

        logger.info(
            "start_proxy",
            address=address,
        )

        try:
            self._serve_forever()

        except KeyboardInterrupt:
            logger.info("proxy_interrupted")

        except Exception:
            logger.exception("proxy_server_error")
            raise

        finally:
            self.stop()

            self.executor.shutdown(
                wait=True,
                cancel_futures=False,
            )

            logger.info("proxy_stopped")

    def _serve_forever(self) -> None:
        if self.server_socket is None:
            raise RuntimeError("Server socket is not initialized")

        while not self._stop_event.is_set():
            try:
                client_socket, client_address = self.server_socket.accept()

            except TimeoutError:
                continue

            except OSError:
                if self._stop_event.is_set():
                    break
                raise

            try:
                self.executor.submit(
                    self._accept_client,
                    client_socket,
                    client_address,
                )

            except RuntimeError:
                client_socket.close()

                if not self._stop_event.is_set():
                    raise

    def _accept_client(
        self,
        client_socket: socket.socket,
        client_address: tuple,
    ) -> None:
        connected = False

        self._register_client_socket(client_socket)

        try:
            self.metrics.client_connected()
            connected = True

            logger.info(
                "client_connected",
                client_address=client_address,
                active_connections=self.metrics.client_connections_active,
            )

            with self.client_semaphore:
                with client_socket:
                    self.client_handler.handle_client(client_socket)

        except TimeoutError:
            logger.info(
                "client_timeout",
                client_address=client_address,
            )

        except ConnectionError:
            logger.info(
                "client_connection_closed",
                client_address=client_address,
            )

        except OSError as error:
            if self._stop_event.is_set():
                logger.info(
                    "client_cancelled",
                    client_address=client_address,
                )
            else:
                logger.exception(
                    "client_socket_error",
                    client_address=client_address,
                    error=str(error),
                )

        except Exception:
            logger.exception(
                "client_error",
                client_address=client_address,
            )

        finally:
            self._unregister_client_socket(client_socket)
            try:
                client_socket.close()
            except OSError:
                pass

            if connected:
                self.metrics.client_disconnected()

            logger.info(
                "client_disconnected",
                client_address=client_address,
                active_connections=self.metrics.client_connections_active,
            )

    def stop(self) -> None:
        self._stop_event.set()

        server_socket = self.server_socket
        self.server_socket = None

        if server_socket is not None:
            try:
                server_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

            try:
                server_socket.close()
            except OSError:
                pass

        with self._client_sockets_lock:
            client_sockets = tuple(self._client_sockets)

        for client_socket in client_sockets:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

            try:
                client_socket.close()
            except OSError:
                pass

    def _register_client_socket(
        self,
        client_socket: socket.socket,
    ) -> None:
        with self._client_sockets_lock:
            self._client_sockets.add(client_socket)

    def _unregister_client_socket(
        self,
        client_socket: socket.socket,
    ) -> None:
        with self._client_sockets_lock:
            self._client_sockets.discard(client_socket)
