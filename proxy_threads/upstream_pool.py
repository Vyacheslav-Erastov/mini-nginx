from contextlib import contextmanager
from dataclasses import dataclass
from io import BufferedReader
import itertools
import socket
import threading

from proxy_threads.config import Config, Upstream
from proxy_threads.timeouts import TimeoutPolicy


@dataclass
class UpstreamConnection:
    reader: BufferedReader
    writer: socket.socket
    reusable: bool = False

    @property
    def closed(self):
        return self.reader.closed or self.writer.fileno() == -1

    @property
    def can_reused(self):
        old_timeout = self.writer.gettimeout()
        try:
            self.writer.setblocking(False)

            if self.writer.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
                return False

            try:
                self.writer.recv(1, socket.MSG_PEEK)
            except BlockingIOError:
                return True

            return False
        finally:
            self.writer.settimeout(old_timeout)


@dataclass
class UpstreamState:
    upstream: Upstream
    semaphore: threading.BoundedSemaphore
    idle_lock: threading.Lock
    idle_connections: list[UpstreamConnection]


class UpstreamPool:
    def __init__(self, config: Config, timeout_policy: TimeoutPolicy):
        if not config.upstreams:
            raise ValueError("At least one upstream must be configured")
        self._round_robin = itertools.cycle(config.upstreams)
        self._round_robin_lock = threading.Lock()
        self._states = {
            upstream: UpstreamState(
                upstream=upstream,
                semaphore=threading.BoundedSemaphore(
                    config.limits.max_conns_per_upstream
                ),
                idle_lock=threading.Lock(),
                idle_connections=[],
            )
            for upstream in config.upstreams
        }
        self.timeout_policy = timeout_policy

    @contextmanager
    def get_connection(self):
        with self._round_robin_lock:
            upstream = next(self._round_robin)
        state = self._states[upstream]

        acquired = state.semaphore.acquire(timeout=self.timeout_policy.connect)

        if not acquired:
            raise TimeoutError(
                f"Timed out waiting for connection to {upstream.host}:{upstream.port}"
            )

        connection = None

        try:
            with state.idle_lock:
                while state.idle_connections:
                    candidate = state.idle_connections.pop()

                    if not candidate.closed and candidate.can_reused:
                        connection = candidate
                        break

                    self._close_connection(candidate)

            if connection is None:
                upstream_socket = socket.create_connection(
                    address=(upstream.host, upstream.port),
                    timeout=self.timeout_policy.connect,
                )

                try:
                    upstream_reader = upstream_socket.makefile("rb")
                except Exception:
                    upstream_socket.close()
                    raise

                connection = UpstreamConnection(
                    reader=upstream_reader,
                    writer=upstream_socket,
                )

            connection.reusable = False

            yield upstream, connection

        finally:
            if connection is not None:
                if connection.reusable and not connection.closed:
                    with state.idle_lock:
                        state.idle_connections.append(connection)
                else:
                    self._close_connection(connection)

            state.semaphore.release()

    def _close_connection(
        self,
        connection: UpstreamConnection,
    ) -> None:
        try:
            connection.reader.close()
        except OSError:
            pass

        try:
            connection.writer.close()
        except OSError:
            pass

    def close(self) -> None:
        for state in self._states.values():
            with state.idle_lock:
                connections = state.idle_connections
                state.idle_connections = []

            for connection in connections:
                self._close_connection(connection)
