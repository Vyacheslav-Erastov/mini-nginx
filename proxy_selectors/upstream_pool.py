from __future__ import annotations

from dataclasses import dataclass, field
import itertools
import socket
from time import monotonic

from proxy_selectors.config import Config, Upstream
from proxy_selectors.timeouts import TimeoutPolicy


class UpstreamPoolExhausted(ConnectionError):
    pass


@dataclass
class IdleConnection:
    socket: socket.socket
    idle_since: float


@dataclass
class UpstreamState:
    total_connections: int = 0
    idle_connections: list[IdleConnection] = field(default_factory=list)


@dataclass
class UpstreamLease:
    upstream: Upstream
    socket: socket.socket
    connected: bool
    released: bool = False


class SelectorUpstreamPool:
    def __init__(
        self,
        config: Config,
        timeout_policy: TimeoutPolicy,
    ):
        if not config.upstreams:
            raise ValueError("At least one upstream must be configured")

        self._upstreams = config.upstreams
        self._round_robin = itertools.cycle(config.upstreams)
        self._max_connections = config.limits.max_conns_per_upstream
        self._idle_timeout = timeout_policy.read
        self._states = {upstream: UpstreamState() for upstream in config.upstreams}

    def acquire(self) -> UpstreamLease:
        for _ in range(len(self._upstreams)):
            upstream = next(self._round_robin)
            state = self._states[upstream]

            while state.idle_connections:
                idle = state.idle_connections.pop()

                if self._can_reuse(idle):
                    return UpstreamLease(
                        upstream=upstream,
                        socket=idle.socket,
                        connected=True,
                    )

                self._close_socket(idle.socket)
                state.total_connections -= 1

            if state.total_connections >= self._max_connections:
                continue

            upstream_socket = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM,
            )
            upstream_socket.setblocking(False)
            state.total_connections += 1

            return UpstreamLease(
                upstream=upstream,
                socket=upstream_socket,
                connected=False,
            )

        raise UpstreamPoolExhausted("All upstream connection pools are exhausted")

    def release(
        self,
        lease: UpstreamLease,
        reusable: bool,
    ) -> None:
        if lease.released:
            return

        lease.released = True
        state = self._states[lease.upstream]

        idle = IdleConnection(
            socket=lease.socket,
            idle_since=monotonic(),
        )

        if reusable and self._can_reuse(idle):
            state.idle_connections.append(idle)
            return

        self._close_socket(lease.socket)
        state.total_connections -= 1

    def close(self) -> None:
        for state in self._states.values():
            connections = state.idle_connections
            state.idle_connections = []

            for idle in connections:
                self._close_socket(idle.socket)

            state.total_connections -= len(connections)

    def _can_reuse(self, idle: IdleConnection) -> bool:
        upstream_socket = idle.socket

        if upstream_socket.fileno() == -1:
            return False

        if monotonic() - idle.idle_since >= self._idle_timeout:
            return False

        try:
            if upstream_socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR):
                return False

            upstream_socket.recv(1, socket.MSG_PEEK)
        except BlockingIOError:
            return True
        except OSError:
            return False

        return False

    def _close_socket(self, upstream_socket: socket.socket) -> None:
        try:
            upstream_socket.close()
        except OSError:
            pass
