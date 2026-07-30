from __future__ import annotations

import errno
import os
import selectors
import socket
from enum import Enum, auto
from time import monotonic
from typing import Callable, TypeVar

from proxy_selectors.metrics import Metrics
from proxy_selectors.timeouts import TimeoutPolicy
from proxy_selectors.upstream_pool import (
    SelectorUpstreamPool,
    UpstreamLease,
)
from proxy_selectors.utils import logs
from proxy_selectors.utils.http import HttpRequestHead, HttpResponseHead


logger = logs.get_logger(__name__)

CHUNK_SIZE = 8192
MAX_BUFFER_SIZE = 64 * 1024
MAX_HTTP_HEAD_SIZE = 64 * 1024
Head = TypeVar("Head")


class HandlerState(Enum):
    READING_REQUEST = auto()
    CONNECTING_UPSTREAM = auto()
    SENDING_REQUEST = auto()
    READING_RESPONSE = auto()
    FLUSHING_RESPONSE = auto()
    CLOSED = auto()


class SelectorClientHandler:
    def __init__(
        self,
        selector: selectors.BaseSelector,
        client_socket: socket.socket,
        client_address: tuple,
        upstream_pool: SelectorUpstreamPool,
        timeout_policy: TimeoutPolicy,
        metrics: Metrics,
        on_close: Callable[[SelectorClientHandler], None],
    ):
        self.selector = selector
        self.client_socket = client_socket
        self.client_address = client_address
        self.upstream_pool = upstream_pool
        self.timeout_policy = timeout_policy
        self.metrics = metrics
        self.on_close = on_close

        self.state = HandlerState.READING_REQUEST
        self.closed = False
        self.client_counted = False

        self.upstream_lease: UpstreamLease | None = None
        self.upstream_socket: socket.socket | None = None

        self.client_input = bytearray()
        self.upstream_input = bytearray()
        self.to_upstream = bytearray()
        self.to_client = bytearray()

        self.request: HttpRequestHead | None = None
        self.response: HttpResponseHead | None = None
        self.request_body_remaining = 0
        self.response_body_remaining = 0
        self.request_body_size = 0
        self.response_body_size = 0

        self.client_keep_alive = False
        self.upstream_reusable = False

        self.request_started_at: float | None = None
        self.request_logger = None
        self.deadlines: dict[str, float] = {}

        self.connection_logger = logger.bind(
            client_address=client_address,
        )

    def start(self) -> None:
        self.client_socket.setblocking(False)
        self.selector.register(
            self.client_socket,
            selectors.EVENT_READ,
            data=self,
        )
        self._arm_timeout("client_read", self.timeout_policy.read)

        self.metrics.client_connected()
        self.client_counted = True

        self.connection_logger.info(
            "client_connected",
            active_connections=self.metrics.client_connections_active,
        )

    def handle_event(
        self,
        sock: socket.socket,
        mask: int,
    ) -> None:
        if self.closed:
            return

        try:
            if (
                sock is self.upstream_socket
                and self.state == HandlerState.CONNECTING_UPSTREAM
                and mask & selectors.EVENT_WRITE
            ):
                self._finish_upstream_connect()

            if self.closed:
                return

            if mask & selectors.EVENT_READ:
                if sock is self.client_socket:
                    self._read_client()
                elif sock is self.upstream_socket:
                    self._read_upstream()

            if self.closed:
                return

            if mask & selectors.EVENT_WRITE:
                if sock is self.client_socket:
                    self._write_client()
                elif sock is self.upstream_socket:
                    self._write_upstream()

        except Exception as error:
            current_logger = self.request_logger or self.connection_logger
            current_logger.exception(
                "connection_error",
                error=str(error),
            )
            self.close()

    def next_deadline(self) -> float | None:
        if not self.deadlines:
            return None

        return min(self.deadlines.values())

    def check_timeouts(self, now: float) -> None:
        if self.closed:
            return

        expired = {name for name, deadline in self.deadlines.items() if deadline <= now}

        if not expired:
            return

        priority = (
            "request_total",
            "upstream_connect",
            "upstream_read",
            "upstream_write",
            "client_read",
            "client_write",
        )
        timeout_name = next(name for name in priority if name in expired)

        if self.request_started_at is None:
            self.connection_logger.info(
                "client_idle_timeout",
                timeout=timeout_name,
            )
        else:
            self.request_logger.warning(
                "request_timeout",
                timeout=timeout_name,
                duration_ms=round(
                    (now - self.request_started_at) * 1000,
                    2,
                ),
            )
            self.metrics.request_timed_out()
            self.metrics.request_finished(
                duration_seconds=now - self.request_started_at,
            )
            self.request_started_at = None

        self.close()

    def close(self) -> None:
        if self.closed:
            return

        self.closed = True
        self.state = HandlerState.CLOSED
        self.deadlines.clear()

        if self.request_started_at is not None:
            now = monotonic()
            self.metrics.request_failed()
            self.metrics.request_finished(
                duration_seconds=now - self.request_started_at,
            )
            self.request_started_at = None

        self._release_upstream(reusable=False)

        try:
            self.selector.unregister(self.client_socket)
        except KeyError:
            pass

        try:
            self.client_socket.close()
        except OSError:
            pass

        if self.client_counted:
            self.metrics.client_disconnected()
            self.client_counted = False

        self.connection_logger.info(
            "client_disconnected",
            active_connections=self.metrics.client_connections_active,
        )
        self.on_close(self)

    def _read_client(self) -> None:
        try:
            data = self.client_socket.recv(CHUNK_SIZE)
        except BlockingIOError:
            return

        if not data:
            self.close()
            return

        self.client_input.extend(data)
        self._arm_timeout("client_read", self.timeout_policy.read)
        self._process_client_input()

    def _process_client_input(self) -> None:
        if self.state == HandlerState.READING_REQUEST:
            request = self._pop_head(
                self.client_input,
                HttpRequestHead.from_bytes,
            )

            if request is None:
                return

            self._start_request(request)

        if self.request is None:
            return

        body_size = min(
            len(self.client_input),
            self.request_body_remaining,
        )

        if body_size:
            self.to_upstream.extend(self.client_input[:body_size])
            del self.client_input[:body_size]

            self.request_body_remaining -= body_size
            self.request_body_size += body_size

            if self.state == HandlerState.SENDING_REQUEST:
                self._enable_event(
                    self.upstream_socket,
                    selectors.EVENT_WRITE,
                )
                self._arm_timeout(
                    "upstream_write",
                    self.timeout_policy.write,
                )

        if self.request_body_remaining == 0:
            self._disable_event(
                self.client_socket,
                selectors.EVENT_READ,
            )
            self._clear_timeout("client_read")
        elif len(self.to_upstream) >= MAX_BUFFER_SIZE:
            self._disable_event(
                self.client_socket,
                selectors.EVENT_READ,
            )
            self._clear_timeout("client_read")

    def _start_request(self, request: HttpRequestHead) -> None:
        self.request = request
        self.client_keep_alive = request.keep_alive
        self.request_body_remaining = request.content_length

        self.request_body_size = 0
        self.response_body_size = 0

        self.upstream_lease = self.upstream_pool.acquire()
        self.upstream_socket = self.upstream_lease.socket
        upstream = self.upstream_lease.upstream
        upstream_address = f"{upstream.host}:{upstream.port}"

        self.request_started_at = monotonic()
        self._arm_timeout("request_total", self.timeout_policy.total)
        self.request_logger = logger.bind(
            method=request.method,
            path=request.path,
            client_address=self.client_address,
            upstream=upstream_address,
        )
        self.metrics.request_started(upstream=upstream_address)
        self.request_logger.info(
            "request_started",
            upstream_connection_reused=self.upstream_lease.connected,
        )

        self.to_upstream.extend(bytes(request))

        if self.upstream_lease.connected:
            self.state = HandlerState.SENDING_REQUEST
            self.selector.register(
                self.upstream_socket,
                selectors.EVENT_WRITE,
                data=self,
            )
            self._arm_timeout(
                "upstream_write",
                self.timeout_policy.write,
            )
        else:
            self._connect_upstream()

    def _connect_upstream(self) -> None:
        if self.upstream_lease is None or self.upstream_socket is None:
            raise RuntimeError("Upstream connection is not acquired")

        upstream = self.upstream_lease.upstream
        connect_result = self.upstream_socket.connect_ex((upstream.host, upstream.port))

        if connect_result == 0:
            self.state = HandlerState.SENDING_REQUEST
            self.selector.register(
                self.upstream_socket,
                selectors.EVENT_WRITE,
                data=self,
            )
            self._arm_timeout(
                "upstream_write",
                self.timeout_policy.write,
            )
            return

        if connect_result not in (
            errno.EINPROGRESS,
            errno.EWOULDBLOCK,
            errno.EALREADY,
        ):
            raise OSError(
                connect_result,
                os.strerror(connect_result),
            )

        self.state = HandlerState.CONNECTING_UPSTREAM
        self.selector.register(
            self.upstream_socket,
            selectors.EVENT_WRITE,
            data=self,
        )
        self._arm_timeout(
            "upstream_connect",
            self.timeout_policy.connect,
        )

    def _finish_upstream_connect(self) -> None:
        if self.upstream_socket is None:
            raise RuntimeError("Upstream socket is not initialized")

        connect_error = self.upstream_socket.getsockopt(
            socket.SOL_SOCKET,
            socket.SO_ERROR,
        )

        if connect_error:
            raise OSError(
                connect_error,
                os.strerror(connect_error),
            )

        self._clear_timeout("upstream_connect")
        self.state = HandlerState.SENDING_REQUEST
        self.selector.modify(
            self.upstream_socket,
            selectors.EVENT_WRITE,
            data=self,
        )
        self._arm_timeout(
            "upstream_write",
            self.timeout_policy.write,
        )

    def _write_upstream(self) -> None:
        if self.upstream_socket is None:
            return

        if not self.to_upstream:
            self._disable_event(
                self.upstream_socket,
                selectors.EVENT_WRITE,
            )
            self._clear_timeout("upstream_write")

            if self.request_body_remaining == 0:
                self._start_reading_response()
            return

        try:
            sent = self.upstream_socket.send(self.to_upstream)
        except BlockingIOError:
            return

        if sent == 0:
            raise ConnectionError("Upstream closed while sending request")

        del self.to_upstream[:sent]

        if self.to_upstream:
            self._arm_timeout(
                "upstream_write",
                self.timeout_policy.write,
            )
        else:
            self._disable_event(
                self.upstream_socket,
                selectors.EVENT_WRITE,
            )
            self._clear_timeout("upstream_write")

            if self.request_body_remaining == 0:
                self._start_reading_response()

        if self.request_body_remaining > 0 and len(self.to_upstream) < MAX_BUFFER_SIZE:
            self._enable_event(
                self.client_socket,
                selectors.EVENT_READ,
            )
            self._arm_timeout("client_read", self.timeout_policy.read)

    def _start_reading_response(self) -> None:
        if self.upstream_socket is None:
            raise RuntimeError("Upstream socket is not initialized")

        self.state = HandlerState.READING_RESPONSE
        self._enable_event(
            self.upstream_socket,
            selectors.EVENT_READ,
        )
        self._arm_timeout(
            "upstream_read",
            self.timeout_policy.read,
        )

    def _read_upstream(self) -> None:
        if self.upstream_socket is None:
            return

        try:
            data = self.upstream_socket.recv(CHUNK_SIZE)
        except BlockingIOError:
            return

        if not data:
            raise ConnectionError("Upstream closed before the complete response")

        self.upstream_input.extend(data)
        self._arm_timeout("upstream_read", self.timeout_policy.read)
        self._process_upstream_input()

    def _process_upstream_input(self) -> None:
        if self.response is None:
            response = self._pop_head(
                self.upstream_input,
                HttpResponseHead.from_bytes,
            )

            if response is None:
                return

            self.response = response
            self.upstream_reusable = response.keep_alive

            if self._response_has_no_body(response):
                self.response_body_remaining = 0
            else:
                if response.get_header("content-length") is None:
                    self.upstream_reusable = False

                self.response_body_remaining = response.content_length

            response.set_header(
                "connection",
                "keep-alive" if self.client_keep_alive else "close",
            )
            self._queue_client_data(bytes(response))

        body_size = min(
            len(self.upstream_input),
            self.response_body_remaining,
        )

        if body_size:
            self._queue_client_data(bytes(self.upstream_input[:body_size]))
            del self.upstream_input[:body_size]

            self.response_body_remaining -= body_size
            self.response_body_size += body_size

        if len(self.to_client) >= MAX_BUFFER_SIZE:
            self._disable_event(
                self.upstream_socket,
                selectors.EVENT_READ,
            )
            self._clear_timeout("upstream_read")

        if self.response_body_remaining == 0:
            self._clear_timeout("upstream_read")

            if self.upstream_input:
                self.upstream_reusable = False
                self.upstream_input.clear()

            self.state = HandlerState.FLUSHING_RESPONSE
            self._release_upstream(
                reusable=self.upstream_reusable,
            )

            if not self.to_client:
                self._finish_request()

    def _queue_client_data(self, data: bytes) -> None:
        was_empty = not self.to_client
        self.to_client.extend(data)
        self._enable_event(
            self.client_socket,
            selectors.EVENT_WRITE,
        )

        if was_empty:
            self._arm_timeout(
                "client_write",
                self.timeout_policy.write,
            )

    def _write_client(self) -> None:
        if not self.to_client:
            self._disable_event(
                self.client_socket,
                selectors.EVENT_WRITE,
            )
            self._clear_timeout("client_write")
            return

        try:
            sent = self.client_socket.send(self.to_client)
        except BlockingIOError:
            return

        if sent == 0:
            raise ConnectionError("Client closed while sending response")

        del self.to_client[:sent]

        if self.to_client:
            self._arm_timeout(
                "client_write",
                self.timeout_policy.write,
            )
        else:
            self._disable_event(
                self.client_socket,
                selectors.EVENT_WRITE,
            )
            self._clear_timeout("client_write")

            if self.state == HandlerState.FLUSHING_RESPONSE:
                self._finish_request()

        if (
            self.upstream_socket is not None
            and self.state == HandlerState.READING_RESPONSE
            and len(self.to_client) < MAX_BUFFER_SIZE
        ):
            self._enable_event(
                self.upstream_socket,
                selectors.EVENT_READ,
            )
            self._arm_timeout(
                "upstream_read",
                self.timeout_policy.read,
            )

    def _finish_request(self) -> None:
        if (
            self.request is None
            or self.response is None
            or self.request_started_at is None
        ):
            return

        duration = monotonic() - self.request_started_at
        keep_alive = self.client_keep_alive

        self.metrics.request_completed(
            status_code=self.response.status_code,
            request_bytes=self.request_body_size,
            response_bytes=self.response_body_size,
        )
        self.metrics.request_finished(duration_seconds=duration)

        self.request_logger.info(
            "request_completed",
            status=self.response.status_code,
            duration_ms=round(duration * 1000, 2),
            request_bytes=self.request_body_size,
            response_bytes=self.response_body_size,
        )

        self._clear_timeout("request_total")
        self.request_started_at = None
        self.request_logger = None
        self.request = None
        self.response = None
        self.request_body_remaining = 0
        self.response_body_remaining = 0
        self.request_body_size = 0
        self.response_body_size = 0
        self.client_keep_alive = False
        self.upstream_reusable = False
        self.to_upstream.clear()
        self.upstream_input.clear()

        if not keep_alive:
            self.close()
            return

        self.state = HandlerState.READING_REQUEST
        self._enable_event(
            self.client_socket,
            selectors.EVENT_READ,
        )
        self._arm_timeout("client_read", self.timeout_policy.read)

        if self.client_input:
            self._process_client_input()

    def _release_upstream(self, reusable: bool) -> None:
        lease = self.upstream_lease

        if lease is None:
            return

        try:
            self.selector.unregister(lease.socket)
        except KeyError:
            pass

        self.upstream_pool.release(
            lease=lease,
            reusable=reusable,
        )
        self.upstream_lease = None
        self.upstream_socket = None

    def _response_has_no_body(
        self,
        response: HttpResponseHead,
    ) -> bool:
        return (self.request is not None and self.request.method == "HEAD") or (
            100 <= response.status_code < 200 or response.status_code in (204, 304)
        )

    def _arm_timeout(self, name: str, seconds: float) -> None:
        self.deadlines[name] = monotonic() + seconds

    def _clear_timeout(self, name: str) -> None:
        self.deadlines.pop(name, None)

    def _enable_event(
        self,
        sock: socket.socket | None,
        event: int,
    ) -> None:
        if sock is None:
            return

        try:
            key = self.selector.get_key(sock)
        except KeyError:
            self.selector.register(
                sock,
                event,
                data=self,
            )
        else:
            events = key.events | event

            if events != key.events:
                self.selector.modify(
                    sock,
                    events,
                    data=self,
                )

    def _disable_event(
        self,
        sock: socket.socket | None,
        event: int,
    ) -> None:
        if sock is None:
            return

        try:
            key = self.selector.get_key(sock)
        except KeyError:
            return

        events = key.events & ~event

        if events:
            self.selector.modify(
                sock,
                events,
                data=self,
            )
        else:
            self.selector.unregister(sock)

    def _pop_head(
        self,
        buffer: bytearray,
        parser: Callable[[bytes | bytearray], tuple[Head, int] | None],
    ) -> Head | None:
        parsed = parser(buffer)

        if parsed is None:
            if len(buffer) > MAX_HTTP_HEAD_SIZE:
                raise ValueError("HTTP head is too large")
            return None

        head, consumed = parsed
        del buffer[:consumed]
        return head
