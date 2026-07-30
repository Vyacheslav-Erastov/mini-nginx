from dataclasses import dataclass
from io import BufferedReader
import socket
from time import perf_counter

from proxy_threads.config import Config
from proxy_threads.metrics import Metrics
from proxy_threads.timeouts import TimeoutPolicy
from proxy_threads.upstream_pool import UpstreamPool
from proxy_threads.utils import logs
from proxy_threads.utils.http import HttpRequestHead, HttpResponseHead

logger = logs.get_logger(__name__)


@dataclass
class RequestResult:
    body_size: int
    keep_client_connection: bool


@dataclass
class ResponseResult:
    status_code: int
    keep_upstream_coonnection: bool
    body_size: int


@dataclass
class ProxyResult:
    request_result: RequestResult
    response_result: ResponseResult


class ClientHandler:
    def __init__(
        self,
        config: Config,
        timeout_policy: TimeoutPolicy,
        upstream_pool: UpstreamPool,
        metrics: Metrics,
        chunk_size=8192,
    ):
        self.config = config
        self.timeout_policy = timeout_policy
        self.chunk_size = chunk_size
        self.upstream_pool = upstream_pool
        self.metrics = metrics

    def _stream_reader_to_socket(
        self,
        reader: BufferedReader,
        socket: socket.socket,
        bytes_to_read: int | None = None,
    ):
        remaining = bytes_to_read
        payload_size = 0

        while remaining is None or remaining > 0:
            read_size = self.chunk_size

            if remaining is not None:
                read_size = min(read_size, remaining)

            chunk = reader.read(read_size)

            if not chunk:
                if remaining is not None and remaining > 0:
                    raise ConnectionError("Stream ended before all data was received")
                break

            socket.sendall(chunk)

            payload_size += len(chunk)

            if remaining is not None:
                remaining -= len(chunk)

        return payload_size

    def _stream_request_to_upstream(
        self,
        client_reader: BufferedReader,
        upstream_socket: socket.socket,
        request: HttpRequestHead,
    ) -> RequestResult:

        keep_client_connection = request.keep_alive

        upstream_socket.settimeout(self.timeout_policy.write)

        upstream_socket.sendall(bytes(request))

        body_size = self._stream_reader_to_socket(
            reader=client_reader,
            socket=upstream_socket,
            bytes_to_read=request.content_length,
        )

        return RequestResult(
            body_size=body_size, keep_client_connection=keep_client_connection
        )

    def _stream_response_to_client(
        self,
        upstream_reader: BufferedReader,
        client_socket: socket.socket,
        keep_client_connection: bool,
    ) -> tuple[int, int, bool]:

        response = HttpResponseHead.from_reader(upstream_reader)

        keep_upstream_connection = response.keep_alive

        response.set_header(
            "connection",
            ("keep-alive" if keep_client_connection else "close"),
        )

        client_socket.settimeout(self.timeout_policy.write)

        client_socket.sendall(bytes(response))

        body_size = self._stream_reader_to_socket(
            reader=upstream_reader,
            socket=client_socket,
            bytes_to_read=response.content_length,
        )

        return ResponseResult(
            status_code=response.status_code,
            keep_upstream_coonnection=keep_upstream_connection,
            body_size=body_size,
        )

    def _proxy_request(
        self,
        client_reader: BufferedReader,
        client_socket: socket.socket,
        upstream_reader: BufferedReader,
        upstream_socket: socket.socket,
        request: HttpRequestHead,
    ) -> ProxyResult:

        keep_client_connection = request.keep_alive

        request_result = self._stream_request_to_upstream(
            client_reader=client_reader,
            upstream_socket=upstream_socket,
            request=request,
        )

        upstream_socket.settimeout(self.timeout_policy.read)

        response_result = self._stream_response_to_client(
            upstream_reader=upstream_reader,
            client_socket=client_socket,
            keep_client_connection=keep_client_connection,
        )

        return ProxyResult(
            request_result=request_result, response_result=response_result
        )

    def handle_client(self, client_socket: socket.socket):
        client_address = client_socket.getpeername()

        try:
            with client_socket.makefile("rb") as client_reader:
                while True:
                    started_at = perf_counter()
                    try:
                        client_socket.settimeout(self.timeout_policy.read)

                        request: HttpRequestHead = HttpRequestHead.from_reader(
                            client_reader
                        )
                    except ConnectionError:
                        break

                    with self.upstream_pool.get_connection() as (
                        upstream,
                        connection,
                    ):
                        try:
                            upstream_address = f"{upstream.host}:{upstream.port}"

                            request_logger = logger.bind(
                                method=request.method,
                                path=request.path,
                                client_address=client_address,
                                upstream=upstream_address,
                            )

                            self.metrics.request_started(
                                upstream=upstream_address,
                            )

                            request_logger.info("request_started")

                            result: ProxyResult = self._proxy_request(
                                client_reader,
                                client_socket,
                                connection.reader,
                                connection.writer,
                                request,
                            )

                            connection.reusable = (
                                result.response_result.keep_upstream_coonnection
                            )

                            self.metrics.request_completed(
                                status_code=result.response_result.status_code,
                                request_bytes=result.request_result.body_size,
                                response_bytes=result.response_result.body_size,
                            )

                            request_logger.info(
                                "request_completed",
                                status=result.response_result.status_code,
                                duration_ms=round(
                                    (perf_counter() - started_at) * 1000,
                                    2,
                                ),
                                request_bytes=result.request_result.body_size,
                                response_bytes=result.response_result.body_size,
                            )

                        except ConnectionError:
                            break

                        except TimeoutError:
                            self.metrics.request_timed_out()

                            request_logger.warning(
                                "request_timeout",
                                duration_ms=round(
                                    (perf_counter() - started_at) * 1000,
                                    2,
                                ),
                            )

                            break

                        except Exception:
                            self.metrics.request_failed()

                            request_logger.exception(
                                "request_failed",
                                duration_ms=round(
                                    (perf_counter() - started_at) * 1000,
                                    2,
                                ),
                            )

                            break

                        finally:
                            self.metrics.request_finished(
                                duration_seconds=perf_counter() - started_at,
                            )

                        if not result.request_result.keep_client_connection:
                            break

        except Exception:
            logger.exception("handle_client_error")

        finally:
            self._close_connection(client_socket)

    def _close_connection(
        self,
        connection: socket.socket | None,
    ) -> None:
        if connection is None:
            return

        try:
            connection.close()
        except OSError:
            pass
