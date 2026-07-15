import asyncio
from asyncio.streams import StreamReader, StreamWriter
from dataclasses import dataclass
from time import perf_counter

from proxy.config import Config, Upstream
from proxy.metrics import Metrics
from proxy.timeouts import TimeoutPolicy
from proxy.upstream_pool import UpstreamPool
from proxy.utils import logs
from proxy.utils.http import HttpRequestHead, HttpResponseHead

logger = logs.get_logger(__name__)


@dataclass
class RequestResult:
    body_size: int


@dataclass
class ResponseResult:
    status_code: int
    keep_connection: bool
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

    async def _stream_reader_to_writer(
        self,
        reader: StreamReader,
        writer: StreamWriter,
        bytes_to_read: int | None = None,
    ):
        remaining = bytes_to_read
        payload_size = 0

        while remaining is None or remaining > 0:
            read_size = self.chunk_size

            if remaining is not None:
                read_size = min(read_size, remaining)

            chunk = await asyncio.wait_for(
                reader.read(read_size), timeout=self.timeout_policy.read
            )

            if not chunk:
                if remaining is not None and remaining > 0:
                    raise ConnectionError("Stream ended before all data was received")
                break

            writer.write(chunk)
            await asyncio.wait_for(writer.drain(), timeout=self.timeout_policy.write)

            payload_size += len(chunk)

            if remaining is not None:
                remaining -= len(chunk)

        return payload_size

    async def _stream_request_to_upstream(
        self,
        client_reader: StreamReader,
        upstream_writer: StreamWriter,
        request: HttpRequestHead,
    ) -> tuple[int, int, bool]:

        request.set_header(
            "connection",
            "close",
        )

        upstream_writer.write(bytes(request))

        await asyncio.wait_for(
            upstream_writer.drain(),
            timeout=self.timeout_policy.write,
        )

        body_size = await self._stream_reader_to_writer(
            reader=client_reader,
            writer=upstream_writer,
            bytes_to_read=request.content_length,
        )

        return RequestResult(body_size=body_size)

    async def _stream_response_to_client(
        self,
        upstream_reader: StreamReader,
        client_writer: StreamWriter,
        is_client_keep_alive: bool,
    ) -> tuple[int, int, bool]:

        response = await asyncio.wait_for(
            HttpResponseHead.from_reader(upstream_reader),
            timeout=self.timeout_policy.read,
        )

        response.set_header(
            "connection",
            ("keep-alive" if is_client_keep_alive else "close"),
        )

        client_writer.write(bytes(response))

        await asyncio.wait_for(
            client_writer.drain(),
            timeout=self.timeout_policy.write,
        )

        body_size = await self._stream_reader_to_writer(
            reader=upstream_reader,
            writer=client_writer,
            bytes_to_read=None,
        )

        return ResponseResult(
            status_code=response.status_code,
            keep_connection=is_client_keep_alive,
            body_size=body_size,
        )

    async def _proxy_request(
        self,
        client_reader: StreamReader,
        client_writer: StreamWriter,
        upstream: Upstream,
        request: HttpRequestHead,
    ) -> ProxyResult:
        upstream_writer: StreamWriter | None = None
        stream_tasks: list[asyncio.Task] = []

        try:
            is_client_keep_alive = request.keep_alive

            upstream_reader, upstream_writer = await asyncio.wait_for(
                asyncio.open_connection(
                    host=upstream.host,
                    port=upstream.port,
                ),
                timeout=self.timeout_policy.connect,
            )

            request_to_upstream = asyncio.create_task(
                self._stream_request_to_upstream(
                    client_reader=client_reader,
                    upstream_writer=upstream_writer,
                    request=request,
                )
            )

            response_to_client = asyncio.create_task(
                self._stream_response_to_client(
                    upstream_reader=upstream_reader,
                    client_writer=client_writer,
                    is_client_keep_alive=is_client_keep_alive,
                )
            )

            stream_tasks = [
                request_to_upstream,
                response_to_client,
            ]

            req_res, resp_res = await asyncio.gather(*stream_tasks)

            return ProxyResult(request_result=req_res, response_result=resp_res)

        finally:
            for task in stream_tasks:
                if not task.done():
                    task.cancel()

            if stream_tasks:
                await asyncio.gather(
                    *stream_tasks,
                    return_exceptions=True,
                )

            await self._close_writer(upstream_writer)

    async def handle_client(
        self, client_reader: StreamReader, client_writer: StreamWriter
    ):
        try:
            while True:
                async with self.upstream_pool.get_upstream() as upstream:
                    try:
                        request: HttpRequestHead = await asyncio.wait_for(
                            HttpRequestHead.from_reader(client_reader),
                            timeout=self.timeout_policy.read,
                        )

                        started_at = perf_counter()
                        upstream_address = f"{upstream.host}:{upstream.port}"

                        request_logger = logger.bind(
                            method=request.method,
                            path=request.path,
                            client_address=client_writer.get_extra_info("peername"),
                            upstream=upstream_address,
                        )

                        self.metrics.request_started(
                            upstream=upstream_address,
                        )

                        request_logger.info("request_started")

                        result = await asyncio.wait_for(
                            self._proxy_request(
                                client_reader, client_writer, upstream, request
                            ),
                            timeout=self.timeout_policy.total,
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

                    except asyncio.TimeoutError:
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

                    if not result.response_result.keep_connection:
                        break

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception("handle_client_error")

        finally:
            await self._close_writer(client_writer)

    async def _close_writer(
        self,
        writer: StreamWriter | None,
    ) -> None:
        if writer is None:
            return

        writer.close()

        try:
            await writer.wait_closed()
        except ConnectionError, OSError:
            pass
