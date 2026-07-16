from __future__ import annotations

from dataclasses import dataclass
from asyncio.streams import StreamReader


@dataclass
class HttpRequestHead:
    method: str
    path: str
    version: str
    headers: dict[str, str]

    @classmethod
    async def from_reader(
        cls,
        reader: StreamReader,
    ) -> HttpRequestHead:
        request_line = await reader.readline()

        if not request_line:
            raise ConnectionError("Client disconnected")

        method, path, version = request_line.decode().strip().split(maxsplit=2)

        headers: dict[str, str] = {}

        while True:
            line = await reader.readline()

            if not line:
                raise ConnectionError("Client disconnected while sending headers")

            if line == b"\r\n":
                break

            name, value = line.decode().rstrip("\r\n").split(":", maxsplit=1)

            name = name.strip().lower()
            value = value.strip()

            if name in headers:
                headers[name] = f"{headers[name]}, {value}"
            else:
                headers[name] = value

        return cls(
            method=method,
            path=path,
            version=version,
            headers=headers,
        )

    def get_header(
        self,
        name: str,
        default: str | None = None,
    ) -> str | None:
        return self.headers.get(
            name.lower(),
            default,
        )

    def set_header(
        self,
        name: str,
        value: str,
    ) -> None:
        self.headers[name.lower()] = value

    def remove_header(
        self,
        name: str,
    ) -> None:
        self.headers.pop(
            name.lower(),
            None,
        )

    @property
    def content_length(self) -> int:
        value = self.get_header("content-length")

        if value is None:
            return 0

        return int(value)

    @property
    def keep_alive(self) -> bool:
        connection = self.get_header(
            "connection",
            "",
        ).lower()

        if self.version == "HTTP/1.1":
            return connection != "close"

        return connection == "keep-alive"

    def __bytes__(self) -> bytes:
        lines = [
            f"{self.method} {self.path} {self.version}",
        ]

        lines.extend(f"{name}: {value}" for name, value in self.headers.items())

        lines.extend(["", ""])

        return "\r\n".join(lines).encode()


@dataclass
class HttpResponseHead:
    version: str
    status_code: int
    reason: str
    headers: dict[str, str]

    @classmethod
    async def from_reader(
        cls,
        reader: StreamReader,
    ) -> HttpResponseHead:
        status_line = await reader.readline()

        if not status_line:
            raise ConnectionError("Upstream disconnected before response")

        parts = status_line.decode().strip().split(maxsplit=2)

        if len(parts) < 2:
            raise ValueError(f"Invalid response line: {status_line}")

        headers: dict[str, str] = {}

        while True:
            line = await reader.readline()

            if not line:
                raise ConnectionError(
                    "Upstream disconnected while sending response headers"
                )

            if line == b"\r\n":
                break

            name, value = line.decode().rstrip("\r\n").split(":", maxsplit=1)

            name = name.strip().lower()
            value = value.strip()

            if name in headers:
                headers[name] = f"{headers[name]}, {value}"
            else:
                headers[name] = value

        return cls(
            version=parts[0],
            status_code=int(parts[1]),
            reason=parts[2] if len(parts) == 3 else "",
            headers=headers,
        )

    @property
    def content_length(self) -> int:
        value = self.get_header("content-length")

        if value is None:
            return 0

        return int(value)

    @property
    def keep_alive(self) -> bool:
        connection = self.get_header(
            "connection",
            "",
        ).lower()

        if self.version == "HTTP/1.1":
            return connection != "close"

        return connection == "keep-alive"

    def get_header(
        self,
        name: str,
        default: str | None = None,
    ) -> str | None:
        return self.headers.get(
            name.lower(),
            default,
        )

    def set_header(
        self,
        name: str,
        value: str,
    ) -> None:
        self.headers[name.lower()] = value

    def remove_header(
        self,
        name: str,
    ) -> None:
        self.headers.pop(
            name.lower(),
            None,
        )

    def __bytes__(self) -> bytes:
        status_line = f"{self.version} {self.status_code}"

        if self.reason:
            status_line += f" {self.reason}"

        lines = [status_line]

        lines.extend(f"{name}: {value}" for name, value in self.headers.items())

        lines.extend(["", ""])

        return "\r\n".join(lines).encode()
