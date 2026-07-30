from __future__ import annotations

from dataclasses import dataclass


HTTP_HEAD_END = b"\r\n\r\n"


def _split_head(
    data: bytes | bytearray,
) -> tuple[list[bytes], int] | None:
    head_end = data.find(HTTP_HEAD_END)

    if head_end == -1:
        return None

    consumed = head_end + len(HTTP_HEAD_END)
    lines = bytes(data[:head_end]).split(b"\r\n")

    return lines, consumed


def _parse_headers(lines: list[bytes]) -> dict[str, str]:
    headers: dict[str, str] = {}

    for line in lines:
        name, value = line.decode().split(":", maxsplit=1)

        name = name.strip().lower()
        value = value.strip()

        if name in headers:
            headers[name] = f"{headers[name]}, {value}"
        else:
            headers[name] = value

    return headers


@dataclass
class HttpRequestHead:
    method: str
    path: str
    version: str
    headers: dict[str, str]

    @classmethod
    def from_bytes(
        cls,
        data: bytes | bytearray,
    ) -> tuple[HttpRequestHead, int] | None:
        split = _split_head(data)

        if split is None:
            return None

        lines, consumed = split

        if not lines or not lines[0]:
            raise ValueError("Request line is empty")

        parts = lines[0].decode().split(maxsplit=2)

        if len(parts) != 3:
            raise ValueError(f"Invalid request line: {lines[0]!r}")

        return (
            cls(
                method=parts[0],
                path=parts[1],
                version=parts[2],
                headers=_parse_headers(lines[1:]),
            ),
            consumed,
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
    def from_bytes(
        cls,
        data: bytes | bytearray,
    ) -> tuple[HttpResponseHead, int] | None:
        split = _split_head(data)

        if split is None:
            return None

        lines, consumed = split

        if not lines or not lines[0]:
            raise ValueError("Response status line is empty")

        parts = lines[0].decode().split(maxsplit=2)

        if len(parts) < 2:
            raise ValueError(f"Invalid response line: {lines[0]!r}")

        return (
            cls(
                version=parts[0],
                status_code=int(parts[1]),
                reason=parts[2] if len(parts) == 3 else "",
                headers=_parse_headers(lines[1:]),
            ),
            consumed,
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
