from dataclasses import dataclass, field


@dataclass
class Metrics:
    client_connections_total: int = 0
    client_connections_active: int = 0

    requests_total: int = 0
    requests_active: int = 0
    requests_completed: int = 0
    requests_failed: int = 0
    requests_timed_out: int = 0

    request_duration_seconds_total: float = 0
    request_duration_seconds_max: float = 0

    request_bytes_total: int = 0
    response_bytes_total: int = 0

    responses_by_status: dict[int, int] = field(default_factory=dict)
    requests_by_upstream: dict[str, int] = field(default_factory=dict)

    def client_connected(self) -> None:
        self.client_connections_total += 1
        self.client_connections_active += 1

    def client_disconnected(self) -> None:
        self.client_connections_active -= 1

    def request_started(
        self,
        upstream: str,
    ) -> None:
        self.requests_total += 1
        self.requests_active += 1

        self.requests_by_upstream[upstream] = (
            self.requests_by_upstream.get(upstream, 0) + 1
        )

    def request_completed(
        self,
        status_code: int,
        request_bytes: int,
        response_bytes: int,
    ) -> None:
        self.requests_completed += 1
        self.request_bytes_total += request_bytes
        self.response_bytes_total += response_bytes

        self.responses_by_status[status_code] = (
            self.responses_by_status.get(status_code, 0) + 1
        )

    def request_failed(self) -> None:
        self.requests_failed += 1

    def request_timed_out(self) -> None:
        self.requests_failed += 1
        self.requests_timed_out += 1

    def request_finished(
        self,
        duration_seconds: float,
    ) -> None:
        self.requests_active -= 1
        self.request_duration_seconds_total += duration_seconds
        self.request_duration_seconds_max = max(
            self.request_duration_seconds_max,
            duration_seconds,
        )

    def snapshot(self) -> dict:
        average_duration = 0

        if self.requests_total:
            average_duration = self.request_duration_seconds_total / self.requests_total

        return {
            "client_connections_total": self.client_connections_total,
            "client_connections_active": self.client_connections_active,
            "requests_total": self.requests_total,
            "requests_active": self.requests_active,
            "requests_completed": self.requests_completed,
            "requests_failed": self.requests_failed,
            "requests_timed_out": self.requests_timed_out,
            "average_duration_ms": round(
                average_duration * 1000,
                2,
            ),
            "max_duration_ms": round(
                self.request_duration_seconds_max * 1000,
                2,
            ),
            "request_bytes_total": self.request_bytes_total,
            "response_bytes_total": self.response_bytes_total,
            "responses_by_status": self.responses_by_status.copy(),
            "requests_by_upstream": self.requests_by_upstream.copy(),
        }
