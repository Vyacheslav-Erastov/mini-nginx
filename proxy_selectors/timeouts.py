from __future__ import annotations

from dataclasses import dataclass

from proxy.config import Timeouts


@dataclass(frozen=True)
class TimeoutPolicy:
    connect: float
    read: float
    write: float
    total: float

    @classmethod
    def from_config(cls, timeouts: Timeouts) -> TimeoutPolicy:
        return cls(
            connect=timeouts.connect_ms / 1000,
            read=timeouts.read_ms / 1000,
            write=timeouts.write_ms / 1000,
            total=timeouts.total_ms / 1000,
        )
