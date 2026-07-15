from __future__ import annotations

from dataclasses import dataclass, field

import yaml


@dataclass(frozen=True)
class Upstream:
    host: str
    port: int


@dataclass
class Timeouts:
    connect_ms: int = 1000
    read_ms: int = 15000
    write_ms: int = 15000
    total_ms: int = 5000


@dataclass
class Limits:
    max_client_conns: int = 10000
    max_conns_per_upstream: int = 100


@dataclass
class Logging:
    level: str = "info"


@dataclass
class Config:
    listen_host: str = "127.0.0.1"
    listen_port: int = 8080
    upstreams: list[Upstream] = field(default_factory=list)
    limits: Limits = field(default_factory=Limits)
    timeouts: Timeouts = field(default_factory=Timeouts)
    logging: Logging = field(default_factory=Logging)

    @classmethod
    def from_yaml(
        cls,
        file_path: str,
    ) -> Config:
        with open(file_path) as config_file:
            data = yaml.safe_load(config_file)

        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict) -> Config:
        listen_host, listen_port = data["listen"].rsplit(":", maxsplit=1)

        upstreams = [
            Upstream(
                host=upstream_data["host"],
                port=upstream_data["port"],
            )
            for upstream_data in data["upstreams"]
        ]

        if not upstreams:
            raise ValueError("At least one upstream is required")

        timeouts = Timeouts(**data.get("timeouts", {}))

        limits = Limits(**data.get("limits", {}))

        logging = Logging(**data.get("logging", {}))

        return cls(
            listen_host=listen_host,
            listen_port=int(listen_port),
            upstreams=upstreams,
            timeouts=timeouts,
            limits=limits,
            logging=logging,
        )
