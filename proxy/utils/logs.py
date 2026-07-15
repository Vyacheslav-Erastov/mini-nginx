import logging
import sys

import structlog


def configure_logging(
    level: int = logging.INFO,
) -> None:
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(
                fmt="iso",
                utc=True,
            ),
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.PrintLoggerFactory(
            file=sys.stdout,
        ),
        wrapper_class=structlog.make_filtering_bound_logger(
            level,
        ),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str):
    return structlog.get_logger(name)
