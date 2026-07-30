import multiprocessing
import os
from pathlib import Path

from structlog.contextvars import bind_contextvars, clear_contextvars

from proxy_threads.config import Config
from proxy_threads.proxy_server import ProxyServer
from proxy_threads.utils.logs import configure_logging, get_logger


CONFIG_PATH = Path(__file__).with_name("config.yml")

logger = get_logger(__name__)


def run_worker(worker_id: int, config: Config):
    configure_logging(level=config.logging.level)

    clear_contextvars()

    bind_contextvars(
        pid=os.getpid(),
        worker_id=worker_id,
    )

    logger.info("worker_started")

    server = ProxyServer(config)

    server.start()


def run_master(config: Config):
    processes = []

    for worker_id in range(config.workers):
        process = multiprocessing.Process(
            target=run_worker,
            args=(worker_id, config),
        )
        process.start()
        processes.append(process)

    for process in processes:
        process.join()


def run():
    config = Config.from_yaml(str(CONFIG_PATH))

    run_master(config)


def main():
    try:
        run()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
