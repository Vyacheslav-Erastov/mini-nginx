from pathlib import Path

from proxy_selectors.config import Config
from proxy_selectors.proxy_server import SelectorProxyServer
from proxy_selectors.utils.logs import configure_logging


DEFAULT_CONFIG_PATH = Path(__file__).with_name("config.yml")


def main() -> None:
    config = Config.from_yaml(str(DEFAULT_CONFIG_PATH))
    configure_logging(level=config.logging.level)

    server = SelectorProxyServer(config)

    try:
        server.start()
    except KeyboardInterrupt:
        server.stop()


if __name__ == "__main__":
    main()
