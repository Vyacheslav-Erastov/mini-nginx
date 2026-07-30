import asyncio
from pathlib import Path

from proxy.config import Config
from proxy.proxy_server import ProxyServer
from proxy.utils.logs import configure_logging


CONFIG_PATH = Path(__file__).with_name("config.yml")


async def run():
    config = Config.from_yaml(str(CONFIG_PATH))

    configure_logging(level=config.logging.level)

    server = ProxyServer(config)

    await server.start()


def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
