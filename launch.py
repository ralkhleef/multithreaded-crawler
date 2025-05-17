import multiprocessing
multiprocessing.set_start_method("fork", force=True)  # required on macOS

import sys
import signal
from configparser import ConfigParser
from argparse import ArgumentParser

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler


def main(config_file: str, restart: bool):
    """Read config, register with cache server, and start crawler."""

    # 1) load configuration
    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)

    # 2) obtain cache‑server endpoint (handles registration)
    config.cache_server = get_cache_server(config, restart)

    # 3) spin up crawler instance
    crawler = Crawler(config, restart)

    # 4) graceful shutdown on Ctrl‑C so atexit hooks execute
    def _sigint_handler(sig, frame):
        print("\n⚠️  Ctrl‑C received — stopping crawler …")
        try:
            crawler.stop()  # crawler implements stop() in recent codebase
        except AttributeError:
            pass  # older versions fall through
        sys.exit(0)

    signal.signal(signal.SIGINT, _sigint_handler)

    # 5) run crawl loop (blocks until complete or SIGINT)
    crawler.start()


if __name__ == "__main__":
    cli = ArgumentParser(description="ICS 161 Web‑crawler launcher")
    cli.add_argument(
        "--restart",
        action="store_true",
        default=False,
        help="Delete existing frontier.shelve and start fresh",
    )
    cli.add_argument(
        "--config_file",
        type=str,
        default="config.ini",
        help="Path to config.ini",
    )
    opts = cli.parse_args()
    main(opts.config_file, opts.restart)
