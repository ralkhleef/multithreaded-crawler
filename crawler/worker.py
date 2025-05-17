from threading import Thread
import time
from inspect import getsource

from utils.download import download
from utils import get_logger
import scraper  # scraper.py module


class Worker(Thread):
    """Crawler worker thread.

    Each Worker repeatedly:
      1. asks the Frontier for the next URL (blocking politely),
      2. downloads the page,
      3. passes the response to `scraper.scraper`,
      4. enqueues any returned links, and
      5. marks the fetched URL complete.
    """

    def __init__(self, worker_id: int, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier

        # scraper.py must not import high‑level HTTP libs directly
        disallowed = {
            "from requests import", "import requests",
            "from urllib.request import", "import urllib.request",
        }
        assert {getsource(scraper).find(s) for s in disallowed} == {-1}, (
            "Do not use requests / urllib in scraper.py; use utils.download." )

        super().__init__(daemon=True)
    #  Main fetch–process loop
    def run(self):
        while True:
            url = self.frontier.get_tbd_url()
            if url is None:
                self.logger.info("Frontier empty – shutting down thread.")
                break

            # Download through provided helper (handles cache server)
            resp = download(url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {url} [status {resp.status}] via cache {self.config.cache_server}")

            # Scrape page and enqueue new links
            outlinks = scraper.scraper(url, resp)
            for link in outlinks:
                self.frontier.add_url(link)

            # Mark this URL as processed
            self.frontier.mark_url_complete(url)

            # global throttle 
            time.sleep(self.config.time_delay)