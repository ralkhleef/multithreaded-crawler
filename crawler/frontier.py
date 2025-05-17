# Replaces the stock Frontier and supports **exactly** the requirements of
# ICS 161 A2: – three workers, ≥500 ms between requests to the *same domain*,
# resumable disk‑based frontier via shelve, and seed‑URL bootstrapping.

import os
import shelve
import time
from collections import deque, defaultdict
from threading import RLock
from urllib.parse import urlparse

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

POLITENESS_DELAY = 0.5  # seconds between hits to the same domain


class Frontier:
    """Thread‑safe frontier queue with per‑domain politeness."""

    def __init__(self, config, restart: bool):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self._lock = RLock()

        # Queue of *normalized* URLs awaiting fetch
        self.to_be_downloaded: deque[str] = deque()

        # Politeness clock per domain – stores last‑access `time.time()`
        self._domain_last: defaultdict[str, float] = defaultdict(float)

        # Shelve DB path
        self._db_path = self.config.save_file

        # Handle restart vs resume behaviour
        if restart and os.path.exists(self._db_path):
            self.logger.info("Restart requested – deleting existing save file.")
            os.remove(self._db_path)

        # Open (or create) shelve in *writeback* mode for atomic updates
        self._db = shelve.open(self._db_path, writeback=False)

        if restart or not self._db:
            self.logger.info("Seeding frontier from config URLs …")
            for seed in self.config.seed_urls:
                self._enqueue_seed(seed)
        else:
            self._resume_from_save()

    # ------------------------------------------------------------------
    #  Public API used by Worker threads
    # ------------------------------------------------------------------

    def get_tbd_url(self) -> str | None:
        """Return the next crawlable URL that respects politeness.

        Blocks (sleep) inside until a permissible request is ready or the
        frontier is exhausted (returns *None*).
        """
        while True:
            with self._lock:
                if not self.to_be_downloaded:
                    return None

                url = self.to_be_downloaded.pop()  # LIFO gives depth‑first
                domain = urlparse(url).netloc
                wait = POLITENESS_DELAY - (time.time() - self._domain_last[domain])

                if wait <= 0:
                    # OK to crawl – reserve the slot and return URL
                    self._domain_last[domain] = time.time()
                    return url

                # Politeness not satisfied – push back and fall through
                self.to_be_downloaded.appendleft(url)

            # Sleep *outside* the lock so other threads may progress
            time.sleep(wait)

    def add_url(self, url: str):
        url = normalize(url)
        if not is_valid(url):
            return

        url_hash = get_urlhash(url)
        with self._lock:
            if url_hash not in self._db:
                self._db[url_hash] = (url, False)
                self._db.sync()
                self.to_be_downloaded.append(url)

    def mark_url_complete(self, url: str):
        url_hash = get_urlhash(url)
        with self._lock:
            if url_hash in self._db:
                self._db[url_hash] = (url, True)
                self._db.sync()
            else:
                self.logger.error(f"Completed URL {url} not present in DB.")

    # ------------------------------------------------------------------
    #  Internal helpers
    # ------------------------------------------------------------------

    def _enqueue_seed(self, url: str):
        url = normalize(url)
        if not is_valid(url):
            self.logger.warning(f"Seed URL filtered by is_valid: {url}")
            return
        url_hash = get_urlhash(url)
        self._db[url_hash] = (url, False)
        self.to_be_downloaded.append(url)
        self._db.sync()

    def _resume_from_save(self):
        total = len(self._db)
        resumed = 0
        for (url, completed) in self._db.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                resumed += 1
        self.logger.info(f"Resumed {resumed} pending URLs from {total} stored.")

    # ------------------------------------------------------------------
    #  Clean‑up (optional)
    # ------------------------------------------------------------------

    def __del__(self):
        try:
            self._db.close()
        except Exception:
            pass