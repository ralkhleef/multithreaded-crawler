import os
import shelve
import time
from collections import deque, defaultdict
from threading import RLock
from urllib.parse import urlparse

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

POLITENESS_DELAY = 0.5  # Wait 0.5 seconds between requests to the same domain


class Frontier:
    """This class controls which URLs get crawled next, making sure we don't overload servers (politeness).
    It's also thread-safe so multiple threads can use it at once."""

    def __init__(self, config, restart: bool):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self._lock = RLock()  # Lock to protect shared data

        # Queue of normalized URLs waiting to be crawled
        self.to_be_downloaded: deque[str] = deque()

        # Keeps track of when we last accessed each domain
        self._domain_last: defaultdict[str, float] = defaultdict(float)

        # Where we save our shelve DB (keeps track of visited and unvisited URLs)
        self._db_path = self.config.save_file

        # If restarting, delete any old saved data
        if restart and os.path.exists(self._db_path):
            self.logger.info("Restart requested – deleting existing save file.")
            os.remove(self._db_path)

        # Open or create shelve DB
        self._db = shelve.open(self._db_path, writeback=False)

        # If we’re restarting or this is a new DB, add seed URLs
        if restart or not self._db:
            self.logger.info("Seeding frontier from config URLs …")
            for seed in self.config.seed_urls:
                self._enqueue_seed(seed)
        else:
            self._resume_from_save()

    # --- Public method used by the worker threads ---

    def get_tbd_url(self) -> str | None:
        """Get a URL that’s ready to crawl (respecting politeness). Returns None if all done."""
        while True:
            with self._lock:
                if not self.to_be_downloaded:
                    return None

                url = self.to_be_downloaded.pop()  # Use LIFO strategy
                domain = urlparse(url).netloc
                wait = POLITENESS_DELAY - (time.time() - self._domain_last[domain])

                if wait <= 0:
                    # We've waited long enough – update timestamp and return URL
                    self._domain_last[domain] = time.time()
                    return url

                # Not polite yet – put the URL back
                self.to_be_downloaded.appendleft(url)

            # Let other threads do stuff while we wait
            time.sleep(wait)

    def add_url(self, url: str):
        """Add a new URL to the frontier if it's valid and not seen before."""
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
        """Mark a URL as finished so we don't crawl it again."""
        url_hash = get_urlhash(url)
        with self._lock:
            if url_hash in self._db:
                self._db[url_hash] = (url, True)
                self._db.sync()
            else:
                self.logger.error(f"Completed URL {url} not present in DB.")

    # --- Helpers ---

    def _enqueue_seed(self, url: str):
        """Add the seed URL to the frontier when we start crawling."""
        url = normalize(url)
        if not is_valid(url):
            self.logger.warning(f"Seed URL filtered by is_valid: {url}")
            return
        url_hash = get_urlhash(url)
        self._db[url_hash] = (url, False)
        self.to_be_downloaded.append(url)
        self._db.sync()

    def _resume_from_save(self):
        """On resume, load any unfinished URLs back into the queue."""
        total = len(self._db)
        resumed = 0
        for (url, completed) in self._db.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                resumed += 1
        self.logger.info(f"Resumed {resumed} pending URLs from {total} stored.")

    # --- Clean-up ---

    def __del__(self):
        """Make sure to close the shelve database when done."""
        try:
            self._db.close()
        except Exception:
            pass
