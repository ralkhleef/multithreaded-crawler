from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker

# This is the main controller class for running the crawler
class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        # Save the configuration settings
        self.config = config

        # Set up a logger to track progress and errors
        self.logger = get_logger("CRAWLER")

        # Create the frontier (manages the queue of URLs and politeness)
        self.frontier = frontier_factory(config, restart)

        # List to hold all the worker threads
        self.workers = list()

        # Assign the worker factory (how each worker gets created)
        self.worker_factory = worker_factory

    # Starts all the workers asynchronously
    def start_async(self):
        # Create a worker thread for each configured thread count
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier)
            for worker_id in range(self.config.threads_count)
        ]
        # Start each worker
        for worker in self.workers:
            worker.start()

    # Starts the workers and waits for them to finish
    def start(self):
        self.start_async()
        self.join()

    # Waits for all workers to complete (blocking)
    def join(self):
        for worker in self.workers:
            worker.join()
