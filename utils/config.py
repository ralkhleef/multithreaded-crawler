import re

class Config(object):
    def __init__(self, config):
        # Read the user agent from the config file
        self.user_agent = config["IDENTIFICATION"]["USERAGENT"].strip()
        print(self.user_agent)

        # Make sure the user agent is customized (not left as default)
        assert self.user_agent != "DEFAULT AGENT", "Set useragent in config.ini"

        # Ensure the user agent has only allowed characters
        assert re.match(r"^[a-zA-Z0-9_ ,]+$", self.user_agent), \
            "User agent should not have any special characters outside '_', ',' and 'space'"

        # Number of threads the crawler will use
        self.threads_count = int(config["LOCAL PROPERTIES"]["THREADCOUNT"])

        # Path where crawl data or state will be saved
        self.save_file = config["LOCAL PROPERTIES"]["SAVE"]

        # Server host and port for connecting (can be for caching or other services)
        self.host = config["CONNECTION"]["HOST"]
        self.port = int(config["CONNECTION"]["PORT"])

        # Comma-separated seed URLs from where the crawler starts
        self.seed_urls = config["CRAWLER"]["SEEDURL"].split(",")

        # Politeness delay between requests (in seconds)
        self.time_delay = float(config["CRAWLER"]["POLITENESS"])

        # Placeholder for cache server (optional, used elsewhere if needed)
        self.cache_server = None
