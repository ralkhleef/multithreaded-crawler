from rtypes import pcc_set, dimension, primarykey

# This class is used to keep track of crawler nodes in the distributed system.
# It's stored in a shared data structure using PCC (Programmable Collective Communication).

@pcc_set
class Register(object):
    # Each crawler has a unique ID (string)
    crawler_id = primarykey(str)

    # This holds load balancer info – stored as a tuple
    load_balancer = dimension(tuple)

    # Whether the crawler is fresh or newly active
    fresh = dimension(bool)

    # If the crawler is marked invalid for some reason
    invalid = dimension(bool)

    def __init__(self, crawler_id, fresh):
        self.crawler_id = crawler_id          # Set the crawler’s unique ID
        self.load_balancer = tuple()          # Start with no load balancer info
        self.fresh = fresh                    # Mark whether it’s a fresh (new) crawler
        self.invalid = False                  # Assume it’s valid when registered
