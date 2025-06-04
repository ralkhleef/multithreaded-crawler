import os
from spacetime import Node
from utils.pcc_models import Register

# This function connects to the data framework and registers our crawler
def init(df, user_agent, fresh):
    # Try to read a Register object for our user agent
    reg = df.read_one(Register, user_agent)
    
    # If it doesn't exist, create and add it
    if not reg:
        reg = Register(user_agent, fresh)
        df.add_one(Register, reg)
        df.commit()
        df.push_await()

    # Keep waiting until the load balancer assigns us a cache server
    while not reg.load_balancer:
        df.pull_await()  # wait for update
        if reg.invalid:
            raise RuntimeError("User agent string is not acceptable.")
        if reg.load_balancer:
            df.delete_one(Register, reg)
            df.commit()
            df.push()

    return reg.load_balancer  # Return the cache server info (host, port)

# This sets up the Spacetime Node and returns the assigned cache server info
def get_cache_server(config, restart):
    init_node = Node(
        init, Types=[Register], dataframe=(config.host, config.port))
    
    # Start the node with our user agent, and tell it if we're restarting
    return init_node.start(
        config.user_agent, restart or not os.path.exists(config.save_file))
