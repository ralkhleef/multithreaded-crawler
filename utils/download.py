import requests
import cbor
import time

from utils.response import Response

# This function is used to download a webpage using the cache server (Spacetime)
def download(url, config, logger=None):
    # Get the cache server's host and port from the config
    host, port = config.cache_server

    # Send a GET request to the cache server with the URL and our user agent
    resp = requests.get(
        f"http://{host}:{port}/",
        params=[("q", f"{url}"), ("u", f"{config.user_agent}")])

    try:
        # If we got a valid response with content, decode it using CBOR and return a Response object
        if resp and resp.content:
            return Response(cbor.loads(resp.content))
    except (EOFError, ValueError) as e:
        # Catch errors if CBOR decoding fails
        pass

    # Log an error if we couldn't decode the response or something else went wrong
    logger.error(f"Spacetime Response error {resp} with url {url}.")
    
    # Return an error Response object with details
    return Response({
        "error": f"Spacetime Response error {resp} with url {url}.",
        "status": resp.status_code,
        "url": url})

