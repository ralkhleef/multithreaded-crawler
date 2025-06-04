import pickle

# This class holds the response data from the cache server
class Response(object):
    def __init__(self, resp_dict):
        # The URL that was requested
        self.url = resp_dict["url"]

        # The HTTP status code returned (e.g., 200, 404)
        self.status = resp_dict["status"]

        # If there was an error, store it; otherwise, set to None
        self.error = resp_dict["error"] if "error" in resp_dict else None

        # Try to load the actual raw HTTP response object (HTML, headers, etc.)
        try:
            self.raw_response = (
                pickle.loads(resp_dict["response"])  # Deserialize the response
                if "response" in resp_dict else
                None
            )
        except TypeError:
            # If something went wrong, keep it as None
            self.raw_response = None
