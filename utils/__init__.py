from pathlib import Path
import logging
from hashlib import sha256
from urllib.parse import urlparse, urlunparse

# Create a "Logs" directory if it doesn't already exist
LOG_DIR = Path("Logs")
LOG_DIR.mkdir(exist_ok=True)

def get_logger(name: str, filename: str | None = None) -> logging.Logger:
    """
    This sets up a logger that writes messages to both a file and the console.
    If the logger was already created before, it just reuses it (to avoid duplicates).
    """
    logger = logging.getLogger(name)

    # If this logger is already setup, just return it
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # Log file path (defaults to using the logger name)
    log_path = LOG_DIR / f"{filename or name}.log"

    # Write logs to file and console
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    stream_handler = logging.StreamHandler()

    # Format for the log messages (with timestamp, level, etc.)
    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(fmt)
    stream_handler.setFormatter(fmt)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger

def canonicalise(url: str) -> str:
    """
    This turns URLs into a consistent form, so that similar-looking ones 
    (like with or without a trailing slash) are treated the same.
    """
    parsed = urlparse(url.strip())

    # Lowercase the hostname
    host = (parsed.hostname or "").lower()

    # Remove default ports from netloc
    if parsed.port and parsed.port in {80, 443}:
        netloc = host
    else:
        netloc = parsed.netloc.lower()

    # Normalize the path by removing trailing slash if needed
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]

    # Rebuild the URL without the scheme, params, or fragment
    return urlunparse(("", netloc, path, "", parsed.query, ""))

def url_hash(url: str) -> str:
    """
    Hash the canonical form of the URL using SHA256.
    This gives a unique, consistent ID for any URL.
    """
    return sha256(canonicalise(url).encode("utf-8")).hexdigest()
