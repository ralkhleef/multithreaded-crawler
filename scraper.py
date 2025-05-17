import atexit
import os
import re
from collections import Counter, defaultdict
from urllib.parse import urldefrag, urljoin, urlparse
from bs4 import BeautifulSoup

# Globals & constants
ASSIGNMENT_DOMAINS = {
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
}
TODAY_PATH_PREFIX = "/department/information_computer_sciences"

# A URL is considered unique by (scheme, netloc, path, params, query) –
# fragments are discarded via urldefrag() before adding to this set
unique_urls: set[str] = set()

# Mapping URL  word‑count (after stop‑word removal)
page_word_counts: dict[str, int] = {}

# Global unigram frequency across all pages
word_frequencies: Counter[str] = Counter()

# Sub‑domain page counts (for anything ending in .uci.edu)
subdomain_counts: defaultdict[str, int] = defaultdict(int)

# Load stop‑words once
_STOPWORDS_PATH = os.path.join(os.path.dirname(__file__), "stopwords.txt")
try:
    with open(_STOPWORDS_PATH, "r", encoding="utf-8") as fp:
        STOPWORDS = {w.strip().lower() for w in fp if w.strip()}
except FileNotFoundError:
    STOPWORDS = set()

# Ensure log directory exists
os.makedirs("Logs", exist_ok=True)

#  Scraper entry called by Worker

def scraper(url: str, resp):
    """Process *resp* fetched from *url* and return outlinks to crawl."""
    # Part 1 – analytics / content processing
    if resp.status == 200 and _is_html(resp):
        _process_page(url, resp)

    # Part 2 – hyperlink extraction & filtering
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

#  Helper – extract hyperlinks

def extract_next_links(url: str, resp):
    """Return a list of absolute, defragmented URLs extracted from *resp*."""
    if resp.status != 200 or not _is_html(resp):
        return []

    outlinks: list[str] = []
    try:
        soup = BeautifulSoup(resp.raw_response.content, "lxml")
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if not href or href.lower().startswith("javascript:"):
                continue
            try:
                abs_url = urljoin(resp.url, href)
                abs_url, _ = urldefrag(abs_url)  # discard fragments early
                outlinks.append(abs_url)
            except ValueError:
                # Malformed link (e.g., mailto:)
                continue
    except Exception as exc:
        print(f"⚠️ extract_next_links error on {url}: {exc}")

    return outlinks

#  URL filtering policy (domains + trap heuristics)

def is_valid(url: str) -> bool:
    """Return *True* if this URL should be enqueued for crawling."""
    try:
        # 1. Basic parse / scheme check
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False

        # 2. Disallow fragments & "junk" queries early
        if len(parsed.query) > 50:
            # super‑long queries are usually trackers or dynamic traps
            return False

        # 3. Avoid non‑text resource file extensions
        if _is_binary_resource(parsed.path):
            return False

        # 4. Domain / path whitelisting
        host = parsed.hostname.lower() if parsed.hostname else ""

        if host.endswith("uci.edu"):
            # today.uci.edu special path restriction
            if host == "today.uci.edu":
                return parsed.path.startswith(TODAY_PATH_PREFIX)
            # Accept if the registered domain exactly matches list
            return any(host.endswith(domain) for domain in ASSIGNMENT_DOMAINS)
        return False

    except Exception as exc:
        print(f"⚠️ is_valid error on {url}: {exc}")
        return False

#  Page processing & analytics helpers

def _is_html(resp) -> bool:
    content_type = resp.raw_response.headers.get("Content-Type", "").lower()
    return "html" in content_type


def _process_page(url: str, resp):
    """Record analytics for a fetched HTML page."""
    # Defragment URL for uniqueness accounting
    url, _ = urldefrag(url)
    if url in unique_urls:
        return

    try:
        html = resp.raw_response.content.decode("utf-8", errors="replace")
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(separator=" ")
        tokens = re.findall(r"[A-Za-z]+", text.lower())
        clean_tokens = [t for t in tokens if t not in STOPWORDS]

        # Update global structures
        unique_urls.add(url)
        page_word_counts[url] = len(clean_tokens)
        word_frequencies.update(clean_tokens)

        host = urlparse(url).hostname or ""
        if host.endswith("uci.edu"):
            subdomain_counts[host] += 1

        # Lightweight progress logging
        print(f"✅ {len(unique_urls):,} pages | {url} ({len(clean_tokens)} words)")

    except Exception as exc:
        print(f"⚠️ _process_page error on {url}: {exc}")

#  Exit hook – write report.txt once crawler terminates

def _write_report():
    os.makedirs("Logs", exist_ok=True)
    report_path = os.path.join("Logs", "report.txt")

    longest_url = max(page_word_counts, key=page_word_counts.get, default="")
    longest_len = page_word_counts.get(longest_url, 0)
    top50 = word_frequencies.most_common(50)
    sub_list = sorted(subdomain_counts.items())

    with open(report_path, "w", encoding="utf-8") as fp:
        fp.write("ICS Web Crawler – Assignment 2 Report\n")
        fp.write("=" * 60 + "\n\n")
        fp.write(f"1) Unique pages count: {len(unique_urls):,}\n\n")
        fp.write(
            f"2) Longest page by word‑count:\n   {longest_url}\n   {longest_len:,} words after stop‑word removal\n\n"
        )
        fp.write("3) 50 most common words (after stop‑word removal):\n")
        for word, freq in top50:
            fp.write(f"   {word:<15} {freq:,}\n")
        fp.write("\n4) Sub‑domains within uci.edu (alphabetical):\n")
        for sub, cnt in sub_list:
            fp.write(f"   {sub}, {cnt}\n")
        fp.write("\n-- End of report --\n")

    print(f"📄 Report written to {report_path}")


atexit.register(_write_report)
#  Regex helpers

_BINARY_EXTENSIONS = re.compile(
    r".*\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|mid|mp2|mp3|mp4|wav|avi|mov|"
    r"mpeg|ram|m4v|mkv|ogg|ogv|pdf|ps|eps|tex|pptx?|docx?|xlsx?|names|data|"
    r"dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1|thmx|mso|"
    r"arff|rtf|jar|csv|rm|smil|wmv|swf|wma|zip|rar|gz)(/|$)",
    re.IGNORECASE,
)

def _is_binary_resource(path: str) -> bool:
    return bool(_BINARY_EXTENSIONS.match(path))
