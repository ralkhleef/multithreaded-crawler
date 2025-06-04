import atexit
import os
import re
from collections import Counter, defaultdict
from urllib.parse import urldefrag, urljoin, urlparse
from bs4 import BeautifulSoup

# Set of domains we are allowed to crawl (our scope)
ASSIGNMENT_DOMAINS = {
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
}

# Special case for today.uci.edu — we only allow certain paths
TODAY_PATH_PREFIX = "/department/information_computer_sciences"

# Store all the URLs we've seen so we don’t crawl duplicates
unique_urls: set[str] = set()

# Maps each URL to the number of non-stop words on its page
page_word_counts: dict[str, int] = {}

# Stores total word frequency across all crawled pages
word_frequencies: Counter[str] = Counter()

# Counts how many pages we visited per subdomain
subdomain_counts: defaultdict[str, int] = defaultdict(int)

# Load the stopwords once (words we want to ignore)
_STOPWORDS_PATH = os.path.join(os.path.dirname(__file__), "stopwords.txt")
try:
    with open(_STOPWORDS_PATH, "r", encoding="utf-8") as fp:
        STOPWORDS = {w.strip().lower() for w in fp if w.strip()}
except FileNotFoundError:
    STOPWORDS = set()

# Make sure there’s a folder to save the report
os.makedirs("Logs", exist_ok=True)

# --- MAIN SCRAPER FUNCTION ---

def scraper(url: str, resp):
    """Called by the crawler. This processes the response and returns links to follow."""
    if resp.status == 200 and _is_html(resp):
        _process_page(url, resp)  # Update analytics

    # Extract new URLs to crawl
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]


# --- EXTRACT LINKS FROM A PAGE ---

def extract_next_links(url: str, resp):
    """Gets all <a> links from the page and turns them into absolute URLs."""
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
                abs_url, _ = urldefrag(abs_url)  # Remove fragment like #section
                outlinks.append(abs_url)
            except ValueError:
                continue
    except Exception as exc:
        print(f"⚠️ extract_next_links error on {url}: {exc}")

    return outlinks


# --- FILTER OUT BAD/INVALID URLS ---

def is_valid(url: str) -> bool:
    """Returns True if this URL is safe and in scope to crawl."""
    try:
        parsed = urlparse(url)

        # Only crawl HTTP or HTTPS
        if parsed.scheme not in {"http", "https"}:
            return False

        # Ignore tracking links with very long queries
        if len(parsed.query) > 50:
            return False

        # Skip media and binary files
        if _is_binary_resource(parsed.path):
            return False

        # Only allow URLs in our UCI domains
        host = parsed.hostname.lower() if parsed.hostname else ""
        if host.endswith("uci.edu"):
            if host == "today.uci.edu":
                return parsed.path.startswith(TODAY_PATH_PREFIX)
            return any(host.endswith(domain) for domain in ASSIGNMENT_DOMAINS)

        return False
    except Exception as exc:
        print(f"⚠️ is_valid error on {url}: {exc}")
        return False


# --- CHECK IF RESPONSE IS HTML ---

def _is_html(resp) -> bool:
    content_type = resp.raw_response.headers.get("Content-Type", "").lower()
    return "html" in content_type


# --- ANALYTICS AND TRACKING FOR EACH PAGE ---

def _process_page(url: str, resp):
    """Collects data like word counts, unique pages, and subdomains."""
    url, _ = urldefrag(url)
    if url in unique_urls:
        return

    try:
        html = resp.raw_response.content.decode("utf-8", errors="replace")
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(separator=" ")
        tokens = re.findall(r"[A-Za-z]+", text.lower())
        clean_tokens = [t for t in tokens if t not in STOPWORDS]

        unique_urls.add(url)
        page_word_counts[url] = len(clean_tokens)
        word_frequencies.update(clean_tokens)

        host = urlparse(url).hostname or ""
        if host.endswith("uci.edu"):
            subdomain_counts[host] += 1

        print(f"✅ {len(unique_urls):,} pages | {url} ({len(clean_tokens)} words)")

    except Exception as exc:
        print(f"⚠️ _process_page error on {url}: {exc}")


# --- ON EXIT, WRITE ANALYTICS TO REPORT.TXT ---

def _write_report():
    os.makedirs("Logs", exist_ok=True)
    report_path = os.path.join("Logs", "report.txt")

    longest_url = max(page_word_counts, key=page_word_counts.get, default="")
    longest_len = page_word_counts.get(longest_url, 0)
    top50 = word_frequencies.most_common(50)
    sub_list = sorted(subdomain_counts.items())

    with open(report_path, "w", encoding="utf-8") as fp:
        fp.write("ICS Web Crawler – Assignment 2 Report\n")
        fp.write("=" * 60 + "\n\n")
        fp.write(f"1) Unique pages count: {len(unique_urls):,}\n\n")
        fp.write(f"2) Longest page by word-count:\n   {longest_url}\n   {longest_len:,} words\n\n")
        fp.write("3) 50 most common words (after stop-word removal):\n")
        for word, freq in top50:
            fp.write(f"   {word:<15} {freq:,}\n")
        fp.write("\n4) Sub-domains within uci.edu (alphabetical):\n")
        for sub, cnt in sub_list:
            fp.write(f"   {sub}, {cnt}\n")
        fp.write("\n-- End of report --\n")

    print(f"📄 Report written to {report_path}")


# This makes sure the report is saved when the crawler stops
atexit.register(_write_report)


# --- HELPER: Check if path looks like a file (e.g., .pdf, .zip) ---

_BINARY_EXTENSIONS = re.compile(
    r".*\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|mid|mp2|mp3|mp4|wav|avi|mov|"
    r"mpeg|ram|m4v|mkv|ogg|ogv|pdf|ps|eps|tex|pptx?|docx?|xlsx?|names|data|"
    r"dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1|thmx|mso|"
    r"arff|rtf|jar|csv|rm|smil|wmv|swf|wma|zip|rar|gz)(/|$)",
    re.IGNORECASE,
)

def _is_binary_resource(path: str) -> bool:
    """Returns True if the file has a binary or media extension."""
    return bool(_BINARY_EXTENSIONS.match(path))
