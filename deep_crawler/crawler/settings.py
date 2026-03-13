import random
from pathlib import Path

# ── DB creds ────────────
DB_HOST, DB_PORT = "localhost", 5432
DB_NAME, DB_USER, DB_PASS = os.environ.get("DB_NAME", "Scraped"), os.environ.get("DB_USER", "postgres"), os.environ.get("DB_PASS", "your_password")

# ── Networking ────────
USER_AGENTS = [ 
 "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.78 Safari/537.36 Edg/125.0.2535.51",
 "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.6367.60 Safari/537.36",
 "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 Chrome/124.0.6367.60 Safari/537.36",
 "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/123.0.6312.105 Safari/537.36",
 "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
 "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
 "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6; rv:125.0) Gecko/20100101 Firefox/125.0",
 "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 Chrome/124.0.6367.60 Mobile Safari/537.36",
 "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
 "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.6261.95 Safari/537.36 Edg/122.0.2365.52",
 "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 Chrome/122.0.6261.95 Safari/537.36"
]
PROXIES = [
    {
        "server":  os.environ.get("PROXY_SERVER", "https://isp.decodo.com:10000"),
        "username": os.environ.get("PROXY_USERNAME", "your_proxy_username"),
        "password": os.environ.get("PROXY_PASSWORD", "your_proxy_password")
    }
]

PER_DOMAIN_DELAY_SEC = 1.0  # polite delay between requests per domain
def rnd_ua()   -> str:        return random.choice(USER_AGENTS)
PROXY_POOL = [f'https://{os.environ.get("PROXY_USERNAME")}:{os.environ.get("PROXY_PASSWORD")}@{os.environ.get("PROXY_SERVER", "isp.decodo.com:10000")}']
    
LOG_DIR = Path("logs"); LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_LEVEL = "INFO"  # or "DEBUG" during testing
LOG_ROTATE_MB = 5
LOG_BACKUPS = 3

NAV_TIMEOUT   = 35_000                # ms
POLITE_DELAY  = (0.3, 1.0)            # s
MAX_RETRIES   = 3
MAX_INTERNAL  = 60                    # pages per site
MAX_DEPTH = 2
MAX_PAGES_PER_DOMAIN = 30
TRIAGE_BATCH_SIZE = 80
MAX_INTERNAL_LINKS_PER_PAGE = 500     # cap links stored per crawled page
MAX_LINKS_PER_PATH_PREFIX = 10        # cap URLs per path pattern in prefilter
MAX_LINKS_FOR_TRIAGE = 700            # hard cap before sending to GLM triage
FIRST_PARTY_ONLY = True
ALLOW_LINKEDIN_CAPTURE = True

PREFER_CRAWL4AI_TEXT = True          # If True, use Crawl4AI text for Flash; else use extract_page fallback
COMPARE_CONTENT_LOGGING = True       # Emit per-page comparison logs to content_compare.ndjson
USE_THREE_LAYER_FALLBACK = False     # Re-enable trafilatura/justext cleaner if available
# Politeness and reliability
MAX_CONCURRENCY_PER_DOMAIN = 2
SKIP_EXT  = (".jpg",".jpeg",".png",".gif",".svg",".webp",".css",".js",".ico",".mp4")
BROCH_EXT = (".pdf",".ppt",".pptx")
from pathlib import Path
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True, parents=True)
FAILURES_XLSX = (DATA_DIR / "triage_failures.xlsx")
# ── LLM ───────────

DEFAULT_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9,en-IN;q=0.8"
}
