# Tofler Scraper Configuration

# Browser Settings
MAX_CONCURRENT_BROWSERS = 10
HEADLESS = True  # Set to False for debugging
BROWSER_TIMEOUT = 60000  # 60 seconds

# Delays (in seconds) - min, max for random selection
# Balanced for speed vs anti-detection
SEARCH_DELAY = (4, 8)
PROFILE_DELAY = (2, 4)
SCROLL_DELAY = (0.3, 1.0)
PAGINATION_DELAY = (2, 4)
GENERAL_DELAY = (2, 5)

# Retry Settings
MAX_RETRIES = 2
RETRY_DELAY = 10  # seconds

# Proxy Configuration (Decodo ISP)
USE_PROXY = False  # Set to True to enable proxy
PROXY_CONFIG = {
    "server": os.environ.get("PROXY_SERVER", "https://isp.decodo.com:10000"),
    "username": os.environ.get("PROXY_USERNAME", "your_proxy_username"),
    "password": os.environ.get("PROXY_PASSWORD", "your_proxy_password")
}

# File Paths
INPUT_EXCEL = "companies.xlsx"
OUTPUT_DIR = "output"
LOGS_DIR = "logs"
PROGRESS_FILE = "progress.json"

# Sheet Names
INPUT_SHEET = "companies"
DIRECTORS_SHEET = "directors"
MANAGEMENT_SHEET = "management"

# Column Mappings
INPUT_COLUMNS = ["id", "name"]

DIRECTORS_COLUMNS = [
    "company_id",
    "person_name",
    "designation",
    "related_company",
    "industry",
    "status",
    "Designation_in_other_company",
    "contact"
]

MANAGEMENT_COLUMNS = [
    "id",
    "company_id",
    "name",
    "designation",
    "contact"
]

# User Agents (tested, real browser UAs)
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

# CAPTCHA Detection Patterns (more specific to avoid false positives)
CAPTCHA_INDICATORS = [
    "solve this captcha",
    "complete the captcha",
    "i'm not a robot",
    "verify you are human",
    "are you a robot",
    "unusual traffic from your",
    "automated queries",
    "verify it's you",
    "security check",
    "please verify",
]

# Search Engine Settings (Google as primary)
SEARCH_ENGINE_URL = "https://www.google.com/search?q="
SEARCH_TOP_RESULTS = 5  # Check top 5 results for Tofler link

# CSS Selectors
SELECTORS = {
    # Google search selectors
    "search_result_link": "div.g a[href*='tofler.in'], div.yuRUbf a",
    
    # Tofler page selectors
    "search_result": 'a[href*="/company/"]',
    "people_module": "#people-module",
    "people_table_row": "#people-module table tbody tr",
    "person_designation": "td:nth-child(1)",
    "person_link": "td:nth-child(2) a",
    "person_din": "td:nth-child(3)",
    "directorship_table_body": "#directorshipsTableBody",
    "directorship_row": "#directorshipsTableBody tr",
    "company_name_link": "td:nth-child(1) a",
    "industry": "td:nth-child(3)",
    "status_badge": "td:nth-child(4) .badge, td:nth-child(4)",
    "designation": "td:nth-child(7)",
    "pagination_next": "#directorshipsTable-next-btn",
    "pagination_disabled": "[disabled]"
}

# Viewports for randomization
VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
]

# Incremental save threshold
SAVE_THRESHOLD = 100  # Save Excel every 100 companies

# Context recycling
CONTEXT_RECYCLE_THRESHOLD = 50  # Restart browser context after 50 companies
