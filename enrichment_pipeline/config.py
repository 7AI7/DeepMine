# Enrichment Pipeline Configuration

import json
import random
from pathlib import Path

# ── Proxy ──────────────────────────────────────────────────────
USE_PROXY = False  # Set True when residential proxies are purchased
PROXY_FILE = "proxies.txt"  # one line per proxy: IP:PORT:USER:PASS
PROXY_ROTATE_AFTER = 8  # rotate proxy after N requests per worker

def load_proxies(filepath: str = PROXY_FILE) -> list[dict]:
    """Load proxies from file. Each line: IP:PORT:USER:PASS"""
    proxies = []
    p = Path(filepath)
    if not p.exists():
        return proxies
    for line in p.read_text().strip().splitlines():
        parts = line.strip().split(":")
        if len(parts) == 4:
            proxies.append({
                "server": f"http://{parts[0]}:{parts[1]}",
                "username": parts[2],
                "password": parts[3],
            })
        elif len(parts) == 2:
            proxies.append({"server": f"http://{parts[0]}:{parts[1]}"})
    return proxies

# ── Browser ────────────────────────────────────────────────────
MAX_WORKERS = 1   # Reduced from 15 for single-company test
HEADLESS = False  # Headed mode for manual CAPTCHA handling
BROWSER_TIMEOUT = 60000  # ms

VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
]

BROWSER_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-accelerated-2d-canvas",
    "--no-first-run",
    "--no-zygote",
    "--disable-gpu",
]

# ── User Agents (latest desktop-only, updated Feb 2026) ──────────────────
USER_AGENTS = [
    # Chrome on Windows (latest)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Chrome on macOS (latest)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    # Edge on Windows (latest)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
    # Firefox on Windows (latest)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    # Safari on macOS (latest)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
]

def random_ua() -> str:
    return random.choice(USER_AGENTS)

def random_viewport() -> dict:
    return random.choice(VIEWPORTS)

# ── Delays (seconds, randomized between min-max) ──────────────
BING_SEARCH_DELAY = (3, 7)
GOOGLE_SEARCH_DELAY = (5, 10)
PAGE_LOAD_DELAY = (2, 4)
MAPS_SEARCH_DELAY = (20, 30)
MAPS_RESULT_DELAY = (2, 4)  # between clicking individual Maps results

# ── CAPTCHA indicators ─────────────────────────────────────────
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

# ── File paths ─────────────────────────────────────────────────
INPUT_EXCEL = "companies.xlsx"
INPUT_SHEET = "companies"
OUTPUT_DIR = "output"
OUTPUT_EXCEL = "output/enriched_companies.xlsx"
LOGS_DIR = "logs"
PROGRESS_FILE = "progress.json"

# ── Name matching ──────────────────────────────────────────────
# Stop words removed during name comparison
# NOTE: "India" is deliberately NOT here — it helps disambiguation
NAME_STOP_WORDS = {"pvt", "ltd", "private", "limited", "the", "and", "of", "&"}

BING_NAME_THRESHOLD = 0.60   # Tofler, LinkedIn — looser (Bing truncates)
GOOGLE_NAME_THRESHOLD = 0.70  # TheCompanyCheck — stricter (wrong-company proven)
MAPS_NAME_THRESHOLD = 0.75    # Google Maps — strictest (very noisy results)

# ── Incremental save ───────────────────────────────────────────
SAVE_EVERY_N = 50  # save Excel after every N companies

# ── Context recycling ──────────────────────────────────────────
CONTEXT_RECYCLE_AFTER = 50  # recreate browser context after N requests
