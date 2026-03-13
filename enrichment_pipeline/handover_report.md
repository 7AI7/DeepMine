# Comprehensive Enrichment Pipeline Handover Report

**Date/Time:** 2026-03-06
**Project:** Automated Company Enrichment Pipeline (Playwright-based)

This document is the authoritative context guide for the incoming AI agent. Read this entirely before writing any code or modifying the architecture. Do not hallucinate file names or past decisions.

## 1. Executive Summary & Session History

The user is building an OSINT/Enrichment pipeline that takes a list of companies (`companies.xlsx`) and sequentially scrapes intelligence data (LinkedIn URLs, follower counts, TheCompanyCheck revenues, and Google Maps addresses) using headless browsers.

**What Happened in the Previous Session:**
1.  **Bing Decoding Fix:** We initially spent time fixing `scrapers/bing_snippet.py`. Bing obfuscates its outbound links (`bing.com/ck/`) using alternating methods (base64 appending with `a1`, or URL encoding in a `?u=` param). We built a robust 3-tiered fallback decoder.
2.  **The Pivot (LinkedIn Follower Counts):** The user required explicit LinkedIn follower counts (e.g., "64K+ followers"). We discovered Bing snippets rarely include this metric. Therefore, we made a strategic architectural pivot: we completely stripped Bing from the `LinkedInScraper` and migrated it to rely entirely on `GoogleSnippetScraper`.
3.  **Google DOM Adaptation:** When we migrated LinkedIn to Google, it returned 0 results. By dumping the raw HTML, we learned that Google dynamically alters its DOM based on the query. Standard results use `<div class="g">`, but LinkedIn social profiles use a rich-snippet container: `<div class="N54PNb">`. Furthermore, follower counts are injected into `<span>` or `<cite>` elements near the header breadcrumbs, not in the standard description tags.
4.  **Successful Extraction:** We heavily modified the Javascript evaluator in `scrapers/google_snippet.py` to target both `div.g` and `div.N54PNb`. The regex in `linkedin.py` was updated to `r'([\d,]+(?:\.\d+)?[KkMm]?\+?)\s*followers'`. LinkedIn scraping now perfectly extracts URLs and follower counts.

---

## 2. Codebase Architecture & Deep Dependency Analysis

The system uses a worker-based multiprocessing pattern orchestration.

### Core Orchestration
*   **`main.py`**: The entry point. Handles task chunking, initializes Playwright browsers per worker, and sequentially fires the scrapers.
*   **`config.py`**: Manages environment variables, `USER_AGENTS` (must be desktop-only to avoid mobile DOM structures), and proxy formatting.
*   **`utils/name_matcher.py`**: A critical utility containing `is_match()` and `match_score()` (difflib) to ensure the scraped snippet actually belongs to the target company by sanitizing LLC/INC suffixes.

### The Scraper Engine (Upstream/Downstream Dependencies)

#### `scrapers/google_snippet.py` (The Engine)
*   **Role:** The core upstream dependency for multiple modules. It executes Google searches and runs `page.evaluate()` to convert complex DOM nodes into a generic `{title, url, snippet, followers}` dictionary.
*   **Upstream Dependencies:** Requires `config.py` (Proxy/UA) and relies on Playwright stealth protocols to bypass initial HTTP 429s.
*   **Downstream Dependencies:** `LinkedInScraper` and `CompanyCheckScraper` are *entirely dependent* on this engine returning valid JSON. If Google alters its DOM layout, this file must be updated or all downstream scrapers fail.

#### `scrapers/linkedin.py` (Downstream Consumer)
*   **Role:** Formats the `"site:linkedin.com/company [Names]"` query, parses the followers string using regex, and enforces the `name_matcher.py` confidence threshold.
*   **Status:** **WORKING**. Actively relies on the `div.N54PNb` DOM logic in `google_snippet.py`.

#### `scrapers/thecompanycheck.py` (Downstream Consumer)
*   **Role:** Extracts revenue and employee count data from TheCompanyCheck listings via Google.
*   **Status:** **FAILING (Current Blocker)**. Currently returning 0 results.

#### `scrapers/bing_snippet.py` (Orphaned Engine)
*   **Role:** Formerly the main search engine. 
*   **Status:** Fixed, but currently unassigned. Available as a fallback if Google proxy bans become too severe.

---

## 3. The Immediate Blocker: TheCompanyCheck DOM Failure

**Symptom:** Running the pipeline or `test_scraper.py` for `TheCompanyCheck` returns `N/A` for all fields.
**Diagnosis:** Because `google_snippet.py` perfectly extracts LinkedIn results but fails on `site:thecompanycheck.com`, it is highly probable that Google is wrapping business directory results in yet *another* unique CSS class or tag structure that is neither `div.g` nor `div.N54PNb`.

**Provided Debug Tools:**
1.  **`test_scraper.py`**: An isolated script specifically hardcoded to test the CompanyCheck scraper without running `main.py`.
2.  **`google_test_results.txt`**: A raw Playwright HTML dump of the exact Google SERP spanning the query `site:thecompanycheck.com "Pricol Limited"`.

---

## 4. Prioritized Prospective Features

The development ideology prioritizes **Core Functionality** over *Observability* or *Security/Scaling*.

### Priority 1: Core Functionality (Data Extraction)
These tasks represent immediate blockers to the user getting their desired Excel output.

*   `[TAG-FIX-TCC]` **Fix TheCompanyCheck Extractor:** Analyze `google_test_results.txt` using a Python regex script to find the specific `<div>` container class Google uses for TCC results. Update the JS `querySelectorAll` in `google_snippet.py` without breaking the `N54PNb` LinkedIn logic.
*   `[TAG-FIX-MAPS]` **Verify Google Maps Scraper:** Once TCC is fixed, ensure `google_maps.py` is capable of loading and handling the maps DOM to extract address and phone numbers.

### Priority 2: Pipeline Stability & Anti-Bot
These tasks ensure the pipeline can run on a list of 100+ companies without requiring babysitting.

*   `[TAG-STAB-PROXY]` **Automated Proxy Cycling:** If `google_snippet.py` detects a CAPTCHA, it currently pauses. Implement logic to instantly throw away the current Playwright context, rotate to a new proxy IP, and retry the search seamlessly.
*   `[TAG-STAB-HEADLESS]` **Headless CAPTCHA bypassing:** If proxy rotation is insufficient, implement a 2Captcha/Anti-Captcha API integration for fully remote, uninterrupted headless running.

### Priority 3: Observability, Logging, and Data Quality (Lower Priority)
These tasks refine the system after it is functional.

*   `[TAG-OBS-OUTPUT]` **Incremental Saving Validation:** Ensure `utils/excel_handler.py` correctly saves data row-by-row so crashes don't lose entire batches.
*   `[TAG-OBS-METRICS]` **Success Rate Logging:** Expand `utils/logger.py` to track the percentage of successful extractions per scraper to easily identify if a specific data source fundamentally alters its layout mid-run.

---

## 5. Unified Unified Task Checklist

The incoming AI agent should work through these tagged tasks sequentially:

- [ ] `[TAG-FIX-TCC]` Build a python script to parse `google_test_results.txt` and identify the missing DOM containers.
- [ ] `[TAG-FIX-TCC]` Update `scrapers/google_snippet.py` Javascript evaluation block with the new CSS selectors. 
- [ ] `[TAG-FIX-TCC]` Run `python test_scraper.py` to prove TCC data flows correctly.
- [ ] `[TAG-FIX-MAPS]` Trigger `main.py` with `MAX_WORKERS=1` to test the Google Maps scraper on a single company.
- [ ] `[TAG-STAB-PROXY]` Modify `google_snippet.py` to automatically rotate proxies instead of sleeping when a CAPTCHA page is hit.
- [ ] `[TAG-STAB-HEADLESS]` Ensure the full pipeline runs flawlessly with `HEADLESS=True`. 
- [ ] `[TAG-OBS-OUTPUT]` Verify the final `.xlsx` aggregation output formatting.
