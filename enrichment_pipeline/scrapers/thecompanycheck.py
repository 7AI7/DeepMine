# Enrichment Pipeline — TheCompanyCheck Scraper
#
# Extracts: revenue_2023, net_profit_2023, employee_count, tcc_source_url
# from TheCompanyCheck company pages via Google search.
#
# URL formats on TheCompanyCheck:
#   /company/{slug}/{CIN}  → CIN-format page (most common in Google results)
#                            Revenue visible in About paragraph: "revenue of ₹X Cr"
#   /company/b/{slug}/{id} → b-format page (preferred, when Google returns it)
#                            Revenue visible in Key Metrics cards
#
# Strategy: search Google for site:thecompanycheck.com "{company_name}",
# prefer /company/b/ URLs, fall back to /company/{CIN} URLs.
# Skip /legal/, /people-profile/, /director/ pages.
# Extract data from full page text via regex — handles both page formats.

import asyncio
import random
import re

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from utils.logger import setup_logger
from utils.name_matcher import is_match
from scrapers.google_snippet import GoogleSnippetScraper

logger = setup_logger("thecompanycheck")

# Default return when data unavailable
_NA_RESULT = {
    "revenue_2023": "N/A",
    "net_profit_2023": "N/A",
    "employee_count": "N/A",
    "tcc_source_url": "N/A",
}


class CompanyCheckScraper:
    """
    Extract revenue, net profit, and employee count from TheCompanyCheck.

    Flow:
      1. Google search: site:thecompanycheck.com "{company_name}"
      2. Pick best URL: prefer /company/b/, fall back to /company/{CIN}
         Skip /legal/, /people-profile/, /director/ pages
      3. Navigate to that page
      4. Extract metrics from full page text via regex

    Uses the shared GoogleSnippetScraper instance (one per worker).
    """
    
    def __init__(self, google_scraper: GoogleSnippetScraper):
        self.google = google_scraper
    
    async def scrape(self, company_name: str) -> dict:
        """
        Search Google → pick best TCC URL → navigate → extract metrics.

        Returns:
            {
                "revenue_2023":    "₹1,931.67 Cr" or "N/A",
                "net_profit_2023": "₹X Cr" or "N/A",
                "employee_count":  "314" or "N/A",
                "tcc_source_url":  "https://..." or "N/A",
            }
        """
        result = dict(_NA_RESULT)
        
        # ── Step 1: Google search for TheCompanyCheck URL ──
        query = f'site:thecompanycheck.com "{company_name}"'
        search_results = await self.google.search(query, max_results=5)

        if not search_results:
            logger.debug(f"TCC: no Google results for '{company_name}'")
            return result

        if search_results[0].get("error") == "captcha":
            logger.warning(f"TCC: CAPTCHA on Google for '{company_name}'")
            return result

        # ── Step 2: Pick best URL ──
        # Prefer /company/b/ (Key Metrics visible), fall back to any /company/ URL.
        # Skip non-company pages (legal, people-profile, etc.).
        b_url = None
        fallback_url = None
        for sr in search_results:
            url = sr.get("url", "")
            title = sr.get("title", "")

            # Skip pages that are definitely not company overview pages
            if any(skip in url for skip in ["/legal/", "/people-profile/", "/director/"]):
                logger.debug(f"TCC: skipping non-company URL: {url}")
                continue

            # Name verification — reject wrong companies
            if not is_match(company_name, title, config.GOOGLE_NAME_THRESHOLD):
                logger.debug(f"REJECTED TCC match: '{company_name}' vs '{title}'")
                continue

            if "/company/b/" in url:
                b_url = url
                break
            elif "/company/" in url and fallback_url is None:
                fallback_url = url

        b_url = b_url or fallback_url

        if not b_url:
            logger.info(f"TCC: no usable URL found for '{company_name}'")
            return result
        
        # ── Step 3: Navigate to the best available TCC company page ──
        try:
            await self.google.page.goto(
                b_url, wait_until="domcontentloaded", timeout=config.BROWSER_TIMEOUT
            )
        except PlaywrightTimeoutError:
            logger.warning(f"TCC: timeout loading {b_url}")
            return result
        except Exception as e:
            logger.error(f"TCC: navigation error for {b_url}: {e}")
            return result
        
        await asyncio.sleep(random.uniform(*config.PAGE_LOAD_DELAY))
        
        result["tcc_source_url"] = b_url
        
        # ── Step 4: Extract Key Metrics ──
        try:
            metrics = await self._extract_key_metrics()
            result.update(metrics)
        except Exception as e:
            logger.error(f"TCC: extraction error for '{company_name}': {e}")
        
        logger.info(
            f"TCC: {company_name} → revenue={result['revenue_2023']}, "
            f"profit={result['net_profit_2023']}, employees={result['employee_count']}"
        )
        return result
    
    async def _extract_key_metrics(self) -> dict:
        """
        Extract revenue, net profit, and employee count from the full page text.

        Handles two page formats:

        CIN-format (/company/{slug}/{CIN}) — most common:
          About paragraph: "In 2023, it reported revenue of ₹1,931.67 Cr ...
                            Employing 314 professional"

        b-format (/company/b/{slug}/{id}) — preferred when available:
          Key Metrics cards: "ANNUAL REVENUE  $53.43 M (USD)"
                             "EMPLOYEE COUNT  1210"

        Uses regex cascades so both formats are handled transparently.
        """
        page = self.google.page
        
        metrics = {
            "revenue_2023": "N/A",
            "net_profit_2023": "N/A",
            "employee_count": "N/A",
        }

        all_text = await page.evaluate("""
            () => document.body.innerText
        """)
        
        if not all_text:
            return metrics

        # ── Revenue ───────────────────────────────────────────────────────────
        # About paragraph format: "revenue of ₹1,931.67 Cr"
        rev_match = re.search(
            r'revenue\s+of\s+([\u20b9\$][\d,]+(?:\.\d+)?\s*(?:Cr|M|B|K)?(?:\s*\(USD\))?)',
            all_text, re.IGNORECASE
        )
        # Fallback: Key Metrics card "ANNUAL REVENUE\n$53.43 M (USD)" (b-format pages)
        if not rev_match:
            rev_match = re.search(
                r'ANNUAL\s*REVENUE\s*\n?\s*([\u20b9\$][\d.,]+\s*(?:Cr|M|B|K)?(?:\s*\(USD\))?)',
                all_text, re.IGNORECASE
            )
        if rev_match:
            metrics["revenue_2023"] = rev_match.group(1).strip()

        # ── Net Profit ────────────────────────────────────────────────────────
        # About paragraph may say "net profit of ₹X Cr" on some pages
        profit_match = re.search(
            r'net\s*profit\s+of\s+([\u20b9\$][\d,]+(?:\.\d+)?\s*(?:Cr|M|B|K)?(?:\s*\(USD\))?)',
            all_text, re.IGNORECASE
        )
        if not profit_match:
            profit_match = re.search(
                r'NET\s*PROFIT\s*\n?\s*([\u20b9\$][\d.,]+\s*(?:Cr|M|B|K)?(?:\s*\(USD\))?)',
                all_text, re.IGNORECASE
            )
        if profit_match:
            metrics["net_profit_2023"] = profit_match.group(1).strip()

        # ── Employee Count ────────────────────────────────────────────────────
        # Format 1: "Employing 314 professional"
        emp_match = re.search(r'Employing\s+([\d,]+)\s+professional', all_text, re.IGNORECASE)
        # Format 2: "workforce of 314 employees"
        if not emp_match:
            emp_match = re.search(r'workforce\s+of\s+([\d,]+)\s+employees', all_text, re.IGNORECASE)
        # Format 3: Key Metrics card "EMPLOYEE COUNT\n1210" (b-format pages)
        if not emp_match:
            emp_match = re.search(r'EMPLOYEE\s*COUNT\s*\n?\s*([\d,]+)', all_text, re.IGNORECASE)
        # Format 4: generic "Employees\n1210"
        if not emp_match:
            emp_match = re.search(r'Employees\s*\n?\s*([\d,]+)', all_text, re.IGNORECASE)
        if emp_match:
            metrics["employee_count"] = emp_match.group(1).strip()

        return metrics