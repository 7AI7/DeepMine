# Enrichment Pipeline — LinkedIn URL + Followers via Bing Snippet
#
# Extracts LinkedIn company page URL and follower count from Bing search results.
# Does NOT visit LinkedIn directly — LinkedIn blocks datacenter proxies.

import re

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from utils.logger import setup_logger
from utils.name_matcher import is_match
from scrapers.google_snippet import GoogleSnippetScraper

logger = setup_logger("linkedin")


class LinkedInScraper:
    """
    Extract LinkedIn company URL + follower count from Google snippets.
    
    Uses the shared GoogleSnippetScraper instance (one per worker).
    Does NOT create its own browser.
    """
    
    def __init__(self, google_scraper: GoogleSnippetScraper):
        self.google = google_scraper
    
    async def scrape(self, company_name: str) -> dict:
        """
        Search Google for LinkedIn company page.
        
        Query: site:linkedin.com/company "{company_name}"
        
        Returns:
            {
                "linkedin_url": "https://in.linkedin.com/company/..." or "N/A",
                "linkedin_followers": "3,812" or "N/A",
            }
        """
        result = {
            "linkedin_url": "N/A",
            "linkedin_followers": "N/A",
        }
        
        query = f'site:linkedin.com/company "{company_name}"'
        search_results = await self.google.search(query, max_results=5)
        
        if not search_results:
            logger.debug(f"LinkedIn: no Google results for '{company_name}'")
            return result
        
        # Check for CAPTCHA
        if search_results and search_results[0].get("error") == "captcha":
            logger.warning(f"LinkedIn: CAPTCHA on Google for '{company_name}'")
            return result
        
        # Find first LinkedIn company URL with matching name
        for sr in search_results:
            url = sr.get("url", "")
            title = sr.get("title", "")
            snippet = sr.get("snippet", "")
            
            # Must be a LinkedIn company page URL
            if "linkedin.com/company/" not in url:
                continue
            
            # Name verification — reject wrong companies
            if not is_match(company_name, title, config.BING_NAME_THRESHOLD):
                logger.debug(
                    f"REJECTED LinkedIn match: '{company_name}' vs '{title}' "
                    f"(url: {url})"
                )
                continue
            
            # Found a match
            result["linkedin_url"] = url
            
            # Try to extract follower count
            # Google often puts it in a separate element above the title
            followers_raw = sr.get("followers", "")
            followers = self._extract_followers(followers_raw)
            
            if followers:
                result["linkedin_followers"] = followers
            else:
                # Fallback to snippet
                followers = self._extract_followers(snippet)
                if followers:
                    result["linkedin_followers"] = followers
                else:
                    # Also check title — sometimes followers appear there
                    followers = self._extract_followers(title)
                    if followers:
                        result["linkedin_followers"] = followers
            
            logger.info(
                f"LinkedIn: {company_name} → {url} "
                f"(followers: {result['linkedin_followers']})"
            )
            break
        
        return result
    
    def _extract_followers(self, text: str) -> str | None:
        """
        Extract follower count from text.
        
        Patterns:
          - "3,812 followers on LinkedIn"
          - "12K followers"
          - "1.2M followers"
        
        Returns the raw count string, or None if not found.
        """
        match = re.search(r'([\d,]+(?:\.\d+)?[KkMm]?\+?)\s*followers', text, re.IGNORECASE)
        if match:
            return match.group(1)
        return None
