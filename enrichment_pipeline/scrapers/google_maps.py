# Enrichment Pipeline — Google Maps Scraper
#
# Searches Google Maps for a company name, scrapes ALL results with matching names.
# Extracts: address, maps_link, phone, rating, review_count, email, category.
# Does NOT extract website (confirmed — same across all locations).
#
# REQUIRES residential proxy — Google Maps aggressively blocks datacenter IPs.
# Blocks images/fonts/CSS to reduce bandwidth (~60% saving).

import asyncio
import random
import re
from urllib.parse import quote_plus

from playwright.async_api import (
    async_playwright, Page, BrowserContext, Browser,
    TimeoutError as PlaywrightTimeoutError,
)

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from utils.logger import setup_logger
from utils.name_matcher import is_match

logger = setup_logger("google_maps")

# Resource types to block for bandwidth savings
# NOTE: Do NOT block stylesheet/CSS — it breaks element visibility on Maps
BLOCKED_RESOURCE_TYPES = {"image", "font", "media"}
BLOCKED_URL_PATTERNS = [
    "*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.svg", "*.ico",
    "*.woff", "*.woff2", "*.ttf", "*.eot",
    "maps.gstatic.com/mapfiles",
]


class GoogleMapsScraper:
    """
    Search Google Maps → scrape ALL same-name results → extract location details.
    
    Lifecycle:
      1. __init__(proxy, ua)
      2. await initialize()    — launch browser, block resources
      3. await scrape(name)    — search + extract all matching locations 
      4. await close()
    
    Each worker creates ONE GoogleMapsScraper with its own browser.
    """
    
    def __init__(self, proxy_config: dict | None = None, user_agent: str | None = None):
        self.proxy_config = proxy_config
        self.user_agent = user_agent or config.random_ua()
        self.playwright = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None
        self.request_count = 0
    
    async def initialize(self) -> None:
        """Launch browser, create context, block unnecessary resources."""
        self.playwright = await async_playwright().start()
        
        self.browser = await self.playwright.chromium.launch(
            headless=config.HEADLESS,
            args=config.BROWSER_ARGS,
        )
        
        await self._create_context()
    
    async def _create_context(self) -> None:
        """Create context with proxy, stealth, and resource blocking."""
        if self.context:
            await self.context.close()
        
        context_opts = {
            "user_agent": self.user_agent,
            "viewport": config.random_viewport(),
            "locale": "en-US",
            "timezone_id": "Asia/Kolkata",
            "geolocation": {"latitude": 20.5937, "longitude": 78.9629},  # India center
            "permissions": ["geolocation"],
        }
        
        if self.proxy_config:
            context_opts["proxy"] = self.proxy_config
        
        self.context = await self.browser.new_context(**context_opts)
        
        # Apply stealth
        try:
            from playwright_stealth import Stealth
            stealth = Stealth()
            await stealth.apply_stealth_async(self.context)
        except ImportError:
            logger.warning("playwright_stealth not installed")
        
        self.page = await self.context.new_page()
        
        # Block images, fonts, CSS to save ~60% bandwidth
        await self.page.route(
            "**/*",
            lambda route: (
                route.abort()
                if route.request.resource_type in BLOCKED_RESOURCE_TYPES
                else route.continue_()
            )
        )
        
        self.request_count = 0
    
    async def _maybe_recycle_context(self) -> None:
        """Recycle context after PROXY_ROTATE_AFTER requests."""
        if self.request_count >= config.PROXY_ROTATE_AFTER:
            logger.debug(f"Recycling Maps context after {self.request_count} requests")
            self.user_agent = config.random_ua()
            await self._create_context()
    
    async def scrape(self, company_name: str) -> list[dict]:
        """
        Search Google Maps and extract details for all matching locations.
        
        Args:
            company_name: Company to search for (no city appended — confirmed)
        
        Returns:
            List of dicts, one per matching location:
            [{
                "maps_name": "J S Auto Cast Foundry",
                "address": "Sf No 165/1 ..., Coimbatore 641107",
                "maps_link": "https://www.google.com/maps/place/...",
                "phone": "+91-...",
                "rating": "4.5",
                "review_count": "123",
                "email": "N/A",
                "category": "Factory",
            }]
            
            Empty list if no matching results or error.
        """
        await self._maybe_recycle_context()
        self.request_count += 1
        
        search_url = f"https://www.google.com/maps/search/{quote_plus(company_name)}"
        
        try:
            await self.page.goto(search_url, wait_until="domcontentloaded", timeout=config.BROWSER_TIMEOUT)
        except PlaywrightTimeoutError:
            logger.warning(f"Maps: timeout for '{company_name}'")
            return []
        except Exception as e:
            logger.error(f"Maps: navigation error for '{company_name}': {e}")
            return []
        
        # Wait for Maps to render results
        await asyncio.sleep(random.uniform(3, 5))
        
        # Check if we landed on a single-result page (no feed) vs multi-result feed
        has_feed = await self.page.locator('div[role="feed"]').count() > 0
        
        if has_feed:
            return await self._scrape_multi_results(company_name)
        else:
            # Single result — check if it's a place page
            return await self._scrape_single_result(company_name)
    
    async def _scrape_multi_results(self, company_name: str) -> list[dict]:
        """Scrape all matching results from the Maps search feed."""
        page = self.page
        locations = []
        
        # Scroll the results feed to load all items
        try:
            feed = page.locator('div[role="feed"]')
            previous_count = 0
            max_scrolls = 10
            
            for _ in range(max_scrolls):
                await feed.evaluate("el => el.scrollTop = el.scrollHeight")
                await asyncio.sleep(1.5)
                
                # Count result links
                current_count = await page.locator(
                    'div[role="feed"] a[href*="/maps/place/"]'
                ).count()
                
                if current_count == previous_count:
                    break
                previous_count = current_count
            
            logger.debug(f"Maps: {current_count} total results for '{company_name}'")
        except Exception as e:
            logger.warning(f"Maps: feed scroll error: {e}")
            return locations
        
        # Get all result items
        result_links = page.locator('div[role="feed"] a[href*="/maps/place/"]')
        count = await result_links.count()
        
        for i in range(count):
            try:
                link = result_links.nth(i)
                
                # Extract business name from aria-label first (most reliable)
                biz_name = ""
                aria = await link.get_attribute("aria-label")
                if aria:
                    biz_name = aria.strip()
                
                if not biz_name:
                    # Fallback: try text content from child elements
                    name_el = link.locator('[class*="fontHeadlineSmall"], [class*="qBF1Pd"]')
                    if await name_el.count() > 0:
                        biz_name = (await name_el.first.text_content()).strip()
                
                if not biz_name:
                    continue
                
                # Name filter — only process results with matching names
                if not is_match(company_name, biz_name, config.MAPS_NAME_THRESHOLD):
                    logger.debug(f"Maps: skipping non-matching result '{biz_name}'")
                    continue
                
                # Scroll element into view before clicking
                await link.scroll_into_view_if_needed(timeout=5000)
                await asyncio.sleep(0.5)
                
                # Click on this result to open details panel
                try:
                    await link.click(timeout=10000)
                except PlaywrightTimeoutError:
                    logger.debug(f"Maps: click timeout on result {i} '{biz_name}', trying force click")
                    await link.click(force=True, timeout=5000)
                
                await asyncio.sleep(random.uniform(*config.MAPS_RESULT_DELAY))
                
                # Wait for details panel to load
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
                await asyncio.sleep(1)
                
                # Extract details from the opened panel
                location = await self._extract_location_details(biz_name)
                if location:
                    locations.append(location)
                
                # Go back to results list
                back_btn = page.locator('button[aria-label="Back"]')
                if await back_btn.count() > 0:
                    await back_btn.click()
                    await asyncio.sleep(1.5)
                
            except Exception as e:
                logger.debug(f"Maps: error on result {i} '{biz_name}': {e}")
                # Try to recover — navigate back if stuck
                try:
                    back_btn = page.locator('button[aria-label="Back"]')
                    if await back_btn.count() > 0:
                        await back_btn.click()
                        await asyncio.sleep(1)
                except Exception:
                    pass
                continue
        
        logger.info(f"Maps: {company_name} → {len(locations)} matching locations")
        return locations
    
    async def _scrape_single_result(self, company_name: str) -> list[dict]:
        """Handle single-result Maps page (no feed, direct place page)."""
        # Check name in page title / heading
        try:
            heading = await self.page.locator('h1').first.text_content()
        except Exception:
            heading = ""
        
        if not heading or not is_match(company_name, heading.strip(), config.MAPS_NAME_THRESHOLD):
            logger.debug(f"Maps: single result name mismatch: '{heading}'")
            return []
        
        location = await self._extract_location_details(heading.strip())
        return [location] if location else []
    
    async def _extract_location_details(self, biz_name: str) -> dict | None:
        """
        Extract details from the currently-open Maps place panel.
        
        Returns dict with: maps_name, address, maps_link, phone, rating,
        review_count, email, category. Or None on failure.
        """
        page = self.page
        
        location = {
            "maps_name": biz_name,
            "address": "N/A",
            "maps_link": page.url,
            "phone": "N/A",
            "rating": "N/A",
            "review_count": "N/A",
            "email": "N/A",
            "category": "N/A",
        }
        
        try:
            # Address
            addr_btn = page.locator('button[data-item-id="address"]')
            if await addr_btn.count() > 0:
                addr_text = await addr_btn.first.get_attribute("aria-label")
                if addr_text:
                    # aria-label is like "Address: Sf No 165/1 ..."
                    location["address"] = addr_text.replace("Address: ", "").strip()
            
            # Phone
            phone_btn = page.locator('button[data-item-id^="phone:"]')
            if await phone_btn.count() > 0:
                phone_text = await phone_btn.first.get_attribute("aria-label")
                if phone_text:
                    location["phone"] = phone_text.replace("Phone: ", "").strip()
            
            # Rating + review count
            rating_el = page.locator('div[role="img"][aria-label*="stars"]')
            if await rating_el.count() > 0:
                rating_label = await rating_el.first.get_attribute("aria-label")
                if rating_label:
                    # "4.5 stars" → "4.5"
                    rm = re.search(r'([\d.]+)\s*stars?', rating_label, re.IGNORECASE)
                    if rm:
                        location["rating"] = rm.group(1)
            
            # Review count — near the rating, text like "(123)" or "123 reviews"
            review_el = page.locator('span[aria-label*="reviews"]')
            if await review_el.count() > 0:
                review_label = await review_el.first.get_attribute("aria-label")
                if review_label:
                    rvm = re.search(r'([\d,]+)\s*reviews?', review_label, re.IGNORECASE)
                    if rvm:
                        location["review_count"] = rvm.group(1)
            
            # Email — rare on Maps but check
            email_btn = page.locator('a[href^="mailto:"]')
            if await email_btn.count() > 0:
                href = await email_btn.first.get_attribute("href")
                if href:
                    location["email"] = href.replace("mailto:", "").strip()
            
            # Category — below business name
            cat_el = page.locator('button[jsaction*="category"]')
            if await cat_el.count() > 0:
                location["category"] = (await cat_el.first.text_content()).strip()
            else:
                # Fallback: look for category text near the heading
                cat_spans = page.locator('span[jsan*="category"], button[class*="DkEaL"]')
                if await cat_spans.count() > 0:
                    location["category"] = (await cat_spans.first.text_content()).strip()
            
            # Maps link — use the share/current URL
            location["maps_link"] = page.url
            
        except Exception as e:
            logger.debug(f"Maps: extraction error for '{biz_name}': {e}")
        
        return location
    
    async def close(self) -> None:
        """Cleanup browser resources."""
        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        except Exception as e:
            logger.debug(f"Cleanup error (non-critical): {e}")
