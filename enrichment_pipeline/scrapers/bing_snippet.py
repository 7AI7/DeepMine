# Enrichment Pipeline — Bing SERP Snippet Extractor
#
# Reusable module: searches Bing, returns structured results {title, url, snippet}.
# Used by: linkedin.py (and optionally others needing Bing search).
# Anti-detection: playwright_stealth, random UA/viewport, session cookie reuse.

import asyncio
import random
import re
from urllib.parse import quote_plus, urlparse, parse_qs, unquote

from playwright.async_api import async_playwright, Page, BrowserContext, Browser, TimeoutError as PlaywrightTimeoutError

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from utils.logger import setup_logger

logger = setup_logger("bing_snippet")


class BingSnippetScraper:
    """
    Searches Bing and extracts search result snippets.
    
    Lifecycle:
      1. __init__(proxy, ua) — store config
      2. await initialize()  — launch browser, create context + page
      3. await search(query)  — run searches (page is reused for session cookies)
      4. await close()        — cleanup
    
    One instance per worker. Page is reused across all searches by that worker.
    Context is recycled after CONTEXT_RECYCLE_AFTER requests (new proxy + UA).
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
        """Launch browser with anti-detection settings."""
        self.playwright = await async_playwright().start()
        
        self.browser = await self.playwright.chromium.launch(
            headless=config.HEADLESS,
            args=config.BROWSER_ARGS,
        )
        
        await self._create_context()
    
    async def _create_context(self) -> None:
        """Create a new browser context with proxy, UA, viewport, and stealth."""
        if self.context:
            await self.context.close()
        
        context_opts = {
            "user_agent": self.user_agent,
            "viewport": config.random_viewport(),
            "locale": "en-US",
            "timezone_id": "Asia/Kolkata",
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
            logger.warning("playwright_stealth not installed — running without stealth")
        
        self.page = await self.context.new_page()
        self.request_count = 0
    
    async def _maybe_recycle_context(self) -> None:
        """Recycle context after N requests (new proxy, UA, viewport)."""
        if self.request_count >= config.CONTEXT_RECYCLE_AFTER:
            logger.debug(f"Recycling Bing context after {self.request_count} requests")
            self.user_agent = config.random_ua()
            await self._create_context()
    
    async def search(self, query: str, max_results: int = 5) -> list[dict]:
        """
        Search Bing and return structured results.
        
        Args:
            query: Full search query, e.g. 'site:linkedin.com/company "Sadhu Forging"'
            max_results: Max number of results to extract (default 5)
        
        Returns:
            List of {"title": str, "url": str, "snippet": str} dicts.
            On CAPTCHA: returns [{"error": "captcha"}].
            On failure: returns [].
        """
        await self._maybe_recycle_context()
        self.request_count += 1
        
        url = f"https://www.bing.com/search?q={quote_plus(query)}"
        
        try:
            await self.page.goto(url, wait_until="domcontentloaded", timeout=config.BROWSER_TIMEOUT)
        except PlaywrightTimeoutError:
            logger.warning(f"Bing timeout for query: {query[:60]}...")
            return []
        except Exception as e:
            logger.error(f"Bing navigation error: {e}")
            return []
        
        # Human-like delay
        await asyncio.sleep(random.uniform(*config.BING_SEARCH_DELAY))
        
        # Check for CAPTCHA / verification — wait for user to solve
        if await self._check_captcha():
            logger.warning(f"CAPTCHA/verification detected on Bing for: {query[:60]}...")
            logger.info("Waiting up to 120s for manual CAPTCHA resolution...")
            resolved = False
            for wait_i in range(24):  # 24 × 5s = 120s
                await asyncio.sleep(5)
                if not await self._check_captcha():
                    logger.info(f"CAPTCHA resolved after {(wait_i+1)*5}s")
                    resolved = True
                    # Check if results exist now
                    await asyncio.sleep(2)
                    break
            if not resolved:
                logger.warning(f"CAPTCHA not resolved within 120s for: {query[:60]}...")
                return [{"error": "captcha"}]
            
            # After CAPTCHA solve, re-navigate to original search URL
            # (Bing verification page redirects to a different URL entirely)
            logger.debug("Re-navigating to original search URL after Bing CAPTCHA solve")
            try:
                await self.page.goto(url, wait_until="domcontentloaded", timeout=config.BROWSER_TIMEOUT)
                await asyncio.sleep(random.uniform(*config.BING_SEARCH_DELAY))
                
                # Check for CAPTCHA again after re-navigation
                if await self._check_captcha():
                    logger.warning("Bing CAPTCHA reappeared after re-navigation")
                    return [{"error": "captcha"}]
            except Exception as e:
                logger.error(f"Bing re-navigation error: {e}")
                return []
        
        # Wait for actual result items to appear
        try:
            await self.page.wait_for_selector('li.b_algo', timeout=10000)
        except PlaywrightTimeoutError:
            logger.debug("Bing: no li.b_algo results found within 10s")
        
        # Extract results using locators + Python-side URL cleaning
        # (matches proven approach from bing website.py reference)
        results = []
        try:
            items = await self.page.locator('li.b_algo').all()
            logger.debug(f"Bing: found {len(items)} li.b_algo items on page")
            if not items:
                items = await self.page.locator('#b_results > li').all()
                logger.debug(f"Bing: fallback found {len(items)} #b_results > li items")
            
            for item in items[:max_results]:
                try:
                    title_el = item.locator('h2 a').first
                    if not await title_el.count():
                        continue
                    
                    title = (await title_el.text_content() or '').strip()
                    raw_href = await title_el.get_attribute('href') or ''
                    
                    # Get cite text as last-resort fallback for URL reconstruction
                    cite_text = ''
                    cite_el = item.locator('cite').first
                    if await cite_el.count():
                        cite_text = (await cite_el.text_content() or '').strip()
                    
                    # Clean Bing tracking URLs — 3-level fallback
                    url = self._clean_href(raw_href, cite_text)
                    
                    # Extract snippet
                    snippet = ''
                    for sel in ['div.b_caption p', '.b_lineclamp2', '.b_lineclamp3', 'p']:
                        snippet_el = item.locator(sel).first
                        if await snippet_el.count():
                            snippet = (await snippet_el.text_content() or '').strip()
                            break
                    
                    if title and url:
                        results.append({
                            'title': title,
                            'url': url,
                            'snippet': snippet,
                        })
                except Exception as e:
                    logger.debug(f"Error extracting single Bing result: {e}")
                    continue
        except Exception as e:
            logger.debug(f"Error during Bing result extraction: {e}")
        
        if not results:
            # Debug: log page title and URL to understand what page we're on
            page_title = await self.page.title()
            page_url = self.page.url
            logger.debug(f"Bing: 0 results. Page title='{page_title}', url='{page_url}'")
        else:
            logger.debug(f"Bing: {len(results)} results for '{query[:50]}...'")
        return results
    
    @staticmethod
    def _clean_href(href: str, cite_text: str = '') -> str:
        """
        Decode Bing tracking URLs (bing.com/ck/...) to get the real destination URL.
        
        3-level fallback chain for bing.com/ck/... tracking URLs:
          Level 1: ?u= param starts with 'a1'  → strip prefix + base64-decode
          Level 2: ?u= param present but no a1  → URL-unquote (older Bing format)
          Level 3: no ?u= param at all          → reconstruct from cite tag text
        If not a tracking URL at all, return href unchanged.
        """
        import base64 as _b64
        if href and "/ck/" in href and "bing.com" in href:
            u_list = parse_qs(urlparse(href).query).get("u")
            if u_list:
                raw = u_list[0]
                # Level 1: newer Bing format — 'a1' + base64
                if raw.startswith("a1"):
                    try:
                        b64_part = raw[2:]
                        b64_part += '=' * (-len(b64_part) % 4)
                        return _b64.b64decode(b64_part).decode('utf-8')
                    except Exception:
                        pass
                # Level 2: older Bing format — URL-encoded string
                decoded = unquote(raw)
                if decoded.startswith('http'):
                    return decoded
            # Level 3: no ?u= param — reconstruct from cite tag text
            # cite text looks like: "linkedin.com › company › pricollimited"
            if cite_text:
                clean = cite_text.replace(' ', '').replace('›', '/').replace('>', '/')
                if not clean.startswith('http'):
                    clean = 'https://' + clean
                return clean
            # Absolute last resort: return raw tracking URL (caller can detect and skip)
            return href
        return href
    
    async def _check_captcha(self) -> bool:
        """Check if the current page is a CAPTCHA or verification challenge."""
        try:
            content = await self.page.content()
            title = await self.page.title()
            combined = (content + title).lower()
            
            # Bing-specific verification indicators
            bing_verification = [
                "one last step",
                "why did this happen",
                "blocked",
                "verify yourself",
            ]
            all_indicators = config.CAPTCHA_INDICATORS + bing_verification
            return any(indicator in combined for indicator in all_indicators)
        except Exception:
            return False
    
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
