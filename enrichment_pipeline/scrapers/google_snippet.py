# Enrichment Pipeline — Google SERP Scraper
#
# Searches Google and extracts structured results (URL, title, snippet, followers).
# Used by:
#   - LinkedInScraper    → site:linkedin.com/company queries
#   - CompanyCheckScraper → site:thecompanycheck.com queries
#
# DOM selectors confirmed against live Google SERPs (March 2026):
#   - Result card:  div.tF2Cxc  (replaced the old div.g)
#   - Title:        h3.LC20lb
#   - Link:         a[jsname="UWckNb"]  (avoids favicon/breadcrumb anchors)
#   - Snippet:      div.VwiC3b
#   - Followers:    cite, span.VuuXrf  (LinkedIn rich-snippet)
#
# REQUIRES residential proxy — Google blocks datacenter IPs aggressively.

import asyncio
import random
import re
from urllib.parse import quote_plus, urlparse, parse_qs, unquote

from playwright.async_api import async_playwright, Page, BrowserContext, Browser, TimeoutError as PlaywrightTimeoutError

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from utils.logger import setup_logger

logger = setup_logger("google_snippet")


class GoogleSnippetScraper:
    """
    Searches Google and extracts search result snippets.

    Lifecycle (same as BingSnippetScraper):
      1. __init__(proxy, ua)
      2. await initialize()
      3. await search(query)
      4. await close()

    Key differences from Bing:
      - Modern Google DOM uses div.tF2Cxc cards (div.g is obsolete)
      - Google sometimes wraps URLs in /url?q=... redirects — we unwrap them
      - Longer delays required (GOOGLE_SEARCH_DELAY = 5-10s)
      - Residential proxy MANDATORY
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
        """Recycle context after N requests."""
        if self.request_count >= config.CONTEXT_RECYCLE_AFTER:
            logger.debug(f"Recycling Google context after {self.request_count} requests")
            self.user_agent = config.random_ua()
            await self._create_context()
    
    async def search(self, query: str, max_results: int = 3) -> list[dict]:
        """
        Search Google and return structured results.
        
        Args:
            query: Full query, e.g. 'site:thecompanycheck.com "J S Auto Cast Foundry"'
            max_results: Max results to extract (default 3 — Google blocks with too many)
        
        Returns:
            List of {"title": str, "url": str, "snippet": str} dicts.
            On CAPTCHA: returns [{"error": "captcha"}].
        """
        await self._maybe_recycle_context()
        self.request_count += 1
        
        url = f"https://www.google.com/search?q={quote_plus(query)}"
        
        try:
            await self.page.goto(url, wait_until="domcontentloaded", timeout=config.BROWSER_TIMEOUT)
        except PlaywrightTimeoutError:
            logger.warning(f"Google timeout for query: {query[:60]}...")
            return []
        except Exception as e:
            logger.error(f"Google navigation error: {e}")
            return []
        
        # Human-like delay — longer for Google
        await asyncio.sleep(random.uniform(*config.GOOGLE_SEARCH_DELAY))
        
        # Check for CAPTCHA — wait for user to solve
        if await self._check_captcha():
            logger.warning(f"CAPTCHA detected on Google for: {query[:60]}...")
            logger.info("Waiting up to 120s for manual CAPTCHA resolution...")
            resolved = False
            for wait_i in range(24):  # 24 × 5s = 120s
                await asyncio.sleep(5)
                if not await self._check_captcha():
                    logger.info(f"Google CAPTCHA resolved after {(wait_i+1)*5}s")
                    resolved = True
                    break
            if not resolved:
                logger.warning(f"Google CAPTCHA not resolved within 120s for: {query[:60]}...")
                return [{"error": "captcha"}]
            
            # After CAPTCHA solve, re-navigate to original search URL
            # (CAPTCHA page often redirects to a different page)
            logger.debug("Re-navigating to original search URL after CAPTCHA solve")
            try:
                await self.page.goto(url, wait_until="domcontentloaded", timeout=config.BROWSER_TIMEOUT)
                await asyncio.sleep(random.uniform(*config.GOOGLE_SEARCH_DELAY))
                
                # Check for CAPTCHA again after re-navigation
                if await self._check_captcha():
                    logger.warning("Google CAPTCHA reappeared after re-navigation")
                    return [{"error": "captcha"}]
            except Exception as e:
                # "interrupted by another navigation" means Google redirected the
                # page itself (e.g. appending &sei=...) — not a real failure.
                # Just wait for it to settle and continue to extraction.
                if "interrupted by another navigation" in str(e):
                    logger.debug("Re-navigation interrupted by Google redirect — waiting for page to settle")
                    await asyncio.sleep(random.uniform(*config.GOOGLE_SEARCH_DELAY))
                else:
                    logger.error(f"Google re-navigation error: {e}")
                    return []
        
        # Wait for result containers to appear
        # Note: div.g is obsolete in current Google layout; tF2Cxc is the modern card
        try:
            await self.page.wait_for_selector('div.tF2Cxc, div.N54PNb, #search', timeout=10000)
        except PlaywrightTimeoutError:
            logger.debug("Google: no results container found within 10s")
        
        # Extract results — Google's DOM is more complex
        results = await self.page.evaluate(f"""
            () => {{
                const results = [];

                // Modern Google layout (2024+): each organic result lives in div.tF2Cxc
                // which wraps div.N54PNb containing the title anchor and snippet.
                // Older layout used div.g; that class no longer appears in real SERPs.
                let items = document.querySelectorAll('div.tF2Cxc');
                if (items.length === 0) {{
                    // Fallback for any legacy or alternate layout
                    items = document.querySelectorAll('div.N54PNb, div.g');
                }}

                for (let i = 0; i < Math.min(items.length, {max_results}); i++) {{
                    const item = items[i];

                    // ── Title ──────────────────────────────────────────────
                    // Prefer h3.LC20lb (confirmed present in current layout).
                    // Fall back to any h3, then to a bold span inside an anchor.
                    let titleEl = item.querySelector('h3.LC20lb')
                                || item.querySelector('h3');
                    if (!titleEl) {{
                        const aTags = item.querySelectorAll('a[href]');
                        for (const a of aTags) {{
                            const span = a.querySelector('span');
                            if (span && span.textContent.trim().length > 5) {{
                                titleEl = span;
                                break;
                            }}
                        }}
                    }}

                    // ── URL ────────────────────────────────────────────────
                    // Use the specific jsname="UWckNb" anchor (the main title link).
                    // This avoids picking up favicon / breadcrumb anchors that appear
                    // earlier in the DOM order and would give only base-domain URLs.
                    const linkEl = item.querySelector('a[jsname="UWckNb"]')
                                || item.querySelector('a.zReHs')
                                || item.querySelector('a[href]');

                    // ── Snippet ────────────────────────────────────────────
                    // div.VwiC3b is confirmed present; div[data-sncf] and span.st
                    // are kept as fallbacks for older layouts.
                    const snippetEl = item.querySelector('div.VwiC3b')
                                   || item.querySelector('div[data-sncf]')
                                   || item.querySelector('span.st');

                    // ── Followers (LinkedIn rich-snippet) ──────────────────
                    // Stays unchanged — cite / span.VuuXrf inside div.N54PNb
                    // still carries follower counts for LinkedIn results.
                    let followersText = '';
                    const citeEls = item.querySelectorAll('cite, span.VuuXrf, span.st');
                    for (const el of citeEls) {{
                        const txt = el.textContent || '';
                        if (txt.toLowerCase().includes('followers')) {{
                            followersText = txt.trim();
                            break;
                        }}
                    }}

                    if (titleEl && linkEl) {{
                        let url = linkEl.href;
                        // Unwrap Google's /url?q=... redirect wrapper (rare but kept)
                        if (url.includes('/url?')) {{
                            try {{
                                const params = new URL(url).searchParams;
                                url = params.get('q') || params.get('url') || url;
                            }} catch(e) {{}}
                        }}

                        results.push({{
                            title: titleEl.textContent.trim(),
                            url: url,
                            snippet: snippetEl ? snippetEl.textContent.trim() : '',
                            followers: followersText,
                        }});
                    }}
                }}
                return results;
            }}
        """)
        
        logger.debug(f"Google: {len(results)} results for '{query[:50]}...'")
        return results
    
    async def _check_captcha(self) -> bool:
        """Check if Google is showing a CAPTCHA challenge."""
        try:
            content = await self.page.content()
            title = await self.page.title()
            combined = (content + title).lower()
            
            # Google-specific CAPTCHA indicators
            google_captcha = [
                "unusual traffic",
                "automated queries",
                "sorry/index",
                "recaptcha",
                "before you continue",
            ]
            all_indicators = config.CAPTCHA_INDICATORS + google_captcha
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
