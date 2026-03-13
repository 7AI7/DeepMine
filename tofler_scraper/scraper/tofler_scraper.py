"""Core Tofler scraping logic with Bing search and CAPTCHA detection"""
import asyncio
import random
import re
import json
from urllib.parse import quote
import config

class CaptchaDetectedError(Exception):
    """Raised when CAPTCHA is detected"""
    pass

class ToflerScraper:
    def __init__(self, logger):
        self.logger = logger
    
    async def random_delay(self, min_sec, max_sec):
        """Random delay to simulate human behavior"""
        await asyncio.sleep(random.uniform(min_sec, max_sec))
    
    async def human_scroll(self, page):
        """Simulate human-like scrolling with natural speed variations"""
        # Random number of scroll steps (2-4 for speed, still natural)
        num_scrolls = random.randint(2, 4)
        current_pos = 0
        
        for _ in range(num_scrolls):
            # Variable scroll distance (humans don't scroll uniformly)
            scroll_delta = random.randint(150, 450)
            current_pos += scroll_delta
            
            # Smooth scroll with easing
            await page.evaluate(f"""
                window.scrollTo({{
                    top: {current_pos},
                    behavior: 'smooth'
                }});
            """)
            
            # Variable wait time between scrolls (optimized)
            await asyncio.sleep(random.uniform(0.3, 1.0))
        
        # Sometimes scroll back up a bit (like humans do when reading)
        if random.random() > 0.6:
            back_scroll = random.randint(50, 200)
            await page.evaluate(f"window.scrollBy(0, -{back_scroll})")
            await asyncio.sleep(random.uniform(0.2, 0.5))
    
    async def random_mouse_movement(self, page):
        """Simulate random human mouse movements"""
        try:
            # Get viewport size
            viewport = page.viewport_size
            if not viewport:
                return
            
            width, height = viewport['width'], viewport['height']
            
            # Generate 2-4 random mouse positions
            num_movements = random.randint(2, 4)
            for _ in range(num_movements):
                x = random.randint(100, width - 100)
                y = random.randint(100, min(height - 100, 600))
                
                # Move mouse with slight delay
                await page.mouse.move(x, y, steps=random.randint(5, 15))
                await asyncio.sleep(random.uniform(0.1, 0.4))
        except Exception as e:
            self.logger.debug(f"Mouse movement skipped: {e}")
    

    
    async def simulate_reading(self, page, min_sec=1, max_sec=3):
        """Simulate time spent reading page content"""
        # Random reading time (optimized - just wait, mouse movement handled elsewhere)
        read_time = random.uniform(min_sec, max_sec)
        await asyncio.sleep(read_time)
    


    
    async def check_for_captcha(self, page):
        """Check if page contains CAPTCHA or blocking indicators
        
        Returns:
            bool: True if CAPTCHA detected
        """
        try:
            content = await page.content()
            content_lower = content.lower()
            
            for indicator in config.CAPTCHA_INDICATORS:
                if indicator.lower() in content_lower:
                    self.logger.warning(f"CAPTCHA/Block detected! Indicator: {indicator}")
                    return True
            
            # Check page title
            title = await page.title()
            if title:
                title_lower = title.lower()
                for indicator in config.CAPTCHA_INDICATORS:
                    if indicator.lower() in title_lower:
                        self.logger.warning(f"CAPTCHA in title: {title}")
                        return True
            
            return False
        except Exception as e:
            self.logger.debug(f"Error checking for CAPTCHA: {e}")
            return False
    
    async def search_via_bing(self, page, company_name):
        """Search for company on Bing and find Tofler link
        
        Strategy:
        1. Search Bing for "<company_name> tofler"
        2. Find all Tofler links by checking cite element (shows clean domain)
        3. Score them by company name match
        4. Click the best match to get the real URL after Bing redirect
        
        Args:
            page: Playwright page
            company_name: Company name to search
            
        Returns:
            str: Tofler company URL if found with 60%+ match, None otherwise
        """
        try:
            # Build Bing search query
            search_query = f"{company_name} tofler"
            search_url = f"https://www.bing.com/search?q={quote(search_query)}"
            
            self.logger.debug(f"Bing search: {search_query}")
            
            await page.goto(search_url, wait_until="domcontentloaded", timeout=config.BROWSER_TIMEOUT)
            await self.random_delay(*config.SEARCH_DELAY)
            
            # Check for CAPTCHA
            if await self.check_for_captcha(page):
                raise CaptchaDetectedError("CAPTCHA detected on Bing search")
            
            # Get organic results
            organic_results = await page.query_selector_all("ol#b_results > li.b_algo")
            if len(organic_results) == 0:
                organic_results = await page.query_selector_all("li.b_algo")
            
            self.logger.debug(f"Found {len(organic_results)} organic results")
            
            # First pass: Find all Tofler links and score them
            candidates = []
            
            for i, result in enumerate(organic_results[:10]):
                try:
                    h2_link = await result.query_selector("h2 a")
                    if not h2_link:
                        continue
                    
                    link_text = await h2_link.inner_text()
                    href = await h2_link.get_attribute("href")
                    
                    # Check cite element for clean domain
                    cite_elem = await result.query_selector("cite")
                    cite_text = await cite_elem.inner_text() if cite_elem else ""
                    
                    # Check if this is a Tofler link
                    is_tofler = "tofler" in href.lower() or "tofler.in" in cite_text.lower()
                    
                    if is_tofler:
                        # Calculate match score using link text
                        match_score = self._calculate_match_score(company_name, link_text, cite_text)
                        
                        self.logger.debug(
                            f"Result {i+1}: '{link_text[:50]}' | Score: {match_score:.0%} | cite: {cite_text[:40]}"
                        )
                        
                        candidates.append({
                            "index": i,
                            "text": link_text,
                            "score": match_score,
                            "result_elem": result,
                            "link_elem": h2_link
                        })
                except Exception as e:
                    self.logger.debug(f"Error parsing result {i}: {e}")
                    continue
            
            if not candidates:
                self.logger.warning(f"No Tofler link found for: {company_name}")
                return None
            
            # Find best match
            best = max(candidates, key=lambda x: x["score"])
            
            if best["score"] < 0.60:
                self.logger.warning(
                    f"Low match score! Searched: '{company_name}' | "
                    f"Best: '{best['text'][:40]}' ({best['score']:.0%})"
                )
                self._log_name_mismatch(company_name, best["text"], "pending_click", best["score"])
                return None
            
            # Click through to get real URL
            self.logger.info(f"Match found: '{best['text'][:40]}' (Score: {best['score']:.0%})")
            self.logger.debug(f"Clicking through Bing redirect to get real URL...")
            
            try:
                # Capture pages before click to detect popup
                old_pages = page.context.pages
                
                await best["link_elem"].click()
                
                # Wait a bit for potential popup or nav
                await asyncio.sleep(3)
                await page.wait_for_load_state("domcontentloaded", timeout=15000)
                
                new_pages = page.context.pages
                final_url = None
                
                if len(new_pages) > len(old_pages):
                    # Popup detected
                    popup = new_pages[-1]
                    try:
                        await popup.wait_for_load_state("domcontentloaded")
                        final_url = popup.url
                        self.logger.debug(f"Popup detected, URL: {final_url}")
                        await popup.close()
                    except Exception as e:
                        self.logger.warning(f"Error handling popup: {e}")
                else:
                    # No popup, check main page
                    final_url = page.url
                
                if final_url and "tofler.in" in final_url:
                    self.logger.debug(f"Got real Tofler URL: {final_url}")
                    return final_url
                else:
                    self.logger.warning(f"Redirect led to non-Tofler URL: {final_url}")
                    return None
            except Exception as e:
                self.logger.error(f"Error clicking through redirect: {e}")
                return None
            
        except Exception as e:
            self.logger.error(f"Error in Bing search: {e}")
            return None
    
    def _calculate_match_score(self, search_name, result_text, url):
        """Calculate match score between search name and result (0.0 to 1.0)"""
        # Normalize names
        def normalize(name):
            return name.lower().strip()\
                .replace("private limited", "")\
                .replace("pvt. ltd.", "")\
                .replace("pvt ltd", "")\
                .replace("pvt.ltd.", "")\
                .replace("limited", "")\
                .replace("ltd.", "")\
                .replace("ltd", "")\
                .replace("-", " ")\
                .replace("  ", " ")\
                .replace("financials", "")\
                .replace("company details", "")\
                .replace("tofler", "")\
                .replace("|", "")\
                .strip()
        
        search_norm = normalize(search_name)
        result_norm = normalize(result_text)
        
        # Also extract company name from URL slug
        # e.g., /j-s-auto-cast-foundry-india-private-limited/
        url_slug = ""
        if "/company/" in url or "-private-" in url:
            import re
            slug_match = re.search(r'tofler\.in/([^/]+)', url)
            if slug_match:
                url_slug = normalize(slug_match.group(1).replace("-", " "))
        
        # Split into words
        search_words = set(search_norm.split())
        result_words = set(result_norm.split())
        url_words = set(url_slug.split()) if url_slug else set()
        
        # Combine result and URL words for matching
        combined_words = result_words | url_words
        
        if len(search_words) == 0:
            return 0.0
        
        # Calculate score: how many search words appear in result/URL
        matching_words = search_words & combined_words
        score = len(matching_words) / len(search_words)
        
        return score
    
    def _log_name_mismatch(self, search_name, found_name, url, score):
        """Log company name mismatches for later review"""
        import os
        from datetime import datetime
        
        mismatch_log = os.path.join(config.LOGS_DIR, "name_mismatches.log")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(mismatch_log, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} | Score: {score:.0%} | Searched: {search_name} | Found: {found_name} | URL: {url}\n")
    
    async def scrape_people_section(self, page):
        """Scrape people from company page with pagination
        
        Returns:
            list: List of dicts with person info
        """
        people = []
        
        try:
            # Check for CAPTCHA first
            if await self.check_for_captcha(page):
                raise CaptchaDetectedError("CAPTCHA detected on company page")
            
            # Scroll to people section
            await page.wait_for_selector(config.SELECTORS["people_module"], timeout=10000)
            await self.human_scroll(page)
            
            page_num = 1
            while True:
                self.logger.debug(f"Scraping people page {page_num}")
                
                # Extract people from table
                rows = await page.query_selector_all(config.SELECTORS["people_table_row"])
                
                for row in rows:
                    try:
                        designation_elem = await row.query_selector(config.SELECTORS["person_designation"])
                        link_elem = await row.query_selector(config.SELECTORS["person_link"])
                        din_elem = await row.query_selector(config.SELECTORS["person_din"])
                        
                        if link_elem:
                            name = await link_elem.inner_text()
                            href = await link_elem.get_attribute("href")
                            designation = await designation_elem.inner_text() if designation_elem else ""
                            din = await din_elem.inner_text() if din_elem else ""
                            
                            person_url = "https://www.tofler.in" + href if href.startswith("/") else href
                            
                            people.append({
                                "name": name.strip(),
                                "designation": designation.strip(),
                                "din": din.strip(),
                                "url": person_url
                            })
                    except Exception as e:
                        self.logger.debug(f"Error extracting person row: {e}")
                        continue
                
                # Check for pagination in People section
                # Look for Next button that's not disabled
                try:
                    people_next_btn = await page.query_selector("#people-module button:has-text('Next'):not([disabled])")
                    if people_next_btn:
                        await people_next_btn.click()
                        await self.random_delay(*config.PAGINATION_DELAY)
                        page_num += 1
                    else:
                        break
                except:
                    break
            
            self.logger.debug(f"Found {len(people)} people total")
            return people
            
        except CaptchaDetectedError:
            raise
        except Exception as e:
            self.logger.error(f"Error scraping people section: {e}")
            return []
    
    async def scrape_person_profile(self, page, person_url):
        """Scrape directorship details from person profile with pagination
        
        Returns:
            list: List of directorship dicts
        """
        try:
            self.logger.debug(f"Scraping profile: {person_url}")
            await page.goto(person_url, wait_until="domcontentloaded", timeout=config.BROWSER_TIMEOUT)
            await self.random_delay(*config.PROFILE_DELAY)
            
            # Check for CAPTCHA
            if await self.check_for_captcha(page):
                raise CaptchaDetectedError("CAPTCHA detected on person profile")
            
            directorships = []
            
            # Try to extract from JavaScript variable first (preferred method)
            try:
                script_content = await page.content()
                match = re.search(r'const directorshipsData = (\[.*?\]);', script_content, re.DOTALL)
                
                if match:
                    json_str = match.group(1)
                    data = json.loads(json_str)
                    
                    for item in data:
                        directorships.append({
                            "name": item.get("name", ""),
                            "industry": item.get("industry", ""),
                            "status": item.get("status", ""),
                            "designation": item.get("designation", "")
                        })
                    
                    self.logger.debug(f"Extracted {len(directorships)} directorships from JSON")
                    return directorships
            except Exception as e:
                self.logger.debug(f"Could not extract from JSON, trying HTML: {e}")
            
            # Fallback: Extract from HTML table with pagination
            page_num = 1
            while True:
                try:
                    rows = await page.query_selector_all(config.SELECTORS["directorship_row"])
                    
                    for row in rows:
                        try:
                            name_elem = await row.query_selector(config.SELECTORS["company_name_link"])
                            industry_elem = await row.query_selector(config.SELECTORS["industry"])
                            status_elem = await row.query_selector(config.SELECTORS["status_badge"])
                            designation_elem = await row.query_selector(config.SELECTORS["designation"])
                            
                            if name_elem:
                                directorships.append({
                                    "name": (await name_elem.inner_text()).strip(),
                                    "industry": (await industry_elem.inner_text()).strip() if industry_elem else "",
                                    "status": (await status_elem.inner_text()).strip() if status_elem else "",
                                    "designation": (await designation_elem.inner_text()).strip() if designation_elem else ""
                                })
                        except Exception as e:
                            self.logger.debug(f"Error extracting directorship row: {e}")
                            continue
                    
                    # Check for pagination
                    next_btn = await page.query_selector(f"{config.SELECTORS['pagination_next']}:not([disabled])")
                    if next_btn:
                        await next_btn.click()
                        await self.random_delay(*config.PAGINATION_DELAY)
                        page_num += 1
                    else:
                        break
                        
                except Exception as e:
                    self.logger.debug(f"Error in directorship pagination: {e}")
                    break
            
            self.logger.debug(f"Extracted {len(directorships)} directorships from HTML")
            return directorships
            
        except CaptchaDetectedError:
            raise
        except Exception as e:
            self.logger.error(f"Error scraping person profile: {e}")
            return []
    
    async def scrape_company(self, page, company_id, company_name):
        """Main scraping function for a single company
        
        Returns:
            tuple: (status, people_data, captcha_detected)
            status: 'successful', 'not_found', 'failed', or 'captcha'
            people_data: List of dicts with person and directorship info
            captcha_detected: Boolean indicating if CAPTCHA was encountered
        """
        try:
            # Search for company via Bing
            company_url = await self.search_via_bing(page, company_name)
            
            if not company_url:
                return ('not_found', [], False)
            
            # Navigate to company page
            await page.goto(company_url, wait_until="domcontentloaded", timeout=config.BROWSER_TIMEOUT)
            
            # Human-like behavior: simulate reading
            await self.simulate_reading(page, 1, 2)
            await self.random_delay(*config.GENERAL_DELAY)
            
            # Check for CAPTCHA
            if await self.check_for_captcha(page):
                return ('captcha', [], True)
            
            # Human-like scrolling before interacting with content
            await self.human_scroll(page)
            
            # Scrape people section
            people = await self.scrape_people_section(page)
            
            if not people:
                self.logger.warning(f"No people found for company: {company_name}")
                return ('successful', [], False)
            
            # Scrape each person's profile
            people_data = []
            for person in people:
                try:
                    directorships = await self.scrape_person_profile(page, person["url"])
                    people_data.append({
                        "person": person,
                        "directorships": directorships
                    })
                except CaptchaDetectedError:
                    self.logger.warning(f"CAPTCHA on person profile: {person['name']}")
                    return ('captcha', people_data, True)
                
                await self.random_delay(*config.PROFILE_DELAY)
            
            return ('successful', people_data, False)
            
        except CaptchaDetectedError:
            return ('captcha', [], True)
        except Exception as e:
            self.logger.error(f"Error scraping company {company_id}: {e}")
            return ('failed', [], False)
