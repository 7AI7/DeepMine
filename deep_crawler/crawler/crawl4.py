# crawl4_adapter.py
# Purpose: Crawl4AI discovery + cleaning with footer policy, robots/politeness, UA/proxy rotation, and robust logging.

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from typing import List
from urllib import robotparser
import trafilatura
from html_to_markdown import convert as html_to_md
import re
from urllib.parse import urljoin, urlparse
from inspect import signature
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, BrowserConfig  # top-level imports per current docs
from . import settings
from .keywords_central import( same_domain, contains_skip_term,
    is_mailtel_or_anchor, contains_skip_prefix,
    contains_keywords_word_boundary, contains_language_indicator,
    UNIVERSAL_SKIP_TERMS)

log = logging.getLogger("crawl4_adapter")
log.setLevel(logging.DEBUG)
NON_HTML_SUFFIXES = (".jpg",".jpeg",".png",".gif",".webp",".svg",".ico",
                     ".mp4",".mp3",".avi",".mov",".webm",
                     ".zip",".rar",".7z",".gz",".tar",
                     ".doc",".docx",".xls",".xlsx",".ppt",".pptx")
# Per-domain politeness state
_domain_last_hit = defaultdict(float)
_DOMAIN_SEMAPHORES: dict[str, asyncio.Semaphore] = {}

# Concurrency caps and retries
MAX_CONCURRENCY_PER_DOMAIN = getattr(settings, "MAX_CONCURRENCY_PER_DOMAIN", 2)
MAX_RETRIES = getattr(settings, "MAX_RETRIES", 2)
BASE_BACKOFF = 0.5  # seconds

@dataclass
class PageRecord:
    url: str
    depth: int
    internal_links: List[str]
    cleaned_text: str
    cleaned_kind: str  # "fit_markdown" | "cleaned_html" | "raw_html"
    anchor_links: list[dict] | None = None
    brochure_pdf: str | None = None  # ONLY store first found
    linkedin_company: str | None = None  # ONLY store first found
    raw_html: str | None = None

def is_brochure_pdf(url: str) -> bool:
    """Check if URL is a brochure PDF (contains 'brochure' AND ends with '.pdf')."""
    url_lower = url.lower()
    return 'brochure' in url_lower and url_lower.endswith('.pdf')

def is_linkedin_company_link(url: str) -> bool:
    """
    Check if URL is a LinkedIn company page.
    Accepts ANY link that contains /company/ in the path.
    """
    from urllib.parse import urlparse
    
    url_lower = url.lower()
    parsed = urlparse(url_lower)
    
    # Check domain is linkedin.com
    if 'linkedin.com' not in parsed.netloc:
        return False
    
    # Path must start with /company/ and have at least a company name
    path = parsed.path.rstrip('/')
    
    if path.startswith('/company/'):
        # Must have at least the company name after /company/
        path_after = path[len('/company/'):]
        if path_after:  # Has company name (anything after /company/)
            return True
    
    return False

def _excluded_tags_for_depth(depth: int) -> list[str]:
    # Depth 0 (homepage): keep footer; remove header/nav
    return [] if depth == 0 else ["header", "nav", "footer"]

def _pick_ua() -> str | None:
    pool = getattr(settings, "USER_AGENT_POOL", [])
    return random.choice(pool) if pool else None

def _pick_proxy() -> str | None:
    pool = getattr(settings, "PROXY_POOL", [])
    return pool[0] if pool else None 

def _sem_for(domain: str) -> asyncio.Semaphore:
    if domain not in _DOMAIN_SEMAPHORES:
        _DOMAIN_SEMAPHORES[domain] = asyncio.Semaphore(MAX_CONCURRENCY_PER_DOMAIN)
    return _DOMAIN_SEMAPHORES[domain]

def _jitter_delay(base: float = 0.2) -> float:
    return base + random.random() * base

async def _polite_sleep(domain: str, min_gap: float | None = None):
    gap_target = min_gap if min_gap is not None else getattr(settings, "PER_DOMAIN_DELAY_SEC", 1.0)
    now = time.time()
    last = _domain_last_hit.get(domain, 0.0)
    gap = now - last
    need = max(0.0, gap_target - gap)
    if need > 0:
        await asyncio.sleep(need)
    _domain_last_hit[domain] = time.time()

def _extract_internal_links(out) -> list[str]:
    """
    Return a list of href strings for internal links, handling both dict and object shapes:
    - dict: out.links -> {"internal": [str|dict|obj], "external": [...]}
    - object: out.links.internal -> [obj] where obj may have .href or .url
    """
    links = getattr(out, "links", None)
    if not links:
        return []
    if isinstance(links, dict):
        items = links.get("internal") or []
    else:
        items = getattr(links, "internal", []) or []
    hrefs: list[str] = []
    for item in items:
        if isinstance(item, str):
            href = item
        elif isinstance(item, dict):
            href = item.get("href") or item.get("url")
        else:
            href = getattr(item, "href", None) or getattr(item, "url", None)
        if href:
            hrefs.append(href)
    return hrefs

def _canon_url(u: str) -> str:
    """Canonicalize URL for visited/frontier dedup: lowercase, normalize path, drop fragments."""
    p = urlparse(u.strip())
    netloc = p.netloc.lower()
    path = p.path or "/"
    
    # Collapse index files to directory root
    if path.endswith(("/index.html", "/index.htm")):
        path = path[:-len("/index.html")] or "/"
    
    # Remove trailing slash (except root)
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    
    return f"{netloc}{path}".lower()

@lru_cache(maxsize=4096)
def _robots_for_host(host: str, scheme: str = "https") -> robotparser.RobotFileParser:
    rp = robotparser.RobotFileParser()
    rp.set_url(f"{scheme}://{host}/robots.txt")
    try:
        rp.read()
    except Exception as e:
        log.debug("Robots fetch failed (proceeding): %s", e)
    return rp

async def _fetch_page(crawler, url: str, depth: int, seed_url: str):
    domain = urlparse(url).netloc
    seed_host = urlparse(seed_url).netloc
    await asyncio.sleep(_jitter_delay())
    await _polite_sleep(domain)
    ua, proxy = _pick_ua() or None, _pick_proxy() or None
    bc_kwargs = {"headless": True}

    if ua:
        bc_kwargs["user_agent"] = ua
    if proxy:
        # Use proxy_config dict instead of deprecated proxy string
        bc_kwargs["proxy_config"] = {
            "server": proxy  # proxy is already full URL with credentials
        }
    cfg_params = set(signature(CrawlerRunConfig).parameters.keys())
    rc_kwargs = {}
    
    # JavaScript-heavy site support
    rc_kwargs["wait_until"] = "domcontentloaded"
    rc_kwargs["page_timeout"] = 100000
    rc_kwargs["delay_before_return_html"] = 1.0
    rc_kwargs["remove_overlay_elements"] = True

    if "excluded_tags" in cfg_params:
        rc_kwargs["excluded_tags"] = _excluded_tags_for_depth(depth)
    if "exclude_external_links" in cfg_params:
        rc_kwargs["exclude_external_links"] = True

    run_cfg = CrawlerRunConfig(**rc_kwargs)
    backoff = BASE_BACKOFF

    for attempt in range(MAX_RETRIES + 1):
        try:
            async with _sem_for(domain):
                out = await crawler.arun(
                    url=url,
                    config=run_cfg,
                    browser_config=BrowserConfig(**bc_kwargs),
                )
            if out and out.success:
                return out
            log.warning("Crawl4AI returned unsuccessful result: url=%s depth=%s", url, depth)
            return None
        except Exception as e:
            log.error("Fetch error (attempt %s/%s): url=%s err=%s", attempt + 1, MAX_RETRIES + 1, url, e)
            if attempt >= MAX_RETRIES:
                return None
            await asyncio.sleep(backoff + random.random() * 0.2)
            backoff *= 2

async def crawl_domain(seed_url: str, allowed_root_prefixes: list[str] | None = None, depth_limit: int | None = None) -> List[PageRecord]:
    """BFS up to settings.MAX_DEPTH and settings.MAX_PAGES_PER_DOMAIN, yielding PageRecord with pre-cleaned text."""
    recs: List[PageRecord] = []
    visited = {seed_url}
    frontier: list[tuple[str, int]] = [(seed_url, 0)]
    max_pages = getattr(settings, "MAX_PAGES_PER_DOMAIN", 30)
    max_depth = getattr(settings, "MAX_DEPTH", 2)
    log.info("Starting crawl: seed=%s depth<=%s pages<=%s", seed_url, max_depth, max_pages)
    log.info(f"Config: max_depth={max_depth} max_pages={max_pages} allowed_roots={allowed_root_prefixes or 'None'} depth_limit={depth_limit}")
    
    async with AsyncWebCrawler() as crawler:
        while frontier and len(recs) < max_pages:
            remaining = max_pages - len(recs)
            batch_size = min(10, remaining)  # Never fetch more than the remaining budget
            batch = frontier[:batch_size]
            frontier = frontier[batch_size:]
            
            tasks = [_fetch_page(crawler, url, depth, seed_url) for url, depth in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for (url, depth), out in zip(batch, results):
                if isinstance(out, Exception):
                    log.error(f"Batch fetch error: url={url} err={out}")
                    continue
                if not out:
                    log.warning(f"No result for url={url}")
                    continue
                
                # Check depth_limit here before processing
                if depth_limit is not None and depth > depth_limit:
                    continue
                
                raw_links = _extract_internal_links(out)
                max_internal = getattr(settings, "MAX_INTERNAL_LINKS_PER_PAGE", 500)
                internal = []
                for h in raw_links:
                    if len(recs) >= max_pages:
                        break
                    if len(internal) >= max_internal:
                        log.warning("Capped internal_links at %d for %s (raw=%d)", max_internal, url, len(raw_links))
                        break

                    if is_mailtel_or_anchor(h):
                        continue
                    
                    absu = urljoin(out.url, h)
                    # PageRecord.internal_links remain strictly first‑party; LinkedIn is allowed at enqueue step only
                    if not same_domain(absu, seed_url):
                        continue

                    path_lower = urlparse(absu).path.lower()
                    if path_lower.endswith(".pdf") or path_lower.endswith(NON_HTML_SUFFIXES):
                        # do not enqueue binaries; PDFs are retained via aggregated list in the orchestrator
                        continue

                    if contains_skip_term(path_lower) or contains_skip_prefix(path_lower):
                        continue

                    internal.append(absu)

                cleaned = None
                kind = None
                is_homepage = (depth == 0)
                if is_homepage:
                    # For the homepage, preserve the complete page (footer included)
                    html_content = getattr(out, "cleaned_html", None) or getattr(out, "html", None)
                    if html_content:
                        cleaned = html_to_md(html_content)
                        if cleaned and cleaned.strip():
                            kind = "html_to_markdown_home"
                    # If no HTML available, leave cleaned/kind as-is to fall through to post-processing
                else:
                    # TIER 1: Try Crawl4AI markdown first (fastest, 0ms overhead)
                    if getattr(out, "markdown", None):
                        if hasattr(out.markdown, "fit_markdown"):
                            cleaned = out.markdown.fit_markdown
                            kind = "crawl4ai_fit"
                        elif isinstance(out.markdown, str) and out.markdown.strip():
                            cleaned = out.markdown
                            kind = "crawl4ai_markdown"
                    
                    # TIER 2: If empty or too short, try Trafilatura (intelligent extraction)
                    # Removes boilerplate, keeps main content, preserves structure
                    if not cleaned or len(cleaned.strip()) < 100:
                        try:
                            html_content = getattr(out, "html", None)
                            if html_content:
                                trafilatura_result = trafilatura.extract(
                                    html_content,
                                    output_format='markdown',
                                    include_links=False,
                                    include_images=False,  # Skip images to save tokens
                                    include_tables=True,  # Keep tables (specs/features)
                                    include_comments=False,
                                    favor_recall = True,
                                    no_fallback=False,  # Allow fallback extraction
                                    # Don't drop team/mgmt sections
                                )
                                if trafilatura_result and len(trafilatura_result.strip()) > 500:
                                    cleaned = trafilatura_result
                                    kind = "trafilatura"
                                    log.info(f"✓ Trafilatura extracted {len(cleaned)} chars from {url}")
                        except Exception as e:
                            log.debug(f"Trafilatura extraction failed for {url}: {e}")

                    # TIER 3: If still empty, try html-to-markdown (preserves ALL structure)
                    if not cleaned or len(cleaned.strip()) < 500:
                        try:
                            html_content = getattr(out, "cleaned_html", None) or getattr(out, "html", None)
                            if html_content:
                                cleaned = html_to_md(html_content)
                                if cleaned and len(cleaned.strip()) > 80:
                                    kind = "html_to_markdown"
                                    log.info(f"✓ html-to-markdown extracted {len(cleaned)} chars from {url}")
                        except Exception as e:
                            log.debug(f"html-to-markdown extraction failed for {url}: {e}")
                    
                    
                    # TIER 4: Use content_extractor.py fallback (BeautifulSoup + policy)
                    # This is our existing 3-layer fallback with trafilatura/justext
                    if not cleaned or len(cleaned.strip()) < 50:
                        try:
                            from .content_extractor import extract_page
                            html_content = getattr(out, "html", None)
                            if html_content:
                                extracted = extract_page(html_content, url, is_homepage)
                                if extracted and len(extracted.strip()) > 50:
                                    cleaned = extracted
                                    kind = "content_extractor"
                                    log.info(f"✓ content_extractor extracted {len(cleaned)} chars from {url}")
                        except Exception as e:
                            log.debug(f"content_extractor failed for {url}: {e}")
                    
                    # TIER 5: Last resort - Convert cleaned_html with BeautifulSoup
                    # Extract text while preserving heading structure
                    if not cleaned or len(cleaned.strip()) < 20:
                        try:
                            from bs4 import BeautifulSoup
                            html_content = getattr(out, "cleaned_html", None) or getattr(out, "html", None)
                            if html_content:
                                soup = BeautifulSoup(html_content, "html.parser")
                                
                                # Remove scripts, styles, nav, ads
                                for tag in soup(["script", "style", "noscript", "nav", "header"]):
                                    tag.decompose()
                                if not is_homepage:
                                    for tag in soup.find_all("footer"):
                                        tag.decompose()
                                
                                # Extract with structure markers
                                parts = []
                                for elem in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'li', 'td', 'th']):
                                    text = elem.get_text(strip=True)
                                    if not text:
                                        continue
                                    
                                    # Add structure markers for headings
                                    tag_name = elem.name
                                    if tag_name == 'h1':
                                        parts.append(f"# {text}")
                                    elif tag_name == 'h2':
                                        parts.append(f"## {text}")
                                    elif tag_name == 'h3':
                                        parts.append(f"### {text}")
                                    elif tag_name in ['h4', 'h5', 'h6']:
                                        parts.append(f"#### {text}")
                                    else:
                                        parts.append(text)
                                
                                if parts:
                                    cleaned = "\n\n".join(parts)
                                    kind = "beautifulsoup_structured"
                                    log.info(f"✓ BeautifulSoup structured extraction: {len(cleaned)} chars from {url}")
                        except Exception as e:
                            log.debug(f"BeautifulSoup structured extraction failed for {url}: {e}")
                    
                    # ABSOLUTE LAST RESORT: Mark as empty (don't send to LLM)
                    if not cleaned or len(cleaned.strip()) < 20:
                        cleaned = ""
                        kind = "empty"
                        log.warning(f"⚠ Empty content after all extraction tiers: {url}")
                
                # ═══════════════════════════════════════════════════════════════
                # POST-PROCESSING: Clean up extracted markdown
                # ═══════════════════════════════════════════════════════════════
                
                if cleaned and kind not in ("empty",):
                    # Remove multiple blank lines (3+ → 2)
                    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
                    
                    # Remove trailing whitespace per line
                    cleaned = '\n'.join(line.rstrip() for line in cleaned.split('\n'))
                    
                    # Remove markdown horizontal rules (visual noise)
                    cleaned = re.sub(r'^[\-\*\_]{3,}$', '', cleaned, flags=re.MULTILINE)
                    
                    # Normalize heading spacing
                    cleaned = re.sub(r'\n(#{1,6})', r'\n\n\1', cleaned)  # Blank before heading
                    cleaned = re.sub(r'(#{1,6}[^\n]+)\n([^\n#])', r'\1\n\n\2', cleaned)  # Blank after heading
                    
                    # Ensure ends with single newline
                    cleaned = cleaned.strip() + '\n' if cleaned.strip() else ""
                
                # Log extraction results
                if cleaned and len(cleaned.strip()) > 20:
                    log.info(f"✓ Final extraction: {kind} | {len(cleaned)} chars | {url}")
                else:
                    log.warning(f"⚠ Minimal/empty content: {kind} | {len(cleaned) if cleaned else 0} chars | {url}")
                

                anchor_links = [
                    {
                        "url": urljoin(out.url, (li.get("href") or "").strip()),
                        "anchor": (li.get("text") or "").strip(),
                    }
                    for li in (getattr(out, "links", []) or [])
                    if isinstance(li, dict) and li.get("href")
                ]
                log.debug(f"Anchor_links count for {out.url}: {len(anchor_links) if anchor_links else 0}")


                brochure_pdf = None
                linkedin_company = None
                
                if anchor_links:
                    for link_data in anchor_links:
                        link_url = link_data.get("url", "")
                        
                        # Capture FIRST brochure PDF found on this page
                        if not brochure_pdf and is_brochure_pdf(link_url):
                            brochure_pdf = link_url
                            log.info("📄 Found brochure PDF: %s", link_url)
                        
                        # Capture FIRST LinkedIn company link found on this page
                        if not linkedin_company and is_linkedin_company_link(link_url):
                            linkedin_company = link_url
                            log.info("🔗 Found LinkedIn company: %s", link_url)
                        
                        # Exit early if both found
                        if brochure_pdf and linkedin_company:
                            break
                
                raw_html = getattr(out, "html", None) if depth == 0 else None
                rec = PageRecord(
                    url=out.url,
                    depth=depth,
                    internal_links=internal,
                    cleaned_text=cleaned,
                    cleaned_kind=kind,
                    anchor_links=anchor_links,
                    brochure_pdf=brochure_pdf,
                    linkedin_company=linkedin_company,
                    raw_html=raw_html
                )
                recs.append(rec)
                log.info("Content source: url=%s depth=%s kind=%s chars=%s", out.url, depth, kind, len(cleaned))
                log.info("Fetched: url=%s depth=%s internal_links=%s", out.url, depth, len(internal))
                
                if depth >= max_depth:
                    continue

                # Enqueue same-domain children not yet visited
                for h in _extract_internal_links(out):
                    child = urljoin(out.url, h)
                    if not same_domain(child, seed_url):
                        continue
                    
                    path_lower = urlparse(child).path.lower()

                    if allowed_root_prefixes:
                        child_path = urlparse(child).path or "/"
                        if not any(child_path.startswith(rp) for rp in allowed_root_prefixes):
                            continue

                    if path_lower.endswith(".pdf") or path_lower.endswith(NON_HTML_SUFFIXES):                   
                        continue
                        
                    if contains_skip_term(path_lower) or contains_skip_prefix(path_lower):
                        continue

                    if contains_language_indicator(path_lower):                    
                        continue

                    canon = _canon_url(child)
                    if canon in visited:               
                        continue

                    visited.add(canon)
                
                    if len(recs) + len(frontier) >= max_pages * 2:
                        # Don't let frontier grow unboundedly on massive sites
                        break
                    frontier.append((child, depth + 1))
            
        log.info("Crawl complete: seed=%s pages=%s", seed_url, len(recs))
        return recs
#python orchestrator.py --excel-range Book2.xlsx 100001 100001 --triage-parallel 1

