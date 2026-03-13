# site_crawler_new.py
# Purpose: Orchestrate Crawl4AI discovery + cleaning, blacklist/dedupe, LLM triage (15/batch), audit, and Flash JSONL build.

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
from typing import Optional
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
import httpx
from crawler.db import run as db_run
from crawler import settings
from datetime import datetime
import pandas as pd
import re
import time
from zhipuai import ZhipuAI
from bs4 import BeautifulSoup
from phonenumbers import PhoneNumberMatcher, Leniency
import extruct
from collections import defaultdict
from w3lib.html import get_base_url
from logging.handlers import RotatingFileHandler
from crawler.crawl4 import crawl_domain, PageRecord
from crawler.keywords_central import (contains_skip_term, is_mailtel_or_anchor,
                            contains_skip_prefix, UNIVERSAL_SKIP_TERMS, same_domain,
                              is_linkedin, contains_language_indicator)
from crawler.content_extractor import extract_page  # local post-strip fallback
from dotenv import load_dotenv; load_dotenv()

# Logging
log_path = settings.LOG_DIR / "site_crawler.log"
file_handler = RotatingFileHandler(str(log_path), maxBytes=settings.LOG_ROTATE_MB*1024*1024, backupCount=settings.LOG_BACKUPS, encoding="utf-8")
file_handler.setLevel(getattr(logging, settings.LOG_LEVEL, logging.DEBUG))
file_handler.setFormatter(logging.Formatter(" %(levelname)s | %(name)s | %(message)s"))
root_logger = logging.getLogger()

# Avoid duplicate handlers if reloaded
if not any(isinstance(h, RotatingFileHandler) and getattr(h, "baseFilename", "").endswith("site_crawler.log") for h in root_logger.handlers):
    root_logger.addHandler(file_handler)
log = logging.getLogger("site_crawler")
logging.basicConfig(level=logging.DEBUG, format=" %(levelname)s | %(name)s | %(message)s")
log.setLevel(logging.DEBUG)

# Output folders
OUT_DIR = settings.DATA_DIR / "jsonl"
AB_DIR = settings.DATA_DIR / "ab"
OUT_DIR.mkdir(parents=True, exist_ok=True)
AB_DIR.mkdir(parents=True, exist_ok=True)

# Triage settings
TRIAGE_MODEL = "glm-4-flash-250414"
_API_KEY1 = os.environ.get("ZHIPUAI_API_KEY", "")
_API_KEY2 = os.environ.get("ZHIPUAI_API_KEY1", "")
_API_KEY3 = os.environ.get("ZHIPUAI_API_KEY2", "")
# Build list of available keys (filter empty strings) — matches run_glm_on_cleaned.py
_API_KEYS = [k for k in [_API_KEY1, _API_KEY2, _API_KEY3] if k]
# GLM client instances (one per key)
_GLM_CLIENTS = [ZhipuAI(api_key=key) for key in _API_KEYS if key]
# Round-robin counter (module-level state)
_api_key_index = 0
_RATE_LIMIT = 100 
_RATE_WINDOW = 30  # seconds
_key_request_times = defaultdict(list)
_rate_limit_lock = None  # Will be initialized on first async call
_rate_limit_initialized = False

async def _get_next_glm_client_async() -> ZhipuAI:
    """Get next GLM client with async rate limit checking.
    Lock is held only for the counter check/increment — NOT during sleep,
    so parallel workers are never blocked waiting for rate limit reset.
    """
    global _api_key_index, _rate_limit_lock, _rate_limit_initialized

    # Initialize lock on first call (can't do at module level in async context)
    if not _rate_limit_initialized:
        _rate_limit_lock = asyncio.Lock()
        _rate_limit_initialized = True

    if not _GLM_CLIENTS:
        raise ValueError("No GLM API keys configured! Set ZHIPUAI_API_KEY and ZHIPUAI_API_KEY1")

    max_wait_cycles = 8
    for wait_cycle in range(max_wait_cycles):
        wait_time = None

        # Lock held only for the counter check+increment (microseconds)
        async with _rate_limit_lock:
            now = time.time()
            for _ in range(len(_GLM_CLIENTS)):
                client_idx = _api_key_index % len(_GLM_CLIENTS)
                client = _GLM_CLIENTS[client_idx]
                key = _API_KEYS[client_idx]
                _api_key_index += 1

                recent = [t for t in _key_request_times[key] if now - t < _RATE_WINDOW]
                _key_request_times[key] = recent

                if len(recent) < _RATE_LIMIT:
                    _key_request_times[key].append(now)
                    log.debug(f"Using GLM client #{client_idx + 1} ({len(recent) + 1}/{_RATE_LIMIT} req in window)")
                    return client  # ← lock released here via context manager

                log.warning(f"GLM client #{client_idx + 1} at rate limit, trying next...")

            # All keys at limit — calculate how long to wait (lock still held briefly)
            all_recent = []
            for k in _API_KEYS:
                all_recent.extend(_key_request_times[k])
            if all_recent:
                oldest = min(all_recent)
                wait_time = max(1, min(_RATE_WINDOW - (now - oldest) + 1, _RATE_WINDOW))
            else:
                wait_time = 1
        # ← Lock released BEFORE sleeping — other workers can proceed freely

        log.warning(f"All {len(_GLM_CLIENTS)} keys at limit. Waiting {wait_time:.1f}s (cycle {wait_cycle + 1}/{max_wait_cycles})")
        await asyncio.sleep(wait_time)  # ← no lock held here

    raise RuntimeError(
        f"Rate limit exceeded after {max_wait_cycles} cycles. "
        f"Keys: {len(_GLM_CLIENTS)}, Limit: {_RATE_LIMIT}/{_RATE_WINDOW}s. "
        f"Reduce --triage-parallel or add more API keys."
    )

if _GLM_CLIENTS:
    log.info(f"Loaded {len(_GLM_CLIENTS)} GLM API clients for round-robin rotation (rate limit: {_RATE_LIMIT} req/min)")
else:
    log.warning("No GLM API keys found! Set ZHIPUAI_API_KEY1 and ZHIPUAI_API_KEY environment variables")



# Tracking params to drop during canonicalization
TRACKING_KEYS = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "gclid", "fbclid"}

def _normalize_url_for_dedup(url: str) -> str:
    """
    Normalize URL for deduplication and matching.
    Handles: scheme differences, trailing slashes, query params, fragments.
    """
    url = url.strip()
    if not url:
        return ""
    
    parsed = urlparse(url.lower())
    netloc = parsed.netloc or parsed.path.split("/")[0]
    
    # Normalize path: remove trailing slash for non-root paths
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    
    # Remove tracking parameters
    if parsed.query:
        params = parse_qsl(parsed.query, keep_blank_values=True)
        params = [(k, v) for k, v in params if k.lower() not in TRACKING_KEYS]
        query = urlencode(params) if params else ""
    else:
        query = ""
    
    # Ignore fragments (#anchor)
    result = f"{netloc}{path}"
    if query:
        result += f"?{query}"
    
    return result

def _visible_text(html: str) -> str:
    """Extract visible text from HTML, removing scripts/styles."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)

def _extract_contacts_and_jsonld_from_homepage(html: str, page_url: str) -> dict:
    """
    Extract phones, emails, and JSON-LD metadata from homepage HTML.
    Returns dict with: phones_raw, emails_raw, jsonld_name, jsonld_address, jsonld_phones, jsonld_emails
    """
    out = {
        "phones_raw": [],
        "emails_raw": [],
        "jsonld_name": None,
        "jsonld_address": None,
        "jsonld_phones": [],
        "jsonld_emails": [],
    }
    
    if not html:
        return out
    
    # 1. Extract phones from visible text using phonenumbers library
    text = _visible_text(html)
    try:
        matches = PhoneNumberMatcher(text, "ZZ", leniency=Leniency.POSSIBLE)
        out["phones_raw"].extend([m.raw_string.strip() for m in matches if getattr(m, "raw_string", "").strip()])
    except Exception:
        pass
    
    # 2. Extract emails from visible text
    try:
        out["emails_raw"].extend([m.group(0).strip() for m in EMAIL_RE.finditer(text)])
    except Exception:
        pass
    
    # 3. Extract from mailto: and tel: links
    try:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if href.lower().startswith("mailto:"):
                email = href.split(":", 1)[1].split("?")[0].strip()
                if email:
                    out["emails_raw"].append(email)
            elif href.lower().startswith("tel:"):
                tel = href.split(":", 1)[1].strip()
                if tel:
                    out["phones_raw"].append(tel)
    except Exception:
        pass
    
    # 4. Extract JSON-LD Organization data
    try:
        data = extruct.extract(html, base_url=get_base_url(html, page_url), syntaxes=["json-ld"])
        for block in (data.get("json-ld") or []):
            if not isinstance(block, dict):
                continue
            types = block.get("@type")
            if not isinstance(types, list):
                types = [types]

            ORG_TYPES = {"Organization", "Corporation", "LocalBusiness", "EducationalOrganization"}
            if any(t in ORG_TYPES for t in types):
                org = block
                if not out["jsonld_name"]:  # Only set if not already captured
                    out["jsonld_name"] = org.get("name")
                out["jsonld_name"] = org.get("name") or out["jsonld_name"]
                
                # Extract address
                addr = org.get("address")
                if isinstance(addr, dict):
                    parts = [
                        addr.get("streetAddress", ""),
                        addr.get("addressLocality", ""),
                        addr.get("addressRegion", ""),
                        addr.get("postalCode", ""),
                        addr.get("addressCountry", "")
                    ]
                    flat = ", ".join([p for p in parts if p]).strip(", ")
                    out["jsonld_address"] = flat or out["jsonld_address"]

                elif isinstance(addr, str) and addr.strip():
                    out["jsonld_address"] = addr.strip()

                # Extract contacts
                tel = org.get("telephone")
                em = org.get("email")
                if tel:
                    out["jsonld_phones"].append(tel)
                if em:
                    out["jsonld_emails"].append(em)
                
                # Extract from ContactPoint
                cps = org.get("contactPoint") or org.get("contactPoints") or []
                if isinstance(cps, dict):
                    cps = [cps]
                for cp in cps:
                    if not isinstance(cp, dict):
                        continue
                    if cp.get("telephone"):
                        out["jsonld_phones"].append(cp["telephone"])
                    if cp.get("email"):
                        out["jsonld_emails"].append(cp["email"])
    except Exception:
        pass
    
    # Deduplicate
    def dedup(seq, key=lambda x: x):
        seen, outseq = set(), []
        for s in seq:
            if not s:
                continue
            k = key(s)
            if k not in seen:
                seen.add(k)
                outseq.append(s)
        return outseq
    
    out["phones_raw"] = dedup(out["phones_raw"])
    out["emails_raw"] = dedup(out["emails_raw"], key=lambda x: x.lower())
    out["jsonld_phones"] = dedup(out["jsonld_phones"])
    out["jsonld_emails"] = dedup(out["jsonld_emails"], key=lambda x: x.lower())
    
    return out


def _csv_join(values: list[str]) -> str:
    """Join list of strings with comma+space."""
    clean = [v.strip() for v in values if v and v.strip()]
    return ", ".join(clean)


def _out_dir_for(website: str, company_id: Optional[int], root_dir: Optional[Path] = None) -> Path:
    dom = urlparse(website).netloc
    folder = f"{company_id}_{dom}" if company_id is not None else dom
    out_dir = root_dir / folder # Use the provided root
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir

def extract_first_special_links(pages: list[PageRecord]) -> tuple[str | None, str | None]:
    """
    Extract FIRST brochure PDF and LinkedIn company link from all crawled pages.
    
    Returns: (brochure_pdf, linkedin_company) - Both may be None if not found
    """
    brochure_pdf = None
    linkedin_company = None
    
    for page in pages:
        if not brochure_pdf and hasattr(page, 'brochure_pdf') and page.brochure_pdf:
            brochure_pdf = page.brochure_pdf
            log.info(f"✓ Captured brochure PDF: {brochure_pdf}")
        
        if not linkedin_company and hasattr(page, 'linkedin_company') and page.linkedin_company:
            linkedin_company = page.linkedin_company
            log.info(f"✓ Captured LinkedIn company: {linkedin_company}")
        
        if brochure_pdf and linkedin_company:
            break
    
    return brochure_pdf, linkedin_company


def insert_special_links_to_db(company_id: int, brochure_pdf: str | None, linkedin_company: str | None) -> None:
    """
    Insert special links directly to database using parameterized queries.
    
    Database columns: companies.brochurelink, companies.linkedinpage
    """
    if not company_id:
        log.warning("⚠️  No company_id, skipping special links DB insertion")
        return
    
    if not brochure_pdf and not linkedin_company:
        log.debug(f"No special links for company {company_id}")
        return
    
    try:
        updates = []
        params = []
        
        if brochure_pdf:
            updates.append("brochurelink = %s")
            params.append(brochure_pdf)
        
        if linkedin_company:
            updates.append("linkedinpage = %s")
            params.append(linkedin_company)
        
        params.append(company_id)
        
        sql = f"UPDATE companies SET {', '.join(updates)} WHERE id = %s"
        db_run(sql, tuple(params))
        
        log.info(f"✅ Inserted special links for company {company_id}: "
                f"brochure={'✓' if brochure_pdf else '✗'}, "
                f"linkedin={'✓' if linkedin_company else '✗'}")
    
    except Exception as e:
        log.error(f"❌ Failed to insert special links for company {company_id}: {e}")
        log_failure_to_excel(
            company_id=company_id,
            domain="",
            stage="special_links_db_insert",
            error=str(e),
            urls=([brochure_pdf] if brochure_pdf else []) + ([linkedin_company] if linkedin_company else [])
        )

async def crawl_company_domain(website: str, company_id: int, root_dir: Path | None = None ):
    """
    Orchestrate discovery + filtering + triage for one website.
    Writes artifacts under data/ab/{company_id}_{host} and returns (pages, kept_urls).
    """
    dom = urlparse(website).netloc
    folder_name = f"{company_id}_{dom}" if company_id is not None else dom
    out_dir = _out_dir_for(website, company_id, root_dir=root_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Homepage-only crawl to collect anchors
    home_only = await crawl_domain(website, allowed_root_prefixes=None, depth_limit=0)
    home_html, home_url = None, None
    for rec in home_only:
        if getattr(rec, "depth", 0) == 0:
            home_html = getattr(rec, "raw_html", None)
            home_url = rec.url
            break

    contacts_meta = {}
    phones_csv = ""
    emails_csv = ""

    if home_html:
        contactsMeta = _extract_contacts_and_jsonld_from_homepage(home_html, home_url or website)
        # Dedupe and join with comma+space
        all_phones = contacts_meta.get("phones_raw", []) + contacts_meta.get("jsonld_phones", [])
        all_emails = contacts_meta.get("emails_raw", []) + contacts_meta.get("jsonld_emails", [])
        phones_csv = _csv_join(all_phones)
        emails_csv = _csv_join(all_emails)
        contacts_file = out_dir / 'homepage_contacts.json'
        contacts_to_save = {
            "phones_raw": contactsMeta.get('phones_raw', []),
            "emails_raw": contactsMeta.get('emails_raw', []),
            "jsonld_name": contactsMeta.get('jsonld_name'),
            "jsonld_address": contactsMeta.get('jsonld_address'),
            "jsonld_phones": contactsMeta.get('jsonld_phones', []),
            "jsonld_emails": contactsMeta.get('jsonld_emails', [])
        }
        contacts_file.write_text(json.dumps(contacts_to_save, ensure_ascii=False, indent=2), encoding='utf-8')
        log.info(f'Saved homepage contacts to {contacts_file}')

        log.info(f"Extracted {len(all_phones)} phones, {len(all_emails)} emails from homepage")
    home_links: list[dict] = []
    for rec in home_only:
        if getattr(rec, "depth", 0) == 0:
            # anchor_links were populated in crawl4.PageRecord; each item: {"url","anchor"}
            for li in (getattr(rec, "anchor_links", []) or []):
                if li.get("url"):
                    home_links.append(li)

    # 2) Triage homepage links (URL-only; anchors kept for roots) and persist anchors
    if home_links:
        kept_urls, _ = await triage_links_llm(urlparse(website).netloc, [li["url"] for li in home_links], company_id)
        kept_items = [li for li in home_links if li["url"] in set(kept_urls)]
        (out_dir / "homepage_anchors.json").write_text(
            json.dumps(kept_items, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        allowed_roots = _select_root_prefixes(kept_items)
    else:
        kept_items = []
        allowed_roots = []

    # 3) Full crawl constrained by allowed_roots
    pages = await crawl_domain(website, allowed_root_prefixes=None or None)

    brochure_pdf, linkedin_company = extract_first_special_links(pages)
    
    # Save special_links.json (like homepage_contacts.json) for later parse/save
    special = {"brochure_pdf": brochure_pdf, "linkedin_company": linkedin_company}
    (out_dir / "special_links.json").write_text(
        json.dumps(special, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("Saved special_links.json: brochure=%s linkedin=%s",
             bool(brochure_pdf), bool(linkedin_company))
    
    # Insert to database immediately (before triage, before extraction)
    if company_id:
        insert_special_links_to_db(company_id, brochure_pdf, linkedin_company)
    else:
        log.warning("⚠️  No company_id provided, skipping special links DB insertion")
        # 4) Aggregate ALL links (UNFILTERED - before any prefilter)
    all_links_unfiltered: list[str] = []
    for r in pages:
        # r.internal_links are raw same-domain hrefs - collect ALL before filtering
        for link in r.internal_links:
            if isinstance(link, dict):
                url = link.get("url") or link.get("href")
            else:
                url = str(link)
            if url:
                all_links_unfiltered.append(url)
    
    log.info(f"Discovered {len(all_links_unfiltered)} total internal links (before any filtering)")
    
    # Save TRULY unfiltered links (required file)
    (out_dir / "links_unfiltered.txt").write_text("\n".join(all_links_unfiltered), encoding="utf-8")
    
    # 4b) First pass prefilter: per-page rules
    all_links_filtered1: list[str] = []
    for r in pages:
        all_links_filtered1.extend(prefilter_links(r.url, r.internal_links))
    
    # 4c) Second pass prefilter: global canonicalization/dedupe
    all_links_filtered = prefilter_links(website, all_links_filtered1)
    
    log.info(f"After prefiltering: {len(all_links_filtered)} links remain (removed {len(all_links_unfiltered) - len(all_links_filtered)})")
    
    # Save filtered links (required file)
    (out_dir / "links_filtered.txt").write_text("\n".join(all_links_filtered), encoding="utf-8")
    
    # 5) Hard cap before triage to prevent runaway API calls
    max_for_triage = getattr(settings, "MAX_LINKS_FOR_TRIAGE", 700)
    if len(all_links_filtered) > max_for_triage:
        log.warning(f"Capping links for triage: {len(all_links_filtered)} -> {max_for_triage}")
        all_links_filtered = all_links_filtered[:max_for_triage]
    
    # 5) Triage (using FILTERED links)
    kept, stats = await triage_links_llm(dom, all_links_filtered, company_id)
    kept = list(dict.fromkeys(kept))
    
    log.info(f"Triage kept: {len(kept)} links out of {len(all_links_filtered)} filtered")
    
    # Filter pages to only include triage-kept URLs
    # Filter pages to only include triage-kept URLs with normalization
    # Create normalized map for matching (handles trailing slashes, query params, etc)
    kept_normalized = {_normalize_url_for_dedup(u): u for u in kept}
    
    kept_pages = []
    unmatched_kept = []
    for kept_url in kept:
        for page in pages:
            if _normalize_url_for_dedup(page.url) == _normalize_url_for_dedup(kept_url):
                if page not in kept_pages:  # Avoid duplicates
                    kept_pages.append(page)
                break
        else:
            unmatched_kept.append(kept_url)
    
    log.info(f"Matched {len(kept_pages)}/{len(kept)} triage-kept URLs from crawled pages")
    if unmatched_kept:
        log.warning(f"Could not find crawled content for {len(unmatched_kept)} triage-kept URLs: {unmatched_kept[:3]}")
    
    # Save kept URLs (required file)
    (out_dir / "triage_kept.txt").write_text("\n".join(kept), encoding="utf-8")


    # Save cleaned pages (only triage-kept URLs, deduplicated)
    kept_canonical = {_normalize_url_for_dedup(u): u for u in kept}

    # Filter fetched pages to those in triage-kept list using canonical matching
    selected_pages = []
    for page in pages:
        page_canonical = _normalize_url_for_dedup(page.url)
        if page_canonical in kept_canonical:
            selected_pages.append(page)
            
    # Write only the selected pages, deduplicated
    seen_urls = set()
    with (out_dir / "cleaned_pages.ndjson").open("w", encoding="utf-8") as f:
        
        for rec in selected_pages:
            norm_url = _normalize_url_for_dedup(rec.url)
            if norm_url in seen_urls:
                log.debug(f"Skip duplicate: {rec.url}")
                continue
            seen_urls.add(norm_url)
            doc = {
                "url": rec.url,
                "depth": rec.depth,
                "text": rec.cleaned_text
            }
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    log.info(f"Saved {len(seen_urls)} triage-kept pages to cleaned_pages.ndjson "
            f"(out of {len(pages)} fetched, {len(kept)} triage_kept)")

    return kept_pages, kept


def canonicalize(u: str) -> str:
    """Normalize URL for dedupe: lowercase host, strip tracking query keys, remove trailing slash."""
    p = urlparse(u)
    host = p.netloc.lower()
    path = p.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    # Keep non-tracking query params
    if p.query:
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if k.lower() not in TRACKING_KEYS]
        query = urlencode(q, doseq=True)
    else:
        query = ""

    return urlunparse((p.scheme, host, path, "", query, ""))

NON_HTML_SUFFIXES = (".jpg",".jpeg",".png",".gif",".webp",".svg",".ico",
                     ".mp4",".mp3",".avi",".mov",".webm",
                     ".zip",".rar",".7z",".gz",".tar",
                     ".doc",".docx",".xls",".xlsx",".ppt",".pptx")


def prefilter_links(base_url: str, links: list[dict] | list[str]) -> list[str]:
    """Drop mailto/tel/#/js, enforce same-domain (+LinkedIn capture), apply skip terms, canonicalize, and dedupe."""
    out: list[str] = []
    base = canonicalize(base_url)

    def _href_and_anchor(item) -> tuple[str, str]:
        if isinstance(item, dict):
            return item.get("url") or item.get("href") or "", (item.get("anchor") or "").strip()
        return str(item), ""
    
    for it in links:
        href_raw, _anc = _href_and_anchor(it)
        if not href_raw:
            continue
        if is_mailtel_or_anchor(href_raw):
            continue
        absu = urljoin(base, href_raw)
        cu = canonicalize(absu)
        path_lower = urlparse(cu).path.lower()

        # Keep PDFs (for brochure link capture), drop other binaries
        if not path_lower.endswith(".pdf"):
            if path_lower.endswith(NON_HTML_SUFFIXES):
                continue

        # Language and subtree skips
        if contains_language_indicator(cu):
            continue
        if contains_skip_term(path_lower) or contains_skip_prefix(path_lower):
            continue

        # Same-domain or LinkedIn capture
        if not same_domain(cu, base):
            if settings.ALLOW_LINKEDIN_CAPTURE and is_linkedin(cu):
                out.append(cu)
            continue
        out.append(cu)

    # Dedupe, preserve order
    seen, uniq = set(), []
    for u in out:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)

    # Path-prefix grouping: cap URLs per path pattern to avoid link explosion
    # e.g. /products/tag/*, /products/category/* each capped at MAX_LINKS_PER_PATH_PREFIX
    max_per_prefix = getattr(settings, "MAX_LINKS_PER_PATH_PREFIX", 10)
    prefix_counts: dict[str, int] = {}
    capped: list[str] = []
    for u in uniq:
        parts = [seg for seg in urlparse(u).path.strip("/").split("/") if seg]
        prefix = "/".join(parts[:2]) if len(parts) >= 2 else "/".join(parts[:1]) if parts else ""
        count = prefix_counts.get(prefix, 0)
        if count < max_per_prefix:
            capped.append(u)
            prefix_counts[prefix] = count + 1
    return capped

def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

def _select_root_prefixes(kept_items: list[dict]) -> list[str]:
    """
    kept_items: [{"url": "...", "anchor": "..."}] from triage on homepage links
    Returns sorted, unique top-level path prefixes like ["/products", "/services"].
    """
    roots = set()
    for it in kept_items:
        p = urlparse(it.get("url", "")).path
        parts = [seg for seg in p.split("/") if seg]
        if parts:
            roots.add("/" + parts[0])
    return sorted(roots)

def _triage_payload_urls(batch: list[str], domain: str) -> dict:
    arr = [{"url": u} for u in batch]
    sys = "\n".join([
        "Task: Decide if each first-party URL is relevant to manufacturer data extraction.",
        "Return a JSON object with key 'items' as an array. Each item: {url:string, keep:boolean}. No prose.",
        "Keep: products, services, infrastructure, facilities, applications, sectors, contact, about, management, clients, certifications.",
        "Discard: hiring, careers, news, blog, events, investor, CSR, policy, login, search, subscribe.",
    ])
    usr = json.dumps(arr, ensure_ascii=False)
    return {
        "model": TRIAGE_MODEL,
        "temperature": 0,
        "top_p": 0.1,
        "messages": [
            {"role": "system", "content": sys},
            {"role": "user", "content": usr},
        ],
    }

async def _triage_one_batch(batch_urls: list[str], domain: str, company_id: int | None,
                            stats: dict, kept_out: list) -> None:
    """Send one batch to GLM with retries. Appends kept URLs to kept_out."""
    payload = _triage_payload_urls(batch_urls, domain)
    backoff = 0.5
    for attempt in range(3):
        try:
            glm_client = await _get_next_glm_client_async()
            response = await asyncio.to_thread(
                glm_client.chat.completions.create,
                model=payload["model"],
                messages=payload["messages"],
                temperature=payload["temperature"],
                top_p=payload.get("top_p", 1.0),
                response_format={"type": "json_object"}
            )
            content = response.choices[0].message.content
            raw = (content or "").strip()

            arr = None
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and isinstance(parsed.get("items"), list):
                    arr = parsed["items"]
                elif isinstance(parsed, list):
                    arr = parsed
            except Exception:
                arr = None

            if arr is None:
                start, end = raw.find("["), raw.rfind("]")
                if start != -1 and end != -1 and end > start:
                    try:
                        arr = json.loads(raw[start:end+1])
                    except Exception:
                        arr = None

            if not isinstance(arr, list):
                raise ValueError("triage: expected list")

            for item in arr:
                if isinstance(item, str):
                    kept_out.append(item)
                elif isinstance(item, dict):
                    if item.get("keep") and item.get("url"):
                        kept_out.append(item["url"])
            return  # success

        except Exception as e:
            stats["last_error"] = str(e)
            error_str = str(e).lower()
            if "rate" in error_str or "limit" in error_str or "429" in error_str:
                log.warning("Rate limit hit (attempt %s/3): domain=%s", attempt + 1, domain)
                await asyncio.sleep(2)
                if attempt < 2:
                    continue
            log.error("Triage error (attempt %s/3): domain=%s err=%s", attempt + 1, domain, repr(e))
            if attempt == 2:
                kept_out.extend(batch_urls)  # keep-all fallback
                stats["failed_batches"] += 1
                log_failure_to_excel(company_id, domain, batch_urls, "triage_fail", str(e))
                return
            await asyncio.sleep(backoff + random.random() * 0.2)
            backoff *= 2


async def triage_links_llm(domain: str, links: list[str], company_id: int | None = None) -> tuple[list[str], dict]:
    """
    Triage links using GLM-4-Flash with concurrent batch processing.
    Sends up to len(_GLM_CLIENTS) batches simultaneously (one per API key).
    Returns (kept_urls, stats).
    """
    kept = []
    stats = {"total_batches": 0, "failed_batches": 0, "last_error": ""}
    n_keys = max(len(_GLM_CLIENTS), 1)
    log.info(f"Triage start: company_id={company_id} domain={domain} links={len(links)} "
             f"batch_size={settings.TRIAGE_BATCH_SIZE} parallel_keys={n_keys}")

    batches = list(chunk(links, settings.TRIAGE_BATCH_SIZE))
    stats["total_batches"] = len(batches)

    # Process batches in groups of n_keys (concurrent per group)
    for i in range(0, len(batches), n_keys):
        group = batches[i:i + n_keys]
        group_kept = [[] for _ in group]
        tasks = [
            _triage_one_batch(
                [b if isinstance(b, str) else (b.get("url", "") if isinstance(b, dict) else "") for b in batch],
                domain, company_id, stats, group_kept[j]
            )
            for j, batch in enumerate(group)
        ]
        await asyncio.gather(*tasks)
        for sub in group_kept:
            kept.extend(sub)

    kept = list(dict.fromkeys(kept))  # dedupe, preserve order
    log.info(f"Triage done: company_id={company_id} kept={len(kept)}/{len(links)} "
             f"batches={stats['total_batches']} failed={stats['failed_batches']}")
    return kept, stats

FAIL_LOG = settings.FAILURES_XLSX

def log_failure_to_excel(company_id: int | None, domain: str, urls: list[str], stage: str, err: str):
    row = {
        "timestamp": datetime.utcnow().isoformat(timespec="seconds"),
        "company_id": company_id,
        "domain": domain,
        "stage": stage,
        "error": err,
        "urls": ";".join(urls[:50])  # truncate for readability
    }
    # Append or create
    if FAIL_LOG.exists():
        try:
            df = pd.read_excel(FAIL_LOG)
        except Exception:
            df = pd.DataFrame()
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        df = pd.DataFrame([row])
    df.to_excel(FAIL_LOG, index=False)