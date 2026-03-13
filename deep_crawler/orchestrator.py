from __future__ import annotations
import argparse
import json
from crawler.db import run as db_run
import random
import time
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse
from logging.handlers import RotatingFileHandler
from typing import Optional, List, Dict, Any
import asyncio, logging
from zhipuai import ZhipuAI
import os
from crawler.glm_client import GLMClient
from crawler.gemini_batch_manager import GeminiBatchManager
from crawler.page_utils import concatenate_pages
from crawler.crawl4 import crawl_domain, PageRecord  # uses allowed_root_prefixes + depth_limit [added] [file:34]
from crawler.site_crawler_new import (
    prefilter_links,
    triage_links_llm,
)  # reuses your latest, URL-only triage path [file:33]
from crawler.excel_utils import (
    fetch_sites_from_excel,
    save_results_to_excel,
    normalize_url_to_homepage
)
# Gemini (Batch + Standard caches)
from crawler.gemini_standard import get_client as get_std_client, create_cache as create_std_cache  # PAGE_META anchor-ready [file:35]
from collections import defaultdict
_host_sem: dict[str, asyncio.Semaphore] = defaultdict(lambda: asyncio.Semaphore(2))  # at most 2 per host

# DB (optional save path)
try:
    # Provide a typed gateway if available in your latest db_handler.py
    from crawler.db_handler import DBHandler # expects methods: _cap_and_map_sql, upsert_company, insert_* [file:36]
except Exception:
    DBHandler = None
LOG = logging.getLogger("orchestrator")
# Settings/logs
try:
    from crawler import settings  # expects LOG_DIR, TRIAGE_BATCH_SIZE, etc. [file:33]
except Exception:
    class settings:
        LOG_DIR = Path("logs")
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        TRIAGE_BATCH_SIZE = 15

# Local failure log to CSV to avoid Excel concurrency issues; site_crawler already logs triage failures inline
FAIL_DIR = Path("data") / "failures"
FAIL_DIR.mkdir(parents=True, exist_ok=True)

def _iter_artifact_dirs(root: Path):
    for p in sorted(root.iterdir()):
        if not p.is_dir():
            continue
        name = p.name
        if "_" not in name:
            continue
        cid_str, host = name.split("_", 1)
        try:
            cid = int(cid_str)
        except Exception:
            continue
        yield cid, host, p

def _load_homepage_contacts(out_dir: Path) -> dict:
                    p = out_dir / "homepage_contacts.json"
                    if p.exists():
                        try:
                            return json.loads(p.read_text(encoding="utf-8"))
                        except Exception:
                            return {}
                    return {}

async def triage_parallel(sites: list[tuple[int, str]], parallel: int = 5, root: Path = Path("data/ab")) -> list[tuple[int, str]]:
    """
    Run crawl+prefilter+triage+clean for many sites concurrently with a queue of N workers.
    Returns the list of (company_id, website) that completed successfully.
    """
    import asyncio
    from collections import defaultdict
    from urllib.parse import urlparse
    from crawler.site_crawler_new import crawl_company_domain  # local import to avoid circular deps

    q: asyncio.Queue[tuple[int, str]] = asyncio.Queue()
    for item in sites:
        await q.put(item)

    done: list[tuple[int, str]] = []
    err_count = 0

    # Host-scoped backpressure: at most 2 sites per host in flight
    _host_sem = defaultdict(lambda: asyncio.Semaphore(2))

    async def worker(wid: int):
        nonlocal err_count
        log = logging.getLogger("orchestrator")
        while not q.empty():
            cid, site = await q.get()
            try:
                host = urlparse(site).netloc
                async with _host_sem[host]:
                    log.info("W%02d start triage id=%s host=%s", wid, cid, host)
                    await crawl_company_domain(site, cid, root_dir=root)  # writes triage_kept.txt + cleaned_pages.ndjson
                    done.append((cid, site))
                    log.info("W%02d done triage id=%s host=%s", wid, cid, host)
            except Exception as e:
                err_count += 1
                log.exception("W%02d triage failed id=%s site=%s err=%s", wid, cid, site, repr(e))
            finally:
                q.task_done()

    n = max(1, int(parallel))
    workers = [asyncio.create_task(worker(i + 1)) for i in range(n)]
    await q.join()
    for w in workers:
        w.cancel()
    logging.getLogger("orchestrator").info("Parallel triage finished: ok=%s failed=%s", len(done), err_count)
    return done

async def retry_pipeline_from_excel(excel_path: str, output_root: str, parallel: int):
    """
    Reads failed sites from an Excel file and reruns the full pipeline,
    placing new artifacts in the specified output_root directory.
    """
    try:
        df = pd.read_excel(excel_path)
        LOG.info(f"Loaded {len(df)} records from {excel_path}")
    except FileNotFoundError:
        LOG.error(f"Failure file not found: {excel_path}")
        return
    except Exception as e:
        LOG.error(f"Failed to read Excel file {excel_path}: {e}")
        return

    if 'company_id' not in df.columns or 'host_url' not in df.columns:
        LOG.error("Excel file must contain 'company_id' and 'host_url' columns.")
        return

    sites_to_retry = [(int(row['company_id']), f"https://{row['host_url']}") for _, row in df.iterrows()]

    if not sites_to_retry:
        LOG.info("No sites to retry.")
        return

    output_root_path = Path(output_root)
    output_root_path.mkdir(parents=True, exist_ok=True)
    LOG.info(f"Starting retry pipeline for {len(sites_to_retry)} sites. Output will be in: {output_root_path}")

    # Run the parallel triage and cleaning, pointing to the new output directory
    await triage_parallel(sites_to_retry, parallel=parallel, root=output_root_path)

    LOG.info("Retry pipeline complete. Artifacts are ready for batch submission.")

def _artifacts_available(out_dir: Path) -> bool:
    return (out_dir / "triage_kept.txt").exists() and (out_dir / "cleaned_pages.ndjson").exists()

def gen_content_with_retry(client, messages, max_retries=4, base=0.6):
    """
    GLM-specific retry logic for API calls
    """
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="glm-4-flash",
                messages=messages,
                temperature=0.0,
                max_tokens=4096
            )
            return response
        except Exception as e:
            error_str = str(e).lower()
            # Retry on server errors or rate limits
            if attempt == max_retries - 1:
                raise
            if "rate" in error_str or "429" in error_str or "503" in error_str:
                sleep_s = base * (2 ** attempt) + random.uniform(0, 0.4)
                time.sleep(sleep_s)
            else:
                # Client error, don't retry
                raise

def log_failure_to_csv(company_id: Optional[int], domain: str, stage: str, err: str, urls: Optional[List[str]] = None):
    ts = datetime.now(timezone.utc).isoformat(timespec="minutes")
    row = {
        "timestamp": ts,
        "company_id": company_id,
        "domain": domain,
        "stage": stage,
        "error": err,
        "urls": ";".join(urls or [])[:8000]
    }
    path = FAIL_DIR / f"failures_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv"
    new = f'{row["timestamp"]},{row["company_id"]},{row["domain"]},{row["stage"]},"{row["error"].replace('"', "'")}","{row["urls"].replace('"', "'")}"\n'
    if not path.exists():
        path.write_text("timestamp,company_id,domain,stage,error,urls\n", encoding="utf-8")
    with path.open("a", encoding="utf-8") as f:
        f.write(new)

# Consistent logger
LOG = logging.getLogger("orchestrator")
def _setup_logging():
    LOG.setLevel(logging.INFO)
    handler = RotatingFileHandler(str(settings.LOG_DIR / "orchestrator.log"), maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
    stream = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s", "%H:%M:%S")
    handler.setFormatter(fmt)
    stream.setFormatter(fmt)
    LOG.handlers = [handler, stream]

def _out_dir_for(website: str, company_id: Optional[int], root: Path) -> Path:
    dom = urlparse(website).netloc
    folder = f"{company_id}_{dom}" if company_id is not None else dom
    # The root is now parameterized instead of hardcoded
    out_dir = root / folder
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir

def _atomic_write(path: Path, data: str):
    tmp = path.with_suffix(path.suffix + ".part")
    tmp.write_text(data, encoding="utf-8")
    tmp.replace(path) 
    
def fetch_sites_from_db(start_id: int, count: int) -> list[tuple[int, str]]:
    """
    Fetch (company_id, website) for a window starting at start_id.
    Skips NULL/blank websites and returns ascending by id.
    """
    rows = db_run(
        "SELECT id, website "
        "FROM companies "
        "WHERE website IS NOT NULL AND website <> '' AND id >= %s "
        "ORDER BY id ASC LIMIT %s",
        (start_id, count),
        fetch=True,
    )
    out: list[tuple[int, str]] = []
    for r in rows or []:
        cid = r[0] if isinstance(r, (list, tuple)) else r.get("id")
        site = r[1] if isinstance(r, (list, tuple)) else r.get("website")
        if cid and site:
            out.append((int(cid), str(site)))
    return out


async def triage_only_company(website: str, company_id: int, root: Path = Path("data/ab")) -> tuple[list, list]:
    # Produces triage_kept.txt and cleaned_pages.ndjson
    return await crawl_with_anchor_roots(website, company_id, root)

def _infer_website_from_artifacts(company_id: int) -> Optional[str]:
    """
    Locate data/ab/{companyId}_{host} and reconstruct https://{host}
    for preview-only runs without --url.
    """
    base = Path("data") / "ab"
    if not base.exists():
        return None
    for p in base.iterdir():
        if not p.is_dir():
            continue
        name = p.name
        if name.startswith(f"{company_id}_") and "_" in name:
            host = name.split("_", 1)[1]
            if host:
                return f"https://{host}"
    return None

def _select_root_prefixes(kept_items: List[Dict[str, str]]) -> List[str]:
    # kept_items: [{"url","anchor"}]; returns ["/products","/services"] etc.
    roots = set()
    for it in kept_items:
        p = urlparse(it.get("url", "")).path
        parts = [seg for seg in p.split("/") if seg]
        if parts:
            roots.add("/" + parts[0])
    return sorted(roots)

async def triage_range_and_optional_batch(
        start_id: int,
        count: int,
        do_batch: bool,
        ttl_hours: int = 72,
        parallel: int = 5,
    ):
        """
        UPDATED: Accumulate-then-batch pattern for GLM + Gemini.

        Flow:
        1) Fetch COUNT sites starting at START_ID from DB
        2) Triage/clean them in parallel (N workers) â†’ artifacts
        3) Load pages for each company, count pages
        4) Route: â‰¤10 pages â†’ GLM queue, >10 pages â†’ Gemini queue
        5) Process GLM queue immediately (one-by-one with GLMClient)
        6) Accumulate Gemini queue
        7) Submit ONE Gemini batch job for all accumulated companies
        8) Poll for batch completion
        9) Distribute Gemini results
        """
        # Step 1: Fetch sites from DB
        sites = fetch_sites_from_db(start_id, count)
        if not sites:
            LOG.info("No sites found for range start=%s count=%s", start_id, count)
            return

        # Step 2: Triage all companies in parallel
        LOG.info("Starting triage for %d companies (parallel=%d)", len(sites), parallel)
        default_root = Path("data/ab")
        triaged = await triage_parallel(sites, parallel=parallel, root=default_root)

        if not triaged:
            LOG.warning("No companies triaged successfully")
            return

        LOG.info("Triage complete: %d/%d companies succeeded", len(triaged), len(sites))

        # Step 3: Load pages and route companies
        glm_companies = []  # (cid, domain, pages, output_dir)
        gemini_companies = []  # (cid, domain, pages, output_dir)

        for cid, site in triaged:
            try:
                out_dir = _out_dir_for(site, cid, root=default_root)
                domain = urlparse(site).netloc

                # Load pages from cleaned_pages.ndjson
                cleaned_file = out_dir / "cleaned_pages.ndjson"
                if not cleaned_file.exists():
                    LOG.warning("Company %d: cleaned_pages.ndjson missing, skipping", cid)
                    log_failure_to_csv(
                        company_id=cid,
                        domain=domain,
                        stage="routing_missing_cleaned",
                        err="cleaned_pages.ndjson not found",
                        urls=[]
                    )
                    continue

                # Load and count pages
                pages = []
                with cleaned_file.open("r", encoding="utf-8") as f:
                    for line in f:
                        if not line.strip():
                            continue
                        try:
                            doc = json.loads(line)
                            pages.append({
                                "url": doc.get("url", ""),
                                "text": doc.get("text", "")
                            })
                        except json.JSONDecodeError:
                            continue

                page_count = len(pages)

                if page_count == 0:
                    LOG.warning("Company %d: 0 pages after loading, skipping", cid)
                    log_failure_to_csv(
                        company_id=cid,
                        domain=domain,
                        stage="routing_zero_pages",
                        err="No valid pages in cleaned_pages.ndjson",
                        urls=[]
                    )
                    continue

                # Routing decision
                if page_count <= 10:
                    routing_decision = "GLM-4-Flash"
                    glm_companies.append((cid, domain, pages, out_dir))
                else:
                    routing_decision = "Gemini Batch"
                    gemini_companies.append((cid, domain, pages, out_dir))

                # Save routing_info.json
                routing_info = {
                    "page_count": page_count,
                    "routing_decision": routing_decision,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                (out_dir / "routing_info.json").write_text(
                    json.dumps(routing_info, ensure_ascii=False, indent=2),
                    encoding="utf-8"
                )

                LOG.info(
                    "Company %d: %d pages â†’ %s",
                    cid,
                    page_count,
                    routing_decision
                )

            except Exception as e:
                LOG.exception("Routing failed for company %d: %s", cid, e)
                log_failure_to_csv(
                    company_id=cid,
                    domain=urlparse(site).netloc,
                    stage="routing_error",
                    err=str(e),
                    urls=[]
                )

        LOG.info(
            "Routing complete: GLM=%d companies, Gemini=%d companies",
            len(glm_companies),
            len(gemini_companies)
        )

        # Step 4: Process GLM companies immediately (one-by-one)
        if glm_companies:
            LOG.info("Processing %d GLM companies...", len(glm_companies))
            glm_client = GLMClient()

            for cid, domain, pages, out_dir in glm_companies:
                try:
                    LOG.info("GLM extraction: company_id=%d pages=%d", cid, len(pages))
                    result = glm_client.extract(
                        pages=pages,
                        company_id=cid,
                        domain=domain,
                        output_dir=out_dir
                    )
                    LOG.info("GLM extraction complete: company_id=%d", cid)
                    meta = _load_homepage_contacts(out_dir)
                    result.setdefault("company", {})

                    # Name precedence: LLM first, then JSON-LD
                    if not result["company"].get("name") and meta.get("jsonld_name"):
                        result["company"]["name"] = meta["jsonld_name"]

                    # Address precedence: JSON-LD first, then keep LLM
                    if meta.get("jsonld_address"):
                        result["company"]["address"] = meta["jsonld_address"]

                    # Contacts: merge homepage HTML + JSON-LD + LLM, dedupe
                    phones_list = []
                    emails_list = []
                    phones_list.extend(meta.get("phones_raw", []))
                    phones_list.extend(meta.get("jsonld_phones", []))
                    emails_list.extend(meta.get("emails_raw", []))
                    emails_list.extend(meta.get("jsonld_emails", []))

                    # Add LLM contacts if present
                    if result["company"].get("phone"):
                        phones_list.append(result["company"]["phone"])
                    if result["company"].get("email"):
                        emails_list.append(result["company"]["email"])

                    # Dedupe function
                    def _dedup(seq, key=lambda x: x):
                        seen, out = set(), []
                        for s in [t.strip() for t in seq if t]:
                            k = key(s)
                            if k not in seen:
                                seen.add(k)
                                out.append(s)
                        return out

                    # Final deduped, comma+space joined
                    phones_csv = ", ".join(_dedup(phones_list))
                    emails_csv = ", ".join(_dedup(emails_list, key=lambda x: x.lower()))

                    if phones_csv:
                        result["company"]["phone"] = phones_csv
                    if emails_csv:
                        result["company"]["email"] = emails_csv
                    # Optionally save to DB if do_batch flag is set
                    if do_batch:  # Reuse flag for "full processing"
                        save_to_db(f"https://{domain}", cid, result)

                except Exception as e:
                    LOG.exception("GLM extraction failed: company_id=%d", cid)
                    log_failure_to_csv(
                        company_id=cid,
                        domain=domain,
                        stage="glm_extraction_error",
                        err=str(e),
                        urls=[]
                    )

        # Step 5: Accumulate and batch process Gemini companies
        if gemini_companies:
            LOG.info("Batching %d Gemini companies...", len(gemini_companies))
            try:
                batch_manager = GeminiBatchManager()
                # Submit batch (returns immediately, doesn't wait for completion)
                submission_results = await batch_manager.batch_extract_companies(gemini_companies)

                # Log submission status
                for res in submission_results:
                    if res.get('success'):
                        LOG.info(f"âœ… Batch submitted for company {res['company_id']} ({res['domain']})")
                        LOG.info(f"   Batch ID: {res['batch_id']}")
                        LOG.info(f"   Check status later: python check_batch_status.py --batch-dir {res['batch_dir']}")
                    else:
                        LOG.error(f"âŒ Failed to submit batch for company {res['company_id']}: {res.get('error')}")

                # Return empty results (actual results will come from check_batch_status.py later)
                results = []

                LOG.info("Gemini batch complete: %d/%d companies succeeded", 
                         len(results), len(gemini_companies))

                meta = _load_homepage_contacts(out_dir)
                result.setdefault("company", {})

                # Name precedence: LLM first, then JSON-LD
                if not result["company"].get("name") and meta.get("jsonld_name"):
                    result["company"]["name"] = meta["jsonld_name"]

                # Address precedence: JSON-LD first, then keep LLM
                if meta.get("jsonld_address"):
                    result["company"]["address"] = meta["jsonld_address"]

                # Contacts: merge homepage HTML + JSON-LD + LLM, dedupe
                phones_list = []
                emails_list = []
                phones_list.extend(meta.get("phones_raw", []))
                phones_list.extend(meta.get("jsonld_phones", []))
                emails_list.extend(meta.get("emails_raw", []))
                emails_list.extend(meta.get("jsonld_emails", []))

                # Add LLM contacts if present
                if result["company"].get("phone"):
                    phones_list.append(result["company"]["phone"])
                if result["company"].get("email"):
                    emails_list.append(result["company"]["email"])

                # Dedupe function
                def _dedup(seq, key=lambda x: x):
                    seen, out = set(), []
                    for s in [t.strip() for t in seq if t]:
                        k = key(s)
                        if k not in seen:
                            seen.add(k)
                            out.append(s)
                    return out

                # Final deduped, comma+space joined
                phones_csv = ", ".join(_dedup(phones_list))
                emails_csv = ", ".join(_dedup(emails_list, key=lambda x: x.lower()))

                if phones_csv:
                    result["company"]["phone"] = phones_csv
                if emails_csv:
                    result["company"]["email"] = emails_csv
                # Optionally save to DB
                if do_batch:
                    for cid, result in results.items():
                        # Find domain for this cid
                        domain = next(
                            (d for c, d, _, _ in gemini_companies if c == cid),
                            "unknown"
                        )
                        try:
                            save_to_db(f"https://{domain}", cid, result)
                        except Exception as e:
                            LOG.exception("DB save failed for company_id=%d", cid)
                            log_failure_to_csv(
                                company_id=cid,
                                domain=domain,
                                stage="db_save_error",
                                err=str(e),
                                urls=[]
                            )

            except Exception as e:
                LOG.exception("Gemini batch processing failed: %s", e)
                for cid, domain, _, _ in gemini_companies:
                    log_failure_to_csv(
                        company_id=cid,
                        domain=domain,
                        stage="gemini_batch_error",
                        err=str(e),
                        urls=[]
                    )

        LOG.info("Triage and extraction complete for range start_id=%d count=%d", 
                 start_id, count)
        
async def excel_range_extraction(
    excel_path: str,
    start_id: str,
    end_id: str,
    parallel: int = 5,
) -> None:
    """
    Excel-based extraction pipeline.
    
    Flow:
    1) Fetch sites from Excel Sheet3 (S0001 to S0050)
    2) Normalize URLs to homepage
    3) Assign IDs: 100001, 100002, etc.
    4) Triage in parallel â†’ artifacts
    5) Route: â‰¤10 pages â†’ GLM, >10 pages â†’ Gemini
    6) Process & save to extraction_result.json
    7) Bulk save to Excel sheets
    """
    excel_path_obj = Path(excel_path)
    if not excel_path_obj.exists():
        LOG.error(f"Excel file not found: {excel_path}")
        return
    
    # Step 1: Fetch from Excel
    LOG.info(f"Fetching sites from Excel: {excel_path} (IDs {start_id} to {end_id})")
    try:
        sites_data = fetch_sites_from_excel(
            excel_path=excel_path,
            sheet_name="Sheet1",
            start_id=start_id,
            end_id=end_id
        )
    except Exception as e:
        LOG.exception(f"Failed to fetch from Excel: {e}")
        return
    
    if not sites_data:
        LOG.info("No sites fetched from Excel")
        return
    
    LOG.info(f"Fetched {len(sites_data)} sites from Excel")
    
    # Convert to pipeline format
    sites = [(company_id, normalized_url) for company_id, normalized_url, _ in sites_data]
    
    # Step 2: Triage (REUSE existing function)
    LOG.info(f"Starting triage for {len(sites)} companies (parallel={parallel})")
    visited_seeds = set()
    default_root = Path("data/ab")
    triaged = await triage_parallel(sites, parallel=parallel, root=default_root)
    visited_seeds = set()
    if not triaged:
        LOG.warning("No companies triaged successfully")
        return
    
    LOG.info(f"Triage complete: {len(triaged)}/{len(sites)} companies succeeded")
    
    # Step 3: Route companies (SAME AS EXISTING)
    glm_companies = []
    gemini_companies = []
    
    for cid, site in triaged:
        try:
            normalized_seed = normalize_url_to_homepage(site)
            if normalized_seed in visited_seeds:
                LOG.info(f"Skip duplicate crawl for {normalized_seed}")
                continue
            visited_seeds.add(normalized_seed)
            out_dir = _out_dir_for(site, cid, root=default_root)
            domain = urlparse(site).netloc
            cleaned_file = out_dir / "cleaned_pages.ndjson"
            
            if not cleaned_file.exists():
                LOG.warning(f"Company {cid}: cleaned_pages.ndjson missing")
                continue
            file_size = cleaned_file.stat().st_size
            if file_size == 0:
                LOG.warning(f"Company {cid}: cleaned_pages.ndjson is empty (0 bytes), skipping")
                log_failure_to_csv(
                    company_id=cid,
                    domain=domain,
                    stage="empty_cleaned_pages",
                    err="cleaned_pages.ndjson has 0 bytes",
                    urls=[]
                )
                continue
            
            # Load pages
            pages = []
            with cleaned_file.open("r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        try:
                            doc = json.loads(line)
                            pages.append({
                                "url": doc.get("url", ""),
                                "text": doc.get("text", "")
                            })
                        except json.JSONDecodeError:
                            continue
            
            page_count = len(pages)
            if page_count == 0:
                continue
            
            # Route by page count
            if page_count <= 10:
                glm_companies.append((cid, domain, pages, out_dir))
            else:
                gemini_companies.append((cid, domain, pages, out_dir))
            
            # Save routing info
            (out_dir / "routing_info.json").write_text(
                json.dumps({
                    "page_count": page_count,
                    "routing_decision": "GLM-4-Flash" if page_count <= 10 else "Gemini Batch",
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            
        except Exception as e:
            LOG.exception(f"Routing failed for company {cid}: {e}")
    
    # Step 4: Process GLM companies
    glm_results = {}
    if glm_companies:
        LOG.info(f"Processing {len(glm_companies)} GLM companies...")
        glm_client = GLMClient()
        for cid, domain, pages, out_dir in glm_companies:
            try:
                result = glm_client.extract(
                    pages=pages,
                    company_id=cid,
                    domain=domain,
                    output_dir=out_dir
                )
                
                # Save to JSON
                (out_dir / "extraction_result.json").write_text(
                    json.dumps(result, ensure_ascii=False, indent=2),
                    encoding="utf-8"
                )
                glm_results[cid] = result
                
            except Exception as e:
                LOG.exception(f"GLM extraction failed: company_id={cid}")
    
    # Step 5: Batch Gemini companies
    gemini_results = {}
    if gemini_companies:
        LOG.info(f"Batching {len(gemini_companies)} Gemini companies...")
        try:
            batch_manager = GeminiBatchManager()
            results = await batch_manager.batch_extract_companies(gemini_companies)
            
            # Save each to JSON
            for cid, result in results.items():
                out_dir = next((d for c, _, _, d in gemini_companies if c == cid), None)
                if out_dir:
                    (out_dir / "extraction_result.json").write_text(
                        json.dumps(result, ensure_ascii=False, indent=2),
                        encoding="utf-8"
                    )
                    gemini_results[cid] = result
                    
        except Exception as e:
            LOG.exception(f"Gemini batch processing failed: {e}")
    
    # Step 6: Collect all results and add normalized URLs
    all_results = {**glm_results, **gemini_results}
    
    for cid, result in all_results.items():
        if "company" not in result:
            result["company"] = {}
        
        # Get normalized URL
        normalized_url = next(
            (norm_url for c_id, norm_url, _ in sites_data if c_id == cid),
            None
        )
        if normalized_url:
            result["company"]["website"] = normalized_url
    
    # Step 7: Bulk save to Excel
    if all_results:
        LOG.info(f"Saving {len(all_results)} results to Excel: {excel_path}")
        try:
            save_results_to_excel(excel_path=excel_path, results=all_results)
            LOG.info("Excel save complete!")
        except Exception as e:
            LOG.exception(f"Failed to save to Excel: {e}")
    
    LOG.info(
        f"Excel extraction complete: Total={len(sites)} "
        f"GLM={len(glm_results)} Gemini={len(gemini_results)}"
    )

async def crawl_with_anchor_roots(website: str, company_id: Optional[int], root: Path) -> tuple[List[PageRecord], List[str]]:
    """
    Stage 1: crawl homepage (depth 0) to collect anchors
    Stage 2: triage homepage URLs to compute allowed_roots
    Stage 3: full crawl constrained by allowed_roots
    Stage 4: aggregate links and triage to kept URLs
    """

    dom = urlparse(website).netloc
    out_dir = _out_dir_for(website, company_id, root)

    # Stage 1: homepage-only
    home_only = await crawl_domain(website, depth_limit=0)
    homepage_links: List[Dict[str, str]] = []
    for rec in home_only:
        if getattr(rec, "depth", 0) == 0:
            for li in (getattr(rec, "anchor_links", []) or []):
                if li.get("url"):
                    homepage_links.append(li)

    # Stage 2: triage homepage URLs (URL-only payload) to compute roots
    kept_items: List[Dict[str, str]] = []
    if homepage_links:
        kept_home_urls, _ = await triage_links_llm(dom, [li["url"] for li in homepage_links], company_id)
        kept_items = [li for li in homepage_links if li["url"] in set(kept_home_urls)]
        (out_dir / "homepage_anchors.json").write_text(json.dumps(kept_items, ensure_ascii=False, indent=2), encoding="utf-8")

    allowed_roots = _select_root_prefixes(kept_items) if kept_items else []

    # Stage 3: full crawl
    pages = await crawl_domain(website, allowed_root_prefixes=allowed_roots or None)

    # Stage 4: aggregate internal links and triage
    all_links: List[str] = []
    for r in pages:
        all_links.extend(prefilter_links(r.url, r.internal_links))
    all_links = prefilter_links(website, all_links)  # dedupe + canonicalize again

    out_dir.mkdir(parents=True, exist_ok=True)
    # Persist link audits
    _atomic_write(out_dir / "internal_links_filtered.json", json.dumps(all_links, ensure_ascii=False, indent=2))
    _atomic_write(out_dir / "links_filtered.txt", "\n".join(all_links))

    kept, stats = await triage_links_llm(dom, all_links, company_id)
    kept = list(dict.fromkeys(kept))
    _atomic_write(out_dir / "triage_kept.txt", "\n".join(kept))

    # Write cleaned pages ndjson including homepage anchor mapping
    anchor_map = {li["url"]: li.get("anchor", "") for li in kept_items} if kept_items else {}
    tmp = (out_dir / "cleaned_pages.ndjson").with_suffix(".ndjson.part")
    with tmp.open("w", encoding="utf-8") as f:
        for rec in pages:
            f.write(json.dumps({"url": rec.url, "anchor": anchor_map.get(rec.url), "depth": rec.depth, "text": rec.cleaned_text}, ensure_ascii=False) + "\n")
    tmp.replace(out_dir / "cleaned_pages.ndjson")

    return pages, kept

def _load_kept_urls(out_dir: Path) -> List[str]:
    triage_file = out_dir / "triage_kept.txt"
    if triage_file.exists():
        return [line.strip() for line in triage_file.read_text(encoding="utf-8").splitlines() if line.strip()]
    return []

def _load_anchors(out_dir: Path) -> Dict[str, str]:
    anc = out_dir / "homepage_anchors.json"
    if not anc.exists():
        return {}
    try:
        arr = json.loads(anc.read_text(encoding="utf-8"))
        if isinstance(arr, list):
            return {it.get("url"): it.get("anchor") for it in arr if isinstance(it, dict)}
    except Exception:
        return {}
    return {}

async def standard_preview_extract(website: str, company_id: Optional[int]) -> Optional[Dict[str, Any]]:
    """
    GLM-4-Flash preview extraction (no caching, direct API calls)
    """
    from crawler.gemini_prompts import get_standard_static_context
    
    out_dir = _out_dir_for(website, company_id, Path("data/ab"))
    sample_urls = _load_kept_urls(out_dir)[:6]
    if not sample_urls:
        LOG.info("No kept URLs; skipping standard preview")
        return None
    
    anchors = _load_anchors(out_dir)
    client = ZhipuAI(api_key=os.environ.get('ZHIPUAI_API_KEY'))  # Returns ZhipuAI client
    
    merged: Dict[str, Any] = {}
    
    # Pre-load page content
    page_contents = {}
    nd = out_dir / "cleaned_pages.ndjson"
    if nd.exists():
        with nd.open("r", encoding="utf-8") as f:
            for line in f:
                doc = json.loads(line)
                u = doc.get("url")
                if u in sample_urls:
                    page_contents[u] = doc.get("text", "")
    
    system, schema = get_standard_static_context()
    
    for url in sample_urls:
        try:
            page_text = page_contents.get(url, "")
            if not page_text:
                continue
            
            user_content = (
                f"PAGE_META: url={url} anchor={anchors.get(url, '')}\n\n"
                f"PAGE_TEXT:\n{page_text[:60000]}\n\n"
                f"Extract according to the schema."
            )
            
            messages = [
                {"role": "system", "content": system},
                {"role": "user", "content": user_content}
            ]
            
            resp = gen_content_with_retry(client, messages)
            raw = resp.choices[0].message.content.strip()
            
            if not raw:
                continue
            
            try:
                doc = json.loads(raw)
            except json.JSONDecodeError:
                LOG.warning("Invalid JSON from GLM for url=%s", url)
                continue
            
            # Shallow merge
            for k, v in (doc or {}).items():
                if v is None:
                    continue
                if isinstance(v, dict):
                    merged.setdefault(k, {}).update({ik: iv for ik, iv in v.items() if iv})
                elif isinstance(v, list):
                    merged.setdefault(k, [])
                    merged[k].extend([it for it in v if it])
                else:
                    merged.setdefault(k, v)
        
        except Exception as e:
            LOG.error("Standard extract failed url=%s err=%s", url, repr(e))
            log_failure_to_csv(company_id, urlparse(website).netloc, "standard_extract", repr(e), [url])
    
    def _dedupe_list(items):
        if not isinstance(items, list):
            return items
        out, seen = [], set()
        for it in items:
            if isinstance(it, dict):
                key = json.dumps(it, sort_keys=True, ensure_ascii=False)
            else:
                key = it
            if key in seen:
                continue
            seen.add(key)
            out.append(it)
        return out
    
    # Deduplicate lists
    for k, v in list(merged.items()):
        if isinstance(v, list):
            merged[k] = _dedupe_list(v)
    
    (out_dir / "standard_preview.json").write_text(
        json.dumps(merged, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    return merged

async def submit_gemini_batch_for_company(
    website: str,
    company_id: Optional[int],
    cleaned: Path,
    kept: Path,
    out_dir: Path
) -> Optional[str]:
    """
    UPDATED: Route to GLMClient or GeminiBatchManager based on page count.

    Args:
        website: Company website URL
        company_id: Company ID
        cleaned: Path to cleaned_pages.ndjson
        kept: Path to triage_kept.txt  
        out_dir: Output directory

    Returns:
        "GLM_IMMEDIATE" if processed with GLM
        "GEMINI_BATCH" if submitted to Gemini batch
        None if error
    """
    domain = urlparse(website).netloc

    # Load pages from cleaned_pages.ndjson
    pages = []
    try:
        with cleaned.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    doc = json.loads(line)
                    pages.append({
                        "url": doc.get("url", ""),
                        "text": doc.get("text", "")
                    })
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        LOG.exception(f"Failed to load pages for {domain}: {e}")
        return None

    page_count = len(pages)

    if page_count == 0:
        LOG.warning(f"No pages for {domain}, skipping")
        return None

    LOG.info(f"Company {company_id} ({domain}): {page_count} pages")

    # Save routing info
    routing_info = {
        "page_count": page_count,
        "routing_decision": "GLM-4-Flash" if page_count <= 10 else "Gemini Batch",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    _atomic_write(
        out_dir / "routing_info.json",
        json.dumps(routing_info, ensure_ascii=False, indent=2)
    )

    # Route based on page count
    if page_count <= 10:
        # Use GLMClient for immediate extraction
        LOG.info(f"Routing to GLM-4-Flash (â‰¤10 pages)")
        try:
            glm_client = GLMClient()
            result = glm_client.extract(
                pages=pages,
                company_id=company_id or -1,
                domain=domain,
                output_dir=out_dir
            )
            LOG.info(f"GLM extraction complete for {domain}")
            return "GLM_IMMEDIATE"
        except Exception as e:
            LOG.exception(f"GLM extraction failed for {domain}: {e}")
            return None
    else:
        # Use GeminiBatchManager for single company
        LOG.info(f"Routing to Gemini Batch (>10 pages)")
        try:
            batch_manager = GeminiBatchManager()
            companies = [(company_id or -1, domain, pages, out_dir)]
            results = await batch_manager.batch_extract_companies(companies)
            LOG.info(f"Gemini batch submitted for {domain}")
            return "GEMINI_BATCH"
        except Exception as e:
            LOG.exception(f"Gemini batch failed for {domain}: {e}")
            return None

def save_to_db(website: str, company_id: Optional[int], doc: Dict[str, Any]) -> None:
    if DBHandler is None:
        LOG.info("DBHandler not available; skipping DB save")
        return
    try:
        # DBHandler should accept (company_id, website) or equivalent constructor
        h = DBHandler(company_id, website)  # conforms to your latest db_handler interface
        # Centralized cap/map happens inside the handler's entrypoint in your latest code
        if hasattr(h, "save_company_doc"):
            h.save_company_doc(doc)
        else:
            # Fallback: manual call sequence if entrypoint missing
            if hasattr(h, "_cap_and_map_sql"):
                doc = h._cap_and_map_sql(doc)
            if hasattr(h, "upsert_company"):
                h.upsert_company(doc.get("company") or {})
            if hasattr(h, "insert_products"):
                h.insert_products(company_id or -1, doc.get("products") or {})
            if hasattr(h, "insert_addresses"):
                h.insert_addresses(company_id or -1, doc.get("addresses") or [])
            if hasattr(h, "insert_management"):
                h.insert_management(company_id or -1, doc.get("management") or [])
            if hasattr(h, "insert_clients"):
                h.insert_clients(company_id or -1, doc.get("clients") or [])
            if hasattr(h, "insert_infrastructure"):
                h.insert_infrastructure(company_id or -1, doc.get("infrastructure") or {})
        LOG.info("DB save complete")
    except Exception as e:
        LOG.error("DB save failed: %s", repr(e))
        log_failure_to_csv(company_id, urlparse(website).netloc, "db_save", repr(e), [])

async def orchestrate_company(website: str, company_id: Optional[int], do_preview: bool, do_batch: bool, do_db: bool):
    """
    Full pipeline for one company: crawl â†’ triage â†’ artifacts â†’ (preview) â†’ (batch) â†’ (db)
    """
    out_dir = _out_dir_for(website, company_id)
    # Fast path: Standard-only from artifacts when --preview is used without batch/db
    if do_preview and not do_batch and not do_db and _artifacts_available(out_dir):
        try:
            await standard_preview_extract(website, company_id)  # reuses existing triage_kept + cleaned_pages
            return
        except Exception as e:
            LOG.exception("Standard-only preview failed; falling back to full pipeline")
    try:
        pages, kept = await crawl_with_anchor_roots(website, company_id)
    except Exception as e:
        LOG.exception("Crawl pipeline failed")
        log_failure_to_csv(company_id, urlparse(website).netloc, "crawl_pipeline", repr(e), [])
        return

    preview_doc = None
    if do_preview:
        preview_doc = await standard_preview_extract(website, company_id)

    if do_batch:
        cleaned = out_dir / "cleaned_pages.ndjson"
        kept = out_dir / "triage_kept.txt"
        await submit_gemini_batch_for_company(website, company_id, cleaned, kept, out_dir)

    if do_db and preview_doc:
        save_to_db(website, company_id, preview_doc)

def parse_args():
    parser = argparse.ArgumentParser(description="Orchestrator")
    # Global/single-site flags
    parser.add_argument("--url", help="Company website, e.g., https://example.com")
    parser.add_argument("--company-id", type=int, default=None)
    parser.add_argument("maybe_company_id", nargs="?", type=int)
    parser.add_argument(
        "--triage-parallel",
        type=int,
        default=5,
        help="Number of concurrent triage workers (default 5)",
    )
    parser.add_argument("--preview", action="store_true")
    parser.add_argument("--batch", action="store_true")
    parser.add_argument("--db", action="store_true")
    parser.add_argument(
        "--triage-range",
        nargs=2,
        type=int,
        metavar=("START_ID", "COUNT"),
        help="Run crawl+triage (and optional --batch) for COUNT sites starting at START_ID from DB",
    )
    parser.add_argument(
        "--excel-range",
        nargs=3,
        metavar=("EXCEL_PATH", "START_ID", "END_ID"),
        help="Run extraction for Excel companies (e.g., --excel-range Book2.xlsx S0001 S0050)",
    )
    # Subcommands
    sub = parser.add_subparsers(dest="cmd")

    sp_res = sub.add_parser(
        "resume-batch", help="Build+submit JSONL for all artifact dirs without triage"
    )
    sp_res.add_argument("--root", "-r", default="data/ab", help="Artifacts root dir")
    sp_res.add_argument("--ttl-hours", type=int, default=72, help="Cache TTL hours")
    sp_res.add_argument(
        "--limit", type=int, default=0, help="Max sites to submit (0=all)"
    )
    sp_res.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip companies that already have gemini_requests.jsonl (default: True)"
    )
    sp_res.add_argument(
        "--overwrite",
        action="store_true",
        help="Force overwrite gemini_requests.jsonl even if it exists"
    )

    sp_watch = sub.add_parser(
        "watch-batches",
        help="Poll batch status for all sites and write batch_status.json",
    )
    sp_watch.add_argument("--root", "-r", default="data/ab", help="Artifacts root dir")
    sp_watch.add_argument(
        "--poll-sec", type=int, default=15, help="Polling interval seconds"
    )

    sp_retry = sub.add_parser(
        "retry-pipeline", help="Rerun full pipeline for failed sites into a clean directory"
    )
    sp_retry.add_argument(
        "--file", "-f",
        default="batch_submission_failures.xlsx",
        help="Excel file with failed sites."
    )
    sp_retry.add_argument(
        "--output-root", "-o",
        default=f"data/ab_retry_{datetime.now().strftime('%Y%m%d_%H%M')}",
        help="Directory to store new artifacts for retried sites."
    )
    sp_retry.set_defaults(
        func=lambda a: asyncio.run(retry_pipeline_from_excel(a.file, a.output_root, a.triage_parallel))
    )
    sp_process = sub.add_parser(
    "process-results",
    help="Parse batch_output.ndjson files and insert into DB"
    )
    sp_process.add_argument("--root", "-r", default="data/ab", help="Artifacts root directory")
    sp_process.add_argument("--workers", "-w", type=int, default=10, help="Parallel workers")
    sp_process.set_defaults(
        func=lambda a: process_results_cmd(a.root, a.workers)
    )
    return parser.parse_args()

def process_results_cmd(root_dir: str, workers: int):
    """CLI entry point for process-results"""
    from crawler.batch_processor import process_all_batch_outputs
    LOG.info("Starting batch results processing: root=%s workers=%d", root_dir, workers)
    process_all_batch_outputs(root_dir, max_workers=workers)
    LOG.info("Batch processing complete")
    
def main():
    _setup_logging()
    args = parse_args()

    # Run subcommands and exit early
    if hasattr(args, "func"):
        args.func(args)
        return

    # Range path (DB-backed fetch + triage, optional batch)
    if args.triage_range:
        start_id, count = args.triage_range
        LOG.info(
            "Range triage start_id=%s count=%s batch=%s",
            start_id,
            count,
            args.batch,
        )
        asyncio.run(
            triage_range_and_optional_batch(
                start_id, count, do_batch=args.batch, parallel=args.triage_parallel
            )
        )
        LOG.info("Done")
        return
        # Excel range path
    if args.excel_range:
        excel_path, start_id, end_id = args.excel_range
        LOG.info(
            "Excel range: file=%s start=%s end=%s parallel=%s",
            excel_path, start_id, end_id, args.triage_parallel,
        )
        asyncio.run(
            excel_range_extraction(
                excel_path=excel_path,
                start_id=start_id,
                end_id=end_id,
                parallel=args.triage_parallel
            )
        )
        LOG.info("Done")
        return
    
    # Single-site paths
    company_id = args.company_id if args.company_id is not None else args.maybe_company_id
    website = args.url

    # Preview-only path from artifacts when URL not supplied
    if website is None:
        if args.preview and company_id is not None:
            inferred = _infer_website_from_artifacts(company_id)
            if inferred:
                website = inferred
                LOG.info(
                    "Inferred website from artifacts: %s (company_id=%s)",
                    website,
                    company_id,
                )
            else:
                raise SystemExit(
                    "Preview-only fast path requires existing artifacts in data/ab/{companyId}_{host}; could not infer website."
                )
        else:
            raise SystemExit(
                "Either --url must be provided, or use --preview with a company id (e.g., `--preview --company-id 3` or `--preview 3`)."
            )

    
    LOG.info("Start orchestrator: url=%s id=%s preview=%s batch=%s db=%s", args.url, args.company_id, args.preview, args.batch, args.db)
    asyncio.run(orchestrate_company(args.url, args.company_id, args.preview, args.batch, args.db))
    LOG.info("Done")

if __name__ == "__main__":
    main()