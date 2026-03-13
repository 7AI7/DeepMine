# Enrichment Pipeline — Main Orchestrator
#
# Reads companies from Excel, runs 15 async workers, each with:
#   - GoogleSnippetScraper (for LinkedIn and TheCompanyCheck)
#   - GoogleMapsScraper    (for Maps data)
#
# Each worker processes companies from a shared queue.
# Results saved incrementally to output Excel.
# Resume capability via progress.json.

import asyncio
import random
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from utils.logger import setup_logger
from utils.excel_handler import (
    read_companies, save_results, save_incremental,
    load_progress, mark_completed, OUTPUT_COLUMNS,
)
from scrapers.google_snippet import GoogleSnippetScraper
from scrapers.thecompanycheck import CompanyCheckScraper
from scrapers.linkedin import LinkedInScraper
from scrapers.google_maps import GoogleMapsScraper

logger = setup_logger("main")


async def worker(
    worker_id: int,
    queue: asyncio.Queue,
    results: list[dict],
    results_lock: asyncio.Lock,
    proxy_slice: list[dict],
):
    """
    Single worker coroutine. Processes companies from the shared queue.
    
    Each worker has its OWN browser instances:
      - 1 Google browser (for LinkedIn search + TheCompanyCheck)
      - 1 Maps browser (for Google Maps)
    = 2 Chromium processes per worker
    
    Args:
        worker_id: Worker index (0-14)
        queue: Shared asyncio.Queue of company dicts
        results: Shared list to append results to
        results_lock: Lock for thread-safe results append
        proxy_slice: List of proxy configs assigned to this worker
    """
    tag = f"[W{worker_id:02d}]"
    logger.info(f"{tag} Starting worker with {len(proxy_slice)} proxies")
    
    # Pick a proxy from the worker's slice (rotate through them)
    proxy_idx = 0
    
    def get_proxy():
        nonlocal proxy_idx
        if not proxy_slice:
            return None
        p = proxy_slice[proxy_idx % len(proxy_slice)]
        proxy_idx += 1
        return p
    
    # ── Initialize browser instances ──
    google_scraper = GoogleSnippetScraper(proxy_config=get_proxy(), user_agent=config.random_ua())
    maps_scraper = GoogleMapsScraper(proxy_config=get_proxy(), user_agent=config.random_ua())
    
    try:
        await google_scraper.initialize()
        await maps_scraper.initialize()
        logger.info(f"{tag} Both browsers initialized")
    except Exception as e:
        logger.error(f"{tag} Browser init failed: {e}")
        return
    
    # Create scraper instances that use the shared browsers
    linkedin_scraper = LinkedInScraper(google_scraper)
    tcc_scraper = CompanyCheckScraper(google_scraper)
    
    companies_processed = 0
    
    try:
        while True:
            # Get next company from queue
            try:
                company = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            
            company_name = company["name"]
            logger.info(f"{tag} Processing: {company_name}")
            start = time.time()
            
            row = {"company_name": company_name}
            
            # ── Step A: TheCompanyCheck (Google → revenue + employees) ──
            try:
                tcc_data = await tcc_scraper.scrape(company_name)
                row["revenue_2023"]    = tcc_data.get("revenue_2023", "N/A")
                row["net_profit_2023"] = tcc_data.get("net_profit_2023", "N/A")
                row["employee_count"]  = tcc_data.get("employee_count", "N/A")
                row["tcc_source_url"]  = tcc_data.get("tcc_source_url", "N/A")
            except Exception as e:
                logger.error(f"{tag} TCC failed for '{company_name}': {e}")
                row["revenue_2023"]    = "N/A"
                row["net_profit_2023"] = "N/A"
                row["employee_count"]  = "N/A"
                row["tcc_source_url"]  = "N/A"
            
            # ── Step B: LinkedIn (Bing snippet → URL + followers) ──
            try:
                li_data = await linkedin_scraper.scrape(company_name)
                row["linkedin_url"] = li_data.get("linkedin_url", "N/A")
                row["linkedin_followers"] = li_data.get("linkedin_followers", "N/A")
            except Exception as e:
                logger.error(f"{tag} LinkedIn failed for '{company_name}': {e}")
                row["linkedin_url"] = "N/A"
                row["linkedin_followers"] = "N/A"
            
            # ── Step C: Google Maps (Playwright → all matching locations) ──
            try:
                maps_data = await maps_scraper.scrape(company_name)
            except Exception as e:
                logger.error(f"{tag} Maps failed for '{company_name}': {e}")
                maps_data = []
            
            # ── Step D: Merge and save ──
            async with results_lock:
                if maps_data:
                    for loc in maps_data:
                        merged = {**row, **loc}
                        results.append(merged)
                else:
                    row.update({
                        "maps_name": "N/A",
                        "address": "N/A",
                        "maps_link": "N/A",
                        "phone": "N/A",
                        "rating": "N/A",
                        "review_count": "N/A",
                        "email": "N/A",
                        "category": "N/A",
                    })
                    results.append(row)
            
            # Mark completed
            mark_completed(company_name)
            companies_processed += 1
            
            elapsed = time.time() - start
            logger.info(
                f"{tag} Done: {company_name} "
                f"({companies_processed} done, {elapsed:.1f}s, "
                f"maps_locations={len(maps_data)})"
            )
            
            # Incremental save every SAVE_EVERY_N companies
            if companies_processed % config.SAVE_EVERY_N == 0:
                async with results_lock:
                    batch = results.copy()
                    results.clear()
                save_incremental(batch)
                logger.info(f"{tag} Saved batch of {len(batch)} rows")
            
            queue.task_done()
    
    except Exception as e:
        logger.error(f"{tag} Worker crashed: {e}", exc_info=True)
    
    finally:
        # Cleanup
        await google_scraper.close()
        await maps_scraper.close()
        logger.info(f"{tag} Worker finished ({companies_processed} companies)")


async def main():
    """
    Main entry point — orchestrates the enrichment pipeline.
    
    1. Load companies from Excel
    2. Load progress (skip already-done companies)
    3. Distribute proxies across workers
    4. Launch workers
    5. Save remaining results
    """
    logger.info("=" * 60)
    logger.info("SHIVA Enrichment Pipeline — Starting")
    logger.info("=" * 60)
    
    # ── Load companies ──
    try:
        companies = read_companies(config.INPUT_EXCEL, config.INPUT_SHEET)
    except Exception as e:
        logger.error(f"Failed to read input Excel: {e}")
        return
    
    logger.info(f"Loaded {len(companies)} companies from {config.INPUT_EXCEL}")
    
    # ── Load progress ──
    completed = load_progress()
    remaining = [c for c in companies if c["name"] not in completed]
    logger.info(f"Already completed: {len(completed)}, remaining: {len(remaining)}")
    
    if not remaining:
        logger.info("All companies already processed!")
        return
    
    # ── Load proxies ──
    proxies = config.load_proxies() if config.USE_PROXY else []
    logger.info(f"Loaded {len(proxies)} proxies (USE_PROXY={config.USE_PROXY})")
    
    # ── Distribute proxies across workers ──
    num_workers = min(config.MAX_WORKERS, len(remaining))
    proxy_slices = [[] for _ in range(num_workers)]
    if proxies:
        for i, proxy in enumerate(proxies):
            proxy_slices[i % num_workers].append(proxy)
    
    logger.info(f"Launching {num_workers} workers")
    
    # ── Fill queue ──
    queue = asyncio.Queue()
    for company in remaining:
        queue.put_nowait(company)
    
    # ── Shared results list ──
    results: list[dict] = []
    results_lock = asyncio.Lock()
    
    # ── Launch workers ──
    start_time = time.time()
    
    tasks = [
        asyncio.create_task(
            worker(i, queue, results, results_lock, proxy_slices[i])
        )
        for i in range(num_workers)
    ]
    
    await asyncio.gather(*tasks, return_exceptions=True)
    
    # ── Save remaining results ──
    if results:
        save_incremental(results)
        logger.info(f"Final save: {len(results)} rows")
    
    elapsed = time.time() - start_time
    total_done = len(completed) + len(remaining) - queue.qsize()
    logger.info(
        f"Pipeline finished in {elapsed:.0f}s "
        f"({total_done}/{len(companies)} companies)"
    )


if __name__ == "__main__":
    asyncio.run(main())
