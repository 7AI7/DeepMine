"""
file: run_batch_on_cleaned.py

Run Gemini batch on already processed companies under data/ab/_

Only processes companies with gemini_extraction folders.
Skips companies with only glm_extraction folders.
Blacklists company IDs 100018 and 100014.
Batches all eligible companies into a single Batch API job.

Requires: GEMINI_API_KEY set, crawler/gemini_batch_manager.py with latest fixes

Usage:
  python run_batch_on_cleaned.py                                  # All eligible companies
  python run_batch_on_cleaned.py --limit 10                       # First 10 only
  python run_batch_on_cleaned.py --skip-if-has-output             # Skip if final_output.json exists
  python run_batch_on_cleaned.py --ids 100001 100002 100003
 # Process IDs 100400 to 100500 (inclusive)
python run_batch_on_cleaned.py --range-start 204501 --range-end 204799

# Process IDs 100200 and above
python run_batch_on_cleaned.py --range-start 100200

# Process IDs up to 100150
python run_batch_on_cleaned.py --range-end 100150

# Combine range with limit (first 10 companies in range)
python run_batch_on_cleaned.py --range-start 100300 --range-end 100400 --limit 10       # Specific IDs
  GEMINI_API_KEY=your_key python run_batch_on_cleaned.py          # Set API key
"""

import os
import re
import json
import argparse
from typing import Optional, Dict, List
import asyncio

from pathlib import Path
import logging

from crawler.gemini_batch_manager import GeminiBatchManager

LOG = logging.getLogger("run_batch_on_cleaned")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
)

def load_cleaned_pages(ndjson_path: Path) -> list[dict]:
    """Load pages from cleaned_pages.ndjson as a list of dicts."""
    pages: list[dict] = []
    
    if not ndjson_path.exists():
        return pages  # Return empty list if file doesn't exist
    
    try:
        with ndjson_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    pages.append(json.loads(line))
                except json.JSONDecodeError:
                    LOG.warning(f"Skipping invalid JSON line in {ndjson_path}")
    except Exception as e:
        LOG.error(f"Error reading {ndjson_path}: {e}")
    
    return pages

def discover_prepared_companies(
    base_dir: Path = Path("data/ab"),
    limit: Optional[int] = None,
    specific_ids: Optional[List[int]] = None,
    range_start: Optional[int] = None,
    range_end: Optional[int] = None,
) -> List[tuple]:

    """
    Discover companies that should be processed by Gemini batch
    
    CRITERIA - Process ONLY if:
    ✓ Has cleaned_pages.ndjson file
    ✓ cleaned_pages.ndjson is NOT empty
    ✓ NEITHER glm_extraction/output.json NOR gemini_extraction/final_output.json exists
    
    Skip if:
    ✗ No cleaned_pages.ndjson
    ✗ cleaned_pages.ndjson is empty
    ✗ Already has glm_extraction/output.json (GLM extracted)
    ✗ Already has gemini_extraction/final_output.json (Gemini extracted)
    ✗ Company ID in blacklist
    """
    companies = {}
    blacklist = {100018, 100014}
    
    for entry in sorted(base_dir.iterdir()):
        if not entry.is_dir():
            continue
        
        # Extract company ID
        try:
            company_id = int(entry.name.split('_')[0])
        except (ValueError, IndexError):
            continue
        
        # FILTER 1: Skip blacklisted
        if company_id in blacklist:
            LOG.debug(f"Skip (blacklisted): {entry.name}")
            continue
        if range_start is not None and company_id < range_start:
            LOG.debug(f"Skip (below range): {entry.name} (id={company_id})")
            continue
            
        if range_end is not None and company_id > range_end:
            LOG.debug(f"Skip (above range): {entry.name} (id={company_id})")
            continue
        # FILTER 2: Must have cleaned_pages.ndjson
        cleaned = entry / "cleaned_pages.ndjson"
        if not cleaned.exists():
            LOG.debug(f"Skip (no cleaned_pages.ndjson): {entry.name}")
            continue
        
        # FILTER 3: cleaned_pages.ndjson must NOT be empty
        try:
            with cleaned.open('r', encoding='utf-8') as f:
                first_line = f.readline()
                if not first_line.strip():
                    LOG.debug(f"Skip (empty cleaned_pages.ndjson): {entry.name}")
                    continue
        except Exception as e:
            LOG.warning(f"Skip (error reading cleaned_pages.ndjson): {entry.name} - {e}")
            continue
        
        # FILTER 4: Must NOT have glm_extraction/output.json
        glm_output = entry / "glm_extraction" / "output.json"
        if glm_output.exists():
            LOG.debug(f"Skip (has glm_extraction/output.json): {entry.name}")
            continue
        
        # FILTER 5: Must NOT have gemini_extraction/final_output.json
        #gemini_output = entry / "gemini_extraction" / "final_output.json"
        #if gemini_output.exists():
         #   LOG.debug(f"Skip (has gemini_extraction/final_output.json): {entry.name}")
          #  continue
        
        # All filters passed - include this company
        companies[company_id] = entry
        LOG.info(f"Include: {entry.name} (id={company_id})")
    
    # Apply optional filters
    if specific_ids:
        companies = {k: v for k, v in companies.items() if k in specific_ids}
        LOG.info(f"Filtered by specific IDs: {len(companies)} companies")
    
    if limit:
        companies = dict(list(companies.items())[:limit])
        LOG.info(f"Limited to first {limit}: {len(companies)} companies")
    
    companies_tuples = []
    for company_id, company_path in companies.items():
        # Extract domain: "100001_www.example.com" → "www.example.com"
        domain = company_path.name.split('_', 1)[1]
        
        # Load pages using existing helper function
        cleaned_pages_file = company_path / "cleaned_pages.ndjson"
        pages = load_cleaned_pages(cleaned_pages_file)
        
        if not pages:
            LOG.debug(f"Skip {company_id}: No pages after loading")
            continue
        
        companies_tuples.append((company_id, domain, pages, company_path))
        LOG.info(f"Prepared {company_id}: {len(pages)} pages")

    LOG.info(f"Total prepared: {len(companies_tuples)} companies")
    return companies_tuples




async def main():
    """Main orchestration function."""
    parser = argparse.ArgumentParser(
        description="Run Gemini batch on cleaned pages only (skips GLM extraction)"
    )
    parser.add_argument(
        "--base",
        default="data/ab",
        help="Base dir containing company folders"
    )
    parser.add_argument(
        "--ids",
        nargs="*",
        type=int,
        default=None,
        help="Restrict to these company IDs (overrides discovery)"
    )
    parser.add_argument(
        "--range-start",
        type=int,
        default=None,
        help="Start of company ID range (inclusive)"
    )

    parser.add_argument(
        "--range-end",
        type=int,
        default=None,
        help="End of company ID range (inclusive)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of companies to process"
    )
    parser.add_argument(
        "--skip-if-has-output",
        action="store_true",
        help="Skip companies that already have final_output.json"
    )
    
    args = parser.parse_args()
    
    # ============================================================
    # Validate environment
    # ============================================================
    if not os.getenv("GEMINI_API_KEY"):
        LOG.error("GEMINI_API_KEY environment variable is not set")
        raise SystemExit(1)
    
    # ============================================================
    # Discover and filter companies
    # ============================================================
    base_dir = Path(args.base)
    ids_filter = set(args.ids) if args.ids else None
    
    companies_prepared = discover_prepared_companies(
        base_dir,
        args.limit,  # Changed: pass limit here instead
        ids_filter,
        args.range_start,
        args.range_end
    )

    if not companies_prepared:
        LOG.error("No eligible companies found to process")
        return
    
    # ============================================================
    # Submit all eligible companies to a single batch job
    # ============================================================
    LOG.info(f"\n{'='*70}")
    LOG.info(f"Submitting {len(companies_prepared)} companies to Gemini Batch API")
    LOG.info(f"{'='*70}\n")
    
    mgr = GeminiBatchManager()
    
    # batch_extract_companies expects list of 4-tuples:
    # (company_id, domain, pages, out_dir)
    results = await mgr.batch_extract_companies(companies_prepared)
    
    # ============================================================
    # Report results
    # ============================================================
    ok = 0
    failed = 0
    
    for r in results or []:
        if r.get("success"):
            ok += 1
            LOG.info(
                f"✅ SUCCESS: {r.get('company_id')} {r.get('domain')} → "
                f"{r.get('output_path')}"
            )
        else:
            failed += 1
            LOG.error(
                f"❌ FAIL: {r.get('company_id')} {r.get('domain')} → "
                f"{r.get('error')}"
            )
    
    LOG.info(f"\n{'='*70}")
    LOG.info(f"SUMMARY: {ok} success, {failed} failed out of {len(companies_prepared)}")
    LOG.info(f"{'='*70}\n")


if __name__ == "__main__":
    asyncio.run(main())
