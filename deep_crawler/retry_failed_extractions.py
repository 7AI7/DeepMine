#!/usr/bin/env python3
"""
Retry failed JSON parse errors from failures CSV.
Matches gemini_batch.py API call pattern exactly.

Usage: python retry_failed_extractions.py [--limit N]
"""

import json
import pandas as pd
from pathlib import Path
from typing import Optional, Dict
import asyncio
import logging
from datetime import datetime, timezone
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
LOG = logging.getLogger(__name__)

# Load API keys (same as gemini_batch.py)
import os
API_KEYS = [
    os.environ.get("ZHIPUAI_API_KEY1"),
    os.environ.get("ZHIPUAI_API_KEY2"),
    os.environ.get("ZHIPUAI_API_KEY"),
]
API_KEYS = [k for k in API_KEYS if k]
if not API_KEYS:
    raise ValueError("No ZHIPUAI_API_KEY environment variables found")

LOG.info(f"✓ Loaded {len(API_KEYS)} API keys")

def find_company_dir(company_id: int, domain: str, base_dir: str = "data/ab") -> Optional[Path]:
    """Find company directory: data/ab/{id}_{domain}/"""
    base = Path(base_dir)

    # Try exact match
    dir_name = f"{company_id}_{domain}"
    exact = base / dir_name
    if exact.exists():
        return exact

    # Try pattern match
    for d in base.iterdir():
        if d.is_dir() and d.name.startswith(f"{company_id}_"):
            return d

    return None

def load_cleaned_pages(cleaned_path: Path) -> Dict[str, Dict]:
    """Load cleaned_pages.ndjson and index by URL"""
    pages = {}

    with open(cleaned_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue

            page = json.loads(line)
            url = page.get('url')
            if url:
                pages[url] = page

    return pages

def _sync_api_call(api_key: str, system: str, user_content: str):
    """
    EXACT COPY of gemini_batch.py _sync_api_call
    Synchronous API call with response_format JSON mode
    """
    from zhipuai import ZhipuAI
    client = ZhipuAI(api_key=api_key)

    response = client.chat.completions.create(
        model="glm-4-flash-250414",
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user_content}
        ],
        # Core parameters
        temperature=0.0,
        top_p=0.7,
        max_tokens=4096,
        # JSON mode (CRITICAL)
        response_format={"type": "json_object"},
    )

    return response

async def extract_with_glm(page_url: str, page_text: str, company_id: int, domain: str, api_key: str) -> Optional[Dict]:
    """
    Extract data using GLM API
    Matches gemini_batch.py process_single() logic
    """
    from crawler.gemini_prompts import get_standard_static_context

    # Get prompt (same as build_jsonl)
    system, schema = get_standard_static_context()
    system = system + "\n\nIMPORTANT: You MUST respond with ONLY a valid JSON object. No markdown, no explanations, no text outside the JSON."

    # Truncate to match build_jsonl
    body = page_text[:60000]

    # Build user prompt (same format as build_jsonl)
    user_content = (
        f"PAGE_META: url={page_url} depth=0 anchor=\n\n"
        f"PAGE_TEXT:\n{body}\n\n"
        f"Extract structured data according to the schema. Return ONLY valid JSON."
    )

    try:
        # Execute API call in thread pool (same as gemini_batch.py)
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: _sync_api_call(api_key, system, user_content)
        )

        # Extract content (same as gemini_batch.py)
        content = response.choices[0].message.content.strip()

        # Enhanced JSON extraction (same as gemini_batch.py)
        json_match = re.search(r'``````', content, re.DOTALL)
        if json_match:
            content = json_match.group(1)
        else:
            # Fallback: extract first JSON object
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                content = json_match.group(0)

        # Validate JSON (same as gemini_batch.py)
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as je:
            LOG.error(f"JSON parse error for {page_url}: {str(je)[:80]}")
            return None

        # Return in EXACT batch_output format (same as gemini_batch.py)
        return {
            "custom_id": f"{company_id}::{domain}::{hash(page_url) & 0xfffffff}",
            "response": {
                "candidates": [{
                    "content": {"parts": [{"text": content}]},
                    "finishReason": response.choices[0].finish_reason or "STOP"
                }],
                "usageMetadata": {
                    "promptTokenCount": response.usage.prompt_tokens,
                    "candidatesTokenCount": response.usage.completion_tokens,
                    "totalTokenCount": response.usage.total_tokens
                }
            }
        }

    except Exception as e:
        LOG.error(f"GLM extraction failed for {page_url}: {repr(e)[:150]}")
        return None

async def retry_failures(
    failures_csv: str = "data/failures/failures_20251006.csv",
    base_dir: str = "data/ab",
    limit: Optional[int] = None,
    workers_per_key: int = 2
):
    """Retry failed JSON parse errors with parallel processing"""

    # Load failures
    df = pd.read_csv(failures_csv)

    # Filter JSON parse errors
    json_errors = df[
        (df['stage'] == 'glm_parallel_extraction') &
        (df['error'].str.contains('JSON parse error', case=False, na=False))
    ]

    LOG.info(f"Found {len(json_errors)} JSON parse errors")

    if limit:
        json_errors = json_errors.head(limit)
        LOG.info(f"Processing first {limit} errors")

    # Stats
    stats = {
        "processed": 0,
        "success": 0,
        "failed": 0,
        "missing_dir": 0,
        "missing_page": 0,
        "appended": 0
    }

    # Per-key semaphores
    semaphores = {key: asyncio.Semaphore(workers_per_key) for key in API_KEYS}

    # Group by company
    for company_id, group in json_errors.groupby('company_id'):
        domain = group['domain'].iloc[0]
        urls = group['urls'].dropna().tolist()

        if not urls:
            LOG.warning(f"No URLs for company {company_id} ({domain})")
            continue

        LOG.info(f"\nProcessing company {company_id}: {domain} ({len(urls)} failed URLs)")

        # Find company directory
        company_dir = find_company_dir(company_id, domain, base_dir)
        if not company_dir:
            LOG.warning(f"  Directory not found: {company_id}_{domain}")
            stats["missing_dir"] += len(urls)
            continue

        LOG.info(f"  Directory: {company_dir}")

        # Load cleaned pages
        cleaned_path = company_dir / "cleaned_pages.ndjson"
        if not cleaned_path.exists():
            LOG.warning(f"  cleaned_pages.ndjson not found")
            stats["missing_page"] += len(urls)
            continue

        cleaned_pages = load_cleaned_pages(cleaned_path)
        LOG.info(f"  Loaded {len(cleaned_pages)} cleaned pages")

        # Prepare batch output path
        batch_output_path = company_dir / "batch_output.ndjson"

        # Process URLs in parallel (like gemini_batch.py)
        async def process_url(url: str, idx: int) -> Optional[Dict]:
            """Process single URL with API key rotation"""
            stats["processed"] += 1

            # Find cleaned page
            if url not in cleaned_pages:
                LOG.warning(f"  Page not found: {url}")
                stats["missing_page"] += 1
                return None

            page = cleaned_pages[url]
            page_text = page.get('text', '')

            if not page_text:
                LOG.warning(f"  Empty text: {url}")
                stats["missing_page"] += 1
                return None

            # Round-robin API key assignment (same as gemini_batch.py)
            key_idx = idx % len(API_KEYS)
            api_key = API_KEYS[key_idx]
            semaphore = semaphores[api_key]

            async with semaphore:
                LOG.info(f"  [{idx}/{len(urls)}] [Key{key_idx+1}] Extracting: {url}")

                result = await extract_with_glm(url, page_text, company_id, domain, api_key)

                if result:
                    stats["success"] += 1
                    LOG.info(f"  ✓ Success")
                    return result
                else:
                    stats["failed"] += 1
                    return None

        # Execute all URLs in parallel
        tasks = [process_url(url, idx) for idx, url in enumerate(urls, 1)]
        results = await asyncio.gather(*tasks)

        # Append successful results to batch_output
        success_results = [r for r in results if r is not None]

        if success_results:
            with open(batch_output_path, 'a', encoding='utf-8') as f:
                for result in success_results:
                    f.write(json.dumps(result, ensure_ascii=False) + '\n')
                    stats["appended"] += 1

            LOG.info(f"  ✓ Appended {len(success_results)} results to {batch_output_path.name}")

    # Summary
    LOG.info(f"\n{'='*80}")
    LOG.info("RETRY SUMMARY")
    LOG.info(f"{'='*80}")
    LOG.info(f"Processed:    {stats['processed']}")
    LOG.info(f"Success:      {stats['success']}")
    LOG.info(f"Appended:     {stats['appended']}")
    LOG.info(f"Failed:       {stats['failed']}")
    LOG.info(f"Missing dir:  {stats['missing_dir']}")
    LOG.info(f"Missing page: {stats['missing_page']}")
    LOG.info(f"{'='*80}")

    # Save summary
    summary_path = Path("data/summaries/retry_summary.json")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(stats, indent=2), encoding='utf-8')

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Retry failed JSON parse extractions')
    parser.add_argument('--failures', default='data/failures/failures_20251006.csv', help='Failures CSV path')
    parser.add_argument('--base-dir', default='data/ab', help='Base directory for companies')
    parser.add_argument('--limit', type=int, help='Limit number of retries (for testing)')
    parser.add_argument('--workers', type=int, default=2, help='Workers per API key')

    args = parser.parse_args()

    asyncio.run(retry_failures(
        failures_csv=args.failures,
        base_dir=args.base_dir,
        limit=args.limit,
        workers_per_key=args.workers
    ))

if __name__ == "__main__":
    main()
