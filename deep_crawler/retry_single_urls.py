#!/usr/bin/env python3
"""
Retry specific failed URLs with automatic directory and company mapping.

Usage:
    python retry_single_urls.py "http://chennaicnc.com/?page_id=2839"
    python retry_single_urls.py "http://chennaicnc.com/?page_id=2839" "http://chennaicnc.com/?page_id=2873%2F"
"""

import sys
import json
import asyncio
import logging
from pathlib import Path
from typing import Optional, Dict
from urllib.parse import urlparse

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
LOG = logging.getLogger(__name__)

# Load API keys
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

def find_company_by_domain(domain: str, base_dir: str = "data/ab") -> Optional[tuple]:
    """
    Find company directory and ID by domain.
    Returns: (company_id, company_dir) or None
    """
    base = Path(base_dir)

    # Normalize domain
    domain_clean = domain.replace("www.", "").lower()

    # Search all directories
    for d in base.iterdir():
        if not d.is_dir() or "_" not in d.name:
            continue

        parts = d.name.split("_", 1)
        try:
            cid = int(parts[0])
        except ValueError:
            continue

        dir_domain = parts[1].replace("www.", "").lower()

        # Match domain
        if dir_domain == domain_clean or domain_clean.startswith(dir_domain):
            return (cid, d)

    return None

def load_cleaned_page(cleaned_pages_path: Path, target_url: str) -> Optional[Dict]:
    """Load specific page from cleaned_pages.ndjson"""
    if not cleaned_pages_path.exists():
        return None

    # Normalize URL for matching
    target_normalized = target_url.strip().lower()

    with open(cleaned_pages_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue

            page = json.loads(line)
            page_url = page.get('url', '').strip().lower()

            # Match URL (handle URL encoding variations)
            if page_url == target_normalized or page_url.replace('%2f', '/') == target_normalized.replace('%2f', '/'):
                return page

    return None

def _sync_api_call(api_key: str, system: str, user_content: str):
    """Synchronous GLM API call"""
    from zhipuai import ZhipuAI
    client = ZhipuAI(api_key=api_key)

    response = client.chat.completions.create(
        model="glm-4.5-flash",
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user_content}
        ],
        temperature=0.0,
        top_p=0.7,
        max_tokens=4096,
        response_format={"type": "json_object"},
    )

    return response

async def extract_single_url(
    url: str,
    company_id: int,
    domain: str,
    cleaned_pages_path: Path,
    batch_output_path: Path
) -> bool:
    """Extract data for a single URL and append to batch_output"""

    LOG.info(f"Processing URL: {url}")

    # Load cleaned page
    page = load_cleaned_page(cleaned_pages_path, url)
    if not page:
        LOG.error(f"  ✗ Page not found in cleaned_pages.ndjson: {url}")
        return False

    page_text = page.get('text', '')
    if not page_text:
        LOG.error(f"  ✗ Empty page text: {url}")
        return False

    LOG.info(f"  ✓ Found page (text length: {len(page_text)})")

    # Get prompt
    from crawler.gemini_prompts import get_standard_static_context
    system, schema = get_standard_static_context()
    system = system + "\n\nIMPORTANT: You MUST respond with ONLY a valid JSON object. No markdown, no explanations, no text outside the JSON."

    # Truncate
    body = page_text[:60000]

    # Build user prompt
    user_content = (
        f"PAGE_META: url={url} depth=0 anchor=\n\n"
        f"PAGE_TEXT:\n{body}\n\n"
        f"Extract structured data according to the schema. Return ONLY valid JSON."
    )

    # Call API
    try:
        LOG.info(f"  → Calling GLM API...")

        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: _sync_api_call(API_KEYS[0], system, user_content)
        )

        # Extract content
        content = response.choices[0].message.content.strip()

        # Validate JSON
        try:
            parsed = json.loads(content)
            LOG.info(f"  ✓ Valid JSON response received")
        except json.JSONDecodeError as je:
            LOG.error(f"  ✗ JSON parse error: {je}")
            return False

        # Format as batch_output entry
        entry = {
            "custom_id": f"{company_id}::{domain}::{hash(url) & 0xfffffff}",
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

        # Append to batch_output
        with open(batch_output_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')

        LOG.info(f"  ✓ Appended to {batch_output_path.name}")
        return True

    except Exception as e:
        # Check if it's a content filter error
        error_str = str(e)
        if "1301" in error_str or "contentFilter" in error_str:
            LOG.warning(f"  ⚠ Content filtered (GLM safety): {url}")
            LOG.warning(f"    This URL contains content that triggers GLM's safety filter")
            LOG.warning(f"    RECOMMENDATION: Skip this URL - it cannot be processed")
        else:
            LOG.error(f"  ✗ GLM API error: {repr(e)[:200]}")
        return False

async def main(urls: list):
    """Process list of URLs"""
    if not urls:
        print("Usage: python retry_single_urls.py <url1> [url2] [url3]...")
        print("\nExample:")
        print('  python retry_single_urls.py "http://chennaicnc.com/?page_id=2839"')
        sys.exit(1)

    LOG.info(f"Processing {len(urls)} URL(s)")
    LOG.info("="*80)

    results = {"success": 0, "failed": 0, "content_filtered": 0}

    for url in urls:
        LOG.info(f"\nURL: {url}")

        # Extract domain
        parsed = urlparse(url)
        domain = parsed.netloc

        # Find company directory
        result = find_company_by_domain(domain)
        if not result:
            LOG.error(f"  ✗ No company directory found for domain: {domain}")
            LOG.error(f"    Searched in: data/ab/")
            results["failed"] += 1
            continue

        company_id, company_dir = result
        LOG.info(f"  ✓ Found company: {company_dir.name} (ID: {company_id})")

        # Check files
        cleaned_pages = company_dir / "cleaned_pages.ndjson"
        batch_output = company_dir / "batch_output.ndjson"

        if not cleaned_pages.exists():
            LOG.error(f"  ✗ cleaned_pages.ndjson not found in {company_dir}")
            results["failed"] += 1
            continue

        if not batch_output.exists():
            LOG.warning(f"  ⚠ batch_output.ndjson doesn't exist, will create it")
            batch_output.touch()

        # Extract
        success = await extract_single_url(
            url=url,
            company_id=company_id,
            domain=domain,
            cleaned_pages_path=cleaned_pages,
            batch_output_path=batch_output
        )

        if success:
            results["success"] += 1
        else:
            # Check if it was content filtered
            if "content filter" in str(url).lower():
                results["content_filtered"] += 1
            else:
                results["failed"] += 1

    # Summary
    LOG.info("\n" + "="*80)
    LOG.info("SUMMARY")
    LOG.info("="*80)
    LOG.info(f"Success:          {results['success']}")
    LOG.info(f"Failed:           {results['failed']}")
    LOG.info(f"Content Filtered: {results['content_filtered']}")
    LOG.info("="*80)

    if results["content_filtered"] > 0:
        LOG.info("\n⚠ Content filtered URLs cannot be processed due to GLM safety filters")
        LOG.info("  These pages contain content that triggers API safety mechanisms")
        LOG.info("  RECOMMENDATION: Skip these URLs or manually review the pages")

if __name__ == "__main__":
    urls = sys.argv[1:]
    asyncio.run(main(urls))
