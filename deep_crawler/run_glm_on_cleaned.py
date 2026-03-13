"""
GLM Extractor for companies with glm_extraction folder

Processes cleaned pages from ndjson files in data/ab/<id>_<domain>/glm_extraction/

Sends 150 companies at a time (50 per API key × 3 keys) with two strategies:
  Option 1: Send all 150, wait for all responses, then next batch
  Option 2: Send as responses complete, keep queue at 150

Skips:
  - Companies with gemini_extraction/ folder
  - Companies without glm_extraction/ folder
  - Companies where glm_extraction/output.json already exists (unless --force)

Saves output.json to each company's glm_extraction/ folder (replaces existing)

Usage:
  python run_glm_on_cleaned.py --strategy option1 [--all | --ids 100001 100002 | --range 100001-100100]
  python run_glm_on_cleaned.py --strategy option2 [--all | --ids 100001 100002 | --range 100001-100100]
  python run_glm_on_cleaned.py --all --force  # Force re-extract even if output.json exists
"""

import os
import re
import json
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from collections import deque
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import GLM client and parser
from zhipuai import ZhipuAI


LOG = logging.getLogger("run_glm_on_cleaned")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'
)

DATA_ROOT = Path("data/ab")
BATCH_SIZE = 150  # 50 per key × 3 keys
MAX_RETRIES = 3
BACKOFF_BASE = 2  # exponential backoff: 2^retry seconds


class GLMExtractorManager:
    """Manages GLM extraction for 150 companies at a time"""
    
    def __init__(self, api_keys: List[str]):
        """
        Initialize with 3 API keys for rotation
        api_keys: List of 3 Zhipu API keys
        """
        if len(api_keys) != 3:
            raise ValueError("Exactly 3 API keys required (1 per 50 concurrent requests)")
        
        self.api_keys = api_keys
        self.clients = [ZhipuAI(api_key=key) for key in api_keys]
        self.errors = []
        self.lock = threading.Lock()
    
    def get_eligible_companies(self) -> Dict[int, Path]:
        """
        Find all companies with:
        - cleaned_pages.ndjson EXISTS and has <= 10 lines
        - cleaned_pages.ndjson is NOT empty
        Returns: {company_id: company_folder_path}
        """
        eligible = {}
        for company_folder in sorted(DATA_ROOT.iterdir()):
            if not company_folder.is_dir():
                continue
            
            # Extract company_id
            try:
                company_id = int(company_folder.name.split('_')[0])
            except (ValueError, IndexError):
                continue
            
            # Check cleaned_pages.ndjson
            ndjson_path = company_folder / "cleaned_pages.ndjson"
            if not ndjson_path.exists():
                continue
            
            # Count lines and check if not empty
            try:
                with ndjson_path.open('r', encoding='utf-8') as f:
                    lines = f.readlines()
                    line_count = len(lines)
                    
                    # Skip if empty or > 10 lines
                    if line_count == 0 or line_count > 10:
                        continue
                    
                    eligible[company_id] = company_folder
            except Exception as e:
                LOG.warning(f"Could not read {ndjson_path}: {e}")
                continue
        
        return eligible

        
    
    def filter_by_range(self, eligible: Dict[int, Path], ranges: List[Tuple[int, int]]) -> Dict[int, Path]:
        """Filter companies by ID ranges: [(100001, 100050), (100100, 100200)]"""
        filtered = {}
        for cid, path in eligible.items():
            for start, end in ranges:
                if start <= cid <= end:
                    filtered[cid] = path
                    break
        return filtered
    
    def read_cleaned_pages(self, company_folder: Path) -> Optional[str]:
        """Read cleaned_pages.ndjson and concatenate all pages"""
        ndjson_path = company_folder / "cleaned_pages.ndjson"
        
        if not ndjson_path.exists():
            LOG.warning(f"No cleaned_pages.ndjson: {company_folder.name}")
            return None
        
        try:
            pages = []
            with ndjson_path.open('r', encoding='utf-8') as f:
                for idx, line in enumerate(f):
                    data = json.loads(line)
                    url = data.get('url', f'page_{idx}')
                    content = data.get('content', '')
                    pages.append(f"=== PAGE: {url} ===\n{content}\n")
            
            return ''.join(pages)
        
        except Exception as e:
            LOG.error(f"Error reading {ndjson_path}: {e}")
            return None
    
    def extract_with_glm(self, company_id: int, concatenated_content: str, client_idx: int = 0) -> Optional[Dict]:
        """
        Send request to GLM-4-Flash-250414 and get response
        
        Args:
            company_id: Company ID for logging
            concatenated_content: All pages concatenated
            client_idx: Which API key to use (0, 1, or 2)
        
        Returns: Parsed extraction dict or None on failure
        """
        from crawler.gemini_prompts import get_whole_website_prompt
        
        try:
            # Get extraction prompt
            system_prompt, schema = get_whole_website_prompt(is_split=False)
            
            # Create GLM request
            client = self.clients[client_idx]
            
            response = client.chat.completions.create(
                model='GLM-4-Flash-250414',
                messages=[
                    {
                        'role': 'system',
                        'content': system_prompt
                    },
                    {
                        'role': 'user',
                        'content': concatenated_content
                    }
                ],
                temperature=0,
                top_p=0.1,
                response_format={'type': 'json_object'}
            )
            
            # Parse response
            response_text = response.choices[0].message.content
            response_json = json.loads(response_text)
            
            LOG.info(f"✓ Company {company_id}: GLM extraction successful")
            return response_json
        
        except Exception as e:
            LOG.error(f"✗ Company {company_id}: GLM extraction failed: {e}")
            return None
    
    def parse_and_save(self, company_id: int, glm_response: Dict, company_folder: Path) -> bool:
        """
        Save GLM response to output.json after merging with homepage_contacts.json
        
        1. Load homepage_contacts.json if exists
        2. Extract phones + emails from GLM response
        3. Merge homepage contacts with GLM contacts
        4. Dedupe (case-sensitive for phones, case-insensitive for emails)
        5. Save as CSV in output.json company.phone and company.email fields
        
        Returns: True on success, False on failure
        """
        try:
            # Load homepage_contacts.json if exists
            homepage_file = company_folder / "homepage_contacts.json"
            homepage_contacts = {}
            
            if homepage_file.exists():
                try:
                    homepage_contacts = json.loads(homepage_file.read_text(encoding='utf-8'))
                except Exception as e:
                    LOG.warning(f"Company {company_id}: Failed to read homepage_contacts.json: {e}")
                    homepage_contacts = {}
            
            # Dedupe function (from orchestrator.py)
            def _dedup(seq, key=lambda x: x):
                seen, out = set(), []
                for s in [t.strip() for t in seq if t]:
                    k = key(s)
                    if k not in seen:
                        seen.add(k)
                        out.append(s)
                return out
            
            # Collect all phones and emails
            phones_list = []
            emails_list = []
            
            # Add homepage contacts
            phones_list.extend(homepage_contacts.get('phones_raw', []))
            phones_list.extend(homepage_contacts.get('jsonld_phones', []))
            emails_list.extend(homepage_contacts.get('emails_raw', []))
            emails_list.extend(homepage_contacts.get('jsonld_emails', []))
            
            # Add GLM extracted contacts if present
            if glm_response.get('company', {}).get('phone'):
                phones_list.append(glm_response['company']['phone'])
            
            if glm_response.get('company', {}).get('email'):
                emails_list.append(glm_response['company']['email'])
            
            # Dedupe: phones case-sensitive, emails case-insensitive
            phones_csv = ", ".join(_dedup(phones_list))
            emails_csv = ", ".join(_dedup(emails_list, key=lambda x: x.lower()))
            
            # Update GLM response with merged+deduped contacts
            if not glm_response.get('company'):
                glm_response['company'] = {}
            
            if phones_csv:
                glm_response['company']['phone'] = phones_csv
            else:
                glm_response['company']['phone'] = None
            
            if emails_csv:
                glm_response['company']['email'] = emails_csv
            else:
                glm_response['company']['email'] = None
            
            # Save to output.json
            glm_folder = company_folder / "glm_extraction"
            glm_folder.mkdir(exist_ok=True)

            # Save to output.json
            output_path = glm_folder / "output.json"
            output_path.write_text(
                json.dumps(glm_response, ensure_ascii=False, indent=2),
                encoding='utf-8'
            )
            LOG.info(f"✓ Company {company_id}: Saved output.json with merged contacts (phones={bool(phones_csv)}, emails={bool(emails_csv)})")
            return True
        
        except Exception as e:
            error_msg = f"Company {company_id}: Parse/save error: {e}"
            LOG.error(f"✗ {error_msg}")
            with self.lock:
                self.errors.append(error_msg)
            return False


    
    def process_with_retry(self, company_id: int, company_folder: Path, client_idx: int) -> bool:
        """Process single company with retry + backoff logic"""
        concatenated_content = self.read_cleaned_pages(company_folder)
        if concatenated_content is None:
            error_msg = f"Company {company_id}: Could not read cleaned_pages.ndjson"
            with self.lock:
                self.errors.append(error_msg)
            return False
        
        for attempt in range(MAX_RETRIES):
            try:
                glm_response = self.extract_with_glm(company_id, concatenated_content, client_idx)
                
                if glm_response is None:
                    if attempt < MAX_RETRIES - 1:
                        backoff = BACKOFF_BASE ** attempt
                        LOG.warning(f"Company {company_id}: Retry {attempt + 1}/{MAX_RETRIES} in {backoff}s")
                        time.sleep(backoff)
                        continue
                    else:
                        error_msg = f"Company {company_id}: All {MAX_RETRIES} retries failed"
                        with self.lock:
                            self.errors.append(error_msg)
                        return False
                
                # Successfully got response, now parse and save
                return self.parse_and_save(company_id, glm_response, company_folder)
            
            except Exception as e:
                error_msg = f"Company {company_id}: Attempt {attempt + 1} error: {e}"
                LOG.error(error_msg)
                if attempt < MAX_RETRIES - 1:
                    backoff = BACKOFF_BASE ** attempt
                    time.sleep(backoff)
                else:
                    with self.lock:
                        self.errors.append(error_msg)
                    return False
        
        return False
    
    def extract_option1(self, companies_batch: Dict[int, Path]):
        """
        OPTION 1: Send all 150, wait for all responses, then next batch
        
        Pros: Simple, clean batches
        Cons: Slower if some fail (wait for all before retry)
        """
        LOG.info(f"[OPTION 1] Processing {len(companies_batch)} companies...")
        
        with ThreadPoolExecutor(max_workers=150) as executor:
            futures = {}
            
            # Submit all tasks
            for idx, (company_id, company_folder) in enumerate(companies_batch.items()):
                client_idx = (idx // 50) % 3  # Rotate across 3 API keys
                future = executor.submit(
                    self.process_with_retry,
                    company_id,
                    company_folder,
                    client_idx
                )
                futures[future] = company_id
            
            # Wait for all to complete
            completed = 0
            for future in as_completed(futures):
                completed += 1
                result = future.result()
                if completed % 10 == 0:
                    LOG.info(f"[OPTION 1] Progress: {completed}/{len(companies_batch)}")
            
            LOG.info(f"[OPTION 1] Batch complete: {completed}/{len(companies_batch)}")
    
    def extract_option2(self, companies_batch: Dict[int, Path]):
        """
        OPTION 2: Send as responses complete, keep queue at 150
        
        Pros: Faster (restart immediately on completion)
        Cons: Complex queue management
        """
        LOG.info(f"[OPTION 2] Processing {len(companies_batch)} companies...")
        
        queue = deque((cid, folder) for cid, folder in companies_batch.items())
        futures = {}
        completed = 0
        
        with ThreadPoolExecutor(max_workers=150) as executor:
            # Pre-fill queue
            while len(futures) < min(150, len(queue)):
                company_id, company_folder = queue.popleft()
                client_idx = (completed + len(futures)) % 3
                future = executor.submit(
                    self.process_with_retry,
                    company_id,
                    company_folder,
                    client_idx
                )
                futures[future] = company_id
            
            # Process completions and refill
            for future in as_completed(futures):
                completed += 1
                company_id = futures.pop(future)
                
                try:
                    result = future.result()
                except Exception as e:
                    LOG.error(f"Company {company_id}: Unexpected error: {e}")
                
                # Refill from queue
                if queue:
                    next_company_id, next_folder = queue.popleft()
                    client_idx = completed % 3
                    next_future = executor.submit(
                        self.process_with_retry,
                        next_company_id,
                        next_folder,
                        client_idx
                    )
                    futures[next_future] = next_company_id
                
                if completed % 10 == 0:
                    LOG.info(f"[OPTION 2] Progress: {completed}/{len(companies_batch)}")
            
            LOG.info(f"[OPTION 2] All complete: {completed}/{len(companies_batch)}")

    def extract_option3(self, companies_batch: Dict[int, Path]):
        """
        OPTION 3: Send 50 per key sequentially with delays
        - 50 companies → Key 1 (wait)
        - 50 companies → Key 2 (wait) 
        - 50 companies → Key 3 (wait)
        - Then repeat
        
        Respects: 100 req/min per key = 1 req/0.6s = safe with 1s delay
        """
        LOG.info(f"[OPTION 3] Processing {len(companies_batch)} companies (50 per key, sequential)")
        
        companies_list = list(companies_batch.items())
        completed = 0
        
        # Process 50 companies per key
        for key_idx in range(3):
            start = key_idx * 50
            end = min(start + 50, len(companies_list))
            batch_for_key = companies_list[start:end]
            
            if not batch_for_key:
                continue
            
            LOG.info(f"[OPTION 3] Key {key_idx + 1}: Processing {len(batch_for_key)} companies")
            
            with ThreadPoolExecutor(max_workers=10) as executor:  # Only 10 concurrent per key
                futures = {}
                
                for idx, (company_id, company_folder) in enumerate(batch_for_key):
                    future = executor.submit(
                        self.process_with_retry,
                        company_id,
                        company_folder,
                        key_idx
                    )
                    futures[future] = company_id
                    
                    # Delay to avoid 429: ~0.6s per request (100 req/min = 1 req/0.6s)
                    time.sleep(0.6)
                
                # Wait for all to complete
                for future in as_completed(futures):
                    completed += 1
                    future.result()
                    if completed % 10 == 0:
                        LOG.info(f"[OPTION 3] Progress: {completed}/{len(companies_batch)}")

def main():
    parser = argparse.ArgumentParser(description='GLM Extractor for cleaned pages')
    
    # API keys
    parser.add_argument('--api-keys', nargs=3, required=False,
                       help='3 Zhipu API keys (or set env vars: ZHIPUAI_API_KEY1, ZHIPUAI_API_KEY2, ZHIPUAI_API_KEY3)')
    
    # Selection mode
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--all', action='store_true', help='Process all eligible companies')
    mode.add_argument('--ids', nargs='+', type=int, help='Process specific company IDs')
    mode.add_argument('--range', type=str, help='Process range: 100001-100100')
    
    # Strategy
    parser.add_argument('--strategy', choices=['option1', 'option2','option3'], default='option1',
                       help='Batch strategy: option1=send all wait all, option2=keep queue at 150, option3=working')
    
    # Force re-extract
    parser.add_argument('--force', action='store_true', help='Re-extract even if output.json exists')
    
    args = parser.parse_args()
    
    # Get API keys
    if args.api_keys:
        api_keys = args.api_keys
    else:
        api_keys = [
            os.environ.get('ZHIPUAI_API_KEY1'),
            os.environ.get('ZHIPUAI_API_KEY2'),
            os.environ.get('ZHIPUAI_API_KEY')
        ]
        if any(k is None for k in api_keys):
            LOG.error("❌ Provide 3 API keys via --api-keys or env vars ZHIPUAI_API_KEY{1,2,3}")
            return
    
    # Initialize manager
    manager = GLMExtractorManager(api_keys)
    
    # Get eligible companies
    eligible = manager.get_eligible_companies()
    LOG.info(f"Found {len(eligible)} eligible companies (with glm_extraction/, without gemini_extraction/)")
    
    # Filter by selection mode
    if args.all:
        selected = eligible
    elif args.ids:
        selected = {cid: path for cid, path in eligible.items() if cid in args.ids}
        LOG.info(f"Selected {len(selected)} companies by ID")
    elif args.range:
        start, end = map(int, args.range.split('-'))
        ranges = [(start, end)]
        selected = manager.filter_by_range(eligible, ranges)
        LOG.info(f"Selected {len(selected)} companies in range {start}-{end}")
    
    if not selected:
        LOG.warning("No companies selected")
        return
    
    # Filter out companies that already have output.json (unless --force)
    if not args.force:
        pre_filtered = selected
        selected = {
            cid: path for cid, path in selected.items()
            if not (path / "glm_extraction" / "output.json").exists()
        }
        LOG.info(f"After filtering existing outputs: {len(pre_filtered)} → {len(selected)}")
    
    # Process in batches of 150
    selected_list = list(selected.items())
    for batch_num, i in enumerate(range(0, len(selected_list), BATCH_SIZE)):
        batch = dict(selected_list[i:i + BATCH_SIZE])
        
        LOG.info(f"\n{'='*70}")
        LOG.info(f"Batch {batch_num + 1}: {len(batch)} companies")
        LOG.info(f"{'='*70}")
        
        if args.strategy == 'option1':
            manager.extract_option1(batch)
        elif args.strategy == 'option3':
            manager.extract_option3(batch)
        else:
            manager.extract_option2(batch)
    
    # Summary
    LOG.info(f"\n{'='*70}")
    LOG.info(f"FINAL SUMMARY")
    LOG.info(f"{'='*70}")
    LOG.info(f"Total processed: {len(selected)}")
    LOG.info(f"Errors: {len(manager.errors)}")
    
    if manager.errors:
        LOG.info(f"\nErrors:")
        for error in manager.errors[:10]:  # Show first 10
            LOG.info(f"  - {error}")
        if len(manager.errors) > 10:
            LOG.info(f"  ... and {len(manager.errors) - 10} more")


if __name__ == '__main__':
    main()
