"""
Gemini Batch Manager for Multi-Company Whole-Website Extraction

Accumulates multiple companies and submits as single Batch API job.
Uses existing caching methodology from gemini_batchs.py.
15-30x faster than per-company batching!

Key Features:
- Reuses existing cache system (unified_cache_map.json)
- Smart delimiter approach for split requests (no extra caches)
- Whole-website extraction (not per-page)
- Multi-company batching (60 companies → 1 batch job)
"""
import time
from pathlib import Path
from typing import Dict, Any, List, Tuple
import json
import asyncio
import os
import hashlib
from datetime import datetime, timezone
from google import genai
from google.genai import types

from crawler.token_utils import estimate_tokens_with_overhead
from crawler.page_utils import concatenate_pages, split_pages_in_half
from crawler.merge_utils import merge_split_extractions
from crawler.gemini_prompts import build_prompt_pack_file

import logging
LOG = logging.getLogger('gemini_batch_manager')

# Cache directory (same as existing gemini_batchs.py)
CACHE_DIR = Path("data") / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


class GeminiBatchManager:
    """
    Manages Gemini Batch API for multiple companies.
    Accumulates companies, submits as single batch, distributes results.
    """

    def __init__(self, api_key: str | None = None):
        """
        Initialize Gemini Batch Manager.

        Args:
            api_key: Gemini API key (defaults to GEMINI_API_KEY env var)
        """
        if api_key is None:
            api_key = os.environ.get('GEMINI_API_KEY')

        if not api_key:
            raise ValueError('GEMINI_API_KEY not found')

        self.client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
        LOG.info('Gemini Batch Manager initialized')

    def _get_or_create_cache(self, ttl_hours: int = 168) -> str:
        """
        Get or create cache. When prompt changes (hash changes):
        1. Delete old cache if hash changed
        2. Create new cache with new content
        Only TTL is updatable in Gemini cached content API.
        """
        from google import genai
        from google.genai import types
        
        pack_path = build_prompt_pack_file()
        pack_text = pack_path.read_text(encoding='utf-8')
        client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
        
        # Compute hash of prompt pack
        pack_hash = hashlib.sha256(pack_text.encode('utf-8')).hexdigest()[:16]
        
        # Check cache map
        cache_map_file = CACHE_DIR / "unified_cache_map.json"
        if cache_map_file.exists():
            cache_map = json.loads(cache_map_file.read_text(encoding='utf-8'))
        else:
            cache_map = {}
        
        # If hash changed, DELETE old cache (cannot update content, only TTL)
        if pack_hash in cache_map:
            # Hash EXISTS = prompt UNCHANGED = REUSE
            cache_name = cache_map[pack_hash]
            try:
                client.caches.get(name=cache_name)
                LOG.info(f'Reusing cache: {cache_name}')
                return cache_name  # ← REUSE, don't create new
            except:
                pass  # Cache expired, create new

        # Delete OLD caches with DIFFERENT hashes
        for old_hash, old_name in list(cache_map.items()):
            try:
                client.caches.delete(name=old_name)
                LOG.info(f'Deleted old cache: {old_name} (hash: {old_hash})')
            except Exception as e:
                LOG.warning(f'Could not delete old cache {old_name}: {e}')
            del cache_map[old_hash]  # Remove from map regardless
        
        # Create NEW cache with NEW prompt pack
        LOG.info(f'Creating new cache (hash: {pack_hash})')
        
        # Upload prompt pack file
        uploaded = client.files.upload(file=str(pack_path))
        LOG.info(f'Uploaded prompt pack: {uploaded.uri}')
        
        cfg = types.CreateCachedContentConfig(
            display_name=f"whole_website_prompt_{pack_hash}",
            system_instruction=pack_text,          # full JSON prompt pack text (system + schema)
            ttl=f"{ttl_hours*3600}s",
        )
        cache = client.caches.create(model="models/gemini-2.0-flash-001", config=cfg)
        cache_name = cache.name

        LOG.info(f'Created cache: {cache_name}')
        
        # Save NEW cache to map
        cache_map[pack_hash] = cache_name
        cache_map_file.write_text(json.dumps(cache_map, ensure_ascii=False, indent=2), encoding='utf-8')
        
        return cache_name

    async def batch_extract_companies(
        self,
        companies: List[Tuple[int, str, List[Dict[str, str]], Path]]
    ) -> Dict[int, Dict[str, Any]]:
        """
        Extract data for multiple companies in a single Batch API job.

        Args:
            companies: List of (company_id, domain, pages, output_dir) tuples

        Returns:
            Dict mapping company_id → extraction result

        Flow:
            1. Get/create shared cache (prompt pack)
            2. Build JSONL with all company requests
            3. Submit single Batch job
            4. Poll for completion
            5. Download results
            6. Parse and distribute to individual company directories
        """
        if not companies:
            LOG.warning('No companies to batch extract')
            return {}

        LOG.info(f'Starting batch extraction for {len(companies)} companies')

        # Create batch directory
        batch_timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        batch_dir = Path('data/batch_jobs') / f'batch_{batch_timestamp}'
        batch_dir.mkdir(parents=True, exist_ok=True)

        # Get or create cache (shared across all companies)
        cache_name = self._get_or_create_cache()

        # Build JSONL and manifest
        requests_jsonl, company_manifest = self._build_batch_jsonl(
            companies,
            cache_name,
            batch_dir
        )

        # Submit batch
        batch_job_name = await self._submit_batch(requests_jsonl, batch_dir)
        (batch_dir / 'batch_job_name_for_recovery.txt').write_text(batch_job_name, encoding='utf-8')
        LOG.info(f'Batch job name saved for recovery: {batch_job_name}')

        LOG.info(f'✅ Batch submitted: {batch_job_name}')

        return [{"success": True, "company_id": cid, "batch_id": batch_job_name} 
            for cid, domain, pages, out_dir in companies]
    
    def _build_batch_jsonl(
    self,
    companies: List[Tuple[int, str, List[Dict], Path]],
    cache_name: str,
    batch_dir: Path
) -> Tuple[Path, Dict]:
        """Build JSONL matching the PROVEN pattern from gemini_batchs.py"""
        LOG.info(f"Building JSONL for {len(companies)} companies")
        requests_jsonl = batch_dir / "batch_requests.jsonl"
        company_manifest: Dict[str, Tuple[int, str, bool]] = {}
        
        from crawler.gemini_prompts import get_whole_website_prompt
        system_prompt, schema = get_whole_website_prompt(is_split=False)
        schema_json = json.dumps(schema, ensure_ascii=False)

        lines: List[str] = []
        for cid, domain, pages, out_dir in companies:
            concatenated_text = concatenate_pages(pages)

            total_tokens = estimate_tokens_with_overhead(concatenated_text, system_prompt, schema_json)

            gemini_dir = out_dir / 'gemini_extraction'
            gemini_dir.mkdir(parents=True, exist_ok=True)
            (gemini_dir / 'merged_input.txt').write_text(concatenated_text, encoding='utf-8')

            if total_tokens > 128000:
                LOG.info(f"Company {cid} exceeds 128K tokens ({total_tokens}) - splitting.")
                pages_part1, pages_part2 = split_pages_in_half(pages)
                
                part1_text = "[EXTRACTION_CONTEXT: SPLIT_PART=1/2]\n\n" + concatenate_pages(pages_part1)
                (gemini_dir / 'merged_input_part1.txt').write_text(part1_text, encoding='utf-8')
            
                
                req1 = {
                    "key": f"{cid}_part1",
                    "request": {
                        "model": "models/gemini-2.0-flash-001",
                        "contents": [{
                            "role": "user",
                            "parts": [
                                {"text": f"SITE_META: domain={domain} company_id={cid} split=1/2"},
                                {"text": f"SITE_TEXT:\n{part1_text}"}
                            ]
                        }],
                        "cachedContent": cache_name,
                        "generation_config": {
                            "response_mime_type": "application/json",
                            "temperature": 0.0
                        }
                    }
                }
                line1 = json.dumps(req1, ensure_ascii=False)
                # Defensive validation (same for req1/req2 lines):
                json.loads(line1)
                lines.append(line1)
                part2_text = "[EXTRACTION_CONTEXT: SPLIT_PART=2/2]\n\n" + concatenate_pages(pages_part2)
                (gemini_dir / 'merged_input_part2.txt').write_text(part2_text, encoding='utf-8')
                
                req2 = {
                    "key": f"{cid}_part2",
                    "request": {
                        "model": "models/gemini-2.0-flash-001",
                        "contents": [{
                            "role": "user",
                            "parts": [
                                {"text": f"SITE_META: domain={domain} company_id={cid} split=2/2"},
                                {"text": f"SITE_TEXT:\n{part2_text}"}
                            ]
                        }],
                        "cachedContent": cache_name,
                        "generation_config": {
                            "response_mime_type": "application/json",
                            "temperature": 0.0
                        }
                    }
                }
                line2 = json.dumps(req2, ensure_ascii=False)
                # Defensive validation (same for req1/req2 lines):
                json.loads(line2)
                lines.append(line2)
                
                company_manifest[str(cid)] = (cid, str(out_dir), True)
            else:
                LOG.info(f"Company {cid}: {total_tokens} tokens (no split needed)")
                
                req = {
                    "key": str(cid),
                    "request": {
                        "model": "models/gemini-2.0-flash-001",
                        "contents": [{
                            "role": "user",
                            "parts": [
                                {"text": f"SITE_META: domain={domain} company_id={cid}"},
                                {"text": f"SITE_TEXT:\n{concatenated_text}"}
                            ]
                        }],
                        "cachedContent": cache_name,
                        "generation_config": {
                            "response_mime_type": "application/json",
                            "temperature": 0.0
                        }
                    }
                }
                line = json.dumps(req, ensure_ascii=False)
                # Defensive validation (same for req1/req2 lines):
                try:
                    json.loads(line)
                    print("success")
                except :
                    print("failed")
                lines.append(line)
                company_manifest[str(cid)] = (cid, str(out_dir), False)

        # Write all lines without empty lines
        requests_jsonl.write_text('\n'.join(lines), encoding='utf-8')
        
        (batch_dir / 'company_manifest.json').write_text(json.dumps(company_manifest, ensure_ascii=False, indent=2), encoding='utf-8')
        LOG.info(f'Built JSONL with proven pattern: {requests_jsonl}')
        return requests_jsonl, company_manifest


    async def _submit_batch(self, requests_jsonl: Path, batch_dir: Path) -> str:
        """Submit batch job to Gemini Batch API."""

        client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
        LOG.info('Preparing to upload batch file...')
        if not requests_jsonl.exists():
            raise FileNotFoundError(f"JSONL file not found: {requests_jsonl}")

        p = Path(requests_jsonl)
        data = p.read_bytes()
        file_size = p.stat().st_size
        if file_size == 0:
            raise ValueError("JSONL file is empty!")
        
        with p.open('r', encoding='utf-8') as f:
            first_line = f.readline().rstrip('\r\n')
        LOG.info(f"FIRST LINE repr: {first_line[:500]!r}")
        
        
        try:
            # 1) Upload file — mirror gemini_batch.py exactly
            uploaded_file = client.files.upload(
                file=str(p),
                config=types.UploadFileConfig(
                    display_name=p.name,
                    mime_type="application/json"
                )
            )
        
            LOG.info(f"Batch file uploaded: {uploaded_file.name}")
        except Exception as e:
            LOG.warning("Upload failed with application/jsonl")
            
            LOG.error("All upload attempts failed")
            # Best-possible error introspection without mutating data
            resp1 = getattr(e, "response", None)
            LOG.error(f"e: {type(e).__name__} {str(e)} resp={resp1}")
            raise

        LOG.info(f"Batch file uploaded: {getattr(uploaded_file, 'name', '<unknown>')}")

        import time
        time.sleep(2)
        # Rest of the method...
        LOG.info("Proceeding to batch creation...")
        batch_job = client.batches.create(
            model="models/gemini-2.0-flash-001",
            src=uploaded_file.name,
            config={"display_name": p.name}
        )
        
        batch_job_name = getattr(batch_job, 'name', None) or str(batch_job)
        LOG.info(f'Batch job submitted: {batch_job_name}')
        
        (batch_dir / 'batch_job_name.txt').write_text(batch_job_name, encoding='utf-8')
        return batch_job_name


    def _parse_and_distribute_results(
        self,
        responses_jsonl: Path,
        company_manifest: Dict[str, Tuple],
        batch_dir: Path
    ) -> Dict[int, Dict[str, Any]]:
        """Parse batch responses and distribute to company directories."""
        LOG.info('Parsing and distributing results...')

        results = {}
        split_results = {}  # Track split parts: {company_id: [part1_result, part2_result]}

        with responses_jsonl.open('r', encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue

                response = json.loads(line)
                custom_id = response.get('custom_id', '')

                # Check if this is a split part
                if custom_id.endswith('_part1') or custom_id.endswith('_part2'):
                    # Extract company_id and part number
                    base_id = custom_id.rsplit('_part', 1)[0]
                    part_num = 1 if custom_id.endswith('_part1') else 2

                    cid, out_dir_str, is_split = company_manifest[base_id]
                    out_dir = Path(out_dir_str)

                    try:
                        # Extract JSON from response
                        content = response['response']['candidates'][0]['content']['parts'][0]['text']
                        result_dict = json.loads(content)

                        # Store split part
                        if cid not in split_results:
                            split_results[cid] = [None, None]
                        split_results[cid][part_num - 1] = result_dict

                        # Merge if both parts received
                        if all(split_results[cid]):
                            merged = merge_split_extractions(split_results[cid][0], split_results[cid][1])
                            gemini_dir = out_dir / 'gemini_extraction'
                            (gemini_dir / 'final_output.json').write_text(
                                json.dumps(merged, ensure_ascii=False, indent=2),
                                encoding='utf-8'
                            )
                            results[cid] = merged
                            LOG.info(f'Company {cid}: Merged split results')

                    except Exception as e:
                        LOG.exception(f'Failed to parse split result for company {cid}: {e}')

                else:
                    # Single request (non-split)
                    cid, out_dir_str, is_split = company_manifest[custom_id]
                    out_dir = Path(out_dir_str) 
                    
                    try:
                        # Extract JSON from response
                        content = response['response']['candidates'][0]['content']['parts'][0]['text']
                        result_dict = json.loads(content)

                        gemini_dir = out_dir / 'gemini_extraction'
                        (gemini_dir / 'final_output.json').write_text(
                            json.dumps(result_dict, ensure_ascii=False, indent=2),
                            encoding='utf-8'
                        )
                        results[cid] = result_dict
                        LOG.info(f'Company {cid}: Extracted successfully')

                    except Exception as e:
                        LOG.exception(f'Failed to parse result for company {cid}: {e}')

        return results
