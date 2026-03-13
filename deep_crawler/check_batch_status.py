"""
batch_results_processor.py

Processes COMPLETED batch jobs:
1. Download results from output_uri
2. Parse responses
3. Merge split parts if needed
4. Write final_output.json to each company's gemini_extraction/ folder

Usage:
    python check_batch_status.py --batch-dir data/batch_jobs/batch_20251101_140615
    `python check_batch_status.py --all`  # All completed batches
"""

import os
import json
import argparse
import asyncio
from pathlib import Path
from typing import Dict, Tuple, Optional, List
import logging

from google import genai

LOG = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
)

def load_homepage_contacts(outdir: Path) -> Dict:
    """Load external contacts extracted from homepage (regex + JSON-LD)."""
    contacts_file = outdir / "homepage_contacts.json"
    if contacts_file.exists():
        try:
            return json.loads(contacts_file.read_text(encoding='utf-8'))
        except Exception:
            return {}
    return {}

def _fix_truncated_json(json_str: str) -> str:
    """
    Attempt to repair truncated JSON by closing incomplete structures.
    
    Common patterns:
    - Missing closing braces/brackets
    - Incomplete string values
    - Cut-off arrays
    """
    # Remove trailing incomplete data
    json_str = json_str.rstrip()
    
    # Count opening/closing braces and brackets
    open_braces = json_str.count('{')
    close_braces = json_str.count('}')
    open_brackets = json_str.count('[')
    close_brackets = json_str.count(']')
    
    # Check if inside a string (odd number of quotes before truncation)
    in_string = (json_str.count('"') % 2) != 0
    
    # If truncated mid-string, close the string
    if in_string:
        json_str += '"'
    
    # Find last complete key-value pair
    # Look for pattern: "key": value, or "key": "value",
    last_comma = json_str.rfind(',')
    last_colon = json_str.rfind(':')
    
    # If truncated mid-value, try to salvage
    if last_colon > last_comma:
        # Truncated after colon - remove incomplete value
        json_str = json_str[:last_colon].rstrip()
        # Remove trailing comma if present
        if json_str.endswith(','):
            json_str = json_str[:-1]
    
    # Close arrays
    while open_brackets > close_brackets:
        json_str += ']'
        close_brackets += 1
    
    # Close objects
    while open_braces > close_braces:
        json_str += '}'
        close_braces += 1
    
    return json_str

def merge_external_contacts_into_result(result_dict: Dict, external_contacts: Dict) -> Dict:
    """
    Merge external contacts (regex, phonenumbers, JSON-LD) into LLM extraction result.
    
    Same logic as GLM in orchestrator.py lines 200-230.
    """
    if not isinstance(result_dict, dict):
        LOG.error(f"merge_external_contacts_into_result: Expected dict, got {type(result_dict).__name__}")
        # If it's a list, extract first element
        if isinstance(result_dict, list) and len(result_dict) > 0:
            result_dict = result_dict[0]
            LOG.warning(f"Converted list to dict by taking first element")
        else:
            # Return as-is if we can't fix it
            return result_dict
        
    if not external_contacts:
        return result_dict
    
    # Ensure company key exists
    result_dict.setdefault('company', {})
    
    # Name precedence: LLM first, then JSON-LD
    if not result_dict['company'].get('name') and external_contacts.get('jsonld_name'):
        result_dict['company']['name'] = external_contacts['jsonld_name']
    
    # Address precedence: JSON-LD first, then keep LLM
    if external_contacts.get('jsonld_address'):
        result_dict['company']['address'] = external_contacts['jsonld_address']
    
    # Contacts: merge homepage (HTML + JSON-LD) + LLM, dedupe
    phones_list = []
    emails_list = []
    
    # Add external contacts
    phones_list.extend(external_contacts.get('phones_raw', []))
    phones_list.extend(external_contacts.get('jsonld_phones', []))
    emails_list.extend(external_contacts.get('emails_raw', []))
    emails_list.extend(external_contacts.get('jsonld_emails', []))
    
    # Add LLM contacts if present
    if result_dict['company'].get('phone'):
        phones_list.append(result_dict['company']['phone'])
    if result_dict['company'].get('email'):
        emails_list.append(result_dict['company']['email'])
    
    # Dedupe function
    def dedup(seq, key=lambda x: x):
        seen, out = set(), []
        for s in (t.strip() for t in seq if t):
            k = key(s)
            if k not in seen:
                seen.add(k)
                out.append(s)
        return out
    
    # Dedupe and join with comma-space
    phones_csv = ', '.join(dedup(phones_list))
    emails_csv = ', '.join(dedup(emails_list, key=lambda x: x.lower()))
    
    # Update result with merged contacts
    if phones_csv:
        result_dict['company']['phone'] = phones_csv
    if emails_csv:
        result_dict['company']['email'] = emails_csv
    
    return result_dict


class BatchResultsProcessor:
    """Download and process completed batch results."""
    
    def __init__(self):
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set")
        self.client = genai.Client(api_key=api_key)
    
    def get_batch_job_name(self, batch_dir: Path) -> Optional[str]:
        """Read batch job name from batch_job_name.txt."""
        batch_name_file = batch_dir / "batch_job_name.txt"
        if not batch_name_file.exists():
            LOG.warning(f"No batch_job_name.txt in {batch_dir}")
            return None
        
        batch_name = batch_name_file.read_text(encoding='utf-8').strip()
        return batch_name if batch_name else None
    
    def get_company_manifest(self, batch_dir: Path) -> Dict:
        """Read company_manifest.json from batch directory."""
        manifest_file = batch_dir / "company_manifest.json"
        if not manifest_file.exists():
            LOG.warning(f"No company_manifest.json in {batch_dir}")
            return {}
        
        try:
            return json.loads(manifest_file.read_text(encoding='utf-8'))
        except json.JSONDecodeError as e:
            LOG.error(f"Invalid JSON in company_manifest.json: {e}")
            return {}
    
    def check_batch_completion(self, batch_job_name: str) -> Tuple[bool, Optional[str]]:
        """
        Check if batch is completed and return output file reference.
        
        Returns:
            (is_completed, file_name_or_uri)
        """
        try:
            batch = self.client.batches.get(name=batch_job_name)
            state = getattr(batch.state, 'name', str(batch.state))
            
            if state != 'JOB_STATE_SUCCEEDED':
                LOG.info(f"Batch {batch_job_name} state: {state} (not ready yet)")
                return False, None
            
            # ============================================================
            # FIX: Use batch.dest.file_name (NEW Gemini Batch API format)
            # ============================================================
            output_file_name = None
            
            # Try to get file_name from dest (file-based batch)
            if hasattr(batch, 'dest') and batch.dest:
                output_file_name = getattr(batch.dest, 'file_name', None)
                if output_file_name:
                    LOG.info(f"Found result file: {output_file_name}")
                    return True, output_file_name
                
                # Check for inline responses (rare for large batches)
                elif hasattr(batch.dest, 'inlined_responses') and batch.dest.inlined_responses:
                    LOG.info("Results are inline (not file-based)")
                    return True, "INLINE"
            
            # Legacy fallback for older API versions
            output_uri = getattr(batch, 'output_uri', None) or \
                         getattr(batch, 'result_output_uri', None) or \
                         getattr(batch, 'outputUri', None)
            
            if output_uri:
                LOG.info(f"Found legacy output_uri: {output_uri}")
                return True, output_uri
            
            # If nothing found, log diagnostic info
            LOG.error(f"No output found on BatchJob")
            LOG.error(f"batch.dest: {getattr(batch, 'dest', None)}")
            LOG.error(f"Available batch attributes: {[a for a in dir(batch) if not a.startswith('_')]}")
            return False, None
        
        except Exception as e:
            LOG.error(f"Failed to query batch {batch_job_name}: {e}")
            import traceback
            LOG.error(traceback.format_exc())
            return False, None
    
    def download_results(self, output_reference: str, batch_dir: Path) -> Optional[Path]:
        """
        Download batch results using file reference from batch.dest.file_name.
        
        Args:
            output_reference: Either file name (e.g., "files/abc123"), URI, or "INLINE"
            batch_dir: Directory to save results
        """
        responses_jsonl = batch_dir / "batch_responses.jsonl"
        
        if responses_jsonl.exists():
            LOG.info(f"Results already downloaded: {responses_jsonl}")
            return responses_jsonl
        
        if not output_reference or output_reference == "INLINE":
            LOG.error(f"Cannot download: invalid reference '{output_reference}'")
            return None
        
        LOG.info(f"Downloading results from {output_reference}...")
        
        try:
            # ============================================================
            # Method 1: Files API download (NEW format: files/xyz)
            # ============================================================
            if output_reference.startswith('files/'):
                LOG.info(f"Using Files API to download: {output_reference}")
                
                try:
                    # Download using SDK's files.download method
                    file_content = self.client.files.download(file=output_reference)
                    
                    # Write to file
                    responses_jsonl.write_bytes(file_content)
                    LOG.info(f"✅ Downloaded {len(file_content)} bytes via Files API")
                    return responses_jsonl
                
                except Exception as e:
                    LOG.error(f"Files API download failed: {e}")
                    # Fallback: try constructing HTTP URL
                    file_id = output_reference.split('/')[-1]
                    https_url = f"https://generativelanguage.googleapis.com/v1beta/files/{file_id}"
                    LOG.info(f"Trying fallback URL: {https_url}")
            
            # ============================================================
            # Method 2: Direct HTTP download
            # ============================================================
            elif output_reference.startswith('http'):
                https_url = output_reference
            
            # ============================================================
            # Method 3: GCS URI (gs://bucket/...)
            # ============================================================
            elif output_reference.startswith('gs://'):
                https_url = output_reference.replace('gs://', 'https://storage.googleapis.com/')
                LOG.info(f"Converting GCS URI to HTTPS: {https_url}")
            
            else:
                LOG.error(f"Unknown output reference format: {output_reference}")
                return None
            
            # Download via requests
            import requests
            response = requests.get(https_url, timeout=300, stream=True)
            response.raise_for_status()
            
            # Download with progress
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            chunk_size = 1024 * 1024  # 1 MB chunks
            
            with responses_jsonl.open('wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size:
                            pct = (downloaded / total_size) * 100
                            LOG.debug(f"Downloaded {downloaded}/{total_size} bytes ({pct:.1f}%)")
            
            LOG.info(f"✅ Downloaded {downloaded} bytes to {responses_jsonl}")
            return responses_jsonl
        
        except Exception as e:
            LOG.error(f"Failed to download results: {e}")
            import traceback
            LOG.error(traceback.format_exc())
            return None
    
    def parse_and_distribute_results(
        self,
        responses_jsonl: Path,
        company_manifest: Dict,
        batch_dir: Path
    ) -> Dict[int, Dict]:
        """
        Parse batch responses and write final_output.json to each company.
        
        Handles:
        - Single requests (no split)
        - Split requests (part1 + part2) that need merging
        """
        LOG.info(f"Parsing {responses_jsonl}...")
        
        results = {}
        split_results = {}  # Track parts: {company_id: [part1_result, part2_result]}
        errors = []
        
        if not responses_jsonl.exists():
            LOG.error(f"Responses file not found: {responses_jsonl}")
            return results
        
        with responses_jsonl.open('r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if not line.strip():
                    continue
                
                try:
                    response = json.loads(line)
                    # Gemini Batch API uses 'key' field for request identifier
                    custom_id = response.get('key', '')

                    if not custom_id:
                        LOG.error(f"Line {line_num}: Missing 'key' field in response")
                        LOG.error(f"Response structure: {json.dumps(response, indent=2)[:500]}")
                        errors.append({"line": line_num, "error": "Missing key field"})
                        continue

                    # Extract text content from response
                    try:
                        content = response['response']['candidates'][0]['content']['parts'][0]['text']
                        result_dict = json.loads(content)
                        if isinstance(result_dict, list):
                            if len(result_dict) == 0:
                                LOG.error(f"Line {line_num}: Empty array in response for key {custom_id}")
                                errors.append({"key": custom_id, "error": "Empty array response"})
                                continue
                            # Take first element from array (most common: single company per request)
                            result_dict = result_dict[0]
                            LOG.warning(f"Line {line_num}: Unwrapped array response for key {custom_id}. Got first item.")
                        
                        # Now result_dict is guaranteed to be a dict
                        if not isinstance(result_dict, dict):
                            LOG.error(f"Line {line_num}: Expected dict or list, got {type(result_dict).__name__}")
                            errors.append({"key": custom_id, "error": f"Invalid type: {type(result_dict).__name__}"})
                            continue
                        
                    except json.JSONDecodeError as e:
                        LOG.error(f"Line {line_num}: Failed to extract content: {e}")
                        
                        # Attempt to fix truncated JSON
                        LOG.warning(f"Attempting to repair truncated JSON for key {custom_id}...")
                        
                        try:
                            # Strategy 1: Find last complete object and close it
                            fixed_json = _fix_truncated_json(content)
                            result_dict = json.loads(fixed_json)
                            LOG.info(f"✅ Successfully repaired truncated JSON for key {custom_id}")
                        
                        except Exception as repair_error:
                            LOG.error(f"❌ JSON repair failed: {repair_error}")
                            
                            # Save malformed response for manual inspection
                            error_file = batch_dir / f'error_key_{custom_id}_malformed.json'
                            error_file.write_text(content, encoding='utf-8')
                            LOG.error(f"Malformed JSON saved to: {error_file}")
                            LOG.error(f"Response preview: {content[:500]}")
                            
                            errors.append({
                                "key": custom_id, 
                                "error": str(e),
                                "saved_to": str(error_file)
                            })
                            continue
                    
                    # Handle split parts
                    if '_part1' in custom_id or '_part2' in custom_id:
                        # Extract base ID and part number
                        if '_part1' in custom_id:
                            base_id = custom_id.replace('_part1', '')
                            part_idx = 0
                        else:
                            base_id = custom_id.replace('_part2', '')
                            part_idx = 1
                        
                        cid_str = base_id
                        
                        if cid_str not in company_manifest:
                            LOG.error(f"Custom ID {base_id} not in manifest")
                            errors.append({"custom_id": custom_id, "error": "Not in manifest"})
                            continue
                        
                        cid, out_dir_str = company_manifest[cid_str][:2]
                        out_dir = Path(out_dir_str)
                        
                        # Store split part
                        if cid not in split_results:
                            split_results[cid] = [None, None]
                        
                        split_results[cid][part_idx] = result_dict
                        LOG.debug(f"Company {cid}: Got split part {part_idx + 1}/2")
                        
                        # Check if both parts received
                        if all(split_results[cid]):
                            # Merge parts
                            try:
                                merged = self._merge_split_results(
                                    split_results[cid][0],
                                    split_results[cid][1]
                                )
                                
                                # ============================================================
                                # MERGE: Load external contacts and merge with LLM result
                                # ============================================================
                                external_contacts = load_homepage_contacts(out_dir)
                                merged = merge_external_contacts_into_result(merged, external_contacts)
                                
                                gemini_dir = out_dir / 'gemini_extraction'
                                gemini_dir.mkdir(parents=True, exist_ok=True)
                                
                                output_file = gemini_dir / 'final_output.json'
                                output_file.write_text(
                                    json.dumps(merged, ensure_ascii=False, indent=2),
                                    encoding='utf-8'
                                )
                                
                                results[cid] = merged
                                LOG.info(f"✅ Company {cid}: Merged split results → {output_file}")
                            
                            except Exception as merge_err:
                                LOG.error(f"Failed to merge parts for {cid}: {merge_err}")
                                errors.append({"company_id": cid, "error": f"Merge failed: {merge_err}"})
                    
                    else:
                        # Single request (no split)
                        cid_str = custom_id
                        
                        if cid_str not in company_manifest:
                            LOG.error(f"Custom ID {cid_str} not in manifest")
                            errors.append({"custom_id": custom_id, "error": "Not in manifest"})
                            continue
                        
                        cid, out_dir_str = company_manifest[cid_str][:2]
                        out_dir = Path(out_dir_str)
                        
                        # ============================================================
                        # MERGE: Load external contacts and merge with LLM result
                        # ============================================================
                        external_contacts = load_homepage_contacts(out_dir)
                        result_dict = merge_external_contacts_into_result(result_dict, external_contacts)
                        
                        gemini_dir = out_dir / 'gemini_extraction'
                        gemini_dir.mkdir(parents=True, exist_ok=True)
                        
                        output_file = gemini_dir / 'final_output.json'
                        output_file.write_text(
                            json.dumps(result_dict, ensure_ascii=False, indent=2),
                            encoding='utf-8'
                        )
                        
                        results[cid] = result_dict
                        LOG.info(f"✅ Company {cid}: Extracted → {output_file}")

                
                except json.JSONDecodeError as e:
                    LOG.error(f"Line {line_num}: Invalid JSON: {e}")
                    errors.append({"line": line_num, "error": str(e)})
                except Exception as e:
                    LOG.exception(f"Line {line_num}: Unexpected error: {e}")
                    errors.append({"line": line_num, "error": str(e)})
        
        # Report summary
        LOG.info(f"\n{'='*70}")
        LOG.info(f"Parse Summary:")
        LOG.info(f"  Successful: {len(results)}")
        LOG.info(f"  Errors: {len(errors)}")
        if errors:
            LOG.warning(f"  Failed lines: {[e.get('line') or e.get('custom_id') for e in errors]}")
            LOG.warning(f"\n{'='*70}")
            LOG.warning(f"⚠️  {len(errors)} companies had parsing errors")
            LOG.warning(f"{'='*70}")
            
            # Extract company IDs that failed
            failed_keys = [err.get('key') for err in errors if err.get('key')]
            
            if failed_keys:
                LOG.info(f"Failed company IDs: {failed_keys}")
                LOG.info(f"\nTo re-extract these companies:")
                LOG.info(f"1. Check error files in {batch_dir}")
                LOG.info(f"2. If JSON is truncated, the company may have too much data")
                LOG.info(f"3. Re-run with lower SPLIT_THRESHOLD in gemini_batch_manager.py")
                
                # Save failed IDs for retry
                failed_file = batch_dir / 'failed_companies.json'
                failed_file.write_text(json.dumps(failed_keys, indent=2), encoding='utf-8')
                LOG.info(f"\nFailed company IDs saved to: {failed_file}")
        
        return results
    
    def _merge_split_results(self, part1: Dict, part2: Dict) -> Dict:
        """
        Merge two split extraction results.
        
        For now, just concatenate arrays/lists and merge dicts.
        Customize this based on your schema.
        """
        merged = part1.copy()
        
        for key, value in part2.items():
            if key not in merged:
                merged[key] = value
            elif isinstance(merged[key], list) and isinstance(value, list):
                # Concatenate lists
                merged[key].extend(value)
            elif isinstance(merged[key], dict) and isinstance(value, dict):
                # Merge dicts (value takes precedence)
                merged[key].update(value)
            else:
                # Part 2 overwrites part 1 for other types
                merged[key] = value
        
        return merged
    
    async def process_batch_dir(self, batch_dir: Path) -> Dict:
        """Process a single batch directory."""
        LOG.info(f"\n{'='*70}")
        LOG.info(f"Processing batch: {batch_dir.name}")
        LOG.info(f"{'='*70}")
        
        batch_job_name = self.get_batch_job_name(batch_dir)
        if not batch_job_name:
            LOG.error(f"Cannot find batch job name in {batch_dir}")
            return {"status": "ERROR", "error": "No batch_job_name.txt"}
        
        # Check if completed
        is_completed, output_reference = self.check_batch_completion(batch_job_name)
        if not is_completed:
            LOG.warning(f"Batch not completed yet, skipping")
            return {"status": "PENDING", "batch_id": batch_job_name}
        
        if not output_reference:
            LOG.error(f"Batch completed but no output reference found")
            return {"status": "ERROR", "error": "No output reference"}
        
        LOG.info(f"✅ Batch completed! Output reference: {output_reference}")
        
        # Download results
        responses_file = self.download_results(output_reference, batch_dir)
        if not responses_file:
            return {"status": "ERROR", "error": "Download failed"}
        
        # Parse and distribute
        company_manifest = self.get_company_manifest(batch_dir)
        results = self.parse_and_distribute_results(responses_file, company_manifest, batch_dir)
        
        return {
            "status": "SUCCESS",
            "batch_id": batch_job_name,
            "companies_processed": len(results),
            "results": results
        }
    
    async def process_all_batches(self, base_dir: Path = Path("data/batch_jobs")) -> List[Dict]:
        """Process all completed batches."""
        results = []
        
        if not base_dir.exists():
            LOG.error(f"Batch directory not found: {base_dir}")
            return results
        
        batch_dirs = sorted([d for d in base_dir.iterdir() if d.is_dir() and d.name.startswith("batch_")])
        
        if not batch_dirs:
            LOG.warning(f"No batch directories found in {base_dir}")
            return results
        
        LOG.info(f"Found {len(batch_dirs)} batch directories")
        
        for batch_dir in batch_dirs:
            result = await self.process_batch_dir(batch_dir)
            results.append(result)
        
        return results


async def main():
    """Main CLI."""
    parser = argparse.ArgumentParser(description="Process completed batch results")
    parser.add_argument(
        "--batch-dir",
        type=Path,
        default=None,
        help="Process specific batch directory"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process all completed batches"
    )
    
    args = parser.parse_args()
    
    processor = BatchResultsProcessor()
    
    if args.batch_dir:
        if not args.batch_dir.exists():
            LOG.error(f"Batch directory not found: {args.batch_dir}")
            return
        
        result = await processor.process_batch_dir(args.batch_dir)
        LOG.info(f"\nResult: {json.dumps(result, indent=2)}")
    
    elif args.all:
        results = await processor.process_all_batches()
        
        # Summary
        success = sum(1 for r in results if r.get('status') == 'SUCCESS')
        pending = sum(1 for r in results if r.get('status') == 'PENDING')
        error = sum(1 for r in results if r.get('status') == 'ERROR')
        
        LOG.info(f"\n{'='*70}")
        LOG.info(f"FINAL SUMMARY:")
        LOG.info(f"  ✅ Processed: {success}")
        LOG.info(f"  ⏳ Pending: {pending}")
        LOG.info(f"  ❌ Errors: {error}")
        LOG.info(f"  Total: {len(results)}")
        LOG.info(f"{'='*70}\n")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
