# crawler/gemini_batch.py

from __future__ import annotations
import json, time, os, asyncio, re
from pathlib import Path
from typing import Optional, List, Dict
from datetime import datetime, timezone
import traceback

# Load API keys
API_KEYS = [
    os.environ.get("ZHIPUAI_API_KEY1"),
    os.environ.get("ZHIPUAI_API_KEY2"),
    os.environ.get("ZHIPUAI_API_KEY"),
]

API_KEYS = [k for k in API_KEYS if k]
if not API_KEYS:
    raise ValueError("No ZHIPUAI_API_KEY environment variables found. Set ZHIPUAI_API_KEY1, ZHIPUAI_API_KEY2, ZHIPUAI_API_KEY3")

print(f"✓ Loaded {len(API_KEYS)} API keys for parallel processing")


def get_client():
    """Return first client for compatibility"""
    from zhipuai import ZhipuAI
    return ZhipuAI(api_key=API_KEYS[0])


def create_cache(client, ttl_hours: int = 168) -> str:
    """No caching for GLM"""
    print(f"✓ GLM Parallel: {len(API_KEYS)} keys × 30 workers = {len(API_KEYS) * 30} concurrent requests")
    return ""


def build_jsonl(
    company_id: int,
    domain: str,
    cache_name: str,
    cleaned_pages_path: Path,
    triage_kept_path: Path,
    out_jsonl_path: Path
) -> tuple[Path, int]:
    """Build JSONL with JSON-optimized prompts"""
    from crawler.gemini_prompts import get_standard_static_context
    
    system, schema = get_standard_static_context()
    system = system + "\n\nIMPORTANT: You MUST respond with ONLY a valid JSON object. No markdown, no explanations, no text outside the JSON."
    # Load anchors
    anchors = {}
    anc_file = cleaned_pages_path.parent / "homepage_anchors.json"
    if anc_file.exists():
        try:
            arr = json.loads(anc_file.read_text(encoding="utf-8"))
            anchors = {it.get("url"): it.get("anchor") for it in arr if isinstance(it, dict)}
        except Exception:
            pass
    if not triage_kept_path.exists():
        print(f"⚠️ triage_kept.txt not found")
        out_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        out_jsonl_path.write_text("", encoding="utf-8")
        return (out_jsonl_path, 0)
    
    # Load kept URLs
    triage_kept = set(
        line.strip() 
        for line in triage_kept_path.read_text(encoding="utf-8").splitlines() 
        if line.strip()
    )
    
    if not triage_kept:
        print(f"⚠️ triage_kept.txt is empty (0 URLs)")
        out_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        out_jsonl_path.write_text("", encoding="utf-8")
        return (out_jsonl_path, 0)
    
    if not cleaned_pages_path.exists():
        print(f"⚠️ cleaned_pages.ndjson not found")
        out_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        out_jsonl_path.write_text("", encoding="utf-8")
        return (out_jsonl_path, 0)
    
    lines = []
    with cleaned_pages_path.open("r", encoding="utf-8") as f:
        for ln in f:
            page = json.loads(ln)
            url = page.get("url")
            if url not in triage_kept:
                continue
            
            # Truncate to avoid token limits
            body = (page.get("text") or "")[:60000]
            
            # Enhanced prompt for JSON mode
            user_content = (
                f"PAGE_META: url={url} depth={page.get('depth', 0)} anchor={anchors.get(url, '')}\n\n"
                f"PAGE_TEXT:\n{body}\n\n"
                f"Extract structured data according to the schema. Return ONLY valid JSON."
            )
            
            request_id = f"{company_id}::{domain}::{hash(url) & 0xfffffff}"
            
            req = {
                "custom_id": request_id,
                "system": system,
                "user_content": user_content,
                "schema": schema,
                "url": url
            }
            
            lines.append(json.dumps(req, ensure_ascii=False))
    
    out_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    out_jsonl_path.write_text("\n".join(lines), encoding="utf-8")

    request_count = len(lines)
    if request_count > 0:
        print(f"✓ Built JSONL with {request_count} requests: {out_jsonl_path.name}")
    else:
        print(f"⚠️ Built JSONL with 0 requests (no matching URLs)")

    return (out_jsonl_path, request_count)


def submit_batch(client, jsonl_path: Path, display_name: Optional[str] = None) -> str:
    """Submit parallel batch with 6 workers"""
    import uuid
    
    batch_id = f"glm_parallel_{uuid.uuid4().hex[:8]}_{int(time.time())}"
    output_path = jsonl_path.parent / "batch_output.ndjson"
    
    workers_per_key = 30  # ← CHANGED: 30 concurrent per key
    total_workers = len(API_KEYS) * workers_per_key
    
    print(f"\n{'='*70}")
    print(f"Starting batch: {jsonl_path.name}")
    print(f"Workers: {total_workers} ({len(API_KEYS)} keys × {workers_per_key} workers/key)")
    print(f"{'='*70}\n")
    
    try:
        results = asyncio.run(_process_parallel(jsonl_path, output_path, workers_per_key))
    except KeyboardInterrupt:
        print("\n✗ Interrupted by user")
        raise
    except Exception as e:
        print(f"✗ FATAL ERROR: {repr(e)}")
        traceback.print_exc()
        raise
    
    # Write metadata
    metadata = {
        "batch_id": batch_id,
        "state": "SUCCEEDED" if results["errors"] == 0 else "PARTIAL",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "total": results["total"],
        "success": results["success"],
        "errors": results["errors"],
        "output_file": str(output_path),
        "mode": "parallel_multi_key",
        "api_keys": len(API_KEYS),
        "workers_per_key": workers_per_key,
        "total_workers": total_workers,
        "elapsed_seconds": results["elapsed"]
    }
    
    (jsonl_path.parent / "batch_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    
    # Summary
    print(f"\n{'='*70}")
    print(f"✓ Batch complete: {results['success']} success, {results['errors']} errors")
    print(f"✓ Time: {results['elapsed']:.1f}s")
    if results['total'] > 0 and results['elapsed'] > 0:
        print(f"✓ Speed: {results['total'] / results['elapsed']:.2f} req/sec")
    print(f"{'='*70}\n")
    
    return batch_id


async def _process_parallel(jsonl_path: Path, output_path: Path, workers_per_key: int) -> Dict:
    """Core parallel processing engine with advanced features"""
    
    # Load requests
    with jsonl_path.open("r", encoding="utf-8") as f:
        requests = [json.loads(line) for line in f if line.strip()]
    
    total = len(requests)
    if total == 0:
        print("⚠️ No requests to process")
        return {"total": 0, "success": 0, "errors": 0, "elapsed": 0}
    
    success_count = 0
    error_count = 0
    
    # Per-key semaphores (2 concurrent per key)
    semaphores = {key: asyncio.Semaphore(workers_per_key) for key in API_KEYS}
    
    # Advanced rate limit tracking per key
    rate_limit_state = {
        key: {
            "hits": 0,
            "last_hit_time": 0,
            "total_requests": 0,
            "total_errors": 0
        }
        for key in API_KEYS
    }
    
    counter_lock = asyncio.Lock()
    start_time = time.time()
    
    async def process_single(req: Dict, idx: int) -> Dict:
        """Process single request with full error handling"""
        nonlocal success_count, error_count
        
        # Round-robin API key assignment
        key_idx = idx % len(API_KEYS)
        api_key = API_KEYS[key_idx]
        semaphore = semaphores[api_key]
        key_state = rate_limit_state[api_key]
        
        custom_id = req.get("custom_id")
        system = req.get("system")
        user_content = req.get("user_content")
        url = req.get("url", "")
        
        async with semaphore:
            # Adaptive backoff for rate limits
            if key_state["hits"] > 0:
                backoff = min(30.0, 2.0 * (2 ** key_state["hits"]))
                time_since_hit = time.time() - key_state["last_hit_time"]
                if time_since_hit < backoff:
                    await asyncio.sleep(backoff - time_since_hit)
            
            # Progress indicator
            progress = f"[{idx}/{total}] ({idx*100//total}%)"
            key_label = f"[Key{key_idx+1}]"
            print(f"  {progress} {key_label} Processing: {custom_id}")
            
            key_state["total_requests"] += 1
            
            try:
                # Execute API call in thread pool
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: _sync_api_call(api_key, system, user_content)
                )
                
                content = response.choices[0].message.content.strip()
                
                # Enhanced JSON extraction (handles markdown, mixed text)
                json_match = re.search(r'``````', content, re.DOTALL)
                if json_match:
                    content = json_match.group(1)
                else:
                    # Fallback: extract first JSON object
                    json_match = re.search(r'\{.*\}', content, re.DOTALL)
                    if json_match:
                        content = json_match.group(0)
                
                # Validate JSON
                try:
                    parsed = json.loads(content)
                except json.JSONDecodeError as je:
                    error_msg = f"JSON parse error: {str(je)[:200]}"
                    print(f"    ⚠️ JSON parse error: {str(je)[:80]}")
                    await loop.run_in_executor(None, lambda: _log_failure(custom_id, url, f"{error_msg} | Raw response: {content[:500]}"))
                    
                    async with counter_lock:
                        error_count += 1
                        key_state["total_errors"] += 1
                    return {
                        "custom_id": custom_id,
                        "error": {
                            "message": "Invalid JSON",
                            "parse_error": str(je)[:200],
                            "response_preview": content[:300]
                        }
                    }
                
                # Success - decay rate limit counter
                key_state["hits"] = max(0, key_state["hits"] - 1)
                
                async with counter_lock:
                    success_count += 1
                
                # Return in expected format
                return {
                    "custom_id": custom_id,
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
                error_str = str(e).lower()
                
                # Detect rate limiting
                if "rate" in error_str or "429" in error_str or "quota" in error_str or "限流" in error_str:
                    key_state["hits"] += 1
                    key_state["last_hit_time"] = time.time()
                    print(f"    ⚠️ RATE LIMIT (Key{key_idx+1}) - hit #{key_state['hits']}")
                
                print(f"    ✗ ERROR: {repr(e)[:150]}")
                
                # Log failure
                await loop.run_in_executor(None, lambda: _log_failure(custom_id, url, str(e)))
                
                async with counter_lock:
                    error_count += 1
                    key_state["total_errors"] += 1
                
                return {
                    "custom_id": custom_id,
                    "error": {"message": str(e)[:1000], "type": type(e).__name__}
                }
    
    # Execute all requests in parallel
    print(f"Starting parallel processing: {total} requests\n")
    
    tasks = [process_single(req, idx) for idx, req in enumerate(requests, 1)]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    
    # Write output
    output_path.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in results),
        encoding="utf-8"
    )
    
    elapsed = time.time() - start_time
    
    # Log per-key statistics
    print(f"\n{'─'*70}")
    print("Per-key statistics:")
    for i, key in enumerate(API_KEYS, 1):
        stats = rate_limit_state[key]
        print(f"  Key {i}: {stats['total_requests']} requests, {stats['total_errors']} errors, {stats['hits']} rate limits")
    print(f"{'─'*70}")
    
    return {
        "total": total,
        "success": success_count,
        "errors": error_count,
        "elapsed": elapsed
    }


def _sync_api_call(api_key: str, system: str, user_content: str):
    """
    Synchronous API call with FULL feature set:
    - response_format: Forces JSON output (no markdown)
    - temperature=0: Deterministic output
    - top_p=0.7: Focused sampling
    - max_tokens=4096: Sufficient for complex extractions
    - do_sample=False: Greedy decoding for consistency
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
        temperature=0.0,           # Deterministic (0=greedy, 1=creative)
        top_p=0.7,                 # Nucleus sampling (0.7=focused)
        max_tokens=4096,           # Max output tokens
        
        # JSON mode (CRITICAL)
        response_format={"type": "json_object"},  # Forces valid JSON output
        
        # Advanced parameters (optional, model-dependent)
        # do_sample=False,         # Greedy decoding (not supported by all models)
        # repetition_penalty=1.0,  # 1.0=no penalty, >1.0=discourage repetition
        # seed=42,                 # For reproducibility (not supported by GLM-4-Flash)
    )
    
    return response


def _log_failure(custom_id: str, url: str, error: str):
    """Thread-safe failure logging to CSV"""
    try:
        parts = custom_id.split("::")
        cid = int(parts[0]) if len(parts) > 0 and parts[0].isdigit() else None
        domain = parts[1] if len(parts) > 1 else "unknown"
        
        FAIL_DIR = Path("data") / "failures"
        FAIL_DIR.mkdir(parents=True, exist_ok=True)
        
        ts = datetime.now(timezone.utc).isoformat(timespec="minutes")
        fail_path = FAIL_DIR / f"failures_{datetime.now(timezone.utc).strftime('%Y%m%d')}.csv"
        
        # Sanitize for CSV
        error_safe = error[:1000].replace('"', "'").replace('\n', ' ')
        url_safe = url[:8000].replace('"', "'").replace('\n', ' ')
        
        new_line = f'{ts},{cid},{domain},glm_parallel_extraction,"{error_safe}","{url_safe}"\n'
        
        if not fail_path.exists():
            fail_path.write_text("timestamp,company_id,domain,stage,error,urls\n", encoding="utf-8")
        
        with fail_path.open("a", encoding="utf-8") as f:
            f.write(new_line)
    except Exception:
        pass  # Silent fail - don't break extraction


def wait_batch(client, batch_name: str, poll_sec: int = 60):
    """Mock batch object for parallel processing (already complete)"""
    
    class MockBatch:
        def __init__(self, name: str):
            self.name = name
            self.state = "SUCCEEDED"
            self.error_message = None
            self.output_file = None
            self.output_files = []
            self.output_uris = []
    
    return MockBatch(batch_name)
