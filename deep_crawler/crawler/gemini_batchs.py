# crawler/gemini_batch.py
from __future__ import annotations
import json, hashlib, time, os
from pathlib import Path
from google import genai
from google.genai import types
from pathlib import Path
from typing import Optional, Tuple
CACHE_DIR = Path("data") / "cache"  # add near top-level
CACHE_DIR.mkdir(parents=True, exist_ok=True)  # ensure exists
from crawler.gemini_prompts import get_standard_static_context, build_prompt_pack_file

def _approx_tokens(s: str) -> int:
    # heuristic ~4 chars/token in English; adjust if needed
    return max(1, len(s) // 4)
def get_client() -> genai.Client:
    # GEMINI_API_KEY must be set in env
    return genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

def create_cache(client: genai.Client, ttl_hours: int = 168) -> str:
    system, schema = get_standard_static_context()
    model_name = "gemini-2.0-flash"

    # Compute approximate token size of cached context
    pack_path = build_prompt_pack_file()  # uses system + pretty schema + examples
    pack_text = pack_path.read_text(encoding="utf-8")
    pack_tokens = _approx_tokens(pack_text)
    print(f"prompt_pack_tokens={pack_tokens}")  # replaces old static_tokens print
    compact_schema_tokens = _approx_tokens(json.dumps(schema, ensure_ascii=False))
    pretty_schema_tokens = _approx_tokens(json.dumps(schema, ensure_ascii=False, indent=2))
    print(f"system={pack_tokens} compact_schema={compact_schema_tokens} pretty_schema={pretty_schema_tokens} pack={pack_tokens}")
    # If below ~4k tokens, skip explicit caching to avoid 400 INVALID_ARGUMENT
    #if static_tokens < 4096:
     #   return ""  # signal caller to inline static context per request

    # Upload the pack and create a cache using a file_data part
    uploaded = client.files.upload(file=str(pack_path))
    
    # Existing: keying and map load
    pack_hash = hashlib.sha256(pack_text.encode("utf-8")).hexdigest()

    key = hashlib.sha256(json.dumps(
        {"model": model_name, "pack_hash": pack_hash},
        sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    print("prompt_pack_sha256=", pack_hash)
    map_path = CACHE_DIR / "unified_cache_map.json"
    cache_map = {}
    if map_path.exists():
        try:
            cache_map = json.loads(map_path.read_text(encoding="utf-8")) or {}
        except Exception:
            cache_map = {}
    if isinstance(cache_map.get(key), str) and cache_map[key]:
        return cache_map[key]

    cache_name = ""
    # Use the typed config if available in your SDK build
    try:
        cfg = types.CreateCachedContentConfig(
            contents=[{
                "role": "user",
                "parts": [{
                    "file_data": {
                        "file_uri": getattr(uploaded, "uri", getattr(uploaded, "name", "")),
                        "mime_type": getattr(uploaded, "mime_type", "text/plain")
                    }
                }]
            }],
            ttl=f"{ttl_hours*3600}s",
        )
        cache = client.caches.create(model=model_name, config=cfg)
        cache_name = getattr(cache, "name", "") or str(cache)
    except Exception:
        # Fallback to dict config for SDK variants that don't expose the typed config
        cache = client.caches.create(
            model=model_name,
            config={
                "contents": [{
                    "role": "user",
                    "parts": [{
                        "file_data": {
                            "file_uri": getattr(uploaded, "uri", getattr(uploaded, "name", "")),
                            "mime_type": getattr(uploaded, "mime_type", "text/plain")
                        }
                    }]
                }],
                "ttl": f"{ttl_hours*3600}s",
            },
        )
        cache_name = getattr(cache, "name", "") or str(cache)
    if cache_name:
        cache_map[key] = cache_name
        map_path.write_text(json.dumps(cache_map, ensure_ascii=False, indent=2), encoding="utf-8")
    return cache_name

# In crawler/gemini_batch.py

def build_jsonl(company_id: int, domain: str, cache_name: str, cleaned_pages_path: Path,
                triage_kept_path: Path, out_jsonl_path: Path) -> Path:
    
    anchors = {}
    anc_file = cleaned_pages_path.parent / "homepage_anchors.json"
    system, schema = get_standard_static_context()
    
    if anc_file.exists():
        try:
            arr = json.loads(anc_file.read_text(encoding="utf-8"))
            anchors = {it.get("url"): it.get("anchor") for it in arr if isinstance(it, dict)}
        except Exception:
            anchors = {}
    
    triage_kept = set(line.strip() for line in triage_kept_path.read_text(encoding="utf-8").splitlines() if line.strip())
    
    lines = []
    with cleaned_pages_path.open("r", encoding="utf-8") as f:
        for ln in f:
            page = json.loads(ln)
            url = page.get("url")
            if url not in triage_kept:
                continue
            
            body = (page.get("text") or "")[:60000]
            dynamic_parts = [
                {"text": f"PAGE_META: url={url} depth={page.get('depth',0)} anchor={anchors.get(url, '')}"},
                {"text": f"PAGE_TEXT:\n{body}"}
            ]
            
            request_id = f"{company_id}::{domain}::{hash(url) & 0xfffffff}"
            
            # Use the fully qualified model name
            model_name = "models/gemini-2.0-flash"

            if cache_name:
                req = {
                    "custom_id": request_id,
                    "request": {
                        "model": model_name,
                        "contents": [{"role": "user", "parts": dynamic_parts}],
                        "cached_content": cache_name,
                        "generation_config": {
                            "response_mime_type": "application/json", 
                            "temperature": 0.0
                        }
                    }
                }
            else:
                static_parts = [{"text": system}, {"text": json.dumps(schema)}]
                req = {
                    "custom_id": request_id,
                    "request": {
                        "model": model_name,
                        "contents": [{"role": "user", "parts": static_parts + dynamic_parts}],
                        "generation_config": {
                            "response_mime_type": "application/json", 
                            "temperature": 0.0
                        }
                    }
                }
            
            lines.append(json.dumps(req, ensure_ascii=False))
    
    out_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    out_jsonl_path.write_text("\n".join(lines), encoding="utf-8")
    return out_jsonl_path

def submit_batch(client: genai.Client, jsonl_path: Path, display_name: Optional[str] = None) -> str:
    """
    Uploads a JSONL file and creates a Batch job, returning the provider's batch name/ID.
    """
    p = Path(jsonl_path)

    # 1. Upload the file
    uploaded_file = client.files.upload(
        file=str(p),
        config=types.UploadFileConfig(
            display_name=display_name or p.name,
            mime_type="application/jsonl"
        )
    )

    # 2. Create the batch job with the fully qualified model name
    batch_job = client.batches.create(
        model="models/gemini-2.0-flash",
        src=uploaded_file.name,
        config={
            "display_name": display_name or p.name
        }
    )

    # 3. Return the batch job's name for the watcher
    return getattr(batch_job, "name", None) or getattr(batch_job, "id", None) or str(batch_job)

def wait_batch(client: genai.Client, batch_name: str, poll_sec: int = 15):
    while True:
        b = client.batches.get(name=batch_name)
        if b.state in ("SUCCEEDED","FAILED","CANCELLED"):
            return b
        time.sleep(poll_sec)