from __future__ import annotations
import os, json, hashlib
from typing import Dict, Any, List
from google import genai
from google.genai import types 
from pathlib import Path
from .gemini_prompts import get_standard_static_context, build_prompt_pack_file
from urllib.parse import urlparse
def get_client() -> genai.Client:
    return genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

CACHE_DIR = Path("data") / "cache"  # add near top-level
CACHE_DIR.mkdir(parents=True, exist_ok=True)  # ensure exists

def _approx_tokens(s: str) -> int:
    # heuristic ~4 chars/token in English; adjust if needed
    return max(1, len(s) // 4)

def create_cache(client: genai.Client, ttl_hours: int = 96) -> str:
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

def extract_pages_standard(company_id: int, kept_urls: List[str], cleaned_docs: Dict[str,str]) -> Dict[str, Any]:
    """
    kept_urls: list of URLs to extract (e.g., from triage_kept.txt)
    cleaned_docs: {url: cleaned_text}
    Returns one merged JSON for the company.
    """
    client = get_client()
    cache_name = create_cache(client, ttl_hours=96)
    anchors: dict[str, str] = {}
    try:
        if kept_urls:
            dom = urlparse(kept_urls[0]).netloc
            out_dir = Path("data") / "ab" / f"{company_id}_{dom}"
            anc_file = out_dir / "homepage_anchors.json"
            if anc_file.exists():
                arr = json.loads(anc_file.read_text(encoding="utf-8"))
                if isinstance(arr, list):
                    anchors = { (it.get("url") or ""): (it.get("anchor") or "") for it in arr if isinstance(it, dict) }
    except Exception:
        anchors = {}
    merged: Dict[str, Any] = {}
    for url in kept_urls:
        body = (cleaned_docs.get(url) or "")[:60000]
        if not body:
            continue
        resp = client.responses.generate(
            model="gemini-2.0-flash",
            cached_content_name=cache_name,
            contents=[{"parts":[
                {"text": f"PAGE_META: url={url} anchor={anchors.get(url, '')}"},
                {"text": f"PAGE_TEXT:\n{body}"}
            ]}],
            config={"response_mime_type":"application/json","temperature":0.0}
        )
        try:
            item = json.loads(resp.output_text or "{}")
        except Exception:
            continue
        # Shallow merge arrays by union, objects by last-writer-wins for scalars
        for k, v in item.items():
            if v is None:
                continue
            if isinstance(v, list):
                merged.setdefault(k, [])
                merged[k].extend([x for x in v if x not in merged[k]])
            elif isinstance(v, dict):
                merged.setdefault(k, {})
                merged[k].update({kk: vv for kk, vv in v.items() if vv is not None})
            else:
                merged[k] = v
    # stamp provenance
    merged.setdefault("provenance", {})
    merged["provenance"]["pages"] = kept_urls
    return merged
