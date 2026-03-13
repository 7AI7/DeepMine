# tools/preview_batch.py
"""
Preview batch_output.ndjson with proper GLM-4.5 Flash parsing
Handles both flat string lists and nested dict structures
Usage: python tools/preview_batch.py path/to/batch_output.ndjson
"""

import json
import sys
from pathlib import Path
import pandas as pd

def strip_code_fence(text):
    """Remove markdown code fences if present"""
    text = text.strip()
    if text.startswith("```json"):
        text = text.split("``````", 1)[0].strip()
    elif text.startswith("```"):
        text = text.split("```json", 1).rsplit("```")
    return text

def parse_line(line):
    """Parse GLM-4.5 Flash batch output line"""
    try:
        outer = json.loads(line)
    except json.JSONDecodeError:
        return None
    
    try:
        candidates = outer.get('response', {}).get('candidates', [])
        if not candidates:
            return None
        
        parts = candidates[0].get('content', {}).get('parts', [])
        if not parts:
            return None
        
        text = parts[0].get('text', '')
        text = strip_code_fence(text)
        
        return json.loads(text)
    except (KeyError, IndexError, json.JSONDecodeError, TypeError):
        return None

def flatten_nested_field(data, field_name):
    """
    Extract and flatten nested dict structures in products/applications/services.
    Handles both:
      - Flat: ["string1", "string2"]
      - Nested: [{"product_category": [...], "product": [...]}]
    """
    if field_name not in data or not data[field_name]:
        return []
    
    result = []
    for item in data[field_name]:
        if isinstance(item, str):
            result.append(item)
        elif isinstance(item, dict):
            # Extract from nested dict fields
            for key in ['product_category', 'product', 'application', 'service', 'serving_sector']:
                if key in item and isinstance(item[key], list):
                    result.extend(item[key])
                elif key in item and item[key]:
                    result.append(str(item[key]))
        elif isinstance(item, list):
            result.extend(str(x) for x in item if x)
    
    return list(dict.fromkeys(x for x in result if x))

def merge_arrays(dst, src, key=None):
    """Merge arrays with deduplication"""
    if not isinstance(src, list):
        return
    
    seen = set()
    
    def sig(x):
        if key and isinstance(x, dict):
            return tuple((k, str(x.get(k, ""))) for k in key)
        return json.dumps(x, sort_keys=True, ensure_ascii=False)
    
    for item in dst:
        seen.add(sig(item))
    
    for item in src:
        s = sig(item)
        if s not in seen:
            dst.append(item)
            seen.add(s)

def merge_doc(dst, src):
    """Merge source document (handles both flat and nested schemas)"""
    if not src:
        return
    
    # Company fields
    for field in ['name', 'website', 'email', 'phone', 'address', 'city', 'state',
                  'country', 'website_last_updated_on_year', 'linkedin_page',
                  'infrastructure_available', 'brochure_link', 'contact_person_name',
                  'contact_person_designation', 'contact_person_contact']:
        if field in src and src[field] not in (None, "", []):
            if dst.get(field) in (None, "", []):
                dst[field] = src[field]
    
    # Array fields - flatten nested structures
    for field in ['products', 'applications', 'services', 'serving_sectors']:
        dst.setdefault(field, [])
        flat_items = flatten_nested_field(src, field)
        if flat_items:
            merge_arrays(dst[field], flat_items)
    
    # Structured fields
    for field, keys in [
        ('addresses', ['address', 'city', 'state', 'pincode']),
        ('clients', ['client_name']),
        ('management', ['designation', 'name']),
        ('infrastructure_blocks', ['block_name']),
        ('machines', ['machine_name', 'block_name', 'capacity_value'])
    ]:
        dst.setdefault(field, [])
        if field in src and isinstance(src[field], list):
            merge_arrays(dst[field], src[field], key=keys)

def parse_and_merge(ndjson_path):
    """Parse NDJSON and merge by company"""
    merged = {}
    parsed = 0
    failed = 0
    
    with open(ndjson_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            
            doc = parse_line(line)
            if not doc:
                failed += 1
                continue
            
            parsed += 1
            name = doc.get('name') or 'unknown'
            key = name.strip().lower()
            
            merged.setdefault(key, {})
            merge_doc(merged[key], doc)
    
    print(f"Parsed: {parsed}, Failed: {failed}, Companies: {len(merged)}\\n")
    return merged

def safe_str_list(items):
    """Convert mixed list of strings/dicts to clean string list"""
    result = []
    for item in items:
        if isinstance(item, str) and item:
            result.append(item)
        elif isinstance(item, dict):
            for key in ['product', 'application', 'service', 'client_name']:
                if key in item and item[key]:
                    val = item[key]
                    if isinstance(val, list):
                        result.extend(str(v) for v in val if v)
                    else:
                        result.append(str(val))
    return result

def display_company(name, doc):
    """Display company data"""
    print(f"\\n{'='*80}")
    print(f"COMPANY: {name}")
    print(f"{'='*80}\\n")
    
    # Company Info
    info = {k: doc.get(k) for k in ['name', 'website', 'email', 'phone',
            'address', 'city', 'state', 'country', 'linkedin_page']
            if doc.get(k)}
    
    if info:
        print("### Company Info")
        for k, v in info.items():
            print(f"  {k}: {v}")
        print()
    
    # Products
    if doc.get('products'):
        prods = safe_str_list(doc['products'])
        if prods:
            print(f"### Products ({len(prods)})")
            for i, p in enumerate(prods[:30], 1):
                print(f"  {i}. {p}")
            if len(prods) > 30:
                print(f"  ... +{len(prods)-30} more")
            print()
    
    # Applications
    if doc.get('applications'):
        apps = safe_str_list(doc['applications'])
        if apps:
            print(f"### Applications ({len(apps)})")
            print(f"  {', '.join(apps)}")
            print()
    
    # Services
    if doc.get('services'):
        svcs = safe_str_list(doc['services'])
        if svcs:
            print(f"### Services ({len(svcs)})")
            print(f"  {', '.join(svcs)}")
            print()
    
    # Serving Sectors
    if doc.get('serving_sectors'):
        sectors = safe_str_list(doc['serving_sectors'])
        if sectors:
            print(f"### Serving Sectors ({len(sectors)})")
            print(f"  {', '.join(sectors)}")
            print()
    
    # Addresses, Management, Clients, Infrastructure, Machines
    for field, label in [
        ('addresses', 'Addresses'),
        ('management', 'Management'),
        ('clients', 'Clients'),
        ('infrastructure_blocks', 'Infrastructure Blocks'),
        ('machines', 'Machines')
    ]:
        items = doc.get(field, [])
        valid = [x for x in items if isinstance(x, dict) and any(x.values())]
        if valid:
            print(f"### {label} ({len(valid)})")
            if field == 'clients':
                for i, c in enumerate(valid[:30], 1):
                    print(f"  {i}. {c.get('client_name', '')}")
                if len(valid) > 30:
                    print(f"  ... +{len(valid)-30} more")
            else:
                df = pd.DataFrame(valid)
                print(df.to_string(index=False, max_rows=30))
            print()

def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/preview_batch.py <batch_output.ndjson>")
        sys.exit(1)
    
    path = Path(sys.argv[1])
    if not path.exists():
        print(f"Error: {path} not found")
        sys.exit(1)
    
    print(f"Processing {path}...\\n")
    merged = parse_and_merge(path)
    
    if not merged:
        print("No valid data found")
        return
    
    for name, doc in merged.items():
        display_company(name.title(), doc)
    
    print(f"\\n{'='*80}")
    print(f"Total: {len(merged)} companies")
    print(f"{'='*80}\\n")

if __name__ == "__main__":
    main()


#python tools/preview_batch.py data/ab/10_www.example.com/batch_output.ndjson