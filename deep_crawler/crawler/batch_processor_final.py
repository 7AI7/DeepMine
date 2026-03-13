"""
crawler/batch_processor.py

PRODUCTION-READY: Handles both Gemini AND GLM batch output formats.
- Proper error handling with CSV logging
- Parameterized queries
- Resume support
- Transaction safety
- Parallel processing
- Data validation
- Retry failed companies
"""

from __future__ import annotations
import json
import logging
import re
import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime, timezone
from urllib.parse import urlparse
import traceback
import psycopg2
import psycopg2.extras
from crawler.db import run as db_run, POOL

LOG = logging.getLogger(__name__)

# ============================================================================
# FAILURE LOGGING (MATCHES ORCHESTRATOR PATTERN)
# ============================================================================

FAIL_DIR = Path("data") / "failures"

def log_processing_failure(
    company_id: Optional[int],
    domain: str,
    stage: str,
    error: str,
    urls: List[str] = None,
    traceback_str: str = ""
):
    """Log failure to CSV matching orchestrator.py pattern"""
    FAIL_DIR.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).isoformat(timespec="minutes")
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    path = FAIL_DIR / f"failures_{today}.csv"

    # Clean error message
    error_clean = str(error).replace('"', "'").replace("\n", " ")[:500]

    # Format URLs
    urls_str = "|".join(urls) if urls else ""

    row = f'{ts},{company_id or ""},"{domain}",{stage},"{error_clean}","{urls_str}"'

    # Write header if new file
    if not path.exists():
        path.write_text("timestamp,company_id,domain,stage,error,urls\n", encoding="utf-8")

    # Append failure
    with path.open("a", encoding="utf-8") as f:
        f.write(row + "\n")

    # Also log traceback to separate file if provided
    if traceback_str:
        tb_path = FAIL_DIR / f"tracebacks_{today}.log"
        with tb_path.open("a", encoding="utf-8") as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"{ts} | {domain} | {stage}\n")
            f.write(f"{'='*80}\n")
            f.write(traceback_str)
            f.write("\n")

# ============================================================================
# STATE MANAGEMENT
# ============================================================================

def get_state_file(root_dir: Path) -> Path:
    return root_dir / ".batch_state.json"

def load_state(root_dir: Path) -> Dict[str, Any]:
    """Load processing state."""
    state_file = get_state_file(root_dir)
    if not state_file.exists():
        return {"processed": [], "failed": [], "last_run": None, "stats": {}}

    try:
        with state_file.open('r') as f:
            return json.load(f)
    except Exception as e:
        LOG.warning(f"Failed to load state: {e}")
        return {"processed": [], "failed": [], "last_run": None, "stats": {}}

def save_state(root_dir: Path, state: Dict[str, Any]):
    """Save processing state."""
    state_file = get_state_file(root_dir)
    state["last_run"] = datetime.now(timezone.utc).isoformat()

    try:
        with state_file.open('w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        LOG.error(f"Failed to save state: {e}")

def mark_processed(root_dir: Path, website: str, success: bool = True):
    """Mark a website as processed or failed."""
    state = load_state(root_dir)

    if success:
        if website not in state["processed"]:
            state["processed"].append(website)
        if website in state["failed"]:
            state["failed"].remove(website)
    else:
        if website not in state["failed"]:
            state["failed"].append(website)

    save_state(root_dir, state)

# ============================================================================
# PARSING - HANDLES BOTH GEMINI AND GLM FORMATS
# ============================================================================

def strip_markdown_fence(text: str) -> str:
    """Remove markdown code fences"""
    text = text.strip()
    if text.startswith('```json'):
        text = text.split('```json', 1)[1].rsplit('```', 1)[0].strip()
    elif text.startswith('```'):
        text = text.split('```', 1)[1].rsplit('```', 1)[0].strip()
    return text

def parse_batch_line(line: str, log_warnings: bool = True) -> Optional[Dict[str, Any]]:
    """Parse one NDJSON line - handles both Gemini and GLM formats."""
    try:
        outer = json.loads(line)

        # Check for API-level errors
        if "error" in outer:
            if log_warnings:
                custom_id = outer.get('custom_id', 'unknown')
                error_msg = outer.get('error', {}).get('message', 'Unknown error')
                LOG.debug(f"Skipping error entry {custom_id}: {error_msg}")
            return None

        # Extract response
        response = outer.get("response", {})
        candidates = response.get("candidates", [])

        if not candidates:
            if log_warnings:
                LOG.debug(f"Empty candidates in {outer.get('custom_id', 'unknown')}")
            return None

        candidate = candidates[0]

        # Check finish reason
        finish_reason = candidate.get("finishReason", "STOP")
        if finish_reason == "length":
            if log_warnings:
                LOG.warning(f"Truncated response: {outer.get('custom_id', 'unknown')}")
        elif finish_reason == "SAFETY":
            if log_warnings:
                LOG.warning(f"Content filtered: {outer.get('custom_id', 'unknown')}")
            return None

        # Extract text
        parts = candidate.get("content", {}).get("parts", [])
        if not parts or not isinstance(parts[0], dict):
            return None

        text = parts[0].get("text", "").strip()
        if not text:
            return None

        # Strip markdown fences if present
        text = strip_markdown_fence(text)

        # Parse JSON
        parsed = json.loads(text)

        # Handle {"answer": {...}} wrapper
        if "answer" in parsed and isinstance(parsed["answer"], dict):
            parsed = parsed["answer"]

        return parsed

    except json.JSONDecodeError as e:
        if log_warnings:
            LOG.debug(f"JSON decode error: {e}")
        return None
    except Exception as e:
        if log_warnings:
            LOG.debug(f"Parse error: {e}")
        return None

def normalize_glm_to_gemini_schema(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert GLM flat schema to Gemini nested schema.
    Handles arrays, None values, type mismatches, and all edge cases.
    """
    if not doc:
        return {"company": {}, "products": [], "addresses": [], 
                "management": [], "clients": [], 
                "infrastructure": {"blocks": [], "machines": []}}

    # Helper: Convert array to string (comma-separated)
    def array_to_string(val):
        """Convert list to comma-separated string, handle None"""
        if val is None:
            return None
        if isinstance(val, list):
            # Filter out None values and convert to strings
            items = [str(x) for x in val if x is not None]
            return ", ".join(items) if items else None
        return str(val) if val else None

    # Helper: Safe strip
    def safe_strip(val):
        """Strip string safely, handle None"""
        if val is None:
            return None
        return str(val).strip() if val else None

    # Company-level fields
    company_fields = [
        'name', 'website', 'email', 'phone', 'address', 'city', 'state', 'country',
        'contact_name', 'website_last_updated_on_year',
        'linkedin_page', 'linkedin_employee_range', 'linkedin_followers_range',
        'contact_person_designation', 'contact_person_name', 'contact_person_contact',
        'infrastructure_available', 'brochure_link'
    ]

    company = {}
    for field in company_fields:
        if field in doc:
            val = doc[field]

            # Convert arrays to strings for ALL text fields (not just contact fields)
            if field in ['contact_person_designation', 'contact_person_name', 
                        'contact_person_contact', 'brochure_link',
                        'address', 'city', 'state', 'country']:  # ← ADDED city/state/country
                company[field] = array_to_string(val)
            else:
                company[field] = val

    # Extract products (same as before)
    products = []

    if doc.get('products') and isinstance(doc['products'], list):
        for item in doc['products']:
            if isinstance(item, str):
                products.append({
                    "product_category": None,
                    "product": safe_strip(item),
                    "application": None,
                    "service": None,
                    "serving_sector": None
                })
            elif isinstance(item, dict):
                cats = item.get('product_category', [])
                prods = item.get('product', [])
                apps = item.get('application', [])
                servs = item.get('service', [])
                sectors = item.get('serving_sector', [])

                if not isinstance(cats, list):
                    cats = [cats] if cats else []
                if not isinstance(prods, list):
                    prods = [prods] if prods else []
                if not isinstance(apps, list):
                    apps = [apps] if apps else []
                if not isinstance(servs, list):
                    servs = [servs] if servs else []
                if not isinstance(sectors, list):
                    sectors = [sectors] if sectors else []

                cats = [safe_strip(x) for x in cats if x]
                prods = [safe_strip(x) for x in prods if x]
                apps = [safe_strip(x) for x in apps if x]
                servs = [safe_strip(x) for x in servs if x]
                sectors = [safe_strip(x) for x in sectors if x]

                if prods:
                    for prod in prods:
                        products.append({
                            "product_category": cats[0] if cats else None,
                            "product": prod,
                            "application": apps[0] if apps else None,
                            "service": servs[0] if servs else None,
                            "serving_sector": sectors[0] if sectors else None
                        })
                elif cats:
                    for cat in cats:
                        products.append({
                            "product_category": cat,
                            "product": None,
                            "application": None,
                            "service": None,
                            "serving_sector": None
                        })

    if doc.get('product_categories') and isinstance(doc['product_categories'], list):
        for cat in doc['product_categories']:
            if cat and isinstance(cat, str):
                products.append({
                    "product_category": safe_strip(cat),
                    "product": None,
                    "application": None,
                    "service": None,
                    "serving_sector": None
                })

    # Addresses (same as before)
    addresses = []
    for a in (doc.get('addresses') or []):
        if isinstance(a, dict) and a.get("address"):
            addresses.append(a)

    # Management - FILTER OUT entries with null designation
    management = []
    seen_mgmt = set()

    mgmt_names = doc.get('management', [])
    if not isinstance(mgmt_names, list):
        mgmt_names = [mgmt_names] if mgmt_names else []

    for m in mgmt_names:
        if isinstance(m, dict):
            name = safe_strip(m.get('name'))
            designation = safe_strip(m.get('designation'))

            # FILTER: Skip if designation is None (NOT NULL constraint)
            if not designation:
                continue

            # FILTER: Skip junk names
            if name and name not in ['Mr.', 'Ms.', 'Mrs.', 'Dr.', ''] and len(name) > 2:
                if name.lower() not in seen_mgmt:
                    management.append(m)
                    seen_mgmt.add(name.lower())

    # Clients (same as before)
    clients = []
    seen_clients = set()
    for c in (doc.get('clients') or []):
        if isinstance(c, dict):
            cname = safe_strip(c.get('client_name'))
            if cname and cname.lower() not in seen_clients:
                clients.append(c)
                seen_clients.add(cname.lower())
        elif c and isinstance(c, str):
            cname = safe_strip(c)
            if cname and cname.lower() not in seen_clients:
                clients.append({"client_name": cname})
                seen_clients.add(cname.lower())

    # Infrastructure (same as before)
    infra_blocks = doc.get('infrastructure_blocks', [])
    if not isinstance(infra_blocks, list):
        infra_blocks = []

    cleaned_blocks = []
    seen_blocks = set()
    for block in infra_blocks:
        bname = None

        if isinstance(block, dict):
            bname = block.get('block_name') or block.get('name')
        elif isinstance(block, str):
            bname = block

        bname = safe_strip(bname)
        if bname and bname.lower() not in seen_blocks:
            cleaned_blocks.append({"block_name": bname})
            seen_blocks.add(bname.lower())

    # Machines (same as before)
    machines = []
    for m in (doc.get('machines') or []):
        if isinstance(m, dict):
            mname = safe_strip(m.get('machine_name'))
            if mname:
                machines.append(m)

    return {
        "company": company,
        "products": products,
        "addresses": addresses,
        "management": management,
        "clients": clients,
        "infrastructure": {
            "blocks": cleaned_blocks,
            "machines": machines
        }
    }

def merge_ndjson(ndjson_path: Path) -> Dict[str, Any]:
    """Parse and merge all variants from NDJSON - handles both Gemini and GLM formats."""
    result = {
        "company": {},
        "products": [],
        "addresses": [],
        "management": [],
        "clients": [],
        "infrastructure": {"blocks": [], "machines": []}
    }

    if not ndjson_path.exists():
        return result

    parsed_count = 0
    error_count = 0

    try:
        with ndjson_path.open("r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                doc = parse_batch_line(line, log_warnings=(line_num <= 5))  # Only log first 5
                if not doc:
                    error_count += 1
                    continue

                parsed_count += 1

                # Normalize GLM format to Gemini format
                normalized = normalize_glm_to_gemini_schema(doc)

                # Merge company scalars (prefer non-None)
                comp = normalized.get("company") or {}
                for k, v in comp.items():
                    if v not in (None, "", []) and not result["company"].get(k):
                        result["company"][k] = v

                # Extend arrays
                for key in ["products", "addresses", "management", "clients"]:
                    items = normalized.get(key) or []
                    if isinstance(items, list):
                        result[key].extend(items)

                # Infrastructure
                infra = normalized.get("infrastructure") or {}
                result["infrastructure"]["blocks"].extend(infra.get("blocks") or [])
                result["infrastructure"]["machines"].extend(infra.get("machines") or [])

        if parsed_count > 0:
            LOG.debug(f"Parsed {parsed_count} entries, {error_count} errors from {ndjson_path.name}")

    except Exception as e:
        LOG.error(f"Failed to parse {ndjson_path}: {e}")

    return result

def validate_merged_data(merged: Dict[str, Any], website: str) -> Tuple[bool, str]:
    """Validate merged data quality"""
    company = merged.get("company", {})
    
    # Check if ANY data exists at all
    has_any_company = len(company.keys()) > 0
    has_products = len(merged.get("products", [])) > 0
    has_addresses = len(merged.get("addresses", [])) > 0
    has_mgmt = len(merged.get("management", [])) > 0
    has_clients = len(merged.get("clients", [])) > 0
    has_infra = (len(merged.get("infrastructure", {}).get("blocks", [])) > 0 or
                 len(merged.get("infrastructure", {}).get("machines", [])) > 0)
    
    if not any([has_any_company, has_products, has_addresses, has_mgmt, has_clients, has_infra]):
        return False, "Completely empty response - no data extracted"
    
    return True, "OK"

# ============================================================================
# DATABASE HELPERS
# ============================================================================

def get_company_id_by_website(website: str) -> Optional[int]:
    """Fetch company_id by website."""
    try:
        rows = db_run("SELECT id FROM companies WHERE website = %s", (website,), fetch=True)
        return rows[0][0] if rows else None
    except Exception as e:
        LOG.error(f"Failed to fetch company_id: {e}")
        return None

def check_already_processed(company_id: Optional[int], website: str) -> bool:
    """Check if company already has data."""
    if not company_id:
        return False

    try:
        rows = db_run(
            "SELECT EXISTS(SELECT 1 FROM products WHERE company_id=%s LIMIT 1) OR "
            "EXISTS(SELECT 1 FROM addresses WHERE company_id=%s LIMIT 1)",
            (company_id, company_id),
            fetch=True
        )
        return rows[0][0] if rows else False
    except:
        return False

# ============================================================================
# INSERTION FUNCTIONS (TRANSACTION-SAFE, PARAMETERIZED QUERIES)
# ============================================================================

def upsert_company(conn, company_id: Optional[int], website: str, comp: Dict) -> Optional[int]:
    """Upsert company with parameterized query. Uses existing connection."""
    try:
        cols = [
            'name', 'website', 'email', 'phone', 'address', 'city', 'state', 'country',
        'contact_name', 'website_last_updated_on_year',
        'linkedin_page', 
        'contact_person_designation', 'contact_person_name', 'contact_person_contact',
        'infrastructure_available', 'brochure_link'
        ]

        vals = tuple(comp.get(c) for c in cols)

        with conn.cursor() as cur:
            if company_id:
                # Update existing
                set_clause = ", ".join([f"{c}=COALESCE(%s,{c})" for c in cols])
                cur.execute(
                    f"UPDATE companies SET {set_clause}, last_crawled=NOW() WHERE id=%s RETURNING id",
                    vals + (company_id,)
                )
                result = cur.fetchone()
                return result[0] if result else company_id
            else:
                # Insert new
                placeholders = ",".join(["%s"] * len(cols))
                conflict_updates = ", ".join([f"{c}=COALESCE(EXCLUDED.{c},companies.{c})" for c in cols if c != "website"])
                cur.execute(
                    f"INSERT INTO companies ({','.join(cols)}) VALUES ({placeholders}) "
                    f"ON CONFLICT (website) DO UPDATE SET {conflict_updates}, last_crawled=NOW() RETURNING id",
                    vals
                )
                result = cur.fetchone()
                return result[0] if result else None

    except Exception as e:
        LOG.error(f"Failed to upsert company: {e}")
        raise

def insert_products(conn, company_id: int, products: List[Dict]) -> int:
    """Insert products with deduplication. Uses existing connection."""
    if not products:
        return 0

    # Deduplicate by product name
    seen = set()
    unique = []

    for p in products:
        if not isinstance(p, dict):
            continue

        pname = p.get("product")
        if pname and pname.strip() and pname not in seen:
            unique.append(p)
            seen.add(pname)

    if not unique:
        return 0

    # Batch insert
    rows = []
    for p in unique:
        rows.append((
            company_id,
            p.get("product_category"),
            p.get("product"),
            p.get("application"),
            p.get("service"),
            p.get("serving_sector")
        ))

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO products (company_id, product_category, product, application, service, serving_sector)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (company_id, product) DO NOTHING
                """,
                rows,
                page_size=100
            )
        return len(rows)
    except Exception as e:
        LOG.error(f"Failed to insert products: {e}")
        raise

def insert_addresses(conn, company_id: int, addresses: List[Dict]) -> int:
    """Insert addresses. Uses existing connection."""
    if not addresses:
        return 0

    rows = []
    for a in addresses:
        if not isinstance(a, dict) or not a.get("address"):
            continue

        rows.append((
            company_id,
            a.get("address"),
            a.get("city"),
            a.get("states") or a.get("state"),
            a.get("country"),
            a.get("pincode"),
            a.get("address_label"),
            a.get("location_section")
        ))

    if not rows:
        return 0

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO addresses (company_id, address, city, states, country, pincode, address_label, location_section)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (company_id, address) DO NOTHING
                """,
                rows,
                page_size=100
            )
        return len(rows)
    except Exception as e:
        LOG.error(f"Failed to insert addresses: {e}")
        raise

def insert_management(conn, company_id: int, management: List[Dict]) -> int:
    """Insert management. Uses existing connection."""
    if not management:
        return 0

    rows = []
    for m in management:
        if not isinstance(m, dict) or not m.get("name"):
            continue

        rows.append((company_id, m.get("designation"), m.get("name")))

    if not rows:
        return 0

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO management (company_id, designation, name)
                VALUES (%s,%s,%s)
                ON CONFLICT (company_id, name) DO NOTHING
                """,
                rows,
                page_size=100
            )
        return len(rows)
    except Exception as e:
        LOG.error(f"Failed to insert management: {e}")
        raise

def insert_clients(conn, company_id: int, clients: List) -> int:
    """Insert clients. Uses existing connection."""
    if not clients:
        return 0

    rows = []
    for c in clients:
        cname = c.get("client_name") if isinstance(c, dict) else c
        if cname and str(cname).strip():
            rows.append((company_id, str(cname).strip()))

    if not rows:
        return 0

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO clients (company_id, client_name)
                VALUES (%s,%s)
                ON CONFLICT (company_id, lower(client_name)) DO NOTHING
                """,
                rows,
                page_size=100
            )
        return len(rows)
    except Exception as e:
        LOG.error(f"Failed to insert clients: {e}")
        raise

def insert_infrastructure(conn, company_id: int, infra: Dict) -> Tuple[int, int]:
    """Insert blocks and machines. Uses existing connection."""
    blocks = infra.get("blocks") or []
    machines = infra.get("machines") or []

    block_ids = {}
    block_count = 0

    # Insert blocks
    with conn.cursor() as cur:
        for b in blocks:
            if not isinstance(b, dict):
                continue

            bname = b.get("block_name")
            if not bname or not bname.strip():
                continue

            try:
                cur.execute(
                    """
                    INSERT INTO company_infra_blocks (company_id, block_name)
                    VALUES (%s,%s)
                    ON CONFLICT (company_id, lower(block_name)) DO NOTHING
                    RETURNING id
                    """,
                    (company_id, bname.strip())
                )
                result = cur.fetchone()
                if result:
                    block_ids[bname.lower()] = result[0]
                    block_count += 1
                else:
                    # Fetch existing
                    cur.execute(
                        "SELECT id FROM company_infra_blocks WHERE company_id=%s AND lower(block_name)=lower(%s)",
                        (company_id, bname.strip())
                    )
                    result = cur.fetchone()
                    if result:
                        block_ids[bname.lower()] = result[0]
            except Exception as e:
                LOG.warning(f"Failed to insert block '{bname}': {e}")
                continue

    # Insert machines
    machine_count = 0
    if machines:
        machine_rows = []
        for m in machines:
            if not isinstance(m, dict):
                continue

            mname = m.get("machine_name")
            if not mname or not mname.strip():
                continue

            block_name = m.get("block_name")
            block_id = block_ids.get(block_name.lower()) if block_name else None

            extra = m.get("extra")
            extra_json = json.dumps(extra) if extra else None

            machine_rows.append((
                company_id,
                block_id,
                mname.strip(),
                m.get("brand_name"),
                m.get("qty"),
                m.get("capacity_value"),
                m.get("capacity_unit"),
                extra_json
            ))

        if machine_rows:
            try:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(
                        cur,
                        """
                        INSERT INTO company_machines
                        (company_id, block_id, machine_name, brand_name, qty, capacity_value, capacity_unit, extra)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                        """,
                        machine_rows,
                        page_size=100
                    )
                    machine_count = len(machine_rows)
            except Exception as e:
                LOG.error(f"Failed to insert machines: {e}")
                raise

    return block_count, machine_count

# ============================================================================
# MAIN PROCESSING (TRANSACTION-SAFE)
# ============================================================================

def process_one_site(site_dir: Path, company_id: Optional[int], website: str, root_dir: Path) -> bool:
    """Process one site directory with full transaction safety."""
    domain = urlparse(website).netloc

    # Check state
    state = load_state(root_dir)
    if website in state["processed"]:
        LOG.info(f"⏭ SKIP (already done): {domain}")
        return True

    # Check DB
    if check_already_processed(company_id, website):
        LOG.info(f"⏭ SKIP (in DB): {domain}")
        mark_processed(root_dir, website, success=True)
        return True

    output_file = site_dir / "batch_output.ndjson"
    if not output_file.exists():
        LOG.warning(f"⚠ No batch_output.ndjson: {domain}")
        log_processing_failure(
            company_id=company_id,
            domain=domain,
            stage="batch_processing_missing_file",
            error="batch_output.ndjson not found",
            urls=[]
        )
        return False

    LOG.info(f"🔄 PROCESSING: {domain}")

    conn = None
    try:
        # Parse
        merged = merge_ndjson(output_file)

        # Validate
        is_valid, validation_msg = validate_merged_data(merged, website)
        if not is_valid:
            LOG.warning(f"⚠ Validation failed for {domain}: {validation_msg}")
            log_processing_failure(
                company_id=company_id,
                domain=domain,
                stage="batch_processing_validation",
                error=validation_msg,
                urls=[]
            )
            mark_processed(root_dir, website, success=False)
            return False

        # Get database connection
        conn = POOL.getconn()
        conn.autocommit = False  # CRITICAL: Manual transaction control

        try:
            # Upsert company
            final_id = upsert_company(conn, company_id, website, merged["company"])
            if not final_id:
                raise Exception("Failed to upsert company - no ID returned")

            # Insert child tables (all in same transaction)
            p_count = insert_products(conn, final_id, merged["products"])
            a_count = insert_addresses(conn, final_id, merged["addresses"])
            m_count = insert_management(conn, final_id, merged["management"])
            c_count = insert_clients(conn, final_id, merged["clients"])
            b_count, machine_count = insert_infrastructure(conn, final_id, merged["infrastructure"])

            # Commit transaction
            conn.commit()

            LOG.info(
                f"✅ SUCCESS: {domain} | "
                f"products={p_count} addresses={a_count} mgmt={m_count} "
                f"clients={c_count} blocks={b_count} machines={machine_count}"
            )
            mark_processed(root_dir, website, success=True)
            return True

        except Exception as db_error:
            # Rollback on any error
            if conn:
                conn.rollback()
            raise db_error

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        tb = traceback.format_exc()

        LOG.error(f"✗ FAILED: {domain} - {error_msg}")

        # Log to CSV with traceback
        log_processing_failure(
            company_id=company_id,
            domain=domain,
            stage="batch_processing",
            error=error_msg,
            urls=[],
            traceback_str=tb
        )

        mark_processed(root_dir, website, success=False)
        return False

    finally:
        if conn:
            POOL.putconn(conn)

# ============================================================================
# BATCH PROCESSING (SEQUENTIAL & PARALLEL)
# ============================================================================

def process_all_sequential(root_dir: str = "data/ab"):
    """Process all sites sequentially (safer, easier to debug)."""
    root = Path(root_dir)
    if not root.exists():
        LOG.error(f"Root directory {root} does not exist")
        return

    state = load_state(root)
    LOG.info(f"Already processed: {len(state['processed'])} sites")
    LOG.info(f"Previously failed: {len(state['failed'])} sites")

    tasks = []
    for site_dir in sorted(root.iterdir()):
        if not site_dir.is_dir():
            continue

        name = site_dir.name
        if "_" not in name:
            continue

        parts = name.split("_", 1)
        try:
            cid = int(parts[0])
        except ValueError:
            cid = None

        host = parts[1]
        website = f"https://{host}"

        tasks.append((site_dir, cid, website))

    LOG.info(f"Total sites to process: {len(tasks)}")

    success = 0
    failed = 0
    skipped = 0

    for idx, (site_dir, cid, website) in enumerate(tasks, 1):
        LOG.info(f"[{idx}/{len(tasks)}] Processing {urlparse(website).netloc}")

        if website in state["processed"]:
            skipped += 1
            continue

        if process_one_site(site_dir, cid, website, root):
            success += 1
        else:
            failed += 1

    LOG.info("=" * 80)
    LOG.info(f"COMPLETE: {success} success, {failed} failed, {skipped} skipped")
    LOG.info("=" * 80)

async def process_all_parallel(root_dir: str = "data/ab", max_workers: int = 4):
    """Process all sites in parallel (faster, but harder to debug)."""
    root = Path(root_dir)
    if not root.exists():
        LOG.error(f"Root directory {root} does not exist")
        return

    state = load_state(root)
    LOG.info(f"Already processed: {len(state['processed'])} sites")
    LOG.info(f"Previously failed: {len(state['failed'])} sites")

    tasks = []
    for site_dir in sorted(root.iterdir()):
        if not site_dir.is_dir():
            continue

        name = site_dir.name
        if "_" not in name:
            continue

        parts = name.split("_", 1)
        try:
            cid = int(parts[0])
        except ValueError:
            cid = None

        host = parts[1]
        website = f"https://{host}"

        if website not in state["processed"]:
            tasks.append((site_dir, cid, website))

    LOG.info(f"Sites to process: {len(tasks)} (parallel workers: {max_workers})")

    semaphore = asyncio.Semaphore(max_workers)

    async def process_with_semaphore(site_dir, cid, website):
        async with semaphore:
            # Run in thread pool (DB operations are blocking)
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                process_one_site,
                site_dir, cid, website, root
            )

    results = await asyncio.gather(*[
        process_with_semaphore(s, c, w) for s, c, w in tasks
    ], return_exceptions=True)

    success = sum(1 for r in results if r is True)
    failed = sum(1 for r in results if r is False or isinstance(r, Exception))

    LOG.info("=" * 80)
    LOG.info(f"COMPLETE: {success} success, {failed} failed")
    LOG.info("=" * 80)

# ============================================================================
# RETRY FAILED COMPANIES
# ============================================================================

def retry_failed_companies(root_dir: str = "data/ab"):
    """Retry companies marked as failed in state file."""
    root = Path(root_dir)
    state = load_state(root)
    failed_websites = state.get("failed", [])

    if not failed_websites:
        LOG.info("No failed companies to retry")
        return

    LOG.info(f"Retrying {len(failed_websites)} failed companies")

    # Build lookup
    site_lookup = {}
    for site_dir in root.iterdir():
        if not site_dir.is_dir():
            continue

        name = site_dir.name
        if "_" not in name:
            continue

        parts = name.split("_", 1)
        try:
            cid = int(parts[0])
        except ValueError:
            cid = None

        host = parts[1]
        website = f"https://{host}"
        site_lookup[website] = (site_dir, cid)

    # Retry each
    success = 0
    still_failed = 0

    for website in failed_websites[:]:  # Copy to allow modification
        if website not in site_lookup:
            LOG.warning(f"⚠ Failed site not found: {website}")
            continue

        site_dir, cid = site_lookup[website]
        LOG.info(f"🔄 RETRY: {urlparse(website).netloc}")

        if process_one_site(site_dir, cid, website, root):
            success += 1
        else:
            still_failed += 1

    LOG.info("=" * 80)
    LOG.info(f"RETRY COMPLETE: {success} recovered, {still_failed} still failing")
    LOG.info("=" * 80)

# ============================================================================
# CLI INTERFACE
# ============================================================================

if __name__ == "__main__":
    import sys
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )

    parser = argparse.ArgumentParser(description="Batch processor for GLM/Gemini output")
    parser.add_argument("root_dir", nargs="?", default="data/ab", help="Root directory")
    parser.add_argument("--retry-failed", action="store_true", help="Retry failed companies")
    parser.add_argument("--parallel", type=int, metavar="N", help="Use parallel processing with N workers")

    args = parser.parse_args()

    if args.retry_failed:
        retry_failed_companies(args.root_dir)
    elif args.parallel:
        asyncio.run(process_all_parallel(args.root_dir, max_workers=args.parallel))
    else:
        process_all_sequential(args.root_dir)
