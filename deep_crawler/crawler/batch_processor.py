"""
crawler/batch_processor.py
PRODUCTION: Proper error handling, parameterized queries, resume support.
"""
from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime, timezone
import traceback
import psycopg2.extras

from crawler.db import run as db_run

LOG = logging.getLogger(__name__)

# ============================================================================
# STATE MANAGEMENT
# ============================================================================

def get_state_file(root_dir: Path) -> Path:
    return root_dir / ".batch_state.json"

def load_state(root_dir: Path) -> Dict[str, Any]:
    """Load processing state."""
    state_file = get_state_file(root_dir)
    if not state_file.exists():
        return {"processed": [], "failed": [], "last_run": None}
    
    try:
        with state_file.open('r') as f:
            return json.load(f)
    except Exception as e:
        LOG.warning(f"Failed to load state: {e}")
        return {"processed": [], "failed": [], "last_run": None}

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
# PARSING
# ============================================================================

def parse_batch_line(line: str) -> Optional[Dict[str, Any]]:
    """Parse one NDJSON line."""
    try:
        outer = json.loads(line)
        parts = (outer.get("response", {})
                .get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", []))
        
        if parts and isinstance(parts[0], dict):
            text = parts[0].get("text", "").strip()
            if text:
                return json.loads(text)
    except Exception as e:
        LOG.debug(f"Failed to parse line: {e}")
    
    return None

def merge_ndjson(ndjson_path: Path) -> Dict[str, Any]:
    """Parse and merge all variants from NDJSON."""
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
    
    try:
        with ndjson_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                doc = parse_batch_line(line)
                if not doc:
                    continue
                
                # Merge company scalars (prefer non-None)
                comp = doc.get("company") or {}
                for k, v in comp.items():
                    if v and not result["company"].get(k):
                        result["company"][k] = v
                
                # Extend arrays
                for key in ["products", "addresses", "management", "clients"]:
                    items = doc.get(key) or []
                    if isinstance(items, list):
                        result[key].extend(items)
                
                # Infrastructure
                infra = doc.get("infrastructure") or {}
                result["infrastructure"]["blocks"].extend(infra.get("blocks") or [])
                result["infrastructure"]["machines"].extend(infra.get("machines") or [])
    
    except Exception as e:
        LOG.error(f"Failed to parse {ndjson_path}: {e}")
    
    return result

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
            "SELECT EXISTS(SELECT 1 FROM products WHERE company_id=%s) OR "
            "EXISTS(SELECT 1 FROM addresses WHERE company_id=%s)",
            (company_id, company_id),
            fetch=True
        )
        return rows[0][0] if rows else False
    except:
        return False

# ============================================================================
# INSERTION FUNCTIONS (PARAMETERIZED QUERIES)
# ============================================================================

def upsert_company(company_id: Optional[int], website: str, comp: Dict) -> Optional[int]:
    """Upsert company with parameterized query."""
    try:
        cols = [
            "name", "website", "email", "phone", "address", "city", "state", "country",
            "contact_name", "website_last_updated_on_year",
            "linkedin_page",
            "contact_person_designation", "contact_person_name", "contact_person_contact",
            "infrastructure_available", "brochure_link"
        ]
        
        vals = tuple(comp.get(c) for c in cols)
        
        if company_id:
            # Update existing
            set_clause = ", ".join([f"{c}=COALESCE(%s,{c})" for c in cols])
            db_run(
                f"UPDATE companies SET {set_clause}, last_crawled=NOW() WHERE id=%s",
                vals + (company_id,),
                fetch=False
            )
            return company_id
        else:
            # Insert new
            placeholders = ",".join(["%s"] * len(cols))
            conflict_updates = ", ".join([f"{c}=COALESCE(EXCLUDED.{c},companies.{c})" for c in cols if c != "website"])
            
            db_run(
                f"INSERT INTO companies ({','.join(cols)}) VALUES ({placeholders}) "
                f"ON CONFLICT (website) DO UPDATE SET {conflict_updates}, last_crawled=NOW()",
                vals,
                fetch=False
            )
            
            return get_company_id_by_website(website)
    
    except Exception as e:
        LOG.error(f"Failed to upsert company: {e}")
        return None

def insert_products(company_id: int, products: List[Dict]) -> int:
    """Insert products with deduplication and error handling."""
    if not products:
        return 0
    
    # Deduplicate by product name
    seen = set()
    unique = []
    for p in products:
        if not isinstance(p, dict):
            continue
        pname = p.get("product")
        if pname and pname not in seen:
            unique.append(p)
            seen.add(pname)
    
    if not unique:
        return 0
    
    # Batch insert with parameterized queries
    success = 0
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
        # Use executemany for batch insert
        from crawler.db import POOL
        conn = POOL.getconn()
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
            conn.commit()
            success = len(rows)
        finally:
            POOL.putconn(conn)
    
    except Exception as e:
        LOG.error(f"Failed to batch insert products: {e}")
    
    return success

def insert_addresses(company_id: int, addresses: List[Dict]) -> int:
    """Insert addresses with error handling."""
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
            a.get("address_label"),
            a.get("location_section")
        ))
    
    if not rows:
        return 0
    
    success = 0
    try:
        from crawler.db import POOL
        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur,
                    """
                    INSERT INTO addresses (company_id, address, city, states, country, address_label, location_section)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (company_id, address) DO NOTHING
                    """,
                    rows,
                    page_size=100
                )
            conn.commit()
            success = len(rows)
        finally:
            POOL.putconn(conn)
    
    except Exception as e:
        LOG.error(f"Failed to insert addresses: {e}")
    
    return success

def insert_management(company_id: int, management: List[Dict]) -> int:
    """Insert management with error handling."""
    if not management:
        return 0
    
    rows = []
    for m in management:
        if not isinstance(m, dict) or not m.get("name"):
            continue
        rows.append((company_id, m.get("designation"), m.get("name")))
    
    if not rows:
        return 0
    
    success = 0
    try:
        from crawler.db import POOL
        conn = POOL.getconn()
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
            conn.commit()
            success = len(rows)
        finally:
            POOL.putconn(conn)
    
    except Exception as e:
        LOG.error(f"Failed to insert management: {e}")
    
    return success

def insert_clients(company_id: int, clients: List) -> int:
    """Insert clients with error handling."""
    if not clients:
        return 0
    
    rows = []
    for c in clients:
        cname = c.get("client_name") if isinstance(c, dict) else c
        if cname:
            rows.append((company_id, cname))
    
    if not rows:
        return 0
    
    success = 0
    try:
        from crawler.db import POOL
        conn = POOL.getconn()
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
            conn.commit()
            success = len(rows)
        finally:
            POOL.putconn(conn)
    
    except Exception as e:
        LOG.error(f"Failed to insert clients: {e}")
    
    return success

def insert_infrastructure(company_id: int, infra: Dict) -> Tuple[int, int]:
    """Insert blocks and machines with error handling."""
    blocks = infra.get("blocks") or []
    machines = infra.get("machines") or []
    
    block_ids = {}
    block_count = 0
    
    # Insert blocks one-by-one (need IDs for machines)
    for b in blocks:
        if not isinstance(b, dict):
            continue
        
        bname = b.get("block_name")
        if not bname:
            continue
        
        try:
            db_run(
                """
                INSERT INTO company_infra_blocks (company_id, block_name)
                VALUES (%s,%s)
                ON CONFLICT (company_id, lower(block_name)) DO NOTHING
                """,
                (company_id, bname),
                fetch=False
            )
            
            rows = db_run(
                "SELECT id FROM company_infra_blocks WHERE company_id=%s AND lower(block_name)=lower(%s)",
                (company_id, bname),
                fetch=True
            )
            
            if rows:
                block_ids[bname.lower()] = rows[0][0]
                block_count += 1
        
        except Exception as e:
            LOG.error(f"Failed to insert block '{bname}': {e}")
            continue
    
    # Insert machines in batch
    machine_count = 0
    if machines:
        machine_rows = []
        for m in machines:
            if not isinstance(m, dict):
                continue
            
            mname = m.get("machine_name")
            if not mname:
                continue
            
            block_name = m.get("block_name")
            block_id = block_ids.get(block_name.lower()) if block_name else None
            
            extra = m.get("extra")
            extra_json = json.dumps(extra) if extra else None
            
            machine_rows.append((
                company_id,
                block_id,
                mname,
                m.get("brand_name"),
                m.get("qty"),
                m.get("capacity_value"),
                m.get("capacity_unit"),
                extra_json
            ))
        
        if machine_rows:
            try:
                from crawler.db import POOL
                conn = POOL.getconn()
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
                    conn.commit()
                    machine_count = len(machine_rows)
                finally:
                    POOL.putconn(conn)
            
            except Exception as e:
                LOG.error(f"Failed to batch insert machines: {e}")
    
    return block_count, machine_count

# ============================================================================
# MAIN PROCESSING
# ============================================================================

def process_one_site(site_dir: Path, company_id: Optional[int], website: str, root_dir: Path) -> bool:
    """Process one site directory."""
    
    # Check state
    state = load_state(root_dir)
    if website in state["processed"]:
        LOG.info(f"⏭  SKIP (already done): {website}")
        return True
    
    # Check DB
    if check_already_processed(company_id, website):
        LOG.info(f"⏭  SKIP (in DB): {website}")
        mark_processed(root_dir, website, success=True)
        return True
    
    output_file = site_dir / "batch_output.ndjson"
    if not output_file.exists():
        LOG.warning(f"⚠  No batch_output.ndjson: {website}")
        return False
    
    LOG.info(f"🔄 PROCESSING: {website}")
    
    try:
        # Parse
        merged = merge_ndjson(output_file)
        if not merged.get("company"):
            LOG.warning(f"⚠  No company data: {website}")
            return False
        
        # Upsert company
        final_id = upsert_company(company_id, website, merged["company"])
        if not final_id:
            LOG.error(f"✗ Failed to upsert company: {website}")
            mark_processed(root_dir, website, success=False)
            return False
        
        # Insert child tables
        p_count = insert_products(final_id, merged["products"])
        a_count = insert_addresses(final_id, merged["addresses"])
        m_count = insert_management(final_id, merged["management"])
        c_count = insert_clients(final_id, merged["clients"])
        b_count, machine_count = insert_infrastructure(final_id, merged["infrastructure"])
        
        LOG.info(f"✅ SUCCESS: {website} | products={p_count} addresses={a_count} mgmt={m_count} clients={c_count} blocks={b_count} machines={machine_count}")
        
        mark_processed(root_dir, website, success=True)
        return True
    
    except Exception as e:
        LOG.error(f"✗ FAILED: {website} - {e}")
        traceback.print_exc()
        mark_processed(root_dir, website, success=False)
        return False

def process_all(root_dir: str = "data/ab"):
    """Process all sites sequentially."""
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
    
    for site_dir, cid, website in tasks:
        if website in state["processed"]:
            skipped += 1
            continue
        
        if process_one_site(site_dir, cid, website, root):
            success += 1
        else:
            failed += 1
    
    LOG.info("=" * 60)
    LOG.info(f"COMPLETE: {success} success, {failed} failed, {skipped} skipped")
    LOG.info("=" * 60)

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    
    root = sys.argv[1] if len(sys.argv) > 1 else "data/ab"
    process_all(root)
