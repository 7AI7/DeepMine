# db_handler.py — add near top; uses db.run from db.py
from .db import run as db_run
from typing import Dict, Any, List, Optional  
import json

MAX_PRODUCTS = 100
MAX_CLIENTS = 15
MAX_MGMT = 10
MAX_EACH_OF_SERVICE_APP_SECTOR = 20

def _cap_and_map_sql(self, doc: dict) -> dict:
    data = dict(doc or {})
    comp = data.get("company") or {}
    # First address to companies.*, remainder stays in addresses
    addrs = data.get("addresses") or []
    if addrs:
        first = addrs[0]
        comp["address"] = first.get("address") or comp.get("address")
        comp["city"] = first.get("city") or comp.get("city")
        # state must be a single token
        st = (first.get("state") or first.get("states") or "").strip()
        if st:
            comp["state"] = st.split(",")[0].strip()
        comp["country"] = first.get("country") or comp.get("country")
        data["addresses"] = addrs[1:]
    # Remove Google/JD/turnover and LinkedIn extras, keep linkedin_page
    for k in list(comp.keys()):
        if k.lower().startswith("google_") or k.lower().startswith("jd_") or k.startswith("turnover_") or k in ("linkedin_employee_range","linkedin_followers_range"):
            comp.pop(k, None)
    data["company"] = comp
    # Caps
    prod = data.get("products") or {}
    def _cap(lst, n): return list(dict.fromkeys([x for x in (lst or []) if x]))[:n]
    prod["product"] = _cap(prod.get("product"), 100)
    prod["product_category"] = _cap(prod.get("product_category"), 20)
    prod["application"] = _cap(prod.get("application"), 20)
    prod["service"] = _cap(prod.get("service"), 20)
    prod["serving_sector"] = _cap(prod.get("serving_sector"), 20)
    data["products"] = prod
    # Clients
    uniq_clients, seen = [], set()
    for it in data.get("clients") or []:
        name = (it or {}).get("client_name")
        if name and name not in seen:
            seen.add(name); uniq_clients.append({"client_name": name})
        if len(uniq_clients) >= 15: break
    data["clients"] = uniq_clients
    # Management
    uniq_mgmt, seen_m = [], set()
    for it in data.get("management") or []:
        des = (it or {}).get("designation","").strip(); nm = (it or {}).get("name","").strip()
        key = (des, nm)
        if des and nm and key not in seen_m:
            seen_m.add(key); uniq_mgmt.append({"designation": des, "name": nm})
        if len(uniq_mgmt) >= 10: break
    data["management"] = uniq_mgmt
    # Infra flag
    infra = data.get("infrastructure") or {}
    has_infra = bool((infra.get("blocks") or []) or (infra.get("machines") or []))
    if has_infra:
        comp["infrastructure_available"] = True
    data["company"] = comp
    return data

def upsert_company(self, comp: dict):
    cid, site = self.cid, self.website
    cols = ["name","website","email","phone","address","city","state","country",
            "website_last_updated_on_year","linkedin_page","infrastructure_available",
            "brochure_link","responsible_person_designation","responsible_person_name","responsible_person_contact"]
    vals = [comp.get(c) for c in cols]
    if cid:
        self.db.run("""
          UPDATE companies SET
            name=COALESCE(%s,name),
            website=COALESCE(%s,website),
            email=COALESCE(%s,email),
            phone=COALESCE(%s,phone),
            address=COALESCE(%s,address),
            city=COALESCE(%s,city),
            state=COALESCE(%s,state),
            country=COALESCE(%s,country),
            website_last_updated_on_year=COALESCE(%s,website_last_updated_on_year),
            linkedin_page=COALESCE(%s,linkedin_page),
            infrastructure_available=COALESCE(%s,infrastructure_available),
            brochure_link=COALESCE(%s,brochure_link),
            probable_responsible_person_designation=COALESCE(%s,probable_responsible_person_designation),
            responsible_person_name=COALESCE(%s,responsible_person_name),
            responsible_person_contact=COALESCE(%s,responsible_person_contact),
            last_crawled=NOW()
          WHERE id=%s
        """, (*vals, cid), fetch=False)
    elif site:
        self.db.run(f"""
          INSERT INTO companies ({",".join(cols)})
          VALUES ({",".join(["%s"]*len(cols))})
          ON CONFLICT (website) DO UPDATE SET
            name=COALESCE(EXCLUDED.name,companies.name),
            email=COALESCE(EXCLUDED.email,companies.email),
            phone=COALESCE(EXCLUDED.phone,companies.phone),
            address=COALESCE(EXCLUDED.address,companies.address),
            city=COALESCE(EXCLUDED.city,companies.city),
            state=COALESCE(EXCLUDED.state,companies.state),
            country=COALESCE(EXCLUDED.country,companies.country),
            website_last_updated_on_year=COALESCE(EXCLUDED.website_last_updated_on_year,companies.website_last_updated_on_year),
            linkedin_page=COALESCE(EXCLUDED.linkedin_page,companies.linkedin_page),
            infrastructure_available=COALESCE(EXCLUDED.infrastructure_available,companies.infrastructure_available),
            brochure_link=COALESCE(EXCLUDED.brochure_link,companies.brochure_link),
            probable_responsible_person_designation=COALESCE(EXCLUDED.probable_responsible_person_designation,companies.probable_responsible_person_designation),
            responsible_person_name=COALESCE(EXCLUDED.responsible_person_name,companies.responsible_person_name),
            responsible_person_contact=COALESCE(EXCLUDED.responsible_person_contact,companies.responsible_person_contact),
            last_crawled=NOW()
        """, vals, fetch=False)

def save_company_doc(self, raw_doc: dict):
    doc = self._cap_and_map_sql(raw_doc)
    self.upsert_company(doc.get("company") or {})
    self.insert_products(self.cid, doc.get("products") or {})
    self.insert_addresses(self.cid, doc.get("addresses") or [])
    self.insert_management(self.cid, doc.get("management") or [])
    self.insert_clients(self.cid, doc.get("clients") or [])
    self.insert_infrastructure(self.cid, doc.get("infrastructure") or {})
    
def insert_products(company_id: int, products: Dict[str, List[str]]):
    if not products: return
    for col in ("product_category","product","application","service","serving_sector"):
        for v in products.get(col) or []:
            db_run(f"INSERT INTO products (company_id, {col}) VALUES (%s,%s) ON CONFLICT DO NOTHING", (company_id, v), fetch=False)

def insert_addresses(company_id: int, addrs: List[Dict[str, Any]]):
    for a in addrs or []:
        db_run(
            """
            INSERT INTO addresses (company_id, address, city, states, country, address_label, location_section, pincode)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            (
                company_id,
                a.get("address"),
                a.get("city"),
                a.get("states") or a.get("state"),  # tolerate either key
                a.get("country"),
                a.get("address_label"),
                a.get("location_section"),
                a.get("pincode"),
            ),
            fetch=False,
        )

def insert_management(self, company_id: int, mgmt: List[Dict[str, Any]]) -> None:
    """
    Inserts management rows: (company_id, designation, name)
    De-duplicates on (company_id, lower(designation), lower(name)).
    """
    for m in mgmt or []:
        desig = (m.get("designation") or "").strip()
        name = (m.get("name") or "").strip()
        if not desig or not name:
            continue
        db_run(
            """
            INSERT INTO management (company_id, designation, name)
            SELECT %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM management
                WHERE company_id = %s
                  AND lower(designation) = lower(%s)
                  AND lower(name) = lower(%s)
            )
            """,
            (company_id, desig, name, company_id, desig, name),
            fetch=False,
        )

def insert_clients(self, company_id: int, clients: List[Dict[str, Any]] | List[str]) -> None:
    """
    Inserts clients rows: (company_id, client_name)
    Accepts list of dicts or list of strings; de-duplicates on (company_id, lower(client_name)).
    """
    for c in clients or []:
        name = (c.get("client_name") if isinstance(c, dict) else c) or ""
        name = name.strip()
        if not name:
            continue
        db_run(
            """
            INSERT INTO clients (company_id, client_name)
            SELECT %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM clients
                WHERE company_id = %s
                  AND lower(client_name) = lower(%s)
            )
            """,
            (company_id, name, company_id, name),
            fetch=False,
        )

def _get_or_create_block_id(self, company_id: int, block_name: str) -> Optional[int]:
    """
    Returns id of company_infra_blocks row for (company_id, block_name), creating it if missing.
    """
    if not block_name:
        return None
    # Try fetch
    row = db_run(
        "SELECT id FROM company_infra_blocks WHERE company_id=%s AND lower(block_name)=lower(%s) LIMIT 1",
        (company_id, block_name),
        fetch=True,
    )
    if row:
        return row[0]["id"] if isinstance(row, list) else row["id"]
    # Create
    db_run(
        """
        INSERT INTO company_infra_blocks (company_id, block_name)
        SELECT %s, %s
        WHERE NOT EXISTS (
            SELECT 1 FROM company_infra_blocks
            WHERE company_id=%s AND lower(block_name)=lower(%s)
        )
        """,
        (company_id, block_name, company_id, block_name),
        fetch=False,
    )
    row = db_run(
        "SELECT id FROM company_infra_blocks WHERE company_id=%s AND lower(block_name)=lower(%s) LIMIT 1",
        (company_id, block_name),
        fetch=True,
    )
    return row[0]["id"] if row else None

def insert_infrastructure(self, company_id: int, infra: Dict[str, Any]) -> None:
    """
    Inserts infrastructure blocks and machines:
      - Blocks from infra.get('blocks') with block_name
      - Machines from infra.get('machines') mapping optional block_name → block_id
    Also sets companies.infrastructure_available = true if any rows are inserted.
    """
    any_inserted = False

    # 1) Blocks
    for b in (infra.get("blocks") or []):
        block_name = (b.get("block_name") or "").strip()
        if not block_name:
            continue
        before = db_run(
            "SELECT 1 FROM company_infra_blocks WHERE company_id=%s AND lower(block_name)=lower(%s) LIMIT 1",
            (company_id, block_name),
            fetch=True,
        )
        if not before:
            db_run(
                """
                INSERT INTO company_infra_blocks (company_id, block_name)
                VALUES (%s, %s)
                """,
                (company_id, block_name),
                fetch=False,
            )
            any_inserted = True

    # 2) Machines
    for m in (infra.get("machines") or []):
        machine_name = (m.get("machine_name") or "").strip()
        if not machine_name:
            continue
        brand = (m.get("brand_name") or "").strip() or None
        qty = m.get("qty")
        cap_val = m.get("capacity_value")
        cap_unit = (m.get("capacity_unit") or "").strip() or None
        block_name = (m.get("block_name") or "").strip()
        extra = m.get("extra") if isinstance(m.get("extra"), (dict, list)) else None

        block_id = self._get_or_create_block_id(company_id, block_name) if block_name else None

        # De-dup on a practical composite: company, lower(machine_name), optional brand, capacity, and block
        db_run(
            """
            INSERT INTO company_machines
                (company_id, block_id, machine_name, brand_name, qty, capacity_value, capacity_unit, extra)
            SELECT %s, %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1 FROM company_machines
                WHERE company_id=%s
                  AND lower(machine_name)=lower(%s)
                  AND COALESCE(brand_name,'') = COALESCE(%s,'')
                  AND COALESCE(capacity_unit,'') = COALESCE(%s,'')
                  AND COALESCE(capacity_value::text,'') = COALESCE(%s::text,'')
                  AND COALESCE(block_id,-1) = COALESCE(%s,-1)
            )
            """,
            (
                company_id,
                block_id,
                machine_name,
                brand,
                qty,
                cap_val,
                cap_unit,
                json.dumps(extra) if extra is not None else None,
                company_id,
                machine_name,
                brand,
                cap_unit,
                cap_val,
                block_id,
            ),
            fetch=False,
        )
        any_inserted = True

    # 3) Flag availability if anything present
    if any_inserted:
        db_run(
            "UPDATE companies SET infrastructure_available = TRUE WHERE id = %s AND COALESCE(infrastructure_available, FALSE) = FALSE",
            (company_id,),
            fetch=False,
        )
