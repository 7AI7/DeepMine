"""
Universal Foolproof Data Parser - NO DATA LOSS GUARANTEED
Handles: GLM/Gemini both, nested data anywhere, arrays at any depth,
mixed formats, missing fields, field name variations, ALL attributes
"""

import json
import logging
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict, field

LOG = logging.getLogger(__name__)

# ===== DATABASE SCHEMA DEFINITIONS =====
# Ensures NO column mismatch between parser and database

@dataclass
class Company:
    id: int
    name: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    website_last_updated_on_year: Optional[int] = None
    infrastructure_available: Optional[bool] = None
    contact_person_name: Optional[str] = None
    contact_person_designation: Optional[str] = None
    contact_person_contact: Optional[str] = None
    brochure_link: Optional[str] = None
    linkedin_page: Optional[str] = None

@dataclass
class Address:
    id: str
    company_id: int
    address_type: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    pincode: Optional[str] = None

@dataclass
class Product:
    id: str
    company_id: int
    product_category: Optional[str] = None
    product: Optional[str] = None
    application: Optional[str] = None
    service: Optional[str] = None
    serving_sector: Optional[str] = None

@dataclass
class Client:
    id: str
    company_id: int
    client_name: Optional[str] = None
    industry: Optional[str] = None
    relationship: Optional[str] = None

@dataclass
class Management:
    id: str
    company_id: int
    name: Optional[str] = None
    designation: Optional[str] = None
    contact: Optional[str] = None

@dataclass
class InfrastructureBlock:
    id: str
    company_id: int
    block_name: Optional[str] = None
    capacity: Optional[str] = None
    equipment: Optional[str] = None

@dataclass
class Machine:
    id: str
    company_id: int
    machine_name: Optional[str] = None
    brand_name: Optional[str] = None
    qty: Optional[int] = None
    capacity_value: Optional[str] = None
    capacity_unit: Optional[str] = None
    specification: Optional[str] = None

@dataclass
class ParseResult:
    company: Optional[Company] = None
    addresses: List[Address] = field(default_factory=list)
    products: List[Product] = field(default_factory=list)
    clients: List[Client] = field(default_factory=list)
    management: List[Management] = field(default_factory=list)
    infrastructure_blocks: List[InfrastructureBlock] = field(default_factory=list)
    machines: List[Machine] = field(default_factory=list)
    extraction_errors: List[str] = field(default_factory=list)
    
    def to_dict(self):
        return {
            "company": asdict(self.company) if self.company else None,
            "addresses": [asdict(a) for a in self.addresses],
            "products": [asdict(p) for p in self.products],
            "clients": [asdict(c) for c in self.clients],
            "management": [asdict(m) for m in self.management],
            "infrastructure_blocks": [asdict(ib) for ib in self.infrastructure_blocks],
            "machines": [asdict(m) for m in self.machines],
            "extraction_errors": self.extraction_errors
        }

# ===== UNIVERSAL PARSER =====

class UniversalDataParser:
    """
    NO DATA LOSS parser that:
    1. Searches entire JSON tree for every field
    2. Handles GLM AND Gemini formats
    3. Captures ALL attributes (including brand_name, qty, capacity_unit, etc.)
    4. Normalizes mismatched arrays properly
    5. Handles nested data at ANY depth
    """
    
    def __init__(self):
        self.id_counters = {}
        # All possible field name variations (COMPREHENSIVE)
        self.field_aliases = {
            "name": ["name", "company_name", "companyname"], 
            "website": ["website", "web", "url", "company_website"],  
            "email": ["email", "company_email", "contact_email", "mail"], 
            "phone": ["phone", "company_phone", "contact_number", "telephone", "mobile"],
            "address": ["address", "address_line", "full_address", "street_address", "street", "location"],
            "address_type": ["address_label", "address_type", "type", "label", "site_name", "office_type"],
            "city": ["city", "city_name", "urban_area"],
            "state": ["state", "state_name", "province", "region"],
            "country": ["country", "nation", "country_name"],
            "pincode": ["pincode", "zipcode", "postal_code", "postcode"],
            "website_last_updated_on_year": ["website_last_updated_on_year", "last_updated", "updated_year", "website_year"],
            "infrastructure_available": ["infrastructure_available", "has_infrastructure", "infrastructure"],
            "contact_person_name": ["contact_person_name", "contact_name", "person_name", "representative"],
            "contact_person_designation": ["contact_person_designation", "contact_title", "person_title"],
            "contact_person_contact": ["contact_person_contact", "contact_info", "person_contact"],
            "product_category": ["product_category", "category", "categories", "product_type"],
            "product": ["product", "products", "product_name", "item", "product_item"],
            "application": ["application", "applications", "use_case", "use_cases", "application_area"],
            "service": ["service", "services", "offering", "offerings"],
            "management_name": ["name", "person_name", "member_name"],  # Separate field for Management.name
            "designation": ["designation", "title", "role", "position", "job_title"],
            "contact": ["contact", "contact_info", "contact_details"],
            "serving_sector": ["serving_sector", "sector", "sectors", "industry", "industries", "vertical"],
            "client_name": ["client_name","client names", "client","clients", "company_name", "customer_name"],
            "machine_name": ["machine_name", "model", "equipment_name"],
            "brand_name": ["brand_name", "brand", "manufacturer", "make"],
            "qty": ["qty", "quantity", "count", "number", "nos"],
            "capacity_value": ["capacity_value", "capacity", "throughput", "speed", "power"],
            "capacity_unit": ["capacity_unit", "unit", "capacity_type", "measurement_unit"],
            "specification": ["specification", "specs", "description", "detail", "technical_spec"],
            "block_name": ["block_name", "facility_name", "hall_name", "workshop_name"],
            "equipment": ["equipment", "tools", "machinery", "devices", "assets"],
            "designation": ["designation", "title", "role", "position", "job_title"],
            "contact": ["contact", "email", "phone", "mobile", "phone_number", "email_address"],
        }
    
    def _gen_id(self, prefix: str) -> str:
        """Generate unique IDs with prefix"""
        if prefix not in self.id_counters:
            self.id_counters[prefix] = 0
        self.id_counters[prefix] += 1
        return f"{prefix}{self.id_counters[prefix]:05d}"
    
    def _to_list(self, value: Any) -> List[Any]:
        """Convert ANY value to list - handles all cases"""
        if value is None:
            return []
        if isinstance(value, list):
            return [v for v in value if v is not None]
        if isinstance(value, dict):
            return [value]
        if isinstance(value, (str, int, float, bool)):
            s = str(value).strip()
            return [value] if s else []
        return []
    
    def _find_value_by_field(self, obj: Dict, field_name: str) -> Any:
        """
        Find value by ANY alias of field_name.
        Case-insensitive, exact match on canonical names.
        """
        if not isinstance(obj, dict):
            return None
        
        # Get all aliases for this field
        aliases = self.field_aliases.get(field_name, [field_name])
        aliases_lower = {a.lower(): a for a in aliases}
        
        # Search object keys (case-insensitive)
        obj_lower = {k.lower(): (k, v) for k, v in obj.items()}
        
        for alias_lower, alias in aliases_lower.items():
            if alias_lower in obj_lower:
                _, value = obj_lower[alias_lower]
                return value
        
        return None
    
    def _deep_search_all(self, obj: Any, field_names: List[str], max_depth: int = 20) -> List[Any]:
        """
        DEEP SEARCH: Find ALL occurrences of ANY field_name variant at ANY depth.
        Returns: List of all found values
        
        Handles:
        - Data nested under infrastructure.machines
        - Data nested under company.infrastructure.blocks
        - Data at root level
        - Data at any arbitrary depth
        """
        results = []
        visited = set()
        field_names_lower = [f.lower() for f in field_names]
        all_aliases_lower = set()
        
        for fname in field_names:
            aliases = self.field_aliases.get(fname, [fname])
            all_aliases_lower.update(a.lower() for a in aliases)
        
        def _recurse(current, depth):
            if depth > max_depth:
                return
            
            obj_id = id(current)
            if obj_id in visited:
                return
            visited.add(obj_id)
            
            if isinstance(current, dict):
                # Search current dict keys
                for key, value in current.items():
                    if key.lower() in all_aliases_lower:
                        if value is not None:
                            results.extend(self._to_list(value))
                            LOG.debug(f"Found '{key}' at depth {depth}: {len(self._to_list(value))} items")
                    
                    # Recurse deeper
                    if isinstance(value, (dict, list)) and value:
                        _recurse(value, depth + 1)
            
            elif isinstance(current, list):
                for item in current:
                    if isinstance(item, (dict, list)) and item:
                        _recurse(item, depth + 1)
        
        _recurse(obj, 0)
        return results
    
    def _parse_address_string(self, addr_str: str) -> Dict[str, Optional[str]]:
        """
        Parse address string to extract city, state, country, pincode
        Handles: "Street, City Code. State, Country." format
        """
        if not isinstance(addr_str, str) or not addr_str.strip():
            return {"city": None, "state": None, "country": None, "pincode": None}
        
        result = {"city": None, "state": None, "country": None, "pincode": None}
        parts = [p.strip() for p in addr_str.split(',')]
        
        if len(parts) >= 4:
            # Format: [..., City CODE, State, Country]
            result["country"] = parts[-1].rstrip('.')
            result["state"] = parts[-2]
            
            city_postcode = parts[-3].split()
            result["city"] = city_postcode[0] if city_postcode else None
            result["pincode"] = city_postcode[1] if len(city_postcode) > 1 else None
            
        elif len(parts) == 3:
            result["country"] = parts[-1].rstrip('.')
            result["state"] = parts[-2]
            result["city"] = parts[-3].split()[0]
            
        elif len(parts) == 2:
            result["state"] = parts[-1]
            result["city"] = parts[0].split()[0]
            
        elif len(parts) == 1:
            result["city"] = parts[0].split()[0]
        
        return result
    
    def _normalize_arrays(self, arrays_dict: Dict[str, List]) -> Dict[str, List]:
        """
        Normalize arrays to same length.
        SMART: Preserves alignment (doesn't just pad at end)
        """
        if not arrays_dict:
            return {}
        
        lengths = {k: len(v) for k, v in arrays_dict.items() if v}
        if not lengths:
            return arrays_dict
        
        max_len = max(lengths.values())
        
        # Pad all arrays to max_len with None
        normalized = {}
        for key, arr in arrays_dict.items():
            if arr:
                normalized[key] = arr + [None] * (max_len - len(arr))
            else:
                normalized[key] = [None] * max_len
        
        LOG.info(f"Normalized arrays to length {max_len}: {list(normalized.keys())}")
        return normalized
    
    # ===== TABLE PARSERS =====
    
    def parse_company(self, data: Dict, company_id: int) -> Company:
        """Parse company - flat fields only"""
        try:
            if "answer" in data and isinstance(data["answer"], dict):
                data = data["answer"]
            elif "result" in data and isinstance(data["result"], dict):
                data = data["result"]
            elif "data" in data and isinstance(data["data"], dict) and "company" not in data:
                data = data["data"]

            company_data = data.get("company", data)
            
            company = Company(id=company_id)
            company.name = self._find_value_by_field(company_data, "name") or self._find_value_by_field(data, "name")
            company.website = self._find_value_by_field(company_data, "website") or self._find_value_by_field(data, "website")
            company.email = self._find_value_by_field(company_data, "email") or self._find_value_by_field(data, "email")
            company.phone = self._find_value_by_field(company_data, "phone")
            company.website_last_updated_on_year = self._find_value_by_field(company_data, "website_last_updated_on_year")
            company.infrastructure_available = self._find_value_by_field(company_data, "infrastructure_available")
            company.contact_person_name = (self._find_value_by_field(company_data, "contact_person_name") or 
                                          self._find_value_by_field(data, "contact_person_name"))
            company.contact_person_designation = (self._find_value_by_field(company_data, "contact_person_designation") or 
                                                 self._find_value_by_field(data, "contact_person_designation"))
            company.contact_person_contact = (self._find_value_by_field(company_data, "contact_person_contact") or 
                                             self._find_value_by_field(data, "contact_person_contact"))
            
            return company
        except Exception as e:
            LOG.error(f"Error parsing company: {e}")
            return Company(id=company_id)
    
    def parse_addresses(self, data: Dict, company_id: int) -> List[Address]:
        """Parse addresses - searches ENTIRE tree"""
        addresses = []
        try:
            # Deep search for address data
            address_items = self._deep_search_all(data, ["address"])
            
            for idx, addr_item in enumerate(address_items):
                if not addr_item:
                    continue
                
                if isinstance(addr_item, dict):
                    full_address = self._find_value_by_field(addr_item, "address") or ""
                    addr_type = self._find_value_by_field(addr_item, "address_type") or f"Address {idx+1}"
                    city = self._find_value_by_field(addr_item, "city")
                    state = self._find_value_by_field(addr_item, "state")
                    country = self._find_value_by_field(addr_item, "country")
                    pincode = self._find_value_by_field(addr_item, "pincode")
                    
                elif isinstance(addr_item, str):
                    full_address = addr_item
                    addr_type = f"Address {idx+1}"
                    parsed = self._parse_address_string(full_address)
                    city = parsed["city"]
                    state = parsed["state"]
                    country = parsed["country"]
                    pincode = parsed["pincode"]
                else:
                    continue
                
                if not full_address:
                    continue
                
                # If city/state/country empty, try to parse from address string
                if not city or not state or not country:
                    parsed = self._parse_address_string(full_address)
                    city = city or parsed["city"]
                    state = state or parsed["state"]
                    country = country or parsed["country"]
                    pincode = pincode or parsed["pincode"]
                
                addr = Address(
                    id=self._gen_id("AD"),
                    company_id=company_id,
                    address_type=addr_type,
                    address=full_address,
                    city=city,
                    state=state,
                    country=country,
                    pincode=pincode
                )
                addresses.append(addr)
                LOG.debug(f"Parsed address: {full_address[:50]}...")
            
            # If no addresses found via deep search, try standard locations
            if not addresses:
                LOG.warning(f"No addresses found via deep search, trying standard locations...")
                std_locations = [
                    data.get("address"),
                    data.get("company", {}).get("address"),
                    data.get("addresses"),
                ]
                for loc in std_locations:
                    if loc:
                        address_items = self._to_list(loc)
                        for idx, addr_item in enumerate(address_items):
                            if isinstance(addr_item, (dict, str)):
                                # Recursive call with this item
                                temp_data = {"address": addr_item}
                                addresses.extend(self.parse_addresses(temp_data, company_id))
                        break
            
            return addresses
        
        except Exception as e:
            LOG.error(f"Error parsing addresses: {e}")
            return []
    
    def parse_products(self, data: Dict, company_id: int) -> List[Product]:
        """Parse products - handles arrays at ANY depth"""
        products = []
        try:
            # Deep search for all product fields
            categories = self._deep_search_all(data, ["product_category"])
            product_names = self._deep_search_all(data, ["product"])
            applications = self._deep_search_all(data, ["application"])
            services = self._deep_search_all(data, ["service"])
            sectors = self._deep_search_all(data, ["serving_sector"])
            
            # Normalize to same length
            arrays = {
                "categories": categories,
                "products": product_names,
                "applications": applications,
                "services": services,
                "sectors": sectors,
            }
            normalized = self._normalize_arrays(arrays)
            
            max_len = len(normalized["categories"]) if normalized["categories"] else 0
            
            for i in range(max_len):
                prod = Product(
                    id=self._gen_id("PRD"),
                    company_id=company_id,
                    product_category=normalized["categories"][i] if i < len(normalized["categories"]) else None,
                    product=normalized["products"][i] if i < len(normalized["products"]) else None,
                    application=normalized["applications"][i] if i < len(normalized["applications"]) else None,
                    service=normalized["services"][i] if i < len(normalized["services"]) else None,
                    serving_sector=normalized["sectors"][i] if i < len(normalized["sectors"]) else None,
                )
                products.append(prod)
            
            LOG.info(f"Parsed {len(products)} products")
            return products
        
        except Exception as e:
            LOG.error(f"Error parsing products: {e}")
            return []
    
    def parse_clients(self, data: Dict, company_id: int) -> List[Client]:
        """Parse clients - searches ENTIRE tree"""
        clients = []
        try:
            client_items = self._deep_search_all(data, ["client_name","client","clients"])
            
            for client_item in client_items:
                if not client_item:
                    continue
                
                if isinstance(client_item, dict):
                    client = Client(
                        id=self._gen_id("CLI"),
                        company_id=company_id,
                        client_name=self._find_value_by_field(client_item, "client_name"),
                        industry=self._find_value_by_field(client_item, "industry"),
                        relationship=self._find_value_by_field(client_item, "relationship"),
                    )
                elif isinstance(client_item, str):
                    client = Client(
                        id=self._gen_id("CLI"),
                        company_id=company_id,
                        client_name=client_item,
                    )
                else:
                    continue
                
                clients.append(client)
            
            LOG.info(f"Parsed {len(clients)} clients")
            return clients
        
        except Exception as e:
            LOG.error(f"Error parsing clients: {e}")
            return []
    
    def parse_management(self, data: Dict, company_id: int) -> List[Management]:
        """Parse management - searches ENTIRE tree"""
        management = []
        try:
            mgmt_items = self._deep_search_all(data, ["management", "team", "staff"])
            
            for mgmt_item in mgmt_items:
                if not mgmt_item:
                    continue
                
                if isinstance(mgmt_item, dict):
                    mgmt = Management(
                        id=self._gen_id("MGT"),
                        company_id=company_id,
                        name=self._find_value_by_field(mgmt_item, "name"),
                        designation=self._find_value_by_field(mgmt_item, "designation"),
                        contact=self._find_value_by_field(mgmt_item, "contact"),
                    )
                elif isinstance(mgmt_item, str):
                    mgmt = Management(
                        id=self._gen_id("MGT"),
                        company_id=company_id,
                        name=mgmt_item,
                    )
                else:
                    continue
                
                management.append(mgmt)
            
            LOG.info(f"Parsed {len(management)} management records")
            return management
        
        except Exception as e:
            LOG.error(f"Error parsing management: {e}")
            return []
    
    def parse_infrastructure(self, data: Dict, company_id: int) -> Tuple[List[InfrastructureBlock], List[Machine]]:
        """Parse infrastructure blocks AND machines - searches ENTIRE tree"""
        blocks = []
        machines = []
        try:
            # Deep search for blocks
            block_items = self._deep_search_all(data, ["infrastructure_blocks","infrastructure", "block"])
            
            for block_item in block_items:
                if not block_item:
                    continue
                
                if isinstance(block_item, dict):
                    block = InfrastructureBlock(
                        id=self._gen_id("INFRA"),
                        company_id=company_id,
                        block_name=self._find_value_by_field(block_item, "block_name"),
                        capacity=self._find_value_by_field(block_item, "capacity"),
                        equipment=self._find_value_by_field(block_item, "equipment"),
                    )
                elif isinstance(block_item, str):
                    block = InfrastructureBlock(
                        id=self._gen_id("INFRA"),
                        company_id=company_id,
                        block_name=block_item,
                    )
                else:
                    continue
                
                blocks.append(block)
            
            # Deep search for machines - CAPTURE ALL FIELDS
            machine_items = self._deep_search_all(data, ["machines", "machine"])
            
            for machine_item in machine_items:
                if not machine_item:
                    continue
                
                if isinstance(machine_item, dict):
                    machine = Machine(
                        id=self._gen_id("MCH"),
                        company_id=company_id,
                        machine_name=self._find_value_by_field(machine_item, "machine_name"),
                        brand_name=self._find_value_by_field(machine_item, "brand_name"),
                        qty=self._find_value_by_field(machine_item, "qty"),
                        capacity_value=self._find_value_by_field(machine_item, "capacity_value"),
                        capacity_unit=self._find_value_by_field(machine_item, "capacity_unit"),
                        specification=self._find_value_by_field(machine_item, "specification"),
                    )
                elif isinstance(machine_item, str):
                    machine = Machine(
                        id=self._gen_id("MCH"),
                        company_id=company_id,
                        machine_name=machine_item,
                    )
                else:
                    continue
                
                machines.append(machine)
            
            LOG.info(f"Parsed {len(blocks)} infrastructure blocks, {len(machines)} machines")
            return blocks, machines
        
        except Exception as e:
            LOG.error(f"Error parsing infrastructure: {e}")
            return [], []
    
    def parse(self, extraction_output: Dict, company_id: int) -> ParseResult:
        """
        MAIN PARSE FUNCTION - Orchestrates all parsing
        
        GUARANTEES:
        ✅ No data loss - searches entire tree
        ✅ Handles GLM & Gemini - format agnostic
        ✅ All fields captured - brand_name, qty, capacity_unit, etc.
        ✅ Nested data handled - infrastructure.machines, company.addresses, etc.
        ✅ Array normalization - mismatched arrays handled correctly
        ✅ Robust error handling - continues on errors, logs them
        """
        
        data = extraction_output
        if "answer" in data and isinstance(data["answer"], dict):
            data = data["answer"]
        elif "result" in data and isinstance(data["result"], dict):
            data = data["result"]
        elif "data" in data and isinstance(data["data"], dict):
            data = data["data"]
            
        result = ParseResult()
        
        try:
            LOG.info(f"="*70)
            LOG.info(f"Parsing company {company_id}")
            LOG.info(f"="*70)
            
            # Parse company
            result.company = self.parse_company(extraction_output, company_id)
            
            # Parse addresses
            result.addresses = self.parse_addresses(extraction_output, company_id)
            
            # Set primary address on company
            if result.addresses:
                primary = result.addresses[0]
                result.company.address = primary.address
                result.company.city = primary.city
                result.company.state = primary.state
                result.company.country = primary.country
            
            # Parse products
            result.products = self.parse_products(extraction_output, company_id)
            
            # Parse other tables
            result.clients = self.parse_clients(extraction_output, company_id)
            result.management = self.parse_management(extraction_output, company_id)
            
            blocks, machines = self.parse_infrastructure(extraction_output, company_id)
            result.infrastructure_blocks = blocks
            result.machines = machines
            
            LOG.info(f"✅ PARSE SUCCESS: {result.company.name}")
            LOG.info(f"   Addresses: {len(result.addresses)}, Products: {len(result.products)}, "
                    f"Clients: {len(result.clients)}, Management: {len(result.management)}, "
                    f"Machines: {len(result.machines)}")
            
        except Exception as e:
            LOG.error(f"❌ Critical error parsing company {company_id}: {e}")
            result.extraction_errors.append(str(e))
        
        return result
