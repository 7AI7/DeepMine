"""
FINAL PRODUCTION-READY Parser Integration
- Error recovery: Continues on company failures
- Skips corrupted files gracefully
- Logs all errors for audit
- No partial data loss
- Exact column ordering in Excel
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
import pandas as pd
from dataclasses import asdict

# IMPORT THE PARSER
try:
    from robust_data_parser import UniversalDataParser, ParseResult
except ImportError as e:
    print(f"❌ FATAL: Cannot import robust_data_parser: {e}")
    print("Make sure robust_data_parser.py is in the same directory")
    exit(1)

# Setup logging with FILE + CONSOLE
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"parse_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
LOG = logging.getLogger(__name__)

# COLUMN ORDERING (EXACT order for Excel)
EXCEL_COLUMNS = {
    'companies': ['id', 'name', 'website', 'email', 'phone', 'address', 'city', 'state', 
                  'country', 'website_last_updated_on_year', 'infrastructure_available',
                  'contact_person_name', 'contact_person_designation', 'contact_person_contact',
                  'brochure_link', 'linkedin_page'],
    'addresses': ['id', 'company_id', 'address_type', 'address', 'city', 'state', 'country', 'pincode'],
    'products': ['id', 'company_id', 'product_category', 'product', 'application', 'service', 'serving_sector'],
    'clients': ['id', 'company_id', 'client_name', 'industry', 'relationship'],
    'management': ['id', 'company_id', 'name', 'designation', 'contact'],
    'infrastructure_blocks': ['id', 'company_id', 'block_name', 'capacity', 'equipment'],
    'machines': ['id', 'company_id', 'machine_name', 'brand_name', 'qty', 'capacity_value', 'capacity_unit', 'specification'],
}

class DataIntegration:
    """Orchestrates reading → parsing → saving with FULL error recovery"""
    
    def __init__(self, data_root: str = "data"):
        self.data_root = Path(data_root)
        self.parser = UniversalDataParser()
        self.results = []
        self.failed_companies = []  # Track failures
        self.skipped_companies = []  # Track skipped (no file)
        
        LOG.info(f"Initialized with data_root: {self.data_root.absolute()}")
        
        if not self.data_root.exists():
            LOG.error(f"❌ Data root not found: {self.data_root}")
    
    def detect_extraction_type(self, company_folder: Path) -> Optional[str]:
        """Detect if company has GLM or Gemini extraction"""
        glm_file = company_folder / "glm_extraction" / "output.json"
        gemini_file = company_folder / "gemini_extraction" / "final_output.json"
        
        if glm_file.exists():
            return "glm"
        elif gemini_file.exists():
            return "gemini"
        return None
    
    def find_extraction_file(self, company_folder: Path) -> Optional[Path]:
        """Find the extraction file for a company"""
        ext_type = self.detect_extraction_type(company_folder)
        
        if ext_type == "glm":
            return company_folder / "glm_extraction" / "output.json"
        elif ext_type == "gemini":
            return company_folder / "gemini_extraction" / "final_output.json"
        
        return None
    
    def load_json_file(self, filepath: Path) -> Optional[Dict]:
        """Load JSON file with ERROR RECOVERY"""
        try:
            if not filepath.exists():
                LOG.error(f"  ❌ File not found: {filepath}")
                return None
            
            with filepath.open('r', encoding='utf-8') as f:
                data = json.load(f)
                
                if not data:
                    LOG.error(f"  ❌ File is empty: {filepath}")
                    return None
                
                if not isinstance(data, dict):
                    LOG.error(f"  ❌ File is not JSON dict: {filepath}")
                    return None
                
                return data
        
        except json.JSONDecodeError as e:
            LOG.error(f"  ❌ JSON decode error in {filepath}: {e}")
            return None
        except UnicodeDecodeError as e:
            LOG.error(f"  ❌ File encoding error {filepath}: {e}")
            return None
        except PermissionError as e:
            LOG.error(f"  ❌ Permission denied {filepath}: {e}")
            return None
        except Exception as e:
            LOG.error(f"  ❌ Unexpected error loading {filepath}: {type(e).__name__}: {e}")
            return None
    
    def process_company_folder(self, company_folder: Path) -> Optional[ParseResult]:
        """Process a single company folder with ERROR RECOVERY"""
        result = None
        
        try:
            # Extract company_id and domain
            folder_name = company_folder.name
            parts = folder_name.split('_', 1)
            
            try:
                company_id = int(parts[0])
            except (ValueError, IndexError):
                LOG.error(f"⚠️ Invalid folder name format: {folder_name} (expected: {{ID}}_{{domain}})")
                self.skipped_companies.append((folder_name, "Invalid folder name"))
                return None
            
            domain = parts[1] if len(parts) > 1 else "unknown"
            
            LOG.info(f"\n{'='*70}")
            LOG.info(f"Processing Company {company_id} ({domain})")
            
            # Find extraction file
            extraction_file = self.find_extraction_file(company_folder)
            if not extraction_file:
                LOG.warning(f"⚠️ No extraction file found")
                self.skipped_companies.append((company_id, "No extraction file (no glm_extraction/ or gemini_extraction/)"))
                return None
            
            # Detect type
            ext_type = self.detect_extraction_type(company_folder)
            LOG.info(f"  Type: {ext_type.upper()}")
            LOG.info(f"  File: {extraction_file.relative_to(self.data_root)}")
            
            # Load JSON
            data = self.load_json_file(extraction_file)
            if data is None:
                self.failed_companies.append((company_id, "Failed to load JSON"))
                return None
            
            # Parse with error handling
            try:
                result = self.parser.parse(data, company_id)
                
                # Inject special links from special_links.json (saved during triage)
                special_file = company_folder / "special_links.json"
                if special_file.exists():
                    try:
                        special = json.loads(special_file.read_text(encoding='utf-8'))
                        if result.company:
                            if special.get("brochure_pdf"):
                                result.company.brochure_link = special["brochure_pdf"]
                            if special.get("linkedin_company"):
                                result.company.linkedin_page = special["linkedin_company"]
                        LOG.info(f"  📎 Special links: brochure={bool(special.get('brochure_pdf'))}, linkedin={bool(special.get('linkedin_company'))}")
                    except Exception as e:
                        LOG.warning(f"  ⚠️ Failed to read special_links.json: {e}")
                
                self.results.append(result)
                
                # Log summary
                LOG.info(f"  ✅ SUCCESS")
                LOG.info(f"     Company: {result.company.name if result.company else 'N/A'}")
                LOG.info(f"     Addresses: {len(result.addresses)}")
                LOG.info(f"     Products: {len(result.products)}")
                LOG.info(f"     Clients: {len(result.clients)}")
                LOG.info(f"     Management: {len(result.management)}")
                LOG.info(f"     Machines: {len(result.machines)}")
                
                if result.extraction_errors:
                    LOG.warning(f"  ⚠️ Parse errors: {len(result.extraction_errors)}")
                    for err in result.extraction_errors[:3]:  # Show first 3
                        LOG.warning(f"     - {err}")
                
                return result
            
            except Exception as e:
                LOG.error(f"  ❌ Parse error: {type(e).__name__}: {e}")
                self.failed_companies.append((company_id, f"Parse error: {type(e).__name__}"))
                return None
        
        except Exception as e:
            LOG.error(f"  ❌ Unexpected error: {type(e).__name__}: {e}")
            self.failed_companies.append((company_folder.name, f"Unexpected: {type(e).__name__}"))
            return None
    
    def process_all_companies(self):
        """Auto-discover and process ALL companies with ERROR RECOVERY"""
        ab_folder = self.data_root / "ab"
        
        if not ab_folder.exists():
            LOG.error(f"❌ Folder not found: {ab_folder}")
            return
        
        # Find all company folders
        company_folders = []
        try:
            company_folders = sorted([f for f in ab_folder.iterdir() if f.is_dir()])
        except PermissionError as e:
            LOG.error(f"❌ Permission denied reading {ab_folder}: {e}")
            return
        except Exception as e:
            LOG.error(f"❌ Error reading {ab_folder}: {e}")
            return
        
        if not company_folders:
            LOG.warning(f"⚠️ No company folders found in {ab_folder}")
            return
        
        LOG.info(f"\n🔍 Found {len(company_folders)} company folders")
        
        # Process each company
        for idx, folder in enumerate(company_folders, 1):
            LOG.info(f"[{idx}/{len(company_folders)}]", extra={'company': folder.name})
            try:
                self.process_company_folder(folder)
            except KeyboardInterrupt:
                LOG.info("⚠️ Processing interrupted by user")
                break
            except Exception as e:
                LOG.error(f"❌ Unhandled error processing {folder.name}: {e}")
                continue
    
    def results_to_excel(self, output_file: str = "parsed_results.xlsx"):
        """Convert results to Excel with ERROR RECOVERY"""
        output_path = Path(output_file)
        
        LOG.info(f"\n{'='*70}")
        LOG.info(f"Saving to Excel: {output_file}")
        
        # Check if file exists (backup it)
        if output_path.exists():
            backup_file = output_path.with_stem(output_path.stem + "_backup")
            try:
                output_path.rename(backup_file)
                LOG.info(f"  📦 Backed up existing file to: {backup_file.name}")
            except Exception as e:
                LOG.error(f"  ❌ Cannot backup existing file: {e}")
                LOG.error(f"  Using temp name instead...")
                output_file = output_path.with_stem(output_path.stem + "_new").name
        
        # Collect data by table
        try:
            companies_data = []
            addresses_data = []
            products_data = []
            clients_data = []
            management_data = []
            infrastructure_data = []
            machines_data = []
            
            for result in self.results:
                try:
                    if result.company:
                        companies_data.append(asdict(result.company))
                    
                    for addr in result.addresses:
                        addresses_data.append(asdict(addr))
                    
                    for prod in result.products:
                        products_data.append(asdict(prod))
                    
                    for client in result.clients:
                        clients_data.append(asdict(client))
                    
                    for mgmt in result.management:
                        management_data.append(asdict(mgmt))
                    
                    for infra in result.infrastructure_blocks:
                        infrastructure_data.append(asdict(infra))
                    
                    for machine in result.machines:
                        machines_data.append(asdict(machine))
                
                except Exception as e:
                    LOG.error(f"  ❌ Error converting result to dict: {e}")
                    continue
            
            # Write Excel with column ordering
            try:
                with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                    if companies_data:
                        df = pd.DataFrame(companies_data)[EXCEL_COLUMNS['companies']]
                        df.to_excel(writer, sheet_name='companies', index=False)
                        LOG.info(f"  ✓ Sheet 'companies': {len(df)} rows")
                    
                    if addresses_data:
                        df = pd.DataFrame(addresses_data)[EXCEL_COLUMNS['addresses']]
                        df.to_excel(writer, sheet_name='addresses', index=False)
                        LOG.info(f"  ✓ Sheet 'addresses': {len(df)} rows")
                    
                    if products_data:
                        df = pd.DataFrame(products_data)[EXCEL_COLUMNS['products']]
                        df.to_excel(writer, sheet_name='products', index=False)
                        LOG.info(f"  ✓ Sheet 'products': {len(df)} rows")
                    
                    if clients_data:
                        df = pd.DataFrame(clients_data)[EXCEL_COLUMNS['clients']]
                        df.to_excel(writer, sheet_name='clients', index=False)
                        LOG.info(f"  ✓ Sheet 'clients': {len(df)} rows")
                    
                    if management_data:
                        df = pd.DataFrame(management_data)[EXCEL_COLUMNS['management']]
                        df.to_excel(writer, sheet_name='management', index=False)
                        LOG.info(f"  ✓ Sheet 'management': {len(df)} rows")
                    
                    if infrastructure_data:
                        df = pd.DataFrame(infrastructure_data)[EXCEL_COLUMNS['infrastructure_blocks']]
                        df.to_excel(writer, sheet_name='infrastructure_blocks', index=False)
                        LOG.info(f"  ✓ Sheet 'infrastructure_blocks': {len(df)} rows")
                    
                    if machines_data:
                        df = pd.DataFrame(machines_data)[EXCEL_COLUMNS['machines']]
                        df.to_excel(writer, sheet_name='machines', index=False)
                        LOG.info(f"  ✓ Sheet 'machines': {len(df)} rows")
                
                LOG.info(f"✅ Excel file saved: {output_file}\n")
            
            except PermissionError:
                LOG.error(f"  ❌ Permission denied: File may be open in Excel")
                return False
            except Exception as e:
                LOG.error(f"  ❌ Error writing Excel: {type(e).__name__}: {e}")
                return False
        
        except Exception as e:
            LOG.error(f"  ❌ Error collecting data: {e}")
            return False
        
        return True
    
    def results_to_csv(self, output_dir: str = "parsed_csv"):
        """Save results as CSV with ERROR RECOVERY"""
        output_path = Path(output_dir)
        
        LOG.info(f"\n{'='*70}")
        LOG.info(f"Saving to CSV: {output_dir}/")
        
        try:
            output_path.mkdir(exist_ok=True)
        except PermissionError as e:
            LOG.error(f"  ❌ Permission denied creating {output_dir}: {e}")
            return False
        except Exception as e:
            LOG.error(f"  ❌ Error creating {output_dir}: {e}")
            return False
        
        try:
            companies_data = []
            addresses_data = []
            products_data = []
            clients_data = []
            management_data = []
            infrastructure_data = []
            machines_data = []
            
            for result in self.results:
                if result.company:
                    companies_data.append(asdict(result.company))
                for addr in result.addresses:
                    addresses_data.append(asdict(addr))
                for prod in result.products:
                    products_data.append(asdict(prod))
                for client in result.clients:
                    clients_data.append(asdict(client))
                for mgmt in result.management:
                    management_data.append(asdict(mgmt))
                for infra in result.infrastructure_blocks:
                    infrastructure_data.append(asdict(infra))
                for machine in result.machines:
                    machines_data.append(asdict(machine))
            
            if companies_data:
                pd.DataFrame(companies_data)[EXCEL_COLUMNS['companies']].to_csv(
                    output_path / "companies.csv", index=False)
                LOG.info(f"  ✓ companies.csv: {len(companies_data)} rows")
            
            if addresses_data:
                pd.DataFrame(addresses_data)[EXCEL_COLUMNS['addresses']].to_csv(
                    output_path / "addresses.csv", index=False)
                LOG.info(f"  ✓ addresses.csv: {len(addresses_data)} rows")
            
            if products_data:
                pd.DataFrame(products_data)[EXCEL_COLUMNS['products']].to_csv(
                    output_path / "products.csv", index=False)
                LOG.info(f"  ✓ products.csv: {len(products_data)} rows")
            
            if clients_data:
                pd.DataFrame(clients_data)[EXCEL_COLUMNS['clients']].to_csv(
                    output_path / "clients.csv", index=False)
                LOG.info(f"  ✓ clients.csv: {len(clients_data)} rows")
            
            if management_data:
                pd.DataFrame(management_data)[EXCEL_COLUMNS['management']].to_csv(
                    output_path / "management.csv", index=False)
                LOG.info(f"  ✓ management.csv: {len(management_data)} rows")
            
            if infrastructure_data:
                pd.DataFrame(infrastructure_data)[EXCEL_COLUMNS['infrastructure_blocks']].to_csv(
                    output_path / "infrastructure_blocks.csv", index=False)
                LOG.info(f"  ✓ infrastructure_blocks.csv: {len(infrastructure_data)} rows")
            
            if machines_data:
                pd.DataFrame(machines_data)[EXCEL_COLUMNS['machines']].to_csv(
                    output_path / "machines.csv", index=False)
                LOG.info(f"  ✓ machines.csv: {len(machines_data)} rows")
            
            LOG.info(f"✅ All CSVs saved to {output_dir}/\n")
            return True
        
        except Exception as e:
            LOG.error(f"  ❌ Error writing CSVs: {e}")
            return False
    
    def print_summary(self):
        """Print summary of parsing results"""
        LOG.info(f"\n{'='*70}")
        LOG.info(f"FINAL SUMMARY")
        LOG.info(f"{'='*70}")
        LOG.info(f"✅ Successful: {len(self.results)} companies")
        LOG.info(f"❌ Failed: {len(self.failed_companies)} companies")
        LOG.info(f"⏭️  Skipped: {len(self.skipped_companies)} companies")
        
        if self.failed_companies:
            LOG.info(f"\n❌ FAILED COMPANIES:")
            for cid, reason in self.failed_companies:
                LOG.info(f"   {cid}: {reason}")
        
        if self.skipped_companies:
            LOG.info(f"\n⏭️  SKIPPED COMPANIES:")
            for cid, reason in self.skipped_companies:
                LOG.info(f"   {cid}: {reason}")
        
        LOG.info(f"\n📊 DATA SUMMARY:")
        total_addresses = sum(len(r.addresses) for r in self.results)
        total_products = sum(len(r.products) for r in self.results)
        total_clients = sum(len(r.clients) for r in self.results)
        total_machines = sum(len(r.machines) for r in self.results)
        
        LOG.info(f"   Addresses: {total_addresses}")
        LOG.info(f"   Products: {total_products}")
        LOG.info(f"   Clients: {total_clients}")
        LOG.info(f"   Machines: {total_machines}")
        LOG.info(f"\n📁 Logs: {log_file}")
        LOG.info(f"{'='*70}\n")

# ===== MAIN =====

if __name__ == "__main__":
    LOG.info(f"\n{'='*70}")
    LOG.info(f"PARSER STARTED")
    LOG.info(f"{'='*70}\n")
    
    # Initialize
    integration = DataIntegration(data_root="data")
    
    # Process all companies
    integration.process_all_companies()
    
    # Save results
    integration.results_to_excel("parsed_results.xlsx")
    
    # Print summary
    integration.print_summary()
