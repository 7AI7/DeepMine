"""
Finds all companies with empty cleaned_pages.ndjson files
Saves results to Excel with columns: id, url, folder_name
"""
import logging
from pathlib import Path
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s'
)
LOG = logging.getLogger(__name__)

# Configuration
DATA_ROOT = Path("data/ab")
OUTPUT_EXCEL = Path("empty_cleaned_pages.xlsx")


def find_empty_cleaned_pages():
    """
    Scan all company folders and identify those with empty cleaned_pages.ndjson
    Returns: List of dicts with {id, url, folder_name, status}
    """
    empty_files = []
    
    if not DATA_ROOT.exists():
        LOG.error(f"Data directory not found: {DATA_ROOT}")
        return empty_files
    
    # Iterate through all folders
    for company_folder in sorted(DATA_ROOT.iterdir()):
        if not company_folder.is_dir():
            continue
        
        folder_name = company_folder.name
        
        # Extract ID and URL from folder name
        try:
            parts = folder_name.split('_', 1)
            company_id = int(parts[0])
            company_url = parts[1] if len(parts) > 1 else "NO_URL"
        except (ValueError, IndexError):
            LOG.warning(f"Skipping invalid folder name: {folder_name}")
            continue
        
        # Check cleaned_pages.ndjson
        ndjson_path = company_folder / "cleaned_pages.ndjson"
        
        if not ndjson_path.exists():
            empty_files.append({
                'id': company_id,
                'url': company_url,
                'folder_name': folder_name,
                'status': 'FILE_MISSING'
            })
            LOG.info(f"Missing: {folder_name}")
            continue
        
        # Check if file is empty
        try:
            file_size = ndjson_path.stat().st_size
            
            if file_size == 0:
                empty_files.append({
                    'id': company_id,
                    'url': company_url,
                    'folder_name': folder_name,
                    'status': 'EMPTY_0_BYTES'
                })
                LOG.info(f"Empty (0 bytes): {folder_name}")
                continue
            
            # Check line count
            with ndjson_path.open('r', encoding='utf-8') as f:
                lines = f.readlines()
                line_count = len(lines)
                
                if line_count == 0:
                    empty_files.append({
                        'id': company_id,
                        'url': company_url,
                        'folder_name': folder_name,
                        'status': 'EMPTY_0_LINES'
                    })
                    LOG.info(f"Empty (0 lines): {folder_name}")
        
        except Exception as e:
            LOG.error(f"Error reading {ndjson_path}: {e}")
            empty_files.append({
                'id': company_id,
                'url': company_url,
                'folder_name': folder_name,
                'status': f'ERROR: {e}'
            })
    
    return empty_files


def main():
    LOG.info("=" * 70)
    LOG.info("FINDING EMPTY cleaned_pages.ndjson FILES")
    LOG.info("=" * 70)
    
    # Find empty files
    empty_files = find_empty_cleaned_pages()
    
    if not empty_files:
        LOG.info("✓ No empty cleaned_pages.ndjson files found!")
        return
    
    # Create DataFrame
    df = pd.DataFrame(empty_files)
    
    # Sort by ID
    df = df.sort_values('id')
    
    # Save to Excel
    df.to_excel(OUTPUT_EXCEL, index=False, sheet_name='Empty Files')
    
    LOG.info("")
    LOG.info("=" * 70)
    LOG.info(f"📊 SUMMARY:")
    LOG.info(f"   Total empty files: {len(empty_files)}")
    LOG.info(f"   Saved to: {OUTPUT_EXCEL}")
    LOG.info("=" * 70)
    
    # Show first 10 entries
    LOG.info("\n📋 First 10 entries:")
    for item in empty_files[:10]:
        LOG.info(f"   {item['id']}: {item['url']} ({item['status']})")
    
    if len(empty_files) > 10:
        LOG.info(f"   ... and {len(empty_files) - 10} more")


if __name__ == "__main__":
    main()
