#!/usr/bin/env python3
"""
Reprocess existing cleaned_pages.ndjson files to convert HTML to markdown.
Processes companies in numerical order by company ID.

Usage:
    # Single company
    python reprocess_by_company_id.py --company-id 101

    # Range of companies (ID 100 to 105)
    python reprocess_by_company_id.py --start-id 100 --end-id 105

    # All companies
    python reprocess_by_company_id.py --all
"""

import argparse
import json
import re
import sys
from pathlib import Path
from html import unescape
from bs4 import BeautifulSoup

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))
from crawler import settings


def html_to_markdown(html_text: str) -> str:
    """
    Convert HTML to clean markdown using BeautifulSoup.
    Preserves structure (headings, lists, tables) for LLM extraction.
    """
    if not html_text or len(html_text.strip()) < 10:
        return ""
    
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # Convert headings to markdown
    for i in range(1, 7):
        for tag in soup.find_all(f'h{i}'):
            tag.replace_with(f"\n{'#' * i} {tag.get_text().strip()}\n")
    
    # Convert lists
    for li in soup.find_all('li'):
        li.replace_with(f"\n- {li.get_text()}")
    
    # Convert tables (simple pipe format)
    for tr in soup.find_all('tr'):
        cells = [td.get_text().strip() for td in tr.find_all(['td', 'th'])]
        if cells:
            tr.replace_with("\n | " + " | ".join(cells) + " |")
    
    # Get final text
    text = soup.get_text()
    
    # Clean up whitespace
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    
    return unescape(text.strip())


def extract_company_id(folder_name: str) -> int:
    """Extract company ID from folder name like '101_www.example.com'"""
    match = re.match(r'^(\d+)_', folder_name)
    return int(match.group(1)) if match else -1


def find_company_folders(data_dir: Path, company_ids: list[int] = None) -> list[tuple[int, Path]]:
    """
    Find all company folders in data/ab directory.
    Returns list of (company_id, folder_path) tuples sorted by company_id.
    """
    if not data_dir.exists():
        print(f"❌ Data directory not found: {data_dir}")
        return []
    
    folders = []
    
    for folder in data_dir.iterdir():
        if not folder.is_dir():
            continue
        
        company_id = extract_company_id(folder.name)
        
        if company_id == -1:
            continue
        
        # Filter by company_ids if specified
        if company_ids and company_id not in company_ids:
            continue
        
        # Check if cleaned_pages.ndjson exists
        ndjson_file = folder / "cleaned_pages.ndjson"
        if ndjson_file.exists():
            folders.append((company_id, folder))
    
    # Sort by company ID
    folders.sort(key=lambda x: x[0])
    
    return folders


def reprocess_cleaned_pages(ndjson_path: Path, dry_run: bool = False) -> dict:
    """
    Reprocess a single cleaned_pages.ndjson file.
    Returns stats dict.
    """
    if not ndjson_path.exists():
        return {"error": "File not found"}
    
    stats = {
        "total": 0,
        "converted": 0,
        "skipped": 0,
        "errors": 0,
        "total_original_len": 0,      
        "total_markdown_len": 0 
    }
    
    tmp_path = ndjson_path.with_suffix('.ndjson.tmp')
    backup_path = ndjson_path.with_suffix('.ndjson.backup')
    
    try:
        # Stream processing
        with open(ndjson_path, 'r', encoding='utf-8') as fin:
            with open(tmp_path, 'w', encoding='utf-8') as fout:
                for line_num, line in enumerate(fin, 1):
                    if not line.strip():
                        continue
                    
                    stats["total"] += 1
                    
                    try:
                        page = json.loads(line)
                        text = page.get("text", "")
                        
                        # Skip if already markdown (no HTML tags)
                        if not ('<' in text and '>' in text):
                            fout.write(line)
                            stats["skipped"] += 1
                            continue
                        
                        # Convert HTML to markdown
                        markdown = html_to_markdown(text)
                        
                        # Calculate length change
                        original_len = len(text)
                        markdown_len = len(markdown)
                        reduction_pct = ((original_len - markdown_len) / original_len * 100) if original_len > 0 else 0
                        
                        # Validation: check if conversion was successful
                        if not markdown or len(markdown) < 50:
                            print(f"    ⚠️ Line {line_num}: Conversion produced short output, keeping original")
                            fout.write(line)
                            stats["errors"] += 1
                            continue
                        if dry_run and stats["converted"] < 5:
                            print(f"    Page {line_num}: {original_len:,} chars → {markdown_len:,} chars ({reduction_pct:+.1f}%)")
                        
                        # Update page
                        page["text"] = markdown
                        page["cleaned_kind"] = "html_to_markdown"
                        stats["total_original_len"] += original_len    
                        stats["total_markdown_len"] += markdown_len    
                        fout.write(json.dumps(page, ensure_ascii=False) + '\n')
                        stats["converted"] += 1
                    
                    except Exception as e:
                        print(f"    ❌ Line {line_num}: {e}")
                        fout.write(line)
                        stats["errors"] += 1
        
        # Atomic write
        if not dry_run:
            if backup_path.exists():
                backup_path.unlink()
            
            ndjson_path.rename(backup_path)
            tmp_path.rename(ndjson_path)
        else:
            tmp_path.unlink()  # Clean up temp file in dry run
        
        return stats
    
    except Exception as e:
        print(f"    ❌ Fatal error: {e}")
        if tmp_path.exists():
            tmp_path.unlink()
        return {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(
        description="Reprocess cleaned_pages.ndjson files by company ID"
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--company-id",
        type=int,
        help="Process single company by ID (e.g., 101)"
    )
    group.add_argument(
        "--start-id",
        type=int,
        help="Start company ID for range (use with --end-id)"
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Process all companies"
    )
    
    parser.add_argument(
        "--end-id",
        type=int,
        help="End company ID for range (use with --start-id)"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/ab"),
        help="Data directory (default: data/ab)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without writing files"
    )
    
    args = parser.parse_args()
    
    # Determine company IDs to process
    company_ids = None
    
    if args.company_id:
        company_ids = [args.company_id]
        print(f"Processing company ID: {args.company_id}")
    elif args.start_id and args.end_id:
        company_ids = list(range(args.start_id, args.end_id + 1))
        print(f"Processing company IDs: {args.start_id} to {args.end_id}")
    elif args.start_id:
        parser.error("--start-id requires --end-id")
    elif args.all:
        print("Processing ALL companies")
    
    # Find company folders
    folders = find_company_folders(args.data_dir, company_ids)
    
    if not folders:
        print(f"❌ No company folders found")
        return
    
    print(f"✓ Found {len(folders)} companies to process")
    
    if args.dry_run:
        print("⚠️  DRY RUN MODE - No changes will be written\n")
    
    # Process each company
    total_stats = {
        "total": 0,
        "converted": 0,
        "skipped": 0,
        "errors": 0
    }
    
    for company_id, folder in folders:
        ndjson_path = folder / "cleaned_pages.ndjson"
        
        print(f"\n{'='*60}")
        print(f"[{company_id}] {folder.name}")
        print(f"{'='*60}")
        
        stats = reprocess_cleaned_pages(ndjson_path, dry_run=args.dry_run)
        
        if "error" in stats:
            print(f"  ❌ Error: {stats['error']}")
            continue
        
        # Update totals
        for key in total_stats:
            total_stats[key] += stats.get(key, 0)
        
        # Print stats
        print(f"  ✓ Total pages: {stats['total']}")
        print(f"  ✓ Converted: {stats['converted']}")
        print(f"  ✓ Skipped (already markdown): {stats['skipped']}")
        if stats['errors'] > 0:
            print(f"  ⚠️ Errors: {stats['errors']}")
        if stats['converted'] > 0:
            orig_len = stats.get('total_original_len', 0)
            md_len = stats.get('total_markdown_len', 0)
            if orig_len > 0:
                reduction = ((orig_len - md_len) / orig_len * 100)
                print(f"  📊 Size change: {orig_len:,} → {md_len:,} chars ({reduction:+.1f}%)")
        if not args.dry_run:
            backup_path = ndjson_path.with_suffix('.ndjson.backup')
            print(f"  ✓ Backup saved: {backup_path.name}")
    
    # Final summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Companies processed: {len(folders)}")
    print(f"Total pages: {total_stats['total']}")
    print(f"Converted to markdown: {total_stats['converted']}")
    print(f"Already markdown (skipped): {total_stats['skipped']}")
    print(f"Errors: {total_stats['errors']}")
    
    if args.dry_run:
        print("\n⚠️  DRY RUN COMPLETE - No files were modified")
    else:
        print("\n✅ REPROCESSING COMPLETE")


if __name__ == "__main__":
    main()

# python reprocess_by_company_id.py --company-id 135
#python reprocess_by_company_id.py --start-id 100 --end-id 105
#python reprocess_by_company_id.py --all