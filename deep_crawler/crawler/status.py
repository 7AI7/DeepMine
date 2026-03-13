# crawler/status_fixed.py
from pathlib import Path
from crawler import gemini_batch as gbatch

def main():
    root = Path("data/ab")  # or your retry folder
    client = gbatch.get_client()
    
    for site_dir in sorted(root.iterdir()):
        if not site_dir.is_dir():
            continue
        bname_file = site_dir / "batch_name.txt"
        if not bname_file.exists():
            continue
        
        name = bname_file.read_text(encoding="utf-8").strip()
        try:
            b = client.batches.get(name=name)
            state = getattr(b, "state", str(b))
            
            # Correct field for file-based batch output
            dest = getattr(b, "dest", None)
            if dest and hasattr(dest, "file_name") and dest.file_name:
                result_file_name = dest.file_name
                print(f"{site_dir.name} | {name} | state={state} | output_file={result_file_name}")
                
                # Download the output JSONL
                file_content = client.files.download(file=result_file_name)
                output_path = site_dir / "batch_output.ndjson"
                output_path.write_bytes(file_content)
                print(f"  → Downloaded to {output_path}")
            else:
                print(f"{site_dir.name} | {name} | state={state} | NO OUTPUT FILE FOUND")
        except Exception as e:
            print(f"{site_dir.name} | {name} | ERROR: {e}")

if __name__ == "__main__":
    main()

