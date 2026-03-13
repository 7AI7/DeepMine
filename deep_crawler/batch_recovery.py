"""
batch_recovery.py

Utility to check and resume a stuck batch job without resubmitting.
"""

import os
import asyncio
import json
from pathlib import Path
from google import genai
from datetime import datetime, timezone
import logging

LOG = logging.getLogger(__name__)

async def resume_batch_from_file(batch_dir_path: str):
    """
    Resume polling a batch that was interrupted.
    
    Args:
        batch_dir_path: Path to the batch_jobs/batch_* directory
    """
    batch_dir = Path(batch_dir_path)
    
    # Look for saved batch job name
    recovery_file = batch_dir / 'batch_job_name_for_recovery.txt'
    if not recovery_file.exists():
        print(f"❌ No recovery file found in {batch_dir}")
        print(f"   Expected: {recovery_file}")
        return
    
    batch_job_name = recovery_file.read_text(encoding='utf-8').strip()
    print(f"📋 Found batch job: {batch_job_name}")
    
    # Initialize client
    api_key = os.environ.get('GEMINI_API_KEY')
    if not api_key:
        raise ValueError('GEMINI_API_KEY not set')
    
    client = genai.Client(api_key=api_key)
    
    # Query status
    batch = client.batches.get(name=batch_job_name)
    state = getattr(batch.state, 'name', str(batch.state))
    
    print(f"\n🔍 Batch Status:")
    print(f"   State: {state}")
    print(f"   Created: {batch.create_time}")
    
    elapsed = (datetime.now(timezone.utc) - batch.create_time).total_seconds()
    print(f"   Elapsed: {int(elapsed/3600)}h {int((elapsed % 3600)/60)}m")
    
    if hasattr(batch, 'batch_stats'):
        stats = batch.batch_stats
        print(f"   Requests: {stats.request_count} total, {stats.pending_request_count} pending")
    
    if state == 'JOB_STATE_SUCCEEDED':
        print(f"\n✅ Batch SUCCEEDED! You can now download results.")
        print(f"   Output URI: {getattr(batch, 'output_uri', 'N/A')}")
        return True
    
    elif state == 'JOB_STATE_FAILED':
        print(f"\n❌ Batch FAILED.")
        err = getattr(batch, 'error', None)
        if err:
            print(f"   Error: {getattr(err, 'message', 'unknown')}")
        return False
    
    elif state == 'JOB_STATE_PENDING':
        if elapsed > 72 * 3600:
            print(f"\n⚠️  TIMEOUT: Batch exceeded 72h (will auto-cancel soon)")
            return False
        else:
            hours_left = (72 * 3600 - elapsed) / 3600
            print(f"\n⏳ Still PENDING. Will auto-timeout in {int(hours_left)}h")
            print(f"   You can:")
            print(f"   1. Continue waiting: python -m crawler.batch_recovery {batch_dir_path} --wait")
            print(f"   2. Cancel now: python -m crawler.batch_recovery {batch_dir_path} --cancel")
            return None
    
    else:
        print(f"\n❓ Batch in state: {state}")
        return None

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m crawler.batch_recovery <batch_dir_path> [--wait|--cancel]")
        sys.exit(1)
    
    batch_dir = sys.argv[1]
    action = sys.argv[2] if len(sys.argv) > 2 else None
    
    result = asyncio.run(resume_batch_from_file(batch_dir))
