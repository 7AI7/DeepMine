"""Progress tracking and resumability"""
import json
from pathlib import Path
from datetime import datetime
import config

class ProgressTracker:
    def __init__(self):
        self.progress_file = Path(config.PROGRESS_FILE)
        self.data = self._load_progress()
    
    def _load_progress(self):
        """Load existing progress or initialize new"""
        if self.progress_file.exists():
            with open(self.progress_file, 'r') as f:
                return json.load(f)
        return {
            "total_companies": 0,
            "processed": 0,
            "successful": 0,
            "not_found": 0,
            "failed": 0,
            "last_processed_id": None,
            "processed_ids": [],
            "timestamp": datetime.now().isoformat()
        }
    
    def save_progress(self):
        """Save progress to file"""
        self.data["timestamp"] = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def update(self, company_id, status):
        """Update progress after processing a company
        
        Args:
            company_id: Company ID
            status: 'successful', 'not_found', or 'failed'
        """
        self.data["processed"] += 1
        self.data["last_processed_id"] = company_id
        self.data["processed_ids"].append(company_id)
        
        if status == "successful":
            self.data["successful"] += 1
        elif status == "not_found":
            self.data["not_found"] += 1
        elif status == "failed":
            self.data["failed"] += 1
        
        self.save_progress()
    
    def is_processed(self, company_id):
        """Check if company has been processed"""
        return company_id in self.data.get("processed_ids", [])
    
    def set_total(self, total):
        """Set total companies count"""
        self.data["total_companies"] = total
        self.save_progress()
    
    def get_summary(self):
        """Get progress summary"""
        return {
            "total": self.data["total_companies"],
            "processed": self.data["processed"],
            "successful": self.data["successful"],
            "not_found": self.data["not_found"],
            "failed": self.data["failed"],
            "remaining": self.data["total_companies"] - self.data["processed"]
        }
