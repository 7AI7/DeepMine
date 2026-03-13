"""Logging utilities for Tofler scraper"""
import logging
from pathlib import Path
from datetime import datetime
import config

class ScraperLogger:
    def __init__(self):
        self.logs_dir = Path(config.LOGS_DIR)
        self.logs_dir.mkdir(exist_ok=True)
        
        # Main logger
        self.logger = self._setup_main_logger()
        
        # Separate log files
        self.not_found_file = self.logs_dir / "companies_not_found.log"
        self.failed_file = self.logs_dir / "failed_companies.log"
        
    def _setup_main_logger(self):
        logger = logging.getLogger("tofler_scraper")
        logger.setLevel(logging.DEBUG)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter('%(levelname)s: %(message)s')
        console_handler.setFormatter(console_format)
        
        # File handler
        file_handler = logging.FileHandler(self.logs_dir / "scraper.log")
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        file_handler.setFormatter(file_format)
        
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        
        return logger
    
    def log_not_found(self, company_id, company_name):
        """Log companies not found on Tofler"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.not_found_file, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} | {company_id} | {company_name}\n")
        self.logger.warning(f"Company not found: {company_id} - {company_name}")
    
    def log_failed(self, company_id, company_name, error):
        """Log companies that failed after retries"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.failed_file, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} | {company_id} | {company_name} | {error}\n")
        self.logger.error(f"Company failed: {company_id} - {company_name} | Error: {error}")
    
    def info(self, message):
        self.logger.info(message)
    
    def debug(self, message):
        self.logger.debug(message)
    
    def warning(self, message):
        self.logger.warning(message)
    
    def error(self, message):
        self.logger.error(message)
