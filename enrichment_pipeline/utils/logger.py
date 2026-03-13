# Enrichment Pipeline — Logging Setup

import logging
from pathlib import Path

def setup_logger(name: str, log_dir: str = "logs") -> logging.Logger:
    """
    Create a logger that writes to both console (INFO) and file (DEBUG).
    Creates the log directory if it doesn't exist.
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Avoid adding handlers multiple times if called repeatedly
    if logger.handlers:
        return logger
    
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Console handler — INFO level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    # File handler — DEBUG level (full trace)
    fh = logging.FileHandler(
        Path(log_dir) / "enrichment.log", encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    # Rejected matches log — separate file for name mismatch debugging
    rh = logging.FileHandler(
        Path(log_dir) / "rejected_matches.log", encoding="utf-8"
    )
    rh.setLevel(logging.DEBUG)
    rh.setFormatter(formatter)
    rh.addFilter(lambda record: "REJECTED" in record.getMessage())
    logger.addHandler(rh)
    
    return logger
