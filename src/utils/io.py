import logging
import sys
from pathlib import Path

def resolve_csv() -> Path:
    """
    Find hh.csv in several common locations.

    Returns:
        Path: Path to the existing hh.csv file.

    Raises:
        SystemExit: If file is not found.
    """
    candidates = [
        Path("parsing/hh.csv"),
        Path("../parsing/hh.csv"),
        Path("hh.csv"),
    ]
    for candidate_path in candidates:
        if candidate_path.exists():
            return candidate_path
    logging.error("hh.csv not found.")
    sys.exit(1)
