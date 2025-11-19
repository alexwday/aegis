"""Configuration for WM Readthrough ETL.

This config provides settings for the template-driven WM Readthrough ETL.
The ETL processes multiple banks using section definitions from CSV templates.
"""

import os
import yaml
from pathlib import Path
from typing import List, Dict, Any

# LLM Configuration
# Single model used for all section extractions (can be overridden per section later)
DEFAULT_MODEL = os.getenv("WM_READTHROUGH_MODEL", "gpt-4-turbo")
TEMPERATURE = float(os.getenv("WM_READTHROUGH_TEMPERATURE", "0.7"))
MAX_TOKENS = int(os.getenv("WM_READTHROUGH_MAX_TOKENS", "4096"))

# Concurrency control
MAX_CONCURRENT_BANKS = int(os.getenv("WM_READTHROUGH_MAX_CONCURRENT", "5"))

# Template configuration
# Path to section definitions template (relative to this config directory)
SECTION_TEMPLATE_PATH = Path(__file__).parent.parent / "templates" / "section_definitions.xlsx"

# Model configuration dictionary (for backward compatibility with main_refactored.py)
MODELS = {
    "page1_wm_narrative": DEFAULT_MODEL,  # Used as fallback in extract_section()
}


def get_monitored_institutions() -> List[Dict[str, Any]]:
    """
    Load monitored institutions from YAML file.

    Returns:
        List of institution dictionaries with bank_id, bank_symbol, bank_name, type

    Institution Types:
        - Monitored_US_Banks: Subset of US banks requiring detailed tracking
        - US_Banks: All US banking institutions
        - Canadian_Asset_Managers: Canadian asset management firms
    """
    config_dir = Path(__file__).parent
    monitored_file = config_dir / "monitored_institutions.yaml"

    if not monitored_file.exists():
        raise FileNotFoundError(f"Monitored institutions file not found: {monitored_file}")

    with open(monitored_file, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    # Convert YAML structure to list of dictionaries
    # YAML format: ticker: {id, name, type, path_safe_name}
    institutions = []
    for ticker, info in data.items():
        institutions.append(
            {
                "bank_id": info["id"],
                "bank_symbol": ticker,
                "bank_name": info["name"],
                "type": info.get("type", ""),
                "path_safe_name": info.get("path_safe_name", ""),
            }
        )

    return institutions
