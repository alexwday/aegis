"""Configuration for CM Readthrough ETL.

This config allows overriding the default model names used in the ETL
for capital markets readthrough analysis. The ETL processes multiple banks
to extract Investment Banking & Trading outlook and categorized Q&A.
"""

import os
import yaml
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any

# Model configuration for different LLM calls
# The ETL uses two distinct model tiers:
# 1. IB/Trading outlook extraction from transcripts
# 2. Q&A categorization and extraction
MODELS = {
    # Model for IB & Trading outlook extraction - using gpt-4-turbo for 32768 token support
    "ib_trading_extraction": os.getenv("CM_READTHROUGH_IB_MODEL", "gpt-4-turbo"),

    # Model for Q&A categorization and extraction - using gpt-4-turbo for 32768 token support
    "qa_categorization": os.getenv("CM_READTHROUGH_QA_MODEL", "gpt-4-turbo"),
}

# No fallback to main config - each ETL has its own config

# Other configuration options
TEMPERATURE = float(os.getenv("CM_READTHROUGH_TEMPERATURE", "0.7"))
MAX_TOKENS = int(os.getenv("CM_READTHROUGH_MAX_TOKENS", "4096"))  # Max tokens supported by gpt-4-turbo

def get_monitored_institutions() -> List[Dict[str, Any]]:
    """
    Load monitored institutions from YAML file.

    Returns:
        List of institution dictionaries with bank_id, bank_symbol, bank_name
    """
    config_dir = Path(__file__).parent
    monitored_file = config_dir / "monitored_institutions.yaml"

    if not monitored_file.exists():
        raise FileNotFoundError(f"Monitored institutions file not found: {monitored_file}")

    with open(monitored_file, 'r') as f:
        data = yaml.safe_load(f)

    # Convert YAML structure to list of dictionaries
    # YAML format: ticker: {id, name, type, path_safe_name}
    institutions = []
    for ticker, info in data.items():
        institutions.append({
            "bank_id": info["id"],
            "bank_symbol": ticker,
            "bank_name": info["name"],
            "type": info.get("type", ""),
            "path_safe_name": info.get("path_safe_name", "")
        })

    return institutions

def get_categories() -> List[Dict[str, Any]]:
    """
    Load capital markets categories from Excel file.

    Returns:
        List of category dictionaries with Category and Description
    """
    config_dir = Path(__file__).parent
    categories_file = config_dir / "capital_markets_categories.xlsx"

    if not categories_file.exists():
        raise FileNotFoundError(f"Categories file not found: {categories_file}")

    df = pd.read_excel(categories_file)
    # Assuming the Excel has columns: Category, Description, etc.
    return df.to_dict('records')