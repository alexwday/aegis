"""Configuration for CM Readthrough ETL.

This config allows overriding the default model names used in the ETL
for capital markets readthrough analysis. The ETL processes multiple banks
to extract Investment Banking & Trading outlook and categorized Q&A.
"""

import os
import pandas as pd
from pathlib import Path

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

def get_monitored_institutions():
    """Load monitored institutions from Excel file."""
    config_dir = Path(__file__).parent
    monitored_file = config_dir / "monitored_institutions.xlsx"

    if not monitored_file.exists():
        raise FileNotFoundError(f"Monitored institutions file not found: {monitored_file}")

    df = pd.read_excel(monitored_file)
    return df.to_dict('records')

def get_categories():
    """Load capital markets categories from Excel file."""
    config_dir = Path(__file__).parent
    categories_file = config_dir / "capital_markets_categories.xlsx"

    if not categories_file.exists():
        raise FileNotFoundError(f"Categories file not found: {categories_file}")

    df = pd.read_excel(categories_file)
    # Assuming the Excel has columns: Category, Description, Keywords
    return df.to_dict('records')