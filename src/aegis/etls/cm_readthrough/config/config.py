"""Configuration for CM Readthrough ETL.

This config allows overriding the default model names used in the ETL
for capital markets readthrough analysis. The ETL processes multiple banks
to extract capital markets outlook statements and categorized analyst questions
across three sections: Outlook, Market Volatility/Regulatory Q&A, and Pipelines/Activity Q&A.
"""

import os
import yaml
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any

# Model configuration for different LLM calls
# The redesigned ETL uses three model configurations:
# 1. Outlook extraction from full transcripts
# 2. Q&A question extraction
# 3. Batch formatting of all outlook statements
MODELS = {
    # Model for outlook extraction - using gpt-4-turbo for 32768 token support
    "outlook_extraction": os.getenv("CM_READTHROUGH_OUTLOOK_MODEL", "gpt-4-turbo"),

    # Model for Q&A question extraction - using gpt-4-turbo for 32768 token support
    "qa_extraction": os.getenv("CM_READTHROUGH_QA_MODEL", "gpt-4-turbo"),

    # Model for batch formatting - using gpt-4-turbo for consistency
    "batch_formatting": os.getenv("CM_READTHROUGH_FORMAT_MODEL", "gpt-4-turbo"),
}

# No fallback to main config - each ETL has its own config

# Other configuration options
TEMPERATURE = float(os.getenv("CM_READTHROUGH_TEMPERATURE", "0.7"))
MAX_TOKENS = int(os.getenv("CM_READTHROUGH_MAX_TOKENS", "4096"))

# Concurrency control
MAX_CONCURRENT_BANKS = int(os.getenv("CM_READTHROUGH_MAX_CONCURRENT", "5"))

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

def load_categories(filename: str) -> List[Dict[str, Any]]:
    """
    Load category structure from Excel file.

    Excel structure:
    - Column A: Category (required)
    - Column B: Description (required)
    - Column C: Example 1 (optional)
    - Column D: Example 2 (optional)
    - Column E: Example 3 (optional)

    Args:
        filename: Excel file in config directory (e.g., "outlook_categories.xlsx")

    Returns:
        List of category dictionaries with structure:
        {
            "category": "M&A",
            "description": "Mergers and acquisitions...",
            "examples": ["Pipeline commentary", "Deal flow"]
        }
    """
    config_dir = Path(__file__).parent
    file_path = config_dir / filename

    if not file_path.exists():
        raise FileNotFoundError(f"Categories file not found: {file_path}")

    df = pd.read_excel(file_path)

    # Expected columns: Category, Description, Example 1, Example 2, Example 3
    categories = []
    for _, row in df.iterrows():
        # Skip rows without category or description
        if pd.isna(row.iloc[0]) or pd.isna(row.iloc[1]):
            continue

        category = {
            "category": str(row.iloc[0]).strip(),
            "description": str(row.iloc[1]).strip(),
            "examples": []
        }

        # Add examples (columns 2, 3, 4 - optional)
        for i in range(2, min(5, len(row))):
            if not pd.isna(row.iloc[i]):
                example = str(row.iloc[i]).strip()
                if example:  # Only add non-empty examples
                    category["examples"].append(example)

        categories.append(category)

    return categories


def get_outlook_categories() -> List[Dict[str, Any]]:
    """
    Load outlook categories from Excel file.

    Section: Forward-looking outlook statements
    Categories: Investment Banking activity, Global Markets, Sponsor activity, Market catalysts, Competition shifts

    Returns:
        List of category dictionaries for outlook extraction
    """
    return load_categories("outlook_categories.xlsx")


def get_qa_market_volatility_regulatory_categories() -> List[Dict[str, Any]]:
    """
    Load Q&A categories for market volatility and regulatory themes.

    Section: Market volatility, line-draws, and regulatory changes
    Categories: Global Markets, Risk Management, Corporate Banking, Regulatory Changes

    Returns:
        List of category dictionaries for Q&A extraction
    """
    return load_categories("qa_market_volatility_regulatory_categories.xlsx")


def get_qa_pipelines_activity_categories() -> List[Dict[str, Any]]:
    """
    Load Q&A categories for pipeline strength and activity levels.

    Section: Pipeline resilience and areas of activity
    Categories: Investment Banking and M&A activity, Transaction Banking

    Returns:
        List of category dictionaries for Q&A extraction
    """
    return load_categories("qa_pipelines_activity_categories.xlsx")


