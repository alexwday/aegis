"""Configuration for WM Readthrough ETL.

This config allows overriding the default model names used in the ETL
for wealth management readthrough analysis. The ETL processes multiple banks
to extract WM narratives, themed Q&A questions, Canadian AM data, and banking metrics
across five pages with different structures and content types.
"""

import os
import yaml
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any

# Model configuration for different LLM calls
# The WM Readthrough ETL uses six model configurations:
# 1. Page 1: WM narrative extraction from full transcripts
# 2. Page 2: Themed Q&A extraction (3 themes)
# 3. Page 3: Canadian AM dual extraction (AUM/flows + focus areas)
# 4. Page 4: Three-column table extraction (NII/NIM, Credit/PCL, Tariffs)
# 5. Page 5+: Six-theme Q&A extraction
# 6. Subtitle generation (requires tool calling support)
MODELS = {
    # Model for Page 1 WM narrative extraction - using gpt-4-turbo for 32768 token support
    "page1_wm_narrative": os.getenv("WM_READTHROUGH_PAGE1_MODEL", "gpt-4-turbo"),

    # Model for Page 2 three-theme Q&A extraction - using gpt-4-turbo
    "page2_three_themes": os.getenv("WM_READTHROUGH_PAGE2_MODEL", "gpt-4-turbo"),

    # Model for Page 3 Canadian AM extraction - using gpt-4-turbo
    "page3_canadian_am": os.getenv("WM_READTHROUGH_PAGE3_MODEL", "gpt-4-turbo"),

    # Model for Page 4 table data extraction - using gpt-4-turbo
    "page4_table_data": os.getenv("WM_READTHROUGH_PAGE4_MODEL", "gpt-4-turbo"),

    # Model for Page 5+ six-theme Q&A extraction - using gpt-4-turbo
    "page5_six_themes": os.getenv("WM_READTHROUGH_PAGE5_MODEL", "gpt-4-turbo"),

    # Model for subtitle generation - using gpt-4.1 for tool calling support
    "subtitle_generation": os.getenv("WM_READTHROUGH_SUBTITLE_MODEL", "gpt-4.1-2025-04-14"),
}

# No fallback to main config - each ETL has its own config

# Other configuration options
TEMPERATURE = float(os.getenv("WM_READTHROUGH_TEMPERATURE", "0.7"))
MAX_TOKENS = int(os.getenv("WM_READTHROUGH_MAX_TOKENS", "4096"))

# Concurrency control
MAX_CONCURRENT_BANKS = int(os.getenv("WM_READTHROUGH_MAX_CONCURRENT", "5"))


def get_monitored_institutions() -> List[Dict[str, Any]]:
    """
    Load monitored institutions from YAML file.

    Returns:
        List of institution dictionaries with bank_id, bank_symbol, bank_name, type
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
        filename: Excel file in config directory (e.g., "page2_themes.xlsx")

    Returns:
        List of category dictionaries with structure:
        {
            "category": "Tariffs and Uncertainty",
            "description": "Questions about tariff impacts...",
            "examples": ["Example 1", "Example 2"]
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


def get_page2_themes() -> List[Dict[str, Any]]:
    """
    Load Page 2 themes from Excel file.

    Page 2 Section: Three main themes for all US banks
    Themes:
    1. Tariffs and Uncertainty
    2. Assets trends and fee-income
    3. Recruitment

    Returns:
        List of category dictionaries for Page 2 Q&A extraction
    """
    return load_categories("page2_themes.xlsx")


def get_page5_themes() -> List[Dict[str, Any]]:
    """
    Load Page 5+ themes from Excel file.

    Page 5+ Section: Six themes for all US banks
    Themes:
    1. NIM
    2. NII guidance
    3. Loan growth
    4. Deposits
    5. Expenses and Technology
    6. Tariffs and Uncertainty

    Returns:
        List of category dictionaries for Page 5+ Q&A extraction
    """
    return load_categories("page5_themes.xlsx")
