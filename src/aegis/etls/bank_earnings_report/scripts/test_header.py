"""
Test script for header section (header_params + dividend).

Run this on your work computer to test the header extraction.

Usage:
    cd /path/to/aegis
    source venv/bin/activate
    python -m aegis.etls.bank_earnings_report.scripts.test_header --bank RY --year 2024 --quarter Q3
"""

import argparse
import asyncio
import json
import uuid

from aegis.utils.logging import setup_logging, get_logger
from aegis.etls.bank_earnings_report.main import (
    get_bank_info_from_config,
    extract_header_params,
)
from aegis.etls.bank_earnings_report.retrieval.supplementary import (
    retrieve_dividend,
    format_dividend_json,
)

setup_logging()
logger = get_logger()


async def test_header(bank: str, year: int, quarter: str):
    """Test header section extraction."""

    execution_id = str(uuid.uuid4())
    context = {"execution_id": execution_id}

    print("\n" + "=" * 80)
    print("TESTING HEADER SECTION")
    print("=" * 80)

    # 1. Get bank info from config
    print(f"\n### 1. Bank Info Lookup: '{bank}' ###")
    try:
        bank_info = get_bank_info_from_config(bank)
        print(json.dumps(bank_info, indent=2))
    except ValueError as e:
        print(f"ERROR: {e}")
        return

    # 2. Extract header params (no DB needed)
    print(f"\n### 2. Header Params (0_header_params.json) ###")
    header_params = extract_header_params(bank_info, year, quarter)
    print(json.dumps(header_params, indent=2))

    # 3. Retrieve dividend from database
    print(f"\n### 3. Raw Dividend Data from DB ###")
    # Convert symbol format: RY -> RY-CA for DB lookup
    db_symbol = f"{bank_info['bank_symbol']}-CA"
    print(f"Using bank_symbol: {db_symbol}")

    dividend_data = await retrieve_dividend(db_symbol, year, quarter, context)
    if dividend_data:
        print(json.dumps(dividend_data, indent=2))
    else:
        print("No dividend data found!")

    # 4. Format dividend JSON
    print(f"\n### 4. Formatted Dividend (0_header_dividend.json) ###")
    dividend_json = format_dividend_json(dividend_data)
    print(json.dumps(dividend_json, indent=2))

    # 5. Show combined output
    print(f"\n### 5. Combined Header Output ###")
    combined = {
        "0_header_params": header_params,
        "0_header_dividend": dividend_json,
    }
    print(json.dumps(combined, indent=2))

    print("\n" + "=" * 80)
    print("TEST COMPLETE")
    print("=" * 80 + "\n")


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description="Test header section extraction")
    parser.add_argument("--bank", required=True, help="Bank ID, name, or symbol (e.g., RY, RBC)")
    parser.add_argument("--year", type=int, required=True, help="Fiscal year (e.g., 2024)")
    parser.add_argument(
        "--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"], help="Quarter"
    )

    args = parser.parse_args()

    asyncio.run(test_header(args.bank, args.year, args.quarter))


if __name__ == "__main__":
    main()
