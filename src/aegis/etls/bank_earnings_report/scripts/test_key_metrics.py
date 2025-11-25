"""
Test script for key metrics tiles extraction.

Run this on your work computer to test the key metrics extraction pipeline.

Usage:
    cd /path/to/aegis
    source venv/bin/activate
    python -m aegis.etls.bank_earnings_report.scripts.test_key_metrics --bank RY --year 2024 --quarter Q3
"""

import argparse
import asyncio
import json
import uuid

from aegis.connections.oauth_connector import setup_authentication
from aegis.utils.logging import setup_logging, get_logger
from aegis.utils.ssl import setup_ssl
from aegis.etls.bank_earnings_report.main import get_bank_info_from_config
from aegis.etls.bank_earnings_report.retrieval.supplementary import (
    retrieve_all_metrics,
    retrieve_metrics_by_names,
    format_key_metrics_json,
)
from aegis.etls.bank_earnings_report.extraction.key_metrics import (
    select_top_metrics,
    format_metrics_for_llm,
)

setup_logging()
logger = get_logger()


async def test_key_metrics(bank: str, year: int, quarter: str, skip_llm: bool = False):
    """Test key metrics extraction pipeline."""

    execution_id = str(uuid.uuid4())

    print("\n" + "=" * 80)
    print("TESTING KEY METRICS TILES EXTRACTION")
    print("=" * 80)

    # 1. Get bank info from config
    print(f"\n### 1. Bank Info Lookup: '{bank}' ###")
    try:
        bank_info = get_bank_info_from_config(bank)
        print(json.dumps(bank_info, indent=2))
    except ValueError as e:
        print(f"ERROR: {e}")
        return

    # 2. Setup auth for LLM calls
    print(f"\n### 2. Setting up authentication ###")
    ssl_config = setup_ssl()
    auth_config = await setup_authentication(execution_id, ssl_config)

    if not auth_config["success"]:
        print(f"ERROR: Authentication failed: {auth_config.get('error')}")
        return

    context = {
        "execution_id": execution_id,
        "auth_config": auth_config,
        "ssl_config": ssl_config,
    }
    print("Authentication successful!")

    # 3. Retrieve all metrics
    print(f"\n### 3. Retrieving All Metrics ###")
    db_symbol = f"{bank_info['bank_symbol']}-CA"
    print(f"Using bank_symbol: {db_symbol}")

    all_metrics = await retrieve_all_metrics(db_symbol, year, quarter, context)
    print(f"Retrieved {len(all_metrics)} metrics")

    if not all_metrics:
        print("ERROR: No metrics found!")
        return

    # Show first 5 metrics as sample
    print("\nSample metrics (first 5):")
    for m in all_metrics[:5]:
        print(f"  - {m['parameter']}: {m['actual']} (QoQ: {m['qoq']}%, YoY: {m['yoy']}%)")

    # 4. Show formatted table for LLM
    print(f"\n### 4. Metrics Table for LLM ###")
    metrics_table = format_metrics_for_llm(all_metrics)
    # Just show first 20 lines
    table_lines = metrics_table.split("\n")
    print("\n".join(table_lines[:22]))
    if len(table_lines) > 22:
        print(f"... ({len(table_lines) - 22} more rows)")

    # 5. LLM Selection (or skip)
    if skip_llm:
        print(f"\n### 5. Skipping LLM Selection (--skip-llm flag) ###")
        # Use fallback selection
        from aegis.etls.bank_earnings_report.extraction.key_metrics import _fallback_metric_selection
        selected_names = _fallback_metric_selection(all_metrics, 6)
        print("Using fallback selection:")
    else:
        print(f"\n### 5. LLM Selecting Top 6 Metrics ###")
        selected_names = await select_top_metrics(
            metrics=all_metrics,
            bank_name=bank_info["bank_name"],
            quarter=quarter,
            fiscal_year=year,
            context=context,
            num_metrics=6,
        )
        print("LLM selected:")

    for i, name in enumerate(selected_names, 1):
        print(f"  {i}. {name}")

    # 6. Retrieve selected metrics
    print(f"\n### 6. Retrieving Selected Metrics ###")
    selected_metrics = await retrieve_metrics_by_names(
        db_symbol, year, quarter, selected_names, context
    )
    print(f"Retrieved {len(selected_metrics)} metrics")

    # 7. Format JSON output
    print(f"\n### 7. Formatted Key Metrics JSON (1_keymetrics_tiles.json) ###")
    key_metrics_json = format_key_metrics_json(selected_metrics)
    print(json.dumps(key_metrics_json, indent=2))

    print("\n" + "=" * 80)
    print("TEST COMPLETE")
    print("=" * 80 + "\n")


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description="Test key metrics extraction")
    parser.add_argument("--bank", required=True, help="Bank ID, name, or symbol (e.g., RY, RBC)")
    parser.add_argument("--year", type=int, required=True, help="Fiscal year (e.g., 2024)")
    parser.add_argument(
        "--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"], help="Quarter"
    )
    parser.add_argument(
        "--skip-llm", action="store_true", help="Skip LLM selection and use fallback"
    )

    args = parser.parse_args()

    asyncio.run(test_key_metrics(args.bank, args.year, args.quarter, args.skip_llm))


if __name__ == "__main__":
    main()
