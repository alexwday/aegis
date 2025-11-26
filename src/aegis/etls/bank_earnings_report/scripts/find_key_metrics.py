#!/usr/bin/env python
"""
Standalone script to find key metrics in the database.

Loads RBC Q3 2025 data, filters for Enterprise segment, lists all parameters,
and uses LLM to match them to the 7 recommended key metrics.

Usage:
    python -m aegis.etls.bank_earnings_report.scripts.find_key_metrics
"""

import asyncio
import json
import uuid
from typing import Any, Dict, List

from aegis.connections.llm_connector import complete_with_tools
from aegis.connections.postgres_connector import fetch_all
from aegis.utils.settings import config
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication


# The 7 recommended key metrics we're looking for
TARGET_METRICS = [
    "Adjusted Diluted EPS",
    "Adjusted ROE",
    "Net Interest Margin",
    "Efficiency Ratio",
    "Total Revenue",
    "PPPT",
    "Operating Leverage",
]


async def get_enterprise_parameters(execution_id: str) -> List[Dict[str, Any]]:
    """Fetch all Enterprise segment parameters for RBC Q3 2025."""

    query = """
        SELECT DISTINCT
            parameter,
            units,
            actual,
            qoq,
            yoy
        FROM benchmarking_report
        WHERE bank_symbol = 'RY'
          AND fiscal_year = 2025
          AND quarter = 'Q3'
          AND segment = 'Enterprise'
        ORDER BY parameter
    """

    try:
        results = await fetch_all(query, execution_id=str(execution_id))
        return [dict(row) for row in results]
    except Exception as e:
        if "does not exist" in str(e):
            print("\n❌ ERROR: The 'benchmarking_report' table does not exist.")
            print("   This table needs to be created and populated with bank metrics data.")
            print("\n   The table should have columns:")
            print("   - bank_symbol (e.g., 'RY' for Royal Bank)")
            print("   - fiscal_year (e.g., 2025)")
            print("   - quarter (e.g., 'Q3')")
            print("   - segment (e.g., 'Enterprise')")
            print("   - parameter (metric name)")
            print("   - actual (current value)")
            print("   - units (e.g., 'millions', '%')")
            print("   - qoq (quarter-over-quarter change)")
            print("   - yoy (year-over-year change)")
            return []
        raise


async def match_metrics_with_llm(
    available_params: List[Dict[str, Any]],
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """Use LLM to match available parameters to target metrics."""

    # Format parameters for LLM
    param_list = "\n".join([
        f"- {p['parameter']} (units: {p['units']}, value: {p['actual']})"
        for p in available_params
    ])

    system_prompt = """You are a financial data analyst helping to map database field names
to standard bank metrics.

You will be given:
1. A list of available parameters from a database
2. A list of 7 target metrics we need to find

Your task is to find the BEST MATCH in the database for each target metric.
Some matches may be exact, others may use different naming conventions.

For example:
- "Adjusted Diluted EPS" might be stored as "Diluted EPS (Adjusted)" or "Adj. Diluted EPS"
- "PPPT" might be "Pre-Provision Pre-Tax Earnings" or "Pre-Provision Profit"
- "Adjusted ROE" might be "Return on Equity (Adjusted)" or "ROE - Adjusted"

If a metric has no reasonable match, return null for that metric."""

    user_prompt = f"""## AVAILABLE PARAMETERS IN DATABASE:

{param_list}

## TARGET METRICS TO FIND:

1. Adjusted Diluted EPS - The headline EPS metric, adjusted for one-time items
2. Adjusted ROE - Return on Equity, adjusted basis
3. Net Interest Margin (NIM) - The spread between interest earned and paid
4. Efficiency Ratio - Non-interest expense / Revenue (lower is better)
5. Total Revenue - Top-line revenue figure
6. PPPT (Pre-Provision Pre-Tax Earnings) - Earnings before provisions and taxes
7. Operating Leverage - Revenue growth minus expense growth

For each target metric, find the best matching parameter from the database list.
Return exact parameter names as they appear in the database."""

    tool_definition = {
        "type": "function",
        "function": {
            "name": "map_metrics",
            "description": "Map target metrics to database parameters",
            "parameters": {
                "type": "object",
                "properties": {
                    "adjusted_diluted_eps": {
                        "type": ["string", "null"],
                        "description": "Database parameter matching Adjusted Diluted EPS"
                    },
                    "adjusted_roe": {
                        "type": ["string", "null"],
                        "description": "Database parameter matching Adjusted ROE"
                    },
                    "net_interest_margin": {
                        "type": ["string", "null"],
                        "description": "Database parameter matching Net Interest Margin"
                    },
                    "efficiency_ratio": {
                        "type": ["string", "null"],
                        "description": "Database parameter matching Efficiency Ratio"
                    },
                    "total_revenue": {
                        "type": ["string", "null"],
                        "description": "Database parameter matching Total Revenue"
                    },
                    "pppt": {
                        "type": ["string", "null"],
                        "description": "Database parameter matching PPPT/Pre-Provision Pre-Tax"
                    },
                    "operating_leverage": {
                        "type": ["string", "null"],
                        "description": "Database parameter matching Operating Leverage"
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Explanation of the mappings and any issues found"
                    }
                },
                "required": [
                    "adjusted_diluted_eps",
                    "adjusted_roe",
                    "net_interest_margin",
                    "efficiency_ratio",
                    "total_revenue",
                    "pppt",
                    "operating_leverage",
                    "reasoning"
                ]
            }
        }
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    model_config = config.llm.large

    response = await complete_with_tools(
        messages=messages,
        tools=[tool_definition],
        context=context,
        llm_params={
            "model": model_config.model,
            "temperature": 0.1,
            "max_tokens": 2000,
        },
    )

    if response.get("choices") and response["choices"][0].get("message"):
        message = response["choices"][0]["message"]
        if message.get("tool_calls"):
            tool_call = message["tool_calls"][0]
            return json.loads(tool_call["function"]["arguments"])

    return {"error": "No tool call response"}


async def main():
    """Main entry point."""

    print("=" * 80)
    print("KEY METRICS FINDER - RBC Q3 2025 Enterprise")
    print("=" * 80)

    # Setup context
    execution_id = uuid.uuid4()
    ssl_config = setup_ssl()
    auth_config = await setup_authentication(str(execution_id), ssl_config)

    context = {
        "execution_id": execution_id,
        "ssl_config": ssl_config,
        "auth_config": auth_config,
    }

    # Get available parameters
    print("\n📊 Fetching Enterprise parameters from database...")
    params = await get_enterprise_parameters(execution_id)

    if not params:
        print("\n⚠️  No parameters found. Cannot proceed with metric matching.")
        return

    print(f"\n✅ Found {len(params)} parameters:\n")
    print("-" * 80)
    for p in params:
        value_str = f"{p['actual']}" if p['actual'] is not None else "N/A"
        qoq_str = f"{p['qoq']:+.1f}%" if p['qoq'] is not None else "—"
        yoy_str = f"{p['yoy']:+.1f}%" if p['yoy'] is not None else "—"
        print(f"  {p['parameter']:<40} | {value_str:>12} | QoQ: {qoq_str:>8} | YoY: {yoy_str:>8}")
    print("-" * 80)

    # Use LLM to match
    print("\n🤖 Using LLM to match parameters to target metrics...")
    matches = await match_metrics_with_llm(params, context)

    print("\n" + "=" * 80)
    print("MAPPING RESULTS")
    print("=" * 80)

    target_labels = {
        "adjusted_diluted_eps": "Adjusted Diluted EPS",
        "adjusted_roe": "Adjusted ROE",
        "net_interest_margin": "Net Interest Margin",
        "efficiency_ratio": "Efficiency Ratio",
        "total_revenue": "Total Revenue",
        "pppt": "PPPT",
        "operating_leverage": "Operating Leverage",
    }

    print("\n📋 Recommended KEY_METRICS list for key_metrics.py:\n")
    print("KEY_METRICS = [")
    for key, label in target_labels.items():
        db_param = matches.get(key)
        if db_param:
            print(f'    "{db_param}",  # {label}')
        else:
            print(f'    # MISSING: {label} - no match found')
    print("]")

    print("\n📝 LLM Reasoning:")
    print("-" * 80)
    print(matches.get("reasoning", "No reasoning provided"))
    print("-" * 80)

    # Summary
    found = sum(1 for k in target_labels if matches.get(k))
    print(f"\n✅ Found {found}/7 metrics")

    if found < 7:
        print("\n⚠️  Missing metrics - you may need to:")
        print("   1. Check if the metric exists under a different name")
        print("   2. Add the metric to the database")
        print("   3. Use a different metric as a substitute")


if __name__ == "__main__":
    asyncio.run(main())
