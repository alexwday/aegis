"""
Segment Metrics Analyzer - Identifies common and bank-specific metrics across Canadian banks.

This script connects to the benchmarking_report table, analyzes segment availability
and metric coverage across Canadian Big 6 banks, and uses an LLM to recommend
3 core metrics that should be displayed for all banks.

Usage:
    python scripts/analyze_segment_metrics.py                 # Full analysis
    python scripts/analyze_segment_metrics.py --no-llm        # Skip LLM selection
    python scripts/analyze_segment_metrics.py --quarter Q2 --year 2025  # Specific period
"""

import argparse
import asyncio
import json
import sys
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import text

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aegis.connections.llm_connector import complete_with_tools
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import get_logger, setup_logging
from aegis.utils.settings import config
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication

# Initialize logging
setup_logging()
logger = get_logger()

# =============================================================================
# Configuration - Same as segment_metrics.py
# =============================================================================

MONITORED_PLATFORMS = [
    "Canadian Banking",
    "Canadian Wealth & Insurance",
    "U.S. & International Banking",
    "Personal Banking",
    "Commercial Banking",
    "Capital Markets",
    "Corporate Support",
]

CANADIAN_BANK_SYMBOLS = [
    "RY-CA",
    "BMO-CA",
    "CM-CA",
    "NA-CA",
    "BNS-CA",
    "TD-CA",
    "LB-CA",
]

BANK_NAMES = {
    "RY-CA": "Royal Bank of Canada",
    "BMO-CA": "Bank of Montreal",
    "CM-CA": "Canadian Imperial Bank of Commerce",
    "NA-CA": "National Bank of Canada",
    "BNS-CA": "Bank of Nova Scotia",
    "TD-CA": "Toronto-Dominion Bank",
    "LB-CA": "Laurentian Bank",
}

# Metrics to exclude from core metric consideration (enterprise-level/capital metrics)
EXCLUDED_METRICS = [
    "CET1 Ratio",
    "CET1 Capital",
    "Tier 1 Capital Ratio",
    "Total Capital Ratio",
    "Leverage Ratio",
    "RWA",
    "Risk-Weighted Assets",
    "LCR",
    "Liquidity Coverage Ratio",
    "NSFR",
    "Dividends Declared",
    "Book Value per Share",
    "Tangible Book Value per Share",
    "Share Count",
    "Market Cap",
]


# =============================================================================
# Database Functions
# =============================================================================


async def get_available_periods() -> List[Dict[str, Any]]:
    """
    Get all available fiscal year/quarter combinations for Canadian banks.

    Returns:
        List of dicts with fiscal_year, quarter, and bank count
    """
    query = """
        SELECT DISTINCT fiscal_year, quarter, COUNT(DISTINCT bank_symbol) as bank_count
        FROM benchmarking_report
        WHERE bank_symbol = ANY(:symbols)
        GROUP BY fiscal_year, quarter
        ORDER BY fiscal_year DESC, quarter DESC
    """

    async with get_connection() as conn:
        result = await conn.execute(text(query), {"symbols": CANADIAN_BANK_SYMBOLS})
        rows = result.fetchall()

    return [
        {"fiscal_year": row.fiscal_year, "quarter": row.quarter, "bank_count": row.bank_count}
        for row in rows
    ]


async def get_bank_segments(
    bank_symbol: str, fiscal_year: int, quarter: str
) -> List[str]:
    """
    Get all available segments (platforms) for a specific bank and period.

    Args:
        bank_symbol: Bank symbol (e.g., 'RY-CA')
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., 'Q2')

    Returns:
        List of platform names found in the database
    """
    query = """
        SELECT DISTINCT "Platform"
        FROM benchmarking_report
        WHERE bank_symbol = :bank_symbol
          AND fiscal_year = :fiscal_year
          AND quarter = :quarter
          AND "Platform" != 'Enterprise'
        ORDER BY "Platform"
    """

    async with get_connection() as conn:
        result = await conn.execute(
            text(query),
            {"bank_symbol": bank_symbol, "fiscal_year": fiscal_year, "quarter": quarter},
        )
        rows = result.fetchall()

    return [row.Platform for row in rows]


async def get_segment_metrics(
    bank_symbol: str, fiscal_year: int, quarter: str, platform: str
) -> List[str]:
    """
    Get all metric parameters for a specific bank, period, and segment.

    Args:
        bank_symbol: Bank symbol
        fiscal_year: Fiscal year
        quarter: Quarter
        platform: Segment/platform name

    Returns:
        List of parameter (metric) names
    """
    query = """
        SELECT DISTINCT "Parameter"
        FROM benchmarking_report
        WHERE bank_symbol = :bank_symbol
          AND fiscal_year = :fiscal_year
          AND quarter = :quarter
          AND "Platform" = :platform
        ORDER BY "Parameter"
    """

    async with get_connection() as conn:
        result = await conn.execute(
            text(query),
            {
                "bank_symbol": bank_symbol,
                "fiscal_year": fiscal_year,
                "quarter": quarter,
                "platform": platform,
            },
        )
        rows = result.fetchall()

    return [row.Parameter for row in rows]


async def get_metric_details(
    metric_name: str, fiscal_year: int, quarter: str
) -> Dict[str, Any]:
    """
    Get sample metric data across banks for context.

    Args:
        metric_name: Name of the metric
        fiscal_year: Fiscal year
        quarter: Quarter

    Returns:
        Dict with metric details and sample values
    """
    query = """
        SELECT
            bank_symbol,
            "Platform",
            "Actual",
            "QoQ",
            "YoY",
            "Units"
        FROM benchmarking_report
        WHERE "Parameter" = :parameter
          AND fiscal_year = :fiscal_year
          AND quarter = :quarter
          AND bank_symbol = ANY(:symbols)
          AND "Platform" != 'Enterprise'
        LIMIT 10
    """

    async with get_connection() as conn:
        result = await conn.execute(
            text(query),
            {
                "parameter": metric_name,
                "fiscal_year": fiscal_year,
                "quarter": quarter,
                "symbols": CANADIAN_BANK_SYMBOLS,
            },
        )
        rows = result.fetchall()

    samples = []
    for row in rows:
        samples.append(
            {
                "bank": row.bank_symbol,
                "platform": row.Platform,
                "actual": row.Actual,
                "qoq": row.QoQ,
                "yoy": row.YoY,
                "units": row.Units,
            }
        )

    return {"metric": metric_name, "samples": samples}


# =============================================================================
# Analysis Functions
# =============================================================================


def analyze_segment_coverage(
    segment_data: Dict[str, Dict[str, List[str]]]
) -> Dict[str, Dict[str, Any]]:
    """
    Analyze which segments are available across banks.

    Args:
        segment_data: Dict mapping bank_symbol -> {segment: [metrics]}

    Returns:
        Dict with segment availability analysis
    """
    # Count segment occurrences
    segment_banks = defaultdict(set)
    for bank, segments in segment_data.items():
        for segment in segments.keys():
            segment_banks[segment].add(bank)

    # Build analysis
    analysis = {}
    for segment in MONITORED_PLATFORMS:
        banks_with_segment = segment_banks.get(segment, set())
        analysis[segment] = {
            "available_banks": list(banks_with_segment),
            "bank_count": len(banks_with_segment),
            "coverage_pct": len(banks_with_segment) / len(CANADIAN_BANK_SYMBOLS) * 100,
            "is_universal": len(banks_with_segment) == len(CANADIAN_BANK_SYMBOLS),
        }

    return analysis


def analyze_metric_coverage(
    segment_data: Dict[str, Dict[str, List[str]]], segment: str
) -> Dict[str, Any]:
    """
    Analyze metric coverage for a specific segment across all banks.

    Args:
        segment_data: Dict mapping bank_symbol -> {segment: [metrics]}
        segment: Segment name to analyze

    Returns:
        Dict with common and bank-specific metrics
    """
    # Get metrics per bank for this segment
    bank_metrics = {}
    for bank, segments in segment_data.items():
        if segment in segments:
            bank_metrics[bank] = set(segments[segment])

    if not bank_metrics:
        return {
            "segment": segment,
            "banks_with_segment": 0,
            "common_metrics": [],
            "bank_specific_metrics": {},
            "all_metrics": [],
        }

    # Find common metrics (present in ALL banks that have this segment)
    all_bank_metric_sets = list(bank_metrics.values())
    common_metrics = set.intersection(*all_bank_metric_sets) if all_bank_metric_sets else set()

    # Find bank-specific metrics (unique to each bank)
    bank_specific = {}
    for bank, metrics in bank_metrics.items():
        unique = metrics - common_metrics
        if unique:
            bank_specific[bank] = list(sorted(unique))

    # All unique metrics across all banks
    all_metrics = set.union(*all_bank_metric_sets) if all_bank_metric_sets else set()

    return {
        "segment": segment,
        "banks_with_segment": len(bank_metrics),
        "common_metrics": list(sorted(common_metrics)),
        "common_count": len(common_metrics),
        "bank_specific_metrics": bank_specific,
        "all_metrics": list(sorted(all_metrics)),
        "total_unique_metrics": len(all_metrics),
    }


# =============================================================================
# LLM Selection
# =============================================================================


async def select_core_metrics_llm(
    common_metrics: List[str],
    segment: str,
    context: Dict[str, Any],
    num_metrics: int = 3,
) -> Dict[str, Any]:
    """
    Use LLM to select the best core metrics from common metrics.

    Args:
        common_metrics: List of metrics available in all banks
        segment: Segment name for context
        context: Execution context with auth/ssl config
        num_metrics: Number of metrics to select

    Returns:
        Dict with selected metrics and reasoning
    """
    if len(common_metrics) <= num_metrics:
        return {
            "selected_metrics": common_metrics,
            "reasoning": "All common metrics selected (fewer than requested)",
            "method": "auto",
        }

    # Filter out excluded metrics
    eligible_metrics = [m for m in common_metrics if m not in EXCLUDED_METRICS]

    if len(eligible_metrics) <= num_metrics:
        return {
            "selected_metrics": eligible_metrics[:num_metrics],
            "reasoning": "Selected from eligible metrics after exclusions",
            "method": "filtered",
        }

    # Build LLM prompt
    system_prompt = f"""You are a senior financial analyst selecting core performance metrics for a bank earnings report.

Your task is to select exactly {num_metrics} metrics that should be displayed as CORE METRICS for the "{segment}" business segment.

## SELECTION CRITERIA

These core metrics will be shown for EVERY bank, so they must be:

1. **Fundamental Performance Indicators**: The most essential metrics that every analyst expects to see
2. **Comparable Across Banks**: Metrics that are meaningful to compare between different institutions
3. **Standard Industry Metrics**: Widely recognized in the banking industry
4. **Informative**: Tell the key story about segment performance

## IMPORTANT

- Select metrics that make sense together (e.g., don't select 3 different revenue metrics)
- Prefer metrics that are commonly reported in earnings presentations
- Consider what investors and analysts focus on for this segment type
- Return metric names EXACTLY as provided"""

    user_prompt = f"""Select {num_metrics} core metrics for the "{segment}" segment from these common metrics available across all Canadian banks:

{chr(10).join(f"- {m}" for m in eligible_metrics)}

These will be the standard core metrics shown in the left column of the segment performance tile."""

    # Define tool for structured output
    tool_definition = {
        "type": "function",
        "function": {
            "name": "select_core_metrics",
            "description": f"Select {num_metrics} core metrics for the {segment} segment",
            "parameters": {
                "type": "object",
                "properties": {
                    "selected_metrics": {
                        "type": "array",
                        "items": {"type": "string", "enum": eligible_metrics},
                        "description": f"Exactly {num_metrics} metric names to use as core metrics",
                        "minItems": num_metrics,
                        "maxItems": num_metrics,
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Brief explanation of why these metrics were chosen as core metrics",
                    },
                },
                "required": ["selected_metrics", "reasoning"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
            context=context,
            llm_params={
                "model": config.llm.medium.model,
                "temperature": 0.3,
                "max_tokens": 1000,
            },
        )

        # Parse response
        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])

                return {
                    "selected_metrics": function_args.get("selected_metrics", []),
                    "reasoning": function_args.get("reasoning", ""),
                    "method": "llm",
                    "model": config.llm.medium.model,
                }

        # Fallback
        return {
            "selected_metrics": eligible_metrics[:num_metrics],
            "reasoning": "Fallback selection - LLM did not return structured response",
            "method": "fallback",
        }

    except Exception as e:
        logger.error(f"LLM selection error: {e}")
        return {
            "selected_metrics": eligible_metrics[:num_metrics],
            "reasoning": f"Fallback selection due to error: {str(e)}",
            "method": "error_fallback",
        }


# =============================================================================
# Report Generation
# =============================================================================


def print_analysis_report(
    fiscal_year: int,
    quarter: str,
    segment_data: Dict[str, Dict[str, List[str]]],
    segment_coverage: Dict[str, Dict[str, Any]],
    metric_analyses: Dict[str, Dict[str, Any]],
    core_selections: Dict[str, Dict[str, Any]],
):
    """Print comprehensive analysis report to console."""
    print("\n" + "=" * 100)
    print(f"SEGMENT METRICS ANALYSIS - {quarter} {fiscal_year}")
    print("=" * 100)

    # Bank overview
    print("\n📊 BANKS ANALYZED")
    print("-" * 100)
    banks_found = list(segment_data.keys())
    banks_missing = [b for b in CANADIAN_BANK_SYMBOLS if b not in banks_found]
    print(f"Found: {len(banks_found)}/{len(CANADIAN_BANK_SYMBOLS)} Canadian banks")
    for bank in banks_found:
        segment_count = len(segment_data[bank])
        print(f"  ✓ {bank} ({BANK_NAMES.get(bank, 'Unknown')}): {segment_count} segments")
    if banks_missing:
        print(f"\nMissing:")
        for bank in banks_missing:
            print(f"  ✗ {bank} ({BANK_NAMES.get(bank, 'Unknown')})")

    # Segment coverage
    print("\n\n📈 SEGMENT COVERAGE")
    print("-" * 100)
    for segment in MONITORED_PLATFORMS:
        coverage = segment_coverage.get(segment, {})
        bank_count = coverage.get("bank_count", 0)
        pct = coverage.get("coverage_pct", 0)
        status = "✓" if coverage.get("is_universal") else "△" if bank_count > 0 else "✗"
        print(f"{status} {segment}: {bank_count}/{len(CANADIAN_BANK_SYMBOLS)} banks ({pct:.0f}%)")
        if coverage.get("available_banks") and not coverage.get("is_universal"):
            print(f"    Banks: {', '.join(coverage['available_banks'])}")

    # Per-segment metric analysis
    print("\n\n📋 METRIC ANALYSIS BY SEGMENT")
    print("=" * 100)

    for segment in MONITORED_PLATFORMS:
        analysis = metric_analyses.get(segment)
        if not analysis or analysis.get("banks_with_segment", 0) == 0:
            print(f"\n❌ {segment}: No data available")
            continue

        print(f"\n\n{'='*100}")
        print(f"📊 {segment.upper()}")
        print(f"{'='*100}")
        print(f"Banks with segment: {analysis['banks_with_segment']}")
        print(f"Total unique metrics: {analysis['total_unique_metrics']}")
        print(f"Common across all banks: {analysis['common_count']}")

        # Common metrics
        print(f"\n🔵 COMMON METRICS ({analysis['common_count']}):")
        print("-" * 80)
        for metric in analysis["common_metrics"]:
            excluded_marker = " [EXCLUDED]" if metric in EXCLUDED_METRICS else ""
            print(f"  • {metric}{excluded_marker}")

        # Bank-specific metrics
        if analysis["bank_specific_metrics"]:
            print(f"\n🟡 BANK-SPECIFIC METRICS:")
            print("-" * 80)
            for bank, metrics in analysis["bank_specific_metrics"].items():
                print(f"\n  {bank}:")
                for metric in metrics[:10]:  # Limit display
                    print(f"    • {metric}")
                if len(metrics) > 10:
                    print(f"    ... and {len(metrics) - 10} more")

        # Core metric selection
        selection = core_selections.get(segment)
        if selection:
            print(f"\n🎯 RECOMMENDED CORE METRICS:")
            print("-" * 80)
            print(f"Method: {selection.get('method', 'unknown')}")
            print(f"Selection:")
            for i, metric in enumerate(selection.get("selected_metrics", []), 1):
                print(f"  {i}. {metric}")
            if selection.get("reasoning"):
                print(f"\nReasoning: {selection['reasoning']}")

    # Summary
    print("\n\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)

    all_core_metrics = set()
    for selection in core_selections.values():
        all_core_metrics.update(selection.get("selected_metrics", []))

    print(f"\n📌 All recommended core metrics across segments:")
    for metric in sorted(all_core_metrics):
        segments_using = [
            s for s, sel in core_selections.items() if metric in sel.get("selected_metrics", [])
        ]
        print(f"  • {metric}")
        print(f"    Used in: {', '.join(segments_using)}")

    print("\n" + "=" * 100 + "\n")


# =============================================================================
# Main Execution
# =============================================================================


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Analyze segment metrics across Canadian banks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--year",
        type=int,
        help="Fiscal year to analyze (defaults to most recent)",
    )
    parser.add_argument(
        "--quarter",
        type=str,
        help="Quarter to analyze (defaults to most recent)",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Skip LLM-based metric selection",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        help="Output results to JSON file",
    )

    args = parser.parse_args()

    execution_id = str(uuid.uuid4())
    logger.info(f"Starting segment metrics analysis [execution_id={execution_id}]")

    try:
        # Get available periods
        periods = await get_available_periods()
        if not periods:
            print("❌ No data found for Canadian banks in benchmarking_report table")
            return

        # Select period
        if args.year and args.quarter:
            fiscal_year = args.year
            quarter = args.quarter
        else:
            # Use most recent period
            latest = periods[0]
            fiscal_year = latest["fiscal_year"]
            quarter = latest["quarter"]

        print(f"\n📅 Analyzing period: {quarter} {fiscal_year}")
        print(f"   Available periods: {len(periods)}")
        print(f"   Banks with data: {periods[0]['bank_count'] if periods else 0}")

        # Collect segment data for each bank
        segment_data = {}
        for bank_symbol in CANADIAN_BANK_SYMBOLS:
            segments = await get_bank_segments(bank_symbol, fiscal_year, quarter)

            if segments:
                segment_data[bank_symbol] = {}
                for segment in segments:
                    if segment in MONITORED_PLATFORMS:
                        metrics = await get_segment_metrics(
                            bank_symbol, fiscal_year, quarter, segment
                        )
                        segment_data[bank_symbol][segment] = metrics

        if not segment_data:
            print(f"❌ No segment data found for {quarter} {fiscal_year}")
            return

        # Analyze segment coverage
        segment_coverage = analyze_segment_coverage(segment_data)

        # Analyze metrics per segment
        metric_analyses = {}
        for segment in MONITORED_PLATFORMS:
            metric_analyses[segment] = analyze_metric_coverage(segment_data, segment)

        # LLM-based core metric selection
        core_selections = {}
        if not args.no_llm:
            print("\n🤖 Running LLM metric selection...")

            # Setup auth context
            ssl_config = setup_ssl()
            auth_config = await setup_authentication(execution_id, ssl_config)

            if not auth_config.get("success"):
                print(f"⚠️ Authentication failed: {auth_config.get('error')}")
                print("   Running without LLM selection")
                args.no_llm = True
            else:
                context = {
                    "execution_id": execution_id,
                    "auth_config": auth_config,
                    "ssl_config": ssl_config,
                }

                for segment in MONITORED_PLATFORMS:
                    analysis = metric_analyses.get(segment)
                    if analysis and analysis.get("common_metrics"):
                        print(f"   Selecting core metrics for {segment}...")
                        selection = await select_core_metrics_llm(
                            analysis["common_metrics"],
                            segment,
                            context,
                        )
                        core_selections[segment] = selection

        if args.no_llm:
            # Manual fallback - use standard metrics
            for segment in MONITORED_PLATFORMS:
                analysis = metric_analyses.get(segment)
                if analysis and analysis.get("common_metrics"):
                    # Default to first 3 non-excluded metrics
                    eligible = [
                        m for m in analysis["common_metrics"] if m not in EXCLUDED_METRICS
                    ]
                    core_selections[segment] = {
                        "selected_metrics": eligible[:3],
                        "reasoning": "Default selection (first 3 eligible metrics)",
                        "method": "manual",
                    }

        # Print report
        print_analysis_report(
            fiscal_year,
            quarter,
            segment_data,
            segment_coverage,
            metric_analyses,
            core_selections,
        )

        # Output JSON if requested
        if args.output_json:
            output = {
                "fiscal_year": fiscal_year,
                "quarter": quarter,
                "banks_analyzed": list(segment_data.keys()),
                "segment_coverage": segment_coverage,
                "metric_analyses": metric_analyses,
                "core_selections": core_selections,
            }
            with open(args.output_json, "w") as f:
                json.dump(output, f, indent=2)
            print(f"📁 Results saved to: {args.output_json}")

    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
