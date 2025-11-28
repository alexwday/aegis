"""
Find common platforms across Big 6 Canadian banks for Q2 2025.

This script queries the benchmarking_report table to find:
1. All platforms available for each of the Big 6 banks
2. Common platforms that exist across all 6 banks
3. Platforms with more than 6 metrics available

Usage:
    python -m aegis.etls.bank_earnings_report.scripts.find_common_platforms
"""

import asyncio
from collections import defaultdict
from typing import Dict, List

from sqlalchemy import text

from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import get_logger, setup_logging

setup_logging()
logger = get_logger()

# Big 6 Canadian Banks (excluding Laurentian)
BIG_6_BANKS = [
    {"symbol": "RY-CA", "name": "Royal Bank of Canada"},
    {"symbol": "TD-CA", "name": "Toronto-Dominion Bank"},
    {"symbol": "BNS-CA", "name": "Bank of Nova Scotia"},
    {"symbol": "BMO-CA", "name": "Bank of Montreal"},
    {"symbol": "CM-CA", "name": "CIBC"},
    {"symbol": "NA-CA", "name": "National Bank of Canada"},
]

FISCAL_YEAR = 2025
QUARTER = "Q2"


async def get_platforms_for_bank(bank_symbol: str) -> List[Dict]:
    """
    Get all platforms and their metric counts for a bank.

    Returns:
        List of dicts with platform name and metric count
    """
    async with get_connection() as conn:
        result = await conn.execute(
            text(
                """
                SELECT "Platform", COUNT(*) as metric_count
                FROM benchmarking_report
                WHERE "bank_symbol" = :bank_symbol
                  AND "fiscal_year" = :fiscal_year
                  AND "quarter" = :quarter
                  AND "Platform" != 'Enterprise'
                GROUP BY "Platform"
                ORDER BY metric_count DESC
                """
            ),
            {
                "bank_symbol": bank_symbol,
                "fiscal_year": FISCAL_YEAR,
                "quarter": QUARTER,
            },
        )

        platforms = []
        for row in result.fetchall():
            platforms.append({"platform": row[0], "metric_count": row[1]})

        return platforms


async def main():
    """Main function to find common platforms across Big 6 banks."""
    print(f"\n{'=' * 80}")
    print(f"Platform Analysis for Big 6 Canadian Banks - {QUARTER} {FISCAL_YEAR}")
    print(f"{'=' * 80}\n")

    # Collect platforms for each bank
    bank_platforms: Dict[str, List[Dict]] = {}
    all_platforms: Dict[str, Dict[str, int]] = defaultdict(dict)  # platform -> {bank: count}

    for bank in BIG_6_BANKS:
        platforms = await get_platforms_for_bank(bank["symbol"])
        bank_platforms[bank["symbol"]] = platforms

        print(f"\n{bank['name']} ({bank['symbol']}):")
        print("-" * 50)

        if not platforms:
            print("  No platforms found")
            continue

        for p in platforms:
            print(f"  {p['platform']:<40} {p['metric_count']:>4} metrics")
            all_platforms[p["platform"]][bank["symbol"]] = p["metric_count"]

    # Find common platforms (exist in all 6 banks)
    print(f"\n\n{'=' * 80}")
    print("COMMON PLATFORMS (exist in all 6 banks)")
    print(f"{'=' * 80}\n")

    common_platforms: List[Dict] = []
    for platform, bank_counts in all_platforms.items():
        if len(bank_counts) == 6:
            min_count = min(bank_counts.values())
            max_count = max(bank_counts.values())
            avg_count = sum(bank_counts.values()) / 6
            common_platforms.append(
                {
                    "platform": platform,
                    "min_metrics": min_count,
                    "max_metrics": max_count,
                    "avg_metrics": avg_count,
                    "bank_counts": bank_counts,
                }
            )

    # Sort by average metric count descending
    common_platforms.sort(key=lambda x: x["avg_metrics"], reverse=True)

    for p in common_platforms:
        print(f"\n{p['platform']}")
        print(f"  Min: {p['min_metrics']}, Max: {p['max_metrics']}, Avg: {p['avg_metrics']:.1f}")
        print("  Per bank:", end="")
        for bank in BIG_6_BANKS:
            count = p["bank_counts"].get(bank["symbol"], 0)
            print(f"  {bank['symbol'].split('-')[0]}:{count}", end="")
        print()

    # Filter to platforms with > 6 metrics in all banks
    print(f"\n\n{'=' * 80}")
    print("COMMON PLATFORMS WITH >6 METRICS (in all banks)")
    print(f"{'=' * 80}\n")

    robust_platforms = [p for p in common_platforms if p["min_metrics"] > 6]

    if not robust_platforms:
        print("No platforms found with >6 metrics across all banks")
    else:
        print(f"{'Platform':<40} {'Min':>6} {'Max':>6} {'Avg':>8}")
        print("-" * 60)
        for p in robust_platforms:
            print(
                f"{p['platform']:<40} {p['min_metrics']:>6} "
                f"{p['max_metrics']:>6} {p['avg_metrics']:>8.1f}"
            )

    # Summary
    print(f"\n\n{'=' * 80}")
    print("SUMMARY")
    print(f"{'=' * 80}\n")

    total_unique_platforms = len(all_platforms)
    total_common_platforms = len(common_platforms)
    total_robust_platforms = len(robust_platforms)

    print(f"Total unique platforms across all banks: {total_unique_platforms}")
    print(f"Platforms common to all 6 banks: {total_common_platforms}")
    print(f"Common platforms with >6 metrics: {total_robust_platforms}")

    if robust_platforms:
        print("\nRecommended MONITORED_PLATFORMS list:")
        print("-" * 40)
        for p in robust_platforms:
            print(f'    "{p["platform"]}",')


if __name__ == "__main__":
    asyncio.run(main())
