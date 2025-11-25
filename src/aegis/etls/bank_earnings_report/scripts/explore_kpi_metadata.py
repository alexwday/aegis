"""
Exploration script for kpi_metadata table and join with benchmarking_report.

Run this on your work computer to understand the KPI metadata structure.

Usage:
    cd /path/to/aegis
    source venv/bin/activate
    python -m aegis.etls.bank_earnings_report.scripts.explore_kpi_metadata
"""

import asyncio
from tabulate import tabulate

from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from sqlalchemy import text

setup_logging()
logger = get_logger()


async def explore_kpi_metadata():
    """Explore the kpi_metadata table and its relationship with benchmarking_report."""

    async with get_connection() as conn:
        print("\n" + "=" * 80)
        print("KPI_METADATA TABLE EXPLORATION")
        print("=" * 80)

        # 1. Total row count
        print("\n### 1. Total KPI Metadata Rows ###")
        result = await conn.execute(text("SELECT COUNT(*) FROM kpi_metadata"))
        count = result.scalar()
        print(f"Total KPIs defined: {count}")

        # 2. Show all columns in kpi_metadata
        print("\n### 2. KPI Metadata Columns ###")
        result = await conn.execute(text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'kpi_metadata'
            ORDER BY ordinal_position
        """))
        rows = result.fetchall()
        print(tabulate(rows, headers=["column_name", "data_type"], tablefmt="simple"))

        # 3. Sample of kpi_metadata
        print("\n### 3. Sample KPI Metadata (first 10) ###")
        result = await conn.execute(text("""
            SELECT id, kpi_name, LEFT(description, 60) as description, unit, higher_is_better
            FROM kpi_metadata
            ORDER BY id
            LIMIT 10
        """))
        rows = result.fetchall()
        print(tabulate(
            rows,
            headers=["id", "kpi_name", "description", "unit", "higher_is_better"],
            tablefmt="simple"
        ))

        # 4. Check how kpi_name maps to Parameter in benchmarking_report
        print("\n### 4. KPI Name to Parameter Mapping Check ###")
        print("Checking if kpi_metadata.kpi_name matches benchmarking_report.Parameter...")

        result = await conn.execute(text("""
            SELECT
                km.kpi_name,
                COUNT(br."Parameter") as matches_in_benchmarking
            FROM kpi_metadata km
            LEFT JOIN benchmarking_report br ON km.kpi_name = br."Parameter"
            GROUP BY km.kpi_name
            ORDER BY matches_in_benchmarking DESC
            LIMIT 20
        """))
        rows = result.fetchall()
        print(tabulate(rows, headers=["kpi_name", "matches_in_benchmarking"], tablefmt="simple"))

        # 5. Parameters in benchmarking_report NOT in kpi_metadata
        print("\n### 5. Parameters in benchmarking_report NOT in kpi_metadata ###")
        result = await conn.execute(text("""
            SELECT DISTINCT br."Parameter"
            FROM benchmarking_report br
            LEFT JOIN kpi_metadata km ON br."Parameter" = km.kpi_name
            WHERE km.kpi_name IS NULL
            ORDER BY br."Parameter"
            LIMIT 20
        """))
        rows = result.fetchall()
        if rows:
            for row in rows:
                print(f"  {row[0]}")
        else:
            print("  All parameters have matching KPI metadata!")

        # 6. Sample joined data for RBC Q3 2024
        print("\n### 6. Sample Joined Data: RBC Q3 2024 (Enterprise Platform) ###")
        result = await conn.execute(text("""
            SELECT
                br."Parameter",
                br."Actual",
                br."QoQ",
                br."YoY",
                br."Units",
                LEFT(km.description, 40) as description,
                km.unit as meta_unit,
                km.higher_is_better
            FROM benchmarking_report br
            LEFT JOIN kpi_metadata km ON br."Parameter" = km.kpi_name
            WHERE br."bank_symbol" = 'RY-CA'
              AND br."fiscal_year" = 2024
              AND br."quarter" = 'Q3'
              AND br."Platform" = 'Enterprise'
            ORDER BY br."Parameter"
            LIMIT 20
        """))
        rows = result.fetchall()
        if rows:
            print(tabulate(
                rows,
                headers=["Parameter", "Actual", "QoQ", "YoY", "Units", "description", "meta_unit", "higher_better"],
                tablefmt="simple"
            ))
        else:
            print("No data found for RBC Q3 2024. Trying to find available periods...")
            result = await conn.execute(text("""
                SELECT DISTINCT "fiscal_year", "quarter"
                FROM benchmarking_report
                WHERE "bank_symbol" = 'RY-CA'
                ORDER BY "fiscal_year" DESC, "quarter" DESC
                LIMIT 5
            """))
            rows = result.fetchall()
            print("Available RBC periods:")
            for row in rows:
                print(f"  {row[1]} {row[0]}")

        # 7. Count of metrics per bank/period (Enterprise only)
        print("\n### 7. Count of Enterprise Metrics per Bank/Period ###")
        result = await conn.execute(text("""
            SELECT "bank_symbol", "fiscal_year", "quarter", COUNT(*) as metric_count
            FROM benchmarking_report
            WHERE "Platform" = 'Enterprise'
            GROUP BY "bank_symbol", "fiscal_year", "quarter"
            ORDER BY "fiscal_year" DESC, "quarter" DESC, "bank_symbol"
            LIMIT 15
        """))
        rows = result.fetchall()
        print(tabulate(rows, headers=["bank_symbol", "year", "quarter", "metric_count"], tablefmt="simple"))

        # 8. Show key_drivers and analyst_usage columns
        print("\n### 8. KPI Metadata with key_drivers and analyst_usage ###")
        result = await conn.execute(text("""
            SELECT kpi_name, LEFT(key_drivers, 50) as key_drivers, LEFT(analyst_usage, 50) as analyst_usage
            FROM kpi_metadata
            WHERE key_drivers IS NOT NULL OR analyst_usage IS NOT NULL
            LIMIT 10
        """))
        rows = result.fetchall()
        if rows:
            print(tabulate(
                rows,
                headers=["kpi_name", "key_drivers", "analyst_usage"],
                tablefmt="simple"
            ))
        else:
            print("No key_drivers or analyst_usage data found.")


def main():
    """Entry point."""
    print("\nConnecting to database and exploring kpi_metadata table...")
    asyncio.run(explore_kpi_metadata())
    print("\n" + "=" * 80)
    print("Exploration complete!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
