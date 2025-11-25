"""
Exploration script for benchmarking_report table (supplementary data).

Run this on your work computer to inspect the table structure and find dividend data.

Usage:
    cd /path/to/aegis
    source venv/bin/activate
    python -m aegis.etls.bank_earnings_report.scripts.explore_supplementary
"""

import asyncio
from tabulate import tabulate

from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from sqlalchemy import text

setup_logging()
logger = get_logger()


async def explore_table():
    """Explore the benchmarking_report table structure and dividend data."""

    async with get_connection() as conn:
        print("\n" + "=" * 80)
        print("BENCHMARKING_REPORT TABLE EXPLORATION")
        print("=" * 80)

        # 1. Total row count
        print("\n### 1. Total Row Count ###")
        result = await conn.execute(text("SELECT COUNT(*) FROM benchmarking_report"))
        count = result.scalar()
        print(f"Total rows: {count:,}")

        # 2. Distinct banks
        print("\n### 2. Distinct Banks ###")
        result = await conn.execute(text("""
            SELECT DISTINCT "bank", "bank_symbol"
            FROM benchmarking_report
            ORDER BY "bank"
        """))
        rows = result.fetchall()
        print(tabulate(rows, headers=["bank", "bank_symbol"], tablefmt="simple"))

        # 3. Distinct periods (yr_Qtr, quarter, fiscal_year)
        print("\n### 3. Sample Periods (first 20) ###")
        result = await conn.execute(text("""
            SELECT DISTINCT "yr_Qtr", "quarter", "fiscal_year"
            FROM benchmarking_report
            ORDER BY "fiscal_year" DESC, "quarter" DESC
            LIMIT 20
        """))
        rows = result.fetchall()
        print(tabulate(rows, headers=["yr_Qtr", "quarter", "fiscal_year"], tablefmt="simple"))

        # 4. Distinct parameters (looking for dividend-related)
        print("\n### 4. All Distinct Parameters ###")
        result = await conn.execute(text("""
            SELECT DISTINCT "Parameter"
            FROM benchmarking_report
            ORDER BY "Parameter"
        """))
        rows = result.fetchall()
        for row in rows:
            param = row[0]
            # Highlight dividend-related parameters
            if param and 'dividend' in param.lower():
                print(f"  >>> {param} <<<  (DIVIDEND RELATED)")
            else:
                print(f"  {param}")

        # 5. Search for dividend in parameter names (case-insensitive)
        print("\n### 5. Dividend-Related Parameters (case-insensitive search) ###")
        result = await conn.execute(text("""
            SELECT DISTINCT "Parameter", "Platform", "Units"
            FROM benchmarking_report
            WHERE LOWER("Parameter") LIKE '%dividend%'
            ORDER BY "Parameter"
        """))
        rows = result.fetchall()
        if rows:
            print(tabulate(rows, headers=["Parameter", "Platform", "Units"], tablefmt="simple"))
        else:
            print("No parameters containing 'dividend' found.")

        # 6. If dividend found, show sample data for one bank/period
        print("\n### 6. Sample Dividend Data (if exists) ###")
        result = await conn.execute(text("""
            SELECT "bank", "bank_symbol", "fiscal_year", "quarter", "Parameter",
                   "Actual", "QoQ", "YoY", "Units", "Platform"
            FROM benchmarking_report
            WHERE LOWER("Parameter") LIKE '%dividend%'
            ORDER BY "fiscal_year" DESC, "quarter" DESC
            LIMIT 10
        """))
        rows = result.fetchall()
        if rows:
            print(tabulate(
                rows,
                headers=["bank", "symbol", "year", "qtr", "Parameter", "Actual", "QoQ", "YoY", "Units", "Platform"],
                tablefmt="simple"
            ))
        else:
            print("No dividend data found.")

        # 7. Distinct Platforms
        print("\n### 7. Distinct Platforms ###")
        result = await conn.execute(text("""
            SELECT DISTINCT "Platform"
            FROM benchmarking_report
            ORDER BY "Platform"
        """))
        rows = result.fetchall()
        for row in rows:
            print(f"  {row[0]}")

        # 8. Distinct Sources
        print("\n### 8. Distinct Sources ###")
        result = await conn.execute(text("""
            SELECT DISTINCT "source"
            FROM benchmarking_report
            ORDER BY "source"
        """))
        rows = result.fetchall()
        for row in rows:
            print(f"  {row[0]}")

        # 9. Sample of all columns for one row
        print("\n### 9. Sample Row (all columns) ###")
        result = await conn.execute(text("""
            SELECT *
            FROM benchmarking_report
            LIMIT 1
        """))
        row = result.fetchone()
        if row:
            columns = result.keys()
            for col, val in zip(columns, row):
                print(f"  {col}: {val}")

        # 10. Check for RBC dividend specifically
        print("\n### 10. RBC Dividend Data (all periods) ###")
        result = await conn.execute(text("""
            SELECT "fiscal_year", "quarter", "Parameter", "Actual", "QoQ", "YoY", "Units"
            FROM benchmarking_report
            WHERE ("bank_symbol" = 'RY-CA' OR "bank" = 'RBC')
              AND LOWER("Parameter") LIKE '%dividend%'
            ORDER BY "fiscal_year" DESC, "quarter" DESC
        """))
        rows = result.fetchall()
        if rows:
            print(tabulate(
                rows,
                headers=["year", "qtr", "Parameter", "Actual", "QoQ", "YoY", "Units"],
                tablefmt="simple"
            ))
        else:
            print("No RBC dividend data found. Trying broader search...")
            # Try searching for any RBC data to see what parameters exist
            result = await conn.execute(text("""
                SELECT DISTINCT "Parameter"
                FROM benchmarking_report
                WHERE "bank_symbol" = 'RY-CA' OR "bank" = 'RBC'
                ORDER BY "Parameter"
                LIMIT 30
            """))
            rows = result.fetchall()
            print("\nRBC parameters available:")
            for row in rows:
                print(f"  {row[0]}")

        # 11. Check for exact "Dividends Declared" parameter
        print("\n### 11. 'Dividends Declared' Parameter (exact match) ###")
        result = await conn.execute(text("""
            SELECT "bank", "bank_symbol", "fiscal_year", "quarter",
                   "Actual", "QoQ", "YoY", "Units", "Platform"
            FROM benchmarking_report
            WHERE "Parameter" = 'Dividends Declared'
            ORDER BY "bank", "fiscal_year" DESC, "quarter" DESC
            LIMIT 20
        """))
        rows = result.fetchall()
        if rows:
            print(tabulate(
                rows,
                headers=["bank", "symbol", "year", "qtr", "Actual", "QoQ", "YoY", "Units", "Platform"],
                tablefmt="simple"
            ))
        else:
            print("No 'Dividends Declared' parameter found. Checking similar names...")
            result = await conn.execute(text("""
                SELECT DISTINCT "Parameter"
                FROM benchmarking_report
                WHERE LOWER("Parameter") LIKE '%divid%'
                ORDER BY "Parameter"
            """))
            rows = result.fetchall()
            print("\nParameters containing 'divid':")
            for row in rows:
                print(f"  {row[0]}")


def main():
    """Entry point."""
    print("\nConnecting to database and exploring benchmarking_report table...")
    asyncio.run(explore_table())
    print("\n" + "=" * 80)
    print("Exploration complete!")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
