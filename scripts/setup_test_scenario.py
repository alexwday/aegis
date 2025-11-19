"""
Setup Test Scenario for ETL Orchestrator

This script prepares the database for end-to-end testing by:
1. Clearing the aegis_reports table
2. Setting up aegis_data_availability with ONE test transcript
3. Verifying the test transcript exists in aegis_transcripts

Test case: Royal Bank of Canada (RY) 2025 Q2
"""

import asyncio
from sqlalchemy import text
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger

setup_logging()
logger = get_logger()

# Test configuration
TEST_BANK_SYMBOL = "RY"
TEST_BANK_NAME = "Royal Bank of Canada"
TEST_BANK_ID = 1
TEST_FISCAL_YEAR = 2025
TEST_QUARTER = "Q2"


async def main():
    """Set up test scenario."""
    logger.info("=" * 80)
    logger.info("SETTING UP ETL ORCHESTRATOR TEST SCENARIO")
    logger.info("=" * 80)

    async with get_connection() as conn:
        # Step 1: Clear aegis_reports table
        logger.info("Step 1: Clearing aegis_reports table...")
        result = await conn.execute(text("DELETE FROM aegis_reports"))
        logger.info(f"Deleted {result.rowcount} existing reports")

        # Step 2: Verify test transcript exists
        logger.info(f"\nStep 2: Verifying test transcript exists...")
        result = await conn.execute(
            text("""
                SELECT COUNT(*) as count,
                       MIN(section_name) as sample_section
                FROM aegis_transcripts
                WHERE ticker = :ticker
                AND fiscal_year = :year
                AND fiscal_quarter = :quarter
            """),
            {
                "ticker": TEST_BANK_SYMBOL,
                "year": TEST_FISCAL_YEAR,
                "quarter": TEST_QUARTER
            }
        )
        row = result.fetchone()
        if row.count == 0:
            logger.error(f"ERROR: No transcript found for {TEST_BANK_SYMBOL} {TEST_FISCAL_YEAR} {TEST_QUARTER}")
            return False
        logger.info(f"✓ Found {row.count} transcript records")
        logger.info(f"  Sample section: {row.sample_section}")

        # Step 3: Clear and setup aegis_data_availability with ONLY test bank
        logger.info(f"\nStep 3: Setting up aegis_data_availability with test bank only...")

        # Clear all entries
        result = await conn.execute(text("DELETE FROM aegis_data_availability"))
        logger.info(f"Cleared {result.rowcount} existing availability records")

        # Insert test bank only
        await conn.execute(
            text("""
                INSERT INTO aegis_data_availability (
                    bank_id, bank_name, bank_symbol,
                    fiscal_year, quarter,
                    database_names,
                    last_updated
                ) VALUES (
                    :bank_id, :bank_name, :bank_symbol,
                    :year, :quarter,
                    ARRAY['transcripts']::TEXT[],
                    NOW()
                )
            """),
            {
                "bank_id": TEST_BANK_ID,
                "bank_name": TEST_BANK_NAME,
                "bank_symbol": TEST_BANK_SYMBOL,
                "year": TEST_FISCAL_YEAR,
                "quarter": TEST_QUARTER
            }
        )
        logger.info(f"✓ Added {TEST_BANK_NAME} ({TEST_BANK_SYMBOL}) {TEST_FISCAL_YEAR} {TEST_QUARTER}")

        # Step 4: Verify setup
        logger.info(f"\nStep 4: Verifying setup...")

        # Check data availability
        result = await conn.execute(
            text("SELECT COUNT(*) as count FROM aegis_data_availability")
        )
        avail_count = result.fetchone().count
        logger.info(f"✓ aegis_data_availability: {avail_count} record(s)")

        # Check reports
        result = await conn.execute(
            text("SELECT COUNT(*) as count FROM aegis_reports")
        )
        reports_count = result.fetchone().count
        logger.info(f"✓ aegis_reports: {reports_count} record(s)")

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("TEST SCENARIO READY!")
        logger.info("=" * 80)
        logger.info(f"Test Bank: {TEST_BANK_NAME} ({TEST_BANK_SYMBOL})")
        logger.info(f"Test Period: {TEST_FISCAL_YEAR} {TEST_QUARTER}")
        logger.info(f"Expected ETLs: call_summary, key_themes")
        logger.info(f"\nExpected orchestrator behavior:")
        logger.info(f"  RUN 1: Should identify 2 gaps and generate both reports")
        logger.info(f"  RUN 2: Should find no gaps (reports already exist)")
        logger.info("=" * 80)

        return True


if __name__ == "__main__":
    success = asyncio.run(main())
    if not success:
        exit(1)
