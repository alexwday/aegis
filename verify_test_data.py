"""
Verify test data in the database.
"""

import asyncio
from sqlalchemy import text
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger

setup_logging()
logger = get_logger()

async def verify_data():
    """Check what test data exists."""
    async with get_connection() as conn:
        # Check RBC data
        result = await conn.execute(text("""
            SELECT
                institution_id,
                company_name,
                fiscal_year,
                fiscal_quarter,
                section_name,
                qa_group_id,
                COUNT(*) as chunk_count
            FROM aegis_transcripts
            WHERE company_name = 'Royal Bank of Canada'
            GROUP BY
                institution_id,
                company_name,
                fiscal_year,
                fiscal_quarter,
                section_name,
                qa_group_id
            ORDER BY qa_group_id
        """))

        logger.info("Test data in database:")
        for row in result:
            logger.info(f"  institution_id={row[0]}, bank={row[1]}, year={row[2]}, quarter={row[3]}, section={row[4]}, qa_group={row[5]}, chunks={row[6]}")

        # Try different queries
        logger.info("\nTrying different query approaches:")

        # Query 1: Using institution_id as text
        result = await conn.execute(text("""
            SELECT COUNT(*)
            FROM aegis_transcripts
            WHERE institution_id = 'RY-CA'
                AND fiscal_year = 2024
                AND fiscal_quarter = 'Q3'
                AND section_name = 'Q&A'
        """))
        count = result.scalar()
        logger.info(f"  Query with institution_id='RY-CA': {count} records")

        # Query 2: Using institution_id as number
        result = await conn.execute(text("""
            SELECT COUNT(*)
            FROM aegis_transcripts
            WHERE institution_id = '1'
                AND fiscal_year = 2024
                AND fiscal_quarter = 'Q3'
                AND section_name = 'Q&A'
        """))
        count = result.scalar()
        logger.info(f"  Query with institution_id='1': {count} records")

        # Query 3: Using company_name
        result = await conn.execute(text("""
            SELECT COUNT(*)
            FROM aegis_transcripts
            WHERE company_name = 'Royal Bank of Canada'
                AND fiscal_year = 2024
                AND fiscal_quarter = 'Q3'
                AND section_name = 'Q&A'
        """))
        count = result.scalar()
        logger.info(f"  Query with company_name='Royal Bank of Canada': {count} records")

if __name__ == "__main__":
    asyncio.run(verify_data())