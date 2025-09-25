"""
Script to check transcript tables and insert test Q&A data.
"""

import asyncio
from sqlalchemy import text
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger

setup_logging()
logger = get_logger()

async def check_tables():
    """Check existing tables related to transcripts."""
    async with get_connection() as conn:
        # Check for transcript-related tables
        result = await conn.execute(text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND (table_name LIKE '%transcript%' OR table_name LIKE '%earnings%' OR table_name LIKE '%call%')
            ORDER BY table_name;
        """))

        tables = [row[0] for row in result]
        logger.info(f"Found transcript-related tables: {tables}")

        # Check structure of each table
        for table in tables:
            logger.info(f"\n=== Table: {table} ===")
            result = await conn.execute(text(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{table}'
                ORDER BY ordinal_position;
            """))

            for row in result:
                logger.info(f"  {row[0]}: {row[1]} (nullable: {row[2]})")

        # Check sample data
        if tables:
            sample_table = tables[0]
            logger.info(f"\n=== Sample data from {sample_table} (first 2 rows) ===")
            result = await conn.execute(text(f"SELECT * FROM {sample_table} LIMIT 2"))

            for row in result:
                logger.info(f"  {dict(row)}")

if __name__ == "__main__":
    asyncio.run(check_tables())