#!/usr/bin/env python3
"""
Simple script to create aegis_transcripts table.

This script creates the table with minimal complexity - just the basic structure.
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from src.aegis.utils.settings import config
from src.aegis.utils.logging import get_logger

logger = get_logger()


def create_table():
    """Create aegis_transcripts table with simple schema."""
    
    # Build connection string
    connection_string = (
        f"postgresql://{config.postgres_user}:{config.postgres_password}"
        f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}"
    )
    
    engine = create_engine(connection_string)
    
    # Simple SQL to create the table
    create_table_sql = """
    -- Drop existing table if requested
    DROP TABLE IF EXISTS aegis_transcripts CASCADE;
    
    -- Enable pgvector extension
    CREATE EXTENSION IF NOT EXISTS vector;
    
    -- Create the table with basic structure
    CREATE TABLE aegis_transcripts (
        id SERIAL PRIMARY KEY,
        file_path TEXT,
        filename TEXT,
        date_last_modified TIMESTAMP WITH TIME ZONE,
        title TEXT,
        transcript_type TEXT,
        event_id TEXT,
        version_id TEXT,
        fiscal_year INTEGER NOT NULL,
        fiscal_quarter TEXT NOT NULL,
        institution_type TEXT,
        institution_id TEXT,
        ticker TEXT NOT NULL,
        company_name TEXT,
        section_name TEXT,
        speaker_block_id INTEGER,
        qa_group_id INTEGER,
        classification_ids TEXT[],
        classification_names TEXT[],
        block_summary TEXT,
        chunk_id INTEGER,
        chunk_tokens INTEGER,
        chunk_content TEXT,
        chunk_paragraph_ids TEXT[],
        chunk_embedding vector(3072),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        with engine.begin() as conn:
            # Execute each statement separately
            statements = create_table_sql.split(';')
            
            for stmt in statements:
                stmt = stmt.strip()
                if not stmt:
                    continue
                    
                try:
                    logger.info(f"Executing: {stmt[:50]}...")
                    conn.execute(text(stmt))
                    logger.info("✓ Statement executed successfully")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.info("  (Already exists, continuing...)")
                    else:
                        logger.error(f"✗ Failed: {e}")
                        raise
            
            # Verify table was created
            result = conn.execute(text(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_name = 'aegis_transcripts'"
            ))
            
            if result.scalar() > 0:
                logger.info("\n✅ SUCCESS: Table 'aegis_transcripts' created successfully!")
                
                # Show column count
                result = conn.execute(text(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_name = 'aegis_transcripts'"
                ))
                col_count = result.scalar()
                logger.info(f"   Table has {col_count} columns")
                
                return True
            else:
                logger.error("\n❌ ERROR: Table was not created")
                return False
                
    except Exception as e:
        logger.error(f"\n❌ ERROR creating table: {e}")
        return False


def main():
    """Main entry point."""
    logger.info("="*60)
    logger.info("Creating aegis_transcripts table (simple version)")
    logger.info("="*60)
    
    success = create_table()
    
    if success:
        logger.info("\nYou can now load data with:")
        logger.info("  python scripts/setup_aegis_transcripts.py --load-csv /path/to/data.csv")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())