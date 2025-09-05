#!/usr/bin/env python3
"""
Test script for aegis_transcripts table creation.

This script tests the creation and basic operations on the aegis_transcripts table.
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.aegis.utils.settings import config
from src.aegis.utils.logging import get_logger
from sqlalchemy import create_engine, text

logger = get_logger()

def test_connection():
    """Test basic database connection."""
    try:
        connection_string = (
            f"postgresql://{config.postgres_user}:{config.postgres_password}"
            f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}"
        )
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("✓ Database connection successful")
            return True
    except Exception as e:
        logger.error(f"✗ Database connection failed: {e}")
        return False

def test_pgvector():
    """Test if pgvector extension is available."""
    try:
        connection_string = (
            f"postgresql://{config.postgres_user}:{config.postgres_password}"
            f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}"
        )
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT * FROM pg_available_extensions WHERE name = 'vector'"
            ))
            row = result.fetchone()
            
            if row:
                logger.info(f"✓ pgvector extension available: version {row[1]}")
                
                # Try to create the extension
                try:
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
                    conn.commit()
                    logger.info("✓ pgvector extension enabled")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.info("✓ pgvector extension already enabled")
                    else:
                        logger.warning(f"⚠ Could not enable pgvector: {e}")
                
                return True
            else:
                logger.error("✗ pgvector extension not available")
                logger.error("  Please run: ./scripts/fix_pgvector.sh")
                return False
    except Exception as e:
        logger.error(f"✗ Error checking pgvector: {e}")
        return False

def test_create_table():
    """Test creating the aegis_transcripts table."""
    try:
        from scripts.setup_aegis_transcripts import AegisTranscriptsSetup
        
        setup = AegisTranscriptsSetup()
        
        # Check if table exists
        if setup.check_table_exists():
            logger.info("⚠ Table aegis_transcripts already exists")
            return True
        
        # Create table
        logger.info("Creating aegis_transcripts table...")
        success = setup.create_table()
        
        if success:
            logger.info("✓ Table created successfully")
            
            # Verify it exists
            if setup.check_table_exists():
                logger.info("✓ Table existence verified")
                
                # Get table info
                connection_string = (
                    f"postgresql://{config.postgres_user}:{config.postgres_password}"
                    f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}"
                )
                engine = create_engine(connection_string)
                
                with engine.connect() as conn:
                    result = conn.execute(text(
                        "SELECT column_name, data_type FROM information_schema.columns "
                        "WHERE table_name = 'aegis_transcripts' ORDER BY ordinal_position"
                    ))
                    columns = result.fetchall()
                    logger.info(f"✓ Table has {len(columns)} columns")
                    
                    # Check for important columns
                    column_names = [col[0] for col in columns]
                    important_cols = ['id', 'ticker', 'fiscal_year', 'chunk_content', 'chunk_embedding']
                    for col in important_cols:
                        if col in column_names:
                            logger.info(f"  ✓ Column '{col}' exists")
                        else:
                            logger.error(f"  ✗ Column '{col}' missing")
                
                return True
            else:
                logger.error("✗ Table not found after creation")
                return False
        else:
            logger.error("✗ Table creation failed")
            return False
            
    except Exception as e:
        logger.error(f"✗ Error during table creation: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    logger.info("=" * 60)
    logger.info("Testing aegis_transcripts Table Setup")
    logger.info("=" * 60)
    
    all_passed = True
    
    # Test 1: Database connection
    logger.info("\n1. Testing database connection...")
    if not test_connection():
        all_passed = False
        logger.error("   Cannot proceed without database connection")
        return 1
    
    # Test 2: pgvector extension
    logger.info("\n2. Testing pgvector extension...")
    if not test_pgvector():
        all_passed = False
        logger.error("   Cannot proceed without pgvector")
        return 1
    
    # Test 3: Create table
    logger.info("\n3. Testing table creation...")
    if not test_create_table():
        all_passed = False
    
    # Summary
    logger.info("\n" + "=" * 60)
    if all_passed:
        logger.info("✓ All tests passed!")
        logger.info("=" * 60)
        return 0
    else:
        logger.error("✗ Some tests failed")
        logger.info("=" * 60)
        return 1

if __name__ == "__main__":
    sys.exit(main())