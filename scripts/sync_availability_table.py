#!/usr/bin/env python
"""
Sync aegis_data_availability table with actual transcript data.

This script:
1. On first run (--rebuild): Wipes and rebuilds the entire table from transcript data
2. On subsequent runs: Updates only the 'transcripts' tag in database_names array
3. For fixing mismatched IDs (--complete-wipe): DELETEs all records then rebuilds

Usage:
    python scripts/sync_availability_table.py                # Update mode (preserves other tags)
    python scripts/sync_availability_table.py --rebuild      # Rebuild mode (truncates and recreates)
    python scripts/sync_availability_table.py --complete-wipe # Complete wipe (DELETE all, then rebuild)
    python scripts/sync_availability_table.py --dry-run      # Show what would be done
    python scripts/sync_availability_table.py --verify       # Show current table state
"""

import argparse
import sys
import os
from typing import Dict, List, Set, Tuple
from sqlalchemy import text
from collections import defaultdict

# Add parent directory to path to import aegis modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger

# Initialize logging
setup_logging()
logger = get_logger()


def get_transcript_availability() -> Dict[Tuple[int, int, str], Dict]:
    """
    Query aegis_transcripts table to get all available bank/year/quarter combinations.
    
    Returns:
        Dictionary keyed by (bank_id, fiscal_year, quarter) tuples
    """
    with get_connection() as conn:
        result = conn.execute(text("""
            SELECT DISTINCT
                institution_id as bank_id,
                company_name as bank_name,
                ticker as bank_symbol,
                fiscal_year,
                fiscal_quarter as quarter
            FROM aegis_transcripts
            WHERE institution_id IS NOT NULL
            AND fiscal_year IS NOT NULL
            AND fiscal_quarter IS NOT NULL
            ORDER BY institution_id, fiscal_year, fiscal_quarter
        """))
        
        availability = {}
        for row in result:
            key = (row.bank_id, row.fiscal_year, row.quarter)
            availability[key] = {
                'bank_id': row.bank_id,
                'bank_name': row.bank_name,
                'bank_symbol': row.bank_symbol,
                'fiscal_year': row.fiscal_year,
                'quarter': row.quarter
            }
        
        return availability


def get_current_availability() -> Dict[Tuple[int, int, str], List[str]]:
    """
    Get current aegis_data_availability table contents.
    
    Returns:
        Dictionary mapping (bank_id, fiscal_year, quarter) to list of database_names
    """
    with get_connection() as conn:
        result = conn.execute(text("""
            SELECT 
                bank_id,
                fiscal_year,
                quarter,
                database_names
            FROM aegis_data_availability
        """))
        
        current = {}
        for row in result:
            key = (row.bank_id, row.fiscal_year, row.quarter)
            # Parse the PostgreSQL array format
            if row.database_names:
                if isinstance(row.database_names, list):
                    current[key] = row.database_names
                else:
                    # Parse string representation of array
                    names = row.database_names.strip('{}').split(',')
                    current[key] = [n.strip('"') for n in names if n.strip()]
            else:
                current[key] = []
        
        return current


def rebuild_table(transcript_data: Dict, dry_run: bool = False, complete_wipe: bool = False):
    """
    Wipe and rebuild the entire aegis_data_availability table.
    
    Args:
        transcript_data: Dictionary of transcript availability
        dry_run: If True, only show what would be done
        complete_wipe: If True, DELETE all records (slower but ensures clean state)
    """
    if complete_wipe:
        logger.info("Starting COMPLETE WIPE mode - will DELETE all records and recreate table")
    else:
        logger.info("Starting REBUILD mode - will wipe and recreate table")
    
    if dry_run:
        logger.info("DRY RUN - No changes will be made")
        if complete_wipe:
            logger.info("Would DELETE all existing records")
        logger.info(f"Would insert {len(transcript_data)} records")
        for key, data in list(transcript_data.items())[:5]:  # Show first 5
            logger.info(f"  Would insert: {data['bank_name']} - {data['fiscal_year']} {data['quarter']}")
        if len(transcript_data) > 5:
            logger.info(f"  ... and {len(transcript_data) - 5} more records")
        return
    
    with get_connection() as conn:
        # Start transaction
        trans = conn.begin()
        
        try:
            if complete_wipe:
                # DELETE all records (ensures complete removal even with FK constraints)
                logger.info("Deleting ALL records from aegis_data_availability table...")
                result = conn.execute(text("DELETE FROM aegis_data_availability"))
                logger.info(f"Deleted {result.rowcount} existing records")
            else:
                # 1. Delete all records (more compatible with restricted permissions)
                logger.info("Deleting all records from aegis_data_availability table...")
                result = conn.execute(text("DELETE FROM aegis_data_availability"))
                logger.info(f"Deleted {result.rowcount} existing records")
            
            # 2. Insert all transcript data
            logger.info(f"Inserting {len(transcript_data)} records...")
            
            for data in transcript_data.values():
                conn.execute(text("""
                    INSERT INTO aegis_data_availability 
                    (bank_id, bank_name, bank_symbol, fiscal_year, quarter, database_names)
                    VALUES (:bank_id, :bank_name, :bank_symbol, :fiscal_year, :quarter, :database_names)
                """), {
                    'bank_id': data['bank_id'],
                    'bank_name': data['bank_name'],
                    'bank_symbol': data['bank_symbol'],
                    'fiscal_year': data['fiscal_year'],
                    'quarter': data['quarter'],
                    'database_names': ['transcripts']  # Only transcripts tag for rebuild
                })
            
            trans.commit()
            logger.info(f"✅ Successfully rebuilt table with {len(transcript_data)} records")
            
        except Exception as e:
            trans.rollback()
            logger.error(f"❌ Error during rebuild: {e}")
            raise


def update_transcript_tags(transcript_data: Dict, dry_run: bool = False):
    """
    Update only the 'transcripts' tag in database_names array.
    Preserves other subagent tags.
    
    Args:
        transcript_data: Dictionary of transcript availability
        dry_run: If True, only show what would be done
    """
    logger.info("Starting UPDATE mode - will only modify 'transcripts' tags")
    
    # Get current data
    current_data = get_current_availability()
    
    # Track changes
    to_add = []
    to_remove = []
    to_insert = []
    
    # Find records where we need to ADD transcripts tag
    for key, data in transcript_data.items():
        if key in current_data:
            if 'transcripts' not in current_data[key]:
                to_add.append(key)
        else:
            # Record doesn't exist, need to insert
            to_insert.append((key, data))
    
    # Find records where we need to REMOVE transcripts tag
    for key, tags in current_data.items():
        if 'transcripts' in tags and key not in transcript_data:
            to_remove.append(key)
    
    logger.info(f"Changes to make:")
    logger.info(f"  - Add 'transcripts' tag to {len(to_add)} records")
    logger.info(f"  - Remove 'transcripts' tag from {len(to_remove)} records")
    logger.info(f"  - Insert {len(to_insert)} new records")
    
    if dry_run:
        logger.info("DRY RUN - No changes will be made")
        
        if to_add[:3]:
            logger.info("Sample additions:")
            for key in to_add[:3]:
                logger.info(f"  Would add 'transcripts' to: bank_id={key[0]}, year={key[1]}, quarter={key[2]}")
        
        if to_remove[:3]:
            logger.info("Sample removals:")
            for key in to_remove[:3]:
                logger.info(f"  Would remove 'transcripts' from: bank_id={key[0]}, year={key[1]}, quarter={key[2]}")
        
        if to_insert[:3]:
            logger.info("Sample insertions:")
            for key, data in to_insert[:3]:
                logger.info(f"  Would insert: {data['bank_name']} - {data['fiscal_year']} {data['quarter']}")
        
        return
    
    with get_connection() as conn:
        trans = conn.begin()
        
        try:
            # 1. Add 'transcripts' tag where needed
            for key in to_add:
                conn.execute(text("""
                    UPDATE aegis_data_availability
                    SET database_names = array_append(database_names, 'transcripts')
                    WHERE bank_id = :bank_id 
                    AND fiscal_year = :fiscal_year 
                    AND quarter = :quarter
                    AND NOT ('transcripts' = ANY(database_names))
                """), {
                    'bank_id': key[0],
                    'fiscal_year': key[1],
                    'quarter': key[2]
                })
            
            # 2. Remove 'transcripts' tag where needed
            for key in to_remove:
                conn.execute(text("""
                    UPDATE aegis_data_availability
                    SET database_names = array_remove(database_names, 'transcripts')
                    WHERE bank_id = :bank_id 
                    AND fiscal_year = :fiscal_year 
                    AND quarter = :quarter
                """), {
                    'bank_id': key[0],
                    'fiscal_year': key[1],
                    'quarter': key[2]
                })
            
            # 3. Insert new records
            for key, data in to_insert:
                conn.execute(text("""
                    INSERT INTO aegis_data_availability 
                    (bank_id, bank_name, bank_symbol, fiscal_year, quarter, database_names)
                    VALUES (:bank_id, :bank_name, :bank_symbol, :fiscal_year, :quarter, :database_names)
                """), {
                    'bank_id': data['bank_id'],
                    'bank_name': data['bank_name'],
                    'bank_symbol': data['bank_symbol'],
                    'fiscal_year': data['fiscal_year'],
                    'quarter': data['quarter'],
                    'database_names': ['transcripts']
                })
            
            trans.commit()
            logger.info(f"✅ Successfully updated table")
            logger.info(f"  - Added 'transcripts' tag to {len(to_add)} records")
            logger.info(f"  - Removed 'transcripts' tag from {len(to_remove)} records")
            logger.info(f"  - Inserted {len(to_insert)} new records")
            
        except Exception as e:
            trans.rollback()
            logger.error(f"❌ Error during update: {e}")
            raise


def verify_results():
    """
    Show summary of current aegis_data_availability state.
    """
    with get_connection() as conn:
        # Count total records
        result = conn.execute(text("""
            SELECT COUNT(*) as total FROM aegis_data_availability
        """))
        total = result.scalar()
        
        # Count records with transcripts tag
        result = conn.execute(text("""
            SELECT COUNT(*) as with_transcripts 
            FROM aegis_data_availability 
            WHERE 'transcripts' = ANY(database_names)
        """))
        with_transcripts = result.scalar()
        
        # Get bank summary
        result = conn.execute(text("""
            SELECT 
                bank_name,
                bank_symbol,
                COUNT(*) as periods,
                COUNT(CASE WHEN 'transcripts' = ANY(database_names) THEN 1 END) as with_transcripts
            FROM aegis_data_availability
            GROUP BY bank_id, bank_name, bank_symbol
            ORDER BY bank_id
        """))
        
        logger.info("\n" + "="*60)
        logger.info("VERIFICATION SUMMARY")
        logger.info("="*60)
        logger.info(f"Total records: {total}")
        logger.info(f"Records with 'transcripts' tag: {with_transcripts}")
        logger.info("\nPer-bank summary:")
        
        for row in result:
            logger.info(f"  {row.bank_name} ({row.bank_symbol}): {row.periods} periods, {row.with_transcripts} with transcripts")


def main():
    """
    Main entry point for the sync script.
    """
    parser = argparse.ArgumentParser(
        description="Sync aegis_data_availability with transcript data"
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Wipe and rebuild the entire table (use for first run)"
    )
    parser.add_argument(
        "--complete-wipe",
        action="store_true",
        help="DELETE all records before rebuild (ensures clean state, removes mismatched bank IDs)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Show summary of current table state"
    )
    
    args = parser.parse_args()
    
    try:
        if args.verify:
            verify_results()
            return
        
        # Get transcript data
        logger.info("Fetching transcript availability data...")
        transcript_data = get_transcript_availability()
        logger.info(f"Found {len(transcript_data)} bank/period combinations in transcripts")
        
        if args.rebuild or args.complete_wipe:
            rebuild_table(transcript_data, dry_run=args.dry_run, complete_wipe=args.complete_wipe)
        else:
            update_transcript_tags(transcript_data, dry_run=args.dry_run)
        
        # Show verification unless dry-run
        if not args.dry_run:
            verify_results()
            
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()