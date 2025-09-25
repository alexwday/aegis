#!/usr/bin/env python
"""
Sync aegis_data_availability table with actual transcript and report data.

This script:
1. On first run (--rebuild): Wipes and rebuilds the entire table from transcript and report data
2. On subsequent runs: Updates the 'transcripts' and 'reports' tags in database_names array
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
            # Convert bank_id to int since institution_id is TEXT in transcripts table
            bank_id = int(row.bank_id) if row.bank_id else None
            if bank_id is None:
                continue
            key = (bank_id, row.fiscal_year, row.quarter)
            availability[key] = {
                'bank_id': bank_id,
                'bank_name': row.bank_name,
                'bank_symbol': row.bank_symbol,
                'fiscal_year': row.fiscal_year,
                'quarter': row.quarter
            }

        return availability


def get_report_availability() -> Dict[Tuple[int, int, str], Dict]:
    """
    Query aegis_reports table to get all available bank/year/quarter combinations.

    Returns:
        Dictionary keyed by (bank_id, fiscal_year, quarter) tuples
    """
    with get_connection() as conn:
        result = conn.execute(text("""
            SELECT DISTINCT
                bank_id,
                bank_name,
                bank_symbol,
                fiscal_year,
                quarter
            FROM aegis_reports
            WHERE bank_id IS NOT NULL
            AND fiscal_year IS NOT NULL
            AND quarter IS NOT NULL
            ORDER BY bank_id, fiscal_year, quarter
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


def rebuild_table(transcript_data: Dict, report_data: Dict, dry_run: bool = False, complete_wipe: bool = False):
    """
    Wipe and rebuild the entire aegis_data_availability table.

    Args:
        transcript_data: Dictionary of transcript availability
        report_data: Dictionary of report availability
        dry_run: If True, only show what would be done
        complete_wipe: If True, DELETE all records (slower but ensures clean state)
    """
    if complete_wipe:
        logger.info("Starting COMPLETE WIPE mode - will DELETE all records and recreate table")
    else:
        logger.info("Starting REBUILD mode - will wipe and recreate table")
    
    # Merge transcript and report data to get all unique bank/period combinations
    all_combinations = {}
    for key, data in transcript_data.items():
        all_combinations[key] = data.copy()
        all_combinations[key]['databases'] = ['transcripts']

    for key, data in report_data.items():
        if key in all_combinations:
            # Add reports to existing entry
            if 'reports' not in all_combinations[key]['databases']:
                all_combinations[key]['databases'].append('reports')
        else:
            # New entry with only reports
            all_combinations[key] = data.copy()
            all_combinations[key]['databases'] = ['reports']

    if dry_run:
        logger.info("DRY RUN - No changes will be made")
        if complete_wipe:
            logger.info("Would DELETE all existing records")
        logger.info(f"Would insert {len(all_combinations)} records")
        logger.info(f"  - {len(transcript_data)} with transcripts")
        logger.info(f"  - {len(report_data)} with reports")
        for key, data in list(all_combinations.items())[:5]:  # Show first 5
            databases = ', '.join(data['databases'])
            logger.info(f"  Would insert: {data['bank_name']} - {data['fiscal_year']} {data['quarter']} [{databases}]")
        if len(all_combinations) > 5:
            logger.info(f"  ... and {len(all_combinations) - 5} more records")
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
            
            # 2. Insert all combined data
            logger.info(f"Inserting {len(all_combinations)} records...")

            for data in all_combinations.values():
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
                    'database_names': data['databases']
                })

            trans.commit()
            logger.info(f"✅ Successfully rebuilt table with {len(all_combinations)} records")
            logger.info(f"  - {len(transcript_data)} periods have transcripts")
            logger.info(f"  - {len(report_data)} periods have reports")
            
        except Exception as e:
            trans.rollback()
            logger.error(f"❌ Error during rebuild: {e}")
            raise


def update_database_tags(transcript_data: Dict, report_data: Dict, dry_run: bool = False):
    """
    Update the 'transcripts' and 'reports' tags in database_names array.
    Preserves other subagent tags.

    Args:
        transcript_data: Dictionary of transcript availability
        report_data: Dictionary of report availability
        dry_run: If True, only show what would be done
    """
    logger.info("Starting UPDATE mode - will modify 'transcripts' and 'reports' tags")
    
    # Get current data
    current_data = get_current_availability()
    
    # Track changes separately for transcripts and reports
    transcripts_to_add = []
    transcripts_to_remove = []
    reports_to_add = []
    reports_to_remove = []
    to_insert = []

    # Process transcripts
    for key, data in transcript_data.items():
        if key in current_data:
            if 'transcripts' not in current_data[key]:
                transcripts_to_add.append(key)
        else:
            # Record doesn't exist, need to insert
            to_insert.append((key, data, ['transcripts']))

    for key, tags in current_data.items():
        if 'transcripts' in tags and key not in transcript_data:
            transcripts_to_remove.append(key)

    # Process reports
    for key, data in report_data.items():
        if key in current_data:
            if 'reports' not in current_data[key]:
                reports_to_add.append(key)
        else:
            # Check if this key is already in to_insert from transcripts
            existing_insert = None
            for i, (k, d, databases) in enumerate(to_insert):
                if k == key:
                    existing_insert = i
                    break
            if existing_insert is not None:
                # Add reports to existing insert
                to_insert[existing_insert] = (key, data, to_insert[existing_insert][2] + ['reports'])
            else:
                # New entry with only reports
                to_insert.append((key, data, ['reports']))

    for key, tags in current_data.items():
        if 'reports' in tags and key not in report_data:
            reports_to_remove.append(key)
    
    logger.info(f"Changes to make:")
    logger.info(f"  Transcripts:")
    logger.info(f"    - Add tag to {len(transcripts_to_add)} records")
    logger.info(f"    - Remove tag from {len(transcripts_to_remove)} records")
    logger.info(f"  Reports:")
    logger.info(f"    - Add tag to {len(reports_to_add)} records")
    logger.info(f"    - Remove tag from {len(reports_to_remove)} records")
    logger.info(f"  - Insert {len(to_insert)} new records")
    
    if dry_run:
        logger.info("DRY RUN - No changes will be made")
        
        if transcripts_to_add[:3]:
            logger.info("Sample transcript additions:")
            for key in transcripts_to_add[:3]:
                logger.info(f"  Would add 'transcripts' to: bank_id={key[0]}, year={key[1]}, quarter={key[2]}")

        if reports_to_add[:3]:
            logger.info("Sample report additions:")
            for key in reports_to_add[:3]:
                logger.info(f"  Would add 'reports' to: bank_id={key[0]}, year={key[1]}, quarter={key[2]}")

        if to_insert[:3]:
            logger.info("Sample insertions:")
            for key, data, databases in to_insert[:3]:
                db_str = ', '.join(databases)
                logger.info(f"  Would insert: {data['bank_name']} - {data['fiscal_year']} {data['quarter']} [{db_str}]")
        
        return
    
    with get_connection() as conn:
        trans = conn.begin()
        
        try:
            # 1. Add 'transcripts' tag where needed
            for key in transcripts_to_add:
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
            for key in transcripts_to_remove:
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

            # 3. Add 'reports' tag where needed
            for key in reports_to_add:
                conn.execute(text("""
                    UPDATE aegis_data_availability
                    SET database_names = array_append(database_names, 'reports')
                    WHERE bank_id = :bank_id
                    AND fiscal_year = :fiscal_year
                    AND quarter = :quarter
                    AND NOT ('reports' = ANY(database_names))
                """), {
                    'bank_id': key[0],
                    'fiscal_year': key[1],
                    'quarter': key[2]
                })

            # 4. Remove 'reports' tag where needed
            for key in reports_to_remove:
                conn.execute(text("""
                    UPDATE aegis_data_availability
                    SET database_names = array_remove(database_names, 'reports')
                    WHERE bank_id = :bank_id
                    AND fiscal_year = :fiscal_year
                    AND quarter = :quarter
                """), {
                    'bank_id': key[0],
                    'fiscal_year': key[1],
                    'quarter': key[2]
                })

            # 5. Insert new records
            for key, data, databases in to_insert:
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
                    'database_names': databases
                })

            trans.commit()
            logger.info(f"✅ Successfully updated table")
            logger.info(f"  Transcripts:")
            logger.info(f"    - Added tag to {len(transcripts_to_add)} records")
            logger.info(f"    - Removed tag from {len(transcripts_to_remove)} records")
            logger.info(f"  Reports:")
            logger.info(f"    - Added tag to {len(reports_to_add)} records")
            logger.info(f"    - Removed tag from {len(reports_to_remove)} records")
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

        # Count records with reports tag
        result = conn.execute(text("""
            SELECT COUNT(*) as with_reports
            FROM aegis_data_availability
            WHERE 'reports' = ANY(database_names)
        """))
        with_reports = result.scalar()
        
        # Get bank summary
        result = conn.execute(text("""
            SELECT
                bank_name,
                bank_symbol,
                COUNT(*) as periods,
                COUNT(CASE WHEN 'transcripts' = ANY(database_names) THEN 1 END) as with_transcripts,
                COUNT(CASE WHEN 'reports' = ANY(database_names) THEN 1 END) as with_reports
            FROM aegis_data_availability
            GROUP BY bank_id, bank_name, bank_symbol
            ORDER BY bank_id
        """))
        
        logger.info("\n" + "="*60)
        logger.info("VERIFICATION SUMMARY")
        logger.info("="*60)
        logger.info(f"Total records: {total}")
        logger.info(f"Records with 'transcripts' tag: {with_transcripts}")
        logger.info(f"Records with 'reports' tag: {with_reports}")
        logger.info("\nPer-bank summary:")

        for row in result:
            logger.info(f"  {row.bank_name} ({row.bank_symbol}): {row.periods} periods, {row.with_transcripts} transcripts, {row.with_reports} reports")


def main():
    """
    Main entry point for the sync script.
    """
    parser = argparse.ArgumentParser(
        description="Sync aegis_data_availability with transcript and report data"
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

        # Get report data
        logger.info("Fetching report availability data...")
        report_data = get_report_availability()
        logger.info(f"Found {len(report_data)} bank/period combinations in reports")

        if args.rebuild or args.complete_wipe:
            rebuild_table(transcript_data, report_data, dry_run=args.dry_run, complete_wipe=args.complete_wipe)
        else:
            update_database_tags(transcript_data, report_data, dry_run=args.dry_run)
        
        # Show verification unless dry-run
        if not args.dry_run:
            verify_results()
            
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()