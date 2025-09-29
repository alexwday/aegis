#!/usr/bin/env python
"""
Sync script for aegis_data_availability table.

This script syncs data from multiple PostgreSQL tables to the aegis_data_availability table
based on the configuration defined at the top of this file.

Usage:
    # Run sync for all configured tables
    python scripts/sync_availability_table.py

    # Run in dry-run mode to see what would happen
    python scripts/sync_availability_table.py --dry-run

    # Run in rebuild mode (clear and rebuild tags)
    python scripts/sync_availability_table.py --mode rebuild

    # Verify current state
    python scripts/sync_availability_table.py --verify-only
"""

import argparse
import sys
import os
import asyncio
import yaml
from typing import Dict, List, Tuple, Optional
from sqlalchemy import text
from dataclasses import dataclass
from pathlib import Path

# Add parent directory to path to import aegis modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger

# Initialize logging
setup_logging()
logger = get_logger()


# ============================================================================
# CONFIGURATION - EDIT THIS SECTION TO ADD/MODIFY DATA SOURCES
# ============================================================================

@dataclass
class TableConfig:
    """Configuration for a source table."""
    table_name: str           # PostgreSQL table name
    bank_id_type: str         # How to identify banks: 'id', 'name', or 'symbol'
    bank_id_field: str        # Column in YOUR table containing the bank identifier (based on bank_id_type)
    year_field: str           # Column containing fiscal year
    quarter_field: str        # Column containing quarter
    tag: str                  # Database tag for aegis_data_availability
    enabled: bool = True      # Whether to sync this table
    # Optional: Only if your table has these additional fields
    bank_name_field: Optional[str] = None      # Column with bank name (if available)
    bank_symbol_field: Optional[str] = None    # Column with bank symbol (if available)


# Define all tables to sync
TABLE_CONFIGS = [
    TableConfig(
        table_name="aegis_transcripts",
        bank_id_type="id",              # This table uses bank IDs to identify banks
        bank_id_field="institution_id",  # The column in aegis_transcripts that contains the bank ID
        year_field="fiscal_year",
        quarter_field="fiscal_quarter",
        tag="transcripts",
        enabled=True,
        # Optional: These columns also exist in the table for additional info
        bank_name_field="company_name",
        bank_symbol_field="ticker"
    ),
    TableConfig(
        table_name="aegis_reports",
        bank_id_type="id",               # This table uses bank IDs to identify banks
        bank_id_field="bank_id",         # The column in aegis_reports that contains the bank ID
        year_field="fiscal_year",
        quarter_field="quarter",
        tag="reports",
        enabled=True,
        # Optional: These columns also exist in the table for additional info
        bank_name_field="bank_name",
        bank_symbol_field="bank_symbol"
    ),

    # EXAMPLE: Table that only has bank symbols (no ID or name columns)
    # TableConfig(
    #     table_name="aegis_supplementary",
    #     bank_id_type="symbol",        # This table uses bank SYMBOLS to identify banks
    #     bank_id_field="bank_ticker",  # The column that contains the bank symbol
    #     year_field="year",
    #     quarter_field="period",
    #     tag="supplementary",
    #     enabled=False
    #     # No bank_name_field or bank_symbol_field needed - will be resolved from BANK_MAPPINGS
    # ),

    # EXAMPLE: Table that only has bank names (no ID or symbol columns)
    # TableConfig(
    #     table_name="aegis_rts",
    #     bank_id_type="name",             # This table uses bank NAMES to identify banks
    #     bank_id_field="institution_name", # The column that contains the bank name
    #     year_field="report_year",
    #     quarter_field="report_quarter",
    #     tag="rts",
    #     enabled=False
    #     # No bank_name_field or bank_symbol_field needed - will be resolved from BANK_MAPPINGS
    # ),

    # EXAMPLE: Table with bank ID and optional name/symbol
    # TableConfig(
    #     table_name="aegis_pillar3",
    #     bank_id_type="id",            # This table uses bank IDs to identify banks
    #     bank_id_field="bank_id",      # The column that contains the bank ID
    #     year_field="fiscal_year",
    #     quarter_field="quarter",
    #     tag="pillar3",
    #     enabled=False,
    #     # Optional: Include if these columns exist for additional info
    #     bank_name_field="bank_name",
    #     bank_symbol_field="bank_symbol"
    # ),
]

# ============================================================================
# END CONFIGURATION
# ============================================================================


def load_bank_mappings():
    """
    Load bank mappings from monitored_institutions.yaml file.

    Returns:
        Tuple of (BANK_MAPPINGS, BANK_NAME_TO_ID, BANK_SYMBOL_TO_ID)
    """
    yaml_path = Path(__file__).parent / "monitored_institutions.yaml"

    if not yaml_path.exists():
        raise FileNotFoundError(f"Bank mappings file not found: {yaml_path}")

    with open(yaml_path, 'r') as f:
        institutions = yaml.safe_load(f)

    bank_mappings = {}
    bank_name_to_id = {}
    bank_symbol_to_id = {}

    for ticker_with_country, info in institutions.items():
        if isinstance(info, dict) and 'id' in info and 'name' in info:
            bank_id = info['id']
            bank_name = info['name']

            # Extract ticker symbol without country code (e.g., "RY-CA" -> "RY")
            # Some tickers might not have country codes
            ticker_parts = ticker_with_country.split('-')
            ticker_symbol = ticker_parts[0] if ticker_parts else ticker_with_country

            # Store the mapping
            bank_mappings[bank_id] = (bank_name, ticker_symbol)
            bank_name_to_id[bank_name] = bank_id
            bank_symbol_to_id[ticker_symbol] = bank_id

            # Also store with full ticker (with country code) for better matching
            bank_symbol_to_id[ticker_with_country] = bank_id

    logger.info(f"Loaded {len(bank_mappings)} bank mappings from monitored_institutions.yaml")

    return bank_mappings, bank_name_to_id, bank_symbol_to_id


# Load bank mappings from YAML file
BANK_MAPPINGS, BANK_NAME_TO_ID, BANK_SYMBOL_TO_ID = load_bank_mappings()


def get_bank_info(value: str, id_type: str) -> Optional[Tuple[int, str, str]]:
    """
    Look up complete bank information from monitored_institutions.yaml.

    This function ensures we always use the canonical bank details from our
    definitive source (monitored_institutions.yaml), regardless of what data
    might be in the source table.

    Flow:
    1. Source table provides a bank identifier (ID, name, or symbol)
    2. We look up that identifier in monitored_institutions.yaml
    3. We return the complete, canonical bank details from YAML
    4. These details are used to either:
       - Add our tag to an existing aegis_data_availability record
       - Create a new record with full details from YAML + our tag

    Args:
        value: The bank identifier value from source table
        id_type: Type of identifier ('id', 'name', or 'symbol')

    Returns:
        Tuple of (bank_id, bank_name, bank_symbol) from monitored_institutions.yaml
        or None if bank not found in YAML (these records will be skipped)
    """
    # Step 1: Convert the provided identifier to bank_id
    bank_id = None

    if id_type == 'id':
        try:
            bank_id = int(value)
        except (ValueError, TypeError):
            return None
    elif id_type == 'name':
        # Look up bank ID using the name mapping from YAML
        bank_id = BANK_NAME_TO_ID.get(value)
    elif id_type == 'symbol':
        # Look up bank ID using the symbol mapping from YAML
        bank_id = BANK_SYMBOL_TO_ID.get(value)

    # Step 2: If we found a bank_id, get the full canonical details from YAML
    if bank_id and bank_id in BANK_MAPPINGS:
        bank_name, bank_symbol = BANK_MAPPINGS[bank_id]
        # Always return the canonical details from monitored_institutions.yaml
        return (bank_id, bank_name, bank_symbol)

    # Bank not found in monitored_institutions.yaml - will be skipped
    return None


async def get_table_availability(config: TableConfig) -> Dict[Tuple[int, int, str], Dict]:
    """
    Query a table to get all available bank/year/quarter combinations.

    Args:
        config: Table configuration

    Returns:
        Dictionary keyed by (bank_id, fiscal_year, quarter) tuples
    """
    # Build SELECT clause based on available fields
    select_fields = [f"{config.bank_id_field} as bank_identifier"]

    if config.bank_name_field and config.bank_id_type != 'name':
        select_fields.append(f"{config.bank_name_field} as bank_name")

    if config.bank_symbol_field and config.bank_id_type != 'symbol':
        select_fields.append(f"{config.bank_symbol_field} as bank_symbol")

    select_fields.extend([
        f"{config.year_field} as fiscal_year",
        f"{config.quarter_field} as quarter"
    ])

    query = f"""
        SELECT DISTINCT
            {', '.join(select_fields)}
        FROM {config.table_name}
        WHERE {config.bank_id_field} IS NOT NULL
        AND {config.year_field} IS NOT NULL
        AND {config.quarter_field} IS NOT NULL
        ORDER BY {config.bank_id_field}, {config.year_field}, {config.quarter_field}
    """

    async with get_connection() as conn:
        result = await conn.execute(text(query))

        availability = {}
        skipped_count = 0

        for row in result:
            # Get bank information based on the identifier type
            bank_info = get_bank_info(str(row.bank_identifier), config.bank_id_type)

            if bank_info is None:
                skipped_count += 1
                logger.debug(f"Skipping record with unknown bank identifier: {row.bank_identifier}")
                continue

            bank_id, resolved_bank_name, resolved_bank_symbol = bank_info

            # Use provided fields if available, otherwise use resolved values
            if config.bank_name_field and config.bank_id_type != 'name' and hasattr(row, 'bank_name'):
                bank_name = row.bank_name or resolved_bank_name
            else:
                bank_name = resolved_bank_name

            if config.bank_symbol_field and config.bank_id_type != 'symbol' and hasattr(row, 'bank_symbol'):
                bank_symbol = row.bank_symbol or resolved_bank_symbol
            else:
                bank_symbol = resolved_bank_symbol

            key = (bank_id, row.fiscal_year, row.quarter)
            availability[key] = {
                'bank_id': bank_id,
                'bank_name': bank_name,
                'bank_symbol': bank_symbol,
                'fiscal_year': row.fiscal_year,
                'quarter': row.quarter
            }

        if skipped_count > 0:
            logger.warning(f"  Skipped {skipped_count} records with unknown bank identifiers")

        return availability


async def get_current_availability() -> Dict[Tuple[int, int, str], List[str]]:
    """
    Get current aegis_data_availability table contents.

    Returns:
        Dictionary mapping (bank_id, fiscal_year, quarter) to list of database_names
    """
    async with get_connection() as conn:
        result = await conn.execute(text("""
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


async def update_database_tag(
    table_data: Dict,
    tag: str,
    mode: str = 'update',
    dry_run: bool = False
) -> Dict[str, int]:
    """
    Update or rebuild the aegis_data_availability table with data from a source table.

    Args:
        table_data: Dictionary of availability data from the source table
        tag: The database tag to add/update
        mode: 'update' to add/remove tag, 'rebuild' to wipe and recreate
        dry_run: If True, only show what would be done

    Returns:
        Dictionary with counts of changes made
    """
    # Get current data
    current_data = await get_current_availability()

    if mode == 'rebuild':
        # Remove the tag from all existing records, then add where needed
        records_to_update = []
        records_to_insert = []

        # First pass: remove tag from all current records
        for key, tags in current_data.items():
            if tag in tags:
                records_to_update.append(('remove', key))

        # Second pass: add tag to records that exist in source
        for key, data in table_data.items():
            if key in current_data:
                records_to_update.append(('add', key))
            else:
                records_to_insert.append((key, data))

    else:  # update mode
        # Track changes
        to_add = []
        to_remove = []
        to_insert = []

        # Process source table data
        for key, data in table_data.items():
            if key in current_data:
                if tag not in current_data[key]:
                    to_add.append(key)
            else:
                # Record doesn't exist, need to insert
                to_insert.append((key, data))

        # Check for records to remove tag from
        for key, tags in current_data.items():
            if tag in tags and key not in table_data:
                to_remove.append(key)

        records_to_update = [('add', k) for k in to_add] + [('remove', k) for k in to_remove]
        records_to_insert = to_insert

    # Count changes
    add_count = len([r for r in records_to_update if r[0] == 'add'])
    remove_count = len([r for r in records_to_update if r[0] == 'remove'])
    insert_count = len(records_to_insert)

    logger.info(f"  Changes for '{tag}':")
    logger.info(f"    - Add tag to {add_count} records")
    logger.info(f"    - Remove tag from {remove_count} records")
    logger.info(f"    - Insert {insert_count} new records")

    if dry_run:
        return {'added': add_count, 'removed': remove_count, 'inserted': insert_count}

    # Execute the changes
    async with get_connection() as conn:
        try:
            # Process updates
            for action, key in records_to_update:
                if action == 'add':
                    await conn.execute(text(f"""
                        UPDATE aegis_data_availability
                        SET database_names = array_append(database_names, :tag)
                        WHERE bank_id = :bank_id
                        AND fiscal_year = :fiscal_year
                        AND quarter = :quarter
                        AND NOT (:tag = ANY(database_names))
                    """), {
                        'bank_id': key[0],
                        'fiscal_year': key[1],
                        'quarter': key[2],
                        'tag': tag
                    })
                else:  # remove
                    await conn.execute(text(f"""
                        UPDATE aegis_data_availability
                        SET database_names = array_remove(database_names, :tag)
                        WHERE bank_id = :bank_id
                        AND fiscal_year = :fiscal_year
                        AND quarter = :quarter
                    """), {
                        'bank_id': key[0],
                        'fiscal_year': key[1],
                        'quarter': key[2],
                        'tag': tag
                    })

            # Process inserts
            for key, data in records_to_insert:
                await conn.execute(text("""
                    INSERT INTO aegis_data_availability
                    (bank_id, bank_name, bank_symbol, fiscal_year, quarter, database_names)
                    VALUES (:bank_id, :bank_name, :bank_symbol, :fiscal_year, :quarter, :database_names)
                """), {
                    'bank_id': data['bank_id'],
                    'bank_name': data['bank_name'],
                    'bank_symbol': data['bank_symbol'],
                    'fiscal_year': data['fiscal_year'],
                    'quarter': data['quarter'],
                    'database_names': [tag]
                })

            logger.info(f"  ✅ Successfully updated '{tag}'")

        except Exception as e:
            logger.error(f"  ❌ Error updating '{tag}': {e}")
            raise

    return {'added': add_count, 'removed': remove_count, 'inserted': insert_count}


async def verify_results():
    """
    Show comprehensive summary of current aegis_data_availability state.
    """
    async with get_connection() as conn:
        # Get overall statistics
        result = await conn.execute(text("""
            SELECT
                COUNT(DISTINCT bank_id) as unique_banks,
                COUNT(DISTINCT fiscal_year) as unique_years,
                COUNT(DISTINCT quarter) as unique_quarters,
                COUNT(*) as total_records
            FROM aegis_data_availability
        """))
        stats = result.fetchone()

        logger.info("\n" + "="*70)
        logger.info("VERIFICATION SUMMARY")
        logger.info("="*70)
        logger.info(f"\nOverall Statistics:")
        logger.info(f"  Total records: {stats.total_records}")
        logger.info(f"  Unique banks: {stats.unique_banks}")
        logger.info(f"  Unique years: {stats.unique_years}")
        logger.info(f"  Unique quarters: {stats.unique_quarters}")

        # Get tag statistics
        result = await conn.execute(text("""
            SELECT
                tag,
                COUNT(*) as record_count,
                COUNT(DISTINCT bank_id) as bank_count,
                MIN(fiscal_year) as earliest_year,
                MAX(fiscal_year) as latest_year
            FROM (
                SELECT
                    bank_id,
                    fiscal_year,
                    quarter,
                    unnest(database_names) as tag
                FROM aegis_data_availability
            ) t
            GROUP BY tag
            ORDER BY tag
        """))

        logger.info(f"\nPer-Tag Statistics:")
        logger.info(f"  {'Tag':<15} {'Records':<10} {'Banks':<8} {'Years'}")
        logger.info(f"  {'-'*15} {'-'*10} {'-'*8} {'-'*20}")

        for row in result:
            years = f"{row.earliest_year}-{row.latest_year}"
            logger.info(f"  {row.tag:<15} {row.record_count:<10} {row.bank_count:<8} {years}")

        # Get bank coverage summary
        result = await conn.execute(text("""
            WITH expanded AS (
                SELECT
                    bank_id,
                    bank_name,
                    bank_symbol,
                    fiscal_year,
                    quarter,
                    unnest(database_names) as tag
                FROM aegis_data_availability
            )
            SELECT
                bank_name,
                bank_symbol,
                COUNT(*) as periods,
                array_agg(DISTINCT tag ORDER BY tag) as tags
            FROM expanded
            GROUP BY bank_id, bank_name, bank_symbol
            ORDER BY bank_id
            LIMIT 10
        """))

        logger.info(f"\nTop Banks Coverage:")
        for row in result:
            tags = ', '.join(row.tags) if row.tags else 'none'
            logger.info(f"  {row.bank_name} ({row.bank_symbol}): {row.periods} period-tags [{tags}]")


async def sync_all_tables(mode: str = 'update', dry_run: bool = False):
    """
    Sync all enabled tables based on configuration.

    Args:
        mode: 'update' or 'rebuild'
        dry_run: If True, only show what would be done
    """
    # Filter to enabled tables only
    enabled_tables = [config for config in TABLE_CONFIGS if config.enabled]

    if not enabled_tables:
        logger.error("No tables enabled for sync")
        return

    logger.info(f"Syncing {len(enabled_tables)} table(s)")
    logger.info(f"Mode: {mode}")
    if dry_run:
        logger.info("DRY RUN - No changes will be made")

    logger.info("\n" + "="*60)

    # Track overall results
    total_stats = {'added': 0, 'removed': 0, 'inserted': 0}
    failed_tables = []

    for config in enabled_tables:
        logger.info(f"\nProcessing: {config.table_name} -> '{config.tag}' tag")
        logger.info(f"  Configuration:")
        logger.info(f"    Bank ID type: {config.bank_id_type}")
        logger.info(f"    Bank ID field: {config.bank_id_field}")
        if config.bank_name_field:
            logger.info(f"    Bank name field: {config.bank_name_field}")
        if config.bank_symbol_field:
            logger.info(f"    Bank symbol field: {config.bank_symbol_field}")
        logger.info(f"    Year field: {config.year_field}")
        logger.info(f"    Quarter field: {config.quarter_field}")

        try:
            # Get data from source table
            logger.info(f"  Fetching data from {config.table_name}...")
            table_data = await get_table_availability(config)
            logger.info(f"  Found {len(table_data)} bank/period combinations")

            # Update the aegis_data_availability table
            stats = await update_database_tag(
                table_data=table_data,
                tag=config.tag,
                mode=mode,
                dry_run=dry_run
            )

            # Update totals
            total_stats['added'] += stats['added']
            total_stats['removed'] += stats['removed']
            total_stats['inserted'] += stats['inserted']

        except Exception as e:
            logger.error(f"  ❌ Failed to sync {config.table_name}: {e}")
            failed_tables.append(config.table_name)

    # Summary
    logger.info("\n" + "="*60)
    logger.info("SYNC SUMMARY")
    logger.info("="*60)
    logger.info(f"Total changes across all tables:")
    logger.info(f"  - Tags added: {total_stats['added']}")
    logger.info(f"  - Tags removed: {total_stats['removed']}")
    logger.info(f"  - Records inserted: {total_stats['inserted']}")

    if failed_tables:
        logger.error(f"Failed tables: {', '.join(failed_tables)}")
        sys.exit(1)


async def async_main():
    """
    Main entry point for the sync script.
    """
    parser = argparse.ArgumentParser(
        description="Sync script for aegis_data_availability table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        "--mode",
        choices=['update', 'rebuild'],
        default='update',
        help="Sync mode: 'update' adds/removes tags, 'rebuild' clears and rebuilds"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only show verification summary, don't sync"
    )

    args = parser.parse_args()

    try:
        if args.verify_only:
            await verify_results()
            return

        # Run the sync
        await sync_all_tables(mode=args.mode, dry_run=args.dry_run)

        # Show verification unless dry-run
        if not args.dry_run:
            await verify_results()

    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)


def main():
    """Main entry point that runs the async main function."""
    asyncio.run(async_main())


if __name__ == "__main__":
    main()