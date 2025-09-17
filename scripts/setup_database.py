#!/usr/bin/env python3
"""
Unified database setup script for Aegis.

This script handles all database operations:
- Creating tables (aegis_data_availability, process_monitor_logs, aegis_transcripts)
- Loading initial data
- Loading CSV data for transcripts
- Checking database status
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, inspect

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.aegis.utils.settings import config
from src.aegis.utils.logging import get_logger

logger = get_logger()


class AegisDatabaseSetup:
    """
    Comprehensive database setup for all Aegis tables.
    """
    
    def __init__(self):
        """Initialize database connection."""
        self.connection_string = (
            f"postgresql://{config.postgres_user}:{config.postgres_password}"
            f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}"
        )
        self.engine = create_engine(self.connection_string)
        self.inspector = inspect(self.engine)
        self.data_dir = Path(__file__).parent.parent / "data"
        
        # Define tables and their schema files
        self.tables = {
            'aegis_data_availability': 'aegis_data_availability_schema.sql',
            'process_monitor_logs': 'process_monitor_logs_schema.sql',
            'aegis_transcripts': 'aegis_transcripts_schema.sql'
        }
        
        # Define tables with initial data files
        self.data_files = {
            'aegis_data_availability': 'aegis_data_availability_data.sql',
            'process_monitor_logs': 'process_monitor_logs_data.sql'
        }
    
    def check_pgvector(self) -> bool:
        """Check if pgvector extension is available."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT * FROM pg_available_extensions WHERE name = 'vector'"
                ))
                return result.fetchone() is not None
        except Exception as e:
            logger.error(f"Failed to check pgvector: {e}")
            return False
    
    def enable_pgvector(self) -> bool:
        """Enable pgvector extension."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
                logger.info("✓ pgvector extension enabled")
                return True
        except Exception as e:
            if "already exists" in str(e).lower():
                logger.info("pgvector extension already enabled")
                return True
            logger.error(f"Failed to enable pgvector: {e}")
            return False
    
    def create_table(self, table_name: str, drop_existing: bool = False) -> bool:
        """
        Create a specific table.
        
        Args:
            table_name: Name of the table to create
            drop_existing: Whether to drop existing table first
            
        Returns:
            True if successful, False otherwise
        """
        if table_name not in self.tables:
            logger.error(f"Unknown table: {table_name}")
            return False
        
        schema_file = self.data_dir / self.tables[table_name]
        if not schema_file.exists():
            logger.error(f"Schema file not found: {schema_file}")
            return False
        
        try:
            # For aegis_transcripts, ensure pgvector is enabled
            if table_name == 'aegis_transcripts':
                if not self.check_pgvector():
                    logger.error("pgvector extension not available. Please install it first.")
                    logger.info("Run: brew install pgvector")
                    logger.info("Then: ./scripts/fix_pgvector.sh")
                    return False
                self.enable_pgvector()
            
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            with self.engine.begin() as conn:
                if drop_existing:
                    logger.info(f"Dropping existing table {table_name}...")
                    conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                
                logger.info(f"Creating table {table_name}...")
                
                # Execute the schema SQL
                # For simple schemas, just execute the whole thing
                conn.execute(text(schema_sql))
                
                logger.info(f"✓ Table {table_name} created successfully")
                return True
                
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            return False
    
    def load_initial_data(self, table_name: str) -> bool:
        """
        Load initial SQL data for a table.
        
        Args:
            table_name: Name of the table to load data into
            
        Returns:
            True if successful or no data file, False on error
        """
        if table_name not in self.data_files:
            logger.debug(f"No initial data file for {table_name}")
            return True
        
        data_file = self.data_dir / self.data_files[table_name]
        if not data_file.exists():
            logger.debug(f"Data file not found: {data_file}")
            return True
        
        try:
            with open(data_file, 'r') as f:
                data_sql = f.read()
            
            with self.engine.begin() as conn:
                logger.info(f"Loading initial data for {table_name}...")
                
                # Execute data SQL statements
                for statement in data_sql.split(';'):
                    statement = statement.strip()
                    if statement and not statement.startswith('--'):
                        conn.execute(text(statement))
                
                logger.info(f"✓ Initial data loaded for {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to load data for {table_name}: {e}")
            return False
    
    def load_transcripts_csv(
        self, 
        csv_path: str, 
        batch_size: int = 100,
        skip_errors: bool = True,
        truncate_first: bool = True
    ) -> int:
        """
        Load CSV data into aegis_transcripts table.
        
        Args:
            csv_path: Path to CSV file
            batch_size: Number of rows to process at a time
            skip_errors: Whether to skip rows with errors
            truncate_first: Whether to truncate the table before loading (default: True)
            
        Returns:
            Number of rows successfully loaded
        """
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            return 0
        
        if 'aegis_transcripts' not in self.inspector.get_table_names():
            logger.error("Table aegis_transcripts does not exist. Create it first with --create-tables")
            return 0
        
        # Clear the table if requested (use DELETE instead of TRUNCATE for permission compatibility)
        if truncate_first:
            try:
                with self.engine.begin() as conn:
                    logger.info("Clearing aegis_transcripts table before loading...")
                    result = conn.execute(text("DELETE FROM aegis_transcripts"))
                    logger.info(f"✓ Table cleared successfully ({result.rowcount} rows deleted)")
            except Exception as e:
                logger.error(f"Failed to clear table: {e}")
                logger.info("Tip: If you don't have DELETE permissions, use --no-truncate to append data")
                return 0
        
        logger.info(f"Loading CSV data from {csv_path}...")
        df = pd.read_csv(csv_path)
        logger.info(f"Found {len(df)} rows in CSV")
        
        # Expected columns
        columns = [
            'file_path', 'filename', 'date_last_modified', 'title',
            'transcript_type', 'event_id', 'version_id', 'fiscal_year',
            'fiscal_quarter', 'institution_type', 'institution_id', 'ticker',
            'company_name', 'section_name', 'speaker_block_id', 'qa_group_id',
            'classification_ids', 'classification_names', 'block_summary',
            'chunk_id', 'chunk_tokens', 'chunk_content', 'chunk_paragraph_ids',
            'chunk_embedding'
        ]
        
        # Add missing columns
        for col in columns:
            if col not in df.columns:
                logger.warning(f"Column '{col}' not in CSV, adding with NULL values")
                df[col] = None
        
        rows_inserted = 0
        rows_failed = 0
        
        with self.engine.begin() as conn:
            for batch_start in range(0, len(df), batch_size):
                batch_end = min(batch_start + batch_size, len(df))
                batch = df.iloc[batch_start:batch_end]
                
                logger.info(f"Processing batch {batch_start//batch_size + 1}: rows {batch_start}-{batch_end}")
                
                for idx, row in batch.iterrows():
                    try:
                        row_data = {}
                        
                        for col in columns:
                            value = row.get(col)
                            
                            # Handle NaN
                            if isinstance(value, float) and np.isnan(value):
                                value = None
                            
                            # Handle array columns
                            if col in ['classification_ids', 'classification_names', 'chunk_paragraph_ids']:
                                if value is None:
                                    row_data[col] = []
                                elif isinstance(value, list):
                                    row_data[col] = value
                                elif isinstance(value, str):
                                    try:
                                        row_data[col] = json.loads(value) if value else []
                                    except:
                                        row_data[col] = [v.strip() for v in value.split(',') if v.strip()]
                                else:
                                    row_data[col] = []
                            
                            # Handle embedding column
                            elif col == 'chunk_embedding':
                                if value is not None:
                                    if isinstance(value, list):
                                        embedding = value
                                    elif isinstance(value, str):
                                        try:
                                            embedding = json.loads(value)
                                        except:
                                            embedding = None
                                    else:
                                        embedding = None
                                    
                                    if embedding:
                                        row_data[col] = f"[{','.join(map(str, embedding))}]"
                                    else:
                                        row_data[col] = None
                                else:
                                    row_data[col] = None
                            
                            else:
                                row_data[col] = value
                        
                        # Skip rows without embeddings (optional)
                        if row_data.get('chunk_embedding') is None:
                            continue
                        
                        # Insert the row
                        insert_sql = text(f"""
                            INSERT INTO aegis_transcripts ({','.join(columns)})
                            VALUES ({','.join([f':{col}' for col in columns])})
                            ON CONFLICT DO NOTHING
                        """)
                        
                        conn.execute(insert_sql, row_data)
                        rows_inserted += 1
                        
                    except Exception as e:
                        rows_failed += 1
                        if skip_errors:
                            logger.debug(f"Failed row {idx}: {e}")
                        else:
                            raise
                
                logger.info(f"  Progress: {rows_inserted} inserted, {rows_failed} failed")
        
        logger.info(f"✓ Successfully loaded {rows_inserted} rows ({rows_failed} failed)")
        return rows_inserted
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get status of all tables.
        
        Returns:
            Dictionary with table status information
        """
        status = {'tables': {}}
        
        for table_name in self.tables:
            exists = table_name in self.inspector.get_table_names()
            row_count = 0
            
            if exists:
                try:
                    with self.engine.connect() as conn:
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                        row_count = result.scalar()
                except:
                    row_count = -1
            
            status['tables'][table_name] = {
                'exists': exists,
                'row_count': row_count
            }
        
        status['pgvector'] = self.check_pgvector()
        
        return status
    
    def setup_all_tables(self, drop_existing: bool = False) -> bool:
        """
        Setup all Aegis tables.
        
        Args:
            drop_existing: Whether to drop existing tables first
            
        Returns:
            True if all successful, False if any failed
        """
        success = True
        
        for table_name in self.tables:
            if not self.create_table(table_name, drop_existing):
                success = False
                continue
            
            if not self.load_initial_data(table_name):
                success = False
        
        return success


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Aegis Database Setup - Manage all database tables and data'
    )
    
    # Table creation
    parser.add_argument(
        '--create-tables',
        action='store_true',
        help='Create all database tables'
    )
    parser.add_argument(
        '--drop-existing',
        action='store_true',
        help='Drop existing tables before creating (CAUTION: Data loss!)'
    )
    
    # CSV loading
    parser.add_argument(
        '--load-csv',
        type=str,
        metavar='PATH',
        help='Load transcript data from CSV file (deletes all existing rows first by default)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Batch size for CSV loading (default: 100)'
    )
    parser.add_argument(
        '--no-truncate',
        action='store_true',
        help='Do NOT delete existing rows before loading CSV (append mode)'
    )
    
    # Status
    parser.add_argument(
        '--status',
        action='store_true',
        help='Show database status'
    )
    
    args = parser.parse_args()
    
    # If no arguments, show help
    if not any([args.create_tables, args.load_csv, args.status]):
        parser.print_help()
        return 0
    
    setup = AegisDatabaseSetup()
    
    # Show status
    if args.status:
        status = setup.get_status()
        logger.info("\n" + "="*60)
        logger.info("Database Status")
        logger.info("="*60)
        
        logger.info(f"\npgvector extension: {'✓ Available' if status['pgvector'] else '✗ Not available'}")
        
        logger.info("\nTables:")
        for table, info in status['tables'].items():
            if info['exists']:
                logger.info(f"  ✓ {table}: {info['row_count']:,} rows")
            else:
                logger.info(f"  ✗ {table}: not created")
        
        return 0
    
    # Create tables
    if args.create_tables:
        logger.info("="*60)
        logger.info("Creating Database Tables")
        logger.info("="*60)
        
        if setup.setup_all_tables(drop_existing=args.drop_existing):
            logger.info("\n✓ All tables created successfully!")
        else:
            logger.error("\n✗ Some tables failed to create")
            return 1
    
    # Load CSV
    if args.load_csv:
        logger.info("\n" + "="*60)
        logger.info("Loading Transcript Data from CSV")
        logger.info("="*60)
        
        rows = setup.load_transcripts_csv(
            args.load_csv,
            batch_size=args.batch_size,
            truncate_first=(not args.no_truncate)
        )
        
        if rows > 0:
            logger.info(f"\n✓ Successfully loaded {rows:,} rows")
        else:
            logger.error("\n✗ No rows were loaded")
            return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())