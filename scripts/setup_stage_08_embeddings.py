#!/usr/bin/env python3
"""
Setup script for stage_08_embeddings table.

This script creates the stage_08_embeddings table for storing earnings transcript
embeddings and provides utilities for loading data from CSV files.
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.aegis.utils.settings import config
from src.aegis.utils.logging import get_logger

logger = get_logger()


class Stage08EmbeddingsSetup:
    """
    Setup and data loading utilities for stage_08_embeddings table.
    """
    
    def __init__(self):
        """Initialize database connection."""
        self.connection_string = (
            f"postgresql://{config.postgres_user}:{config.postgres_password}"
            f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}"
        )
        self.engine = create_engine(self.connection_string)
        
    def create_table(self, drop_existing: bool = False) -> bool:
        """
        Create the stage_08_embeddings table.
        
        Args:
            drop_existing: Whether to drop existing table before creating
            
        Returns:
            True if successful, False otherwise
        """
        schema_path = Path(__file__).parent.parent / "data" / "stage_08_postgres_schema.sql"
        
        if not schema_path.exists():
            logger.error(f"Schema file not found: {schema_path}")
            return False
            
        try:
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            
            with self.engine.connect() as conn:
                # Begin transaction
                trans = conn.begin()
                
                try:
                    if drop_existing:
                        logger.warning("Dropping existing stage_08_embeddings table...")
                        conn.execute(text("DROP TABLE IF EXISTS stage_08_embeddings CASCADE"))
                    
                    logger.info("Creating stage_08_embeddings table...")
                    # Execute the entire schema SQL file
                    for statement in schema_sql.split(';'):
                        statement = statement.strip()
                        if statement and not statement.startswith('--'):
                            conn.execute(text(statement))
                    
                    trans.commit()
                    logger.info("✓ Table stage_08_embeddings created successfully")
                    return True
                    
                except Exception as e:
                    trans.rollback()
                    logger.error(f"Failed to create table: {e}")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to read schema file: {e}")
            return False
    
    def check_table_exists(self) -> bool:
        """
        Check if the stage_08_embeddings table exists.
        
        Returns:
            True if table exists, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'stage_08_embeddings'
                    )
                """))
                return result.scalar()
        except Exception as e:
            logger.error(f"Failed to check table existence: {e}")
            return False
    
    def load_csv(
        self, 
        csv_path: str, 
        batch_size: int = 1000,
        skip_duplicates: bool = True
    ) -> int:
        """
        Load data from CSV file into stage_08_embeddings table.
        
        Args:
            csv_path: Path to the CSV file
            batch_size: Number of rows to insert per batch
            skip_duplicates: Whether to skip duplicate rows based on unique constraints
            
        Returns:
            Number of rows successfully inserted
        """
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            return 0
        
        if not self.check_table_exists():
            logger.error("Table stage_08_embeddings does not exist. Run with --create first.")
            return 0
        
        try:
            logger.info(f"Loading data from {csv_path}...")
            
            # Read CSV file
            df = pd.read_csv(csv_path)
            logger.info(f"Found {len(df)} rows in CSV file")
            
            # Prepare data for insertion
            rows_inserted = 0
            
            # Convert DataFrame to list of tuples for batch insertion
            columns = [
                'file_path', 'filename', 'date_last_modified', 'title',
                'transcript_type', 'event_id', 'version_id', 'fiscal_year',
                'fiscal_quarter', 'institution_type', 'institution_id', 'ticker',
                'company_name', 'section_name', 'speaker_block_id', 'qa_group_id',
                'classification_ids', 'classification_names', 'block_summary',
                'chunk_id', 'chunk_tokens', 'chunk_content', 'chunk_paragraph_ids',
                'chunk_embedding'
            ]
            
            # Process in batches
            with psycopg2.connect(self.connection_string) as conn:
                with conn.cursor() as cursor:
                    for i in range(0, len(df), batch_size):
                        batch_df = df.iloc[i:i+batch_size]
                        
                        # Convert DataFrame to list of tuples
                        values = []
                        for _, row in batch_df.iterrows():
                            # Convert string arrays to PostgreSQL arrays
                            row_data = []
                            for col in columns:
                                if col in ['classification_ids', 'classification_names', 'chunk_paragraph_ids']:
                                    # Handle array columns
                                    if col in row and pd.notna(row[col]):
                                        try:
                                            # Try to parse as JSON array
                                            value = json.loads(row[col]) if isinstance(row[col], str) else row[col]
                                        except:
                                            # If not JSON, split by comma
                                            value = row[col].split(',') if isinstance(row[col], str) else []
                                    else:
                                        value = []
                                    row_data.append(value)
                                elif col == 'chunk_embedding':
                                    # Handle embedding column (convert string to array)
                                    if col in row and pd.notna(row[col]):
                                        try:
                                            # Parse embedding vector
                                            if isinstance(row[col], str):
                                                embedding = json.loads(row[col])
                                            else:
                                                embedding = row[col]
                                            row_data.append(embedding)
                                        except:
                                            logger.warning(f"Failed to parse embedding for row {i + _}")
                                            row_data.append(None)
                                    else:
                                        row_data.append(None)
                                else:
                                    # Handle regular columns
                                    value = row.get(col)
                                    if pd.isna(value):
                                        value = None
                                    row_data.append(value)
                            
                            if row_data[-1] is not None:  # Only add if embedding exists
                                values.append(tuple(row_data))
                        
                        if values:
                            # Build INSERT statement with ON CONFLICT handling
                            insert_query = sql.SQL("""
                                INSERT INTO stage_08_embeddings ({})
                                VALUES %s
                                ON CONFLICT DO NOTHING
                            """).format(
                                sql.SQL(', ').join([sql.Identifier(col) for col in columns])
                            )
                            
                            if skip_duplicates:
                                execute_values(cursor, insert_query, values)
                            else:
                                # Without conflict handling
                                insert_query = sql.SQL("""
                                    INSERT INTO stage_08_embeddings ({})
                                    VALUES %s
                                """).format(
                                    sql.SQL(', ').join([sql.Identifier(col) for col in columns])
                                )
                                execute_values(cursor, insert_query, values)
                            
                            rows_inserted += len(values)
                            logger.info(f"Inserted batch {i//batch_size + 1}: {len(values)} rows")
                    
                    conn.commit()
            
            logger.info(f"✓ Successfully inserted {rows_inserted} rows")
            return rows_inserted
            
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            return 0
    
    def verify_data(self) -> Dict[str, Any]:
        """
        Verify data in the stage_08_embeddings table.
        
        Returns:
            Dictionary with verification statistics
        """
        if not self.check_table_exists():
            logger.error("Table stage_08_embeddings does not exist")
            return {}
        
        try:
            with self.engine.connect() as conn:
                stats = {}
                
                # Total row count
                result = conn.execute(text("SELECT COUNT(*) FROM stage_08_embeddings"))
                stats['total_rows'] = result.scalar()
                
                # Unique tickers
                result = conn.execute(text("SELECT COUNT(DISTINCT ticker) FROM stage_08_embeddings"))
                stats['unique_tickers'] = result.scalar()
                
                # Unique fiscal periods
                result = conn.execute(text("""
                    SELECT COUNT(DISTINCT (ticker, fiscal_year, fiscal_quarter))
                    FROM stage_08_embeddings
                """))
                stats['unique_periods'] = result.scalar()
                
                # Section breakdown
                result = conn.execute(text("""
                    SELECT section_name, COUNT(*) as count
                    FROM stage_08_embeddings
                    GROUP BY section_name
                """))
                stats['sections'] = {row[0]: row[1] for row in result}
                
                # Sample data
                result = conn.execute(text("""
                    SELECT ticker, fiscal_year, fiscal_quarter, 
                           section_name, COUNT(*) as chunks
                    FROM stage_08_embeddings
                    GROUP BY ticker, fiscal_year, fiscal_quarter, section_name
                    ORDER BY fiscal_year DESC, fiscal_quarter DESC
                    LIMIT 10
                """))
                stats['sample_data'] = [
                    {
                        'ticker': row[0],
                        'fiscal_year': row[1],
                        'fiscal_quarter': row[2],
                        'section': row[3],
                        'chunks': row[4]
                    }
                    for row in result
                ]
                
                return stats
                
        except Exception as e:
            logger.error(f"Failed to verify data: {e}")
            return {}


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Setup and manage stage_08_embeddings table'
    )
    parser.add_argument(
        '--create',
        action='store_true',
        help='Create the stage_08_embeddings table'
    )
    parser.add_argument(
        '--drop-existing',
        action='store_true',
        help='Drop existing table before creating (use with caution!)'
    )
    parser.add_argument(
        '--load-csv',
        type=str,
        help='Path to CSV file to load into the table'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for CSV loading (default: 1000)'
    )
    parser.add_argument(
        '--verify',
        action='store_true',
        help='Verify data in the table'
    )
    parser.add_argument(
        '--check-exists',
        action='store_true',
        help='Check if table exists'
    )
    
    args = parser.parse_args()
    
    setup = Stage08EmbeddingsSetup()
    
    if args.check_exists:
        exists = setup.check_table_exists()
        if exists:
            logger.info("✓ Table stage_08_embeddings exists")
        else:
            logger.info("✗ Table stage_08_embeddings does not exist")
        sys.exit(0 if exists else 1)
    
    if args.create:
        success = setup.create_table(drop_existing=args.drop_existing)
        if not success:
            sys.exit(1)
    
    if args.load_csv:
        rows = setup.load_csv(args.load_csv, batch_size=args.batch_size)
        if rows == 0:
            logger.error("No rows were loaded")
            sys.exit(1)
    
    if args.verify:
        stats = setup.verify_data()
        if stats:
            logger.info("Table statistics:")
            logger.info(f"  Total rows: {stats.get('total_rows', 0):,}")
            logger.info(f"  Unique tickers: {stats.get('unique_tickers', 0)}")
            logger.info(f"  Unique periods: {stats.get('unique_periods', 0)}")
            if 'sections' in stats:
                logger.info("  Sections:")
                for section, count in stats['sections'].items():
                    logger.info(f"    {section}: {count:,}")
            if 'sample_data' in stats:
                logger.info("  Sample data:")
                for sample in stats['sample_data']:
                    logger.info(
                        f"    {sample['ticker']} {sample['fiscal_year']}-{sample['fiscal_quarter']} "
                        f"{sample['section']}: {sample['chunks']} chunks"
                    )
        else:
            logger.error("Failed to get verification statistics")
            sys.exit(1)


if __name__ == "__main__":
    main()