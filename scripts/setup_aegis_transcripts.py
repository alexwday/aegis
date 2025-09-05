#!/usr/bin/env python3
"""
Setup script for aegis_transcripts table.

This script creates the aegis_transcripts table for storing earnings transcript
embeddings and provides utilities for loading data from CSV files.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.aegis.utils.settings import config
from src.aegis.utils.logging import get_logger

logger = get_logger()


class AegisTranscriptsSetup:
    """
    Setup and data loading utilities for aegis_transcripts table.
    """
    
    def __init__(self):
        """Initialize database connection."""
        self.connection_string = (
            f"postgresql://{config.postgres_user}:{config.postgres_password}"
            f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}"
        )
        self.engine = create_engine(self.connection_string)
        self.inspector = inspect(self.engine)
        
    def create_table(self, drop_existing: bool = False, use_simple: bool = True) -> bool:
        """
        Create the aegis_transcripts table.
        
        Args:
            drop_existing: Whether to drop existing table before creating
            use_simple: Use simplified schema without indexes
            
        Returns:
            True if successful, False otherwise
        """
        if use_simple:
            schema_path = Path(__file__).parent.parent / "data" / "aegis_transcripts_schema_simple.sql"
        else:
            schema_path = Path(__file__).parent.parent / "data" / "aegis_transcripts_schema.sql"
        
        if not schema_path.exists():
            logger.error(f"Schema file not found: {schema_path}")
            return False
            
        try:
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            
            with self.engine.begin() as conn:
                if drop_existing:
                    logger.warning("Dropping existing aegis_transcripts table...")
                    try:
                        conn.execute(text("DROP TABLE IF EXISTS aegis_transcripts CASCADE"))
                    except SQLAlchemyError:
                        pass  # Table might not exist, that's OK
                
                logger.info("Creating aegis_transcripts table...")
                
                # First ensure pgvector extension is enabled
                try:
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
                    logger.info("pgvector extension enabled")
                except SQLAlchemyError as e:
                    if "already exists" not in str(e).lower():
                        logger.warning(f"Could not create pgvector extension: {e}")
                
                # Read and clean the SQL
                schema_sql = schema_sql.replace('\r\n', '\n').replace('\r', '\n')
                
                # Remove the DROP and CREATE EXTENSION statements since we handle them above
                lines = []
                skip_next = False
                for line in schema_sql.split('\n'):
                    if 'DROP TABLE IF EXISTS' in line.upper():
                        continue
                    if 'CREATE EXTENSION' in line.upper():
                        skip_next = True
                        continue
                    if skip_next and ';' in line:
                        skip_next = False
                        continue
                    lines.append(line)
                
                schema_sql = '\n'.join(lines)
                
                # Split statements more carefully
                statements = []
                current = []
                in_function = False
                in_comment = False
                
                for line in schema_sql.split('\n'):
                    # Skip pure comment lines
                    stripped = line.strip()
                    if stripped.startswith('--'):
                        continue
                    
                    # Track function blocks
                    if '$$' in line:
                        in_function = not in_function
                    
                    # Track multi-line comments
                    if '/*' in line:
                        in_comment = True
                    if '*/' in line:
                        in_comment = False
                        continue
                    
                    if in_comment:
                        continue
                    
                    current.append(line)
                    
                    # End of statement
                    if ';' in line and not in_function:
                        stmt = '\n'.join(current).strip()
                        if stmt and not stmt.startswith('--'):
                            statements.append(stmt)
                        current = []
                
                # Don't forget the last statement
                if current:
                    stmt = '\n'.join(current).strip()
                    if stmt and not stmt.startswith('--'):
                        statements.append(stmt)
                
                # Execute each statement
                for i, statement in enumerate(statements, 1):
                    if not statement.strip():
                        continue
                    
                    try:
                        logger.debug(f"Executing statement {i}/{len(statements)}")
                        conn.execute(text(statement))
                    except SQLAlchemyError as e:
                        error_msg = str(e).lower()
                        # Skip known harmless errors
                        if "already exists" in error_msg and "extension" in error_msg:
                            logger.debug("Extension already exists, continuing...")
                        elif "already exists" in error_msg and "function" in error_msg:
                            logger.debug("Function already exists, continuing...")
                        else:
                            logger.error(f"Error executing statement {i}: {e}")
                            logger.error(f"Statement was: {statement[:200]}...")
                            raise
                
                logger.info("✓ Table aegis_transcripts created successfully")
                return True
                    
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            return False
    
    def check_table_exists(self) -> bool:
        """
        Check if the aegis_transcripts table exists.
        
        Returns:
            True if table exists, False otherwise
        """
        try:
            return 'aegis_transcripts' in self.inspector.get_table_names()
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
        Load data from CSV file into aegis_transcripts table.
        
        Args:
            csv_path: Path to the CSV file
            batch_size: Number of rows to insert per batch
            skip_duplicates: Whether to skip duplicate rows
            
        Returns:
            Number of rows successfully inserted
        """
        if not os.path.exists(csv_path):
            logger.error(f"CSV file not found: {csv_path}")
            return 0
        
        if not self.check_table_exists():
            logger.error("Table aegis_transcripts does not exist. Run with --create first.")
            return 0
        
        try:
            logger.info(f"Loading data from {csv_path}...")
            
            # Read CSV file
            df = pd.read_csv(csv_path)
            logger.info(f"Found {len(df)} rows in CSV file")
            
            # Prepare column mappings
            columns = [
                'file_path', 'filename', 'date_last_modified', 'title',
                'transcript_type', 'event_id', 'version_id', 'fiscal_year',
                'fiscal_quarter', 'institution_type', 'institution_id', 'ticker',
                'company_name', 'section_name', 'speaker_block_id', 'qa_group_id',
                'classification_ids', 'classification_names', 'block_summary',
                'chunk_id', 'chunk_tokens', 'chunk_content', 'chunk_paragraph_ids',
                'chunk_embedding'
            ]
            
            rows_inserted = 0
            
            # Process in batches
            with self.engine.begin() as conn:
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i:i+batch_size].copy()
                    
                    # Process array columns
                    for col in ['classification_ids', 'classification_names', 'chunk_paragraph_ids']:
                        if col in batch_df.columns:
                            batch_df[col] = batch_df[col].apply(
                                lambda x: self._parse_array(x) if x is not None and not (isinstance(x, float) and pd.isna(x)) else None
                            )
                    
                    # Process embedding column
                    if 'chunk_embedding' in batch_df.columns:
                        batch_df['chunk_embedding'] = batch_df['chunk_embedding'].apply(
                            lambda x: self._parse_embedding(x) if x is not None and not (isinstance(x, float) and pd.isna(x)) else None
                        )
                    
                    # Filter out rows without embeddings
                    # Use a custom check since embeddings are arrays
                    valid_mask = batch_df['chunk_embedding'].apply(lambda x: x is not None and (isinstance(x, list) or isinstance(x, str)))
                    valid_df = batch_df[valid_mask].copy()
                    
                    if not valid_df.empty:
                        # Prepare the insert statement
                        placeholders = ', '.join([f':{col}' for col in columns])
                        columns_str = ', '.join(columns)
                        
                        if skip_duplicates:
                            insert_stmt = text(f"""
                                INSERT INTO aegis_transcripts ({columns_str})
                                VALUES ({placeholders})
                                ON CONFLICT DO NOTHING
                            """)
                        else:
                            insert_stmt = text(f"""
                                INSERT INTO aegis_transcripts ({columns_str})
                                VALUES ({placeholders})
                            """)
                        
                        # Execute batch insert
                        for _, row in valid_df.iterrows():
                            row_dict = {}
                            for col in columns:
                                if col in row:
                                    value = row[col]
                                    # Handle different column types
                                    if value is None:
                                        row_dict[col] = None
                                    elif isinstance(value, float) and pd.isna(value):
                                        row_dict[col] = None
                                    elif col in ['classification_ids', 'classification_names', 'chunk_paragraph_ids']:
                                        # Handle array columns
                                        if isinstance(value, list):
                                            row_dict[col] = value if value else []
                                        else:
                                            row_dict[col] = []
                                    elif col == 'chunk_embedding':
                                        # Format embedding for PostgreSQL
                                        if value is not None and isinstance(value, (list, tuple)):
                                            row_dict[col] = f"[{','.join(map(str, value))}]"
                                        else:
                                            row_dict[col] = None
                                    else:
                                        row_dict[col] = value
                                else:
                                    row_dict[col] = None
                            
                            try:
                                conn.execute(insert_stmt, row_dict)
                                rows_inserted += 1
                            except SQLAlchemyError as e:
                                if not skip_duplicates:
                                    logger.warning(f"Failed to insert row: {e}")
                        
                        logger.info(f"Inserted batch {i//batch_size + 1}: {len(valid_df)} rows")
            
            logger.info(f"✓ Successfully inserted {rows_inserted} rows")
            return rows_inserted
            
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def _parse_array(self, value):
        """Parse array column from CSV."""
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                # Try JSON parse first
                return json.loads(value)
            except:
                # Fall back to comma split
                return [v.strip() for v in value.split(',') if v.strip()]
        return []
    
    def _parse_embedding(self, value):
        """Parse embedding vector from CSV."""
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except:
                # Try to parse as comma-separated
                try:
                    return [float(x) for x in value.strip('[]').split(',')]
                except:
                    return None
        return None
    
    def verify_data(self) -> Dict[str, Any]:
        """
        Verify data in the aegis_transcripts table.
        
        Returns:
            Dictionary with verification statistics
        """
        if not self.check_table_exists():
            logger.error("Table aegis_transcripts does not exist")
            return {}
        
        try:
            with self.engine.connect() as conn:
                stats = {}
                
                # Total row count
                result = conn.execute(text("SELECT COUNT(*) FROM aegis_transcripts"))
                stats['total_rows'] = result.scalar()
                
                # Unique tickers
                result = conn.execute(text("SELECT COUNT(DISTINCT ticker) FROM aegis_transcripts"))
                stats['unique_tickers'] = result.scalar()
                
                # Unique fiscal periods
                result = conn.execute(text("""
                    SELECT COUNT(DISTINCT (ticker, fiscal_year, fiscal_quarter))
                    FROM aegis_transcripts
                """))
                stats['unique_periods'] = result.scalar()
                
                # Section breakdown
                result = conn.execute(text("""
                    SELECT section_name, COUNT(*) as count
                    FROM aegis_transcripts
                    GROUP BY section_name
                """))
                stats['sections'] = {row[0]: row[1] for row in result}
                
                # Sample data
                result = conn.execute(text("""
                    SELECT ticker, fiscal_year, fiscal_quarter, 
                           section_name, COUNT(*) as chunks
                    FROM aegis_transcripts
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
    
    def check_pgvector(self) -> bool:
        """Check if pgvector extension is installed."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT EXISTS(
                        SELECT 1 FROM pg_extension WHERE extname = 'vector'
                    )
                """))
                return result.scalar()
        except Exception as e:
            logger.error(f"Failed to check pgvector: {e}")
            return False


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Setup and manage aegis_transcripts table'
    )
    parser.add_argument(
        '--create',
        action='store_true',
        help='Create the aegis_transcripts table'
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
    parser.add_argument(
        '--check-pgvector',
        action='store_true',
        help='Check if pgvector extension is installed'
    )
    
    args = parser.parse_args()
    
    setup = AegisTranscriptsSetup()
    
    if args.check_pgvector:
        installed = setup.check_pgvector()
        if installed:
            logger.info("✓ pgvector extension is installed")
        else:
            logger.warning("✗ pgvector extension is not installed")
            logger.info("Install with: CREATE EXTENSION vector; (requires superuser)")
        sys.exit(0 if installed else 1)
    
    if args.check_exists:
        exists = setup.check_table_exists()
        if exists:
            logger.info("✓ Table aegis_transcripts exists")
        else:
            logger.info("✗ Table aegis_transcripts does not exist")
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