#!/usr/bin/env python3
"""
Simple CSV loader for aegis_transcripts table.

This script loads CSV data into the aegis_transcripts table with robust error handling.
"""

import argparse
import json
import sys
from pathlib import Path
import pandas as pd
import numpy as np

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from src.aegis.utils.settings import config
from src.aegis.utils.logging import get_logger

logger = get_logger()


def safe_parse_array(value):
    """Safely parse array columns from CSV."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, float) and np.isnan(value):
        return []
    if isinstance(value, str):
        value = value.strip()
        if not value or value.lower() == 'nan':
            return []
        try:
            # Try JSON parse
            result = json.loads(value)
            return result if isinstance(result, list) else []
        except:
            # Try comma-separated
            return [v.strip() for v in value.split(',') if v.strip()]
    return []


def safe_parse_embedding(value):
    """Safely parse embedding vector from CSV."""
    if value is None:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value or value.lower() == 'nan':
            return None
        try:
            # Try JSON parse
            result = json.loads(value)
            if isinstance(result, list) and len(result) > 0:
                return result
        except:
            # Try to parse as comma-separated
            try:
                result = [float(x) for x in value.strip('[]').split(',')]
                if len(result) > 0:
                    return result
            except:
                pass
    return None


def load_csv(csv_path: str, batch_size: int = 100, skip_errors: bool = True):
    """
    Load CSV data into aegis_transcripts table.
    
    Args:
        csv_path: Path to CSV file
        batch_size: Number of rows to process at a time
        skip_errors: Whether to skip rows with errors
        
    Returns:
        Number of rows successfully loaded
    """
    
    # Build connection
    connection_string = (
        f"postgresql://{config.postgres_user}:{config.postgres_password}"
        f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}"
    )
    engine = create_engine(connection_string)
    
    # Read CSV
    logger.info(f"Reading CSV file: {csv_path}")
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
    
    # Add missing columns with None
    for col in columns:
        if col not in df.columns:
            logger.warning(f"Column '{col}' not in CSV, adding with NULL values")
            df[col] = None
    
    rows_inserted = 0
    rows_failed = 0
    
    with engine.begin() as conn:
        # Process in batches
        for batch_start in range(0, len(df), batch_size):
            batch_end = min(batch_start + batch_size, len(df))
            batch = df.iloc[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_start//batch_size + 1}: rows {batch_start}-{batch_end}")
            
            for idx, row in batch.iterrows():
                try:
                    # Build the row data
                    row_data = {}
                    
                    for col in columns:
                        value = row.get(col)
                        
                        # Handle NaN values
                        if isinstance(value, float) and np.isnan(value):
                            value = None
                        
                        # Handle specific column types
                        if col in ['classification_ids', 'classification_names', 'chunk_paragraph_ids']:
                            row_data[col] = safe_parse_array(value)
                        elif col == 'chunk_embedding':
                            embedding = safe_parse_embedding(value)
                            if embedding is not None:
                                # Format for PostgreSQL vector type
                                row_data[col] = f"[{','.join(map(str, embedding))}]"
                            else:
                                row_data[col] = None
                        else:
                            row_data[col] = value
                    
                    # Skip rows without embeddings (optional)
                    if row_data.get('chunk_embedding') is None:
                        logger.debug(f"Skipping row {idx}: no embedding")
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
                        logger.warning(f"Failed to insert row {idx}: {e}")
                    else:
                        logger.error(f"Failed to insert row {idx}: {e}")
                        raise
            
            logger.info(f"  Inserted: {rows_inserted}, Failed: {rows_failed}")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Loading complete!")
    logger.info(f"  Total rows processed: {len(df)}")
    logger.info(f"  Successfully inserted: {rows_inserted}")
    logger.info(f"  Failed: {rows_failed}")
    logger.info(f"{'='*60}")
    
    return rows_inserted


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Load CSV data into aegis_transcripts table')
    parser.add_argument('csv_file', help='Path to CSV file')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size (default: 100)')
    parser.add_argument('--stop-on-error', action='store_true', help='Stop on first error')
    
    args = parser.parse_args()
    
    if not Path(args.csv_file).exists():
        logger.error(f"CSV file not found: {args.csv_file}")
        return 1
    
    try:
        rows = load_csv(
            args.csv_file,
            batch_size=args.batch_size,
            skip_errors=not args.stop_on_error
        )
        
        if rows > 0:
            logger.info(f"\n✅ Successfully loaded {rows} rows")
            return 0
        else:
            logger.error("\n❌ No rows were loaded")
            return 1
            
    except Exception as e:
        logger.error(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())