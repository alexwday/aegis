#!/usr/bin/env python3
"""
Unified database setup script for all Aegis tables.

This script creates all required tables for the Aegis system:
- aegis_data_availability
- process_monitor_logs  
- stage_08_embeddings
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Dict, Any

from sqlalchemy import create_engine, text

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
        self.data_dir = Path(__file__).parent.parent / "data"
        
        # Define tables and their schema files
        self.tables = {
            'aegis_data_availability': 'aegis_data_availability_schema.sql',
            'process_monitor_logs': 'process_monitor_logs_schema.sql',
            'aegis_transcripts': 'aegis_transcripts_schema.sql'
        }
        
        # Define tables with data files (for initial data loading)
        self.data_files = {
            'aegis_data_availability': 'aegis_data_availability_data.sql',
            'process_monitor_logs': 'process_monitor_logs_data.sql'
        }
    
    def check_extension(self, extension_name: str) -> bool:
        """
        Check if a PostgreSQL extension is installed.
        
        Args:
            extension_name: Name of the extension to check
            
        Returns:
            True if extension is installed, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT EXISTS(
                        SELECT 1 FROM pg_extension WHERE extname = :ext
                    )
                """), {'ext': extension_name})
                return result.scalar()
        except Exception as e:
            logger.error(f"Failed to check extension {extension_name}: {e}")
            return False
    
    def install_extension(self, extension_name: str) -> bool:
        """
        Install a PostgreSQL extension.
        
        Args:
            extension_name: Name of the extension to install
            
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    conn.execute(text(f"CREATE EXTENSION IF NOT EXISTS {extension_name}"))
                    trans.commit()
                    logger.info(f"✓ Extension {extension_name} installed successfully")
                    return True
                except Exception as e:
                    trans.rollback()
                    logger.error(f"Failed to install extension {extension_name}: {e}")
                    return False
        except Exception as e:
            logger.error(f"Failed to connect for extension install: {e}")
            return False
    
    def check_table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = :table
                    )
                """), {'table': table_name})
                return result.scalar()
        except Exception as e:
            logger.error(f"Failed to check table {table_name}: {e}")
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
            # Check for pgvector extension if creating transcripts table
            if table_name == 'aegis_transcripts':
                if not self.check_extension('vector'):
                    logger.warning("pgvector extension not found. Attempting to install...")
                    if not self.install_extension('vector'):
                        logger.error("Failed to install pgvector extension. Please install it manually.")
                        logger.error("Run: CREATE EXTENSION vector; in PostgreSQL as superuser")
                        return False
            
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
            
            with self.engine.connect() as conn:
                trans = conn.begin()
                
                try:
                    if drop_existing:
                        logger.warning(f"Dropping existing table {table_name}...")
                        conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                    
                    logger.info(f"Creating table {table_name}...")
                    
                    # Execute schema SQL
                    # Split by semicolon but be careful with functions
                    statements = []
                    current_statement = []
                    in_function = False
                    
                    for line in schema_sql.split('\n'):
                        if '$$' in line:
                            in_function = not in_function
                        current_statement.append(line)
                        
                        if ';' in line and not in_function:
                            statements.append('\n'.join(current_statement))
                            current_statement = []
                    
                    if current_statement:
                        statements.append('\n'.join(current_statement))
                    
                    for statement in statements:
                        statement = statement.strip()
                        if statement and not statement.startswith('--'):
                            conn.execute(text(statement))
                    
                    trans.commit()
                    logger.info(f"✓ Table {table_name} created successfully")
                    return True
                    
                except Exception as e:
                    trans.rollback()
                    logger.error(f"Failed to create table {table_name}: {e}")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to process schema file: {e}")
            return False
    
    def load_initial_data(self, table_name: str) -> bool:
        """
        Load initial data for a table if data file exists.
        
        Args:
            table_name: Name of the table to load data into
            
        Returns:
            True if successful or no data file, False on error
        """
        if table_name not in self.data_files:
            logger.info(f"No initial data file for {table_name}")
            return True
        
        data_file = self.data_dir / self.data_files[table_name]
        if not data_file.exists():
            logger.info(f"Data file not found: {data_file}")
            return True
        
        try:
            with open(data_file, 'r') as f:
                data_sql = f.read()
            
            with self.engine.connect() as conn:
                trans = conn.begin()
                
                try:
                    logger.info(f"Loading initial data for {table_name}...")
                    
                    # Execute data SQL
                    for statement in data_sql.split(';'):
                        statement = statement.strip()
                        if statement and not statement.startswith('--'):
                            conn.execute(text(statement))
                    
                    trans.commit()
                    logger.info(f"✓ Initial data loaded for {table_name}")
                    return True
                    
                except Exception as e:
                    trans.rollback()
                    logger.error(f"Failed to load data for {table_name}: {e}")
                    return False
                    
        except Exception as e:
            logger.error(f"Failed to read data file: {e}")
            return False
    
    def setup_all_tables(self, drop_existing: bool = False, load_data: bool = True) -> bool:
        """
        Setup all Aegis tables.
        
        Args:
            drop_existing: Whether to drop existing tables first
            load_data: Whether to load initial data
            
        Returns:
            True if all successful, False if any failed
        """
        success = True
        
        for table_name in self.tables:
            logger.info(f"\n{'='*60}")
            logger.info(f"Setting up table: {table_name}")
            logger.info(f"{'='*60}")
            
            # Check if table exists
            exists = self.check_table_exists(table_name)
            
            if exists and not drop_existing:
                logger.info(f"Table {table_name} already exists, skipping creation")
                continue
            
            # Create table
            if not self.create_table(table_name, drop_existing):
                logger.error(f"Failed to create table {table_name}")
                success = False
                continue
            
            # Load initial data if requested
            if load_data:
                if not self.load_initial_data(table_name):
                    logger.error(f"Failed to load data for {table_name}")
                    success = False
        
        return success
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get status of all tables.
        
        Returns:
            Dictionary with table status information
        """
        status = {
            'tables': {},
            'extensions': {}
        }
        
        # Check tables
        for table_name in self.tables:
            exists = self.check_table_exists(table_name)
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
        
        # Check extensions
        status['extensions']['pgvector'] = self.check_extension('vector')
        
        return status


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Setup all Aegis database tables'
    )
    parser.add_argument(
        '--create-all',
        action='store_true',
        help='Create all tables'
    )
    parser.add_argument(
        '--create-table',
        type=str,
        help='Create a specific table'
    )
    parser.add_argument(
        '--drop-existing',
        action='store_true',
        help='Drop existing tables before creating (use with caution!)'
    )
    parser.add_argument(
        '--skip-data',
        action='store_true',
        help='Skip loading initial data'
    )
    parser.add_argument(
        '--status',
        action='store_true',
        help='Show status of all tables'
    )
    
    args = parser.parse_args()
    
    setup = AegisDatabaseSetup()
    
    if args.status:
        status = setup.get_status()
        logger.info("\nDatabase Status:")
        logger.info("="*60)
        
        logger.info("\nTables:")
        for table, info in status['tables'].items():
            exists_str = "✓" if info['exists'] else "✗"
            if info['exists']:
                logger.info(f"  {exists_str} {table}: {info['row_count']:,} rows")
            else:
                logger.info(f"  {exists_str} {table}: not created")
        
        logger.info("\nExtensions:")
        for ext, installed in status['extensions'].items():
            status_str = "✓" if installed else "✗"
            logger.info(f"  {status_str} {ext}: {'installed' if installed else 'not installed'}")
        
        sys.exit(0)
    
    if args.create_table:
        success = setup.create_table(args.create_table, drop_existing=args.drop_existing)
        if success and not args.skip_data:
            setup.load_initial_data(args.create_table)
        sys.exit(0 if success else 1)
    
    if args.create_all:
        success = setup.setup_all_tables(
            drop_existing=args.drop_existing,
            load_data=not args.skip_data
        )
        
        if success:
            logger.info("\n" + "="*60)
            logger.info("✓ All tables created successfully!")
            logger.info("="*60)
            
            # Show final status
            status = setup.get_status()
            logger.info("\nFinal Status:")
            for table, info in status['tables'].items():
                if info['exists']:
                    logger.info(f"  ✓ {table}: {info['row_count']:,} rows")
        else:
            logger.error("\n✗ Some tables failed to create")
            sys.exit(1)
    
    if not any([args.create_all, args.create_table, args.status]):
        parser.print_help()


if __name__ == "__main__":
    main()