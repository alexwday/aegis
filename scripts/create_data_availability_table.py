#!/usr/bin/env python
"""
Create and populate the aegis_data_availability table.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.aegis.connections.postgres_connector import _get_engine as get_postgres_engine
from sqlalchemy import text
from datetime import datetime


def create_table():
    """Create the aegis_data_availability table."""
    engine = get_postgres_engine()
    
    # Drop table if exists (for testing - remove in production)
    drop_sql = """
    DROP TABLE IF EXISTS aegis_data_availability CASCADE;
    """
    
    create_sql = """
    CREATE TABLE aegis_data_availability (
        id SERIAL PRIMARY KEY,
        
        -- Bank identification (denormalized for simplicity)
        bank_id INTEGER NOT NULL,
        bank_name VARCHAR(100) NOT NULL,
        bank_symbol VARCHAR(10) NOT NULL,
        bank_aliases TEXT[],
        bank_tags TEXT[],
        
        -- Period
        fiscal_year INTEGER NOT NULL,
        quarter VARCHAR(2) NOT NULL CHECK (quarter IN ('Q1', 'Q2', 'Q3', 'Q4')),
        
        -- Availability across databases
        database_names TEXT[],
        
        -- Metadata
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_updated_by VARCHAR(100),
        
        -- Unique constraint
        UNIQUE(bank_id, fiscal_year, quarter)
    );
    """
    
    # Create indexes
    index_sql = """
    CREATE INDEX idx_aegis_bank_period ON aegis_data_availability(bank_id, fiscal_year, quarter);
    CREATE INDEX idx_aegis_period ON aegis_data_availability(fiscal_year, quarter);
    CREATE INDEX idx_aegis_bank ON aegis_data_availability(bank_id);
    """
    
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(text(drop_sql))
            conn.execute(text(create_sql))
            conn.execute(text(index_sql))
            trans.commit()
            print("✅ Table aegis_data_availability created successfully")
        except Exception as e:
            trans.rollback()
            print(f"❌ Error creating table: {e}")
            raise


def populate_sample_data():
    """Populate the table with sample data."""
    engine = get_postgres_engine()
    
    # Define bank information
    banks = [
        {
            'id': 1, 'name': 'Royal Bank of Canada', 'symbol': 'RY',
            'aliases': ['RBC', 'Royal Bank', 'Royal', 'RY'],
            'tags': ['canadian_big_six', 'tier1_bank', 'canadian']
        },
        {
            'id': 2, 'name': 'Toronto-Dominion Bank', 'symbol': 'TD',
            'aliases': ['TD', 'TD Bank', 'Toronto Dominion', 'TD Canada Trust'],
            'tags': ['canadian_big_six', 'tier1_bank', 'canadian']
        },
        {
            'id': 3, 'name': 'Bank of Montreal', 'symbol': 'BMO',
            'aliases': ['BMO', 'Bank of Montreal', 'Montreal Bank'],
            'tags': ['canadian_big_six', 'tier1_bank', 'canadian']
        },
        {
            'id': 4, 'name': 'Bank of Nova Scotia', 'symbol': 'BNS',
            'aliases': ['Scotia', 'Scotiabank', 'BNS', 'Nova Scotia Bank'],
            'tags': ['canadian_big_six', 'tier1_bank', 'canadian']
        },
        {
            'id': 5, 'name': 'Canadian Imperial Bank of Commerce', 'symbol': 'CM',
            'aliases': ['CIBC', 'CM', 'Imperial Bank', 'Canadian Imperial'],
            'tags': ['canadian_big_six', 'tier1_bank', 'canadian']
        },
        {
            'id': 6, 'name': 'National Bank of Canada', 'symbol': 'NA',
            'aliases': ['NBC', 'National Bank', 'National', 'NA'],
            'tags': ['canadian_big_six', 'tier1_bank', 'canadian']
        },
        {
            'id': 7, 'name': 'JPMorgan Chase', 'symbol': 'JPM',
            'aliases': ['JPM', 'JP Morgan', 'Chase', 'JPMorgan'],
            'tags': ['us_bank', 'tier1_bank', 'bulge_bracket']
        },
        {
            'id': 8, 'name': 'Bank of America', 'symbol': 'BAC',
            'aliases': ['BofA', 'BAC', 'Bank of America', 'BoA'],
            'tags': ['us_bank', 'tier1_bank', 'bulge_bracket']
        },
        {
            'id': 9, 'name': 'Wells Fargo', 'symbol': 'WFC',
            'aliases': ['Wells', 'WFC', 'Wells Fargo'],
            'tags': ['us_bank', 'tier1_bank']
        },
        {
            'id': 10, 'name': 'Citigroup', 'symbol': 'C',
            'aliases': ['Citi', 'Citibank', 'C'],
            'tags': ['us_bank', 'tier1_bank', 'bulge_bracket']
        }
    ]
    
    # Define periods and database availability
    # Canadian banks (fiscal year Nov-Oct)
    # US banks (calendar year)
    
    records = []
    
    # 2023 data - all quarters for all banks
    for bank in banks[:6]:  # Canadian Big Six
        for quarter in ['Q1', 'Q2', 'Q3', 'Q4']:
            records.append({
                'bank_id': bank['id'],
                'bank_name': bank['name'],
                'bank_symbol': bank['symbol'],
                'bank_aliases': bank['aliases'],
                'bank_tags': bank['tags'],
                'fiscal_year': 2023,
                'quarter': quarter,
                'database_names': ['transcripts', 'benchmarking', 'reports', 'rts'],
                'last_updated_by': 'initial_load'
            })
    
    # 2024 data - Q1, Q2, Q3 for all banks (Q4 not yet reported)
    for bank in banks[:6]:  # Canadian Big Six
        for quarter in ['Q1', 'Q2', 'Q3']:
            # Different databases have different coverage
            if quarter == 'Q3':
                # Q3 most recent, all databases
                dbs = ['transcripts', 'benchmarking', 'reports', 'rts']
            else:
                # Older quarters have all databases
                dbs = ['transcripts', 'benchmarking', 'reports', 'rts', 'pillar3']
            
            records.append({
                'bank_id': bank['id'],
                'bank_name': bank['name'],
                'bank_symbol': bank['symbol'],
                'bank_aliases': bank['aliases'],
                'bank_tags': bank['tags'],
                'fiscal_year': 2024,
                'quarter': quarter,
                'database_names': dbs,
                'last_updated_by': f'{quarter}_2024_pipeline'
            })
    
    # US banks - less coverage
    for bank in banks[6:10]:  # US banks
        for quarter in ['Q1', 'Q2', 'Q3']:
            records.append({
                'bank_id': bank['id'],
                'bank_name': bank['name'],
                'bank_symbol': bank['symbol'],
                'bank_aliases': bank['aliases'],
                'bank_tags': bank['tags'],
                'fiscal_year': 2024,
                'quarter': quarter,
                'database_names': ['transcripts', 'reports'],  # Limited coverage
                'last_updated_by': 'us_banks_pipeline'
            })
    
    # Insert records
    insert_sql = """
    INSERT INTO aegis_data_availability 
    (bank_id, bank_name, bank_symbol, bank_aliases, bank_tags,
     fiscal_year, quarter, database_names, last_updated_by)
    VALUES (:bank_id, :bank_name, :bank_symbol, :bank_aliases, :bank_tags,
            :fiscal_year, :quarter, :database_names, :last_updated_by)
    """
    
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            for record in records:
                conn.execute(text(insert_sql), record)
            trans.commit()
            print(f"✅ Inserted {len(records)} sample records")
        except Exception as e:
            trans.rollback()
            print(f"❌ Error inserting data: {e}")
            raise


def verify_data():
    """Verify the data was inserted correctly."""
    engine = get_postgres_engine()
    
    queries = [
        ("Total records", "SELECT COUNT(*) as count FROM aegis_data_availability"),
        ("Unique banks", "SELECT COUNT(DISTINCT bank_id) as count FROM aegis_data_availability"),
        ("2024 Q3 banks", """
            SELECT bank_name, database_names 
            FROM aegis_data_availability 
            WHERE fiscal_year = 2024 AND quarter = 'Q3'
            ORDER BY bank_id
            LIMIT 5
        """),
        ("Canadian Big Six tags", """
            SELECT DISTINCT bank_name 
            FROM aegis_data_availability 
            WHERE 'canadian_big_six' = ANY(bank_tags)
            ORDER BY bank_name
        """)
    ]
    
    with engine.connect() as conn:
        for title, query in queries:
            result = conn.execute(text(query))
            print(f"\n{title}:")
            for row in result:
                print(f"  {row}")


if __name__ == "__main__":
    print("Creating aegis_data_availability table...")
    create_table()
    
    print("\nPopulating with sample data...")
    populate_sample_data()
    
    print("\nVerifying data...")
    verify_data()
    
    print("\n✅ Database setup complete!")