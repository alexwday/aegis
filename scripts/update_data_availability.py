#!/usr/bin/env python
"""
Update the aegis_data_availability table with realistic data for August 2025.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.aegis.connections.postgres_connector import _get_engine as get_postgres_engine
from sqlalchemy import text
from datetime import datetime


def clear_and_repopulate():
    """Clear existing data and populate with current data for August 2025."""
    engine = get_postgres_engine()
    
    # Clear existing data
    clear_sql = "DELETE FROM aegis_data_availability"
    
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
    
    records = []
    
    # Current date: August 24, 2025
    # Canadian banks fiscal year: Nov 1 - Oct 31
    # So FY2025 started Nov 1, 2024
    # We're in Q4 FY2025 (Aug-Oct 2025) - not yet reported
    
    # FY2024 - All quarters should be available (Nov 2023 - Oct 2024)
    for bank in banks[:6]:  # Canadian Big Six
        for quarter in ['Q1', 'Q2', 'Q3', 'Q4']:
            # Different databases have different processing times
            if quarter == 'Q4':  # Most recent complete year
                dbs = ['transcripts', 'benchmarking', 'reports', 'rts']
            else:
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
                'last_updated_by': f'FY2024_{quarter}_pipeline'
            })
    
    # FY2025 - Q1, Q2, Q3 available (Q4 is current quarter, not yet reported)
    for bank in banks[:6]:  # Canadian Big Six
        for quarter in ['Q1', 'Q2', 'Q3']:
            # Q3 is most recent (May-Jul 2025, reported in early August)
            if quarter == 'Q3':
                # Q3 just reported, might not have all databases yet
                dbs = ['transcripts', 'reports']
            else:
                # Q1 and Q2 have full coverage
                dbs = ['transcripts', 'benchmarking', 'reports', 'rts', 'pillar3']
            
            records.append({
                'bank_id': bank['id'],
                'bank_name': bank['name'],
                'bank_symbol': bank['symbol'],
                'bank_aliases': bank['aliases'],
                'bank_tags': bank['tags'],
                'fiscal_year': 2025,
                'quarter': quarter,
                'database_names': dbs,
                'last_updated_by': f'FY2025_{quarter}_pipeline'
            })
    
    # US banks - Calendar year, so 2024 complete, 2025 Q1 and Q2 available
    # (Q3 2025 ends Sept 30, so not yet reported in August)
    
    # 2024 - Full year
    for bank in banks[6:10]:  # US banks
        for quarter in ['Q1', 'Q2', 'Q3', 'Q4']:
            records.append({
                'bank_id': bank['id'],
                'bank_name': bank['name'],
                'bank_symbol': bank['symbol'],
                'bank_aliases': bank['aliases'],
                'bank_tags': bank['tags'],
                'fiscal_year': 2024,
                'quarter': quarter,
                'database_names': ['transcripts', 'reports'],  # US banks limited coverage
                'last_updated_by': f'us_2024_{quarter}_pipeline'
            })
    
    # 2025 - Q1 and Q2 only (Q3 not yet reported)
    for bank in banks[6:10]:  # US banks
        for quarter in ['Q1', 'Q2']:
            records.append({
                'bank_id': bank['id'],
                'bank_name': bank['name'],
                'bank_symbol': bank['symbol'],
                'bank_aliases': bank['aliases'],
                'bank_tags': bank['tags'],
                'fiscal_year': 2025,
                'quarter': quarter,
                'database_names': ['transcripts', 'reports'],
                'last_updated_by': f'us_2025_{quarter}_pipeline'
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
            # Clear old data
            conn.execute(text(clear_sql))
            print(f"✅ Cleared old data")
            
            # Insert new data
            for record in records:
                conn.execute(text(insert_sql), record)
            
            trans.commit()
            print(f"✅ Inserted {len(records)} records with data current to August 2025")
        except Exception as e:
            trans.rollback()
            print(f"❌ Error updating data: {e}")
            raise


def verify_data():
    """Verify the updated data."""
    engine = get_postgres_engine()
    
    queries = [
        ("Total records", "SELECT COUNT(*) as count FROM aegis_data_availability"),
        ("Fiscal years", "SELECT DISTINCT fiscal_year FROM aegis_data_availability ORDER BY fiscal_year DESC"),
        ("Latest Canadian bank data (FY2025)", """
            SELECT bank_name, fiscal_year, quarter, database_names 
            FROM aegis_data_availability 
            WHERE bank_id = 1 AND fiscal_year = 2025
            ORDER BY quarter DESC
        """),
        ("Latest US bank data (2025)", """
            SELECT bank_name, fiscal_year, quarter, database_names 
            FROM aegis_data_availability 
            WHERE bank_id = 7 AND fiscal_year = 2025
            ORDER BY quarter DESC
        """),
        ("Summary by year", """
            SELECT fiscal_year, COUNT(DISTINCT bank_id) as banks, 
                   array_agg(DISTINCT quarter ORDER BY quarter) as quarters
            FROM aegis_data_availability 
            GROUP BY fiscal_year 
            ORDER BY fiscal_year DESC
        """)
    ]
    
    with engine.connect() as conn:
        for title, query in queries:
            result = conn.execute(text(query))
            print(f"\n{title}:")
            for row in result:
                print(f"  {row}")


if __name__ == "__main__":
    print("Updating aegis_data_availability for August 2025...")
    print("="*60)
    print("Current date context:")
    print("  Today: August 24, 2025")
    print("  Canadian banks FY2025: Nov 1, 2024 - Oct 31, 2025")
    print("  Current quarter: Q4 FY2025 (Aug-Oct 2025) - NOT YET REPORTED")
    print("  Latest reported: Q3 FY2025 (May-Jul 2025)")
    print("="*60)
    
    clear_and_repopulate()
    
    print("\nVerifying updated data...")
    verify_data()
    
    print("\n✅ Database updated for August 2025!")