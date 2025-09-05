"""
Script to update aegis_data_availability table with realistic transcripts data.
Transcripts are only available for Canadian and US banks for Q1 and Q2 2025.
"""

import yaml
from aegis.connections.postgres_connector import execute_query, fetch_all

def load_monitored_institutions():
    """Load the monitored institutions from YAML file."""
    with open("docs/monitored_institutions.yaml", "r") as f:
        data = yaml.safe_load(f)
    return data

def update_aegis_availability():
    """Update the aegis_data_availability table with realistic transcripts data."""
    
    # Load monitored institutions
    institutions = load_monitored_institutions()
    
    # Extract Canadian and US banks only
    canadian_banks = {}
    us_banks = {}
    
    for ticker, info in institutions.items():
        if info["type"] == "Canadian_Banks":
            canadian_banks[ticker] = info
        elif info["type"] == "US_Banks":
            us_banks[ticker] = info
    
    print(f"Found {len(canadian_banks)} Canadian banks and {len(us_banks)} US banks")
    
    # First, clear existing mock data
    print("Clearing existing mock data...")
    execute_query("DELETE FROM aegis_data_availability")
    
    # Periods where transcripts are available
    transcripts_periods = [
        (2025, "Q1"),
        (2025, "Q2")
    ]
    
    # Other periods for other databases (2024 Q1-Q4, 2025 Q1-Q3)
    other_periods = [
        (2024, "Q1"),
        (2024, "Q2"),
        (2024, "Q3"),
        (2024, "Q4"),
        (2025, "Q1"),
        (2025, "Q2"),
        (2025, "Q3")
    ]
    
    # Insert data for Canadian banks
    print("Inserting data for Canadian banks...")
    for ticker, info in canadian_banks.items():
        # Remove -CA suffix for bank_symbol
        bank_symbol = ticker.split("-")[0]
        
        for year, quarter in other_periods:
            # Determine which databases are available
            databases = []
            
            # Transcripts only for Q1 and Q2 2025
            if (year, quarter) in transcripts_periods:
                databases.append("transcripts")
            
            # Other databases have varying availability
            if year == 2024 or (year == 2025 and quarter in ["Q1", "Q2"]):
                databases.extend(["benchmarking", "reports", "rts"])
                if quarter != "Q4":  # Pillar3 not available in Q4
                    databases.append("pillar3")
            elif year == 2025 and quarter == "Q3":
                databases.append("reports")  # Only reports in Q3 2025
            
            if databases:
                query = """
                INSERT INTO aegis_data_availability 
                (bank_id, bank_name, bank_symbol, fiscal_year, quarter, database_names)
                VALUES (:bank_id, :bank_name, :bank_symbol, :fiscal_year, :quarter, :database_names)
                """
                
                params = {
                    "bank_id": info["id"],
                    "bank_name": info["name"],
                    "bank_symbol": bank_symbol,
                    "fiscal_year": year,
                    "quarter": quarter,
                    "database_names": databases
                }
                
                execute_query(query, params)
    
    # Insert data for US banks
    print("Inserting data for US banks...")
    for ticker, info in us_banks.items():
        # Remove -US suffix for bank_symbol
        bank_symbol = ticker.split("-")[0]
        
        for year, quarter in other_periods:
            # Determine which databases are available
            databases = []
            
            # Transcripts only for Q1 and Q2 2025
            if (year, quarter) in transcripts_periods:
                databases.append("transcripts")
            
            # Other databases have varying availability
            if year == 2024 or (year == 2025 and quarter in ["Q1", "Q2"]):
                databases.extend(["benchmarking", "reports", "rts"])
                if quarter != "Q4":  # Pillar3 not available in Q4
                    databases.append("pillar3")
            elif year == 2025 and quarter == "Q3":
                databases.append("reports")  # Only reports in Q3 2025
            
            if databases:
                query = """
                INSERT INTO aegis_data_availability 
                (bank_id, bank_name, bank_symbol, fiscal_year, quarter, database_names)
                VALUES (:bank_id, :bank_name, :bank_symbol, :fiscal_year, :quarter, :database_names)
                """
                
                params = {
                    "bank_id": info["id"],
                    "bank_name": info["name"],
                    "bank_symbol": bank_symbol,
                    "fiscal_year": year,
                    "quarter": quarter,
                    "database_names": databases
                }
                
                execute_query(query, params)
    
    # Verify the update
    print("\nVerifying update...")
    
    # Check transcripts availability
    query = """
    SELECT bank_symbol, bank_name, fiscal_year, quarter, database_names
    FROM aegis_data_availability
    WHERE 'transcripts' = ANY(database_names)
    ORDER BY bank_id, fiscal_year, quarter
    LIMIT 10
    """
    
    results = fetch_all(query)
    print(f"\nSample banks with transcripts data ({len(results)} shown):")
    for row in results:
        print(f"  {row['bank_symbol']} ({row['bank_name']}) - {row['fiscal_year']} {row['quarter']}")
    
    # Count total transcripts entries
    query = """
    SELECT COUNT(*) as count
    FROM aegis_data_availability
    WHERE 'transcripts' = ANY(database_names)
    """
    
    count_result = fetch_all(query)
    print(f"\nTotal entries with transcripts: {count_result[0]['count']}")
    
    # Expected: 7 Canadian banks * 2 quarters + 7 US banks * 2 quarters = 28 entries
    
    print("\nAegis availability table updated successfully!")

if __name__ == "__main__":
    update_aegis_availability()