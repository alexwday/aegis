"""
Generate SQL data file for aegis_data_availability table based on monitored institutions YAML.
This ensures the initial setup data matches our monitored institutions configuration.
"""

import yaml
from pathlib import Path

def generate_aegis_data_sql():
    """Generate SQL insert statements for aegis_data_availability."""
    
    # Load monitored institutions
    yaml_path = Path(__file__).parent.parent / "docs/monitored_institutions.yaml"
    with open(yaml_path, "r") as f:
        institutions = yaml.safe_load(f)
    
    # Filter for Canadian and US banks only (these have transcripts)
    canadian_banks = {k: v for k, v in institutions.items() if v["type"] == "Canadian_Banks"}
    us_banks = {k: v for k, v in institutions.items() if v["type"] == "US_Banks"}
    
    # Output SQL file path
    output_path = Path(__file__).parent.parent / "data/aegis_data_availability_data.sql"
    
    sql_statements = []
    sql_statements.append("-- Aegis Data Availability Initial Data")
    sql_statements.append("-- Generated from monitored_institutions.yaml")
    sql_statements.append("-- This file contains realistic data availability for Canadian and US banks")
    sql_statements.append("")
    sql_statements.append("-- Clear existing data")
    sql_statements.append("DELETE FROM aegis_data_availability;")
    sql_statements.append("")
    
    # Periods and their database availability
    periods_config = [
        # 2024 periods - no transcripts (transcripts only for 2025 Q1-Q2)
        (2024, "Q1", ["benchmarking", "reports", "rts"]),
        (2024, "Q2", ["benchmarking", "reports", "rts"]),
        (2024, "Q3", ["benchmarking", "reports", "rts"]),
        (2024, "Q4", ["benchmarking", "reports", "rts"]),
        # 2025 periods
        (2025, "Q1", ["transcripts", "benchmarking", "reports", "rts"]),
        (2025, "Q2", ["transcripts", "benchmarking", "reports", "rts"]),
        (2025, "Q3", ["reports"]),  # Limited data for Q3
    ]
    
    sql_statements.append("-- Canadian Banks")
    for ticker, info in canadian_banks.items():
        bank_symbol = ticker.split("-")[0]
        bank_id = info["id"]
        bank_name = info["name"]
        
        for year, quarter, databases in periods_config:
            # Format database array for PostgreSQL
            db_array = "{" + ",".join(databases) + "}"
            
            sql_statements.append(
                f"INSERT INTO aegis_data_availability (bank_id, bank_name, bank_symbol, fiscal_year, quarter, database_names) "
                f"VALUES ({bank_id}, '{bank_name}', '{bank_symbol}', {year}, '{quarter}', '{db_array}');"
            )
    
    sql_statements.append("")
    sql_statements.append("-- US Banks")
    for ticker, info in us_banks.items():
        bank_symbol = ticker.split("-")[0]
        bank_id = info["id"]
        bank_name = info["name"]
        
        for year, quarter, databases in periods_config:
            # Format database array for PostgreSQL
            db_array = "{" + ",".join(databases) + "}"
            
            sql_statements.append(
                f"INSERT INTO aegis_data_availability (bank_id, bank_name, bank_symbol, fiscal_year, quarter, database_names) "
                f"VALUES ({bank_id}, '{bank_name}', '{bank_symbol}', {year}, '{quarter}', '{db_array}');"
            )
    
    # Write to file
    with open(output_path, "w") as f:
        f.write("\n".join(sql_statements))
    
    print(f"Generated SQL file: {output_path}")
    print(f"Total banks: {len(canadian_banks) + len(us_banks)}")
    print(f"Total records: {(len(canadian_banks) + len(us_banks)) * len(periods_config)}")
    
    # Summary
    print("\nDatabase availability by period:")
    for year, quarter, databases in periods_config:
        has_transcripts = "transcripts" in databases
        print(f"  {year} {quarter}: {', '.join(databases)} {'(with transcripts)' if has_transcripts else ''}")

if __name__ == "__main__":
    generate_aegis_data_sql()