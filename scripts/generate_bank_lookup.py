#!/usr/bin/env python
"""
Generate simple bank lookup YAML file from aegis_data_availability table.

This script creates a clean YAML lookup file with bank ID, symbol, and name
for easy reference without database access.

Usage:
    python generate_bank_lookup.py
    
Output:
    bank_lookup.yaml - Simple YAML file with bank information
"""

import yaml
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
from sqlalchemy import text

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger

# Initialize logging
setup_logging()
logger = get_logger()


def fetch_banks() -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch all unique banks from data availability table (source of truth).
    
    Returns:
        Dictionary with institution types as keys and bank lists as values
    """
    logger.info("Fetching banks from data availability table (source of truth)")
    
    try:
        with get_connection() as conn:
            # Get unique banks from data availability table
            # This is our source of truth for bank mappings
            result = conn.execute(text("""
                SELECT DISTINCT 
                    bank_id,
                    bank_symbol,
                    bank_name
                FROM aegis_data_availability
                ORDER BY bank_id
            """)).fetchall()
            
            # Load institution types from monitored_institutions.yaml
            yaml_path = Path(__file__).parent.parent / "docs" / "monitored_institutions.yaml"
            
            # Create mapping from YAML
            id_to_type = {}
            with open(yaml_path, 'r') as f:
                content = f.read()
                for line in content.split('\n'):
                    if ':' in line and not line.startswith('#'):
                        if '{' in line and 'id:' in line:
                            # Extract ID and type
                            import re
                            id_match = re.search(r'id:\s*(\d+)', line)
                            type_match = re.search(r'type:\s*"([^"]+)"', line)
                            if id_match and type_match:
                                id_to_type[int(id_match.group(1))] = type_match.group(1)
            
            # Group banks by type
            banks_by_type = {}
            for row in result:
                bank_data = {
                    "id": row.bank_id,
                    "symbol": row.bank_symbol,
                    "name": row.bank_name
                }
                
                # Get type from mapping or use default
                inst_type = id_to_type.get(row.bank_id, "Unknown")
                
                if inst_type not in banks_by_type:
                    banks_by_type[inst_type] = []
                
                banks_by_type[inst_type].append(bank_data)
            
            total_banks = sum(len(banks) for banks in banks_by_type.values())
            logger.info(f"Found {total_banks} banks in {len(banks_by_type)} categories")
            logger.info(f"Using aegis_data_availability as source of truth")
            for inst_type, banks in banks_by_type.items():
                logger.info(f"  - {inst_type}: {len(banks)} banks")
            
            return banks_by_type
            
    except Exception as e:
        logger.error(f"Error fetching bank data: {e}")
        raise



def save_yaml_file(banks_by_type: Dict[str, List[Dict[str, Any]]], output_filename: str = "bank_lookup.yaml"):
    """
    Save banks to a clean YAML file with custom formatting.
    
    Args:
        banks_by_type: Dictionary with institution types as keys
        output_filename: Name of output file
    """
    # Save to docs folder
    docs_dir = Path(__file__).parent.parent / "docs"
    docs_dir.mkdir(exist_ok=True)
    output_file = docs_dir / output_filename
    
    logger.info(f"Saving YAML file to {output_file}")
    
    try:
        with open(output_file, 'w') as f:
            # Write header
            f.write("# Bank Lookup Reference\n")
            f.write(f"# Generated: {datetime.now().isoformat()}\n")
            f.write("# Source: aegis_data_availability table (authoritative source)\n")
            f.write("# Format: {id: <bank_id>, symbol: <ticker>, name: <full_name>}\n")
            f.write("# Note: Bank IDs 1-7 are Canadian, 8+ are US banks\n\n")
            
            # Write each institution type section
            for inst_type in sorted(banks_by_type.keys()):
                banks = banks_by_type[inst_type]
                f.write(f"{inst_type}:\n")
                for bank in banks:
                    f.write(f"  - {{id: {bank['id']:2d}, symbol: {bank['symbol']:5s}, name: {bank['name']}}}\n")
                f.write("\n")
        
        logger.info(f"Successfully saved YAML file ({output_file.stat().st_size:,} bytes)")
        
    except Exception as e:
        logger.error(f"Error saving YAML file: {e}")
        raise


def print_summary(banks_by_type: Dict[str, List[Dict[str, Any]]]):
    """
    Print a summary of the generated lookup file.
    
    Args:
        banks_by_type: Dictionary with institution types as keys
    """
    total_banks = sum(len(banks) for banks in banks_by_type.values())
    
    print("\n" + "="*60)
    print("BANK LOOKUP YAML GENERATED")
    print("="*60)
    print(f"Generated at: {datetime.now().isoformat()}")
    print(f"Total banks: {total_banks}")
    
    for inst_type in sorted(banks_by_type.keys()):
        banks = banks_by_type[inst_type]
        print(f"  - {inst_type}: {len(banks)} banks")
    print()
    
    print("SAMPLE ENTRIES:")
    print("-"*60)
    for inst_type in sorted(banks_by_type.keys())[:1]:  # Show first category
        banks = banks_by_type[inst_type]
        for bank in banks[:3]:  # Show first 3 banks
            print(f"  {inst_type}: ID {bank['id']:2d} | {bank['symbol']:5s} | {bank['name']}")
    
    print("\nFILE SAVED: docs/bank_lookup.yaml")
    print("="*60)


def main():
    """Main execution function."""
    try:
        # Fetch bank data from database
        banks_by_type = fetch_banks()
        
        # Save to YAML file
        save_yaml_file(banks_by_type)
        
        # Print summary
        print_summary(banks_by_type)
        
    except Exception as e:
        logger.error(f"Failed to generate lookup file: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()