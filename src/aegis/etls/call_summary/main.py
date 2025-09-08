"""
Call Summary ETL Script - Generates call summary reports using direct transcript functions.

This script directly calls the transcripts subagent's internal functions to bypass
the full orchestration layer for efficient ETL processing.

Usage:
    python -m aegis.etls.call_summary.main --bank "Royal Bank of Canada" --year 2024 --quarter Q3 --query "revenue and growth"
    python -m aegis.etls.call_summary.main --bank RY --year 2024 --quarter Q3 --query "What did management say about expenses?"
    python -m aegis.etls.call_summary.main --bank TD --year 2024 --quarter Q2 --query "Extract all forward guidance" --output report.txt
"""

import argparse
import json
import sys
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy import text

# Import direct transcript functions
from aegis.model.subagents.transcripts.retrieval import retrieve_full_section
from aegis.model.subagents.transcripts.formatting import (
    format_full_section_chunks,
    generate_research_statement
)
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger

# Initialize logging
setup_logging()
logger = get_logger()


def get_bank_info(bank_name: str) -> Dict[str, Any]:
    """
    Look up bank information from the aegis_data_availability table.
    
    Args:
        bank_name: Name or symbol of the bank
        
    Returns:
        Dictionary with bank_id, bank_name, and bank_symbol
        
    Raises:
        ValueError: If bank not found
    """
    with get_connection() as conn:
        # Try exact match first
        result = conn.execute(text(
            """
            SELECT DISTINCT bank_id, bank_name, bank_symbol
            FROM aegis_data_availability
            WHERE LOWER(bank_name) = LOWER(:bank_name)
               OR LOWER(bank_symbol) = LOWER(:bank_name)
            LIMIT 1
            """
        ), {"bank_name": bank_name}).fetchone()
        
        if not result:
            # Try partial match
            result = conn.execute(text(
                """
                SELECT DISTINCT bank_id, bank_name, bank_symbol
                FROM aegis_data_availability
                WHERE LOWER(bank_name) LIKE LOWER(:pattern)
                   OR LOWER(bank_symbol) LIKE LOWER(:pattern)
                LIMIT 1
                """
            ), {"pattern": f"%{bank_name}%"}).fetchone()
        
        if not result:
            # List available banks for user
            available = conn.execute(text(
                """
                SELECT DISTINCT bank_symbol, bank_name
                FROM aegis_data_availability
                ORDER BY bank_symbol
                """
            )).fetchall()
            
            bank_list = "\n".join([f"  - {r.bank_symbol}: {r.bank_name}" for r in available])
            raise ValueError(
                f"Bank '{bank_name}' not found. Available banks:\n{bank_list}"
            )
        
        return {
            "bank_id": result.bank_id,
            "bank_name": result.bank_name,
            "bank_symbol": result.bank_symbol
        }


def verify_data_availability(bank_id: int, fiscal_year: int, quarter: str) -> bool:
    """
    Check if transcript data is available for the specified bank and period.
    
    Args:
        bank_id: Bank ID
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")
        
    Returns:
        True if transcript data is available, False otherwise
    """
    with get_connection() as conn:
        result = conn.execute(text(
            """
            SELECT database_names
            FROM aegis_data_availability
            WHERE bank_id = :bank_id
              AND fiscal_year = :fiscal_year
              AND quarter = :quarter
            """
        ), {
            "bank_id": bank_id,
            "fiscal_year": fiscal_year,
            "quarter": quarter
        }).fetchone()
        
        if result and result.database_names:
            return 'transcripts' in result.database_names
        
        return False


def generate_call_summary(
    bank_name: str,
    fiscal_year: int,
    quarter: str,
    query: str
) -> str:
    """
    Generate a call summary by directly calling transcript functions.
    
    Args:
        bank_name: Name or symbol of the bank
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")
        query: Query string for what to extract from the transcript
        
    Returns:
        The generated call summary content
    """
    execution_id = str(uuid.uuid4())
    logger.info(
        "etl.call_summary.started",
        execution_id=execution_id,
        bank_name=bank_name,
        fiscal_year=fiscal_year,
        quarter=quarter,
        query=query[:100]  # Log first 100 chars of query
    )
    
    try:
        # Step 1: Look up bank information
        bank_info = get_bank_info(bank_name)
        logger.info(
            "etl.call_summary.bank_found",
            execution_id=execution_id,
            bank_id=bank_info["bank_id"],
            bank_name=bank_info["bank_name"],
            bank_symbol=bank_info["bank_symbol"]
        )
        
        # Step 2: Verify data availability
        if not verify_data_availability(bank_info["bank_id"], fiscal_year, quarter):
            error_msg = f"No transcript data available for {bank_info['bank_name']} {quarter} {fiscal_year}"
            logger.warning(
                "etl.call_summary.no_data",
                execution_id=execution_id,
                message=error_msg
            )
            
            # Check what periods are available
            with get_connection() as conn:
                available_periods = conn.execute(text(
                    """
                    SELECT DISTINCT fiscal_year, quarter
                    FROM aegis_data_availability
                    WHERE bank_id = :bank_id
                      AND 'transcripts' = ANY(database_names)
                    ORDER BY fiscal_year DESC, quarter DESC
                    LIMIT 10
                    """
                ), {"bank_id": bank_info["bank_id"]}).fetchall()
                
                if available_periods:
                    period_list = ", ".join([f"{p.quarter} {p.fiscal_year}" for p in available_periods])
                    error_msg += f"\n\nAvailable periods for {bank_info['bank_name']}: {period_list}"
            
            return f"⚠️ {error_msg}"
        
        # Step 3: Setup context for function calls
        ssl_config = setup_ssl()
        auth_config = setup_authentication(execution_id, ssl_config)
        
        if not auth_config["success"]:
            error_msg = f"Authentication failed: {auth_config.get('error', 'Unknown error')}"
            logger.error(
                "etl.call_summary.auth_failed",
                execution_id=execution_id,
                error=error_msg
            )
            return f"⚠️ {error_msg}"
        
        context = {
            "execution_id": execution_id,
            "auth_config": auth_config,
            "ssl_config": ssl_config
        }
        
        # Step 4: Create bank-period combination
        combo = {
            "bank_id": bank_info["bank_id"],
            "bank_name": bank_info["bank_name"],
            "bank_symbol": bank_info["bank_symbol"],
            "fiscal_year": fiscal_year,
            "quarter": quarter,
            "query_intent": query
        }
        
        logger.info(
            "etl.call_summary.retrieving_transcript",
            execution_id=execution_id,
            combo=combo
        )
        
        # Step 5: DIRECTLY CALL RETRIEVAL FUNCTION
        chunks = retrieve_full_section(
            combo=combo,
            sections="ALL",  # Get full transcript
            context=context
        )
        
        if not chunks:
            return f"⚠️ No transcript chunks found for {bank_info['bank_name']} {quarter} {fiscal_year}"
        
        logger.info(
            "etl.call_summary.chunks_retrieved",
            execution_id=execution_id,
            num_chunks=len(chunks)
        )
        
        # Step 6: FORMAT THE CHUNKS
        formatted_content = format_full_section_chunks(
            chunks=chunks,
            combo=combo,
            context=context
        )
        
        logger.info(
            "etl.call_summary.content_formatted",
            execution_id=execution_id,
            content_length=len(formatted_content)
        )
        
        # Step 7: GENERATE RESEARCH WITH CUSTOM ETL PROMPT
        custom_prompt = f"""Based on the transcript above, please address the following query:

{query}

Provide a comprehensive response with specific quotes and speaker attribution where relevant."""
        
        research = generate_research_statement(
            formatted_content=formatted_content,
            combo=combo,
            context=context,
            method=0,  # We used full retrieval
            method_reasoning="ETL direct retrieval of full transcript",
            custom_prompt=custom_prompt  # Pass our ETL-specific prompt
        )
        
        logger.info(
            "etl.call_summary.completed",
            execution_id=execution_id,
            research_length=len(research)
        )
        
        # Format the final output
        output = f"""
================================================================================
CALL SUMMARY ETL REPORT
================================================================================
Bank: {bank_info['bank_name']} ({bank_info['bank_symbol']})
Period: {quarter} {fiscal_year}
Query: {query}
Generated: {datetime.now().isoformat()}
Execution ID: {execution_id}
================================================================================

{research}

================================================================================
END OF REPORT
================================================================================
"""
        
        return output
        
    except Exception as e:
        error_msg = f"Error generating call summary: {str(e)}"
        logger.error(
            "etl.call_summary.error",
            execution_id=execution_id,
            error=error_msg,
            exc_info=True
        )
        return f"❌ {error_msg}"


def main():
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Generate call summary reports using direct transcript function calls",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract revenue information
  python -m aegis.etls.call_summary.main --bank RY --year 2024 --quarter Q3 --query "Extract all revenue metrics and growth rates"
  
  # Management commentary
  python -m aegis.etls.call_summary.main --bank TD --year 2024 --quarter Q2 --query "What did management say about expenses?"
  
  # Forward guidance
  python -m aegis.etls.call_summary.main --bank BMO --year 2024 --quarter Q3 --query "Extract all forward-looking statements and guidance"
  
  # Save to file
  python -m aegis.etls.call_summary.main --bank RY --year 2024 --quarter Q3 --query "Summarize the call" --output report.txt
        """
    )
    
    parser.add_argument(
        "--bank",
        required=True,
        help="Bank name or symbol (e.g., 'Royal Bank of Canada', 'RY')"
    )
    
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Fiscal year (e.g., 2024)"
    )
    
    parser.add_argument(
        "--quarter",
        required=True,
        choices=["Q1", "Q2", "Q3", "Q4"],
        help="Quarter (Q1, Q2, Q3, Q4)"
    )
    
    parser.add_argument(
        "--query",
        required=True,
        help="Query string describing what to extract from the transcript"
    )
    
    parser.add_argument(
        "--output",
        help="Optional output file path (defaults to stdout)"
    )
    
    args = parser.parse_args()
    
    # Generate the call summary
    print(f"\n🔄 Generating report for {args.bank} {args.quarter} {args.year}...")
    print(f"   Query: {args.query}\n")
    
    result = generate_call_summary(
        bank_name=args.bank,
        fiscal_year=args.year,
        quarter=args.quarter,
        query=args.query
    )
    
    # Output the result
    if args.output:
        with open(args.output, 'w') as f:
            f.write(result)
        print(f"✅ Report saved to: {args.output}")
    else:
        print(result)


if __name__ == "__main__":
    main()