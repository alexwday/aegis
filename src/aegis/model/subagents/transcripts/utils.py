"""
Utility functions for transcripts subagent.
"""

from pathlib import Path
import yaml
from typing import Dict, Any, List
from sqlalchemy import text

from ....utils.logging import get_logger
from ....connections.postgres_connector import get_connection


async def load_financial_categories() -> Dict[int, Dict[str, str]]:
    """Load financial categories from YAML file."""
    yaml_path = Path(__file__).parent.parent.parent / "prompts" / "transcripts" / "financial_categories.yaml"
    
    try:
        with open(yaml_path, 'r') as f:
            categories_data = yaml.safe_load(f)
        
        # Convert to dict keyed by ID
        categories = {}
        for cat in categories_data:
            categories[cat['id']] = {
                'name': cat['name'],
                'description': cat['description']
            }
        return categories
    except Exception as e:
        logger = get_logger()
        logger.warning(f"Failed to load financial categories: {e}")
        # Return minimal fallback categories
        return {
            0: {'name': 'Non-Relevant', 'description': 'Non-relevant content'},
            1: {'name': 'Capital Markets', 'description': 'Investment banking and capital markets'},
            2: {'name': 'Trading', 'description': 'Trading and markets revenue'},
            # ... etc
        }


async def get_filter_diagnostics(combo: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get diagnostic counts for each filter to help debug why queries return 0 results.
    
    Returns dict with counts for:
    - Total records in table
    - Records matching bank_id
    - Records matching fiscal_year
    - Records matching quarter
    - Records matching all filters
    """
    logger = get_logger()
    execution_id = context.get("execution_id")
    
    diagnostics = {}
    
    try:
        async with get_connection() as conn:
            # Total records
            result = await conn.execute(text("SELECT COUNT(*) FROM aegis_transcripts"))
            diagnostics['total_records'] = result.scalar()

            # Records matching bank_id
            result = await conn.execute(
                text("SELECT COUNT(*) FROM aegis_transcripts WHERE institution_id = :bank_id_str OR institution_id::text = :bank_id_str"),
                {"bank_id_str": str(combo["bank_id"])}
            )
            diagnostics['matching_bank_id'] = result.scalar()

            # Records matching fiscal_year
            result = await conn.execute(
                text("SELECT COUNT(*) FROM aegis_transcripts WHERE fiscal_year = :fiscal_year"),
                {"fiscal_year": combo["fiscal_year"]}
            )
            diagnostics['matching_year'] = result.scalar()

            # Records matching quarter
            result = await conn.execute(
                text("SELECT COUNT(*) FROM aegis_transcripts WHERE fiscal_quarter = :quarter"),
                {"quarter": combo["quarter"]}
            )
            diagnostics['matching_quarter'] = result.scalar()

            # Records matching bank + year
            result = await conn.execute(
                text("""
                    SELECT COUNT(*) FROM aegis_transcripts
                    WHERE (institution_id = :bank_id_str OR institution_id::text = :bank_id_str)
                    AND fiscal_year = :fiscal_year
                """),
                {"bank_id_str": str(combo["bank_id"]), "fiscal_year": combo["fiscal_year"]}
            )
            diagnostics['matching_bank_and_year'] = result.scalar()

            # Records matching bank + quarter
            result = await conn.execute(
                text("""
                    SELECT COUNT(*) FROM aegis_transcripts
                    WHERE (institution_id = :bank_id_str OR institution_id::text = :bank_id_str)
                    AND fiscal_quarter = :quarter
                """),
                {"bank_id_str": str(combo["bank_id"]), "quarter": combo["quarter"]}
            )
            diagnostics['matching_bank_and_quarter'] = result.scalar()

            # Records matching year + quarter
            result = await conn.execute(
                text("""
                    SELECT COUNT(*) FROM aegis_transcripts
                    WHERE fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                """),
                {"fiscal_year": combo["fiscal_year"], "quarter": combo["quarter"]}
            )
            diagnostics['matching_year_and_quarter'] = result.scalar()

            # Records matching all filters
            result = await conn.execute(
                text("""
                    SELECT COUNT(*) FROM aegis_transcripts
                    WHERE (institution_id = :bank_id_str OR institution_id::text = :bank_id_str)
                    AND fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                """),
                {
                    "bank_id_str": str(combo["bank_id"]),
                    "fiscal_year": combo["fiscal_year"],
                    "quarter": combo["quarter"]
                }
            )
            diagnostics['matching_all_filters'] = result.scalar()

            # Get sample institution_ids if no match
            if diagnostics['matching_all_filters'] == 0:
                result = await conn.execute(
                    text("""
                        SELECT DISTINCT institution_id, company_name
                        FROM aegis_transcripts
                        WHERE fiscal_year = :fiscal_year
                        AND fiscal_quarter = :quarter
                        LIMIT 5
                    """),
                    {"fiscal_year": combo["fiscal_year"], "quarter": combo["quarter"]}
                )
                sample_banks = [(row[0], row[1]) for row in result]
                diagnostics['sample_available_banks'] = sample_banks
            
    except Exception as e:
        logger.error(f"Failed to get filter diagnostics: {e}")
        diagnostics['error'] = str(e)
    
    return diagnostics