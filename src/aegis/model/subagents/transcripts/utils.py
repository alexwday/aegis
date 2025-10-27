"""
Utility functions for transcripts subagent.
"""

from pathlib import Path
import yaml
from typing import Dict, Any, List
from sqlalchemy import text

from ....utils.logging import get_logger
from ....utils.sql_prompt import prompt_manager
from ....connections.postgres_connector import get_connection


def load_transcripts_yaml(filename: str, compose_with_globals: bool = False) -> Dict[str, Any]:
    """
    Load a prompt from the SQL database for transcripts subagent.

    Migrated from YAML to SQL database. Loads from prompts table with layer='transcripts'.

    Args:
        filename: Name of prompt (e.g., "method_selection", "reranking")
        compose_with_globals: If True, compose prompts with global contexts (fiscal, project, etc.)

    Returns:
        Parsed prompt content as dictionary. If compose_with_globals=True, adds 'composed_prompt'
        field with global contexts prepended.

    Raises:
        FileNotFoundError: If prompt doesn't exist in database
    """
    logger = get_logger()

    # Use prompt name directly (no .yaml extension expected)
    prompt_name = filename

    try:
        # Load from SQL database
        prompt_data = prompt_manager.get_latest_prompt(
            model="aegis",
            layer="transcripts",
            name=prompt_name,
            system_prompt=False  # Get full record
        )
    except Exception as e:
        raise FileNotFoundError(f"Transcripts prompt not found in database: {prompt_name}") from e

    # If composition requested, load global prompts and compose
    if compose_with_globals and prompt_data.get("uses_global"):
        global_prompts = _load_global_prompts_for_transcripts(prompt_data["uses_global"])

        # Find the main prompt content
        main_content = None
        content_key = None
        for key in ['system_prompt', 'system_prompt_template', 'content']:
            if key in prompt_data:
                main_content = prompt_data[key]
                content_key = key
                break

        if main_content and global_prompts:
            # Compose: globals + main content
            composed = "\n\n---\n\n".join(global_prompts + [main_content])
            prompt_data['composed_prompt'] = composed
            prompt_data[f'original_{content_key}'] = main_content  # Save original

    return prompt_data


def _load_global_prompts_for_transcripts(uses_global: List[str]) -> List[str]:
    """
    Load global prompts from SQL database for transcripts.

    Migrated from YAML to SQL database. Loads from prompts table with layer='global'.

    Transcripts-specific order: project → fiscal → database → restrictions

    Args:
        uses_global: List of global prompt names (e.g., ['fiscal', 'project'])

    Returns:
        List of global prompt content strings in transcripts canonical order
    """
    logger = get_logger()

    # Transcripts-specific order: project FIRST, then fiscal, then restrictions at end
    TRANSCRIPTS_GLOBAL_ORDER = ["project", "fiscal", "database", "restrictions"]
    prompt_parts = []

    if not uses_global:
        return prompt_parts

    for global_name in TRANSCRIPTS_GLOBAL_ORDER:
        if global_name not in uses_global:
            continue

        if global_name == "fiscal":
            # Load fiscal dynamically (still from fiscal.py)
            try:
                from ....utils.prompt_loader import _load_fiscal_prompt
                prompt_parts.append(_load_fiscal_prompt())
            except Exception as e:
                logger.warning(f"Failed to load fiscal global prompt: {e}")
        elif global_name == "database":
            # Database uses filtered prompt
            try:
                from ....utils.database_filter import get_database_prompt
                database_prompt = get_database_prompt(None)  # No filtering for transcripts
                prompt_parts.append(database_prompt)
            except Exception as e:
                logger.warning(f"Failed to load database global prompt: {e}")
        else:
            # Load from SQL database
            try:
                global_data = prompt_manager.get_latest_prompt(
                    model="aegis",
                    layer="global",
                    name=global_name,
                    system_prompt=False
                )
                if global_data.get("system_prompt"):
                    prompt_parts.append(global_data["system_prompt"].strip())
            except Exception as e:
                logger.warning(f"Failed to load {global_name} global prompt from database: {e}")

    return prompt_parts


async def load_financial_categories() -> Dict[int, Dict[str, str]]:
    """
    Load financial categories from SQL database.

    Migrated from YAML to SQL database. Loads from prompts table with layer='transcripts', name='financial_categories'.

    Returns:
        Dictionary mapping category IDs to their names and descriptions

    Raises:
        RuntimeError: If categories cannot be loaded
    """
    logger = get_logger()

    try:
        # Load from SQL database
        categories_data = prompt_manager.get_latest_prompt(
            model="aegis",
            layer="transcripts",
            name="financial_categories",
            system_prompt=False
        )

        # Extract the categories list from the database structure
        # Could be in system_prompt (as JSON), user_prompt, or a custom field
        categories_list = None

        # Try to find categories in various fields
        for field in ['system_prompt', 'user_prompt', 'description']:
            if field in categories_data and categories_data[field]:
                try:
                    import json
                    # Try to parse as JSON
                    parsed = json.loads(categories_data[field])
                    if isinstance(parsed, dict) and "categories" in parsed:
                        categories_list = parsed["categories"]
                        break
                    elif isinstance(parsed, list):
                        categories_list = parsed
                        break
                except (json.JSONDecodeError, TypeError):
                    # Not JSON, skip
                    pass

        if not categories_list:
            raise KeyError("'categories' data not found in database record")

        # Convert to dict keyed by ID
        categories = {}
        for cat in categories_list:
            categories[cat["id"]] = {"name": cat["name"], "description": cat["description"]}
        return categories
    except Exception as e:
        logger.error(f"Failed to load financial categories from database: {e}")
        raise RuntimeError(
            "Critical error: Cannot load financial categories from database. "
            "Method selection will not work without this data."
        ) from e


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