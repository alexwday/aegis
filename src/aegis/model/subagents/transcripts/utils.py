"""
Utility functions for transcripts subagent.
"""

from pathlib import Path
import yaml
from typing import Dict, Any, List
from sqlalchemy import text

from ....utils.logging import get_logger
from ....connections.postgres_connector import get_connection


def load_transcripts_yaml(filename: str, compose_with_globals: bool = False) -> Dict[str, Any]:
    """
    Load a YAML file from the transcripts prompts directory.

    This is a custom loader for transcripts that doesn't depend on Aegis prompt_loader,
    allowing transcripts to use its own YAML structure independently.

    Args:
        filename: Name of YAML file (e.g., "method_selection.yaml")
        compose_with_globals: If True, compose prompts with global contexts (fiscal, project, etc.)

    Returns:
        Parsed YAML content as dictionary. If compose_with_globals=True, adds 'composed_prompt'
        field with global contexts prepended.

    Raises:
        FileNotFoundError: If YAML file doesn't exist
        yaml.YAMLError: If YAML file is malformed
    """
    # Path to transcripts prompts: model/prompts/transcripts/
    yaml_path = Path(__file__).parent.parent.parent / "prompts" / "transcripts" / filename

    if not yaml_path.exists():
        raise FileNotFoundError(f"Transcripts YAML file not found: {yaml_path}")

    with open(yaml_path, 'r', encoding='utf-8') as f:
        yaml_data = yaml.safe_load(f)

    # If composition requested, load global prompts and compose
    if compose_with_globals and "uses_global" in yaml_data:
        global_prompts = _load_global_prompts_for_transcripts(yaml_data["uses_global"])

        # Find the main prompt content (could be 'system_prompt', 'system_prompt_template', or 'content')
        main_content = None
        content_key = None
        for key in ['system_prompt', 'system_prompt_template', 'content']:
            if key in yaml_data:
                main_content = yaml_data[key]
                content_key = key
                break

        if main_content and global_prompts:
            # Compose: globals + main content
            composed = "\n\n---\n\n".join(global_prompts + [main_content])
            yaml_data['composed_prompt'] = composed
            yaml_data[f'original_{content_key}'] = main_content  # Save original

    return yaml_data


def _load_global_prompts_for_transcripts(uses_global: List[str]) -> List[str]:
    """
    Load global prompts from Aegis global folder for transcripts.

    Uses the existing Aegis global prompt infrastructure (fiscal.py, project.yaml, etc.)
    without depending on Aegis's prompt_loader module.

    Transcripts-specific order: project → fiscal → database → restrictions

    Args:
        uses_global: List of global prompt names (e.g., ['fiscal', 'project'])

    Returns:
        List of global prompt content strings in transcripts canonical order
    """
    # Transcripts-specific order: project FIRST, then fiscal, then restrictions at end
    TRANSCRIPTS_GLOBAL_ORDER = ["project", "fiscal", "database", "restrictions"]
    prompt_parts = []

    if not uses_global:
        return prompt_parts

    # Path to global prompts
    global_prompts_path = Path(__file__).parent.parent.parent / "prompts" / "global"

    for global_name in TRANSCRIPTS_GLOBAL_ORDER:
        if global_name not in uses_global:
            continue

        if global_name == "fiscal":
            # Load fiscal dynamically
            try:
                import importlib.util
                fiscal_path = global_prompts_path / "fiscal.py"
                spec = importlib.util.spec_from_file_location("fiscal", fiscal_path)
                fiscal_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(fiscal_module)
                prompt_parts.append(fiscal_module.get_fiscal_statement())
            except Exception as e:
                logger = get_logger()
                logger.warning(f"Failed to load fiscal global prompt: {e}")
        else:
            # Load from YAML
            try:
                yaml_file = global_prompts_path / f"{global_name}.yaml"
                if yaml_file.exists():
                    with open(yaml_file, 'r', encoding='utf-8') as f:
                        global_data = yaml.safe_load(f)
                        if "content" in global_data:
                            prompt_parts.append(global_data["content"].strip())
            except Exception as e:
                logger = get_logger()
                logger.warning(f"Failed to load {global_name} global prompt: {e}")

    return prompt_parts


async def load_financial_categories() -> Dict[int, Dict[str, str]]:
    """
    Load financial categories from YAML file using custom loader.

    Returns:
        Dictionary mapping category IDs to their names and descriptions

    Raises:
        RuntimeError: If categories cannot be loaded
    """
    try:
        categories_yaml = load_transcripts_yaml("financial_categories.yaml")

        # Extract the categories list from the YAML structure
        if "categories" not in categories_yaml:
            raise KeyError("'categories' key not found in financial_categories.yaml")

        # Convert to dict keyed by ID
        categories = {}
        for cat in categories_yaml["categories"]:
            categories[cat["id"]] = {"name": cat["name"], "description": cat["description"]}
        return categories
    except Exception as e:
        logger = get_logger()
        logger.error(f"Failed to load financial categories: {e}")
        raise RuntimeError(
            "Critical error: Cannot load financial categories from YAML. "
            "Method selection will not work without this file."
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