"""
Key Themes ETL - Extract and Group Earnings Call Themes

This ETL processes earnings call Q&A sessions to:
1. Load all Q&A blocks into an index
2. Process each independently to extract themes (parallelizable)
3. Make ONE comprehensive grouping decision with full visibility
4. Apply grouping programmatically
5. Generate formatted document

Usage:
    python -m aegis.etls.key_themes.main --bank "Royal Bank of Canada" --year 2024 --quarter Q3
"""

import argparse
import asyncio
import hashlib
import json
import uuid
import os
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd
from sqlalchemy import text
from docx import Document
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH

from aegis.etls.key_themes.document_converter import (
    HTMLToDocx,
    add_page_numbers_with_footer,
    add_theme_header_with_background,
    get_standard_report_metadata,
)
from aegis.etls.key_themes.transcript_utils import retrieve_full_section
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.llm_connector import complete, complete_with_tools
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from aegis.utils.prompt_loader import load_prompt_from_db
from aegis.utils.sql_prompt import postgresql_prompts
from aegis.utils.settings import config

import yaml

setup_logging()
logger = get_logger()


class ETLConfig:
    """
    ETL configuration loader that reads YAML configs and resolves model references.

    This class loads ETL-specific configuration from YAML files and provides
    easy access to configuration values with automatic model tier resolution.
    """

    def __init__(self, config_path: str):
        """Initialize the ETL configuration loader."""
        self.config_path = config_path
        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def get_model(self, model_key: str) -> str:
        """
        Get the actual model name for a given model key.

        Resolves model tier references (small/medium/large) to actual model names
        from the global settings configuration.
        """
        if "models" not in self._config or model_key not in self._config["models"]:
            raise KeyError(f"Model key '{model_key}' not found in configuration")

        tier = self._config["models"][model_key].get("tier")
        if not tier:
            raise ValueError(f"No tier specified for model '{model_key}'")

        # Resolve tier to actual model from global config
        tier_map = {
            "small": config.llm.small.model,
            "medium": config.llm.medium.model,
            "large": config.llm.large.model,
        }

        if tier not in tier_map:
            raise ValueError(
                f"Invalid tier '{tier}' for model '{model_key}'. "
                f"Valid tiers: {list(tier_map.keys())}"
            )

        return tier_map[tier]

    @property
    def temperature(self) -> float:
        """Get the LLM temperature parameter."""
        return self._config.get("llm", {}).get("temperature", 0.1)

    @property
    def max_tokens(self) -> int:
        """Get the LLM max_tokens parameter."""
        return self._config.get("llm", {}).get("max_tokens", 32768)


etl_config = ETLConfig(os.path.join(os.path.dirname(__file__), "config", "config.yaml"))

_MONITORED_INSTITUTIONS = None


def _load_monitored_institutions() -> Dict[int, Dict[str, Any]]:
    """
    Load and cache monitored institutions configuration.

    Returns:
        Dictionary mapping bank_id to institution details (id, name, symbol, type, path_safe_name)
    """
    global _MONITORED_INSTITUTIONS
    if _MONITORED_INSTITUTIONS is None:
        config_path = os.path.join(
            os.path.dirname(__file__), "config", "monitored_institutions.yaml"
        )
        with open(config_path, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f)

        # Build dict with bank_id as key, adding symbol from YAML key
        _MONITORED_INSTITUTIONS = {}
        for key, value in yaml_data.items():
            symbol = key.split("-")[0]  # Extract symbol from "RY-CA" -> "RY"
            _MONITORED_INSTITUTIONS[value["id"]] = {**value, "symbol": symbol}
    return _MONITORED_INSTITUTIONS


def get_bank_info_from_config(bank_identifier: str) -> Dict[str, Any]:
    """
    Look up bank from monitored institutions configuration file.

    Args:
        bank_identifier: Bank ID (as string/int), symbol (e.g., "RY"), or name

    Returns:
        Dictionary with bank_id, bank_name, bank_symbol, bank_type

    Raises:
        ValueError: If bank not found in monitored institutions
    """
    institutions = _load_monitored_institutions()

    # Try lookup by ID
    if bank_identifier.isdigit():
        bank_id = int(bank_identifier)
        if bank_id in institutions:
            inst = institutions[bank_id]
            return {
                "bank_id": inst["id"],
                "bank_name": inst["name"],
                "bank_symbol": inst["symbol"],
                "bank_type": inst["type"],
            }

    # Try lookup by symbol or name
    bank_identifier_upper = bank_identifier.upper()
    bank_identifier_lower = bank_identifier.lower()

    for inst in institutions.values():
        # Match by symbol (case-insensitive)
        if inst["symbol"].upper() == bank_identifier_upper:
            return {
                "bank_id": inst["id"],
                "bank_name": inst["name"],
                "bank_symbol": inst["symbol"],
                "bank_type": inst["type"],
            }

        # Match by name (case-insensitive, partial match)
        if bank_identifier_lower in inst["name"].lower():
            return {
                "bank_id": inst["id"],
                "bank_name": inst["name"],
                "bank_symbol": inst["symbol"],
                "bank_type": inst["type"],
            }

    # Build helpful error message with available banks
    available = [f"{inst['symbol']} ({inst['name']})" for inst in institutions.values()]
    raise ValueError(
        f"Bank '{bank_identifier}' not found in monitored institutions.\n"
        f"Available banks: {', '.join(sorted(available))}"
    )


async def verify_and_get_availability(
    bank_id: int, bank_name: str, fiscal_year: int, quarter: str
) -> None:
    """
    Verify transcript data is available for the specified bank and period.

    Raises ValueError with available periods if data not found.

    Args:
        bank_id: Bank ID
        bank_name: Bank name (for error messages)
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")

    Raises:
        ValueError: If transcript data not available (includes available periods)
    """
    async with get_connection() as conn:
        result = await conn.execute(
            text(
                """
                SELECT database_names
                FROM aegis_data_availability
                WHERE bank_id = :bank_id
                  AND fiscal_year = :fiscal_year
                  AND quarter = :quarter
                """
            ),
            {"bank_id": bank_id, "fiscal_year": fiscal_year, "quarter": quarter},
        )
        row = result.fetchone()

        if row and row[0] and "transcripts" in row[0]:
            return

        raise ValueError(f"No transcript data available for {bank_name} {quarter} {fiscal_year}")


def format_categories_for_prompt(categories: List[Dict[str, Any]]) -> str:
    """
    Format category dictionaries into standardized XML format for prompt injection.

    This is the standardized formatting function used across all ETLs (Call Summary,
    Key Themes, CM Readthrough) to ensure consistent category presentation to LLMs.

    Args:
        categories: List of category dicts with standardized 6-column format

    Returns:
        Formatted XML string with category information
    """
    formatted_sections = []

    for cat in categories:
        # Map transcript_sections to human-readable description
        section_desc = {
            "MD": "Management Discussion section only",
            "QA": "Q&A section only",
            "ALL": "Both Management Discussion and Q&A sections",
        }.get(cat.get("transcript_sections", "ALL"), "ALL sections")

        section = "<category>\n"
        section += f"<name>{cat['category_name']}</name>\n"
        section += f"<section>{section_desc}</section>\n"
        section += f"<description>{cat['category_description']}</description>\n"

        # Collect non-empty examples
        examples = []
        for i in range(1, 4):
            example_key = f"example_{i}"
            if cat.get(example_key) and cat[example_key].strip():
                examples.append(cat[example_key])

        if examples:
            section += "<examples>\n"
            for example in examples:
                section += f"  <example>{example}</example>\n"
            section += "</examples>\n"

        section += "</category>"
        formatted_sections.append(section)

    return "\n\n".join(formatted_sections)


def load_categories_from_xlsx(execution_id: str) -> List[Dict[str, Any]]:
    """
    Load categories from the key themes categories XLSX file.

    Args:
        execution_id: Execution ID for logging

    Returns:
        List of dictionaries with transcript_sections, category_name, category_description,
        example_1, example_2, example_3
    """
    file_name = "key_themes_categories.xlsx"

    current_dir = os.path.dirname(os.path.abspath(__file__))
    xlsx_path = os.path.join(current_dir, "config", "categories", file_name)

    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(f"Categories file not found: {xlsx_path}")

    try:
        df = pd.read_excel(xlsx_path, sheet_name=0)

        # Required columns for standard format
        required_columns = ["transcript_sections", "category_name", "category_description"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in {file_name}: {missing_columns}")

        # Optional example columns
        optional_columns = ["example_1", "example_2", "example_3"]
        for col in optional_columns:
            if col not in df.columns:
                df[col] = ""  # Add empty column if not present

        # Convert to list of dicts, ensuring all 6 columns are present
        categories = []
        for _, row in df.iterrows():
            category = {
                "transcript_sections": str(row["transcript_sections"]).strip(),
                "category_name": str(row["category_name"]).strip(),
                "category_description": str(row["category_description"]).strip(),
                "example_1": str(row["example_1"]).strip() if pd.notna(row["example_1"]) else "",
                "example_2": str(row["example_2"]).strip() if pd.notna(row["example_2"]) else "",
                "example_3": str(row["example_3"]).strip() if pd.notna(row["example_3"]) else "",
            }
            categories.append(category)

        if not categories:
            raise ValueError(f"No categories in {file_name}")

        logger.info(
            "etl.key_themes.categories_loaded",
            execution_id=execution_id,
            file_name=file_name,
            num_categories=len(categories),
        )
        return categories

    except Exception as e:
        logger.error(
            "etl.key_themes.categories_load_error",
            execution_id=execution_id,
            xlsx_path=xlsx_path,
            error=str(e),
        )
        raise ValueError(f"Failed to load categories from {xlsx_path}: {str(e)}") from e


class QABlock:
    """Represents a single Q&A block with its extracted information."""

    def __init__(self, qa_id: str, position: int, original_content: str):
        self.qa_id = qa_id
        self.position = position
        self.original_content = original_content
        self.category_name = None  # Changed from theme_title to category_name
        self.summary = None
        self.formatted_content = None
        self.assigned_group = None
        self.is_valid = True
        self.completion_status = "complete"  # "complete", "question_only", or "answer_only"


class ThemeGroup:
    """Represents a group of related Q&A blocks under a unified theme."""

    def __init__(self, group_title: str, qa_ids: List[str], rationale: str = ""):
        self.group_title = group_title
        self.qa_ids = qa_ids
        self.rationale = rationale
        self.qa_blocks = []


async def load_qa_blocks(
    bank_name: str, fiscal_year: int, quarter: str, context: Dict[str, Any]
) -> Dict[str, QABlock]:
    """
    Step 1: Load all Q&A blocks and create an index.
    Uses retrieve_full_section from transcript_utils for consistency.

    Returns:
        Dictionary indexed by qa_id containing QABlock objects
    """
    logger = get_logger()

    bank_info = get_bank_info_from_config(bank_name)

    combo = {
        "bank_name": bank_info["bank_name"],
        "bank_id": bank_info["bank_id"],
        "bank_symbol": bank_info["bank_symbol"],
        "fiscal_year": fiscal_year,
        "quarter": quarter,
    }

    # Use retrieve_full_section to get Q&A chunks
    chunks = await retrieve_full_section(combo=combo, sections="QA", context=context)

    logger.debug(
        "load_qa_index.retrieved",
        bank_name=bank_name,
        quarter=quarter,
        fiscal_year=fiscal_year,
        num_chunks=len(chunks),
    )

    if not chunks:
        return {}

    # Group chunks by qa_group_id
    qa_groups = {}
    for chunk in chunks:
        qa_group_id = chunk.get("qa_group_id")
        if qa_group_id is not None:
            if qa_group_id not in qa_groups:
                qa_groups[qa_group_id] = []
            qa_groups[qa_group_id].append(chunk)

    # Build QABlock index
    qa_index = {}
    for qa_group_id, group_chunks in qa_groups.items():
        # Concatenate content from all chunks in this Q&A group
        qa_content = "\n".join(
            [chunk.get("content", "") for chunk in group_chunks if chunk.get("content")]
        )

        if qa_content:
            qa_id = f"qa_{qa_group_id}"
            qa_index[qa_id] = QABlock(qa_id, qa_group_id, qa_content)

    return qa_index


async def classify_qa_block(
    qa_block: QABlock,
    categories: List[Dict[str, str]],
    previous_classifications: List[Dict[str, str]],
    context: Dict[str, Any],
):
    """
    Step 1A: Validate and classify a single Q&A block into predefined category.
    Uses cumulative context from previous classifications for consistency.

    Args:
        qa_block: Q&A block to classify
        categories: List of predefined categories from xlsx
        previous_classifications: List of prior classifications for context
        context: Execution context
    """
    execution_id = context.get("execution_id")

    logger.info(
        "category_classification.processing_qa",
        execution_id=execution_id,
        qa_id=qa_block.qa_id,
        content_length=len(qa_block.original_content),
        num_previous_classifications=len(previous_classifications),
    )

    prompt_data = load_prompt_from_db(
        layer="key_themes_etl",
        name="theme_extraction",
        compose_with_globals=False,
        available_databases=None,
        execution_id=execution_id,
    )

    # Format categories using standardized XML format
    categories_str = format_categories_for_prompt(categories)

    # Format previous classifications
    if previous_classifications:
        prev_class_str = "\n".join(
            [
                f"{pc['qa_id']}: {pc['category_name']} - {pc['summary'][:100]}..."
                for pc in previous_classifications
            ]
        )
    else:
        prev_class_str = "No previous classifications yet (this is the first Q&A)."

    # In-place modification of prompt (matches Call Summary pattern)
    prompt_data["system_prompt"] = prompt_data["system_prompt"].format(
        bank_name=context.get("bank_name", "Bank"),
        quarter=context.get("quarter", "Q"),
        fiscal_year=context.get("fiscal_year", "Year"),
        categories_list=categories_str,
        num_categories=len(categories),
        previous_classifications=prev_class_str,
    )

    messages = [
        {"role": "system", "content": prompt_data["system_prompt"]},
        {
            "role": "user",
            "content": f"Validate and classify this Q&A session:\n\n{qa_block.original_content}",
        },
    ]

    max_retries = 3
    result = None

    for attempt in range(max_retries):
        try:
            response = await complete_with_tools(
                messages=messages,
                tools=[prompt_data["tool_definition"]],
                context=context,
                llm_params={
                    "model": etl_config.get_model("theme_extraction"),
                    "temperature": etl_config.temperature,
                    "max_tokens": etl_config.max_tokens,
                },
            )

            if response:
                tool_calls = (
                    response.get("choices", [{}])[0].get("message", {}).get("tool_calls", [])
                )
                if tool_calls:
                    result = json.loads(tool_calls[0]["function"]["arguments"])
                    break

        except Exception:
            if attempt < max_retries - 1:
                await asyncio.sleep(2**attempt)
                continue
            raise

    if result:
        is_valid = result.get("is_valid", True)

        if not is_valid:
            logger.warning(
                "category_classification.rejected",
                execution_id=execution_id,
                qa_id=qa_block.qa_id,
                rejection_reason=result.get("rejection_reason", "No reason provided"),
            )
            qa_block.is_valid = False
            qa_block.category_name = None
            qa_block.summary = None
            qa_block.completion_status = ""
        else:
            category_name = result.get("category_name", "")
            summary = result.get("summary", "")
            completion_status = result.get("completion_status", "complete")

            logger.info(
                "category_classification.accepted",
                execution_id=execution_id,
                qa_id=qa_block.qa_id,
                category_name=category_name,
                completion_status=completion_status,
                summary_preview=summary[:100],
            )

            qa_block.is_valid = True
            qa_block.category_name = category_name
            qa_block.summary = summary
            qa_block.completion_status = completion_status


async def format_qa_html(qa_block: QABlock, context: Dict[str, Any]):
    """
    Step 2B: Format Q&A block with HTML tags for emphasis.
    Only formats valid Q&A blocks that passed validation.
    """

    if not qa_block.is_valid:
        qa_block.formatted_content = None
        return

    execution_id = context.get("execution_id")
    prompt_data = load_prompt_from_db(
        layer="key_themes_etl",
        name="html_formatting",
        compose_with_globals=False,
        available_databases=None,
        execution_id=execution_id,
    )

    system_prompt = prompt_data["system_prompt"].format(
        bank_name=context.get("bank_name", "Bank"),
        quarter=context.get("quarter", "Q"),
        fiscal_year=context.get("fiscal_year", "Year"),
    )

    # Add completion status context for incomplete Q&As
    user_content = f"Format this Q&A exchange with HTML tags for emphasis:\n\n"
    if qa_block.completion_status == "question_only":
        user_content += "[NOTE: This block contains only the analyst question. Executive response may be in a separate block.]\n\n"
    elif qa_block.completion_status == "answer_only":
        user_content += "[NOTE: This block contains only the executive response. Analyst question may be in a separate block.]\n\n"
    user_content += qa_block.original_content

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = await complete(
                messages,
                context,
                {
                    "model": etl_config.get_model("html_formatting"),
                    "temperature": etl_config.temperature,
                    "max_tokens": etl_config.max_tokens,
                },
            )

            if isinstance(response, dict):
                qa_block.formatted_content = (
                    response.get("choices", [{}])[0].get("message", {}).get("content", "")
                )
            else:
                qa_block.formatted_content = str(response)
            break

        except Exception:
            if attempt < max_retries - 1:
                await asyncio.sleep(2**attempt)
            else:
                qa_block.formatted_content = qa_block.original_content


async def classify_all_qa_blocks_sequential(
    qa_index: Dict[str, QABlock],
    categories: List[Dict[str, str]],
    context: Dict[str, Any],
):
    """
    Step 1: Classify all Q&A blocks sequentially with cumulative context.

    Args:
        qa_index: Dictionary of QA blocks indexed by qa_id
        categories: List of predefined categories from xlsx
        context: Execution context
    """
    previous_classifications = []

    # Process in order by position
    sorted_qa_blocks = sorted(qa_index.values(), key=lambda x: x.position)

    for qa_block in sorted_qa_blocks:
        await classify_qa_block(qa_block, categories, previous_classifications, context)

        # Add to cumulative context if valid
        if qa_block.is_valid:
            previous_classifications.append(
                {
                    "qa_id": qa_block.qa_id,
                    "category_name": qa_block.category_name,
                    "summary": qa_block.summary,
                }
            )

    logger.info(
        "classification.completed",
        execution_id=context.get("execution_id"),
        total_classified=len(previous_classifications),
        total_invalid=len([qa for qa in qa_index.values() if not qa.is_valid]),
    )


async def format_all_qa_blocks_parallel(qa_index: Dict[str, QABlock], context: Dict[str, Any]):
    """
    Step 2: Format all valid Q&A blocks in parallel with HTML tags.

    Args:
        qa_index: Dictionary of QA blocks indexed by qa_id
        context: Execution context
    """
    # Only format valid Q&As
    valid_qa_blocks = [qa for qa in qa_index.values() if qa.is_valid]

    if not valid_qa_blocks:
        return

    # Parallel formatting
    tasks = [format_qa_html(qa_block, context) for qa_block in valid_qa_blocks]
    await asyncio.gather(*tasks)

    logger.info(
        "formatting.completed",
        execution_id=context.get("execution_id"),
        total_formatted=len(valid_qa_blocks),
    )


async def determine_comprehensive_grouping(
    qa_index: Dict[str, QABlock], categories: List[Dict[str, str]], context: Dict[str, Any]
) -> List[ThemeGroup]:
    """
    Step 3: Make ONE comprehensive grouping decision for all themes.
    Only processes valid Q&A blocks.

    Args:
        qa_index: Dictionary of QA blocks indexed by qa_id
        categories: List of category definitions from xlsx
        context: Execution context with auth, ssl, bank info
    """

    valid_qa_blocks = {qa_id: qa_block for qa_id, qa_block in qa_index.items() if qa_block.is_valid}

    if not valid_qa_blocks:
        return []

    execution_id = context.get("execution_id")
    prompt_data = load_prompt_from_db(
        layer="key_themes_etl",
        name="theme_grouping",
        compose_with_globals=False,
        available_databases=None,
        execution_id=execution_id,
    )

    # CRITICAL: Log what we got from the database
    logger.info(
        "regrouping.prompt_loaded",
        execution_id=execution_id,
        prompt_keys=list(prompt_data.keys()),
        has_system_prompt=bool(prompt_data.get("system_prompt")),
        has_tool_definition=bool(prompt_data.get("tool_definition")),
        tool_definition_type=type(prompt_data.get("tool_definition")).__name__,
    )

    qa_blocks_info = []
    for qa_id, qa_block in sorted(valid_qa_blocks.items(), key=lambda x: x[1].position):
        qa_blocks_info.append(
            f"ID: {qa_id}\n"
            f"Category: {qa_block.category_name}\n"
            f"Summary: {qa_block.summary}\n"
        )

    qa_blocks_str = "\n\n".join(qa_blocks_info)

    # Format categories using standardized XML format
    categories_str = format_categories_for_prompt(categories)

    system_prompt = prompt_data["system_prompt"].format(
        bank_name=context.get("bank_name", "Bank"),
        bank_symbol=context.get("bank_symbol", "BANK"),
        quarter=context.get("quarter", "Q"),
        fiscal_year=context.get("fiscal_year", "Year"),
        total_qa_blocks=len(valid_qa_blocks),
        qa_blocks_info=qa_blocks_str,
        categories_list=categories_str,
        num_categories=len(categories),
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": "Review category assignments, regroup if needed, and create final titles.",
        },
    ]

    # Debug: Log the tool definition being sent
    logger.debug(
        "regrouping.tool_definition",
        execution_id=execution_id,
        tool_def_type=type(prompt_data.get("tool_definition")).__name__,
        tool_def_keys=list(prompt_data.get("tool_definition", {}).keys()) if isinstance(prompt_data.get("tool_definition"), dict) else None,
        tool_def_preview=str(prompt_data.get("tool_definition"))[:500],
    )

    # CRITICAL: Check if tool_definition exists before proceeding
    if "tool_definition" not in prompt_data or prompt_data["tool_definition"] is None:
        logger.error(
            "regrouping.missing_tool_definition",
            execution_id=execution_id,
            prompt_data_keys=list(prompt_data.keys()),
            message="tool_definition is missing or None in prompt_data - cannot proceed with LLM call"
        )
        raise KeyError(
            f"theme_grouping prompt missing 'tool_definition'. "
            f"Available keys: {list(prompt_data.keys())}"
        )

    logger.info(
        "regrouping.llm_request",
        execution_id=execution_id,
        num_qa_blocks=len(valid_qa_blocks),
        system_prompt_length=len(system_prompt),
        tool_definition_exists=True,
    )

    max_retries = 3
    result = None

    for attempt in range(max_retries):
        try:
            # CRITICAL: Log right before making LLM call
            logger.info(
                "regrouping.about_to_call_llm",
                execution_id=execution_id,
                attempt=attempt + 1,
                messages_count=len(messages),
                tools_count=1,
            )

            response = await complete_with_tools(
                messages=messages,
                tools=[prompt_data["tool_definition"]],
                context=context,
                llm_params={
                    "model": etl_config.get_model("theme_grouping"),
                    "temperature": etl_config.temperature,
                    "max_tokens": etl_config.max_tokens,
                },
            )

            # CRITICAL: Log that LLM call returned
            logger.info(
                "regrouping.llm_returned",
                execution_id=execution_id,
                response_received=True,
            )

            # Debug: Log raw response structure
            logger.debug(
                "regrouping.llm_response",
                execution_id=execution_id,
                response_type=type(response).__name__,
                response_keys=list(response.keys()) if isinstance(response, dict) else None,
                has_choices=bool(response.get("choices")) if isinstance(response, dict) else False,
            )

            if response:
                tool_calls = (
                    response.get("choices", [{}])[0].get("message", {}).get("tool_calls", [])
                )

                # Debug: Log tool_calls structure
                logger.debug(
                    "regrouping.tool_calls_structure",
                    execution_id=execution_id,
                    has_tool_calls=bool(tool_calls),
                    num_tool_calls=len(tool_calls) if tool_calls else 0,
                    tool_call_preview=str(tool_calls)[:300] if tool_calls else None,
                )

                if tool_calls:
                    # Debug: Log the function name that was called
                    function_name = tool_calls[0].get("function", {}).get("name")
                    logger.debug(
                        "regrouping.function_called",
                        execution_id=execution_id,
                        function_name=function_name,
                    )

                    try:
                        # Get the arguments string and clean it
                        arguments_str = tool_calls[0]["function"]["arguments"]

                        # If it's already a dict, use it directly
                        if isinstance(arguments_str, dict):
                            result = arguments_str
                            logger.debug(
                                "regrouping.arguments_already_dict",
                                execution_id=execution_id,
                            )
                        else:
                            # Log what we received for debugging
                            logger.debug(
                                "regrouping.raw_arguments",
                                execution_id=execution_id,
                                arg_type=type(arguments_str).__name__,
                                arg_length=len(str(arguments_str)),
                                arg_preview=repr(str(arguments_str)[:200]),
                            )

                            # Clean the string by stripping whitespace and common issues
                            arguments_str = str(arguments_str).strip()

                            # Remove any markdown code block markers if present
                            if arguments_str.startswith("```"):
                                # Remove ```json or ``` at start
                                arguments_str = arguments_str.split("\n", 1)[1] if "\n" in arguments_str else arguments_str[3:]
                            if arguments_str.endswith("```"):
                                arguments_str = arguments_str.rsplit("\n", 1)[0] if "\n" in arguments_str else arguments_str[:-3]

                            arguments_str = arguments_str.strip()
                            result = json.loads(arguments_str)

                        # Validate the parsed result has the expected structure
                        if not isinstance(result, dict):
                            logger.error(
                                "regrouping.invalid_result_type",
                                execution_id=execution_id,
                                result_type=type(result).__name__,
                                result_repr=repr(result)[:200],
                            )
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2**attempt)
                                continue
                            raise ValueError(
                                f"LLM returned invalid result type: {type(result).__name__}"
                            )

                        if "theme_groups" not in result:
                            logger.error(
                                "regrouping.missing_theme_groups",
                                execution_id=execution_id,
                                result_keys=list(result.keys()),
                                result_repr=repr(result)[:300],
                            )
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2**attempt)
                                continue
                            raise KeyError(
                                f"LLM response missing 'theme_groups' key. "
                                f"Got keys: {list(result.keys())}"
                            )

                        logger.info(
                            "regrouping.parsed_result",
                            execution_id=execution_id,
                            num_groups=len(result.get("theme_groups", [])),
                        )

                        break
                    except json.JSONDecodeError as e:
                        logger.error(
                            "regrouping.json_decode_error",
                            execution_id=execution_id,
                            error=str(e),
                            error_position=e.pos if hasattr(e, 'pos') else None,
                            arguments_type=type(tool_calls[0]["function"]["arguments"]).__name__,
                            arguments_repr=repr(str(tool_calls[0]["function"]["arguments"])[:300]),
                        )
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2**attempt)
                            continue
                        raise

        except Exception as e:
            logger.error(
                "regrouping.unexpected_error",
                execution_id=execution_id,
                error=str(e),
                error_type=type(e).__name__,
                attempt=attempt + 1,
                max_retries=max_retries,
            )
            if attempt < max_retries - 1:
                await asyncio.sleep(2**attempt)
                continue
            return None

    if result and "theme_groups" in result:
        theme_groups = []
        for group_data in result["theme_groups"]:
            group = ThemeGroup(
                group_title=group_data["group_title"],
                qa_ids=group_data["qa_ids"],
                rationale=group_data.get("rationale", "Regrouped by category"),
            )
            theme_groups.append(group)

        return theme_groups

    # Fallback: Use initial category assignments
    logger.warning(
        "regrouping.fallback",
        execution_id=execution_id,
        message="Using initial category assignments as fallback",
    )

    category_groups = {}
    for qa_id, qa_block in valid_qa_blocks.items():
        category = qa_block.category_name
        if category not in category_groups:
            category_groups[category] = []
        category_groups[category].append(qa_id)

    theme_groups = []
    for category, qa_ids in category_groups.items():
        group = ThemeGroup(
            group_title=f"{category}",
            qa_ids=qa_ids,
            rationale="Fallback - grouped by initial classification",
        )
        theme_groups.append(group)

    return theme_groups


def apply_grouping_to_index(qa_index: Dict[str, QABlock], theme_groups: List[ThemeGroup]):
    """
    Step 4: Apply grouping decisions to the Q&A index.
    """

    for qa_block in qa_index.values():
        qa_block.assigned_group = None

    for group in theme_groups:
        for qa_id in group.qa_ids:
            if qa_id in qa_index:
                qa_block = qa_index[qa_id]
                qa_block.assigned_group = group
                group.qa_blocks.append(qa_block)


def create_document(
    theme_groups: List[ThemeGroup], bank_name: str, fiscal_year: int, quarter: str, output_path: str
):
    """
    Step 5: Create Word document with grouped themes matching call summary style.
    """
    doc = Document()

    bank_symbol = bank_name.split()[0] if bank_name else "RBC"
    if bank_name == "Royal Bank of Canada":
        bank_symbol = "RY"

    sections = doc.sections
    for section in sections:
        section.top_margin = Inches(0.5)
        section.bottom_margin = Inches(0.6)
        section.left_margin = Inches(0.6)
        section.right_margin = Inches(0.5)
        section.gutter = Inches(0)

    add_page_numbers_with_footer(doc, bank_symbol, quarter, fiscal_year)

    etl_dir = os.path.dirname(os.path.abspath(__file__))
    banner_path = None

    config_dir = os.path.join(etl_dir, "config")
    for ext in ["jpg", "jpeg", "png"]:
        potential_banner = os.path.join(config_dir, f"banner.{ext}")
        if os.path.exists(potential_banner):
            banner_path = potential_banner
            break

    if not banner_path:
        call_summary_config_dir = os.path.join(os.path.dirname(etl_dir), "call_summary", "config")
        for ext in ["jpg", "jpeg", "png"]:
            potential_banner = os.path.join(call_summary_config_dir, f"banner.{ext}")
            if os.path.exists(potential_banner):
                banner_path = potential_banner
                break

    if banner_path:
        try:

            doc.add_picture(banner_path, width=Inches(7.4))
            last_paragraph = doc.paragraphs[-1]
            last_paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
            last_paragraph.paragraph_format.space_after = Pt(6)

        except Exception:
            pass

    for i, group in enumerate(theme_groups, 1):

        add_theme_header_with_background(doc, i, group.group_title)

        sorted_blocks = sorted(group.qa_blocks, key=lambda x: x.position)

        for j, qa_block in enumerate(sorted_blocks, 1):

            conv_para = doc.add_paragraph()
            conv_para.paragraph_format.space_before = Pt(6)
            conv_para.paragraph_format.space_after = Pt(4)

            conv_text = f"Conversation {j}:"
            conv_run = conv_para.add_run(conv_text)
            conv_run.font.underline = True
            conv_run.font.size = Pt(11)
            conv_run.font.color.rgb = RGBColor(0, 0, 0)

            content = qa_block.formatted_content or qa_block.original_content
            for line in content.split("\n"):
                if line.strip():
                    if line.strip() in ["---", "***", "___", "<hr>", "<hr/>", "<hr />"]:
                        continue

                    p = doc.add_paragraph()
                    p.paragraph_format.left_indent = Inches(0.3)
                    p.paragraph_format.space_after = Pt(3)
                    p.paragraph_format.line_spacing = 1.15

                    parser = HTMLToDocx(p, font_size=Pt(9))
                    parser.feed(line.strip())
                    parser.close()

                    if not p.runs:
                        run = p.add_run(line.strip())
                        run.font.size = Pt(9)

            if j < len(sorted_blocks):
                separator = doc.add_paragraph()
                separator.paragraph_format.left_indent = Inches(0.3)
                separator.paragraph_format.space_before = Pt(6)
                separator.paragraph_format.space_after = Pt(6)

                separator_run = separator.add_run("_" * 50)
                separator_run.font.size = Pt(8)
                separator_run.font.color.rgb = RGBColor(200, 200, 200)

        if i < len(theme_groups):
            spacing = doc.add_paragraph()
            spacing.paragraph_format.space_before = Pt(12)
            spacing.paragraph_format.space_after = Pt(12)

    doc.save(output_path)


async def generate_key_themes(bank_name: str, fiscal_year: int, quarter: str) -> str:
    """
    Generate key themes report from earnings call Q&A.

    Args:
        bank_name: ID, name, or symbol of the bank
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")

    Returns:
        Success or error message string
    """
    execution_id = str(uuid.uuid4())
    logger.info(
        "etl.key_themes.started",
        execution_id=execution_id,
        bank_name=bank_name,
        fiscal_year=fiscal_year,
        quarter=quarter,
    )

    try:
        bank_info = get_bank_info_from_config(bank_name)

        await verify_and_get_availability(
            bank_info["bank_id"], bank_info["bank_name"], fiscal_year, quarter
        )

        ssl_config = setup_ssl()
        auth_config = await setup_authentication(execution_id, ssl_config)

        if not auth_config["success"]:
            error_msg = f"Authentication failed: {auth_config.get('error', 'Unknown error')}"
            logger.error("etl.key_themes.auth_failed", execution_id=execution_id, error=error_msg)
            raise RuntimeError(error_msg)

        context = {
            "execution_id": execution_id,
            "ssl_config": ssl_config,
            "auth_config": auth_config,
        }

        context["bank_name"] = bank_info["bank_name"]
        context["bank_symbol"] = bank_info["bank_symbol"]
        context["quarter"] = quarter
        context["fiscal_year"] = fiscal_year

        # Load categories from xlsx
        categories = load_categories_from_xlsx(execution_id)

        qa_index = await load_qa_blocks(bank_info["bank_name"], fiscal_year, quarter, context)

        if not qa_index:
            error_msg = f"No Q&A data found for {bank_info['bank_name']} {quarter} {fiscal_year}"
            logger.error(
                "etl.key_themes.no_data",
                execution_id=execution_id,
                bank_name=bank_info["bank_name"],
                fiscal_year=fiscal_year,
                quarter=quarter,
            )
            raise ValueError(error_msg)

        # Stage 1: Sequential classification with cumulative context
        await classify_all_qa_blocks_sequential(qa_index, categories, context)

        # Stage 2: Parallel HTML formatting
        await format_all_qa_blocks_parallel(qa_index, context)

        # Stage 3: Review classifications, regroup if needed, generate final titles
        theme_groups = await determine_comprehensive_grouping(qa_index, categories, context)

        logger.info(
            "etl.key_themes.grouping_completed",
            execution_id=execution_id,
            num_groups=len(theme_groups),
        )

        apply_grouping_to_index(qa_index, theme_groups)

        output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
        os.makedirs(output_dir, exist_ok=True)

        content_hash = hashlib.md5(
            f"{bank_info['bank_id']}_{fiscal_year}_{quarter}_{datetime.now().isoformat()}".encode()
        ).hexdigest()[:8]

        filename_base = f"{bank_info['bank_symbol']}_{fiscal_year}_{quarter}_{content_hash}"
        docx_filename = f"{filename_base}.docx"
        filepath = os.path.join(output_dir, docx_filename)

        create_document(theme_groups, bank_info["bank_name"], fiscal_year, quarter, filepath)

        logger.info("etl.key_themes.document_saved", execution_id=execution_id, filepath=filepath)

        report_metadata = get_standard_report_metadata()
        generation_timestamp = datetime.now()

        try:
            async with get_connection() as conn:
                deleted = await conn.execute(
                    text(
                        """
                    DELETE FROM aegis_reports
                    WHERE bank_id = :bank_id
                      AND fiscal_year = :fiscal_year
                      AND quarter = :quarter
                      AND report_type = :report_type
                    RETURNING id
                    """
                    ),
                    {
                        "bank_id": bank_info["bank_id"],
                        "fiscal_year": fiscal_year,
                        "quarter": quarter,
                        "report_type": report_metadata["report_type"],
                    },
                )
                deleted_rows = deleted.fetchall()

                if deleted_rows:
                    remaining_reports = await conn.execute(
                        text(
                            """
                        SELECT COUNT(*) as count
                        FROM aegis_reports
                        WHERE bank_id = :bank_id
                          AND fiscal_year = :fiscal_year
                          AND quarter = :quarter
                        """
                        ),
                        {
                            "bank_id": bank_info["bank_id"],
                            "fiscal_year": fiscal_year,
                            "quarter": quarter,
                        },
                    )
                    count_result = remaining_reports.scalar()

                    if count_result == 0:
                        await conn.execute(
                            text(
                                """
                            UPDATE aegis_data_availability
                            SET database_names = array_remove(database_names, 'reports')
                            WHERE bank_id = :bank_id
                              AND fiscal_year = :fiscal_year
                              AND quarter = :quarter
                              AND 'reports' = ANY(database_names)
                            """
                            ),
                            {
                                "bank_id": bank_info["bank_id"],
                                "fiscal_year": fiscal_year,
                                "quarter": quarter,
                            },
                        )

                result = await conn.execute(
                    text(
                        """
                    INSERT INTO aegis_reports (
                        report_name,
                        report_description,
                        report_type,
                        bank_id,
                        bank_name,
                        bank_symbol,
                        fiscal_year,
                        quarter,
                        local_filepath,
                        s3_document_name,
                        s3_pdf_name,
                        generation_date,
                        generated_by,
                        execution_id,
                        metadata
                    ) VALUES (
                        :report_name,
                        :report_description,
                        :report_type,
                        :bank_id,
                        :bank_name,
                        :bank_symbol,
                        :fiscal_year,
                        :quarter,
                        :local_filepath,
                        :s3_document_name,
                        :s3_pdf_name,
                        :generation_date,
                        :generated_by,
                        :execution_id,
                        :metadata
                    )
                    RETURNING id
                    """
                    ),
                    {
                        "report_name": report_metadata["report_name"],
                        "report_description": report_metadata["report_description"],
                        "report_type": report_metadata["report_type"],
                        "bank_id": bank_info["bank_id"],
                        "bank_name": bank_info["bank_name"],
                        "bank_symbol": bank_info["bank_symbol"],
                        "fiscal_year": fiscal_year,
                        "quarter": quarter,
                        "local_filepath": filepath,
                        "s3_document_name": docx_filename,
                        "s3_pdf_name": None,
                        "generation_date": generation_timestamp,
                        "generated_by": "key_themes_etl",
                        "execution_id": execution_id,
                        "metadata": json.dumps(
                            {
                                "theme_groups": len(theme_groups),
                                "total_qa_blocks": sum(
                                    len(group.qa_blocks) for group in theme_groups
                                ),
                                "invalid_qa_filtered": sum(
                                    1 for qa in qa_index.values() if not qa.is_valid
                                ),
                            }
                        ),
                    },
                )
                result.fetchone()

                await conn.execute(
                    text(
                        """
                    UPDATE aegis_data_availability
                    SET database_names =
                        CASE
                            WHEN 'reports' = ANY(database_names) THEN database_names
                            ELSE array_append(database_names, 'reports')
                        END
                    WHERE bank_id = :bank_id
                      AND fiscal_year = :fiscal_year
                      AND quarter = :quarter
                      AND NOT ('reports' = ANY(database_names))
                    RETURNING bank_id
                """
                    ),
                    {
                        "bank_id": bank_info["bank_id"],
                        "fiscal_year": fiscal_year,
                        "quarter": quarter,
                    },
                )

                await conn.commit()

        except Exception as e:
            logger.error("etl.key_themes.database_error", execution_id=execution_id, error=str(e))
            raise

        total_qa = sum(len(group.qa_blocks) for group in theme_groups)
        invalid_qa = sum(1 for qa in qa_index.values() if not qa.is_valid)
        logger.info(
            "etl.key_themes.completed",
            execution_id=execution_id,
            theme_groups=len(theme_groups),
            valid_qa=total_qa,
            invalid_qa_filtered=invalid_qa,
            filepath=filepath,
        )

        return (
            f"✅ Complete: {filepath}\n   Theme groups: {len(theme_groups)}, "
            f"Valid Q&A: {total_qa}, Filtered: {invalid_qa}"
        )

    except (
        KeyError,
        TypeError,
        AttributeError,
        json.JSONDecodeError,
        FileNotFoundError,
    ) as e:
        error_msg = f"Error generating key themes report: {str(e)}"
        logger.error(
            "etl.key_themes.error", execution_id=execution_id, error=error_msg, exc_info=True
        )
        return f"❌ {error_msg}"
    except (ValueError, RuntimeError) as e:
        logger.error("etl.key_themes.error", execution_id=execution_id, error=str(e))
        return f"⚠️ {str(e)}"


def main():
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Generate key themes report from earnings call Q&A",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--bank", required=True, help="Bank ID, name, or symbol")
    parser.add_argument("--year", type=int, required=True, help="Fiscal year")
    parser.add_argument(
        "--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"], help="Quarter"
    )

    args = parser.parse_args()

    postgresql_prompts()

    print(f"\n🔄 Generating key themes report for {args.bank} {args.quarter} {args.year}...\n")

    result = asyncio.run(
        generate_key_themes(bank_name=args.bank, fiscal_year=args.year, quarter=args.quarter)
    )

    print(result)


if __name__ == "__main__":
    main()
