"""
Call Summary ETL Script.

Usage:
    python -m aegis.etls.call_summary.main --bank "Royal Bank of Canada" --year 2024 --quarter Q3
    python -m aegis.etls.call_summary.main --bank RY --year 2024 --quarter Q3
    python -m aegis.etls.call_summary.main --bank 1 --year 2024 --quarter Q3
"""

import argparse
import asyncio
import json
import uuid
import os
import hashlib
from datetime import datetime
from itertools import groupby
from typing import Dict, Any, List
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import yaml
from docx import Document
from docx.shared import Pt

from aegis.etls.call_summary.document_converter import (
    get_standard_report_metadata,
    setup_document_formatting,
    add_banner_image,
    add_document_title,
    add_section_heading,
    add_table_of_contents,
    mark_document_for_update,
    add_structured_content_to_doc,
)
from aegis.etls.call_summary.transcript_utils import (
    retrieve_full_section,
    format_full_section_chunks,
)
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.llm_connector import complete_with_tools
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from aegis.utils.prompt_loader import load_prompt_from_db
from aegis.utils.sql_prompt import postgresql_prompts
from aegis.utils.settings import config

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


def load_categories_from_xlsx(bank_type: str, execution_id: str) -> List[Dict[str, Any]]:
    """
    Load categories from the appropriate XLSX file based on bank type.

    Args:
        bank_type: Either "Canadian_Banks" or "US_Banks"
        execution_id: Execution ID for logging

    Returns:
        List of dictionaries with transcript_sections, category_name, category_description,
        example_1, example_2, example_3
    """
    file_name = (
        "canadian_banks_categories.xlsx"
        if bank_type == "Canadian_Banks"
        else "us_banks_categories.xlsx"
    )

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

        # Optional columns with defaults for backward compatibility
        optional_columns = ["example_1", "example_2", "example_3"]
        for col in optional_columns:
            if col not in df.columns:
                df[col] = ""  # Add empty column if not present

        # Ensure report_section column exists even for legacy sheets
        if "report_section" not in df.columns:
            df["report_section"] = "Results Summary"

        # Convert to list of dicts, ensuring all 6 columns are present
        categories = []
        for _, row in df.iterrows():
            category = {
                "transcript_sections": str(row["transcript_sections"]).strip(),
                "report_section": (
                    str(row["report_section"]).strip()
                    if pd.notna(row["report_section"])
                    else "Results Summary"
                ),
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
            "etl.call_summary.categories_loaded",
            execution_id=execution_id,
            file_name=file_name,
            num_categories=len(categories),
        )
        return categories

    except Exception as e:
        error_msg = f"Failed to load categories from {xlsx_path}: {str(e)}"
        logger.error(
            "etl.call_summary.categories_load_error",
            execution_id=execution_id,
            xlsx_path=xlsx_path,
            error=str(e),
        )
        raise RuntimeError(error_msg) from e


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

    Args:
        bank_id: Bank ID
        bank_name: Bank name (for error messages)
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")

    Raises:
        ValueError: If transcript data not available
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


async def _generate_research_plan(
    context: dict, research_prompts: dict, transcript_text: str, execution_id: str
) -> dict:
    """Generate research plan using LLM."""
    system_prompt = research_prompts["system_prompt"]
    user_prompt = research_prompts["user_prompt_template"].format(transcript_text=transcript_text)

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = await complete_with_tools(
                messages=messages,
                tools=[research_prompts["tool_definition"]],
                context=context,
                llm_params={
                    "model": etl_config.get_model("research_plan"),
                    "temperature": etl_config.temperature,
                    "max_tokens": etl_config.max_tokens,
                },
            )

            tool_call = response["choices"][0]["message"]["tool_calls"][0]
            research_plan_data = json.loads(tool_call["function"]["arguments"])

            logger.info(
                "etl.call_summary.research_plan_generated",
                execution_id=execution_id,
                num_plans=len(research_plan_data["category_plans"]),
            )
            return research_plan_data

        except (KeyError, IndexError, json.JSONDecodeError, TypeError) as e:
            logger.error(
                "etl.call_summary.research_plan_error",
                execution_id=execution_id,
                error=str(e),
                attempt=attempt + 1,
            )
            if attempt < max_retries - 1:
                continue
            raise RuntimeError(
                f"Error generating research plan after {max_retries} attempts: {str(e)}"
            ) from e

    raise RuntimeError(f"Failed to generate research plan after {max_retries} attempts")


def _format_quote_snippet(evidence: dict, index: int) -> str:
    """Format a single piece of evidence into a quote snippet."""
    if evidence.get("type") != "quote" or not evidence.get("content"):
        return ""

    snippet = evidence["content"][:80]
    if len(evidence["content"]) > 80:
        snippet += "..."
    return f'Q{index+1}: "{snippet}"'


def _build_statement_text(result: dict, stmt: dict) -> str:
    """Build formatted statement text with evidence quotes."""
    statement_text = f"[{result['name']}] {stmt['statement']}"

    if "evidence" not in stmt or not stmt["evidence"]:
        return statement_text

    quote_snippets = [_format_quote_snippet(ev, idx) for idx, ev in enumerate(stmt["evidence"][:3])]
    quote_snippets = [q for q in quote_snippets if q]  # Filter empty strings

    if quote_snippets:
        statement_text += f"\n  → Quotes: {' | '.join(quote_snippets)}"

    return statement_text


def _build_extracted_themes(category_results: list) -> str:
    """Build extracted themes summary from completed category results."""
    if not category_results:
        return "Starting extraction - no prior themes"

    completed_results = [r for r in category_results if not r.get("rejected", False)]
    if not completed_results:
        return "No themes extracted yet"

    all_statements = []
    for result in completed_results:
        if "summary_statements" not in result:
            continue

        for stmt in result["summary_statements"]:
            statement_text = _build_statement_text(result, stmt)
            all_statements.append(statement_text)

    if all_statements:
        return "\n".join(all_statements)
    return "No specific themes extracted yet"


def _build_previous_summary(category_results: list) -> str:
    """Build previous sections summary from completed category results."""
    if not category_results:
        return ""

    completed_results = [r for r in category_results if not r.get("rejected", False)]
    if not completed_results:
        return "No previous sections completed yet"

    completed_names = [r["name"] for r in completed_results]
    return f"Already completed: {', '.join(completed_names)}"


async def _process_categories(
    categories: list, research_plan_data: dict, extraction_prompts: dict, etl_context: dict
) -> list:
    """
    Process all categories and extract data from transcripts.

    Args:
        categories: List of category configurations
        research_plan_data: Research plan from LLM
        extraction_prompts: Prompts for extraction
        etl_context: Dict with keys: retrieval_params, bank_info, quarter,
            fiscal_year, context, execution_id

    Returns:
        List of category results (both accepted and rejected)
    """
    retrieval_params = etl_context["retrieval_params"]
    bank_info = etl_context["bank_info"]
    quarter = etl_context["quarter"]
    fiscal_year = etl_context["fiscal_year"]
    context = etl_context["context"]
    execution_id = etl_context["execution_id"]
    category_results = []

    for i, category in enumerate(categories, 1):
        category_plan = next(
            (p for p in research_plan_data["category_plans"] if p.get("index") == i), None
        )

        if not category_plan:
            category_results.append(
                {
                    "index": i,
                    "name": category["category_name"],
                    "report_section": category.get("report_section", "Results Summary"),
                    "rejected": True,
                    "rejection_reason": (
                        "Category not applicable to this transcript based on "
                        "research plan analysis"
                    ),
                }
            )
            continue

        chunks = await retrieve_full_section(
            combo=retrieval_params, sections=category["transcript_sections"], context=context
        )

        if not chunks:
            category_results.append(
                {
                    "index": i,
                    "name": category["category_name"],
                    "report_section": category.get("report_section", "Results Summary"),
                    "rejected": True,
                    "rejection_reason": (
                        f"No {category['transcript_sections']} section data available"
                    ),
                }
            )
            continue

        formatted_section = await format_full_section_chunks(
            chunks=chunks, combo=retrieval_params, context=context
        )

        previous_summary = _build_previous_summary(category_results)
        extracted_themes = _build_extracted_themes(category_results)

        system_prompt = extraction_prompts["system_prompt"].format(
            category_index=i,
            total_categories=len(categories),
            bank_name=bank_info["bank_name"],
            bank_symbol=bank_info["bank_symbol"],
            quarter=quarter,
            fiscal_year=fiscal_year,
            category_name=category["category_name"],
            category_description=category["category_description"],
            transcripts_section=category["transcript_sections"],
            research_plan=category_plan["extraction_strategy"],
            cross_category_notes=category_plan.get("cross_category_notes", ""),
            previous_sections=previous_summary,
            extracted_themes=extracted_themes,
        )

        user_prompt = extraction_prompts["user_prompt"].format(formatted_section=formatted_section)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        max_extraction_retries = 3

        for attempt in range(max_extraction_retries):
            try:
                response = await complete_with_tools(
                    messages=messages,
                    tools=[extraction_prompts["tool_definition"]],
                    context=context,
                    llm_params={
                        "model": etl_config.get_model("category_extraction"),
                        "temperature": etl_config.temperature,
                        "max_tokens": etl_config.max_tokens,
                    },
                )

                tool_call = response["choices"][0]["message"]["tool_calls"][0]
                extracted_data = json.loads(tool_call["function"]["arguments"])

                extracted_data["index"] = i
                extracted_data["name"] = category["category_name"]
                extracted_data["report_section"] = category.get("report_section", "Results Summary")

                category_results.append(extracted_data)

                logger.info(
                    "etl.call_summary.category_completed",
                    execution_id=execution_id,
                    category_name=category["category_name"],
                    rejected=extracted_data.get("rejected", False),
                )

                break

            except (KeyError, IndexError, json.JSONDecodeError, TypeError) as e:
                logger.error(
                    "etl.call_summary.category_extraction_error",
                    execution_id=execution_id,
                    category_name=category["category_name"],
                    error=str(e),
                    attempt=attempt + 1,
                )

                if attempt < max_extraction_retries - 1:
                    continue
                category_results.append(
                    {
                        "index": i,
                        "name": category["category_name"],
                        "report_section": category.get("report_section", "Results Summary"),
                        "rejected": True,
                        "rejection_reason": (
                            f"Error after {max_extraction_retries} attempts: {str(e)}"
                        ),
                    }
                )

    return category_results


def _generate_document(valid_categories: list, etl_context: dict) -> tuple:
    """
    Generate Word document from category results.

    Args:
        valid_categories: List of accepted category results
        etl_context: Dict with keys: bank_info, quarter, fiscal_year, execution_id

    Returns:
        Tuple of (filepath, docx_filename)
    """
    bank_info = etl_context["bank_info"]
    quarter = etl_context["quarter"]
    fiscal_year = etl_context["fiscal_year"]
    execution_id = etl_context["execution_id"]
    doc = Document()
    setup_document_formatting(doc)

    etl_dir = os.path.dirname(os.path.abspath(__file__))
    config_dir = os.path.join(etl_dir, "config")
    add_banner_image(doc, config_dir)

    add_document_title(doc, quarter, fiscal_year, bank_info["bank_symbol"])
    add_table_of_contents(doc)

    try:
        mark_document_for_update(doc)
    except (AttributeError, ValueError):
        pass

    sorted_categories = sorted(
        valid_categories,
        key=lambda x: (
            0 if x.get("report_section", "Results Summary") == "Results Summary" else 1,
            x.get("index", 0),
        ),
    )

    for idx, (section_name, section_categories) in enumerate(
        groupby(sorted_categories, key=lambda x: x.get("report_section", "Results Summary"))
    ):
        section_categories = list(section_categories)
        add_section_heading(doc, section_name, is_first_section=idx == 0)

        for i, category_data in enumerate(section_categories, 1):
            add_structured_content_to_doc(doc, category_data, heading_level=2)

            if i < len(section_categories):
                spacer = doc.add_paragraph()
                spacer.paragraph_format.space_after = Pt(6)
                spacer.add_run()

    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
    os.makedirs(output_dir, exist_ok=True)

    content_hash = hashlib.md5(
        f"{bank_info['bank_id']}_{fiscal_year}_{quarter}_{datetime.now().isoformat()}".encode()
    ).hexdigest()[:8]

    filename_base = f"{bank_info['bank_symbol']}_{fiscal_year}_{quarter}_{content_hash}"
    docx_filename = f"{filename_base}.docx"
    filepath = os.path.join(output_dir, docx_filename)
    doc.save(filepath)

    logger.info("etl.call_summary.document_saved", execution_id=execution_id, filepath=filepath)

    return filepath, docx_filename


async def _save_to_database(
    category_results: list,
    valid_categories: list,
    filepath: str,
    docx_filename: str,
    etl_context: dict,
) -> None:
    """
    Save report metadata to database.

    Args:
        category_results: All category results
        valid_categories: Accepted category results
        filepath: Local file path
        docx_filename: Document filename
        etl_context: Dict with keys: bank_info, quarter, fiscal_year, bank_type, execution_id
    """
    bank_info = etl_context["bank_info"]
    quarter = etl_context["quarter"]
    fiscal_year = etl_context["fiscal_year"]
    bank_type = etl_context["bank_type"]
    execution_id = etl_context["execution_id"]

    report_metadata = get_standard_report_metadata()
    generation_timestamp = datetime.now()

    try:
        async with get_connection() as conn:
            delete_result = await conn.execute(
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
            delete_result.fetchall()

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
                    "generated_by": "call_summary_etl",
                    "execution_id": execution_id,
                    "metadata": json.dumps(
                        {
                            "bank_type": bank_type,
                            "categories_processed": len(category_results),
                            "categories_included": len(valid_categories),
                            "categories_rejected": len(category_results) - len(valid_categories),
                        }
                    ),
                },
            )
            result.fetchone()

            await conn.commit()

    except SQLAlchemyError as e:
        logger.error("etl.call_summary.database_error", execution_id=execution_id, error=str(e))


async def generate_call_summary(bank_name: str, fiscal_year: int, quarter: str) -> str:
    """
    Generate a call summary by directly calling transcript functions.

    Args:
        bank_name: ID, name, or symbol of the bank
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")

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
    )

    try:
        # Get bank info from monitored institutions config
        bank_info = get_bank_info_from_config(bank_name)

        # Verify data availability (single database check)
        await verify_and_get_availability(
            bank_info["bank_id"], bank_info["bank_name"], fiscal_year, quarter
        )

        ssl_config = setup_ssl()
        auth_config = await setup_authentication(execution_id, ssl_config)

        if not auth_config["success"]:
            error_msg = f"Authentication failed: {auth_config.get('error', 'Unknown error')}"
            logger.error("etl.call_summary.auth_failed", execution_id=execution_id, error=error_msg)
            raise RuntimeError(error_msg)

        context = {
            "execution_id": execution_id,
            "auth_config": auth_config,
            "ssl_config": ssl_config,
        }

        categories = load_categories_from_xlsx(bank_info["bank_type"], execution_id)

        retrieval_params = {
            "bank_id": bank_info["bank_id"],
            "bank_name": bank_info["bank_name"],
            "bank_symbol": bank_info["bank_symbol"],
            "fiscal_year": fiscal_year,
            "quarter": quarter,
            "query_intent": "Generate comprehensive research plan for earnings call summary",
        }

        chunks = await retrieve_full_section(
            combo=retrieval_params, sections="ALL", context=context
        )

        if not chunks:
            raise ValueError(
                f"No transcript chunks found for {bank_info['bank_name']} "
                f"{quarter} {fiscal_year}"
            )

        formatted_transcript = await format_full_section_chunks(
            chunks=chunks, combo=retrieval_params, context=context
        )

        research_prompts = load_prompt_from_db(
            layer="call_summary_etl",
            name="research_plan",
            compose_with_globals=False,
            available_databases=None,
            execution_id=execution_id,
        )

        # Format categories using standardized XML format
        categories_text = format_categories_for_prompt(categories)

        research_prompts["system_prompt"] = research_prompts["system_prompt"].format(
            categories_list=categories_text,
            bank_name=bank_info["bank_name"],
            bank_symbol=bank_info["bank_symbol"],
            quarter=quarter,
            fiscal_year=fiscal_year,
        )

        research_prompts["user_prompt_template"] = research_prompts["user_prompt"]

        research_plan_data = await _generate_research_plan(
            context, research_prompts, formatted_transcript, execution_id
        )

        extraction_prompts = load_prompt_from_db(
            layer="call_summary_etl",
            name="category_extraction",
            compose_with_globals=False,
            available_databases=None,
            execution_id=execution_id,
        )

        etl_context = {
            "retrieval_params": retrieval_params,
            "bank_info": bank_info,
            "quarter": quarter,
            "fiscal_year": fiscal_year,
            "context": context,
            "execution_id": execution_id,
            "bank_type": bank_info["bank_type"],
        }

        category_results = await _process_categories(
            categories=categories,
            research_plan_data=research_plan_data,
            extraction_prompts=extraction_prompts,
            etl_context=etl_context,
        )

        valid_categories = [c for c in category_results if not c.get("rejected", False)]

        if not valid_categories:
            raise ValueError("All categories were rejected - no content to generate document")

        filepath, docx_filename = _generate_document(
            valid_categories=valid_categories, etl_context=etl_context
        )

        await _save_to_database(
            category_results=category_results,
            valid_categories=valid_categories,
            filepath=filepath,
            docx_filename=docx_filename,
            etl_context=etl_context,
        )

        logger.info(
            "etl.call_summary.completed",
            execution_id=execution_id,
            stage="full_report",
            num_categories=len(valid_categories),
        )

        return (
            f"✅ Complete: {filepath}\n   Categories: "
            f"{len(valid_categories)}/{len(category_results)} included"
        )

    except (
        KeyError,
        TypeError,
        AttributeError,
        json.JSONDecodeError,
        SQLAlchemyError,
        FileNotFoundError,
    ) as e:
        # System errors with ❌ prefix (unexpected errors)
        error_msg = f"Error generating call summary: {str(e)}"
        logger.error(
            "etl.call_summary.error", execution_id=execution_id, error=error_msg, exc_info=True
        )
        return f"❌ {error_msg}"
    except (ValueError, RuntimeError) as e:
        # User-friendly errors with ⚠️ prefix (expected errors)
        logger.error("etl.call_summary.error", execution_id=execution_id, error=str(e))
        return f"⚠️ {str(e)}"


def main():
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Generate call summary reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--bank", required=True, help="Bank ID, name, or symbol")
    parser.add_argument("--year", type=int, required=True, help="Fiscal year")
    parser.add_argument(
        "--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"], help="Quarter"
    )

    args = parser.parse_args()

    postgresql_prompts()

    print(f"\n🔄 Generating report for {args.bank} {args.quarter} {args.year}...\n")

    result = asyncio.run(
        generate_call_summary(bank_name=args.bank, fiscal_year=args.year, quarter=args.quarter)
    )

    print(result)


if __name__ == "__main__":
    main()
