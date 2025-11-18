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
from aegis.model.subagents.transcripts.retrieval import retrieve_full_section
from aegis.model.subagents.transcripts.formatting import format_full_section_chunks
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.llm_connector import complete_with_tools
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from aegis.utils.prompt_loader import load_prompt_from_db
from aegis.utils.sql_prompt import postgresql_prompts
from aegis.etls.config_loader import ETLConfig

setup_logging()
logger = get_logger()

etl_config = ETLConfig(os.path.join(os.path.dirname(__file__), "config", "config.yaml"))

_MONITORED_INSTITUTIONS = None


def _load_monitored_institutions() -> Dict[int, Dict[str, Any]]:
    """
    Load and cache monitored institutions configuration.

    Returns:
        Dictionary mapping bank_id to institution details (id, name, type, path_safe_name)
    """
    global _MONITORED_INSTITUTIONS
    if _MONITORED_INSTITUTIONS is None:
        config_path = os.path.join(
            os.path.dirname(__file__), "config", "monitored_institutions.yaml"
        )
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        _MONITORED_INSTITUTIONS = {inst["id"]: inst for inst in data.values()}
    return _MONITORED_INSTITUTIONS


def load_categories_from_xlsx(bank_type: str, execution_id: str) -> List[Dict[str, str]]:
    """
    Load categories from the appropriate XLSX file based on bank type.

    Args:
        bank_type: Either "Canadian_Banks" or "US_Banks"
        execution_id: Execution ID for logging

    Returns:
        List of dictionaries with transcripts_section, category_name, and category_description
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

        required_columns = ["transcripts_section", "category_name", "category_description"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in {file_name}: {missing_columns}")

        categories = df.to_dict("records")

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


def get_bank_type(bank_id: int) -> str:
    """
    Look up bank type from monitored institutions configuration.

    Args:
        bank_id: Bank ID from database

    Returns:
        "Canadian_Banks" or "US_Banks"

    Raises:
        ValueError: If bank_id not found in monitored institutions
    """
    institutions = _load_monitored_institutions()
    if bank_id not in institutions:
        raise ValueError(f"Bank ID {bank_id} not found in monitored institutions")
    return institutions[bank_id]["type"]


async def get_bank_info(bank_name: str) -> Dict[str, Any]:
    """
    Look up bank information from the aegis_data_availability table.

    Args:
        bank_name: Name, symbol, or ID of the bank

    Returns:
        Dictionary with bank_id, bank_name, and bank_symbol

    Raises:
        ValueError: If bank not found
    """
    async with get_connection() as conn:
        if bank_name.isdigit():
            result = await conn.execute(
                text(
                    """
                SELECT DISTINCT bank_id, bank_name, bank_symbol
                FROM aegis_data_availability
                WHERE bank_id = :bank_id
                LIMIT 1
                """
                ),
                {"bank_id": int(bank_name)},
            )
            row = result.fetchone()
            result = row._asdict() if row else None
        else:
            result = await conn.execute(
                text(
                    """
                SELECT DISTINCT bank_id, bank_name, bank_symbol
                FROM aegis_data_availability
                WHERE LOWER(bank_name) = LOWER(:bank_name)
                   OR LOWER(bank_symbol) = LOWER(:bank_name)
                LIMIT 1
                """
                ),
                {"bank_name": bank_name},
            )
            row = result.fetchone()
            result = row._asdict() if row else None

            if not result:
                partial_result = await conn.execute(
                    text(
                        """
                    SELECT DISTINCT bank_id, bank_name, bank_symbol
                    FROM aegis_data_availability
                    WHERE LOWER(bank_name) LIKE LOWER(:pattern)
                       OR LOWER(bank_symbol) LIKE LOWER(:pattern)
                    LIMIT 1
                    """
                    ),
                    {"pattern": f"%{bank_name}%"},
                )
                row = partial_result.fetchone()
                result = row._asdict() if row else None

        if not result:
            raise ValueError(f"Bank '{bank_name}' not found")

        return {
            "bank_id": result["bank_id"],
            "bank_name": result["bank_name"],
            "bank_symbol": result["bank_symbol"],
        }


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
            combo=retrieval_params, sections=category["transcripts_section"], context=context
        )

        if not chunks:
            category_results.append(
                {
                    "index": i,
                    "name": category["category_name"],
                    "report_section": category.get("report_section", "Results Summary"),
                    "rejected": True,
                    "rejection_reason": (
                        f"No {category['transcripts_section']} section data available"
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
            transcripts_section=category["transcripts_section"],
            research_plan=category_plan["extraction_strategy"],
            cross_category_notes=category_plan.get("cross_category_notes", ""),
            previous_sections=previous_summary,
            extracted_themes=extracted_themes,
        )

        user_prompt = f"Extract content from this transcript section:\n\n{formatted_section}"

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


async def verify_data_availability(bank_id: int, fiscal_year: int, quarter: str) -> bool:
    """
    Check if transcript data is available for the specified bank and period.

    Args:
        bank_id: Bank ID
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")

    Returns:
        True if transcript data is available, False otherwise
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

        if row and row[0]:
            return "transcripts" in row[0]

        return False


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
        bank_info = await get_bank_info(bank_name)

        if not await verify_data_availability(bank_info["bank_id"], fiscal_year, quarter):
            error_msg = (
                f"No transcript data available for {bank_info['bank_name']} {quarter} {fiscal_year}"
            )

            async with get_connection() as conn:
                result = await conn.execute(
                    text(
                        """
                    SELECT DISTINCT fiscal_year, quarter
                    FROM aegis_data_availability
                    WHERE bank_id = :bank_id
                      AND 'transcripts' = ANY(database_names)
                    ORDER BY fiscal_year DESC, quarter DESC
                    LIMIT 10
                    """
                    )
                )
                available_periods = await result.fetchall()

                if available_periods:
                    period_list = ", ".join(
                        [f"{p['quarter']} {p['fiscal_year']}" for p in available_periods]
                    )
                    error_msg += (
                        f"\n\nAvailable periods for {bank_info['bank_name']}: {period_list}"
                    )

            raise ValueError(error_msg)

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

        bank_type = get_bank_type(bank_info["bank_id"])
        categories = load_categories_from_xlsx(bank_type, execution_id)

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

        categories_text = ""
        for i, category in enumerate(categories, 1):
            section_desc = {
                "MD": "Management Discussion section only",
                "QA": "Q&A section only",
                "ALL": "Both Management Discussion and Q&A sections",
            }.get(category["transcripts_section"], "ALL sections")

            categories_text += f"""
Category {i}:
- Name: {category['category_name']}
- Section: {section_desc}
- Instructions: {category['category_description']}
"""

        research_prompts["system_prompt"] = research_prompts["system_prompt"].format(
            categories_list=categories_text,
            bank_name=bank_info["bank_name"],
            bank_symbol=bank_info["bank_symbol"],
            quarter=quarter,
            fiscal_year=fiscal_year,
        )

        research_prompts["user_prompt_template"] = (
            "Analyze this transcript and create the research plan:\n\n{transcript_text}"
        )

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
            "bank_type": bank_type,
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
