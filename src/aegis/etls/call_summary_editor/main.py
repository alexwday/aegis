"""
Interactive Call Summary Editor ETL.

Usage:
    python -m aegis.etls.call_summary_editor.main \\
        --bank "Royal Bank of Canada" --year 2024 --quarter Q3
    python -m aegis.etls.call_summary_editor.main --bank RY --year 2024 --quarter Q3
    python -m aegis.etls.call_summary_editor.main --bank 1 --year 2024 --quarter Q3
    python -m aegis.etls.call_summary_editor.main benchmark \\
        --predicted path/to/report.html --expected path/to/expected_items.json
"""

import argparse
import asyncio
import json
import time
import uuid
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Dict, Any, List
from sqlalchemy import text
import pandas as pd
import yaml

from aegis.etls.call_summary_editor.transcript_utils import VALID_SECTION_KEYS
from aegis.etls.call_summary_editor.interactive_html import (
    build_report_state as build_interactive_report_state,
    generate_html as generate_interactive_html,
)
from aegis.etls.call_summary_editor.interactive_pipeline import (
    analyze_config_coverage,
    build_interactive_bank_data,
    count_included_categories,
    generate_bucket_headlines,
)
from aegis.etls.call_summary_editor.nas_source import (
    extract_raw_blocks,
    find_transcript_xml,
    get_nas_connection,
    parse_transcript_xml,
)
from aegis.etls.call_summary_editor.benchmark import (
    benchmark_recall,
    load_expected_items,
    load_predicted_items,
    render_benchmark_report,
)
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from aegis.utils.settings import config

setup_logging()
logger = get_logger()

# --- Concurrency default (overridable via config.yaml) ---
MAX_CONCURRENT_EXTRACTIONS = 5


# --- ETL Exception Hierarchy ---


class CallSummaryError(Exception):
    """Base exception for call summary ETL errors."""


class CallSummarySystemError(CallSummaryError):
    """Unexpected system/infrastructure error."""


class CallSummaryUserError(CallSummaryError):
    """Expected user-facing error (bad input, no data, etc.)."""


@dataclass
class CallSummaryResult:
    """Successful call summary generation result."""

    filepath: str
    total_categories: int
    included_categories: int
    total_cost: float = 0.0
    total_tokens: int = 0


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

    def get_max_tokens(self, task_key: str) -> int:
        """
        Get max_tokens for a specific task, falling back to default.

        Supports both legacy (flat int) and per-task (dict) config formats.

        Args:
            task_key: Task identifier (e.g., "research_plan", "category_extraction")

        Returns:
            max_tokens value for the task
        """
        max_tokens_config = self._config.get("llm", {}).get("max_tokens", {})
        if isinstance(max_tokens_config, int):
            return max_tokens_config
        return max_tokens_config.get(task_key, max_tokens_config.get("default", 32768))

    @property
    def max_concurrent_extractions(self) -> int:
        """Get the maximum number of concurrent category extractions."""
        return self._config.get("concurrency", {}).get(
            "max_concurrent_extractions", MAX_CONCURRENT_EXTRACTIONS
        )

    @property
    def min_importance(self) -> float:
        """Inclusion threshold for sentences/categories in the interactive report."""
        return float(self._config.get("pipeline", {}).get("min_importance", 4.0))

    @property
    def headline_sample_size(self) -> int:
        """Maximum sample snippets fed to the headline LLM per bucket."""
        return int(self._config.get("pipeline", {}).get("headline_sample_size", 8))

    @property
    def selected_importance_threshold(self) -> float:
        """Initial threshold for evidence auto-selected into the report draft."""
        return float(self._config.get("pipeline", {}).get("selected_importance_threshold", 6.5))

    @property
    def candidate_importance_threshold(self) -> float:
        """Initial threshold for evidence kept visible as a review candidate."""
        return float(self._config.get("pipeline", {}).get("candidate_importance_threshold", 4.0))

    @property
    def min_bucket_score_for_assignment(self) -> float:
        """Minimum existing-bucket relevance score required for automatic assignment."""
        return float(self._config.get("pipeline", {}).get("min_bucket_score_for_assignment", 6.0))

    @property
    def enable_headlines(self) -> bool:
        """Whether optional generated headlines should be produced."""
        return bool(self._config.get("pipeline", {}).get("enable_headlines", False))

    def get_stage_params(self, stage: str) -> Dict[str, Any]:
        """Resolve model, temperature, and max_tokens for a named pipeline stage."""
        return {
            "model": self.get_model(stage),
            "temperature": self.temperature,
            "max_tokens": self.get_max_tokens(stage),
        }


etl_config = ETLConfig(os.path.join(os.path.dirname(__file__), "config", "config.yaml"))


@lru_cache(maxsize=1)
def _load_monitored_institutions() -> Dict[int, Dict[str, Any]]:
    """
    Load and cache monitored institutions configuration.

    Uses @lru_cache for thread-safe, testable caching (clear via .cache_clear()).

    Returns:
        Dictionary mapping bank_id to institution details (id, name, symbol, type, path_safe_name)
    """
    config_path = os.path.join(os.path.dirname(__file__), "config", "monitored_institutions.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        yaml_data = yaml.safe_load(f)

    institutions = {}
    for key, value in yaml_data.items():
        symbol = key.split("-")[0]  # Extract symbol from "RY-CA" -> "RY"
        institutions[value["id"]] = {**value, "symbol": symbol, "full_ticker": key}
    return institutions


def _get_total_llm_cost(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Aggregate all accumulated LLM costs from context.

    Returns:
        Dict with total_cost, total_prompt_tokens, total_completion_tokens, total_tokens
    """
    costs = context.get("_llm_costs", [])
    total_cost = sum(c.get("total_cost", 0) for c in costs)
    prompt_tokens = sum(c.get("prompt_tokens", 0) for c in costs)
    completion_tokens = sum(c.get("completion_tokens", 0) for c in costs)
    return {
        "total_cost": round(total_cost, 4),
        "total_prompt_tokens": prompt_tokens,
        "total_completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
        "llm_calls": len(costs),
    }


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

        # Convert to list of dicts, ensuring all required fields are non-empty
        categories = []
        for idx, row in df.iterrows():
            for field in required_columns:
                if pd.isna(row[field]) or str(row[field]).strip() == "":
                    raise ValueError(f"Missing value for '{field}' in {file_name} (row {idx + 2})")

            transcript_sections = str(row["transcript_sections"]).strip()
            valid_sections = VALID_SECTION_KEYS
            if transcript_sections not in valid_sections:
                raise ValueError(
                    f"Invalid transcript_sections '{transcript_sections}' "
                    f"in {file_name} (row {idx + 2}). Must be one of: {valid_sections}"
                )

            category = {
                "transcript_sections": transcript_sections,
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
            "etl.call_summary_editor.categories_loaded",
            execution_id=execution_id,
            file_name=file_name,
            num_categories=len(categories),
        )
        return categories

    except Exception as e:
        error_msg = f"Failed to load categories from {xlsx_path}: {str(e)}"
        logger.error(
            "etl.call_summary_editor.categories_load_error",
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
    bank_identifier = bank_identifier.strip()

    def _to_bank_info(inst: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "bank_id": inst["id"],
            "bank_name": inst["name"],
            "bank_symbol": inst["symbol"],
            "full_ticker": inst.get("full_ticker"),
            "bank_type": inst["type"],
            "path_safe_name": inst.get("path_safe_name"),
        }

    # Try lookup by ID
    if bank_identifier.isdigit():
        bank_id = int(bank_identifier)
        if bank_id in institutions:
            return _to_bank_info(institutions[bank_id])

    # Try lookup by symbol first (supports full ticker input, e.g. "C-US")
    bank_identifier_upper = bank_identifier.upper()
    bank_identifier_lower = bank_identifier.lower()
    symbol_candidate = bank_identifier_upper.split("-")[0]

    for inst in institutions.values():
        if inst["symbol"].upper() == symbol_candidate:
            return _to_bank_info(inst)

    # Fallback: match by name (case-insensitive, partial match)
    for inst in institutions.values():
        if bank_identifier_lower in inst["name"].lower():
            return _to_bank_info(inst)

    # Build helpful error message with available banks
    available = [f"{inst['symbol']} ({inst['name']})" for inst in institutions.values()]
    raise ValueError(
        f"Bank '{bank_identifier}' not found in monitored institutions.\n"
        f"Available banks: {', '.join(sorted(available))}"
    )


def _timing_summary(marks: list) -> dict:
    """
    Convert timing marks to elapsed-seconds summary.

    Args:
        marks: List of (stage_name, timestamp) tuples

    Returns:
        Dict mapping "{stage}_s" to elapsed seconds, plus "total_s"
    """
    if len(marks) < 2:
        return {}
    summary = {}
    for i in range(1, len(marks)):
        summary[f"{marks[i][0]}_s"] = round(marks[i][1] - marks[i - 1][1], 2)
    summary["total_s"] = round(marks[-1][1] - marks[0][1], 2)
    return summary


def _get_interactive_report_metadata() -> Dict[str, str]:
    """Metadata for interactive HTML call summary editor reports."""
    return {
        "report_name": "Earnings Call Summary Editor",
        "report_description": (
            "Interactive HTML earnings call summary editor with sentence-level "
            "classification, transcript review, and report drafting controls."
        ),
        "report_type": "call_summary_editor",
    }


def _generate_interactive_report(
    report_state: Dict[str, Any],
    etl_context: Dict[str, Any],
    min_importance: float,
) -> tuple[str, str]:
    """Write the interactive HTML report to the ETL output directory."""
    bank_info = etl_context["bank_info"]
    quarter = etl_context["quarter"]
    fiscal_year = etl_context["fiscal_year"]
    execution_id = etl_context["execution_id"]

    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
    os.makedirs(output_dir, exist_ok=True)

    ticker_for_filename = bank_info.get("full_ticker") or bank_info["bank_symbol"]
    filename_base = f"{ticker_for_filename}_{fiscal_year}_{quarter}_call_summary_editor"
    html_filename = f"{filename_base}.html"
    filepath = os.path.join(output_dir, html_filename)

    html_content = generate_interactive_html(
        state=report_state,
        fiscal_year=fiscal_year,
        fiscal_quarter=quarter,
        min_importance=min_importance,
    )
    with open(filepath, "w", encoding="utf-8") as handle:
        handle.write(html_content)

    logger.info("etl.call_summary_editor.html_saved", execution_id=execution_id, filepath=filepath)
    return filepath, html_filename


async def _save_interactive_report_to_database(
    *,
    filepath: str,
    html_filename: str,
    total_categories: int,
    included_categories: int,
    etl_context: Dict[str, Any],
) -> None:
    """Persist interactive HTML report metadata to aegis_reports."""
    bank_info = etl_context["bank_info"]
    quarter = etl_context["quarter"]
    fiscal_year = etl_context["fiscal_year"]
    execution_id = etl_context["execution_id"]

    report_metadata = _get_interactive_report_metadata()
    generation_timestamp = datetime.now()

    async with get_connection() as conn:
        await conn.execute(
            text(
                """
                DELETE FROM aegis_reports
                WHERE bank_id = :bank_id
                  AND fiscal_year = :fiscal_year
                  AND quarter = :quarter
                  AND report_type = :report_type
                """
            ),
            {
                "bank_id": bank_info["bank_id"],
                "fiscal_year": fiscal_year,
                "quarter": quarter,
                "report_type": report_metadata["report_type"],
            },
        )

        await conn.execute(
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
                "s3_document_name": html_filename,
                "s3_pdf_name": None,
                "generation_date": generation_timestamp,
                "generated_by": "call_summary_editor_etl",
                "execution_id": execution_id,
                "metadata": json.dumps(
                    {
                        "output_format": "html",
                        "bank_type": bank_info["bank_type"],
                        "categories_processed": total_categories,
                        "categories_included": included_categories,
                        "categories_rejected": total_categories - included_categories,
                    }
                ),
            },
        )
        await conn.commit()

    logger.info(
        "etl.call_summary_editor.database_saved",
        execution_id=execution_id,
        filepath=filepath,
        total_categories=total_categories,
        included_categories=included_categories,
    )


async def generate_call_summary(  # pylint: disable=too-many-statements
    bank_name: str, fiscal_year: int, quarter: str
) -> CallSummaryResult:
    """
    Generate an interactive HTML call summary editor report.

    Args:
        bank_name: ID, name, or symbol of the bank
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")

    Returns:
        CallSummaryResult with filepath and category counts

    Raises:
        CallSummaryUserError: For expected errors (bad input, no data)
        CallSummarySystemError: For unexpected system/infrastructure errors
    """
    marks = [("start", time.monotonic())]
    execution_id = str(uuid.uuid4())
    logger.info(
        "etl.call_summary_editor.started",
        execution_id=execution_id,
        bank_name=bank_name,
        fiscal_year=fiscal_year,
        quarter=quarter,
    )

    nas_conn = None
    try:
        bank_info = get_bank_info_from_config(bank_name)

        ssl_config = setup_ssl()
        auth_config = await setup_authentication(execution_id, ssl_config)

        if not auth_config["success"]:
            error_msg = f"Authentication failed: {auth_config.get('error', 'Unknown error')}"
            logger.error(
                "etl.call_summary_editor.auth_failed", execution_id=execution_id, error=error_msg
            )
            raise CallSummarySystemError(error_msg)

        context = {
            "execution_id": execution_id,
            "auth_config": auth_config,
            "ssl_config": ssl_config,
            "_llm_costs": [],
        }
        marks.append(("setup", time.monotonic()))

        categories = load_categories_from_xlsx(bank_info["bank_type"], execution_id)
        min_importance = etl_config.min_importance
        selected_importance_threshold = etl_config.selected_importance_threshold
        candidate_importance_threshold = etl_config.candidate_importance_threshold
        min_bucket_score_for_assignment = etl_config.min_bucket_score_for_assignment

        nas_conn = get_nas_connection()
        xml_result = find_transcript_xml(nas_conn, bank_info, fiscal_year, quarter)
        if xml_result is None:
            raise CallSummaryUserError(
                f"No transcript XML found for {bank_info['bank_name']} {quarter} {fiscal_year}"
            )
        marks.append(("retrieval", time.monotonic()))

        parsed_transcript = parse_transcript_xml(xml_result.xml_bytes)
        period_label = f"{bank_info['bank_name']} {quarter} {fiscal_year}"
        if parsed_transcript is None:
            raise CallSummaryUserError(f"Failed to parse transcript XML for {period_label}")

        ticker = bank_info.get("full_ticker") or bank_info["bank_symbol"]
        md_raw_blocks, qa_raw_blocks = extract_raw_blocks(parsed_transcript, ticker)
        if not md_raw_blocks and not qa_raw_blocks:
            raise CallSummaryUserError(
                f"Transcript XML contained no usable content for {period_label}"
            )
        marks.append(("parse", time.monotonic()))

        qa_boundary_llm_params = etl_config.get_stage_params("qa_boundary")
        md_llm_params = etl_config.get_stage_params("md_classification")
        qa_llm_params = etl_config.get_stage_params("qa_classification")
        headline_llm_params = etl_config.get_stage_params("headline")
        config_review_llm_params = etl_config.get_stage_params("config_review")

        etl_context = {
            "bank_info": bank_info,
            "quarter": quarter,
            "fiscal_year": fiscal_year,
            "context": context,
            "execution_id": execution_id,
            "bank_type": bank_info["bank_type"],
            "transcript_source": {
                "type": "nas",
                "path": xml_result.file_path,
            },
        }

        bank_data = await build_interactive_bank_data(
            md_raw_blocks=md_raw_blocks,
            qa_raw_blocks=qa_raw_blocks,
            categories=categories,
            bank_info=bank_info,
            fiscal_year=fiscal_year,
            fiscal_quarter=quarter,
            transcript_title=parsed_transcript.get("title", ""),
            context=context,
            qa_boundary_llm_params=qa_boundary_llm_params,
            md_llm_params=md_llm_params,
            qa_llm_params=qa_llm_params,
            report_inclusion_threshold=min_importance,
            selected_importance_threshold=selected_importance_threshold,
            candidate_importance_threshold=candidate_importance_threshold,
            min_bucket_score_for_assignment=min_bucket_score_for_assignment,
            max_concurrent_md_blocks=etl_config.max_concurrent_extractions,
        )
        banks_data = {bank_data["ticker"]: bank_data}
        marks.append(("classification", time.monotonic()))

        config_review_by_bank = {
            bank_data["ticker"]: await analyze_config_coverage(
                bank_data=bank_data,
                categories=categories,
                min_importance=min_importance,
                context=context,
                llm_params=config_review_llm_params,
            )
        }
        marks.append(("config_review", time.monotonic()))

        bucket_headlines = {}
        if etl_config.enable_headlines:
            bucket_headlines = await generate_bucket_headlines(
                banks_data=banks_data,
                categories=categories,
                min_importance=min_importance,
                context=context,
                llm_params=headline_llm_params,
                sample_size=etl_config.headline_sample_size,
            )
        report_state = build_interactive_report_state(
            banks_data=banks_data,
            categories=categories,
            fiscal_year=fiscal_year,
            fiscal_quarter=quarter,
            min_importance=min_importance,
            bucket_headlines=bucket_headlines,
            config_review_by_bank=config_review_by_bank,
        )
        included_categories = count_included_categories(banks_data, min_importance)
        total_categories = len(categories)
        marks.append(("headlines", time.monotonic()))

        filepath, html_filename = _generate_interactive_report(
            report_state=report_state,
            etl_context=etl_context,
            min_importance=min_importance,
        )
        marks.append(("document", time.monotonic()))

        await _save_interactive_report_to_database(
            filepath=filepath,
            html_filename=html_filename,
            total_categories=total_categories,
            included_categories=included_categories,
            etl_context=etl_context,
        )
        marks.append(("save", time.monotonic()))

        cost_summary = _get_total_llm_cost(context)
        logger.info(
            "etl.call_summary_editor.completed",
            execution_id=execution_id,
            num_categories=included_categories,
            llm_calls=cost_summary["llm_calls"],
            total_tokens=cost_summary["total_tokens"],
            total_cost=cost_summary["total_cost"],
            transcript_source="nas",
            transcript_path=xml_result.file_path,
            **_timing_summary(marks),
        )

        return CallSummaryResult(
            filepath=filepath,
            total_categories=total_categories,
            included_categories=included_categories,
            total_cost=cost_summary["total_cost"],
            total_tokens=cost_summary["total_tokens"],
        )

    except CallSummaryError:
        raise
    except (ValueError, RuntimeError) as exc:
        logger.error("etl.call_summary_editor.error", execution_id=execution_id, error=str(exc))
        raise CallSummaryUserError(str(exc)) from exc
    except Exception as exc:
        error_msg = f"Error generating call summary: {str(exc)}"
        logger.error(
            "etl.call_summary_editor.unexpected_error",
            execution_id=execution_id,
            error=str(exc),
            exc_info=True,
        )
        raise CallSummarySystemError(error_msg) from exc
    finally:
        # Always report LLM spend so partial-run costs are visible on failure.
        partial = _get_total_llm_cost(context) if "context" in locals() else None
        if partial and partial.get("llm_calls", 0) > 0:
            logger.info(
                "etl.call_summary_editor.llm_spend_final",
                execution_id=execution_id,
                llm_calls=partial["llm_calls"],
                total_tokens=partial["total_tokens"],
                total_cost=partial["total_cost"],
            )
        if nas_conn is not None:
            try:
                nas_conn.close()
            except Exception:  # pylint: disable=broad-except
                pass


async def preflight_call_summary(bank_name: str, fiscal_year: int, quarter: str) -> Dict[str, Any]:
    """Validate bank lookup, XLSX categories, NAS XML presence, and XML parseability.

    Runs everything up to (but not including) the LLM classification stages so a
    fleet run can be gated on cheap checks. Returns a status dict; never raises
    for data-quality issues.
    """
    status: Dict[str, Any] = {
        "bank_name": bank_name,
        "fiscal_year": fiscal_year,
        "quarter": quarter,
        "ok": False,
        "failure": None,
    }
    nas_conn = None
    try:
        bank_info = get_bank_info_from_config(bank_name)
        status["bank_symbol"] = bank_info["bank_symbol"]
        status["bank_type"] = bank_info["bank_type"]

        categories = load_categories_from_xlsx(bank_info["bank_type"], execution_id="preflight")
        status["categories_loaded"] = len(categories)

        nas_conn = get_nas_connection()
        xml_result = find_transcript_xml(nas_conn, bank_info, fiscal_year, quarter)
        if xml_result is None:
            status["failure"] = "no transcript XML found on NAS"
            return status
        status["transcript_path"] = xml_result.file_path

        parsed_transcript = parse_transcript_xml(xml_result.xml_bytes)
        if parsed_transcript is None:
            status["failure"] = "transcript XML failed to parse"
            return status

        ticker = bank_info.get("full_ticker") or bank_info["bank_symbol"]
        md_raw_blocks, qa_raw_blocks = extract_raw_blocks(parsed_transcript, ticker)
        status["md_blocks"] = len(md_raw_blocks)
        status["qa_blocks"] = len(qa_raw_blocks)

        if not md_raw_blocks and not qa_raw_blocks:
            status["failure"] = "transcript contained no MD or QA blocks"
            return status

        status["ok"] = True
        return status
    except Exception as exc:  # pylint: disable=broad-except
        status["failure"] = f"{type(exc).__name__}: {exc}"
        return status
    finally:
        if nas_conn is not None:
            try:
                nas_conn.close()
            except Exception:  # pylint: disable=broad-except
                pass


def main():
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Generate interactive call summary editor reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest="command")
    benchmark_parser = subparsers.add_parser(
        "benchmark",
        help="Run recall benchmark against a saved report HTML or JSON payload.",
    )
    benchmark_parser.add_argument(
        "--predicted",
        required=True,
        help="Path to a saved report HTML or JSON payload containing predicted evidence.",
    )
    benchmark_parser.add_argument(
        "--expected",
        required=True,
        help="Path to analyst-reviewed JSON expectations.",
    )
    benchmark_parser.add_argument(
        "--output",
        help="Optional output file for the benchmark report.",
    )
    benchmark_parser.add_argument(
        "--format",
        choices=["markdown", "json"],
        default="markdown",
        help="Benchmark output format.",
    )

    parser.add_argument("--bank", help="Bank ID, name, or symbol")
    parser.add_argument("--year", type=int, help="Fiscal year")
    parser.add_argument("--quarter", choices=["Q1", "Q2", "Q3", "Q4"], help="Quarter")
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="Validate bank config, NAS transcript, and XML parseability without calling the LLM.",
    )

    args = parser.parse_args()

    if args.command == "benchmark":
        predicted_items = load_predicted_items(args.predicted)
        expected_items = load_expected_items(args.expected)
        result = benchmark_recall(predicted_items, expected_items)
        if args.format == "json":
            output_text = json.dumps(result, indent=2)
        else:
            output_text = render_benchmark_report(result)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as handle:
                handle.write(output_text)
        print(output_text)
        sys.exit(0)

    if not args.bank or args.year is None or not args.quarter:
        parser.error("--bank, --year, and --quarter are required unless using the benchmark command")

    if args.preflight:
        print(f"\n🔍 Preflight check: {args.bank} {args.quarter} {args.year}\n")
        status = asyncio.run(
            preflight_call_summary(bank_name=args.bank, fiscal_year=args.year, quarter=args.quarter)
        )
        for key, value in status.items():
            print(f"  {key}: {value}")
        sys.exit(0 if status["ok"] else 1)

    print(f"\n🔄 Generating report for {args.bank} {args.quarter} {args.year}...\n")

    try:
        result = asyncio.run(
            generate_call_summary(bank_name=args.bank, fiscal_year=args.year, quarter=args.quarter)
        )
        print(
            f"✅ Complete: {result.filepath}\n"
            f"   Categories: {result.included_categories}/{result.total_categories} included\n"
            f"   LLM cost: ${result.total_cost:.4f}, Tokens: {result.total_tokens:,}"
        )
    except CallSummaryUserError as e:
        print(f"⚠️ {e}", file=sys.stderr)
        sys.exit(1)
    except CallSummaryError as e:
        print(f"❌ {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
