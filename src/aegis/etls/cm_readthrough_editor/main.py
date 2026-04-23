"""
Interactive CM Readthrough Editor ETL.

Usage:
    python -m aegis.etls.cm_readthrough_editor --year 2024 --quarter Q3
    python -m aegis.etls.cm_readthrough_editor --bank RY --year 2024 --quarter Q3
    python -m aegis.etls.cm_readthrough_editor benchmark \\
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
from typing import Dict, Any, List, Optional
from sqlalchemy import text
import pandas as pd
import yaml

from aegis.etls.cm_readthrough_editor.transcript_utils import VALID_SECTION_KEYS
from aegis.etls.cm_readthrough_editor.interactive_html import (
    build_report_state as build_interactive_report_state,
    generate_html as generate_interactive_html,
)
from aegis.etls.cm_readthrough_editor.interactive_pipeline import (
    build_interactive_bank_data,
    count_included_categories,
    generate_report_section_subtitles,
)
from aegis.etls.cm_readthrough_editor.nas_source import (
    extract_raw_blocks,
    find_transcript_xml,
    get_nas_connection,
    parse_transcript_xml,
)
from aegis.etls.cm_readthrough_editor.benchmark import (
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


class CMReadthroughEditorError(Exception):
    """Base exception for CM readthrough editor ETL errors."""


class CMReadthroughEditorSystemError(CMReadthroughEditorError):
    """Unexpected system/infrastructure error."""


class CMReadthroughEditorUserError(CMReadthroughEditorError):
    """Expected user-facing error (bad input, no data, etc.)."""


@dataclass
class CMReadthroughEditorResult:
    """Successful CM readthrough editor generation result."""

    filepath: str
    total_categories: int
    included_categories: int
    banks_requested: int = 0
    banks_included: int = 0
    banks_with_findings: int = 0
    skipped_banks: int = 0
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

        Args:
            task_key: Task identifier (e.g., "research_plan", "category_extraction")

        Returns:
            max_tokens value for the task
        """
        max_tokens_config = self._config.get("llm", {}).get("max_tokens", {})
        return max_tokens_config.get(task_key, max_tokens_config.get("default", 32768))

    @property
    def max_concurrent_banks(self) -> int:
        """Get the maximum number of banks to process concurrently."""
        return int(self._config.get("concurrency", {}).get("max_concurrent_banks", MAX_CONCURRENT_EXTRACTIONS))

    @property
    def min_importance(self) -> float:
        """Inclusion threshold for sentences/categories in the interactive report."""
        return float(self._config.get("pipeline", {}).get("min_importance", 4.0))

    @property
    def selected_importance_threshold(self) -> float:
        """Initial threshold for evidence auto-selected into the report draft."""
        return float(self._config.get("pipeline", {}).get("selected_importance_threshold", 6.5))

    @property
    def candidate_importance_threshold(self) -> float:
        """Initial threshold for evidence kept visible as a review candidate."""
        return float(self._config.get("pipeline", {}).get("candidate_importance_threshold", 4.0))

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


def load_categories_from_xlsx() -> List[Dict[str, Any]]:
    """
    Load merged CM editor categories from the Outlook and flat Q&A workbooks.

    Returns:
        List of normalized category dictionaries for both report sections.
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    category_dir = os.path.join(current_dir, "config", "categories")
    workbook_specs = [
        ("outlook_categories.xlsx", "Outlook"),
        ("qa_categories.xlsx", "Q&A"),
    ]
    required_columns = ["transcript_sections", "category_name", "category_description"]
    optional_columns = ["example_1", "example_2", "example_3", "category_group"]
    categories = []

    for file_name, report_section in workbook_specs:
        xlsx_path = os.path.join(category_dir, file_name)
        if not os.path.exists(xlsx_path):
            raise FileNotFoundError(f"Categories file not found: {xlsx_path}")

        try:
            all_sheets = pd.ExcelFile(xlsx_path).sheet_names
            df = pd.read_excel(xlsx_path, sheet_name=0)
        except Exception as exc:
            logger.error(
                "Failed to read category configuration file",
                xlsx_path=xlsx_path,
                error=str(exc),
            )
            raise RuntimeError(f"Failed to read categories from {xlsx_path}: {exc}") from exc

        if len(all_sheets) > 1:
            logger.warning(
                "Categories workbook contains multiple sheets; only the first is loaded",
                xlsx_path=xlsx_path,
                loaded_sheet=all_sheets[0],
                ignored_sheets=all_sheets[1:],
            )

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in {file_name}: {missing_columns}")

        for col in optional_columns:
            if col not in df.columns:
                df[col] = ""

        for idx, row in df.iterrows():
            category_name = str(row["category_name"]).strip() if pd.notna(row["category_name"]) else ""
            if category_name.lower() == "category":
                continue

            for field in required_columns:
                if pd.isna(row[field]) or str(row[field]).strip() == "":
                    raise ValueError(f"Missing value for '{field}' in {file_name} (row {idx + 2})")

            transcript_sections = str(row["transcript_sections"]).strip()
            if transcript_sections not in VALID_SECTION_KEYS:
                raise ValueError(
                    f"Invalid transcript_sections '{transcript_sections}' "
                    f"in {file_name} (row {idx + 2}). Must be one of: {VALID_SECTION_KEYS}"
                )

            categories.append(
                {
                    "transcript_sections": transcript_sections,
                    "report_section": report_section,
                    "category_name": category_name,
                    "category_description": str(row["category_description"]).strip(),
                    "example_1": str(row["example_1"]).strip() if pd.notna(row["example_1"]) else "",
                    "example_2": str(row["example_2"]).strip() if pd.notna(row["example_2"]) else "",
                    "example_3": str(row["example_3"]).strip() if pd.notna(row["example_3"]) else "",
                    "category_group": str(row["category_group"]).strip() if pd.notna(row["category_group"]) else "",
                }
            )

    if not categories:
        raise ValueError("No categories loaded for cm_readthrough_editor")

    # Bucket ids are positional (`bucket_0`, `bucket_1`, ...), so two rows with
    # the same `category_name` do not actually collide at the id level. They
    # only become ambiguous when they appear in the *same* report section,
    # since that is the UI grouping a user sees. Allow the same name (e.g.
    # "Expenses") to be reused across different report sections — Outlook
    # vs Q&A — while still rejecting same-name,
    # same-section rows that would produce two indistinguishable groups.
    seen_names: Dict[tuple, int] = {}
    for idx, category in enumerate(categories, start=2):  # +2 for header + 1-based row
        key = (
            category["report_section"].strip().lower(),
            category["category_name"].strip().lower(),
        )
        if key in seen_names:
            raise ValueError(
                f"Duplicate category_name '{category['category_name']}' "
                f"in report_section '{category['report_section']}' "
                f"in {file_name} (rows {seen_names[key]} and {idx})"
            )
        seen_names[key] = idx

    logger.info(
        "Loaded category configuration",
        categories=len(categories),
        report_sections=sorted({category["report_section"] for category in categories}),
    )
    return categories


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


INTERACTIVE_REPORT_METADATA: Dict[str, str] = {
    "report_name": "Capital Markets Readthrough Editor",
    "report_description": (
        "Interactive HTML capital markets readthrough editor covering multiple "
        "peer-bank transcripts with transcript review and report drafting controls."
    ),
    "report_type": "cm_readthrough_editor",
}


def _all_banks_from_config() -> List[Dict[str, Any]]:
    """Return monitored institutions in config order."""
    institutions = _load_monitored_institutions()
    return [
        {
            "bank_id": inst["id"],
            "bank_name": inst["name"],
            "bank_symbol": inst["symbol"],
            "full_ticker": inst.get("full_ticker"),
            "bank_type": inst["type"],
            "path_safe_name": inst.get("path_safe_name"),
        }
        for inst in institutions.values()
    ]


def _resolve_requested_banks(bank_identifier: Optional[str]) -> List[Dict[str, Any]]:
    """Resolve either one requested bank or the full monitored bank universe."""
    if bank_identifier:
        return [get_bank_info_from_config(bank_identifier)]
    return _all_banks_from_config()


def _scope_slug(requested_banks: List[Dict[str, Any]], requested_all_banks: bool) -> str:
    """Build a filename-safe scope label."""
    if requested_all_banks:
        return "all_banks"
    if len(requested_banks) == 1:
        return requested_banks[0].get("full_ticker") or requested_banks[0]["bank_symbol"]
    return f"{len(requested_banks)}_banks"


def _count_bank_findings(bank_data: Dict[str, Any]) -> int:
    """Count extracted findings for bank-selector badges."""
    count = 0
    for block in bank_data.get("md_blocks", []):
        count += sum(
            1
            for sentence in block.get("sentences", [])
            if sentence.get("status") in {"selected", "candidate"}
        )
    for conversation in bank_data.get("qa_conversations", []):
        render_mode = conversation.get("render_mode", "answer")
        findings = (
            conversation.get("question_sentences", [])
            if render_mode == "question"
            else conversation.get("answer_sentences", [])
        )
        count += sum(
            1
            for sentence in findings
            if sentence.get("status") in {"selected", "candidate"}
        )
    return count


def _count_banks_with_selected_findings(banks_data: Dict[str, Dict[str, Any]]) -> int:
    """Count banks that have at least one selected report finding."""
    selected_banks = 0
    for bank_data in banks_data.values():
        has_selected = False
        for block in bank_data.get("md_blocks", []):
            if any(sentence.get("status") == "selected" for sentence in block.get("sentences", [])):
                has_selected = True
                break
        if not has_selected:
            for conversation in bank_data.get("qa_conversations", []):
                render_mode = conversation.get("render_mode", "answer")
                findings = (
                    conversation.get("question_sentences", [])
                    if render_mode == "question"
                    else conversation.get("answer_sentences", [])
                )
                if any(sentence.get("status") == "selected" for sentence in findings):
                    has_selected = True
                    break
        if has_selected:
            selected_banks += 1
    return selected_banks


def _generate_interactive_report(
    report_state: Dict[str, Any],
    etl_context: Dict[str, Any],
    min_importance: float,
) -> tuple[str, str]:
    """Write the interactive HTML report to the ETL output directory."""
    quarter = etl_context["quarter"]
    fiscal_year = etl_context["fiscal_year"]

    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
    os.makedirs(output_dir, exist_ok=True)

    scope_slug = etl_context["scope_slug"]
    filename_base = f"CM_Readthrough_Editor_{fiscal_year}_{quarter}_{scope_slug}"
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

    logger.info(
        "Saved interactive HTML report",
        filepath=filepath,
        scope=scope_slug,
        fiscal_year=fiscal_year,
        quarter=quarter,
    )
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
    quarter = etl_context["quarter"]
    fiscal_year = etl_context["fiscal_year"]
    execution_id = etl_context["execution_id"]

    report_metadata = INTERACTIVE_REPORT_METADATA
    generation_timestamp = datetime.now()

    # `get_connection` uses `engine.begin()`, so the DELETE+INSERT below
    # run inside a single transaction that commits on exit (or rolls back
    # on exception). No explicit commit is required.
    async with get_connection() as conn:
        await conn.execute(
            text(
                """
                DELETE FROM aegis_reports
                WHERE fiscal_year = :fiscal_year
                  AND quarter = :quarter
                  AND report_type = :report_type
                """
            ),
            {
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
                "bank_id": None,
                "bank_name": None,
                "bank_symbol": None,
                "fiscal_year": fiscal_year,
                "quarter": quarter,
                "local_filepath": filepath,
                "s3_document_name": html_filename,
                "s3_pdf_name": None,
                "generation_date": generation_timestamp,
                "generated_by": "cm_readthrough_editor_etl",
                "execution_id": execution_id,
                "metadata": json.dumps(
                    {
                        "output_format": "html",
                        "scope": etl_context["scope_slug"],
                        "banks_requested": etl_context["banks_requested"],
                        "banks_included": etl_context["banks_included"],
                        "banks_with_findings": etl_context["banks_with_findings"],
                        "skipped_banks": etl_context.get("skipped_banks", []),
                        "section_subtitles": etl_context.get("section_subtitles", {}),
                        "categories_processed": total_categories,
                        "categories_included": included_categories,
                        "categories_rejected": total_categories - included_categories,
                    }
                ),
            },
        )

    logger.info(
        "Saved report metadata to database",
        filepath=filepath,
        total_categories=total_categories,
        included_categories=included_categories,
        banks_requested=etl_context["banks_requested"],
        banks_included=etl_context["banks_included"],
        fiscal_year=fiscal_year,
        quarter=quarter,
    )


async def generate_cm_readthrough_editor(  # pylint: disable=too-many-statements
    bank_name: Optional[str], fiscal_year: int, quarter: str
) -> CMReadthroughEditorResult:
    """
    Generate an interactive HTML CM readthrough editor report.

    Args:
        bank_name: Optional bank ID/name/symbol filter. If omitted, process all monitored banks.
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")

    Returns:
        CMReadthroughEditorResult with filepath and category counts

    Raises:
        CMReadthroughEditorUserError: For expected errors (bad input, no data)
        CMReadthroughEditorSystemError: For unexpected system/infrastructure errors
    """
    marks = [("start", time.monotonic())]
    execution_id = str(uuid.uuid4())
    completed = False
    requested_banks = _resolve_requested_banks(bank_name)
    requested_all_banks = bank_name is None
    logger.info(
        "Starting cm readthrough editor ETL",
        requested_banks=len(requested_banks),
        bank_filter=bank_name,
        fiscal_year=fiscal_year,
        quarter=quarter,
    )

    nas_conn = None
    try:
        ssl_config = setup_ssl()
        auth_config = await setup_authentication(execution_id, ssl_config)

        if not auth_config["success"]:
            error_msg = f"Authentication failed: {auth_config.get('error', 'Unknown error')}"
            logger.error("Authentication setup failed", error=error_msg)
            raise CMReadthroughEditorSystemError(error_msg)

        context = {
            "execution_id": execution_id,
            "auth_config": auth_config,
            "ssl_config": ssl_config,
            "_llm_costs": [],
            "suppress_llm_console_logs": True,
        }
        logger.info(
            "Runtime setup complete",
            auth_method=auth_config.get("method", "unknown"),
            max_concurrent_banks=etl_config.max_concurrent_banks,
        )
        marks.append(("setup", time.monotonic()))

        categories = load_categories_from_xlsx()
        min_importance = etl_config.min_importance
        selected_importance_threshold = etl_config.selected_importance_threshold
        candidate_importance_threshold = etl_config.candidate_importance_threshold

        nas_conn = get_nas_connection()
        qa_boundary_llm_params = etl_config.get_stage_params("qa_boundary")
        md_llm_params = etl_config.get_stage_params("outlook_extraction")
        qa_llm_params = etl_config.get_stage_params("qa_extraction")
        subtitle_llm_params = etl_config.get_stage_params("subtitle_generation")
        semaphore = asyncio.Semaphore(etl_config.max_concurrent_banks)
        nas_lock = asyncio.Lock()
        skipped_banks: List[Dict[str, Any]] = []
        banks_data: Dict[str, Dict[str, Any]] = {}

        async def _process_bank(bank_info: Dict[str, Any]) -> Dict[str, Any]:
            async with semaphore:
                ticker = bank_info.get("full_ticker") or bank_info["bank_symbol"]
                async with nas_lock:
                    xml_result = await asyncio.to_thread(
                        find_transcript_xml,
                        nas_conn,
                        bank_info,
                        fiscal_year,
                        quarter,
                    )
                if xml_result is None:
                    return {
                        "ticker": ticker,
                        "bank_name": bank_info["bank_name"],
                        "status": "skipped",
                        "reason": "no transcript XML found on NAS",
                    }

                parsed_transcript = parse_transcript_xml(xml_result.xml_bytes)
                if parsed_transcript is None:
                    return {
                        "ticker": ticker,
                        "bank_name": bank_info["bank_name"],
                        "status": "skipped",
                        "reason": "transcript XML failed to parse",
                        "transcript_path": xml_result.file_path,
                    }

                md_raw_blocks, qa_raw_blocks = extract_raw_blocks(parsed_transcript, ticker)
                if not md_raw_blocks and not qa_raw_blocks:
                    return {
                        "ticker": ticker,
                        "bank_name": bank_info["bank_name"],
                        "status": "skipped",
                        "reason": "transcript contained no MD or QA speaker blocks",
                        "transcript_path": xml_result.file_path,
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
                    selected_importance_threshold=selected_importance_threshold,
                    candidate_importance_threshold=candidate_importance_threshold,
                )
                bank_data["transcript_path"] = xml_result.file_path
                bank_data["finding_count"] = _count_bank_findings(bank_data)
                return {
                    "ticker": ticker,
                    "bank_name": bank_info["bank_name"],
                    "status": "ok",
                    "bank_data": bank_data,
                }

        raw_results = await asyncio.gather(
            *[_process_bank(bank_info) for bank_info in requested_banks],
            return_exceptions=True,
        )
        marks.append(("classification", time.monotonic()))

        for bank_info, result in zip(requested_banks, raw_results):
            if isinstance(result, Exception):
                logger.error(
                    "Bank processing failed",
                    bank=bank_info["bank_name"],
                    ticker=bank_info.get("full_ticker") or bank_info["bank_symbol"],
                    error=str(result),
                    exc_info=True,
                )
                skipped_banks.append(
                    {
                        "ticker": bank_info.get("full_ticker") or bank_info["bank_symbol"],
                        "bank_name": bank_info["bank_name"],
                        "reason": str(result),
                    }
                )
                continue

            if result["status"] != "ok":
                skipped_banks.append(
                    {
                        "ticker": result["ticker"],
                        "bank_name": result["bank_name"],
                        "reason": result["reason"],
                    }
                )
                logger.info(
                    "Skipping bank from consolidated CM editor",
                    bank=result["bank_name"],
                    ticker=result["ticker"],
                    reason=result["reason"],
                )
                continue

            bank_data = result["bank_data"]
            banks_data[bank_data["ticker"]] = bank_data

        if not banks_data:
            scope_label = (
                f"bank filter '{bank_name}'" if bank_name else "the monitored bank set"
            )
            raise CMReadthroughEditorUserError(
                f"No usable transcript XML was found for {scope_label} in {quarter} {fiscal_year}."
            )

        section_subtitles = await generate_report_section_subtitles(
            banks_data=banks_data,
            context=context,
            llm_params=subtitle_llm_params,
        )
        report_state = build_interactive_report_state(
            banks_data=banks_data,
            categories=categories,
            fiscal_year=fiscal_year,
            fiscal_quarter=quarter,
            min_importance=min_importance,
            bucket_headlines={},
            config_review_by_bank={},
            section_subtitles=section_subtitles,
            cm_main_title=(
                f"Read Through For Capital Markets: {quarter}/{str(fiscal_year)[2:]} Select Banks"
            ),
            report_title="Capital Markets Readthrough",
        )
        included_categories = count_included_categories(banks_data, min_importance)
        total_categories = len(categories)
        banks_with_findings = _count_banks_with_selected_findings(banks_data)
        marks.append(("state_built", time.monotonic()))

        etl_context = {
            "quarter": quarter,
            "fiscal_year": fiscal_year,
            "context": context,
            "execution_id": execution_id,
            "scope_slug": _scope_slug(requested_banks, requested_all_banks),
            "banks_requested": len(requested_banks),
            "banks_included": len(banks_data),
            "banks_with_findings": banks_with_findings,
            "skipped_banks": skipped_banks,
            "section_subtitles": section_subtitles,
        }

        filepath, html_filename = _generate_interactive_report(
            report_state=report_state,
            etl_context=etl_context,
            min_importance=min_importance,
        )
        marks.append(("document", time.monotonic()))

        if requested_all_banks:
            await _save_interactive_report_to_database(
                filepath=filepath,
                html_filename=html_filename,
                total_categories=total_categories,
                included_categories=included_categories,
                etl_context=etl_context,
            )
            marks.append(("save", time.monotonic()))
        else:
            logger.info(
                "Skipping database persistence for filtered CM editor build",
                scope=etl_context["scope_slug"],
                banks_requested=len(requested_banks),
            )

        cost_summary = _get_total_llm_cost(context)
        logger.info(
            "CM readthrough editor ETL complete",
            requested_banks=len(requested_banks),
            included_banks=len(banks_data),
            skipped_banks=len(skipped_banks),
            banks_with_findings=banks_with_findings,
            included_categories=included_categories,
            total_categories=total_categories,
            llm_calls=cost_summary["llm_calls"],
            total_tokens=cost_summary["total_tokens"],
            total_cost=cost_summary["total_cost"],
            transcript_source="nas",
            **_timing_summary(marks),
        )
        completed = True

        return CMReadthroughEditorResult(
            filepath=filepath,
            total_categories=total_categories,
            included_categories=included_categories,
            banks_requested=len(requested_banks),
            banks_included=len(banks_data),
            banks_with_findings=banks_with_findings,
            skipped_banks=len(skipped_banks),
            total_cost=cost_summary["total_cost"],
            total_tokens=cost_summary["total_tokens"],
        )

    except CMReadthroughEditorError:
        raise
    except ValueError as exc:
        # Data-quality / user-input failures (bad XLSX rows, invalid args).
        logger.error("CM readthrough editor failed (user error)", error=str(exc))
        raise CMReadthroughEditorUserError(str(exc)) from exc
    except RuntimeError as exc:
        # Wrapped infrastructure/LLM failures (NAS read, XLSX parse, LLM retries).
        logger.error("CM readthrough editor failed (system error)", error=str(exc))
        raise CMReadthroughEditorSystemError(str(exc)) from exc
    except Exception as exc:
        error_msg = f"Error generating cm readthrough editor report: {str(exc)}"
        logger.error(
            "CM readthrough editor crashed",
            error=str(exc),
            exc_info=True,
        )
        raise CMReadthroughEditorSystemError(error_msg) from exc
    finally:
        # Always report LLM spend so partial-run costs are visible on failure.
        partial = _get_total_llm_cost(context) if "context" in locals() else None
        if not completed and partial and partial.get("llm_calls", 0) > 0:
            logger.info(
                "Partial LLM spend before exit",
                llm_calls=partial["llm_calls"],
                total_tokens=partial["total_tokens"],
                total_cost=partial["total_cost"],
            )
        if nas_conn is not None:
            try:
                nas_conn.close()
            except Exception:  # pylint: disable=broad-except
                pass


async def preflight_cm_readthrough_editor(
    bank_name: Optional[str], fiscal_year: int, quarter: str
) -> Dict[str, Any]:
    """Validate requested banks' NAS/XML availability without calling the LLM."""
    requested_banks = _resolve_requested_banks(bank_name)
    summary: Dict[str, Any] = {
        "bank_filter": bank_name,
        "fiscal_year": fiscal_year,
        "quarter": quarter,
        "requested_banks": len(requested_banks),
        "ok_banks": 0,
        "failed_banks": 0,
        "statuses": [],
    }
    nas_conn = None
    try:
        categories = load_categories_from_xlsx()
        summary["categories_loaded"] = len(categories)
        nas_conn = get_nas_connection()

        for bank_info in requested_banks:
            status: Dict[str, Any] = {
                "bank_name": bank_info["bank_name"],
                "bank_symbol": bank_info["bank_symbol"],
                "bank_type": bank_info["bank_type"],
                "ok": False,
                "failure": None,
            }
            try:
                xml_result = find_transcript_xml(nas_conn, bank_info, fiscal_year, quarter)
                if xml_result is None:
                    status["failure"] = "no transcript XML found on NAS"
                else:
                    status["transcript_path"] = xml_result.file_path
                    parsed_transcript = parse_transcript_xml(xml_result.xml_bytes)
                    if parsed_transcript is None:
                        status["failure"] = "transcript XML failed to parse"
                    else:
                        ticker = bank_info.get("full_ticker") or bank_info["bank_symbol"]
                        md_raw_blocks, qa_raw_blocks = extract_raw_blocks(parsed_transcript, ticker)
                        status["md_blocks"] = len(md_raw_blocks)
                        status["qa_blocks"] = len(qa_raw_blocks)
                        if not md_raw_blocks and not qa_raw_blocks:
                            status["failure"] = "transcript contained no MD or QA blocks"
                        else:
                            status["ok"] = True
            except Exception as exc:  # pylint: disable=broad-except
                status["failure"] = f"{type(exc).__name__}: {exc}"
            summary["statuses"].append(status)

        summary["ok_banks"] = sum(1 for status in summary["statuses"] if status["ok"])
        summary["failed_banks"] = len(summary["statuses"]) - summary["ok_banks"]
        return summary
    except Exception as exc:  # pylint: disable=broad-except
        fatal_failure = f"{type(exc).__name__}: {exc}"
        summary["fatal_failure"] = fatal_failure
        if not summary["statuses"]:
            summary["statuses"] = [
                {
                    "bank_name": bank_info["bank_name"],
                    "bank_symbol": bank_info["bank_symbol"],
                    "bank_type": bank_info["bank_type"],
                    "ok": False,
                    "failure": fatal_failure,
                }
                for bank_info in requested_banks
            ]
        summary["ok_banks"] = sum(1 for status in summary["statuses"] if status["ok"])
        summary["failed_banks"] = len(summary["statuses"]) - summary["ok_banks"]
        return summary
    finally:
        if nas_conn is not None:
            try:
                nas_conn.close()
            except Exception:  # pylint: disable=broad-except
                pass


def main():
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Generate consolidated interactive CM readthrough editor reports",
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

    parser.add_argument(
        "--bank",
        help="Optional bank ID, name, or symbol filter. Omit to process all monitored banks.",
    )
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

    if args.year is None or not args.quarter:
        parser.error("--year and --quarter are required unless using the benchmark command")

    if args.preflight:
        scope_label = args.bank or "all monitored banks"
        print(f"\n🔍 Preflight check: {scope_label} {args.quarter} {args.year}\n")
        status = asyncio.run(
            preflight_cm_readthrough_editor(bank_name=args.bank, fiscal_year=args.year, quarter=args.quarter)
        )
        print(f"  requested_banks: {status['requested_banks']}")
        print(f"  ok_banks: {status['ok_banks']}")
        print(f"  failed_banks: {status['failed_banks']}")
        for bank_status in status["statuses"]:
            bank_label = f"{bank_status['bank_symbol']} ({bank_status['bank_name']})"
            outcome = "OK" if bank_status["ok"] else f"FAIL: {bank_status['failure']}"
            print(f"  - {bank_label}: {outcome}")
        sys.exit(0 if status["failed_banks"] == 0 else 1)

    try:
        result = asyncio.run(
            generate_cm_readthrough_editor(bank_name=args.bank, fiscal_year=args.year, quarter=args.quarter)
        )
        print(
            f"✅ Complete: {result.filepath}\n"
            f"   Banks: {result.banks_included}/{result.banks_requested} included "
            f"({result.banks_with_findings} with selected findings, {result.skipped_banks} skipped)\n"
            f"   Categories: {result.included_categories}/{result.total_categories} included\n"
            f"   LLM cost: ${result.total_cost:.4f}, Tokens: {result.total_tokens:,}"
        )
    except CMReadthroughEditorUserError as e:
        print(f"⚠️ {e}", file=sys.stderr)
        sys.exit(1)
    except CMReadthroughEditorError as e:
        print(f"❌ {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
