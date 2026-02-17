"""
CM Readthrough ETL Script - Generates capital markets readthrough reports across multiple banks.

This redesigned script uses an 8-phase pipeline:
1. Outlook extraction from full transcripts (parallel across banks)
2. Q&A Section 2 extraction - 4 categories (parallel across banks)
3. Q&A Section 3 extraction - 2 categories (parallel across banks)
4. Aggregation and sorting (3 result sets)
5. Subtitle generation - Section 1 (Outlook)
6. Subtitle generation - Section 2 (Q&A themes)
7. Subtitle generation - Section 3 (Q&A themes)
8. Batch formatting and document generation (3 sections)

Document Structure:
- Section 1: Outlook statements (2-column table)
- Section 2: Q&A for Global Markets, Risk Management, Corporate Banking,
  Regulatory Changes (3-column table)
- Section 3: Q&A for Investment Banking/M&A, Transaction Banking (3-column table)

Usage:
    python -m aegis.etls.cm_readthrough.main --year 2024 --quarter Q3
    python -m aegis.etls.cm_readthrough.main --year 2024 --quarter Q3 --output cm_readthrough.docx
"""

import argparse
import asyncio
import functools
import json
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type

import pandas as pd
import yaml
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from aegis.etls.cm_readthrough.document_converter import create_combined_document
from aegis.etls.cm_readthrough.transcript_utils import (
    SECTIONS_KEY_MD,
    SECTIONS_KEY_QA,
    SECTIONS_KEY_ALL,
    retrieve_full_section,
    format_full_section_chunks,
)
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.llm_connector import complete_with_tools
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from aegis.utils.settings import config
from aegis.utils.prompt_loader import load_prompt_from_db
from aegis.utils.sql_prompt import postgresql_prompts

setup_logging()
logger = get_logger()

# --- Module-level defaults (used as fallbacks if YAML config keys are missing) ---
MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 10.0
MAX_CONCURRENT_BANKS = 5
MAX_CONCURRENT_SUBTITLE_GENERATION = 3


class CMReadthroughError(Exception):
    """Base exception for CM readthrough ETL errors."""


class CMReadthroughSystemError(CMReadthroughError):
    """Unexpected system/infrastructure error."""


class CMReadthroughUserError(CMReadthroughError):
    """Expected user-facing error (bad input, no data, auth issues)."""


@dataclass
class CMReadthroughResult:
    """Successful CM readthrough generation result."""

    filepath: str
    execution_id: str
    banks_processed: int
    banks_with_outlook: int
    banks_with_section2: int
    banks_with_section3: int
    total_cost: float = 0.0
    total_tokens: int = 0


class OutlookStatement(BaseModel):
    """Single outlook statement extracted from transcript."""

    category: str
    category_group: str = ""
    statement: str
    relevance_score: int = Field(default=5, ge=1, le=10)
    is_new_category: bool = False


class OutlookExtractionResponse(BaseModel):
    """Validated tool response for outlook extraction."""

    has_content: bool
    statements: List[OutlookStatement] = Field(default_factory=list)


class QAQuestion(BaseModel):
    """Single analyst question extracted from Q&A."""

    category: str
    verbatim_question: str
    analyst_name: str
    analyst_firm: str
    is_new_category: bool = False


class QAExtractionResponse(BaseModel):
    """Validated tool response for Q&A extraction."""

    has_content: bool
    questions: List[QAQuestion] = Field(default_factory=list)


class SubtitleResponse(BaseModel):
    """Validated tool response for subtitle generation."""

    subtitle: str


class FormattedOutlookItem(BaseModel):
    """Single formatted outlook statement from batch formatter."""

    category: str
    category_group: str = ""
    statement: str
    relevance_score: int = 0
    formatted_quote: Optional[str] = None
    formatted_statement: Optional[str] = None
    is_new_category: bool = False


class BatchFormattingResponse(BaseModel):
    """Validated tool response for batch formatting."""

    formatted_quotes: Dict[str, List[FormattedOutlookItem]] = Field(default_factory=dict)


class DuplicateQuestion(BaseModel):
    """A single identified duplicate question from Q&A deduplication."""

    bank: str
    section: str  # "section2" or "section3"
    category: str
    question_index: int
    duplicate_of_section: str
    duplicate_of_category: str
    duplicate_of_question_index: int
    reasoning: str = ""


class QADeduplicationResponse(BaseModel):
    """Validated tool response for Q&A deduplication."""

    analysis_notes: str = ""
    duplicate_questions: List[DuplicateQuestion] = Field(default_factory=list)


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
            task_key: Task identifier (e.g., "outlook_extraction", "qa_extraction")

        Returns:
            max_tokens value for the task
        """
        max_tokens_config = self._config.get("llm", {}).get("max_tokens", {})
        if isinstance(max_tokens_config, int):
            return max_tokens_config
        return max_tokens_config.get(task_key, max_tokens_config.get("default", 32768))

    @property
    def max_concurrent_banks(self) -> int:
        """Get the maximum concurrent banks parameter."""
        return self._config.get("concurrency", {}).get("max_concurrent_banks", MAX_CONCURRENT_BANKS)

    @property
    def max_concurrent_subtitle_generation(self) -> int:
        """Get max concurrent subtitle generation tasks."""
        return self._config.get("concurrency", {}).get(
            "max_concurrent_subtitle_generation", MAX_CONCURRENT_SUBTITLE_GENERATION
        )

    @property
    def max_retries(self) -> int:
        """Get the maximum number of LLM call retries."""
        return self._config.get("retry", {}).get("max_retries", MAX_RETRIES)

    @property
    def retry_base_delay(self) -> float:
        """Get base delay in seconds for retry backoff."""
        return self._config.get("retry", {}).get("base_delay", RETRY_BASE_DELAY)

    @property
    def retry_max_delay(self) -> float:
        """Get maximum delay in seconds for retry backoff."""
        return self._config.get("retry", {}).get("max_delay", RETRY_MAX_DELAY)


etl_config = ETLConfig(os.path.join(os.path.dirname(__file__), "config", "config.yaml"))


@functools.lru_cache(maxsize=1)
def _load_monitored_institutions() -> Dict[str, Dict[str, Any]]:
    """
    Load and cache monitored institutions configuration.

    Returns:
        Dictionary mapping ticker to institution details (id, name, type, path_safe_name)
    """
    config_path = os.path.join(os.path.dirname(__file__), "config", "monitored_institutions.yaml")
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_monitored_institutions() -> List[Dict[str, Any]]:
    """
    Get list of monitored institutions.

    Returns:
        List of institution dictionaries with bank_id, bank_symbol, bank_name
    """
    institutions_dict = _load_monitored_institutions()
    institutions = []
    for ticker, info in institutions_dict.items():
        institutions.append(
            {
                "bank_id": info["id"],
                "bank_symbol": ticker,
                "bank_name": info["name"],
                "type": info.get("type", ""),
                "path_safe_name": info.get("path_safe_name", ""),
            }
        )
    return institutions


def _sanitize_for_prompt(text: str) -> str:
    """
    Escape curly braces in text for safe use in .format() templates.

    Prevents KeyError/IndexError when transcript/category content contains { or }.
    """
    return str(text).replace("{", "{{").replace("}", "}}")


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


def _accumulate_llm_cost(context: Dict[str, Any], metrics: Dict[str, Any]) -> None:
    """
    Accumulate LLM cost and token usage from response metrics.

    Appends to context["_llm_costs"] for later aggregation.
    """
    if "_llm_costs" not in context:
        context["_llm_costs"] = []
    context["_llm_costs"].append(
        {
            "prompt_tokens": metrics.get("prompt_tokens", 0),
            "completion_tokens": metrics.get("completion_tokens", 0),
            "total_cost": metrics.get("total_cost", 0),
        }
    )


def _get_total_llm_cost(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Aggregate accumulated LLM cost and tokens.

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


def _extract_tool_args(response: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and decode first tool call arguments from an LLM response."""
    tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls", [])
    if not tool_calls:
        raise RuntimeError("LLM response did not include a required tool call")
    raw_args = tool_calls[0]["function"]["arguments"]
    return json.loads(raw_args)


async def _complete_with_tools_validated(
    *,
    messages: List[Dict[str, str]],
    tools: List[Dict[str, Any]],
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
    response_model: Type[BaseModel],
    stage: str,
    allow_default_on_failure: bool = False,
    default_value: Optional[Any] = None,
) -> Any:
    """
    Execute complete_with_tools with retries and schema validation.

    Args:
        messages: Chat messages
        tools: Function-calling tools
        context: Execution context
        llm_params: LLM parameters
        response_model: Pydantic model class for tool args
        stage: Stage name used in logs
        allow_default_on_failure: If True, return default_value after max retries
        default_value: Default return value when failures are allowed
    """
    execution_id = context.get("execution_id")

    for attempt in range(etl_config.max_retries):
        try:
            response = await complete_with_tools(
                messages=messages,
                tools=tools,
                context=context,
                llm_params=llm_params,
            )
            metrics = response.get("metrics", {})
            _accumulate_llm_cost(context, metrics)
            logger.info(
                "etl.cm_readthrough.llm_usage",
                execution_id=execution_id,
                stage=stage,
                prompt_tokens=metrics.get("prompt_tokens", 0),
                completion_tokens=metrics.get("completion_tokens", 0),
                total_cost=metrics.get("total_cost", 0),
                response_time=metrics.get("response_time", 0),
            )

            raw_data = _extract_tool_args(response)
            return response_model.model_validate(raw_data).model_dump()

        except (KeyError, IndexError, json.JSONDecodeError, TypeError, ValidationError) as e:
            logger.warning(
                "etl.cm_readthrough.llm_parse_error",
                execution_id=execution_id,
                stage=stage,
                attempt=attempt + 1,
                error=str(e),
            )
            continue
        except Exception as e:
            logger.error(
                "etl.cm_readthrough.llm_stage_error",
                execution_id=execution_id,
                stage=stage,
                attempt=attempt + 1,
                error=str(e),
            )
            if attempt < etl_config.max_retries - 1:
                delay = min(etl_config.retry_base_delay * (2**attempt), etl_config.retry_max_delay)
                delay += random.uniform(0, 0.5 * delay)
                await asyncio.sleep(delay)

    message = (
        f"LLM stage '{stage}' failed after {etl_config.max_retries} attempts "
        f"(parse/validation/tool-call/transport failure)"
    )
    if allow_default_on_failure:
        logger.warning(
            "etl.cm_readthrough.llm_stage_defaulted",
            execution_id=execution_id,
            stage=stage,
            reason=message,
        )
        return default_value
    raise RuntimeError(message)


def _load_prompt_bundle(execution_id: str) -> Dict[str, Dict[str, Any]]:
    """
    Load all CM readthrough prompt payloads once per run.

    Returns:
        Mapping with keys: outlook_extraction, qa_extraction_dynamic,
        subtitle_generation, batch_formatting
    """
    return {
        "outlook_extraction": load_prompt_from_db(
            layer="cm_readthrough_etl",
            name="outlook_extraction",
            compose_with_globals=False,
            available_databases=None,
            execution_id=execution_id,
        ),
        "qa_extraction_dynamic": load_prompt_from_db(
            layer="cm_readthrough_etl",
            name="qa_extraction_dynamic",
            compose_with_globals=False,
            available_databases=None,
            execution_id=execution_id,
        ),
        "subtitle_generation": load_prompt_from_db(
            layer="cm_readthrough_etl",
            name="subtitle_generation",
            compose_with_globals=False,
            available_databases=None,
            execution_id=execution_id,
        ),
        "batch_formatting": load_prompt_from_db(
            layer="cm_readthrough_etl",
            name="batch_formatting",
            compose_with_globals=False,
            available_databases=None,
            execution_id=execution_id,
        ),
        "qa_deduplication": load_prompt_from_db(
            layer="cm_readthrough_etl",
            name="qa_deduplication",
            compose_with_globals=False,
            available_databases=None,
            execution_id=execution_id,
        ),
    }


def load_outlook_categories(execution_id: str) -> List[Dict[str, Any]]:
    """
    Load outlook categories from Excel file.

    Args:
        execution_id: Execution ID for logging

    Returns:
        List of category dictionaries with transcript_sections, category_name, category_description,
        example_1, example_2, example_3
    """
    file_name = "outlook_categories.xlsx"
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

        # Convert to list of dicts, ensuring all required fields are non-empty
        categories = []
        has_category_group = "category_group" in df.columns
        for idx, row in df.iterrows():
            for field in required_columns:
                if pd.isna(row[field]) or str(row[field]).strip() == "":
                    raise ValueError(f"Missing value for '{field}' in {file_name} (row {idx + 2})")

            category = {
                "transcript_sections": str(row["transcript_sections"]).strip(),
                "category_name": str(row["category_name"]).strip(),
                "category_description": str(row["category_description"]).strip(),
                "example_1": str(row["example_1"]).strip() if pd.notna(row["example_1"]) else "",
                "example_2": str(row["example_2"]).strip() if pd.notna(row["example_2"]) else "",
                "example_3": str(row["example_3"]).strip() if pd.notna(row["example_3"]) else "",
                "category_group": (
                    str(row["category_group"]).strip()
                    if has_category_group and pd.notna(row.get("category_group"))
                    else ""
                ),
            }
            categories.append(category)

        if not categories:
            raise ValueError(f"No categories in {file_name}")

        logger.info(
            "etl.cm_readthrough.categories_loaded",
            execution_id=execution_id,
            file_name=file_name,
            num_categories=len(categories),
        )
        return categories

    except Exception as e:
        error_msg = f"Failed to load outlook categories from {xlsx_path}: {str(e)}"
        logger.error(
            "etl.cm_readthrough.categories_load_error",
            execution_id=execution_id,
            xlsx_path=xlsx_path,
            error=str(e),
        )
        raise RuntimeError(error_msg) from e


def load_qa_market_volatility_regulatory_categories(execution_id: str) -> List[Dict[str, Any]]:
    """
    Load Q&A market volatility/regulatory categories from Excel file.

    Args:
        execution_id: Execution ID for logging

    Returns:
        List of category dictionaries with standardized 6-column format
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    xlsx_path = os.path.join(
        current_dir, "config", "categories", "qa_market_volatility_regulatory_categories.xlsx"
    )

    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(f"Categories file not found: {xlsx_path}")

    try:
        df = pd.read_excel(xlsx_path, sheet_name=0)

        # Required columns for standard format
        required_columns = ["transcript_sections", "category_name", "category_description"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(
                f"Missing required columns in Excel file: {missing_columns}. "
                f"Required columns: {required_columns}"
            )

        # Optional example columns
        optional_columns = ["example_1", "example_2", "example_3"]
        for col in optional_columns:
            if col not in df.columns:
                df[col] = ""  # Add empty column if not present

        categories = []
        for idx, row in df.iterrows():
            for field in required_columns:
                if pd.isna(row[field]) or str(row[field]).strip() == "":
                    raise ValueError(
                        f"Missing value for '{field}' in "
                        f"qa_market_volatility_regulatory_categories.xlsx (row {idx + 2})"
                    )

            category = {
                "transcript_sections": str(row["transcript_sections"]).strip(),
                "category_name": str(row["category_name"]).strip(),
                "category_description": str(row["category_description"]).strip(),
                "example_1": str(row["example_1"]).strip() if pd.notna(row["example_1"]) else "",
                "example_2": str(row["example_2"]).strip() if pd.notna(row["example_2"]) else "",
                "example_3": str(row["example_3"]).strip() if pd.notna(row["example_3"]) else "",
            }

            categories.append(category)

        logger.info(
            "etl.cm_readthrough.categories_loaded",
            execution_id=execution_id,
            file_name="qa_market_volatility_regulatory_categories.xlsx",
            num_categories=len(categories),
        )
        return categories

    except Exception as e:
        error_msg = (
            f"Failed to load Q&A market volatility/regulatory categories from {xlsx_path}: {str(e)}"
        )
        logger.error(
            "etl.cm_readthrough.categories_load_error",
            execution_id=execution_id,
            xlsx_path=xlsx_path,
            error=str(e),
        )
        raise RuntimeError(error_msg) from e


def load_qa_pipelines_activity_categories(execution_id: str) -> List[Dict[str, Any]]:
    """
    Load Q&A pipelines/activity categories from Excel file.

    Args:
        execution_id: Execution ID for logging

    Returns:
        List of category dictionaries with standardized 6-column format
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    xlsx_path = os.path.join(
        current_dir, "config", "categories", "qa_pipelines_activity_categories.xlsx"
    )

    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(f"Categories file not found: {xlsx_path}")

    try:
        df = pd.read_excel(xlsx_path, sheet_name=0)

        # Required columns for standard format
        required_columns = ["transcript_sections", "category_name", "category_description"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(
                f"Missing required columns in Excel file: {missing_columns}. "
                f"Required columns: {required_columns}"
            )

        # Optional example columns
        optional_columns = ["example_1", "example_2", "example_3"]
        for col in optional_columns:
            if col not in df.columns:
                df[col] = ""  # Add empty column if not present

        categories = []
        for idx, row in df.iterrows():
            for field in required_columns:
                if pd.isna(row[field]) or str(row[field]).strip() == "":
                    raise ValueError(
                        f"Missing value for '{field}' in "
                        f"qa_pipelines_activity_categories.xlsx (row {idx + 2})"
                    )

            category = {
                "transcript_sections": str(row["transcript_sections"]).strip(),
                "category_name": str(row["category_name"]).strip(),
                "category_description": str(row["category_description"]).strip(),
                "example_1": str(row["example_1"]).strip() if pd.notna(row["example_1"]) else "",
                "example_2": str(row["example_2"]).strip() if pd.notna(row["example_2"]) else "",
                "example_3": str(row["example_3"]).strip() if pd.notna(row["example_3"]) else "",
            }

            categories.append(category)

        logger.info(
            "etl.cm_readthrough.categories_loaded",
            execution_id=execution_id,
            file_name="qa_pipelines_activity_categories.xlsx",
            num_categories=len(categories),
        )
        return categories

    except Exception as e:
        error_msg = f"Failed to load Q&A pipelines/activity categories from {xlsx_path}: {str(e)}"
        logger.error(
            "etl.cm_readthrough.categories_load_error",
            execution_id=execution_id,
            xlsx_path=xlsx_path,
            error=str(e),
        )
        raise RuntimeError(error_msg) from e


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
            SECTIONS_KEY_MD: "Management Discussion section only",
            SECTIONS_KEY_QA: "Q&A section only",
            SECTIONS_KEY_ALL: "Both Management Discussion and Q&A sections",
        }.get(cat.get("transcript_sections", SECTIONS_KEY_ALL), "ALL sections")

        section = "<category>\n"
        section += f"<name>{_sanitize_for_prompt(cat['category_name'])}</name>\n"
        group = cat.get("category_group", "")
        if group:
            section += f"<group>{_sanitize_for_prompt(group)}</group>\n"
        section += f"<section>{section_desc}</section>\n"
        section += (
            f"<description>{_sanitize_for_prompt(cat['category_description'])}</description>\n"
        )

        # Collect non-empty examples
        examples = []
        for i in range(1, 4):
            example_key = f"example_{i}"
            val = cat.get(example_key)
            if val and isinstance(val, str) and val.strip():
                examples.append(val)

        if examples:
            section += "<examples>\n"
            for example in examples:
                section += f"  <example>{_sanitize_for_prompt(example)}</example>\n"
            section += "</examples>\n"

        section += "</category>"
        formatted_sections.append(section)

    return "\n\n".join(formatted_sections)


async def find_latest_available_quarter(
    bank_id: int, min_fiscal_year: int, min_quarter: str, bank_name: str = ""
) -> Optional[Tuple[int, str]]:
    """
    Find the latest available quarter for a bank, at or after the minimum specified.

    Args:
        bank_id: Bank ID
        min_fiscal_year: Minimum fiscal year
        min_quarter: Minimum quarter
        bank_name: Bank name for logging

    Returns:
        Tuple of (fiscal_year, quarter) if found, None otherwise
    """
    async with get_connection() as conn:
        quarter_map = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
        min_quarter_num = quarter_map.get(min_quarter, 1)

        query = text(
            """
            SELECT fiscal_year, quarter
            FROM aegis_data_availability
            WHERE bank_id = :bank_id
              AND 'transcripts' = ANY(database_names)
              AND (fiscal_year > :min_year
                   OR (fiscal_year = :min_year
                       AND CASE quarter
                           WHEN 'Q1' THEN 1
                           WHEN 'Q2' THEN 2
                           WHEN 'Q3' THEN 3
                           WHEN 'Q4' THEN 4
                       END >= :min_quarter))
            ORDER BY fiscal_year DESC,
                     CASE quarter
                         WHEN 'Q4' THEN 4
                         WHEN 'Q3' THEN 3
                         WHEN 'Q2' THEN 2
                         WHEN 'Q1' THEN 1
                     END DESC
            LIMIT 1
        """
        )

        result = await conn.execute(
            query, {"bank_id": bank_id, "min_year": min_fiscal_year, "min_quarter": min_quarter_num}
        )

        row = result.first()
        if row:
            latest_year = row.fiscal_year
            latest_quarter = row.quarter

            if latest_year > min_fiscal_year or (
                latest_year == min_fiscal_year
                and quarter_map.get(latest_quarter, 0) > min_quarter_num
            ):
                logger.info(
                    f"[LATEST QUARTER MODE] {bank_name or f'Bank {bank_id}'}: "
                    f"Using more recent data {latest_year} {latest_quarter} "
                    f"(requested minimum was {min_fiscal_year} {min_quarter})"
                )
            else:
                logger.info(
                    f"[REQUESTED QUARTER] {bank_name or f'Bank {bank_id}'}: "
                    f"Using requested quarter {latest_year} {latest_quarter}"
                )

            return (latest_year, latest_quarter)

        return None


async def extract_outlook_from_transcript(
    bank_info: Dict[str, Any],
    transcript_content: str,
    categories: List[Dict[str, Any]],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    prompt_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Extract categorized outlook statements from full transcript.

    Args:
        bank_info: Bank information dictionary
        transcript_content: Full transcript text
        categories: List of category dicts with category, description, examples
        fiscal_year: Year
        quarter: Quarter
        context: Execution context

    Returns:
        {
            "has_content": bool,
            "statements": [
                {"category": "M&A", "statement": "...", "is_new_category": false},
                {"category": "Trading", "statement": "...", "is_new_category": false}
            ]
        }
    """
    execution_id = context.get("execution_id")
    prompts = prompt_data or load_prompt_from_db(
        layer="cm_readthrough_etl",
        name="outlook_extraction",
        compose_with_globals=False,
        available_databases=None,
        execution_id=execution_id,
    )

    categories_text = format_categories_for_prompt(categories)
    system_prompt = prompts["system_prompt"].format(categories_list=categories_text)
    user_prompt = prompts["user_prompt"].format(
        bank_name=_sanitize_for_prompt(bank_info["bank_name"]),
        fiscal_year=fiscal_year,
        quarter=quarter,
        transcript_content=_sanitize_for_prompt(transcript_content),
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    tools = [prompts["tool_definition"]]

    llm_params = {
        "model": etl_config.get_model("outlook_extraction"),
        "temperature": etl_config.temperature,
        "max_tokens": etl_config.get_max_tokens("outlook_extraction"),
    }

    validated = await _complete_with_tools_validated(
        messages=messages,
        tools=tools,
        context=context,
        llm_params=llm_params,
        response_model=OutlookExtractionResponse,
        stage=f"outlook_extraction:{bank_info['bank_symbol']}",
    )

    if not validated.get("has_content", False):
        logger.info(f"[NO OUTLOOK] {bank_info['bank_name']}: No relevant outlook found")
        return {"has_content": False, "statements": [], "emerging_categories": [], "failed": False}

    all_statements = validated.get("statements", [])

    # Separate standard vs emerging categories
    standard = [s for s in all_statements if not s.get("is_new_category", False)]
    emerging = [s for s in all_statements if s.get("is_new_category", False)]

    if emerging:
        emerging_names = list(set(s["category"] for s in emerging))
        logger.info(
            "etl.cm_readthrough.emerging_categories_detected",
            execution_id=context.get("execution_id"),
            bank=bank_info["bank_name"],
            section="outlook",
            categories=emerging_names,
            count=len(emerging),
        )

    logger.info(
        f"[OUTLOOK EXTRACTED] {bank_info['bank_name']}: "
        f"{len(standard)} statements, {len(emerging)} emerging"
    )
    return {
        "has_content": len(standard) > 0,
        "statements": standard,
        "emerging_categories": emerging,
        "failed": False,
    }


async def extract_questions_from_qa(
    bank_info: Dict[str, Any],
    qa_content: str,
    categories: List[Dict[str, Any]],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    prompt_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Extract categorized analyst questions from Q&A section.

    Args:
        bank_info: Bank information dictionary
        qa_content: Q&A section text
        categories: List of category dicts with category, description, examples
        fiscal_year: Year
        quarter: Quarter
        context: Execution context

    Returns:
        {
            "has_content": bool,
            "questions": [
                {
                    "category": "M&A",
                    "verbatim_question": "...",
                    "analyst_name": "...",
                    "analyst_firm": "...",
                    "is_new_category": false
                }
            ]
        }
    """
    execution_id = context.get("execution_id")
    prompts = prompt_data or load_prompt_from_db(
        layer="cm_readthrough_etl",
        name="qa_extraction_dynamic",
        compose_with_globals=False,
        available_databases=None,
        execution_id=execution_id,
    )

    categories_text = format_categories_for_prompt(categories)
    system_prompt = prompts["system_prompt"].format(categories_list=categories_text)
    user_prompt = prompts["user_prompt"].format(
        bank_name=_sanitize_for_prompt(bank_info["bank_name"]),
        fiscal_year=fiscal_year,
        quarter=quarter,
        qa_content=_sanitize_for_prompt(qa_content),
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    tools = [prompts["tool_definition"]]

    llm_params = {
        "model": etl_config.get_model("qa_extraction"),
        "temperature": etl_config.temperature,
        "max_tokens": etl_config.get_max_tokens("qa_extraction"),
    }

    validated = await _complete_with_tools_validated(
        messages=messages,
        tools=tools,
        context=context,
        llm_params=llm_params,
        response_model=QAExtractionResponse,
        stage=f"qa_extraction:{bank_info['bank_symbol']}",
    )

    if not validated.get("has_content", False):
        logger.info(f"[NO QUESTIONS] {bank_info['bank_name']}: No relevant questions found")
        return {"has_content": False, "questions": [], "emerging_categories": [], "failed": False}

    all_questions = validated.get("questions", [])

    # Separate standard vs emerging categories
    standard = [q for q in all_questions if not q.get("is_new_category", False)]
    emerging = [q for q in all_questions if q.get("is_new_category", False)]

    if emerging:
        emerging_names = list(set(q["category"] for q in emerging))
        logger.info(
            "etl.cm_readthrough.emerging_categories_detected",
            execution_id=context.get("execution_id"),
            bank=bank_info["bank_name"],
            section="qa",
            categories=emerging_names,
            count=len(emerging),
        )

    logger.info(
        f"[QUESTIONS EXTRACTED] {bank_info['bank_name']}: "
        f"{len(standard)} questions, {len(emerging)} emerging"
    )
    return {
        "has_content": len(standard) > 0,
        "questions": standard,
        "emerging_categories": emerging,
        "failed": False,
    }


def aggregate_results(
    bank_outlook: List[Tuple[str, str, Dict]],  # [(bank_name, bank_symbol, outlook_result)]
    bank_section2: List[Tuple[str, str, Dict]],  # [(bank_name, bank_symbol, section2_result)]
    bank_section3: List[Tuple[str, str, Dict]],  # [(bank_name, bank_symbol, section3_result)]
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    Aggregate and sort results by bank for all 3 sections.

    Args:
        bank_outlook: List of tuples (bank_name, bank_symbol, outlook_result)
        bank_section2: List of tuples (bank_name, bank_symbol, section2_result)
        bank_section3: List of tuples (bank_name, bank_symbol, section3_result)

    Returns:
        (all_outlook, all_section2, all_section3) where each is:
        {
            "Bank of America": {
                "bank_symbol": "BAC-US",
                "statements" or "questions": [...]
            }
        }
    """
    all_outlook = {}
    all_section2 = {}
    all_section3 = {}

    for bank_name, bank_symbol, result in bank_outlook:
        if result.get("has_content") and result.get("statements"):
            all_outlook[bank_name] = {
                "bank_symbol": bank_symbol,
                "statements": result["statements"],
            }

    for bank_name, bank_symbol, result in bank_section2:
        if result.get("has_content") and result.get("questions"):
            all_section2[bank_name] = {"bank_symbol": bank_symbol, "questions": result["questions"]}

    for bank_name, bank_symbol, result in bank_section3:
        if result.get("has_content") and result.get("questions"):
            all_section3[bank_name] = {"bank_symbol": bank_symbol, "questions": result["questions"]}

    logger.info(
        f"[AGGREGATION] {len(all_outlook)} banks with outlook, "
        f"{len(all_section2)} banks with section 2 questions, "
        f"{len(all_section3)} banks with section 3 questions"
    )

    return all_outlook, all_section2, all_section3


async def generate_subtitle(
    content_data: Dict[str, Any],
    content_type: str,
    section_context: str,
    default_subtitle: str,
    context: Dict[str, Any],
    prompt_data: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Universal subtitle generation function for any section.

    Args:
        content_data: Dictionary of content by bank (outlook or questions)
        content_type: "outlook" or "questions"
        section_context: Description of the section content
        default_subtitle: Fallback subtitle if generation fails
        context: Execution context

    Returns:
        Generated subtitle string (8-15 words)
    """
    if not content_data:
        return default_subtitle

    execution_id = context.get("execution_id")
    subtitle_prompts = prompt_data or load_prompt_from_db(
        layer="cm_readthrough_etl",
        name="subtitle_generation",
        compose_with_globals=False,
        available_databases=None,
        execution_id=execution_id,
    )

    content_summary = {}
    for bank_name, data in content_data.items():
        if content_type == "outlook":
            items = data.get("statements", [])
            content_summary[bank_name] = [
                {"category": item["category"], "text": item["statement"][:200]}
                for item in items[:3]
            ]
        else:  # questions
            items = data.get("questions", [])
            content_summary[bank_name] = [
                {"category": item["category"], "text": item["verbatim_question"][:200]}
                for item in items[:3]
            ]

    # In-place prompt variable replacement (matches Call Summary pattern)
    user_prompt = subtitle_prompts["user_prompt"].format(
        content_type=content_type,
        section_context=_sanitize_for_prompt(section_context),
        content_json=json.dumps(content_summary, indent=2),
    )

    # Direct message construction (matches Call Summary pattern)
    messages = [
        {"role": "system", "content": subtitle_prompts["system_prompt"]},
        {"role": "user", "content": user_prompt},
    ]

    # Direct tool use (matches Call Summary pattern)
    tools = [subtitle_prompts["tool_definition"]]

    llm_params = {
        "model": etl_config.get_model("subtitle_generation"),
        "temperature": etl_config.temperature,
        "max_tokens": etl_config.get_max_tokens("subtitle_generation"),
        "tool_choice": "required",  # Force tool use
    }

    try:
        logger.info(
            f"[SUBTITLE] Generating {content_type} subtitle from {len(content_data)} banks..."
        )
        validated = await _complete_with_tools_validated(
            messages=messages,
            tools=tools,
            context=context,
            llm_params=llm_params,
            response_model=SubtitleResponse,
            stage=f"subtitle_generation:{content_type}",
            allow_default_on_failure=True,
            default_value={"subtitle": default_subtitle},
        )
        subtitle = validated.get("subtitle", default_subtitle)
        logger.info(f"[SUBTITLE GENERATED] {subtitle}")
        return subtitle

    except Exception as e:
        logger.error(f"Error generating subtitle: {e}")
        return default_subtitle


async def format_outlook_batch(
    all_outlook: Dict[str, Any],
    context: Dict[str, Any],
    prompt_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Single LLM call to format all outlook statements with HTML emphasis.

    Args:
        all_outlook: Dictionary of outlook by bank
        context: Execution context

    Returns:
        Same structure but with "formatted_statement" added to each statement dict
    """
    if not all_outlook:
        return {}

    execution_id = context.get("execution_id")
    formatting_prompts = prompt_data or load_prompt_from_db(
        layer="cm_readthrough_etl",
        name="batch_formatting",
        compose_with_globals=False,
        available_databases=None,
        execution_id=execution_id,
    )

    outlook_for_formatting = {
        bank_name: data["statements"] for bank_name, data in all_outlook.items()
    }

    # In-place prompt variable replacement (matches Call Summary pattern)
    user_prompt = formatting_prompts["user_prompt"].format(
        quotes_json=json.dumps(
            outlook_for_formatting, indent=2
        )  # Note: template still says "quotes"
    )

    # Direct message construction (matches Call Summary pattern)
    messages = [
        {"role": "system", "content": formatting_prompts["system_prompt"]},
        {"role": "user", "content": user_prompt},
    ]

    # Direct tool use (matches Call Summary pattern)
    tools = [formatting_prompts["tool_definition"]]

    llm_params = {
        "model": etl_config.get_model("batch_formatting"),
        "temperature": etl_config.temperature,
        "max_tokens": etl_config.get_max_tokens("batch_formatting"),
    }

    try:
        logger.info(f"[BATCH FORMATTING] Formatting {len(all_outlook)} banks with outlook...")

        validated = await _complete_with_tools_validated(
            messages=messages,
            tools=tools,
            context=context,
            llm_params=llm_params,
            response_model=BatchFormattingResponse,
            stage="batch_formatting",
            allow_default_on_failure=True,
            default_value={"formatted_quotes": {}},
        )
        formatted_outlook = validated.get("formatted_quotes", {})
        result = {}
        for bank_name, data in all_outlook.items():
            if bank_name in formatted_outlook:
                mapped_statements = []
                for item in formatted_outlook[bank_name]:
                    formatted_quote = item.get("formatted_quote") or item.get("formatted_statement")
                    mapped = dict(item)
                    if formatted_quote:
                        mapped["formatted_quote"] = formatted_quote
                    mapped_statements.append(mapped)
                original_statements = data["statements"]
                for i, mapped in enumerate(mapped_statements):
                    if i < len(original_statements):
                        mapped["category_group"] = original_statements[i].get("category_group", "")
                result[bank_name] = {
                    "bank_symbol": data["bank_symbol"],
                    "statements": mapped_statements,
                }
            else:
                result[bank_name] = data

        logger.info(f"[BATCH FORMATTING] Successfully formatted outlook for {len(result)} banks")
        return result

    except Exception as e:
        logger.error(f"Error in batch formatting: {e}")
        return all_outlook  # Fallback to original


def _format_qa_for_dedup(
    all_section2: Dict[str, Any],
    all_section3: Dict[str, Any],
) -> str:
    """
    Format all Q&A questions as indexed XML for LLM deduplication.

    Each question is tagged with section, bank, category, and a sequential index
    so the LLM can reference duplicates by index.

    Args:
        all_section2: Aggregated section 2 questions by bank
        all_section3: Aggregated section 3 questions by bank

    Returns:
        XML-formatted string of all questions with indices
    """
    lines = ["<all_questions>"]
    for section_name, section_data in [("section2", all_section2), ("section3", all_section3)]:
        for bank_name, bank_data in section_data.items():
            for q_idx, question in enumerate(bank_data.get("questions", [])):
                lines.append("<question>")
                lines.append(f"  <section>{section_name}</section>")
                lines.append(f"  <bank>{_sanitize_for_prompt(bank_name)}</bank>")
                lines.append(
                    f"  <category>{_sanitize_for_prompt(question.get('category', ''))}</category>"
                )
                lines.append(f"  <question_index>{q_idx}</question_index>")
                lines.append(
                    f"  <text>{_sanitize_for_prompt(question.get('verbatim_question', ''))}</text>"
                )
                lines.append(
                    f"  <analyst>{_sanitize_for_prompt(question.get('analyst_name', ''))}</analyst>"
                )
                lines.append("</question>")
    lines.append("</all_questions>")
    return "\n".join(lines)


def _apply_qa_dedup_removals(
    all_section2: Dict[str, Any],
    all_section3: Dict[str, Any],
    dedup_response: Dict[str, Any],
    execution_id: str,
) -> int:
    """
    Remove duplicate questions identified by the LLM.

    Processes removals in reverse index order within each bank to preserve indices.

    Args:
        all_section2: Aggregated section 2 questions (mutated in place)
        all_section3: Aggregated section 3 questions (mutated in place)
        dedup_response: Validated QADeduplicationResponse dict
        execution_id: For logging

    Returns:
        Number of questions removed
    """
    duplicates = dedup_response.get("duplicate_questions", [])
    if not duplicates:
        return 0

    # Group removals by section -> bank -> category -> [indices]
    removal_map: Dict[str, Dict[str, Dict[str, List[int]]]] = {}
    for dup in duplicates:
        section = dup.get("section", "")
        bank = dup.get("bank", "")
        category = dup.get("category", "")
        q_idx = dup.get("question_index", -1)
        if section not in removal_map:
            removal_map[section] = {}
        if bank not in removal_map[section]:
            removal_map[section][bank] = {}
        if category not in removal_map[section][bank]:
            removal_map[section][bank][category] = []
        removal_map[section][bank][category].append(q_idx)

    removed = 0
    for section_name, section_data in [("section2", all_section2), ("section3", all_section3)]:
        section_removals = removal_map.get(section_name, {})
        if not section_removals:
            continue

        for bank_name, bank_data in section_data.items():
            bank_removals = section_removals.get(bank_name, {})
            if not bank_removals:
                continue

            questions = bank_data.get("questions", [])
            indices_to_remove = set()

            for q_idx, question in enumerate(questions):
                cat = question.get("category", "")
                cat_removals = bank_removals.get(cat, [])
                if q_idx in cat_removals:
                    indices_to_remove.add(q_idx)

            if indices_to_remove:
                # Remove in reverse order to preserve indices
                for idx in sorted(indices_to_remove, reverse=True):
                    if idx < len(questions):
                        removed_q = questions.pop(idx)
                        removed += 1
                        logger.info(
                            "etl.cm_readthrough.qa_dedup_removed",
                            execution_id=execution_id,
                            section=section_name,
                            bank=bank_name,
                            category=removed_q.get("category", ""),
                            question=removed_q.get("verbatim_question", "")[:80],
                        )

    return removed


async def _deduplicate_qa_results_llm(
    all_section2: Dict[str, Any],
    all_section3: Dict[str, Any],
    context: Dict[str, Any],
    prompt_data: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    LLM-driven deduplication of Q&A questions across sections and categories.

    Args:
        all_section2: Aggregated section 2 questions by bank
        all_section3: Aggregated section 3 questions by bank
        context: Execution context
        prompt_data: Optional pre-loaded prompt data

    Returns:
        Tuple of (all_section2, all_section3) with duplicates removed
    """
    execution_id = context.get("execution_id")

    # Count total questions
    total_q = sum(len(d.get("questions", [])) for d in all_section2.values()) + sum(
        len(d.get("questions", [])) for d in all_section3.values()
    )

    if total_q < 5:
        logger.info(
            "etl.cm_readthrough.qa_dedup_skipped",
            execution_id=execution_id,
            reason=f"Only {total_q} questions, skipping dedup (threshold: 5)",
        )
        return all_section2, all_section3

    prompts = prompt_data or load_prompt_from_db(
        layer="cm_readthrough_etl",
        name="qa_deduplication",
        compose_with_globals=False,
        available_databases=None,
        execution_id=execution_id,
    )

    qa_xml = _format_qa_for_dedup(all_section2, all_section3)
    system_prompt = prompts["system_prompt"]
    user_prompt = prompts["user_prompt"].format(questions_xml=qa_xml)

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    tools = [prompts["tool_definition"]]

    llm_params = {
        "model": etl_config.get_model("qa_deduplication"),
        "temperature": etl_config.temperature,
        "max_tokens": etl_config.get_max_tokens("qa_deduplication"),
    }

    try:
        validated = await _complete_with_tools_validated(
            messages=messages,
            tools=tools,
            context=context,
            llm_params=llm_params,
            response_model=QADeduplicationResponse,
            stage="qa_deduplication",
            allow_default_on_failure=True,
            default_value={"analysis_notes": "Dedup failed", "duplicate_questions": []},
        )

        removed = _apply_qa_dedup_removals(all_section2, all_section3, validated, execution_id)
        logger.info(
            "etl.cm_readthrough.qa_dedup_complete",
            execution_id=execution_id,
            total_questions=total_q,
            duplicates_removed=removed,
            analysis_notes=validated.get("analysis_notes", ""),
        )

    except Exception as e:
        logger.error(
            "etl.cm_readthrough.qa_dedup_error",
            execution_id=execution_id,
            error=str(e),
        )

    return all_section2, all_section3


async def process_all_banks_parallel(
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    use_latest: bool,
    outlook_categories: List[Dict[str, Any]],
    qa_market_vol_reg_categories: List[Dict[str, Any]],
    qa_pipelines_activity_categories: List[Dict[str, Any]],
    prompt_bundle: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Process all banks with concurrent execution.

    Args:
        fiscal_year: Year
        quarter: Quarter
        context: Execution context
        use_latest: If True, use latest available quarter >= specified quarter
        outlook_categories: Loaded outlook categories
        qa_market_vol_reg_categories: Loaded Section 2 Q&A categories
        qa_pipelines_activity_categories: Loaded Section 3 Q&A categories

    Returns:
        Combined results dictionary
    """
    execution_id = context.get("execution_id")
    monitored_banks = get_monitored_institutions()
    prompts = prompt_bundle or {}

    logger.info(
        f"Processing {len(monitored_banks)} banks for {fiscal_year} {quarter} "
        f"(mode: {'latest available' if use_latest else 'exact quarter'})"
    )

    semaphore = asyncio.Semaphore(etl_config.max_concurrent_banks)
    section_cache: Dict[Tuple[int, int, str, str], str] = {}

    async def resolve_bank_period(bank_data: Dict[str, Any]) -> Optional[Tuple[int, str]]:
        if not use_latest:
            return fiscal_year, quarter
        return await find_latest_available_quarter(
            bank_id=bank_data["bank_id"],
            min_fiscal_year=fiscal_year,
            min_quarter=quarter,
            bank_name=bank_data["bank_name"],
        )

    async def get_section_content(combo: Dict[str, Any], section_key: str) -> str:
        cache_key = (combo["bank_id"], combo["fiscal_year"], combo["quarter"], section_key)
        if cache_key in section_cache:
            return section_cache[cache_key]
        chunks = await retrieve_full_section(combo=combo, sections=section_key, context=context)
        content = format_full_section_chunks(chunks=chunks, combo=combo, context=context)
        section_cache[cache_key] = content
        return content

    async def process_bank_outlook(bank_data: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
        async with semaphore:
            try:
                resolved = await resolve_bank_period(bank_data)
                if not resolved:
                    logger.warning(
                        f"[NO DATA] {bank_data['bank_name']}: No transcript data available "
                        f"for {fiscal_year} {quarter} or later"
                    )
                    return (
                        bank_data["bank_name"],
                        bank_data["bank_symbol"],
                        {"has_content": False, "statements": [], "failed": False},
                    )

                actual_year, actual_quarter = resolved
                combo = {
                    "bank_id": bank_data["bank_id"],
                    "bank_name": bank_data["bank_name"],
                    "bank_symbol": bank_data["bank_symbol"],
                    "fiscal_year": actual_year,
                    "quarter": actual_quarter,
                }
                md_content = await get_section_content(combo, "MD")
                qa_content = await get_section_content(combo, "QA")
                transcript = f"{md_content}\n\n{qa_content}".strip()

                logger.info(
                    f"[TRANSCRIPT] {bank_data['bank_name']} {actual_year} {actual_quarter}: "
                    f"Retrieved {len(md_content)} MD chars + {len(qa_content)} QA chars"
                )

                if not transcript or transcript.startswith("No transcript data available"):
                    return (
                        bank_data["bank_name"],
                        bank_data["bank_symbol"],
                        {"has_content": False, "statements": [], "failed": False},
                    )

                result = await extract_outlook_from_transcript(
                    bank_data,
                    transcript,
                    outlook_categories,
                    actual_year,
                    actual_quarter,
                    context,
                    prompt_data=prompts.get("outlook_extraction"),
                )
                return bank_data["bank_name"], bank_data["bank_symbol"], result

            except Exception as e:
                logger.error(
                    "etl.cm_readthrough.outlook_pipeline_failure",
                    execution_id=execution_id,
                    bank=bank_data.get("bank_symbol", bank_data.get("bank_name", "UNKNOWN")),
                    error=str(e),
                )
                return (
                    bank_data.get("bank_name", "Unknown"),
                    bank_data.get("bank_symbol", ""),
                    {"has_content": False, "statements": [], "failed": True, "error": str(e)},
                )

    async def process_bank_qa(
        bank_data: Dict[str, Any],
        categories: List[Dict[str, Any]],
        section_name: str,
    ) -> Tuple[str, str, Dict[str, Any]]:
        async with semaphore:
            try:
                resolved = await resolve_bank_period(bank_data)
                if not resolved:
                    logger.warning(
                        f"[NO DATA] {bank_data['bank_name']}: No Q&A data available "
                        f"for {fiscal_year} {quarter} or later"
                    )
                    return (
                        bank_data["bank_name"],
                        bank_data["bank_symbol"],
                        {"has_content": False, "questions": [], "failed": False},
                    )

                actual_year, actual_quarter = resolved
                combo = {
                    "bank_id": bank_data["bank_id"],
                    "bank_name": bank_data["bank_name"],
                    "bank_symbol": bank_data["bank_symbol"],
                    "fiscal_year": actual_year,
                    "quarter": actual_quarter,
                }
                qa_content = await get_section_content(combo, "QA")
                logger.info(
                    f"[Q&A SECTION] {bank_data['bank_name']} {actual_year} {actual_quarter}: "
                    f"Retrieved {len(qa_content)} chars"
                )

                if not qa_content or qa_content.startswith("No transcript data available"):
                    return (
                        bank_data["bank_name"],
                        bank_data["bank_symbol"],
                        {"has_content": False, "questions": [], "failed": False},
                    )

                result = await extract_questions_from_qa(
                    bank_data,
                    qa_content,
                    categories,
                    actual_year,
                    actual_quarter,
                    context,
                    prompt_data=prompts.get("qa_extraction_dynamic"),
                )
                return bank_data["bank_name"], bank_data["bank_symbol"], result

            except Exception as e:
                logger.error(
                    "etl.cm_readthrough.qa_pipeline_failure",
                    execution_id=execution_id,
                    section=section_name,
                    bank=bank_data.get("bank_symbol", bank_data.get("bank_name", "UNKNOWN")),
                    error=str(e),
                )
                return (
                    bank_data.get("bank_name", "Unknown"),
                    bank_data.get("bank_symbol", ""),
                    {"has_content": False, "questions": [], "failed": True, "error": str(e)},
                )

    logger.info(f"[PHASES 1-3] Starting concurrent extraction for {len(monitored_banks)} banks...")
    outlook_tasks = [process_bank_outlook(bank) for bank in monitored_banks]
    section2_tasks = [
        process_bank_qa(bank, qa_market_vol_reg_categories, "section2") for bank in monitored_banks
    ]
    section3_tasks = [
        process_bank_qa(bank, qa_pipelines_activity_categories, "section3")
        for bank in monitored_banks
    ]

    bank_outlook, bank_section2, bank_section3 = await asyncio.gather(
        asyncio.gather(*outlook_tasks, return_exceptions=True),
        asyncio.gather(*section2_tasks, return_exceptions=True),
        asyncio.gather(*section3_tasks, return_exceptions=True),
    )

    def clean_results(
        raw_results: List[Any], stage_name: str
    ) -> List[Tuple[str, str, Dict[str, Any]]]:
        clean: List[Tuple[str, str, Dict[str, Any]]] = []
        for result in raw_results:
            if isinstance(result, Exception):
                logger.error(
                    "etl.cm_readthrough.parallel_stage_exception",
                    execution_id=execution_id,
                    stage=stage_name,
                    error=str(result),
                )
                continue
            clean.append(result)
        return clean

    bank_outlook_clean = clean_results(bank_outlook, "outlook")
    bank_section2_clean = clean_results(bank_section2, "section2")
    bank_section3_clean = clean_results(bank_section3, "section3")

    failed_outlook_extractions = sum(1 for _, _, r in bank_outlook_clean if r.get("failed"))
    failed_section2_extractions = sum(1 for _, _, r in bank_section2_clean if r.get("failed"))
    failed_section3_extractions = sum(1 for _, _, r in bank_section3_clean if r.get("failed"))

    # Collect emerging categories across all banks
    all_emerging = []
    for bank_name, bank_symbol, result in bank_outlook_clean:
        for ec in result.get("emerging_categories", []):
            all_emerging.append({"bank": bank_name, "section": "outlook", **ec})
    for bank_name, bank_symbol, result in bank_section2_clean:
        for ec in result.get("emerging_categories", []):
            all_emerging.append({"bank": bank_name, "section": "section2", **ec})
    for bank_name, bank_symbol, result in bank_section3_clean:
        for ec in result.get("emerging_categories", []):
            all_emerging.append({"bank": bank_name, "section": "section3", **ec})

    if all_emerging:
        unique_emerging_names = list(set(ec.get("category", "") for ec in all_emerging))
        logger.info(
            "etl.cm_readthrough.emerging_categories_summary",
            execution_id=execution_id,
            unique_categories=unique_emerging_names,
            total_count=len(all_emerging),
        )

    logger.info("[PHASE 4] Aggregating results...")
    all_outlook, all_section2, all_section3 = aggregate_results(
        bank_outlook_clean, bank_section2_clean, bank_section3_clean
    )

    logger.info("[PHASE 4.5] Q&A deduplication...")
    all_section2, all_section3 = await _deduplicate_qa_results_llm(
        all_section2,
        all_section3,
        context,
        prompt_data=prompts.get("qa_deduplication"),
    )

    subtitle_semaphore = asyncio.Semaphore(etl_config.max_concurrent_subtitle_generation)

    async def generate_subtitle_limited(*args, **kwargs) -> str:
        async with subtitle_semaphore:
            return await generate_subtitle(*args, **kwargs)

    logger.info("[PHASES 5-7] Generating subtitles for all 3 sections...")
    subtitle1, subtitle2, subtitle3 = await asyncio.gather(
        generate_subtitle_limited(
            all_outlook,
            "outlook",
            "Forward-looking outlook statements on IB activity, markets, pipelines",
            "Outlook: Capital markets activity across major institutions",
            context,
            prompt_data=prompts.get("subtitle_generation"),
        ),
        generate_subtitle_limited(
            all_section2,
            "questions",
            "Analyst questions on market volatility, risk management, regulatory changes",
            (
                "Conference calls: Benefits and threats of market volatility, "
                "line-draws and regulatory changes"
            ),
            context,
            prompt_data=prompts.get("subtitle_generation"),
        ),
        generate_subtitle_limited(
            all_section3,
            "questions",
            "Analyst questions on pipeline strength, M&A activity, transaction banking",
            "Conference calls: How well pipelines are holding up and areas of activity",
            context,
            prompt_data=prompts.get("subtitle_generation"),
        ),
    )

    logger.info("[PHASE 8] Applying batch formatting...")
    formatted_outlook = await format_outlook_batch(
        all_outlook, context, prompt_data=prompts.get("batch_formatting")
    )
    formatted_section2 = all_section2
    formatted_section3 = all_section3

    results = {
        "metadata": {
            "fiscal_year": fiscal_year,
            "quarter": quarter,
            "banks_processed": len(monitored_banks),
            "banks_with_outlook": len(formatted_outlook),
            "banks_with_section2": len(formatted_section2),
            "banks_with_section3": len(formatted_section3),
            "failed_outlook_extractions": failed_outlook_extractions,
            "failed_section2_extractions": failed_section2_extractions,
            "failed_section3_extractions": failed_section3_extractions,
            "generation_date": datetime.now().isoformat(),
            "mode": "latest_available" if use_latest else "exact_quarter",
            "subtitle_section1": subtitle1,
            "subtitle_section2": subtitle2,
            "subtitle_section3": subtitle3,
            "emerging_categories": all_emerging,
        },
        "outlook": formatted_outlook,
        "section2_questions": formatted_section2,
        "section3_questions": formatted_section3,
    }

    logger.info(
        f"[PIPELINE COMPLETE] {results['metadata']['banks_with_outlook']} banks with outlook, "
        f"{results['metadata']['banks_with_section2']} banks with section 2, "
        f"{results['metadata']['banks_with_section3']} banks with section 3, "
        f"failed extractions: "
        f"{failed_outlook_extractions + failed_section2_extractions + failed_section3_extractions}"
    )

    return results


async def save_to_database(
    results: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    execution_id: str,
    local_filepath: str = None,
    s3_document_name: str = None,
) -> None:
    """
    Save the report to the database.

    Args:
        results: Structured results
        fiscal_year: Year
        quarter: Quarter
        execution_id: Execution UUID
        local_filepath: Path to local DOCX file (optional)
        s3_document_name: S3 document key (optional)
    """
    stage = "connecting"
    try:
        async with get_connection() as conn:
            stage = "deleting existing report"
            # Delete any existing report for the same period/type
            delete_result = await conn.execute(
                text(
                    """
                DELETE FROM aegis_reports
                WHERE fiscal_year = :fiscal_year
                  AND quarter = :quarter
                  AND report_type = :report_type
                RETURNING id
                """
                ),
                {
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                    "report_type": "cm_readthrough",
                },
            )
            delete_result.fetchall()

            stage = "inserting new report"
            # Insert new report
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
                    "report_name": "Capital Markets Readthrough",
                    "report_description": (
                        "AI-generated analysis of capital markets commentary from "
                        "quarterly earnings calls across major U.S. and European banks. "
                        "Extracts investment banking and trading outlook, analyst questions "
                        "on market dynamics, risk management, M&A pipelines, and "
                        "transaction banking."
                    ),
                    "report_type": "cm_readthrough",
                    "bank_id": None,  # Cross-bank report, no specific bank
                    "bank_name": None,  # Cross-bank report, no specific bank
                    "bank_symbol": None,  # Cross-bank report, no specific bank
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                    "local_filepath": local_filepath,
                    "s3_document_name": s3_document_name,
                    "s3_pdf_name": None,
                    "generation_date": datetime.now(),
                    "generated_by": "cm_readthrough_etl",
                    "execution_id": str(execution_id),
                    "metadata": json.dumps(results),
                },
            )
            result.fetchone()

            stage = "commit"
            await conn.commit()

        logger.info(f"Report saved to database with execution_id: {execution_id}")
    except SQLAlchemyError as e:
        logger.error(
            "etl.cm_readthrough.database_error",
            execution_id=execution_id,
            stage=stage,
            error=str(e),
        )
        raise


async def generate_cm_readthrough(
    fiscal_year: int, quarter: str, use_latest: bool = False, output_path: Optional[str] = None
) -> CMReadthroughResult:
    """
    Generate CM readthrough report for all monitored institutions.

    Args:
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")
        use_latest: If True, use latest available quarter >= specified quarter
        output_path: Optional custom output path

    Returns:
        CMReadthroughResult with filepath and coverage metrics

    Raises:
        CMReadthroughUserError: Expected user-facing failures (no data, auth, etc.)
        CMReadthroughSystemError: Unexpected system/infrastructure failures
    """
    execution_id = str(uuid.uuid4())
    marks = [("start", time.monotonic())]
    logger.info(
        "etl.cm_readthrough.started",
        execution_id=execution_id,
        fiscal_year=fiscal_year,
        quarter=quarter,
        use_latest=use_latest,
    )

    try:
        # Stage 1: Setup & Validation - Load categories and establish authentication
        outlook_categories = load_outlook_categories(execution_id)
        qa_market_vol_reg_categories = load_qa_market_volatility_regulatory_categories(execution_id)
        qa_pipelines_activity_categories = load_qa_pipelines_activity_categories(execution_id)

        logger.info(
            "etl.cm_readthrough.categories_loaded",
            execution_id=execution_id,
            outlook_categories=len(outlook_categories),
            section2_categories=len(qa_market_vol_reg_categories),
            section3_categories=len(qa_pipelines_activity_categories),
        )
        marks.append(("categories_loaded", time.monotonic()))

        ssl_config = setup_ssl()
        auth_config = await setup_authentication(execution_id=execution_id, ssl_config=ssl_config)

        if not auth_config["success"]:
            error_msg = f"Authentication failed: {auth_config.get('error', 'Unknown error')}"
            logger.error(
                "etl.cm_readthrough.auth_failed", execution_id=execution_id, error=error_msg
            )
            raise CMReadthroughSystemError(error_msg)

        context = {
            "execution_id": execution_id,
            "ssl_config": ssl_config,
            "auth_config": auth_config,
            "_llm_costs": [],
        }
        marks.append(("auth_ready", time.monotonic()))
        prompt_bundle = _load_prompt_bundle(execution_id)
        marks.append(("prompts_loaded", time.monotonic()))

        # Stage 2: Transcript Retrieval & Extraction (Parallel)
        results = await process_all_banks_parallel(
            fiscal_year=fiscal_year,
            quarter=quarter,
            context=context,
            use_latest=use_latest,
            outlook_categories=outlook_categories,
            qa_market_vol_reg_categories=qa_market_vol_reg_categories,
            qa_pipelines_activity_categories=qa_pipelines_activity_categories,
            prompt_bundle=prompt_bundle,
        )
        marks.append(("extraction_complete", time.monotonic()))

        if not results or (
            not results.get("outlook")
            and not results.get("section2_questions")
            and not results.get("section3_questions")
        ):
            raise CMReadthroughUserError(
                f"No results generated for {quarter} {fiscal_year}. "
                "No banks had available data for the specified period."
            )

        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(exist_ok=True)

        if output_path:
            docx_path = Path(output_path)
        else:
            docx_path = output_dir / f"CM_Readthrough_{fiscal_year}_{quarter}.docx"

        create_combined_document(results, str(docx_path))
        logger.info(
            "etl.cm_readthrough.document_saved", execution_id=execution_id, filepath=str(docx_path)
        )
        marks.append(("document_saved", time.monotonic()))

        docx_filename = docx_path.name

        await save_to_database(
            results=results,
            fiscal_year=fiscal_year,
            quarter=quarter,
            execution_id=execution_id,
            local_filepath=str(docx_path),
            s3_document_name=docx_filename,
        )
        marks.append(("persisted", time.monotonic()))

        metadata = results.get("metadata", {})
        banks_with_outlook = metadata.get("banks_with_outlook", 0)
        banks_with_section2 = metadata.get("banks_with_section2", 0)
        banks_with_section3 = metadata.get("banks_with_section3", 0)
        total_banks = metadata.get("banks_processed", 0)
        cost_summary = _get_total_llm_cost(context)

        logger.info(
            "etl.cm_readthrough.completed",
            execution_id=execution_id,
            banks_with_data=f"{banks_with_outlook}/{total_banks} outlook, "
            f"{banks_with_section2}/{total_banks} section2, "
            f"{banks_with_section3}/{total_banks} section3",
            total_cost=cost_summary["total_cost"],
            total_tokens=cost_summary["total_tokens"],
            llm_calls=cost_summary["llm_calls"],
            **_timing_summary(marks),
        )

        return CMReadthroughResult(
            filepath=str(docx_path),
            execution_id=execution_id,
            banks_processed=total_banks,
            banks_with_outlook=banks_with_outlook,
            banks_with_section2=banks_with_section2,
            banks_with_section3=banks_with_section3,
            total_cost=cost_summary["total_cost"],
            total_tokens=cost_summary["total_tokens"],
        )

    except CMReadthroughError:
        raise
    except (ValueError, RuntimeError) as e:
        logger.error(
            "etl.cm_readthrough.error", execution_id=execution_id, error=str(e), exc_info=True
        )
        raise CMReadthroughUserError(str(e)) from e
    except Exception as e:
        error_msg = f"Error generating CM readthrough: {str(e)}"
        logger.error(
            "etl.cm_readthrough.error", execution_id=execution_id, error=error_msg, exc_info=True
        )
        raise CMReadthroughSystemError(error_msg) from e


def main():
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Generate CM readthrough report for all monitored institutions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--year", type=int, required=True, help="Fiscal year")
    parser.add_argument(
        "--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"], help="Quarter"
    )
    parser.add_argument(
        "--use-latest",
        action="store_true",
        help="Use latest available quarter if newer than specified",
    )
    parser.add_argument("--output", type=str, help="Output file path (optional)")

    args = parser.parse_args()

    postgresql_prompts()

    print(f"\nGenerating CM readthrough for {args.quarter} {args.year}...\n")

    try:
        result = asyncio.run(
            generate_cm_readthrough(
                fiscal_year=args.year,
                quarter=args.quarter,
                use_latest=args.use_latest,
                output_path=args.output,
            )
        )
        print(f"Complete: {result.filepath}")
        print(
            f"Banks: {result.banks_with_outlook}/{result.banks_processed} outlook, "
            f"{result.banks_with_section2}/{result.banks_processed} section2, "
            f"{result.banks_with_section3}/{result.banks_processed} section3"
        )
        print(
            f"LLM usage: cost=${result.total_cost:.4f}, tokens={result.total_tokens}, "
            f"execution_id={result.execution_id}"
        )
    except CMReadthroughUserError as e:
        print(f"User error: {e}", file=sys.stderr)
        sys.exit(1)
    except CMReadthroughError as e:
        print(f"System error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
