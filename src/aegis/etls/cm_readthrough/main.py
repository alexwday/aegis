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
import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import yaml
from sqlalchemy import text

from aegis.etls.cm_readthrough.document_converter import create_combined_document
from aegis.etls.cm_readthrough.transcript_utils import (
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

    @property
    def max_concurrent_banks(self) -> int:
        """Get the maximum concurrent banks parameter."""
        return self._config.get("concurrency", {}).get("max_concurrent_banks", 5)


etl_config = ETLConfig(os.path.join(os.path.dirname(__file__), "config", "config.yaml"))

_MONITORED_INSTITUTIONS = None


def _load_monitored_institutions() -> Dict[str, Dict[str, Any]]:
    """
    Load and cache monitored institutions configuration.

    Returns:
        Dictionary mapping ticker to institution details (id, name, type, path_safe_name)
    """
    global _MONITORED_INSTITUTIONS
    if _MONITORED_INSTITUTIONS is None:
        config_path = os.path.join(
            os.path.dirname(__file__), "config", "monitored_institutions.yaml"
        )
        with open(config_path, "r", encoding="utf-8") as f:
            _MONITORED_INSTITUTIONS = yaml.safe_load(f)
    return _MONITORED_INSTITUTIONS


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
        for _, row in df.iterrows():
            # Skip rows with missing required fields
            if pd.isna(row["category_name"]) or pd.isna(row["category_description"]):
                continue

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
        for _, row in df.iterrows():
            # Skip rows with missing required fields
            if pd.isna(row["category_name"]) or pd.isna(row["category_description"]):
                continue

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

    # Direct prompt loading (matches Call Summary pattern)
    outlook_prompts = load_prompt_from_db(
        layer="cm_readthrough_etl",
        name="outlook_extraction",
        compose_with_globals=False,
        available_databases=None,
        execution_id=execution_id,
    )

    categories_text = format_categories_for_prompt(categories)

    # In-place prompt variable replacement (matches Call Summary pattern)
    outlook_prompts["system_prompt"] = outlook_prompts["system_prompt"].format(
        categories_list=categories_text
    )

    outlook_prompts["user_prompt"] = outlook_prompts["user_prompt"].format(
        bank_name=bank_info["bank_name"],
        fiscal_year=fiscal_year,
        quarter=quarter,
        transcript_content=transcript_content,
    )

    # Direct message construction (matches Call Summary pattern)
    messages = [
        {"role": "system", "content": outlook_prompts["system_prompt"]},
        {"role": "user", "content": outlook_prompts["user_prompt"]},
    ]

    # Direct tool use (matches Call Summary pattern)
    tools = [outlook_prompts["tool_definition"]]

    llm_params = {
        "model": etl_config.get_model("outlook_extraction"),
        "temperature": etl_config.temperature,
        "max_tokens": etl_config.max_tokens,
    }

    try:
        response = await complete_with_tools(
            messages=messages, tools=tools, context=context, llm_params=llm_params
        )

        tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")
        if tool_calls:
            tool_call = tool_calls[0]
            result = json.loads(tool_call["function"]["arguments"])

            if not result.get("has_content", False):
                logger.info(f"[NO OUTLOOK] {bank_info['bank_name']}: No relevant outlook found")
                return {"has_content": False, "statements": []}

            statements = result.get("statements", [])
            new_categories = [s["category"] for s in statements if s.get("is_new_category", False)]
            if new_categories:
                logger.info(
                    f"[NEW CATEGORIES] {bank_info['bank_name']}: "
                    f"Identified new categories: {', '.join(new_categories)}"
                )

            logger.info(
                f"[OUTLOOK EXTRACTED] {bank_info['bank_name']}: "
                f"{len(statements)} statements ({len(new_categories)} new categories)"
            )
            return result
        else:
            logger.warning(f"No tool call in response for {bank_info['bank_name']}")
            return {"has_content": False, "statements": []}

    except Exception as e:
        logger.error(f"Error extracting outlook for {bank_info['bank_name']}: {e}")
        return {"has_content": False, "statements": []}


async def extract_questions_from_qa(
    bank_info: Dict[str, Any],
    qa_content: str,
    categories: List[Dict[str, Any]],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
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

    # Direct prompt loading (matches Call Summary pattern)
    qa_prompts = load_prompt_from_db(
        layer="cm_readthrough_etl",
        name="qa_extraction_dynamic",
        compose_with_globals=False,
        available_databases=None,
        execution_id=execution_id,
    )

    categories_text = format_categories_for_prompt(categories)

    # In-place prompt variable replacement (matches Call Summary pattern)
    qa_prompts["system_prompt"] = qa_prompts["system_prompt"].format(
        categories_list=categories_text
    )

    qa_prompts["user_prompt"] = qa_prompts["user_prompt"].format(
        bank_name=bank_info["bank_name"],
        fiscal_year=fiscal_year,
        quarter=quarter,
        qa_content=qa_content,
    )

    # Direct message construction (matches Call Summary pattern)
    messages = [
        {"role": "system", "content": qa_prompts["system_prompt"]},
        {"role": "user", "content": qa_prompts["user_prompt"]},
    ]

    # Direct tool use (matches Call Summary pattern)
    tools = [qa_prompts["tool_definition"]]

    llm_params = {
        "model": etl_config.get_model("qa_extraction"),
        "temperature": etl_config.temperature,
        "max_tokens": etl_config.max_tokens,
    }

    try:
        response = await complete_with_tools(
            messages=messages, tools=tools, context=context, llm_params=llm_params
        )

        tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")
        if tool_calls:
            tool_call = tool_calls[0]
            result = json.loads(tool_call["function"]["arguments"])

            if not result.get("has_content", False):
                logger.info(f"[NO QUESTIONS] {bank_info['bank_name']}: No relevant questions found")
                return {"has_content": False, "questions": []}

            questions = result.get("questions", [])
            new_categories = [q["category"] for q in questions if q.get("is_new_category", False)]
            if new_categories:
                logger.info(
                    f"[NEW CATEGORIES] {bank_info['bank_name']}: "
                    f"Identified new Q&A categories: {', '.join(set(new_categories))}"
                )

            logger.info(
                f"[QUESTIONS EXTRACTED] {bank_info['bank_name']}: "
                f"{len(questions)} questions ({len(new_categories)} new categories)"
            )
            return result
        else:
            logger.warning(f"No tool call in response for {bank_info['bank_name']}")
            return {"has_content": False, "questions": []}

    except Exception as e:
        logger.error(f"Error extracting questions for {bank_info['bank_name']}: {e}")
        return {"has_content": False, "questions": []}


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

    # Direct prompt loading (matches Call Summary pattern)
    subtitle_prompts = load_prompt_from_db(
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
    subtitle_prompts["user_prompt"] = subtitle_prompts["user_prompt"].format(
        content_type=content_type,
        section_context=section_context,
        content_json=json.dumps(content_summary, indent=2),
    )

    # Direct message construction (matches Call Summary pattern)
    messages = [
        {"role": "system", "content": subtitle_prompts["system_prompt"]},
        {"role": "user", "content": subtitle_prompts["user_prompt"]},
    ]

    # Direct tool use (matches Call Summary pattern)
    tools = [subtitle_prompts["tool_definition"]]

    llm_params = {
        "model": etl_config.get_model("subtitle_generation"),
        "temperature": etl_config.temperature,
        "max_tokens": 100,  # Subtitle is short
        "tool_choice": "required",  # Force tool use
    }

    try:
        logger.info(
            f"[SUBTITLE] Generating {content_type} subtitle from {len(content_data)} banks..."
        )

        response = await complete_with_tools(
            messages=messages, tools=tools, context=context, llm_params=llm_params
        )

        tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")
        if tool_calls:
            tool_call = tool_calls[0]
            result = json.loads(tool_call["function"]["arguments"])
            subtitle = result.get("subtitle", default_subtitle)

            logger.info(f"[SUBTITLE GENERATED] {subtitle}")
            return subtitle
        else:
            logger.warning(
                f"No tool call in subtitle generation, using default: {default_subtitle}"
            )
            return default_subtitle

    except Exception as e:
        logger.error(f"Error generating subtitle: {e}")
        return default_subtitle


async def format_outlook_batch(
    all_outlook: Dict[str, Any], context: Dict[str, Any]
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

    # Direct prompt loading (matches Call Summary pattern)
    formatting_prompts = load_prompt_from_db(
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
    formatting_prompts["user_prompt"] = formatting_prompts["user_prompt"].format(
        quotes_json=json.dumps(
            outlook_for_formatting, indent=2
        )  # Note: template still says "quotes"
    )

    # Direct message construction (matches Call Summary pattern)
    messages = [
        {"role": "system", "content": formatting_prompts["system_prompt"]},
        {"role": "user", "content": formatting_prompts["user_prompt"]},
    ]

    # Direct tool use (matches Call Summary pattern)
    tools = [formatting_prompts["tool_definition"]]

    llm_params = {
        "model": etl_config.get_model("batch_formatting"),
        "temperature": etl_config.temperature,
        "max_tokens": etl_config.max_tokens,
    }

    try:
        logger.info(f"[BATCH FORMATTING] Formatting {len(all_outlook)} banks with outlook...")

        response = await complete_with_tools(
            messages=messages, tools=tools, context=context, llm_params=llm_params
        )

        tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")
        if tool_calls:
            tool_call = tool_calls[0]
            formatted_result = json.loads(tool_call["function"]["arguments"])
            formatted_outlook = formatted_result.get(
                "formatted_quotes", {}
            )  # Tool returns "quotes" key

            result = {}
            for bank_name, data in all_outlook.items():
                if bank_name in formatted_outlook:
                    result[bank_name] = {
                        "bank_symbol": data["bank_symbol"],
                        "statements": formatted_outlook[bank_name],
                    }
                else:
                    result[bank_name] = data

            logger.info(
                f"[BATCH FORMATTING] Successfully formatted outlook for {len(result)} banks"
            )
            return result
        else:
            logger.warning("No tool call in formatting response, returning original")
            return all_outlook

    except Exception as e:
        logger.error(f"Error in batch formatting: {e}")
        return all_outlook  # Fallback to original


async def process_all_banks_parallel(
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    use_latest: bool,
    outlook_categories: List[Dict[str, Any]],
    qa_market_vol_reg_categories: List[Dict[str, Any]],
    qa_pipelines_activity_categories: List[Dict[str, Any]],
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

    logger.info(
        f"Processing {len(monitored_banks)} banks for {fiscal_year} {quarter} "
        f"(mode: {'latest available' if use_latest else 'exact quarter'})"
    )

    semaphore = asyncio.Semaphore(etl_config.max_concurrent_banks)

    async def process_bank_outlook(bank_data):
        async with semaphore:
            try:
                # Handle use_latest logic
                actual_year, actual_quarter = fiscal_year, quarter
                if use_latest:
                    latest = await find_latest_available_quarter(
                        bank_id=bank_data["bank_id"],
                        min_fiscal_year=fiscal_year,
                        min_quarter=quarter,
                        bank_name=bank_data["bank_name"],
                    )
                    if latest:
                        actual_year, actual_quarter = latest
                    else:
                        logger.warning(
                            f"[NO DATA] {bank_data['bank_name']}: No transcript data available "
                            f"for {fiscal_year} {quarter} or later"
                        )
                        return (
                            bank_data["bank_name"],
                            bank_data["bank_symbol"],
                            {"has_content": False, "statements": []},
                        )

                # Direct transcript retrieval (matches Call Summary pattern)
                combo = {
                    "bank_id": bank_data["bank_id"],
                    "bank_name": bank_data["bank_name"],
                    "bank_symbol": bank_data["bank_symbol"],
                    "fiscal_year": actual_year,
                    "quarter": actual_quarter,
                }

                try:
                    md_chunks = await retrieve_full_section(
                        combo=combo, sections="MD", context=context
                    )
                    md_content = await format_full_section_chunks(
                        chunks=md_chunks, combo=combo, context=context
                    )

                    qa_chunks = await retrieve_full_section(
                        combo=combo, sections="QA", context=context
                    )
                    qa_content = await format_full_section_chunks(
                        chunks=qa_chunks, combo=combo, context=context
                    )

                    transcript = f"{md_content}\n\n{qa_content}"

                    logger.info(
                        f"[TRANSCRIPT] {bank_data['bank_name']} {actual_year} {actual_quarter}: "
                        f"Retrieved {len(md_content)} MD chars + {len(qa_content)} QA chars"
                    )

                except Exception as e:
                    logger.error(f"Error retrieving transcript for {bank_data['bank_name']}: {e}")
                    transcript = None

                if not transcript:
                    return (
                        bank_data["bank_name"],
                        bank_data["bank_symbol"],
                        {"has_content": False, "statements": []},
                    )

                result = await extract_outlook_from_transcript(
                    bank_data, transcript, outlook_categories, fiscal_year, quarter, context
                )

                return (bank_data["bank_name"], bank_data["bank_symbol"], result)

            except Exception as e:
                logger.error(f"Error processing outlook for bank {bank_data}: {e}")
                return (
                    bank_data.get("bank_name", "Unknown"),
                    bank_data.get("bank_symbol", ""),
                    {"has_content": False, "statements": []},
                )

    async def process_bank_section2(bank_data):
        async with semaphore:
            try:
                # Handle use_latest logic
                actual_year, actual_quarter = fiscal_year, quarter
                if use_latest:
                    latest = await find_latest_available_quarter(
                        bank_id=bank_data["bank_id"],
                        min_fiscal_year=fiscal_year,
                        min_quarter=quarter,
                        bank_name=bank_data["bank_name"],
                    )
                    if latest:
                        actual_year, actual_quarter = latest
                    else:
                        logger.warning(
                            f"[NO DATA] {bank_data['bank_name']}: No Q&A data available "
                            f"for {fiscal_year} {quarter} or later"
                        )
                        return (
                            bank_data["bank_name"],
                            bank_data["bank_symbol"],
                            {"has_content": False, "questions": []},
                        )

                # Direct Q&A retrieval (matches Call Summary pattern)
                combo = {
                    "bank_id": bank_data["bank_id"],
                    "bank_name": bank_data["bank_name"],
                    "bank_symbol": bank_data["bank_symbol"],
                    "fiscal_year": actual_year,
                    "quarter": actual_quarter,
                }

                try:
                    qa_chunks = await retrieve_full_section(
                        combo=combo, sections="QA", context=context
                    )
                    qa_content = await format_full_section_chunks(
                        chunks=qa_chunks, combo=combo, context=context
                    )

                    logger.info(
                        f"[Q&A SECTION] {bank_data['bank_name']} {actual_year} {actual_quarter}: "
                        f"Retrieved {len(qa_content)} chars"
                    )

                except Exception as e:
                    logger.error(f"Error retrieving Q&A for {bank_data['bank_name']}: {e}")
                    qa_content = None

                if not qa_content:
                    return (
                        bank_data["bank_name"],
                        bank_data["bank_symbol"],
                        {"has_content": False, "questions": []},
                    )

                result = await extract_questions_from_qa(
                    bank_data,
                    qa_content,
                    qa_market_vol_reg_categories,
                    fiscal_year,
                    quarter,
                    context,
                )

                return (bank_data["bank_name"], bank_data["bank_symbol"], result)

            except Exception as e:
                logger.error(f"Error processing Section 2 for bank {bank_data}: {e}")
                return (
                    bank_data.get("bank_name", "Unknown"),
                    bank_data.get("bank_symbol", ""),
                    {"has_content": False, "questions": []},
                )

    async def process_bank_section3(bank_data):
        async with semaphore:
            try:
                # Handle use_latest logic
                actual_year, actual_quarter = fiscal_year, quarter
                if use_latest:
                    latest = await find_latest_available_quarter(
                        bank_id=bank_data["bank_id"],
                        min_fiscal_year=fiscal_year,
                        min_quarter=quarter,
                        bank_name=bank_data["bank_name"],
                    )
                    if latest:
                        actual_year, actual_quarter = latest
                    else:
                        logger.warning(
                            f"[NO DATA] {bank_data['bank_name']}: No Q&A data available "
                            f"for {fiscal_year} {quarter} or later"
                        )
                        return (
                            bank_data["bank_name"],
                            bank_data["bank_symbol"],
                            {"has_content": False, "questions": []},
                        )

                # Direct Q&A retrieval (matches Call Summary pattern)
                combo = {
                    "bank_id": bank_data["bank_id"],
                    "bank_name": bank_data["bank_name"],
                    "bank_symbol": bank_data["bank_symbol"],
                    "fiscal_year": actual_year,
                    "quarter": actual_quarter,
                }

                try:
                    qa_chunks = await retrieve_full_section(
                        combo=combo, sections="QA", context=context
                    )
                    qa_content = await format_full_section_chunks(
                        chunks=qa_chunks, combo=combo, context=context
                    )

                    logger.info(
                        f"[Q&A SECTION] {bank_data['bank_name']} {actual_year} {actual_quarter}: "
                        f"Retrieved {len(qa_content)} chars"
                    )

                except Exception as e:
                    logger.error(f"Error retrieving Q&A for {bank_data['bank_name']}: {e}")
                    qa_content = None

                if not qa_content:
                    return (
                        bank_data["bank_name"],
                        bank_data["bank_symbol"],
                        {"has_content": False, "questions": []},
                    )

                result = await extract_questions_from_qa(
                    bank_data,
                    qa_content,
                    qa_pipelines_activity_categories,
                    fiscal_year,
                    quarter,
                    context,
                )

                return (bank_data["bank_name"], bank_data["bank_symbol"], result)

            except Exception as e:
                logger.error(f"Error processing Section 3 for bank {bank_data}: {e}")
                return (
                    bank_data.get("bank_name", "Unknown"),
                    bank_data.get("bank_symbol", ""),
                    {"has_content": False, "questions": []},
                )

    logger.info(f"[PHASES 1-3] Starting concurrent extraction for {len(monitored_banks)} banks...")

    outlook_tasks = [process_bank_outlook(bank) for bank in monitored_banks]
    section2_tasks = [process_bank_section2(bank) for bank in monitored_banks]
    section3_tasks = [process_bank_section3(bank) for bank in monitored_banks]

    bank_outlook, bank_section2, bank_section3 = await asyncio.gather(
        asyncio.gather(*outlook_tasks, return_exceptions=True),
        asyncio.gather(*section2_tasks, return_exceptions=True),
        asyncio.gather(*section3_tasks, return_exceptions=True),
    )

    bank_outlook_clean = []
    for r in bank_outlook:
        if isinstance(r, Exception):
            logger.error(f"Outlook extraction exception: {r}")
        else:
            bank_outlook_clean.append(r)

    bank_section2_clean = []
    for r in bank_section2:
        if isinstance(r, Exception):
            logger.error(f"Section 2 extraction exception: {r}")
        else:
            bank_section2_clean.append(r)

    bank_section3_clean = []
    for r in bank_section3:
        if isinstance(r, Exception):
            logger.error(f"Section 3 extraction exception: {r}")
        else:
            bank_section3_clean.append(r)

    logger.info("[PHASE 4] Aggregating results...")
    all_outlook, all_section2, all_section3 = aggregate_results(
        bank_outlook_clean, bank_section2_clean, bank_section3_clean
    )

    logger.info("[PHASES 5-7] Generating subtitles for all 3 sections...")
    subtitle1, subtitle2, subtitle3 = await asyncio.gather(
        generate_subtitle(
            all_outlook,
            "outlook",
            "Forward-looking outlook statements on IB activity, markets, pipelines",
            "Outlook: Capital markets activity across major institutions",
            context,
        ),
        generate_subtitle(
            all_section2,
            "questions",
            "Analyst questions on market volatility, risk management, regulatory changes",
            (
                "Conference calls: Benefits and threats of market volatility, "
                "line-draws and regulatory changes"
            ),
            context,
        ),
        generate_subtitle(
            all_section3,
            "questions",
            "Analyst questions on pipeline strength, M&A activity, transaction banking",
            "Conference calls: How well pipelines are holding up and areas of activity",
            context,
        ),
    )

    logger.info("[PHASE 8] Skipping batch formatting (disabled for performance)")
    formatted_outlook = all_outlook  # Use unformatted statements

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
            "generation_date": datetime.now().isoformat(),
            "mode": "latest_available" if use_latest else "exact_quarter",
            "subtitle_section1": f"Outlook: {subtitle1}",
            "subtitle_section2": f"Conference calls: {subtitle2}",
            "subtitle_section3": f"Conference calls: {subtitle3}",
        },
        "outlook": formatted_outlook,
        "section2_questions": formatted_section2,
        "section3_questions": formatted_section3,
    }

    logger.info(
        f"[PIPELINE COMPLETE] {results['metadata']['banks_with_outlook']} banks with outlook, "
        f"{results['metadata']['banks_with_section2']} banks with section 2, "
        f"{results['metadata']['banks_with_section3']} banks with section 3"
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
    async with get_connection() as conn:
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

        await conn.commit()

    logger.info(f"Report saved to database with execution_id: {execution_id}")


async def generate_cm_readthrough(
    fiscal_year: int, quarter: str, use_latest: bool = False, output_path: Optional[str] = None
) -> str:
    """
    Generate CM readthrough report for all monitored institutions.

    Args:
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")
        use_latest: If True, use latest available quarter >= specified quarter
        output_path: Optional custom output path

    Returns:
        Success or error message string
    """
    execution_id = str(uuid.uuid4())
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

        ssl_config = setup_ssl()
        auth_config = await setup_authentication(execution_id=execution_id, ssl_config=ssl_config)

        if not auth_config["success"]:
            error_msg = f"Authentication failed: {auth_config.get('error', 'Unknown error')}"
            logger.error(
                "etl.cm_readthrough.auth_failed", execution_id=execution_id, error=error_msg
            )
            raise RuntimeError(error_msg)

        context = {
            "execution_id": execution_id,
            "ssl_config": ssl_config,
            "auth_config": auth_config,
        }

        # Stage 2: Transcript Retrieval & Extraction (Parallel)
        results = await process_all_banks_parallel(
            fiscal_year=fiscal_year,
            quarter=quarter,
            context=context,
            use_latest=use_latest,
            outlook_categories=outlook_categories,
            qa_market_vol_reg_categories=qa_market_vol_reg_categories,
            qa_pipelines_activity_categories=qa_pipelines_activity_categories,
        )

        if not results or (
            not results.get("outlook")
            and not results.get("section2_questions")
            and not results.get("section3_questions")
        ):
            raise ValueError(
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

        docx_filename = docx_path.name

        await save_to_database(
            results=results,
            fiscal_year=fiscal_year,
            quarter=quarter,
            execution_id=execution_id,
            local_filepath=str(docx_path),
            s3_document_name=docx_filename,
        )

        metadata = results.get("metadata", {})
        banks_with_outlook = metadata.get("banks_with_outlook", 0)
        banks_with_section2 = metadata.get("banks_with_section2", 0)
        banks_with_section3 = metadata.get("banks_with_section3", 0)
        total_banks = metadata.get("banks_processed", 0)

        logger.info(
            "etl.cm_readthrough.completed",
            execution_id=execution_id,
            banks_with_data=f"{banks_with_outlook}/{total_banks} outlook, "
            f"{banks_with_section2}/{total_banks} section2, "
            f"{banks_with_section3}/{total_banks} section3",
        )

        return (
            f"✅ Complete: {docx_path}\n"
            f"   Banks: {banks_with_outlook}/{total_banks} outlook, "
            f"{banks_with_section2}/{total_banks} section2, "
            f"{banks_with_section3}/{total_banks} section3"
        )

    except (
        KeyError,
        TypeError,
        AttributeError,
        json.JSONDecodeError,
        FileNotFoundError,
    ) as e:
        # System errors - unexpected, likely code bugs
        error_msg = f"Error generating CM readthrough: {str(e)}"
        logger.error(
            "etl.cm_readthrough.error", execution_id=execution_id, error=error_msg, exc_info=True
        )
        return f"❌ {error_msg}"
    except (ValueError, RuntimeError) as e:
        # User-friendly errors - expected conditions (no data, auth failure, etc.)
        logger.error("etl.cm_readthrough.error", execution_id=execution_id, error=str(e))
        return f"⚠️ {str(e)}"


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

    print(f"\n🔄 Generating CM readthrough for {args.quarter} {args.year}...\n")

    result = asyncio.run(
        generate_cm_readthrough(
            fiscal_year=args.year,
            quarter=args.quarter,
            use_latest=args.use_latest,
            output_path=args.output,
        )
    )

    print(result)


if __name__ == "__main__":
    main()
