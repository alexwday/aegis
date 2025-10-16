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
- Section 2: Q&A for Global Markets, Risk Management, Corporate Banking, Regulatory Changes (3-column table)
- Section 3: Q&A for Investment Banking/M&A, Transaction Banking (3-column table)

Usage:
    python -m aegis.etls.cm_readthrough.main --year 2024 --quarter Q3
    python -m aegis.etls.cm_readthrough.main --year 2024 --quarter Q3 --output cm_readthrough.docx
"""

import argparse
import asyncio
import hashlib
import json
import sys
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy import text
import yaml
from pathlib import Path

# Import document converter functions
from aegis.etls.cm_readthrough.document_converter import (
    convert_docx_to_pdf,
    structured_data_to_markdown,
    create_combined_document
)

# Import direct transcript functions
from aegis.model.subagents.transcripts.retrieval import retrieve_full_section
from aegis.model.subagents.transcripts.formatting import format_full_section_chunks
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.llm_connector import complete_with_tools
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from aegis.utils.settings import config
from aegis.etls.cm_readthrough.config.config import (
    MODELS,
    TEMPERATURE,
    MAX_TOKENS,
    MAX_CONCURRENT_BANKS,
    get_monitored_institutions,
    get_outlook_categories,
    get_qa_market_volatility_regulatory_categories,
    get_qa_pipelines_activity_categories
)

# Initialize logging
setup_logging()
logger = get_logger()

# =============================================================================
# CATEGORY FORMATTING
# =============================================================================

def format_categories_for_prompt(categories: List[Dict[str, Any]]) -> str:
    """
    Format category dictionaries into a structured prompt format.

    Args:
        categories: List of category dicts with category, description, examples

    Returns:
        Formatted string for prompt injection
    """
    formatted_sections = []

    for cat in categories:
        section = f"<example_category>\n"
        section += f"Category: {cat['category']}\n"
        section += f"Description: {cat['description']}\n"

        if cat.get('examples') and len(cat['examples']) > 0:
            section += "Examples:\n"
            for example in cat['examples']:
                section += f"  - {example}\n"

        section += "</example_category>"
        formatted_sections.append(section)

    return "\n\n".join(formatted_sections)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

async def get_bank_info(bank_identifier: Any) -> Dict[str, Any]:
    """
    Resolve bank identifier (ID, symbol, or name) to full bank information.

    Args:
        bank_identifier: Bank ID (int), symbol (str), or name (str)

    Returns:
        Dict with bank_id, bank_name, and bank_symbol
    """
    async with get_connection() as conn:
        # Try different identifier types
        if isinstance(bank_identifier, int) or (isinstance(bank_identifier, str) and bank_identifier.isdigit()):
            # Bank ID provided
            query = text("""
                SELECT DISTINCT bank_id, bank_name, bank_symbol
                FROM aegis_data_availability
                WHERE bank_id = :bank_id
                LIMIT 1
            """)
            result = await conn.execute(query, {"bank_id": int(bank_identifier)})
        else:
            # Try symbol or name
            query = text("""
                SELECT DISTINCT bank_id, bank_name, bank_symbol
                FROM aegis_data_availability
                WHERE UPPER(bank_symbol) = UPPER(:identifier)
                   OR UPPER(bank_name) = UPPER(:identifier)
                LIMIT 1
            """)
            result = await conn.execute(query, {"identifier": str(bank_identifier)})

        row = result.first()
        if not row:
            raise ValueError(f"Bank not found: {bank_identifier}")

        return {
            "bank_id": row.bank_id,
            "bank_name": row.bank_name,
            "bank_symbol": row.bank_symbol
        }


async def find_latest_available_quarter(
    bank_id: int,
    min_fiscal_year: int,
    min_quarter: str,
    bank_name: str = ""
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
        # Convert quarter to sortable format
        quarter_map = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4}
        min_quarter_num = quarter_map.get(min_quarter, 1)

        query = text("""
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
        """)

        result = await conn.execute(
            query,
            {
                "bank_id": bank_id,
                "min_year": min_fiscal_year,
                "min_quarter": min_quarter_num
            }
        )

        row = result.first()
        if row:
            latest_year = row.fiscal_year
            latest_quarter = row.quarter

            # Log if we're using a more recent quarter
            if (latest_year > min_fiscal_year or
                (latest_year == min_fiscal_year and quarter_map.get(latest_quarter, 0) > min_quarter_num)):
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


def load_prompt_template(prompt_file: str) -> Dict[str, Any]:
    """Load prompt template from YAML file."""
    prompt_path = Path(__file__).parent / "prompts" / prompt_file

    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt file not found: {prompt_path}")

    with open(prompt_path, 'r') as f:
        return yaml.safe_load(f)


async def retrieve_full_transcript(
    bank_info: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    use_latest: bool
) -> Optional[str]:
    """
    Retrieve full transcript (MD + Q&A sections) as single string.

    Args:
        bank_info: Bank information dictionary
        fiscal_year: Fiscal year
        quarter: Quarter
        context: Execution context
        use_latest: Whether to use latest available quarter

    Returns:
        Combined transcript string or None if not available
    """
    # Determine which quarter to use
    actual_year, actual_quarter = fiscal_year, quarter

    if use_latest:
        latest = await find_latest_available_quarter(
            bank_id=bank_info["bank_id"],
            min_fiscal_year=fiscal_year,
            min_quarter=quarter,
            bank_name=bank_info["bank_name"]
        )
        if latest:
            actual_year, actual_quarter = latest
        else:
            logger.warning(
                f"[NO DATA] {bank_info['bank_name']}: No transcript data available "
                f"for {fiscal_year} {quarter} or later"
            )
            return None

    # Build combo dict for transcript retrieval
    combo = {
        "bank_id": bank_info["bank_id"],
        "bank_name": bank_info["bank_name"],
        "bank_symbol": bank_info["bank_symbol"],
        "fiscal_year": actual_year,
        "quarter": actual_quarter
    }

    try:
        # Get Management Discussion section
        md_chunks = await retrieve_full_section(combo=combo, sections="MD", context=context)
        md_content = await format_full_section_chunks(chunks=md_chunks, combo=combo, context=context)

        # Get Q&A section
        qa_chunks = await retrieve_full_section(combo=combo, sections="QA", context=context)
        qa_content = await format_full_section_chunks(chunks=qa_chunks, combo=combo, context=context)

        # Log retrieval
        logger.info(
            f"[TRANSCRIPT] {bank_info['bank_name']} {actual_year} {actual_quarter}: "
            f"Retrieved {len(md_content)} MD chars + {len(qa_content)} QA chars"
        )

        return f"{md_content}\n\n{qa_content}"

    except Exception as e:
        logger.error(f"Error retrieving transcript for {bank_info['bank_name']}: {e}")
        return None


async def retrieve_qa_section(
    bank_info: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    use_latest: bool
) -> Optional[str]:
    """
    Retrieve only Q&A section as single string.

    Args:
        bank_info: Bank information dictionary
        fiscal_year: Fiscal year
        quarter: Quarter
        context: Execution context
        use_latest: Whether to use latest available quarter

    Returns:
        Q&A section string or None if not available
    """
    # Determine which quarter to use
    actual_year, actual_quarter = fiscal_year, quarter

    if use_latest:
        latest = await find_latest_available_quarter(
            bank_id=bank_info["bank_id"],
            min_fiscal_year=fiscal_year,
            min_quarter=quarter,
            bank_name=bank_info["bank_name"]
        )
        if latest:
            actual_year, actual_quarter = latest
        else:
            logger.warning(
                f"[NO DATA] {bank_info['bank_name']}: No Q&A data available "
                f"for {fiscal_year} {quarter} or later"
            )
            return None

    # Build combo dict
    combo = {
        "bank_id": bank_info["bank_id"],
        "bank_name": bank_info["bank_name"],
        "bank_symbol": bank_info["bank_symbol"],
        "fiscal_year": actual_year,
        "quarter": actual_quarter
    }

    try:
        # Get Q&A section only
        qa_chunks = await retrieve_full_section(combo=combo, sections="QA", context=context)
        qa_content = await format_full_section_chunks(chunks=qa_chunks, combo=combo, context=context)

        # Log retrieval
        logger.info(
            f"[Q&A SECTION] {bank_info['bank_name']} {actual_year} {actual_quarter}: "
            f"Retrieved {len(qa_content)} chars"
        )

        return qa_content

    except Exception as e:
        logger.error(f"Error retrieving Q&A for {bank_info['bank_name']}: {e}")
        return None


# =============================================================================
# PHASE 1: OUTLOOK EXTRACTION
# =============================================================================

async def extract_outlook_from_transcript(
    bank_info: Dict[str, Any],
    transcript_content: str,
    categories: List[Dict[str, Any]],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any]
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
    # Load prompt template
    prompt_template = load_prompt_template("outlook_extraction.yaml")

    # Format categories using helper function
    categories_text = format_categories_for_prompt(categories)

    # Format messages
    messages = [
        {
            "role": "system",
            "content": prompt_template["system_template"].format(categories_list=categories_text)
        },
        {
            "role": "user",
            "content": prompt_template["user_template"].format(
                bank_name=bank_info["bank_name"],
                fiscal_year=fiscal_year,
                quarter=quarter,
                transcript_content=transcript_content
            )
        }
    ]

    # Create tool definition
    tools = [{
        "type": "function",
        "function": {
            "name": prompt_template["tool_name"],
            "description": prompt_template["tool_description"],
            "parameters": {
                "type": "object",
                "properties": prompt_template["tool_parameters"],
                "required": ["has_content", "statements"]
            }
        }
    }]

    # Call LLM
    llm_params = {
        "model": MODELS["outlook_extraction"],
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS
    }

    try:
        response = await complete_with_tools(
            messages=messages,
            tools=tools,
            context=context,
            llm_params=llm_params
        )

        # Extract tool call results
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
                logger.info(f"[NEW CATEGORIES] {bank_info['bank_name']}: Identified new categories: {', '.join(new_categories)}")

            logger.info(f"[OUTLOOK EXTRACTED] {bank_info['bank_name']}: {len(statements)} statements ({len(new_categories)} new categories)")
            return result
        else:
            logger.warning(f"No tool call in response for {bank_info['bank_name']}")
            return {"has_content": False, "statements": []}

    except Exception as e:
        logger.error(f"Error extracting outlook for {bank_info['bank_name']}: {e}")
        return {"has_content": False, "statements": []}


# =============================================================================
# PHASE 2: Q&A EXTRACTION
# =============================================================================

async def extract_questions_from_qa(
    bank_info: Dict[str, Any],
    qa_content: str,
    categories: List[Dict[str, Any]],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any]
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
    # Load prompt template
    prompt_template = load_prompt_template("qa_extraction_dynamic.yaml")

    # Format categories using helper function
    categories_text = format_categories_for_prompt(categories)

    # Format messages
    messages = [
        {
            "role": "system",
            "content": prompt_template["system_template"].format(categories_list=categories_text)
        },
        {
            "role": "user",
            "content": prompt_template["user_template"].format(
                bank_name=bank_info["bank_name"],
                fiscal_year=fiscal_year,
                quarter=quarter,
                qa_content=qa_content
            )
        }
    ]

    # Create tool definition
    tools = [{
        "type": "function",
        "function": {
            "name": prompt_template["tool_name"],
            "description": prompt_template["tool_description"],
            "parameters": {
                "type": "object",
                "properties": prompt_template["tool_parameters"],
                "required": ["has_content", "questions"]
            }
        }
    }]

    # Call LLM
    llm_params = {
        "model": MODELS["qa_extraction"],
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS
    }

    try:
        response = await complete_with_tools(
            messages=messages,
            tools=tools,
            context=context,
            llm_params=llm_params
        )

        # Extract tool call results
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
                logger.info(f"[NEW CATEGORIES] {bank_info['bank_name']}: Identified new Q&A categories: {', '.join(set(new_categories))}")

            logger.info(f"[QUESTIONS EXTRACTED] {bank_info['bank_name']}: {len(questions)} questions ({len(new_categories)} new categories)")
            return result
        else:
            logger.warning(f"No tool call in response for {bank_info['bank_name']}")
            return {"has_content": False, "questions": []}

    except Exception as e:
        logger.error(f"Error extracting questions for {bank_info['bank_name']}: {e}")
        return {"has_content": False, "questions": []}


# =============================================================================
# PHASE 3: AGGREGATION
# =============================================================================

def aggregate_results(
    bank_outlook: List[Tuple[str, str, Dict]],  # [(bank_name, bank_symbol, outlook_result)]
    bank_section2: List[Tuple[str, str, Dict]],  # [(bank_name, bank_symbol, section2_result)]
    bank_section3: List[Tuple[str, str, Dict]]   # [(bank_name, bank_symbol, section3_result)]
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

    # Filter banks with content and organize
    for bank_name, bank_symbol, result in bank_outlook:
        if result.get("has_content") and result.get("statements"):
            all_outlook[bank_name] = {
                "bank_symbol": bank_symbol,
                "statements": result["statements"]
            }

    for bank_name, bank_symbol, result in bank_section2:
        if result.get("has_content") and result.get("questions"):
            all_section2[bank_name] = {
                "bank_symbol": bank_symbol,
                "questions": result["questions"]
            }

    for bank_name, bank_symbol, result in bank_section3:
        if result.get("has_content") and result.get("questions"):
            all_section3[bank_name] = {
                "bank_symbol": bank_symbol,
                "questions": result["questions"]
            }

    logger.info(
        f"[AGGREGATION] {len(all_outlook)} banks with outlook, "
        f"{len(all_section2)} banks with section 2 questions, "
        f"{len(all_section3)} banks with section 3 questions"
    )

    return all_outlook, all_section2, all_section3


# =============================================================================
# PHASES 5-7: SUBTITLE GENERATION (3 sections)
# =============================================================================

async def generate_subtitle(
    content_data: Dict[str, Any],
    content_type: str,
    section_context: str,
    default_subtitle: str,
    context: Dict[str, Any]
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

    # Load universal prompt template
    prompt_template = load_prompt_template("subtitle_generation.yaml")

    # Prepare content summary for subtitle generation
    content_summary = {}
    for bank_name, data in content_data.items():
        # Handle both outlook (statements) and questions
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

    # Format messages
    messages = [
        {
            "role": "system",
            "content": prompt_template["system_template"]
        },
        {
            "role": "user",
            "content": prompt_template["user_template"].format(
                content_type=content_type,
                section_context=section_context,
                content_json=json.dumps(content_summary, indent=2)
            )
        }
    ]

    # Create tool definition
    tools = [{
        "type": "function",
        "function": {
            "name": prompt_template["tool_name"],
            "description": prompt_template["tool_description"],
            "parameters": {
                "type": "object",
                "properties": prompt_template["tool_parameters"],
                "required": ["subtitle"]
            }
        }
    }]

    # Call LLM
    llm_params = {
        "model": MODELS["batch_formatting"],  # Reuse formatting model
        "temperature": TEMPERATURE,
        "max_tokens": 100,  # Subtitle is short
        "tool_choice": "required"  # Force tool use
    }

    try:
        logger.info(f"[SUBTITLE] Generating {content_type} subtitle from {len(content_data)} banks...")

        response = await complete_with_tools(
            messages=messages,
            tools=tools,
            context=context,
            llm_params=llm_params
        )

        # Extract tool call results
        tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")
        if tool_calls:
            tool_call = tool_calls[0]
            result = json.loads(tool_call["function"]["arguments"])
            subtitle = result.get("subtitle", default_subtitle)

            logger.info(f"[SUBTITLE GENERATED] {subtitle}")
            return subtitle
        else:
            logger.warning(f"No tool call in subtitle generation, using default: {default_subtitle}")
            return default_subtitle

    except Exception as e:
        logger.error(f"Error generating subtitle: {e}")
        return default_subtitle




# =============================================================================
# PHASE 8: BATCH FORMATTING
# =============================================================================

async def format_outlook_batch(
    all_outlook: Dict[str, Any],
    context: Dict[str, Any]
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

    # Load prompt template
    prompt_template = load_prompt_template("batch_formatting.yaml")

    # Prepare outlook for formatting (remove bank_symbol for cleaner JSON)
    outlook_for_formatting = {
        bank_name: data["statements"]
        for bank_name, data in all_outlook.items()
    }

    # Format messages
    messages = [
        {
            "role": "system",
            "content": prompt_template["system_template"]
        },
        {
            "role": "user",
            "content": prompt_template["user_template"].format(
                quotes_json=json.dumps(outlook_for_formatting, indent=2)  # Note: template still says "quotes"
            )
        }
    ]

    # Create tool definition
    tools = [{
        "type": "function",
        "function": {
            "name": prompt_template["tool_name"],
            "description": prompt_template["tool_description"],
            "parameters": {
                "type": "object",
                "properties": prompt_template["tool_parameters"],
                "required": ["formatted_quotes"]  # Note: template still says "quotes"
            }
        }
    }]

    # Call LLM
    llm_params = {
        "model": MODELS["batch_formatting"],
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS
    }

    try:
        logger.info(f"[BATCH FORMATTING] Formatting {len(all_outlook)} banks with outlook...")

        response = await complete_with_tools(
            messages=messages,
            tools=tools,
            context=context,
            llm_params=llm_params
        )

        # Extract tool call results
        tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")
        if tool_calls:
            tool_call = tool_calls[0]
            formatted_result = json.loads(tool_call["function"]["arguments"])
            formatted_outlook = formatted_result.get("formatted_quotes", {})  # Tool returns "quotes" key

            # Merge formatted outlook back with bank_symbol
            result = {}
            for bank_name, data in all_outlook.items():
                if bank_name in formatted_outlook:
                    result[bank_name] = {
                        "bank_symbol": data["bank_symbol"],
                        "statements": formatted_outlook[bank_name]
                    }
                else:
                    # Fallback: keep original if formatting failed for this bank
                    result[bank_name] = data

            logger.info(f"[BATCH FORMATTING] Successfully formatted outlook for {len(result)} banks")
            return result
        else:
            logger.warning("No tool call in formatting response, returning original")
            return all_outlook

    except Exception as e:
        logger.error(f"Error in batch formatting: {e}")
        return all_outlook  # Fallback to original


# =============================================================================
# MAIN ORCHESTRATION
# =============================================================================

async def process_all_banks_parallel(
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    use_latest: bool = False
) -> Dict[str, Any]:
    """
    Process all banks with concurrent execution.

    Args:
        fiscal_year: Year
        quarter: Quarter
        context: Execution context
        use_latest: If True, use latest available quarter >= specified quarter

    Returns:
        Combined results dictionary
    """
    # Load configuration
    monitored_banks = get_monitored_institutions()
    outlook_categories = get_outlook_categories()
    qa_market_vol_reg_categories = get_qa_market_volatility_regulatory_categories()
    qa_pipelines_activity_categories = get_qa_pipelines_activity_categories()

    logger.info(
        f"Processing {len(monitored_banks)} banks for {fiscal_year} {quarter} "
        f"(mode: {'latest available' if use_latest else 'exact quarter'})"
    )
    logger.info(
        f"Categories - Outlook: {len(outlook_categories)}, "
        f"Market Vol/Reg: {len(qa_market_vol_reg_categories)}, "
        f"Pipelines/Activity: {len(qa_pipelines_activity_categories)}"
    )

    # Concurrency control
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_BANKS)

    # Phase 1: Extract outlook from all banks (concurrent)
    async def process_bank_outlook(bank_data):
        async with semaphore:
            try:
                bank_info = await get_bank_info(bank_data["bank_id"])

                # Get full transcript
                transcript = await retrieve_full_transcript(
                    bank_info, fiscal_year, quarter, context, use_latest
                )

                if not transcript:
                    return (bank_info["bank_name"], bank_info["bank_symbol"], {"has_content": False, "statements": []})

                # Extract outlook
                result = await extract_outlook_from_transcript(
                    bank_info, transcript, outlook_categories,
                    fiscal_year, quarter, context
                )

                return (bank_info["bank_name"], bank_info["bank_symbol"], result)

            except Exception as e:
                logger.error(f"Error processing outlook for bank {bank_data}: {e}")
                return (bank_data.get("bank_name", "Unknown"), bank_data.get("bank_symbol", ""), {"has_content": False, "statements": []})

    # Phase 2: Extract Section 2 questions from all banks (concurrent)
    async def process_bank_section2(bank_data):
        async with semaphore:
            try:
                bank_info = await get_bank_info(bank_data["bank_id"])

                # Get Q&A section only
                qa_content = await retrieve_qa_section(
                    bank_info, fiscal_year, quarter, context, use_latest
                )

                if not qa_content:
                    return (bank_info["bank_name"], bank_info["bank_symbol"], {"has_content": False, "questions": []})

                # Extract Section 2 questions
                result = await extract_questions_from_qa(
                    bank_info, qa_content, qa_market_vol_reg_categories,
                    fiscal_year, quarter, context
                )

                return (bank_info["bank_name"], bank_info["bank_symbol"], result)

            except Exception as e:
                logger.error(f"Error processing Section 2 for bank {bank_data}: {e}")
                return (bank_data.get("bank_name", "Unknown"), bank_data.get("bank_symbol", ""), {"has_content": False, "questions": []})

    # Phase 3: Extract Section 3 questions from all banks (concurrent)
    async def process_bank_section3(bank_data):
        async with semaphore:
            try:
                bank_info = await get_bank_info(bank_data["bank_id"])

                # Get Q&A section only
                qa_content = await retrieve_qa_section(
                    bank_info, fiscal_year, quarter, context, use_latest
                )

                if not qa_content:
                    return (bank_info["bank_name"], bank_info["bank_symbol"], {"has_content": False, "questions": []})

                # Extract Section 3 questions
                result = await extract_questions_from_qa(
                    bank_info, qa_content, qa_pipelines_activity_categories,
                    fiscal_year, quarter, context
                )

                return (bank_info["bank_name"], bank_info["bank_symbol"], result)

            except Exception as e:
                logger.error(f"Error processing Section 3 for bank {bank_data}: {e}")
                return (bank_data.get("bank_name", "Unknown"), bank_data.get("bank_symbol", ""), {"has_content": False, "questions": []})

    # Execute Phases 1, 2, 3 concurrently (all 3 extraction phases in parallel)
    logger.info(f"[PHASES 1-3] Starting concurrent extraction for {len(monitored_banks)} banks...")

    outlook_tasks = [process_bank_outlook(bank) for bank in monitored_banks]
    section2_tasks = [process_bank_section2(bank) for bank in monitored_banks]
    section3_tasks = [process_bank_section3(bank) for bank in monitored_banks]

    # Run all 3 phases in parallel
    bank_outlook, bank_section2, bank_section3 = await asyncio.gather(
        asyncio.gather(*outlook_tasks, return_exceptions=True),
        asyncio.gather(*section2_tasks, return_exceptions=True),
        asyncio.gather(*section3_tasks, return_exceptions=True)
    )

    # Filter out exceptions and log
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

    # Phase 4: Aggregation
    logger.info("[PHASE 4] Aggregating results...")
    all_outlook, all_section2, all_section3 = aggregate_results(
        bank_outlook_clean, bank_section2_clean, bank_section3_clean
    )

    # Phases 5-7: Subtitle generation (all 3 in parallel using universal prompt)
    logger.info("[PHASES 5-7] Generating subtitles for all 3 sections...")
    subtitle1, subtitle2, subtitle3 = await asyncio.gather(
        generate_subtitle(
            all_outlook,
            "outlook",
            "Forward-looking outlook statements on IB activity, markets, pipelines",
            "Outlook: Capital markets activity across major institutions",
            context
        ),
        generate_subtitle(
            all_section2,
            "questions",
            "Analyst questions on market volatility, risk management, regulatory changes",
            "Conference calls: Benefits and threats of market volatility, line-draws and regulatory changes",
            context
        ),
        generate_subtitle(
            all_section3,
            "questions",
            "Analyst questions on pipeline strength, M&A activity, transaction banking",
            "Conference calls: How well pipelines are holding up and areas of activity",
            context
        )
    )

    # Phase 8: Batch formatting
    logger.info("[PHASE 8] Batch formatting outlook...")
    formatted_outlook = await format_outlook_batch(all_outlook, context)

    # Questions typically don't need formatting (verbatim extraction)
    formatted_section2 = all_section2
    formatted_section3 = all_section3

    # Prepare results for document generation
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
            "subtitle_section3": f"Conference calls: {subtitle3}"
        },
        "outlook": formatted_outlook,
        "section2_questions": formatted_section2,
        "section3_questions": formatted_section3
    }

    logger.info(
        f"[PIPELINE COMPLETE] {results['metadata']['banks_with_outlook']} banks with outlook, "
        f"{results['metadata']['banks_with_section2']} banks with section 2, "
        f"{results['metadata']['banks_with_section3']} banks with section 3"
    )

    return results


# =============================================================================
# DATABASE STORAGE
# =============================================================================

async def save_to_database(
    results: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    markdown_content: str,
    execution_id: str
) -> None:
    """
    Save the report to the database.

    Args:
        results: Structured results
        fiscal_year: Year
        quarter: Quarter
        markdown_content: Markdown version of report
        execution_id: Execution UUID
    """
    async with get_connection() as conn:
        # Save to aegis_reports table
        query = text("""
            INSERT INTO aegis_reports (
                execution_id,
                report_type,
                fiscal_year,
                quarter,
                markdown_content,
                metadata,
                generation_date
            ) VALUES (
                :execution_id,
                'cm_readthrough',
                :fiscal_year,
                :quarter,
                :markdown_content,
                :metadata,
                NOW()
            )
        """)

        await conn.execute(
            query,
            {
                "execution_id": str(execution_id),
                "fiscal_year": fiscal_year,
                "quarter": quarter,
                "markdown_content": markdown_content,
                "metadata": json.dumps(results)
            }
        )

        await conn.commit()

    logger.info(f"Report saved to database with execution_id: {execution_id}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

async def main():
    """Main entry point for the CM Readthrough ETL."""
    parser = argparse.ArgumentParser(
        description="Generate CM Readthrough report for all monitored institutions"
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Fiscal year (e.g., 2024)"
    )
    parser.add_argument(
        "--quarter",
        type=str,
        required=True,
        help="Quarter (e.g., Q3)"
    )
    parser.add_argument(
        "--use-latest",
        action="store_true",
        help="Use latest available quarter if newer than specified (minimum quarter mode)"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file path (optional)"
    )
    parser.add_argument(
        "--no-pdf",
        action="store_true",
        help="Skip PDF generation"
    )

    args = parser.parse_args()

    # Generate execution ID
    execution_id = uuid.uuid4()
    logger.info(f"Starting CM Readthrough ETL with execution_id: {execution_id}")

    # Setup context
    ssl_config = setup_ssl()
    auth_config = await setup_authentication(execution_id=str(execution_id), ssl_config=ssl_config)

    context = {
        "execution_id": str(execution_id),
        "ssl_config": ssl_config,
        "auth_config": auth_config
    }

    # Process all banks
    try:
        results = await process_all_banks_parallel(
            fiscal_year=args.year,
            quarter=args.quarter,
            context=context,
            use_latest=args.use_latest
        )

        if not results or (not results.get("outlook") and not results.get("questions")):
            logger.warning("No results generated")
            return

        # Generate document
        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(exist_ok=True)

        # Create filename with timestamp for uniqueness
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        content_hash = hashlib.md5(
            f"{args.year}_{args.quarter}_{timestamp}".encode()
        ).hexdigest()[:8]

        if args.output:
            docx_path = Path(args.output)
        else:
            docx_path = output_dir / f"CM_Readthrough_{args.year}_{args.quarter}_{content_hash}.docx"

        # Create Word document
        create_combined_document(results, str(docx_path))
        logger.info(f"Word document created: {docx_path}")

        # Convert to PDF if requested
        if not args.no_pdf:
            pdf_path = str(docx_path).replace(".docx", ".pdf")
            if convert_docx_to_pdf(str(docx_path), pdf_path):
                logger.info(f"PDF created: {pdf_path}")

        # Generate markdown for database
        markdown_content = structured_data_to_markdown(results)

        # Save to database
        await save_to_database(
            results=results,
            fiscal_year=args.year,
            quarter=args.quarter,
            markdown_content=markdown_content,
            execution_id=execution_id
        )

        logger.info(f"CM Readthrough ETL completed successfully")

    except Exception as e:
        logger.error(f"Error in CM Readthrough ETL: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())
