"""
WM Readthrough ETL Script - Generates wealth management readthrough reports across multiple banks.

This modular script uses a page-by-page pipeline:
- Page 1: WM narratives for monitored US banks (IMPLEMENTED)
- Page 2: Three-theme Q&A for all US banks (TODO)
- Page 3: Canadian AM analysis (AUM/flows + focus areas) (TODO)
- Page 4: Three-column table (NII/NIM, Credit/PCL, Tariffs) (TODO)
- Page 5+: Six-theme Q&A for all US banks (TODO)

Document Structure:
- Page 1: WM narratives with embedded quotes
- Page 2: Three-theme Q&A table
- Page 3: Canadian AM dual analysis
- Page 4: Three-column banking metrics table
- Page 5+: Six-theme Q&A table

Usage:
    python -m aegis.etls.wm_readthrough.main --year 2025 --quarter Q1
    python -m aegis.etls.wm_readthrough.main --year 2025 --quarter Q1 --output wm_readthrough.docx
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
from pathlib import Path

# Import document converter functions
from aegis.etls.wm_readthrough.document_converter import (
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
from aegis.utils.prompt_loader import load_prompt_from_db
from aegis.etls.wm_readthrough.config.config import (
    MODELS,
    TEMPERATURE,
    MAX_TOKENS,
    MAX_CONCURRENT_BANKS,
    get_monitored_institutions,
    get_page2_themes,
    get_page5_themes
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
# HELPER FUNCTIONS (REUSABLE ACROSS ALL PAGES)
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


# =============================================================================
# PROMPT LOADING (matching cm_readthrough pattern)
# =============================================================================

def load_prompt_template(prompt_file: str, execution_id: str = None) -> Dict[str, Any]:
    """
    Load prompt template from database (matching cm_readthrough pattern).

    Args:
        prompt_file: YAML filename (e.g., "wm_narrative_extraction.yaml")
        execution_id: Execution ID for tracking

    Returns:
        Dict with system_template, user_template, tool_name, tool_description, tool_parameters

    Note:
        The prompt_file parameter accepts filenames for compatibility, but we strip
        the .yaml extension and load from database using load_prompt_from_db().
        YAML files in /prompts/ are kept as reference for database uploads.
    """
    # Convert filename to prompt name (remove .yaml extension)
    prompt_name = prompt_file.replace(".yaml", "")

    # Load from database
    prompt_data = load_prompt_from_db(
        layer="wm_readthrough_etl",
        name=prompt_name,
        compose_with_globals=False,  # ETL doesn't use global contexts
        available_databases=None,
        execution_id=execution_id
    )

    # Convert database format to wm_readthrough's expected format
    result = {
        'system_template': prompt_data['system_prompt'],
        'user_template': prompt_data.get('user_prompt', '')
    }

    # Extract tool definition components if present
    if prompt_data.get('tool_definition'):
        tool_def = prompt_data['tool_definition']
        result['tool_name'] = tool_def['function']['name']
        result['tool_description'] = tool_def['function']['description']
        result['tool_parameters'] = tool_def['function']['parameters']['properties']

    return result


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
# PAGE 1: WM NARRATIVE EXTRACTION (MONITORED US BANKS)
# =============================================================================

async def extract_page1_wm_narrative(
    bank_info: Dict[str, Any],
    transcript_content: str,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Extract wealth management narrative from full transcript (MD-focused, Q&A for support).

    Args:
        bank_info: Bank information dictionary
        transcript_content: Full transcript text
        fiscal_year: Year
        quarter: Quarter
        context: Execution context

    Returns:
        {
            "has_content": bool,
            "key_theme": "Brief WM theme",
            "narrative_summary": "Full paragraph with embedded quotes and statistics",
            "supporting_quotes": [
                {"quote": "...", "speaker": "...", "source": "MD/QA", "context": "..."}
            ]
        }
    """
    # Load prompt template
    execution_id = context.get('execution_id')
    prompt_template = load_prompt_template("wm_narrative_extraction.yaml", execution_id)

    # Format messages
    messages = [
        {
            "role": "system",
            "content": prompt_template["system_template"]
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
                "required": ["has_content", "key_theme", "narrative_summary", "supporting_quotes"]
            }
        }
    }]

    # Call LLM
    llm_params = {
        "model": MODELS["page1_wm_narrative"],
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
                logger.info(f"[NO WM CONTENT] {bank_info['bank_name']}: No WM narrative found")
                return {"has_content": False, "key_theme": "", "narrative_summary": "", "supporting_quotes": []}

            logger.info(
                f"[PAGE 1 EXTRACTED] {bank_info['bank_name']}: "
                f"{len(result.get('narrative_summary', ''))} chars, "
                f"{len(result.get('supporting_quotes', []))} quotes"
            )
            return result
        else:
            logger.warning(f"No tool call in response for {bank_info['bank_name']}")
            return {"has_content": False, "key_theme": "", "narrative_summary": "", "supporting_quotes": []}

    except Exception as e:
        logger.error(f"Error extracting Page 1 for {bank_info['bank_name']}: {e}")
        return {"has_content": False, "key_theme": "", "narrative_summary": "", "supporting_quotes": []}


# =============================================================================
# PAGE 2: THREE-THEME Q&A (ALL US BANKS) - TODO
# =============================================================================

async def extract_page2_three_themes(
    bank_info: Dict[str, Any],
    qa_content: str,
    themes: List[Dict[str, Any]],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Extract Q&A questions for three themes (Tariffs/Uncertainty, Assets/Fee-income, Recruitment).

    Args:
        bank_info: Bank metadata dictionary
        qa_content: Q&A section content
        themes: List of theme configurations (not used - themes hardcoded in prompt)
        fiscal_year: Year
        quarter: Quarter
        context: Execution context

    Returns:
        {
            "has_content": bool,
            "questions": [
                {"theme": "...", "verbatim_question": "...", "analyst_name": "...", "analyst_firm": "..."}
            ]
        }
    """
    execution_id = context.get("execution_id")

    # Load prompt template
    prompt_template = load_prompt_template("three_theme_qa_extraction.yaml", execution_id)

    # Format messages
    messages = [
        {
            "role": "system",
            "content": prompt_template["system_template"]
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
        "model": MODELS["page2_three_themes"],
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
                logger.info(f"[NO PAGE 2 CONTENT] {bank_info['bank_name']}: No themed questions found")
                return {"has_content": False, "questions": []}

            logger.info(
                f"[PAGE 2 EXTRACTED] {bank_info['bank_name']}: "
                f"{len(result.get('questions', []))} themed questions"
            )
            return result
        else:
            logger.warning(f"No tool call in response for {bank_info['bank_name']}")
            return {"has_content": False, "questions": []}

    except Exception as e:
        logger.error(f"Error extracting Page 2 for {bank_info['bank_name']}: {e}")
        return {"has_content": False, "questions": []}


# =============================================================================
# PAGE 3: CANADIAN AM ANALYSIS - TODO
# =============================================================================

async def extract_page3_canadian_am(
    bank_info: Dict[str, Any],
    transcript_content: str,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    use_latest: bool = False
) -> Dict[str, Any]:
    """
    Extract Canadian AM dual analysis (AUM/net flows + focus areas with themed questions).

    This function makes TWO LLM calls:
    1. Extract AUM/net flows metrics from full transcript
    2. Extract focus areas with themed questions from Q&A section

    Args:
        bank_info: Bank metadata dictionary
        transcript_content: Full transcript content (MD + Q&A)
        fiscal_year: Year
        quarter: Quarter
        context: Execution context

    Returns:
        {
            "has_content": bool,
            "aum_netflows": {
                "total_aum": "...",
                "aum_breakdown": "...",
                "net_flows": "...",
                "notable_metrics": "..."
            },
            "focus_areas": [
                {
                    "theme_title": "LLM-generated",
                    "questions": [{"verbatim_question": "...", "analyst_name": "...", "analyst_firm": "..."}]
                }
            ]
        }
    """
    execution_id = context.get("execution_id")

    # ========== LLM CALL 1: Extract AUM/Net Flows ==========
    aum_template = load_prompt_template("aum_netflows_extraction.yaml", execution_id)

    aum_messages = [
        {"role": "system", "content": aum_template["system_template"]},
        {"role": "user", "content": aum_template["user_template"].format(
            bank_name=bank_info["bank_name"],
            fiscal_year=fiscal_year,
            quarter=quarter,
            transcript_content=transcript_content
        )}
    ]

    # Create tool definition
    aum_tools = [{
        "type": "function",
        "function": {
            "name": aum_template["tool_name"],
            "description": aum_template["tool_description"],
            "parameters": {
                "type": "object",
                "properties": aum_template["tool_parameters"],
                "required": ["has_content", "total_aum", "aum_breakdown", "net_flows", "notable_metrics"]
            }
        }
    }]

    llm_params = {
        "model": MODELS["page3_canadian_am"],
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS
    }

    aum_result = {}
    try:
        response = await complete_with_tools(
            messages=aum_messages,
            tools=aum_tools,
            context=context,
            llm_params=llm_params
        )

        tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")
        if tool_calls:
            tool_call = tool_calls[0]
            aum_result = json.loads(tool_call["function"]["arguments"])
            logger.info(f"[PAGE 3.1 EXTRACTED] {bank_info['bank_name']}: AUM metrics")
        else:
            logger.warning(f"No AUM tool call for {bank_info['bank_name']}")
            aum_result = {"has_content": False}

    except Exception as e:
        logger.error(f"Error extracting AUM for {bank_info['bank_name']}: {e}")
        aum_result = {"has_content": False}

    # ========== LLM CALL 2: Extract Focus Areas ==========
    # First, retrieve Q&A section
    qa_content = await retrieve_qa_section(bank_info, fiscal_year, quarter, context, use_latest=use_latest)

    focus_result = {}
    if qa_content:
        focus_template = load_prompt_template("am_focus_areas_extraction.yaml", execution_id)

        focus_messages = [
            {"role": "system", "content": focus_template["system_template"]},
            {"role": "user", "content": focus_template["user_template"].format(
                bank_name=bank_info["bank_name"],
                fiscal_year=fiscal_year,
                quarter=quarter,
                qa_content=qa_content
            )}
        ]

        # Create tool definition
        focus_tools = [{
            "type": "function",
            "function": {
                "name": focus_template["tool_name"],
                "description": focus_template["tool_description"],
                "parameters": {
                    "type": "object",
                    "properties": focus_template["tool_parameters"],
                    "required": ["has_content", "focus_areas"]
                }
            }
        }]

        try:
            response = await complete_with_tools(
                messages=focus_messages,
                tools=focus_tools,
                context=context,
                llm_params=llm_params
            )

            tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")
            if tool_calls:
                tool_call = tool_calls[0]
                focus_result = json.loads(tool_call["function"]["arguments"])
                logger.info(
                    f"[PAGE 3.2 EXTRACTED] {bank_info['bank_name']}: "
                    f"{len(focus_result.get('focus_areas', []))} themes"
                )
            else:
                logger.warning(f"No focus areas tool call for {bank_info['bank_name']}")
                focus_result = {"has_content": False, "focus_areas": []}

        except Exception as e:
            logger.error(f"Error extracting focus areas for {bank_info['bank_name']}: {e}")
            focus_result = {"has_content": False, "focus_areas": []}
    else:
        logger.info(f"[NO Q&A] {bank_info['bank_name']}: Cannot extract focus areas without Q&A")
        focus_result = {"has_content": False, "focus_areas": []}

    # Combine results
    has_content = aum_result.get("has_content", False) or focus_result.get("has_content", False)

    return {
        "has_content": has_content,
        "aum_netflows": {
            "total_aum": aum_result.get("total_aum", "N/A"),
            "aum_breakdown": aum_result.get("aum_breakdown", "N/A"),
            "net_flows": aum_result.get("net_flows", "N/A"),
            "notable_metrics": aum_result.get("notable_metrics", "N/A")
        },
        "focus_areas": focus_result.get("focus_areas", [])
    }


# =============================================================================
# PAGE 4: THREE-COLUMN TABLE (ALL US BANKS) - TODO
# =============================================================================

async def extract_page4_table_data(
    bank_info: Dict[str, Any],
    transcript_content: str,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Extract three-column table data (NII/NIM, Credit/PCL, Tariffs/Uncertainty).

    Args:
        bank_info: Bank metadata dictionary
        transcript_content: Full transcript content
        fiscal_year: Year
        quarter: Quarter
        context: Execution context

    Returns:
        {
            "has_content": bool,
            "nii_nim_summary": "...",
            "credit_pcl_summary": "...",
            "tariff_uncertainty_summary": "..."
        }
    """
    execution_id = context.get("execution_id")

    # Load prompt template
    prompt_template = load_prompt_template("three_column_table_extraction.yaml", execution_id)

    # Format messages
    messages = [
        {
            "role": "system",
            "content": prompt_template["system_template"]
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
                "required": ["has_content", "nii_nim_summary", "credit_pcl_summary", "tariff_uncertainty_summary"]
            }
        }
    }]

    # Call LLM
    llm_params = {
        "model": MODELS["page4_table_data"],
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
                logger.info(f"[NO PAGE 4 CONTENT] {bank_info['bank_name']}: No table data found")
                return {
                    "has_content": False,
                    "nii_nim_summary": "",
                    "credit_pcl_summary": "",
                    "tariff_uncertainty_summary": ""
                }

            logger.info(f"[PAGE 4 EXTRACTED] {bank_info['bank_name']}: Table data extracted")
            return result
        else:
            logger.warning(f"No tool call in response for {bank_info['bank_name']}")
            return {
                "has_content": False,
                "nii_nim_summary": "",
                "credit_pcl_summary": "",
                "tariff_uncertainty_summary": ""
            }

    except Exception as e:
        logger.error(f"Error extracting Page 4 for {bank_info['bank_name']}: {e}")
        return {
            "has_content": False,
            "nii_nim_summary": "",
            "credit_pcl_summary": "",
            "tariff_uncertainty_summary": ""
        }


# =============================================================================
# PAGE 5+: SIX-THEME Q&A (ALL US BANKS) - TODO
# =============================================================================

async def extract_page5_six_themes(
    bank_info: Dict[str, Any],
    qa_content: str,
    themes: List[Dict[str, Any]],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Extract Q&A questions for six themes (NIM, NII guidance, Loan growth, Deposits, Expenses/Tech, Tariffs).

    Args:
        bank_info: Bank metadata dictionary
        qa_content: Q&A section content
        themes: List of theme configurations (not used - themes hardcoded in prompt)
        fiscal_year: Year
        quarter: Quarter
        context: Execution context

    Returns:
        {
            "has_content": bool,
            "questions": [
                {"theme": "...", "verbatim_question": "...", "analyst_name": "...", "analyst_firm": "..."}
            ]
        }
    """
    execution_id = context.get("execution_id")

    # Load prompt template
    prompt_template = load_prompt_template("six_theme_qa_extraction.yaml", execution_id)

    # Format messages
    messages = [
        {
            "role": "system",
            "content": prompt_template["system_template"]
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
        "model": MODELS["page5_six_themes"],
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
                logger.info(f"[NO PAGE 5 CONTENT] {bank_info['bank_name']}: No six-theme questions found")
                return {"has_content": False, "questions": []}

            logger.info(
                f"[PAGE 5 EXTRACTED] {bank_info['bank_name']}: "
                f"{len(result.get('questions', []))} six-theme questions"
            )
            return result
        else:
            logger.warning(f"No tool call in response for {bank_info['bank_name']}")
            return {"has_content": False, "questions": []}

    except Exception as e:
        logger.error(f"Error extracting Page 5 for {bank_info['bank_name']}: {e}")
        return {"has_content": False, "questions": []}


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
    Process all banks with concurrent execution across all 5 pages.

    Page 1: Monitored US banks (subset) - WM narratives
    Page 2: All US banks - Three-theme Q&A
    Page 3: Canadian Asset Managers - Dual analysis (AUM + focus areas)
    Page 4: All US banks - Three-column table
    Page 5: All US banks - Six-theme Q&A

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
    page2_themes = get_page2_themes()
    page5_themes = get_page5_themes()

    # Filter banks by type with defensive checking
    us_banks = [b for b in monitored_banks if b.get("type") == "US_Banks"]
    canadian_am = [b for b in monitored_banks if b.get("type") == "Canadian_Asset_Managers"]
    monitored_us = [b for b in monitored_banks if b.get("type") == "Monitored_US_Banks"]

    # Log any banks without type field or with unknown types
    untyped_banks = [b for b in monitored_banks if not b.get("type")]
    if untyped_banks:
        logger.warning(
            f"[CONFIG WARNING] {len(untyped_banks)} banks missing 'type' field: "
            f"{[b.get('bank_name', 'Unknown') for b in untyped_banks]}"
        )

    all_types = set(b.get("type") for b in monitored_banks if b.get("type"))
    expected_types = {"US_Banks", "Canadian_Asset_Managers", "Monitored_US_Banks"}
    unexpected_types = all_types - expected_types
    if unexpected_types:
        logger.warning(
            f"[CONFIG WARNING] Unexpected bank types found: {unexpected_types}"
        )

    logger.info(
        f"[START] Processing {fiscal_year} {quarter} | "
        f"US Banks: {len(us_banks)}, Canadian AM: {len(canadian_am)}, Monitored US: {len(monitored_us)} | "
        f"Mode: {'latest available' if use_latest else 'exact quarter'}"
    )

    # Concurrency control
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_BANKS)

    # ========== PHASE 1: PAGE 1 - MONITORED US BANKS WM NARRATIVES ==========
    async def process_bank_page1(bank_data):
        async with semaphore:
            try:
                bank_info = await get_bank_info(bank_data["bank_id"])
                transcript = await retrieve_full_transcript(
                    bank_info, fiscal_year, quarter, context, use_latest
                )
                if not transcript:
                    return (bank_info["bank_name"], bank_info["bank_symbol"], {"has_content": False})

                result = await extract_page1_wm_narrative(
                    bank_info, transcript, fiscal_year, quarter, context
                )
                return (bank_info["bank_name"], bank_info["bank_symbol"], result)
            except Exception as e:
                logger.error(f"Error processing Page 1 for {bank_data}: {e}")
                return (bank_data.get("bank_name", "Unknown"), bank_data.get("bank_symbol", ""), {"has_content": False})

    logger.info(f"[PHASE 1] Starting Page 1 extraction for {len(monitored_us)} monitored banks...")
    page1_tasks = [process_bank_page1(bank) for bank in monitored_us]
    page1_results = await asyncio.gather(*page1_tasks, return_exceptions=True)

    all_page1 = {}
    for r in page1_results:
        if not isinstance(r, Exception):
            bank_name, bank_symbol, result = r
            if result.get("has_content"):
                all_page1[bank_name] = {
                    "bank_symbol": bank_symbol,
                    "key_theme": result.get("key_theme", ""),
                    "narrative_summary": result.get("narrative_summary", ""),
                    "supporting_quotes": result.get("supporting_quotes", [])
                }

    logger.info(f"[PHASE 1 COMPLETE] {len(all_page1)} banks with Page 1 content")

    # ========== PHASE 2: PAGE 2 - ALL US BANKS THREE-THEME Q&A ==========
    async def process_bank_page2(bank_data):
        async with semaphore:
            try:
                bank_info = await get_bank_info(bank_data["bank_id"])
                qa_content = await retrieve_qa_section(
                    bank_info, fiscal_year, quarter, context, use_latest
                )
                if not qa_content:
                    return (bank_info["bank_name"], bank_info["bank_symbol"], {"has_content": False, "questions": []})

                result = await extract_page2_three_themes(
                    bank_info, qa_content, page2_themes, fiscal_year, quarter, context
                )
                return (bank_info["bank_name"], bank_info["bank_symbol"], result)
            except Exception as e:
                logger.error(f"Error processing Page 2 for {bank_data}: {e}")
                return (bank_data.get("bank_name", "Unknown"), bank_data.get("bank_symbol", ""), {"has_content": False, "questions": []})

    logger.info(f"[PHASE 2] Starting Page 2 extraction for {len(us_banks)} US banks...")
    page2_tasks = [process_bank_page2(bank) for bank in us_banks]
    page2_results = await asyncio.gather(*page2_tasks, return_exceptions=True)

    all_page2 = {}
    for r in page2_results:
        if not isinstance(r, Exception):
            bank_name, bank_symbol, result = r
            if result.get("has_content"):
                all_page2[bank_name] = {
                    "bank_symbol": bank_symbol,
                    "questions": result.get("questions", [])
                }

    logger.info(f"[PHASE 2 COMPLETE] {len(all_page2)} banks with Page 2 content")

    # ========== PHASE 3: PAGE 3 - CANADIAN AM DUAL ANALYSIS ==========
    async def process_bank_page3(bank_data):
        async with semaphore:
            try:
                bank_info = await get_bank_info(bank_data["bank_id"])
                transcript = await retrieve_full_transcript(
                    bank_info, fiscal_year, quarter, context, use_latest
                )
                if not transcript:
                    return (bank_info["bank_name"], bank_info["bank_symbol"], {"has_content": False})

                result = await extract_page3_canadian_am(
                    bank_info, transcript, fiscal_year, quarter, context, use_latest
                )
                return (bank_info["bank_name"], bank_info["bank_symbol"], result)
            except Exception as e:
                logger.error(f"Error processing Page 3 for {bank_data}: {e}")
                return (bank_data.get("bank_name", "Unknown"), bank_data.get("bank_symbol", ""), {"has_content": False})

    logger.info(f"[PHASE 3] Starting Page 3 extraction for {len(canadian_am)} Canadian AM...")
    page3_tasks = [process_bank_page3(bank) for bank in canadian_am]
    page3_results = await asyncio.gather(*page3_tasks, return_exceptions=True)

    all_page3 = {}
    for r in page3_results:
        if not isinstance(r, Exception):
            bank_name, bank_symbol, result = r
            if result.get("has_content"):
                all_page3[bank_name] = {
                    "bank_symbol": bank_symbol,
                    "aum_netflows": result.get("aum_netflows", {}),
                    "focus_areas": result.get("focus_areas", [])
                }

    logger.info(f"[PHASE 3 COMPLETE] {len(all_page3)} banks with Page 3 content")

    # ========== PHASE 4: PAGE 4 - ALL US BANKS THREE-COLUMN TABLE ==========
    async def process_bank_page4(bank_data):
        async with semaphore:
            try:
                bank_info = await get_bank_info(bank_data["bank_id"])
                transcript = await retrieve_full_transcript(
                    bank_info, fiscal_year, quarter, context, use_latest
                )
                if not transcript:
                    return (bank_info["bank_name"], bank_info["bank_symbol"], {"has_content": False})

                result = await extract_page4_table_data(
                    bank_info, transcript, fiscal_year, quarter, context
                )
                return (bank_info["bank_name"], bank_info["bank_symbol"], result)
            except Exception as e:
                logger.error(f"Error processing Page 4 for {bank_data}: {e}")
                return (bank_data.get("bank_name", "Unknown"), bank_data.get("bank_symbol", ""), {"has_content": False})

    logger.info(f"[PHASE 4] Starting Page 4 extraction for {len(us_banks)} US banks...")
    page4_tasks = [process_bank_page4(bank) for bank in us_banks]
    page4_results = await asyncio.gather(*page4_tasks, return_exceptions=True)

    all_page4 = {}
    for r in page4_results:
        if not isinstance(r, Exception):
            bank_name, bank_symbol, result = r
            if result.get("has_content"):
                all_page4[bank_name] = {
                    "bank_symbol": bank_symbol,
                    "nii_nim_summary": result.get("nii_nim_summary", ""),
                    "credit_pcl_summary": result.get("credit_pcl_summary", ""),
                    "tariff_uncertainty_summary": result.get("tariff_uncertainty_summary", "")
                }

    logger.info(f"[PHASE 4 COMPLETE] {len(all_page4)} banks with Page 4 content")

    # ========== PHASE 5: PAGE 5 - ALL US BANKS SIX-THEME Q&A ==========
    async def process_bank_page5(bank_data):
        async with semaphore:
            try:
                bank_info = await get_bank_info(bank_data["bank_id"])
                qa_content = await retrieve_qa_section(
                    bank_info, fiscal_year, quarter, context, use_latest
                )
                if not qa_content:
                    return (bank_info["bank_name"], bank_info["bank_symbol"], {"has_content": False, "questions": []})

                result = await extract_page5_six_themes(
                    bank_info, qa_content, page5_themes, fiscal_year, quarter, context
                )
                return (bank_info["bank_name"], bank_info["bank_symbol"], result)
            except Exception as e:
                logger.error(f"Error processing Page 5 for {bank_data}: {e}")
                return (bank_data.get("bank_name", "Unknown"), bank_data.get("bank_symbol", ""), {"has_content": False, "questions": []})

    logger.info(f"[PHASE 5] Starting Page 5 extraction for {len(us_banks)} US banks...")
    page5_tasks = [process_bank_page5(bank) for bank in us_banks]
    page5_results = await asyncio.gather(*page5_tasks, return_exceptions=True)

    all_page5 = {}
    for r in page5_results:
        if not isinstance(r, Exception):
            bank_name, bank_symbol, result = r
            if result.get("has_content"):
                all_page5[bank_name] = {
                    "bank_symbol": bank_symbol,
                    "questions": result.get("questions", [])
                }

    logger.info(f"[PHASE 5 COMPLETE] {len(all_page5)} banks with Page 5 content")

    # ========== AGGREGATE FINAL RESULTS ==========
    results = {
        "metadata": {
            "fiscal_year": fiscal_year,
            "quarter": quarter,
            "banks_processed_total": len(us_banks) + len(canadian_am) + len(monitored_us),
            "banks_processed_us": len(us_banks),
            "banks_processed_canadian_am": len(canadian_am),
            "banks_processed_monitored_us": len(monitored_us),
            "banks_with_page1": len(all_page1),
            "banks_with_page2": len(all_page2),
            "banks_with_page3": len(all_page3),
            "banks_with_page4": len(all_page4),
            "banks_with_page5": len(all_page5),
            "generation_date": datetime.now().isoformat(),
            "mode": "latest_available" if use_latest else "exact_quarter"
        },
        "page1_results": all_page1,
        "page2_results": all_page2,
        "page3_results": all_page3,
        "page4_results": all_page4,
        "page5_results": all_page5
    }

    logger.info(
        f"[PIPELINE COMPLETE] All 5 pages processed | "
        f"P1: {len(all_page1)}, P2: {len(all_page2)}, P3: {len(all_page3)}, "
        f"P4: {len(all_page4)}, P5: {len(all_page5)} banks with content"
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
    execution_id: str,
    local_filepath: str = None,
    s3_document_name: str = None,
    s3_pdf_name: str = None
) -> None:
    """
    Save the report to the database.

    Args:
        results: Structured results
        fiscal_year: Year
        quarter: Quarter
        markdown_content: Markdown version of report
        execution_id: Execution UUID
        local_filepath: Path to local DOCX file (optional)
        s3_document_name: S3 document key (optional)
        s3_pdf_name: S3 PDF key (optional)
    """
    async with get_connection() as conn:
        # Save to aegis_reports table
        query = text("""
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
                markdown_content,
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
                :markdown_content,
                NOW(),
                :generated_by,
                :execution_id,
                :metadata
            )
        """)

        await conn.execute(
            query,
            {
                "report_name": "WM Readthrough",
                "report_description": (
                    "AI-generated analysis of wealth management commentary from quarterly earnings calls "
                    "across major banks. Extracts WM narratives, themed Q&A, Canadian AM metrics, "
                    "and banking performance indicators."
                ),
                "report_type": "wm_readthrough",
                "bank_id": None,  # Cross-bank report
                "bank_name": None,
                "bank_symbol": None,
                "fiscal_year": fiscal_year,
                "quarter": quarter,
                "local_filepath": local_filepath,
                "s3_document_name": s3_document_name,
                "s3_pdf_name": s3_pdf_name,
                "markdown_content": markdown_content,
                "generated_by": "wm_readthrough_etl",
                "execution_id": str(execution_id),
                "metadata": json.dumps(results)
            }
        )

        await conn.commit()

    logger.info(f"Report saved to database with execution_id: {execution_id}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

async def main():
    """Main entry point for the WM Readthrough ETL."""
    parser = argparse.ArgumentParser(
        description="Generate WM Readthrough report with all 5 pages"
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Fiscal year (e.g., 2025)"
    )
    parser.add_argument(
        "--quarter",
        type=str,
        required=True,
        help="Quarter (e.g., Q1)"
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
    logger.info(f"Starting WM Readthrough ETL with execution_id: {execution_id}")

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

        # Check if any page has results
        has_any_results = (
            results.get("page1_results") or
            results.get("page2_results") or
            results.get("page3_results") or
            results.get("page4_results") or
            results.get("page5_results")
        )

        if not has_any_results:
            logger.warning("No results generated for any page")
            return

        # Generate document
        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        content_hash = hashlib.md5(
            f"{args.year}_{args.quarter}_{timestamp}".encode()
        ).hexdigest()[:8]

        if args.output:
            docx_path = Path(args.output)
        else:
            docx_path = output_dir / f"WM_Readthrough_{args.year}_{args.quarter}_{content_hash}.docx"

        # Create Word document
        create_combined_document(results, str(docx_path))
        logger.info(f"Word document created: {docx_path}")

        # Convert to PDF if requested
        pdf_path = None
        if not args.no_pdf:
            pdf_path = str(docx_path).replace(".docx", ".pdf")
            if convert_docx_to_pdf(str(docx_path), pdf_path):
                logger.info(f"PDF created: {pdf_path}")

        # Generate markdown for database
        markdown_content = structured_data_to_markdown(results)

        # Extract filenames for S3 placeholders
        docx_filename = docx_path.name
        pdf_filename = Path(pdf_path).name if pdf_path else None

        # Save to database
        await save_to_database(
            results=results,
            fiscal_year=args.year,
            quarter=args.quarter,
            markdown_content=markdown_content,
            execution_id=execution_id,
            local_filepath=str(docx_path),
            s3_document_name=docx_filename,
            s3_pdf_name=pdf_filename
        )

        logger.info(f"WM Readthrough ETL completed successfully")

    except Exception as e:
        logger.error(f"Error in WM Readthrough ETL: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main())
