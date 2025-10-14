"""
CM Readthrough ETL Script - Generates capital markets readthrough reports across multiple banks.

This script processes earnings call transcripts for all monitored institutions to extract
Investment Banking & Trading outlook commentary and categorized analyst questions.

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
import os
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy import text
import yaml
from pathlib import Path

# Import document converter functions
from aegis.etls.cm_readthrough.document_converter import (
    convert_docx_to_pdf,
    structured_data_to_markdown,
    get_standard_report_metadata,
    create_combined_document
)

# Import direct transcript functions
from aegis.model.subagents.transcripts.retrieval import retrieve_full_section
from aegis.model.subagents.transcripts.formatting import format_full_section_chunks
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.llm_connector import complete_with_tools, complete
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from aegis.utils.settings import config
from aegis.etls.cm_readthrough.config.config import (
    MODELS,
    TEMPERATURE,
    MAX_TOKENS,
    get_monitored_institutions,
    get_categories
)

# Initialize logging
setup_logging()
logger = get_logger()

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

async def verify_data_availability(
    bank_id: int,
    fiscal_year: int,
    quarter: str
) -> bool:
    """
    Verify that transcript data is available for the specified period.

    Args:
        bank_id: Bank ID
        fiscal_year: Year (e.g., 2024)
        quarter: Quarter (e.g., "Q3")

    Returns:
        True if data is available
    """
    async with get_connection() as conn:
        query = text("""
            SELECT database_names
            FROM aegis_data_availability
            WHERE bank_id = :bank_id
              AND fiscal_year = :fiscal_year
              AND quarter = :quarter
        """)

        result = await conn.execute(
            query,
            {
                "bank_id": bank_id,
                "fiscal_year": fiscal_year,
                "quarter": quarter
            }
        )

        row = result.first()
        if not row or not row.database_names:
            return False

        # Check if transcripts database is available
        return "transcripts" in row.database_names


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

# =============================================================================
# IB & TRADING EXTRACTION
# =============================================================================

async def extract_ib_trading_outlook(
    bank_info: Dict[str, Any],
    transcript_content: str,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Extract Investment Banking and Trading outlook from transcript.

    Args:
        bank_info: Bank information dictionary
        transcript_content: Full transcript text
        fiscal_year: Year
        quarter: Quarter
        context: Execution context

    Returns:
        Extracted IB and Trading insights
    """
    # Load prompt template
    prompt_template = load_prompt_template("ib_trading_extraction.yaml")

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
                transcript_content=transcript_content  # No truncation - full transcript
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
                "required": ["quotes"]  # Only quotes array is required
            }
        }
    }]

    # Call LLM
    llm_params = {
        "model": MODELS["ib_trading_extraction"],
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

        # Extract tool call results from OpenAI response structure
        tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")
        if tool_calls:
            tool_call = tool_calls[0]
            result = json.loads(tool_call["function"]["arguments"])
            # Return empty dict if bank has no relevant content (will be filtered out later)
            if not result.get("has_content", True):
                logger.info(f"[NO IB CONTENT] {bank_info['bank_name']}: Transcript does not contain substantive IB/Trading outlook")
                return {}
            return result
        else:
            logger.warning(f"No tool calls in response for {bank_info['bank_name']}")
            return {}

    except Exception as e:
        logger.error(f"Error extracting IB/Trading outlook for {bank_info['bank_name']}: {e}")
        return {}


async def format_ib_quote(
    quote_text: str,
    context: Dict[str, Any]
) -> str:
    """
    Format an IB/Trading quote with HTML emphasis tags.

    Args:
        quote_text: The paraphrased quote text
        context: Execution context

    Returns:
        Formatted quote with HTML tags
    """
    # Load prompt template
    prompt_template = load_prompt_template("ib_quote_formatting.yaml")

    # Format messages
    messages = [
        {
            "role": "system",
            "content": prompt_template["system_template"]
        },
        {
            "role": "user",
            "content": prompt_template["user_template"].format(quote_text=quote_text)
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
                "required": ["formatted_quote"]
            }
        }
    }]

    # Call LLM
    llm_params = {
        "model": MODELS["qa_categorization"],  # Use same model as QA (gpt-4-turbo)
        "temperature": 0.3,  # Lower for consistent formatting
        "max_tokens": MAX_TOKENS
    }

    try:
        response = await complete_with_tools(
            messages=messages,
            tools=tools,
            context=context,
            llm_params=llm_params
        )

        # Extract tool call results from OpenAI response structure
        tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")
        if tool_calls:
            tool_call = tool_calls[0]
            result = json.loads(tool_call["function"]["arguments"])
            return result.get("formatted_quote", quote_text)
        else:
            logger.warning("No tool call in formatting response, returning original")
            return quote_text

    except Exception as e:
        logger.error(f"Error formatting quote: {e}")
        return quote_text  # Fallback to original


# =============================================================================
# Q&A CATEGORIZATION
# =============================================================================

async def categorize_qa_block(
    bank_info: Dict[str, Any],
    qa_block: Dict[str, Any],
    previous_qa_block: Optional[Dict[str, Any]],
    categories: List[Dict[str, Any]],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Categorize and extract a single Q&A block.

    Args:
        bank_info: Bank information
        qa_block: Q&A block data with qa_group_id and qa_content
        previous_qa_block: Previous Q&A block for context (or None)
        categories: List of category definitions
        fiscal_year: Year
        quarter: Quarter
        context: Execution context

    Returns:
        Categorized question data
    """
    # Load prompt template
    prompt_template = load_prompt_template("qa_categorization.yaml")

    # Format categories list
    categories_text = "\n".join([
        f"- {cat.get('Category', '')}: {cat.get('Description', '')}"
        for cat in categories
    ])

    # Format previous context if available
    if previous_qa_block:
        previous_context = f"""<previous_qa_block>
{previous_qa_block.get('qa_content', '')}
</previous_qa_block>"""
    else:
        previous_context = "This is the first question in the Q&A session."

    # Format messages - categories and previous context go in system prompt
    messages = [
        {
            "role": "system",
            "content": prompt_template["system_template"].format(
                categories_list=categories_text,
                previous_context=previous_context
            )
        },
        {
            "role": "user",
            "content": prompt_template["user_template"].format(
                bank_name=bank_info["bank_name"],
                fiscal_year=fiscal_year,
                quarter=quarter,
                qa_block_content=qa_block.get("qa_content", "")
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
                "required": ["is_relevant", "category", "verbatim_question"]
            }
        }
    }]

    # Call LLM
    llm_params = {
        "model": MODELS["qa_categorization"],
        "temperature": 0.3,  # Lower temperature for categorization
        "max_tokens": MAX_TOKENS  # Use config value (4096)
    }

    try:
        response = await complete_with_tools(
            messages=messages,
            tools=tools,
            context=context,
            llm_params=llm_params
        )

        # Extract tool call results from OpenAI response structure
        tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")
        if tool_calls:
            tool_call = tool_calls[0]
            result = json.loads(tool_call["function"]["arguments"])

            # Add bank info and qa_group_id to result
            if result.get("is_relevant", False):
                result["bank_name"] = bank_info["bank_name"]
                result["bank_symbol"] = bank_info["bank_symbol"]
                result["qa_group_id"] = qa_block.get("qa_group_id")

            return result
        else:
            return {"is_relevant": False}

    except Exception as e:
        logger.error(f"Error categorizing Q&A for {bank_info['bank_name']}: {e}")
        return {"is_relevant": False}

# =============================================================================
# MAIN PROCESSING
# =============================================================================

async def process_bank(
    bank_info: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    categories: List[Dict[str, Any]],
    context: Dict[str, Any],
    use_latest: bool = False
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Process a single bank's transcript for IB/Trading and Q&A extraction.

    Args:
        bank_info: Bank information
        fiscal_year: Year
        quarter: Quarter
        categories: Category definitions
        context: Execution context
        use_latest: If True, use latest available quarter >= specified quarter

    Returns:
        Tuple of (IB/Trading insights, Categorized Q&As, Period info)
    """
    logger.info(f"Processing {bank_info['bank_name']} for {fiscal_year} {quarter} (latest mode: {use_latest})")

    # Determine which quarter to use
    actual_year = fiscal_year
    actual_quarter = quarter

    if use_latest:
        latest_data = await find_latest_available_quarter(
            bank_id=bank_info["bank_id"],
            min_fiscal_year=fiscal_year,
            min_quarter=quarter,
            bank_name=bank_info["bank_name"]
        )

        if latest_data:
            actual_year, actual_quarter = latest_data
        else:
            logger.warning(
                f"[NO DATA] {bank_info['bank_name']}: No transcript data available "
                f"for {fiscal_year} {quarter} or later"
            )
            return {}, [], {"year": fiscal_year, "quarter": quarter, "data_found": False}
    else:
        # Verify exact quarter availability
        if not await verify_data_availability(bank_info["bank_id"], fiscal_year, quarter):
            logger.warning(
                f"[NO DATA] {bank_info['bank_name']}: No transcript data available "
                f"for exact quarter {fiscal_year} {quarter}"
            )
            return {}, [], {"year": fiscal_year, "quarter": quarter, "data_found": False}

    # Retrieve full transcript
    try:
        # Build combo dict for transcript retrieval
        combo = {
            "bank_id": bank_info["bank_id"],
            "bank_name": bank_info["bank_name"],
            "bank_symbol": bank_info["bank_symbol"],
            "fiscal_year": actual_year,
            "quarter": actual_quarter
        }

        # Get Management Discussion section
        md_chunks = await retrieve_full_section(
            combo=combo,
            sections="MD",
            context=context
        )

        md_content = await format_full_section_chunks(
            chunks=md_chunks,
            combo=combo,
            context=context
        )

        # Log MD section details
        md_block_count = len(md_chunks) if md_chunks else 0
        if md_block_count > 0:
            logger.info(
                f"[TRANSCRIPT MD] {bank_info['bank_name']} {actual_year} {actual_quarter}: "
                f"Found MD section with {md_block_count} blocks, {len(md_content)} characters"
            )
        else:
            logger.warning(
                f"[TRANSCRIPT MD] {bank_info['bank_name']} {actual_year} {actual_quarter}: "
                f"No MD section found"
            )

        # Get Q&A section
        qa_chunks = await retrieve_full_section(
            combo=combo,
            sections="QA",
            context=context
        )

        qa_content = await format_full_section_chunks(
            chunks=qa_chunks,
            combo=combo,
            context=context
        )

        # Log QA section details
        qa_block_count = len(qa_chunks) if qa_chunks else 0
        if qa_block_count > 0:
            logger.info(
                f"[TRANSCRIPT QA] {bank_info['bank_name']} {actual_year} {actual_quarter}: "
                f"Found Q&A section with {qa_block_count} blocks, {len(qa_content)} characters"
            )
        else:
            logger.warning(
                f"[TRANSCRIPT QA] {bank_info['bank_name']} {actual_year} {actual_quarter}: "
                f"No Q&A section found"
            )

        # Log combined transcript
        logger.info(
            f"[TRANSCRIPT COMBINED] {bank_info['bank_name']} {actual_year} {actual_quarter}: "
            f"Combined transcript created with {len(md_content) + len(qa_content)} total characters"
        )

    except Exception as e:
        logger.error(f"Error retrieving transcript for {bank_info['bank_name']}: {e}")
        return {}, [], {"year": actual_year, "quarter": actual_quarter, "data_found": False}

    # Extract IB & Trading outlook
    full_transcript = f"{md_content}\n\n{qa_content}"
    ib_trading_insights = await extract_ib_trading_outlook(
        bank_info=bank_info,
        transcript_content=full_transcript,
        fiscal_year=actual_year,
        quarter=actual_quarter,
        context=context
    )

    # Format quotes with HTML if extraction was successful
    if ib_trading_insights and ib_trading_insights.get("quotes"):
        logger.info(f"[IB FORMATTING] {bank_info['bank_name']}: Formatting {len(ib_trading_insights['quotes'])} quotes")
        for quote_obj in ib_trading_insights['quotes']:
            original_quote = quote_obj.get("quote", "")
            if original_quote:
                formatted_quote = await format_ib_quote(original_quote, context)
                quote_obj["formatted_quote"] = formatted_quote
                quote_obj["original_quote"] = original_quote  # Keep original for reference

    # Process Q&A questions using qa_group_id blocks
    categorized_qas = []

    # Retrieve Q&A chunks and group by qa_group_id (same approach as key_themes ETL)
    try:
        async with get_connection() as conn:
            query = text("""
                SELECT
                    qa_group_id,
                    chunk_content as content
                FROM aegis_transcripts
                WHERE (institution_id = :bank_id_str OR institution_id::text = :bank_id_str)
                AND fiscal_year = :fiscal_year
                AND fiscal_quarter = :quarter
                AND section_name = 'Q&A'
                ORDER BY qa_group_id, chunk_id
            """)

            result = await conn.execute(query, {
                "bank_id_str": str(bank_info['bank_id']),
                "fiscal_year": actual_year,
                "quarter": actual_quarter
            })

            rows = result.fetchall()
            chunks = [{"qa_group_id": row[0], "content": row[1]} for row in rows]
    except Exception as e:
        logger.error(f"Error retrieving Q&A chunks for {bank_info['bank_name']}: {e}")
        chunks = []

    # Group chunks by qa_group_id
    qa_groups = {}
    for chunk in chunks:
        qa_group_id = chunk.get('qa_group_id')
        if qa_group_id is not None:
            if qa_group_id not in qa_groups:
                qa_groups[qa_group_id] = []
            qa_groups[qa_group_id].append(chunk)

    # Convert to list of Q&A blocks with combined content
    qa_blocks = []
    for qa_group_id in sorted(qa_groups.keys()):
        group_chunks = qa_groups[qa_group_id]
        # Combine all chunks for this qa_group_id
        qa_content = "\n".join([
            chunk.get('content', '')
            for chunk in group_chunks
            if chunk.get('content')
        ])

        if qa_content:
            qa_blocks.append({
                "qa_group_id": qa_group_id,
                "qa_content": qa_content
            })

    logger.info(
        f"[QA BLOCKS] {bank_info['bank_name']}: Retrieved {len(qa_blocks)} Q&A blocks"
    )

    # Process each Q&A block with previous context
    for i, qa_block in enumerate(qa_blocks):
        # Get previous block for context if available
        previous_block = qa_blocks[i-1] if i > 0 else None

        result = await categorize_qa_block(
            bank_info=bank_info,
            qa_block=qa_block,
            previous_qa_block=previous_block,
            categories=categories,
            fiscal_year=actual_year,
            quarter=actual_quarter,
            context=context
        )

        if result.get("is_relevant", False):
            categorized_qas.append(result)

    logger.info(
        f"[PROCESSING COMPLETE] {bank_info['bank_name']} {actual_year} {actual_quarter}: "
        f"{len(ib_trading_insights.get('investment_banking', []))} IB insights, "
        f"{len(ib_trading_insights.get('trading_outlook', []))} trading insights, "
        f"{len(categorized_qas)} relevant Q&As from {len(qa_blocks)} total"
    )

    period_info = {
        "year": actual_year,
        "quarter": actual_quarter,
        "data_found": True,
        "requested_year": fiscal_year,
        "requested_quarter": quarter,
        "used_latest": use_latest and (actual_year != fiscal_year or actual_quarter != quarter)
    }

    return ib_trading_insights, categorized_qas, period_info

async def process_all_banks(
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    use_latest: bool = False
) -> Dict[str, Any]:
    """
    Process all monitored banks for the specified period.

    Args:
        fiscal_year: Year
        quarter: Quarter
        context: Execution context
        use_latest: If True, use latest available quarter >= specified quarter

    Returns:
        Combined results from all banks
    """
    # Load monitored institutions
    try:
        monitored_banks = get_monitored_institutions()
    except Exception as e:
        logger.error(f"Error loading monitored institutions: {e}")
        return {}

    # Load categories
    try:
        categories = get_categories()
    except Exception as e:
        logger.error(f"Error loading categories: {e}")
        return {}

    logger.info(
        f"Processing {len(monitored_banks)} monitored institutions for {fiscal_year} {quarter} "
        f"(mode: {'latest available' if use_latest else 'exact quarter'})"
    )

    # Process each bank
    all_ib_trading = {}
    all_qas = []
    period_tracking = {}

    for bank_data in monitored_banks:
        try:
            bank_info = await get_bank_info(bank_data.get("bank_id") or bank_data.get("bank_symbol"))

            ib_trading, qas, period_info = await process_bank(
                bank_info=bank_info,
                fiscal_year=fiscal_year,
                quarter=quarter,
                categories=categories,
                context=context,
                use_latest=use_latest
            )

            # Track period used for each bank
            period_tracking[bank_info["bank_name"]] = period_info

            # Store results with bank_symbol for ticker mapping
            if ib_trading:
                ib_trading["bank_symbol"] = bank_info.get("bank_symbol", "")
                all_ib_trading[bank_info["bank_name"]] = ib_trading

            all_qas.extend(qas)

        except Exception as e:
            logger.error(f"Error processing bank {bank_data}: {e}")
            continue

    # Log summary of periods used
    if use_latest:
        banks_with_latest = [
            f"{bank}: {info['year']} {info['quarter']}"
            for bank, info in period_tracking.items()
            if info.get('used_latest', False)
        ]
        if banks_with_latest:
            logger.info(
                f"[LATEST QUARTER SUMMARY] Banks using more recent data than requested:\n"
                + "\n".join(banks_with_latest)
            )

    banks_without_data = [
        bank for bank, info in period_tracking.items()
        if not info.get('data_found', True)
    ]
    if banks_without_data:
        logger.warning(
            f"[NO DATA SUMMARY] Banks with no available data:\n"
            + "\n".join(banks_without_data)
        )

    # Organize results
    results = {
        "metadata": {
            "fiscal_year": fiscal_year,
            "quarter": quarter,
            "banks_processed": len(all_ib_trading),
            "total_qas": len(all_qas),
            "generation_date": datetime.now().isoformat(),
            "mode": "latest_available" if use_latest else "exact_quarter",
            "period_tracking": period_tracking
        },
        "ib_trading_outlook": all_ib_trading,
        "categorized_qas": organize_qas_by_category(all_qas)
    }

    return results

def organize_qas_by_category(qas: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Organize Q&As by category.

    Args:
        qas: List of categorized Q&As

    Returns:
        Dictionary organized by category
    """
    organized = {}

    for qa in qas:
        category = qa.get("category", "Other")
        if category not in organized:
            organized[category] = []
        organized[category].append(qa)

    return organized

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
        results = await process_all_banks(
            fiscal_year=args.year,
            quarter=args.quarter,
            context=context,
            use_latest=args.use_latest
        )

        if not results or not results.get("ib_trading_outlook"):
            logger.warning("No results generated")
            return

        # Generate document
        output_dir = Path(__file__).parent / "output"
        output_dir.mkdir(exist_ok=True)

        # Create filename with timestamp for uniqueness (avoid sort_keys issue with None values)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        content_hash = hashlib.md5(
            f"{args.year}_{args.quarter}_{timestamp}".encode()
        ).hexdigest()[:8]

        if args.output:
            docx_path = Path(args.output)
        else:
            docx_path = output_dir / f"CM_Readthrough_{args.year}_{args.quarter}_{content_hash}.docx"

        # Create Word document (implement in document_converter.py)
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