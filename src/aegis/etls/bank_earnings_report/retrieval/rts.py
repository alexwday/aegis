"""
RTS (Regulatory/Risk/Technical Supplement) retrieval for Bank Earnings Report ETL.

This module loads the full RTS document and uses a single LLM call to extract
qualitative performance drivers for all business segments at once.

The rts_embedding table contains chunks from bank regulatory filings with:
- Raw text content from the filing
- Source sections: Hierarchical section paths from markdown headings
- Page numbers for reference

Pipeline:
1. Load all RTS chunks for the bank/quarter (single DB call)
2. Format into a single document
3. Single LLM call extracts drivers for all segments simultaneously
"""

import json
from typing import Any, Dict, List

from sqlalchemy import bindparam, text

from aegis.connections.llm_connector import complete_with_tools
from aegis.connections.postgres_connector import get_connection
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.utils.logging import get_logger
from aegis.utils.prompt_loader import load_prompt_from_db


async def retrieve_all_rts_chunks(
    bank: str,
    year: int,
    quarter: str,
    context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Retrieve ALL chunks from RTS for a given bank/quarter.

    This loads the entire RTS document without any filtering, allowing
    the LLM to find relevant sections directly.

    Args:
        bank: Bank symbol with suffix (e.g., "RY-CA")
        year: Fiscal year
        quarter: Quarter (e.g., "Q3")
        context: Execution context

    Returns:
        List of all chunk dicts ordered by chunk_id
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.rts.load_all_chunks_start",
        execution_id=execution_id,
        bank=bank,
        period=f"{quarter} {year}",
    )

    try:
        async with get_connection() as conn:
            sql = text(
                """
                SELECT
                    id, chunk_id, page_no, summary_title, source_section,
                    raw_text, propositions
                FROM rts_embedding
                WHERE bank = :bank AND year = :year AND quarter = :quarter
                ORDER BY chunk_id
                """
            ).bindparams(
                bindparam("bank", value=bank),
                bindparam("year", value=year),
                bindparam("quarter", value=quarter),
            )

            result = await conn.execute(sql)
            chunks = []

            for row in result.fetchall():
                propositions = row[6]
                if isinstance(propositions, str):
                    try:
                        propositions = json.loads(propositions)
                    except json.JSONDecodeError:
                        propositions = []

                chunks.append(
                    {
                        "id": row[0],
                        "chunk_id": row[1],
                        "page_no": row[2],
                        "summary_title": row[3],
                        "source_section": row[4],
                        "raw_text": row[5],
                        "propositions": propositions or [],
                    }
                )

            logger.info(
                "etl.rts.load_all_chunks_complete",
                execution_id=execution_id,
                bank=bank,
                period=f"{quarter} {year}",
                total_chunks=len(chunks),
            )
            return chunks

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("etl.rts.load_all_chunks_error", error=str(e))
        return []


def format_full_rts_for_llm(chunks: List[Dict[str, Any]]) -> str:
    """
    Format all RTS chunks into a single document for LLM processing.

    Args:
        chunks: List of all chunks sorted by chunk_id

    Returns:
        Formatted full RTS content string
    """
    if not chunks:
        return "No RTS content available."

    lines = ["# Full Regulatory Filing Document", ""]

    current_section = None
    for chunk in chunks:
        section = chunk.get("source_section", "")
        page = chunk.get("page_no", "?")
        raw_text = chunk.get("raw_text", "")

        if section and section != current_section:
            lines.append(f"\n## {section}")
            lines.append(f"[Page {page}]")
            lines.append("")
            current_section = section

        if raw_text:
            lines.append(raw_text)
            lines.append("")

    return "\n".join(lines)


async def generate_all_segment_drivers_from_full_rts(
    chunks: List[Dict[str, Any]],
    segment_names: List[str],
    context: Dict[str, Any],
) -> Dict[str, str]:
    """
    Generate qualitative drivers statements for ALL segments in a single LLM call.

    This is more efficient than calling the LLM once per segment, and provides
    better consistency across segment summaries.

    Args:
        chunks: All RTS chunks for the bank/quarter
        segment_names: List of segment names to extract drivers for
        context: Execution context

    Returns:
        Dict mapping segment name to drivers statement (empty string if not found)
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    result: Dict[str, str] = {name: "" for name in segment_names}

    if not chunks:
        logger.warning("etl.rts.no_chunks_for_batch_drivers", execution_id=execution_id)
        return result

    if not segment_names:
        logger.warning("etl.rts.no_segments_provided", execution_id=execution_id)
        return result

    full_rts = format_full_rts_for_llm(chunks)

    segment_list = "\n".join(f"- {name}" for name in segment_names)

    # Load prompt from database
    prompt_data = load_prompt_from_db(
        layer="bank_earnings_report_etl",
        name="rts_4_segments_drivers",
        compose_with_globals=False,
        execution_id=execution_id,
    )

    # Format prompts with dynamic content
    system_prompt = prompt_data["system_prompt"].format(segment_list=segment_list)
    user_prompt = prompt_data["user_prompt"].format(
        segment_list=segment_list,
        full_rts=full_rts,
    )

    # Build tool definition with dynamic properties for each segment
    segment_properties = {}
    for name in segment_names:
        safe_key = name.lower().replace(" ", "_").replace("&", "and").replace(".", "")
        segment_properties[safe_key] = {
            "type": "object",
            "properties": {
                "found": {
                    "type": "boolean",
                    "description": f"Whether content for {name} was found in the document",
                },
                "drivers_statement": {
                    "type": "string",
                    "description": (
                        f"2-3 sentence qualitative drivers statement for {name}. "
                        "No numbers, percentages, or dollar amounts. "
                        "Empty string if segment not found."
                    ),
                },
            },
            "required": ["found", "drivers_statement"],
        }

    tool_def = {
        "type": "function",
        "function": {
            "name": "all_segment_drivers",
            "description": "Extract qualitative drivers statements for all business segments",
            "parameters": {
                "type": "object",
                "properties": segment_properties,
                "required": list(segment_properties.keys()),
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("rts_4_segments_drivers")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_def],
            context=context,
            llm_params={
                "model": model,
                "temperature": etl_config.temperature,
                "max_tokens": etl_config.max_tokens,
            },
        )

        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])

                for name in segment_names:
                    safe_key = name.lower().replace(" ", "_").replace("&", "and").replace(".", "")
                    segment_data = function_args.get(safe_key, {})

                    if segment_data.get("found") and segment_data.get("drivers_statement"):
                        result[name] = segment_data["drivers_statement"]
                        logger.info(
                            "etl.rts.batch_driver_extracted",
                            execution_id=execution_id,
                            segment=name,
                            statement_length=len(segment_data["drivers_statement"]),
                        )
                    else:
                        logger.info(
                            "etl.rts.batch_driver_not_found",
                            execution_id=execution_id,
                            segment=name,
                        )

                logger.info(
                    "etl.rts.batch_drivers_complete",
                    execution_id=execution_id,
                    segments_requested=len(segment_names),
                    segments_found=sum(1 for v in result.values() if v),
                )
                return result

        logger.warning("etl.rts.batch_drivers_no_tool_call", execution_id=execution_id)
        return result

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("etl.rts.batch_drivers_error", error=str(e))
        return result


async def get_all_segment_drivers_from_rts(
    bank: str,
    year: int,
    quarter: str,
    segment_names: List[str],
    context: Dict[str, Any],
) -> Dict[str, str]:
    """
    Get qualitative drivers statements for ALL segments in a single LLM call.

    This is more efficient than calling once per segment:
    - Single RTS load from database
    - Single LLM call for all segments
    - Better consistency in tone/style across segments

    Args:
        bank: Bank symbol (e.g., "RY-CA")
        year: Fiscal year
        quarter: Quarter (e.g., "Q3")
        segment_names: List of segment names to extract drivers for
        context: Execution context

    Returns:
        Dict mapping segment name to drivers statement (empty string if not found)
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.rts.batch_pipeline_start",
        execution_id=execution_id,
        bank=bank,
        period=f"{quarter} {year}",
        segments=segment_names,
    )

    all_chunks = await retrieve_all_rts_chunks(
        bank=bank,
        year=year,
        quarter=quarter,
        context=context,
    )

    if not all_chunks:
        logger.warning("etl.rts.no_chunks_loaded_batch", execution_id=execution_id)
        return {name: "" for name in segment_names}

    drivers = await generate_all_segment_drivers_from_full_rts(
        chunks=all_chunks,
        segment_names=segment_names,
        context=context,
    )

    logger.info(
        "etl.rts.batch_pipeline_complete",
        execution_id=execution_id,
        total_chunks=len(all_chunks),
        segments_with_drivers=sum(1 for v in drivers.values() if v),
    )

    return drivers


async def extract_rts_items_of_note(
    bank_symbol: str,
    bank_name: str,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    max_items: int = 8,
) -> Dict[str, Any]:
    """
    Extract key defining items from RTS regulatory filings.

    Items of Note are the events and developments that MOST SIGNIFICANTLY DEFINED
    this quarter for the bank - not just what's mentioned, but what matters most
    to understanding the bank's quarter.

    Each item is scored by significance (1-10) to enable ranking.

    Args:
        bank_symbol: Bank symbol (e.g., "RY")
        bank_name: Full bank name
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context
        max_items: Maximum items to extract (default 8)

    Returns:
        Dict with:
            - source: "RTS"
            - items: List of item dicts with description, impact, segment, timing, score
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.rts.items_of_note_start",
        execution_id=execution_id,
        bank=bank_symbol,
        period=f"{quarter} {fiscal_year}",
    )

    db_symbol = f"{bank_symbol}-CA"
    chunks = await retrieve_all_rts_chunks(db_symbol, fiscal_year, quarter, context)

    if not chunks:
        logger.warning(
            "etl.rts.items_of_note_no_chunks",
            execution_id=execution_id,
        )
        return {"source": "RTS", "items": []}

    full_rts = format_full_rts_for_llm(chunks)

    if not full_rts.strip() or full_rts == "No RTS content available.":
        return {"source": "RTS", "items": []}

    # Load prompt from database
    prompt_data = load_prompt_from_db(
        layer="bank_earnings_report_etl",
        name="rts_1_keymetrics_items",
        compose_with_globals=False,
        execution_id=execution_id,
    )

    # Format prompts with dynamic content
    system_prompt = prompt_data["system_prompt"].format(
        bank_name=bank_name,
        quarter=quarter,
        fiscal_year=fiscal_year,
    )
    user_prompt = prompt_data["user_prompt"].format(
        bank_name=bank_name,
        quarter=quarter,
        fiscal_year=fiscal_year,
        full_rts=full_rts,
    )

    # Build tool definition with dynamic constraints
    tool_def = prompt_data["tool_definition"]
    tool_def["function"]["parameters"]["properties"]["items"]["maxItems"] = max_items

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("rts_1_keymetrics_items")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_def],
            context=context,
            llm_params={
                "model": model,
                "temperature": etl_config.temperature,
                "max_tokens": etl_config.max_tokens,
            },
        )

        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])
                items = function_args.get("items", [])
                notes = function_args.get("extraction_notes", "")

                logger.info(
                    "etl.rts.items_of_note_complete",
                    execution_id=execution_id,
                    items_count=len(items),
                    extraction_notes=notes,
                )

                return {"source": "RTS", "items": items, "notes": notes}

        logger.warning(
            "etl.rts.items_of_note_no_result",
            execution_id=execution_id,
        )
        return {"source": "RTS", "items": []}

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "etl.rts.items_of_note_error",
            execution_id=execution_id,
            error=str(e),
        )
        return {"source": "RTS", "items": []}


async def extract_rts_overview(
    bank_symbol: str,
    bank_name: str,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extract a high-level overview summary from RTS regulatory filings.

    Creates a single paragraph (3-5 sentences) that captures:
    - Quarter's key financial performance themes
    - Strategic developments and priorities
    - Capital and risk positioning
    - Forward-looking perspective from regulatory disclosures

    This will be combined with transcript overview for the final summary.

    Args:
        bank_symbol: Bank symbol (e.g., "RY")
        bank_name: Full bank name
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context

    Returns:
        Dict with:
            - source: "RTS"
            - narrative: Overview paragraph string
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.rts.overview_start",
        execution_id=execution_id,
        bank=bank_symbol,
        period=f"{quarter} {fiscal_year}",
    )

    db_symbol = f"{bank_symbol}-CA"
    chunks = await retrieve_all_rts_chunks(db_symbol, fiscal_year, quarter, context)

    if not chunks:
        logger.warning(
            "etl.rts.overview_no_chunks",
            execution_id=execution_id,
        )
        return {"source": "RTS", "narrative": ""}

    full_rts = format_full_rts_for_llm(chunks)

    if not full_rts.strip() or full_rts == "No RTS content available.":
        return {"source": "RTS", "narrative": ""}

    # Load prompt from database
    prompt_data = load_prompt_from_db(
        layer="bank_earnings_report_etl",
        name="rts_1_keymetrics_overview",
        compose_with_globals=False,
        execution_id=execution_id,
    )

    # Format user prompt with dynamic content
    user_prompt = prompt_data["user_prompt"].format(
        bank_name=bank_name,
        quarter=quarter,
        fiscal_year=fiscal_year,
        full_rts=full_rts,
    )

    messages = [
        {"role": "system", "content": prompt_data["system_prompt"]},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("rts_1_keymetrics_overview")

        response = await complete_with_tools(
            messages=messages,
            tools=[prompt_data["tool_definition"]],
            context=context,
            llm_params={
                "model": model,
                "temperature": etl_config.temperature,
                "max_tokens": etl_config.max_tokens,
            },
        )

        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])
                overview = function_args.get("overview", "")

                logger.info(
                    "etl.rts.overview_complete",
                    execution_id=execution_id,
                    overview_length=len(overview),
                )

                return {"source": "RTS", "narrative": overview}

        logger.warning(
            "etl.rts.overview_no_result",
            execution_id=execution_id,
        )
        return {"source": "RTS", "narrative": ""}

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "etl.rts.overview_error",
            execution_id=execution_id,
            error=str(e),
        )
        return {"source": "RTS", "narrative": ""}


async def extract_rts_narrative_paragraphs(
    bank_symbol: str,
    bank_name: str,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extract 4 structured narrative paragraphs from RTS regulatory filings.

    Creates 4 paragraphs covering different aspects of the quarter:
    1. Financial Performance - Overall earnings narrative and key drivers
    2. Business Segments - Highlights from major business lines
    3. Risk & Capital - Credit quality, capital position, risk management
    4. Strategic Outlook - Forward-looking themes and priorities

    These paragraphs will be interleaved with transcript quotes
    in the Management Narrative section.

    Args:
        bank_symbol: Bank symbol (e.g., "RY")
        bank_name: Full bank name
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context

    Returns:
        Dict with:
            - paragraphs: List of 4 paragraph dicts, each with theme and content
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.rts.narrative_paragraphs_start",
        execution_id=execution_id,
        bank=bank_symbol,
        period=f"{quarter} {fiscal_year}",
    )

    db_symbol = f"{bank_symbol}-CA"
    chunks = await retrieve_all_rts_chunks(db_symbol, fiscal_year, quarter, context)

    if not chunks:
        logger.warning(
            "etl.rts.narrative_paragraphs_no_chunks",
            execution_id=execution_id,
        )
        return {"paragraphs": []}

    full_rts = format_full_rts_for_llm(chunks)

    if not full_rts.strip() or full_rts == "No RTS content available.":
        return {"paragraphs": []}

    # Load prompt from database
    prompt_data = load_prompt_from_db(
        layer="bank_earnings_report_etl",
        name="rts_2_narrative_paragraphs",
        compose_with_globals=False,
        execution_id=execution_id,
    )

    # Format prompts with dynamic content
    system_prompt = prompt_data["system_prompt"].format(bank_name=bank_name)
    user_prompt = prompt_data["user_prompt"].format(
        bank_name=bank_name,
        quarter=quarter,
        fiscal_year=fiscal_year,
        full_rts=full_rts,
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("rts_2_narrative_paragraphs")

        response = await complete_with_tools(
            messages=messages,
            tools=[prompt_data["tool_definition"]],
            context=context,
            llm_params={
                "model": model,
                "temperature": etl_config.temperature,
                "max_tokens": etl_config.max_tokens,
            },
        )

        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])

                paragraphs = [
                    {
                        "theme": "Financial Performance",
                        "content": function_args.get("financial_performance", ""),
                    },
                    {
                        "theme": "Business Segments",
                        "content": function_args.get("business_segments", ""),
                    },
                    {
                        "theme": "Risk & Capital",
                        "content": function_args.get("risk_capital", ""),
                    },
                    {
                        "theme": "Strategic Outlook",
                        "content": function_args.get("strategic_outlook", ""),
                    },
                ]

                # Filter out empty paragraphs
                paragraphs = [p for p in paragraphs if p["content"].strip()]

                logger.info(
                    "etl.rts.narrative_paragraphs_complete",
                    execution_id=execution_id,
                    paragraph_count=len(paragraphs),
                    themes=[p["theme"] for p in paragraphs],
                )

                return {"paragraphs": paragraphs}

        logger.warning(
            "etl.rts.narrative_paragraphs_no_result",
            execution_id=execution_id,
        )
        return {"paragraphs": []}

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "etl.rts.narrative_paragraphs_error",
            execution_id=execution_id,
            error=str(e),
        )
        return {"paragraphs": []}
