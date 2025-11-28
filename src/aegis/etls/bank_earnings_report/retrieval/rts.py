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


# =============================================================================
# RTS Document Loading
# =============================================================================


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

        # Add section header if changed
        if section and section != current_section:
            lines.append(f"\n## {section}")
            lines.append(f"[Page {page}]")
            lines.append("")
            current_section = section

        if raw_text:
            lines.append(raw_text)
            lines.append("")

    return "\n".join(lines)


# =============================================================================
# Batch Segment Driver Extraction
# =============================================================================


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

    # Initialize result with empty strings for all segments
    result: Dict[str, str] = {name: "" for name in segment_names}

    if not chunks:
        logger.warning("etl.rts.no_chunks_for_batch_drivers", execution_id=execution_id)
        return result

    if not segment_names:
        logger.warning("etl.rts.no_segments_provided", execution_id=execution_id)
        return result

    full_rts = format_full_rts_for_llm(chunks)

    # Build segment list for prompt
    segment_list = "\n".join(f"- {name}" for name in segment_names)

    system_prompt = f"""You are a senior financial analyst writing a bank quarterly earnings report.

Your task is to extract performance driver statements for EACH of the following business segments:

{segment_list}

For EACH segment, you will:
1. FIND the section(s) in the regulatory filing that discuss that segment
2. EXTRACT the key performance drivers mentioned
3. WRITE a concise qualitative drivers statement (2-3 sentences)

## CRITICAL REQUIREMENTS

1. **NO METRICS OR NUMBERS**: Do NOT include specific dollar amounts, percentages, basis points, \
or any numerical values. The metrics are shown separately in the report.
2. **QUALITATIVE ONLY**: Focus on the business drivers, trends, and factors - not the numbers.
3. **Length**: 2-3 sentences maximum per segment
4. **Tone**: Professional, factual, analyst-style
5. **Consistency**: Use similar style and depth across all segments

## WHERE TO FIND SEGMENT INFORMATION

Look for sections with headings like:
- The segment name itself (e.g., "Canadian Banking", "Capital Markets")
- "Business Segment Results"
- "Segment Performance"
- "Operating Results by Segment"
- "Results by Business Segment"

Each segment's discussion typically includes explanations of what drove performance changes.

## WHAT TO INCLUDE IN EACH STATEMENT

- Business drivers (e.g., "higher trading activity", "increased client demand")
- Market conditions (e.g., "favorable rate environment", "challenging credit conditions")
- Strategic factors (e.g., "expansion into new markets", "cost discipline initiatives")
- Operational factors (e.g., "improved efficiency", "technology investments")

## WHAT TO EXCLUDE

- Specific dollar amounts (e.g., "$2.1B", "CAD 500 million")
- Percentages (e.g., "8% growth", "up 12%")
- Basis points (e.g., "expanded 15 bps")
- Quarter-over-quarter or year-over-year comparisons with numbers
- The segment name in the statement (it's already shown in the header)

## IF A SEGMENT IS NOT FOUND

If you cannot find content specifically about a segment, return an empty string for that segment.
Do NOT make up information or use content from other segments."""

    user_prompt = f"""Below is the complete regulatory filing document. For each of the \
following segments, find the relevant section and write a 2-3 sentence QUALITATIVE \
drivers statement:

{segment_list}

Remember: NO specific metrics, percentages, or dollar amounts. Focus only on the business drivers.

{full_rts}

Extract the qualitative drivers statement for each segment listed above."""

    # Build properties for each segment dynamically
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

    tool_definition = {
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
        model = etl_config.get_model("segment_drivers_extraction")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
            context=context,
            llm_params={"model": model, "temperature": 0.2, "max_tokens": 2000},
        )

        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])

                # Map back from safe keys to original segment names
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


# =============================================================================
# Main Entry Point
# =============================================================================


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

    # Step 1: Load all chunks (single DB call)
    all_chunks = await retrieve_all_rts_chunks(
        bank=bank,
        year=year,
        quarter=quarter,
        context=context,
    )

    if not all_chunks:
        logger.warning("etl.rts.no_chunks_loaded_batch", execution_id=execution_id)
        return {name: "" for name in segment_names}

    # Step 2: Generate all drivers in one LLM call
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
