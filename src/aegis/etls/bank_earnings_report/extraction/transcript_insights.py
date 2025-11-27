"""
LLM-based extraction of insights from transcript Management Discussion section.

Extracts two types of content:
1. Overview Summary - High-level paragraph capturing quarter themes and sentiment
2. Items of Note - List of notable highlights, developments, and concerns

These transcript-based extractions will later be combined with RTS-based extractions
to create the final overview and items of note sections.
"""

import json
from typing import Any, Dict

from aegis.connections.llm_connector import complete_with_tools
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.etls.bank_earnings_report.retrieval.transcripts import (
    format_md_section_for_llm,
    retrieve_md_chunks,
)
from aegis.utils.logging import get_logger


async def extract_transcript_overview(
    bank_info: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extract a high-level overview summary from the Management Discussion section.

    Creates a single paragraph (3-5 sentences) that captures:
    - Quarter's key themes and tone
    - Strategic direction and priorities
    - Overall sentiment from management
    - Forward-looking perspective

    This will later be combined with RTS overview for the final summary.

    Args:
        bank_info: Bank information dict with bank_id, bank_name, bank_symbol
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context

    Returns:
        Dict with:
            - source: "Transcript"
            - narrative: Overview paragraph string
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.bank_earnings_report.transcript_overview_start",
        execution_id=execution_id,
        bank=bank_info["bank_symbol"],
        period=f"{quarter} {fiscal_year}",
    )

    # Retrieve MD section chunks
    chunks = await retrieve_md_chunks(
        bank_id=bank_info["bank_id"],
        fiscal_year=fiscal_year,
        quarter=quarter,
        context=context,
    )

    if not chunks:
        logger.warning(
            "etl.bank_earnings_report.transcript_overview_no_chunks",
            execution_id=execution_id,
        )
        return {"source": "Transcript", "narrative": ""}

    # Format MD section for LLM
    md_content = format_md_section_for_llm(
        chunks=chunks,
        bank_name=bank_info["bank_name"],
        quarter=quarter,
        fiscal_year=fiscal_year,
    )

    if not md_content.strip():
        return {"source": "Transcript", "narrative": ""}

    # Build prompts
    system_prompt = """You are a senior financial analyst creating an executive summary from \
bank earnings call transcripts.

## YOUR TASK

Write a single paragraph (3-5 sentences, 60-100 words) that captures the key themes and tone \
from management's prepared remarks. This overview sets the stage for a quarterly earnings report.

## WHAT TO INCLUDE

- Overall quarter sentiment (confident, cautious, optimistic, etc.)
- Key strategic themes management emphasized
- Forward-looking direction or priorities
- General business momentum or challenges

## WHAT TO AVOID

- Specific metrics or numbers (those are in other sections)
- Direct quotes (those are in the Management Narrative section)
- Detailed segment breakdowns
- Generic boilerplate language

## STYLE

- Executive summary tone - concise and insightful
- Third person perspective ("Management expressed...", "The bank continues...")
- Focus on qualitative themes, not quantitative results
- Should feel like the opening paragraph of an analyst report"""

    user_prompt = f"""Write a brief overview paragraph summarizing the key themes from \
{bank_info['bank_name']}'s {quarter} {fiscal_year} earnings call management discussion.

{md_content}

Provide a 3-5 sentence overview that captures the quarter's tone and strategic themes."""

    # Tool definition
    tool_definition = {
        "type": "function",
        "function": {
            "name": "create_overview_summary",
            "description": "Create a high-level overview paragraph from management remarks",
            "parameters": {
                "type": "object",
                "properties": {
                    "overview": {
                        "type": "string",
                        "description": (
                            "Overview paragraph (3-5 sentences, 60-100 words). "
                            "Captures key themes, tone, and strategic direction. "
                            "No specific metrics or quotes."
                        ),
                    },
                },
                "required": ["overview"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("transcript_overview_extraction")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
            context=context,
            llm_params={
                "model": model,
                "temperature": 0.3,
                "max_tokens": 1000,
            },
        )

        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])
                overview = function_args.get("overview", "")

                logger.info(
                    "etl.bank_earnings_report.transcript_overview_complete",
                    execution_id=execution_id,
                    overview_length=len(overview),
                )

                return {"source": "Transcript", "narrative": overview}

        logger.warning(
            "etl.bank_earnings_report.transcript_overview_no_result",
            execution_id=execution_id,
        )
        return {"source": "Transcript", "narrative": ""}

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.transcript_overview_error",
            execution_id=execution_id,
            error=str(e),
        )
        return {"source": "Transcript", "narrative": ""}


async def extract_transcript_items_of_note(
    bank_info: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    max_items: int = 10,
) -> Dict[str, Any]:
    """
    Extract notable items/highlights from the Management Discussion section.

    Identifies 8-12 specific, actionable items that management highlighted:
    - Key achievements or milestones
    - Notable developments or changes
    - Concerns or challenges acknowledged
    - Strategic initiatives mentioned
    - Forward guidance themes

    These will later be combined with RTS items and deduplicated.

    Args:
        bank_info: Bank information dict with bank_id, bank_name, bank_symbol
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context
        max_items: Maximum items to extract (default 10)

    Returns:
        Dict with:
            - source: "Transcript"
            - items: List of notable item strings
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.bank_earnings_report.transcript_items_start",
        execution_id=execution_id,
        bank=bank_info["bank_symbol"],
        period=f"{quarter} {fiscal_year}",
    )

    # Retrieve MD section chunks
    chunks = await retrieve_md_chunks(
        bank_id=bank_info["bank_id"],
        fiscal_year=fiscal_year,
        quarter=quarter,
        context=context,
    )

    if not chunks:
        logger.warning(
            "etl.bank_earnings_report.transcript_items_no_chunks",
            execution_id=execution_id,
        )
        return {"source": "Transcript", "items": []}

    # Format MD section for LLM
    md_content = format_md_section_for_llm(
        chunks=chunks,
        bank_name=bank_info["bank_name"],
        quarter=quarter,
        fiscal_year=fiscal_year,
    )

    if not md_content.strip():
        return {"source": "Transcript", "items": []}

    # Build prompts
    system_prompt = f"""You are a senior financial analyst identifying key items of note from \
bank earnings call transcripts.

## YOUR TASK

Extract {max_items} notable items that management highlighted in their prepared remarks. \
These are the "headlines" - specific developments, achievements, or concerns worth noting.

## WHAT MAKES A GOOD ITEM OF NOTE

- Specific and actionable (not generic)
- Something management emphasized or called attention to
- A notable development, achievement, milestone, or concern
- Forward-looking themes or strategic priorities
- Changes from prior quarters or expectations

## EXAMPLES OF GOOD ITEMS

- "M&A pipeline at highest level since 2021, signaling strong deal activity"
- "Credit normalization proceeding as expected with no surprises"
- "Deposit competition has stabilized, easing margin pressure"
- "Digital banking adoption accelerating with record mobile engagement"
- "Commercial real estate exposure being actively managed down"

## EXAMPLES OF BAD ITEMS

- "Revenue increased this quarter" - too vague, just a metric
- "We delivered strong results" - generic boilerplate
- "NIM was 2.45%" - just a number, no insight

## FORMAT

- Each item should be 1 sentence (15-25 words)
- Start with the key point, not "Management said..." or "The bank reported..."
- Be specific about what's notable
- Include context that makes it meaningful"""

    user_prompt = f"""Extract the {max_items} most notable items from \
{bank_info['bank_name']}'s {quarter} {fiscal_year} earnings call management discussion.

{md_content}

Identify the key highlights, developments, and concerns management emphasized."""

    # Tool definition
    tool_definition = {
        "type": "function",
        "function": {
            "name": "extract_items_of_note",
            "description": f"Extract {max_items} notable items from management remarks",
            "parameters": {
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "description": (
                                "Notable item (15-25 words). Specific highlight, "
                                "development, or concern. Not generic or metric-only."
                            ),
                        },
                        "description": f"List of {max_items} notable items from the call",
                        "minItems": 1,
                        "maxItems": max_items,
                    },
                },
                "required": ["items"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("transcript_items_extraction")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
            context=context,
            llm_params={
                "model": model,
                "temperature": 0.3,
                "max_tokens": 2000,
            },
        )

        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])
                items = function_args.get("items", [])

                logger.info(
                    "etl.bank_earnings_report.transcript_items_complete",
                    execution_id=execution_id,
                    items_count=len(items),
                )

                return {"source": "Transcript", "items": items}

        logger.warning(
            "etl.bank_earnings_report.transcript_items_no_result",
            execution_id=execution_id,
        )
        return {"source": "Transcript", "items": []}

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.transcript_items_error",
            execution_id=execution_id,
            error=str(e),
        )
        return {"source": "Transcript", "items": []}
