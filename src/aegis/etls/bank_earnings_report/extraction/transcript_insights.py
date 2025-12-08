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

    md_content = format_md_section_for_llm(
        chunks=chunks,
        bank_name=bank_info["bank_name"],
        quarter=quarter,
        fiscal_year=fiscal_year,
    )

    if not md_content.strip():
        return {"source": "Transcript", "narrative": ""}

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
        model = etl_config.get_model("transcript_1_keymetrics_overview")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
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
    max_items: int = 8,
) -> Dict[str, Any]:
    """
    Extract key defining items from the Management Discussion section.

    Items of Note are the events and developments that MOST SIGNIFICANTLY DEFINED
    this quarter for the bank - not just what management mentioned, but what matters
    most to understanding the bank's quarter.

    Each item is scored by significance (1-10) to enable ranking.

    Args:
        bank_info: Bank information dict with bank_id, bank_name, bank_symbol
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context
        max_items: Maximum items to extract (default 8)

    Returns:
        Dict with:
            - source: "Transcript"
            - items: List of item dicts with description, impact, segment, timing, score
    """
    logger = get_logger()
    execution_id = context.get("execution_id")
    bank_name = bank_info["bank_name"]

    logger.info(
        "etl.bank_earnings_report.transcript_items_start",
        execution_id=execution_id,
        bank=bank_info["bank_symbol"],
        period=f"{quarter} {fiscal_year}",
    )

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

    md_content = format_md_section_for_llm(
        chunks=chunks,
        bank_name=bank_info["bank_name"],
        quarter=quarter,
        fiscal_year=fiscal_year,
    )

    if not md_content.strip():
        return {"source": "Transcript", "items": []}

    system_prompt = f"""You are a senior financial analyst identifying the KEY DEFINING ITEMS \
for {bank_name}'s {quarter} {fiscal_year} quarter from their earnings call transcript.

## YOUR MISSION

Find the items that MOST SIGNIFICANTLY DEFINED this quarter for the bank. Not just what \
management mentioned, but what truly MATTERS - the events, decisions, and developments that \
an analyst would point to when explaining "what happened this quarter" to investors.

Think: "If I had to explain what defined {bank_name}'s {quarter} to an investor in 30 seconds, \
which items from this transcript would I mention?"

## WHAT MAKES AN ITEM "DEFINING"

A defining item has HIGH IMPACT on the bank through one or more of:

1. **Financial Materiality**: Significant dollar impact on earnings, capital, or valuation
   - Major acquisitions or divestitures (>$500M)
   - Large impairments or write-downs
   - Significant legal settlements or regulatory penalties

2. **Strategic Significance**: Changes the bank's trajectory or market position
   - Entry or exit from major business lines
   - Transformational deals or partnerships
   - Major restructuring programs

3. **Investor Relevance**: Would be highlighted in analyst reports or earnings headlines
   - Items that explain earnings beat/miss
   - Risk events that affect outlook
   - One-time items that distort comparisons

## WHAT TO EXCLUDE

**Routine Operations (NEVER extract):**
- Capital note/debenture issuance or redemption
- Preferred share activity
- NCIB share repurchases
- Regular dividend announcements
- Normal PCL provisions
- Routine debt refinancing

**Performance Results (NOT items):**
- "Revenue increased X%"
- "NIM expanded Y bps"
- "Expenses down Z%"
These are RESULTS, not defining ITEMS.

**Forward Guidance (NOT items):**
- Outlook commentary
- "M&A pipeline remains strong"
- General strategic aspirations
These are COMMENTARY, not defining items.

## SIGNIFICANCE SCORING (1-10)

Score each item based on how much it DEFINED the quarter:

- **9-10**: Quarter-defining event (major M&A close, significant impairment, transformational)
- **7-8**: Highly significant (large one-time item, notable strategic move)
- **5-6**: Moderately significant (meaningful but not headline-level)
- **3-4**: Minor significance (worth noting but not quarter-defining)
- **1-2**: Low significance (borderline whether to include)

Be discriminating - not every item is highly significant. A quarter might have only 1-2 truly \
defining items and several minor ones. That's fine.

## OUTPUT FORMAT

For each item:
- **Description**: What happened (10-20 words, factual)
- **Impact**: Dollar amount exactly as stated ('+$150M', '-$1.2B', 'TBD')
- **Segment**: Affected business segment
- **Timing**: When/duration
- **Score**: Significance score (1-10)"""

    user_prompt = f"""Review {bank_name}'s {quarter} {fiscal_year} earnings call transcript and \
identify the items that MOST SIGNIFICANTLY DEFINED this quarter for the bank.

{md_content}

Extract items based on their IMPACT TO THE BANK, not just their mention in the call. \
Score each item by significance (1-10). Quality over quantity - it's better to return 3 truly \
defining items than 8 marginal ones."""

    tool_definition = {
        "type": "function",
        "function": {
            "name": "extract_transcript_items_of_note",
            "description": "Extract key defining items from earnings call with significance scores",
            "parameters": {
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "description": {
                                    "type": "string",
                                    "description": (
                                        "Brief description of the defining item (10-20 words). "
                                        "What happened - factual, not commentary."
                                    ),
                                },
                                "impact": {
                                    "type": "string",
                                    "description": (
                                        "Dollar impact ONLY. "
                                        "Format: '+$150M', '-$45M', '~$100M', '-$1.2B', 'TBD'. "
                                        "No qualifiers or additional text."
                                    ),
                                },
                                "segment": {
                                    "type": "string",
                                    "description": (
                                        "Affected segment: 'Canadian Banking', 'Capital Markets', "
                                        "'Wealth & Insurance', 'U.S. Banking', 'All', or 'N/A'"
                                    ),
                                },
                                "timing": {
                                    "type": "string",
                                    "description": (
                                        "Timing: 'One-time', 'Q3 2025', 'Through 2025', etc."
                                    ),
                                },
                                "significance_score": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 10,
                                    "description": (
                                        "How much this item DEFINED the quarter (1-10). "
                                        "10=quarter-defining, 7-8=highly significant, "
                                        "5-6=moderate, 3-4=minor, 1-2=low."
                                    ),
                                },
                            },
                            "required": [
                                "description",
                                "impact",
                                "segment",
                                "timing",
                                "significance_score",
                            ],
                        },
                        "description": (
                            "Defining items with significance scores (quality over quantity)"
                        ),
                        "maxItems": max_items,
                    },
                    "extraction_notes": {
                        "type": "string",
                        "description": (
                            "Brief note: what defined this quarter, or why few items found."
                        ),
                    },
                },
                "required": ["items", "extraction_notes"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("transcript_1_keymetrics_items")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
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
                    "etl.bank_earnings_report.transcript_items_complete",
                    execution_id=execution_id,
                    items_count=len(items),
                    extraction_notes=notes,
                )

                return {"source": "Transcript", "items": items, "notes": notes}

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
