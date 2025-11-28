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
        model = etl_config.get_model("transcript_overview_extraction")

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
    max_items: int = 10,
) -> Dict[str, Any]:
    """
    Extract key highlights from the Management Discussion section.

    Items of Note are HEADLINE-WORTHY events that management emphasized:
    - Major M&A activity (acquisitions, divestitures)
    - Significant impairments or write-downs
    - Notable legal/regulatory matters
    - Strategic restructuring programs
    - Material one-time items

    Focus on items that would appear in analyst takeaways, not routine commentary.

    Args:
        bank_info: Bank information dict with bank_id, bank_name, bank_symbol
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context
        max_items: Maximum items to extract (default 10)

    Returns:
        Dict with:
            - source: "Transcript"
            - items: List of item dicts with description, impact, segment, timing
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

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

    system_prompt = """You are a senior financial analyst extracting KEY HIGHLIGHTS from \
bank earnings call transcripts for an executive earnings summary.

## WHAT "ITEMS OF NOTE" MEANS

Items of Note are the HEADLINE-WORTHY events and decisions that management specifically \
calls out or emphasizes during the earnings call. These are the significant developments \
that investors and analysts would want to highlight.

Think: "What did management want investors to remember from this call?"

## TYPES OF ITEMS TO EXTRACT (in order of importance)

1. **Major M&A Activity**: Acquisitions, divestitures, strategic transactions management highlighted
2. **Large Impairments/Write-downs**: Significant charges management called out
3. **Notable Legal/Regulatory**: Settlements, fines, or regulatory matters discussed
4. **Strategic Restructuring**: Major programs management announced or updated
5. **Significant One-Time Items**: Material gains or losses management emphasized

## MATERIALITY THRESHOLD

Only extract items that management HIGHLIGHTED as significant:
- Was this item specifically called out in prepared remarks?
- Did management spend time explaining this item?
- Would this appear in the earnings call headline?

## WHAT TO EXTRACT FOR EACH ITEM

1. **Description**: Clear, specific description of what management announced (10-20 words)
2. **Impact**: Dollar amount EXACTLY as stated by management. Format: '+$150M', '-$45M'. \
Use "TBD" only if management discussed the item but didn't quantify it.
3. **Segment**: Affected segment or "All" if enterprise-wide
4. **Timing**: One-time, recurring, or expected duration as stated

## WHAT NOT TO EXTRACT

- Performance results (revenue up, earnings beat) - these are results, not notable items
- Forward guidance without specific events - general outlook is not an item
- Routine operational updates with numbers
- Items just mentioned in passing without emphasis
- Anything that wouldn't be in an analyst's key takeaways

## EXAMPLES OF GOOD ITEMS (Management Highlighted)

| Description | Impact | Segment | Timing |
|-------------|--------|---------|--------|
| Completion of HSBC Canada acquisition | -$13.5B | Canadian Banking | Closed Q1 2024 |
| City National goodwill impairment charge | -$450M | U.S. Banking | Q2 2024 |
| Settlement of long-standing litigation | -$85M | Capital Markets | Resolved |
| Branch network optimization program | -$200M | Canadian Banking | Through 2025 |

## EXAMPLES OF BAD ITEMS (DO NOT EXTRACT)

- "Revenue increased 8% year-over-year" - performance result, not an event
- "We're investing in digital capabilities" - ongoing strategy, not a notable item
- "M&A pipeline remains strong" - commentary, not a specific event
- "Credit costs were elevated" - performance observation, not a highlighted item
- "We expect NIM to stabilize" - guidance, not an event"""

    user_prompt = f"""Extract KEY HIGHLIGHTS from {bank_info['bank_name']}'s {quarter} \
{fiscal_year} earnings call.

{md_content}

Identify the HEADLINE-WORTHY items that management specifically called out or emphasized - \
major M&A, significant charges, notable legal matters, or strategic programs. Focus on items \
that would appear in analyst takeaways. If management didn't highlight any notable items, \
return a short list or empty - do not pad with routine commentary."""

    tool_definition = {
        "type": "function",
        "function": {
            "name": "extract_items_of_note",
            "description": "Extract specific $ impact events from earnings call",
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
                                        "Brief description of the specific event (10-20 words). "
                                        "What happened, not commentary about it."
                                    ),
                                },
                                "impact": {
                                    "type": "string",
                                    "description": (
                                        "Dollar impact ONLY - no additional text. "
                                        "Format: sign + $ + number + unit. "
                                        "Examples: '+$150M', '-$45M', '~$100M', '-$1.2B', 'TBD'. "
                                        "Use M for millions, B for billions. "
                                        "Do NOT add qualifiers like 'before-tax' or 'cumulative'."
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
                                        "Timing info: 'One-time', 'Recurring', 'Q3 2025', "
                                        "'Through 2025', 'Resolution 2026', etc."
                                    ),
                                },
                            },
                            "required": ["description", "impact", "segment", "timing"],
                        },
                        "description": (
                            "List of significant impact items (may be empty if none found)"
                        ),
                        "maxItems": max_items,
                    },
                    "extraction_notes": {
                        "type": "string",
                        "description": (
                            "Brief note on extraction: how many items found, "
                            "or why none were found if list is empty."
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
        model = etl_config.get_model("transcript_items_extraction")

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
