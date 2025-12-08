"""
LLM-based extraction for Management Narrative section (transcript quotes).

Process:
1. Retrieve Management Discussion (MD) section chunks from transcript
2. Format entire section for LLM context
3. Single LLM call to extract top 5 impactful quotes with speaker attribution
4. Return structured JSON for the report template

Future: RTS summaries will be extracted separately, then a final LLM call
will interleave RTS entries and transcript quotes into the final narrative flow.
"""

import json
from typing import Any, Dict, List

from aegis.connections.llm_connector import complete_with_tools
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.etls.bank_earnings_report.retrieval.transcripts import (
    format_md_section_for_llm,
    retrieve_md_chunks,
)
from aegis.utils.logging import get_logger


async def extract_transcript_quotes(
    bank_info: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    num_quotes: int = 5,
) -> List[Dict[str, Any]]:
    """
    Extract top management quotes from earnings call Management Discussion section.

    Process:
    1. Retrieve all MD section chunks from transcript
    2. Format for LLM with speaker block organization
    3. Single LLM call to extract top N impactful quotes
    4. Return list of quote entries formatted for template

    Args:
        bank_info: Bank information dict with bank_id, bank_name, bank_symbol
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context
        num_quotes: Number of quotes to extract (default 5)

    Returns:
        List of quote entries, each with:
            - type: "transcript"
            - content: The quote or paraphrased statement
            - speaker: Full name of speaker
            - title: Speaker's title (CEO, CFO, etc.)
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.bank_earnings_report.management_narrative_extraction_start",
        execution_id=execution_id,
        bank=bank_info["bank_symbol"],
        period=f"{quarter} {fiscal_year}",
        num_quotes=num_quotes,
    )

    chunks = await retrieve_md_chunks(
        bank_id=bank_info["bank_id"],
        fiscal_year=fiscal_year,
        quarter=quarter,
        context=context,
    )

    if not chunks:
        logger.warning(
            "etl.bank_earnings_report.no_md_chunks",
            execution_id=execution_id,
            bank=bank_info["bank_symbol"],
        )
        return []

    md_content = format_md_section_for_llm(
        chunks=chunks,
        bank_name=bank_info["bank_name"],
        quarter=quarter,
        fiscal_year=fiscal_year,
    )

    if not md_content.strip():
        logger.warning(
            "etl.bank_earnings_report.empty_md_content",
            execution_id=execution_id,
            bank=bank_info["bank_symbol"],
        )
        return []

    system_prompt = f"""You are a senior financial analyst extracting impactful management quotes \
from bank earnings call transcripts.

## CONTEXT

These quotes appear in the "Management Narrative" section alongside RTS summaries. The RTS content \
provides factual context and metrics. Your quotes provide something different: EXECUTIVE VOICE.

## WHAT THESE QUOTES ARE FOR

- **Qualitative insight** - The "why" behind the numbers, not the numbers themselves
- **Executive conviction** - Confidence, caution, or concern on key issues
- **Forward-looking sentiment** - Where management sees things heading
- **Strategic color** - Priorities, focus areas, how leadership is thinking
- **Tone and mood** - What's the sentiment in the C-suite?

## WHAT THESE QUOTES ARE NOT FOR

❌ Specific metrics (NIM expanded 5 bps, revenue grew 8%)
❌ Quantitative guidance (targeting $500M cost saves)
❌ Data points that belong in metrics sections
❌ Generic boilerplate ("We delivered strong results")

## GOOD QUOTE EXAMPLES

- "We're managing through this credit normalization cycle from a position of strength"
- "Client engagement remains elevated and the dialogue with corporates has never been better"
- "We're being disciplined on expenses given the uncertain macro backdrop"
- "The competitive environment for deposits has stabilized meaningfully"
- "We see significant opportunity as markets normalize and activity picks up"

## BAD QUOTE EXAMPLES

- "NIM came in at 2.45%, up 5 basis points" - too metric-focused
- "We delivered another strong quarter" - too generic, no insight
- "Revenue grew 8% year-over-year" - belongs in metrics section

## EXTRACTION GUIDELINES

- **Use verbatim text** from the transcript - do not rephrase or reword
- **Use ellipsis (...)** to trim unnecessary words and condense lengthy quotes
- Keep each quote to 1-2 sentences (20-40 words max)
- Cut filler words, preamble, and tangents while preserving the speaker's actual words
- Capture the executive's perspective and conviction
- Focus on qualitative statements that provide insight
- Select quotes from different speakers when possible (CEO, CFO, CRO)

## EXAMPLE

Original: "I think what we're seeing is that client engagement remains very strong and robust, \
and you know, our backlog has been growing now for four consecutive quarters which is really \
encouraging to see."

Condensed: "Client engagement remains very strong and robust... our backlog has been growing \
for four consecutive quarters."

## OUTPUT

Return exactly {num_quotes} verbatim quotes (condensed with ellipsis as needed)."""

    user_prompt = f"""Extract the {num_quotes} most impactful management quotes from \
{bank_info['bank_name']}'s {quarter} {fiscal_year} earnings call.

{md_content}

Select {num_quotes} quotes that best capture management's key messages for this quarter."""

    tool_definition = {
        "type": "function",
        "function": {
            "name": "extract_management_quotes",
            "description": f"Extract the top {num_quotes} management quotes from earnings call",
            "parameters": {
                "type": "object",
                "properties": {
                    "quotes": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "content": {
                                    "type": "string",
                                    "description": (
                                        "Verbatim quote from transcript (20-40 words). "
                                        "Use ellipsis (...) to condense. No rephrasing."
                                    ),
                                },
                                "speaker": {
                                    "type": "string",
                                    "description": (
                                        "Full name of the speaker "
                                        "(e.g., 'Dave McKay', 'Nadine Ahn')"
                                    ),
                                },
                                "title": {
                                    "type": "string",
                                    "description": (
                                        "Speaker's title/role (e.g., 'President & CEO', 'CFO', "
                                        "'Chief Risk Officer')"
                                    ),
                                },
                            },
                            "required": ["content", "speaker", "title"],
                        },
                        "description": f"Array of exactly {num_quotes} management quotes",
                        "minItems": num_quotes,
                        "maxItems": num_quotes,
                    },
                },
                "required": ["quotes"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("transcript_2_narrative_quotes")

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

                quotes = function_args.get("quotes", [])

                formatted_quotes = []
                for quote in quotes:
                    if quote.get("content") and quote.get("speaker") and quote.get("title"):
                        formatted_quotes.append(
                            {
                                "type": "transcript",
                                "content": quote["content"],
                                "speaker": quote["speaker"],
                                "title": quote["title"],
                            }
                        )

                logger.info(
                    "etl.bank_earnings_report.management_narrative_extraction_complete",
                    execution_id=execution_id,
                    quotes_extracted=len(formatted_quotes),
                    speakers=[q["speaker"] for q in formatted_quotes],
                )

                return formatted_quotes

        logger.warning(
            "etl.bank_earnings_report.management_narrative_no_result",
            execution_id=execution_id,
        )
        return []

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.management_narrative_extraction_error",
            execution_id=execution_id,
            error=str(e),
        )
        return []
