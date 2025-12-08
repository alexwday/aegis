"""
Combination of RTS narrative paragraphs with transcript quotes.

Takes 4 RTS paragraphs and up to 5 transcript quotes, selects the 3 most
relevant quotes, and interleaves them at strategic points between the
RTS paragraphs to create a cohesive Management Narrative section.

Final structure:
- RTS Paragraph 1 (Financial Performance)
  └─ Quote 1 (placed after paragraph 1)
- RTS Paragraph 2 (Business Segments)
  └─ Quote 2 (placed after paragraph 2)
- RTS Paragraph 3 (Risk & Capital)
  └─ Quote 3 (placed after paragraph 3)
- RTS Paragraph 4 (Strategic Outlook)
"""

import json
from typing import Any, Dict, List

from aegis.connections.llm_connector import complete_with_tools
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.utils.logging import get_logger
from aegis.utils.prompt_loader import load_prompt_from_db


def format_content_for_combination(
    rts_paragraphs: List[Dict[str, Any]],
    transcript_quotes: List[Dict[str, Any]],
) -> str:
    """
    Format RTS paragraphs and transcript quotes for LLM combination.

    Args:
        rts_paragraphs: List of paragraph dicts with theme and content
        transcript_quotes: List of quote dicts with speaker, title, content

    Returns:
        Formatted string for LLM prompt
    """
    lines = ["## RTS PARAGRAPHS", ""]

    for i, para in enumerate(rts_paragraphs, 1):
        lines.append(f"### Paragraph {i}: {para.get('theme', 'Unknown')}")
        lines.append(para.get("content", ""))
        lines.append("")

    lines.extend(["## TRANSCRIPT QUOTES", ""])

    for i, quote in enumerate(transcript_quotes, 1):
        speaker = quote.get("speaker", "Unknown")
        title = quote.get("title", "")
        content = quote.get("content", "")
        lines.append(f"**Quote {i}** - {speaker}, {title}")
        lines.append(f'"{content}"')
        lines.append("")

    return "\n".join(lines)


async def combine_narrative_entries(
    rts_paragraphs: List[Dict[str, Any]],
    transcript_quotes: List[Dict[str, Any]],
    bank_name: str,
    quarter: str,
    fiscal_year: int,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Combine RTS paragraphs with transcript quotes into interleaved narrative.

    Uses LLM to select the 3 most relevant quotes and determine optimal
    placement between the 4 RTS paragraphs.

    Structure:
    - Paragraph 1 → Quote A → Paragraph 2 → Quote B → Paragraph 3 → Quote C → Paragraph 4

    Args:
        rts_paragraphs: List of 4 paragraph dicts with theme and content
        transcript_quotes: List of up to 5 quote dicts
        bank_name: Bank name for context
        quarter: Quarter (e.g., "Q2")
        fiscal_year: Fiscal year
        context: Execution context

    Returns:
        Dict with:
            - entries: Interleaved list of RTS and transcript entries
            - combination_notes: Explanation of quote selection and placement
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.narrative_combination.start",
        execution_id=execution_id,
        paragraph_count=len(rts_paragraphs),
        quote_count=len(transcript_quotes),
    )

    # Handle edge cases without LLM call
    if not rts_paragraphs and not transcript_quotes:
        return {"entries": [], "combination_notes": "No content from either source"}

    # If no RTS paragraphs, just return transcript quotes
    if not rts_paragraphs:
        entries = [
            {
                "type": "transcript",
                "content": q.get("content", ""),
                "speaker": q.get("speaker", ""),
                "title": q.get("title", ""),
            }
            for q in transcript_quotes
        ]
        return {
            "entries": entries,
            "combination_notes": "Only transcript quotes available",
        }

    # If no transcript quotes, just return RTS paragraphs
    if not transcript_quotes:
        entries = [{"type": "rts", "content": p.get("content", "")} for p in rts_paragraphs]
        return {
            "entries": entries,
            "combination_notes": "Only RTS paragraphs available",
        }

    # If we have 3 or fewer quotes, use them all (no LLM selection needed)
    # Just need to determine placement
    num_quotes_to_place = min(3, len(transcript_quotes), len(rts_paragraphs) - 1)

    if num_quotes_to_place == 0:
        entries = [{"type": "rts", "content": p.get("content", "")} for p in rts_paragraphs]
        return {
            "entries": entries,
            "combination_notes": "Not enough gaps to place quotes",
        }

    # Build quote index list for enum constraint
    quote_indices = list(range(1, len(transcript_quotes) + 1))

    formatted_content = format_content_for_combination(rts_paragraphs, transcript_quotes)

    # Load prompt from database
    prompt_data = load_prompt_from_db(
        layer="bank_earnings_report_etl",
        name="combined_2_narrative_interleave",
        compose_with_globals=False,
        execution_id=execution_id,
    )

    # Format prompts with dynamic content
    system_prompt = prompt_data["system_prompt"].format(
        bank_name=bank_name,
        quarter=quarter,
        fiscal_year=fiscal_year,
        num_quotes=len(transcript_quotes),
        num_quotes_to_place=num_quotes_to_place,
    )
    user_prompt = prompt_data["user_prompt"].format(
        num_quotes_to_place=num_quotes_to_place,
        formatted_content=formatted_content,
    )

    # Build tool definition with dynamic constraints
    tool_def = prompt_data["tool_definition"]
    tool_def["function"]["parameters"]["properties"]["placements"]["items"]["properties"][
        "quote_number"
    ]["enum"] = quote_indices
    tool_def["function"]["parameters"]["properties"]["placements"]["items"]["properties"][
        "after_paragraph"
    ]["enum"] = list(range(1, num_quotes_to_place + 1))
    tool_def["function"]["parameters"]["properties"]["placements"][
        "description"
    ] = f"Exactly {num_quotes_to_place} quote placements"
    tool_def["function"]["parameters"]["properties"]["placements"]["minItems"] = num_quotes_to_place
    tool_def["function"]["parameters"]["properties"]["placements"]["maxItems"] = num_quotes_to_place

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("combined_2_narrative_interleave")

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

                placements = function_args.get("placements", [])
                notes = function_args.get("combination_notes", "")

                # Build placement map: paragraph_index -> quote
                placement_map = {}
                for placement in placements:
                    quote_num = placement.get("quote_number", 0) - 1  # Convert to 0-indexed
                    after_para = placement.get("after_paragraph", 0) - 1  # Convert to 0-indexed

                    if 0 <= quote_num < len(transcript_quotes) and after_para >= 0:
                        placement_map[after_para] = transcript_quotes[quote_num]

                # Build interleaved entries
                entries = []
                for i, para in enumerate(rts_paragraphs):
                    # Add RTS paragraph
                    entries.append({"type": "rts", "content": para.get("content", "")})

                    # Add quote after this paragraph if one is placed here
                    if i in placement_map:
                        quote = placement_map[i]
                        entries.append(
                            {
                                "type": "transcript",
                                "content": quote.get("content", ""),
                                "speaker": quote.get("speaker", ""),
                                "title": quote.get("title", ""),
                            }
                        )

                logger.info(
                    "etl.narrative_combination.complete",
                    execution_id=execution_id,
                    total_entries=len(entries),
                    rts_entries=len(rts_paragraphs),
                    quote_entries=len(placement_map),
                )

                return {"entries": entries, "combination_notes": notes}

        # Fallback: interleave quotes sequentially
        logger.warning(
            "etl.narrative_combination.no_result",
            execution_id=execution_id,
        )
        return _fallback_interleave(rts_paragraphs, transcript_quotes)

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "etl.narrative_combination.error",
            execution_id=execution_id,
            error=str(e),
        )
        return _fallback_interleave(rts_paragraphs, transcript_quotes)


def _fallback_interleave(
    rts_paragraphs: List[Dict[str, Any]],
    transcript_quotes: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Fallback interleaving without LLM selection.

    Places first 3 quotes after paragraphs 1, 2, 3 in order.

    Args:
        rts_paragraphs: List of RTS paragraph dicts
        transcript_quotes: List of transcript quote dicts

    Returns:
        Combined entries dict
    """
    entries = []
    quotes_to_use = transcript_quotes[:3]

    for i, para in enumerate(rts_paragraphs):
        entries.append({"type": "rts", "content": para.get("content", "")})

        # Add quote after paragraphs 0, 1, 2 (not after the last one)
        if i < len(quotes_to_use):
            quote = quotes_to_use[i]
            entries.append(
                {
                    "type": "transcript",
                    "content": quote.get("content", ""),
                    "speaker": quote.get("speaker", ""),
                    "title": quote.get("title", ""),
                }
            )

    return {
        "entries": entries,
        "combination_notes": "Fallback: quotes placed sequentially due to LLM error",
    }
