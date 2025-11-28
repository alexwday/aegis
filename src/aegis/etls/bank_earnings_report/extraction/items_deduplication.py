"""
Deduplication and selection of Items of Note from multiple sources.

Combines items from RTS regulatory filings and earnings call transcripts,
removes duplicates based on semantic similarity, and selects the top items
to display in the final report.
"""

import json
from typing import Any, Dict, List

from aegis.connections.llm_connector import complete_with_tools
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.utils.logging import get_logger


def format_items_for_dedup(
    rts_items: List[Dict[str, Any]],
    transcript_items: List[Dict[str, Any]],
) -> str:
    """
    Format items from both sources into a table for LLM deduplication.

    Args:
        rts_items: Items extracted from RTS filings
        transcript_items: Items extracted from earnings transcripts

    Returns:
        Formatted markdown table string
    """
    lines = [
        "## Items from RTS (Regulatory Filing)",
        "",
        "| ID | Description | Impact | Segment | Timing |",
        "|----|-------------|--------|---------|--------|",
    ]

    for i, item in enumerate(rts_items, 1):
        lines.append(
            f"| R{i} | {item.get('description', '')} | {item.get('impact', '')} | "
            f"{item.get('segment', '')} | {item.get('timing', '')} |"
        )

    lines.extend(
        [
            "",
            "## Items from Transcript (Earnings Call)",
            "",
            "| ID | Description | Impact | Segment | Timing |",
            "|----|-------------|--------|---------|--------|",
        ]
    )

    for i, item in enumerate(transcript_items, 1):
        lines.append(
            f"| T{i} | {item.get('description', '')} | {item.get('impact', '')} | "
            f"{item.get('segment', '')} | {item.get('timing', '')} |"
        )

    return "\n".join(lines)


async def deduplicate_and_select_items(
    rts_items: List[Dict[str, Any]],
    transcript_items: List[Dict[str, Any]],
    bank_name: str,
    quarter: str,
    fiscal_year: int,
    context: Dict[str, Any],
    max_items: int = 8,
) -> Dict[str, Any]:
    """
    Deduplicate and select top items from RTS and transcript sources.

    Uses LLM to:
    1. Identify duplicate items that refer to the same event
    2. Select the best version when duplicates exist (prefer more detail/precision)
    3. Rank all unique items by significance
    4. Return top N items with source attribution

    Args:
        rts_items: Items from RTS extraction
        transcript_items: Items from transcript extraction
        bank_name: Bank name for context
        quarter: Quarter (e.g., "Q2")
        fiscal_year: Fiscal year
        context: Execution context
        max_items: Maximum items to return (default 8)

    Returns:
        Dict with:
            - items: List of deduplicated items with source field
            - dedup_notes: Explanation of deduplication decisions
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.items_dedup.start",
        execution_id=execution_id,
        rts_count=len(rts_items),
        transcript_count=len(transcript_items),
    )

    # Handle edge cases without LLM call
    total_items = len(rts_items) + len(transcript_items)
    if total_items == 0:
        return {"items": [], "dedup_notes": "No items from either source"}

    # If only one source has items and count is small, skip deduplication
    if len(rts_items) == 0 and len(transcript_items) <= max_items:
        items_with_source = [{**item, "source": "Transcript"} for item in transcript_items]
        return {
            "items": items_with_source,
            "dedup_notes": "Only transcript items available, no deduplication needed",
        }

    if len(transcript_items) == 0 and len(rts_items) <= max_items:
        items_with_source = [{**item, "source": "RTS"} for item in rts_items]
        return {
            "items": items_with_source,
            "dedup_notes": "Only RTS items available, no deduplication needed",
        }

    # Build combined list with IDs for reference
    all_items = []
    for i, item in enumerate(rts_items, 1):
        all_items.append({"id": f"R{i}", "source": "RTS", **item})
    for i, item in enumerate(transcript_items, 1):
        all_items.append({"id": f"T{i}", "source": "Transcript", **item})

    # Build the item ID enum for the LLM
    item_ids = [item["id"] for item in all_items]

    formatted_items = format_items_for_dedup(rts_items, transcript_items)

    system_prompt = f"""You are a senior financial analyst reviewing Items of Note from two \
sources for {bank_name}'s {quarter} {fiscal_year} earnings report.

## YOUR TASK

1. **Identify Duplicates**: Find items that refer to the SAME underlying event
   - Same acquisition/deal mentioned in both sources
   - Same regulatory matter or settlement
   - Same restructuring program or impairment

2. **Select Best Version**: When duplicates exist, pick the item with:
   - More precise dollar amount
   - More specific timing
   - More detailed description
   - Default to RTS if both are equally detailed (regulatory filing is authoritative)

3. **Rank by Significance**: Order unique items by:
   - Magnitude of dollar impact (larger = more significant)
   - Strategic importance (M&A > routine provisions)
   - Investor interest (high-profile > routine)

4. **Return Top {max_items}**: Select the {max_items} most significant unique items

## IMPORTANT RULES

- Two items are duplicates ONLY if they refer to the exact same event
- Items about similar topics (e.g., two different legal matters) are NOT duplicates
- Preserve the original description, impact, segment, and timing from the selected source
- Every returned item must include its source ("RTS" or "Transcript")"""

    user_prompt = f"""Review these Items of Note from {bank_name}'s {quarter} {fiscal_year} report:

{formatted_items}

Identify any duplicates, select the best version of each, rank by significance, and return \
the top {max_items} unique items. Each item you return must reference the original ID \
(R1, R2, T1, T2, etc.) so we can trace it back to the source."""

    tool_definition = {
        "type": "function",
        "function": {
            "name": "select_items_of_note",
            "description": "Select and rank deduplicated items of note",
            "parameters": {
                "type": "object",
                "properties": {
                    "selected_items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "item_id": {
                                    "type": "string",
                                    "enum": item_ids,
                                    "description": "ID of the selected item (R1, R2, T1, T2, etc.)",
                                },
                                "duplicate_of": {
                                    "type": "string",
                                    "description": (
                                        "If this item was chosen over a duplicate, "
                                        "list the ID of the duplicate (e.g., 'T2'). "
                                        "Leave empty if no duplicate."
                                    ),
                                },
                            },
                            "required": ["item_id"],
                        },
                        "description": f"Top {max_items} unique items, in order of significance",
                        "maxItems": max_items,
                    },
                    "deduplication_notes": {
                        "type": "string",
                        "description": (
                            "Brief explanation of duplicates found and selection rationale. "
                            "Example: 'R2 and T1 both describe HSBC integration costs; "
                            "selected R2 for more precise amount.'"
                        ),
                    },
                },
                "required": ["selected_items", "deduplication_notes"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("items_deduplication")

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

                selected_ids = function_args.get("selected_items", [])
                dedup_notes = function_args.get("deduplication_notes", "")

                # Map IDs back to full item data
                id_to_item = {item["id"]: item for item in all_items}
                final_items = []

                for selection in selected_ids:
                    item_id = selection.get("item_id")
                    if item_id in id_to_item:
                        item = id_to_item[item_id]
                        # Return item without the temporary ID field
                        final_items.append(
                            {
                                "description": item["description"],
                                "impact": item["impact"],
                                "segment": item["segment"],
                                "timing": item["timing"],
                                "source": item["source"],
                            }
                        )

                logger.info(
                    "etl.items_dedup.complete",
                    execution_id=execution_id,
                    input_count=total_items,
                    output_count=len(final_items),
                    dedup_notes=dedup_notes,
                )

                return {"items": final_items, "dedup_notes": dedup_notes}

        # Fallback: combine and truncate without deduplication
        logger.warning(
            "etl.items_dedup.no_result",
            execution_id=execution_id,
        )
        return _fallback_combine(rts_items, transcript_items, max_items)

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "etl.items_dedup.error",
            execution_id=execution_id,
            error=str(e),
        )
        return _fallback_combine(rts_items, transcript_items, max_items)


def _fallback_combine(
    rts_items: List[Dict[str, Any]],
    transcript_items: List[Dict[str, Any]],
    max_items: int,
) -> Dict[str, Any]:
    """
    Fallback combination without LLM deduplication.

    Interleaves items from both sources, prioritizing RTS.

    Args:
        rts_items: Items from RTS
        transcript_items: Items from transcript
        max_items: Maximum items to return

    Returns:
        Combined items dict
    """
    combined = []

    # Add RTS items first (more authoritative)
    for item in rts_items:
        combined.append({**item, "source": "RTS"})

    # Then transcript items
    for item in transcript_items:
        combined.append({**item, "source": "Transcript"})

    return {
        "items": combined[:max_items],
        "dedup_notes": "Fallback: combined without deduplication due to LLM error",
    }
