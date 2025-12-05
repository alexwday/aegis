"""
Deduplication, merging, and selection of Items of Note from multiple sources.

Process:
1. Items extracted from RTS and Transcript (each with significance scores)
2. LLM identifies duplicates (same event in both sources)
3. Duplicates are merged into single items:
   - Combined description (best details from both)
   - RTS impact value (priority)
   - Source: "RTS & Transcript"
   - Higher significance score
4. All items (merged + unique) sorted by score
5. Top N become featured, rest become remaining
"""

import json
from typing import Any, Dict, List

from aegis.connections.llm_connector import complete_with_tools
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.utils.logging import get_logger


def _format_items_for_dedup(
    rts_items: List[Dict[str, Any]],
    transcript_items: List[Dict[str, Any]],
) -> str:
    """Format items from both sources for LLM deduplication."""
    lines = [
        "## Items from RTS (Regulatory Filing)",
        "",
        "| ID | Description | Impact | Segment | Timing | Score |",
        "|----|-------------|--------|---------|--------|-------|",
    ]

    for i, item in enumerate(rts_items, 1):
        score = item.get("significance_score", 5)
        lines.append(
            f"| R{i} | {item.get('description', '')} | {item.get('impact', '')} | "
            f"{item.get('segment', '')} | {item.get('timing', '')} | {score} |"
        )

    lines.extend(
        [
            "",
            "## Items from Transcript (Earnings Call)",
            "",
            "| ID | Description | Impact | Segment | Timing | Score |",
            "|----|-------------|--------|---------|--------|-------|",
        ]
    )

    for i, item in enumerate(transcript_items, 1):
        score = item.get("significance_score", 5)
        lines.append(
            f"| T{i} | {item.get('description', '')} | {item.get('impact', '')} | "
            f"{item.get('segment', '')} | {item.get('timing', '')} | {score} |"
        )

    return "\n".join(lines)


async def deduplicate_and_merge_items(
    rts_items: List[Dict[str, Any]],
    transcript_items: List[Dict[str, Any]],
    bank_name: str,
    quarter: str,
    fiscal_year: int,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Identify duplicate items across sources and merge them.

    Uses LLM to identify items that describe the same underlying event,
    then merges those items into a single combined item.

    Args:
        rts_items: Items from RTS extraction (with significance_score)
        transcript_items: Items from transcript extraction (with significance_score)
        bank_name: Bank name for context
        quarter: Quarter (e.g., "Q2")
        fiscal_year: Fiscal year
        context: Execution context

    Returns:
        Dict with:
            - items: List of all items (merged + unique) with source attribution
            - merge_notes: Explanation of merges performed
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
    if len(rts_items) == 0 and len(transcript_items) == 0:
        return {"items": [], "merge_notes": "No items from either source"}

    if len(rts_items) == 0:
        items_with_source = [{**item, "source": "Transcript"} for item in transcript_items]
        return {
            "items": items_with_source,
            "merge_notes": "Only transcript items available, no deduplication needed",
        }

    if len(transcript_items) == 0:
        items_with_source = [{**item, "source": "RTS"} for item in rts_items]
        return {
            "items": items_with_source,
            "merge_notes": "Only RTS items available, no deduplication needed",
        }

    # Build reference maps
    rts_map = {f"R{i}": {**item, "source": "RTS"} for i, item in enumerate(rts_items, 1)}
    transcript_map = {
        f"T{i}": {**item, "source": "Transcript"} for i, item in enumerate(transcript_items, 1)
    }

    all_ids = list(rts_map.keys()) + list(transcript_map.keys())
    formatted_items = _format_items_for_dedup(rts_items, transcript_items)

    system_prompt = f"""You are analyzing Items of Note from {bank_name}'s {quarter} \
{fiscal_year} earnings report. Items come from two sources: RTS (regulatory filing) \
and Transcript (earnings call).

## YOUR TASK

1. **Identify Duplicates**: Find items from DIFFERENT sources describing the SAME event
   - Same acquisition, divestiture, or deal
   - Same impairment or write-down
   - Same legal settlement or regulatory matter
   - Same restructuring program

2. **Merge Duplicates**: For each duplicate pair, create ONE merged item that:
   - Combines the best details from both descriptions into a clear, comprehensive description
   - Uses the RTS impact value (more authoritative than transcript)
   - Uses the higher significance score of the two
   - Sets segment and timing from whichever source has more detail

3. **Keep Unique Items**: Items appearing in only one source remain unchanged

## IMPORTANT RULES

- Items are duplicates ONLY if they refer to the EXACT SAME event
- Two items about similar topics (e.g., two different legal matters) are NOT duplicates
- When merging descriptions, create a single cohesive statement (don't just concatenate)
- Always prefer RTS for the dollar impact value
- For significance score, take the MAX of the two scores

## OUTPUT

Return ALL items - both merged items and unique items that weren't duplicated."""

    user_prompt = f"""Review these Items of Note and identify any duplicates to merge:

{formatted_items}

For each duplicate pair (same event in both sources), merge them into a single item.
Keep all unique items unchanged. Return the complete list."""

    # Build enum of valid IDs for merges
    rts_ids = list(rts_map.keys())
    transcript_ids = list(transcript_map.keys())

    tool_definition = {
        "type": "function",
        "function": {
            "name": "process_items_of_note",
            "description": "Process items: merge duplicates, keep unique items",
            "parameters": {
                "type": "object",
                "properties": {
                    "merged_items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "rts_id": {
                                    "type": "string",
                                    "enum": rts_ids,
                                    "description": "ID of the RTS item being merged",
                                },
                                "transcript_id": {
                                    "type": "string",
                                    "enum": transcript_ids,
                                    "description": "ID of the Transcript item being merged",
                                },
                                "merged_description": {
                                    "type": "string",
                                    "description": (
                                        "Combined description using best details from both "
                                        "sources. Single cohesive statement, 15-25 words."
                                    ),
                                },
                                "impact": {
                                    "type": "string",
                                    "description": (
                                        "Dollar impact from RTS (priority). "
                                        "Format: '+$150M', '-$1.2B', 'TBD'"
                                    ),
                                },
                                "segment": {
                                    "type": "string",
                                    "description": "Affected segment (use more detailed source)",
                                },
                                "timing": {
                                    "type": "string",
                                    "description": "Timing info (use more detailed source)",
                                },
                            },
                            "required": [
                                "rts_id",
                                "transcript_id",
                                "merged_description",
                                "impact",
                                "segment",
                                "timing",
                            ],
                        },
                        "description": "Items that appear in BOTH sources (merged)",
                    },
                    "unique_item_ids": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": all_ids,
                        },
                        "description": (
                            "IDs of items that appear in only ONE source (not duplicated). "
                            "Include all R* and T* IDs that weren't merged."
                        ),
                    },
                    "merge_notes": {
                        "type": "string",
                        "description": (
                            "Brief explanation of merges. E.g., 'Merged R1+T2 (HSBC acquisition), "
                            "R3+T1 (City National impairment). 4 unique items unchanged.'"
                        ),
                    },
                },
                "required": ["merged_items", "unique_item_ids", "merge_notes"],
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

                merged_items = function_args.get("merged_items", [])
                unique_ids = function_args.get("unique_item_ids", [])
                merge_notes = function_args.get("merge_notes", "")

                # Build final items list
                final_items = []

                # Add merged items
                merged_rts_ids = set()
                merged_transcript_ids = set()

                for merged in merged_items:
                    rts_id = merged.get("rts_id")
                    transcript_id = merged.get("transcript_id")

                    if rts_id and transcript_id:
                        merged_rts_ids.add(rts_id)
                        merged_transcript_ids.add(transcript_id)

                        # Get original scores
                        rts_score = rts_map.get(rts_id, {}).get("significance_score", 5)
                        transcript_score = transcript_map.get(transcript_id, {}).get(
                            "significance_score", 5
                        )
                        max_score = max(rts_score, transcript_score)

                        final_items.append(
                            {
                                "description": merged.get("merged_description", ""),
                                "impact": merged.get("impact", ""),
                                "segment": merged.get("segment", ""),
                                "timing": merged.get("timing", ""),
                                "source": "RTS & Transcript",
                                "significance_score": max_score,
                            }
                        )

                # Add unique items
                for item_id in unique_ids:
                    if item_id in rts_map and item_id not in merged_rts_ids:
                        final_items.append(rts_map[item_id])
                    elif item_id in transcript_map and item_id not in merged_transcript_ids:
                        final_items.append(transcript_map[item_id])

                # Also add any items that weren't mentioned (safety net)
                mentioned_ids = set(unique_ids) | merged_rts_ids | merged_transcript_ids
                for item_id, item in rts_map.items():
                    if item_id not in mentioned_ids:
                        final_items.append(item)
                for item_id, item in transcript_map.items():
                    if item_id not in mentioned_ids:
                        final_items.append(item)

                logger.info(
                    "etl.items_dedup.complete",
                    execution_id=execution_id,
                    input_rts=len(rts_items),
                    input_transcript=len(transcript_items),
                    merged_count=len(merged_items),
                    unique_count=len(unique_ids),
                    final_count=len(final_items),
                    merge_notes=merge_notes,
                )

                return {"items": final_items, "merge_notes": merge_notes}

        # Fallback: no deduplication
        logger.warning("etl.items_dedup.no_result", execution_id=execution_id)
        return _fallback_combine(rts_items, transcript_items)

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("etl.items_dedup.error", execution_id=execution_id, error=str(e))
        return _fallback_combine(rts_items, transcript_items)


def _fallback_combine(
    rts_items: List[Dict[str, Any]],
    transcript_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Fallback: combine without deduplication."""
    items = []
    for item in rts_items:
        items.append({**item, "source": "RTS"})
    for item in transcript_items:
        items.append({**item, "source": "Transcript"})

    return {
        "items": items,
        "merge_notes": "Fallback: combined without deduplication due to LLM error",
    }


def select_featured_and_remaining(
    items: List[Dict[str, Any]],
    featured_count: int = 4,
) -> Dict[str, Any]:
    """
    Select featured items based on significance score.

    Args:
        items: All items (merged + unique) with significance_score
        featured_count: Number of items for featured section (default 4)

    Returns:
        Dict with:
            - featured: Top N items by score
            - remaining: All other items by score
            - selection_notes: Explanation
    """
    logger = get_logger()

    # Sort by significance score (descending)
    sorted_items = sorted(
        items,
        key=lambda x: x.get("significance_score", 5),
        reverse=True,
    )

    featured = sorted_items[:featured_count]
    remaining = sorted_items[featured_count:]

    # Clean items for output (remove internal score)
    def clean_item(item: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "description": item.get("description", ""),
            "impact": item.get("impact", ""),
            "segment": item.get("segment", ""),
            "timing": item.get("timing", ""),
            "source": item.get("source", ""),
        }

    # Count sources in featured
    source_counts = {}
    for item in featured:
        source = item.get("source", "Unknown")
        source_counts[source] = source_counts.get(source, 0) + 1

    selection_notes = (
        f"Selected top {len(featured)} by significance score. "
        f"Sources: {source_counts}. Remaining: {len(remaining)}."
    )

    logger.info(
        "etl.items_selection.complete",
        featured_count=len(featured),
        remaining_count=len(remaining),
        source_distribution=source_counts,
    )

    return {
        "featured": [clean_item(item) for item in featured],
        "remaining": [clean_item(item) for item in remaining],
        "selection_notes": selection_notes,
    }


async def process_items_of_note(
    rts_items: List[Dict[str, Any]],
    transcript_items: List[Dict[str, Any]],
    bank_name: str,
    quarter: str,
    fiscal_year: int,
    context: Dict[str, Any],
    featured_count: int = 4,
) -> Dict[str, Any]:
    """
    Full pipeline: deduplicate, merge, and select items.

    This is the main entry point that:
    1. Deduplicates and merges items from both sources
    2. Selects top items for featured section
    3. Returns structured result for display

    Args:
        rts_items: Items from RTS extraction
        transcript_items: Items from transcript extraction
        bank_name: Bank name
        quarter: Quarter
        fiscal_year: Fiscal year
        context: Execution context
        featured_count: Number of featured items (default 4)

    Returns:
        Dict with:
            - featured: Top items for collapsed display
            - remaining: Other items for expanded display
            - merge_notes: Deduplication explanation
            - selection_notes: Selection explanation
    """
    # Step 1: Deduplicate and merge
    dedup_result = await deduplicate_and_merge_items(
        rts_items=rts_items,
        transcript_items=transcript_items,
        bank_name=bank_name,
        quarter=quarter,
        fiscal_year=fiscal_year,
        context=context,
    )

    # Step 2: Select featured and remaining
    selection_result = select_featured_and_remaining(
        items=dedup_result.get("items", []),
        featured_count=featured_count,
    )

    return {
        "featured": selection_result.get("featured", []),
        "remaining": selection_result.get("remaining", []),
        "merge_notes": dedup_result.get("merge_notes", ""),
        "selection_notes": selection_result.get("selection_notes", ""),
    }
