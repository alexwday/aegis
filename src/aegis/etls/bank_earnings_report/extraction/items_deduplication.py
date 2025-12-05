"""
Score-based selection and combination of Items of Note from multiple sources.

Combines items from RTS regulatory filings and earnings call transcripts using
significance scores assigned during extraction. No LLM deduplication needed -
we take the top items from each source based on their scores.

Process:
1. Sort RTS items by significance_score (descending)
2. Sort Transcript items by significance_score (descending)
3. Take top 2 from each source -> these become the 4 "featured" items
4. Combine remaining items from both sources
5. Sort featured items by score for final display order
6. Sort remaining items by score for final display order
"""

from typing import Any, Dict, List

from aegis.utils.logging import get_logger


def combine_and_select_items(
    rts_items: List[Dict[str, Any]],
    transcript_items: List[Dict[str, Any]],
    featured_per_source: int = 2,
) -> Dict[str, Any]:
    """
    Combine items from both sources using score-based selection.

    Takes the top N items from each source (by significance_score) for the
    featured section, then combines all remaining items for the expanded section.
    Both sections are sorted by significance score.

    Args:
        rts_items: Items from RTS extraction (with significance_score)
        transcript_items: Items from transcript extraction (with significance_score)
        featured_per_source: Number of top items to take from each source (default 2)

    Returns:
        Dict with:
            - featured: Top items (N from each source, sorted by score)
            - remaining: All other items (sorted by score)
            - selection_notes: Explanation of selection
    """
    logger = get_logger()

    # Add source field and ensure score exists
    rts_with_source = []
    for item in rts_items:
        rts_with_source.append(
            {
                **item,
                "source": "RTS",
                "significance_score": item.get("significance_score", 5),  # Default to 5 if missing
            }
        )

    transcript_with_source = []
    for item in transcript_items:
        transcript_with_source.append(
            {
                **item,
                "source": "Transcript",
                "significance_score": item.get("significance_score", 5),  # Default to 5 if missing
            }
        )

    # Sort each source by significance_score (descending)
    rts_sorted = sorted(
        rts_with_source,
        key=lambda x: x.get("significance_score", 0),
        reverse=True,
    )
    transcript_sorted = sorted(
        transcript_with_source,
        key=lambda x: x.get("significance_score", 0),
        reverse=True,
    )

    # Take top N from each source for featured
    rts_featured = rts_sorted[:featured_per_source]
    transcript_featured = transcript_sorted[:featured_per_source]

    # Combine featured and sort by score
    featured_combined = rts_featured + transcript_featured
    featured_sorted = sorted(
        featured_combined,
        key=lambda x: x.get("significance_score", 0),
        reverse=True,
    )

    # Get remaining items from each source
    rts_remaining = rts_sorted[featured_per_source:]
    transcript_remaining = transcript_sorted[featured_per_source:]

    # Combine remaining and sort by score
    remaining_combined = rts_remaining + transcript_remaining
    remaining_sorted = sorted(
        remaining_combined,
        key=lambda x: x.get("significance_score", 0),
        reverse=True,
    )

    # Build selection notes
    rts_count = len(rts_items)
    transcript_count = len(transcript_items)
    featured_rts = len(rts_featured)
    featured_transcript = len(transcript_featured)

    selection_notes = (
        f"Selected top {featured_rts} from RTS ({rts_count} total) and "
        f"top {featured_transcript} from Transcript ({transcript_count} total). "
        f"Featured: {len(featured_sorted)}, Remaining: {len(remaining_sorted)}."
    )

    logger.info(
        "etl.items_selection.complete",
        rts_total=rts_count,
        transcript_total=transcript_count,
        featured_count=len(featured_sorted),
        remaining_count=len(remaining_sorted),
    )

    # Clean up items for output (remove internal score field from display)
    def clean_item(item: Dict[str, Any]) -> Dict[str, Any]:
        """Remove significance_score from final output."""
        return {
            "description": item.get("description", ""),
            "impact": item.get("impact", ""),
            "segment": item.get("segment", ""),
            "timing": item.get("timing", ""),
            "source": item.get("source", ""),
        }

    return {
        "featured": [clean_item(item) for item in featured_sorted],
        "remaining": [clean_item(item) for item in remaining_sorted],
        "selection_notes": selection_notes,
    }


def get_all_items_sorted(
    rts_items: List[Dict[str, Any]],
    transcript_items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Get all items from both sources sorted by significance score.

    Utility function for cases where we just need a flat sorted list
    without the featured/remaining split.

    Args:
        rts_items: Items from RTS extraction
        transcript_items: Items from transcript extraction

    Returns:
        All items sorted by significance_score (descending)
    """
    # Add source field
    all_items = []
    for item in rts_items:
        all_items.append(
            {
                **item,
                "source": "RTS",
                "significance_score": item.get("significance_score", 5),
            }
        )
    for item in transcript_items:
        all_items.append(
            {
                **item,
                "source": "Transcript",
                "significance_score": item.get("significance_score", 5),
            }
        )

    # Sort by score
    return sorted(
        all_items,
        key=lambda x: x.get("significance_score", 0),
        reverse=True,
    )
