"""
LLM-based extraction for segment performance metrics selection.

Process:
1. Identify which monitored segments exist in the database for the given bank/period
2. For each segment found, retrieve all available metrics from Supp Pack
3. Use LLM to select the top 3 most impactful metrics for each segment
4. Selection is segment-aware - different segments value different metrics

The end result is a container for each segment with 3 highlighted metrics.
"""

import json
from typing import Any, Dict, List, Optional

from aegis.connections.llm_connector import complete_with_tools
from aegis.utils.logging import get_logger
from aegis.utils.settings import config


# =============================================================================
# Monitored Segments - These are the business segments we track
# =============================================================================

# Standard business segment names used by Canadian Big 6 banks
# These are the platform names as they appear in the benchmarking_report table
# The keys here match the exact Platform values from the database

MONITORED_SEGMENTS = {
    "U.S. & International Banking": {
        "aliases": [
            "U.S. Personal & Commercial Banking",
            "U.S. Banking",
            "US Retail",
            "U.S. P&C",
            "U.S. Retail",
            "International Banking",
        ],
        "description": (
            "U.S. and international retail banking operations including personal banking, "
            "commercial lending, and cross-border services"
        ),
        "key_focus": [
            "loan growth",
            "deposit costs",
            "credit provisions",
            "efficiency ratio",
            "cross-border synergies",
        ],
    },
    "Wealth Management": {
        "aliases": [
            "Wealth & Asset Management",
            "Global Wealth Management",
            "Wealth",
            "Asset Management",
            "Private Banking",
            "Global Asset Management",
        ],
        "description": (
            "Wealth management including private banking, asset management, "
            "brokerage, and investment services"
        ),
        "key_focus": ["AUM growth", "fee income", "net flows", "client acquisition", "margins"],
    },
    "Capital Markets": {
        "aliases": [
            "Wholesale Banking",
            "Investment Banking",
            "Corporate & Investment Banking",
            "Global Markets",
            "CIB",
            "Markets",
        ],
        "description": "Investment banking, trading, advisory, and corporate banking services",
        "key_focus": [
            "trading revenue",
            "advisory fees",
            "underwriting",
            "market share",
            "ROE",
        ],
    },
    "Insurance": {
        "aliases": [
            "Insurance Services",
            "Global Insurance",
            "Canadian Insurance",
        ],
        "description": "Insurance products including life, health, property, and reinsurance",
        "key_focus": [
            "premium growth",
            "claims ratio",
            "underwriting income",
            "investment income",
        ],
    },
    "Corporate Support": {
        "aliases": [
            "Corporate & Other",
            "Corporate",
            "Other",
            "Corporate Functions",
        ],
        "description": "Corporate treasury, technology, and other enterprise functions",
        "key_focus": ["funding costs", "technology investment", "operational efficiency"],
    },
}


# Metrics that are less relevant for segment-level analysis
# (These are enterprise-level or capital metrics better shown elsewhere)
EXCLUDED_SEGMENT_METRICS = [
    "CET1 Ratio",
    "CET1 Capital",
    "Tier 1 Capital Ratio",
    "Total Capital Ratio",
    "Leverage Ratio",
    "RWA",
    "Risk-Weighted Assets",
    "LCR",
    "Liquidity Coverage Ratio",
    "NSFR",
    "Dividends Declared",
    "Book Value per Share",
    "Tangible Book Value per Share",
    "Share Count",
    "Market Cap",
]


def normalize_segment_name(platform_name: str) -> Optional[str]:
    """
    Normalize a platform name from the database to our standard segment names.

    Args:
        platform_name: The Platform value from benchmarking_report

    Returns:
        Normalized segment name if matched, None otherwise
    """
    if not platform_name:
        return None

    # Skip Enterprise - that's the overall bank, not a segment
    if platform_name.lower() == "enterprise":
        return None

    # First, check for exact match
    if platform_name in MONITORED_SEGMENTS:
        return platform_name

    # Then check aliases (case-insensitive)
    platform_lower = platform_name.lower()
    for segment_name, segment_info in MONITORED_SEGMENTS.items():
        if platform_lower == segment_name.lower():
            return segment_name
        for alias in segment_info["aliases"]:
            if platform_lower == alias.lower():
                return segment_name

    # Finally, try partial matching for common patterns
    for segment_name, segment_info in MONITORED_SEGMENTS.items():
        # Check if any key words match
        segment_words = segment_name.lower().split()
        platform_words = platform_lower.split()

        # If at least 2 words match, consider it a match
        matching_words = sum(1 for w in segment_words if w in platform_words)
        if matching_words >= 2 or (len(segment_words) == 1 and segment_words[0] in platform_lower):
            return segment_name

    return None


def format_segment_metrics_for_llm(
    metrics: List[Dict[str, Any]],
    segment_name: str,
    exclude_names: Optional[List[str]] = None,
) -> str:
    """
    Format segment metrics into a table for LLM selection.

    Args:
        metrics: List of metric dicts from retrieve_segment_metrics()
        segment_name: Name of the segment for context
        exclude_names: List of metric names to exclude

    Returns:
        Formatted table string for LLM prompt
    """

    def fmt_pct(val):
        if val is None:
            return "—"
        sign = "+" if val > 0 else ""
        return f"{sign}{val:.1f}%"

    def fmt_val(m):
        if m["actual"] is None:
            return "N/A"
        if m["units"] == "%" or m.get("is_bps"):
            return f"{m['actual']:.2f}%"
        elif m["units"] == "millions":
            if m["actual"] >= 1000:
                return f"${m['actual'] / 1000:,.2f}B"
            return f"${m['actual']:,.0f}M"
        else:
            return f"{m['actual']:,.2f}"

    # Build exclusion set
    exclude_set = set(exclude_names or []) | set(EXCLUDED_SEGMENT_METRICS)

    # Filter to relevant metrics
    filtered_metrics = [m for m in metrics if m["parameter"] not in exclude_set]

    if not filtered_metrics:
        return "No metrics available for this segment."

    lines = [
        f"## {segment_name} Metrics",
        "",
        "| Metric | Value | QoQ | YoY | Description |",
        "|--------|-------|-----|-----|-------------|",
    ]

    for m in filtered_metrics:
        name = m["parameter"]
        val = fmt_val(m)
        qoq = fmt_pct(m["qoq"])
        yoy = fmt_pct(m["yoy"])
        desc = (
            m.get("description", "")[:60] + "..."
            if len(m.get("description", "")) > 60
            else m.get("description", "")
        )
        lines.append(f"| {name} | {val} | {qoq} | {yoy} | {desc} |")

    return "\n".join(lines)


async def select_top_segment_metrics(
    metrics: List[Dict[str, Any]],
    segment_name: str,
    bank_name: str,
    quarter: str,
    fiscal_year: int,
    context: Dict[str, Any],
    num_metrics: int = 3,
) -> Dict[str, Any]:
    """
    Use LLM to select the top N most impactful metrics for a specific business segment.

    The LLM is made aware of:
    - Which segment it's selecting for (different segments value different metrics)
    - The key focus areas for that segment type
    - The magnitude and direction of changes

    Args:
        metrics: List of metric dicts from retrieve_segment_metrics()
        segment_name: Name of the segment (e.g., "Canadian P&C")
        bank_name: Name of the bank for context
        quarter: Quarter (e.g., "Q3")
        fiscal_year: Fiscal year (e.g., 2024)
        context: Execution context with auth_config, ssl_config
        num_metrics: Number of metrics to select (default 3)

    Returns:
        Dict with:
            - selected_metrics: List of metric names chosen
            - reasoning: LLM's explanation for selections
            - metrics_data: Full data for selected metrics
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if not metrics:
        logger.warning(
            "etl.bank_earnings_report.no_segment_metrics",
            execution_id=execution_id,
            segment=segment_name,
        )
        return {
            "selected_metrics": [],
            "reasoning": "No metrics available for selection",
            "metrics_data": [],
        }

    # Get segment info for context
    segment_info = MONITORED_SEGMENTS.get(segment_name, {})
    segment_description = segment_info.get("description", "Business segment")
    key_focus_areas = segment_info.get("key_focus", [])

    # Filter out excluded metrics
    exclude_set = set(EXCLUDED_SEGMENT_METRICS)
    available_metrics = [m for m in metrics if m["parameter"] not in exclude_set]

    if len(available_metrics) <= num_metrics:
        # Return all available if we have fewer than requested
        return {
            "selected_metrics": [m["parameter"] for m in available_metrics],
            "reasoning": "All available metrics selected (fewer than requested)",
            "metrics_data": available_metrics,
        }

    # Format metrics table for LLM
    metrics_table = format_segment_metrics_for_llm(metrics, segment_name)

    # Get available metric names for enum validation
    available_names = [m["parameter"] for m in available_metrics]

    # Build the system prompt with segment awareness
    system_prompt = f"""You are a senior financial analyst preparing a bank quarterly earnings \
report. Your task is to select the {num_metrics} most impactful metrics to highlight for a \
specific business segment.

## SEGMENT CONTEXT

You are analyzing the **{segment_name}** segment.

{segment_description}

**Key focus areas for this segment type:**
{chr(10).join(f"- {focus}" for focus in key_focus_areas)}

## SELECTION CRITERIA

Select the {num_metrics} metrics that:
1. **Segment Relevance**: Most relevant to this specific segment's business model
2. **Magnitude of Change**: Show significant QoQ or YoY movements (positive or negative)
3. **Narrative Value**: Tell the most important story about segment performance
4. **Investor Interest**: What analysts would most want to know about this segment

Consider both positive AND negative trends - a significant decline may be more noteworthy than \
a modest improvement.

## IMPORTANT RULES

- Select EXACTLY {num_metrics} metrics
- Choose metrics that complement each other (don't select 3 similar metrics)
- Consider what makes this segment unique vs other segments
- Return metric names EXACTLY as shown in the table"""

    # Build the user prompt
    user_prompt = f"""Analyze {bank_name}'s {segment_name} segment for {quarter} {fiscal_year}.

{metrics_table}

From these metrics, select the {num_metrics} most impactful to highlight for this segment. \
Consider the segment's key focus areas and what would be most meaningful to investors.

Return the exact metric names from the table above."""

    # Define the tool for structured output
    tool_definition = {
        "type": "function",
        "function": {
            "name": "select_segment_metrics",
            "description": f"Select the top {num_metrics} metrics for the {segment_name} segment",
            "parameters": {
                "type": "object",
                "properties": {
                    "selected_metrics": {
                        "type": "array",
                        "items": {"type": "string", "enum": available_names},
                        "description": (
                            f"List of exactly {num_metrics} metric names to highlight. "
                            "Choose based on relevance to this segment and significance of changes."
                        ),
                        "minItems": num_metrics,
                        "maxItems": num_metrics,
                    },
                    "reasoning": {
                        "type": "string",
                        "description": (
                            "Brief explanation (1-2 sentences) of why these metrics were chosen "
                            "for this segment. Reference specific trends and segment relevance."
                        ),
                    },
                },
                "required": ["selected_metrics", "reasoning"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        # Use medium model - segment selection is less complex than enterprise-wide
        model_config = config.llm.medium

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
            context=context,
            llm_params={
                "model": model_config.model,
                "temperature": 0.3,
                "max_tokens": 1000,
            },
        )

        # Parse the tool call response
        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])

                selected_names = function_args.get("selected_metrics", [])
                reasoning = function_args.get("reasoning", "")

                # Validate selected metrics exist in available metrics
                validated_names = [n for n in selected_names if n in available_names]

                if len(validated_names) < len(selected_names):
                    logger.warning(
                        "etl.bank_earnings_report.segment_metric_validation",
                        execution_id=execution_id,
                        segment=segment_name,
                        original=selected_names,
                        validated=validated_names,
                    )

                # Get full metric data for selected metrics
                metrics_dict = {m["parameter"]: m for m in available_metrics}
                metrics_data = [metrics_dict[n] for n in validated_names if n in metrics_dict]

                logger.info(
                    "etl.bank_earnings_report.segment_metrics_selected",
                    execution_id=execution_id,
                    segment=segment_name,
                    selected=validated_names,
                )

                return {
                    "selected_metrics": validated_names,
                    "reasoning": reasoning,
                    "metrics_data": metrics_data,
                }

        # Fallback if no tool call
        logger.warning(
            "etl.bank_earnings_report.no_segment_tool_call",
            execution_id=execution_id,
            segment=segment_name,
        )
        return _fallback_segment_selection(available_metrics, segment_name, num_metrics)

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.segment_selection_error",
            execution_id=execution_id,
            segment=segment_name,
            error=str(e),
        )
        return _fallback_segment_selection(available_metrics, segment_name, num_metrics)


def _fallback_segment_selection(
    metrics: List[Dict[str, Any]],
    segment_name: str,
    num_metrics: int,
) -> Dict[str, Any]:
    """
    Fallback metric selection when LLM fails.

    Selects metrics based on absolute magnitude of YoY change.

    Args:
        metrics: List of metric dicts
        segment_name: Segment name for context
        num_metrics: Number of metrics to select

    Returns:
        Selection dict with selected_metrics, reasoning, and metrics_data
    """

    # Sort by absolute YoY change (largest movements first)
    def sort_key(m):
        yoy = m.get("yoy")
        if yoy is None:
            return 0
        return abs(yoy)

    sorted_metrics = sorted(metrics, key=sort_key, reverse=True)
    selected = sorted_metrics[:num_metrics]

    return {
        "selected_metrics": [m["parameter"] for m in selected],
        "reasoning": (
            f"Fallback selection for {segment_name} - chose metrics with largest YoY changes"
        ),
        "metrics_data": selected,
    }
