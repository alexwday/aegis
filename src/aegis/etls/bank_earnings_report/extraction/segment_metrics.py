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
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.utils.logging import get_logger


MONITORED_PLATFORMS = [
    "Canadian Banking",
    "U.S. & International Banking",
    "Capital Markets",
    "Canadian Wealth & Insurance",
    "Corporate Support",
]

SEGMENT_METADATA = {
    "Canadian Banking": {
        "description": (
            "Canadian retail and commercial banking including personal banking, mortgages, "
            "deposits, business lending, and credit cards"
        ),
        "key_focus": [
            "loan growth",
            "deposit growth",
            "net interest margin",
            "credit quality",
            "efficiency",
        ],
    },
    "Canadian Wealth & Insurance": {
        "description": (
            "Canadian wealth management and insurance operations including private banking, "
            "asset management, and insurance products"
        ),
        "key_focus": [
            "AUM growth",
            "fee income",
            "net flows",
            "premium revenue",
            "client acquisition",
        ],
    },
    "Capital Markets": {
        "description": "Investment banking, trading, advisory, and corporate banking services",
        "key_focus": [
            "trading revenue",
            "advisory fees",
            "underwriting",
            "market share",
            "ROE",
        ],
    },
    "U.S. & International Banking": {
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
    "Corporate Support": {
        "description": "Corporate treasury, technology, and other enterprise functions",
        "key_focus": ["funding costs", "technology investment", "operational efficiency"],
    },
}

CORE_SEGMENT_METRICS = {
    "Canadian Banking": [
        "Net Income",
        "NIM",
        "Total Revenue",
    ],
    "Canadian Wealth & Insurance": [
        "Total Revenue",
        "Net Income",
        "Non Interest Income",
    ],
    "Capital Markets": [
        "Net Revenue",
        "Non Interest Income",
        "Net Income",
    ],
    "U.S. & International Banking": [
        "Net Income",
        "Net Interest Income (TEB)",
        "Average Loans",
    ],
    "Corporate Support": [
        "Net Revenue",
        "Non Interest Expenses",
        "Net Income",
    ],
}

DEFAULT_CORE_METRICS = [
    "Total Revenue",
    "Net Income",
    "Efficiency Ratio",
]

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


def is_monitored_platform(platform_name: str) -> bool:
    """
    Check if a platform name exactly matches one of our monitored platforms.

    Args:
        platform_name: The Platform value from benchmarking_report

    Returns:
        True if exact match found, False otherwise
    """
    if not platform_name:
        return False

    return platform_name in MONITORED_PLATFORMS


def format_segment_metrics_for_llm(
    metrics: List[Dict[str, Any]],
    segment_name: str,
    exclude_names: Optional[List[str]] = None,
) -> str:
    """
    Format segment metrics into a table for LLM selection.

    Uses proper bps/% formatting so LLM understands the data correctly.

    Args:
        metrics: List of metric dicts from retrieve_segment_metrics()
        segment_name: Name of the segment for context
        exclude_names: List of metric names to exclude

    Returns:
        Formatted table string for LLM prompt
    """
    from aegis.etls.bank_earnings_report.retrieval.supplementary import (
        format_value_for_llm,
        format_delta_for_llm,
    )

    segment_core_metrics = CORE_SEGMENT_METRICS.get(segment_name, DEFAULT_CORE_METRICS)
    exclude_set = (
        set(exclude_names or []) | set(EXCLUDED_SEGMENT_METRICS) | set(segment_core_metrics)
    )

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
        units = m.get("units", "")
        is_bps = m.get("is_bps", False)

        val = format_value_for_llm(m)
        qoq = format_delta_for_llm(m.get("qoq"), units, is_bps)
        yoy = format_delta_for_llm(m.get("yoy"), units, is_bps)
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

    segment_info = SEGMENT_METADATA.get(segment_name, {})
    segment_description = segment_info.get("description", "Business segment")
    key_focus_areas = segment_info.get("key_focus", [])

    segment_core_metrics = CORE_SEGMENT_METRICS.get(segment_name, DEFAULT_CORE_METRICS)
    exclude_set = set(EXCLUDED_SEGMENT_METRICS) | set(segment_core_metrics)
    available_metrics = [m for m in metrics if m["parameter"] not in exclude_set]

    if len(available_metrics) <= num_metrics:
        return {
            "selected_metrics": [m["parameter"] for m in available_metrics],
            "reasoning": "All available metrics selected (fewer than requested)",
            "metrics_data": available_metrics,
        }

    metrics_table = format_segment_metrics_for_llm(metrics, segment_name)

    available_names = [m["parameter"] for m in available_metrics]

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

    user_prompt = f"""Analyze {bank_name}'s {segment_name} segment for {quarter} {fiscal_year}.

{metrics_table}

From these metrics, select the {num_metrics} most impactful to highlight for this segment. \
Consider the segment's key focus areas and what would be most meaningful to investors.

Return the exact metric names from the table above."""

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
        model = etl_config.get_model("segment_metrics_selection")

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

                selected_names = function_args.get("selected_metrics", [])
                reasoning = function_args.get("reasoning", "")

                validated_names = [n for n in selected_names if n in available_names]

                if len(validated_names) < len(selected_names):
                    logger.warning(
                        "etl.bank_earnings_report.segment_metric_validation",
                        execution_id=execution_id,
                        segment=segment_name,
                        original=selected_names,
                        validated=validated_names,
                    )

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
