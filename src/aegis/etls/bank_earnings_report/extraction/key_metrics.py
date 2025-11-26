"""
LLM-based extraction for key metrics selection.

Process:
1. FIXED 7 Key Metrics - These are mandatory and always displayed in the main tiles/chart
   - The LLM must select ONE of these 7 to feature on the 8-quarter trend chart
   - Selection is based on which metric has the most compelling trend story
2. DYNAMIC 5 Additional Metrics - Selected from ALL remaining metrics (excluding the 7 key)
   - LLM chooses based on analytical value and significant trends
"""

import json
from typing import Any, Dict, List

from aegis.connections.llm_connector import complete_with_tools
from aegis.utils.logging import get_logger
from aegis.utils.settings import config


# =============================================================================
# Fixed Key Metrics - These 7 metrics are ALWAYS shown in the Key Metrics section
# =============================================================================

# These are the core metrics that analysts expect to see for bank earnings.
# The LLM does not choose these - they are mandatory.
# The LLM only decides which ONE of these 7 to feature on the trend chart.
#
# Selection rationale:
# 1. Core Cash Diluted EPS - Headline metric in every earnings release, drives analyst models
# 2. Return on Equity - Primary measure of return on shareholder capital (target 15-17%)
# 3. NIM (AIEA) - Net Interest Margin, core spread business health (~50% of bank revenue)
# 4. Efficiency Ratio - Cost discipline benchmark (lower = better)
# 5. Total Revenue - Top-line indicator, shows business momentum
# 6. Pre Provision Profit - Core earnings power before credit costs
# 7. Provisions for Credit Losses - Credit quality indicator
KEY_METRICS = [
    "Core Cash Diluted EPS",
    "Return on Equity",
    "NIM (AIEA)",
    "Efficiency Ratio",
    "Total Revenue",
    "Pre Provision Profit",
    "Provisions for Credit Losses",
]


# =============================================================================
# Excluded Metrics - Capital/Risk metrics have their own section
# =============================================================================

EXCLUDED_METRICS = [
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
    "PCL",
    # Note: "Provision for Credit Losses" removed - now a KEY_METRIC
    "GIL",
    "Gross Impaired Loans",
    "Net Impaired Loans",
    "ACL",
    "Allowance for Credit Losses",
]


def format_key_metrics_for_llm(metrics: List[Dict[str, Any]], key_metric_names: List[str]) -> str:
    """
    Format the 7 fixed key metrics into a table for LLM chart selection.

    Uses proper bps/% formatting so LLM understands the data correctly.

    Args:
        metrics: List of metric dicts from retrieve_all_metrics()
        key_metric_names: List of the 7 fixed key metric names

    Returns:
        Formatted table string for LLM prompt
    """
    # Import shared formatting functions
    from aegis.etls.bank_earnings_report.retrieval.supplementary import (
        format_value_for_llm,
        format_delta_for_llm,
    )

    # Filter to only key metrics that exist in the data
    key_metrics = [m for m in metrics if m["parameter"] in key_metric_names]

    lines = [
        "| Metric | Value | QoQ | YoY | 2Y | 3Y | 5Y |",
        "|--------|-------|-----|-----|----|----|----| ",
    ]

    for m in key_metrics:
        name = m["parameter"]
        units = m.get("units", "")
        is_bps = m.get("is_bps", False)

        val = format_value_for_llm(m)
        qoq = format_delta_for_llm(m.get("qoq"), units, is_bps)
        yoy = format_delta_for_llm(m.get("yoy"), units, is_bps)
        y2 = format_delta_for_llm(m.get("2y"), units, is_bps)
        y3 = format_delta_for_llm(m.get("3y"), units, is_bps)
        y5 = format_delta_for_llm(m.get("5y"), units, is_bps)
        lines.append(f"| {name} | {val} | {qoq} | {yoy} | {y2} | {y3} | {y5} |")

    return "\n".join(lines)


def format_remaining_metrics_for_llm(
    metrics: List[Dict[str, Any]], exclude_names: List[str]
) -> str:
    """
    Format remaining metrics (excluding key metrics and capital/risk) for LLM selection.

    Uses proper bps/% formatting so LLM understands the data correctly.

    Args:
        metrics: List of metric dicts from retrieve_all_metrics()
        exclude_names: List of metric names to exclude (key metrics + capital/risk)

    Returns:
        Formatted table string for LLM prompt
    """
    # Import shared formatting functions
    from aegis.etls.bank_earnings_report.retrieval.supplementary import (
        format_value_for_llm,
        format_delta_for_llm,
    )

    # Filter out excluded metrics
    exclude_set = set(exclude_names)
    remaining_metrics = [m for m in metrics if m["parameter"] not in exclude_set]

    if not remaining_metrics:
        return "No additional metrics available."

    lines = [
        "| Metric | Value | QoQ | YoY | 2Y | 3Y | 5Y |",
        "|--------|-------|-----|-----|----|----|----| ",
    ]

    for m in remaining_metrics:
        name = m["parameter"]
        units = m.get("units", "")
        is_bps = m.get("is_bps", False)

        val = format_value_for_llm(m)
        qoq = format_delta_for_llm(m.get("qoq"), units, is_bps)
        yoy = format_delta_for_llm(m.get("yoy"), units, is_bps)
        y2 = format_delta_for_llm(m.get("2y"), units, is_bps)
        y3 = format_delta_for_llm(m.get("3y"), units, is_bps)
        y5 = format_delta_for_llm(m.get("5y"), units, is_bps)
        lines.append(f"| {name} | {val} | {qoq} | {yoy} | {y2} | {y3} | {y5} |")

    return "\n".join(lines)


async def select_chart_and_tile_metrics(
    metrics: List[Dict[str, Any]],
    bank_name: str,
    quarter: str,
    fiscal_year: int,
    context: Dict[str, Any],
    num_tile_metrics: int = 6,
    num_dynamic_metrics: int = 5,
) -> Dict[str, Any]:
    """
    Use LLM to select chart metric and dynamic metrics for the earnings report.

    Process:
    1. The 7 KEY_METRICS are fixed - they MUST all be displayed
    2. LLM selects ONE of these 7 to feature on the 8-quarter trend chart
    3. LLM selects 5 ADDITIONAL metrics from the remaining pool for slim tiles

    Args:
        metrics: List of metric dicts from retrieve_all_metrics()
        bank_name: Name of the bank for context
        quarter: Quarter (e.g., "Q3")
        fiscal_year: Fiscal year (e.g., 2024)
        context: Execution context with auth_config, ssl_config
        num_tile_metrics: Number of tile metrics (fixed at 6, plus 1 chart = 7)
        num_dynamic_metrics: Number of dynamic slim tile metrics to select (default 5)

    Returns:
        Dict with:
            - chart_metric: The metric selected for the 8Q trend chart (from the 7 key metrics)
            - tile_metrics: The other 6 key metrics (for tiles)
            - dynamic_metrics: List of 5 additional metrics for slim tiles
            - reasoning: LLM's explanation for selections
            - available_metrics: Count of metrics available
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if not metrics:
        logger.warning(
            "etl.bank_earnings_report.no_metrics_for_selection",
            execution_id=execution_id,
        )
        return {
            "chart_metric": None,
            "tile_metrics": [],
            "dynamic_metrics": [],
            "reasoning": "No metrics available for selection",
            "available_metrics": 0,
            "prompt": "",
        }

    # Find which key metrics exist in the data
    available_metric_names = {m["parameter"] for m in metrics}
    available_key_metrics = [m for m in KEY_METRICS if m in available_metric_names]

    if not available_key_metrics:
        logger.warning(
            "etl.bank_earnings_report.no_key_metrics_available",
            execution_id=execution_id,
        )
        return _fallback_selection(metrics, [], num_tile_metrics, num_dynamic_metrics)

    # Build exclusion list for dynamic metrics (key metrics + capital/risk)
    exclusion_list = KEY_METRICS + EXCLUDED_METRICS

    # Format tables for LLM
    key_metrics_table = format_key_metrics_for_llm(metrics, available_key_metrics)
    remaining_metrics_table = format_remaining_metrics_for_llm(metrics, exclusion_list)

    # Build the system prompt
    system_prompt = """You are a senior financial analyst preparing a bank quarterly earnings \
report. Your task is to make two selections based on the data provided.

## TASK 1: Select Chart Metric (from 7 Fixed Key Metrics)

You will be given data for 7 KEY METRICS that are ALWAYS displayed in the report:
- Core Cash Diluted EPS (headline metric, drives analyst models)
- Return on Equity (return on shareholder capital)
- NIM (AIEA) - Net Interest Margin (core spread business health)
- Efficiency Ratio (cost discipline benchmark)
- Total Revenue (top-line growth indicator)
- Pre Provision Profit (core earnings power before credit costs)
- Provisions for Credit Losses (credit quality indicator)

Your job is to select ONE of these 7 metrics to feature on an 8-quarter trend chart.

Selection criteria - choose the metric with:
- The most compelling trend story based on QoQ, YoY, and multi-year (2Y-5Y) changes
- A pattern that tells a meaningful narrative (strong growth, notable recovery, significant shift)
- Data that would be most valuable to highlight visually for investors

Analyze the trend data carefully. Look for:
- Consistent directional movement (sustained growth or decline)
- Inflection points (recovery after decline, acceleration of growth)
- Magnitude of changes (large YoY or multi-year movements)
- Divergence from historical patterns

## TASK 2: Select 5 Additional Highlight Metrics (from Remaining Pool)

From ALL remaining metrics (excluding the 7 key metrics and capital/risk metrics), \
select 5 metrics to display as "Additional Highlights" in slim tiles.

Selection criteria:
- Metrics that provide valuable context beyond the 7 key metrics
- Metrics with significant or noteworthy QoQ/YoY trends
- Metrics that signal important operational or balance sheet dynamics
- Metrics a financial analyst would find insightful for this quarter

IMPORTANT: Do NOT select any capital or risk metrics (CET1, RWA, LCR, PCL, GIL, etc.) - \
these have their own dedicated section in the report.

Return EXACTLY the metric names as shown in the tables - do not modify them."""

    # Build the user prompt
    user_prompt = f"""Analyze {bank_name}'s {quarter} {fiscal_year} earnings data and make your \
selections.

## THE 7 KEY METRICS (all will be displayed - you choose ONE for the trend chart):

{key_metrics_table}

Review the QoQ, YoY, and multi-year trend columns. Select the ONE metric that has the most \
compelling trend story to visualize on an 8-quarter chart.

## REMAINING METRICS (select 5 for Additional Highlights):

{remaining_metrics_table}

From this pool, select 5 metrics that would provide valuable additional context for analysts. \
Focus on metrics with notable trends or significant analytical value.

Return exact metric names from the tables above."""

    # Define the tool for structured output
    tool_definition = {
        "type": "function",
        "function": {
            "name": "select_metrics",
            "description": "Select chart metric from key metrics and additional highlight metrics",
            "parameters": {
                "type": "object",
                "properties": {
                    "chart_metric": {
                        "type": "string",
                        "enum": available_key_metrics,
                        "description": (
                            "ONE of the 7 key metrics to feature on the 8-quarter trend chart. "
                            "Choose based on the most compelling trend story in the data."
                        ),
                    },
                    "chart_reasoning": {
                        "type": "string",
                        "description": (
                            "Explain why this metric was chosen for the chart. "
                            "Reference specific trend data (e.g., 'YoY +15.2% with consistent "
                            "growth across 2Y/3Y/5Y indicates sustained momentum')."
                        ),
                    },
                    "dynamic_metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            f"List of exactly {num_dynamic_metrics} additional metrics from the "
                            "remaining pool to highlight in slim tiles. Do NOT include any of "
                            "the 7 key metrics or capital/risk metrics."
                        ),
                        "minItems": num_dynamic_metrics,
                        "maxItems": num_dynamic_metrics,
                    },
                    "dynamic_reasoning": {
                        "type": "string",
                        "description": (
                            "Explain why these 5 metrics were selected. What insights or context "
                            "do they provide? Reference specific trends where relevant."
                        ),
                    },
                },
                "required": [
                    "chart_metric",
                    "chart_reasoning",
                    "dynamic_metrics",
                    "dynamic_reasoning",
                ],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        # Use large model for this task - need good reasoning
        model_config = config.llm.large

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
            context=context,
            llm_params={
                "model": model_config.model,
                "temperature": 0.3,  # Some creativity for dynamic selection
                "max_tokens": 2000,
            },
        )

        # Parse the tool call response
        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])

                chart_metric = function_args.get("chart_metric", "")
                chart_reasoning = function_args.get("chart_reasoning", "")
                dynamic_metrics = function_args.get("dynamic_metrics", [])
                dynamic_reasoning = function_args.get("dynamic_reasoning", "")

                # Validate chart metric is one of the 7 key metrics
                if chart_metric not in available_key_metrics:
                    logger.warning(
                        "etl.bank_earnings_report.invalid_chart_metric",
                        execution_id=execution_id,
                        chart_metric=chart_metric,
                        valid_options=available_key_metrics,
                    )
                    chart_metric = available_key_metrics[0]

                # Tile metrics are the other 6 key metrics (excluding chart metric)
                tile_metrics = [m for m in available_key_metrics if m != chart_metric]

                logger.info(
                    "etl.bank_earnings_report.metrics_selected",
                    execution_id=execution_id,
                    chart_metric=chart_metric,
                    tile_metrics=tile_metrics,
                    dynamic_metrics=dynamic_metrics,
                )

                # Build exclusion set for dynamic validation
                exclusion_set = set(KEY_METRICS) | set(EXCLUDED_METRICS)

                # Validate dynamic metrics (must not be in key metrics or excluded)
                validated_dynamic = [
                    m
                    for m in dynamic_metrics
                    if m in available_metric_names and m not in exclusion_set
                ]

                if len(validated_dynamic) < len(dynamic_metrics):
                    logger.warning(
                        "etl.bank_earnings_report.invalid_dynamic_filtered",
                        execution_id=execution_id,
                        original=dynamic_metrics,
                        validated=validated_dynamic,
                    )

                return {
                    "chart_metric": chart_metric,
                    "chart_reasoning": chart_reasoning,
                    "tile_metrics": tile_metrics[:num_tile_metrics],
                    "tile_reasoning": "Fixed key metrics (excluding chart metric)",
                    "dynamic_metrics": validated_dynamic[:num_dynamic_metrics],
                    "dynamic_reasoning": dynamic_reasoning,
                    "available_metrics": len(metrics),
                    "available_key_metrics": available_key_metrics,
                    "prompt": user_prompt,
                    "all_metrics_summary": [
                        {
                            "name": m["parameter"],
                            "value": m["actual"],
                            "units": m.get("units", ""),
                            "is_bps": m.get("is_bps", False),
                            "qoq": m["qoq"],
                            "yoy": m["yoy"],
                            "2y": m.get("2y"),
                            "3y": m.get("3y"),
                            "4y": m.get("4y"),
                            "5y": m.get("5y"),
                        }
                        for m in metrics
                    ],
                }

        # Fallback if no tool call
        logger.warning(
            "etl.bank_earnings_report.no_tool_call_response",
            execution_id=execution_id,
        )
        return _fallback_selection(
            metrics, available_key_metrics, num_tile_metrics, num_dynamic_metrics
        )

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.metric_selection_error",
            execution_id=execution_id,
            error=str(e),
        )
        return _fallback_selection(
            metrics, available_key_metrics, num_tile_metrics, num_dynamic_metrics
        )


def _fallback_selection(
    metrics: List[Dict[str, Any]],
    available_key_metrics: List[str],
    num_tile_metrics: int,
    num_dynamic_metrics: int = 5,
) -> Dict[str, Any]:
    """
    Fallback metric selection when LLM fails.

    Args:
        metrics: List of metric dicts
        available_key_metrics: List of key metric names available in data
        num_tile_metrics: Number of tile metrics (will be 6)
        num_dynamic_metrics: Number of dynamic metrics to select

    Returns:
        Selection dict with chart_metric, tile_metrics, and dynamic_metrics
    """
    # Default chart metric - first available key metric
    chart_metric = available_key_metrics[0] if available_key_metrics else "Net Income"

    # Tile metrics are the other key metrics
    tile_metrics = [m for m in available_key_metrics if m != chart_metric][:num_tile_metrics]

    # Priority metrics for dynamic (additional highlights)
    priority_dynamic = [
        "Operating Leverage",
        "Net Interest Income",
        "Non-Interest Income",
        "Non-Interest Expense",
        "Loan Growth",
        "Deposit Growth",
        "Book Value per Share",
        "Average Assets",
        "Average Loans",
        "Average Deposits",
    ]

    # Build exclusion set
    exclusion_set = set(KEY_METRICS) | set(EXCLUDED_METRICS)
    metric_names = {m["parameter"] for m in metrics}

    # Select dynamic metrics (excluding key metrics and capital/risk)
    dynamic_metrics = []
    for priority in priority_dynamic:
        if len(dynamic_metrics) >= num_dynamic_metrics:
            break
        if priority in metric_names and priority not in exclusion_set:
            dynamic_metrics.append(priority)

    # If we don't have enough, fill from remaining metrics
    if len(dynamic_metrics) < num_dynamic_metrics:
        remaining = [m for m in metric_names if m not in exclusion_set and m not in dynamic_metrics]
        for m in remaining[: num_dynamic_metrics - len(dynamic_metrics)]:
            dynamic_metrics.append(m)

    return {
        "chart_metric": chart_metric,
        "chart_reasoning": "Fallback selection - LLM unavailable",
        "tile_metrics": tile_metrics,
        "tile_reasoning": "Fixed key metrics (excluding chart metric)",
        "dynamic_metrics": dynamic_metrics,
        "dynamic_reasoning": "Fallback selection - LLM unavailable",
        "available_metrics": len(metrics),
        "available_key_metrics": available_key_metrics,
        "prompt": "",
    }


# =============================================================================
# Legacy function for backwards compatibility
# =============================================================================


async def select_top_metrics(
    metrics: List[Dict[str, Any]],
    bank_name: str,
    quarter: str,
    fiscal_year: int,
    context: Dict[str, Any],
    num_metrics: int = 6,
) -> Dict[str, Any]:
    """
    Legacy function - now wraps select_chart_and_tile_metrics.

    Returns only the tile_metrics for backward compatibility.
    """
    result = await select_chart_and_tile_metrics(
        metrics=metrics,
        bank_name=bank_name,
        quarter=quarter,
        fiscal_year=fiscal_year,
        context=context,
        num_tile_metrics=num_metrics,
    )

    # Return in legacy format
    return {
        "selected_metrics": result.get("tile_metrics", []),
        "reasoning": result.get("tile_reasoning", ""),
        "available_metrics": result.get("available_metrics", 0),
        "prompt": result.get("prompt", ""),
        "all_metrics_summary": result.get("all_metrics_summary", []),
    }
