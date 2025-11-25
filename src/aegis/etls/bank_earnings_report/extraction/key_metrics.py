"""
LLM-based extraction for key metrics selection.

Selects:
1. One chart metric from a curated list (for 8-quarter trend visualization)
2. Six tile metrics (excluding the chart metric) for key metrics display
"""

import json
from typing import Any, Dict, List

from aegis.connections.llm_connector import complete_with_tools
from aegis.utils.logging import get_logger
from aegis.utils.settings import config


# =============================================================================
# Chartable Metrics - Pre-approved for 8-quarter trend visualization
# =============================================================================

# These metrics are appropriate for quarterly trend charts because they:
# 1. Are consistently reported quarter-to-quarter
# 2. Show meaningful trends over time
# 3. Are key metrics analysts track for bank performance
CHARTABLE_METRICS = [
    "Net Income",
    "Total Revenue",
    "Diluted EPS",
    "Core Cash Diluted EPS",
    "Return on Equity",
    "Net Interest Margin",
    "Efficiency Ratio",
    "Pre-Provision Earnings",
    "Non-Interest Income",
    "Net Interest Income",
    "Operating Leverage",
    "Loan Growth",
    "Deposit Growth",
]


def format_metrics_for_llm(metrics: List[Dict[str, Any]]) -> str:
    """
    Format metrics list into a compact table for LLM analysis.

    Only includes metric name, value, and change data - no descriptions.
    LLM uses its own financial knowledge to evaluate importance.

    Args:
        metrics: List of metric dicts from retrieve_all_metrics()

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
            return f"${m['actual']:,.0f}M"
        else:
            return f"{m['actual']:,.2f}"

    # Build a compact table
    lines = [
        "| Metric | Value | QoQ | YoY | 2Y | 3Y | 5Y |",
        "|--------|-------|-----|-----|----|----|----| ",
    ]

    for m in metrics:
        name = m["parameter"]
        val = fmt_val(m)
        qoq = fmt_pct(m["qoq"])
        yoy = fmt_pct(m["yoy"])
        y2 = fmt_pct(m.get("2y"))
        y3 = fmt_pct(m.get("3y"))
        y5 = fmt_pct(m.get("5y"))
        lines.append(f"| {name} | {val} | {qoq} | {yoy} | {y2} | {y3} | {y5} |")

    return "\n".join(lines)


def format_chartable_metrics_for_llm(
    metrics: List[Dict[str, Any]], chartable_list: List[str]
) -> str:
    """
    Format only the chartable metrics into a table for LLM chart selection.

    Args:
        metrics: List of metric dicts from retrieve_all_metrics()
        chartable_list: List of metric names eligible for charting

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
            return f"${m['actual']:,.0f}M"
        else:
            return f"{m['actual']:,.2f}"

    # Filter to only chartable metrics that exist in the data
    chartable_metrics = [m for m in metrics if m["parameter"] in chartable_list]

    lines = [
        "| Metric | Value | QoQ | YoY |",
        "|--------|-------|-----|-----|",
    ]

    for m in chartable_metrics:
        name = m["parameter"]
        val = fmt_val(m)
        qoq = fmt_pct(m["qoq"])
        yoy = fmt_pct(m["yoy"])
        lines.append(f"| {name} | {val} | {qoq} | {yoy} |")

    return "\n".join(lines)


async def select_chart_and_tile_metrics(
    metrics: List[Dict[str, Any]],
    bank_name: str,
    quarter: str,
    fiscal_year: int,
    context: Dict[str, Any],
    num_tile_metrics: int = 6,
) -> Dict[str, Any]:
    """
    Use LLM to select metrics for chart and tiles in a single call.

    Selection order:
    1. First, select 1 chart metric from the chartable metrics list
    2. Then, select 6 tile metrics (excluding the chart metric)

    Args:
        metrics: List of metric dicts from retrieve_all_metrics()
        bank_name: Name of the bank for context
        quarter: Quarter (e.g., "Q3")
        fiscal_year: Fiscal year (e.g., 2024)
        context: Execution context with auth_config, ssl_config
        num_tile_metrics: Number of tile metrics to select (default 6)

    Returns:
        Dict with:
            - chart_metric: The metric selected for the 8Q trend chart
            - tile_metrics: List of 6 metrics for the key metrics tiles
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
            "reasoning": "No metrics available for selection",
            "available_metrics": 0,
            "prompt": "",
        }

    # Find which chartable metrics exist in the data
    available_metric_names = {m["parameter"] for m in metrics}
    available_chartable = [m for m in CHARTABLE_METRICS if m in available_metric_names]

    if not available_chartable:
        logger.warning(
            "etl.bank_earnings_report.no_chartable_metrics",
            execution_id=execution_id,
        )
        available_chartable = list(available_metric_names)[:5]  # Fallback

    # Format tables for LLM
    chartable_table = format_chartable_metrics_for_llm(metrics, available_chartable)
    all_metrics_table = format_metrics_for_llm(metrics)

    # Build the system prompt
    system_prompt = """You are a senior financial analyst selecting metrics for a bank earnings \
report.

Your task has TWO parts:

## Part 1: Chart Metric Selection
Select ONE metric for an 8-quarter trend chart. This chart will show the metric's progression \
over the past 2 years.
Choose a metric that:
- Shows an interesting trend or story (growth, decline, recovery, volatility)
- Is meaningful to track over time for investors
- Will make an impactful visual

## Part 2: Tile Metrics Selection
Select SIX metrics for the key metrics tiles display. These should:
- Include core earnings metrics (revenue, net income, EPS, ROE)
- Highlight metrics with notable QoQ or YoY changes
- Provide a balanced view of the quarter's performance
- NOT duplicate the chart metric you already selected

IMPORTANT: Avoid capital/risk metrics (CET1, RWA, LCR, PCL, GIL) - those have their \
own section.

Be dynamic in your selections - don't just pick the same metrics every time. Let the actual \
data guide you to what's most noteworthy this quarter.

Return EXACTLY the metric names as shown in the tables - do not modify them."""

    # Build the user prompt
    user_prompt = f"""Select metrics for {bank_name}'s {quarter} {fiscal_year} earnings report.

## STEP 1: Choose ONE chart metric from these options:

{chartable_table}

Pick the metric that would make the most compelling 8-quarter trend visualization.

## STEP 2: Choose SIX tile metrics from the full list (excluding your chart choice):

{all_metrics_table}

Select 6 metrics that best summarize this quarter's performance.

Return exact metric names from the tables."""

    # Define the tool for structured output
    tool_definition = {
        "type": "function",
        "function": {
            "name": "select_metrics",
            "description": "Select chart metric and tile metrics for the earnings report",
            "parameters": {
                "type": "object",
                "properties": {
                    "chart_metric": {
                        "type": "string",
                        "description": (
                            "The ONE metric selected for the 8-quarter trend chart. "
                            "Must be from the chartable metrics list."
                        ),
                    },
                    "chart_reasoning": {
                        "type": "string",
                        "description": (
                            "Why this metric was chosen for the chart - what trend or story "
                            "will it show over 8 quarters?"
                        ),
                    },
                    "tile_metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "List of 6 metrics for the key metrics tiles. "
                            "Must NOT include the chart metric."
                        ),
                        "minItems": num_tile_metrics,
                        "maxItems": num_tile_metrics,
                    },
                    "tile_reasoning": {
                        "type": "string",
                        "description": (
                            "Why these 6 metrics were selected - what story do they tell "
                            "about this quarter's performance?"
                        ),
                    },
                },
                "required": [
                    "chart_metric",
                    "chart_reasoning",
                    "tile_metrics",
                    "tile_reasoning",
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
                tile_metrics = function_args.get("tile_metrics", [])
                tile_reasoning = function_args.get("tile_reasoning", "")

                logger.info(
                    "etl.bank_earnings_report.metrics_selected",
                    execution_id=execution_id,
                    chart_metric=chart_metric,
                    tile_metrics=tile_metrics,
                )

                # Validate chart metric
                if chart_metric not in available_metric_names:
                    logger.warning(
                        "etl.bank_earnings_report.invalid_chart_metric",
                        execution_id=execution_id,
                        chart_metric=chart_metric,
                    )
                    chart_metric = available_chartable[0] if available_chartable else None

                # Validate tile metrics
                validated_tiles = [m for m in tile_metrics if m in available_metric_names]
                # Remove chart metric from tiles if accidentally included
                validated_tiles = [m for m in validated_tiles if m != chart_metric]

                if len(validated_tiles) < len(tile_metrics):
                    logger.warning(
                        "etl.bank_earnings_report.invalid_tiles_filtered",
                        execution_id=execution_id,
                        original=tile_metrics,
                        validated=validated_tiles,
                    )

                return {
                    "chart_metric": chart_metric,
                    "chart_reasoning": chart_reasoning,
                    "tile_metrics": validated_tiles[:num_tile_metrics],
                    "tile_reasoning": tile_reasoning,
                    "available_metrics": len(metrics),
                    "available_chartable": available_chartable,
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
        return _fallback_selection(metrics, available_chartable, num_tile_metrics)

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.metric_selection_error",
            execution_id=execution_id,
            error=str(e),
        )
        return _fallback_selection(metrics, available_chartable, num_tile_metrics)


def _fallback_selection(
    metrics: List[Dict[str, Any]],
    available_chartable: List[str],
    num_tile_metrics: int,
) -> Dict[str, Any]:
    """
    Fallback metric selection when LLM fails.

    Args:
        metrics: List of metric dicts
        available_chartable: List of chartable metric names available in data
        num_tile_metrics: Number of tile metrics to select

    Returns:
        Selection dict with chart_metric and tile_metrics
    """
    # Default chart metric
    chart_metric = available_chartable[0] if available_chartable else "Net Income"

    # Priority metrics for tiles
    priority_tiles = [
        "Total Revenue",
        "Net Income",
        "Diluted EPS",
        "Return on Equity",
        "Efficiency Ratio",
        "Non-Interest Income",
        "Net Interest Income",
        "Pre-Provision Earnings",
    ]

    # Select tiles (excluding chart metric)
    tile_metrics = []
    metric_names = {m["parameter"] for m in metrics}

    for priority in priority_tiles:
        if len(tile_metrics) >= num_tile_metrics:
            break
        if priority in metric_names and priority != chart_metric:
            tile_metrics.append(priority)

    return {
        "chart_metric": chart_metric,
        "chart_reasoning": "Fallback selection - LLM unavailable",
        "tile_metrics": tile_metrics,
        "tile_reasoning": "Fallback selection - LLM unavailable",
        "available_metrics": len(metrics),
        "available_chartable": available_chartable,
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
