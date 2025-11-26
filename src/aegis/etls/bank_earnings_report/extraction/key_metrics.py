"""
LLM-based extraction for key metrics selection.

Selects:
1. One chart metric (chosen based on YoY/QoQ and 5-year trends for impactful visualization)
2. Six tile metrics (core metrics excluding the chart metric) - Total: 7 key metrics
3. Five dynamic metrics from REMAINING pool (complementary/noteworthy metrics)
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
    num_dynamic_metrics: int = 5,
) -> Dict[str, Any]:
    """
    Use LLM to select metrics for chart, tiles, and dynamic slim tiles in a single call.

    Selection order:
    1. First, select 1 chart metric based on trend analysis (YoY/QoQ and 5-year trends)
    2. Then, select 6 tile metrics (excluding the chart metric) - Total: 7 key metrics
    3. Finally, select 5 dynamic metrics from REMAINING metrics (not in the 7)

    Args:
        metrics: List of metric dicts from retrieve_all_metrics()
        bank_name: Name of the bank for context
        quarter: Quarter (e.g., "Q3")
        fiscal_year: Fiscal year (e.g., 2024)
        context: Execution context with auth_config, ssl_config
        num_tile_metrics: Number of tile metrics to select (default 6)
        num_dynamic_metrics: Number of dynamic slim tile metrics to select (default 5)

    Returns:
        Dict with:
            - chart_metric: The metric selected for the 8Q trend chart (based on trends)
            - tile_metrics: List of 6 metrics for the key metrics tiles
            - dynamic_metrics: List of 5 metrics for the slim tiles row
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

Your task has THREE parts:

## Part 1: Chart Metric Selection (1 metric)
Select ONE metric for an 8-quarter trend chart based on trend analysis.
Analyze the QoQ, YoY, and multi-year trends (2Y, 3Y, 5Y) to identify the metric with:
- The most compelling trend story (strong growth, notable recovery, significant shift)
- Meaningful progression that investors should track
- A visual that highlights a key performance narrative

The chart will show 8 quarters of data - pick the metric whose trend tells the best story.

## Part 2: Tile Metrics Selection (6 metrics)
Select SIX metrics for the key metrics tiles display. These should:
- Include core earnings metrics (revenue, net income, EPS, ROE)
- Highlight metrics with notable QoQ or YoY changes
- Provide a balanced view of the quarter's performance
- NOT include the chart metric you already selected

Together with the chart metric, these 7 metrics form the "Key Metrics" section.

## Part 3: Dynamic Metrics Selection (5 metrics)
Select FIVE additional metrics from the REMAINING pool (not from your 7 key metrics above).
These "Additional Highlights" should:
- Complement the key metrics with deeper context
- Highlight noteworthy items that didn't make the top 7
- Include metrics with significant QoQ/YoY changes worth calling out
- Show balance sheet health, growth indicators, or operational metrics

IMPORTANT: Avoid capital/risk metrics (CET1, RWA, LCR, PCL, GIL) - those have their \
own section.

Be dynamic in your selections - don't just pick the same metrics every time. Let the actual \
data guide you to what's most noteworthy this quarter.

Return EXACTLY the metric names as shown in the tables - do not modify them."""

    # Build the user prompt
    user_prompt = f"""Select metrics for {bank_name}'s {quarter} {fiscal_year} earnings report.

## STEP 1: Choose ONE chart metric from these options (analyze trends):

{chartable_table}

Analyze the QoQ, YoY columns and pick the metric with the most compelling trend story \
for an 8-quarter chart.

## STEP 2: Choose SIX tile metrics from the full list (excluding your chart choice):

{all_metrics_table}

Select 6 core metrics that best summarize this quarter's performance.
These 6 + your chart metric = 7 Key Metrics.

## STEP 3: Choose FIVE additional dynamic metrics from the REMAINING metrics:

From the metrics NOT selected in Steps 1-2, pick 5 additional noteworthy metrics \
to highlight as "Additional Highlights" in slim tiles.

Return exact metric names from the tables."""

    # Define the tool for structured output
    tool_definition = {
        "type": "function",
        "function": {
            "name": "select_metrics",
            "description": "Select chart, tile, and dynamic metrics for the earnings report",
            "parameters": {
                "type": "object",
                "properties": {
                    "chart_metric": {
                        "type": "string",
                        "description": (
                            "The ONE metric selected for the 8-quarter trend chart. "
                            "Chosen based on compelling QoQ/YoY/multi-year trends."
                        ),
                    },
                    "chart_reasoning": {
                        "type": "string",
                        "description": (
                            "Why this metric was chosen for the chart - what trend story "
                            "does the data show? Reference specific QoQ/YoY/5Y numbers."
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
                    "dynamic_metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            f"List of {num_dynamic_metrics} additional metrics from REMAINING "
                            "pool (not chart or tile metrics) for the slim tiles row."
                        ),
                        "minItems": num_dynamic_metrics,
                        "maxItems": num_dynamic_metrics,
                    },
                    "dynamic_reasoning": {
                        "type": "string",
                        "description": (
                            "Why these 5 additional metrics were selected as highlights - "
                            "what noteworthy context do they add?"
                        ),
                    },
                },
                "required": [
                    "chart_metric",
                    "chart_reasoning",
                    "tile_metrics",
                    "tile_reasoning",
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
                tile_metrics = function_args.get("tile_metrics", [])
                tile_reasoning = function_args.get("tile_reasoning", "")
                dynamic_metrics = function_args.get("dynamic_metrics", [])
                dynamic_reasoning = function_args.get("dynamic_reasoning", "")

                logger.info(
                    "etl.bank_earnings_report.metrics_selected",
                    execution_id=execution_id,
                    chart_metric=chart_metric,
                    tile_metrics=tile_metrics,
                    dynamic_metrics=dynamic_metrics,
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

                # Build set of key metrics (chart + tiles) for exclusion
                key_metrics_set = {chart_metric} | set(validated_tiles)

                # Validate dynamic metrics (must not be in key metrics)
                validated_dynamic = [
                    m
                    for m in dynamic_metrics
                    if m in available_metric_names and m not in key_metrics_set
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
                    "tile_metrics": validated_tiles[:num_tile_metrics],
                    "tile_reasoning": tile_reasoning,
                    "dynamic_metrics": validated_dynamic[:num_dynamic_metrics],
                    "dynamic_reasoning": dynamic_reasoning,
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
        return _fallback_selection(
            metrics, available_chartable, num_tile_metrics, num_dynamic_metrics
        )

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.metric_selection_error",
            execution_id=execution_id,
            error=str(e),
        )
        return _fallback_selection(
            metrics, available_chartable, num_tile_metrics, num_dynamic_metrics
        )


def _fallback_selection(
    metrics: List[Dict[str, Any]],
    available_chartable: List[str],
    num_tile_metrics: int,
    num_dynamic_metrics: int = 5,
) -> Dict[str, Any]:
    """
    Fallback metric selection when LLM fails.

    Args:
        metrics: List of metric dicts
        available_chartable: List of chartable metric names available in data
        num_tile_metrics: Number of tile metrics to select
        num_dynamic_metrics: Number of dynamic metrics to select

    Returns:
        Selection dict with chart_metric, tile_metrics, and dynamic_metrics
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

    # Priority metrics for dynamic (additional highlights)
    priority_dynamic = [
        "Operating Leverage",
        "Loan Growth",
        "Deposit Growth",
        "Non-Interest Expense",
        "Book Value per Share",
        "Average Assets",
        "Average Loans",
        "Average Deposits",
    ]

    # Select tiles (excluding chart metric)
    tile_metrics = []
    metric_names = {m["parameter"] for m in metrics}

    for priority in priority_tiles:
        if len(tile_metrics) >= num_tile_metrics:
            break
        if priority in metric_names and priority != chart_metric:
            tile_metrics.append(priority)

    # Build key metrics set for exclusion from dynamic
    key_metrics_set = {chart_metric} | set(tile_metrics)

    # Select dynamic metrics (excluding key metrics)
    dynamic_metrics = []
    for priority in priority_dynamic:
        if len(dynamic_metrics) >= num_dynamic_metrics:
            break
        if priority in metric_names and priority not in key_metrics_set:
            dynamic_metrics.append(priority)

    # If we don't have enough, fill from remaining metrics
    if len(dynamic_metrics) < num_dynamic_metrics:
        remaining = [
            m for m in metric_names if m not in key_metrics_set and m not in dynamic_metrics
        ]
        for m in remaining[: num_dynamic_metrics - len(dynamic_metrics)]:
            dynamic_metrics.append(m)

    return {
        "chart_metric": chart_metric,
        "chart_reasoning": "Fallback selection - LLM unavailable",
        "tile_metrics": tile_metrics,
        "tile_reasoning": "Fallback selection - LLM unavailable",
        "dynamic_metrics": dynamic_metrics,
        "dynamic_reasoning": "Fallback selection - LLM unavailable",
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
