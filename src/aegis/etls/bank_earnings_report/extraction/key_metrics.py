"""
LLM-based extraction for key metrics selection.

Process:
1. FIXED 6 Key Metrics - Selected from 7 candidates, displayed in main tiles
2. DYNAMIC 5 Additional Metrics - Selected from remaining pool for slim tiles
3. CHART Metric - Selected from the 11 visible metrics (6 tiles + 5 slim)
   - Must be one of the already-displayed metrics so user can always see current value
"""

import json
from typing import Any, Dict, List

from aegis.connections.llm_connector import complete_with_tools
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.utils.logging import get_logger
from aegis.utils.prompt_loader import load_prompt_from_db


KEY_METRICS = [
    "Core Cash Diluted EPS",
    "Return on Equity",
    "NIM (AIEA)",
    "Efficiency Ratio",
    "Total Revenue",
    "Pre Provision Profit",
    "Provisions for Credit Losses",
]


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
    "GIL",
    "Gross Impaired Loans",
    "Net Impaired Loans",
    "ACL",
    "Allowance for Credit Losses",
]


def compute_trend_score(metric: Dict[str, Any]) -> float:
    """
    Compute a trend score indicating how suitable a metric is for charting.

    Higher scores indicate more interesting/varied trends that would make
    a compelling chart. Low scores indicate flat/stable metrics.

    The score considers:
    - Magnitude of QoQ, YoY, and multi-year changes
    - Consistency of direction (sustained trends score higher)
    - Recent momentum (QoQ weighted more than 5Y)

    Args:
        metric: Metric dict with qoq, yoy, 2y, 3y, 5y fields

    Returns:
        Float score (higher = better for charting)
    """
    qoq = abs(metric.get("qoq") or 0)
    yoy = abs(metric.get("yoy") or 0)
    y2 = abs(metric.get("2y") or 0)
    y3 = abs(metric.get("3y") or 0)
    y5 = abs(metric.get("5y") or 0)

    score = (qoq * 3.0) + (yoy * 2.5) + (y2 * 1.5) + (y3 * 1.0) + (y5 * 0.5)

    changes = [metric.get("qoq"), metric.get("yoy"), metric.get("2y")]
    non_null = [c for c in changes if c is not None]
    if len(non_null) >= 2:
        all_positive = all(c > 0 for c in non_null)
        all_negative = all(c < 0 for c in non_null)
        if all_positive or all_negative:
            score *= 1.3

    return round(score, 1)


def format_key_metrics_for_llm(metrics: List[Dict[str, Any]], key_metric_names: List[str]) -> str:
    """
    Format the 7 fixed key metrics into a table for LLM chart selection.

    Uses proper bps/% formatting so LLM understands the data correctly.
    Includes a trend score to help identify metrics suitable for charting.

    Args:
        metrics: List of metric dicts from retrieve_all_metrics()
        key_metric_names: List of the 7 fixed key metric names

    Returns:
        Formatted table string for LLM prompt
    """
    from aegis.etls.bank_earnings_report.retrieval.supplementary import (
        format_value_for_llm,
        format_delta_for_llm,
    )

    key_metrics = [m for m in metrics if m["parameter"] in key_metric_names]

    lines = [
        "| Metric | Value | QoQ | YoY | 2Y | Trend Score | Chart Suitable? |",
        "|--------|-------|-----|-----|----| -----------|-----------------|",
    ]

    for m in key_metrics:
        name = m["parameter"]
        units = m.get("units", "")
        is_bps = m.get("is_bps", False)

        val = format_value_for_llm(m)
        qoq = format_delta_for_llm(m.get("qoq"), units, is_bps)
        yoy = format_delta_for_llm(m.get("yoy"), units, is_bps)
        y2 = format_delta_for_llm(m.get("2y"), units, is_bps)

        trend_score = compute_trend_score(m)

        if trend_score >= 15:
            suitability = "EXCELLENT"
        elif trend_score >= 8:
            suitability = "Good"
        elif trend_score >= 4:
            suitability = "Fair"
        else:
            suitability = "Poor (flat)"

        lines.append(f"| {name} | {val} | {qoq} | {yoy} | {y2} | {trend_score} | {suitability} |")

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
    from aegis.etls.bank_earnings_report.retrieval.supplementary import (
        format_value_for_llm,
        format_delta_for_llm,
    )

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
    Use LLM to select tile metrics, dynamic metrics, and chart metric for the earnings report.

    Process:
    1. LLM selects 6 metrics from the 7 KEY_METRICS candidates for main tiles
    2. LLM selects 5 additional metrics from remaining pool for slim tiles
    3. LLM selects 1 chart metric from the 11 visible metrics (6 tiles + 5 slim)

    The chart metric MUST be one of the displayed metrics so users can always
    see the current value when they navigate away from a chart metric.

    Args:
        metrics: List of metric dicts from retrieve_all_metrics()
        bank_name: Name of the bank for context
        quarter: Quarter (e.g., "Q3")
        fiscal_year: Fiscal year (e.g., 2024)
        context: Execution context with auth_config, ssl_config
        num_tile_metrics: Number of tile metrics from key metrics (default 6)
        num_dynamic_metrics: Number of dynamic slim tile metrics to select (default 5)

    Returns:
        Dict with:
            - chart_metric: The metric selected for the 8Q trend chart (from 11 visible)
            - tile_metrics: 6 key metrics for main tiles
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

    available_metric_names = {m["parameter"] for m in metrics}
    available_key_metrics = [m for m in KEY_METRICS if m in available_metric_names]

    if not available_key_metrics:
        logger.error(
            "etl.bank_earnings_report.no_key_metrics_available",
            execution_id=execution_id,
        )
        raise ValueError("No key metrics available in data")

    exclusion_list = KEY_METRICS + EXCLUDED_METRICS

    key_metrics_table = format_key_metrics_for_llm(metrics, available_key_metrics)
    remaining_metrics_table = format_remaining_metrics_for_llm(metrics, exclusion_list)

    # Load prompt from database
    prompt_data = load_prompt_from_db(
        layer="bank_earnings_report_etl",
        name="supplementary_1_keymetrics_selection",
        compose_with_globals=False,
        execution_id=execution_id,
    )

    # Format user prompt with dynamic content
    user_prompt = prompt_data["user_prompt"].format(
        bank_name=bank_name,
        quarter=quarter,
        fiscal_year=fiscal_year,
        key_metrics_table=key_metrics_table,
        remaining_metrics_table=remaining_metrics_table,
    )

    # Build tool definition with dynamic constraints
    tool_def = prompt_data["tool_definition"]
    # Update the tile_metrics enum with available key metrics
    tool_def["function"]["parameters"]["properties"]["tile_metrics"]["items"][
        "enum"
    ] = available_key_metrics

    messages = [
        {"role": "system", "content": prompt_data["system_prompt"]},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("supplementary_1_keymetrics_selection")

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

                tile_metrics = function_args.get("tile_metrics", [])
                tile_reasoning = function_args.get("tile_reasoning", "")
                dynamic_metrics = function_args.get("dynamic_metrics", [])
                dynamic_reasoning = function_args.get("dynamic_reasoning", "")
                chart_metric = function_args.get("chart_metric", "")
                chart_reasoning = function_args.get("chart_reasoning", "")

                validated_tiles = [m for m in tile_metrics if m in available_key_metrics]
                if len(validated_tiles) < num_tile_metrics:
                    for km in available_key_metrics:
                        if km not in validated_tiles and len(validated_tiles) < num_tile_metrics:
                            validated_tiles.append(km)

                exclusion_set = set(KEY_METRICS) | set(EXCLUDED_METRICS)

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

                all_visible_metrics = set(validated_tiles) | set(validated_dynamic)
                if chart_metric not in all_visible_metrics:
                    logger.warning(
                        "etl.bank_earnings_report.invalid_chart_metric",
                        execution_id=execution_id,
                        chart_metric=chart_metric,
                        valid_options=list(all_visible_metrics),
                    )
                    chart_metric = validated_tiles[0] if validated_tiles else ""

                logger.info(
                    "etl.bank_earnings_report.metrics_selected",
                    execution_id=execution_id,
                    chart_metric=chart_metric,
                    tile_metrics=validated_tiles,
                    dynamic_metrics=validated_dynamic,
                )

                return {
                    "chart_metric": chart_metric,
                    "chart_reasoning": chart_reasoning,
                    "tile_metrics": validated_tiles[:num_tile_metrics],
                    "tile_reasoning": tile_reasoning,
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

        logger.error(
            "etl.bank_earnings_report.no_tool_call_response",
            execution_id=execution_id,
        )
        raise RuntimeError("LLM did not return a tool call response for metric selection")

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.metric_selection_error",
            execution_id=execution_id,
            error=str(e),
        )
        raise


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

    return {
        "selected_metrics": result.get("tile_metrics", []),
        "reasoning": result.get("tile_reasoning", ""),
        "available_metrics": result.get("available_metrics", 0),
        "prompt": result.get("prompt", ""),
        "all_metrics_summary": result.get("all_metrics_summary", []),
    }
