"""
LLM-based extraction for key metrics selection.

Uses LLM to analyze available metrics and select the top 6 most important
based on reporting significance and magnitude of change.
"""

import json
from typing import Any, Dict, List

from aegis.connections.llm_connector import complete_with_tools
from aegis.utils.logging import get_logger
from aegis.utils.settings import config


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


async def select_top_metrics(
    metrics: List[Dict[str, Any]],
    bank_name: str,
    quarter: str,
    fiscal_year: int,
    context: Dict[str, Any],
    num_metrics: int = 6,
) -> Dict[str, Any]:
    """
    Use LLM to select the top N most important metrics.

    The LLM considers:
    1. Overall importance for bank earnings reporting
    2. Magnitude of QoQ and YoY changes (significant changes are noteworthy)
    3. Analyst usage and relevance

    Args:
        metrics: List of metric dicts from retrieve_all_metrics()
        bank_name: Name of the bank for context
        quarter: Quarter (e.g., "Q3")
        fiscal_year: Fiscal year (e.g., 2024)
        context: Execution context with auth_config, ssl_config
        num_metrics: Number of metrics to select (default 6)

    Returns:
        Dict with:
            - selected_metrics: List of parameter names (kpi_name)
            - reasoning: LLM's explanation for selection
            - available_metrics: Count of metrics available
            - prompt: The prompt sent to LLM
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if not metrics:
        logger.warning(
            "etl.bank_earnings_report.no_metrics_for_selection",
            execution_id=execution_id,
        )
        return {
            "selected_metrics": [],
            "reasoning": "No metrics available for selection",
            "available_metrics": 0,
            "prompt": "",
        }

    # Format metrics table for LLM
    metrics_table = format_metrics_for_llm(metrics)

    # Build the system prompt
    system_prompt = """You are a senior financial analyst. Select the most important KPIs for a bank's quarterly earnings summary.

Use your financial expertise to identify:
1. Core metrics that always matter (revenue, earnings, EPS, ROE, capital ratios)
2. Metrics with notable changes that tell this quarter's story
3. A balanced view across profitability, efficiency, and risk

Return EXACTLY the metric names as shown in the table - do not modify them."""

    # Build the user prompt
    user_prompt = f"""Select {num_metrics} key metrics for {bank_name}'s {quarter} {fiscal_year} earnings report summary.

{metrics_table}

Choose {num_metrics} metrics that best summarize this quarter's performance. Return the exact metric names from the table."""

    # Define the tool for structured output
    tool_definition = {
        "type": "function",
        "function": {
            "name": "select_key_metrics",
            "description": f"Select the top {num_metrics} most important KPIs for the earnings report",
            "parameters": {
                "type": "object",
                "properties": {
                    "selected_metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of KPI names exactly as they appear in the metrics data",
                        "minItems": num_metrics,
                        "maxItems": num_metrics,
                    },
                    "reasoning": {
                        "type": "string",
                        "description": (
                            "Detailed explanation of the selection rationale. For each selected metric, "
                            "explain WHY it was chosen (e.g., 'Net Income selected as core metric showing "
                            "+8% YoY growth indicating strong quarter'). Also note any metrics that were "
                            "considered but not selected and why."
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
        # Use large model for this task - need good reasoning for metric selection
        model_config = config.llm.large

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
            context=context,
            llm_params={
                "model": model_config.model,
                "temperature": 0.2,  # Slightly higher for nuanced selection
                "max_tokens": 2000,  # Allow room for detailed reasoning
            },
        )

        # Parse the tool call response
        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])

                selected = function_args.get("selected_metrics", [])
                reasoning = function_args.get("reasoning", "")

                logger.info(
                    "etl.bank_earnings_report.metrics_selected",
                    execution_id=execution_id,
                    selected_metrics=selected,
                    reasoning=reasoning[:100],
                )

                # Validate that selected metrics exist in original list
                valid_names = {m["parameter"] for m in metrics}
                validated = [m for m in selected if m in valid_names]

                if len(validated) < len(selected):
                    logger.warning(
                        "etl.bank_earnings_report.invalid_metrics_filtered",
                        execution_id=execution_id,
                        original=selected,
                        validated=validated,
                    )

                return {
                    "selected_metrics": validated,
                    "reasoning": reasoning,
                    "available_metrics": len(metrics),
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
                            "higher_is_better": m.get("higher_is_better"),
                        }
                        for m in metrics
                    ],
                }

        # Fallback if no tool call
        logger.warning(
            "etl.bank_earnings_report.no_tool_call_response",
            execution_id=execution_id,
        )
        fallback = _fallback_metric_selection(metrics, num_metrics)
        return {
            "selected_metrics": fallback,
            "reasoning": "Fallback selection used - no LLM tool call response",
            "available_metrics": len(metrics),
            "prompt": user_prompt,
        }

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.metric_selection_error",
            execution_id=execution_id,
            error=str(e),
        )
        fallback = _fallback_metric_selection(metrics, num_metrics)
        return {
            "selected_metrics": fallback,
            "reasoning": f"Fallback selection used - error: {str(e)}",
            "available_metrics": len(metrics),
            "prompt": user_prompt,
        }


def _fallback_metric_selection(metrics: List[Dict[str, Any]], num_metrics: int) -> List[str]:
    """
    Fallback metric selection when LLM fails.

    Selects metrics based on:
    1. Predefined important metric names
    2. Largest absolute YoY changes

    Args:
        metrics: List of metric dicts
        num_metrics: Number to select

    Returns:
        List of parameter names
    """
    # Priority metrics that should always be included if available
    priority_metrics = [
        "Total Revenue",
        "Net Income",
        "Diluted EPS",
        "Return on Equity",
        "Efficiency Ratio",
        "CET1 Ratio",
        "Pre-Provision Earnings",
        "Operating Leverage",
    ]

    selected = []
    remaining_metrics = list(metrics)

    # First, add priority metrics that exist
    for priority in priority_metrics:
        if len(selected) >= num_metrics:
            break
        for m in remaining_metrics:
            if m["parameter"] == priority:
                selected.append(m["parameter"])
                remaining_metrics.remove(m)
                break

    # Fill remaining slots with metrics that have largest YoY changes
    if len(selected) < num_metrics:
        remaining_metrics.sort(
            key=lambda m: abs(m["yoy"]) if m["yoy"] is not None else 0,
            reverse=True,
        )
        for m in remaining_metrics:
            if len(selected) >= num_metrics:
                break
            if m["parameter"] not in selected:
                selected.append(m["parameter"])

    return selected[:num_metrics]
