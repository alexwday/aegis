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
    Format metrics list into a table string for LLM analysis.

    Args:
        metrics: List of metric dicts from retrieve_all_metrics()

    Returns:
        Formatted table string for LLM prompt
    """
    lines = [
        "| KPI Name | Value | QoQ Change | YoY Change | Description | Analyst Usage |",
        "|----------|-------|------------|------------|-------------|---------------|",
    ]

    for m in metrics:
        qoq_str = f"{m['qoq']:.1f}%" if m['qoq'] is not None else "N/A"
        yoy_str = f"{m['yoy']:.1f}%" if m['yoy'] is not None else "N/A"
        actual_str = f"{m['actual']:.2f}" if m['actual'] is not None else "N/A"
        desc = (m['description'][:50] + "...") if len(m['description']) > 50 else m['description']
        usage = (m['analyst_usage'][:50] + "...") if len(m['analyst_usage']) > 50 else m['analyst_usage']

        lines.append(f"| {m['parameter']} | {actual_str} | {qoq_str} | {yoy_str} | {desc} | {usage} |")

    return "\n".join(lines)


async def select_top_metrics(
    metrics: List[Dict[str, Any]],
    bank_name: str,
    quarter: str,
    fiscal_year: int,
    context: Dict[str, Any],
    num_metrics: int = 6,
) -> List[str]:
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
        List of parameter names (kpi_name) for the selected metrics
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if not metrics:
        logger.warning(
            "etl.bank_earnings_report.no_metrics_for_selection",
            execution_id=execution_id,
        )
        return []

    # Format metrics table for LLM
    metrics_table = format_metrics_for_llm(metrics)

    # Build the system prompt
    system_prompt = """You are a financial analyst expert specializing in bank earnings reports.

Your task is to select the most important key performance indicators (KPIs) for a quarterly earnings report summary tile display.

Selection criteria (in order of importance):
1. **Core Financial Metrics**: Prioritize fundamental metrics like Revenue, Net Income, EPS, ROE that are always important
2. **Significant Changes**: Metrics with notable QoQ or YoY changes (>5% change) deserve attention as they indicate meaningful shifts
3. **Analyst Focus**: Metrics that analysts commonly track and discuss
4. **Balanced Coverage**: Ensure a mix of profitability, efficiency, and capital metrics

You must return EXACTLY the KPI names as they appear in the table - do not modify or paraphrase them."""

    # Build the user prompt
    user_prompt = f"""Select the top {num_metrics} most important KPIs for {bank_name}'s {quarter} {fiscal_year} earnings report.

Available metrics:

{metrics_table}

Return the {num_metrics} most important KPI names that should be featured in the key metrics tiles section of the earnings report."""

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
                        "description": "List of KPI names exactly as they appear in the metrics table",
                        "minItems": num_metrics,
                        "maxItems": num_metrics,
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Brief explanation of why these metrics were selected",
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
        # Use medium model for this task
        model_config = config.llm.medium

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
            context=context,
            llm_params={
                "model": model_config.model,
                "temperature": 0.1,  # Low temperature for consistent selection
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

                return validated

        # Fallback if no tool call
        logger.warning(
            "etl.bank_earnings_report.no_tool_call_response",
            execution_id=execution_id,
        )
        return _fallback_metric_selection(metrics, num_metrics)

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.metric_selection_error",
            execution_id=execution_id,
            error=str(e),
        )
        return _fallback_metric_selection(metrics, num_metrics)


def _fallback_metric_selection(
    metrics: List[Dict[str, Any]], num_metrics: int
) -> List[str]:
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
