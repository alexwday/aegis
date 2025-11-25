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
    Format metrics list into a structured string for LLM analysis.

    Args:
        metrics: List of metric dicts from retrieve_all_metrics()

    Returns:
        Formatted string with all metric details for LLM prompt
    """
    lines = []

    for i, m in enumerate(metrics, 1):
        # Format change percentages
        def fmt_pct(val):
            if val is None:
                return "N/A"
            sign = "+" if val > 0 else ""
            return f"{sign}{val:.1f}%"

        # Format actual value with units
        if m["actual"] is not None:
            if m["units"] == "%" or m["meta_unit"] in ("percentage", "percent"):
                actual_str = f"{m['actual']:.2f}%"
            elif m["units"] == "millions":
                actual_str = f"${m['actual']:,.0f}M"
            else:
                actual_str = f"{m['actual']:,.2f}"
        else:
            actual_str = "N/A"

        # Determine direction indicator
        direction = ""
        if m["higher_is_better"] is True:
            direction = "(↑ better)"
        elif m["higher_is_better"] is False:
            direction = "(↓ better)"

        # Build metric entry
        lines.append(f"--- Metric {i}: {m['parameter']} ---")
        lines.append(f"  Value: {actual_str} {direction}")
        lines.append(
            f"  Changes: QoQ={fmt_pct(m['qoq'])}, YoY={fmt_pct(m['yoy'])}, "
            f"2Y={fmt_pct(m.get('2y'))}, 3Y={fmt_pct(m.get('3y'))}, "
            f"4Y={fmt_pct(m.get('4y'))}, 5Y={fmt_pct(m.get('5y'))}"
        )
        if m["description"]:
            desc = (
                m["description"][:100] + "..." if len(m["description"]) > 100 else m["description"]
            )
            lines.append(f"  Description: {desc}")
        if m["analyst_usage"]:
            usage = (
                m["analyst_usage"][:100] + "..."
                if len(m["analyst_usage"]) > 100
                else m["analyst_usage"]
            )
            lines.append(f"  Analyst Usage: {usage}")
        lines.append("")

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
    system_prompt = """You are a senior financial analyst specializing in Canadian bank earnings reports. Your task is to select the most important key performance indicators (KPIs) for a quarterly earnings report summary display.

## Selection Framework

Think carefully about what makes a metric "newsworthy" for this quarter's report:

### Tier 1: Core Metrics (Always Consider)
These fundamental metrics should be included unless there's a compelling reason not to:
- Total Revenue, Net Income, Diluted EPS
- Return on Equity (ROE), Return on Assets (ROA)
- CET1 Ratio (regulatory capital)

### Tier 2: Noteworthy Changes
Beyond core metrics, look for KPIs with SIGNIFICANT changes that tell a story:
- Large QoQ or YoY moves (typically >5-10% for most metrics)
- Multi-year trend changes (compare 2Y, 3Y, 5Y to see if recent change is acceleration or reversal)
- Consider the "higher_is_better" indicator to understand if a change is positive or negative news

### Tier 3: Contextual Importance
Some metrics become important based on current banking environment:
- Credit quality metrics during economic uncertainty (PCL, NPL ratios)
- Efficiency metrics when cost management is a focus
- Capital ratios during regulatory changes

## Critical Judgment Required

Do NOT select a metric just because:
- It has a large percentage change on a small base (e.g., +50% on an obscure metric)
- The description sounds important but the actual values are unremarkable
- It's listed in "Analyst Usage" but shows no meaningful movement

DO select metrics that:
- Tell the quarter's story (what went well, what's challenged)
- Would be mentioned in a CEO's earnings call opening remarks
- Show meaningful absolute dollar or basis point impacts
- Represent trends that investors would care about

## Output Requirements
Return EXACTLY the KPI names as they appear in the data - do not modify, paraphrase, or abbreviate them."""

    # Build the user prompt
    user_prompt = f"""Select the top {num_metrics} most important KPIs for {bank_name}'s {quarter} {fiscal_year} quarterly earnings report.

## Available Metrics ({len(metrics)} total)

{metrics_table}

## Your Task

Analyze these metrics and select the {num_metrics} that best represent the key story of this quarter's performance.

Consider:
1. Which core financial metrics must be included?
2. Which metrics show significant or unusual changes worth highlighting?
3. Do any metrics show a trend reversal or acceleration when comparing short-term (QoQ, YoY) vs longer-term (3Y, 5Y) changes?
4. What would a CFO or analyst want to see at a glance?

Return exactly {num_metrics} KPI names."""

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
