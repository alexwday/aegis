"""
Capital & Risk extraction for Bank Earnings Report ETL.

This module extracts capital and risk metrics from RTS (Report to Shareholders)
regulatory filings using a single LLM call with tool-based structured extraction.

Extracts two categories of metrics:
1. Regulatory Capital Ratios (CET1, Tier 1, Total Capital, Leverage) + Total RWA
2. Credit Quality Metrics (PCL, GIL, ACL)

All metrics include QoQ and YoY changes where available.
"""

import json
from typing import Any, Dict, Optional

from aegis.connections.llm_connector import complete_with_tools
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.etls.bank_earnings_report.retrieval.rts import (
    format_full_rts_for_llm,
    retrieve_all_rts_chunks,
)
from aegis.utils.logging import get_logger


# Minimum regulatory requirements for capital ratios
CAPITAL_MINIMUMS = {
    "CET1 Ratio": "7.0%",
    "Tier 1 Capital Ratio": "8.5%",
    "Total Capital Ratio": "10.5%",
    "Leverage Ratio": "3.0%",
}

# Metrics where higher values are better (for delta direction coloring)
HIGHER_IS_BETTER = {
    "CET1 Ratio",
    "Tier 1 Capital Ratio",
    "Total Capital Ratio",
    "Leverage Ratio",
    "ACL",
}

# Metrics where lower values are better
LOWER_IS_BETTER = {
    "PCL",
    "GIL",
}


def format_delta(
    value: Optional[float], metric_label: str, is_ratio: bool = True
) -> Dict[str, Any]:
    """
    Format a delta value with direction indicator.

    Args:
        value: The change value (can be None)
        metric_label: Name of the metric for determining direction logic
        is_ratio: Whether this is a ratio metric (shows 'pp' for percentage points)

    Returns:
        Dict with value, direction, and display string
    """
    if value is None:
        return {"value": 0, "direction": "neutral", "display": "—"}

    # Determine if positive change is good or bad
    if metric_label in HIGHER_IS_BETTER:
        direction = "positive" if value > 0 else "negative" if value < 0 else "neutral"
    elif metric_label in LOWER_IS_BETTER:
        direction = "positive" if value < 0 else "negative" if value > 0 else "neutral"
    else:
        # Default: neutral for unknown metrics
        direction = "neutral"

    arrow = "▲" if value > 0 else "▼" if value < 0 else "—"
    unit = "pp" if is_ratio else "%"
    display = f"{arrow} {abs(value):.1f}{unit}" if value != 0 else "—"

    return {"value": abs(value), "direction": direction, "display": display}


def build_capital_risk_tool_definition() -> Dict[str, Any]:
    """
    Build the tool definition for extracting capital and risk metrics.

    Uses a flat schema (like narrative extraction) for better LLM compliance.

    Returns:
        OpenAI-compatible tool definition dict
    """
    return {
        "type": "function",
        "function": {
            "name": "extract_capital_risk_metrics",
            "description": (
                "Extract capital ratios and credit quality metrics from RTS regulatory filings. "
                "All values should be extracted exactly as shown in the document."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    # CET1 Ratio
                    "cet1_value": {
                        "type": "number",
                        "description": "CET1 ratio as percentage (e.g., 13.2 for 13.2%)",
                    },
                    "cet1_qoq": {
                        "type": "number",
                        "description": (
                            "CET1 QoQ change in percentage points (e.g., 0.2 for +20bps)"
                        ),
                    },
                    "cet1_yoy": {
                        "type": "number",
                        "description": "CET1 YoY change in percentage points",
                    },
                    # Tier 1 Ratio
                    "tier1_value": {
                        "type": "number",
                        "description": "Tier 1 Capital ratio as percentage",
                    },
                    "tier1_qoq": {
                        "type": "number",
                        "description": "Tier 1 QoQ change in percentage points",
                    },
                    "tier1_yoy": {
                        "type": "number",
                        "description": "Tier 1 YoY change in percentage points",
                    },
                    # Total Capital Ratio
                    "total_capital_value": {
                        "type": "number",
                        "description": "Total Capital ratio as percentage",
                    },
                    "total_capital_qoq": {
                        "type": "number",
                        "description": "Total Capital QoQ change in percentage points",
                    },
                    "total_capital_yoy": {
                        "type": "number",
                        "description": "Total Capital YoY change in percentage points",
                    },
                    # Leverage Ratio
                    "leverage_value": {
                        "type": "number",
                        "description": "Leverage ratio as percentage",
                    },
                    "leverage_qoq": {
                        "type": "number",
                        "description": "Leverage ratio QoQ change in percentage points",
                    },
                    "leverage_yoy": {
                        "type": "number",
                        "description": "Leverage ratio YoY change in percentage points",
                    },
                    # Total RWA
                    "rwa_value": {
                        "type": "number",
                        "description": "Total RWA in billions CAD (e.g., 612.4 for $612.4B)",
                    },
                    "rwa_qoq": {
                        "type": "number",
                        "description": "RWA QoQ percentage change",
                    },
                    "rwa_yoy": {
                        "type": "number",
                        "description": "RWA YoY percentage change",
                    },
                    # PCL
                    "pcl_value": {
                        "type": "number",
                        "description": "PCL in millions CAD for the quarter",
                    },
                    "pcl_qoq": {
                        "type": "number",
                        "description": "PCL QoQ percentage change",
                    },
                    "pcl_yoy": {
                        "type": "number",
                        "description": "PCL YoY percentage change",
                    },
                    # GIL
                    "gil_value": {
                        "type": "number",
                        "description": "Gross Impaired Loans in millions CAD",
                    },
                    "gil_qoq": {
                        "type": "number",
                        "description": "GIL QoQ percentage change",
                    },
                    "gil_yoy": {
                        "type": "number",
                        "description": "GIL YoY percentage change",
                    },
                    # ACL
                    "acl_value": {
                        "type": "number",
                        "description": "Allowance for Credit Losses in millions CAD",
                    },
                    "acl_qoq": {
                        "type": "number",
                        "description": "ACL QoQ percentage change",
                    },
                    "acl_yoy": {
                        "type": "number",
                        "description": "ACL YoY percentage change",
                    },
                    # Notes
                    "extraction_notes": {
                        "type": "string",
                        "description": "Brief notes on which metrics were found vs not found",
                    },
                },
                "required": [
                    "cet1_value",
                    "pcl_value",
                    "extraction_notes",
                ],
            },
        },
    }


def format_ratio_value(value: Optional[float]) -> str:
    """Format a ratio value as percentage string."""
    if value is None:
        return "—"
    return f"{value:.1f}%"


def format_currency_value(value: Optional[float], in_billions: bool = True) -> str:
    """Format a currency value with appropriate unit."""
    if value is None:
        return "—"
    if in_billions:
        return f"${value:.1f}B"
    return f"${value:,.0f}M"


def _build_ratio_entry(
    function_args: Dict[str, Any], prefix: str, label: str
) -> Optional[Dict[str, Any]]:
    """Build a single ratio entry from flat function args."""
    value = function_args.get(f"{prefix}_value")
    if value is None:
        return None

    return {
        "label": label,
        "value": format_ratio_value(value),
        "min_requirement": CAPITAL_MINIMUMS.get(label, ""),
        "qoq": format_delta(function_args.get(f"{prefix}_qoq"), label, is_ratio=True),
        "yoy": format_delta(function_args.get(f"{prefix}_yoy"), label, is_ratio=True),
    }


def _build_regulatory_capital(function_args: Dict[str, Any]) -> list:
    """Build regulatory capital ratios list from flat LLM response."""
    result = []

    ratio_configs = [
        ("cet1", "CET1 Ratio"),
        ("tier1", "Tier 1 Capital Ratio"),
        ("total_capital", "Total Capital Ratio"),
        ("leverage", "Leverage Ratio"),
    ]

    for prefix, label in ratio_configs:
        entry = _build_ratio_entry(function_args, prefix, label)
        if entry:
            result.append(entry)

    return result


def _build_rwa_section(function_args: Dict[str, Any]) -> Dict[str, Any]:
    """Build RWA section from flat LLM response."""
    rwa_value = function_args.get("rwa_value")

    if rwa_value is None:
        return {"total": "—", "qoq": None, "yoy": None}

    return {
        "total": format_currency_value(rwa_value, in_billions=True),
        "qoq": format_delta(function_args.get("rwa_qoq"), "RWA", is_ratio=False),
        "yoy": format_delta(function_args.get("rwa_yoy"), "RWA", is_ratio=False),
    }


def _build_credit_entry(
    function_args: Dict[str, Any], prefix: str, label: str
) -> Optional[Dict[str, Any]]:
    """Build a single credit quality entry from flat function args."""
    value = function_args.get(f"{prefix}_value")
    if value is None:
        return None

    return {
        "label": label,
        "value": format_currency_value(value, in_billions=False),
        "qoq": format_delta(function_args.get(f"{prefix}_qoq"), label, is_ratio=False),
        "yoy": format_delta(function_args.get(f"{prefix}_yoy"), label, is_ratio=False),
    }


def _build_credit_quality(function_args: Dict[str, Any]) -> list:
    """Build credit quality metrics list from flat LLM response."""
    result = []

    credit_configs = [
        ("pcl", "PCL"),
        ("gil", "GIL"),
        ("acl", "ACL"),
    ]

    for prefix, label in credit_configs:
        entry = _build_credit_entry(function_args, prefix, label)
        if entry:
            result.append(entry)

    return result


def transform_llm_response_to_section(
    function_args: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Transform the LLM tool response into the template-ready section format.

    Args:
        function_args: Parsed arguments from the LLM tool call (flat structure)

    Returns:
        Dict matching the 5_capital_risk template schema
    """
    return {
        "source": "RTS",
        "regulatory_capital": _build_regulatory_capital(function_args),
        "rwa": _build_rwa_section(function_args),
        "credit_quality": _build_credit_quality(function_args),
    }


def _get_empty_section() -> Dict[str, Any]:
    """Return an empty capital risk section structure."""
    return {
        "source": "RTS",
        "regulatory_capital": [],
        "rwa": {"total": "—", "qoq": None, "yoy": None},
        "credit_quality": [],
    }


def _build_system_prompt(bank_name: str) -> str:
    """Build the system prompt for capital risk extraction."""
    return f"""You are extracting capital and credit metrics from {bank_name}'s quarterly RTS.

## METRICS TO FIND

**Capital Ratios** (as percentages, e.g., 13.2):
- CET1 Ratio (Common Equity Tier 1)
- Tier 1 Capital Ratio
- Total Capital Ratio
- Leverage Ratio

**RWA** (in billions CAD, e.g., 612.4):
- Total Risk-Weighted Assets

**Credit Quality** (in millions CAD):
- PCL: Provision for Credit Losses (quarterly amount)
- GIL: Gross Impaired Loans
- ACL: Allowance for Credit Losses

## WHERE TO LOOK

- Capital ratios: "Capital Management", "Capital Position", "Key Metrics", "Financial Highlights"
- RWA: Usually near capital ratios, "Risk-Weighted Assets"
- Credit metrics: "Credit Quality", "Allowance for Credit Losses", "Risk Management"

## RULES

1. Extract EXACT values from the document
2. Capital ratios are percentages (13.2 not 0.132)
3. RWA in billions (612.4 for $612.4 billion)
4. Credit metrics in millions (2100 for $2,100 million)
5. For QoQ/YoY changes, use percentage points for ratios, percentages for others
6. If you cannot find a metric, do not include it in the output"""


def _build_user_prompt(bank_name: str, quarter: str, fiscal_year: int, content: str) -> str:
    """Build the user prompt for capital risk extraction."""
    return f"""Extract capital and credit metrics from {bank_name}'s {quarter} {fiscal_year} RTS.

Find and extract:
- CET1, Tier 1, Total Capital, and Leverage ratios (with any QoQ/YoY changes)
- Total RWA in billions
- PCL, GIL, and ACL in millions (with any QoQ/YoY changes)

Document content:

{content}"""


async def extract_capital_risk_section(
    bank_symbol: str,
    bank_name: str,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extract capital and risk metrics section from RTS regulatory filings.

    Uses a single LLM call with tool-based extraction to populate:
    - Regulatory Capital Ratios (4 metrics with QoQ/YoY)
    - Total RWA
    - Credit Quality Metrics (3 metrics with QoQ/YoY)

    Args:
        bank_symbol: Bank symbol (e.g., "RY")
        bank_name: Full bank name
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q3")
        context: Execution context

    Returns:
        Dict matching the 5_capital_risk template schema
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.capital_risk.extraction_start",
        execution_id=execution_id,
        bank=bank_symbol,
        period=f"{quarter} {fiscal_year}",
    )

    # Retrieve RTS data
    chunks = await retrieve_all_rts_chunks(f"{bank_symbol}-CA", fiscal_year, quarter, context)

    if not chunks:
        logger.warning(
            "etl.capital_risk.no_rts_data",
            execution_id=execution_id,
            bank=bank_symbol,
        )
        return _get_empty_section()

    rts_content = format_full_rts_for_llm(chunks)

    if not rts_content.strip() or rts_content == "No RTS content available.":
        return _get_empty_section()

    messages = [
        {"role": "system", "content": _build_system_prompt(bank_name)},
        {
            "role": "user",
            "content": _build_user_prompt(bank_name, quarter, fiscal_year, rts_content),
        },
    ]

    try:
        response = await complete_with_tools(
            messages=messages,
            tools=[build_capital_risk_tool_definition()],
            context=context,
            llm_params={
                "model": etl_config.get_model("capital_risk_extraction"),
                "temperature": etl_config.temperature,
                "max_tokens": etl_config.max_tokens,
            },
        )

        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                function_args = json.loads(message["tool_calls"][0]["function"]["arguments"])

                logger.info(
                    "etl.capital_risk.extraction_complete",
                    execution_id=execution_id,
                    extraction_notes=function_args.get("extraction_notes", ""),
                )

                section = transform_llm_response_to_section(function_args)

                logger.info(
                    "etl.capital_risk.section_complete",
                    execution_id=execution_id,
                    capital_ratios=len(section["regulatory_capital"]),
                    has_rwa=section["rwa"]["total"] != "—",
                    credit_metrics=len(section["credit_quality"]),
                )

                return section

        logger.warning("etl.capital_risk.no_tool_call", execution_id=execution_id)
        return _get_empty_section()

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "etl.capital_risk.extraction_error",
            execution_id=execution_id,
            error=str(e),
        )
        return _get_empty_section()
