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

    Simplified to focus on metrics commonly available in RTS filings.

    Returns:
        OpenAI-compatible tool definition dict
    """
    return {
        "type": "function",
        "function": {
            "name": "extract_capital_risk_metrics",
            "description": (
                "Extract capital ratios and credit quality metrics from RTS regulatory filings"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    # Regulatory Capital Ratios
                    "cet1_ratio": {
                        "type": "object",
                        "description": "Common Equity Tier 1 (CET1) ratio",
                        "properties": {
                            "value": {
                                "type": "number",
                                "description": "Current CET1 ratio as percentage (e.g., 12.5)",
                            },
                            "qoq_change": {
                                "type": "number",
                                "description": (
                                    "Quarter-over-quarter change in percentage points "
                                    "(e.g., 0.2 for 20 bps increase). Null if not available."
                                ),
                            },
                            "yoy_change": {
                                "type": "number",
                                "description": (
                                    "Year-over-year change in percentage points. "
                                    "Null if not available."
                                ),
                            },
                        },
                        "required": ["value"],
                    },
                    "tier1_ratio": {
                        "type": "object",
                        "description": "Tier 1 Capital ratio",
                        "properties": {
                            "value": {
                                "type": "number",
                                "description": "Current Tier 1 ratio as percentage",
                            },
                            "qoq_change": {"type": "number"},
                            "yoy_change": {"type": "number"},
                        },
                        "required": ["value"],
                    },
                    "total_capital_ratio": {
                        "type": "object",
                        "description": "Total Capital ratio",
                        "properties": {
                            "value": {
                                "type": "number",
                                "description": "Current Total Capital ratio as percentage",
                            },
                            "qoq_change": {"type": "number"},
                            "yoy_change": {"type": "number"},
                        },
                        "required": ["value"],
                    },
                    "leverage_ratio": {
                        "type": "object",
                        "description": "Leverage ratio",
                        "properties": {
                            "value": {
                                "type": "number",
                                "description": "Current Leverage ratio as percentage",
                            },
                            "qoq_change": {"type": "number"},
                            "yoy_change": {"type": "number"},
                        },
                        "required": ["value"],
                    },
                    # Total RWA only (no breakdown)
                    "rwa_total": {
                        "type": "object",
                        "description": "Total Risk-Weighted Assets",
                        "properties": {
                            "value_billions": {
                                "type": "number",
                                "description": "Total RWA in billions CAD",
                            },
                            "qoq_change_pct": {
                                "type": "number",
                                "description": "QoQ percentage change in Total RWA",
                            },
                            "yoy_change_pct": {
                                "type": "number",
                                "description": "YoY percentage change in Total RWA",
                            },
                        },
                        "required": ["value_billions"],
                    },
                    # Credit Quality Metrics
                    "pcl": {
                        "type": "object",
                        "description": "Provision for Credit Losses (PCL)",
                        "properties": {
                            "value_millions": {
                                "type": "number",
                                "description": "PCL in millions CAD for the quarter",
                            },
                            "qoq_change_pct": {
                                "type": "number",
                                "description": "QoQ percentage change in PCL",
                            },
                            "yoy_change_pct": {
                                "type": "number",
                                "description": "YoY percentage change in PCL",
                            },
                        },
                        "required": ["value_millions"],
                    },
                    "gil": {
                        "type": "object",
                        "description": "Gross Impaired Loans (GIL)",
                        "properties": {
                            "value_millions": {
                                "type": "number",
                                "description": "GIL in millions CAD",
                            },
                            "qoq_change_pct": {"type": "number"},
                            "yoy_change_pct": {"type": "number"},
                        },
                        "required": ["value_millions"],
                    },
                    "acl": {
                        "type": "object",
                        "description": "Allowance for Credit Losses (ACL)",
                        "properties": {
                            "value_millions": {
                                "type": "number",
                                "description": "Total ACL in millions CAD",
                            },
                            "qoq_change_pct": {"type": "number"},
                            "yoy_change_pct": {"type": "number"},
                        },
                        "required": ["value_millions"],
                    },
                    "extraction_notes": {
                        "type": "string",
                        "description": (
                            "Brief notes on data availability and any metrics that "
                            "could not be found in the filing"
                        ),
                    },
                },
                "required": ["extraction_notes"],
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


# Mapping configurations for transformation
RATIO_MAPPINGS = [
    ("cet1_ratio", "CET1 Ratio"),
    ("tier1_ratio", "Tier 1 Capital Ratio"),
    ("total_capital_ratio", "Total Capital Ratio"),
    ("leverage_ratio", "Leverage Ratio"),
]

CREDIT_MAPPINGS = [
    ("pcl", "PCL"),
    ("gil", "GIL"),
    ("acl", "ACL"),
]


def _build_regulatory_capital(function_args: Dict[str, Any]) -> list:
    """Build regulatory capital ratios list from LLM response."""
    result = []
    for key, label in RATIO_MAPPINGS:
        ratio_data = function_args.get(key, {})
        if ratio_data and ratio_data.get("value") is not None:
            result.append(
                {
                    "label": label,
                    "value": format_ratio_value(ratio_data.get("value")),
                    "min_requirement": CAPITAL_MINIMUMS.get(label, ""),
                    "qoq": format_delta(ratio_data.get("qoq_change"), label, is_ratio=True),
                    "yoy": format_delta(ratio_data.get("yoy_change"), label, is_ratio=True),
                }
            )
    return result


def _build_rwa_section(function_args: Dict[str, Any]) -> Dict[str, Any]:
    """Build RWA section from LLM response (total only, no breakdown)."""
    rwa_data = function_args.get("rwa_total", {})

    if not rwa_data or rwa_data.get("value_billions") is None:
        return {"total": "—", "qoq": None, "yoy": None}

    return {
        "total": format_currency_value(rwa_data.get("value_billions"), in_billions=True),
        "qoq": format_delta(rwa_data.get("qoq_change_pct"), "RWA", is_ratio=False),
        "yoy": format_delta(rwa_data.get("yoy_change_pct"), "RWA", is_ratio=False),
    }


def _build_credit_quality(function_args: Dict[str, Any]) -> list:
    """Build credit quality metrics list from LLM response."""
    result = []

    for key, label in CREDIT_MAPPINGS:
        credit_data = function_args.get(key, {})
        if credit_data and credit_data.get("value_millions") is not None:
            result.append(
                {
                    "label": label,
                    "value": format_currency_value(
                        credit_data.get("value_millions"), in_billions=False
                    ),
                    "qoq": format_delta(credit_data.get("qoq_change_pct"), label, is_ratio=False),
                    "yoy": format_delta(credit_data.get("yoy_change_pct"), label, is_ratio=False),
                }
            )

    return result


def transform_llm_response_to_section(
    function_args: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Transform the LLM tool response into the template-ready section format.

    Args:
        function_args: Parsed arguments from the LLM tool call

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
    return f"""You are a senior financial analyst extracting regulatory capital and \
credit quality metrics from {bank_name}'s quarterly Report to Shareholders (RTS).

## YOUR TASK

Extract capital ratios, total RWA, and credit quality metrics from the quarterly filing.
These metrics are typically found in the Capital Management and Credit Quality sections.

## METRICS TO EXTRACT

### 1. Regulatory Capital Ratios
Extract these Basel III capital ratios (as percentages):

| Metric | Also Known As | Example |
|--------|---------------|---------|
| CET1 Ratio | Common Equity Tier 1 ratio | 13.2% |
| Tier 1 Capital Ratio | Tier 1 ratio | 14.5% |
| Total Capital Ratio | Total capital ratio | 16.8% |
| Leverage Ratio | Tier 1 leverage ratio | 4.3% |

For each ratio, also look for QoQ and YoY changes (in basis points or percentage points).

### 2. Total Risk-Weighted Assets (RWA)
Extract total RWA in billions CAD. Look for:
- "Risk-weighted assets" or "RWA"
- Usually in Capital Management section
- Example: $612.4 billion

### 3. Credit Quality Metrics
Extract these credit metrics in millions CAD:

| Metric | Also Known As | What It Is |
|--------|---------------|------------|
| PCL | Provision for credit losses, Credit loss expense | Quarterly credit provision |
| GIL | Gross impaired loans, Impaired loans | Total impaired loan balances |
| ACL | Allowance for credit losses, Credit reserves | Total loan loss reserves |

## WHERE TO FIND THESE METRICS

**Capital Ratios**: Look in sections titled:
- "Capital Management"
- "Capital Position"
- "Capital Strength"
- "Key Performance Metrics" or "Financial Highlights"
- Tables with "Regulatory Capital Ratios"

**RWA**: Usually in same section as capital ratios, or:
- "Risk-Weighted Assets"
- "Capital Requirements"

**Credit Quality**: Look in sections titled:
- "Credit Quality"
- "Allowance for Credit Losses"
- "Provision for Credit Losses"
- "Risk Management - Credit Risk"
- "Asset Quality"

## IMPORTANT GUIDELINES

1. **Extract EXACT values** from the document - do not estimate or calculate
2. **Use current quarter values** - not year-to-date or annual
3. **Capital ratios** are percentages (e.g., 13.2 not 0.132)
4. **RWA** should be in billions (e.g., 612.4 for $612.4B)
5. **Credit metrics** should be in millions (e.g., 2100 for $2.1B)
6. **If a metric is not found**, set to null - do not guess
7. **Changes** - look for "increase/decrease of X bps" or comparison tables"""


def _build_user_prompt(bank_name: str, quarter: str, fiscal_year: int, content: str) -> str:
    """Build the user prompt for capital risk extraction."""
    return f"""Extract capital and credit quality metrics from {bank_name}'s {quarter} \
{fiscal_year} quarterly Report to Shareholders.

Focus on finding:
1. Capital ratios (CET1, Tier 1, Total Capital, Leverage) with any QoQ/YoY changes
2. Total Risk-Weighted Assets (RWA) in billions
3. Credit quality metrics (PCL, GIL, ACL) in millions with any changes

Set any metric to null if not found - do not guess values.

---

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
