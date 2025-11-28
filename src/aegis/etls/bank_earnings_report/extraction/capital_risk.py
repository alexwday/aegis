"""
Capital & Risk extraction for Bank Earnings Report ETL.

This module extracts capital and risk metrics from Pillar 3 disclosures
using a single LLM call with tool-based structured extraction.

Extracts three categories of metrics:
1. Regulatory Capital Ratios (CET1, Tier 1, Total Capital, Leverage)
2. RWA Composition (Credit Risk, CVA, Operational Risk, Market Risk)
3. Liquidity & Credit Quality (LCR, PCL, GIL, NIL, ACL)

All metrics include QoQ and YoY changes where available.
"""

import json
from typing import Any, Dict, Optional

from aegis.connections.llm_connector import complete_with_tools
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.etls.bank_earnings_report.retrieval.pillar3 import (
    format_pillar3_for_llm,
    retrieve_all_pillar3_sheets,
)
from aegis.utils.logging import get_logger


# RWA component colors for visualization
RWA_COLORS = {
    "Credit Risk": "#3b82f6",  # Blue
    "Credit Valuation Adjustment": "#0ea5e9",  # Sky
    "Operational Risk": "#06b6d4",  # Cyan
    "Market Risk": "#14b8a6",  # Teal
    "Other": "#10b981",  # Emerald
}

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
    "LCR",
    "NSFR",
    "ACL Coverage",
}

# Metrics where lower values are better
LOWER_IS_BETTER = {
    "PCL",
    "PCL Ratio",
    "GIL",
    "GIL Ratio",
    "NIL",
    "NIL Ratio",
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

    This comprehensive tool captures all metrics needed for the Capital & Risk
    section in a single LLM call.

    Returns:
        OpenAI-compatible tool definition dict
    """
    return {
        "type": "function",
        "function": {
            "name": "extract_capital_risk_metrics",
            "description": (
                "Extract capital ratios, RWA composition, and credit quality metrics "
                "from Pillar 3 regulatory disclosures"
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
                    # RWA Components
                    "rwa_credit_risk": {
                        "type": "object",
                        "description": "Credit Risk RWA",
                        "properties": {
                            "value_billions": {
                                "type": "number",
                                "description": "Credit Risk RWA in billions CAD",
                            },
                        },
                        "required": ["value_billions"],
                    },
                    "rwa_cva": {
                        "type": "object",
                        "description": "Credit Valuation Adjustment (CVA) RWA",
                        "properties": {
                            "value_billions": {
                                "type": "number",
                                "description": "CVA RWA in billions CAD",
                            },
                        },
                        "required": ["value_billions"],
                    },
                    "rwa_operational": {
                        "type": "object",
                        "description": "Operational Risk RWA",
                        "properties": {
                            "value_billions": {
                                "type": "number",
                                "description": "Operational Risk RWA in billions CAD",
                            },
                        },
                        "required": ["value_billions"],
                    },
                    "rwa_market": {
                        "type": "object",
                        "description": "Market Risk RWA",
                        "properties": {
                            "value_billions": {
                                "type": "number",
                                "description": "Market Risk RWA in billions CAD",
                            },
                        },
                        "required": ["value_billions"],
                    },
                    "rwa_total": {
                        "type": "number",
                        "description": "Total RWA in billions CAD",
                    },
                    # Liquidity & Credit Quality
                    "lcr": {
                        "type": "object",
                        "description": "Liquidity Coverage Ratio (LCR)",
                        "properties": {
                            "value": {
                                "type": "number",
                                "description": "Current LCR as percentage",
                            },
                            "qoq_change": {"type": "number"},
                            "yoy_change": {"type": "number"},
                        },
                        "required": ["value"],
                    },
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
                    "nil": {
                        "type": "object",
                        "description": "Net Impaired Loans (NIL) - GIL minus specific allowances",
                        "properties": {
                            "value_millions": {
                                "type": "number",
                                "description": "NIL in millions CAD",
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
                            "could not be found in the disclosures"
                        ),
                    },
                },
                "required": [
                    "cet1_ratio",
                    "tier1_ratio",
                    "total_capital_ratio",
                    "leverage_ratio",
                    "rwa_credit_risk",
                    "rwa_operational",
                    "rwa_total",
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


# Mapping configurations for transformation
RATIO_MAPPINGS = [
    ("cet1_ratio", "CET1 Ratio"),
    ("tier1_ratio", "Tier 1 Capital Ratio"),
    ("total_capital_ratio", "Total Capital Ratio"),
    ("leverage_ratio", "Leverage Ratio"),
]

RWA_MAPPINGS = [
    ("rwa_credit_risk", "Credit Risk"),
    ("rwa_cva", "CVA"),
    ("rwa_operational", "Operational"),
    ("rwa_market", "Market Risk"),
]

CREDIT_MAPPINGS = [
    ("pcl", "PCL"),
    ("gil", "GIL"),
    ("nil", "NIL"),
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


def _build_rwa_components(function_args: Dict[str, Any]) -> Dict[str, Any]:
    """Build RWA composition dict from LLM response."""
    rwa_total = function_args.get("rwa_total", 0) or 0
    components = []

    for key, label in RWA_MAPPINGS:
        rwa_data = function_args.get(key, {})
        if rwa_data and rwa_data.get("value_billions") is not None:
            value = rwa_data["value_billions"]
            percentage = (value / rwa_total * 100) if rwa_total > 0 else 0
            components.append(
                {
                    "label": label,
                    "value": format_currency_value(value, in_billions=True),
                    "percentage": round(percentage, 1),
                    "color": RWA_COLORS.get(label, RWA_COLORS["Other"]),
                }
            )

    return {"components": components, "total": format_currency_value(rwa_total, in_billions=True)}


def _build_liquidity_credit(function_args: Dict[str, Any]) -> list:
    """Build liquidity and credit quality metrics list from LLM response."""
    result = []

    # LCR (ratio metric)
    lcr_data = function_args.get("lcr", {})
    if lcr_data and lcr_data.get("value") is not None:
        result.append(
            {
                "label": "LCR",
                "value": format_ratio_value(lcr_data.get("value")),
                "qoq": format_delta(lcr_data.get("qoq_change"), "LCR", is_ratio=True),
                "yoy": format_delta(lcr_data.get("yoy_change"), "LCR", is_ratio=True),
            }
        )

    # Credit metrics (currency values)
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
        "source": "Pillar 3",
        "regulatory_capital": _build_regulatory_capital(function_args),
        "rwa": _build_rwa_components(function_args),
        "liquidity_credit": _build_liquidity_credit(function_args),
    }


def _get_empty_section() -> Dict[str, Any]:
    """Return an empty capital risk section structure."""
    return {
        "source": "Pillar 3",
        "regulatory_capital": [],
        "rwa": {"components": [], "total": ""},
        "liquidity_credit": [],
    }


def _build_system_prompt(bank_name: str) -> str:
    """Build the system prompt for capital risk extraction."""
    return f"""You are a senior financial analyst extracting regulatory capital and \
risk metrics from {bank_name}'s Pillar 3 disclosure document.

## YOUR TASK

Extract SPECIFIC NUMERICAL VALUES for capital ratios, RWA composition, and credit quality \
metrics from the Pillar 3 regulatory disclosure.

## METRICS TO EXTRACT

### 1. Regulatory Capital Ratios
- **CET1 Ratio**: Common Equity Tier 1 / Risk-Weighted Assets
- **Tier 1 Capital Ratio**: Tier 1 Capital / Risk-Weighted Assets
- **Total Capital Ratio**: Total Capital / Risk-Weighted Assets
- **Leverage Ratio**: Tier 1 Capital / Leverage Exposure

For each ratio, also extract QoQ and YoY changes if available (in percentage points).

### 2. RWA Composition (in billions CAD)
- **Credit Risk RWA**: Risk-weighted assets for credit exposures
- **CVA RWA**: Credit Valuation Adjustment risk-weighted assets
- **Operational Risk RWA**: Risk-weighted assets for operational risk
- **Market Risk RWA**: Risk-weighted assets for trading book exposures
- **Total RWA**: Sum of all risk-weighted assets

### 3. Liquidity & Credit Quality
- **LCR**: Liquidity Coverage Ratio (percentage)
- **PCL**: Provision for Credit Losses (millions CAD, for the quarter)
- **GIL**: Gross Impaired Loans (millions CAD)
- **NIL**: Net Impaired Loans (millions CAD)
- **ACL**: Allowance for Credit Losses (millions CAD)

For credit metrics, extract QoQ and YoY percentage changes if available.

## IMPORTANT GUIDELINES

1. **Extract EXACT values** from the document - do not estimate or calculate
2. **Use the current quarter values** shown in the disclosure
3. **For changes**, use the difference in percentage points for ratios
4. **If a metric is not found**, omit it or set to null - do not guess
5. **RWA values** should be in billions (e.g., 450 for $450B)
6. **Credit values** should be in millions (e.g., 2100 for $2.1B)

## WHERE TO FIND THESE METRICS

- Capital ratios: Look for "Capital Ratios", "Regulatory Capital Summary", "Key Metrics"
- RWA: Look for "Risk-Weighted Assets", "RWA by Risk Type", "Capital Requirements"
- LCR: Look for "Liquidity Coverage Ratio", "Liquidity Metrics"
- Credit metrics: Look for "Credit Quality", "Allowance for Credit Losses", "Impaired Loans"
"""


def _build_user_prompt(bank_name: str, quarter: str, fiscal_year: int, content: str) -> str:
    """Build the user prompt for capital risk extraction."""
    return f"""Extract capital and risk metrics from {bank_name}'s {quarter} \
{fiscal_year} Pillar 3 disclosure.

{content}

Extract all available capital ratios, RWA components, and credit quality metrics. \
Use null for any metrics not found in the document."""


async def extract_capital_risk_section(
    bank_symbol: str,
    bank_name: str,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extract capital and risk metrics section from Pillar 3 disclosures.

    Uses a single LLM call with tool-based extraction to populate:
    - Regulatory Capital Ratios (4 metrics with QoQ/YoY)
    - RWA Composition (4 components with percentages)
    - Liquidity & Credit Quality (5 metrics with QoQ/YoY)

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

    # Retrieve Pillar 3 data
    sheets = await retrieve_all_pillar3_sheets(f"{bank_symbol}-CA", fiscal_year, quarter, context)

    if not sheets:
        logger.warning(
            "etl.capital_risk.no_pillar3_data",
            execution_id=execution_id,
            bank=bank_symbol,
        )
        return _get_empty_section()

    pillar3_content = format_pillar3_for_llm(sheets)

    if not pillar3_content.strip() or pillar3_content == "No Pillar 3 content available.":
        return _get_empty_section()

    messages = [
        {"role": "system", "content": _build_system_prompt(bank_name)},
        {
            "role": "user",
            "content": _build_user_prompt(bank_name, quarter, fiscal_year, pillar3_content),
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
                    rwa_components=len(section["rwa"]["components"]),
                    liquidity_metrics=len(section["liquidity_credit"]),
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
