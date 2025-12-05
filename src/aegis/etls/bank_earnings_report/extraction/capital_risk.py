"""
Capital & Risk extraction for Bank Earnings Report ETL.

This module extracts capital and risk metrics from RTS (Report to Shareholders)
regulatory filings using a single LLM call with tool-based structured extraction.

Extracts a flat list of metrics with their values - no QoQ/YoY changes since
these are typically not available in quarterly RTS filings.

Categories of metrics:
- Capital Ratios (CET1, Tier 1, Total Capital, Leverage)
- Risk-Weighted Assets
- Credit Quality (PCL, GIL, ACL, PCL ratio, etc.)
- Liquidity (LCR if available)
"""

import json
from typing import Any, Dict

from aegis.connections.llm_connector import complete_with_tools
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.etls.bank_earnings_report.retrieval.rts import (
    format_full_rts_for_llm,
    retrieve_all_rts_chunks,
)
from aegis.utils.logging import get_logger


def build_capital_risk_tool_definition() -> Dict[str, Any]:
    """
    Build the tool definition for extracting capital and risk metrics.

    Uses a simple array of metric objects for maximum flexibility.

    Returns:
        OpenAI-compatible tool definition dict
    """
    return {
        "type": "function",
        "function": {
            "name": "extract_capital_risk_metrics",
            "description": (
                "Extract ONLY regulatory capital ratios (CET1, Tier 1, Total Capital, "
                "Leverage, RWA, LCR) and credit quality metrics (PCL, ACL, GIL). "
                "Do NOT include earnings metrics like Net Income, EPS, ROE, NIM."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "metrics": {
                        "type": "array",
                        "description": "List of all capital and risk metrics found",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "description": "Metric name (e.g., 'CET1 Ratio', 'PCL')",
                                },
                                "value": {
                                    "type": "string",
                                    "description": (
                                        "Metric value with unit (e.g., '13.2%', '$612B', '$2,100M')"
                                    ),
                                },
                                "category": {
                                    "type": "string",
                                    "enum": ["capital", "credit"],
                                    "description": "Whether this is a capital or credit metric",
                                },
                            },
                            "required": ["name", "value", "category"],
                        },
                    },
                    "extraction_notes": {
                        "type": "string",
                        "description": "Brief notes on what was found",
                    },
                },
                "required": ["metrics", "extraction_notes"],
            },
        },
    }


def transform_llm_response_to_section(
    function_args: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Transform the LLM tool response into the template-ready section format.

    Args:
        function_args: Parsed arguments from the LLM tool call

    Returns:
        Dict with capital_metrics and credit_metrics lists
    """
    metrics = function_args.get("metrics", [])

    capital_metrics = []
    credit_metrics = []

    for metric in metrics:
        entry = {
            "name": metric.get("name", ""),
            "value": metric.get("value", ""),
        }
        if metric.get("category") == "capital":
            capital_metrics.append(entry)
        else:
            credit_metrics.append(entry)

    return {
        "source": "RTS",
        "capital_metrics": capital_metrics,
        "credit_metrics": credit_metrics,
    }


def _get_empty_section() -> Dict[str, Any]:
    """Return an empty capital risk section structure."""
    return {
        "source": "RTS",
        "capital_metrics": [],
        "credit_metrics": [],
    }


def _build_system_prompt(bank_name: str) -> str:
    """Build the system prompt for capital risk extraction."""
    return f"""You are extracting capital and credit quality metrics from {bank_name}'s \
quarterly Report to Shareholders (RTS).

## YOUR TASK

Extract ONLY regulatory capital ratios and credit quality metrics.
This section is specifically for Basel III capital adequacy and loan loss metrics.

## CAPITAL METRICS TO EXTRACT

Look in "Capital Management", "Capital Position", or regulatory sections:

- **CET1 Ratio** (Common Equity Tier 1 ratio) - e.g., "13.2%"
- **Tier 1 Capital Ratio** - e.g., "14.5%"
- **Total Capital Ratio** - e.g., "16.8%"
- **Leverage Ratio** - e.g., "4.3%"
- **Risk-Weighted Assets (RWA)** - e.g., "$612B"
- **LCR** (Liquidity Coverage Ratio) - e.g., "128%"
- **NSFR** (Net Stable Funding Ratio) - if available

## CREDIT QUALITY METRICS TO EXTRACT

Look in "Credit Quality", "Risk Management", or "Allowance for Credit Losses":

- **PCL** (Provision for Credit Losses) - quarterly amount, e.g., "$720M"
- **ACL** (Allowance for Credit Losses) - total reserve, e.g., "$5.2B"
- **GIL** (Gross Impaired Loans) - e.g., "$3.8B"
- **PCL Ratio** - PCL as % of average loans, e.g., "0.28%"
- **Net Write-offs** - if available
- **ACL Coverage Ratio** - ACL as % of GIL

## DO NOT INCLUDE THESE (they belong in other report sections)

- Net Income, Revenue, Expenses (financial performance metrics)
- EPS, Diluted EPS (earnings metrics)
- ROE, ROA, ROTCE (profitability ratios)
- NIM, Efficiency Ratio (operating metrics)
- Dividends, Book Value (shareholder metrics)
- Any segment-level metrics

## FORMATTING RULES

1. Include the unit in the value (%, $B, $M, bps)
2. Use consistent formatting:
   - Ratios as percentages: "13.2%"
   - Large amounts in billions: "$612B"
   - Smaller amounts in millions: "$720M"
3. Extract the CURRENT QUARTER value only"""


def _build_user_prompt(bank_name: str, quarter: str, fiscal_year: int, content: str) -> str:
    """Build the user prompt for capital risk extraction."""
    return f"""Extract all capital and credit quality metrics from {bank_name}'s \
{quarter} {fiscal_year} RTS.

Find every capital ratio, RWA figure, and credit quality metric mentioned.
Include the value exactly as shown in the document with appropriate units.

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

    Returns a simple list of metrics found, split into capital and credit categories.

    Args:
        bank_symbol: Bank symbol (e.g., "RY")
        bank_name: Full bank name
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q3")
        context: Execution context

    Returns:
        Dict with capital_metrics and credit_metrics lists
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
                    metrics_found=len(function_args.get("metrics", [])),
                    extraction_notes=function_args.get("extraction_notes", ""),
                )

                section = transform_llm_response_to_section(function_args)

                logger.info(
                    "etl.capital_risk.section_complete",
                    execution_id=execution_id,
                    capital_metrics=len(section["capital_metrics"]),
                    credit_metrics=len(section["credit_metrics"]),
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
