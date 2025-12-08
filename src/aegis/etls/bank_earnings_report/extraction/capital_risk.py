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
from aegis.utils.prompt_loader import load_prompt_from_db


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

    # Load prompt from database
    prompt_data = load_prompt_from_db(
        layer="bank_earnings_report_etl",
        name="rts_5_capitalrisk_extraction",
        compose_with_globals=False,
        execution_id=execution_id,
    )

    # Format prompts with dynamic content
    system_prompt = prompt_data["system_prompt"].format(bank_name=bank_name)
    user_prompt = prompt_data["user_prompt"].format(
        bank_name=bank_name,
        quarter=quarter,
        fiscal_year=fiscal_year,
        rts_content=rts_content,
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        response = await complete_with_tools(
            messages=messages,
            tools=[prompt_data["tool_definition"]],
            context=context,
            llm_params={
                "model": etl_config.get_model("rts_5_capitalrisk_extraction"),
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
                    reasoning=function_args.get("reasoning", "")[:500],
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
