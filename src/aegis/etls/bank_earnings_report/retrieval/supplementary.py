"""
Retrieval functions for supplementary (benchmarking_report) data.

This module provides functions to query the benchmarking_report table
for financial metrics like dividends, key metrics, etc.
"""

from typing import Any, Dict, Optional
from sqlalchemy import text

from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import get_logger


async def retrieve_dividend(
    bank_symbol: str, fiscal_year: int, quarter: str, context: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Retrieve dividend data for a specific bank and period.

    Queries the benchmarking_report table for 'Dividends Declared' parameter
    filtered by Platform = 'Enterprise'.

    Args:
        bank_symbol: Bank symbol with suffix (e.g., 'RY-CA')
        fiscal_year: Fiscal year (e.g., 2024)
        quarter: Quarter (e.g., 'Q3')
        context: Execution context with execution_id

    Returns:
        Dict with dividend data or None if not found:
        {
            "actual": float,  # Raw dividend value
            "qoq": float,     # Quarter-over-quarter change (percentage)
            "yoy": float,     # Year-over-year change (percentage)
            "units": str      # Units (e.g., "$" or blank)
        }
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.bank_earnings_report.retrieve_dividend",
        execution_id=execution_id,
        bank_symbol=bank_symbol,
        period=f"{quarter} {fiscal_year}",
    )

    try:
        async with get_connection() as conn:
            result = await conn.execute(
                text("""
                    SELECT "Actual", "QoQ", "YoY", "Units"
                    FROM benchmarking_report
                    WHERE "bank_symbol" = :bank_symbol
                      AND "fiscal_year" = :fiscal_year
                      AND "quarter" = :quarter
                      AND "Parameter" = 'Dividends Declared'
                      AND "Platform" = 'Enterprise'
                    LIMIT 1
                """),
                {
                    "bank_symbol": bank_symbol,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                },
            )

            row = result.fetchone()

            if not row:
                logger.warning(
                    "etl.bank_earnings_report.dividend_not_found",
                    execution_id=execution_id,
                    bank_symbol=bank_symbol,
                    period=f"{quarter} {fiscal_year}",
                )
                return None

            dividend_data = {
                "actual": float(row[0]) if row[0] is not None else None,
                "qoq": float(row[1]) if row[1] is not None else None,
                "yoy": float(row[2]) if row[2] is not None else None,
                "units": row[3] if row[3] else "",
            }

            logger.info(
                "etl.bank_earnings_report.dividend_retrieved",
                execution_id=execution_id,
                bank_symbol=bank_symbol,
                actual=dividend_data["actual"],
                qoq=dividend_data["qoq"],
                yoy=dividend_data["yoy"],
            )

            return dividend_data

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.dividend_error",
            execution_id=execution_id,
            error=str(e),
        )
        return None


def format_dividend_json(dividend_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Format raw dividend data into the JSON structure expected by the template.

    Args:
        dividend_data: Raw dividend data from retrieve_dividend(), or None

    Returns:
        Formatted JSON structure for 0_header_dividend.json:
        {
            "dividend": {
                "amount": "$1.10/share",
                "qoq": {"value": 4.8, "direction": "positive", "display": "▲ 4.8%"},
                "yoy": {"value": 10.0, "direction": "positive", "display": "▲ 10.0%"}
            }
        }
    """
    if not dividend_data or dividend_data.get("actual") is None:
        return {
            "dividend": {
                "amount": "N/A",
                "qoq": {"value": 0, "direction": "neutral", "display": "—"},
                "yoy": {"value": 0, "direction": "neutral", "display": "—"},
            }
        }

    # Format the amount
    actual = dividend_data["actual"]
    amount = f"${actual:.2f}/share"

    # Format QoQ
    qoq_value = dividend_data.get("qoq")
    if qoq_value is not None:
        qoq_direction = "positive" if qoq_value > 0 else "negative" if qoq_value < 0 else "neutral"
        qoq_arrow = "▲" if qoq_value > 0 else "▼" if qoq_value < 0 else "—"
        qoq_display = f"{qoq_arrow} {abs(qoq_value):.1f}%" if qoq_value != 0 else "—"
        qoq = {"value": abs(qoq_value), "direction": qoq_direction, "display": qoq_display}
    else:
        qoq = {"value": 0, "direction": "neutral", "display": "—"}

    # Format YoY
    yoy_value = dividend_data.get("yoy")
    if yoy_value is not None:
        yoy_direction = "positive" if yoy_value > 0 else "negative" if yoy_value < 0 else "neutral"
        yoy_arrow = "▲" if yoy_value > 0 else "▼" if yoy_value < 0 else "—"
        yoy_display = f"{yoy_arrow} {abs(yoy_value):.1f}%" if yoy_value != 0 else "—"
        yoy = {"value": abs(yoy_value), "direction": yoy_direction, "display": yoy_display}
    else:
        yoy = {"value": 0, "direction": "neutral", "display": "—"}

    return {
        "dividend": {
            "amount": amount,
            "qoq": qoq,
            "yoy": yoy,
        }
    }
