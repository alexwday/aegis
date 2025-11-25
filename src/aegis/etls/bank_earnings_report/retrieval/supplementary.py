"""
Retrieval functions for supplementary (benchmarking_report) data.

This module provides functions to query the benchmarking_report table
for financial metrics like dividends, key metrics, etc.
"""

from typing import Any, Dict, List, Optional
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


# =============================================================================
# Key Metrics Retrieval
# =============================================================================


async def retrieve_all_metrics(
    bank_symbol: str, fiscal_year: int, quarter: str, context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Retrieve all metrics for a bank/period with KPI metadata joined.

    Queries benchmarking_report joined with kpi_metadata to get metrics
    with their descriptions, units, and analyst usage context.

    Args:
        bank_symbol: Bank symbol with suffix (e.g., 'RY-CA')
        fiscal_year: Fiscal year (e.g., 2024)
        quarter: Quarter (e.g., 'Q3')
        context: Execution context with execution_id

    Returns:
        List of metric dicts, each containing:
        {
            "parameter": str,      # KPI name
            "actual": float,       # Current value
            "qoq": float,          # Quarter-over-quarter change
            "yoy": float,          # Year-over-year change
            "units": str,          # Units from benchmarking_report
            "description": str,    # From kpi_metadata
            "meta_unit": str,      # Unit type from kpi_metadata
            "higher_is_better": bool,  # Direction indicator
            "analyst_usage": str,  # How analysts use this metric
        }
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.bank_earnings_report.retrieve_all_metrics",
        execution_id=execution_id,
        bank_symbol=bank_symbol,
        period=f"{quarter} {fiscal_year}",
    )

    try:
        async with get_connection() as conn:
            result = await conn.execute(
                text("""
                    SELECT
                        br."Parameter",
                        br."Actual",
                        br."QoQ",
                        br."YoY",
                        br."Units",
                        km.description,
                        km.unit as meta_unit,
                        km.higher_is_better,
                        km.analyst_usage
                    FROM benchmarking_report br
                    INNER JOIN kpi_metadata km ON br."Parameter" = km.kpi_name
                    WHERE br."bank_symbol" = :bank_symbol
                      AND br."fiscal_year" = :fiscal_year
                      AND br."quarter" = :quarter
                      AND br."Platform" = 'Enterprise'
                    ORDER BY br."Parameter"
                """),
                {
                    "bank_symbol": bank_symbol,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                },
            )

            metrics = []
            for row in result:
                metrics.append({
                    "parameter": row[0],
                    "actual": float(row[1]) if row[1] is not None else None,
                    "qoq": float(row[2]) if row[2] is not None else None,
                    "yoy": float(row[3]) if row[3] is not None else None,
                    "units": row[4] if row[4] else "",
                    "description": row[5] if row[5] else "",
                    "meta_unit": row[6] if row[6] else "",
                    "higher_is_better": row[7] if row[7] is not None else None,
                    "analyst_usage": row[8] if row[8] else "",
                })

            logger.info(
                "etl.bank_earnings_report.metrics_retrieved",
                execution_id=execution_id,
                bank_symbol=bank_symbol,
                metric_count=len(metrics),
            )

            return metrics

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.metrics_error",
            execution_id=execution_id,
            error=str(e),
        )
        return []


async def retrieve_metrics_by_names(
    bank_symbol: str,
    fiscal_year: int,
    quarter: str,
    metric_names: List[str],
    context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Retrieve specific metrics by their parameter names.

    Used after LLM selection to fetch the chosen top 6 metrics.

    Args:
        bank_symbol: Bank symbol with suffix (e.g., 'RY-CA')
        fiscal_year: Fiscal year (e.g., 2024)
        quarter: Quarter (e.g., 'Q3')
        metric_names: List of parameter names to retrieve
        context: Execution context with execution_id

    Returns:
        List of metric dicts in the same order as metric_names
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if not metric_names:
        return []

    try:
        async with get_connection() as conn:
            result = await conn.execute(
                text("""
                    SELECT
                        br."Parameter",
                        br."Actual",
                        br."QoQ",
                        br."YoY",
                        br."Units",
                        km.description,
                        km.unit as meta_unit,
                        km.higher_is_better
                    FROM benchmarking_report br
                    LEFT JOIN kpi_metadata km ON br."Parameter" = km.kpi_name
                    WHERE br."bank_symbol" = :bank_symbol
                      AND br."fiscal_year" = :fiscal_year
                      AND br."quarter" = :quarter
                      AND br."Platform" = 'Enterprise'
                      AND br."Parameter" = ANY(:metric_names)
                """),
                {
                    "bank_symbol": bank_symbol,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                    "metric_names": metric_names,
                },
            )

            # Build a lookup dict
            metrics_dict = {}
            for row in result:
                metrics_dict[row[0]] = {
                    "parameter": row[0],
                    "actual": float(row[1]) if row[1] is not None else None,
                    "qoq": float(row[2]) if row[2] is not None else None,
                    "yoy": float(row[3]) if row[3] is not None else None,
                    "units": row[4] if row[4] else "",
                    "description": row[5] if row[5] else "",
                    "meta_unit": row[6] if row[6] else "",
                    "higher_is_better": row[7] if row[7] is not None else None,
                }

            # Return in the order requested
            metrics = []
            for name in metric_names:
                if name in metrics_dict:
                    metrics.append(metrics_dict[name])
                else:
                    logger.warning(
                        "etl.bank_earnings_report.metric_not_found",
                        execution_id=execution_id,
                        metric_name=name,
                    )

            return metrics

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.metrics_by_name_error",
            execution_id=execution_id,
            error=str(e),
        )
        return []


def format_metric_value(actual: Optional[float], units: str, meta_unit: str) -> str:
    """
    Format a metric value with appropriate units.

    Args:
        actual: Raw numeric value
        units: Units from benchmarking_report (e.g., "millions", "%")
        meta_unit: Unit type from kpi_metadata

    Returns:
        Formatted string (e.g., "$9,200 M", "13.8%", "+1.2%")
    """
    if actual is None:
        return "N/A"

    # Handle percentage metrics
    if units == "%" or meta_unit in ("percentage", "percent", "%"):
        return f"{actual:.1f}%"

    # Handle basis points
    if meta_unit in ("bps", "basis_points"):
        return f"{actual:.0f} bps"

    # Handle millions (default for dollar amounts)
    if units == "millions" or meta_unit in ("millions", "currency"):
        if actual >= 1000:
            return f"${actual / 1000:,.1f} B"
        return f"${actual:,.0f} M"

    # Handle ratio metrics
    if meta_unit == "ratio":
        return f"{actual:.2f}x"

    # Default: just format the number
    if abs(actual) >= 1000000:
        return f"{actual / 1000000:,.1f} M"
    elif abs(actual) >= 1000:
        return f"{actual:,.0f}"
    else:
        return f"{actual:.2f}"


def format_delta(
    value: Optional[float], units: str, meta_unit: str, higher_is_better: Optional[bool]
) -> Dict[str, Any]:
    """
    Format a QoQ or YoY delta value.

    Args:
        value: Delta value (percentage or basis points)
        units: Units from benchmarking_report
        meta_unit: Unit type from kpi_metadata
        higher_is_better: Whether higher values are good (affects direction)

    Returns:
        Dict with value, direction, and display string
    """
    if value is None:
        return {"value": 0, "direction": "neutral", "display": "—"}

    # Determine if this is a basis points metric
    is_bps = meta_unit in ("bps", "basis_points", "percentage", "percent", "%")

    # Calculate direction based on higher_is_better
    if higher_is_better is None:
        # Default: positive change is positive
        direction = "positive" if value > 0 else "negative" if value < 0 else "neutral"
    elif higher_is_better:
        direction = "positive" if value > 0 else "negative" if value < 0 else "neutral"
    else:
        # Lower is better (e.g., efficiency ratio) - flip the direction
        direction = "negative" if value > 0 else "positive" if value < 0 else "neutral"

    # Format display
    arrow = "▲" if value > 0 else "▼" if value < 0 else "—"
    abs_val = abs(value)

    if is_bps and abs_val < 10:
        # Show as basis points for small percentage changes
        bps = abs_val * 100
        display = f"{arrow} {bps:.0f}bps" if value != 0 else "—"
    else:
        display = f"{arrow} {abs_val:.1f}%" if value != 0 else "—"

    return {"value": abs_val, "direction": direction, "display": display}


def format_key_metrics_json(metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Format selected metrics into the JSON structure for key metrics tiles.

    Args:
        metrics: List of metric dicts from retrieve_metrics_by_names()

    Returns:
        Formatted JSON structure for 1_keymetrics_tiles.json:
        {
            "source": "Supp Pack",
            "metrics": [
                {
                    "label": "Total Revenue",
                    "value": "$9,200 M",
                    "qoq": {"value": 2.3, "direction": "positive", "display": "▲ 2.3%"},
                    "yoy": {"value": 6.8, "direction": "positive", "display": "▲ 6.8%"}
                },
                ...
            ]
        }
    """
    formatted_metrics = []

    for metric in metrics:
        formatted_metrics.append({
            "label": metric["parameter"],
            "value": format_metric_value(
                metric["actual"], metric["units"], metric["meta_unit"]
            ),
            "qoq": format_delta(
                metric["qoq"], metric["units"], metric["meta_unit"], metric["higher_is_better"]
            ),
            "yoy": format_delta(
                metric["yoy"], metric["units"], metric["meta_unit"], metric["higher_is_better"]
            ),
        })

    return {
        "source": "Supp Pack",
        "metrics": formatted_metrics,
    }
