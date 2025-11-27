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
                text(
                    """
                    SELECT "Actual", "QoQ", "YoY", "Units"
                    FROM benchmarking_report
                    WHERE "bank_symbol" = :bank_symbol
                      AND "fiscal_year" = :fiscal_year
                      AND "quarter" = :quarter
                      AND "Parameter" = 'Dividends Declared'
                      AND "Platform" = 'Enterprise'
                    LIMIT 1
                """
                ),
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
            "2y": float,           # 2-year change
            "3y": float,           # 3-year change
            "4y": float,           # 4-year change
            "5y": float,           # 5-year change
            "units": str,          # Units from benchmarking_report
            "is_bps": bool,        # Whether to display changes as basis points
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
                text(
                    """
                    SELECT
                        br."Parameter",
                        br."Actual",
                        br."QoQ",
                        br."YoY",
                        br."2Y",
                        br."3Y",
                        br."4Y",
                        br."5Y",
                        br."Units",
                        br."BPS",
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
                """
                ),
                {
                    "bank_symbol": bank_symbol,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                },
            )

            metrics = []
            for row in result:
                # BPS field is "Yes"/"No" indicating if metric should display as basis points
                bps_raw = row[9]
                is_bps = bps_raw in ("Yes", "yes", True, 1) if bps_raw else False

                metrics.append(
                    {
                        "parameter": row[0],
                        "actual": float(row[1]) if row[1] is not None else None,
                        "qoq": float(row[2]) if row[2] is not None else None,
                        "yoy": float(row[3]) if row[3] is not None else None,
                        "2y": float(row[4]) if row[4] is not None else None,
                        "3y": float(row[5]) if row[5] is not None else None,
                        "4y": float(row[6]) if row[6] is not None else None,
                        "5y": float(row[7]) if row[7] is not None else None,
                        "units": row[8] if row[8] else "",
                        "is_bps": is_bps,
                        "description": row[10] if row[10] else "",
                        "meta_unit": row[11] if row[11] else "",
                        "higher_is_better": row[12] if row[12] is not None else None,
                        "analyst_usage": row[13] if row[13] else "",
                    }
                )

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
                text(
                    """
                    SELECT
                        br."Parameter",
                        br."Actual",
                        br."QoQ",
                        br."YoY",
                        br."Units",
                        br."BPS",
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
                """
                ),
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
                # BPS field is "Yes"/"No" indicating if metric should display as basis points
                bps_raw = row[5]
                is_bps = bps_raw in ("Yes", "yes", True, 1) if bps_raw else False

                metrics_dict[row[0]] = {
                    "parameter": row[0],
                    "actual": float(row[1]) if row[1] is not None else None,
                    "qoq": float(row[2]) if row[2] is not None else None,
                    "yoy": float(row[3]) if row[3] is not None else None,
                    "units": row[4] if row[4] else "",
                    "is_bps": is_bps,
                    "description": row[6] if row[6] else "",
                    "meta_unit": row[7] if row[7] else "",
                    "higher_is_better": row[8] if row[8] is not None else None,
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
    # Use up to 2 decimal places, but strip trailing zeros
    if units == "millions" or meta_unit in ("millions", "currency"):
        if actual >= 1000:
            formatted = f"{actual / 1000:,.2f}".rstrip("0").rstrip(".")
            return f"${formatted} B"
        formatted = f"{actual:,.2f}".rstrip("0").rstrip(".")
        return f"${formatted} M"

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


# Threshold for converting bps to percentage (100 bps = 1%)
# Industry convention: use bps for small changes, % for larger changes
BPS_TO_PERCENT_THRESHOLD = 100


def format_delta(value: Optional[float], units: str, is_bps: bool = False) -> Dict[str, Any]:
    """
    Format a QoQ or YoY delta value.

    Args:
        value: Delta value (percentage change or basis points)
        units: Units from benchmarking_report (e.g., "millions", "%", "bps")
        is_bps: Whether to display as basis points (from BPS column)

    Returns:
        Dict with value, direction, and display string
        - direction is simply "positive" for increase, "negative" for decrease

    Note:
        For bps values >= 100 (1%), automatically converts to percentage display
        for better readability. This follows industry convention.
    """
    if value is None:
        return {"value": 0, "direction": "neutral", "display": "—"}

    # Simple: increase = positive (green), decrease = negative (red)
    direction = "positive" if value > 0 else "negative" if value < 0 else "neutral"

    # Format display
    arrow = "▲" if value > 0 else "▼" if value < 0 else "—"
    abs_val = abs(value)

    # Use BPS display if flagged or if units indicate basis points
    if is_bps or units == "bps":
        # If bps value is large (>= threshold), convert to percentage for readability
        if abs_val >= BPS_TO_PERCENT_THRESHOLD:
            # Convert bps to percentage: divide by 100
            pct_val = abs_val / 100
            display = f"{arrow} {pct_val:.1f}%" if value != 0 else "—"
        else:
            # Display as basis points - value is already in bps
            display = f"{arrow} {abs_val:.0f}bps" if value != 0 else "—"
    else:
        # Display as percentage
        display = f"{arrow} {abs_val:.1f}%" if value != 0 else "—"

    return {"value": abs_val, "direction": direction, "display": display}


def format_delta_for_llm(value: Optional[float], units: str, is_bps: bool = False) -> str:
    """
    Format a QoQ/YoY delta value for LLM prompt (simple string, not dict).

    Uses same bps/% logic as format_delta but returns plain string for LLM tables.

    Args:
        value: Delta value (percentage change or basis points)
        units: Units from benchmarking_report
        is_bps: Whether to display as basis points

    Returns:
        Formatted string like "+2.3%" or "+15bps" or "—"

    Note:
        For bps values >= 100 (1%), automatically converts to percentage display
        for better readability. This follows industry convention.
    """
    if value is None:
        return "—"

    sign = "+" if value > 0 else ""
    abs_val = abs(value)

    # Use BPS display if flagged or if units indicate basis points
    if is_bps or units == "bps":
        # If bps value is large (>= threshold), convert to percentage for readability
        if abs_val >= BPS_TO_PERCENT_THRESHOLD:
            # Convert bps to percentage: divide by 100
            pct_val = value / 100
            return f"{sign}{pct_val:.1f}%"
        else:
            return f"{sign}{value:.0f}bps"
    else:
        return f"{sign}{value:.1f}%"


def format_value_for_llm(metric: Dict[str, Any]) -> str:
    """
    Format a metric's current value for LLM prompt.

    Uses same M/B and % logic as format_metric_value.

    Args:
        metric: Metric dict with 'actual', 'units', 'meta_unit', 'is_bps'

    Returns:
        Formatted string like "$9.2B", "13.8%", "52.3%"
    """
    actual = metric.get("actual")
    if actual is None:
        return "N/A"

    units = metric.get("units", "")
    meta_unit = metric.get("meta_unit", "")
    is_bps = metric.get("is_bps", False)

    # Handle percentage metrics (ratios like ROE, efficiency ratio, NIM)
    if units == "%" or meta_unit in ("percentage", "percent", "%") or is_bps:
        return f"{actual:.2f}%"

    # Handle millions (default for dollar amounts)
    if units == "millions" or meta_unit in ("millions", "currency"):
        if actual >= 1000:
            return f"${actual / 1000:,.2f}B"
        return f"${actual:,.0f}M"

    # Handle ratio metrics
    if meta_unit == "ratio":
        return f"{actual:.2f}x"

    # Default formatting
    return f"{actual:,.2f}"


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
        formatted_metrics.append(
            {
                "label": metric["parameter"],
                "value": format_metric_value(
                    metric["actual"], metric["units"], metric["meta_unit"]
                ),
                "qoq": format_delta(metric["qoq"], metric["units"], metric.get("is_bps", False)),
                "yoy": format_delta(metric["yoy"], metric["units"], metric.get("is_bps", False)),
            }
        )

    return {
        "source": "Supp Pack",
        "metrics": formatted_metrics,
    }


# =============================================================================
# Historical Data Retrieval for Charts
# =============================================================================


def get_previous_quarters(fiscal_year: int, quarter: str, num_quarters: int = 8) -> List[tuple]:
    """
    Calculate the previous N quarters from a given quarter.

    Args:
        fiscal_year: Starting fiscal year (e.g., 2024)
        quarter: Starting quarter (e.g., 'Q3')
        num_quarters: Number of quarters to retrieve (default 8)

    Returns:
        List of (year, quarter) tuples in chronological order (oldest first)
    """
    quarters = ["Q1", "Q2", "Q3", "Q4"]
    q_idx = quarters.index(quarter)

    result = []
    current_year = fiscal_year
    current_q_idx = q_idx

    # Include the current quarter and go back num_quarters-1 more
    for _ in range(num_quarters):
        result.append((current_year, quarters[current_q_idx]))
        current_q_idx -= 1
        if current_q_idx < 0:
            current_q_idx = 3
            current_year -= 1

    # Reverse to get chronological order (oldest first)
    return list(reversed(result))


async def retrieve_metric_history(
    bank_symbol: str,
    metric_name: str,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    num_quarters: int = 8,
) -> List[Dict[str, Any]]:
    """
    Retrieve historical data for a specific metric across multiple quarters.

    Args:
        bank_symbol: Bank symbol with suffix (e.g., 'RY-CA')
        metric_name: Parameter name to retrieve (e.g., 'Net Income')
        fiscal_year: Current fiscal year
        quarter: Current quarter
        context: Execution context with execution_id
        num_quarters: Number of quarters to retrieve (default 8)

    Returns:
        List of dicts with quarter data in chronological order:
        [
            {"quarter": "Q4 2022", "value": 4200.0},
            {"quarter": "Q1 2023", "value": 4350.0},
            ...
        ]
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    # Get list of quarters to query
    quarters_to_query = get_previous_quarters(fiscal_year, quarter, num_quarters)

    logger.info(
        "etl.bank_earnings_report.retrieve_metric_history",
        execution_id=execution_id,
        bank_symbol=bank_symbol,
        metric_name=metric_name,
        quarters=len(quarters_to_query),
    )

    try:
        async with get_connection() as conn:
            # Build WHERE clause for each quarter pair
            # Can't use IN with tuple of tuples in asyncpg, so build OR conditions
            quarter_conditions = []
            params = {
                "bank_symbol": bank_symbol,
                "metric_name": metric_name,
            }

            for i, (year, q) in enumerate(quarters_to_query):
                quarter_conditions.append(
                    f'("fiscal_year" = :year_{i} AND "quarter" = :quarter_{i})'
                )
                params[f"year_{i}"] = year
                params[f"quarter_{i}"] = q

            quarter_filter = " OR ".join(quarter_conditions)

            result = await conn.execute(
                text(
                    f"""
                    SELECT
                        "fiscal_year",
                        "quarter",
                        "Actual"
                    FROM benchmarking_report
                    WHERE "bank_symbol" = :bank_symbol
                      AND "Parameter" = :metric_name
                      AND "Platform" = 'Enterprise'
                      AND ({quarter_filter})
                    ORDER BY "fiscal_year", "quarter"
                """
                ),
                params,
            )

            # Build a lookup dict from results
            data_lookup = {}
            for row in result:
                key = (row[0], row[1])  # (year, quarter)
                data_lookup[key] = float(row[2]) if row[2] is not None else None

            # Build output in chronological order
            history = []
            for year, q in quarters_to_query:
                value = data_lookup.get((year, q))
                history.append(
                    {
                        "quarter": f"{q} {year}",
                        "fiscal_year": year,
                        "quarter_num": q,
                        "value": value,
                    }
                )

            logger.info(
                "etl.bank_earnings_report.metric_history_retrieved",
                execution_id=execution_id,
                metric_name=metric_name,
                data_points=len([h for h in history if h["value"] is not None]),
            )

            return history

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.metric_history_error",
            execution_id=execution_id,
            error=str(e),
        )
        return []


def format_chart_json(
    metric_name: str, history: List[Dict[str, Any]], is_bps: bool = False
) -> Dict[str, Any]:
    """
    Format historical data into the JSON structure for the chart.

    Args:
        metric_name: Name of the metric being charted
        history: List of historical data points from retrieve_metric_history()
        is_bps: If True, value is a percentage; if False, value is in millions

    Returns:
        Formatted JSON structure for 1_keymetrics_chart.json matching template expectations:
        {
            "label": "Net Income",
            "unit": "$M",
            "decimal_places": 0,
            "quarters": ["Q3 23", "Q4 23", ...],
            "values": [4200, 4350, ...]
        }
    """
    # Determine unit label and decimal places based on is_bps flag
    # is_bps=True: value is a percentage (e.g., ROE 13.5%)
    # is_bps=False: value is in millions (e.g., Net Income $4,200M)
    if is_bps:
        unit_label = "%"
        decimal_places = 2
    else:
        unit_label = "$M"
        decimal_places = 0

    quarters = []
    values = []

    for h in history:
        if h["value"] is not None:
            # Format quarter label as "Q3 23" style
            q_label = h["quarter"]  # e.g., "Q3 2023"
            parts = q_label.split()
            if len(parts) == 2:
                q_label = f"{parts[0]} {parts[1][2:]}"  # "Q3 2023" -> "Q3 23"
            quarters.append(q_label)
            # Round value to match decimal_places for consistent display
            values.append(round(h["value"], decimal_places))

    return {
        "label": metric_name,
        "unit": unit_label,
        "decimal_places": decimal_places,
        "quarters": quarters,
        "values": values,
    }


# =============================================================================
# Segment Performance Retrieval
# =============================================================================


async def retrieve_available_platforms(
    bank_symbol: str, fiscal_year: int, quarter: str, context: Dict[str, Any]
) -> List[str]:
    """
    Retrieve all distinct platforms available for a bank/period.

    This identifies which business segments have data in the benchmarking_report table.

    Args:
        bank_symbol: Bank symbol with suffix (e.g., 'RY-CA')
        fiscal_year: Fiscal year (e.g., 2024)
        quarter: Quarter (e.g., 'Q3')
        context: Execution context with execution_id

    Returns:
        List of platform names (excluding 'Enterprise' which is the overall bank)
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.bank_earnings_report.retrieve_platforms",
        execution_id=execution_id,
        bank_symbol=bank_symbol,
        period=f"{quarter} {fiscal_year}",
    )

    try:
        async with get_connection() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT DISTINCT "Platform"
                    FROM benchmarking_report
                    WHERE "bank_symbol" = :bank_symbol
                      AND "fiscal_year" = :fiscal_year
                      AND "quarter" = :quarter
                      AND "Platform" IS NOT NULL
                      AND "Platform" != 'Enterprise'
                    ORDER BY "Platform"
                """
                ),
                {
                    "bank_symbol": bank_symbol,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                },
            )

            platforms = [row[0] for row in result]

            logger.info(
                "etl.bank_earnings_report.platforms_retrieved",
                execution_id=execution_id,
                bank_symbol=bank_symbol,
                platform_count=len(platforms),
                platforms=platforms,
            )

            return platforms

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.platforms_error",
            execution_id=execution_id,
            error=str(e),
        )
        return []


async def retrieve_segment_metrics(
    bank_symbol: str,
    fiscal_year: int,
    quarter: str,
    platform: str,
    context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Retrieve all metrics for a specific business segment (platform).

    Similar to retrieve_all_metrics but filtered by a specific Platform value
    instead of 'Enterprise'.

    Args:
        bank_symbol: Bank symbol with suffix (e.g., 'RY-CA')
        fiscal_year: Fiscal year (e.g., 2024)
        quarter: Quarter (e.g., 'Q3')
        platform: Platform name (e.g., 'Canadian P&C')
        context: Execution context with execution_id

    Returns:
        List of metric dicts, each containing:
        {
            "parameter": str,      # KPI name
            "actual": float,       # Current value
            "qoq": float,          # Quarter-over-quarter change
            "yoy": float,          # Year-over-year change
            "2y": float,           # 2-year change
            "3y": float,           # 3-year change
            "4y": float,           # 4-year change
            "5y": float,           # 5-year change
            "units": str,          # Units from benchmarking_report
            "is_bps": bool,        # Whether to display changes as basis points
            "description": str,    # From kpi_metadata
            "meta_unit": str,      # Unit type from kpi_metadata
            "higher_is_better": bool,  # Direction indicator
            "analyst_usage": str,  # How analysts use this metric
        }
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.bank_earnings_report.retrieve_segment_metrics",
        execution_id=execution_id,
        bank_symbol=bank_symbol,
        period=f"{quarter} {fiscal_year}",
        platform=platform,
    )

    try:
        async with get_connection() as conn:
            result = await conn.execute(
                text(
                    """
                    SELECT
                        br."Parameter",
                        br."Actual",
                        br."QoQ",
                        br."YoY",
                        br."2Y",
                        br."3Y",
                        br."4Y",
                        br."5Y",
                        br."Units",
                        br."BPS",
                        km.description,
                        km.unit as meta_unit,
                        km.higher_is_better,
                        km.analyst_usage
                    FROM benchmarking_report br
                    LEFT JOIN kpi_metadata km ON br."Parameter" = km.kpi_name
                    WHERE br."bank_symbol" = :bank_symbol
                      AND br."fiscal_year" = :fiscal_year
                      AND br."quarter" = :quarter
                      AND br."Platform" = :platform
                    ORDER BY br."Parameter"
                """
                ),
                {
                    "bank_symbol": bank_symbol,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                    "platform": platform,
                },
            )

            metrics = []
            for row in result:
                # BPS field is "Yes"/"No" indicating if metric should display as basis points
                bps_raw = row[9]
                is_bps = bps_raw in ("Yes", "yes", True, 1) if bps_raw else False

                metrics.append(
                    {
                        "parameter": row[0],
                        "actual": float(row[1]) if row[1] is not None else None,
                        "qoq": float(row[2]) if row[2] is not None else None,
                        "yoy": float(row[3]) if row[3] is not None else None,
                        "2y": float(row[4]) if row[4] is not None else None,
                        "3y": float(row[5]) if row[5] is not None else None,
                        "4y": float(row[6]) if row[6] is not None else None,
                        "5y": float(row[7]) if row[7] is not None else None,
                        "units": row[8] if row[8] else "",
                        "is_bps": is_bps,
                        "description": row[10] if row[10] else "",
                        "meta_unit": row[11] if row[11] else "",
                        "higher_is_better": row[12] if row[12] is not None else None,
                        "analyst_usage": row[13] if row[13] else "",
                    }
                )

            logger.info(
                "etl.bank_earnings_report.segment_metrics_retrieved",
                execution_id=execution_id,
                platform=platform,
                metric_count=len(metrics),
            )

            return metrics

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.segment_metrics_error",
            execution_id=execution_id,
            platform=platform,
            error=str(e),
        )
        return []


def format_segment_metric_display(
    metric: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Format a single metric for display in segment container.

    Uses the same formatting as key metrics tiles.

    Args:
        metric: Metric dict from retrieve_segment_metrics()

    Returns:
        Formatted metric dict:
        {
            "label": "Total Revenue",
            "value": "$4,200 M",
            "qoq": {"value": 2.1, "direction": "positive", "display": "▲ 2.1%"},
            "yoy": {"value": 5.2, "direction": "positive", "display": "▲ 5.2%"}
        }
    """
    return {
        "label": metric["parameter"],
        "value": format_metric_value(metric["actual"], metric["units"], metric["meta_unit"]),
        "qoq": format_delta(metric["qoq"], metric["units"], metric.get("is_bps", False)),
        "yoy": format_delta(metric["yoy"], metric["units"], metric.get("is_bps", False)),
    }


def format_segment_json(
    segment_name: str,
    description: str,
    core_metrics: List[Dict[str, Any]],
    highlighted_metrics: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Format a segment entry with core and highlighted metrics for the template.

    The output has two metric lists:
    - core_metrics: Fixed metrics (Revenue, Net Income, Efficiency Ratio)
    - highlighted_metrics: LLM-selected segment-specific metrics

    Args:
        segment_name: Normalized segment name (e.g., "Canadian P&C")
        description: Segment description (from RTS or placeholder)
        core_metrics: List of core metric dicts (Revenue, Net Income, Efficiency Ratio)
        highlighted_metrics: List of 3 metric dicts selected by LLM

    Returns:
        Formatted segment entry:
        {
            "name": "Canadian P&C",
            "description": "...",
            "core_metrics": [
                {"label": "Total Revenue", "value": "$4,200 M", "qoq": {...}, "yoy": {...}},
                {"label": "Net Income", "value": "$1,200 M", "qoq": {...}, "yoy": {...}},
                {"label": "Efficiency Ratio", "value": "52.3%", "qoq": {...}, "yoy": {...}}
            ],
            "highlighted_metrics": [
                {"label": "NIM", "value": "2.45%", "qoq": {...}, "yoy": {...}},
                {"label": "Loan Growth", "value": "+5.2%", "qoq": {...}, "yoy": {...}},
                {"label": "PCL Ratio", "value": "0.25%", "qoq": {...}, "yoy": {...}}
            ]
        }
    """
    formatted_core = [format_segment_metric_display(m) for m in core_metrics]
    formatted_highlighted = [format_segment_metric_display(m) for m in highlighted_metrics]

    return {
        "name": segment_name,
        "description": description,
        "core_metrics": formatted_core,
        "highlighted_metrics": formatted_highlighted,
    }
