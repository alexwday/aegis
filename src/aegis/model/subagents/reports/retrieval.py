"""
Reports Subagent - Retrieval Functions

This module handles data retrieval from the aegis_reports table for pre-generated reports.
It queries available reports based on bank and period combinations.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from sqlalchemy import text

from ....connections.postgres_connector import get_connection
from ....utils.logging import get_logger


def get_available_reports(
    combo: Dict[str, Any],
    context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Retrieve available reports for a specific bank and period.

    Args:
        combo: Bank-period combination with bank_id, fiscal_year, quarter
        context: Runtime context with execution_id

    Returns:
        List of report dictionaries with metadata and content
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    try:
        with get_connection() as conn:
            # Query for reports matching the bank/period
            result = conn.execute(text(
                """
                SELECT
                    id,
                    report_name,
                    report_description,
                    report_type,
                    bank_id,
                    bank_name,
                    bank_symbol,
                    fiscal_year,
                    quarter,
                    local_filepath,
                    s3_document_name,
                    s3_pdf_name,
                    markdown_content,
                    generation_date,
                    date_last_modified,
                    generated_by,
                    metadata
                FROM aegis_reports
                WHERE bank_id = :bank_id
                  AND fiscal_year = :fiscal_year
                  AND quarter = :quarter
                ORDER BY generation_date DESC
                """
            ), {
                "bank_id": combo["bank_id"],
                "fiscal_year": combo["fiscal_year"],
                "quarter": combo["quarter"]
            })

            reports = []
            for row in result:
                report = {
                    "id": row.id,
                    "report_name": row.report_name,
                    "report_description": row.report_description,
                    "report_type": row.report_type,
                    "bank_id": row.bank_id,
                    "bank_name": row.bank_name,
                    "bank_symbol": row.bank_symbol,
                    "fiscal_year": row.fiscal_year,
                    "quarter": row.quarter,
                    "local_filepath": row.local_filepath,
                    "s3_document_name": row.s3_document_name,
                    "s3_pdf_name": row.s3_pdf_name,
                    "markdown_content": row.markdown_content,
                    "generation_date": row.generation_date,
                    "date_last_modified": row.date_last_modified,
                    "generated_by": row.generated_by,
                    "metadata": row.metadata
                }
                reports.append(report)

            logger.info(
                "subagent.reports.retrieval",
                execution_id=execution_id,
                bank=f"{combo['bank_name']} ({combo['bank_symbol']})",
                period=f"{combo['quarter']} {combo['fiscal_year']}",
                reports_found=len(reports)
            )

            return reports

    except Exception as e:
        logger.error(
            "subagent.reports.retrieval_error",
            execution_id=execution_id,
            error=str(e),
            bank_id=combo.get("bank_id"),
            fiscal_year=combo.get("fiscal_year"),
            quarter=combo.get("quarter")
        )
        return []


def get_unique_report_types(
    bank_period_combinations: List[Dict[str, Any]],
    context: Dict[str, Any]
) -> List[Dict[str, str]]:
    """
    Get unique report types available across all bank-period combinations.

    Args:
        bank_period_combinations: List of bank-period combinations
        context: Runtime context with execution_id

    Returns:
        List of unique report types with name and description
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    try:
        # Build filter conditions for all combinations
        filter_conditions = []
        params = {}

        for i, combo in enumerate(bank_period_combinations):
            filter_conditions.append(
                f"(bank_id = :bank_id_{i} AND fiscal_year = :fiscal_year_{i} AND quarter = :quarter_{i})"
            )
            params[f"bank_id_{i}"] = combo["bank_id"]
            params[f"fiscal_year_{i}"] = combo["fiscal_year"]
            params[f"quarter_{i}"] = combo["quarter"]

        where_clause = " OR ".join(filter_conditions)

        with get_connection() as conn:
            # Get unique report types
            result = conn.execute(text(f"""
                SELECT DISTINCT
                    report_name,
                    report_description,
                    report_type
                FROM aegis_reports
                WHERE {where_clause}
                ORDER BY report_name
                """), params)

            report_types = []
            for row in result:
                report_types.append({
                    "report_name": row.report_name,
                    "report_description": row.report_description,
                    "report_type": row.report_type
                })

            logger.info(
                "subagent.reports.unique_types",
                execution_id=execution_id,
                num_combinations=len(bank_period_combinations),
                unique_types=len(report_types)
            )

            return report_types

    except Exception as e:
        logger.error(
            "subagent.reports.unique_types_error",
            execution_id=execution_id,
            error=str(e)
        )
        return []


def retrieve_reports_by_type(
    bank_period_combinations: List[Dict[str, Any]],
    report_type: str,
    context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Retrieve all reports of a specific type for the given bank-period combinations.

    Args:
        bank_period_combinations: List of bank-period combinations
        report_type: Type of report to retrieve (e.g., "call_summary")
        context: Runtime context with execution_id

    Returns:
        List of report dictionaries
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    all_reports = []

    for combo in bank_period_combinations:
        try:
            with get_connection() as conn:
                result = conn.execute(text(
                    """
                    SELECT
                        id,
                        report_name,
                        report_description,
                        report_type,
                        bank_id,
                        bank_name,
                        bank_symbol,
                        fiscal_year,
                        quarter,
                        local_filepath,
                        s3_document_name,
                        s3_pdf_name,
                        markdown_content,
                        generation_date,
                        date_last_modified,
                        metadata
                    FROM aegis_reports
                    WHERE bank_id = :bank_id
                      AND fiscal_year = :fiscal_year
                      AND quarter = :quarter
                      AND report_type = :report_type
                    ORDER BY generation_date DESC
                    LIMIT 1
                    """
                ), {
                    "bank_id": combo["bank_id"],
                    "fiscal_year": combo["fiscal_year"],
                    "quarter": combo["quarter"],
                    "report_type": report_type
                })

                row = result.fetchone()
                if row:
                    report = {
                        "id": row.id,
                        "report_name": row.report_name,
                        "report_description": row.report_description,
                        "report_type": row.report_type,
                        "bank_id": row.bank_id,
                        "bank_name": row.bank_name,
                        "bank_symbol": row.bank_symbol,
                        "fiscal_year": row.fiscal_year,
                        "quarter": row.quarter,
                        "local_filepath": row.local_filepath,
                        "s3_document_name": row.s3_document_name,
                        "s3_pdf_name": row.s3_pdf_name,
                        "markdown_content": row.markdown_content,
                        "generation_date": row.generation_date,
                        "date_last_modified": row.date_last_modified,
                        "metadata": row.metadata
                    }
                    all_reports.append(report)

                    logger.debug(
                        "subagent.reports.report_found",
                        execution_id=execution_id,
                        report_id=row.id,
                        bank=row.bank_symbol,
                        period=f"{row.quarter} {row.fiscal_year}",
                        report_type=report_type
                    )
                else:
                    logger.debug(
                        "subagent.reports.no_report",
                        execution_id=execution_id,
                        bank=combo["bank_symbol"],
                        period=f"{combo['quarter']} {combo['fiscal_year']}",
                        report_type=report_type
                    )

        except Exception as e:
            logger.error(
                "subagent.reports.retrieve_error",
                execution_id=execution_id,
                error=str(e),
                combo=combo,
                report_type=report_type
            )

    logger.info(
        "subagent.reports.retrieval_complete",
        execution_id=execution_id,
        report_type=report_type,
        requested_combos=len(bank_period_combinations),
        reports_found=len(all_reports)
    )

    return all_reports