"""
Pillar 3 (regulatory capital disclosure) retrieval for Bank Earnings Report ETL.

This module loads Pillar 3 disclosure data and uses a single LLM call to extract
capital and risk metrics for the Capital & Risk section of the report.

The pillar3_embedding table contains sheets from bank Pillar 3 disclosures with:
- Raw text content from each sheet
- Sheet names identifying the disclosure type
- Source sections for hierarchical navigation

Pipeline:
1. Load all Pillar 3 sheets for the bank/quarter (single DB call)
2. Format into a single document organized by sheet
3. Single LLM call extracts all capital metrics simultaneously
"""

from typing import Any, Dict, List

from sqlalchemy import bindparam, text

from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import get_logger


async def retrieve_all_pillar3_sheets(
    bank: str,
    year: int,
    quarter: str,
    context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Retrieve ALL sheets from Pillar 3 disclosures for a given bank/quarter.

    This loads all Pillar 3 disclosure sheets without filtering, allowing
    the LLM to find relevant capital and risk metrics directly.

    Args:
        bank: Bank symbol with suffix (e.g., "RY-CA")
        year: Fiscal year
        quarter: Quarter (e.g., "Q3")
        context: Execution context

    Returns:
        List of all sheet dicts ordered by sheet_name
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.pillar3.load_all_sheets_start",
        execution_id=execution_id,
        bank=bank,
        period=f"{quarter} {year}",
    )

    try:
        async with get_connection() as conn:
            sql = text(
                """
                SELECT
                    id, sheet_name, bank, quarter, year,
                    filename, raw_text, source_section
                FROM pillar3_embedding
                WHERE bank = :bank AND year = :year AND quarter = :quarter
                ORDER BY sheet_name, id
                """
            ).bindparams(
                bindparam("bank", value=bank),
                bindparam("year", value=str(year)),
                bindparam("quarter", value=quarter),
            )

            result = await conn.execute(sql)
            sheets = []

            for row in result.fetchall():
                sheets.append(
                    {
                        "id": row[0],
                        "sheet_name": row[1],
                        "bank": row[2],
                        "quarter": row[3],
                        "year": row[4],
                        "filename": row[5],
                        "raw_text": row[6],
                        "source_section": row[7],
                    }
                )

            logger.info(
                "etl.pillar3.load_all_sheets_complete",
                execution_id=execution_id,
                bank=bank,
                period=f"{quarter} {year}",
                total_sheets=len(sheets),
                unique_sheet_names=len(set(s["sheet_name"] for s in sheets)),
            )
            return sheets

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("etl.pillar3.load_all_sheets_error", error=str(e))
        return []


def format_pillar3_for_llm(sheets: List[Dict[str, Any]]) -> str:
    """
    Format all Pillar 3 sheets into a single document for LLM processing.

    Organizes content by sheet name with clear section headers.

    Args:
        sheets: List of all sheets sorted by sheet_name

    Returns:
        Formatted full Pillar 3 content string
    """
    if not sheets:
        return "No Pillar 3 content available."

    lines = ["# Pillar 3 Regulatory Capital Disclosure", ""]

    current_sheet = None
    for sheet in sheets:
        sheet_name = sheet.get("sheet_name", "Unknown")
        source_section = sheet.get("source_section", "")
        raw_text = sheet.get("raw_text", "")

        # Add sheet header when sheet changes
        if sheet_name != current_sheet:
            lines.append(f"\n## Sheet: {sheet_name}")
            if source_section:
                lines.append(f"Section: {source_section}")
            lines.append("")
            current_sheet = sheet_name

        if raw_text:
            lines.append(raw_text)
            lines.append("")

    return "\n".join(lines)
