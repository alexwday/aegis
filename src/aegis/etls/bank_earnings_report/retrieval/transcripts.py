"""
Transcript retrieval utilities for Bank Earnings Report ETL.

Retrieves transcript sections from earnings call transcripts:
- Q&A sections: grouped by qa_group_id for individual LLM processing
- MD (Management Discussion) sections: grouped by speaker_block_id for quote extraction
"""

from typing import Any, Dict, List

from sqlalchemy import text

from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import get_logger


async def get_transcript_diagnostics(
    bank_id: int,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Get diagnostic counts for transcript filters to help debug zero-result queries.

    Args:
        bank_id: Bank ID
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context

    Returns:
        Dict with diagnostic counts for each filter combination
    """
    logger = get_logger()
    diagnostics = {}
    bank_id_str = str(bank_id)

    try:
        async with get_connection() as conn:
            result = await conn.execute(text("SELECT COUNT(*) FROM aegis_transcripts"))
            diagnostics["total_records"] = result.scalar()

            result = await conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM aegis_transcripts
                    WHERE institution_id::text = :bank_id_str
                    """
                ),
                {"bank_id_str": bank_id_str},
            )
            diagnostics["matching_bank_id"] = result.scalar()

            result = await conn.execute(
                text("SELECT COUNT(*) FROM aegis_transcripts WHERE fiscal_year = :fiscal_year"),
                {"fiscal_year": fiscal_year},
            )
            diagnostics["matching_year"] = result.scalar()

            result = await conn.execute(
                text("SELECT COUNT(*) FROM aegis_transcripts WHERE fiscal_quarter = :quarter"),
                {"quarter": quarter},
            )
            diagnostics["matching_quarter"] = result.scalar()

            result = await conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM aegis_transcripts
                    WHERE institution_id::text = :bank_id_str
                    AND fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                    """
                ),
                {
                    "bank_id_str": bank_id_str,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                },
            )
            diagnostics["matching_all_filters"] = result.scalar()

            result = await conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM aegis_transcripts
                    WHERE institution_id::text = :bank_id_str
                    AND fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                    AND section_name = 'Q&A'
                    """
                ),
                {
                    "bank_id_str": bank_id_str,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                },
            )
            diagnostics["qa_chunks"] = result.scalar()

            result = await conn.execute(
                text(
                    """
                    SELECT COUNT(DISTINCT qa_group_id) FROM aegis_transcripts
                    WHERE institution_id::text = :bank_id_str
                    AND fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                    AND section_name = 'Q&A'
                    AND qa_group_id IS NOT NULL
                    """
                ),
                {
                    "bank_id_str": bank_id_str,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                },
            )
            diagnostics["qa_groups"] = result.scalar()

    except Exception as e:
        logger.error(f"Failed to get transcript diagnostics: {e}")
        diagnostics["error"] = str(e)

    return diagnostics


async def retrieve_qa_chunks(
    bank_id: int,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Retrieve all Q&A section chunks from the transcript.

    Args:
        bank_id: Bank ID
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context

    Returns:
        List of transcript chunks for Q&A section
    """
    logger = get_logger()
    execution_id = context.get("execution_id")
    bank_id_str = str(bank_id)

    diagnostics = await get_transcript_diagnostics(bank_id, fiscal_year, quarter, context)

    logger.info(
        "etl.bank_earnings_report.transcript_diagnostics",
        execution_id=execution_id,
        bank_id=bank_id,
        fiscal_year=fiscal_year,
        quarter=quarter,
        diagnostics=diagnostics,
    )

    try:
        async with get_connection() as conn:
            query = text(
                """
                SELECT
                    id,
                    section_name,
                    speaker_block_id,
                    qa_group_id,
                    chunk_id,
                    chunk_content,
                    block_summary,
                    classification_ids,
                    classification_names,
                    title
                FROM aegis_transcripts
                WHERE institution_id::text = :bank_id_str
                    AND fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                    AND section_name = 'Q&A'
                ORDER BY qa_group_id, chunk_id
                """
            )

            result = await conn.execute(
                query,
                {
                    "bank_id_str": bank_id_str,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                },
            )

            chunks = []
            for row in result:
                if row[3] is not None:
                    chunks.append(
                        {
                            "id": row[0],
                            "section_name": row[1],
                            "speaker_block_id": row[2],
                            "qa_group_id": row[3],
                            "chunk_id": row[4],
                            "content": row[5],
                            "block_summary": row[6],
                            "classification_ids": row[7],
                            "classification_names": row[8],
                            "title": row[9],
                        }
                    )

            logger.info(
                "etl.bank_earnings_report.qa_chunks_retrieved",
                execution_id=execution_id,
                chunks_count=len(chunks),
                qa_groups_expected=diagnostics.get("qa_groups", 0),
            )

            return chunks

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.qa_retrieval_error",
            execution_id=execution_id,
            error=str(e),
        )
        return []


def group_chunks_by_qa_id(chunks: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    """
    Group Q&A chunks by their qa_group_id.

    Each qa_group_id represents a single question-answer exchange.

    Args:
        chunks: List of Q&A transcript chunks

    Returns:
        Dict mapping qa_group_id to list of chunks in that group
    """
    groups: Dict[int, List[Dict[str, Any]]] = {}

    for chunk in chunks:
        qa_id = chunk.get("qa_group_id")
        if qa_id is not None:
            if qa_id not in groups:
                groups[qa_id] = []
            groups[qa_id].append(chunk)

    for qa_id in groups:
        groups[qa_id].sort(key=lambda x: x.get("chunk_id", 0))

    return groups


def format_qa_group_for_llm(
    qa_group_id: int,
    chunks: List[Dict[str, Any]],
    bank_name: str,
    quarter: str,
    fiscal_year: int,
) -> str:
    """
    Format a single Q&A group into text for LLM processing.

    Combines all chunks in the group into a coherent exchange.

    Args:
        qa_group_id: The Q&A group ID
        chunks: List of chunks in this group (sorted by chunk_id)
        bank_name: Bank name for context
        quarter: Quarter
        fiscal_year: Fiscal year

    Returns:
        Formatted text of the Q&A exchange
    """
    if not chunks:
        return ""

    title = chunks[0].get("title", "Earnings Call Transcript")
    classification_names = chunks[0].get("classification_names", [])
    block_summary = chunks[0].get("block_summary", "")

    full_content = "\n\n".join(chunk.get("content", "") for chunk in chunks if chunk.get("content"))

    formatted = f"""## Q&A Exchange #{qa_group_id}

**Bank:** {bank_name}
**Period:** {quarter} {fiscal_year}
**Transcript:** {title}
"""

    if classification_names:
        formatted += f"**Topics:** {', '.join(classification_names)}\n"

    if block_summary:
        formatted += f"**Summary:** {block_summary}\n"

    formatted += f"""
---
**Full Exchange:**

{full_content}
"""

    return formatted


def get_qa_group_summary(chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Get summary information about a Q&A group.

    Args:
        chunks: List of chunks in the group

    Returns:
        Dict with summary info (chunk_count, topics, summary)
    """
    if not chunks:
        return {"chunk_count": 0, "topics": [], "summary": ""}

    return {
        "chunk_count": len(chunks),
        "topics": chunks[0].get("classification_names", []),
        "summary": chunks[0].get("block_summary", ""),
    }


async def retrieve_md_chunks(
    bank_id: int,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Retrieve all Management Discussion section chunks from the transcript.

    Args:
        bank_id: Bank ID
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context

    Returns:
        List of transcript chunks for MD section
    """
    logger = get_logger()
    execution_id = context.get("execution_id")
    bank_id_str = str(bank_id)

    try:
        async with get_connection() as conn:
            count_query = text(
                """
                SELECT COUNT(*) FROM aegis_transcripts
                WHERE institution_id::text = :bank_id_str
                    AND fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                    AND section_name = 'MANAGEMENT DISCUSSION SECTION'
                """
            )
            count_result = await conn.execute(
                count_query,
                {
                    "bank_id_str": bank_id_str,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                },
            )
            md_chunk_count = count_result.scalar()

            block_count_query = text(
                """
                SELECT COUNT(DISTINCT speaker_block_id) FROM aegis_transcripts
                WHERE institution_id::text = :bank_id_str
                    AND fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                    AND section_name = 'MANAGEMENT DISCUSSION SECTION'
                    AND speaker_block_id IS NOT NULL
                """
            )
            block_result = await conn.execute(
                block_count_query,
                {
                    "bank_id_str": bank_id_str,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                },
            )
            speaker_block_count = block_result.scalar()

            logger.info(
                "etl.bank_earnings_report.md_diagnostics",
                execution_id=execution_id,
                bank_id=bank_id,
                fiscal_year=fiscal_year,
                quarter=quarter,
                md_chunks=md_chunk_count,
                speaker_blocks=speaker_block_count,
            )

            query = text(
                """
                SELECT
                    id,
                    section_name,
                    speaker_block_id,
                    qa_group_id,
                    chunk_id,
                    chunk_content,
                    block_summary,
                    classification_ids,
                    classification_names,
                    title
                FROM aegis_transcripts
                WHERE institution_id::text = :bank_id_str
                    AND fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                    AND section_name = 'MANAGEMENT DISCUSSION SECTION'
                ORDER BY speaker_block_id, chunk_id
                """
            )

            result = await conn.execute(
                query,
                {
                    "bank_id_str": bank_id_str,
                    "fiscal_year": fiscal_year,
                    "quarter": quarter,
                },
            )

            chunks = []
            for row in result:
                if row[2] is not None:
                    chunks.append(
                        {
                            "id": row[0],
                            "section_name": row[1],
                            "speaker_block_id": row[2],
                            "qa_group_id": row[3],
                            "chunk_id": row[4],
                            "content": row[5],
                            "block_summary": row[6],
                            "classification_ids": row[7],
                            "classification_names": row[8],
                            "title": row[9],
                        }
                    )

            logger.info(
                "etl.bank_earnings_report.md_chunks_retrieved",
                execution_id=execution_id,
                chunks_count=len(chunks),
                speaker_blocks_expected=speaker_block_count,
            )

            return chunks

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.md_retrieval_error",
            execution_id=execution_id,
            error=str(e),
        )
        return []


def group_chunks_by_speaker_block(chunks: List[Dict[str, Any]]) -> Dict[int, List[Dict[str, Any]]]:
    """
    Group MD chunks by their speaker_block_id.

    Each speaker_block_id represents a single speaker's remarks.

    Args:
        chunks: List of MD transcript chunks

    Returns:
        Dict mapping speaker_block_id to list of chunks in that block
    """
    groups: Dict[int, List[Dict[str, Any]]] = {}

    for chunk in chunks:
        block_id = chunk.get("speaker_block_id")
        if block_id is not None:
            if block_id not in groups:
                groups[block_id] = []
            groups[block_id].append(chunk)

    for block_id in groups:
        groups[block_id].sort(key=lambda x: x.get("chunk_id", 0))

    return groups


def format_md_section_for_llm(
    chunks: List[Dict[str, Any]],
    bank_name: str,
    quarter: str,
    fiscal_year: int,
) -> str:
    """
    Format the entire MD section for LLM processing.

    Groups chunks by speaker_block_id and presents them in order.

    Args:
        chunks: List of MD chunks (will be grouped internally)
        bank_name: Bank name for context
        quarter: Quarter
        fiscal_year: Fiscal year

    Returns:
        Formatted text of the entire MD section
    """
    if not chunks:
        return ""

    groups = group_chunks_by_speaker_block(chunks)
    sorted_block_ids = sorted(groups.keys())

    formatted = f"""# Management Discussion Section

**Bank:** {bank_name}
**Period:** {quarter} {fiscal_year}
**Total Speaker Blocks:** {len(sorted_block_ids)}

---

"""

    for block_id in sorted_block_ids:
        block_chunks = groups[block_id]

        block_summary = block_chunks[0].get("block_summary", "")
        classification_names = block_chunks[0].get("classification_names", [])

        full_content = "\n\n".join(
            chunk.get("content", "") for chunk in block_chunks if chunk.get("content")
        )

        formatted += f"## Speaker Block {block_id}\n\n"

        if block_summary:
            formatted += f"**Summary:** {block_summary}\n"

        if classification_names:
            formatted += f"**Topics:** {', '.join(classification_names)}\n"

        formatted += f"\n{full_content}\n\n---\n\n"

    return formatted
