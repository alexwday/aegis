"""
Transcript retrieval and formatting utilities for Key Themes ETL.

Copied from aegis.model.subagents.transcripts to make ETL self-contained.
"""

from typing import Any, Dict, List
from sqlalchemy import text

from aegis.utils.logging import get_logger
from aegis.connections.postgres_connector import get_connection


async def get_filter_diagnostics(combo: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get diagnostic counts for each filter to help debug why queries return 0 results.

    Returns dict with counts for:
    - Total records in table
    - Records matching bank_id
    - Records matching fiscal_year
    - Records matching quarter
    - Records matching all filters
    """
    logger = get_logger()

    diagnostics = {}

    try:
        async with get_connection() as conn:
            # Total records
            result = await conn.execute(text("SELECT COUNT(*) FROM aegis_transcripts"))
            diagnostics["total_records"] = result.scalar()

            # Records matching bank_id
            result = await conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM aegis_transcripts
                    WHERE institution_id::text = :bank_id_str
                    """
                ),
                {"bank_id_str": str(combo["bank_id"])},
            )
            diagnostics["matching_bank_id"] = result.scalar()

            # Records matching fiscal_year
            result = await conn.execute(
                text("SELECT COUNT(*) FROM aegis_transcripts WHERE fiscal_year = :fiscal_year"),
                {"fiscal_year": combo["fiscal_year"]},
            )
            diagnostics["matching_year"] = result.scalar()

            # Records matching quarter
            result = await conn.execute(
                text("SELECT COUNT(*) FROM aegis_transcripts WHERE fiscal_quarter = :quarter"),
                {"quarter": combo["quarter"]},
            )
            diagnostics["matching_quarter"] = result.scalar()

            # Records matching bank + year
            result = await conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM aegis_transcripts
                    WHERE institution_id::text = :bank_id_str
                    AND fiscal_year = :fiscal_year
                """
                ),
                {"bank_id_str": str(combo["bank_id"]), "fiscal_year": combo["fiscal_year"]},
            )
            diagnostics["matching_bank_and_year"] = result.scalar()

            # Records matching bank + quarter
            result = await conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM aegis_transcripts
                    WHERE institution_id::text = :bank_id_str
                    AND fiscal_quarter = :quarter
                """
                ),
                {"bank_id_str": str(combo["bank_id"]), "quarter": combo["quarter"]},
            )
            diagnostics["matching_bank_and_quarter"] = result.scalar()

            # Records matching year + quarter
            result = await conn.execute(
                text(
                    """
                    SELECT COUNT(*) FROM aegis_transcripts
                    WHERE fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                """
                ),
                {"fiscal_year": combo["fiscal_year"], "quarter": combo["quarter"]},
            )
            diagnostics["matching_year_and_quarter"] = result.scalar()

            # Records matching all filters
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
                    "bank_id_str": str(combo["bank_id"]),
                    "fiscal_year": combo["fiscal_year"],
                    "quarter": combo["quarter"],
                },
            )
            diagnostics["matching_all_filters"] = result.scalar()

            # Get sample institution_ids if no match
            if diagnostics["matching_all_filters"] == 0:
                result = await conn.execute(
                    text(
                        """
                        SELECT DISTINCT institution_id, company_name
                        FROM aegis_transcripts
                        WHERE fiscal_year = :fiscal_year
                        AND fiscal_quarter = :quarter
                        LIMIT 5
                    """
                    ),
                    {"fiscal_year": combo["fiscal_year"], "quarter": combo["quarter"]},
                )
                sample_banks = [(row[0], row[1]) for row in result]
                diagnostics["sample_available_banks"] = sample_banks

    except Exception as e:
        logger.error(f"Failed to get filter diagnostics: {e}")
        diagnostics["error"] = str(e)

    return diagnostics


async def retrieve_full_section(
    combo: Dict[str, Any], sections: str, context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Method 0: Retrieve full transcript sections.

    Args:
        combo: Bank-period combination with bank_id, fiscal_year, quarter
        sections: "MD" for Management Discussion, "QA" for Q&A, "ALL" for both
        context: Execution context

    Returns:
        List of transcript chunks for the specified sections
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    # Map sections to database values
    section_filter = {
        "MD": ["MANAGEMENT DISCUSSION SECTION"],
        "QA": ["Q&A"],
        "ALL": ["MANAGEMENT DISCUSSION SECTION", "Q&A"],
    }

    sections_to_fetch = section_filter.get(sections, ["MANAGEMENT DISCUSSION SECTION", "Q&A"])

    # Get diagnostics if no results expected
    diagnostics = await get_filter_diagnostics(combo, context)

    # Log the filter parameters and diagnostic counts
    logger.info(
        "etl.key_themes.filter_diagnostics",
        execution_id=execution_id,
        filters={
            "bank_id": combo["bank_id"],
            "fiscal_year": combo["fiscal_year"],
            "quarter": combo["quarter"],
            "sections": sections,
        },
        diagnostics=diagnostics,
    )

    try:
        async with get_connection() as conn:
            # Build query to fetch all chunks for specified sections
            # Handle both TEXT and INTEGER institution_id columns
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
                    AND section_name = ANY(:sections)
                ORDER BY
                    CASE WHEN section_name = 'Q&A' THEN qa_group_id ELSE speaker_block_id END,
                    chunk_id
            """
            )

            result = await conn.execute(
                query,
                {
                    "bank_id_str": str(combo["bank_id"]),
                    "fiscal_year": combo["fiscal_year"],
                    "quarter": combo["quarter"],
                    "sections": sections_to_fetch,
                },
            )

            chunks = []
            for row in result:
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

            # Enhanced logging with diagnostic info if no results
            if len(chunks) == 0 and diagnostics.get("matching_all_filters", 0) == 0:
                logger.warning(
                    "etl.key_themes.no_results_found",
                    execution_id=execution_id,
                    bank_id_requested=combo["bank_id"],
                    year_requested=combo["fiscal_year"],
                    quarter_requested=combo["quarter"],
                    total_records_in_db=diagnostics.get("total_records", 0),
                    matching_bank_only=diagnostics.get("matching_bank_id", 0),
                    matching_year_only=diagnostics.get("matching_year", 0),
                    matching_quarter_only=diagnostics.get("matching_quarter", 0),
                    matching_bank_and_year=diagnostics.get("matching_bank_and_year", 0),
                    matching_bank_and_quarter=diagnostics.get("matching_bank_and_quarter", 0),
                    matching_year_and_quarter=diagnostics.get("matching_year_and_quarter", 0),
                    sample_available_banks=diagnostics.get("sample_available_banks", []),
                )

            logger.info(
                "etl.key_themes.full_section_retrieval",
                execution_id=execution_id,
                bank=combo["bank_symbol"],
                period=f"{combo['quarter']} {combo['fiscal_year']}",
                sections=sections,
                chunks_retrieved=len(chunks),
            )

            return chunks

    except Exception as e:
        logger.error("etl.key_themes.full_section_error", execution_id=execution_id, error=str(e))
        return []


def format_priority_blocks_for_synthesis(blocks: List[Dict[str, Any]]) -> str:
    """
    Format priority blocks for prepending to Method 0 or Method 1 content.

    Provides clear section with explanation of purpose and potential duplication.

    Args:
        blocks: List of priority blocks from get_priority_blocks()

    Returns:
        Formatted string for prepending to retrieved content
    """
    if not blocks:
        return ""

    formatted = """## 🔥 PRIORITY CONTENT (Highest Relevance Blocks)

The blocks below were identified as most relevant to your query via similarity search.
They may also appear in the full content below. Focus your synthesis on these priority blocks
while using the full content for additional context.

---

"""

    for i, block in enumerate(blocks, 1):
        block_id = block.get("block_id", "N/A")
        section = block.get("section_name", "Unknown")
        similarity = block.get("similarity_score", 0.0)
        summary = block.get("block_summary", "")
        content = block.get("full_content", "")
        # Format categories for user visibility
        cat_ids = block.get("classification_ids", [])
        cat_names = block.get("classification_names", [])

        if cat_ids and cat_names:
            categories_str = ", ".join(cat_names)
        else:
            categories_str = "Uncategorized"

        # Build block entry
        formatted += f"""### Priority Block {i} | Relevance: {similarity:.1%}

**Metadata:**
- Section: {section}
- Block ID: {block_id}
- Categories: {categories_str}
"""

        if summary:
            formatted += f"- Summary: {summary}\n"

        formatted += f"""
**Content:**
{content}

---

"""

    formatted += "\n## FULL TRANSCRIPT CONTENT\n\n"

    return formatted


async def format_full_section_chunks(
    chunks: List[Dict[str, Any]],
    combo: Dict[str, Any],
    context: Dict[str, Any],
    priority_blocks: List[Dict[str, Any]] = None,
) -> str:
    """
    Format chunks retrieved via Method 0 (Full Section).
    Preserves all content without truncation.

    Args:
        chunks: Retrieved transcript chunks
        combo: Bank-period combination
        context: Execution context
        priority_blocks: Optional priority blocks to prepend (for hybrid retrieval)

    Returns:
        Formatted transcript text with ALL content
    """
    if not chunks:
        return "No transcript data available."

    logger = get_logger()
    execution_id = context.get("execution_id")

    # Filter out chunks missing proper IDs
    valid_chunks = []
    for chunk in chunks:
        if chunk.get("section_name") == "Q&A":
            # Q&A chunks must have qa_group_id
            if chunk.get("qa_group_id") is not None:
                valid_chunks.append(chunk)
            else:
                logger.warning(f"Removed Q&A chunk missing qa_group_id: {chunk.get('id')}")
        elif chunk.get("section_name") == "MANAGEMENT DISCUSSION SECTION":
            # MD chunks must have speaker_block_id
            if chunk.get("speaker_block_id") is not None:
                valid_chunks.append(chunk)
            else:
                logger.warning(f"Removed MD chunk missing speaker_block_id: {chunk.get('id')}")
        else:
            # Keep other chunks if they have either ID
            if chunk.get("speaker_block_id") is not None or chunk.get("qa_group_id") is not None:
                valid_chunks.append(chunk)

    chunks = valid_chunks

    if not chunks:
        return "No valid transcript data available (all chunks missing required IDs)."

    # Get title from first chunk (same for all chunks in combo)
    title = chunks[0].get("title", "Earnings Call Transcript") if chunks else "Transcript"

    # Build header
    formatted = f"""**Institution Details**
- Institution ID: {combo['bank_id']}
- Ticker: {combo['bank_symbol']}
- Company: {combo['bank_name']}
- Period: {combo['quarter']} {combo['fiscal_year']}
- Title: {title}

---

"""

    # Prepend priority blocks if provided
    if priority_blocks:
        formatted += format_priority_blocks_for_synthesis(priority_blocks)

    # Sort chunks by section, then by speaker_block_id for MD or qa_group_id for Q&A, then chunk_id
    sorted_chunks = sorted(
        chunks,
        key=lambda x: (
            0 if x.get("section_name") == "MANAGEMENT DISCUSSION SECTION" else 1,
            (
                x.get("qa_group_id", 0)
                if x.get("section_name") == "Q&A"
                else x.get("speaker_block_id", 0)
            ),
            x.get("chunk_id", 0),
        ),
    )

    # Group by section
    current_section = None
    current_qa_group = None
    section_num = 0
    qa_count = 0

    # Count total Q&A groups for logging
    qa_groups = set(
        chunk.get("qa_group_id")
        for chunk in sorted_chunks
        if chunk.get("section_name") == "Q&A" and chunk.get("qa_group_id")
    )
    if qa_groups:
        logger.info(
            f"Formatting {len(qa_groups)} Q&A exchanges",
            execution_id=execution_id,
            qa_group_ids=sorted(qa_groups),
        )

    for chunk in sorted_chunks:
        section_name = chunk.get("section_name", "Unknown")

        # New section
        if section_name != current_section:
            section_num += 1
            current_section = section_name
            formatted += f"\n## Section {section_num}: {section_name}\n\n"
            current_qa_group = None
            qa_count = 0

        # Handle Q&A grouping
        if section_name == "Q&A" and chunk.get("qa_group_id"):
            if chunk["qa_group_id"] != current_qa_group:
                current_qa_group = chunk["qa_group_id"]
                qa_count += 1
                formatted += f"\n### Question {qa_count} (Q&A Group {current_qa_group})\n\n"

        # Include full content without truncation
        content = chunk.get("content", "")
        if content:
            formatted += f"{content}\n\n"

    return formatted
