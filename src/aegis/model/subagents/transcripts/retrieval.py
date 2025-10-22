"""
Retrieval functions for transcripts subagent.
"""

from typing import Any, Dict, List
from sqlalchemy import text

from ....utils.logging import get_logger
from ....connections.postgres_connector import get_connection
from ....connections.llm_connector import embed

from .utils import get_filter_diagnostics


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
        "subagent.transcripts.filter_diagnostics",
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
                WHERE (institution_id = :bank_id_str OR institution_id::text = :bank_id_str)
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
                    "subagent.transcripts.no_results_found",
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
                "subagent.transcripts.full_section_retrieval",
                execution_id=execution_id,
                bank=combo["bank_symbol"],
                period=f"{combo['quarter']} {combo['fiscal_year']}",
                sections=sections,
                chunks_retrieved=len(chunks),
            )

            return chunks

    except Exception as e:
        logger.error(
            "subagent.transcripts.full_section_error", execution_id=execution_id, error=str(e)
        )
        return []


async def retrieve_by_categories(
    combo: Dict[str, Any], category_ids: List[int], context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Method 1: Retrieve chunks by category IDs.

    Args:
        combo: Bank-period combination with bank_id, fiscal_year, quarter
        category_ids: List of category IDs to filter by
        context: Execution context

    Returns:
        List of transcript chunks matching the categories
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    # Get diagnostics
    diagnostics = await get_filter_diagnostics(combo, context)

    # Log the filter parameters and diagnostic counts
    logger.info(
        "subagent.transcripts.filter_diagnostics_category",
        execution_id=execution_id,
        filters={
            "bank_id": combo["bank_id"],
            "fiscal_year": combo["fiscal_year"],
            "quarter": combo["quarter"],
            "category_ids": category_ids,
        },
        diagnostics=diagnostics,
    )

    try:
        async with get_connection() as conn:
            # Query chunks that contain any of the specified category IDs
            # Convert integer array to text array for comparison
            category_ids_text = [str(cat_id) for cat_id in category_ids]

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
                WHERE (institution_id = :bank_id_str OR institution_id::text = :bank_id_str)
                    AND fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                    AND classification_ids && :category_ids
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
                    "category_ids": category_ids_text,
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
                    "subagent.transcripts.no_results_category",
                    execution_id=execution_id,
                    bank_id_requested=combo["bank_id"],
                    year_requested=combo["fiscal_year"],
                    quarter_requested=combo["quarter"],
                    category_ids_requested=category_ids,
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
                "subagent.transcripts.category_retrieval",
                execution_id=execution_id,
                bank=combo["bank_symbol"],
                period=f"{combo['quarter']} {combo['fiscal_year']}",
                category_ids=category_ids,
                chunks_retrieved=len(chunks),
            )

            return chunks

    except Exception as e:
        logger.error("subagent.transcripts.category_error", execution_id=execution_id, error=str(e))
        return []


async def retrieve_by_similarity(
    combo: Dict[str, Any], search_phrase: str, context: Dict[str, Any], top_k: int = 20
) -> List[Dict[str, Any]]:
    """
    Method 2: Retrieve chunks by similarity search.

    Args:
        combo: Bank-period combination with bank_id, fiscal_year, quarter
        search_phrase: The phrase to embed and search for
        context: Execution context
        top_k: Number of top results to return (default 20)

    Returns:
        List of transcript chunks most similar to the search phrase
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    # Get diagnostics
    diagnostics = await get_filter_diagnostics(combo, context)

    # Log the filter parameters and diagnostic counts
    logger.info(
        "subagent.transcripts.filter_diagnostics_similarity",
        execution_id=execution_id,
        filters={
            "bank_id": combo["bank_id"],
            "fiscal_year": combo["fiscal_year"],
            "quarter": combo["quarter"],
            "search_phrase": search_phrase[:50],  # First 50 chars
        },
        diagnostics=diagnostics,
    )

    try:
        # Create embedding for the search phrase
        embedding_response = await embed(input_text=search_phrase, context=context)

        if not embedding_response or "data" not in embedding_response:
            logger.error("Failed to create embedding for search phrase")
            return []

        embedding_vector = embedding_response["data"][0]["embedding"]

        # Format embedding for PostgreSQL
        embedding_str = f"[{','.join(map(str, embedding_vector))}]"

        async with get_connection() as conn:
            # Similarity search using cosine distance (<=>)
            # Note: PostgreSQL pgvector uses <=> for cosine distance
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
                    title,
                    chunk_embedding <=> CAST(:embedding AS vector) AS distance
                FROM aegis_transcripts
                WHERE (institution_id = :bank_id_str OR institution_id::text = :bank_id_str)
                    AND fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                    AND chunk_embedding IS NOT NULL
                ORDER BY chunk_embedding <=> CAST(:embedding AS vector)
                LIMIT :top_k
            """
            )

            result = await conn.execute(
                query,
                {
                    "bank_id_str": str(combo["bank_id"]),
                    "fiscal_year": combo["fiscal_year"],
                    "quarter": combo["quarter"],
                    "embedding": embedding_str,
                    "top_k": top_k,
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
                        "similarity_score": 1.0 - float(row[10]),  # Convert distance to similarity
                    }
                )

            # Enhanced logging with diagnostic info if no results
            if len(chunks) == 0 and diagnostics.get("matching_all_filters", 0) == 0:
                logger.warning(
                    "subagent.transcripts.no_results_similarity",
                    execution_id=execution_id,
                    bank_id_requested=combo["bank_id"],
                    year_requested=combo["fiscal_year"],
                    quarter_requested=combo["quarter"],
                    search_phrase=search_phrase[:50],
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
                "subagent.transcripts.similarity_retrieval",
                execution_id=execution_id,
                bank=combo["bank_symbol"],
                period=f"{combo['quarter']} {combo['fiscal_year']}",
                search_phrase=search_phrase[:50],
                chunks_retrieved=len(chunks),
                top_similarity=chunks[0]["similarity_score"] if chunks else 0,
            )

            return chunks

    except Exception as e:
        logger.error(
            "subagent.transcripts.similarity_error", execution_id=execution_id, error=str(e)
        )
        return []


async def expand_chunks_to_blocks(
    chunks: List[Dict[str, Any]], combo: Dict[str, Any], context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Programmatically expand chunks to complete speaker blocks or QA groups.

    For MD chunks: Expands to complete speaker_block_id
    For QA chunks: Expands to complete qa_group_id

    This is purely programmatic - no LLM calls, no reranking, no gap filling.
    Just retrieves all chunks belonging to the same block/group.

    Args:
        chunks: List of chunks to expand
        combo: Bank-period combination
        context: Execution context

    Returns:
        List of complete block dictionaries, each containing:
        - block_type: "speaker_block" or "qa_group"
        - block_id: speaker_block_id or qa_group_id
        - section_name: "MANAGEMENT DISCUSSION SECTION" or "Q&A"
        - block_summary: Summary of the block (if available)
        - classification_ids: Array of category IDs
        - classification_names: Array of category names
        - chunks: List of all chunks in this block/group (sorted by chunk_id)
        - similarity_score: Highest similarity score among chunks in this block
        - full_content: Concatenated content of all chunks
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if not chunks:
        return []

    # Identify unique block identifiers
    md_block_ids = set()
    qa_group_ids = set()
    chunk_similarity_map = {}  # Track highest similarity per block/group

    for chunk in chunks:
        section = chunk.get("section_name")
        similarity = chunk.get("similarity_score", 0.0)

        if section == "MANAGEMENT DISCUSSION SECTION" and chunk.get("speaker_block_id"):
            block_id = chunk["speaker_block_id"]
            md_block_ids.add(block_id)
            # Track highest similarity score for this block
            if block_id not in chunk_similarity_map or similarity > chunk_similarity_map[block_id]:
                chunk_similarity_map[block_id] = similarity

        elif section == "Q&A" and chunk.get("qa_group_id"):
            group_id = chunk["qa_group_id"]
            qa_group_ids.add(group_id)
            # Track highest similarity score for this group
            if group_id not in chunk_similarity_map or similarity > chunk_similarity_map[group_id]:
                chunk_similarity_map[group_id] = similarity

    expanded_blocks = []

    try:
        async with get_connection() as conn:
            # Expand MD speaker blocks
            if md_block_ids:
                query = text(
                    """
                    SELECT
                        section_name,
                        speaker_block_id,
                        chunk_id,
                        chunk_content,
                        block_summary,
                        classification_ids,
                        classification_names
                    FROM aegis_transcripts
                    WHERE (institution_id = :bank_id_str OR institution_id::text = :bank_id_str)
                        AND fiscal_year = :fiscal_year
                        AND fiscal_quarter = :quarter
                        AND section_name = 'MANAGEMENT DISCUSSION SECTION'
                        AND speaker_block_id = ANY(:block_ids)
                    ORDER BY speaker_block_id, chunk_id
                """
                )

                result = await conn.execute(
                    query,
                    {
                        "bank_id_str": str(combo["bank_id"]),
                        "fiscal_year": combo["fiscal_year"],
                        "quarter": combo["quarter"],
                        "block_ids": list(md_block_ids),
                    },
                )

                # Group chunks by speaker_block_id
                blocks_dict = {}
                for row in result:
                    block_id = row[1]
                    if block_id not in blocks_dict:
                        blocks_dict[block_id] = {
                            "block_type": "speaker_block",
                            "block_id": block_id,
                            "section_name": row[0],
                            "block_summary": row[4],
                            "classification_ids": row[5],
                            "classification_names": row[6],
                            "chunks": [],
                            "similarity_score": chunk_similarity_map.get(block_id, 0.0),
                        }

                    blocks_dict[block_id]["chunks"].append({"chunk_id": row[2], "content": row[3]})

                # Create full_content and add to expanded_blocks
                for block_id, block_data in blocks_dict.items():
                    block_data["full_content"] = "\n\n".join(
                        chunk["content"] for chunk in block_data["chunks"]
                    )
                    expanded_blocks.append(block_data)

            # Expand QA groups
            if qa_group_ids:
                query = text(
                    """
                    SELECT
                        section_name,
                        qa_group_id,
                        chunk_id,
                        chunk_content,
                        block_summary,
                        classification_ids,
                        classification_names
                    FROM aegis_transcripts
                    WHERE (institution_id = :bank_id_str OR institution_id::text = :bank_id_str)
                        AND fiscal_year = :fiscal_year
                        AND fiscal_quarter = :quarter
                        AND section_name = 'Q&A'
                        AND qa_group_id = ANY(:group_ids)
                    ORDER BY qa_group_id, chunk_id
                """
                )

                result = await conn.execute(
                    query,
                    {
                        "bank_id_str": str(combo["bank_id"]),
                        "fiscal_year": combo["fiscal_year"],
                        "quarter": combo["quarter"],
                        "group_ids": list(qa_group_ids),
                    },
                )

                # Group chunks by qa_group_id
                groups_dict = {}
                for row in result:
                    group_id = row[1]
                    if group_id not in groups_dict:
                        groups_dict[group_id] = {
                            "block_type": "qa_group",
                            "block_id": group_id,
                            "section_name": row[0],
                            "block_summary": row[4],
                            "classification_ids": row[5],
                            "classification_names": row[6],
                            "chunks": [],
                            "similarity_score": chunk_similarity_map.get(group_id, 0.0),
                        }

                    groups_dict[group_id]["chunks"].append(
                        {"chunk_id": row[2], "content": row[3]}
                    )

                # Create full_content and add to expanded_blocks
                for group_id, group_data in groups_dict.items():
                    # Combine all chunk content
                    content_parts = []
                    for chunk in group_data["chunks"]:
                        content_parts.append(chunk["content"])

                    group_data["full_content"] = "\n\n".join(content_parts)
                    expanded_blocks.append(group_data)

            logger.info(
                "subagent.transcripts.blocks_expanded",
                execution_id=execution_id,
                bank=combo["bank_symbol"],
                original_chunks=len(chunks),
                md_blocks=len(md_block_ids),
                qa_groups=len(qa_group_ids),
                total_expanded_blocks=len(expanded_blocks),
            )

            return expanded_blocks

    except Exception as e:
        logger.error(
            "subagent.transcripts.block_expansion_error", execution_id=execution_id, error=str(e)
        )
        return []


async def get_priority_blocks(
    combo: Dict[str, Any], full_intent: str, context: Dict[str, Any], top_k: int = 5
) -> List[Dict[str, Any]]:
    """
    Get priority blocks for method selection and content prepending.

    This is the main entry point for priority block retrieval:
    1. Run similarity search to get top K chunks (programmatic)
    2. Expand chunks to complete speaker blocks/QA groups (programmatic)
    3. Deduplicate by block_id/group_id
    4. Return enriched block objects with metadata

    Args:
        combo: Bank-period combination
        full_intent: Full query intent for similarity search
        context: Execution context
        top_k: Number of top chunks to retrieve before expansion (default 5)

    Returns:
        List of priority blocks (deduplicated, may be fewer than top_k)
        Each block contains: block_type, block_id, section_name, categories,
        summary, full_content, similarity_score, chunks
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "subagent.transcripts.priority_blocks_start",
        execution_id=execution_id,
        bank=combo["bank_symbol"],
        period=f"{combo['quarter']} {combo['fiscal_year']}",
        top_k=top_k,
    )

    # Step 1: Similarity search for top K chunks
    top_chunks = await retrieve_by_similarity(
        combo=combo, search_phrase=full_intent, context=context, top_k=top_k
    )

    if not top_chunks:
        logger.warning(
            "subagent.transcripts.priority_blocks_no_chunks",
            execution_id=execution_id,
            bank=combo["bank_symbol"],
        )
        return []

    # Step 2: Expand to complete blocks (programmatic, no LLM)
    priority_blocks = await expand_chunks_to_blocks(chunks=top_chunks, combo=combo, context=context)

    # Sort by similarity score descending
    priority_blocks.sort(key=lambda x: x.get("similarity_score", 0.0), reverse=True)

    logger.info(
        "subagent.transcripts.priority_blocks_complete",
        execution_id=execution_id,
        bank=combo["bank_symbol"],
        blocks_retrieved=len(priority_blocks),
        top_similarity=priority_blocks[0]["similarity_score"] if priority_blocks else 0,
    )

    return priority_blocks
