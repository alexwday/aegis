"""
Retrieval functions for transcripts subagent.
"""

from typing import Any, Dict, List
from sqlalchemy import text

from ....utils.logging import get_logger
from ....connections.postgres_connector import get_connection
from ....connections.llm_connector import embed

from .utils import get_filter_diagnostics


async def retrieve_full_section(combo: Dict[str, Any], sections: str, context: Dict[str, Any]) -> List[Dict[str, Any]]:
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
        "ALL": ["MANAGEMENT DISCUSSION SECTION", "Q&A"]
    }
    
    sections_to_fetch = section_filter.get(sections, ["MANAGEMENT DISCUSSION SECTION", "Q&A"])
    
    # Get diagnostics if no results expected
    diagnostics = await get_filter_diagnostics(combo, context)
    
    # Log the filter parameters and diagnostic counts
    logger.info(
        f"subagent.transcripts.filter_diagnostics",
        execution_id=execution_id,
        filters={
            "bank_id": combo["bank_id"],
            "fiscal_year": combo["fiscal_year"],
            "quarter": combo["quarter"],
            "sections": sections
        },
        diagnostics=diagnostics
    )
    
    try:
        async with get_connection() as conn:
            # Build query to fetch all chunks for specified sections
            # Handle both TEXT and INTEGER institution_id columns
            query = text("""
                SELECT 
                    id,
                    section_name,
                    speaker_block_id,
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
                    AND section_name = ANY(:sections)
                ORDER BY 
                    CASE WHEN section_name = 'Q&A' THEN qa_group_id ELSE speaker_block_id END,
                    chunk_id
            """)
            
            result = await conn.execute(query, {
                "bank_id_str": str(combo["bank_id"]),
                "fiscal_year": combo["fiscal_year"],
                "quarter": combo["quarter"],
                "sections": sections_to_fetch
            })
            
            chunks = []
            for row in result:
                chunks.append({
                    "id": row[0],
                    "section_name": row[1],
                    "speaker_block_id": row[2],
                    "qa_group_id": row[3],
                    "chunk_id": row[4],
                    "content": row[5],
                    "block_summary": row[6],
                    "classification_ids": row[7],
                    "classification_names": row[8]
                })
            
            # Enhanced logging with diagnostic info if no results
            if len(chunks) == 0 and diagnostics.get('matching_all_filters', 0) == 0:
                logger.warning(
                    f"subagent.transcripts.no_results_found",
                    execution_id=execution_id,
                    bank_id_requested=combo["bank_id"],
                    year_requested=combo["fiscal_year"],
                    quarter_requested=combo["quarter"],
                    total_records_in_db=diagnostics.get('total_records', 0),
                    matching_bank_only=diagnostics.get('matching_bank_id', 0),
                    matching_year_only=diagnostics.get('matching_year', 0),
                    matching_quarter_only=diagnostics.get('matching_quarter', 0),
                    matching_bank_and_year=diagnostics.get('matching_bank_and_year', 0),
                    matching_bank_and_quarter=diagnostics.get('matching_bank_and_quarter', 0),
                    matching_year_and_quarter=diagnostics.get('matching_year_and_quarter', 0),
                    sample_available_banks=diagnostics.get('sample_available_banks', [])
                )
            
            logger.info(
                f"subagent.transcripts.full_section_retrieval",
                execution_id=execution_id,
                bank=combo["bank_symbol"],
                period=f"{combo['quarter']} {combo['fiscal_year']}",
                sections=sections,
                chunks_retrieved=len(chunks)
            )
            
            return chunks
            
    except Exception as e:
        logger.error(
            f"subagent.transcripts.full_section_error",
            execution_id=execution_id,
            error=str(e)
        )
        return []


async def retrieve_by_categories(combo: Dict[str, Any], category_ids: List[int], context: Dict[str, Any]) -> List[Dict[str, Any]]:
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
        f"subagent.transcripts.filter_diagnostics_category",
        execution_id=execution_id,
        filters={
            "bank_id": combo["bank_id"],
            "fiscal_year": combo["fiscal_year"],
            "quarter": combo["quarter"],
            "category_ids": category_ids
        },
        diagnostics=diagnostics
    )
    
    try:
        async with get_connection() as conn:
            # Query chunks that contain any of the specified category IDs
            # Convert integer array to text array for comparison
            category_ids_text = [str(cat_id) for cat_id in category_ids]
            
            query = text("""
                SELECT 
                    id,
                    section_name,
                    speaker_block_id,
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
                    AND classification_ids && :category_ids
                ORDER BY 
                    CASE WHEN section_name = 'Q&A' THEN qa_group_id ELSE speaker_block_id END,
                    chunk_id
            """)
            
            result = await conn.execute(query, {
                "bank_id_str": str(combo["bank_id"]),
                "fiscal_year": combo["fiscal_year"],
                "quarter": combo["quarter"],
                "category_ids": category_ids_text
            })
            
            chunks = []
            for row in result:
                chunks.append({
                    "id": row[0],
                    "section_name": row[1],
                    "speaker_block_id": row[2],
                    "qa_group_id": row[3],
                    "chunk_id": row[4],
                    "content": row[5],
                    "block_summary": row[6],
                    "classification_ids": row[7],
                    "classification_names": row[8]
                })
            
            # Enhanced logging with diagnostic info if no results
            if len(chunks) == 0 and diagnostics.get('matching_all_filters', 0) == 0:
                logger.warning(
                    f"subagent.transcripts.no_results_category",
                    execution_id=execution_id,
                    bank_id_requested=combo["bank_id"],
                    year_requested=combo["fiscal_year"],
                    quarter_requested=combo["quarter"],
                    category_ids_requested=category_ids,
                    total_records_in_db=diagnostics.get('total_records', 0),
                    matching_bank_only=diagnostics.get('matching_bank_id', 0),
                    matching_year_only=diagnostics.get('matching_year', 0),
                    matching_quarter_only=diagnostics.get('matching_quarter', 0),
                    matching_bank_and_year=diagnostics.get('matching_bank_and_year', 0),
                    matching_bank_and_quarter=diagnostics.get('matching_bank_and_quarter', 0),
                    matching_year_and_quarter=diagnostics.get('matching_year_and_quarter', 0),
                    sample_available_banks=diagnostics.get('sample_available_banks', [])
                )
            
            logger.info(
                f"subagent.transcripts.category_retrieval",
                execution_id=execution_id,
                bank=combo["bank_symbol"],
                period=f"{combo['quarter']} {combo['fiscal_year']}",
                category_ids=category_ids,
                chunks_retrieved=len(chunks)
            )
            
            return chunks
            
    except Exception as e:
        logger.error(
            f"subagent.transcripts.category_error",
            execution_id=execution_id,
            error=str(e)
        )
        return []


async def retrieve_by_similarity(combo: Dict[str, Any], search_phrase: str, context: Dict[str, Any], top_k: int = 20) -> List[Dict[str, Any]]:
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
        f"subagent.transcripts.filter_diagnostics_similarity",
        execution_id=execution_id,
        filters={
            "bank_id": combo["bank_id"],
            "fiscal_year": combo["fiscal_year"],
            "quarter": combo["quarter"],
            "search_phrase": search_phrase[:50]  # First 50 chars
        },
        diagnostics=diagnostics
    )
    
    try:
        # Create embedding for the search phrase
        embedding_response = await embed(
            input_text=search_phrase,
            context=context
        )
        
        if not embedding_response or "data" not in embedding_response:
            logger.error("Failed to create embedding for search phrase")
            return []
        
        embedding_vector = embedding_response["data"][0]["embedding"]
        
        # Format embedding for PostgreSQL
        embedding_str = f"[{','.join(map(str, embedding_vector))}]"
        
        async with get_connection() as conn:
            # Similarity search using cosine distance (<=>)
            # Note: PostgreSQL pgvector uses <=> for cosine distance
            query = text("""
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
                    chunk_embedding <=> CAST(:embedding AS vector) AS distance
                FROM aegis_transcripts
                WHERE (institution_id = :bank_id_str OR institution_id::text = :bank_id_str)
                    AND fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                    AND chunk_embedding IS NOT NULL
                ORDER BY chunk_embedding <=> CAST(:embedding AS vector)
                LIMIT :top_k
            """)
            
            result = await conn.execute(query, {
                "bank_id_str": str(combo["bank_id"]),
                "fiscal_year": combo["fiscal_year"],
                "quarter": combo["quarter"],
                "embedding": embedding_str,
                "top_k": top_k
            })
            
            chunks = []
            for row in result:
                chunks.append({
                    "id": row[0],
                    "section_name": row[1],
                    "speaker_block_id": row[2],
                    "qa_group_id": row[3],
                    "chunk_id": row[4],
                    "content": row[5],
                    "block_summary": row[6],
                    "classification_ids": row[7],
                    "classification_names": row[8],
                    "similarity_score": 1.0 - float(row[9])  # Convert distance to similarity
                })
            
            # Enhanced logging with diagnostic info if no results
            if len(chunks) == 0 and diagnostics.get('matching_all_filters', 0) == 0:
                logger.warning(
                    f"subagent.transcripts.no_results_similarity",
                    execution_id=execution_id,
                    bank_id_requested=combo["bank_id"],
                    year_requested=combo["fiscal_year"],
                    quarter_requested=combo["quarter"],
                    search_phrase=search_phrase[:50],
                    total_records_in_db=diagnostics.get('total_records', 0),
                    matching_bank_only=diagnostics.get('matching_bank_id', 0),
                    matching_year_only=diagnostics.get('matching_year', 0),
                    matching_quarter_only=diagnostics.get('matching_quarter', 0),
                    matching_bank_and_year=diagnostics.get('matching_bank_and_year', 0),
                    matching_bank_and_quarter=diagnostics.get('matching_bank_and_quarter', 0),
                    matching_year_and_quarter=diagnostics.get('matching_year_and_quarter', 0),
                    sample_available_banks=diagnostics.get('sample_available_banks', [])
                )
            
            logger.info(
                f"subagent.transcripts.similarity_retrieval",
                execution_id=execution_id,
                bank=combo["bank_symbol"],
                period=f"{combo['quarter']} {combo['fiscal_year']}",
                search_phrase=search_phrase[:50],
                chunks_retrieved=len(chunks),
                top_similarity=chunks[0]["similarity_score"] if chunks else 0
            )
            
            return chunks
            
    except Exception as e:
        logger.error(
            f"subagent.transcripts.similarity_error",
            execution_id=execution_id,
            error=str(e)
        )
        return []