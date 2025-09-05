"""
Transcripts Subagent - Real Implementation

This subagent retrieves earnings transcript data from the aegis_transcripts table
using intelligent retrieval strategies based on query intent.

RETRIEVAL METHODS:
0. Full Section Retrieval - For summarization requests
1. Category-based Retrieval - For topic-specific queries  
2. Similarity Search - For specific questions
"""

import asyncio
import json
import yaml
import numpy as np
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import threading
import queue

# Import Aegis utilities
from ....utils.logging import get_logger
from ....utils.prompt_loader import load_subagent_prompt
from ....utils.settings import config
from ....connections.llm_connector import complete_with_tools, embed
from ....utils.monitor import add_monitor_entry, format_llm_call
from ....connections.postgres_connector import get_connection
from sqlalchemy import text


def get_filter_diagnostics(combo: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
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
    execution_id = context.get("execution_id")
    
    diagnostics = {}
    
    try:
        with get_connection() as conn:
            # Total records
            result = conn.execute(text("SELECT COUNT(*) FROM aegis_transcripts"))
            diagnostics['total_records'] = result.scalar()
            
            # Records matching bank_id
            result = conn.execute(
                text("SELECT COUNT(*) FROM aegis_transcripts WHERE institution_id = :bank_id_str OR institution_id::text = :bank_id_str"),
                {"bank_id_str": str(combo["bank_id"])}
            )
            diagnostics['matching_bank_id'] = result.scalar()
            
            # Records matching fiscal_year
            result = conn.execute(
                text("SELECT COUNT(*) FROM aegis_transcripts WHERE fiscal_year = :fiscal_year"),
                {"fiscal_year": combo["fiscal_year"]}
            )
            diagnostics['matching_year'] = result.scalar()
            
            # Records matching quarter
            result = conn.execute(
                text("SELECT COUNT(*) FROM aegis_transcripts WHERE fiscal_quarter = :quarter"),
                {"quarter": combo["quarter"]}
            )
            diagnostics['matching_quarter'] = result.scalar()
            
            # Records matching bank + year
            result = conn.execute(
                text("""
                    SELECT COUNT(*) FROM aegis_transcripts 
                    WHERE (institution_id = :bank_id_str OR institution_id::text = :bank_id_str)
                    AND fiscal_year = :fiscal_year
                """),
                {"bank_id_str": str(combo["bank_id"]), "fiscal_year": combo["fiscal_year"]}
            )
            diagnostics['matching_bank_and_year'] = result.scalar()
            
            # Records matching bank + quarter
            result = conn.execute(
                text("""
                    SELECT COUNT(*) FROM aegis_transcripts 
                    WHERE (institution_id = :bank_id_str OR institution_id::text = :bank_id_str)
                    AND fiscal_quarter = :quarter
                """),
                {"bank_id_str": str(combo["bank_id"]), "quarter": combo["quarter"]}
            )
            diagnostics['matching_bank_and_quarter'] = result.scalar()
            
            # Records matching year + quarter
            result = conn.execute(
                text("""
                    SELECT COUNT(*) FROM aegis_transcripts 
                    WHERE fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                """),
                {"fiscal_year": combo["fiscal_year"], "quarter": combo["quarter"]}
            )
            diagnostics['matching_year_and_quarter'] = result.scalar()
            
            # Records matching all filters
            result = conn.execute(
                text("""
                    SELECT COUNT(*) FROM aegis_transcripts 
                    WHERE (institution_id = :bank_id_str OR institution_id::text = :bank_id_str)
                    AND fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                """),
                {
                    "bank_id_str": str(combo["bank_id"]), 
                    "fiscal_year": combo["fiscal_year"],
                    "quarter": combo["quarter"]
                }
            )
            diagnostics['matching_all_filters'] = result.scalar()
            
            # Get sample institution_ids if no match
            if diagnostics['matching_all_filters'] == 0:
                result = conn.execute(
                    text("""
                        SELECT DISTINCT institution_id, company_name 
                        FROM aegis_transcripts 
                        WHERE fiscal_year = :fiscal_year 
                        AND fiscal_quarter = :quarter 
                        LIMIT 5
                    """),
                    {"fiscal_year": combo["fiscal_year"], "quarter": combo["quarter"]}
                )
                sample_banks = [(row[0], row[1]) for row in result]
                diagnostics['sample_available_banks'] = sample_banks
            
    except Exception as e:
        logger.error(f"Failed to get filter diagnostics: {e}")
        diagnostics['error'] = str(e)
    
    return diagnostics


def load_financial_categories() -> Dict[int, Dict[str, str]]:
    """Load financial categories from YAML file."""
    yaml_path = Path(__file__).parent.parent.parent / "prompts" / "transcripts" / "financial_categories.yaml"
    
    try:
        with open(yaml_path, 'r') as f:
            categories_data = yaml.safe_load(f)
        
        # Convert to dict keyed by ID
        categories = {}
        for cat in categories_data:
            categories[cat['id']] = {
                'name': cat['name'],
                'description': cat['description']
            }
        return categories
    except Exception as e:
        logger = get_logger()
        logger.warning(f"Failed to load financial categories: {e}")
        # Return minimal fallback categories
        return {
            0: {'name': 'Non-Relevant', 'description': 'Non-relevant content'},
            1: {'name': 'Capital Markets', 'description': 'Investment banking and capital markets'},
            2: {'name': 'Trading', 'description': 'Trading and markets revenue'},
            # ... etc
        }


def retrieve_full_section(combo: Dict[str, Any], sections: str, context: Dict[str, Any]) -> List[Dict[str, Any]]:
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
    diagnostics = get_filter_diagnostics(combo, context)
    
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
        with get_connection() as conn:
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
            
            result = conn.execute(query, {
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


def retrieve_by_categories(combo: Dict[str, Any], category_ids: List[int], context: Dict[str, Any]) -> List[Dict[str, Any]]:
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
    diagnostics = get_filter_diagnostics(combo, context)
    
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
        with get_connection() as conn:
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
            
            result = conn.execute(query, {
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


def retrieve_by_similarity(combo: Dict[str, Any], search_phrase: str, context: Dict[str, Any], top_k: int = 20) -> List[Dict[str, Any]]:
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
    diagnostics = get_filter_diagnostics(combo, context)
    
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
        embedding_response = embed(
            input_text=search_phrase,
            context=context,
            embedding_params={
                "model": "text-embedding-3-large",
                "dimensions": 3072
            }
        )
        
        if not embedding_response or "data" not in embedding_response:
            logger.error("Failed to create embedding for search phrase")
            return []
        
        embedding_vector = embedding_response["data"][0]["embedding"]
        
        # Format embedding for PostgreSQL
        embedding_str = f"[{','.join(map(str, embedding_vector))}]"
        
        with get_connection() as conn:
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
            
            result = conn.execute(query, {
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


def transcripts_agent(
    conversation: List[Dict[str, str]],
    latest_message: str,
    bank_period_combinations: List[Dict[str, Any]],
    basic_intent: str,
    full_intent: str,
    database_id: str,
    context: Dict[str, Any],
) -> Generator[Dict[str, str], None, None]:
    """
    Transcripts subagent - retrieves real transcript data from database.
    
    Uses parallel LLM calls to determine optimal retrieval strategy for each
    bank-period combination, then executes the appropriate database queries.
    """
    
    # Initialize logging and tracking
    logger = get_logger()
    execution_id = context.get("execution_id")
    stage_start = datetime.now(timezone.utc)
    
    logger.info(
        f"subagent.{database_id}.started",
        execution_id=execution_id,
        latest_message=latest_message[:100] if latest_message else "",
        num_combinations=len(bank_period_combinations),
        basic_intent=basic_intent,
    )
    
    try:
        # Load financial categories
        categories = load_financial_categories()
        
        # ==================================================
        # STEP 1: Define the retrieval method selection tool
        # ==================================================
        retrieval_tool = {
            "type": "function",
            "function": {
                "name": "select_retrieval_method",
                "description": "Select the optimal retrieval method for answering the user's query about earnings transcripts",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "method": {
                            "type": "integer",
                            "enum": [0, 1, 2],
                            "description": "Retrieval method: 0=Full Section, 1=Category-based, 2=Similarity Search"
                        },
                        "sections": {
                            "type": "string",
                            "enum": ["MD", "QA", "ALL"],
                            "description": "For method 0 (Full Section): MD=Management Discussion, QA=Q&A Section, ALL=Both sections"
                        },
                        "category_ids": {
                            "type": "array",
                            "items": {"type": "integer", "minimum": 0, "maximum": 22},
                            "description": "For method 1 (Category-based): List of category IDs (0-22) to retrieve"
                        },
                        "search_phrase": {
                            "type": "string",
                            "description": "For method 2 (Similarity Search): Optimized search phrase to embed"
                        }
                    },
                    "required": ["method"]
                }
            }
        }
        
        # ==================================================
        # STEP 2: Build system prompt with categories
        # ==================================================
        
        # Format categories for prompt
        categories_text = "\n".join([
            f"{cat_id}: {cat_info['name']}"
            for cat_id, cat_info in categories.items()
        ])
        
        def determine_retrieval_method(combo: Dict[str, Any]) -> Dict[str, Any]:
            """Determine the retrieval method for a single bank-period combination."""
            
            start_time = datetime.now(timezone.utc)
            
            # Build the prompt for this specific combination
            system_prompt = f"""You are an expert at determining the best way to retrieve information from earnings transcripts.

RETRIEVAL METHODS:
0. Full Section Retrieval - Use when user wants comprehensive summaries or overviews of entire sections
   - Sections available:
     • MD: Management Discussion Section - Opening remarks, prepared statements by CEO/CFO
     • QA: Q&A Section - Analyst questions and management answers
     • ALL: Both MD and QA sections
   
1. Category-based Retrieval - Use when user asks about specific financial topics or themes
   - Retrieves all content tagged with selected categories
   - Can select multiple relevant categories
   
2. Similarity Search - Use for specific questions requiring targeted information
   - Create a search phrase that captures the essence of what needs to be found
   - Combine bank name, period, and specific metric/topic for best results

FINANCIAL CATEGORIES (0-22):
{categories_text}

GUIDELINES FOR EACH METHOD:

Method 0 (Full Section):
- Use when: "summarize the call", "what did management say", "give me the Q&A", "full transcript"
- Choose MD for: management commentary, prepared remarks, opening statements
- Choose QA for: analyst questions, Q&A discussion, analyst concerns
- Choose ALL for: complete overview, full context needed

Method 1 (Category-based):
- Use when: asking about specific topics like expenses, revenue, loans, etc.
- Select ALL relevant categories that might contain the answer
- For broad topics, select multiple related categories
- Category 0 (Non-Relevant) should rarely be selected

Method 2 (Similarity Search):
- Use when: asking for specific metrics, precise facts, targeted information
- Create search phrase by combining:
  • Bank name or ticker symbol
  • Specific metric or topic
  • Period if relevant
  • Example: "RBC net interest margin 3.25% Q1 2025"
  • Example: "loan loss provision 25 basis points"
  • Example: "digital adoption 85% mobile users"

Remember: The intent may be the same across banks but the specific search phrase should be tailored to each bank-period combination."""
            
            user_prompt = f"""Determine the best retrieval method for this specific query:

Bank: {combo['bank_name']} ({combo['bank_symbol']})
Period: {combo['quarter']} {combo['fiscal_year']}
User Intent: {combo.get('query_intent', basic_intent)}
Original User Message: {latest_message}

Select the appropriate retrieval method (0, 1, or 2) and provide the required parameters.

For similarity search (method 2), create a search phrase that would best find the specific information for {combo['bank_symbol']} in {combo['quarter']} {combo['fiscal_year']}."""
            
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            
            # Call LLM with tool
            try:
                model_tier = "medium"
                model_config = getattr(config.llm, model_tier)
                
                response = complete_with_tools(
                    messages=messages,
                    tools=[retrieval_tool],
                    context=context,
                    llm_params={
                        "model": model_config.model,
                        "temperature": 0.3,  # Lower temperature for consistent decisions
                        "max_tokens": 300
                    }
                )
                
                end_time = datetime.now(timezone.utc)
                duration_ms = int((end_time - start_time).total_seconds() * 1000)
                
                # Parse the tool call
                if response.get("choices") and response["choices"][0].get("message"):
                    message = response["choices"][0]["message"]
                    if message.get("tool_calls"):
                        tool_call = message["tool_calls"][0]
                        function_args = json.loads(tool_call["function"]["arguments"])
                        
                        # Record LLM usage
                        if response.get("usage"):
                            usage = response["usage"]
                            cost = (model_config.cost_per_1k_input * usage.get("prompt_tokens", 0) / 1000 +
                                   model_config.cost_per_1k_output * usage.get("completion_tokens", 0) / 1000)
                            
                            llm_calls.append(format_llm_call(
                                model=model_config.model,
                                prompt_tokens=usage.get("prompt_tokens", 0),
                                completion_tokens=usage.get("completion_tokens", 0),
                                cost=cost,
                                duration_ms=duration_ms
                            ))
                        
                        logger.debug(
                            f"subagent.{database_id}.retrieval_decision",
                            execution_id=execution_id,
                            bank=combo['bank_symbol'],
                            period=f"{combo['quarter']} {combo['fiscal_year']}",
                            method=function_args.get("method")
                        )
                        
                        return {
                            "combo": combo,
                            "decision": function_args,
                            "success": True
                        }
                
                # Fallback if no tool call
                return {
                    "combo": combo,
                    "decision": {
                        "method": 2,
                        "search_phrase": f"{combo['bank_symbol']} {latest_message} {combo['quarter']} {combo['fiscal_year']}"
                    },
                    "success": False,
                    "error": "No tool call in response"
                }
                
            except Exception as e:
                logger.error(
                    f"subagent.{database_id}.retrieval_decision_error",
                    execution_id=execution_id,
                    bank=combo['bank_symbol'],
                    error=str(e)
                )
                return {
                    "combo": combo,
                    "decision": {
                        "method": 2,
                        "search_phrase": f"{combo['bank_symbol']} {latest_message} {combo['quarter']} {combo['fiscal_year']}"
                    },
                    "success": False,
                    "error": str(e)
                }
        
        # ==================================================
        # STEP 3: Execute parallel retrieval method decisions
        # ==================================================
        
        retrieval_decisions = {}
        llm_calls = []
        
        # Start yielding immediately to show progress
        yield {
            "type": "subagent",
            "name": database_id,
            "content": f"Analyzing {len(bank_period_combinations)} bank-period combinations to determine optimal retrieval strategies...\n\n"
        }
        
        # Use ThreadPoolExecutor for parallel LLM calls
        with ThreadPoolExecutor(max_workers=min(10, len(bank_period_combinations))) as executor:
            # Submit all tasks
            future_to_combo = {
                executor.submit(determine_retrieval_method, combo): combo 
                for combo in bank_period_combinations
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_combo):
                result = future.result()
                combo = result["combo"]
                key = f"{combo['bank_id']}_{combo['fiscal_year']}_{combo['quarter']}"
                retrieval_decisions[key] = result
                
                # Stream progress update
                decision = result["decision"]
                method_num = decision["method"]
                method_names = {0: "Full Section", 1: "Category-based", 2: "Similarity Search"}
                method_name = method_names.get(method_num, "Unknown")
                
                bank_symbol = combo["bank_symbol"]
                period = f"{combo['quarter']} {combo['fiscal_year']}"
                
                if result["success"]:
                    yield {
                        "type": "subagent",
                        "name": database_id,
                        "content": f"✓ {bank_symbol} {period}: Using {method_name} retrieval\n"
                    }
                else:
                    yield {
                        "type": "subagent",
                        "name": database_id,
                        "content": f"⚠️ {bank_symbol} {period}: Defaulting to similarity search\n"
                    }
        
        # ==================================================
        # STEP 4: Summary of decisions made
        # ==================================================
        
        # Count methods chosen
        method_counts = {0: 0, 1: 0, 2: 0}
        for key, result in retrieval_decisions.items():
            method = result["decision"]["method"]
            method_counts[method] = method_counts.get(method, 0) + 1
        
        yield {
            "type": "subagent",
            "name": database_id,
            "content": f"\n📊 Retrieval Strategy Summary:\n"
        }
        
        method_labels = {
            0: "Full Section Retrieval",
            1: "Category-based Retrieval", 
            2: "Similarity Search"
        }
        
        for method_num, count in method_counts.items():
            if count > 0:
                yield {
                    "type": "subagent",
                    "name": database_id,
                    "content": f"  • {method_labels[method_num]}: {count} combinations\n"
                }
        
        yield {
            "type": "subagent",
            "name": database_id,
            "content": "\n---\n\n"
        }
        
        # ==================================================
        # STEP 5: Execute retrievals based on decisions
        # ==================================================
        
        yield {
            "type": "subagent",
            "name": database_id,
            "content": "Retrieving transcript data from database...\n\n"
        }
        
        # Process each bank-period combination
        for key, result in retrieval_decisions.items():
            combo = result["combo"]
            decision = result["decision"]
            
            bank_name = combo["bank_name"]
            period = f"{combo['quarter']} {combo['fiscal_year']}"
            method = decision["method"]
            
            yield {
                "type": "subagent",
                "name": database_id,
                "content": f"**{bank_name} - {period}**\n"
            }
            
            # Execute the appropriate retrieval method
            chunks = []
            
            if method == 0:
                # Full Section Retrieval
                sections = decision.get("sections", "ALL")
                chunks = retrieve_full_section(combo, sections, context)
                
                sections_map = {"MD": "Management Discussion", "QA": "Q&A", "ALL": "Both Sections"}
                yield {
                    "type": "subagent",
                    "name": database_id,
                    "content": f"*Retrieved {len(chunks)} chunks from {sections_map.get(sections, sections)}*\n\n"
                }
                
            elif method == 1:
                # Category-based Retrieval
                category_ids = decision.get("category_ids", [])
                chunks = retrieve_by_categories(combo, category_ids, context)
                
                category_names = []
                for cat_id in category_ids:
                    if cat_id in categories:
                        category_names.append(categories[cat_id]['name'])
                
                yield {
                    "type": "subagent",
                    "name": database_id,
                    "content": f"*Retrieved {len(chunks)} chunks for categories: {', '.join(category_names)}*\n\n"
                }
                
            elif method == 2:
                # Similarity Search
                search_phrase = decision.get("search_phrase", "")
                chunks = retrieve_by_similarity(combo, search_phrase, context)
                
                yield {
                    "type": "subagent",
                    "name": database_id,
                    "content": f"*Retrieved top {len(chunks)} chunks for: \"{search_phrase}\"*\n\n"
                }
            
            # Display first few chunks as sample
            if chunks:
                yield {
                    "type": "subagent",
                    "name": database_id,
                    "content": "Sample of retrieved content:\n"
                }
                
                for i, chunk in enumerate(chunks[:2], 1):  # Show first 2 chunks
                    content_preview = chunk["content"][:200] + "..." if len(chunk["content"]) > 200 else chunk["content"]
                    
                    yield {
                        "type": "subagent",
                        "name": database_id,
                        "content": f"\nChunk {i}:\n"
                    }
                    
                    yield {
                        "type": "subagent",
                        "name": database_id,
                        "content": f"- Section: {chunk.get('section_name', 'Unknown')}\n"
                    }
                    
                    if chunk.get("block_summary"):
                        yield {
                            "type": "subagent",
                            "name": database_id,
                            "content": f"- Summary: {chunk['block_summary']}\n"
                        }
                    
                    yield {
                        "type": "subagent",
                        "name": database_id,
                        "content": f"- Content: {content_preview}\n"
                    }
                    
                    if method == 2 and "similarity_score" in chunk:
                        yield {
                            "type": "subagent",
                            "name": database_id,
                            "content": f"- Similarity: {chunk['similarity_score']:.3f}\n"
                        }
                
                if len(chunks) > 2:
                    yield {
                        "type": "subagent",
                        "name": database_id,
                        "content": f"\n... and {len(chunks) - 2} more chunks\n"
                    }
            else:
                yield {
                    "type": "subagent",
                    "name": database_id,
                    "content": "No matching chunks found.\n"
                }
            
            yield {
                "type": "subagent",
                "name": database_id,
                "content": "\n---\n\n"
            }
        
        # ==================================================
        # STEP 6: Add monitoring entry
        # ==================================================
        stage_end = datetime.now(timezone.utc)
        add_monitor_entry(
            stage_name="Subagent_Transcripts",
            stage_start_time=stage_start,
            stage_end_time=stage_end,
            status="Success",
            llm_calls=llm_calls if llm_calls else None,
            decision_details=f"Determined retrieval methods for {len(bank_period_combinations)} combinations",
            custom_metadata={
                "subagent": database_id,
                "method_counts": method_counts,
                "total_combinations": len(bank_period_combinations),
                "banks": [combo["bank_id"] for combo in bank_period_combinations]
            }
        )
        
        logger.info(
            f"subagent.{database_id}.completed",
            execution_id=execution_id,
            total_duration_ms=int((stage_end - stage_start).total_seconds() * 1000),
            retrieval_methods=method_counts
        )
        
    except Exception as e:
        # Error handling
        error_msg = str(e)
        logger.error(
            f"subagent.{database_id}.error",
            execution_id=execution_id,
            error=error_msg,
            exc_info=True
        )
        
        add_monitor_entry(
            stage_name="Subagent_Transcripts",
            stage_start_time=stage_start,
            stage_end_time=datetime.now(timezone.utc),
            status="Failure",
            error_message=error_msg,
            custom_metadata={
                "subagent": database_id,
                "error_type": type(e).__name__
            }
        )
        
        yield {
            "type": "subagent",
            "name": database_id,
            "content": f"\n⚠️ Error in Transcripts subagent: {error_msg}\n"
        }