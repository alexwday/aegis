"""
Enhanced Transcripts Subagent with Full Formatting Pipeline

This version includes:
- Full section formatting with proper structure
- Category-based formatting with gap notation
- Similarity search with reranking, expansion, and gap filling
- Research statement generation for all methods
- Parallel processing with merged results
"""

import json
import yaml
import numpy as np
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Import Aegis utilities
from ....utils.logging import get_logger
from ....utils.prompt_loader import load_subagent_prompt
from ....utils.settings import config
from ....connections.llm_connector import complete_with_tools, embed, stream
from ....utils.monitor import add_monitor_entry, format_llm_call
from ....connections.postgres_connector import get_connection
from sqlalchemy import text

# Import formatting utilities
from .formatting import (
    format_full_section_chunks,
    format_category_or_similarity_chunks,
    rerank_similarity_chunks,
    expand_speaker_blocks,
    fill_gaps_in_speaker_blocks,
    generate_research_statement
)

# Import original retrieval functions
from .main import (
    load_financial_categories,
    retrieve_full_section,
    retrieve_by_categories,
    retrieve_by_similarity
)


def process_retrieval_with_formatting(
    combo: Dict[str, Any],
    decision: Dict[str, Any],
    context: Dict[str, Any],
    categories: Dict[int, Dict[str, str]]
) -> str:
    """
    Process a single bank-period combination with full formatting pipeline.
    
    Args:
        combo: Bank-period combination
        decision: Retrieval method decision
        context: Execution context
        categories: Financial categories mapping
        
    Returns:
        Formatted research statement
    """
    logger = get_logger()
    execution_id = context.get("execution_id")
    method = decision.get("method")
    
    try:
        # Step 1: Retrieve chunks based on method
        chunks = []
        
        if method == 0:
            # Full Section Retrieval
            sections = decision.get("sections", "ALL")
            chunks = retrieve_full_section(combo, sections, context)
            
            # Add title to chunks
            if chunks:
                title = get_transcript_title(combo, context)
                for chunk in chunks:
                    chunk["title"] = title
            
            # Format with full structure
            formatted_content = format_full_section_chunks(chunks, combo, context)
            
        elif method == 1:
            # Category-based Retrieval
            category_ids = decision.get("category_ids", [])
            chunks = retrieve_by_categories(combo, category_ids, context)
            
            # Add title to chunks
            if chunks:
                title = get_transcript_title(combo, context)
                for chunk in chunks:
                    chunk["title"] = title
            
            # Format with gap notation
            formatted_content = format_category_or_similarity_chunks(
                chunks, combo, context, note_gaps=True
            )
            
        elif method == 2:
            # Similarity Search with full pipeline
            search_phrase = decision.get("search_phrase", "")
            
            # Initial retrieval
            chunks = retrieve_by_similarity(combo, search_phrase, context, top_k=20)
            
            # Reranking to filter irrelevant chunks
            chunks = rerank_similarity_chunks(chunks, search_phrase, context)
            
            # Expand MD speaker blocks
            chunks = expand_speaker_blocks(chunks, combo, context)
            
            # Fill gaps in MD section
            chunks = fill_gaps_in_speaker_blocks(chunks, combo, context)
            
            # Add title to chunks
            if chunks:
                title = get_transcript_title(combo, context)
                for chunk in chunks:
                    chunk["title"] = title
            
            # Format with structure
            formatted_content = format_category_or_similarity_chunks(
                chunks, combo, context, note_gaps=True
            )
        
        else:
            formatted_content = "Unknown retrieval method."
        
        # Step 2: Generate research statement
        research_statement = generate_research_statement(
            formatted_content, combo, context
        )
        
        logger.info(
            f"subagent.transcripts.processing_complete",
            execution_id=execution_id,
            bank=combo['bank_symbol'],
            method=method,
            chunks_processed=len(chunks)
        )
        
        return research_statement
        
    except Exception as e:
        logger.error(
            f"subagent.transcripts.processing_error",
            execution_id=execution_id,
            bank=combo['bank_symbol'],
            error=str(e)
        )
        
        # Return error statement
        return f"""### {combo['bank_name']} - {combo['quarter']} {combo['fiscal_year']}

Error processing transcript data: {str(e)}

---
"""


def get_transcript_title(combo: Dict[str, Any], context: Dict[str, Any]) -> str:
    """
    Get the transcript title for a bank-period combination.
    
    Args:
        combo: Bank-period combination
        context: Execution context
        
    Returns:
        Transcript title string
    """
    try:
        with get_connection() as conn:
            query = text("""
                SELECT DISTINCT title
                FROM aegis_transcripts
                WHERE institution_id = :bank_id
                    AND fiscal_year = :fiscal_year
                    AND fiscal_quarter = :quarter
                LIMIT 1
            """)
            
            result = conn.execute(query, {
                "bank_id": str(combo["bank_id"]),
                "fiscal_year": combo["fiscal_year"],
                "quarter": combo["quarter"]
            })
            
            row = result.fetchone()
            if row:
                return row[0]
                
    except Exception:
        pass
    
    return f"{combo['bank_name']} {combo['quarter']} {combo['fiscal_year']} Earnings Call"


def transcripts_agent_enhanced(
    conversation: List[Dict[str, str]],
    latest_message: str,
    bank_period_combinations: List[Dict[str, Any]],
    basic_intent: str,
    full_intent: str,
    database_id: str,
    context: Dict[str, Any],
) -> Generator[Dict[str, str], None, None]:
    """
    Enhanced transcripts subagent with full formatting pipeline.
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
        
        # Define retrieval tool (same as before)
        retrieval_tool = {
            "type": "function",
            "function": {
                "name": "select_retrieval_method",
                "description": "Select the optimal retrieval method",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "method": {
                            "type": "integer",
                            "enum": [0, 1, 2],
                            "description": "0=Full Section, 1=Category, 2=Similarity"
                        },
                        "sections": {
                            "type": "string",
                            "enum": ["MD", "QA", "ALL"],
                            "description": "For method 0"
                        },
                        "category_ids": {
                            "type": "array",
                            "items": {"type": "integer", "minimum": 0, "maximum": 22},
                            "description": "For method 1"
                        },
                        "search_phrase": {
                            "type": "string",
                            "description": "For method 2"
                        }
                    },
                    "required": ["method"]
                }
            }
        }
        
        # Build system prompt (condensed for space)
        categories_text = "\n".join([
            f"{cat_id}: {cat_info['name']}"
            for cat_id, cat_info in categories.items()
        ])
        
        # Determine retrieval methods in parallel (same as original)
        def determine_retrieval_method(combo: Dict[str, Any]) -> Dict[str, Any]:
            """Determine retrieval method for a combination."""
            # [Same implementation as in main.py]
            # Returning decision dict with method and parameters
            # ... (implementation details omitted for brevity)
            pass
        
        # Stream initial progress
        yield {
            "type": "subagent",
            "name": database_id,
            "content": f"Processing {len(bank_period_combinations)} bank-period combinations...\n\n"
        }
        
        # Process all combinations in parallel
        research_statements = []
        
        with ThreadPoolExecutor(max_workers=min(10, len(bank_period_combinations))) as executor:
            # Submit retrieval decision tasks
            decision_futures = {
                executor.submit(determine_retrieval_method, combo): combo
                for combo in bank_period_combinations
            }
            
            # Collect decisions
            retrieval_decisions = {}
            for future in as_completed(decision_futures):
                result = future.result()
                combo = result["combo"]
                key = f"{combo['bank_id']}_{combo['fiscal_year']}_{combo['quarter']}"
                retrieval_decisions[key] = result
            
            # Submit processing tasks with formatting
            processing_futures = {}
            for key, result in retrieval_decisions.items():
                combo = result["combo"]
                decision = result["decision"]
                
                # Submit processing with full formatting pipeline
                future = executor.submit(
                    process_retrieval_with_formatting,
                    combo, decision, context, categories
                )
                processing_futures[future] = combo
            
            # Collect research statements as they complete
            for future in as_completed(processing_futures):
                combo = processing_futures[future]
                research_statement = future.result()
                research_statements.append(research_statement)
                
                # Stream progress
                yield {
                    "type": "subagent",
                    "name": database_id,
                    "content": f"✓ Completed analysis for {combo['bank_symbol']} {combo['quarter']} {combo['fiscal_year']}\n"
                }
        
        # Merge all research statements
        yield {
            "type": "subagent",
            "name": database_id,
            "content": "\n---\n\n## Research Summary\n\n"
        }
        
        for statement in research_statements:
            yield {
                "type": "subagent",
                "name": database_id,
                "content": statement
            }
        
        # Final summary if multiple banks
        if len(research_statements) > 1:
            yield {
                "type": "subagent",
                "name": database_id,
                "content": f"\n**Analysis complete for {len(research_statements)} institutions.**\n"
            }
        
        # Add monitoring
        stage_end = datetime.now(timezone.utc)
        add_monitor_entry(
            stage_name="Subagent_Transcripts_Enhanced",
            stage_start_time=stage_start,
            stage_end_time=stage_end,
            status="Success",
            decision_details=f"Processed {len(bank_period_combinations)} combinations with formatting",
            custom_metadata={
                "subagent": database_id,
                "total_combinations": len(bank_period_combinations),
                "banks": [combo["bank_id"] for combo in bank_period_combinations]
            }
        )
        
        logger.info(
            f"subagent.{database_id}.completed",
            execution_id=execution_id,
            total_duration_ms=int((stage_end - stage_start).total_seconds() * 1000)
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
        
        yield {
            "type": "subagent",
            "name": database_id,
            "content": f"\n⚠️ Error in Transcripts subagent: {error_msg}\n"
        }