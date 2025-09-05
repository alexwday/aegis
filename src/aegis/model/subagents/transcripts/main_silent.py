"""
Transcripts subagent - silent processing version.
Only outputs final combined research statements.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Generator, List
from pathlib import Path
import json
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed

from ....utils.logging import get_logger
from ....utils.prompt_loader import load_subagent_prompt
from ....utils.settings import config
from ....connections.llm_connector import complete_with_tools, embed
from ....utils.monitor import add_monitor_entry, format_llm_call
from ....connections.postgres_connector import get_connection
from sqlalchemy import text

# Import formatting functions
from .formatting import (
    format_full_section_chunks,
    format_category_or_similarity_chunks,
    rerank_similarity_chunks,
    expand_speaker_blocks,
    fill_gaps_in_speaker_blocks,
    generate_research_statement
)

# Import utilities
from .utils import load_financial_categories, get_filter_diagnostics

# Import retrieval functions
from .retrieval import (
    retrieve_full_section,
    retrieve_by_categories,
    retrieve_by_similarity
)


def transcripts_agent_silent(
    conversation: List[Dict[str, str]],
    latest_message: str,
    bank_period_combinations: List[Dict[str, Any]],
    basic_intent: str,
    full_intent: str,
    database_id: str,
    context: Dict[str, Any],
) -> Generator[Dict[str, str], None, None]:
    """
    Silent version of transcripts subagent.
    Processes all bank-period combinations and only outputs final combined research.
    """
    
    # Initialize logging and tracking
    logger = get_logger()
    execution_id = context.get("execution_id")
    stage_start = datetime.now(timezone.utc)
    
    logger.info(
        f"subagent.{database_id}.started",
        execution_id=execution_id,
        latest_message=latest_message[:100],
        num_combinations=len(bank_period_combinations),
        basic_intent=basic_intent[:100]
    )
    
    try:
        # Load financial categories
        categories = load_financial_categories()
        
        # ==================================================
        # STEP 1: Prepare prompt for retrieval method decision
        # ==================================================
        
        try:
            system_prompt = load_subagent_prompt(database_id)
            logger.debug(f"subagent.{database_id}.prompt_loaded", execution_id=execution_id)
        except Exception as e:
            logger.warning(f"Failed to load prompt: {e}, using fallback")
            system_prompt = "You are a transcripts analysis agent that retrieves and analyzes earnings call transcripts."
        
        # Define tool for retrieval method selection
        retrieval_tool = {
            "type": "function",
            "function": {
                "name": "select_retrieval_method",
                "description": "Select the optimal method for retrieving transcript data",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "method": {
                            "type": "integer",
                            "description": "0 for full section, 1 for category-based, 2 for similarity search",
                            "enum": [0, 1, 2]
                        },
                        "sections": {
                            "type": "string",
                            "description": "For method 0: MD, QA, or ALL",
                            "enum": ["MD", "QA", "ALL"]
                        },
                        "category_ids": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": "For method 1: List of category IDs (0-22)"
                        },
                        "search_phrase": {
                            "type": "string",
                            "description": "For method 2: Search phrase to find similar content"
                        }
                    },
                    "required": ["method"]
                }
            }
        }
        
        # ==================================================
        # STEP 2: Parallel LLM calls to determine retrieval methods
        # ==================================================
        
        def determine_retrieval_method(combo):
            """Helper function for parallel LLM calls."""
            start_time = datetime.now(timezone.utc)
            
            # Build user prompt for this specific combination
            user_prompt = f"""Determine the optimal retrieval method for this transcript query.

Bank: {combo['bank_name']} ({combo['bank_symbol']})
Period: {combo['quarter']} {combo['fiscal_year']}
Query Intent: {full_intent}
Latest Message: {latest_message}

Available Methods:
0 - Full Section: Retrieve complete MD and/or Q&A sections
1 - Category-based: Retrieve chunks by financial categories (IDs 0-22)
2 - Similarity Search: Find chunks similar to a search phrase

Based on the query, select the most appropriate method and provide necessary parameters."""
            
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            
            # Call LLM with tool
            try:
                model_tier = "large"  # Use large model for better decision making
                model_config = getattr(config.llm, model_tier)
                
                response = complete_with_tools(
                    messages=messages,
                    tools=[retrieval_tool],
                    context=context,
                    llm_params={
                        "model": model_config.model,
                        "temperature": 0.3,
                        "max_tokens": 300
                    }
                )
                
                # Parse the tool call
                if response.get("choices") and response["choices"][0].get("message"):
                    message = response["choices"][0]["message"]
                    if message.get("tool_calls"):
                        tool_call = message["tool_calls"][0]
                        function_args = json.loads(tool_call["function"]["arguments"])
                        
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
                    "success": False
                }
                
            except Exception as e:
                logger.error(f"LLM call failed for {combo['bank_symbol']}: {e}")
                return {
                    "combo": combo,
                    "decision": {
                        "method": 2,
                        "search_phrase": f"{combo['bank_symbol']} {latest_message} {combo['quarter']} {combo['fiscal_year']}"
                    },
                    "success": False
                }
        
        # Execute parallel LLM calls
        retrieval_decisions = {}
        
        with ThreadPoolExecutor(max_workers=min(10, len(bank_period_combinations))) as executor:
            future_to_combo = {
                executor.submit(determine_retrieval_method, combo): combo 
                for combo in bank_period_combinations
            }
            
            # Collect results silently
            for future in as_completed(future_to_combo):
                result = future.result()
                combo = result["combo"]
                key = f"{combo['bank_id']}_{combo['fiscal_year']}_{combo['quarter']}"
                retrieval_decisions[key] = result
        
        # ==================================================
        # STEP 3: Execute retrievals and format research statements
        # ==================================================
        
        all_research_statements = []
        
        for key, result in retrieval_decisions.items():
            combo = result["combo"]
            decision = result["decision"]
            method = decision["method"]
            
            # Execute the appropriate retrieval method
            chunks = []
            
            if method == 0:
                # Full Section Retrieval
                sections = decision.get("sections", "ALL")
                chunks = retrieve_full_section(combo, sections, context)
                
                # Log what sections are in the retrieved chunks
                sections_in_chunks = set(chunk.get('section_name', 'Unknown') for chunk in chunks)
                logger.info(
                    f"subagent.{database_id}.sections_retrieved",
                    execution_id=execution_id,
                    bank=combo['bank_symbol'],
                    requested_sections=sections,
                    actual_sections=list(sections_in_chunks),
                    chunk_count=len(chunks)
                )
                
                formatted_content = format_full_section_chunks(chunks, combo, context)
                
            elif method == 1:
                # Category-based Retrieval
                category_ids = decision.get("category_ids", [])
                chunks = retrieve_by_categories(combo, category_ids, context)
                formatted_content = format_category_or_similarity_chunks(chunks, combo, context, note_gaps=True)
                
            elif method == 2:
                # Similarity Search with full pipeline
                search_phrase = decision.get("search_phrase", "")
                chunks = retrieve_by_similarity(combo, search_phrase, context)
                
                # Apply reranking, expansion, and gap filling
                chunks = rerank_similarity_chunks(chunks, search_phrase, context)
                chunks = expand_speaker_blocks(chunks, combo, context)
                chunks = fill_gaps_in_speaker_blocks(chunks, combo, context)
                
                formatted_content = format_category_or_similarity_chunks(chunks, combo, context, note_gaps=True)
            
            # Generate research statement for this combination
            if chunks and formatted_content:
                research_statement = generate_research_statement(formatted_content, combo, context)
                all_research_statements.append(research_statement)
                
                logger.info(
                    f"subagent.{database_id}.research_generated",
                    execution_id=execution_id,
                    bank=combo['bank_symbol'],
                    period=f"{combo['quarter']} {combo['fiscal_year']}",
                    method=method,
                    chunks_used=len(chunks)
                )
            else:
                # No data found
                all_research_statements.append(
                    f"\n### {combo['bank_name']} - {combo['quarter']} {combo['fiscal_year']}\n"
                    f"No transcript data available for this period.\n"
                )
        
        # ==================================================
        # STEP 4: Output final combined research
        # ==================================================
        
        # Combine all research statements
        final_output = "\n".join(all_research_statements)
        
        # Add header
        header = f"## Earnings Transcript Analysis\n\n"
        header += f"**Query**: {full_intent}\n"
        header += f"**Coverage**: {len(bank_period_combinations)} bank-period combinations\n"
        header += "\n---\n\n"
        
        # Yield the complete research output
        yield {
            "type": "subagent",
            "name": database_id,
            "content": header + final_output
        }
        
        # ==================================================
        # STEP 5: Add monitoring entry
        # ==================================================
        stage_end = datetime.now(timezone.utc)
        add_monitor_entry(
            stage_name="Subagent_Transcripts",
            stage_start_time=stage_start,
            stage_end_time=stage_end,
            status="Success",
            decision_details=f"Generated research for {len(bank_period_combinations)} combinations",
            custom_metadata={
                "subagent": database_id,
                "total_combinations": len(bank_period_combinations),
                "banks": [combo["bank_id"] for combo in bank_period_combinations]
            }
        )
        
        logger.info(
            f"subagent.{database_id}.completed",
            execution_id=execution_id,
            total_duration_ms=int((stage_end - stage_start).total_seconds() * 1000),
            research_statements=len(all_research_statements)
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