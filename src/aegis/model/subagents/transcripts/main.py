"""
Transcripts subagent - retrieves and analyzes earnings call transcripts.
Enhanced with pattern matching for better retrieval method selection.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Generator, List
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from ....utils.logging import get_logger
from ....utils.prompt_loader import load_subagent_prompt
from ....utils.settings import config
from ....connections.llm_connector import complete_with_tools
from ....utils.monitor import add_monitor_entry

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
from .utils import load_financial_categories

# Import retrieval functions
from .retrieval import (
    retrieve_full_section,
    retrieve_by_categories,
    retrieve_by_similarity
)


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
    Main transcripts agent with enhanced retrieval logic.
    
    Key features:
    - Pattern matching for investor questions → retrieves full Q&A section
    - Pattern matching for management commentary → retrieves full MD section
    - Category-based retrieval for financial topics
    - Similarity search for specific queries that don't match patterns
    
    Args:
        conversation: Full chat history
        latest_message: Most recent user message
        bank_period_combinations: List of bank-period combos to query
        basic_intent: Simple interpretation of query
        full_intent: Detailed interpretation
        database_id: "transcripts"
        context: Runtime context with auth and execution_id
        
    Yields:
        Dict with type="subagent", name="transcripts", content=research
    """
    
    # Initialize logging and tracking
    logger = get_logger()
    execution_id = context.get("execution_id")
    stage_start = datetime.now(timezone.utc)
    
    logger.info(
        f"subagent.{database_id}.started",
        execution_id=execution_id,
        latest_message=latest_message[:100],  # Only truncate for logging display
        num_combinations=len(bank_period_combinations),
        basic_intent=basic_intent[:100]  # Only truncate for logging display
    )
    
    try:
        # Load financial categories
        categories = load_financial_categories()
        
        # ==================================================
        # STEP 1: Enhanced prompt for retrieval method decision
        # ==================================================
        
        try:
            system_prompt = load_subagent_prompt(database_id)
            logger.debug(f"subagent.{database_id}.prompt_loaded", execution_id=execution_id)
        except Exception as e:
            logger.warning(f"Failed to load prompt: {e}, using fallback")
            system_prompt = """You are a transcripts analysis agent that retrieves earnings call transcripts.

IMPORTANT RULES FOR METHOD SELECTION:
1. If the query is about "what investors asked", "questions asked", "analyst questions", "Q&A" → Use method 0 with sections="QA"
2. If the query is about "management said", "CEO commentary", "management discussion" → Use method 0 with sections="MD"  
3. If the query wants both management and analyst perspectives → Use method 0 with sections="ALL"
4. If the query is about specific financial topics (revenue, margins, expenses, etc.) → Use method 1 with relevant category IDs
5. Only use method 2 (similarity search) for very specific queries that don't fit the above patterns

Category mapping for method 1:
0: Revenue & Growth | 1: Margins & Profitability | 2: Expenses & Costs
3: Credit & Risk | 4: Capital & Liquidity | 5: Balance Sheet
6: Guidance & Outlook | 7: Business Segments | 8: Digital & Technology  
9: ESG & Sustainability | 10: M&A & Strategy | 11: Market & Competition
12: Regulatory & Compliance | 13: Macroeconomic | 14: Operations
15: Customer | 16: Products & Services | 17: International
18: Investments | 19: Dividends & Buybacks | 20: Management & Governance
21: Real Estate | 22: Other Financial Topics"""
        
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
                        },
                        "reasoning": {
                            "type": "string",
                            "description": "Brief explanation of why this method was chosen"
                        }
                    },
                    "required": ["method", "reasoning"]
                }
            }
        }
        
        # ==================================================
        # STEP 2: Parallel retrieval decisions with pattern matching
        # ==================================================
        
        def determine_retrieval_method(combo):
            """Enhanced helper function with pattern matching for better decisions."""
            start_time = datetime.now(timezone.utc)
            
            # Check for patterns that should trigger full section retrieval
            lower_message = latest_message.lower()
            lower_intent = full_intent.lower()
            
            # Pattern matching for QA section
            qa_patterns = [
                "investor", "analyst", "question", "asked", "q&a", "q and a",
                "what are they asking", "concerns raised", "issues raised",
                "inquir", "respond to", "answer"
            ]
            
            # Pattern matching for MD section
            md_patterns = [
                "management said", "management discuss", "ceo", "cfo", 
                "executive", "prepared remarks", "opening remarks",
                "management comment", "leadership", "outlook provided"
            ]
            
            # Check patterns first for quick decision
            qa_match = any(pattern in lower_message or pattern in lower_intent for pattern in qa_patterns)
            md_match = any(pattern in lower_message or pattern in lower_intent for pattern in md_patterns)
            
            # If clear pattern match, use it directly without LLM call
            if qa_match and not md_match:
                logger.info(
                    f"Pattern match detected for QA section",
                    bank=combo['bank_symbol'],
                    patterns_found=[p for p in qa_patterns if p in lower_message or p in lower_intent]
                )
                return {
                    "combo": combo,
                    "decision": {
                        "method": 0,
                        "sections": "QA",
                        "reasoning": "Query is about investor/analyst questions - retrieving full Q&A section"
                    },
                    "success": True
                }
            elif md_match and not qa_match:
                logger.info(
                    f"Pattern match detected for MD section",
                    bank=combo['bank_symbol'],
                    patterns_found=[p for p in md_patterns if p in lower_message or p in lower_intent]
                )
                return {
                    "combo": combo,
                    "decision": {
                        "method": 0,
                        "sections": "MD",
                        "reasoning": "Query is about management commentary - retrieving full MD section"
                    },
                    "success": True
                }
            elif qa_match and md_match:
                logger.info(
                    f"Pattern match detected for both sections",
                    bank=combo['bank_symbol']
                )
                return {
                    "combo": combo,
                    "decision": {
                        "method": 0,
                        "sections": "ALL",
                        "reasoning": "Query involves both management and analyst perspectives - retrieving full transcript"
                    },
                    "success": True
                }
            
            # No clear pattern match, use LLM for decision
            user_prompt = f"""Determine the optimal retrieval method for this transcript query.

Bank: {combo['bank_name']} ({combo['bank_symbol']})
Period: {combo['quarter']} {combo['fiscal_year']}
Query Intent: {full_intent}
Latest Message: {latest_message}

REMEMBER THE PRIORITY:
1. Full QA section for investor/analyst questions  
2. Full MD section for management commentary
3. Categories for specific financial topics
4. Similarity search only as last resort for very specific queries

Select the method and provide reasoning for your choice."""
            
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
                        "model": model_config.model
                        # Use defaults from config for all parameters
                    }
                )
                
                # Parse the tool call
                if response.get("choices") and response["choices"][0].get("message"):
                    message = response["choices"][0]["message"]
                    if message.get("tool_calls"):
                        tool_call = message["tool_calls"][0]
                        function_args = json.loads(tool_call["function"]["arguments"])
                        
                        logger.info(
                            f"LLM retrieval decision",
                            bank=combo['bank_symbol'],
                            method=function_args.get('method'),
                            reasoning=function_args.get('reasoning', 'No reasoning provided')
                        )
                        
                        return {
                            "combo": combo,
                            "decision": function_args,
                            "success": True
                        }
                
                # Fallback if no tool call
                logger.warning(f"No tool call from LLM, using fallback")
                return {
                    "combo": combo,
                    "decision": {
                        "method": 2,
                        "search_phrase": f"{combo['bank_symbol']} {latest_message} {combo['quarter']} {combo['fiscal_year']}",
                        "reasoning": "LLM did not provide tool call, falling back to similarity search"
                    },
                    "success": False
                }
                
            except Exception as e:
                logger.error(f"LLM call failed for {combo['bank_symbol']}: {e}")
                return {
                    "combo": combo,
                    "decision": {
                        "method": 2,
                        "search_phrase": f"{combo['bank_symbol']} {latest_message} {combo['quarter']} {combo['fiscal_year']}",
                        "reasoning": f"Error in LLM call: {str(e)}"
                    },
                    "success": False
                }
        
        # Execute parallel retrieval decisions
        retrieval_decisions = {}
        
        with ThreadPoolExecutor(max_workers=min(10, len(bank_period_combinations))) as executor:
            future_to_combo = {
                executor.submit(determine_retrieval_method, combo): combo 
                for combo in bank_period_combinations
            }
            
            # Collect results
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
            reasoning = decision.get("reasoning", "No reasoning provided")
            
            # Log the decision
            logger.info(
                f"subagent.{database_id}.retrieval_method_selected",
                execution_id=execution_id,
                bank=combo['bank_symbol'],
                period=f"{combo['quarter']} {combo['fiscal_year']}",
                method=method,
                reasoning=reasoning
            )
            
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
                    chunk_count=len(chunks),
                    reasoning=reasoning
                )
                
                formatted_content = format_full_section_chunks(chunks, combo, context)
                
            elif method == 1:
                # Category-based Retrieval
                category_ids = decision.get("category_ids", [])
                chunks = retrieve_by_categories(combo, category_ids, context)
                
                logger.info(
                    f"subagent.{database_id}.categories_retrieved",
                    execution_id=execution_id,
                    bank=combo['bank_symbol'],
                    category_ids=category_ids,
                    chunk_count=len(chunks),
                    reasoning=reasoning
                )
                
                formatted_content = format_category_or_similarity_chunks(chunks, combo, context, note_gaps=True)
                
            elif method == 2:
                # Similarity Search with full pipeline
                search_phrase = decision.get("search_phrase", "")
                chunks = retrieve_by_similarity(combo, search_phrase, context)
                
                # Apply reranking, expansion, and gap filling
                chunks = rerank_similarity_chunks(chunks, search_phrase, context)
                chunks = expand_speaker_blocks(chunks, combo, context)
                chunks = fill_gaps_in_speaker_blocks(chunks, combo, context)
                
                logger.info(
                    f"subagent.{database_id}.similarity_retrieved",
                    execution_id=execution_id,
                    bank=combo['bank_symbol'],
                    search_phrase=search_phrase[:50],  # Only truncate for logging display,
                    chunk_count=len(chunks),
                    reasoning=reasoning
                )
                
                formatted_content = format_category_or_similarity_chunks(chunks, combo, context, note_gaps=True)
            
            # Generate research statement for this combination
            if chunks and formatted_content:
                # Always generate a synthesized research statement
                # Pass the method type to generate appropriate detail level
                research_statement = generate_research_statement(
                    formatted_content, 
                    combo, 
                    context,
                    method=method,
                    method_reasoning=reasoning
                )
                all_research_statements.append(research_statement)
                
                logger.info(
                    f"subagent.{database_id}.research_generated",
                    execution_id=execution_id,
                    bank=combo['bank_symbol'],
                    period=f"{combo['quarter']} {combo['fiscal_year']}",
                    method=method,
                    chunks_used=len(chunks),
                    reasoning=reasoning
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
                "banks": [combo["bank_id"] for combo in bank_period_combinations],
                "retrieval_methods": {
                    key: result["decision"]["method"] 
                    for key, result in retrieval_decisions.items()
                }
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