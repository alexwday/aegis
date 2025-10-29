"""
Transcripts subagent - retrieves and analyzes earnings call transcripts.
Enhanced with pattern matching for better retrieval method selection.
"""

from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List
import json
import asyncio

from ....utils.logging import get_logger
from ....utils.settings import config
from ....utils.prompt_loader import load_prompt_from_db
from ....connections.llm_connector import complete_with_tools
from ....utils.monitor import add_monitor_entry

# Import formatting functions
from .formatting import (
    format_full_section_chunks,
    format_category_or_similarity_chunks,
    format_priority_blocks_for_method_selection,
    rerank_similarity_chunks,
    expand_speaker_blocks,
    fill_gaps_in_speaker_blocks,
    generate_research_statement,
)

# Import retrieval functions
from .retrieval import (
    retrieve_full_section,
    retrieve_by_categories,
    retrieve_by_similarity,
    get_priority_blocks,
)

# Import utility functions
from .utils import load_financial_categories


async def transcripts_agent(
    conversation: List[Dict[str, str]],
    latest_message: str,
    bank_period_combinations: List[Dict[str, Any]],
    basic_intent: str,
    full_intent: str,
    database_id: str,
    context: Dict[str, Any],
) -> AsyncGenerator[Dict[str, str], None]:
    """
    Main transcripts agent with enhanced retrieval logic.

    Key features:
    - Pattern matching for investor questions → retrieves full Q&A section
    - Pattern matching for management commentary → retrieves full MD section
    - Category-based retrieval for financial topics
    - Similarity search for specific queries that don't match patterns

    Args:
        conversation: Full chat history (currently unused but required by interface)
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
        basic_intent=basic_intent[:100],  # Only truncate for logging display
    )

    try:
        # ==================================================
        # STEP 1: Load prompts for retrieval method decision
        # ==================================================

        try:
            # Load method selection prompts from database with global contexts
            method_prompts = load_prompt_from_db(
                layer="transcripts",
                name="method_selection",
                compose_with_globals=True,
                available_databases=None,  # Transcripts doesn't filter databases
                execution_id=execution_id
            )

            # Use composed prompt if available (includes fiscal, project globals)
            # Otherwise fall back to raw template
            if "composed_prompt" in method_prompts:
                system_prompt_template = method_prompts["composed_prompt"]
                logger.debug(
                    f"subagent.{database_id}.using_composed_prompt",
                    execution_id=execution_id,
                    includes_globals=method_prompts.get("uses_global", []),
                )
            else:
                system_prompt_template = method_prompts["system_prompt"]
                logger.warning(
                    f"subagent.{database_id}.no_composed_prompt",
                    execution_id=execution_id,
                )

            user_prompt_template = method_prompts["user_prompt"]
            retrieval_tool = method_prompts["tool_definition"]

            # Load financial categories and format for system prompt
            categories = await load_financial_categories()
            category_mapping = " | ".join(
                [f"{cat_id}: {cat_data['name']}" for cat_id, cat_data in sorted(categories.items())]
            )

            # Inject category mapping into system prompt (after global composition)
            system_prompt = system_prompt_template.format(category_mapping=category_mapping)

            logger.debug(
                f"subagent.{database_id}.method_selection_prompt_loaded",
                execution_id=execution_id,
                num_categories=len(categories),
            )
        except Exception as e:
            logger.error(f"Failed to load method_selection prompt from database: {e}")
            raise RuntimeError(
                f"Critical error: Could not load method selection prompts from database: {e}"
            )

        # ==================================================
        # STEP 2: Parallel retrieval decisions with LLM method selection
        # ==================================================

        async def determine_retrieval_method(combo):
            """Enhanced helper function with priority blocks for LLM-informed decisions."""
            # STEP 1: Get priority blocks first (programmatic similarity search + expansion)
            priority_blocks = await get_priority_blocks(
                combo=combo, full_intent=full_intent, context=context, top_k=5
            )

            # Use LLM for method selection with priority blocks context
            priority_blocks_formatted = format_priority_blocks_for_method_selection(priority_blocks)

            # Format user prompt using template from YAML
            user_prompt = user_prompt_template.format(
                bank_name=combo["bank_name"],
                bank_symbol=combo["bank_symbol"],
                quarter=combo["quarter"],
                fiscal_year=combo["fiscal_year"],
                full_intent=full_intent,
                latest_message=latest_message,
                priority_blocks_formatted=priority_blocks_formatted,
            )

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]

            # Call LLM with tool
            try:
                # Use medium model for method selection (balance cost/performance)
                model_tier = "medium"
                model_config = getattr(config.llm, model_tier)

                response = await complete_with_tools(
                    messages=messages,
                    tools=[retrieval_tool],
                    context=context,
                    llm_params={
                        "model": model_config.model
                        # Use defaults from config for all parameters
                    },
                )

                # Parse the tool call
                if response.get("choices") and response["choices"][0].get("message"):
                    message = response["choices"][0]["message"]
                    if message.get("tool_calls"):
                        tool_call = message["tool_calls"][0]
                        function_args = json.loads(tool_call["function"]["arguments"])

                        logger.info(
                            "LLM retrieval decision",
                            bank=combo["bank_symbol"],
                            method=function_args.get("method"),
                        )

                        return {
                            "combo": combo,
                            "decision": function_args,
                            "priority_blocks": priority_blocks,
                            "success": True,
                        }

                # Fallback if no tool call - default to full section retrieval
                logger.warning("No tool call from LLM, defaulting to full section retrieval")
                return {
                    "combo": combo,
                    "decision": {
                        "method": 0,
                        "sections": "ALL",
                    },
                    "priority_blocks": priority_blocks,
                    "success": False,
                }

            except Exception as e:
                logger.error(f"LLM call failed for {combo['bank_symbol']}: {e}")
                # Fallback to full section retrieval on error
                return {
                    "combo": combo,
                    "decision": {
                        "method": 0,
                        "sections": "ALL",
                    },
                    "priority_blocks": priority_blocks,
                    "success": False,
                }

        # Execute parallel retrieval decisions
        retrieval_decisions = {}

        # Create tasks for parallel execution
        tasks = [determine_retrieval_method(combo) for combo in bank_period_combinations]

        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks)

        # Collect results
        for result in results:
            combo = result["combo"]
            key = f"{combo['bank_id']}_{combo['fiscal_year']}_{combo['quarter']}"
            retrieval_decisions[key] = result

        # ==================================================
        # STEP 3: Execute retrievals and format research statements (PARALLEL)
        # ==================================================

        async def process_single_combo(result):
            """Process a single bank-period combination: retrieve, format, and synthesize."""
            combo = result["combo"]
            decision = result["decision"]
            priority_blocks = result.get("priority_blocks", [])
            method = decision["method"]

            # Log the decision
            logger.info(
                f"subagent.{database_id}.retrieval_method_selected",
                execution_id=execution_id,
                bank=combo["bank_symbol"],
                period=f"{combo['quarter']} {combo['fiscal_year']}",
                method=method,
                priority_blocks_count=len(priority_blocks),
            )

            # Execute the appropriate retrieval method
            chunks = []
            formatted_content = ""

            if method == 0:
                # Full Section Retrieval with Priority Blocks
                sections = decision.get("sections", "ALL")
                chunks = await retrieve_full_section(combo, sections, context)

                # Log what sections are in the retrieved chunks
                sections_in_chunks = set(chunk.get("section_name", "Unknown") for chunk in chunks)
                logger.info(
                    f"subagent.{database_id}.sections_retrieved",
                    execution_id=execution_id,
                    bank=combo["bank_symbol"],
                    requested_sections=sections,
                    actual_sections=list(sections_in_chunks),
                    chunk_count=len(chunks),
                    priority_blocks_count=len(priority_blocks),
                )

                # Format with priority blocks prepended
                formatted_content = await format_full_section_chunks(
                    chunks, combo, context, priority_blocks=priority_blocks
                )

            elif method == 1:
                # Category-based Retrieval with Priority Blocks
                category_ids = decision.get("category_ids")
                if not category_ids:
                    # If no categories provided, fallback to full section retrieval
                    logger.warning(
                        "No category_ids provided for method 1, falling back to ALL sections"
                    )
                    chunks = await retrieve_full_section(combo, "ALL", context)
                    formatted_content = await format_full_section_chunks(
                        chunks, combo, context, priority_blocks=priority_blocks
                    )
                else:
                    chunks = await retrieve_by_categories(combo, category_ids, context)

                    logger.info(
                        f"subagent.{database_id}.categories_retrieved",
                        execution_id=execution_id,
                        bank=combo["bank_symbol"],
                        category_ids=category_ids,
                        chunk_count=len(chunks),
                        priority_blocks_count=len(priority_blocks),
                    )

                    # Format with priority blocks prepended
                    formatted_content = await format_category_or_similarity_chunks(
                        chunks, combo, context, note_gaps=True, priority_blocks=priority_blocks
                    )

            elif method == 2:
                # Similarity Search with full pipeline
                search_phrase = decision.get("search_phrase", full_intent)
                chunks = await retrieve_by_similarity(combo, search_phrase, context)

                # Apply reranking, expansion, and gap filling
                chunks = await rerank_similarity_chunks(chunks, search_phrase, context)
                chunks = await expand_speaker_blocks(chunks, combo, context)
                chunks = await fill_gaps_in_speaker_blocks(chunks, combo, context)

                logger.info(
                    f"subagent.{database_id}.similarity_retrieved",
                    execution_id=execution_id,
                    bank=combo["bank_symbol"],
                    search_phrase=search_phrase[:50],  # Only truncate for logging display,
                    chunk_count=len(chunks),
                )

                formatted_content = await format_category_or_similarity_chunks(
                    chunks, combo, context, note_gaps=True
                )

            # Generate research statement for this combination
            if chunks and formatted_content:
                # Always generate a synthesized research statement
                research_statement = await generate_research_statement(
                    formatted_content, combo, context
                )

                logger.info(
                    f"subagent.{database_id}.research_generated",
                    execution_id=execution_id,
                    bank=combo["bank_symbol"],
                    period=f"{combo['quarter']} {combo['fiscal_year']}",
                    method=method,
                    chunks_used=len(chunks),
                )
                return research_statement
            else:
                # No data found
                return (
                    f"\n### {combo['bank_name']} - {combo['quarter']} {combo['fiscal_year']}\n"
                    f"No transcript data available for this period.\n"
                )

        # Execute all retrievals in parallel
        retrieval_tasks = [process_single_combo(result) for result in retrieval_decisions.values()]
        all_research_statements = await asyncio.gather(*retrieval_tasks)

        # ==================================================
        # STEP 4: Output final combined research
        # ==================================================

        # Combine all research statements
        final_output = "\n".join(all_research_statements)

        # Add header
        header = "## Earnings Transcript Analysis\n\n"
        header += f"**Query**: {full_intent}\n"
        header += f"**Coverage**: {len(bank_period_combinations)} bank-period combinations\n"
        header += "\n---\n\n"

        # Yield the complete research output
        yield {"type": "subagent", "name": database_id, "content": header + final_output}

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
                    key: result["decision"]["method"] for key, result in retrieval_decisions.items()
                },
            },
        )

        logger.info(
            f"subagent.{database_id}.completed",
            execution_id=execution_id,
            total_duration_ms=int((stage_end - stage_start).total_seconds() * 1000),
            research_statements=len(all_research_statements),
        )

    except Exception as e:
        # Error handling
        error_msg = str(e)
        logger.error(
            f"subagent.{database_id}.error",
            execution_id=execution_id,
            error=error_msg,
            exc_info=True,
        )

        add_monitor_entry(
            stage_name="Subagent_Transcripts",
            stage_start_time=stage_start,
            stage_end_time=datetime.now(timezone.utc),
            status="Failure",
            error_message=error_msg,
            custom_metadata={"subagent": database_id, "error_type": type(e).__name__},
        )

        yield {
            "type": "subagent",
            "name": database_id,
            "content": f"\n⚠️ Error in Transcripts subagent: {error_msg}\n",
        }
