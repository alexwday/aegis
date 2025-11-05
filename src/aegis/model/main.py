"""
Model workflow orchestration module.

This module contains the main workflow execution logic that orchestrates
the agent pipeline for processing requests.
"""

import uuid
import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, AsyncGenerator, List, Optional, Union

from ..connections.oauth_connector import setup_authentication
from ..utils.conversation import process_conversation
from ..utils.database_filter import filter_databases, get_database_prompt
from ..utils.logging import setup_logging, get_logger
from ..utils.monitor import (
    add_monitor_entry,
    initialize_monitor,
    post_monitor_entries_async,
)
from ..utils.settings import config
from ..utils.ssl import setup_ssl
from ..utils.sql_prompt import postgresql_prompts
from .agents import route_query, generate_response, clarify_query, synthesize_responses
import re


def extract_s3_info(content: str) -> List[Dict[str, str]]:
    """
    Extract S3 file information from markers in content.

    Args:
        content: String content that may contain S3 link markers

    Returns:
        List of dicts with S3 file info
    """
    # Pattern to match markers: {{S3_LINK:action:type:key:text}}
    pattern = r"\{\{S3_LINK:([^:]+):([^:]+):([^:]+):([^}]+)\}\}"

    s3_files = []
    for match in re.finditer(pattern, content):
        s3_files.append(
            {
                "action": match.group(1),  # download or open
                "file_type": match.group(2),  # docx or pdf
                "s3_key": match.group(3),  # S3 filename
                "display_text": match.group(4),  # Text to display
            }
        )

    return s3_files


def process_s3_links(content: str) -> str:
    """
    Process S3 link markers in content and replace with actual HTML links.

    Markers have format: {{S3_LINK:action:file_type:s3_key:display_text}}
    Example: {{S3_LINK:download:docx:RY_2025_Q2_abc.docx:Download Document}}
    Example: {{S3_LINK:open:pdf:RY_2025_Q2_abc.pdf:Open PDF}}

    Args:
        content: Text content potentially containing S3 markers

    Returns:
        Content with markers replaced by HTML links
    """
    # Pattern to match S3 link markers
    pattern = r"\{\{S3_LINK:([^:]+):([^:]+):([^:]+):([^}]+)\}\}"

    def replace_marker(match):
        action = match.group(1)  # download or open
        file_type = match.group(2)  # docx, pdf, etc.
        s3_key = match.group(3)  # S3 filename
        display_text = match.group(4)  # Text to display

        # Get S3 base URL from config
        base_url = config.s3_reports_base_url

        if not base_url:
            # If no S3 URL configured, return a placeholder link
            return f"[{display_text}](#no-s3-configured)"

        # Ensure base URL ends with /
        if not base_url.endswith("/"):
            base_url += "/"

        # Construct full S3 URL
        full_url = f"{base_url}{s3_key}"

        # Generate HTML based on action type
        if action == "download":
            # Standard download link
            return f'<a href="{full_url}" download>{display_text}</a>'
        elif action == "open":
            # Special format for PDF viewer (UI will handle this)
            return f'<a href="{full_url}" data-action="open-pdf" target="_blank">{display_text}</a>'
        else:
            # Default fallback
            return f'<a href="{full_url}" target="_blank">{display_text}</a>'

    # Replace all markers with HTML links
    return re.sub(pattern, replace_marker, content)


async def model(
    conversation: Optional[Union[Dict[str, Any], List[Dict[str, str]]]] = None,
    db_names: Optional[List[str]] = None,
) -> AsyncGenerator[Dict[str, str], None]:
    """
    Stream responses for the Aegis model with unified message schema.

    This async generator function streams responses with a consistent schema that allows
    the UI to route messages appropriately. All messages contain type, name, and content.

    Messages are streamed for:
    - Main agent responses (type="agent", name="aegis")
    - Subagent responses (type="subagent", name=<subagent_name>)

    The UI tracks streaming state based on message flow:
    - First message from a subagent = streaming started
    - No more messages or stream end = streaming completed

    Args:
        conversation: Conversation input in one of two formats:
                     1. Dictionary: {"messages": [{"role": "user", "content": "..."}]}
                     2. List: [{"role": "user", "content": "..."}]
        db_names: Optional list of database IDs to filter available databases.
                 Example: ["internal_capm", "internal_wiki", "external_ey"]

    Yields:
        Dictionary with unified schema:
        {
            "type": "agent" or "subagent",
            "name": Source identifier (e.g., "aegis", "transcripts", "rts"),
            "content": Text content to display
        }

    Example:
        >>> msgs = [{"role": "user", "content": "What is Q3 revenue?"}]
        >>> async for message in model({"messages": msgs}):
        ...     print(f"[{message['type']}/{message['name']}]: {message['content']}")
        [agent/aegis]: Analyzing your query about Q3 revenue...
        [agent/aegis]: I'll check transcripts and RTS for details.
        [subagent/transcripts]: Searching Q3 2024 earnings calls...
        [subagent/rts]: Querying revenue tracking system...
    """
    # Stage 0: Initialize logging, execution ID, and process monitoring
    setup_logging()
    logger = get_logger()

    # Initialize PostgreSQL prompts cache
    postgresql_prompts()

    # Generate execution ID for this request
    execution_id = str(uuid.uuid4())
    logger.info("model.generator.started", execution_id=execution_id, db_names=db_names)

    # Log stage 0 initialization
    logger.info("model.stage.initialization.started", execution_id=execution_id)

    # Initialize process monitoring
    initialize_monitor(execution_id, "aegis")
    logger.info(
        "model.stage.initialization.completed",
        execution_id=execution_id,
        status="Success",
        db_names_count=len(db_names) if db_names else 0,
    )

    # Stage 1: Setup SSL configuration (internal - no yield)
    logger.info("model.stage.ssl_setup.started", execution_id=execution_id)
    ssl_start = datetime.now(timezone.utc)
    ssl_config = setup_ssl()
    logger.info(
        "model.stage.ssl_setup.completed",
        execution_id=execution_id,
        status=ssl_config.get("status", "Unknown"),
        verify=ssl_config.get("verify", False),
    )

    add_monitor_entry(
        stage_name="SSL_Setup",
        stage_start_time=ssl_start,
        stage_end_time=datetime.now(timezone.utc),
        status=ssl_config.get("status", "Unknown"),
        decision_details=ssl_config.get("decision_details", "SSL setup completed"),
        error_message=ssl_config.get("error"),
    )

    # Stage 2: Setup authentication (internal - no yield)
    logger.info("model.stage.authentication.started", execution_id=execution_id)
    auth_start = datetime.now(timezone.utc)
    auth_config = await setup_authentication(execution_id, ssl_config)
    logger.info(
        "model.stage.authentication.completed",
        execution_id=execution_id,
        status=auth_config.get("status", "Unknown"),
        method=auth_config.get("method", "Unknown"),
    )

    add_monitor_entry(
        stage_name="Authentication",
        stage_start_time=auth_start,
        stage_end_time=datetime.now(timezone.utc),
        status=auth_config.get("status", "Unknown"),
        decision_details=auth_config.get("decision_details", "Authentication completed"),
        error_message=auth_config.get("error"),
    )

    # Stage 3: Process conversation input (internal - no yield)
    logger.info("model.stage.conversation_processing.started", execution_id=execution_id)
    conv_start = datetime.now(timezone.utc)
    processed_conversation = process_conversation(conversation, execution_id)
    logger.info(
        "model.stage.conversation_processing.completed",
        execution_id=execution_id,
        status=processed_conversation.get("status", "Unknown"),
        messages_in=processed_conversation.get("original_message_count", 0),
        messages_out=processed_conversation.get("message_count", 0),
    )

    add_monitor_entry(
        stage_name="Conversation_Processing",
        stage_start_time=conv_start,
        stage_end_time=datetime.now(timezone.utc),
        status=processed_conversation.get("status", "Unknown"),
        decision_details=processed_conversation.get("decision_details", "Conversation processed"),
        error_message=processed_conversation.get("error"),
        custom_metadata={
            "messages_in": processed_conversation.get("original_message_count", 0),
            "messages_out": processed_conversation.get("message_count", 0),
            "has_latest_message": bool(processed_conversation.get("latest_message")),
        },
    )

    # Stage 4: Process database filters (internal - no yield)
    logger.info("model.stage.filter_processing.started", execution_id=execution_id)
    filter_start = datetime.now(timezone.utc)

    # Log exactly what we received for database filter
    logger.info(
        "model.database_filter.received",
        execution_id=execution_id,
        db_names_input=db_names,
        db_names_count=len(db_names) if db_names else 0,
        db_names_type=type(db_names).__name__,
    )

    # Apply database filters
    filtered_databases = filter_databases(db_names)
    database_prompt = get_database_prompt(db_names)

    filter_metadata = {
        "db_names_requested": db_names,
        "filter_count": len(db_names) if db_names else 0,
        "databases_available": list(filtered_databases.keys()),
        "databases_count": len(filtered_databases),
    }

    logger.info(
        "model.stage.filter_processing.completed",
        execution_id=execution_id,
        status="Success",
        databases_available=len(filtered_databases),
        db_names=list(filtered_databases.keys()),
    )

    add_monitor_entry(
        stage_name="Filter_Processing",
        stage_start_time=filter_start,
        stage_end_time=datetime.now(timezone.utc),
        status="Success",
        decision_details=f"Filtered to {len(filtered_databases)} databases",
        custom_metadata=filter_metadata,
    )

    # Stage 5: Router agent determines path
    logger.info("model.stage.router.started", execution_id=execution_id)
    router_start = datetime.now(timezone.utc)

    # Build context for router with filtered databases
    router_context = {
        "execution_id": execution_id,
        "auth_config": auth_config,
        "ssl_config": ssl_config,
        "database_prompt": database_prompt,  # Pass filtered database prompt
        "available_databases": list(filtered_databases.keys()),
    }

    # Get routing decision
    routing_decision = await route_query(
        conversation_history=processed_conversation.get("messages", []),
        latest_message=processed_conversation.get("latest_message", {}).get("content", ""),
        context=router_context,
    )

    logger.info(
        "model.stage.router.completed",
        execution_id=execution_id,
        route=routing_decision.get("route"),
        confidence=routing_decision.get("confidence"),
    )

    # Format LLM call info if we have tokens
    llm_calls = None
    if routing_decision.get("tokens_used"):
        llm_calls = [
            {
                "model": routing_decision.get("model_used", "unknown"),
                "prompt_tokens": 0,  # Would need breakdown from API
                "completion_tokens": 0,  # Would need breakdown from API
                "total_tokens": routing_decision.get("tokens_used", 0),
                "cost": routing_decision.get("cost", 0),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ]

    add_monitor_entry(
        stage_name="Router",
        stage_start_time=router_start,
        stage_end_time=datetime.now(timezone.utc),
        status=routing_decision.get("status", "Success"),
        decision_details=routing_decision.get("rationale", "Routing decision made"),
        error_message=routing_decision.get("error"),
        llm_calls=llm_calls,
        custom_metadata={
            "route": routing_decision.get("route"),
            "confidence": routing_decision.get("confidence"),
        },
    )

    # Stage 6: Execute based on routing decision
    if routing_decision.get("route") == "direct_response":
        # Direct response path - use response agent
        logger.info("model.stage.response_agent.started", execution_id=execution_id)
        response_start = datetime.now(timezone.utc)

        # Build context for response agent
        response_context = {
            "execution_id": execution_id,
            "auth_config": auth_config,
            "ssl_config": ssl_config,
        }

        # Stream response from response agent
        response_generator = await generate_response(
            conversation_history=processed_conversation.get("messages", []),
            latest_message=processed_conversation.get("latest_message", {}).get("content", ""),
            context=response_context,
            streaming=True,
        )
        async for chunk in response_generator:
            if chunk["type"] == "chunk":
                # Stream content chunks
                yield {
                    "type": "agent",
                    "name": "aegis",
                    "content": chunk["content"],
                }
            elif chunk["type"] == "final":
                # Log final metrics
                logger.info(
                    "model.stage.response_agent.completed",
                    execution_id=execution_id,
                    status=chunk.get("status"),
                    tokens_used=chunk.get("tokens_used"),
                    cost=chunk.get("cost"),
                    response_time_ms=chunk.get("response_time_ms"),
                )

                # Format LLM call info for response agent
                llm_calls = None
                if chunk.get("tokens_used"):
                    llm_calls = [
                        {
                            "model": chunk.get("model_used", "unknown"),
                            "prompt_tokens": 0,  # Would need breakdown
                            "completion_tokens": 0,  # Would need breakdown
                            "total_tokens": chunk.get("tokens_used", 0),
                            "cost": chunk.get("cost", 0),
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                    ]

                add_monitor_entry(
                    stage_name="Response_Agent",
                    stage_start_time=response_start,
                    stage_end_time=datetime.now(timezone.utc),
                    status=chunk.get("status", "Success"),
                    decision_details="Direct response generated",
                    error_message=chunk.get("error"),
                    llm_calls=llm_calls,
                    custom_metadata={
                        "tokens_used": chunk.get("tokens_used"),
                        "cost": chunk.get("cost"),
                        "model_used": chunk.get("model_used"),
                        "response_time_ms": chunk.get("response_time_ms"),
                    },
                )
    else:
        # Research workflow path - need to fetch data
        logger.info("model.stage.research_workflow.started", execution_id=execution_id)

        # Stage 6a: Clarifier - extract banks and periods
        logger.info("model.stage.clarifier.started", execution_id=execution_id)
        clarifier_start = datetime.now(timezone.utc)

        # Build context for clarifier
        clarifier_context = {
            "execution_id": execution_id,
            "auth_config": auth_config,
            "ssl_config": ssl_config,
        }

        # Run clarifier with available databases and full conversation
        clarifier_result = await clarify_query(
            query=processed_conversation.get("latest_message", {}).get("content", ""),
            context=clarifier_context,
            available_databases=list(filtered_databases.keys()),
            messages=processed_conversation.get("messages", []),
        )

        # Determine status based on result type
        if isinstance(clarifier_result, list):
            # Success - received bank-period combinations
            status = "Success"
            needs_clarification = False
        else:
            # Dictionary response means error or clarification needed
            status = clarifier_result.get("status", "unknown")
            needs_clarification = status == "needs_clarification"
            # Capitalize status for consistency
            if status == "error":
                status = "Error"
            elif status == "needs_clarification":
                status = "Needs Clarification"

        logger.info(
            "model.stage.clarifier.completed",
            execution_id=execution_id,
            status=status,
            needs_clarification=needs_clarification,
        )

        add_monitor_entry(
            stage_name="Clarifier",
            stage_start_time=clarifier_start,
            stage_end_time=datetime.now(timezone.utc),
            status=status,
            decision_details=(
                clarifier_result.get("clarifications", "Banks and periods extracted")
                if isinstance(clarifier_result, dict)
                else f"Extracted {len(clarifier_result)} bank-period combinations"
            ),
            error_message=(
                clarifier_result.get("error") if isinstance(clarifier_result, dict) else None
            ),
            custom_metadata={
                "combinations_count": (
                    len(clarifier_result) if isinstance(clarifier_result, list) else None
                ),
                "clarifications": (
                    clarifier_result.get("clarifications")
                    if isinstance(clarifier_result, dict)
                    else None
                ),
            },
        )

        # Check if there was an error or clarification is needed
        if isinstance(clarifier_result, dict) and clarifier_result.get("status") == "error":
            # Handle error from clarifier (e.g., API quota exceeded)
            error_msg = clarifier_result.get(
                "error", "An error occurred while processing your request"
            )
            yield {
                "type": "agent",
                "name": "aegis",
                "content": (
                    f"\n⚠️ Error: {error_msg}\n\n"
                    "Please try again later or contact support if the issue persists."
                ),
            }
        elif (
            isinstance(clarifier_result, dict)
            and clarifier_result.get("status") == "needs_clarification"
        ):
            # Format and stream clarification request back to user
            clarifications = clarifier_result.get("clarifications", [])

            if clarifications:
                # Add space before clarifications
                yield {
                    "type": "agent",
                    "name": "aegis",
                    "content": "\n",
                }

                # Stream opening statement for clarifications
                yield {
                    "type": "agent",
                    "name": "aegis",
                    "content": (
                        "**I need some additional information to complete your request:**\n\n"
                    ),
                }

                # Stream each clarification as a numbered list
                for i, clarification in enumerate(clarifications, 1):
                    yield {
                        "type": "agent",
                        "name": "aegis",
                        "content": f"{i}. {clarification}\n",
                    }

                # Add closing guidance with extra spacing
                yield {
                    "type": "agent",
                    "name": "aegis",
                    "content": (
                        "\nPlease provide these details so I can retrieve "
                        "the specific data you need."
                    ),
                }
            else:
                # Fallback if no clarifications provided
                yield {
                    "type": "agent",
                    "name": "aegis",
                    "content": "Please provide more details about your query.",
                }
        else:
            # Continue with research workflow - we have bank-period combinations
            # clarifier_result is now a list of combinations
            bank_period_combinations = clarifier_result

            # Extract unique bank names for status message
            unique_banks = {}
            for combo in bank_period_combinations:
                bank_id = combo["bank_id"]
                if bank_id not in unique_banks:
                    unique_banks[bank_id] = combo["bank_name"]
            bank_names = list(unique_banks.values())

            # Don't send status messages here - let subagents and summarizer handle all output
            # This ensures summarizer appears after dropdowns, not before

            # Stage: Planner
            logger.info("model.stage.planner.started", execution_id=execution_id)
            planner_start = datetime.now(timezone.utc)

            from aegis.model.agents.planner import plan_database_queries

            # Build planner context
            planner_context = {
                "execution_id": execution_id,
                "auth_config": auth_config,
                "ssl_config": ssl_config,
            }

            # Extract query intent from first combination (all should have same intent)
            # This is now the comprehensive intent from clarifier
            clarifier_intent = None
            if bank_period_combinations and len(bank_period_combinations) > 0:
                clarifier_intent = bank_period_combinations[0].get("query_intent")

            # Call planner with new standardized format
            planner_result = await plan_database_queries(
                query=processed_conversation.get("latest_message", {}).get("content", ""),
                conversation=processed_conversation.get("messages", []),
                bank_period_combinations=bank_period_combinations,
                context=planner_context,
                available_databases=list(filtered_databases.keys()),
                query_intent=clarifier_intent,
            )

            # Record planner completion BEFORE subagents start
            planner_end = datetime.now(timezone.utc)

            logger.info(
                "model.stage.planner.completed",
                execution_id=execution_id,
                status=planner_result.get("status", "Success"),
            )

            # Determine decision details based on status
            if planner_result.get("status") == "success":
                decision_details = (
                    f"Selected databases: {', '.join(planner_result.get('databases', []))}"
                )
            elif planner_result.get("status") == "no_data":
                decision_details = planner_result.get("message", "No data available")
            elif planner_result.get("status") == "no_databases":
                decision_details = planner_result.get("reason", "No databases needed")
            else:
                decision_details = f"Error: {planner_result.get('error', 'Unknown error')}"

            add_monitor_entry(
                stage_name="Planner",
                stage_start_time=planner_start,
                stage_end_time=planner_end,
                status=(
                    "Success"
                    if planner_result.get("status") == "success"
                    else planner_result.get("status", "Error")
                ),
                decision_details=decision_details,
                custom_metadata={
                    "databases_selected": planner_result.get("databases", []),
                    "query_intent": clarifier_intent,
                    "error_message": (
                        planner_result.get("error")
                        if planner_result.get("status") == "error"
                        else None
                    ),
                },
            )

            # Stream planner output
            if planner_result.get("status") == "success":
                databases = planner_result.get("databases", [])

                # Send initial status message BEFORE subagents start
                if databases:
                    # Extract unique periods from combinations
                    unique_periods = set()
                    for combo in bank_period_combinations:
                        period_str = f"{combo['quarter']} {combo['fiscal_year']}"
                        unique_periods.add(period_str)

                    period_desc = ", ".join(sorted(unique_periods))

                    # Send status message
                    yield {
                        "type": "agent",
                        "name": "aegis",
                        "content": (
                            f"Retrieving {period_desc} data for "
                            f"{', '.join(bank_names) if bank_names else 'selected banks'}...\n\n"
                        ),
                    }

                # Import subagent mapping
                from .subagents import SUBAGENT_MAPPING

                # Create a queue for collecting subagent outputs
                output_queue = asyncio.Queue()
                # Collect all database responses for summarization
                database_responses = []

                async def run_subagent(database_id, queue, response_collector):
                    """Run a single subagent and put outputs in the queue."""
                    async with invocation_semaphore:  # Limit concurrency per request
                        subagent_start = datetime.now(timezone.utc)
                        # Normalize database_id to lowercase for consistency
                        normalized_db_id = database_id.lower()

                        try:
                            # Apply 200 second timeout for subagent execution
                            async with asyncio.timeout(200):
                                # Add monitoring entry for subagent start
                                logger.info(
                                    f"subagent.{normalized_db_id}.started",
                                    execution_id=execution_id,
                                    database_id=normalized_db_id,
                                )

                                # Get the appropriate subagent function using normalized ID
                                subagent_func = SUBAGENT_MAPPING.get(normalized_db_id)

                                if not subagent_func:
                                    await queue.put(
                                        {
                                            "type": "subagent",
                                            "name": normalized_db_id,
                                            "content": (
                                                f"⚠️ No subagent found for database: {normalized_db_id}\n"
                                            ),
                                        }
                                    )
                                    return

                                # Collect the full response for this database
                                full_response = ""

                                # Call the subagent with new standardized format
                                # Now both basic_intent and full_intent use the clarifier's comprehensive intent
                                async for chunk in subagent_func(
                                    conversation=processed_conversation.get("messages", []),
                                    latest_message=processed_conversation.get("latest_message", {}).get(
                                        "content", ""
                                    ),
                                    bank_period_combinations=bank_period_combinations,
                                    basic_intent=clarifier_intent,  # Comprehensive intent from clarifier
                                    full_intent=clarifier_intent,  # Same comprehensive intent
                                    database_id=normalized_db_id,
                                    context=planner_context,
                                ):
                                    await queue.put(chunk)
                                    # Collect content for summarization
                                    if chunk.get("type") == "subagent" and chunk.get("content"):
                                        full_response += chunk["content"]

                                # Store the complete response for summarization
                                response_collector.append(
                                    {
                                        "database_id": normalized_db_id,
                                        "full_intent": clarifier_intent,
                                        "response": full_response,
                                    }
                                )

                                # Add monitoring entry for successful subagent completion
                                add_monitor_entry(
                                    stage_name=f"Subagent_{normalized_db_id}",
                                    stage_start_time=subagent_start,
                                    stage_end_time=datetime.now(timezone.utc),
                                    status="Success",
                                    decision_details=f"Retrieved data from {normalized_db_id}",
                                    custom_metadata={
                                        "database_id": normalized_db_id,
                                        "response_length": len(full_response),
                                    },
                                )

                        except Exception as e:
                            logger.error(f"subagent.{normalized_db_id}.error", error=str(e))
                            await queue.put(
                                {
                                    "type": "subagent",
                                    "name": normalized_db_id,
                                    "content": f"⚠️ Error in {normalized_db_id}: {str(e)}\n",
                                }
                            )
                            # Still add error response for summarization
                            response_collector.append(
                                {
                                    "database_id": normalized_db_id,
                                    "full_intent": clarifier_intent,
                                    "response": f"Error retrieving data: {str(e)}",
                                }
                            )

                            # Add monitoring entry for failed subagent
                            add_monitor_entry(
                                stage_name=f"Subagent_{normalized_db_id}",
                                stage_start_time=subagent_start,
                                stage_end_time=datetime.now(timezone.utc),
                                status="Error",
                                decision_details=f"Failed to retrieve data from {normalized_db_id}",
                                error_message=str(e),
                                custom_metadata={
                                    "database_id": normalized_db_id,
                                },
                            )
                        finally:
                            # Signal this subagent is done
                            await queue.put({"type": "done", "database_id": normalized_db_id})

                # Send ALL subagent_start signals at once (creates all dropdowns immediately)
                # This ensures dropdowns appear simultaneously in the UI
                # databases is now a list of strings (database IDs)
                # Normalize to lowercase for consistency
                for database_id in databases:
                    yield {
                        "type": "subagent_start",
                        "name": database_id.lower(),
                    }

                # Per-invocation concurrency limit (max 5 concurrent subagents per request)
                invocation_semaphore = asyncio.Semaphore(5)

                # Now start tasks for each subagent to run concurrently
                tasks = []
                active_subagents = set()

                for database_id in databases:
                    active_subagents.add(database_id)

                    task = asyncio.create_task(
                        run_subagent(database_id, output_queue, database_responses)
                    )
                    tasks.append(task)

                # Collect S3 file info from reports subagent
                s3_files_found = []

                # Stream outputs from all subagents as they arrive
                while active_subagents:
                    try:
                        # Get next message from any subagent with timeout
                        msg = await asyncio.wait_for(output_queue.get(), timeout=0.1)

                        if msg.get("type") == "done":
                            # A subagent finished
                            database_id = msg.get("database_id")
                            active_subagents.discard(database_id)
                            logger.info(
                                f"subagent.{database_id}.finished",
                                execution_id=execution_id,
                                remaining=len(active_subagents),
                            )
                        else:
                            # Extract S3 info before processing (only from reports subagent)
                            if (
                                msg.get("type") == "subagent"
                                and msg.get("name") == "reports"
                                and msg.get("content")
                            ):
                                found_files = extract_s3_info(msg["content"])
                                if found_files:
                                    s3_files_found.extend(found_files)

                            # Process S3 links in subagent content before yielding
                            if msg.get("type") == "subagent" and msg.get("content"):
                                msg["content"] = process_s3_links(msg["content"])
                            # Stream the message
                            yield msg
                    except asyncio.TimeoutError:
                        # Timeout - check if tasks are still running
                        if all(task.done() for task in tasks):
                            break

                # Wait for all tasks to complete
                await asyncio.gather(*tasks, return_exceptions=True)

                # After all subagents complete, synthesize the responses
                if database_responses:
                    logger.info(
                        "model.stage.summarizer.started",
                        execution_id=execution_id,
                        database_count=len(database_responses),
                    )

                    summarizer_start = datetime.now(timezone.utc)

                    # Signal that summarizer is starting (UI will handle appropriately)
                    yield {
                        "type": "summarizer_start",
                        "name": "aegis",
                    }

                    # Add a visual separator before the summary
                    yield {
                        "type": "agent",
                        "name": "aegis",
                        "content": "\n\n---\n\n**Summary:**\n\n",
                    }

                    # Stream the synthesized response (continues in same bubble as "Retrieving...")
                    async for chunk in synthesize_responses(
                        conversation_history=processed_conversation.get("messages", []),
                        latest_message=processed_conversation.get("latest_message", {}).get(
                            "content", ""
                        ),
                        database_responses=database_responses,
                        context=planner_context,
                    ):
                        yield chunk

                    # Programmatically add S3 links if we found any from reports subagent
                    if s3_files_found and config.s3_reports_base_url:
                        base_url = config.s3_reports_base_url
                        if not base_url.endswith("/"):
                            base_url += "/"

                        # Group links by document (pair DOCX and PDF for same report)
                        grouped_links = {}
                        for file_info in s3_files_found:
                            # Extract identifier from display text (e.g., "RY Q2 2025")
                            display_text = file_info["display_text"]
                            # Find the part in parentheses if it exists
                            import re

                            match = re.search(r"\((.*?)\)", display_text)
                            if match:
                                report_id = match.group(1)
                            else:
                                report_id = "Report"

                            if report_id not in grouped_links:
                                grouped_links[report_id] = []

                            full_url = f"{base_url}{file_info['s3_key']}"
                            # Use the full display text which includes the report name
                            if file_info["action"] == "download":
                                grouped_links[report_id].append(
                                    f'<a href="{full_url}" download>{file_info["display_text"]}</a>'
                                )
                            elif file_info["action"] == "open":
                                grouped_links[report_id].append(
                                    f'<a href="{full_url}" data-action="open-pdf" target="_blank">{file_info["display_text"]}</a>'
                                )

                        # Format output based on number of reports
                        if len(grouped_links) == 1:
                            # Single report - simple format
                            links = list(grouped_links.values())[0]
                            yield {
                                "type": "agent",
                                "name": "aegis",
                                "content": "\n\n**Available Downloads:** " + " | ".join(links),
                            }
                        else:
                            # Multiple reports - organized format
                            content_parts = ["\n\n**Available Downloads:**\n"]
                            for report_id, links in grouped_links.items():
                                content_parts.append(f"• **{report_id}**: " + " | ".join(links))

                            yield {
                                "type": "agent",
                                "name": "aegis",
                                "content": "\n".join(content_parts),
                            }

                    logger.info(
                        "model.stage.summarizer.completed",
                        execution_id=execution_id,
                        status="Success",
                    )

                    add_monitor_entry(
                        stage_name="Summarizer",
                        stage_start_time=summarizer_start,
                        stage_end_time=datetime.now(timezone.utc),
                        status="Success",
                        decision_details=(
                            f"Synthesized {len(database_responses)} database responses"
                        ),
                    )

            elif planner_result.get("status") == "no_databases":
                # No databases needed
                yield {
                    "type": "agent",
                    "name": "aegis",
                    "content": "\n{}\n".format(
                        planner_result.get("reason", "No database queries needed.")
                    ),
                }
            elif planner_result.get("status") == "no_data":
                # No data available for the requested banks/periods
                yield {
                    "type": "agent",
                    "name": "aegis",
                    "content": (
                        f"\n⚠️ {planner_result.get('message', 'No databases have data for the requested banks and periods')}\n"
                    ),
                }
            else:
                # Error case
                yield {
                    "type": "agent",
                    "name": "aegis",
                    "content": (
                        f"\n⚠️ Error in planning: {planner_result.get('error', 'Unknown error')}\n"
                    ),
                }

            # Planner monitoring was already added after planning finished

    # Post monitoring data to database
    entries_posted = await post_monitor_entries_async(execution_id)
    logger.info(
        "model.generator.completed",
        execution_id=execution_id,
        entries_posted=entries_posted,
    )
