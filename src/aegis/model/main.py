"""
Model workflow orchestration module.

This module contains the main workflow execution logic that orchestrates
the agent pipeline for processing requests.
"""

import uuid
import threading
from datetime import datetime, timezone
from queue import Queue
from typing import Any, Dict, Generator, List, Optional, Union

from ..connections.oauth_connector import setup_authentication
from ..utils.conversation import process_conversation
from ..utils.database_filter import filter_databases, get_database_prompt
from ..utils.logging import setup_logging, get_logger
from ..utils.monitor import (
    add_monitor_entry,
    initialize_monitor,
    post_monitor_entries,
)
from ..utils.ssl import setup_ssl
from .agents import route_query, generate_response, clarify_query, synthesize_responses


def model(
    conversation: Optional[Union[Dict[str, Any], List[Dict[str, str]]]] = None,
    db_names: Optional[List[str]] = None,
) -> Generator[Dict[str, str], None, None]:
    """
    Stream responses for the Aegis model with unified message schema.

    This generator function streams responses with a consistent schema that allows
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
        >>> for message in model({"messages": msgs}):
        ...     print(f"[{message['type']}/{message['name']}]: {message['content']}")
        [agent/aegis]: Analyzing your query about Q3 revenue...
        [agent/aegis]: I'll check transcripts and RTS for details.
        [subagent/transcripts]: Searching Q3 2024 earnings calls...
        [subagent/rts]: Querying revenue tracking system...
    """
    # Stage 0: Initialize logging, execution ID, and process monitoring
    setup_logging()
    logger = get_logger()

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
    auth_config = setup_authentication(execution_id, ssl_config)
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
    routing_decision = route_query(
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

    add_monitor_entry(
        stage_name="Router",
        stage_start_time=router_start,
        stage_end_time=datetime.now(timezone.utc),
        status=routing_decision.get("status", "Success"),
        decision_details=routing_decision.get("rationale", "Routing decision made"),
        error_message=routing_decision.get("error"),
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
        for chunk in generate_response(
            conversation_history=processed_conversation.get("messages", []),
            latest_message=processed_conversation.get("latest_message", {}).get("content", ""),
            context=response_context,
            streaming=True,
        ):
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

                add_monitor_entry(
                    stage_name="Response_Agent",
                    stage_start_time=response_start,
                    stage_end_time=datetime.now(timezone.utc),
                    status=chunk.get("status", "Success"),
                    decision_details="Direct response generated",
                    error_message=chunk.get("error"),
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
        clarifier_result = clarify_query(
            query=processed_conversation.get("latest_message", {}).get("content", ""),
            context=clarifier_context,
            available_databases=list(filtered_databases.keys()),
            messages=processed_conversation.get("messages", []),
        )

        logger.info(
            "model.stage.clarifier.completed",
            execution_id=execution_id,
            status=clarifier_result.get("status"),
            needs_clarification=clarifier_result.get("status") == "needs_clarification",
        )

        add_monitor_entry(
            stage_name="Clarifier",
            stage_start_time=clarifier_start,
            stage_end_time=datetime.now(timezone.utc),
            status=clarifier_result.get("status", "Unknown"),
            decision_details=clarifier_result.get("clarification", "Banks and periods extracted"),
            error_message=clarifier_result.get("error"),
            custom_metadata={
                "banks": (
                    clarifier_result.get("banks", {}).get("bank_ids")
                    if clarifier_result.get("banks")
                    else None
                ),
                "periods": (
                    clarifier_result.get("periods", {}).get("periods")
                    if clarifier_result.get("periods")
                    else None
                ),
            },
        )

        # Check if there was an error or clarification is needed
        if clarifier_result.get("status") == "error":
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
        elif clarifier_result.get("status") == "needs_clarification":
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
            # Continue with research workflow - we have both banks and periods
            banks_detail = clarifier_result.get("banks", {}).get("banks_detail", {})
            periods = clarifier_result.get("periods", {}).get("periods", {})

            # Stream progress update with specific banks
            bank_names = [info["name"] for info in banks_detail.values()]

            # Don't send status messages here - let subagents and summarizer handle all output
            # This ensures summarizer appears after dropdowns, not before

            # Stage: Planner
            logger.info("model.stage.planner.started", execution_id=execution_id)
            planner_start = datetime.now(timezone.utc)

            from src.aegis.model.agents.planner import plan_database_queries

            # Build planner context
            planner_context = {
                "execution_id": execution_id,
                "auth_config": auth_config,
                "ssl_config": ssl_config,
            }

            # Extract query intent from clarifier's banks result
            query_intent = None
            if clarifier_result.get("banks") and clarifier_result["banks"].get("query_intent"):
                query_intent = clarifier_result["banks"]["query_intent"]

            # Call planner with clarifier results including intent
            planner_result = plan_database_queries(
                query=processed_conversation.get("latest_message", {}).get("content", ""),
                conversation=processed_conversation.get("messages", []),
                banks=clarifier_result.get("banks", {}),
                periods=clarifier_result.get("periods", {}),
                context=planner_context,
                available_databases=list(filtered_databases.keys()),
                query_intent=query_intent,
            )

            # Stream planner output
            if planner_result.get("status") == "success":
                databases = planner_result.get("databases", [])

                # Send initial status message BEFORE subagents start
                if databases:
                    # Format period description based on type
                    period_desc = ""
                    if periods and "apply_all" in periods:
                        period_info = periods["apply_all"]
                        period_desc = (
                            f"{', '.join(period_info['quarters'])} {period_info['fiscal_year']}"
                        )
                    elif periods and "bank_specific" in periods:
                        period_desc = "bank-specific periods"

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
                output_queue = Queue()
                # Collect all database responses for summarization
                database_responses = []

                def run_subagent(db_plan, queue, response_collector):
                    """Run a single subagent and put outputs in the queue."""
                    try:
                        database_id = db_plan.get("database_id")
                        query_intent = db_plan.get("query_intent", "")

                        # Get the appropriate subagent function
                        subagent_func = SUBAGENT_MAPPING.get(database_id)

                        if not subagent_func:
                            queue.put(
                                {
                                    "type": "subagent",
                                    "name": database_id,
                                    "content": (
                                        f"⚠️ No subagent found for database: {database_id}\n"
                                    ),
                                }
                            )
                            return

                        # Get basic intent from clarifier
                        basic_intent = ""
                        if clarifier_result.get("banks") and clarifier_result["banks"].get(
                            "query_intent"
                        ):
                            basic_intent = clarifier_result["banks"]["query_intent"]

                        # Collect the full response for this database
                        full_response = ""

                        # Call the subagent with all required parameters
                        for chunk in subagent_func(
                            conversation=processed_conversation.get("messages", []),
                            latest_message=processed_conversation.get("latest_message", {}).get(
                                "content", ""
                            ),
                            banks=clarifier_result.get("banks", {}),
                            periods=clarifier_result.get("periods", {}),
                            basic_intent=basic_intent,  # From clarifier
                            full_intent=query_intent,  # From planner for this specific database
                            database_id=database_id,
                            context=planner_context,
                        ):
                            queue.put(chunk)
                            # Collect content for summarization
                            if chunk.get("type") == "subagent" and chunk.get("content"):
                                full_response += chunk["content"]

                        # Store the complete response for summarization
                        response_collector.append(
                            {
                                "database_id": database_id,
                                "full_intent": query_intent,
                                "response": full_response,
                            }
                        )

                    except Exception as e:
                        logger.error(f"subagent.{database_id}.error", error=str(e))
                        queue.put(
                            {
                                "type": "subagent",
                                "name": database_id,
                                "content": f"⚠️ Error in {database_id}: {str(e)}\n",
                            }
                        )
                        # Still add error response for summarization
                        response_collector.append(
                            {
                                "database_id": database_id,
                                "full_intent": query_intent if "query_intent" in locals() else "",
                                "response": f"Error retrieving data: {str(e)}",
                            }
                        )
                    finally:
                        # Signal this subagent is done
                        queue.put({"type": "done", "database_id": database_id})

                # Send ALL subagent_start signals at once (creates all dropdowns immediately)
                # This ensures dropdowns appear simultaneously in the UI
                for db_plan in databases:
                    database_id = db_plan.get("database_id")
                    yield {
                        "type": "subagent_start",
                        "name": database_id,
                    }

                # Now start threads for each subagent to run concurrently
                threads = []
                active_subagents = set()

                for db_plan in databases:
                    database_id = db_plan.get("database_id")
                    active_subagents.add(database_id)

                    thread = threading.Thread(
                        target=run_subagent,
                        args=(db_plan, output_queue, database_responses),
                        daemon=True,
                    )
                    thread.start()
                    threads.append(thread)

                    logger.info(
                        f"subagent.{database_id}.started",
                        execution_id=execution_id,
                        database_id=database_id,
                    )

                # Stream outputs from all subagents as they arrive
                while active_subagents:
                    try:
                        # Get next message from any subagent
                        msg = output_queue.get(timeout=0.1)

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
                            # Stream the message
                            yield msg
                    except Exception:
                        # Timeout - check if threads are still alive
                        if not any(t.is_alive() for t in threads):
                            break

                # Wait for all threads to complete
                for thread in threads:
                    thread.join(timeout=1.0)

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
                    for chunk in synthesize_responses(
                        conversation_history=processed_conversation.get("messages", []),
                        latest_message=processed_conversation.get("latest_message", {}).get(
                            "content", ""
                        ),
                        database_responses=database_responses,
                        context=planner_context,
                    ):
                        yield chunk

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
            else:
                # Error case
                yield {
                    "type": "agent",
                    "name": "aegis",
                    "content": (
                        f"\n⚠️ Error in planning: {planner_result.get('error', 'Unknown error')}\n"
                    ),
                }

            logger.info(
                "model.stage.planner.completed",
                execution_id=execution_id,
                status="Success",
            )

            add_monitor_entry(
                stage_name="Planner",
                stage_start_time=planner_start,
                stage_end_time=datetime.now(timezone.utc),
                status="Success",
                decision_details="Research plan created and executed",
            )

    # Post monitoring data to database
    entries_posted = post_monitor_entries(execution_id)
    logger.info(
        "model.generator.completed",
        execution_id=execution_id,
        entries_posted=entries_posted,
    )
