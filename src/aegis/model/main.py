"""
Model workflow orchestration module.

This module contains the main workflow execution logic that orchestrates
the agent pipeline for processing requests.
"""

import uuid
from datetime import datetime, timezone
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
from .agents import route_query, generate_response, clarify_query


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
        
        # Run clarifier with available databases
        clarifier_result = clarify_query(
            query=processed_conversation.get("latest_message", {}).get("content", ""),
            context=clarifier_context,
            available_databases=list(filtered_databases.keys()),
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
                "banks": clarifier_result.get("banks", {}).get("bank_ids") if clarifier_result.get("banks") else None,
                "periods": clarifier_result.get("periods", {}).get("periods") if clarifier_result.get("periods") else None,
            },
        )
        
        # Check if clarification is needed
        if clarifier_result.get("status") == "needs_clarification":
            # Format and stream clarification request back to user
            clarifications = clarifier_result.get("clarifications", [])
            
            if clarifications:
                # Stream opening statement for clarifications
                yield {
                    "type": "agent",
                    "name": "aegis",
                    "content": "I need some additional information to complete your research request:\n\n",
                }
                
                # Stream each clarification as a numbered list
                for i, clarification in enumerate(clarifications, 1):
                    yield {
                        "type": "agent",
                        "name": "aegis",
                        "content": f"{i}. {clarification}\n",
                    }
                
                # Add closing guidance
                yield {
                    "type": "agent",
                    "name": "aegis",
                    "content": "\nPlease provide these details so I can retrieve the specific data you need.",
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
            
            # Format period description
            if periods and "apply_all" in periods:
                period_info = periods["apply_all"]
                period_desc = f"{', '.join(period_info['quarters'])} {period_info['fiscal_year']}"
            else:
                period_desc = "the specified periods"
            
            yield {
                "type": "agent",
                "name": "aegis",
                "content": f"Retrieving {period_desc} data for {', '.join(bank_names) if bank_names else 'selected banks'}...\n",
            }
            
            # TODO: Implement Planner → Subagents → Summarizer
            yield {
                "type": "agent", 
                "name": "aegis",
                "content": "\n[Planner and Subagents would execute here with the extracted banks and periods]\n",
            }

    # Post monitoring data to database
    entries_posted = post_monitor_entries(execution_id)
    logger.info(
        "model.generator.completed",
        execution_id=execution_id,
        entries_posted=entries_posted,
    )
