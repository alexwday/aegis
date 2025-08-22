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
from ..utils.logging import setup_logging, get_logger
from ..utils.monitor import (
    add_monitor_entry,
    initialize_monitor,
    post_monitor_entries,
)
from ..utils.ssl import setup_ssl


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

    # Log the database filters for future use
    filter_metadata = {
        "db_names_requested": db_names,
        "filter_count": len(db_names) if db_names else 0,
    }

    # Future: Apply filters to available databases
    # if db_names:
    #     available_databases = get_all_databases()
    #     filtered_databases = {k: v for k, v in available_databases.items() if k in db_names}
    #     filter_metadata["db_names_applied"] = list(filtered_databases.keys())

    logger.info(
        "model.stage.filter_processing.completed",
        execution_id=execution_id,
        status="Success",
        filter_count=filter_metadata["filter_count"],
        db_names=db_names,
    )

    add_monitor_entry(
        stage_name="Filter_Processing",
        stage_start_time=filter_start,
        stage_end_time=datetime.now(timezone.utc),
        status="Success",
        decision_details=f"Processed {filter_metadata['filter_count']} database filters",
        custom_metadata=filter_metadata,
    )

    # Future: Router agent will determine path
    # Future: Agents will process the request
    # Future: Subagents will be called in parallel
    # Future: Response will be synthesized and streamed

    # Placeholder response for now
    yield {
        "type": "agent",
        "name": "aegis",
        "content": "Model setup complete. Agents not yet implemented.\n",
    }

    # Post monitoring data to database
    entries_posted = post_monitor_entries(execution_id)
    logger.info(
        "model.generator.completed",
        execution_id=execution_id,
        entries_posted=entries_posted,
    )
