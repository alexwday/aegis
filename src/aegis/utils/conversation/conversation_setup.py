"""
Conversation processing module.

This module handles the processing and validation of incoming conversation data
for the Aegis agent workflow system.
"""

from typing import Any, Dict

from aegis.utils.logging import get_logger
from aegis.utils.settings import config


def process_conversation(conversation_input: Any, execution_id: str) -> Dict[str, Any]:
    """
    Process and validate incoming conversation data.

    Takes raw conversation input and applies the following processing steps:
    1. Validates message structure (role and content required)
    2. Filters messages by role based on configuration:
       - Removes system messages if INCLUDE_SYSTEM_MESSAGES=false
       - Only keeps roles listed in ALLOWED_ROLES
    3. Trims to MAX_HISTORY_LENGTH most recent messages (default: 10)
    4. Returns processed messages with metadata

    Args:
        conversation_input: Raw conversation data from API call. Accepts either:
                          1. {"messages": [{"role": str, "content": str}, ...]}
                          2. [{"role": str, "content": str}, ...] (will be wrapped)
        execution_id: Unique identifier for this execution.

    Returns:
        Processed conversation data with validated messages and metadata.

        # Returns: {
        #     "messages": [{"role": "user", "content": "Hello"}, ...],  # Filtered & trimmed
        #     "message_count": 3,  # Count after filtering/trimming
        #     "latest_message": {"role": "assistant", "content": "Hi there"},  # Last msg
        #     "execution_id": "abc-123-def-456"
        # }

        Note: latest_message is the chronologically last message after filtering/trimming,
        regardless of role. Could be "user", "assistant", or "system" (if included).

    Raises:
        ValueError: If conversation format is invalid or required fields missing.
    """
    logger = get_logger()
    logger.debug("Processing conversation")

    try:
        # Handle different input formats
        if isinstance(conversation_input, list):
            # If conversation is just a list, wrap it in a dict
            conversation_input = {"messages": conversation_input}
        elif not isinstance(conversation_input, dict):
            raise ValueError(f"Expected dict or list, got {type(conversation_input).__name__}")

        # Extract messages
        if "messages" not in conversation_input:
            raise ValueError("Missing required 'messages' field")

        messages = conversation_input["messages"]

        if not isinstance(messages, list):
            raise ValueError("Messages must be a list")

        if not messages:
            raise ValueError("Messages list cannot be empty")

        # Validate and filter messages
        processed_messages = []
        for idx, message in enumerate(messages):
            processed_msg = _validate_and_filter_message(message, idx)
            if processed_msg:  # Only add if message passes filtering
                processed_messages.append(processed_msg)

        if not processed_messages:
            raise ValueError("No valid messages after filtering")

        # Keep only the most recent messages based on config
        if len(processed_messages) > config.max_history_length:
            processed_messages = processed_messages[-config.max_history_length :]

        # Extract the latest message (what we need to respond to)
        latest_message = processed_messages[-1]

        # Log processing results
        logger.info(
            "Conversation processed",
            message_count=len(processed_messages),
            latest_role=latest_message["role"],
        )

        return {
            "messages": processed_messages,
            "latest_message": latest_message,
            "message_count": len(processed_messages),
            "execution_id": execution_id,
        }

    except Exception as e:
        logger.error(
            "Failed to process conversation",
            error=str(e),
        )
        raise


def _validate_and_filter_message(message: Any, index: int) -> Dict[str, str] | None:
    """
    Validate and filter a single message based on configuration.

    Args:
        message: Message to validate.
        index: Position in the messages list (for error reporting).

    Returns:
        Validated message with role and content, or None if filtered out.

    Raises:
        ValueError: If message structure is invalid.
    """
    if not isinstance(message, dict):
        raise ValueError(f"Message at index {index} must be a dict")

    # Check required fields
    if "role" not in message:
        raise ValueError(f"Message at index {index} missing 'role' field")

    if "content" not in message:
        raise ValueError(f"Message at index {index} missing 'content' field")

    role = message["role"]
    content = message["content"]

    # Validate role against all possible roles
    valid_roles = {"system", "user", "assistant"}
    if role not in valid_roles:
        raise ValueError(
            f"Message at index {index} has invalid role '{role}'. " f"Must be one of: {valid_roles}"
        )

    # Validate content
    if not isinstance(content, str):
        raise ValueError(f"Message at index {index} content must be a string")

    if not content.strip():
        raise ValueError(f"Message at index {index} content cannot be empty")

    # Filter based on configuration
    # Check if role is allowed
    if role == "system":
        if not config.include_system_messages:
            return None  # Filter out system messages if configured
    elif role not in config.allowed_roles:
        return None  # Filter out roles not in allowed list

    return {"role": role, "content": content}
