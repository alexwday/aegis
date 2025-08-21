#!/usr/bin/env python3
"""
Manual test script for LLM connector.
Replicates the workflow process and tests actual LLM API calls.

Usage:
    python test_connector.py
"""

import sys
from pathlib import Path
from uuid import uuid4

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from aegis.connections.llm import check_connection, complete, stream  # noqa: E402
from aegis.connections.oauth import setup_authentication  # noqa: E402
from aegis.utils.conversation import process_conversation  # noqa: E402
from aegis.utils.logging import get_logger, setup_logging  # noqa: E402
from aegis.utils.settings import config  # noqa: E402
from aegis.utils.ssl import setup_ssl  # noqa: E402

# Set up logging
setup_logging()
logger = get_logger()


def test_workflow_to_llm():
    """
    Test the full workflow up to LLM connector.
    """
    execution_id = str(uuid4())
    logger.info("Starting LLM connector test", execution_id=execution_id)

    # Step 1: Set up SSL configuration
    ssl_config = setup_ssl()
    logger.info("SSL configuration", ssl_config=ssl_config, execution_id=execution_id)

    # Step 2: Process a test conversation
    test_messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What is 2+2?"},
        {"role": "assistant", "content": "2+2 equals 4."},
        {"role": "user", "content": "What is the capital of France?"},
    ]

    processed = process_conversation(test_messages, execution_id)
    logger.info(
        "Processed conversation",
        message_count=processed["message_count"],
        latest_message=processed["latest_message"]["content"][:50],
        execution_id=execution_id,
    )

    # Step 3: Set up authentication
    auth_headers = setup_authentication(execution_id, ssl_config)
    logger.info(
        "Authentication setup", has_auth=auth_headers is not None, execution_id=execution_id
    )

    # Step 4: Test LLM connection
    print("\n" + "=" * 50)
    print("Testing LLM Connection...")
    print("=" * 50)

    is_connected = check_connection(auth_headers, ssl_config, execution_id)
    if is_connected:
        logger.info("✅ LLM connection successful", execution_id=execution_id)
    else:
        logger.error("❌ LLM connection failed", execution_id=execution_id)
        logger.error("Please check your API key and network connection")
        return

    # Step 5: Test completion
    print("\n" + "=" * 50)
    print("Testing LLM Completion...")
    print("=" * 50)

    try:
        response = complete(
            messages=processed["messages"],
            auth_config=auth_headers,
            ssl_config=ssl_config,
            execution_id=execution_id,
            model="gpt-4o-mini",  # Using smaller model for testing
            temperature=0.7,
        )

        if response:
            logger.info("✅ LLM completion successful", execution_id=execution_id)
            print(f"\nResponse: {response[:200]}...")  # First 200 chars
        else:
            logger.error("❌ LLM completion returned empty response", execution_id=execution_id)

    except Exception as e:
        logger.error("❌ LLM completion failed", error=str(e), execution_id=execution_id)

    # Step 6: Test streaming
    print("\n" + "=" * 50)
    print("Testing LLM Streaming...")
    print("=" * 50)

    try:
        print("\nStreaming response: ", end="", flush=True)
        full_response = ""

        for chunk in stream(
            messages=processed["messages"],
            auth_config=auth_headers,
            ssl_config=ssl_config,
            execution_id=execution_id,
            model="gpt-4o-mini",
            temperature=0.7,
        ):
            if chunk:
                print(chunk, end="", flush=True)
                full_response += chunk
                if len(full_response) > 100:  # Limit output for testing
                    print("...[truncated]")
                    break

        if full_response:
            logger.info("✅ LLM streaming successful", execution_id=execution_id)
        else:
            logger.error("❌ LLM streaming returned empty response", execution_id=execution_id)

    except Exception as e:
        logger.error("❌ LLM streaming failed", error=str(e), execution_id=execution_id)

    print("\n" + "=" * 50)
    print("Test Complete")
    print("=" * 50)


if __name__ == "__main__":
    # Check for API key
    if not config.api_key:
        print("❌ Error: API_KEY not set in environment or .env file")
        print("Please set your OpenAI API key:")
        print("  export API_KEY=your-api-key-here")
        print("Or add it to your .env file")
        sys.exit(1)

    # Run the test
    test_workflow_to_llm()
