#!/usr/bin/env python3
# pylint: disable=wrong-import-position
"""
Manual test script for LLM connector.
Replicates the workflow process and tests actual LLM API calls.

Usage:
    python test_connector.py
"""

import sys
from pathlib import Path
from typing import Any, Dict, List
from uuid import uuid4

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from aegis.connections.llm import (
    check_connection,
    complete,
    stream,
    complete_with_tools,
    embed,
    embed_batch,
)  # noqa: E402
from aegis.connections.oauth import setup_authentication  # noqa: E402
from aegis.utils.conversation import process_conversation  # noqa: E402
from aegis.utils.logging import get_logger, setup_logging  # noqa: E402
from aegis.utils.settings import config  # noqa: E402
from aegis.utils.ssl import setup_ssl  # noqa: E402

# Set up logging
setup_logging()
logger = get_logger()


def setup_test_environment(execution_id: str) -> Dict[str, Any]:
    """
    Set up the test environment with SSL and authentication.

    Args:
        execution_id: Unique execution identifier.

    Returns:
        Context dictionary with auth and SSL configuration.
    """
    # Set up SSL configuration
    ssl_config = setup_ssl()
    logger.info("SSL configuration", ssl_config=ssl_config, execution_id=execution_id)

    # Set up authentication
    auth_headers = setup_authentication(execution_id, ssl_config)
    logger.info(
        "Authentication setup", has_auth=auth_headers is not None, execution_id=execution_id
    )

    # Create context for all LLM calls
    return {"execution_id": execution_id, "auth_config": auth_headers, "ssl_config": ssl_config}


def prepare_test_messages(execution_id: str) -> Dict[str, Any]:
    """
    Prepare and process test messages.

    Args:
        execution_id: Unique execution identifier.

    Returns:
        Processed conversation dictionary.
    """
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

    return processed


def test_connection(context: Dict[str, Any]) -> bool:
    """
    Test LLM connection.

    Args:
        context: Context dictionary with configuration.

    Returns:
        True if connection successful, False otherwise.
    """
    print("\n" + "=" * 50)
    print("Testing LLM Connection...")
    print("=" * 50)

    is_connected = check_connection(context)
    if is_connected:
        logger.info("✅ LLM connection successful", execution_id=context["execution_id"])
        return True

    logger.error("❌ LLM connection failed", execution_id=context["execution_id"])
    logger.error("Please check your API key and network connection")
    return False


def test_model_tiers(messages: List[Dict[str, str]], context: Dict[str, Any]) -> None:
    """
    Test completion with all three model tiers.

    Args:
        messages: List of messages to send.
        context: Context dictionary with configuration.
    """
    print("\n" + "=" * 50)
    print("Testing LLM Completion (All Model Tiers)...")
    print("=" * 50)

    for tier in ["small", "medium", "large"]:
        print(f"\nTesting {tier} model...")
        try:
            model_name = getattr(config.llm, tier).model
            response = complete(
                messages=messages,
                context=context,
                llm_params={"model": model_name},
            )

            if response:
                logger.info(
                    f"✅ LLM completion with {tier} model successful",
                    execution_id=context["execution_id"],
                )
                content = response["choices"][0]["message"]["content"]
                print(f"  Response preview: {content[:100]}...")
            else:
                logger.error(
                    f"❌ LLM completion with {tier} model returned empty response",
                    execution_id=context["execution_id"],
                )

        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(
                f"❌ LLM completion with {tier} model failed",
                error=str(e),
                execution_id=context["execution_id"],
            )


def test_streaming(messages: List[Dict[str, str]], context: Dict[str, Any]) -> None:
    """
    Test streaming completion.

    Args:
        messages: List of messages to send.
        context: Context dictionary with configuration.
    """
    print("\n" + "=" * 50)
    print("Testing LLM Streaming...")
    print("=" * 50)

    try:
        print("\nStreaming response: ", end="", flush=True)
        full_response = ""

        for chunk in stream(
            messages=messages,
            context=context,
            llm_params={},
        ):
            if chunk and "choices" in chunk:
                delta = chunk["choices"][0].get("delta", {})
                content = delta.get("content", "")
                if content:
                    print(content, end="", flush=True)
                    full_response += content
                    if len(full_response) > 100:  # Limit output for testing
                        print("...[truncated]")
                        break

        if full_response:
            logger.info("✅ LLM streaming successful", execution_id=context["execution_id"])
        else:
            logger.error(
                "❌ LLM streaming returned empty response", execution_id=context["execution_id"]
            )

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("❌ LLM streaming failed", error=str(e), execution_id=context["execution_id"])


def test_tools(context: Dict[str, Any]) -> None:
    """
    Test completion with tools.

    Args:
        context: Context dictionary with configuration.
    """
    print("\n" + "=" * 50)
    print("Testing LLM with Tools...")
    print("=" * 50)

    try:
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "Get the current weather",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "location": {"type": "string", "description": "City name"},
                        },
                        "required": ["location"],
                    },
                },
            }
        ]

        tool_messages = [{"role": "user", "content": "What's the weather in Paris?"}]

        response = complete_with_tools(
            messages=tool_messages,
            tools=tools,
            context=context,
            llm_params={},
        )

        if response:
            has_tool_calls = "tool_calls" in response.get("choices", [{}])[0].get("message", {})
            if has_tool_calls:
                logger.info(
                    "✅ LLM tool completion successful with tool calls",
                    execution_id=context["execution_id"],
                )
                print("  Tool calls detected in response")
            else:
                logger.info(
                    "✅ LLM tool completion successful (no tool calls needed)",
                    execution_id=context["execution_id"],
                )
                print(f"  Response: {response['choices'][0]['message']['content'][:100]}...")
        else:
            logger.error(
                "❌ LLM tool completion returned empty response",
                execution_id=context["execution_id"],
            )

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "❌ LLM tool completion failed", error=str(e), execution_id=context["execution_id"]
        )


def test_single_embedding(context: Dict[str, Any]) -> None:
    """
    Test single text embedding.

    Args:
        context: Context dictionary with configuration.
    """
    print("\n" + "=" * 50)
    print("Testing Single Embedding...")
    print("=" * 50)

    try:
        test_text = "The quick brown fox jumps over the lazy dog"
        print(f"\nGenerating embedding for: '{test_text}'")

        result = embed(
            input_text=test_text,
            context=context,
            embedding_params={},
        )

        if result and result.get("data"):
            vector_length = len(result["data"][0]["embedding"])
            logger.info(
                "✅ Single embedding generation successful",
                execution_id=context["execution_id"],
                model=result.get("model"),
                vector_length=vector_length,
                usage=result.get("usage"),
            )
            print(f"  Generated vector with {vector_length} dimensions")
            print(f"  Model: {result.get('model')}")
        else:
            logger.error(
                "❌ Single embedding returned empty response", execution_id=context["execution_id"]
            )

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "❌ Single embedding failed", error=str(e), execution_id=context["execution_id"]
        )


def test_batch_embeddings(context: Dict[str, Any]) -> None:
    """
    Test batch embeddings.

    Args:
        context: Context dictionary with configuration.
    """
    print("\n" + "=" * 50)
    print("Testing Batch Embeddings...")
    print("=" * 50)

    try:
        test_texts = [
            "Machine learning is transforming industries",
            "Natural language processing enables human-computer interaction",
            "Deep learning models can recognize patterns in data",
        ]
        print(f"\nGenerating embeddings for {len(test_texts)} texts")

        result = embed_batch(
            input_texts=test_texts,
            context=context,
            embedding_params={},
        )

        if result and result.get("data"):
            num_vectors = len(result["data"])
            vector_length = len(result["data"][0]["embedding"])
            logger.info(
                "✅ Batch embedding generation successful",
                execution_id=context["execution_id"],
                model=result.get("model"),
                num_vectors=num_vectors,
                vector_length=vector_length,
                usage=result.get("usage"),
            )
            print(f"  Generated {num_vectors} vectors with {vector_length} dimensions each")
            print(f"  Model: {result.get('model')}")
        else:
            logger.error(
                "❌ Batch embedding returned empty response", execution_id=context["execution_id"]
            )

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "❌ Batch embedding failed", error=str(e), execution_id=context["execution_id"]
        )


def test_custom_dimensions(context: Dict[str, Any]) -> None:
    """
    Test embeddings with custom dimensions.

    Args:
        context: Context dictionary with configuration.
    """
    print("\n" + "=" * 50)
    print("Testing Embeddings with Custom Dimensions...")
    print("=" * 50)

    try:
        test_text = "Testing custom dimension embedding"
        custom_dims = 256  # Smaller dimension for text-embedding-3 models
        print(f"\nGenerating embedding with {custom_dims} dimensions")

        result = embed(
            input_text=test_text,
            context=context,
            embedding_params={"dimensions": custom_dims},
        )

        if result and result.get("data"):
            vector_length = len(result["data"][0]["embedding"])
            logger.info(
                "✅ Custom dimension embedding successful",
                execution_id=context["execution_id"],
                requested_dims=custom_dims,
                actual_dims=vector_length,
                model=result.get("model"),
            )
            print(f"  Generated vector with {vector_length} dimensions (requested: {custom_dims})")
            if vector_length == custom_dims:
                print("  ✅ Dimensions match requested size")
            else:
                print("  ⚠️ Dimensions don't match (model may not support custom dimensions)")
        else:
            logger.error(
                "❌ Custom dimension embedding returned empty response",
                execution_id=context["execution_id"],
            )

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "❌ Custom dimension embedding failed",
            error=str(e),
            execution_id=context["execution_id"],
        )


def test_workflow_to_llm() -> None:
    """
    Test the full workflow up to LLM connector.
    """
    execution_id = str(uuid4())
    logger.info("Starting LLM connector test", execution_id=execution_id)

    # Set up environment
    context = setup_test_environment(execution_id)

    # Prepare messages
    processed = prepare_test_messages(execution_id)

    # Test connection
    if not test_connection(context):
        return

    # Run all tests
    test_model_tiers(processed["messages"], context)
    test_streaming(processed["messages"], context)
    test_tools(context)
    test_single_embedding(context)
    test_batch_embeddings(context)
    test_custom_dimensions(context)

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
