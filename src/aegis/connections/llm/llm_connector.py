"""
LLM connector module for OpenAI API integration.

This module handles all interactions with OpenAI's API, supporting both
OAuth and API key authentication, with configurable model tiers.
"""

from typing import Any, Dict, Generator, List, Optional
import httpx
from openai import OpenAI

from aegis.utils.logging import get_logger
from aegis.utils.settings import config

# Module-level client cache to reuse connections
_client_cache: Dict[str, OpenAI] = {}


def _get_model_config(
    model: Optional[str],
    temperature: Optional[float],
    max_tokens: Optional[int],
    default_tier: str = "medium",
) -> tuple:
    """
    Determine model configuration based on model name.

    Args:
        model: Model name or None
        temperature: Temperature override or None
        max_tokens: Max tokens override or None
        default_tier: Default tier if model is None ("small", "medium", "large")

    Returns:
        Tuple of (model, temperature, max_tokens, model_tier)
    """
    if model is None:
        tier_config = getattr(config.llm, default_tier)
        return (
            tier_config.model,
            temperature or tier_config.temperature,
            max_tokens or tier_config.max_tokens,
            default_tier,
        )

    # Determine tier from model name
    if model == config.llm.small.model:
        return (
            model,
            temperature or config.llm.small.temperature,
            max_tokens or config.llm.small.max_tokens,
            "small",
        )
    if model == config.llm.large.model:
        return (
            model,
            temperature or config.llm.large.temperature,
            max_tokens or config.llm.large.max_tokens,
            "large",
        )
    if model == config.llm.medium.model:
        return (
            model,
            temperature or config.llm.medium.temperature,
            max_tokens or config.llm.medium.max_tokens,
            "medium",
        )
    # Unknown model, use medium defaults
    return (
        model,
        temperature or config.llm.medium.temperature,
        max_tokens or config.llm.medium.max_tokens,
        "medium",
    )


def _get_llm_client(
    auth_config: Dict[str, Any], ssl_config: Dict[str, Any], model_tier: str = "medium"
) -> OpenAI:
    """
    Get or create an OpenAI client with proper configuration.

    Creates a cached OpenAI client configured with the appropriate
    authentication and SSL settings. Clients are cached by auth token
    to enable connection reuse.

    Args:
        auth_config: Authentication configuration from workflow.
        ssl_config: SSL configuration from workflow.
        model_tier: Model tier for timeout configuration ("small", "medium", "large").

    Returns:
        Configured OpenAI client instance.

    Raises:
        ValueError: If authentication configuration is invalid.
    """
    logger = get_logger()

    # Use token as cache key
    cache_key = auth_config.get("token", "no-auth")

    # Return cached client if exists
    if cache_key in _client_cache:
        logger.debug("Using cached LLM client", cache_key=cache_key[:8] + "...")
        return _client_cache[cache_key]

    # Get timeout based on model tier
    timeout_config = {
        "small": config.llm.small.timeout,
        "medium": config.llm.medium.timeout,
        "large": config.llm.large.timeout,
    }
    timeout = timeout_config.get(model_tier, config.llm.medium.timeout)

    # Configure HTTP client with SSL settings
    http_client_kwargs = {
        "timeout": httpx.Timeout(timeout=timeout),
    }

    # Apply SSL configuration
    if ssl_config.get("verify"):
        if ssl_config.get("cert_path"):
            # Use custom certificate
            http_client_kwargs["verify"] = ssl_config["cert_path"]
        else:
            # Use system certificates
            http_client_kwargs["verify"] = True
    else:
        # Disable SSL verification
        http_client_kwargs["verify"] = False

    # Create HTTP client
    http_client = httpx.Client(**http_client_kwargs)

    # Create OpenAI client
    client = OpenAI(
        api_key=auth_config.get("token", "no-token"),
        base_url=config.llm.base_url,
        http_client=http_client,
    )

    # Cache the client
    _client_cache[cache_key] = client

    logger.info(
        "Created new LLM client",
        base_url=config.llm.base_url,
        auth_method=auth_config.get("method"),
        ssl_verify=ssl_config.get("verify"),
        timeout=timeout,
    )

    return client


def complete(
    messages: List[Dict[str, str]],
    context: Dict[str, Any],
    llm_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Generate a non-streaming completion from the LLM.

    Makes a synchronous call to the OpenAI API and returns the complete
    response. Suitable for simple question-answering and short responses.

    Args:
        messages: List of message dictionaries with 'role' and 'content'.
        context: Runtime context containing:
                 - execution_id: Unique identifier for this execution
                 - auth_config: Authentication configuration
                 - ssl_config: SSL configuration
        llm_params: Optional LLM parameters:
                    - model: Model to use (defaults to medium tier)
                    - temperature: Temperature setting
                    - max_tokens: Maximum tokens
                    - Additional OpenAI API parameters

    Returns:
        Response dictionary containing the completion.

        # Returns: {
        #     "id": "chatcmpl-...",
        #     "choices": [{"message": {"role": "assistant", "content": "..."}}],
        #     "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}
        # }

    Raises:
        Exception: If the API call fails.
    """
    logger = get_logger()

    # Extract LLM parameters with defaults
    if llm_params is None:
        llm_params = {}

    # Get model configuration using helper
    model, temperature, max_tokens, model_tier = _get_model_config(
        llm_params.get("model"), llm_params.get("temperature"), llm_params.get("max_tokens")
    )

    # Remove our known params, pass rest as kwargs
    kwargs = {
        k: v for k, v in llm_params.items() if k not in ["model", "temperature", "max_tokens"]
    }

    logger.info(
        "Generating LLM completion",
        execution_id=context["execution_id"],
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        message_count=len(messages),
    )

    try:
        client = _get_llm_client(context["auth_config"], context["ssl_config"], model_tier)

        response = client.chat.completions.create(
            model=model, messages=messages, temperature=temperature, max_tokens=max_tokens, **kwargs
        )

        # Convert response to dict
        response_dict = response.model_dump()

        logger.info(
            "LLM completion successful",
            execution_id=context["execution_id"],
            model=model,
            usage=response_dict.get("usage"),
        )

        return response_dict

    except Exception as e:
        logger.error(
            "LLM completion failed",
            execution_id=context["execution_id"],
            model=model,
            error=str(e),
        )
        raise


def stream(
    messages: List[Dict[str, str]],
    context: Dict[str, Any],
    llm_params: Optional[Dict[str, Any]] = None,
) -> Generator[Dict[str, Any], None, None]:
    """
    Generate a streaming completion from the LLM.

    Makes a streaming call to the OpenAI API and yields chunks as they
    arrive. Suitable for long responses where you want to show progress.

    Args:
        messages: List of message dictionaries with 'role' and 'content'.
        context: Runtime context containing:
                 - execution_id: Unique identifier for this execution
                 - auth_config: Authentication configuration
                 - ssl_config: SSL configuration
        llm_params: Optional LLM parameters:
                    - model: Model to use (defaults to medium tier)
                    - temperature: Temperature setting
                    - max_tokens: Maximum tokens
                    - Additional OpenAI API parameters

    Yields:
        Response chunks as they arrive from the API.

        # Yields: {
        #     "id": "chatcmpl-...",
        #     "choices": [{"delta": {"content": "Hello"}, "index": 0}],
        #     "created": 1234567890
        # }

    Raises:
        Exception: If the API call fails.
    """
    logger = get_logger()

    # Extract LLM parameters with defaults
    if llm_params is None:
        llm_params = {}

    # Get model configuration using helper
    model, temperature, max_tokens, model_tier = _get_model_config(
        llm_params.get("model"), llm_params.get("temperature"), llm_params.get("max_tokens")
    )

    # Remove our known params, pass rest as kwargs
    kwargs = {
        k: v for k, v in llm_params.items() if k not in ["model", "temperature", "max_tokens"]
    }

    logger.info(
        "Starting LLM streaming",
        execution_id=context["execution_id"],
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        message_count=len(messages),
    )

    try:
        client = _get_llm_client(context["auth_config"], context["ssl_config"], model_tier)

        stream_response = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
            **kwargs,
        )

        chunk_count = 0
        for chunk in stream_response:
            chunk_count += 1
            yield chunk.model_dump()

        logger.info(
            "LLM streaming completed",
            execution_id=context["execution_id"],
            model=model,
            chunks=chunk_count,
        )

    except Exception as e:
        logger.error(
            "LLM streaming failed",
            execution_id=context["execution_id"],
            model=model,
            error=str(e),
        )
        raise


def complete_with_tools(
    messages: List[Dict[str, str]],
    tools: List[Dict[str, Any]],
    context: Dict[str, Any],
    llm_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Generate a completion with tool/function calling capabilities.

    Makes a call to the OpenAI API with tools defined, allowing the model
    to call functions and return structured responses.

    Args:
        messages: List of message dictionaries with 'role' and 'content'.
        tools: List of tool definitions for function calling.
        context: Runtime context containing:
                 - execution_id: Unique identifier for this execution
                 - auth_config: Authentication configuration
                 - ssl_config: SSL configuration
        llm_params: Optional LLM parameters:
                    - model: Model to use (defaults to large tier for tools)
                    - temperature: Temperature setting
                    - max_tokens: Maximum tokens
                    - Additional OpenAI API parameters

    Returns:
        Response dictionary containing the completion with tool calls.

        # Returns: {
        #     "id": "chatcmpl-...",
        #     "choices": [{
        #         "message": {
        #             "role": "assistant",
        #             "tool_calls": [{"id": "...", "function": {"name": "...", "arguments": "..."}}]
        #         }
        #     }],
        #     "usage": {"prompt_tokens": 10, "completion_tokens": 20, "total_tokens": 30}
        # }

    Raises:
        Exception: If the API call fails.
    """
    logger = get_logger()

    # Extract LLM parameters with defaults
    if llm_params is None:
        llm_params = {}

    # Get model configuration using helper (default to large for tools)
    model, temperature, max_tokens, model_tier = _get_model_config(
        llm_params.get("model"),
        llm_params.get("temperature"),
        llm_params.get("max_tokens"),
        default_tier="large",  # Tools need better reasoning
    )

    # Remove our known params, pass rest as kwargs
    kwargs = {
        k: v for k, v in llm_params.items() if k not in ["model", "temperature", "max_tokens"]
    }

    logger.info(
        "Generating LLM completion with tools",
        execution_id=context["execution_id"],
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        message_count=len(messages),
        tool_count=len(tools),
    )

    try:
        client = _get_llm_client(context["auth_config"], context["ssl_config"], model_tier)

        response = client.chat.completions.create(
            model=model,
            messages=messages,
            tools=tools,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )

        # Convert response to dict
        response_dict = response.model_dump()

        logger.info(
            "LLM tool completion successful",
            execution_id=context["execution_id"],
            model=model,
            usage=response_dict.get("usage"),
            has_tool_calls=bool(
                response_dict.get("choices", [{}])[0].get("message", {}).get("tool_calls")
            ),
        )

        return response_dict

    except Exception as e:
        logger.error(
            "LLM tool completion failed",
            execution_id=context["execution_id"],
            model=model,
            error=str(e),
        )
        raise


def check_connection(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Check the LLM connection with a simple prompt.

    Sends a basic test message to verify that authentication and
    connectivity are working properly.

    Args:
        context: Runtime context containing:
                 - execution_id: Unique identifier for this execution
                 - auth_config: Authentication configuration
                 - ssl_config: SSL configuration

    Returns:
        Test response with status and details.

        # Returns: {
        #     "status": "success",
        #     "model": "gpt-3.5-turbo",
        #     "response": "Hello! I'm working properly.",
        #     "auth_method": "api_key"
        # }
    """
    logger = get_logger()

    logger.info(
        "Testing LLM connection",
        execution_id=context["execution_id"],
        auth_method=context["auth_config"].get("method"),
        base_url=config.llm.base_url,
    )

    test_messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Say 'Hello! I'm working properly.' and nothing else."},
    ]

    try:
        # Use small model for testing (faster and cheaper)
        response = complete(
            messages=test_messages,
            context=context,
            llm_params={
                "model": config.llm.small.model,
                "temperature": 0,  # Deterministic for testing
                "max_tokens": 50,
            },
        )

        content = response["choices"][0]["message"]["content"]

        result = {
            "status": "success",
            "model": config.llm.small.model,
            "response": content,
            "auth_method": context["auth_config"].get("method"),
            "base_url": config.llm.base_url,
        }

        logger.info(
            "LLM connection test successful",
            execution_id=context["execution_id"],
            response=content,
        )

        return result

    except Exception as e:  # pylint: disable=broad-exception-caught
        result = {
            "status": "failed",
            "error": str(e),
            "auth_method": context["auth_config"].get("method"),
            "base_url": config.llm.base_url,
        }

        logger.error(
            "LLM connection test failed",
            execution_id=context["execution_id"],
            error=str(e),
        )

        return result
