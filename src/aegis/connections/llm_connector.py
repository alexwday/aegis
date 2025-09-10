"""
LLM connector module for OpenAI API integration.

This module handles all interactions with OpenAI's API, supporting both
OAuth and API key authentication, with configurable model tiers.
"""

from typing import Any, Dict, Generator, List, Optional
import time
import httpx
from openai import OpenAI

from ..utils.logging import get_logger
from ..utils.settings import config

# Module-level client cache to reuse connections
_client_cache: Dict[str, OpenAI] = {}


# Cost tracking utilities integrated directly
def _calculate_cost(
    usage: Dict,
    cost_per_1k_input: float,
    cost_per_1k_output: Optional[float] = None,
    response_time: float = 0.0,
    model: str = "",
) -> Dict:
    """
    Calculate cost metrics from token usage.

    Args:
        usage: Usage dictionary from API response containing token counts
        cost_per_1k_input: Cost per 1000 input tokens in USD
        cost_per_1k_output: Cost per 1000 output tokens in USD (None for embeddings)
        response_time: Time taken for the API call in seconds
        model: Model name used for the operation

    Returns:
        Dictionary with calculated costs and metrics
    """
    prompt_tokens = usage.get("prompt_tokens", 0)
    completion_tokens = usage.get("completion_tokens")
    total_tokens = usage.get("total_tokens", prompt_tokens)

    # Calculate prompt cost
    prompt_cost = (prompt_tokens / 1000.0) * cost_per_1k_input

    # Calculate completion cost (if applicable)
    completion_cost = None
    if completion_tokens is not None and cost_per_1k_output is not None:
        completion_cost = (completion_tokens / 1000.0) * cost_per_1k_output
        total_cost = prompt_cost + completion_cost
    else:
        # For embeddings, only prompt cost
        total_cost = prompt_cost

    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "prompt_cost": round(prompt_cost, 6),
        "completion_cost": round(completion_cost, 6) if completion_cost else None,
        "total_cost": round(total_cost, 6),
        "response_time": round(response_time, 3),
        "model": model,
    }


def _format_cost_for_logging(metrics: Dict) -> Dict:
    """
    Format metrics for structured logging output.

    Args:
        metrics: Dictionary of metrics to format

    Returns:
        Dictionary formatted for logging
    """
    # Simplified format - single line instead of nested dicts
    log_data = {
        "cost": f"${metrics['total_cost']:.6f}",
    }

    return log_data


class ResponseTimer:
    """
    Context manager for timing API responses.
    """

    def __init__(self):
        """Initialize the timer."""
        self.start_time = None
        self.elapsed = 0.0

    def __enter__(self):
        """Start the timer."""
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop the timer and calculate elapsed time."""
        self.elapsed = time.time() - self.start_time
        return False


def _calculate_and_log_metrics(
    usage: Dict[str, Any], model_tier: str, context: Dict[str, Any], operation_type: str
) -> Dict[str, Any]:
    """
    Calculate cost metrics and log them.

    Args:
        usage: Usage dictionary from API response
        model_tier: Model tier (small, medium, large)
        context: Context with model, response_time, execution_id, logger
        operation_type: Type of operation for log message

    Returns:
        Metrics dictionary
    """
    model_config = getattr(config.llm, model_tier)
    metrics = _calculate_cost(
        usage=usage,
        cost_per_1k_input=model_config.cost_per_1k_input,
        cost_per_1k_output=model_config.cost_per_1k_output,
        response_time=context["response_time"],
        model=context["model"],
    )

    # Simplified logging - just show key metrics, not full usage details
    log_data = {
        "execution_id": context["execution_id"],
        "model": context["model"],
        "tokens": usage.get("total_tokens", 0),
        "response_time_ms": int(context["response_time"] * 1000),
        **_format_cost_for_logging(metrics),
    }

    context["logger"].info(f"LLM {operation_type} successful", **log_data)

    return metrics


def _calculate_embedding_metrics(
    usage: Dict[str, Any], context: Dict[str, Any], operation_type: str
) -> Dict[str, Any]:
    """
    Calculate cost metrics for embeddings and log them.

    Args:
        usage: Usage dictionary from API response
        context: Context with model, response_time, execution_id, logger, vector_info
        operation_type: Type of operation for log message

    Returns:
        Metrics dictionary
    """
    metrics = _calculate_cost(
        usage=usage,
        cost_per_1k_input=config.llm.embedding.cost_per_1k_input,
        cost_per_1k_output=None,  # Embeddings don't have output tokens
        response_time=context["response_time"],
        model=context["model"],
    )

    log_data = {
        "execution_id": context["execution_id"],
        "model": context["model"],
        "usage": usage,
        **context.get("vector_info", {}),
        **_format_cost_for_logging(metrics),
    }

    context["logger"].info(f"{operation_type} successful", **log_data)

    return metrics


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
        "embedding": config.llm.embedding.timeout,
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
    llm_params = llm_params or {}

    # Get model configuration using helper
    model, temperature, max_tokens, model_tier = _get_model_config(
        llm_params.get("model"), llm_params.get("temperature"), llm_params.get("max_tokens")
    )

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

        # Check if it's an o-series model (reasoning models)
        # These models don't support the temperature parameter
        is_o_series = (
            model in ['o1', 'o3', 'o4'] or 
            model.startswith('o1-') or 
            model.startswith('o3-') or 
            model.startswith('o4-')
        )
        
        # Build API parameters based on model type
        api_params = {
            "model": model,
            "messages": messages,
        }
        
        if is_o_series:
            # o-series models: no temperature, use max_completion_tokens
            if max_tokens:
                api_params["max_completion_tokens"] = max_tokens
        else:
            # Regular models: standard parameters
            api_params["temperature"] = temperature
            api_params["max_tokens"] = max_tokens
        
        # Add any extra parameters
        api_params.update({
            k: v
            for k, v in llm_params.items()
            if k not in ["model", "temperature", "max_tokens"]
        })

        # Time the API call
        with ResponseTimer() as timer:
            response = client.chat.completions.create(**api_params)

        # Convert response to dict
        response_dict = response.model_dump()

        # Calculate and log metrics
        response_dict["metrics"] = _calculate_and_log_metrics(
            usage=response_dict.get("usage", {}),
            model_tier=model_tier,
            context={
                "model": model,
                "response_time": timer.elapsed,
                "execution_id": context["execution_id"],
                "logger": logger,
            },
            operation_type="completion",
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


def stream(  # pylint: disable=too-many-locals
    # Complex streaming logic requires multiple local vars for metrics, timing, and state tracking.
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
    llm_params = llm_params or {}

    # Get model configuration using helper
    model, temperature, max_tokens, model_tier = _get_model_config(
        llm_params.get("model"), llm_params.get("temperature"), llm_params.get("max_tokens")
    )

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

        # Start timing
        start_time = time.time()

        # Check if it's an o-series model (reasoning models)
        # These models don't support the temperature parameter
        is_o_series = (
            model in ['o1', 'o3', 'o4'] or 
            model.startswith('o1-') or 
            model.startswith('o3-') or 
            model.startswith('o4-')
        )
        
        # Build API parameters based on model type
        api_params = {
            "model": model,
            "messages": messages,
            "stream": True,
        }
        
        if is_o_series:
            # o-series models: no temperature, use max_completion_tokens
            if max_tokens:
                api_params["max_completion_tokens"] = max_tokens
        else:
            # Regular models: standard parameters
            api_params["temperature"] = temperature
            api_params["max_tokens"] = max_tokens
        
        # Add any extra parameters
        api_params.update({
            k: v
            for k, v in llm_params.items()
            if k not in ["model", "temperature", "max_tokens"]
        })
        
        # Remove our known params, pass rest as kwargs
        stream_response = client.chat.completions.create(**api_params)

        chunk_count = 0
        accumulated_usage = None

        for chunk in stream_response:
            chunk_count += 1
            chunk_dict = chunk.model_dump()

            # Accumulate usage from the final chunk (if present)
            if chunk_dict.get("usage"):
                accumulated_usage = chunk_dict["usage"]

            yield chunk_dict

        # Calculate elapsed time
        elapsed = time.time() - start_time

        # Log streaming completion
        if accumulated_usage:
            _calculate_and_log_metrics(
                usage=accumulated_usage,
                model_tier=model_tier,
                context={
                    "model": model,
                    "response_time": elapsed,
                    "execution_id": context["execution_id"],
                    "logger": logger,
                },
                operation_type=f"streaming completed (chunks={chunk_count})",
            )
        else:
            logger.info(
                "LLM streaming completed",
                execution_id=context["execution_id"],
                model=model,
                chunks=chunk_count,
                response_time=elapsed,
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
    llm_params = llm_params or {}

    # Get model configuration using helper (default to large for tools)
    model, temperature, max_tokens, model_tier = _get_model_config(
        llm_params.get("model"),
        llm_params.get("temperature"),
        llm_params.get("max_tokens"),
        default_tier="large",  # Tools need better reasoning
    )

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

        # Check if it's an o-series model (reasoning models)
        # These models don't support temperature parameter
        is_o_series = (
            model in ['o1', 'o3', 'o4'] or 
            model.startswith('o1-') or 
            model.startswith('o3-') or 
            model.startswith('o4-')
        )
        
        # Build API parameters based on model type
        api_params = {
            "model": model,
            "messages": messages,
            "tools": tools,
        }
        
        if is_o_series:
            # O-series models: no temperature, use max_completion_tokens
            if max_tokens:
                api_params["max_completion_tokens"] = max_tokens
        else:
            # Regular models: standard parameters
            api_params["temperature"] = temperature
            api_params["max_tokens"] = max_tokens
        
        # Add any extra parameters
        api_params.update({
            k: v
            for k, v in llm_params.items()
            if k not in ["model", "temperature", "max_tokens"]
        })

        # Time the API call
        with ResponseTimer() as timer:
            response = client.chat.completions.create(**api_params)

        # Convert response to dict
        response_dict = response.model_dump()

        # Check if tools were called
        has_tool_calls = bool(
            response_dict.get("choices", [{}])[0].get("message", {}).get("tool_calls")
        )

        # Calculate and log metrics
        response_dict["metrics"] = _calculate_and_log_metrics(
            usage=response_dict.get("usage", {}),
            model_tier=model_tier,
            context={
                "model": model,
                "response_time": timer.elapsed,
                "execution_id": context["execution_id"],
                "logger": logger,
            },
            operation_type=f"tool completion (has_tool_calls={has_tool_calls})",
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


def embed(
    input_text: str,
    context: Dict[str, Any],
    embedding_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Generate embedding vector for input text.

    Creates a vector representation of the input text using OpenAI's
    embedding models. Suitable for similarity search, clustering, and
    other vector operations.

    Args:
        input_text: Text to generate embedding for.
        context: Runtime context containing:
                 - execution_id: Unique identifier for this execution
                 - auth_config: Authentication configuration
                 - ssl_config: SSL configuration
        embedding_params: Optional embedding parameters:
                          - model: Embedding model to use (defaults to configured model)
                          - dimensions: Vector dimensions (for models that support it)
                          - Additional OpenAI API parameters

    Returns:
        Response dictionary containing the embedding vector.

        # Returns: {
        #     "data": [{
        #         "embedding": [0.123, -0.456, ...],  # Vector of floats
        #         "index": 0,
        #         "object": "embedding"
        #     }],
        #     "model": "text-embedding-3-large",
        #     "usage": {"prompt_tokens": 10, "total_tokens": 10}
        # }

    Raises:
        Exception: If the API call fails.
    """
    logger = get_logger()

    # Extract embedding parameters with defaults
    if embedding_params is None:
        embedding_params = {}

    # Get embedding configuration
    model = embedding_params.get("model", config.llm.embedding.model)
    dimensions = embedding_params.get("dimensions", config.llm.embedding.dimensions)

    # Remove our known params, pass rest as kwargs
    kwargs = {k: v for k, v in embedding_params.items() if k not in ["model", "dimensions"]}

    # Add dimensions if supported by the model
    if "text-embedding-3" in model and dimensions:
        kwargs["dimensions"] = dimensions

    logger.info(
        "Generating text embedding",
        execution_id=context["execution_id"],
        model=model,
        dimensions=dimensions if "text-embedding-3" in model else "default",
        input_length=len(input_text),
    )

    try:
        # Use embedding timeout for client
        client = _get_llm_client(context["auth_config"], context["ssl_config"], "embedding")

        # Time the API call
        with ResponseTimer() as timer:
            response = client.embeddings.create(model=model, input=input_text, **kwargs)

        # Convert response to dict
        response_dict = response.model_dump()

        # Calculate and log metrics
        response_dict["metrics"] = _calculate_embedding_metrics(
            usage=response_dict.get("usage", {}),
            context={
                "model": model,
                "response_time": timer.elapsed,
                "execution_id": context["execution_id"],
                "logger": logger,
                "vector_info": {"vector_length": len(response_dict["data"][0]["embedding"])},
            },
            operation_type="Embedding generation",
        )

        return response_dict

    except Exception as e:
        logger.error(
            "Embedding generation failed",
            execution_id=context["execution_id"],
            model=model,
            error=str(e),
        )
        raise


def embed_batch(
    input_texts: List[str],
    context: Dict[str, Any],
    embedding_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Generate embeddings for multiple texts in a single API call.

    Creates vector representations for multiple input texts efficiently
    using OpenAI's batch embedding capability.

    Args:
        input_texts: List of texts to generate embeddings for.
        context: Runtime context containing:
                 - execution_id: Unique identifier for this execution
                 - auth_config: Authentication configuration
                 - ssl_config: SSL configuration
        embedding_params: Optional embedding parameters:
                          - model: Embedding model to use (defaults to configured model)
                          - dimensions: Vector dimensions (for models that support it)
                          - Additional OpenAI API parameters

    Returns:
        Response dictionary containing embedding vectors for all inputs.

        # Returns: {
        #     "data": [
        #         {"embedding": [...], "index": 0, "object": "embedding"},
        #         {"embedding": [...], "index": 1, "object": "embedding"},
        #         ...
        #     ],
        #     "model": "text-embedding-3-large",
        #     "usage": {"prompt_tokens": 100, "total_tokens": 100}
        # }

    Raises:
        Exception: If the API call fails.
    """
    logger = get_logger()

    # Extract embedding parameters with defaults
    if embedding_params is None:
        embedding_params = {}

    # Get embedding configuration
    model = embedding_params.get("model", config.llm.embedding.model)
    dimensions = embedding_params.get("dimensions", config.llm.embedding.dimensions)

    # Remove our known params, pass rest as kwargs
    kwargs = {k: v for k, v in embedding_params.items() if k not in ["model", "dimensions"]}

    # Add dimensions if supported by the model
    if "text-embedding-3" in model and dimensions:
        kwargs["dimensions"] = dimensions

    logger.info(
        "Generating batch embeddings",
        execution_id=context["execution_id"],
        model=model,
        dimensions=dimensions if "text-embedding-3" in model else "default",
        batch_size=len(input_texts),
        total_chars=sum(len(text) for text in input_texts),
    )

    try:
        # Use embedding timeout for client
        client = _get_llm_client(context["auth_config"], context["ssl_config"], "embedding")

        # Time the API call
        with ResponseTimer() as timer:
            response = client.embeddings.create(model=model, input=input_texts, **kwargs)

        # Convert response to dict
        response_dict = response.model_dump()

        # Calculate and log metrics
        response_dict["metrics"] = _calculate_embedding_metrics(
            usage=response_dict.get("usage", {}),
            context={
                "model": model,
                "response_time": timer.elapsed,
                "execution_id": context["execution_id"],
                "logger": logger,
                "vector_info": {
                    "vectors_generated": len(response_dict["data"]),
                    "vector_length": (
                        len(response_dict["data"][0]["embedding"]) if response_dict["data"] else 0
                    ),
                },
            },
            operation_type="Batch embedding generation",
        )

        return response_dict

    except Exception as e:
        logger.error(
            "Batch embedding generation failed",
            execution_id=context["execution_id"],
            model=model,
            batch_size=len(input_texts),
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
        # Connection check must catch all errors to report any connectivity issues without crashing.
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
