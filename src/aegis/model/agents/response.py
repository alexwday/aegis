"""
Response agent for generating direct responses.

The response agent handles queries that don't require data retrieval,
including greetings, clarifications, concept explanations, and general
assistance about the Aegis system.
"""

from typing import Any, AsyncGenerator, Dict, List, Union

from ...connections.llm_connector import complete, stream
from ...utils.logging import get_logger
from ...utils.sql_prompt import prompt_manager


async def generate_response(
    conversation_history: List[Dict[str, str]],
    latest_message: str,
    context: Dict[str, Any],
    streaming: bool = False,
) -> Union[Dict[str, Any], AsyncGenerator[Dict[str, Any], None]]:
    """
    Generate a direct response without data retrieval.

    This agent handles:
    - Greetings, thanks, and acknowledgments
    - Questions about Aegis capabilities and usage
    - Financial concept explanations and definitions
    - Reformatting existing data from conversation history
    - Clarification requests and conversational corrections

    Args:
        conversation_history: Previous messages in the conversation
        latest_message: The current user query
        context: Runtime context with auth, SSL config, and execution_id
        streaming: Whether to stream the response or return complete

    Returns:
        If streaming=False: Dictionary with response and metadata
        If streaming=True: Generator yielding response chunks

    Response format:
        {
            "status": "Success" or "Error",
            "response": Complete response text (non-streaming),
            "tokens_used": Number of tokens consumed,
            "cost": Cost in dollars,
            "response_time_ms": Response time in milliseconds,
            "model_used": Model identifier,
            "prompt_version": Prompt version string,
            "prompt_last_updated": Prompt last updated date,
            "error": Optional error message
        }

    For streaming, yields chunks with:
        {
            "type": "chunk" or "final",
            "content": Text chunk (for "chunk" type),
            "response": Complete response (for "final" type),
            "tokens_used": Token count (for "final" type),
            "cost": Cost (for "final" type),
            "response_time_ms": Time (for "final" type),
            "model_used": Model (for "final" type)
        }
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    try:
        # Load response agent prompt from database
        response_data = prompt_manager.get_latest_prompt(
            model="aegis",
            layer="aegis",
            name="response",
            system_prompt=False
        )

        # Extract version info for tracking
        prompt_version = response_data.get("version", "unknown")
        prompt_last_updated = response_data.get("updated_at", "unknown")

        # Load global context from database
        available_dbs = context.get("available_databases", [])
        uses_global = response_data.get("uses_global", [])
        global_order = ["fiscal", "project", "database", "restrictions"]
        global_prompt_parts = []

        for global_name in global_order:
            if global_name not in uses_global:
                continue

            if global_name == "fiscal":
                from ...utils.prompt_loader import _load_fiscal_prompt
                global_prompt_parts.append(_load_fiscal_prompt())
            elif global_name == "database":
                from ...utils.database_filter import get_database_prompt
                database_prompt = get_database_prompt(available_dbs)
                global_prompt_parts.append(database_prompt)
            else:
                try:
                    global_data = prompt_manager.get_latest_prompt(
                        model="aegis",
                        layer="global",
                        name=global_name,
                        system_prompt=False
                    )
                    if global_data.get("system_prompt"):
                        global_prompt_parts.append(global_data["system_prompt"].strip())
                except Exception as e:
                    logger.warning(
                        "response.global_prompt_missing",
                        execution_id=execution_id,
                        global_name=global_name,
                        error=str(e)
                    )

        globals_prompt = "\n\n---\n\n".join(global_prompt_parts) if global_prompt_parts else ""

        # Build system prompt
        agent_system_prompt = response_data.get("system_prompt", "")

        # Join globals + agent prompt
        prompt_parts = []
        if globals_prompt:
            prompt_parts.append(globals_prompt)
        if agent_system_prompt:
            prompt_parts.append(agent_system_prompt.strip())
        system_prompt = "\n\n---\n\n".join(prompt_parts)

        # Build conversation context
        messages = [{"role": "system", "content": system_prompt}]

        # Add recent conversation history (last 10 messages)
        for msg in conversation_history[-10:]:
            messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})

        # Load and format user prompt template
        user_prompt_template = response_data.get("user_prompt_template", "")
        if user_prompt_template:
            user_content = user_prompt_template.format(latest_message=latest_message)
            messages.append({"role": "user", "content": user_content})
        else:
            # Fallback to direct message if no template (shouldn't happen)
            messages.append({"role": "user", "content": latest_message})

        # Determine model tier - response agent uses large model by default for best quality
        from ...utils.settings import config

        model_tier_override = context.get("model_tier_override")
        if model_tier_override == "small":
            model = config.llm.small.model
            max_tokens = config.llm.small.max_tokens
        elif model_tier_override == "medium":
            model = config.llm.medium.model
            max_tokens = config.llm.medium.max_tokens
        else:
            model = config.llm.large.model  # Default to large for high-quality responses
            max_tokens = config.llm.large.max_tokens

        # LLM parameters for response generation
        llm_params = {
            "model": model,
            "temperature": 0.7,  # Balanced creativity for natural responses
            "max_tokens": max_tokens,
        }

        if streaming:
            # Stream response for real-time interaction
            return _stream_response(
                messages=messages,
                context=context,
                llm_params=llm_params,
                execution_id=execution_id,
                prompt_version=prompt_version,
                prompt_last_updated=prompt_last_updated,
                model=model,
            )
        else:
            # Generate complete response
            response = await complete(
                messages=messages,
                context=context,
                llm_params=llm_params,
            )

            # Extract response content
            content = ""
            if response.get("choices") and response["choices"][0].get("message"):
                content = response["choices"][0]["message"].get("content", "")

            # Extract metrics
            metrics = response.get("metrics", {})
            usage = response.get("usage", {})

            logger.info(
                "response.generated",
                execution_id=execution_id,
                response_length=len(content),
                tokens_used=usage.get("total_tokens", 0),
                cost=metrics.get("total_cost", 0),
            )

            return {
                "status": "Success",
                "response": content,
                "tokens_used": usage.get("total_tokens", 0),
                "cost": metrics.get("total_cost", 0),
                "response_time_ms": metrics.get("response_time", 0) * 1000,
                "model_used": model,
                "prompt_version": prompt_version,
                "prompt_last_updated": prompt_last_updated,
            }

    except Exception as e:
        logger.error("response.error", execution_id=execution_id, error=str(e))

        # Return error response
        error_response = {
            "status": "Error",
            "response": "I apologize, but I encountered an error generating a response.",
            "tokens_used": 0,
            "cost": 0,
            "response_time_ms": 0,
            "error": str(e),
            "prompt_version": prompt_version if "prompt_version" in locals() else "unknown",
            "prompt_last_updated": (
                prompt_last_updated if "prompt_last_updated" in locals() else "unknown"
            ),
        }

        if streaming:
            # For streaming, return an async generator that yields error as final chunk
            async def error_generator():
                yield {"type": "final", **error_response}

            return error_generator()
        else:
            return error_response


async def _stream_response(
    messages: List[Dict[str, str]],
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
    execution_id: str,
    prompt_version: str,
    prompt_last_updated: str,
    model: str,
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Internal function to handle streaming responses.

    Yields chunks of the response as they're generated, then yields
    a final chunk with complete metadata.
    """
    logger = get_logger()

    try:
        # Track accumulated response
        full_response = ""
        chunk_count = 0
        final_usage = None

        # Stream from LLM
        async for chunk in stream(messages=messages, context=context, llm_params=llm_params):
            if chunk.get("choices") and chunk["choices"][0].get("delta"):
                delta = chunk["choices"][0]["delta"]
                if "content" in delta and delta["content"] is not None:
                    content = delta["content"]
                    full_response += content
                    chunk_count += 1

                    # Yield content chunk
                    yield {
                        "type": "chunk",
                        "content": content,
                    }

            # Capture usage data if present (usually in final chunk)
            if chunk.get("usage"):
                final_usage = chunk["usage"]

        # After streaming completes, always send final chunk
        logger.info(
            "response.streamed",
            execution_id=execution_id,
            chunks=chunk_count,
            response_length=len(full_response),
            tokens_used=final_usage.get("total_tokens", 0) if final_usage else 0,
        )

        # Yield final chunk with metadata
        yield {
            "type": "final",
            "status": "Success",
            "response": full_response,
            "tokens_used": final_usage.get("total_tokens", 0) if final_usage else 0,
            "cost": 0,  # Cost calculation would need to be done based on tokens
            "response_time_ms": 0,  # Would need timing info from stream
            "model_used": model,
            "prompt_version": prompt_version,
            "prompt_last_updated": prompt_last_updated,
        }

    except Exception as e:
        logger.error("response.stream_error", execution_id=execution_id, error=str(e))

        # Yield error as final chunk
        yield {
            "type": "final",
            "status": "Error",
            "response": "I apologize, but I encountered an error while streaming the response.",
            "tokens_used": 0,
            "cost": 0,
            "response_time_ms": 0,
            "error": str(e),
            "prompt_version": prompt_version,
            "prompt_last_updated": prompt_last_updated,
        }
