"""
Router agent for determining query processing path.

The router analyzes user queries and conversation history to decide whether
to use direct response or trigger the research workflow.
"""

import json
from typing import Any, Dict, List

from ...connections.llm_connector import complete_with_tools
from ...utils.logging import get_logger
from ...utils.prompt_loader import load_prompt_from_db


async def route_query(
    conversation_history: List[Dict[str, str]],
    latest_message: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Router agent determines whether to use direct response or research workflow.

    Args:
        conversation_history: Previous messages in the conversation
        latest_message: The current user query
        context: Runtime context with auth, SSL config, and filtered database prompt

    Returns:
        Dictionary with routing decision:
        {
            "route": "direct_response" or "research_workflow",
            "rationale": "Explanation of decision",
            "confidence": 0.0-1.0,
            "status": "Success" or "Error",
            "error": Optional error message,
            "prompt_version": Prompt version string,
            "prompt_last_updated": Prompt last updated date
        }
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    try:
        # Load router prompt from database with global composition
        available_dbs = context.get("available_databases", [])
        router_data = load_prompt_from_db(
            layer="aegis",
            name="router",
            compose_with_globals=True,
            available_databases=available_dbs,
            execution_id=execution_id,
        )

        # Extract version info for return values
        prompt_version = router_data.get("version") or "not_set"
        prompt_last_updated = (
            str(router_data.get("updated_at")) if router_data.get("updated_at") else "unknown"
        )

        # Get composed prompt (already includes globals)
        final_system_prompt = router_data.get(
            "composed_prompt", router_data.get("system_prompt", "")
        )

        # Build user message from template (limit to last 10 messages for context)
        user_prompt_template = router_data.get("user_prompt", "")
        conversation_json = json.dumps(
            conversation_history[-10:] if conversation_history else [], indent=2
        )
        user_content = user_prompt_template.format(
            conversation_history=conversation_json, current_query=latest_message
        )

        # Create messages for LLM
        messages = [
            {"role": "system", "content": final_system_prompt},
            {"role": "user", "content": user_content},
        ]

        # Load tools from database record
        tool_definition = router_data.get("tool_definition")
        if tool_definition:
            # Single tool definition - wrap in array
            tools = [tool_definition] if isinstance(tool_definition, dict) else tool_definition
        else:
            # Check for plural version
            tools = router_data.get("tool_definitions", [])

        if not tools:
            logger.error(
                "router.tools_missing",
                execution_id=execution_id,
                error="Failed to load tools from database",
            )
            return {
                "status": "Error",
                "route": "research_workflow",  # Default to research on error
                "error": "Router tools not found in database",
            }

        # Get model configuration (medium is optimal for fast binary decisions)
        from ...utils.settings import config

        model_tier_override = context.get("model_tier_override")
        if model_tier_override == "small":
            model = config.llm.small.model
            max_tokens = config.llm.small.max_tokens
        elif model_tier_override == "large":
            model = config.llm.large.model
            max_tokens = config.llm.large.max_tokens
        else:
            model = config.llm.medium.model  # Default to medium for speed and accuracy balance
            max_tokens = config.llm.medium.max_tokens

        # Attempt tool call up to 3 times (initial + 2 retries)
        max_attempts = 3
        tokens_used = 0
        cost = 0
        last_error_reason = None

        for attempt in range(1, max_attempts + 1):
            response = await complete_with_tools(
                messages=messages,
                tools=tools,
                context=context,
                llm_params={
                    "model": model,
                    "temperature": 0.1,  # Very low temperature for deterministic routing
                    "max_tokens": max_tokens,
                    "tool_choice": "required",  # Force tool use
                },
            )

            # Extract metrics from response
            metrics = response.get("metrics", {})
            usage = response.get("usage", {})
            tokens_used = usage.get("total_tokens", 0)
            cost = metrics.get("total_cost", 0)

            # Extract tool call response
            if response.get("choices") and response["choices"][0].get("message"):
                message = response["choices"][0]["message"]
                if message.get("tool_calls"):
                    tool_call = message["tool_calls"][0]
                    try:
                        function_args = json.loads(tool_call["function"]["arguments"])
                    except (TypeError, ValueError) as e:
                        last_error_reason = f"tool_args_parse_error: {e}"
                    else:
                        # Handle binary response (property name: routing_decision)
                        binary_route = function_args.get("routing_decision")
                        route = "direct_response" if binary_route == 0 else "research_workflow"

                        # Generate simple rationale based on route
                        if binary_route == 0:
                            rationale = "Direct response from conversation history"
                        else:
                            rationale = "Data retrieval required"

                        logger.info(
                            "router.decision",
                            execution_id=execution_id,
                            route=route,
                            binary=binary_route,
                            rationale=rationale,
                            tokens_used=tokens_used,
                            cost=cost,
                        )

                        return {
                            "status": "Success",
                            "route": route,
                            "rationale": rationale,
                            "tokens_used": tokens_used,
                            "cost": cost,
                            "response_time_ms": metrics.get("response_time", 0) * 1000,
                            "model_used": model,
                            "prompt_version": prompt_version,
                            "prompt_last_updated": prompt_last_updated,
                        }
                else:
                    last_error_reason = "no_tool_calls"
            else:
                last_error_reason = "no_message"

            if attempt < max_attempts:
                logger.warning(
                    "router.retrying_tool_call",
                    execution_id=execution_id,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    reason=last_error_reason,
                )
                continue

        # Fallback to research workflow after retries (safe default)
        logger.warning(
            "router.fallback_to_research",
            execution_id=execution_id,
            reason=last_error_reason,
        )
        return {
            "status": "Success",
            "route": "research_workflow",
            "rationale": "Defaulting to research workflow - no clear routing decision",
            "tokens_used": tokens_used,
            "cost": cost,
            "response_time_ms": (
                metrics.get("response_time", 0) * 1000 if "metrics" in locals() else 0
            ),
            "model_used": model,
            "prompt_version": prompt_version,
            "prompt_last_updated": prompt_last_updated,
        }

    except Exception as e:
        logger.error("router.error", execution_id=execution_id, error=str(e))
        return {
            "status": "Error",
            "route": "research_workflow",
            "rationale": "Defaulting to research workflow due to router error",
            "tokens_used": 0,
            "cost": 0,
            "response_time_ms": 0,
            "error": str(e),
            "prompt_version": prompt_version if "prompt_version" in locals() else "unknown",
            "prompt_last_updated": (
                prompt_last_updated if "prompt_last_updated" in locals() else "unknown"
            ),
        }
