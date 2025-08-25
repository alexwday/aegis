"""
Router agent for determining query processing path.

The router analyzes user queries and conversation history to decide whether
to use direct response or trigger the research workflow.
"""

import json
from typing import Any, Dict, List

from ...connections.llm_connector import complete_with_tools
from ...utils.logging import get_logger
from ...utils.prompt_loader import load_yaml


def route_query(
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
        # Load base router prompt with project context
        router_data = load_yaml("aegis/router.yaml")
        
        # Extract version info for tracking
        prompt_version = router_data.get("version", "unknown")
        prompt_last_updated = router_data.get("last_updated", "unknown")
        prompt_parts = []

        # Add project context
        project_data = load_yaml("global/project.yaml")
        if "content" in project_data:
            prompt_parts.append(project_data["content"].strip())

        # Add filtered database context from main
        if context.get("database_prompt"):
            prompt_parts.append(context["database_prompt"])

        # Add router-specific content
        if "content" in router_data:
            prompt_parts.append(router_data["content"].strip())

        router_prompt = "\n\n---\n\n".join(prompt_parts)

        # Format conversation context for router
        conversation_context = {
            "conversation_history": conversation_history[-10:] if conversation_history else [],
            "current_query": latest_message,
        }

        # Build user message content
        conversation_json = json.dumps(conversation_context["conversation_history"], indent=2)
        current_query = conversation_context["current_query"]
        user_content = f"Conversation: {conversation_json}\nCurrent query: {current_query}"

        # Add available databases from context
        available_dbs = context.get("available_databases", [])
        if available_dbs:
            user_content += f"\nAvailable databases: {', '.join(available_dbs)}"

        # Create messages for LLM
        messages = [
            {"role": "system", "content": router_prompt},
            {"role": "user", "content": user_content},
        ]

        # Extract tool definition from prompt
        tool_def = None
        if "tool_definition: |" in router_prompt:
            tool_json = router_prompt.split("tool_definition: |")[1].strip()
            tool_def = json.loads(tool_json)
        else:
            # Fallback tool definition (binary)
            tool_def = {
                "name": "route",
                "description": "Binary routing decision: 0=direct_response, 1=research_workflow",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "r": {
                            "type": "integer",
                            "enum": [0, 1],
                            "description": "0=direct_response, 1=research_workflow",
                        },
                    },
                    "required": ["r"],
                },
            }

        # Define the routing tool
        tools = [{"type": "function", "function": tool_def}]

        # Call LLM with tool
        # Router uses medium model for fast, accurate binary decisions
        # Get model based on override or default to medium
        from ...utils.settings import config

        model_tier_override = context.get("model_tier_override")
        if model_tier_override == "small":
            model = config.llm.small.model
        elif model_tier_override == "large":
            model = config.llm.large.model
        else:
            model = config.llm.medium.model  # Default to medium

        response = complete_with_tools(
            messages=messages,
            tools=tools,
            context=context,
            llm_params={
                "model": model,
                "temperature": 0.1,  # Very low temperature for deterministic binary routing
                "max_tokens": 50,  # Binary response needs minimal tokens
            },
        )

        # Extract metrics from response
        metrics = response.get("metrics", {})
        usage = response.get("usage", {})

        # Extract tool call response
        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])

                # Handle binary response
                binary_route = function_args.get("r")
                route = "direct_response" if binary_route == 0 else "research_workflow"

                # For binary response, we generate simple rationale based on route
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
                    tokens_used=usage.get("total_tokens", 0),
                    cost=metrics.get("total_cost", 0),
                )

                return {
                    "status": "Success",
                    "route": route,
                    "rationale": rationale,
                    "tokens_used": usage.get("total_tokens", 0),
                    "cost": metrics.get("total_cost", 0),
                    "response_time_ms": metrics.get("response_time", 0) * 1000,
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
            "prompt_version": prompt_version if 'prompt_version' in locals() else "unknown",
            "prompt_last_updated": prompt_last_updated if 'prompt_last_updated' in locals() else "unknown",
        }

    # Default to research workflow if no tool response
    logger.warning("router.default", execution_id=execution_id)
    return {
        "status": "Success",
        "route": "research_workflow",
        "rationale": "Defaulting to research workflow - no clear routing decision",
        "tokens_used": usage.get("total_tokens", 0),
        "cost": metrics.get("total_cost", 0),
        "response_time_ms": metrics.get("response_time", 0) * 1000,
        "prompt_version": prompt_version,
        "prompt_last_updated": prompt_last_updated,
    }
