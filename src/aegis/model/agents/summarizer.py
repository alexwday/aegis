"""
Summarizer agent for synthesizing multiple database responses.

The summarizer agent takes responses from multiple database subagents
and creates a unified, coherent answer that addresses the user's
original query while preserving source attribution.
"""

from typing import Any, AsyncGenerator, Dict, List
from ...connections.llm_connector import stream
from ...utils.logging import get_logger
from ...utils.prompt_loader import load_yaml, load_global_prompts_for_agent


async def synthesize_responses(
    conversation_history: List[Dict[str, str]],
    latest_message: str,
    database_responses: List[Dict[str, Any]],
    context: Dict[str, Any],
) -> AsyncGenerator[Dict[str, str], None]:
    """
    Synthesize multiple database responses into a unified answer.

    This agent:
    - Aggregates information from all database responses
    - Removes redundancy while preserving unique insights
    - Creates a coherent narrative answering the user's query
    - Attributes information to source databases
    - Handles conflicting information gracefully

    Args:
        conversation_history: Previous messages in the conversation
        latest_message: The original user query
        database_responses: List of responses from database subagents, each containing:
            - database_id: Identifier of the database (e.g., "benchmarking")
            - full_intent: The specific query sent to this database
            - response: The full text response from the subagent
        context: Runtime context with auth, SSL config, and execution_id

    Yields:
        Dictionary with type="agent", name="aegis", content=chunk
        This appears in the main chat bubble, not a dropdown
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    try:
        logger.info(
            "summarizer.starting",
            execution_id=execution_id,
            database_count=len(database_responses),
            databases=[r.get("database_id") for r in database_responses],
        )

        # Load summarizer prompt
        summarizer_data = load_yaml("aegis/summarizer.yaml")

        # Load global context (uses_global from YAML)
        available_dbs = context.get("available_databases", [])
        uses_global = summarizer_data.get("uses_global", [])
        globals_prompt = load_global_prompts_for_agent(uses_global, available_dbs)

        # Build system prompt
        prompt_parts = []
        if globals_prompt:
            prompt_parts.append(globals_prompt)

        agent_system_prompt = summarizer_data.get("system_prompt", "")
        if agent_system_prompt:
            prompt_parts.append(agent_system_prompt.strip())

        system_prompt = "\n\n---\n\n".join(prompt_parts)

        # Format database responses for the user prompt
        formatted_responses = []
        for resp in database_responses:
            db_id = resp.get("database_id", "unknown")
            intent = resp.get("full_intent", "")
            content = resp.get("response", "")

            # Clean the response - remove test mode indicators if present
            if content:
                content = content.replace("*[", "").replace(" placeholder data - test mode]*", "")
                content = content.strip()

            formatted_responses.append(
                f"""
<database_response source="{db_id}">
Query Intent: {intent}
Response:
{content}
</database_response>"""
            )

        # Combine all responses
        all_responses = "\n".join(formatted_responses)

        # Load and format user prompt template
        user_prompt_template = summarizer_data.get("user_prompt_template", "")
        user_message = user_prompt_template.format(
            user_query=latest_message, database_responses=all_responses
        )

        # Build messages for the LLM
        messages = [{"role": "system", "content": system_prompt}]

        # Add limited conversation history for context (last 5 messages)
        for msg in conversation_history[-5:]:
            messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})

        # Add user message with database responses and synthesis request
        messages.append({"role": "user", "content": user_message})

        # Determine model - use large model for best synthesis quality
        from ...utils.settings import config

        model = config.llm.large.model

        # LLM parameters for synthesis
        llm_params = {
            "model": model,
            "temperature": 0.3,  # Lower temperature for consistency
            "max_tokens": 300,  # Brief summary only - details in dropdowns
        }

        # Stream the synthesized response
        total_content = ""
        chunk_count = 0
        final_usage = None

        async for chunk in stream(messages=messages, context=context, llm_params=llm_params):
            # Handle OpenAI streaming chunk format
            if chunk.get("choices") and chunk["choices"][0].get("delta"):
                delta = chunk["choices"][0]["delta"]
                if "content" in delta and delta["content"] is not None:
                    content = delta["content"]
                    total_content += content
                    chunk_count += 1

                    # Yield the chunk with agent schema
                    yield {
                        "type": "agent",
                        "name": "aegis",
                        "content": content,
                    }

            # Capture usage data if present
            if chunk.get("usage"):
                final_usage = chunk["usage"]

        # Log completion
        logger.info(
            "summarizer.completed",
            execution_id=execution_id,
            tokens_used=final_usage.get("total_tokens") if final_usage else None,
            total_chars=len(total_content),
            chunk_count=chunk_count,
            databases_synthesized=len(database_responses),
        )

    except Exception as e:
        logger.error(
            "summarizer.error",
            execution_id=execution_id,
            error=str(e),
        )

        # Yield error message
        yield {
            "type": "agent",
            "name": "aegis",
            "content": f"\n⚠️ Error in summarizer: {str(e)}\n",
        }
