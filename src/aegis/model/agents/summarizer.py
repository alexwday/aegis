"""
Summarizer agent for synthesizing multiple database responses.

The summarizer agent takes responses from multiple database subagents
and creates a unified, coherent answer that addresses the user's
original query while preserving source attribution.
"""

from typing import Any, Dict, Generator, List
from ...connections.llm_connector import stream
from ...utils.logging import get_logger
from ...utils.prompt_loader import load_yaml


def synthesize_responses(
    conversation_history: List[Dict[str, str]],
    latest_message: str,
    database_responses: List[Dict[str, Any]],
    context: Dict[str, Any],
) -> Generator[Dict[str, str], None, None]:
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
        # prompt_version = summarizer_data.get("version", "unknown")
        # prompt_last_updated = summarizer_data.get("last_updated", "unknown")
        prompt_parts = []

        # Add global context prompts
        try:
            project_data = load_yaml("global/project.yaml")
            if "content" in project_data:
                prompt_parts.append(project_data["content"].strip())
        except Exception:
            pass

        try:
            restrictions_data = load_yaml("global/restrictions.yaml")
            if "content" in restrictions_data:
                prompt_parts.append(restrictions_data["content"].strip())
        except Exception:
            pass

        # Add summarizer-specific prompt
        if "content" in summarizer_data:
            prompt_parts.append(summarizer_data["content"].strip())

        # Build the synthesis-specific instructions
        synthesis_instructions = """
<synthesis_instructions>
You are providing a QUICK SUMMARY of the database responses. Full details are available in the
dropdown menus BELOW this message.

CRITICAL INSTRUCTION: The dropdown menus are positioned BELOW this summary message.
NEVER say "above" - ALWAYS say "below" when referring to the dropdowns.

Your task:
1. Give a direct, one-sentence answer to the user's question
2. Highlight 2-3 KEY findings only (the most important metrics/insights)
3. Tell the user which dropdown BELOW contains which type of information
4. Keep it BRIEF - under 200 words total

User's Original Query: {user_query}

Database Responses (full details in dropdowns BELOW):
{database_responses}

Format:
- Paragraph 1: Direct answer with 2-3 key metrics
- Paragraph 2: Brief guide to dropdowns BELOW (e.g., "See Benchmarking dropdown below for full
  metrics, Transcripts dropdown below for management commentary")
- DO NOT reproduce all the data - users can expand dropdowns BELOW for that
- Focus on the ESSENTIAL takeaway only

REMINDER: The dropdowns are BELOW this message. Always use "below" when referring to them.
Never use "above".
</synthesis_instructions>
"""

        # Format database responses for the prompt
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

        # Build the complete system prompt
        system_prompt = "\n\n---\n\n".join(prompt_parts)
        system_prompt += synthesis_instructions.format(
            user_query=latest_message, database_responses=all_responses
        )

        # Build messages for the LLM
        messages = [{"role": "system", "content": system_prompt}]

        # Add limited conversation history for context (last 5 messages)
        for msg in conversation_history[-5:]:
            messages.append({"role": msg.get("role", "user"), "content": msg.get("content", "")})

        # Add the synthesis request
        messages.append(
            {
                "role": "user",
                "content": (
                    f"Provide a BRIEF summary (under 200 words) for: {latest_message}\n\n"
                    "IMPORTANT: The dropdown menus are positioned BELOW this summary. "
                    "When referencing them, ALWAYS say 'below' (e.g., 'See the Benchmarking "
                    "dropdown below'). NEVER say 'above'. Give only key findings and direct "
                    "users to the dropdowns BELOW for full details."
                ),
            }
        )

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

        for chunk in stream(messages=messages, context=context, llm_params=llm_params):
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
