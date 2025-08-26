"""
Benchmarking Subagent - Educational Placeholder Implementation

This file demonstrates how to build a subagent that integrates with Aegis.
It shows the key utilities you'll need: logging, prompts, LLM calls, and monitoring.

HOW THIS IS CALLED FROM AEGIS:
The main workflow (src/aegis/model/main.py) calls this when "benchmarking" is needed:

    from .main import benchmarking_agent

    for chunk in benchmarking_agent(
        conversation=conversation,
        latest_message=latest_message,
        bank_period_combinations=bank_period_combinations,
        basic_intent=basic_intent,
        full_intent=full_intent,
        database_id="benchmarking",
        context=context
    ):
        yield chunk

TO REPLACE THIS: Keep the function name and signature exactly the same.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Generator, List

# Import Aegis utilities
from ....utils.logging import get_logger
from ....utils.prompt_loader import load_subagent_prompt
from ....utils.settings import config
from ....connections.llm_connector import stream
from ....utils.monitor import add_monitor_entry, format_llm_call


def benchmarking_agent(
    conversation: List[Dict[str, str]],
    latest_message: str,
    bank_period_combinations: List[Dict[str, Any]],
    basic_intent: str,
    full_intent: str,
    database_id: str,
    context: Dict[str, Any],
) -> Generator[Dict[str, str], None, None]:
    """
    Benchmarking subagent - generates placeholder benchmarking data.

    This function MUST:
    1. Accept these exact 7 parameters
    2. Yield dictionaries with type="subagent", name=database_id, content=text
    3. Handle errors gracefully

    Parameters from Aegis:
        conversation: Full chat history [{"role": "user/assistant", "content": "..."}]
        latest_message: Most recent user message
        bank_period_combinations: Banks/periods to query [{bank_id, bank_name, bank_symbol, fiscal_year, quarter}]
        basic_intent: Simple interpretation (e.g., "efficiency ratio query")
        full_intent: Detailed interpretation
        database_id: Always "benchmarking" for this subagent
        context: Contains execution_id, auth_config, ssl_config
    """

    # ==================================================
    # STEP 1: Initialize logging and tracking
    # ==================================================
    logger = get_logger()
    execution_id = context.get("execution_id")
    stage_start = datetime.now(timezone.utc)

    # Log what we received (with preview of data)
    logger.info(
        f"subagent.{database_id}.started",
        execution_id=execution_id,
        latest_message=(
            latest_message[:100] + "..." if len(latest_message) > 100 else latest_message
        ),
        num_banks=len(bank_period_combinations),
        basic_intent=basic_intent,
        conversation_length=len(conversation),
    )

    # Log the banks and periods we're working with
    for combo in bank_period_combinations[:3]:  # Show first 3 as preview
        logger.debug(
            f"subagent.{database_id}.bank_period",
            execution_id=execution_id,
            bank=f"{combo['bank_name']} ({combo['bank_symbol']})",
            period=f"{combo['quarter']} {combo['fiscal_year']}",
        )

    try:
        # ==================================================
        # STEP 2: Process input to extract key information
        # ==================================================
        # Extract unique banks and periods for our prompt
        banks_text = ", ".join(
            [f"{combo['bank_name']} ({combo['bank_symbol']})" for combo in bank_period_combinations]
        )
        periods_text = ", ".join(
            [f"{combo['quarter']} {combo['fiscal_year']}" for combo in bank_period_combinations]
        )

        logger.debug(
            f"subagent.{database_id}.processed_input",
            execution_id=execution_id,
            banks_summary=banks_text[:100],
            periods_summary=periods_text[:100],
        )

        # ==================================================
        # STEP 3: Load prompt from YAML with global contexts
        # ==================================================
        # This loads from prompts/benchmarking/benchmarking.yaml
        # and automatically includes fiscal, project, and restrictions
        try:
            system_prompt = load_subagent_prompt("benchmarking")
            logger.debug(
                f"subagent.{database_id}.prompt_loaded",
                execution_id=execution_id,
                prompt_size=len(system_prompt),
            )
        except Exception as e:
            logger.warning(
                f"subagent.{database_id}.prompt_load_failed",
                execution_id=execution_id,
                error=str(e),
            )
            # Fallback prompt if YAML loading fails
            system_prompt = """You are the Benchmarking subagent for Aegis.
            Generate realistic placeholder benchmarking data based on the context provided."""

        # ==================================================
        # STEP 4: Build the user prompt with context
        # ==================================================
        user_prompt = f"""Generate benchmarking data for this request:

User Query: {full_intent}
Latest Message: {latest_message}

Banks Requested: {banks_text}
Periods Requested: {periods_text}
Basic Intent: {basic_intent}

Create realistic placeholder benchmarking data that would answer this query.
Include specific details appropriate for benchmarking data.
End with: *[Benchmarking placeholder data - test mode]*"""

        # ==================================================
        # STEP 5: Call the LLM and stream response
        # ==================================================
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        # Track LLM timing for monitoring
        llm_start = datetime.now(timezone.utc)
        total_content = ""
        chunk_count = 0
        final_usage = None

        # Get model configuration from settings
        # Using medium tier for subagents (can be changed)
        model_tier = "medium"
        model_config = getattr(config.llm, model_tier)

        logger.debug(
            f"subagent.{database_id}.llm_call_start",
            execution_id=execution_id,
            message_count=len(messages),
            model=model_config.model,
            model_tier=model_tier,
        )

        # Stream response from LLM
        for chunk in stream(
            messages=messages,
            context=context,  # Contains auth_config and ssl_config
            llm_params={
                "temperature": 0.7,  # Some creativity for realistic data
                "max_tokens": 500,  # Reasonable response length
                "model": model_config.model,  # Use model from config
            },
        ):
            # Process streaming chunks
            if chunk.get("choices") and chunk["choices"][0].get("delta"):
                delta = chunk["choices"][0]["delta"]
                if "content" in delta and delta["content"] is not None:
                    content = delta["content"]
                    total_content += content
                    chunk_count += 1

                    # YIELD CHUNK TO AEGIS MAIN
                    # This is what gets streamed to the user
                    yield {
                        "type": "subagent",
                        "name": database_id,
                        "content": content,
                    }

            # Capture usage stats from final chunk
            if chunk.get("usage"):
                final_usage = chunk["usage"]

        llm_end = datetime.now(timezone.utc)
        llm_duration_ms = int((llm_end - llm_start).total_seconds() * 1000)

        # Log LLM completion
        logger.info(
            f"subagent.{database_id}.llm_completed",
            execution_id=execution_id,
            total_tokens=final_usage.get("total_tokens") if final_usage else 0,
            response_length=len(total_content),
            chunks_streamed=chunk_count,
            duration_ms=llm_duration_ms,
            model=model_config.model,
        )

        # ==================================================
        # STEP 6: Add monitoring entry for observability
        # ==================================================
        # Format LLM call details for monitoring
        llm_calls = []
        if final_usage:
            # Calculate cost using config rates
            prompt_tokens = final_usage.get("prompt_tokens", 0)
            completion_tokens = final_usage.get("completion_tokens", 0)

            # Cost calculation using config values (per 1k tokens)
            cost = model_config.cost_per_1k_input * (
                prompt_tokens / 1000
            ) + model_config.cost_per_1k_output * (completion_tokens / 1000)

            llm_call = format_llm_call(
                model=model_config.model,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                cost=cost,
                duration_ms=llm_duration_ms,
            )
            llm_calls.append(llm_call)

        # Add comprehensive monitoring entry
        stage_end = datetime.now(timezone.utc)
        add_monitor_entry(
            stage_name="Subagent_Benchmarking",
            stage_start_time=stage_start,
            stage_end_time=stage_end,
            status="Success",
            llm_calls=llm_calls if llm_calls else None,
            decision_details=f"Generated benchmarking data for {len(bank_period_combinations)} bank-period combinations",
            custom_metadata={
                "subagent": database_id,
                "banks": [combo["bank_id"] for combo in bank_period_combinations],
                "intent": basic_intent,
                "response_length": len(total_content),
                "model_tier": model_tier,
            },
        )

        logger.info(
            f"subagent.{database_id}.completed",
            execution_id=execution_id,
            total_duration_ms=int((stage_end - stage_start).total_seconds() * 1000),
        )

    except Exception as e:
        # ==================================================
        # ERROR HANDLING
        # ==================================================
        error_msg = str(e)
        logger.error(
            f"subagent.{database_id}.error",
            execution_id=execution_id,
            error=error_msg,
            exc_info=True,  # Include stack trace
        )

        # Add monitoring entry for the failure
        add_monitor_entry(
            stage_name="Subagent_Benchmarking",
            stage_start_time=stage_start,
            stage_end_time=datetime.now(timezone.utc),
            status="Failure",
            error_message=error_msg,
            custom_metadata={
                "subagent": database_id,
                "error_type": type(e).__name__,
            },
        )

        # Yield error message to user
        yield {
            "type": "subagent",
            "name": database_id,
            "content": f"\n⚠️ Error in Benchmarking subagent: {error_msg}\n",
        }


# NOTES FOR REPLACING THIS IMPLEMENTATION:
#
# 1. Keep the function name: benchmarking_agent
# 2. Keep all 7 parameters exactly as shown
# 3. Always yield {"type": "subagent", "name": database_id, "content": "..."}
# 4. Use the utilities demonstrated:
#    - get_logger() for logging
#    - load_subagent_prompt() for prompts
#    - stream() for LLM calls
#    - add_monitor_entry() for tracking
#    - config for model selection and cost calculation
# 5. The context parameter contains:
#    - execution_id: For tracking this request
#    - auth_config: For API authentication
#    - ssl_config: For SSL settings
# 6. Model configuration comes from config.llm.{small|medium|large}:
#    - model: The model name
#    - cost_per_1k_input: Input token cost
#    - cost_per_1k_output: Output token cost
# 7. Replace the LLM call with your own data retrieval logic if needed
