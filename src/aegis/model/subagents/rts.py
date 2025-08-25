"""
RTS (Real-Time System) subagent for live financial data.

This placeholder implementation generates test RTS data
for development and testing purposes.
"""

from typing import Any, Dict, Generator, List
from ...connections.llm_connector import stream
from ...utils.logging import get_logger


def rts_agent(
    conversation: List[Dict[str, str]],
    latest_message: str,
    banks: Dict[str, Any],
    periods: Dict[str, Any],
    basic_intent: str,
    full_intent: str,
    database_id: str,
    context: Dict[str, Any],
) -> Generator[Dict[str, str], None, None]:
    """
    Stream real-time system data response.

    This placeholder generates test real-time financial data including:
    - Current trading metrics
    - Intraday performance
    - Real-time positions
    - Market data
    - Live risk metrics

    Args:
        conversation: Full conversation history
        latest_message: Latest user message
        banks: Banks information from clarifier
        periods: Periods information from clarifier
        basic_intent: Basic intent from clarifier
        full_intent: Full query intent from planner
        database_id: Database ID (should be "rts")
        context: Runtime context with auth, SSL config, execution_id

    Yields:
        Dictionary with type="subagent", name="rts", content=chunk
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    subagent_name = "RTS"
    subagent_description = """The RTS (Real-Time System) database provides:
    - Live trading data and market metrics
    - Current day performance and volumes
    - Real-time positions and exposures
    - Intraday P&L and risk metrics

    - Current pricing and spreads

    This data includes up-to-the-minute financial metrics, trading volumes,
    market positions, and live performance indicators that update throughout
    the trading day."""

    try:
        logger.info(
            f"subagent.{database_id}.starting",
            execution_id=execution_id,
            subagent=subagent_name,
            bank_count=len(banks.get("bank_ids", [])),
        )

        # Extract bank details for the prompt
        banks_detail = banks.get("banks_detail", {})
        bank_names = []
        for bank_id, bank_info in banks_detail.items():
            bank_names.append(f"{bank_info['name']} ({bank_info['symbol']})")

        # Format periods for the prompt
        period_description = ""
        if "periods" in periods:
            period_data = periods["periods"]
            if "apply_all" in period_data:
                p = period_data["apply_all"]
                period_description = f"{', '.join(p['quarters'])} {p['fiscal_year']}"
            elif "bank_specific" in period_data:
                period_parts = []
                for bid, p in period_data["bank_specific"].items():
                    if bid in banks_detail:
                        bank_name = banks_detail[bid]["name"]
                        period_parts.append(
                            f"{bank_name}: {', '.join(p['quarters'])} {p['fiscal_year']}"
                        )
                period_description = "; ".join(period_parts)

        # Build the prompt for GPT to generate placeholder data
        system_prompt = f"""You are a placeholder {subagent_name} subagent for the Aegis system.
This is a TEST implementation. Generate a plausible, realistic response based on the context
provided.

Subagent Role: {subagent_description}

Context:
- Banks: {', '.join(bank_names) if bank_names else 'Not specified'}
- Periods: {period_description if period_description else 'Not specified'}
- User Intent: {basic_intent if basic_intent else 'General query'}
- Full Query: {full_intent}

Generate a response that:
1. Appears to be real data from the {subagent_name} database
2. Is relevant to the banks and periods specified
3. Addresses the user's query intent
4. Includes specific (but fictional) numbers, percentages, or details
5. Formats the response appropriately for the data type

Remember: This is test data for development purposes. Make it realistic but clearly indicate \
it's placeholder data at the end."""
        user_prompt = f"""Based on the query: "{full_intent}"

Generate a {subagent_name} response with placeholder data that would realistically come from \
this database.
Latest user message: {latest_message}"""

        # Create messages for streaming
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        # Stream the response from GPT
        total_content = ""
        chunk_count = 0
        final_usage = None

        for chunk in stream(
            messages=messages,
            context=context,
            llm_params={
                "temperature": 0.7,
                "max_tokens": 500,
            },
        ):
            # Handle OpenAI streaming chunk format (same as response agent)
            if chunk.get("choices") and chunk["choices"][0].get("delta"):
                delta = chunk["choices"][0]["delta"]
                if "content" in delta and delta["content"] is not None:
                    content = delta["content"]
                    total_content += content
                    chunk_count += 1

                    # Yield the chunk with subagent schema
                    yield {
                        "type": "subagent",
                        "name": database_id,
                        "content": content,
                    }

            # Capture usage data if present (usually in final chunk)
            if chunk.get("usage"):
                final_usage = chunk["usage"]

        # Log completion metrics
        logger.info(
            f"subagent.{database_id}.completed",
            execution_id=execution_id,
            subagent=subagent_name,
            tokens_used=final_usage.get("total_tokens") if final_usage else None,
            total_chars=len(total_content),
            chunk_count=chunk_count,
        )

        # Add a small disclaimer at the end
        disclaimer = f"\n\n*[{subagent_name} placeholder data - test mode]*"
        yield {
            "type": "subagent",
            "name": database_id,
            "content": disclaimer,
        }

    except Exception as e:
        logger.error(
            f"subagent.{database_id}.error",
            execution_id=execution_id,
            subagent=subagent_name,
            error=str(e),
        )

        # Yield error message
        yield {
            "type": "subagent",
            "name": database_id,
            "content": f"\n⚠️ Error in {subagent_name} subagent: {str(e)}\n",
        }
