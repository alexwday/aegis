"""
Combination of overview narratives from multiple sources.

Takes overview paragraphs from RTS regulatory filings and earnings call transcripts,
and synthesizes them into a single cohesive executive summary.
"""

import json
from typing import Any, Dict

from aegis.connections.llm_connector import complete_with_tools
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.utils.logging import get_logger
from aegis.utils.prompt_loader import load_prompt_from_db


async def combine_overview_narratives(
    rts_overview: str,
    transcript_overview: str,
    bank_name: str,
    quarter: str,
    fiscal_year: int,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Combine overview narratives from RTS and transcript into a single summary.

    Takes the best elements from both sources to create a comprehensive
    executive overview that captures both the formal regulatory narrative
    and the management's earnings call messaging.

    Args:
        rts_overview: Overview paragraph from RTS extraction
        transcript_overview: Overview paragraph from transcript extraction
        bank_name: Bank name for context
        quarter: Quarter (e.g., "Q2")
        fiscal_year: Fiscal year
        context: Execution context

    Returns:
        Dict with:
            - narrative: Combined overview paragraph
            - combination_notes: Brief explanation of synthesis approach
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.overview_combination.start",
        execution_id=execution_id,
        rts_length=len(rts_overview),
        transcript_length=len(transcript_overview),
    )

    # Handle edge cases without LLM call
    if not rts_overview and not transcript_overview:
        return {
            "narrative": "",
            "combination_notes": "No overview content from either source",
        }

    if not rts_overview:
        return {
            "narrative": transcript_overview,
            "combination_notes": "Only transcript overview available",
        }

    if not transcript_overview:
        return {
            "narrative": rts_overview,
            "combination_notes": "Only RTS overview available",
        }

    # Load prompt from database
    prompt_data = load_prompt_from_db(
        layer="bank_earnings_report_etl",
        name="combined_1_keymetrics_overview",
        compose_with_globals=False,
        execution_id=execution_id,
    )

    # Format prompts with dynamic content
    system_prompt = prompt_data["system_prompt"].format(
        bank_name=bank_name,
        quarter=quarter,
        fiscal_year=fiscal_year,
    )
    user_prompt = prompt_data["user_prompt"].format(
        rts_overview=rts_overview,
        transcript_overview=transcript_overview,
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("combined_1_keymetrics_overview")

        response = await complete_with_tools(
            messages=messages,
            tools=[prompt_data["tool_definition"]],
            context=context,
            llm_params={
                "model": model,
                "temperature": etl_config.temperature,
                "max_tokens": etl_config.max_tokens,
            },
        )

        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])

                combined = function_args.get("combined_overview", "")
                notes = function_args.get("combination_notes", "")

                logger.info(
                    "etl.overview_combination.complete",
                    execution_id=execution_id,
                    combined_length=len(combined),
                    combination_notes=notes,
                )

                return {"narrative": combined, "combination_notes": notes}

        # Fallback: prefer transcript if LLM fails
        logger.warning(
            "etl.overview_combination.no_result",
            execution_id=execution_id,
        )
        return {
            "narrative": transcript_overview,
            "combination_notes": "Fallback: using transcript overview due to LLM error",
        }

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "etl.overview_combination.error",
            execution_id=execution_id,
            error=str(e),
        )
        return {
            "narrative": transcript_overview or rts_overview,
            "combination_notes": f"Fallback due to error: {str(e)[:50]}",
        }
