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

    system_prompt = f"""You are a senior financial analyst creating an executive summary for \
{bank_name}'s {quarter} {fiscal_year} quarterly earnings report.

## YOUR TASK

Synthesize two overview paragraphs (one from the regulatory filing, one from the earnings call) \
into a single cohesive executive summary. The final paragraph should be 4-6 sentences \
(80-120 words).

## SOURCE CHARACTERISTICS

**RTS (Regulatory Filing)**:
- Formal, compliance-oriented language
- Focus on financial performance and capital metrics
- Objective, factual tone
- May include risk and regulatory themes

**Transcript (Earnings Call)**:
- Management's narrative and messaging
- Strategic themes and forward-looking perspective
- More dynamic, confident tone
- May include market context and priorities

## SYNTHESIS GUIDELINES

1. **Combine Strengths**: Take factual foundation from RTS and strategic color from transcript
2. **Avoid Redundancy**: Don't repeat the same theme twice with different wording
3. **Unified Voice**: Write as a single cohesive narrative, not two stitched paragraphs
4. **Balance**: Include both performance themes (RTS) and strategic direction (transcript)
5. **No Metrics**: Keep it qualitative - specific numbers are in other sections

## STYLE

- Executive summary tone - concise and insightful
- Third person perspective
- Should feel like the opening paragraph of a professional analyst report
- Smooth flow from performance to strategy to outlook"""

    user_prompt = f"""Synthesize these two overview paragraphs into a single executive summary:

## From Regulatory Filing (RTS):
{rts_overview}

## From Earnings Call Transcript:
{transcript_overview}

Create a unified 4-6 sentence overview that combines the best elements from both sources."""

    tool_definition = {
        "type": "function",
        "function": {
            "name": "create_combined_overview",
            "description": "Synthesize RTS and transcript overviews into unified summary",
            "parameters": {
                "type": "object",
                "properties": {
                    "combined_overview": {
                        "type": "string",
                        "description": (
                            "Combined overview paragraph (4-6 sentences, 80-120 words). "
                            "Synthesizes key themes from both sources into cohesive narrative. "
                            "No specific metrics."
                        ),
                    },
                    "combination_notes": {
                        "type": "string",
                        "description": (
                            "Brief note on synthesis: what themes came from each source, "
                            "how they were combined. 1-2 sentences."
                        ),
                    },
                },
                "required": ["combined_overview", "combination_notes"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("overview_combination")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
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
