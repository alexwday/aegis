"""
LLM-based extraction for Management Narrative section (transcript quotes).

Process:
1. Retrieve Management Discussion (MD) section chunks from transcript
2. Format entire section for LLM context
3. Single LLM call to extract top 5 impactful quotes with speaker attribution
4. Return structured JSON for the report template

Future: RTS summaries will be extracted separately, then a final LLM call
will interleave RTS entries and transcript quotes into the final narrative flow.
"""

import json
from typing import Any, Dict, List

from aegis.connections.llm_connector import complete_with_tools
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.etls.bank_earnings_report.retrieval.transcripts import (
    format_md_section_for_llm,
    retrieve_md_chunks,
)
from aegis.utils.logging import get_logger
from aegis.utils.prompt_loader import load_prompt_from_db


async def extract_transcript_quotes(
    bank_info: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    num_quotes: int = 5,
) -> List[Dict[str, Any]]:
    """
    Extract top management quotes from earnings call Management Discussion section.

    Process:
    1. Retrieve all MD section chunks from transcript
    2. Format for LLM with speaker block organization
    3. Single LLM call to extract top N impactful quotes
    4. Return list of quote entries formatted for template

    Args:
        bank_info: Bank information dict with bank_id, bank_name, bank_symbol
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context
        num_quotes: Number of quotes to extract (default 5)

    Returns:
        List of quote entries, each with:
            - type: "transcript"
            - content: The quote or paraphrased statement
            - speaker: Full name of speaker
            - title: Speaker's title (CEO, CFO, etc.)
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.bank_earnings_report.management_narrative_extraction_start",
        execution_id=execution_id,
        bank=bank_info["bank_symbol"],
        period=f"{quarter} {fiscal_year}",
        num_quotes=num_quotes,
    )

    chunks = await retrieve_md_chunks(
        bank_id=bank_info["bank_id"],
        fiscal_year=fiscal_year,
        quarter=quarter,
        context=context,
    )

    if not chunks:
        logger.warning(
            "etl.bank_earnings_report.no_md_chunks",
            execution_id=execution_id,
            bank=bank_info["bank_symbol"],
        )
        return []

    md_content = format_md_section_for_llm(
        chunks=chunks,
        bank_name=bank_info["bank_name"],
        quarter=quarter,
        fiscal_year=fiscal_year,
    )

    if not md_content.strip():
        logger.warning(
            "etl.bank_earnings_report.empty_md_content",
            execution_id=execution_id,
            bank=bank_info["bank_symbol"],
        )
        return []

    # Load prompt from database
    prompt_data = load_prompt_from_db(
        layer="bank_earnings_report_etl",
        name="transcript_2_narrative_quotes",
        compose_with_globals=False,
        execution_id=execution_id,
    )

    # Format prompts with dynamic content
    system_prompt = prompt_data["system_prompt"].format(num_quotes=num_quotes)
    user_prompt = prompt_data["user_prompt"].format(
        num_quotes=num_quotes,
        bank_name=bank_info["bank_name"],
        quarter=quarter,
        fiscal_year=fiscal_year,
        md_content=md_content,
    )

    # Build tool definition with dynamic constraints
    tool_def = prompt_data["tool_definition"]
    tool_def["function"]["description"] = f"Extract the top {num_quotes} management quotes"
    tool_def["function"]["parameters"]["properties"]["quotes"]["minItems"] = num_quotes
    tool_def["function"]["parameters"]["properties"]["quotes"]["maxItems"] = num_quotes
    tool_def["function"]["parameters"]["properties"]["quotes"][
        "description"
    ] = f"Array of exactly {num_quotes} management quotes"

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("transcript_2_narrative_quotes")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_def],
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

                quotes = function_args.get("quotes", [])

                formatted_quotes = []
                for quote in quotes:
                    if quote.get("content") and quote.get("speaker") and quote.get("title"):
                        formatted_quotes.append(
                            {
                                "type": "transcript",
                                "content": quote["content"],
                                "speaker": quote["speaker"],
                                "title": quote["title"],
                            }
                        )

                logger.info(
                    "etl.bank_earnings_report.management_narrative_extraction_complete",
                    execution_id=execution_id,
                    quotes_extracted=len(formatted_quotes),
                    speakers=[q["speaker"] for q in formatted_quotes],
                )

                return formatted_quotes

        logger.warning(
            "etl.bank_earnings_report.management_narrative_no_result",
            execution_id=execution_id,
        )
        return []

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.management_narrative_extraction_error",
            execution_id=execution_id,
            error=str(e),
        )
        return []
