"""
LLM-based extraction for Analyst Focus section.

Process:
1. Retrieve Q&A section chunks from transcript
2. Group chunks by qa_group_id (each represents one Q&A exchange)
3. For each Q&A group, call LLM to extract theme/question/answer
4. Return structured JSON for the report template
"""

import json
from typing import Any, Dict, List

from aegis.connections.llm_connector import complete_with_tools
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.etls.bank_earnings_report.retrieval.transcripts import (
    format_qa_group_for_llm,
    get_qa_group_summary,
    group_chunks_by_qa_id,
    retrieve_qa_chunks,
)
from aegis.utils.logging import get_logger
from aegis.utils.prompt_loader import load_prompt_from_db


async def extract_qa_entry(
    qa_group_id: int,
    chunks: List[Dict[str, Any]],
    bank_name: str,
    quarter: str,
    fiscal_year: int,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extract theme, question summary, and answer summary from a single Q&A exchange.

    Args:
        qa_group_id: The Q&A group ID
        chunks: List of chunks in this Q&A group
        bank_name: Bank name
        quarter: Quarter
        fiscal_year: Fiscal year
        context: Execution context

    Returns:
        Dict with theme, question, answer, or None if extraction failed
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    qa_content = format_qa_group_for_llm(qa_group_id, chunks, bank_name, quarter, fiscal_year)
    qa_summary = get_qa_group_summary(chunks)

    if not qa_content.strip():
        logger.warning(
            "etl.bank_earnings_report.empty_qa_content",
            execution_id=execution_id,
            qa_group_id=qa_group_id,
        )
        return None

    # Load prompt from database
    prompt_data = load_prompt_from_db(
        layer="bank_earnings_report_etl",
        name="transcript_3_analystfocus_extraction",
        compose_with_globals=False,
        execution_id=execution_id,
    )

    # Format user prompt with dynamic content
    user_prompt = prompt_data["user_prompt"].format(
        bank_name=bank_name,
        quarter=quarter,
        fiscal_year=fiscal_year,
        qa_content=qa_content,
    )

    messages = [
        {"role": "system", "content": prompt_data["system_prompt"]},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("transcript_3_analystfocus_extraction")

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

                should_skip = function_args.get("should_skip", False)
                if should_skip:
                    logger.info(
                        "etl.bank_earnings_report.qa_skipped",
                        execution_id=execution_id,
                        qa_group_id=qa_group_id,
                        reason="No meaningful financial content",
                    )
                    return None

                theme = function_args.get("theme", "")
                question = function_args.get("question", "")
                answer = function_args.get("answer", "")

                if theme and question and answer:
                    logger.info(
                        "etl.bank_earnings_report.qa_extracted",
                        execution_id=execution_id,
                        qa_group_id=qa_group_id,
                        theme=theme,
                    )
                    return {
                        "theme": theme,
                        "question": question,
                        "answer": answer,
                        "qa_group_id": qa_group_id,
                        "topics": qa_summary.get("topics", []),
                    }

        logger.warning(
            "etl.bank_earnings_report.qa_extraction_no_result",
            execution_id=execution_id,
            qa_group_id=qa_group_id,
        )
        return None

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.qa_extraction_error",
            execution_id=execution_id,
            qa_group_id=qa_group_id,
            error=str(e),
        )
        return None


async def rank_qa_entries(
    entries: List[Dict[str, Any]],
    bank_name: str,
    quarter: str,
    fiscal_year: int,
    context: Dict[str, Any],
    num_featured: int = 4,
) -> List[int]:
    """
    Use LLM to rank Q&A entries and select the top N for featuring.

    Args:
        entries: List of extracted Q&A entries with theme, question, answer
        bank_name: Bank name
        quarter: Quarter
        fiscal_year: Fiscal year
        context: Execution context
        num_featured: Number of top entries to select

    Returns:
        List of indices (0-based) of the top entries to feature
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if len(entries) <= num_featured:
        return list(range(len(entries)))

    # Format entries for ranking
    entries_text = ""
    for i, entry in enumerate(entries):
        entries_text += f"""
### Entry {i + 1}: {entry['theme']}
**Question:** {entry['question']}
**Answer:** {entry['answer']}

---
"""

    # Load prompt from database
    prompt_data = load_prompt_from_db(
        layer="bank_earnings_report_etl",
        name="transcript_3_analystfocus_ranking",
        compose_with_globals=False,
        execution_id=execution_id,
    )

    # Format prompts with dynamic content
    system_prompt = prompt_data["system_prompt"].format(
        num_featured=num_featured,
    )
    user_prompt = prompt_data["user_prompt"].format(
        bank_name=bank_name,
        quarter=quarter,
        fiscal_year=fiscal_year,
        num_featured=num_featured,
        entries_text=entries_text,
    )

    # Build tool definition with dynamic constraints
    tool_def = prompt_data["tool_definition"]
    # Update the array constraints based on num_featured and entries length
    tool_def["function"]["parameters"]["properties"]["featured_entries"]["items"]["maximum"] = len(
        entries
    )
    tool_def["function"]["parameters"]["properties"]["featured_entries"]["minItems"] = num_featured
    tool_def["function"]["parameters"]["properties"]["featured_entries"]["maxItems"] = num_featured
    tool_def["function"]["description"] = f"Select the top {num_featured} Q&A entries to feature"

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("transcript_3_analystfocus_ranking")

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

                featured_nums = function_args.get("featured_entries", [])
                reasoning = function_args.get("reasoning", "")

                featured_indices = []
                for num in featured_nums:
                    idx = num - 1
                    if 0 <= idx < len(entries):
                        featured_indices.append(idx)

                logger.info(
                    "etl.bank_earnings_report.qa_ranking_complete",
                    execution_id=execution_id,
                    featured_indices=featured_indices,
                    reasoning=reasoning,
                )

                if len(featured_indices) >= num_featured:
                    return featured_indices[:num_featured]

        logger.warning(
            "etl.bank_earnings_report.qa_ranking_fallback",
            execution_id=execution_id,
        )
        return list(range(min(num_featured, len(entries))))

    except Exception as e:
        logger.error(
            "etl.bank_earnings_report.qa_ranking_error",
            execution_id=execution_id,
            error=str(e),
        )
        return list(range(min(num_featured, len(entries))))


async def extract_analyst_focus(
    bank_info: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    max_entries: int = 12,
    num_featured: int = 4,
) -> Dict[str, Any]:
    """
    Extract analyst focus entries from earnings call Q&A section.

    Process:
    1. Retrieve all Q&A chunks from transcript
    2. Group by qa_group_id
    3. For each group, extract theme/question/answer via LLM
    4. Rank entries and select top N for featuring
    5. Return structured JSON with featured + all entries

    Args:
        bank_info: Bank information dict with bank_id, bank_name, bank_symbol
        fiscal_year: Fiscal year
        quarter: Quarter
        context: Execution context
        max_entries: Maximum number of Q&A entries to extract (default 12)
        num_featured: Number of entries to feature prominently (default 4)

    Returns:
        Dict with:
            - source: "Transcript"
            - featured: Top N entries selected by LLM
            - entries: All extracted entries
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.bank_earnings_report.analyst_focus_extraction_start",
        execution_id=execution_id,
        bank=bank_info["bank_symbol"],
        period=f"{quarter} {fiscal_year}",
    )

    chunks = await retrieve_qa_chunks(
        bank_id=bank_info["bank_id"],
        fiscal_year=fiscal_year,
        quarter=quarter,
        context=context,
    )

    if not chunks:
        logger.warning(
            "etl.bank_earnings_report.no_qa_chunks",
            execution_id=execution_id,
            bank=bank_info["bank_symbol"],
        )
        return {"source": "Transcript", "featured": [], "entries": []}

    qa_groups = group_chunks_by_qa_id(chunks)
    sorted_qa_ids = sorted(qa_groups.keys())

    logger.info(
        "etl.bank_earnings_report.qa_groups_found",
        execution_id=execution_id,
        total_groups=len(sorted_qa_ids),
        total_chunks=len(chunks),
    )

    entries = []
    for qa_id in sorted_qa_ids:
        qa_chunks = qa_groups[qa_id]

        entry = await extract_qa_entry(
            qa_group_id=qa_id,
            chunks=qa_chunks,
            bank_name=bank_info["bank_name"],
            quarter=quarter,
            fiscal_year=fiscal_year,
            context=context,
        )

        if entry:
            entries.append(entry)

        if len(entries) >= max_entries:
            logger.info(
                "etl.bank_earnings_report.max_entries_reached",
                execution_id=execution_id,
                max_entries=max_entries,
            )
            break

    if not entries:
        logger.warning(
            "etl.bank_earnings_report.no_qa_entries_extracted",
            execution_id=execution_id,
        )
        return {"source": "Transcript", "featured": [], "entries": []}

    featured_indices = await rank_qa_entries(
        entries=entries,
        bank_name=bank_info["bank_name"],
        quarter=quarter,
        fiscal_year=fiscal_year,
        context=context,
        num_featured=num_featured,
    )

    formatted_entries = [
        {
            "theme": e["theme"],
            "question": e["question"],
            "answer": e["answer"],
        }
        for e in entries
    ]

    featured_entries = [
        formatted_entries[i] for i in featured_indices if i < len(formatted_entries)
    ]

    featured_set = set(featured_indices)
    remaining_entries = [
        formatted_entries[i] for i in range(len(formatted_entries)) if i not in featured_set
    ]

    logger.info(
        "etl.bank_earnings_report.analyst_focus_complete",
        execution_id=execution_id,
        total_entries=len(formatted_entries),
        featured_count=len(featured_entries),
        remaining_count=len(remaining_entries),
        featured_themes=[e["theme"] for e in featured_entries],
    )

    return {
        "source": "Transcript",
        "featured": featured_entries,
        "remaining": remaining_entries,
        "entries": formatted_entries,  # Keep for backwards compatibility
    }
