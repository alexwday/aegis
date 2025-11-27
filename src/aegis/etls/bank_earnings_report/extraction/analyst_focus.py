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


# Common financial themes for analyst Q&A
COMMON_THEMES = [
    "NIM Outlook",
    "Credit Quality",
    "Capital Allocation",
    "Expense Management",
    "Loan Growth",
    "Deposit Trends",
    "Fee Income",
    "Trading Revenue",
    "Mortgage Portfolio",
    "CRE Exposure",
    "U.S. Strategy",
    "Digital Banking",
    "M&A Strategy",
    "Regulatory Capital",
    "Dividend Policy",
    "Share Buybacks",
    "Interest Rate Sensitivity",
    "Provision Outlook",
    "Revenue Guidance",
    "Efficiency Ratio",
]


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

    # Format the Q&A content for LLM
    qa_content = format_qa_group_for_llm(qa_group_id, chunks, bank_name, quarter, fiscal_year)
    qa_summary = get_qa_group_summary(chunks)

    if not qa_content.strip():
        logger.warning(
            "etl.bank_earnings_report.empty_qa_content",
            execution_id=execution_id,
            qa_group_id=qa_group_id,
        )
        return None

    # Build the system prompt
    system_prompt = """You are a senior financial analyst extracting key information from bank \
earnings call Q&A transcripts.

## YOUR TASK

Analyze the Q&A exchange and extract:
1. **Theme**: A short label (2-4 words) categorizing the topic (e.g., "NIM Outlook", \
"Credit Quality", "Capital Allocation")
2. **Question**: A concise summary of the analyst's question (1-2 sentences)
3. **Answer**: A summary of management's response with key details and figures (2-4 sentences)

## EXTRACTION GUIDELINES

**For Theme:**
- Use standard financial industry themes
- Be specific but concise (e.g., "CRE Exposure" not "Commercial Real Estate")
- Common themes include: NIM Outlook, Credit Quality, Capital Allocation, Expense Management, \
Loan Growth, Deposit Trends, Fee Income, Trading Revenue, U.S. Strategy, Digital Banking, \
M&A Strategy, Regulatory Capital, Dividend Policy

**For Question:**
- Capture the core inquiry from the analyst
- Include any specific context they mention
- Keep to 1-2 sentences

**For Answer:**
- Summarize management's key points
- Preserve specific numbers, percentages, and guidance
- Include forward-looking statements if present
- Identify the speaker role if clear (CEO, CFO, CRO, etc.)
- Keep to 2-4 sentences

## IMPORTANT

- If the exchange is not financially meaningful (pleasantries, logistics), return should_skip=true
- Preserve exact figures and percentages from the transcript
- Focus on information investors would find valuable"""

    # Build the user prompt
    user_prompt = f"""Analyze this Q&A exchange from {bank_name}'s {quarter} {fiscal_year} \
earnings call and extract the key information.

{qa_content}

Extract the theme, question summary, and answer summary. If this exchange has no meaningful \
financial content, indicate it should be skipped."""

    # Define the tool for structured output
    tool_definition = {
        "type": "function",
        "function": {
            "name": "extract_qa_summary",
            "description": "Extract theme, question, and answer from an earnings call Q&A exchange",
            "parameters": {
                "type": "object",
                "properties": {
                    "should_skip": {
                        "type": "boolean",
                        "description": (
                            "True if this exchange should be skipped (no meaningful financial "
                            "content, just pleasantries, or logistics). False if it contains "
                            "valuable analyst insights."
                        ),
                    },
                    "theme": {
                        "type": "string",
                        "description": (
                            "Short theme label (2-4 words) categorizing the topic. "
                            "Examples: 'NIM Outlook', 'Credit Quality', 'Capital Allocation', "
                            "'CRE Exposure', 'U.S. Strategy'"
                        ),
                    },
                    "question": {
                        "type": "string",
                        "description": (
                            "Concise summary of the analyst's question (1-2 sentences). "
                            "Capture the core inquiry and any specific context mentioned."
                        ),
                    },
                    "answer": {
                        "type": "string",
                        "description": (
                            "Summary of management's response (2-4 sentences). "
                            "Preserve specific numbers, percentages, and guidance. "
                            "Include speaker role if clear (e.g., 'CFO noted that...')."
                        ),
                    },
                },
                "required": ["should_skip", "theme", "question", "answer"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        # Use medium model for Q&A extraction (one call per Q&A group)
        model = etl_config.get_model("analyst_focus_extraction")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
            context=context,
            llm_params={
                "model": model,
                "temperature": 0.2,  # Low temperature for factual extraction
                "max_tokens": 1000,
            },
        )

        # Parse the tool call response
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
        # If we have fewer entries than requested, return all indices
        return list(range(len(entries)))

    # Format entries for LLM review
    entries_text = ""
    for i, entry in enumerate(entries):
        entries_text += f"""
### Entry {i + 1}: {entry['theme']}
**Question:** {entry['question']}
**Answer:** {entry['answer']}

---
"""

    system_prompt = f"""You are a senior financial analyst selecting the most important Q&A \
exchanges from an earnings call for a quarterly report.

## YOUR TASK

Review all Q&A entries and select the {num_featured} MOST important ones to feature prominently.

## SELECTION CRITERIA

Prioritize Q&A exchanges that:
1. **Forward Guidance**: Management's outlook on key metrics (NIM, credit, growth)
2. **Risk Disclosure**: Discussion of risks, challenges, or problem areas
3. **Strategic Initiatives**: Major business decisions, M&A, market expansion
4. **Capital Allocation**: Dividend, buyback, or capital deployment plans
5. **Material Changes**: Significant shifts from prior quarters or guidance

## WHAT TO DEPRIORITIZE

- Generic commentary without specific details
- Routine operational updates
- Repetitive themes (if similar topics, pick the most substantive)
- Backward-looking discussion without forward implications

## OUTPUT

Return the entry numbers (1-indexed as shown) that should be featured."""

    user_prompt = f"""Review these Q&A exchanges from {bank_name}'s {quarter} {fiscal_year} \
earnings call and select the {num_featured} most important to feature.

{entries_text}

Select {num_featured} entry numbers that provide the most valuable insights for investors."""

    # Define the tool for structured output
    tool_definition = {
        "type": "function",
        "function": {
            "name": "select_featured_qa",
            "description": f"Select the top {num_featured} Q&A entries to feature",
            "parameters": {
                "type": "object",
                "properties": {
                    "featured_entries": {
                        "type": "array",
                        "items": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": len(entries),
                        },
                        "description": (
                            f"List of exactly {num_featured} entry numbers (1-indexed) to feature. "
                            "Select based on importance to investors."
                        ),
                        "minItems": num_featured,
                        "maxItems": num_featured,
                    },
                    "reasoning": {
                        "type": "string",
                        "description": (
                            "Brief explanation of why these entries were selected "
                            "as most important."
                        ),
                    },
                },
                "required": ["featured_entries", "reasoning"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("analyst_focus_extraction")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
            context=context,
            llm_params={
                "model": model,
                "temperature": 0.2,
                "max_tokens": 1000,
            },
        )

        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])

                featured_nums = function_args.get("featured_entries", [])
                reasoning = function_args.get("reasoning", "")

                # Convert 1-indexed to 0-indexed and validate
                featured_indices = []
                for num in featured_nums:
                    idx = num - 1  # Convert to 0-indexed
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

        # Fallback: return first N entries
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

    # Step 1: Retrieve Q&A chunks
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

    # Step 2: Group by qa_group_id
    qa_groups = group_chunks_by_qa_id(chunks)
    sorted_qa_ids = sorted(qa_groups.keys())

    logger.info(
        "etl.bank_earnings_report.qa_groups_found",
        execution_id=execution_id,
        total_groups=len(sorted_qa_ids),
        total_chunks=len(chunks),
    )

    # Step 3: Extract each Q&A group (process sequentially to maintain order)
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

        # Stop if we have enough entries
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

    # Step 4: Rank entries and select top N for featuring
    featured_indices = await rank_qa_entries(
        entries=entries,
        bank_name=bank_info["bank_name"],
        quarter=quarter,
        fiscal_year=fiscal_year,
        context=context,
        num_featured=num_featured,
    )

    # Step 5: Format final output
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

    logger.info(
        "etl.bank_earnings_report.analyst_focus_complete",
        execution_id=execution_id,
        total_entries=len(formatted_entries),
        featured_count=len(featured_entries),
        featured_themes=[e["theme"] for e in featured_entries],
    )

    return {
        "source": "Transcript",
        "featured": featured_entries,
        "entries": formatted_entries,
    }
