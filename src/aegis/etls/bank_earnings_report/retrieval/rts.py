"""
RTS (Regulatory/Risk/Technical Supplement) retrieval for Bank Earnings Report ETL.

This module provides semantic search against the rts_embedding table to retrieve
relevant chunks for generating segment performance drivers statements.

The rts_embedding table contains chunks from bank regulatory filings with:
- Embeddings created from: Bank + Quarter + Year + Summary + Section + Table Terms + Propositions
- Propositions: GPT-extracted factual financial statements
- Source sections: Hierarchical section paths from markdown headings

Pipeline:
1. Initial retrieval: Top-k chunks via semantic similarity search
2. LLM reranking: Binary filter for segment relevance using source_section + summary + propositions
3. Page expansion: Pull chunks from relevant pages ±1, with gap filling (≤5 page gaps)
4. Driver generation: LLM synthesizes qualitative drivers (no metrics/deltas)
"""

import json
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import bindparam, text

from aegis.connections.llm_connector import complete_with_tools, embed
from aegis.connections.postgres_connector import get_connection
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.utils.logging import get_logger


# =============================================================================
# Query Formatting for Embeddings
# =============================================================================


def format_query_for_embedding(
    query: str,
    bank: str,
    year: int,
    quarter: str,
) -> str:
    """
    Format a search query to match the indexed embedding format.

    The rts_embedding table embeddings were created from labeled fields:
    Bank + Quarter + Year + Summary + Section + Table Terms + Propositions

    To maximize similarity scores, we construct query strings in the same format.

    Args:
        query: The search topic/terms (e.g., "revenue growth loan performance NIM")
        bank: Bank symbol (e.g., "RY-CA")
        year: Fiscal year (e.g., 2025)
        quarter: Quarter (e.g., "Q3")

    Returns:
        Formatted query string matching the embedding index format
    """
    formatted = (
        f"Bank: {bank} "
        f"Quarter: {quarter} "
        f"Year: {year} "
        f"Summary: {query} "
        f"Propositions: {query}"
    )
    return formatted


# =============================================================================
# Segment Query Terms
# =============================================================================

# Segment-specific search terms for RTS retrieval
SEGMENT_QUERY_TERMS = {
    "Canadian Banking": (
        "Canadian personal commercial banking retail loan growth mortgage deposit "
        "net interest margin credit quality provisions efficiency expenses"
    ),
    "Canadian Wealth & Insurance": (
        "wealth management insurance AUM assets under management fee income "
        "net flows premium revenue advisory services private banking"
    ),
    "Capital Markets": (
        "capital markets trading revenue investment banking advisory fees underwriting "
        "equity fixed income global markets corporate banking ROE"
    ),
    "U.S. & International Banking": (
        "US United States international banking retail commercial loan growth deposit "
        "credit provisions efficiency cross-border City National"
    ),
    "Corporate Support": (
        "corporate treasury technology investment funding costs operational efficiency "
        "enterprise functions support services"
    ),
}


def get_segment_query_terms(segment_name: str) -> str:
    """Get search terms for a specific segment."""
    return SEGMENT_QUERY_TERMS.get(
        segment_name,
        f"{segment_name} performance revenue growth expenses efficiency",
    )


# =============================================================================
# Step 1: Initial Chunk Retrieval via Semantic Search
# =============================================================================


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
async def retrieve_initial_chunks(
    bank: str,
    year: int,
    quarter: str,
    segment_name: str,
    context: Dict[str, Any],
    top_k: int = 20,
) -> List[Dict[str, Any]]:
    """
    Retrieve top-k chunks from RTS embeddings for initial candidate set.

    Args:
        bank: Bank symbol with suffix (e.g., "RY-CA")
        year: Fiscal year
        quarter: Quarter (e.g., "Q3")
        segment_name: Business segment name (e.g., "Capital Markets")
        context: Execution context
        top_k: Number of chunks to retrieve (default 20)

    Returns:
        List of chunk dicts with id, chunk_id, page_no, summary_title,
        source_section, raw_text, propositions, similarity
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    query_terms = get_segment_query_terms(segment_name)
    formatted_query = format_query_for_embedding(query_terms, bank, year, quarter)

    logger.info(
        "etl.rts.initial_retrieve_start",
        execution_id=execution_id,
        segment=segment_name,
        bank=bank,
        period=f"{quarter} {year}",
        top_k=top_k,
    )

    try:
        embedding_response = await embed(
            input_text=formatted_query,
            context=context,
            embedding_params={"model": "text-embedding-3-large", "dimensions": 3072},
        )

        if not embedding_response.get("data"):
            logger.error("etl.rts.embedding_failed", execution_id=execution_id)
            return []

        query_embedding = embedding_response["data"][0]["embedding"]
        embedding_str = "[" + ",".join(str(x) for x in query_embedding) + "]"

        async with get_connection() as conn:
            sql = text(
                """
                SELECT
                    id, chunk_id, page_no, summary_title, source_section,
                    raw_text, propositions,
                    embedding <=> cast(:query_embedding as halfvec) AS distance
                FROM rts_embedding
                WHERE bank = :bank AND year = :year AND quarter = :quarter
                ORDER BY embedding <=> cast(:query_embedding as halfvec)
                LIMIT :top_k
                """
            ).bindparams(
                bindparam("query_embedding", value=embedding_str),
                bindparam("bank", value=bank),
                bindparam("year", value=year),
                bindparam("quarter", value=quarter),
                bindparam("top_k", value=top_k),
            )

            result = await conn.execute(sql)
            chunks = []

            for row in result.fetchall():
                propositions = row[6]
                if isinstance(propositions, str):
                    try:
                        propositions = json.loads(propositions)
                    except json.JSONDecodeError:
                        propositions = []

                chunks.append(
                    {
                        "id": row[0],
                        "chunk_id": row[1],
                        "page_no": row[2],
                        "summary_title": row[3],
                        "source_section": row[4],
                        "raw_text": row[5],
                        "propositions": propositions or [],
                        "similarity": 1 - row[7] if row[7] is not None else 0,
                    }
                )

            logger.info(
                "etl.rts.initial_retrieve_complete",
                execution_id=execution_id,
                segment=segment_name,
                chunks_retrieved=len(chunks),
            )
            return chunks

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("etl.rts.initial_retrieve_error", error=str(e))
        return []


# =============================================================================
# Step 2: LLM Reranking - Binary Relevance Filter
# =============================================================================


def format_chunk_card_for_rerank(chunk: Dict[str, Any], index: int) -> str:
    """
    Format a chunk as a card for LLM reranking.

    Uses source_section + summary_title + propositions (not raw_text).

    Args:
        chunk: Chunk dict
        index: Card index number

    Returns:
        Formatted card string
    """
    section = chunk.get("source_section", "Unknown")
    summary = chunk.get("summary_title", "No summary")
    propositions = chunk.get("propositions", [])

    lines = [
        f"### Card {index}",
        f"**Section Path:** {section}",
        f"**Summary:** {summary}",
    ]

    if propositions and isinstance(propositions, list):
        lines.append("**Key Facts:**")
        for prop in propositions[:7]:  # Limit propositions shown
            lines.append(f"- {prop}")

    return "\n".join(lines)


async def rerank_chunks_for_segment(
    chunks: List[Dict[str, Any]],
    segment_name: str,
    context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Use LLM to filter chunks by binary relevance to the specific segment.

    Args:
        chunks: List of candidate chunks from initial retrieval
        segment_name: Target segment (e.g., "Capital Markets")
        context: Execution context

    Returns:
        Filtered list of chunks that are directly relevant to the segment
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if not chunks:
        return []

    # Format chunks as cards
    cards = []
    for i, chunk in enumerate(chunks, 1):
        cards.append(format_chunk_card_for_rerank(chunk, i))

    cards_text = "\n\n".join(cards)

    system_prompt = f"""You are a financial document analyst. Your task is to determine which \
document excerpts are DIRECTLY relevant to the {segment_name} business segment.

## SEGMENT: {segment_name}

## CRITERIA FOR RELEVANCE

A chunk is RELEVANT if it:
- Explicitly discusses the {segment_name} segment by name
- Contains information about business activities specific to {segment_name}
- Discusses metrics, performance, or drivers for this segment
- Is nested under a section heading that indicates {segment_name} content

A chunk is NOT RELEVANT if it:
- Discusses a different business segment (e.g., Canadian Banking when looking for Capital Markets)
- Is about enterprise-wide or consolidated results without segment breakdown
- Is general corporate information not specific to this segment
- Only mentions the segment in passing without substantive content

## IMPORTANT

Be STRICT. Only mark chunks as relevant if they are DIRECTLY about {segment_name}.
If a chunk is about multiple segments or enterprise-wide, mark it NOT relevant.
We want segment-specific content only."""

    user_prompt = f"""Review each card below and determine if it is DIRECTLY relevant to the \
{segment_name} segment.

{cards_text}

For each card (1 to {len(chunks)}), provide a binary decision: relevant or not_relevant."""

    # Tool for structured output
    tool_definition = {
        "type": "function",
        "function": {
            "name": "classify_chunk_relevance",
            "description": f"Classify each chunk's relevance to {segment_name}",
            "parameters": {
                "type": "object",
                "properties": {
                    "decisions": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "card_number": {"type": "integer"},
                                "relevant": {"type": "boolean"},
                                "reason": {"type": "string"},
                            },
                            "required": ["card_number", "relevant"],
                        },
                        "description": "Binary relevance decision for each card",
                    },
                },
                "required": ["decisions"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("segment_drivers_extraction")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
            context=context,
            llm_params={"model": model, "temperature": 0.1, "max_tokens": 2000},
        )

        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])
                decisions = function_args.get("decisions", [])

                # Build set of relevant card numbers
                relevant_indices = set()
                for decision in decisions:
                    if decision.get("relevant"):
                        relevant_indices.add(decision.get("card_number"))

                # Filter chunks
                filtered = [chunk for i, chunk in enumerate(chunks, 1) if i in relevant_indices]

                logger.info(
                    "etl.rts.rerank_complete",
                    execution_id=execution_id,
                    segment=segment_name,
                    input_chunks=len(chunks),
                    relevant_chunks=len(filtered),
                )
                return filtered

        logger.warning("etl.rts.rerank_no_tool_call", execution_id=execution_id)
        return chunks  # Return original if reranking fails

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("etl.rts.rerank_error", error=str(e))
        return chunks  # Return original on error


# =============================================================================
# Step 3: Page-Based Context Expansion with Gap Filling
# =============================================================================


def get_expanded_page_set(page_numbers: Set[int], max_gap: int = 5) -> Set[int]:
    """
    Expand page set with ±1 context and fill gaps ≤ max_gap.

    Args:
        page_numbers: Set of page numbers from relevant chunks
        max_gap: Maximum gap size to fill (default 5)

    Returns:
        Expanded set of page numbers
    """
    if not page_numbers:
        return set()

    # Step 1: Add ±1 page context
    expanded = set()
    for page in page_numbers:
        expanded.add(page - 1)
        expanded.add(page)
        expanded.add(page + 1)

    # Remove invalid page numbers (< 1)
    expanded = {p for p in expanded if p >= 1}

    # Step 2: Fill gaps ≤ max_gap
    if len(expanded) < 2:
        return expanded

    sorted_pages = sorted(expanded)
    filled = set(sorted_pages)

    for i in range(len(sorted_pages) - 1):
        current = sorted_pages[i]
        next_page = sorted_pages[i + 1]
        gap = next_page - current - 1

        if 0 < gap <= max_gap:
            # Fill the gap
            for page in range(current + 1, next_page):
                filled.add(page)

    return filled


async def expand_chunks_by_page(
    relevant_chunks: List[Dict[str, Any]],
    bank: str,
    year: int,
    quarter: str,
    context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Expand chunk set by pulling all chunks from relevant pages with context.

    Args:
        relevant_chunks: Chunks that passed reranking
        bank: Bank symbol
        year: Fiscal year
        quarter: Quarter
        context: Execution context

    Returns:
        Expanded and sorted list of chunks (by chunk_id)
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if not relevant_chunks:
        return []

    # Get page numbers from relevant chunks
    page_numbers = {c["page_no"] for c in relevant_chunks if c.get("page_no")}

    if not page_numbers:
        return relevant_chunks

    # Expand page set
    expanded_pages = get_expanded_page_set(page_numbers, max_gap=5)

    logger.info(
        "etl.rts.page_expansion",
        execution_id=execution_id,
        original_pages=len(page_numbers),
        expanded_pages=len(expanded_pages),
        page_range=f"{min(expanded_pages)}-{max(expanded_pages)}" if expanded_pages else "none",
    )

    try:
        async with get_connection() as conn:
            # Build page list for SQL IN clause
            page_list = list(expanded_pages)

            sql = text(
                """
                SELECT
                    id, chunk_id, page_no, summary_title, source_section,
                    raw_text, propositions
                FROM rts_embedding
                WHERE bank = :bank AND year = :year AND quarter = :quarter
                  AND page_no = ANY(:pages)
                ORDER BY chunk_id
                """
            ).bindparams(
                bindparam("bank", value=bank),
                bindparam("year", value=year),
                bindparam("quarter", value=quarter),
                bindparam("pages", value=page_list),
            )

            result = await conn.execute(sql)
            chunks = []

            for row in result.fetchall():
                propositions = row[6]
                if isinstance(propositions, str):
                    try:
                        propositions = json.loads(propositions)
                    except json.JSONDecodeError:
                        propositions = []

                chunks.append(
                    {
                        "id": row[0],
                        "chunk_id": row[1],
                        "page_no": row[2],
                        "summary_title": row[3],
                        "source_section": row[4],
                        "raw_text": row[5],
                        "propositions": propositions or [],
                    }
                )

            logger.info(
                "etl.rts.page_expansion_complete",
                execution_id=execution_id,
                chunks_retrieved=len(chunks),
            )
            return chunks

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("etl.rts.page_expansion_error", error=str(e))
        return relevant_chunks


# =============================================================================
# Step 4: Driver Statement Generation (No Metrics/Deltas)
# =============================================================================


def format_chunks_for_drivers(chunks: List[Dict[str, Any]]) -> str:
    """
    Format expanded chunks for driver generation LLM.

    Args:
        chunks: List of chunks sorted by chunk_id

    Returns:
        Formatted context string
    """
    if not chunks:
        return "No relevant content found."

    lines = ["# Regulatory Filing Excerpts (sorted by document order)", ""]

    for chunk in chunks:
        page = chunk.get("page_no", "?")
        section = chunk.get("source_section", "Unknown Section")
        raw_text = chunk.get("raw_text", "")

        lines.append(f"## Page {page} | {section}")
        lines.append("")

        if raw_text:
            # Include full text (up to limit)
            text_content = raw_text[:2000] if len(raw_text) > 2000 else raw_text
            lines.append(text_content)

        lines.append("")
        lines.append("---")
        lines.append("")

    return "\n".join(lines)


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
async def generate_segment_drivers(
    chunks: List[Dict[str, Any]],
    segment_name: str,
    context: Dict[str, Any],
) -> Optional[str]:
    """
    Generate a qualitative drivers statement from expanded chunks.

    IMPORTANT: Output should NOT include specific metrics or delta values.
    The metrics are already shown separately from the supplementary pack.
    This statement should focus on qualitative drivers only.

    Args:
        chunks: Expanded chunks sorted by chunk_id
        segment_name: Business segment name
        context: Execution context

    Returns:
        Drivers statement string or None if generation fails
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if not chunks:
        logger.warning("etl.rts.no_chunks_for_drivers", execution_id=execution_id)
        return None

    chunks_context = format_chunks_for_drivers(chunks)

    # Updated prompting - NO METRICS OR DELTAS
    system_prompt = f"""You are a senior financial analyst writing a bank quarterly earnings report.

Your task is to write a concise QUALITATIVE drivers statement for the {segment_name} segment.

## CRITICAL REQUIREMENTS

1. **NO METRICS OR NUMBERS**: Do NOT include specific dollar amounts, percentages, basis points, \
or any numerical values. The metrics are shown separately in the report.
2. **QUALITATIVE ONLY**: Focus on the business drivers, trends, and factors - not the numbers.
3. **Length**: 2-3 sentences maximum
4. **Tone**: Professional, factual, analyst-style

## WHAT TO INCLUDE

- Business drivers (e.g., "higher trading activity", "increased client demand")
- Market conditions (e.g., "favorable rate environment", "challenging credit conditions")
- Strategic factors (e.g., "expansion into new markets", "cost discipline initiatives")
- Operational factors (e.g., "improved efficiency", "technology investments")

## WHAT TO EXCLUDE

- Specific dollar amounts (e.g., "$2.1B", "CAD 500 million")
- Percentages (e.g., "8% growth", "up 12%")
- Basis points (e.g., "expanded 15 bps")
- Quarter-over-quarter or year-over-year comparisons with numbers
- Any numerical metrics

## EXAMPLE - CORRECT

"Performance driven by higher trading volumes and strong advisory activity amid favorable market \
conditions. Loan growth supported by increased corporate demand, while credit quality remained \
stable. Continued focus on expense management and operational efficiency."

## EXAMPLE - INCORRECT (has numbers)

"Revenue growth of 8% YoY driven by higher trading volumes. Net interest income benefited from \
loan growth of $2.1B, partially offset by margin compression of 5 bps."

## IMPORTANT

Do NOT include the segment name in the statement - it's already shown in the header."""

    user_prompt = f"""Based on the following regulatory filing excerpts for the {segment_name} \
segment, write a concise QUALITATIVE drivers statement.

Remember: NO specific metrics, percentages, or dollar amounts. Focus only on the business drivers.

{chunks_context}

Write a 2-3 sentence qualitative drivers statement."""

    tool_definition = {
        "type": "function",
        "function": {
            "name": "segment_drivers_statement",
            "description": f"Generate a qualitative drivers statement for {segment_name}",
            "parameters": {
                "type": "object",
                "properties": {
                    "drivers_statement": {
                        "type": "string",
                        "description": (
                            "A 2-3 sentence QUALITATIVE statement about performance drivers. "
                            "Must NOT contain any numbers, percentages, or dollar amounts."
                        ),
                    },
                    "confidence": {
                        "type": "string",
                        "enum": ["high", "medium", "low"],
                        "description": "Confidence based on source data relevance",
                    },
                },
                "required": ["drivers_statement", "confidence"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("segment_drivers_extraction")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
            context=context,
            llm_params={"model": model, "temperature": 0.2, "max_tokens": 500},
        )

        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])

                drivers_statement = function_args.get("drivers_statement", "")
                confidence = function_args.get("confidence", "unknown")

                logger.info(
                    "etl.rts.drivers_generated",
                    execution_id=execution_id,
                    segment=segment_name,
                    confidence=confidence,
                    statement_length=len(drivers_statement),
                )
                return drivers_statement

        logger.warning("etl.rts.drivers_no_tool_call", execution_id=execution_id)
        return None

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("etl.rts.drivers_error", error=str(e))
        return None


# =============================================================================
# Alternative Approach: Full RTS Loading (No Similarity Search)
# =============================================================================


async def retrieve_all_rts_chunks(
    bank: str,
    year: int,
    quarter: str,
    context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Retrieve ALL chunks from RTS for a given bank/quarter.

    This loads the entire RTS document without any filtering, allowing
    the LLM to find relevant sections directly.

    Args:
        bank: Bank symbol with suffix (e.g., "RY-CA")
        year: Fiscal year
        quarter: Quarter (e.g., "Q3")
        context: Execution context

    Returns:
        List of all chunk dicts ordered by chunk_id
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.rts.load_all_chunks_start",
        execution_id=execution_id,
        bank=bank,
        period=f"{quarter} {year}",
    )

    try:
        async with get_connection() as conn:
            sql = text(
                """
                SELECT
                    id, chunk_id, page_no, summary_title, source_section,
                    raw_text, propositions
                FROM rts_embedding
                WHERE bank = :bank AND year = :year AND quarter = :quarter
                ORDER BY chunk_id
                """
            ).bindparams(
                bindparam("bank", value=bank),
                bindparam("year", value=year),
                bindparam("quarter", value=quarter),
            )

            result = await conn.execute(sql)
            chunks = []

            for row in result.fetchall():
                propositions = row[6]
                if isinstance(propositions, str):
                    try:
                        propositions = json.loads(propositions)
                    except json.JSONDecodeError:
                        propositions = []

                chunks.append(
                    {
                        "id": row[0],
                        "chunk_id": row[1],
                        "page_no": row[2],
                        "summary_title": row[3],
                        "source_section": row[4],
                        "raw_text": row[5],
                        "propositions": propositions or [],
                    }
                )

            logger.info(
                "etl.rts.load_all_chunks_complete",
                execution_id=execution_id,
                bank=bank,
                period=f"{quarter} {year}",
                total_chunks=len(chunks),
            )
            return chunks

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("etl.rts.load_all_chunks_error", error=str(e))
        return []


def format_full_rts_for_llm(chunks: List[Dict[str, Any]]) -> str:
    """
    Format all RTS chunks into a single document for LLM processing.

    Args:
        chunks: List of all chunks sorted by chunk_id

    Returns:
        Formatted full RTS content string
    """
    if not chunks:
        return "No RTS content available."

    lines = ["# Full Regulatory Filing Document", ""]

    current_section = None
    for chunk in chunks:
        section = chunk.get("source_section", "")
        page = chunk.get("page_no", "?")
        raw_text = chunk.get("raw_text", "")

        # Add section header if changed
        if section and section != current_section:
            lines.append(f"\n## {section}")
            lines.append(f"[Page {page}]")
            lines.append("")
            current_section = section

        if raw_text:
            lines.append(raw_text)
            lines.append("")

    return "\n".join(lines)


async def generate_segment_drivers_from_full_rts(
    chunks: List[Dict[str, Any]],
    segment_name: str,
    context: Dict[str, Any],
) -> Optional[str]:
    """
    Generate a qualitative drivers statement by letting the LLM find the relevant
    section in the full RTS document.

    Args:
        chunks: All RTS chunks for the bank/quarter
        segment_name: Business segment name
        context: Execution context

    Returns:
        Drivers statement string or None if generation fails
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if not chunks:
        logger.warning("etl.rts.no_chunks_for_full_drivers", execution_id=execution_id)
        return None

    full_rts = format_full_rts_for_llm(chunks)

    system_prompt = f"""You are a senior financial analyst writing a bank quarterly earnings report.

Your task is to:
1. FIND the section(s) in the regulatory filing that discuss the {segment_name} segment
2. EXTRACT the key performance drivers mentioned for this segment
3. WRITE a concise qualitative drivers statement (2-3 sentences)

## CRITICAL REQUIREMENTS

1. **NO METRICS OR NUMBERS**: Do NOT include specific dollar amounts, percentages, basis points, \
or any numerical values. The metrics are shown separately in the report.
2. **QUALITATIVE ONLY**: Focus on the business drivers, trends, and factors - not the numbers.
3. **Length**: 2-3 sentences maximum
4. **Tone**: Professional, factual, analyst-style

## SEGMENT TO FIND: {segment_name}

Look for sections with headings like:
- "{segment_name}"
- "Business Segment Results"
- "Segment Performance"
- "Operating Results by Segment"

The segment discussion typically includes explanations of what drove performance changes.

## WHAT TO INCLUDE

- Business drivers (e.g., "higher trading activity", "increased client demand")
- Market conditions (e.g., "favorable rate environment", "challenging credit conditions")
- Strategic factors (e.g., "expansion into new markets", "cost discipline initiatives")
- Operational factors (e.g., "improved efficiency", "technology investments")

## WHAT TO EXCLUDE

- Specific dollar amounts (e.g., "$2.1B", "CAD 500 million")
- Percentages (e.g., "8% growth", "up 12%")
- Basis points (e.g., "expanded 15 bps")
- Quarter-over-quarter or year-over-year comparisons with numbers

## IF SEGMENT NOT FOUND

If you cannot find content specifically about the {segment_name} segment, return an empty string.
Do NOT make up information or use content from other segments.

## IMPORTANT

Do NOT include the segment name in the statement - it's already shown in the header."""

    user_prompt = f"""Below is the complete regulatory filing document. Find the section \
discussing the {segment_name} segment and write a 2-3 sentence QUALITATIVE drivers statement.

Remember: NO specific metrics, percentages, or dollar amounts. Focus only on the business drivers.

{full_rts}

Write the qualitative drivers statement for {segment_name}, or return empty if not found."""

    tool_definition = {
        "type": "function",
        "function": {
            "name": "segment_drivers_statement",
            "description": f"Generate a qualitative drivers statement for {segment_name}",
            "parameters": {
                "type": "object",
                "properties": {
                    "found_segment": {
                        "type": "boolean",
                        "description": f"Whether content for {segment_name} was found",
                    },
                    "drivers_statement": {
                        "type": "string",
                        "description": (
                            "A 2-3 sentence QUALITATIVE statement about performance drivers. "
                            "Must NOT contain any numbers, percentages, or dollar amounts. "
                            "Empty string if segment not found."
                        ),
                    },
                    "source_sections": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Section headings where segment content was found",
                    },
                },
                "required": ["found_segment", "drivers_statement"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("segment_drivers_extraction")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
            context=context,
            llm_params={"model": model, "temperature": 0.2, "max_tokens": 1000},
        )

        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])

                found_segment = function_args.get("found_segment", False)
                drivers_statement = function_args.get("drivers_statement", "")
                source_sections = function_args.get("source_sections", [])

                logger.info(
                    "etl.rts.full_drivers_generated",
                    execution_id=execution_id,
                    segment=segment_name,
                    found_segment=found_segment,
                    source_sections=source_sections,
                    statement_length=len(drivers_statement) if drivers_statement else 0,
                )

                if found_segment and drivers_statement:
                    return drivers_statement
                return None

        logger.warning("etl.rts.full_drivers_no_tool_call", execution_id=execution_id)
        return None

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("etl.rts.full_drivers_error", error=str(e))
        return None


# pylint: disable=too-many-arguments,too-many-positional-arguments
async def get_segment_drivers_from_full_rts(
    bank: str,
    year: int,
    quarter: str,
    segment_name: str,
    context: Dict[str, Any],
) -> Optional[str]:
    """
    Get a qualitative drivers statement by loading the full RTS and letting
    the LLM find the relevant segment section.

    This approach:
    1. Loads ALL chunks for the bank/quarter (no similarity search)
    2. Passes the full RTS content to the LLM
    3. LLM finds the segment-specific section and extracts drivers

    Args:
        bank: Bank symbol (e.g., "RY-CA")
        year: Fiscal year
        quarter: Quarter (e.g., "Q3")
        segment_name: Business segment (e.g., "Capital Markets")
        context: Execution context

    Returns:
        Qualitative drivers statement or None if unavailable
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.rts.full_pipeline_start",
        execution_id=execution_id,
        segment=segment_name,
        bank=bank,
        period=f"{quarter} {year}",
    )

    # Step 1: Load all chunks
    all_chunks = await retrieve_all_rts_chunks(
        bank=bank,
        year=year,
        quarter=quarter,
        context=context,
    )

    if not all_chunks:
        logger.warning("etl.rts.no_chunks_loaded", execution_id=execution_id)
        return None

    # Step 2: Generate drivers from full RTS
    drivers = await generate_segment_drivers_from_full_rts(
        chunks=all_chunks,
        segment_name=segment_name,
        context=context,
    )

    logger.info(
        "etl.rts.full_pipeline_complete",
        execution_id=execution_id,
        segment=segment_name,
        total_chunks=len(all_chunks),
        drivers_generated=drivers is not None,
    )

    return drivers


# =============================================================================
# Main Entry Point - Full Pipeline (Original Similarity Search Approach)
# =============================================================================


# pylint: disable=too-many-arguments,too-many-positional-arguments
async def get_segment_drivers_from_rts(
    bank: str,
    year: int,
    quarter: str,
    segment_name: str,
    context: Dict[str, Any],
    top_k: int = 20,
) -> Optional[str]:
    """
    Get a qualitative drivers statement for a segment using the full pipeline.

    Pipeline:
    1. Initial retrieval: Top-k chunks via semantic similarity
    2. LLM reranking: Binary filter for segment relevance
    3. Page expansion: Pull chunks from relevant pages ±1 with gap filling
    4. Driver generation: LLM synthesizes qualitative drivers (no metrics)

    Args:
        bank: Bank symbol (e.g., "RY-CA")
        year: Fiscal year
        quarter: Quarter (e.g., "Q3")
        segment_name: Business segment (e.g., "Capital Markets")
        context: Execution context
        top_k: Initial retrieval size (default 20)

    Returns:
        Qualitative drivers statement or None if unavailable
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.rts.pipeline_start",
        execution_id=execution_id,
        segment=segment_name,
        bank=bank,
        period=f"{quarter} {year}",
    )

    # Step 1: Initial retrieval
    initial_chunks = await retrieve_initial_chunks(
        bank=bank,
        year=year,
        quarter=quarter,
        segment_name=segment_name,
        context=context,
        top_k=top_k,
    )

    if not initial_chunks:
        logger.warning("etl.rts.no_initial_chunks", execution_id=execution_id)
        return None

    # Step 2: LLM reranking
    relevant_chunks = await rerank_chunks_for_segment(
        chunks=initial_chunks,
        segment_name=segment_name,
        context=context,
    )

    if not relevant_chunks:
        logger.warning("etl.rts.no_relevant_chunks", execution_id=execution_id)
        return None

    # Step 3: Page-based expansion
    expanded_chunks = await expand_chunks_by_page(
        relevant_chunks=relevant_chunks,
        bank=bank,
        year=year,
        quarter=quarter,
        context=context,
    )

    if not expanded_chunks:
        logger.warning("etl.rts.no_expanded_chunks", execution_id=execution_id)
        return None

    # Step 4: Generate drivers statement
    drivers = await generate_segment_drivers(
        chunks=expanded_chunks,
        segment_name=segment_name,
        context=context,
    )

    logger.info(
        "etl.rts.pipeline_complete",
        execution_id=execution_id,
        segment=segment_name,
        initial_chunks=len(initial_chunks),
        relevant_chunks=len(relevant_chunks),
        expanded_chunks=len(expanded_chunks),
        drivers_generated=drivers is not None,
    )

    return drivers
