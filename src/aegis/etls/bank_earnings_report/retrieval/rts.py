"""
RTS (Regulatory/Risk/Technical Supplement) retrieval for Bank Earnings Report ETL.

This module provides semantic search against the rts_embedding table to retrieve
relevant chunks for generating segment performance drivers statements.

The rts_embedding table contains chunks from bank regulatory filings with:
- Embeddings created from: Bank + Quarter + Year + Summary + Section + Table Terms + Propositions
- Propositions: GPT-extracted factual financial statements
- Source sections: Hierarchical section paths from markdown headings

Workflow:
1. For each business segment, format a query matching the embedding index format
2. Retrieve top N chunks via cosine similarity search
3. Use LLM to synthesize a drivers statement from the retrieved chunks
"""

import json
from typing import Any, Dict, List, Optional

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
        bank: Bank name (e.g., "Royal Bank of Canada")
        year: Fiscal year (e.g., 2025)
        quarter: Quarter (e.g., "Q3")

    Returns:
        Formatted query string matching the embedding index format
    """
    # Match the labeled field format used when creating embeddings
    # Place search terms in Summary and Propositions where semantic content is indexed
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
# These terms are designed to find chunks relevant to each segment's performance
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
    """
    Get search terms for a specific segment.

    Args:
        segment_name: Segment name (e.g., "Capital Markets")

    Returns:
        Search terms string for embedding query
    """
    return SEGMENT_QUERY_TERMS.get(
        segment_name,
        f"{segment_name} performance revenue growth expenses efficiency",
    )


# =============================================================================
# Chunk Retrieval via Semantic Search
# =============================================================================


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
async def retrieve_segment_chunks(
    bank: str,
    year: int,
    quarter: str,
    segment_name: str,
    context: Dict[str, Any],
    top_k: int = 20,
) -> List[Dict[str, Any]]:
    """
    Retrieve top chunks from RTS embeddings for a specific segment.

    Uses semantic similarity search to find chunks most relevant to the segment's
    performance narrative.

    Args:
        bank: Bank name (e.g., "Royal Bank of Canada")
        year: Fiscal year
        quarter: Quarter (e.g., "Q3")
        segment_name: Business segment name (e.g., "Capital Markets")
        context: Execution context with auth_config, ssl_config
        top_k: Number of chunks to retrieve (default 20)

    Returns:
        List of chunk dicts with:
            - id: Database ID
            - chunk_id: Sequential chunk number
            - page_no: Page number from original PDF
            - summary_title: GPT-generated summary
            - source_section: Hierarchical section path
            - raw_text: Markdown text content
            - propositions: JSON array of factual statements
            - similarity: Cosine similarity score (0-1)
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    # Get segment-specific search terms
    query_terms = get_segment_query_terms(segment_name)

    # Format query to match embedding index
    formatted_query = format_query_for_embedding(query_terms, bank, year, quarter)

    logger.info(
        "etl.bank_earnings_report.rts_retrieve_start",
        execution_id=execution_id,
        segment=segment_name,
        bank=bank,
        period=f"{quarter} {year}",
        top_k=top_k,
    )

    try:
        # Generate embedding for the formatted query
        embedding_response = await embed(
            input_text=formatted_query,
            context=context,
            embedding_params={"model": "text-embedding-3-large", "dimensions": 3072},
        )

        if not embedding_response.get("data"):
            logger.error(
                "etl.bank_earnings_report.rts_embedding_failed",
                execution_id=execution_id,
                segment=segment_name,
            )
            return []

        query_embedding = embedding_response["data"][0]["embedding"]

        # Format embedding for PostgreSQL halfvec
        embedding_str = "[" + ",".join(str(x) for x in query_embedding) + "]"

        # Perform similarity search
        async with get_connection() as conn:
            sql = text(
                """
                SELECT
                    id,
                    chunk_id,
                    page_no,
                    summary_title,
                    source_section,
                    raw_text,
                    propositions,
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
                distance = row[7]
                similarity = 1 - distance if distance is not None else 0

                # Parse propositions if stored as JSON string
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
                        "similarity": similarity,
                    }
                )

            logger.info(
                "etl.bank_earnings_report.rts_retrieve_complete",
                execution_id=execution_id,
                segment=segment_name,
                chunks_retrieved=len(chunks),
                top_similarity=chunks[0]["similarity"] if chunks else 0,
            )

            return chunks

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "etl.bank_earnings_report.rts_retrieve_error",
            execution_id=execution_id,
            segment=segment_name,
            error=str(e),
        )
        return []


# =============================================================================
# Drivers Statement Generation
# =============================================================================


def format_chunks_for_llm(
    chunks: List[Dict[str, Any]],
    segment_name: str,
    bank_name: str,
    quarter: str,
    fiscal_year: int,
) -> str:
    """
    Format retrieved chunks into context for LLM drivers extraction.

    Args:
        chunks: List of chunk dicts from retrieve_segment_chunks()
        segment_name: Business segment name
        bank_name: Bank name
        quarter: Quarter
        fiscal_year: Fiscal year

    Returns:
        Formatted context string for LLM prompt
    """
    if not chunks:
        return "No relevant content found."

    lines = [
        f"# {bank_name} - {segment_name} Segment",
        f"## {quarter} {fiscal_year} Regulatory Filing Excerpts",
        "",
        "The following excerpts are from the bank's regulatory filings, "
        "ordered by relevance to the segment:",
        "",
    ]

    for i, chunk in enumerate(chunks, 1):
        section = chunk.get("source_section", "Unknown Section")
        summary = chunk.get("summary_title", "")
        raw_text = chunk.get("raw_text", "")
        propositions = chunk.get("propositions", [])
        similarity = chunk.get("similarity", 0)

        lines.append(f"### Excerpt {i} (Relevance: {similarity:.2%})")
        lines.append(f"**Section:** {section}")

        if summary:
            lines.append(f"**Summary:** {summary}")

        lines.append("")

        if raw_text:
            # Truncate very long text
            text_preview = raw_text[:1500] if len(raw_text) > 1500 else raw_text
            lines.append(text_preview)

        if propositions and isinstance(propositions, list):
            lines.append("")
            lines.append("**Key Facts:**")
            for prop in propositions[:5]:  # Limit to 5 propositions per chunk
                lines.append(f"- {prop}")

        lines.append("")
        lines.append("---")
        lines.append("")

    return "\n".join(lines)


# pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
async def generate_segment_drivers(
    chunks: List[Dict[str, Any]],
    segment_name: str,
    bank_name: str,
    quarter: str,
    fiscal_year: int,
    context: Dict[str, Any],
) -> Optional[str]:
    """
    Generate a drivers statement for a segment from RTS chunks.

    Uses LLM to synthesize a concise narrative about key performance drivers
    for the specified business segment.

    Args:
        chunks: List of chunk dicts from retrieve_segment_chunks()
        segment_name: Business segment name (e.g., "Capital Markets")
        bank_name: Bank name
        quarter: Quarter
        fiscal_year: Fiscal year
        context: Execution context with auth_config, ssl_config

    Returns:
        Drivers statement string or None if generation fails
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    if not chunks:
        logger.warning(
            "etl.bank_earnings_report.rts_no_chunks_for_drivers",
            execution_id=execution_id,
            segment=segment_name,
        )
        return None

    # Format chunks for LLM context
    chunks_context = format_chunks_for_llm(chunks, segment_name, bank_name, quarter, fiscal_year)

    # Build prompts
    system_prompt = f"""You are a senior financial analyst writing a bank quarterly earnings report.

Your task is to write a concise drivers statement for the {segment_name} segment.

## REQUIREMENTS

1. **Length**: 2-3 sentences maximum
2. **Content**: Focus on the key performance drivers from the regulatory filings
3. **Specificity**: Include specific metrics, percentages, or dollar amounts when available
4. **Tone**: Professional, factual, analyst-style
5. **Focus**: What drove performance this quarter (positive and negative factors)

## EXAMPLE OUTPUT

"Revenue growth of 8% YoY driven by higher trading volumes and advisory fees. \
Net interest income benefited from loan growth of $2.1B, partially offset by \
margin compression of 5 bps. Expenses well-controlled with efficiency ratio \
improving to 62.3%."

## IMPORTANT

- Do NOT include segment name in the statement (it's already shown in the header)
- Do NOT use phrases like "In Q3 2024" or "This quarter" - context is already clear
- Focus on the WHY behind the numbers, not just the numbers themselves
- If data is limited, acknowledge what can be determined from available information"""

    user_prompt = f"""Based on the following regulatory filing excerpts for {bank_name}'s \
{segment_name} segment in {quarter} {fiscal_year}, write a concise drivers statement.

{chunks_context}

Write a 2-3 sentence drivers statement summarizing the key performance factors."""

    # Define tool for structured output
    tool_definition = {
        "type": "function",
        "function": {
            "name": "segment_drivers_statement",
            "description": f"Generate a drivers statement for the {segment_name} segment",
            "parameters": {
                "type": "object",
                "properties": {
                    "drivers_statement": {
                        "type": "string",
                        "description": (
                            "A 2-3 sentence statement describing the key performance drivers "
                            "for this segment. Should be specific and include metrics "
                            "where available."
                        ),
                    },
                    "confidence": {
                        "type": "string",
                        "enum": ["high", "medium", "low"],
                        "description": (
                            "Confidence level based on relevance and completeness of source data"
                        ),
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
        # Use model from ETL config
        model = etl_config.get_model("segment_drivers_extraction")

        response = await complete_with_tools(
            messages=messages,
            tools=[tool_definition],
            context=context,
            llm_params={
                "model": model,
                "temperature": 0.2,  # Slightly higher for natural language
                "max_tokens": 500,
            },
        )

        # Parse response
        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])

                drivers_statement = function_args.get("drivers_statement", "")
                confidence = function_args.get("confidence", "unknown")

                logger.info(
                    "etl.bank_earnings_report.rts_drivers_generated",
                    execution_id=execution_id,
                    segment=segment_name,
                    confidence=confidence,
                    statement_length=len(drivers_statement),
                )

                return drivers_statement

        logger.warning(
            "etl.bank_earnings_report.rts_no_tool_call",
            execution_id=execution_id,
            segment=segment_name,
        )
        return None

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "etl.bank_earnings_report.rts_drivers_error",
            execution_id=execution_id,
            segment=segment_name,
            error=str(e),
        )
        return None


# =============================================================================
# Main Entry Point for Segment Drivers
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
    Get a drivers statement for a segment by querying RTS and using LLM synthesis.

    This is the main entry point for RTS-based segment drivers.

    Args:
        bank: Bank name (e.g., "Royal Bank of Canada")
        year: Fiscal year
        quarter: Quarter (e.g., "Q3")
        segment_name: Business segment name (e.g., "Capital Markets")
        context: Execution context
        top_k: Number of chunks to retrieve for context

    Returns:
        Drivers statement string or None if unavailable
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.bank_earnings_report.rts_segment_drivers_start",
        execution_id=execution_id,
        segment=segment_name,
        bank=bank,
        period=f"{quarter} {year}",
    )

    # Step 1: Retrieve relevant chunks
    chunks = await retrieve_segment_chunks(
        bank=bank,
        year=year,
        quarter=quarter,
        segment_name=segment_name,
        context=context,
        top_k=top_k,
    )

    if not chunks:
        logger.warning(
            "etl.bank_earnings_report.rts_no_chunks",
            execution_id=execution_id,
            segment=segment_name,
        )
        return None

    # Step 2: Generate drivers statement from chunks
    drivers = await generate_segment_drivers(
        chunks=chunks,
        segment_name=segment_name,
        bank_name=bank,
        quarter=quarter,
        fiscal_year=year,
        context=context,
    )

    return drivers
