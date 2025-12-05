"""
RTS (Regulatory/Risk/Technical Supplement) retrieval for Bank Earnings Report ETL.

This module loads the full RTS document and uses a single LLM call to extract
qualitative performance drivers for all business segments at once.

The rts_embedding table contains chunks from bank regulatory filings with:
- Raw text content from the filing
- Source sections: Hierarchical section paths from markdown headings
- Page numbers for reference

Pipeline:
1. Load all RTS chunks for the bank/quarter (single DB call)
2. Format into a single document
3. Single LLM call extracts drivers for all segments simultaneously
"""

import json
from typing import Any, Dict, List

from sqlalchemy import bindparam, text

from aegis.connections.llm_connector import complete_with_tools
from aegis.connections.postgres_connector import get_connection
from aegis.etls.bank_earnings_report.config.etl_config import etl_config
from aegis.utils.logging import get_logger


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

        if section and section != current_section:
            lines.append(f"\n## {section}")
            lines.append(f"[Page {page}]")
            lines.append("")
            current_section = section

        if raw_text:
            lines.append(raw_text)
            lines.append("")

    return "\n".join(lines)


async def generate_all_segment_drivers_from_full_rts(
    chunks: List[Dict[str, Any]],
    segment_names: List[str],
    context: Dict[str, Any],
) -> Dict[str, str]:
    """
    Generate qualitative drivers statements for ALL segments in a single LLM call.

    This is more efficient than calling the LLM once per segment, and provides
    better consistency across segment summaries.

    Args:
        chunks: All RTS chunks for the bank/quarter
        segment_names: List of segment names to extract drivers for
        context: Execution context

    Returns:
        Dict mapping segment name to drivers statement (empty string if not found)
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    result: Dict[str, str] = {name: "" for name in segment_names}

    if not chunks:
        logger.warning("etl.rts.no_chunks_for_batch_drivers", execution_id=execution_id)
        return result

    if not segment_names:
        logger.warning("etl.rts.no_segments_provided", execution_id=execution_id)
        return result

    full_rts = format_full_rts_for_llm(chunks)

    segment_list = "\n".join(f"- {name}" for name in segment_names)

    system_prompt = f"""You are a senior financial analyst writing a bank quarterly earnings report.

Your task is to extract performance driver statements for EACH of the following business segments:

{segment_list}

For EACH segment, you will:
1. FIND the section(s) in the regulatory filing that discuss that segment
2. EXTRACT the key performance drivers mentioned
3. WRITE a concise qualitative drivers statement (2-3 sentences)

## CRITICAL REQUIREMENTS

1. **NO METRICS OR NUMBERS**: Do NOT include specific dollar amounts, percentages, basis points, \
or any numerical values. The metrics are shown separately in the report.
2. **QUALITATIVE ONLY**: Focus on the business drivers, trends, and factors - not the numbers.
3. **Length**: 2-3 sentences maximum per segment
4. **Tone**: Professional, factual, analyst-style
5. **Consistency**: Use similar style and depth across all segments

## WHERE TO FIND SEGMENT INFORMATION

Look for sections with headings like:
- The segment name itself (e.g., "Canadian Banking", "Capital Markets")
- "Business Segment Results"
- "Segment Performance"
- "Operating Results by Segment"
- "Results by Business Segment"

Each segment's discussion typically includes explanations of what drove performance changes.

## WHAT TO INCLUDE IN EACH STATEMENT

- Business drivers (e.g., "higher trading activity", "increased client demand")
- Market conditions (e.g., "favorable rate environment", "challenging credit conditions")
- Strategic factors (e.g., "expansion into new markets", "cost discipline initiatives")
- Operational factors (e.g., "improved efficiency", "technology investments")

## WHAT TO EXCLUDE

- Specific dollar amounts (e.g., "$2.1B", "CAD 500 million")
- Percentages (e.g., "8% growth", "up 12%")
- Basis points (e.g., "expanded 15 bps")
- Quarter-over-quarter or year-over-year comparisons with numbers
- The segment name in the statement (it's already shown in the header)

## IF A SEGMENT IS NOT FOUND

If you cannot find content specifically about a segment, return an empty string for that segment.
Do NOT make up information or use content from other segments."""

    user_prompt = f"""Below is the complete regulatory filing document. For each of the \
following segments, find the relevant section and write a 2-3 sentence QUALITATIVE \
drivers statement:

{segment_list}

Remember: NO specific metrics, percentages, or dollar amounts. Focus only on the business drivers.

{full_rts}

Extract the qualitative drivers statement for each segment listed above."""

    segment_properties = {}
    for name in segment_names:
        safe_key = name.lower().replace(" ", "_").replace("&", "and").replace(".", "")
        segment_properties[safe_key] = {
            "type": "object",
            "properties": {
                "found": {
                    "type": "boolean",
                    "description": f"Whether content for {name} was found in the document",
                },
                "drivers_statement": {
                    "type": "string",
                    "description": (
                        f"2-3 sentence qualitative drivers statement for {name}. "
                        "No numbers, percentages, or dollar amounts. "
                        "Empty string if segment not found."
                    ),
                },
            },
            "required": ["found", "drivers_statement"],
        }

    tool_definition = {
        "type": "function",
        "function": {
            "name": "all_segment_drivers",
            "description": "Extract qualitative drivers statements for all business segments",
            "parameters": {
                "type": "object",
                "properties": segment_properties,
                "required": list(segment_properties.keys()),
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

                for name in segment_names:
                    safe_key = name.lower().replace(" ", "_").replace("&", "and").replace(".", "")
                    segment_data = function_args.get(safe_key, {})

                    if segment_data.get("found") and segment_data.get("drivers_statement"):
                        result[name] = segment_data["drivers_statement"]
                        logger.info(
                            "etl.rts.batch_driver_extracted",
                            execution_id=execution_id,
                            segment=name,
                            statement_length=len(segment_data["drivers_statement"]),
                        )
                    else:
                        logger.info(
                            "etl.rts.batch_driver_not_found",
                            execution_id=execution_id,
                            segment=name,
                        )

                logger.info(
                    "etl.rts.batch_drivers_complete",
                    execution_id=execution_id,
                    segments_requested=len(segment_names),
                    segments_found=sum(1 for v in result.values() if v),
                )
                return result

        logger.warning("etl.rts.batch_drivers_no_tool_call", execution_id=execution_id)
        return result

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("etl.rts.batch_drivers_error", error=str(e))
        return result


async def get_all_segment_drivers_from_rts(
    bank: str,
    year: int,
    quarter: str,
    segment_names: List[str],
    context: Dict[str, Any],
) -> Dict[str, str]:
    """
    Get qualitative drivers statements for ALL segments in a single LLM call.

    This is more efficient than calling once per segment:
    - Single RTS load from database
    - Single LLM call for all segments
    - Better consistency in tone/style across segments

    Args:
        bank: Bank symbol (e.g., "RY-CA")
        year: Fiscal year
        quarter: Quarter (e.g., "Q3")
        segment_names: List of segment names to extract drivers for
        context: Execution context

    Returns:
        Dict mapping segment name to drivers statement (empty string if not found)
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.rts.batch_pipeline_start",
        execution_id=execution_id,
        bank=bank,
        period=f"{quarter} {year}",
        segments=segment_names,
    )

    all_chunks = await retrieve_all_rts_chunks(
        bank=bank,
        year=year,
        quarter=quarter,
        context=context,
    )

    if not all_chunks:
        logger.warning("etl.rts.no_chunks_loaded_batch", execution_id=execution_id)
        return {name: "" for name in segment_names}

    drivers = await generate_all_segment_drivers_from_full_rts(
        chunks=all_chunks,
        segment_names=segment_names,
        context=context,
    )

    logger.info(
        "etl.rts.batch_pipeline_complete",
        execution_id=execution_id,
        total_chunks=len(all_chunks),
        segments_with_drivers=sum(1 for v in drivers.values() if v),
    )

    return drivers


async def extract_rts_items_of_note(
    bank_symbol: str,
    bank_name: str,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    max_items: int = 8,
) -> Dict[str, Any]:
    """
    Extract key defining items from RTS regulatory filings.

    Items of Note are the events and developments that MOST SIGNIFICANTLY DEFINED
    this quarter for the bank - not just what's mentioned, but what matters most
    to understanding the bank's quarter.

    Each item is scored by significance (1-10) to enable ranking.

    Args:
        bank_symbol: Bank symbol (e.g., "RY")
        bank_name: Full bank name
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context
        max_items: Maximum items to extract (default 8)

    Returns:
        Dict with:
            - source: "RTS"
            - items: List of item dicts with description, impact, segment, timing, score
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.rts.items_of_note_start",
        execution_id=execution_id,
        bank=bank_symbol,
        period=f"{quarter} {fiscal_year}",
    )

    db_symbol = f"{bank_symbol}-CA"
    chunks = await retrieve_all_rts_chunks(db_symbol, fiscal_year, quarter, context)

    if not chunks:
        logger.warning(
            "etl.rts.items_of_note_no_chunks",
            execution_id=execution_id,
        )
        return {"source": "RTS", "items": []}

    full_rts = format_full_rts_for_llm(chunks)

    if not full_rts.strip() or full_rts == "No RTS content available.":
        return {"source": "RTS", "items": []}

    system_prompt = f"""You are a senior financial analyst identifying the KEY DEFINING ITEMS \
for {bank_name}'s {quarter} {fiscal_year} quarter from their regulatory filing (RTS).

## YOUR MISSION

Find the items that MOST SIGNIFICANTLY DEFINED this quarter for the bank. Not just what's \
mentioned in the filing, but what truly MATTERS - the events, decisions, and developments that \
an analyst would point to when explaining "what happened this quarter" to investors.

Think: "If I had to explain what defined {bank_name}'s {quarter} to an investor in 30 seconds, \
which items from this filing would I mention?"

## WHAT MAKES AN ITEM "DEFINING"

A defining item has HIGH IMPACT on the bank through one or more of:

1. **Financial Materiality**: Significant dollar impact on earnings, capital, or valuation
   - Major acquisitions or divestitures (>$500M)
   - Large impairments or write-downs
   - Significant legal settlements or regulatory penalties

2. **Strategic Significance**: Changes the bank's trajectory or market position
   - Entry or exit from major business lines
   - Transformational deals or partnerships
   - Major restructuring programs

3. **Investor Relevance**: Would be highlighted in analyst reports or earnings headlines
   - Items that explain earnings beat/miss
   - Risk events that affect outlook
   - One-time items that distort comparisons

## WHAT TO EXCLUDE

**Routine Operations (NEVER extract):**
- Capital note/debenture issuance or redemption
- Preferred share activity
- NCIB share repurchases
- Regular dividend declarations
- Normal PCL provisions
- Routine debt refinancing

**Performance Results (NOT items):**
- "Revenue increased X%"
- "NIM expanded Y bps"
- "Expenses down Z%"
These are RESULTS, not defining ITEMS.

## SIGNIFICANCE SCORING (1-10)

Score each item based on how much it DEFINED the quarter:

- **9-10**: Quarter-defining event (major M&A close, significant impairment, transformational)
- **7-8**: Highly significant (large one-time item, notable strategic move)
- **5-6**: Moderately significant (meaningful but not headline-level)
- **3-4**: Minor significance (worth noting but not quarter-defining)
- **1-2**: Low significance (borderline whether to include)

Be discriminating - not every item is highly significant. A quarter might have only 1-2 truly \
defining items and several minor ones. That's fine.

## OUTPUT FORMAT

For each item:
- **Description**: What happened (10-20 words, factual)
- **Impact**: Dollar amount exactly as stated ('+$150M', '-$1.2B', 'TBD')
- **Segment**: Affected business segment
- **Timing**: When/duration
- **Score**: Significance score (1-10)"""

    user_prompt = f"""Review {bank_name}'s {quarter} {fiscal_year} regulatory filing and identify \
the items that MOST SIGNIFICANTLY DEFINED this quarter for the bank.

{full_rts}

Extract items based on their IMPACT TO THE BANK, not just their presence in the filing. \
Score each item by significance (1-10). Quality over quantity - it's better to return 3 truly \
defining items than 8 marginal ones."""

    tool_definition = {
        "type": "function",
        "function": {
            "name": "extract_rts_items_of_note",
            "description": (
                "Extract key defining items from regulatory filing with significance scores"
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "description": {
                                    "type": "string",
                                    "description": (
                                        "Brief description of the defining item (10-20 words). "
                                        "What happened - factual, not commentary."
                                    ),
                                },
                                "impact": {
                                    "type": "string",
                                    "description": (
                                        "Dollar impact ONLY. "
                                        "Format: '+$150M', '-$45M', '~$100M', '-$1.2B', 'TBD'. "
                                        "No qualifiers or additional text."
                                    ),
                                },
                                "segment": {
                                    "type": "string",
                                    "description": (
                                        "Affected segment: 'Canadian Banking', 'Capital Markets', "
                                        "'Wealth & Insurance', 'U.S. Banking', 'All', or 'N/A'"
                                    ),
                                },
                                "timing": {
                                    "type": "string",
                                    "description": (
                                        "Timing: 'One-time', 'Q3 2025', 'Through 2025', etc."
                                    ),
                                },
                                "significance_score": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 10,
                                    "description": (
                                        "How much this item DEFINED the quarter (1-10). "
                                        "10=quarter-defining, 7-8=highly significant, "
                                        "5-6=moderate, 3-4=minor, 1-2=low."
                                    ),
                                },
                            },
                            "required": [
                                "description",
                                "impact",
                                "segment",
                                "timing",
                                "significance_score",
                            ],
                        },
                        "description": (
                            "Defining items with significance scores (quality over quantity)"
                        ),
                        "maxItems": max_items,
                    },
                    "extraction_notes": {
                        "type": "string",
                        "description": (
                            "Brief note: what defined this quarter, or why few items found."
                        ),
                    },
                },
                "required": ["items", "extraction_notes"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("rts_items_extraction")

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
                items = function_args.get("items", [])
                notes = function_args.get("extraction_notes", "")

                logger.info(
                    "etl.rts.items_of_note_complete",
                    execution_id=execution_id,
                    items_count=len(items),
                    extraction_notes=notes,
                )

                return {"source": "RTS", "items": items, "notes": notes}

        logger.warning(
            "etl.rts.items_of_note_no_result",
            execution_id=execution_id,
        )
        return {"source": "RTS", "items": []}

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "etl.rts.items_of_note_error",
            execution_id=execution_id,
            error=str(e),
        )
        return {"source": "RTS", "items": []}


async def extract_rts_overview(
    bank_symbol: str,
    bank_name: str,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extract a high-level overview summary from RTS regulatory filings.

    Creates a single paragraph (3-5 sentences) that captures:
    - Quarter's key financial performance themes
    - Strategic developments and priorities
    - Capital and risk positioning
    - Forward-looking perspective from regulatory disclosures

    This will be combined with transcript overview for the final summary.

    Args:
        bank_symbol: Bank symbol (e.g., "RY")
        bank_name: Full bank name
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context

    Returns:
        Dict with:
            - source: "RTS"
            - narrative: Overview paragraph string
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.rts.overview_start",
        execution_id=execution_id,
        bank=bank_symbol,
        period=f"{quarter} {fiscal_year}",
    )

    db_symbol = f"{bank_symbol}-CA"
    chunks = await retrieve_all_rts_chunks(db_symbol, fiscal_year, quarter, context)

    if not chunks:
        logger.warning(
            "etl.rts.overview_no_chunks",
            execution_id=execution_id,
        )
        return {"source": "RTS", "narrative": ""}

    full_rts = format_full_rts_for_llm(chunks)

    if not full_rts.strip() or full_rts == "No RTS content available.":
        return {"source": "RTS", "narrative": ""}

    system_prompt = """You are a senior financial analyst creating an executive summary from \
bank regulatory filings (RTS - Report to Shareholders).

## YOUR TASK

Write a single paragraph (3-5 sentences, 60-100 words) that captures the key themes from \
the regulatory filing. This overview sets the stage for a quarterly earnings report.

## WHAT TO INCLUDE

- Overall quarter financial performance narrative
- Key strategic developments or initiatives mentioned
- Capital position and risk management highlights
- Business segment performance themes
- Any significant regulatory or operational developments

## WHAT TO AVOID

- Specific metrics or numbers (those are in other sections)
- Detailed segment breakdowns with figures
- Generic boilerplate language
- Repetition of standard regulatory disclosures

## STYLE

- Executive summary tone - concise and insightful
- Third person perspective ("The bank reported...", "Management highlighted...")
- Focus on qualitative themes and strategic narrative
- Should feel like the opening paragraph of an analyst report"""

    user_prompt = f"""Write a brief overview paragraph summarizing the key themes from \
{bank_name}'s {quarter} {fiscal_year} regulatory filing (RTS).

{full_rts}

Provide a 3-5 sentence overview that captures the quarter's performance narrative and \
strategic themes as disclosed in the regulatory filing."""

    tool_definition = {
        "type": "function",
        "function": {
            "name": "create_rts_overview",
            "description": "Create a high-level overview paragraph from regulatory filing",
            "parameters": {
                "type": "object",
                "properties": {
                    "overview": {
                        "type": "string",
                        "description": (
                            "Overview paragraph (3-5 sentences, 60-100 words). "
                            "Captures key themes, performance narrative, and strategic direction. "
                            "No specific metrics or quotes."
                        ),
                    },
                },
                "required": ["overview"],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("rts_overview_extraction")

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
                overview = function_args.get("overview", "")

                logger.info(
                    "etl.rts.overview_complete",
                    execution_id=execution_id,
                    overview_length=len(overview),
                )

                return {"source": "RTS", "narrative": overview}

        logger.warning(
            "etl.rts.overview_no_result",
            execution_id=execution_id,
        )
        return {"source": "RTS", "narrative": ""}

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "etl.rts.overview_error",
            execution_id=execution_id,
            error=str(e),
        )
        return {"source": "RTS", "narrative": ""}


async def extract_rts_narrative_paragraphs(
    bank_symbol: str,
    bank_name: str,
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Extract 4 structured narrative paragraphs from RTS regulatory filings.

    Creates 4 paragraphs covering different aspects of the quarter:
    1. Financial Performance - Overall earnings narrative and key drivers
    2. Business Segments - Highlights from major business lines
    3. Risk & Capital - Credit quality, capital position, risk management
    4. Strategic Outlook - Forward-looking themes and priorities

    These paragraphs will be interleaved with transcript quotes
    in the Management Narrative section.

    Args:
        bank_symbol: Bank symbol (e.g., "RY")
        bank_name: Full bank name
        fiscal_year: Fiscal year
        quarter: Quarter (e.g., "Q2")
        context: Execution context

    Returns:
        Dict with:
            - paragraphs: List of 4 paragraph dicts, each with theme and content
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "etl.rts.narrative_paragraphs_start",
        execution_id=execution_id,
        bank=bank_symbol,
        period=f"{quarter} {fiscal_year}",
    )

    db_symbol = f"{bank_symbol}-CA"
    chunks = await retrieve_all_rts_chunks(db_symbol, fiscal_year, quarter, context)

    if not chunks:
        logger.warning(
            "etl.rts.narrative_paragraphs_no_chunks",
            execution_id=execution_id,
        )
        return {"paragraphs": []}

    full_rts = format_full_rts_for_llm(chunks)

    if not full_rts.strip() or full_rts == "No RTS content available.":
        return {"paragraphs": []}

    system_prompt = f"""You are a senior financial analyst extracting MANAGEMENT'S NARRATIVE \
from {bank_name}'s regulatory filing (RTS - Report to Shareholders).

## WHAT THIS IS

The RTS contains management's written narrative explaining the quarter - their perspective, \
reasoning, and outlook. Your job is to extract this NARRATIVE VOICE, not summarize metrics.

## WHAT WE WANT

✓ Management's EXPLANATIONS for what drove performance
✓ Their PERSPECTIVE on business conditions and trends
✓ QUALITATIVE drivers - why things happened, not what the numbers were
✓ OUTLOOK and forward-looking themes management emphasized
✓ STRATEGIC context - priorities, initiatives, market positioning

## WHAT WE DON'T WANT

❌ Metric summaries ("Revenue was $X, up Y%")
❌ Data recaps that belong in a metrics table
❌ Generic descriptions of what the bank does
❌ Boilerplate regulatory language

## THE 4 PARAGRAPHS (in order)

1. **Financial Performance** (3-4 sentences)
   - How management characterized the quarter's performance
   - The narrative around earnings drivers and trends
   - What factors management highlighted as influential
   - The tone and perspective on profitability

2. **Business Segments** (3-4 sentences)
   - Management's narrative on segment performance
   - Which segments they emphasized and why
   - The drivers behind segment results (qualitative)
   - Business mix and strategic positioning themes

3. **Risk & Capital** (3-4 sentences)
   - Management's perspective on credit quality trajectory
   - Their narrative around capital and risk management
   - How they're thinking about provisions and reserves
   - Liquidity and funding themes they highlighted

4. **Strategic Outlook** (3-4 sentences)
   - Management's forward-looking perspective
   - Strategic priorities they emphasized
   - How they see the path ahead
   - Market opportunities and positioning

## STYLE

- Third person ("Management noted...", "The bank highlighted...")
- NARRATIVE prose, not bullet points or data summaries
- 60-100 words per paragraph
- Should read like management's story, not an analyst's data recap"""

    user_prompt = f"""Extract management's narrative voice from {bank_name}'s {quarter} \
{fiscal_year} regulatory filing.

{full_rts}

Create 4 paragraphs capturing management's perspective, explanations, and outlook - \
NOT metric summaries. Focus on the qualitative narrative: why things happened, \
how management sees the business, and their forward-looking view."""

    tool_definition = {
        "type": "function",
        "function": {
            "name": "extract_narrative_paragraphs",
            "description": "Extract 4 structured narrative paragraphs from regulatory filing",
            "parameters": {
                "type": "object",
                "properties": {
                    "financial_performance": {
                        "type": "string",
                        "description": (
                            "Paragraph 1: Financial Performance (3-4 sentences, 60-100 words). "
                            "Overall quarter narrative, earnings drivers, profitability themes."
                        ),
                    },
                    "business_segments": {
                        "type": "string",
                        "description": (
                            "Paragraph 2: Business Segments (3-4 sentences, 60-100 words). "
                            "Segment highlights, performance themes, business mix."
                        ),
                    },
                    "risk_capital": {
                        "type": "string",
                        "description": (
                            "Paragraph 3: Risk & Capital (3-4 sentences, 60-100 words). "
                            "Credit quality, capital position, risk management themes."
                        ),
                    },
                    "strategic_outlook": {
                        "type": "string",
                        "description": (
                            "Paragraph 4: Strategic Outlook (3-4 sentences, 60-100 words). "
                            "Forward-looking themes, priorities, market positioning."
                        ),
                    },
                },
                "required": [
                    "financial_performance",
                    "business_segments",
                    "risk_capital",
                    "strategic_outlook",
                ],
            },
        },
    }

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    try:
        model = etl_config.get_model("rts_narrative_extraction")

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

                paragraphs = [
                    {
                        "theme": "Financial Performance",
                        "content": function_args.get("financial_performance", ""),
                    },
                    {
                        "theme": "Business Segments",
                        "content": function_args.get("business_segments", ""),
                    },
                    {
                        "theme": "Risk & Capital",
                        "content": function_args.get("risk_capital", ""),
                    },
                    {
                        "theme": "Strategic Outlook",
                        "content": function_args.get("strategic_outlook", ""),
                    },
                ]

                # Filter out empty paragraphs
                paragraphs = [p for p in paragraphs if p["content"].strip()]

                logger.info(
                    "etl.rts.narrative_paragraphs_complete",
                    execution_id=execution_id,
                    paragraph_count=len(paragraphs),
                    themes=[p["theme"] for p in paragraphs],
                )

                return {"paragraphs": paragraphs}

        logger.warning(
            "etl.rts.narrative_paragraphs_no_result",
            execution_id=execution_id,
        )
        return {"paragraphs": []}

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error(
            "etl.rts.narrative_paragraphs_error",
            execution_id=execution_id,
            error=str(e),
        )
        return {"paragraphs": []}
