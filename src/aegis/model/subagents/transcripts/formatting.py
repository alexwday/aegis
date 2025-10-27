"""
Formatting and post-processing utilities for transcript data.

Handles:
- Chunk formatting by section and speaker blocks
- Reranking and filtering for similarity search
- Block expansion and gap filling
- Research statement generation
"""

from typing import List, Dict, Any
import json

from ....connections.postgres_connector import get_connection
from ....connections.llm_connector import complete, complete_with_tools
from ....utils.logging import get_logger
from ....utils.settings import config
from sqlalchemy import text


async def format_full_section_chunks(
    chunks: List[Dict[str, Any]],
    combo: Dict[str, Any],
    context: Dict[str, Any],
    priority_blocks: List[Dict[str, Any]] = None,
) -> str:
    """
    Format chunks retrieved via Method 0 (Full Section).
    Preserves all content without truncation.

    Args:
        chunks: Retrieved transcript chunks
        combo: Bank-period combination
        context: Execution context
        priority_blocks: Optional priority blocks to prepend (for hybrid retrieval)

    Returns:
        Formatted transcript text with ALL content
    """
    if not chunks:
        return "No transcript data available."

    logger = get_logger()
    execution_id = context.get("execution_id")

    # Filter out chunks missing proper IDs
    valid_chunks = []
    for chunk in chunks:
        if chunk.get("section_name") == "Q&A":
            # Q&A chunks must have qa_group_id
            if chunk.get("qa_group_id") is not None:
                valid_chunks.append(chunk)
            else:
                logger.warning(f"Removed Q&A chunk missing qa_group_id: {chunk.get('id')}")
        elif chunk.get("section_name") == "MANAGEMENT DISCUSSION SECTION":
            # MD chunks must have speaker_block_id
            if chunk.get("speaker_block_id") is not None:
                valid_chunks.append(chunk)
            else:
                logger.warning(f"Removed MD chunk missing speaker_block_id: {chunk.get('id')}")
        else:
            # Keep other chunks if they have either ID
            if chunk.get("speaker_block_id") is not None or chunk.get("qa_group_id") is not None:
                valid_chunks.append(chunk)

    chunks = valid_chunks

    if not chunks:
        return "No valid transcript data available (all chunks missing required IDs)."

    # Get title from first chunk (same for all chunks in combo)
    title = chunks[0].get("title", "Earnings Call Transcript") if chunks else "Transcript"

    # Build header
    formatted = f"""**Institution Details**
- Institution ID: {combo['bank_id']}
- Ticker: {combo['bank_symbol']}
- Company: {combo['bank_name']}
- Period: {combo['quarter']} {combo['fiscal_year']}
- Title: {title}

---

"""

    # Prepend priority blocks if provided
    if priority_blocks:
        formatted += format_priority_blocks_for_synthesis(priority_blocks)

    # Sort chunks by section, then by speaker_block_id for MD or qa_group_id for Q&A, then chunk_id
    sorted_chunks = sorted(
        chunks,
        key=lambda x: (
            0 if x.get("section_name") == "MANAGEMENT DISCUSSION SECTION" else 1,
            (
                x.get("qa_group_id", 0)
                if x.get("section_name") == "Q&A"
                else x.get("speaker_block_id", 0)
            ),
            x.get("chunk_id", 0),
        ),
    )

    # Group by section
    current_section = None
    current_qa_group = None
    section_num = 0
    qa_count = 0

    # Count total Q&A groups for logging
    qa_groups = set(
        chunk.get("qa_group_id")
        for chunk in sorted_chunks
        if chunk.get("section_name") == "Q&A" and chunk.get("qa_group_id")
    )
    if qa_groups:
        logger.info(
            f"Formatting {len(qa_groups)} Q&A exchanges",
            execution_id=execution_id,
            qa_group_ids=sorted(qa_groups),
        )

    for chunk in sorted_chunks:
        section_name = chunk.get("section_name", "Unknown")

        # New section
        if section_name != current_section:
            section_num += 1
            current_section = section_name
            formatted += f"\n## Section {section_num}: {section_name}\n\n"
            current_qa_group = None
            qa_count = 0

        # Handle Q&A grouping
        if section_name == "Q&A" and chunk.get("qa_group_id"):
            if chunk["qa_group_id"] != current_qa_group:
                current_qa_group = chunk["qa_group_id"]
                qa_count += 1
                formatted += f"\n### Question {qa_count} (Q&A Group {current_qa_group})\n\n"

        # Include full content without truncation
        content = chunk.get("content", "")
        if content:
            formatted += f"{content}\n\n"

    return formatted


async def rerank_similarity_chunks(
    chunks: List[Dict[str, Any]], search_phrase: str, context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Rerank similarity search results by filtering irrelevant chunks.

    Args:
        chunks: Top-k chunks from similarity search
        search_phrase: Original search phrase
        context: Execution context

    Returns:
        Filtered list of relevant chunks
    """
    if not chunks:
        return chunks

    logger = get_logger()
    execution_id = context.get("execution_id")

    # Load reranking prompts from database with global contexts
    from ....utils.prompt_loader import load_prompt_from_db

    try:
        rerank_prompts = load_prompt_from_db(
            layer="transcripts",
            name="reranking",
            compose_with_globals=True,
            available_databases=None,  # Transcripts doesn't filter databases
            execution_id=execution_id
        )

        # Use composed prompt if available (includes fiscal, project globals)
        if "composed_prompt" in rerank_prompts:
            system_prompt = rerank_prompts["composed_prompt"]
        else:
            system_prompt = rerank_prompts["system_prompt"]

        user_template = rerank_prompts["user_prompt"]
        reranking_tool = rerank_prompts["tool_definition"]
    except Exception as e:
        logger.error(f"Failed to load reranking prompt from database: {e}")
        raise RuntimeError(f"Critical error: Could not load reranking prompts from database: {e}")

    # Build chunk summaries for reranking
    chunk_summaries = []
    for i, chunk in enumerate(chunks):
        # Use block_summary if available, otherwise use first 200 chars
        summary = chunk.get("block_summary")
        if not summary:
            content = chunk.get("content", "")
            summary = content[:200] if content else ""
        chunk_summaries.append(f"{i}: {summary}")

    # Format user prompt using template from YAML
    user_prompt = user_template.format(
        search_phrase=search_phrase,
        num_chunks=len(chunks),
        chunk_summaries=chr(10).join(chunk_summaries),
    )

    try:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        # Use medium model for reranking (balance cost/performance)
        model_config = getattr(config.llm, "medium")
        response = await complete_with_tools(
            messages=messages,
            tools=[reranking_tool],
            context=context,
            llm_params={
                "model": model_config.model
                # Use defaults from config
            },
        )

        # Parse tool call response to get irrelevant indices
        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])
                irrelevant_indices = function_args.get("irrelevant_indices", [])

                # Filter out irrelevant chunks (keep chunks NOT in irrelevant_indices)
                relevant_chunks = [
                    chunk for i, chunk in enumerate(chunks) if i not in irrelevant_indices
                ]

                logger.info(
                    "subagent.transcripts.reranking",
                    execution_id=execution_id,
                    original_count=len(chunks),
                    filtered_count=len(irrelevant_indices),
                    kept_count=len(relevant_chunks),
                )

                return relevant_chunks

        logger.warning(
            "subagent.transcripts.reranking_no_tool_call",
            execution_id=execution_id,
        )

    except Exception as e:
        logger.error(
            "subagent.transcripts.reranking_error", execution_id=execution_id, error=str(e)
        )

    # If reranking fails, return original chunks
    return chunks


async def expand_speaker_blocks(
    chunks: List[Dict[str, Any]], combo: Dict[str, Any], context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Expand MD section chunks to complete speaker blocks.

    Args:
        chunks: Filtered chunks after reranking
        combo: Bank-period combination
        context: Execution context

    Returns:
        Expanded list of chunks with complete speaker blocks
    """
    if not chunks:
        return chunks

    logger = get_logger()
    execution_id = context.get("execution_id")

    # Separate MD and Q&A chunks
    md_chunks = [c for c in chunks if c.get("section_name") == "MANAGEMENT DISCUSSION SECTION"]
    qa_chunks = [c for c in chunks if c.get("section_name") == "Q&A"]

    # Q&A chunks don't need expansion (already complete)
    expanded_chunks = qa_chunks.copy()

    if md_chunks:
        # Get unique speaker block IDs from MD section
        speaker_block_ids = set()
        for chunk in md_chunks:
            if chunk.get("speaker_block_id"):
                speaker_block_ids.add(chunk["speaker_block_id"])

        if speaker_block_ids:
            # Fetch all chunks for these speaker blocks
            try:
                async with get_connection() as conn:
                    bank_id_filter = (
                        "(institution_id = :bank_id_str " "OR institution_id::text = :bank_id_str)"
                    )
                    query = text(
                        f"""
                        SELECT
                            id,
                            section_name,
                            speaker_block_id,
                            qa_group_id,
                            chunk_id,
                            chunk_content,
                            block_summary,
                            classification_ids,
                            classification_names,
                            title
                        FROM aegis_transcripts
                        WHERE {bank_id_filter}
                            AND fiscal_year = :fiscal_year
                            AND fiscal_quarter = :quarter
                            AND section_name = 'MANAGEMENT DISCUSSION SECTION'
                            AND speaker_block_id = ANY(:block_ids)
                        ORDER BY speaker_block_id, chunk_id
                    """
                    )

                    result = await conn.execute(
                        query,
                        {
                            "bank_id_str": str(combo["bank_id"]),
                            "fiscal_year": combo["fiscal_year"],
                            "quarter": combo["quarter"],
                            "block_ids": list(speaker_block_ids),
                        },
                    )

                    for row in result:
                        expanded_chunks.append(
                            {
                                "id": row[0],
                                "section_name": row[1],
                                "speaker_block_id": row[2],
                                "qa_group_id": row[3],
                                "chunk_id": row[4],
                                "content": row[5],
                                "block_summary": row[6],
                                "classification_ids": row[7],
                                "classification_names": row[8],
                                "title": row[9],
                            }
                        )

                    logger.info(
                        "subagent.transcripts.block_expansion",
                        execution_id=execution_id,
                        original_blocks=len(speaker_block_ids),
                        expanded_chunks=len(
                            [
                                c
                                for c in expanded_chunks
                                if c.get("section_name") == "MANAGEMENT DISCUSSION SECTION"
                            ]
                        ),
                    )

            except Exception as e:
                logger.error(
                    "subagent.transcripts.expansion_error", execution_id=execution_id, error=str(e)
                )
                # Return original chunks if expansion fails
                return chunks

    return expanded_chunks


async def fill_gaps_in_speaker_blocks(
    chunks: List[Dict[str, Any]], combo: Dict[str, Any], context: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Fill single-block gaps in MD section speaker blocks.

    Args:
        chunks: Expanded chunks
        combo: Bank-period combination
        context: Execution context

    Returns:
        Chunks with gap-filled speaker blocks
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    # Get MD speaker block IDs
    md_block_ids = set()
    for chunk in chunks:
        if chunk.get("section_name") == "MANAGEMENT DISCUSSION SECTION" and chunk.get(
            "speaker_block_id"
        ):
            md_block_ids.add(chunk["speaker_block_id"])

    if not md_block_ids:
        return chunks

    # Find gaps of 1
    sorted_ids = sorted(md_block_ids)
    gaps_to_fill = []

    for i in range(len(sorted_ids) - 1):
        if sorted_ids[i + 1] - sorted_ids[i] == 2:
            # Gap of 1 found
            gap_id = sorted_ids[i] + 1
            gaps_to_fill.append(gap_id)

    if gaps_to_fill:
        logger.info(
            "subagent.transcripts.gap_filling",
            execution_id=execution_id,
            gaps_found=len(gaps_to_fill),
            gap_ids=gaps_to_fill,
        )

        # Fetch gap chunks
        try:
            async with get_connection() as conn:
                query = text(
                    """
                    SELECT
                        id,
                        section_name,
                        speaker_block_id,
                        qa_group_id,
                        chunk_id,
                        chunk_content,
                        block_summary,
                        classification_ids,
                        classification_names,
                        title
                    FROM aegis_transcripts
                    WHERE (institution_id = :bank_id_str OR institution_id::text = :bank_id_str)
                        AND fiscal_year = :fiscal_year
                        AND fiscal_quarter = :quarter
                        AND section_name = 'MANAGEMENT DISCUSSION SECTION'
                        AND speaker_block_id = ANY(:block_ids)
                    ORDER BY speaker_block_id, chunk_id
                """
                )

                result = await conn.execute(
                    query,
                    {
                        "bank_id_str": str(combo["bank_id"]),
                        "fiscal_year": combo["fiscal_year"],
                        "quarter": combo["quarter"],
                        "block_ids": gaps_to_fill,
                    },
                )

                gap_chunks = []
                for row in result:
                    gap_chunks.append(
                        {
                            "id": row[0],
                            "section_name": row[1],
                            "speaker_block_id": row[2],
                            "qa_group_id": row[3],
                            "chunk_id": row[4],
                            "content": row[5],
                            "block_summary": row[6],
                            "classification_ids": row[7],
                            "classification_names": row[8],
                            "title": row[9],
                        }
                    )

                # Add gap chunks to result
                chunks.extend(gap_chunks)

        except Exception as e:
            logger.error(
                "subagent.transcripts.gap_fill_error", execution_id=execution_id, error=str(e)
            )

    return chunks


async def format_category_or_similarity_chunks(
    chunks: List[Dict[str, Any]],
    combo: Dict[str, Any],
    context: Dict[str, Any],
    note_gaps: bool = True,
    priority_blocks: List[Dict[str, Any]] = None,
) -> str:
    """
    Format chunks for Method 1 (Category) or Method 2 (Similarity) with gap notation.

    Args:
        chunks: Retrieved/processed chunks
        combo: Bank-period combination
        context: Execution context
        note_gaps: Whether to note gaps in sequence
        priority_blocks: Optional priority blocks to prepend (for Method 1 hybrid retrieval)

    Returns:
        Formatted transcript text
    """
    if not chunks:
        return "No transcript data available."

    logger = get_logger()

    # Filter out chunks missing proper IDs
    valid_chunks = []
    for chunk in chunks:
        if chunk.get("section_name") == "Q&A":
            # Q&A chunks must have qa_group_id
            if chunk.get("qa_group_id") is not None:
                valid_chunks.append(chunk)
            else:
                logger.warning(f"Removed Q&A chunk missing qa_group_id: {chunk.get('id')}")
        elif chunk.get("section_name") == "MANAGEMENT DISCUSSION SECTION":
            # MD chunks must have speaker_block_id
            if chunk.get("speaker_block_id") is not None:
                valid_chunks.append(chunk)
            else:
                logger.warning(f"Removed MD chunk missing speaker_block_id: {chunk.get('id')}")
        else:
            # Keep other chunks if they have either ID
            if chunk.get("speaker_block_id") is not None or chunk.get("qa_group_id") is not None:
                valid_chunks.append(chunk)

    chunks = valid_chunks

    if not chunks:
        return "No valid transcript data available (all chunks missing required IDs)."

    # Get title from first chunk
    title = chunks[0].get("title", "Earnings Call Transcript") if chunks else "Transcript"

    # Build header
    formatted = f"""**Institution Details**
- Institution ID: {combo['bank_id']}
- Ticker: {combo['bank_symbol']}
- Company: {combo['bank_name']}
- Period: {combo['quarter']} {combo['fiscal_year']}
- Title: {title}

---

"""

    # Prepend priority blocks if provided (for Method 0 and Method 1 hybrid retrieval, not Method 2)
    if priority_blocks:
        formatted += format_priority_blocks_for_synthesis(priority_blocks)

    # Sort chunks by section, then by speaker_block_id for MD or qa_group_id for Q&A, then chunk_id
    sorted_chunks = sorted(
        chunks,
        key=lambda x: (
            0 if x.get("section_name") == "MANAGEMENT DISCUSSION SECTION" else 1,
            (
                x.get("qa_group_id", 0)
                if x.get("section_name") == "Q&A"
                else x.get("speaker_block_id", 0)
            ),
            x.get("chunk_id", 0),
        ),
    )

    # Group by section and track gaps
    current_section = None
    section_num = 0
    last_speaker_block = None
    last_qa_group = None

    for chunk in sorted_chunks:
        section_name = chunk.get("section_name", "Unknown")

        # New section
        if section_name != current_section:
            section_num += 1
            current_section = section_name
            formatted += f"\n## Section {section_num}: {section_name}\n\n"
            last_speaker_block = None
            last_qa_group = None

        # Check for gaps in MD section
        if section_name == "MANAGEMENT DISCUSSION SECTION" and note_gaps:
            speaker_block = chunk.get("speaker_block_id")
            if speaker_block and last_speaker_block and speaker_block - last_speaker_block > 1:
                gap_size = speaker_block - last_speaker_block - 1
                formatted += (
                    f"*[Gap: {gap_size} speaker block{'s' if gap_size > 1 else ''} omitted]*\n\n"
                )
            last_speaker_block = speaker_block

        # Handle Q&A grouping
        if section_name == "Q&A":
            qa_group = chunk.get("qa_group_id")
            if qa_group:
                if note_gaps and last_qa_group and qa_group - last_qa_group > 1:
                    gap_size = qa_group - last_qa_group - 1
                    formatted += (
                        f"*[Gap: {gap_size} Q&A exchange{'s' if gap_size > 1 else ''} omitted]*\n\n"
                    )

                if qa_group != last_qa_group:
                    formatted += f"\n### Q&A Exchange {qa_group}\n\n"
                    last_qa_group = qa_group

        # Add content
        formatted += f"{chunk.get('content', '')}\n\n"

    return formatted


async def generate_research_statement(
    formatted_content: str,
    combo: Dict[str, Any],
    context: Dict[str, Any],
    custom_prompt: str = None,
) -> str:
    """
    Generate a detailed research statement synthesizing the retrieved content.

    Args:
        formatted_content: Formatted transcript content (full, no truncation)
        combo: Bank-period combination
        context: Execution context
        custom_prompt: Optional custom prompt to override default synthesis behavior (for ETL)

    Returns:
        Detailed synthesized research statement with header
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    # Load research synthesis prompts from database with global contexts
    from ....utils.prompt_loader import load_prompt_from_db

    try:
        synthesis_prompts = load_prompt_from_db(
            layer="transcripts",
            name="research_synthesis",
            compose_with_globals=True,
            available_databases=None,  # Transcripts doesn't filter databases
            execution_id=execution_id
        )
    except Exception as e:
        logger.error(f"Failed to load research_synthesis prompt from database: {e}")
        raise RuntimeError(
            f"Critical error: Could not load research synthesis prompts from database: {e}"
        )

    # Use composed prompt if available (includes fiscal, project, restrictions globals)
    if "composed_prompt" in synthesis_prompts:
        system_prompt = synthesis_prompts["composed_prompt"]
    else:
        system_prompt = synthesis_prompts["system_prompt"]

    # Build user prompt based on mode
    if custom_prompt:
        # ETL mode: custom extraction instructions (override template)
        user_prompt = custom_prompt
        logger.info(
            "subagent.transcripts.using_custom_prompt",
            execution_id=execution_id,
            bank=combo["bank_symbol"],
            custom_prompt_length=len(custom_prompt),
        )
    else:
        # Standard conversational mode: use template from YAML
        query_intent = combo.get("query_intent", "General analysis")
        user_prompt = synthesis_prompts["user_prompt"].format(
            bank_name=combo["bank_name"],
            bank_symbol=combo["bank_symbol"],
            quarter=combo["quarter"],
            fiscal_year=combo["fiscal_year"],
            query_intent=query_intent,
            formatted_content=formatted_content,
        )

    try:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        # Use medium model for research synthesis (balance cost/performance)
        model_config = getattr(config.llm, "medium")
        response = await complete(
            messages=messages,
            context=context,
            llm_params={
                "model": model_config.model
                # Use default temperature and max_tokens from config
            },
        )

        research_text = response["choices"][0]["message"]["content"].strip()

        # Format with header
        return f"""### {combo['bank_name']} - {combo['quarter']} {combo['fiscal_year']}

{research_text}

---
"""

    except Exception as e:
        logger.error(
            "subagent.transcripts.research_generation_error",
            execution_id=execution_id,
            error=str(e),
        )

        # Fallback to simple summary
        intent = combo.get("query_intent", "the requested information")
        bank_name = combo["bank_name"]
        quarter = combo["quarter"]
        fiscal_year = combo["fiscal_year"]
        return f"""### {bank_name} - {quarter} {fiscal_year}

Transcript data retrieved for {bank_name} covering {quarter} {fiscal_year}.
The content addresses {intent}.

---
"""


def format_priority_blocks_for_method_selection(blocks: List[Dict[str, Any]]) -> str:
    """
    Format priority blocks for method selection prompt.

    Shows detailed information to help LLM decide which retrieval method to use:
    - Section name (MD vs QA)
    - Block/Group ID
    - Categories with IDs and names
    - Block summary
    - Similarity score
    - Full expanded content

    Args:
        blocks: List of priority blocks from get_priority_blocks()

    Returns:
        Formatted string for inclusion in method selection prompt
    """
    if not blocks:
        return "No priority blocks available."

    formatted = ""

    for i, block in enumerate(blocks, 1):
        block_id = block.get("block_id", "N/A")
        block_type = block.get("block_type", "unknown")
        section = block.get("section_name", "Unknown")
        similarity = block.get("similarity_score", 0.0)
        summary = block.get("block_summary", "No summary available")
        content = block.get("full_content", "No content available")

        # Format categories
        cat_ids = block.get("classification_ids", [])
        cat_names = block.get("classification_names", [])

        if cat_ids and cat_names:
            categories_str = ", ".join(
                [f"{cat_id}: {cat_name}" for cat_id, cat_name in zip(cat_ids, cat_names)]
            )
        else:
            categories_str = "No categories"

        # Build block entry
        formatted += f"""
**Priority Block {i}** (Similarity: {similarity:.3f})
- Type: {block_type.replace('_', ' ').title()}
- Block ID: {block_id}
- Section: {section}
- Categories: [{categories_str}]
- Summary: {summary}

Content:
{content}

---
"""

    return formatted


def format_priority_blocks_for_synthesis(blocks: List[Dict[str, Any]]) -> str:
    """
    Format priority blocks for prepending to Method 0 or Method 1 content.

    Provides clear section with explanation of purpose and potential duplication.

    Args:
        blocks: List of priority blocks from get_priority_blocks()

    Returns:
        Formatted string for prepending to retrieved content
    """
    if not blocks:
        return ""

    formatted = """## 🔥 PRIORITY CONTENT (Highest Relevance Blocks)

The blocks below were identified as most relevant to your query via similarity search.
They may also appear in the full content below. Focus your synthesis on these priority blocks
while using the full content for additional context.

---

"""

    for i, block in enumerate(blocks, 1):
        block_id = block.get("block_id", "N/A")
        section = block.get("section_name", "Unknown")
        similarity = block.get("similarity_score", 0.0)
        summary = block.get("block_summary", "")
        content = block.get("full_content", "")
        # Format categories for user visibility
        cat_ids = block.get("classification_ids", [])
        cat_names = block.get("classification_names", [])

        if cat_ids and cat_names:
            categories_str = ", ".join(cat_names)
        else:
            categories_str = "Uncategorized"

        # Build block entry
        formatted += f"""### Priority Block {i} | Relevance: {similarity:.1%}

**Metadata:**
- Section: {section}
- Block ID: {block_id}
- Categories: {categories_str}
"""

        if summary:
            formatted += f"- Summary: {summary}\n"

        formatted += f"""
**Content:**
{content}

---

"""

    formatted += "\n## FULL TRANSCRIPT CONTENT\n\n"

    return formatted
