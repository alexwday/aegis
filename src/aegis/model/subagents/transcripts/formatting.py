"""
Formatting and post-processing utilities for transcript data.

Handles:
- Chunk formatting by section and speaker blocks
- Reranking and filtering for similarity search
- Block expansion and gap filling
- Research statement generation
"""

from typing import List, Dict, Any, Set, Tuple
from datetime import datetime, timezone
import logging

from ....connections.postgres_connector import get_connection
from ....connections.llm_connector import complete
from ....utils.logging import get_logger
from ....utils.settings import config
from sqlalchemy import text


def format_full_section_chunks(
    chunks: List[Dict[str, Any]], 
    combo: Dict[str, Any],
    context: Dict[str, Any]
) -> str:
    """
    Format chunks retrieved via Method 0 (Full Section).
    Preserves all content without truncation.
    
    Args:
        chunks: Retrieved transcript chunks
        combo: Bank-period combination
        context: Execution context
        
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
        if chunk.get('section_name') == 'Q&A':
            # Q&A chunks must have qa_group_id
            if chunk.get('qa_group_id') is not None:
                valid_chunks.append(chunk)
            else:
                logger.warning(f"Removed Q&A chunk missing qa_group_id: {chunk.get('id')}")
        elif chunk.get('section_name') == 'MANAGEMENT DISCUSSION SECTION':
            # MD chunks must have speaker_block_id
            if chunk.get('speaker_block_id') is not None:
                valid_chunks.append(chunk)
            else:
                logger.warning(f"Removed MD chunk missing speaker_block_id: {chunk.get('id')}")
        else:
            # Keep other chunks if they have either ID
            if chunk.get('speaker_block_id') is not None or chunk.get('qa_group_id') is not None:
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
    
    # Sort chunks by section, then by speaker_block_id for MD or qa_group_id for Q&A, then chunk_id
    sorted_chunks = sorted(chunks, key=lambda x: (
        0 if x.get('section_name') == 'MANAGEMENT DISCUSSION SECTION' else 1,
        x.get('qa_group_id', 0) if x.get('section_name') == 'Q&A' else x.get('speaker_block_id', 0),
        x.get('chunk_id', 0)
    ))
    
    # Group by section
    current_section = None
    current_qa_group = None
    section_num = 0
    qa_count = 0
    
    # Count total Q&A groups for logging
    qa_groups = set(chunk.get('qa_group_id') for chunk in sorted_chunks 
                    if chunk.get('section_name') == 'Q&A' and chunk.get('qa_group_id'))
    if qa_groups:
        logger.info(
            f"Formatting {len(qa_groups)} Q&A exchanges",
            execution_id=execution_id,
            qa_group_ids=sorted(qa_groups)
        )
    
    for chunk in sorted_chunks:
        section_name = chunk.get('section_name', 'Unknown')
        
        # New section
        if section_name != current_section:
            section_num += 1
            current_section = section_name
            formatted += f"\n## Section {section_num}: {section_name}\n\n"
            current_qa_group = None
            qa_count = 0
        
        # Handle Q&A grouping
        if section_name == "Q&A" and chunk.get('qa_group_id'):
            if chunk['qa_group_id'] != current_qa_group:
                current_qa_group = chunk['qa_group_id']
                qa_count += 1
                formatted += f"\n### Question {qa_count} (Q&A Group {current_qa_group})\n\n"
        
        # Add content with speaker info if available
        speaker = chunk.get('speaker', '')
        if speaker:
            formatted += f"**{speaker}**\n"
        
        # Include full content without truncation
        content = chunk.get('content', '')
        if content:
            formatted += f"{content}\n\n"
    
    return formatted


def rerank_similarity_chunks(
    chunks: List[Dict[str, Any]],
    search_phrase: str,
    context: Dict[str, Any]
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
    
    # Build prompt for reranking
    chunk_summaries = []
    for i, chunk in enumerate(chunks):
        # Use block_summary if available, otherwise use first 200 chars of content for reranking decision only
        summary = chunk.get('block_summary', chunk.get('content', '')[:200] if chunk.get('content', '') else '')
        chunk_summaries.append(f"{i}: {summary}")
    
    rerank_prompt = f"""You are analyzing transcript chunks for relevance to a user query.

User Query: {search_phrase}

Below are {len(chunks)} chunk summaries with their index numbers. 
Return ONLY a JSON array of index numbers for chunks that are COMPLETELY IRRELEVANT to the query.
Only include chunks that have absolutely no relation to what the user is asking about.

Chunk Summaries:
{chr(10).join(chunk_summaries)}

Return format: [0, 3, 7] (example of irrelevant chunk indices)
If all chunks are relevant, return: []"""
    
    try:
        messages = [{"role": "user", "content": rerank_prompt}]
        # Use large model for better reranking
        model_config = getattr(config.llm, "large")
        response = complete(
            messages=messages,
            context=context,
            llm_params={
                "model": model_config.model
                # Use defaults from config
            }
        )
        
        # Parse response to get irrelevant indices
        response_text = response['choices'][0]['message']['content'].strip()
        
        # Extract JSON array from response
        import json
        import re
        
        # Try to find JSON array in response
        json_match = re.search(r'\[[\d,\s]*\]', response_text)
        if json_match:
            irrelevant_indices = json.loads(json_match.group())
            
            # Filter out irrelevant chunks
            relevant_chunks = [
                chunk for i, chunk in enumerate(chunks) 
                if i not in irrelevant_indices
            ]
            
            logger.info(
                f"subagent.transcripts.reranking",
                execution_id=execution_id,
                original_count=len(chunks),
                filtered_count=len(irrelevant_indices),
                remaining_count=len(relevant_chunks)
            )
            
            return relevant_chunks
        
    except Exception as e:
        logger.error(
            f"subagent.transcripts.reranking_error",
            execution_id=execution_id,
            error=str(e)
        )
    
    # If reranking fails, return original chunks
    return chunks


def expand_speaker_blocks(
    chunks: List[Dict[str, Any]],
    combo: Dict[str, Any],
    context: Dict[str, Any]
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
    md_chunks = [c for c in chunks if c.get('section_name') == 'MANAGEMENT DISCUSSION SECTION']
    qa_chunks = [c for c in chunks if c.get('section_name') == 'Q&A']
    
    # Q&A chunks don't need expansion (already complete)
    expanded_chunks = qa_chunks.copy()
    
    if md_chunks:
        # Get unique speaker block IDs from MD section
        speaker_block_ids = set()
        for chunk in md_chunks:
            if chunk.get('speaker_block_id'):
                speaker_block_ids.add(chunk['speaker_block_id'])
        
        if speaker_block_ids:
            # Fetch all chunks for these speaker blocks
            try:
                with get_connection() as conn:
                    query = text("""
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
                    """)
                    
                    result = conn.execute(query, {
                        "bank_id_str": str(combo["bank_id"]),
                        "fiscal_year": combo["fiscal_year"],
                        "quarter": combo["quarter"],
                        "block_ids": list(speaker_block_ids)
                    })
                    
                    for row in result:
                        expanded_chunks.append({
                            "id": row[0],
                            "section_name": row[1],
                            "speaker_block_id": row[2],
                            "qa_group_id": row[3],
                            "chunk_id": row[4],
                            "content": row[5],
                            "block_summary": row[6],
                            "classification_ids": row[7],
                            "classification_names": row[8],
                            "title": row[9]
                        })
                    
                    logger.info(
                        f"subagent.transcripts.block_expansion",
                        execution_id=execution_id,
                        original_blocks=len(speaker_block_ids),
                        expanded_chunks=len([c for c in expanded_chunks if c.get('section_name') == 'MANAGEMENT DISCUSSION SECTION'])
                    )
                    
            except Exception as e:
                logger.error(
                    f"subagent.transcripts.expansion_error",
                    execution_id=execution_id,
                    error=str(e)
                )
                # Return original chunks if expansion fails
                return chunks
    
    return expanded_chunks


def fill_gaps_in_speaker_blocks(
    chunks: List[Dict[str, Any]],
    combo: Dict[str, Any],
    context: Dict[str, Any]
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
        if chunk.get('section_name') == 'MANAGEMENT DISCUSSION SECTION' and chunk.get('speaker_block_id'):
            md_block_ids.add(chunk['speaker_block_id'])
    
    if not md_block_ids:
        return chunks
    
    # Find gaps of 1
    sorted_ids = sorted(md_block_ids)
    gaps_to_fill = []
    
    for i in range(len(sorted_ids) - 1):
        if sorted_ids[i+1] - sorted_ids[i] == 2:
            # Gap of 1 found
            gap_id = sorted_ids[i] + 1
            gaps_to_fill.append(gap_id)
    
    if gaps_to_fill:
        logger.info(
            f"subagent.transcripts.gap_filling",
            execution_id=execution_id,
            gaps_found=len(gaps_to_fill),
            gap_ids=gaps_to_fill
        )
        
        # Fetch gap chunks
        try:
            with get_connection() as conn:
                query = text("""
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
                """)
                
                result = conn.execute(query, {
                    "bank_id_str": str(combo["bank_id"]),
                    "fiscal_year": combo["fiscal_year"],
                    "quarter": combo["quarter"],
                    "block_ids": gaps_to_fill
                })
                
                gap_chunks = []
                for row in result:
                    gap_chunks.append({
                        "id": row[0],
                        "section_name": row[1],
                        "speaker_block_id": row[2],
                        "qa_group_id": row[3],
                        "chunk_id": row[4],
                        "content": row[5],
                        "block_summary": row[6],
                        "classification_ids": row[7],
                        "classification_names": row[8],
                        "title": row[9]
                    })
                
                # Add gap chunks to result
                chunks.extend(gap_chunks)
                
        except Exception as e:
            logger.error(
                f"subagent.transcripts.gap_fill_error",
                execution_id=execution_id,
                error=str(e)
            )
    
    return chunks


def format_category_or_similarity_chunks(
    chunks: List[Dict[str, Any]],
    combo: Dict[str, Any],
    context: Dict[str, Any],
    note_gaps: bool = True
) -> str:
    """
    Format chunks for Method 1 (Category) or Method 2 (Similarity) with gap notation.
    
    Args:
        chunks: Retrieved/processed chunks
        combo: Bank-period combination
        context: Execution context
        note_gaps: Whether to note gaps in sequence
        
    Returns:
        Formatted transcript text
    """
    if not chunks:
        return "No transcript data available."
    
    logger = get_logger()
    execution_id = context.get("execution_id")
    
    # Filter out chunks missing proper IDs
    valid_chunks = []
    for chunk in chunks:
        if chunk.get('section_name') == 'Q&A':
            # Q&A chunks must have qa_group_id
            if chunk.get('qa_group_id') is not None:
                valid_chunks.append(chunk)
            else:
                logger.warning(f"Removed Q&A chunk missing qa_group_id: {chunk.get('id')}")
        elif chunk.get('section_name') == 'MANAGEMENT DISCUSSION SECTION':
            # MD chunks must have speaker_block_id
            if chunk.get('speaker_block_id') is not None:
                valid_chunks.append(chunk)
            else:
                logger.warning(f"Removed MD chunk missing speaker_block_id: {chunk.get('id')}")
        else:
            # Keep other chunks if they have either ID
            if chunk.get('speaker_block_id') is not None or chunk.get('qa_group_id') is not None:
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
    
    # Sort chunks by section, then by speaker_block_id for MD or qa_group_id for Q&A, then chunk_id
    sorted_chunks = sorted(chunks, key=lambda x: (
        0 if x.get('section_name') == 'MANAGEMENT DISCUSSION SECTION' else 1,
        x.get('qa_group_id', 0) if x.get('section_name') == 'Q&A' else x.get('speaker_block_id', 0),
        x.get('chunk_id', 0)
    ))
    
    # Group by section and track gaps
    current_section = None
    section_num = 0
    last_speaker_block = None
    last_qa_group = None
    
    for chunk in sorted_chunks:
        section_name = chunk.get('section_name', 'Unknown')
        
        # New section
        if section_name != current_section:
            section_num += 1
            current_section = section_name
            formatted += f"\n## Section {section_num}: {section_name}\n\n"
            last_speaker_block = None
            last_qa_group = None
        
        # Check for gaps in MD section
        if section_name == "MANAGEMENT DISCUSSION SECTION" and note_gaps:
            speaker_block = chunk.get('speaker_block_id')
            if speaker_block and last_speaker_block and speaker_block - last_speaker_block > 1:
                gap_size = speaker_block - last_speaker_block - 1
                formatted += f"*[Gap: {gap_size} speaker block{'s' if gap_size > 1 else ''} omitted]*\n\n"
            last_speaker_block = speaker_block
        
        # Handle Q&A grouping
        if section_name == "Q&A":
            qa_group = chunk.get('qa_group_id')
            if qa_group:
                if note_gaps and last_qa_group and qa_group - last_qa_group > 1:
                    gap_size = qa_group - last_qa_group - 1
                    formatted += f"*[Gap: {gap_size} Q&A exchange{'s' if gap_size > 1 else ''} omitted]*\n\n"
                
                if qa_group != last_qa_group:
                    formatted += f"\n### Q&A Exchange {qa_group}\n\n"
                    last_qa_group = qa_group
        
        # Add content
        speaker = chunk.get('speaker', '')
        if speaker:
            formatted += f"**{speaker}**\n"
        
        formatted += f"{chunk.get('content', '')}\n\n"
    
    return formatted


def generate_research_statement(
    formatted_content: str,
    combo: Dict[str, Any],
    context: Dict[str, Any],
    method: int = None,
    method_reasoning: str = None
) -> str:
    """
    Generate a detailed research statement synthesizing the retrieved content.
    
    Args:
        formatted_content: Formatted transcript content (full, no truncation)
        combo: Bank-period combination
        context: Execution context
        method: Retrieval method used (0=full section, 1=category, 2=similarity)
        method_reasoning: Explanation of why this method was chosen
        
    Returns:
        Detailed synthesized research statement with header
    """
    logger = get_logger()
    execution_id = context.get("execution_id")
    
    # Determine appropriate response style based on method
    if method == 0:
        # Full section retrieval - provide comprehensive synthesis
        response_style = """Provide a DETAILED and COMPREHENSIVE synthesis (3-5 paragraphs) that:
1. If Q&A section: Summarize ALL questions asked and key management responses
2. If MD section: Highlight ALL major points made by management
3. Include specific quotes and details from the transcript
4. Organize information thematically if there are multiple topics
5. Be thorough - this is a full section analysis, not a brief summary"""
    elif method == 1:
        # Category-based - focused on specific topics
        response_style = """Provide a focused synthesis (2-3 paragraphs) that:
1. Addresses the specific financial categories requested
2. Includes relevant quotes and data points
3. Connects related points across different speakers"""
    else:
        # Similarity search - targeted response
        response_style = """Provide a targeted synthesis (2-3 paragraphs) that:
1. Directly addresses the specific query
2. Includes the most relevant quotes and context
3. Notes if information is partial or incomplete"""
    
    # Build prompt for research statement
    prompt = f"""You are analyzing earnings transcript content. Your response MUST be based ONLY on the transcript chunks provided below.

Bank: {combo['bank_name']} ({combo['bank_symbol']})
Period: {combo['quarter']} {combo['fiscal_year']}
User Query Intent: {combo.get('query_intent', 'General analysis')}
Retrieval Method: {'Full Section' if method == 0 else 'Category-based' if method == 1 else 'Similarity Search'}
{f'Method Reasoning: {method_reasoning}' if method_reasoning else ''}

CRITICAL INSTRUCTIONS:
1. Use ONLY the specific transcript content provided below
2. Do NOT add any information not present in the chunks
3. Quote directly from the transcript when possible
4. Be comprehensive and detailed in your synthesis
5. Focus on answering the user's query completely

{response_style}

TRANSCRIPT CHUNKS PROVIDED:
{formatted_content}  # Full content, no truncation

Based ONLY on the above transcript chunks, provide your synthesis:"""
    
    try:
        messages = [{"role": "user", "content": prompt}]
        # Use large model for better research generation
        model_config = getattr(config.llm, "large")
        response = complete(
            messages=messages,
            context=context,
            llm_params={
                "model": model_config.model
                # Use default temperature and max_tokens from config
            }
        )
        
        research_text = response['choices'][0]['message']['content'].strip()
        
        # Format with header
        return f"""### {combo['bank_name']} - {combo['quarter']} {combo['fiscal_year']}

{research_text}

---
"""
        
    except Exception as e:
        logger.error(
            f"subagent.transcripts.research_generation_error",
            execution_id=execution_id,
            error=str(e)
        )
        
        # Fallback to simple summary
        return f"""### {combo['bank_name']} - {combo['quarter']} {combo['fiscal_year']}

Transcript data retrieved for {combo['bank_name']} covering {combo['quarter']} {combo['fiscal_year']}. 
The content addresses {combo.get('query_intent', 'the requested information')}.

---
"""