#!/usr/bin/env python3
"""
Test the enhanced transcripts subagent with full formatting pipeline.
"""

import json
from aegis.utils.settings import config
from aegis.connections.oauth_connector import setup_authentication
from aegis.utils.ssl import setup_ssl

# Import the formatting utilities
from aegis.model.subagents.transcripts.formatting import (
    format_full_section_chunks,
    format_category_or_similarity_chunks,
    rerank_similarity_chunks,
    expand_speaker_blocks,
    fill_gaps_in_speaker_blocks,
    generate_research_statement
)

# Import retrieval functions
from aegis.model.subagents.transcripts.main import (
    retrieve_full_section,
    retrieve_by_categories,
    retrieve_by_similarity
)

def test_formatting_pipeline():
    """Test all formatting stages."""
    
    # Setup SSL and authentication
    ssl_config = setup_ssl()
    auth_config = setup_authentication("test-formatting", ssl_config)
    
    if not auth_config.get("success"):
        print(f"❌ Authentication failed: {auth_config.get('error')}")
        return
    
    # Create context
    context = {
        "execution_id": "test-formatting",
        "auth_config": auth_config,
        "ssl_config": ssl_config
    }
    
    print("=" * 80)
    print("TESTING FORMATTING PIPELINE")
    print("=" * 80)
    
    # Test combo
    combo = {
        "bank_id": 1,
        "bank_name": "Royal Bank of Canada",
        "bank_symbol": "RY",
        "fiscal_year": 2025,
        "quarter": "Q1",
        "query_intent": "Analyze financial performance"
    }
    
    # ========================================
    # TEST 1: Full Section Formatting
    # ========================================
    print("\n📋 TEST 1: Full Section Retrieval and Formatting")
    print("-" * 60)
    
    # Retrieve full Q&A section
    chunks = retrieve_full_section(combo, "QA", context)
    print(f"Retrieved {len(chunks)} chunks from Q&A section")
    
    # Add mock title
    for chunk in chunks:
        chunk["title"] = "RBC Q1 2025 Earnings Call Transcript"
    
    # Format the chunks
    formatted = format_full_section_chunks(chunks, combo, context)
    
    # Check formatting elements
    if "Institution ID: 1" in formatted:
        print("✅ Institution details included")
    if "Section" in formatted:
        print("✅ Section headers included")
    if "Q&A Exchange" in formatted:
        print("✅ Q&A grouping included")
    
    print(f"\nFormatted output preview (first 500 chars):")
    print(formatted[:500] + "...")
    
    # ========================================
    # TEST 2: Category-based with Gaps
    # ========================================
    print("\n\n📋 TEST 2: Category-based Retrieval with Gap Notation")
    print("-" * 60)
    
    # Retrieve by categories (will have gaps)
    chunks = retrieve_by_categories(combo, [10], context)  # Just Expenses category
    print(f"Retrieved {len(chunks)} chunks for category")
    
    # Add title
    for chunk in chunks:
        chunk["title"] = "RBC Q1 2025 Earnings Call Transcript"
    
    # Format with gap notation
    formatted = format_category_or_similarity_chunks(chunks, combo, context, note_gaps=True)
    
    if "[Gap:" in formatted:
        print("✅ Gap notation included")
    else:
        print("⚠️ No gaps detected (may be continuous)")
    
    # ========================================
    # TEST 3: Similarity Search Full Pipeline
    # ========================================
    print("\n\n📋 TEST 3: Similarity Search with Reranking, Expansion, Gap Filling")
    print("-" * 60)
    
    search_phrase = "net interest margin performance"
    
    # Step 1: Initial retrieval
    chunks = retrieve_by_similarity(combo, search_phrase, context, top_k=10)
    print(f"Step 1: Retrieved top {len(chunks)} similar chunks")
    
    # Step 2: Reranking
    original_count = len(chunks)
    chunks = rerank_similarity_chunks(chunks, search_phrase, context)
    print(f"Step 2: After reranking: {len(chunks)} chunks (filtered {original_count - len(chunks)} irrelevant)")
    
    # Step 3: Expand speaker blocks
    pre_expansion = len(chunks)
    chunks = expand_speaker_blocks(chunks, combo, context)
    print(f"Step 3: After expansion: {len(chunks)} chunks (expanded from {pre_expansion})")
    
    # Step 4: Fill gaps
    pre_gap_fill = len(chunks)
    chunks = fill_gaps_in_speaker_blocks(chunks, combo, context)
    print(f"Step 4: After gap filling: {len(chunks)} chunks (added {len(chunks) - pre_gap_fill} gap chunks)")
    
    # Add title
    for chunk in chunks:
        chunk["title"] = "RBC Q1 2025 Earnings Call Transcript"
    
    # Final formatting
    formatted = format_category_or_similarity_chunks(chunks, combo, context, note_gaps=True)
    
    # ========================================
    # TEST 4: Research Statement Generation
    # ========================================
    print("\n\n📋 TEST 4: Research Statement Generation")
    print("-" * 60)
    
    # Generate research statement from formatted content
    research_statement = generate_research_statement(formatted, combo, context)
    
    print("Generated Research Statement:")
    print(research_statement)
    
    if f"{combo['bank_name']} - {combo['quarter']} {combo['fiscal_year']}" in research_statement:
        print("✅ Research statement has proper header")
    
    # ========================================
    # TEST 5: Parallel Processing Simulation
    # ========================================
    print("\n\n📋 TEST 5: Multiple Bank-Period Combinations")
    print("-" * 60)
    
    combos = [
        {
            "bank_id": 1,
            "bank_name": "Royal Bank of Canada",
            "bank_symbol": "RY",
            "fiscal_year": 2025,
            "quarter": "Q1",
            "query_intent": "Revenue analysis"
        },
        {
            "bank_id": 2,
            "bank_name": "Toronto-Dominion Bank",
            "bank_symbol": "TD",
            "fiscal_year": 2025,
            "quarter": "Q2",
            "query_intent": "Revenue analysis"
        }
    ]
    
    research_statements = []
    for combo in combos:
        # Simulate processing each combo
        chunks = retrieve_by_categories(combo, [9], context)  # Revenue category
        
        # Add title
        for chunk in chunks:
            chunk["title"] = f"{combo['bank_name']} {combo['quarter']} {combo['fiscal_year']} Earnings Call"
        
        formatted = format_category_or_similarity_chunks(chunks, combo, context)
        research = generate_research_statement(formatted, combo, context)
        research_statements.append(research)
        print(f"✅ Generated research for {combo['bank_symbol']} {combo['quarter']} {combo['fiscal_year']}")
    
    # Merge research statements
    print("\n\n📊 MERGED RESEARCH OUTPUT")
    print("=" * 60)
    for statement in research_statements:
        print(statement)
    
    print("\n" + "=" * 80)
    print("✅ FORMATTING PIPELINE TESTING COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    test_formatting_pipeline()