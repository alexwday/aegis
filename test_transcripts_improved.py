#!/usr/bin/env python3
"""
Test script for the improved transcripts subagent.
Tests pattern matching for QA and MD section retrieval.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import uuid
from typing import Dict, List, Any
from datetime import datetime, timezone

# Import the transcripts agent
from aegis.model.subagents.transcripts.main import transcripts_agent

# Import utilities for testing
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.utils.logging import setup_logging, get_logger

# Setup logging
setup_logging()
logger = get_logger()


def create_test_context() -> Dict[str, Any]:
    """Create test context with auth and SSL config."""
    execution_id = str(uuid.uuid4())
    ssl_config = setup_ssl()
    auth_config = setup_authentication()
    
    return {
        "execution_id": execution_id,
        "ssl_config": ssl_config,
        "auth_config": auth_config
    }


def test_qa_pattern_matching():
    """Test that questions about investor questions trigger QA section retrieval."""
    print("\n" + "="*80)
    print("TEST: QA Pattern Matching")
    print("="*80)
    
    # Test queries that should trigger QA section retrieval
    test_queries = [
        "What are investors asking about?",
        "What questions were asked during the call?",
        "What concerns did analysts raise?",
        "Show me the Q&A section",
        "What did analysts inquire about?",
        "How did management respond to questions?"
    ]
    
    # Test bank-period combinations
    bank_period_combinations = [
        {
            "bank_id": 1,
            "bank_name": "Royal Bank of Canada",
            "bank_symbol": "RY",
            "fiscal_year": 2024,
            "quarter": "Q3"
        }
    ]
    
    context = create_test_context()
    
    for query in test_queries:
        print(f"\nTesting query: '{query}'")
        print("-" * 40)
        
        conversation = [{"role": "user", "content": query}]
        
        # Run the agent
        output_chunks = []
        for chunk in transcripts_agent(
            conversation=conversation,
            latest_message=query,
            bank_period_combinations=bank_period_combinations,
            basic_intent=f"User wants to know: {query}",
            full_intent=f"Retrieve information about: {query}",
            database_id="transcripts",
            context=context
        ):
            output_chunks.append(chunk["content"])
        
        # Check if output mentions QA section
        full_output = "".join(output_chunks)
        if "Q&A" in full_output or "investor" in full_output.lower() or "analyst" in full_output.lower():
            print("✅ Successfully retrieved QA-related content")
        else:
            print("⚠️ May not have retrieved QA section - check logs")
        
        # Show first 500 chars of output
        print(f"Output preview: {full_output[:500]}...")


def test_md_pattern_matching():
    """Test that questions about management commentary trigger MD section retrieval."""
    print("\n" + "="*80)
    print("TEST: MD Pattern Matching")
    print("="*80)
    
    # Test queries that should trigger MD section retrieval
    test_queries = [
        "What did management say about the outlook?",
        "What was the CEO's commentary?",
        "Show me management discussion",
        "What were the prepared remarks?",
        "What did executives say about performance?",
        "What was leadership's view on the quarter?"
    ]
    
    # Test bank-period combinations
    bank_period_combinations = [
        {
            "bank_id": 1,
            "bank_name": "Royal Bank of Canada",
            "bank_symbol": "RY",
            "fiscal_year": 2024,
            "quarter": "Q3"
        }
    ]
    
    context = create_test_context()
    
    for query in test_queries:
        print(f"\nTesting query: '{query}'")
        print("-" * 40)
        
        conversation = [{"role": "user", "content": query}]
        
        # Run the agent
        output_chunks = []
        for chunk in transcripts_agent(
            conversation=conversation,
            latest_message=query,
            bank_period_combinations=bank_period_combinations,
            basic_intent=f"User wants to know: {query}",
            full_intent=f"Retrieve information about: {query}",
            database_id="transcripts",
            context=context
        ):
            output_chunks.append(chunk["content"])
        
        # Check if output mentions MD section
        full_output = "".join(output_chunks)
        if "management" in full_output.lower() or "CEO" in full_output or "executive" in full_output.lower():
            print("✅ Successfully retrieved MD-related content")
        else:
            print("⚠️ May not have retrieved MD section - check logs")
        
        # Show first 500 chars of output
        print(f"Output preview: {full_output[:500]}...")


def test_specific_query():
    """Test a specific query that should use similarity search."""
    print("\n" + "="*80)
    print("TEST: Specific Query (Similarity Search)")
    print("="*80)
    
    # Test query that's very specific and should use similarity search
    test_queries = [
        "What was the net interest margin trend specifically?",
        "How much did technology spending increase?",
        "What was the exact loan loss provision amount?"
    ]
    
    # Test bank-period combinations
    bank_period_combinations = [
        {
            "bank_id": 1,
            "bank_name": "Royal Bank of Canada",
            "bank_symbol": "RY",
            "fiscal_year": 2024,
            "quarter": "Q3"
        }
    ]
    
    context = create_test_context()
    
    for query in test_queries:
        print(f"\nTesting query: '{query}'")
        print("-" * 40)
        
        conversation = [{"role": "user", "content": query}]
        
        # Run the agent
        output_chunks = []
        for chunk in transcripts_agent(
            conversation=conversation,
            latest_message=query,
            bank_period_combinations=bank_period_combinations,
            basic_intent=f"User wants specific information: {query}",
            full_intent=f"Find specific data about: {query}",
            database_id="transcripts",
            context=context
        ):
            output_chunks.append(chunk["content"])
        
        full_output = "".join(output_chunks)
        print(f"Output preview: {full_output[:500]}...")


def test_multiple_banks():
    """Test with multiple bank-period combinations."""
    print("\n" + "="*80)
    print("TEST: Multiple Banks")
    print("="*80)
    
    query = "What questions did analysts ask about credit quality?"
    
    # Multiple bank-period combinations
    bank_period_combinations = [
        {
            "bank_id": 1,
            "bank_name": "Royal Bank of Canada",
            "bank_symbol": "RY",
            "fiscal_year": 2024,
            "quarter": "Q3"
        },
        {
            "bank_id": 2,
            "bank_name": "TD Bank",
            "bank_symbol": "TD",
            "fiscal_year": 2024,
            "quarter": "Q3"
        }
    ]
    
    context = create_test_context()
    
    print(f"\nTesting query: '{query}'")
    print(f"Banks: {', '.join([combo['bank_symbol'] for combo in bank_period_combinations])}")
    print("-" * 40)
    
    conversation = [{"role": "user", "content": query}]
    
    # Run the agent
    output_chunks = []
    for chunk in transcripts_agent(
        conversation=conversation,
        latest_message=query,
        bank_period_combinations=bank_period_combinations,
        basic_intent="Analyst questions about credit quality",
        full_intent="Retrieve analyst questions about credit quality from earnings calls",
        database_id="transcripts",
        context=context
    ):
        output_chunks.append(chunk["content"])
    
    full_output = "".join(output_chunks)
    
    # Check if both banks are mentioned
    for combo in bank_period_combinations:
        if combo['bank_name'] in full_output or combo['bank_symbol'] in full_output:
            print(f"✅ Found content for {combo['bank_symbol']}")
        else:
            print(f"⚠️ Missing content for {combo['bank_symbol']}")
    
    print(f"\nOutput length: {len(full_output)} characters")


def main():
    """Run all tests."""
    print("\n" + "="*80)
    print("TRANSCRIPT SUBAGENT IMPROVEMENT TESTS")
    print("Testing enhanced pattern matching for QA and MD sections")
    print("="*80)
    
    try:
        # Test QA pattern matching
        test_qa_pattern_matching()
        
        # Test MD pattern matching
        test_md_pattern_matching()
        
        # Test specific queries
        test_specific_query()
        
        # Test multiple banks
        test_multiple_banks()
        
        print("\n" + "="*80)
        print("✅ All tests completed successfully!")
        print("Check the logs for detailed retrieval method decisions.")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        logger.error("Test failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()