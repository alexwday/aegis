#!/usr/bin/env python3
"""
Verification script to ensure no truncation occurs in transcript retrieval.
Tests that full Q&A sections are retrieved and displayed without any truncation.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import uuid
from typing import Dict, Any
from aegis.model.subagents.transcripts.main import transcripts_agent
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.utils.logging import setup_logging, get_logger

# Setup logging
setup_logging()
logger = get_logger()


def test_full_qa_retrieval():
    """Test that full Q&A sections are retrieved without truncation."""
    print("\n" + "="*80)
    print("VERIFICATION: Full Q&A Section Retrieval (No Truncation)")
    print("="*80)
    
    # Create context
    execution_id = str(uuid.uuid4())
    ssl_config = setup_ssl()
    auth_config = setup_authentication()
    context = {
        "execution_id": execution_id,
        "ssl_config": ssl_config,
        "auth_config": auth_config
    }
    
    # Test query that should retrieve full Q&A section
    query = "What questions did analysts ask?"
    
    # Bank-period combination
    bank_period_combinations = [{
        "bank_id": 1,
        "bank_name": "Royal Bank of Canada",
        "bank_symbol": "RY",
        "fiscal_year": 2024,
        "quarter": "Q3",
        "query_intent": "Retrieve all analyst questions from the Q&A section"
    }]
    
    conversation = [{"role": "user", "content": query}]
    
    print(f"\nQuery: '{query}'")
    print(f"Bank: {bank_period_combinations[0]['bank_symbol']} {bank_period_combinations[0]['quarter']} {bank_period_combinations[0]['fiscal_year']}")
    print("-" * 40)
    
    # Collect full output
    full_output = ""
    chunk_count = 0
    
    for chunk in transcripts_agent(
        conversation=conversation,
        latest_message=query,
        bank_period_combinations=bank_period_combinations,
        basic_intent="Analyst questions from Q&A",
        full_intent="Retrieve all analyst questions and management responses from the Q&A section",
        database_id="transcripts",
        context=context
    ):
        chunk_content = chunk.get("content", "")
        full_output += chunk_content
        chunk_count += 1
    
    # Analyze output
    print(f"\n📊 Output Statistics:")
    print(f"- Total output length: {len(full_output):,} characters")
    print(f"- Number of chunks: {chunk_count}")
    
    # Count Q&A exchanges
    qa_count = full_output.count("Question ")
    exchange_count = full_output.count("Q&A Group ")
    
    print(f"- Questions found in output: {qa_count}")
    print(f"- Q&A Groups found in output: {exchange_count}")
    
    # Check for truncation indicators
    truncation_indicators = [
        "...",  # Ellipsis might indicate truncation
        "[truncated]",
        "[omitted]",
        "summary",  # Should not see "summary" for full section retrieval
        "[Gap:"  # Gap notation should only appear in similarity search
    ]
    
    print(f"\n🔍 Checking for truncation indicators:")
    issues_found = False
    for indicator in truncation_indicators:
        if indicator in full_output.lower():
            print(f"  ⚠️  Found '{indicator}' in output - may indicate truncation")
            issues_found = True
    
    if not issues_found:
        print("  ✅ No truncation indicators found")
    
    # Display first and last parts to verify completeness
    print(f"\n📝 Output Preview:")
    print("First 500 characters:")
    print("-" * 40)
    print(full_output[:500])
    print("-" * 40)
    print("\nLast 500 characters:")
    print("-" * 40)
    print(full_output[-500:])
    print("-" * 40)
    
    # Check if it appears to be complete transcript content
    if len(full_output) < 1000:
        print("\n⚠️  WARNING: Output seems too short for a full Q&A section")
    elif qa_count < 2:
        print("\n⚠️  WARNING: Very few questions found - may not be retrieving full section")
    else:
        print("\n✅ Output appears to contain full Q&A section without truncation")
    
    return full_output


def test_full_md_retrieval():
    """Test that full MD sections are retrieved without truncation."""
    print("\n" + "="*80)
    print("VERIFICATION: Full MD Section Retrieval (No Truncation)")
    print("="*80)
    
    # Create context
    execution_id = str(uuid.uuid4())
    ssl_config = setup_ssl()
    auth_config = setup_authentication()
    context = {
        "execution_id": execution_id,
        "ssl_config": ssl_config,
        "auth_config": auth_config
    }
    
    # Test query that should retrieve full MD section
    query = "What did management say about the quarter?"
    
    # Bank-period combination
    bank_period_combinations = [{
        "bank_id": 1,
        "bank_name": "Royal Bank of Canada",
        "bank_symbol": "RY",
        "fiscal_year": 2024,
        "quarter": "Q3",
        "query_intent": "Retrieve management discussion section"
    }]
    
    conversation = [{"role": "user", "content": query}]
    
    print(f"\nQuery: '{query}'")
    print(f"Bank: {bank_period_combinations[0]['bank_symbol']} {bank_period_combinations[0]['quarter']} {bank_period_combinations[0]['fiscal_year']}")
    print("-" * 40)
    
    # Collect full output
    full_output = ""
    
    for chunk in transcripts_agent(
        conversation=conversation,
        latest_message=query,
        bank_period_combinations=bank_period_combinations,
        basic_intent="Management discussion",
        full_intent="Retrieve full management discussion section",
        database_id="transcripts",
        context=context
    ):
        full_output += chunk.get("content", "")
    
    # Analyze output
    print(f"\n📊 Output Statistics:")
    print(f"- Total output length: {len(full_output):,} characters")
    
    if "MANAGEMENT DISCUSSION" in full_output:
        print("✅ Found MANAGEMENT DISCUSSION section")
    else:
        print("⚠️  MANAGEMENT DISCUSSION section header not found")
    
    return full_output


if __name__ == "__main__":
    print("\n" + "="*80)
    print("NO-TRUNCATION VERIFICATION TEST")
    print("Ensuring full transcript sections are retrieved without any truncation")
    print("="*80)
    
    try:
        # Test Q&A retrieval
        qa_output = test_full_qa_retrieval()
        
        # Test MD retrieval
        md_output = test_full_md_retrieval()
        
        print("\n" + "="*80)
        print("VERIFICATION COMPLETE")
        print("="*80)
        print("\n✅ Tests completed. Review the output statistics above.")
        print("   Full sections should be retrieved with NO truncation.")
        print("   All Q&A exchanges should be included in the output.")
        
    except Exception as e:
        print(f"\n❌ Verification failed: {e}")
        logger.error("Verification failed", error=str(e), exc_info=True)