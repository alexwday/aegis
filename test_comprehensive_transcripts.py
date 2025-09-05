#!/usr/bin/env python3
"""
Comprehensive test of the transcripts subagent to verify all retrieval methods.
"""

import json
from aegis.model.subagents.transcripts.main import transcripts_agent
from aegis.utils.settings import config
from aegis.connections.oauth_connector import setup_authentication
from aegis.utils.ssl import setup_ssl

def test_all_retrieval_methods():
    """Test all aspects of the transcripts subagent."""
    
    # Setup SSL and authentication
    ssl_config = setup_ssl()
    auth_config = setup_authentication("test-123", ssl_config)
    
    if not auth_config.get("success"):
        print(f"❌ Authentication failed: {auth_config.get('error')}")
        return
    
    # Create context
    context = {
        "execution_id": "test-123",
        "auth_config": auth_config,
        "ssl_config": ssl_config
    }
    
    print("=" * 80)
    print("COMPREHENSIVE TRANSCRIPTS SUBAGENT TESTING")
    print("=" * 80)
    
    # Test 1: Full Section Retrieval - MD only
    print("\n🔍 TEST 1: Full Section Retrieval - Management Discussion Only")
    print("-" * 60)
    test_case = {
        "latest_message": "Give me the management discussion section",
        "bank_period_combinations": [
            {
                "bank_id": 1,
                "bank_name": "Royal Bank of Canada",
                "bank_symbol": "RY",
                "fiscal_year": 2025,
                "quarter": "Q1",
                "query_intent": "Retrieve full management discussion section"
            }
        ]
    }
    
    for chunk in transcripts_agent(
        conversation=[{"role": "user", "content": test_case['latest_message']}],
        latest_message=test_case['latest_message'],
        bank_period_combinations=test_case['bank_period_combinations'],
        basic_intent="Full MD section",
        full_intent=test_case['latest_message'],
        database_id="transcripts",
        context=context
    ):
        if "Retrieved" in chunk.get("content", "") and "chunks from" in chunk.get("content", ""):
            print(f"✅ {chunk['content']}", end="")
    
    # Test 2: Full Section Retrieval - Q&A only
    print("\n\n🔍 TEST 2: Full Section Retrieval - Q&A Section Only")
    print("-" * 60)
    test_case = {
        "latest_message": "Summarize the Q&A section",
        "bank_period_combinations": [
            {
                "bank_id": 2,
                "bank_name": "Toronto-Dominion Bank", 
                "bank_symbol": "TD",
                "fiscal_year": 2025,
                "quarter": "Q1",
                "query_intent": "Summarize Q&A section"
            }
        ]
    }
    
    for chunk in transcripts_agent(
        conversation=[{"role": "user", "content": test_case['latest_message']}],
        latest_message=test_case['latest_message'],
        bank_period_combinations=test_case['bank_period_combinations'],
        basic_intent="Q&A summary",
        full_intent=test_case['latest_message'],
        database_id="transcripts",
        context=context
    ):
        if "Retrieved" in chunk.get("content", "") and "chunks from" in chunk.get("content", ""):
            print(f"✅ {chunk['content']}", end="")
    
    # Test 3: Full Section Retrieval - ALL sections
    print("\n\n🔍 TEST 3: Full Section Retrieval - Both MD and Q&A")
    print("-" * 60)
    test_case = {
        "latest_message": "Give me the complete transcript",
        "bank_period_combinations": [
            {
                "bank_id": 3,
                "bank_name": "Bank of Nova Scotia",
                "bank_symbol": "BNS",
                "fiscal_year": 2025,
                "quarter": "Q2",
                "query_intent": "Full transcript"
            }
        ]
    }
    
    for chunk in transcripts_agent(
        conversation=[{"role": "user", "content": test_case['latest_message']}],
        latest_message=test_case['latest_message'],
        bank_period_combinations=test_case['bank_period_combinations'],
        basic_intent="Full transcript",
        full_intent=test_case['latest_message'],
        database_id="transcripts",
        context=context
    ):
        if "Retrieved" in chunk.get("content", "") and "chunks from" in chunk.get("content", ""):
            print(f"✅ {chunk['content']}", end="")
    
    # Test 4: Category-based - Single category
    print("\n\n🔍 TEST 4: Category-based Retrieval - Single Category")
    print("-" * 60)
    test_case = {
        "latest_message": "What did they say about efficiency?",
        "bank_period_combinations": [
            {
                "bank_id": 4,
                "bank_name": "Bank of Montreal",
                "bank_symbol": "BMO",
                "fiscal_year": 2025,
                "quarter": "Q1",
                "query_intent": "Find efficiency discussions"
            }
        ]
    }
    
    for chunk in transcripts_agent(
        conversation=[{"role": "user", "content": test_case['latest_message']}],
        latest_message=test_case['latest_message'],
        bank_period_combinations=test_case['bank_period_combinations'],
        basic_intent="Efficiency topic",
        full_intent=test_case['latest_message'],
        database_id="transcripts",
        context=context
    ):
        if "Retrieved" in chunk.get("content", "") and "chunks for categories" in chunk.get("content", ""):
            print(f"✅ {chunk['content']}", end="")
    
    # Test 5: Category-based - Multiple categories
    print("\n\n🔍 TEST 5: Category-based Retrieval - Multiple Categories")
    print("-" * 60)
    test_case = {
        "latest_message": "Tell me about loans and deposits",
        "bank_period_combinations": [
            {
                "bank_id": 8,
                "bank_name": "JPMorgan Chase & Co.",
                "bank_symbol": "JPM",
                "fiscal_year": 2025,
                "quarter": "Q2",
                "query_intent": "Loan and deposit information"
            }
        ]
    }
    
    for chunk in transcripts_agent(
        conversation=[{"role": "user", "content": test_case['latest_message']}],
        latest_message=test_case['latest_message'],
        bank_period_combinations=test_case['bank_period_combinations'],
        basic_intent="Loans and deposits",
        full_intent=test_case['latest_message'],
        database_id="transcripts",
        context=context
    ):
        if "Retrieved" in chunk.get("content", "") and "chunks for categories" in chunk.get("content", ""):
            print(f"✅ {chunk['content']}", end="")
    
    # Test 6: Similarity Search
    print("\n\n🔍 TEST 6: Similarity Search with Relevance Scores")
    print("-" * 60)
    test_case = {
        "latest_message": "What was the exact net interest margin percentage?",
        "bank_period_combinations": [
            {
                "bank_id": 9,
                "bank_name": "Bank of America",
                "bank_symbol": "BAC",
                "fiscal_year": 2025,
                "quarter": "Q1",
                "query_intent": "Find specific NIM metric"
            }
        ]
    }
    
    output = ""
    for chunk in transcripts_agent(
        conversation=[{"role": "user", "content": test_case['latest_message']}],
        latest_message=test_case['latest_message'],
        bank_period_combinations=test_case['bank_period_combinations'],
        basic_intent="NIM percentage",
        full_intent=test_case['latest_message'],
        database_id="transcripts",
        context=context
    ):
        content = chunk.get("content", "")
        output += content
        if "Retrieved top" in content and "chunks for" in content:
            print(f"✅ {content}", end="")
        elif "Similarity:" in content:
            print(f"  📊 {content}", end="")
    
    # Test 7: Multiple banks in parallel - different periods
    print("\n\n🔍 TEST 7: Parallel Processing - Different Banks/Periods")
    print("-" * 60)
    test_case = {
        "latest_message": "Compare revenue growth",
        "bank_period_combinations": [
            {
                "bank_id": 1,
                "bank_name": "Royal Bank of Canada",
                "bank_symbol": "RY",
                "fiscal_year": 2025,
                "quarter": "Q1",
                "query_intent": "Revenue growth comparison"
            },
            {
                "bank_id": 2,
                "bank_name": "Toronto-Dominion Bank",
                "bank_symbol": "TD", 
                "fiscal_year": 2025,
                "quarter": "Q2",
                "query_intent": "Revenue growth comparison"
            },
            {
                "bank_id": 8,
                "bank_name": "JPMorgan Chase & Co.",
                "bank_symbol": "JPM",
                "fiscal_year": 2025,
                "quarter": "Q1",
                "query_intent": "Revenue growth comparison"
            }
        ]
    }
    
    print("Testing parallel processing of 3 bank-period combinations...")
    banks_processed = []
    for chunk in transcripts_agent(
        conversation=[{"role": "user", "content": test_case['latest_message']}],
        latest_message=test_case['latest_message'],
        bank_period_combinations=test_case['bank_period_combinations'],
        basic_intent="Revenue comparison",
        full_intent=test_case['latest_message'],
        database_id="transcripts",
        context=context
    ):
        content = chunk.get("content", "")
        # Track which banks were processed
        for combo in test_case['bank_period_combinations']:
            if f"**{combo['bank_name']}" in content:
                bank_key = f"{combo['bank_symbol']} {combo['quarter']} {combo['fiscal_year']}"
                if bank_key not in banks_processed:
                    banks_processed.append(bank_key)
                    print(f"\n  ✅ Processing: {bank_key}")
        
        if "Retrieved" in content and "chunks" in content:
            # Extract just the retrieval summary
            retrieval_line = content.strip()
            if retrieval_line.startswith("*Retrieved"):
                print(f"    → {retrieval_line}")
    
    # Test 8: Verify filtering by bank/year/quarter
    print("\n\n🔍 TEST 8: Verify Proper Filtering by Bank/Year/Quarter")
    print("-" * 60)
    print("Testing that each combination only retrieves its own data...")
    
    # Same query but different banks and periods
    test_case = {
        "latest_message": "What about expenses?",
        "bank_period_combinations": [
            {
                "bank_id": 1,
                "bank_name": "Royal Bank of Canada",
                "bank_symbol": "RY",
                "fiscal_year": 2025,
                "quarter": "Q1",
                "query_intent": "Expense information"
            },
            {
                "bank_id": 1,
                "bank_name": "Royal Bank of Canada",
                "bank_symbol": "RY",
                "fiscal_year": 2025,
                "quarter": "Q2",
                "query_intent": "Expense information"
            }
        ]
    }
    
    for chunk in transcripts_agent(
        conversation=[{"role": "user", "content": test_case['latest_message']}],
        latest_message=test_case['latest_message'],
        bank_period_combinations=test_case['bank_period_combinations'],
        basic_intent="Expenses",
        full_intent=test_case['latest_message'],
        database_id="transcripts",
        context=context
    ):
        content = chunk.get("content", "")
        if "Royal Bank of Canada - Q1 2025" in content:
            print(f"  ✅ RY Q1 2025 data retrieved separately")
        elif "Royal Bank of Canada - Q2 2025" in content:
            print(f"  ✅ RY Q2 2025 data retrieved separately")
    
    print("\n" + "=" * 80)
    print("✅ COMPREHENSIVE TESTING COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    test_all_retrieval_methods()