#!/usr/bin/env python3
"""
Test the transcripts subagent with different query types.
"""

import json
from aegis.model.subagents.transcripts.main import transcripts_agent
from aegis.utils.settings import config
from aegis.connections.oauth_connector import setup_authentication
from aegis.utils.ssl import setup_ssl

def test_transcripts_subagent():
    """Test the transcripts subagent with various query types."""
    
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
    
    # Test cases with different query types
    test_cases = [
        {
            "name": "Summary Query",
            "latest_message": "Summarize what management said about expenses in Q1 2025",
            "bank_period_combinations": [
                {
                    "bank_id": 1,
                    "bank_name": "Royal Bank of Canada",
                    "bank_symbol": "RY",
                    "fiscal_year": 2025,
                    "quarter": "Q1",
                    "query_intent": "Summarize management discussion about expenses"
                },
                {
                    "bank_id": 2,
                    "bank_name": "Toronto-Dominion Bank",
                    "bank_symbol": "TD",
                    "fiscal_year": 2025,
                    "quarter": "Q1",
                    "query_intent": "Summarize management discussion about expenses"
                }
            ]
        },
        {
            "name": "Category Query",
            "latest_message": "What did banks say about loan growth?",
            "bank_period_combinations": [
                {
                    "bank_id": 8,
                    "bank_name": "JPMorgan Chase & Co.",
                    "bank_symbol": "JPM",
                    "fiscal_year": 2025,
                    "quarter": "Q2",
                    "query_intent": "Find all mentions of loan growth"
                },
                {
                    "bank_id": 9,
                    "bank_name": "Bank of America",
                    "bank_symbol": "BAC",
                    "fiscal_year": 2025,
                    "quarter": "Q2",
                    "query_intent": "Find all mentions of loan growth"
                }
            ]
        },
        {
            "name": "Specific Question",
            "latest_message": "What was RBC's net interest margin in Q1 2025?",
            "bank_period_combinations": [
                {
                    "bank_id": 1,
                    "bank_name": "Royal Bank of Canada",
                    "bank_symbol": "RY",
                    "fiscal_year": 2025,
                    "quarter": "Q1",
                    "query_intent": "Find specific mention of net interest margin value"
                }
            ]
        },
        {
            "name": "Full Transcript Summary",
            "latest_message": "Give me the full Q&A section summary",
            "bank_period_combinations": [
                {
                    "bank_id": 3,
                    "bank_name": "Bank of Nova Scotia",
                    "bank_symbol": "BNS",
                    "fiscal_year": 2025,
                    "quarter": "Q1",
                    "query_intent": "Summarize entire Q&A section"
                },
                {
                    "bank_id": 4,
                    "bank_name": "Bank of Montreal",
                    "bank_symbol": "BMO",
                    "fiscal_year": 2025,
                    "quarter": "Q1",
                    "query_intent": "Summarize entire Q&A section"
                }
            ]
        }
    ]
    
    # Run test cases
    for test_case in test_cases:
        print(f"\n{'='*60}")
        print(f"Testing: {test_case['name']}")
        print(f"Query: {test_case['latest_message']}")
        print(f"Banks: {len(test_case['bank_period_combinations'])} combinations")
        print('='*60)
        
        # Call the transcripts subagent
        output = ""
        for chunk in transcripts_agent(
            conversation=[{"role": "user", "content": test_case['latest_message']}],
            latest_message=test_case['latest_message'],
            bank_period_combinations=test_case['bank_period_combinations'],
            basic_intent=f"Test: {test_case['name']}",
            full_intent=test_case['latest_message'],
            database_id="transcripts",
            context=context
        ):
            if chunk.get("content"):
                print(chunk["content"], end="", flush=True)
                output += chunk["content"]
        
        print("\n")
        
        # Brief pause between tests
        import time
        time.sleep(1)

if __name__ == "__main__":
    print("Testing Transcripts Subagent - Parallel Decision Making")
    print("="*60)
    test_transcripts_subagent()