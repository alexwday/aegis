"""
Test script to verify what data subagents receive.
This will help us understand the exact structure of bank_period_combinations.
"""

import json
from aegis.model.agents.clarifier import clarify_query
from aegis.utils.settings import config
from aegis.connections.oauth_connector import setup_authentication
from aegis.utils.ssl import setup_ssl

def test_clarifier_output():
    """Test what the clarifier returns for a typical query."""
    
    # Setup SSL and authentication properly
    ssl_config = setup_ssl()
    auth_config = setup_authentication("test-123", ssl_config)
    
    if not auth_config.get("success"):
        print(f"❌ Authentication failed: {auth_config.get('error')}")
        return
    
    # Create a test context with proper auth
    context = {
        "execution_id": "test-123",
        "auth_config": auth_config,
        "ssl_config": ssl_config
    }
    
    # Test query asking for all Canadian banks Q3
    test_queries = [
        "What is the revenue for all Canadian banks in Q3 2024?",
        "Show me RBC and TD efficiency ratio for Q1 and Q2 2025",
        "JPMorgan and Bank of America net income for 2025 Q1"
    ]
    
    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Testing query: {query}")
        print('='*60)
        
        # Call clarifier
        result = clarify_query(
            query=query,
            context=context,
            available_databases=["transcripts", "benchmarking", "reports", "rts"],
            messages=[{"role": "user", "content": query}]
        )
        
        # Check if it's a list (success) or dict (needs clarification)
        if isinstance(result, list):
            print(f"\n✅ Success! Got {len(result)} combinations:")
            
            # Show the structure of each combination
            for i, combo in enumerate(result[:3], 1):  # Show first 3
                print(f"\nCombination {i}:")
                print(json.dumps(combo, indent=2))
            
            if len(result) > 3:
                print(f"\n... and {len(result) - 3} more combinations")
            
            # Verify all required fields are present
            print("\n📋 Field verification:")
            required_fields = ["bank_id", "bank_name", "bank_symbol", "fiscal_year", "quarter", "query_intent"]
            for field in required_fields:
                has_field = all(field in combo for combo in result)
                status = "✅" if has_field else "❌"
                print(f"  {status} {field}: Present in all combinations")
                
            # Show unique values
            print("\n📊 Unique values:")
            print(f"  Bank IDs: {sorted(set(c['bank_id'] for c in result))}")
            print(f"  Bank symbols: {sorted(set(c['bank_symbol'] for c in result))}")
            print(f"  Fiscal years: {sorted(set(c['fiscal_year'] for c in result))}")
            print(f"  Quarters: {sorted(set(c['quarter'] for c in result))}")
            
        else:
            print(f"\n⚠️ Needs clarification:")
            print(json.dumps(result, indent=2))

if __name__ == "__main__":
    print("Testing Clarifier Output Structure")
    print("This shows exactly what data subagents receive as bank_period_combinations")
    test_clarifier_output()