#!/usr/bin/env python3
"""
Test script for LLM connector.

This script tests the LLM connection using the workflow.
Make sure to set your API_KEY in the .env file before running.
"""

from aegis.model.workflow.model_workflow import execute_workflow


def main():
    """Test the LLM connector through the workflow."""
    print("Testing LLM Connector...")
    print("-" * 50)
    
    # Create a simple test conversation
    test_conversation = {
        "messages": [
            {"role": "user", "content": "Hello, this is a test message."}
        ]
    }
    
    # Execute workflow which will test LLM connection
    result = execute_workflow(test_conversation)
    
    print(f"Execution ID: {result['execution_id']}")
    print(f"Auth Method: {result['auth_config']['method']}")
    print(f"SSL Verify: {result['ssl_config']['verify']}")
    print(f"Messages Processed: {result['processed_conversation']['message_count']}")
    
    print("\nLLM Connection Test:")
    print("-" * 50)
    llm_test = result.get('llm_test', {})
    print(f"Status: {llm_test.get('status')}")
    print(f"Model: {llm_test.get('model')}")
    print(f"Base URL: {llm_test.get('base_url')}")
    print(f"Response: {llm_test.get('response')}")
    
    if llm_test.get('status') == 'failed':
        print(f"Error: {llm_test.get('error')}")
    
    print("-" * 50)
    if llm_test.get('status') == 'success':
        print("✅ LLM connection test successful!")
    else:
        print("❌ LLM connection test failed!")
    
    return llm_test.get('status') == 'success'


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)