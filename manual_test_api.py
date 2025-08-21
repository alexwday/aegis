#!/usr/bin/env python3
"""
Manual API test script for testing LLM connectivity.

This is NOT part of the automated test suite.
Run this manually to test API connectivity in local or production.

Tests all 3 models (small, medium, large) and all 3 API types (complete, stream, tools).

Usage:
    python manual_test_api.py
"""

import json
from aegis.model.workflow.model_workflow import execute_workflow
from aegis.connections.llm import complete, stream, complete_with_tools
from aegis.utils.settings import config


def test_all_models_and_methods():
    """Test all model tiers with all API call methods."""
    print("=" * 70)
    print("COMPREHENSIVE LLM API TEST")
    print("=" * 70)
    
    # Show configuration
    print(f"\nConfiguration:")
    print(f"  Auth Method: {config.auth_method}")
    print(f"  Base URL: {config.llm.base_url}")
    print(f"  SSL Verify: {config.ssl_verify}")
    
    if config.auth_method == "oauth":
        print(f"  OAuth Endpoint: {config.oauth_endpoint}")
    
    print(f"\nModels:")
    print(f"  Small:  {config.llm.small.model}")
    print(f"  Medium: {config.llm.medium.model}")
    print(f"  Large:  {config.llm.large.model}")
    
    # Get workflow configuration
    print("\n" + "-" * 70)
    print("Step 1: Initializing workflow...")
    try:
        workflow_result = execute_workflow({
            "messages": [{"role": "user", "content": "Test"}]
        })
        auth_config = workflow_result["auth_config"]
        ssl_config = workflow_result["ssl_config"]
        print(f"✅ Workflow initialized")
        print(f"   Execution ID: {workflow_result['execution_id']}")
        print(f"   Auth: {auth_config['method']}")
    except Exception as e:
        print(f"❌ Workflow failed: {e}")
        return False
    
    # Test configurations
    models = [
        ("SMALL", config.llm.small),
        ("MEDIUM", config.llm.medium),
        ("LARGE", config.llm.large)
    ]
    
    all_passed = True
    
    for model_name, model_config in models:
        print("\n" + "-" * 70)
        print(f"Testing {model_name} Model: {model_config.model}")
        print("-" * 70)
        
        # Test 1: Regular completion
        print(f"\n1. Testing regular completion...")
        try:
            response = complete(
                messages=[{"role": "user", "content": f"Say '{model_name} completion works!' exactly."}],
                auth_config=auth_config,
                ssl_config=ssl_config,
                execution_id=f"test-{model_name.lower()}-complete",
                model=model_config.model,
                temperature=0,
                max_tokens=50
            )
            content = response["choices"][0]["message"]["content"]
            tokens = response.get("usage", {}).get("total_tokens", 0)
            print(f"   ✅ Complete: {content}")
            print(f"      Tokens: {tokens}")
        except Exception as e:
            print(f"   ❌ Complete failed: {str(e)[:100]}")
            all_passed = False
        
        # Test 2: Streaming
        print(f"\n2. Testing streaming response...")
        try:
            chunks = []
            for chunk in stream(
                messages=[{"role": "user", "content": f"Count 1, 2, 3 for {model_name} model."}],
                auth_config=auth_config,
                ssl_config=ssl_config,
                execution_id=f"test-{model_name.lower()}-stream",
                model=model_config.model,
                temperature=0,
                max_tokens=50
            ):
                if chunk.get("choices") and chunk["choices"][0].get("delta", {}).get("content"):
                    chunks.append(chunk["choices"][0]["delta"]["content"])
            
            full_response = "".join(chunks)
            print(f"   ✅ Stream: {full_response[:100]}")
            print(f"      Chunks received: {len(chunks)}")
        except Exception as e:
            print(f"   ❌ Stream failed: {str(e)[:100]}")
            all_passed = False
        
        # Test 3: Tool calling (only for medium and large models typically)
        print(f"\n3. Testing tool/function calling...")
        try:
            tools = [
                {
                    "type": "function",
                    "function": {
                        "name": "get_weather",
                        "description": "Get the weather for a location",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "location": {
                                    "type": "string",
                                    "description": "The city and state"
                                }
                            },
                            "required": ["location"]
                        }
                    }
                }
            ]
            
            response = complete_with_tools(
                messages=[{"role": "user", "content": "What's the weather in San Francisco?"}],
                tools=tools,
                auth_config=auth_config,
                ssl_config=ssl_config,
                execution_id=f"test-{model_name.lower()}-tools",
                model=model_config.model,
                temperature=0,
                max_tokens=100
            )
            
            # Check if model made a tool call or just responded
            if response["choices"][0]["message"].get("tool_calls"):
                tool_call = response["choices"][0]["message"]["tool_calls"][0]
                func_name = tool_call["function"]["name"]
                func_args = tool_call["function"]["arguments"]
                print(f"   ✅ Tools: Called {func_name}")
                print(f"      Args: {func_args}")
            else:
                content = response["choices"][0]["message"].get("content", "")
                print(f"   ✅ Tools: Response without tool call")
                print(f"      Content: {content[:100]}")
                
        except Exception as e:
            print(f"   ❌ Tools failed: {str(e)[:100]}")
            all_passed = False
    
    # Summary
    print("\n" + "=" * 70)
    if all_passed:
        print("🎉 ALL TESTS PASSED!")
        print("\nYour LLM connector is working correctly with:")
        print(f"  - Auth method: {config.auth_method}")
        print(f"  - All 3 model tiers")
        print(f"  - All 3 API call types (complete, stream, tools)")
    else:
        print("⚠️ SOME TESTS FAILED")
        print("Please check the errors above.")
    print("=" * 70)
    
    return all_passed


if __name__ == "__main__":
    import sys
    success = test_all_models_and_methods()
    sys.exit(0 if success else 1)