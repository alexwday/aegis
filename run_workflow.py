#!/usr/bin/env python
"""
Simple CLI to test the Aegis model streaming.

Usage:
    python run_workflow.py
"""

import json
import os
import sys

# Set log level to reduce verbosity (optional - comment out for full logs)
# os.environ["LOG_LEVEL"] = "WARNING"  # Uncomment to hide logs
# os.environ["LOG_LEVEL"] = "INFO"     # Uncomment for normal logs
# os.environ["LOG_LEVEL"] = "DEBUG"    # Uncomment for detailed logs

from src.aegis.model.main import model


def main():
    """Run a test workflow with streaming output."""
    # Sample conversation - modify this to test different scenarios
    
    # Test 1: Direct response (greeting)
    # conversation = {
    #     "messages": [
    #         {"role": "user", "content": "Hello Aegis"},
    #     ]
    # }
    
    # Test 2: Direct response (concept explanation)
    conversation = {
        "messages": [
            {"role": "user", "content": "What is efficiency ratio?"},
        ]
    }
    
    # Test 3: Research workflow (data request)
    # conversation = {
    #     "messages": [
    #         {"role": "user", "content": "Show me RBC's Q3 revenue"},
    #     ]
    # }
    
    # Optional database filters (None for now)
    db_names = None
    
    print("🚀 Running Aegis Model...")
    print("-" * 50)
    print("Input conversation:")
    print(json.dumps(conversation, indent=2))
    print(f"Database filters: {db_names}")
    print("-" * 50)
    print("\n📡 Streaming output:\n")
    
    try:
        # Track subagents for summary
        subagents_seen = set()
        
        # Stream responses
        for message in model(conversation, db_names):
            msg_type = message["type"]
            msg_name = message["name"]
            msg_content = message["content"]
            
            if msg_type == "agent":
                # Main agent response
                print(msg_content, end="", flush=True)
            else:
                # Subagent response
                print(f"\n  [{msg_name.upper()}] {msg_content}", end="", flush=True)
                subagents_seen.add(msg_name)
        
        print("\n" + "-" * 50)
        print("✅ Streaming completed successfully!")
        if subagents_seen:
            print(f"📊 Subagents used: {', '.join(sorted(subagents_seen))}")
        else:
            print("📊 Response type: Direct response (no data retrieval needed)")
        
    except Exception as e:
        print(f"\n❌ Model failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()