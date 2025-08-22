#!/usr/bin/env python
"""
Simple CLI to test the Aegis model streaming.

Usage:
    python run_workflow.py
"""

import json
import sys

from src.aegis.model.main import model


def main():
    """Run a test workflow with streaming output."""
    # Sample conversation
    conversation = {
        "messages": [
            {"role": "user", "content": "Hello, I need help with my project"},
            {"role": "assistant", "content": "I'd be happy to help! What specific aspect of your project do you need assistance with?"},
            {"role": "user", "content": "Can you help me understand the Q3 revenue figures?"},
        ]
    }
    
    # Optional database filters
    db_names = ["internal_capm", "internal_wiki"]
    
    print("🚀 Running Aegis Model...")
    print("-" * 50)
    print("Input conversation:")
    print(json.dumps(conversation, indent=2))
    print(f"Database filters: {db_names}")
    print("-" * 50)
    print("\n📡 Streaming output:\n")
    
    try:
        # Track subagents for final summary
        subagents_seen = set()
        agent_messages = []
        
        # Stream responses
        for message in model(conversation, db_names):
            msg_type = message["type"]
            msg_name = message["name"]
            msg_content = message["content"]
            
            if msg_type == "agent":
                # Main agent response
                print(f"[AEGIS] {msg_content}", end="")
                agent_messages.append(msg_content)
            else:
                # Subagent response
                print(f"  [{msg_name.upper()}] {msg_content}", end="")
                subagents_seen.add(msg_name)
        
        print("\n" + "-" * 50)
        print("✅ Streaming completed successfully!")
        print(f"📊 Subagents used: {', '.join(sorted(subagents_seen)) if subagents_seen else 'None'}")
        print(f"📝 Main agent messages: {len(agent_messages)}")
        
    except Exception as e:
        print(f"\n❌ Model failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()