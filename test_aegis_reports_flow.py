#!/usr/bin/env python
"""
Test the complete Aegis workflow with the reports subagent.
This simulates a user asking for a call summary report.
"""

import sys
sys.path.insert(0, 'src')

from aegis.model.main import model
from aegis.utils.logging import setup_logging

# Setup logging
setup_logging()

def test_aegis_reports():
    """Test the full Aegis flow with reports subagent."""

    print("\n" + "="*80)
    print("TESTING AEGIS WITH REPORTS SUBAGENT")
    print("="*80)

    # Create a conversation asking for the call summary
    conversation = {
        "messages": [
            {
                "role": "user",
                "content": "Show me the call summary report for Royal Bank of Canada Q2 2025"
            }
        ]
    }

    print("\nUser Query:")
    print(conversation["messages"][0]["content"])
    print("\n" + "-"*80)
    print("AEGIS RESPONSE:")
    print("-"*80 + "\n")

    # Track what stage we're in
    current_stage = None

    try:
        # Run the model and stream the response
        for chunk in model(conversation):
            # Track stage changes
            if chunk.get("type") == "agent" and chunk.get("name") == "aegis":
                if current_stage != "agent":
                    current_stage = "agent"
                    print("\n[MAIN AGENT]")
            elif chunk.get("type") == "subagent_start":
                print(f"\n[SUBAGENT START: {chunk.get('name')}]")
                current_stage = "subagent"
            elif chunk.get("type") == "subagent":
                if current_stage != "subagent":
                    current_stage = "subagent"
                    print(f"\n[SUBAGENT: {chunk.get('name')}]")
            elif chunk.get("type") == "summarizer_start":
                print("\n[SUMMARIZER START]")
                current_stage = "summarizer"

            # Print the content
            if chunk.get("content"):
                print(chunk["content"], end="")

    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
    except Exception as e:
        print(f"\n\nError during test: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "="*80)
    print("END OF TEST")
    print("="*80)

    print("\nWhat to look for:")
    print("1. Router should route to research workflow")
    print("2. Clarifier should extract RBC and Q2 2025")
    print("3. Planner should select 'reports' database")
    print("4. Reports subagent should retrieve the call summary")
    print("5. Download links should show S3 URLs")
    print("6. Check for: https://example-bucket.s3.amazonaws.com/reports/RY_2025_Q2_97f291be.docx")


if __name__ == "__main__":
    test_aegis_reports()