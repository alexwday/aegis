#!/usr/bin/env python3
"""
Debug script to check theme_grouping prompt tool_definition format.

Run this at work to see what's stored in the database and identify any encoding issues.

Usage:
    python scripts/debug_theme_grouping_prompt.py
"""

import sys
import asyncio
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aegis.connections.postgres_connector import get_connection
from sqlalchemy import text


async def main():
    """Check the theme_grouping prompt tool_definition."""
    print("=" * 80)
    print("THEME GROUPING PROMPT TOOL DEFINITION DEBUG")
    print("=" * 80)
    print()

    async with get_connection() as conn:
        result = await conn.execute(
            text("""
                SELECT name, version, tool_definition
                FROM prompts
                WHERE layer = 'key_themes_etl' AND name = 'theme_grouping'
                ORDER BY version DESC
                LIMIT 1
            """)
        )
        row = result.fetchone()

        if not row:
            print("❌ ERROR: No theme_grouping prompt found in database!")
            print()
            print("Action required:")
            print("  1. Make sure you've uploaded the prompt using the prompt editor")
            print("  2. Check that the name is exactly 'theme_grouping' (not 'grouping')")
            return

        name, version, tool_def = row

        print(f"✓ Found prompt: {name} v{version}")
        print()

        # Check the type
        print(f"Tool definition type: {type(tool_def)}")
        print(f"Tool definition is dict: {isinstance(tool_def, dict)}")
        print()

        # Try to access as dict
        if isinstance(tool_def, dict):
            print("✓ Tool definition is already a dict (GOOD)")
            print()
            print("Function name:", tool_def.get("function", {}).get("name"))
            print()

            # Check for the problematic field
            params = tool_def.get("function", {}).get("parameters", {})
            props = params.get("properties", {})
            theme_groups = props.get("theme_groups", {})

            print("First 300 chars of tool_definition:")
            print("-" * 80)
            print(json.dumps(tool_def, indent=2)[:300])
            print("-" * 80)
            print()

            # Try to use it like the code does
            print("Testing JSON serialization (what LLM returns):")
            print("-" * 80)
            test_response = {
                "theme_groups": [
                    {
                        "group_title": "Test Group",
                        "qa_ids": ["qa_1"],
                        "rationale": "Test"
                    }
                ]
            }

            # Simulate what happens in the code
            arguments_str = json.dumps(test_response)
            print(f"Simulated LLM response (string): {arguments_str[:100]}...")
            print()

            # Test with leading newline (the error you're seeing)
            bad_arguments_str = f"\n{arguments_str}"
            print(f"With leading newline: {repr(bad_arguments_str[:50])}...")
            print()

            try:
                # Old code (would fail)
                result_old = json.loads(bad_arguments_str)
                print("❌ Old code would FAIL")
            except json.JSONDecodeError as e:
                print(f"✓ Old code fails as expected: {e}")
                print()

            # New code (should work)
            cleaned = bad_arguments_str.strip()
            result_new = json.loads(cleaned)
            print(f"✓ New code works after strip(): {result_new.keys()}")
            print()

        else:
            print("❌ ERROR: Tool definition is NOT a dict!")
            print()
            print("This means the prompt editor double-encoded it.")
            print()
            print("Raw value (first 500 chars):")
            print("-" * 80)
            print(str(tool_def)[:500])
            print("-" * 80)
            print()
            print("Action required:")
            print("  1. Delete this prompt from the database")
            print("  2. Re-upload it using the prompt editor")
            print("  3. Make sure to paste the JSON as a parsed object, not a string")

        print()
        print("=" * 80)
        print("DEBUG COMPLETE")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
