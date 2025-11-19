#!/usr/bin/env python3
"""
Debug script to test LLM response parsing for theme_grouping.

This script simulates the exact parsing logic used in the ETL to identify
where JSON parsing might be failing.

Usage:
    python scripts/debug_llm_response_parsing.py --test-response "path/to/response.json"

Or run interactively to test with live LLM response.
"""

import argparse
import json
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aegis.utils.logging import setup_logging, get_logger


def parse_theme_groups(response: dict) -> list:
    """
    Parse theme_groups from LLM response.

    This mirrors the exact logic in the ETL main.py.

    Args:
        response: LLM response dict

    Returns:
        List of theme groups

    Raises:
        Various exceptions if parsing fails
    """
    logger = get_logger()

    print("\n[PARSING RESPONSE]")
    print(f"Response type: {type(response)}")
    print(f"Response keys: {list(response.keys())}")

    # Check for tool_calls
    if "tool_calls" not in response:
        raise ValueError("No 'tool_calls' in response")

    tool_calls = response["tool_calls"]
    print(f"✅ Found tool_calls: {len(tool_calls)} calls")

    if not tool_calls:
        raise ValueError("tool_calls is empty")

    tool_call = tool_calls[0]
    print(f"First tool call keys: {list(tool_call.keys())}")

    # Get arguments
    if "arguments" not in tool_call:
        raise ValueError("No 'arguments' in tool_call")

    arguments_str = tool_call["arguments"]
    print(f"✅ Found arguments: {len(arguments_str)} chars")
    print(f"First 200 chars: {arguments_str[:200]}...")

    # Parse JSON
    try:
        arguments = json.loads(arguments_str)
        print(f"✅ Parsed JSON successfully")
    except json.JSONDecodeError as e:
        print(f"❌ JSON parsing failed: {e}")
        print(f"Failed at position {e.pos}")
        print(f"Context: ...{arguments_str[max(0, e.pos-50):e.pos+50]}...")
        raise

    print(f"Parsed arguments keys: {list(arguments.keys())}")

    # Check for theme_groups
    if "theme_groups" not in arguments:
        raise ValueError(f"No 'theme_groups' key. Available: {list(arguments.keys())}")

    theme_groups = arguments["theme_groups"]
    print(f"✅ Found theme_groups: {len(theme_groups)} groups")

    # Validate structure
    for i, group in enumerate(theme_groups):
        print(f"\n  Group {i+1}:")
        print(f"    Keys: {list(group.keys())}")

        required_keys = ["theme_name", "qa_ids"]
        for key in required_keys:
            if key not in group:
                raise ValueError(f"Group {i+1} missing required key: {key}")
            print(f"    ✅ {key}: {group[key][:50] if isinstance(group[key], str) else group[key]}")

    return theme_groups


def main():
    """Run parsing debug."""
    parser = argparse.ArgumentParser(description="Debug theme_grouping response parsing")
    parser.add_argument(
        "--test-response", help="Path to JSON file with test LLM response"
    )
    args = parser.parse_args()

    setup_logging()
    logger = get_logger()

    print("\n" + "=" * 80)
    print("THEME GROUPING RESPONSE PARSING DEBUG")
    print("=" * 80)

    if args.test_response:
        # Load test response from file
        print(f"\n[LOADING TEST RESPONSE]")
        print(f"File: {args.test_response}")

        try:
            with open(args.test_response, "r") as f:
                response = json.load(f)
            print(f"✅ Loaded response from file")
        except Exception as e:
            print(f"❌ Failed to load file: {e}")
            return

    else:
        # Create a sample response for testing
        print("\n[USING SAMPLE RESPONSE]")
        print("Testing with a valid sample response structure...")

        response = {
            "tool_calls": [
                {
                    "name": "group_themes",
                    "arguments": json.dumps(
                        {
                            "theme_groups": [
                                {
                                    "theme_name": "Sample Theme 1",
                                    "qa_ids": ["qa_1", "qa_2"],
                                },
                                {
                                    "theme_name": "Sample Theme 2",
                                    "qa_ids": ["qa_3"],
                                },
                            ]
                        }
                    ),
                }
            ]
        }

    # Try parsing
    try:
        theme_groups = parse_theme_groups(response)
        print("\n" + "=" * 80)
        print("✅ PARSING SUCCESSFUL")
        print("=" * 80)
        print(f"\nExtracted {len(theme_groups)} theme groups:")
        for i, group in enumerate(theme_groups, 1):
            print(f"\n{i}. {group['theme_name']}")
            print(f"   Q&A IDs: {group['qa_ids']}")

    except Exception as e:
        print("\n" + "=" * 80)
        print("❌ PARSING FAILED")
        print("=" * 80)
        print(f"\nError: {e}")
        logger.exception("parsing_failed")

    print("\n" + "=" * 80)
    print("DEBUG COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
