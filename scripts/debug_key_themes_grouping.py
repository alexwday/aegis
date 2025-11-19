#!/usr/bin/env python3
"""
Debug script for key_themes ETL theme_grouping stage.

This script helps diagnose issues with the theme_grouping prompt and LLM response parsing.
Run this on your work computer to identify where the process is failing.

Usage:
    python scripts/debug_key_themes_grouping.py --bank "Royal Bank of Canada" --year 2025 --quarter Q2
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aegis.utils.settings import config
from aegis.utils.logging import setup_logging, get_logger
from aegis.utils.prompt_loader import load_prompt_from_db
from aegis.connections.postgres_connector import get_connection
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.llm_connector import complete_with_tools
from aegis.etls.key_themes.main import (
    load_categories,
    retrieve_full_sections,
    classify_qa_blocks,
    format_qa_blocks,
)


async def main():
    """Run diagnostic checks on theme_grouping stage."""
    parser = argparse.ArgumentParser(description="Debug key_themes theme_grouping stage")
    parser.add_argument("--bank", required=True, help="Bank name")
    parser.add_argument("--year", type=int, required=True, help="Fiscal year")
    parser.add_argument("--quarter", required=True, help="Quarter (Q1, Q2, Q3, Q4)")
    args = parser.parse_args()

    # Setup logging with DEBUG level
    setup_logging()
    logger = get_logger(__name__)

    execution_id = "debug-grouping-script"
    logger.info(
        "debug.started",
        bank=args.bank,
        year=args.year,
        quarter=args.quarter,
        execution_id=execution_id,
    )

    # Initialize database
    engine = await get_connection()

    # Setup authentication
    auth_config = setup_authentication({"execution_id": execution_id})
    ssl_config = {}
    context = {"execution_id": execution_id, "auth_config": auth_config, "ssl_config": ssl_config}

    print("\n" + "=" * 80)
    print("KEY THEMES GROUPING DEBUG SCRIPT")
    print("=" * 80)

    # Step 1: Check prompt exists and load it
    print("\n[STEP 1] Checking theme_grouping prompt in database...")
    try:
        prompt_result = load_prompt_from_db(
            layer="key_themes_etl",
            name="theme_grouping",
            compose_with_globals=False,
            available_databases=None,
            execution_id=execution_id,
        )
        print("✅ Prompt loaded successfully")
        print(f"   Version: {prompt_result.get('version', 'unknown')}")
        print(f"   System prompt length: {len(prompt_result.get('system_prompt', ''))}")
        print(f"   Has tool definition: {bool(prompt_result.get('tool_definition'))}")

        # Show first 500 chars of system prompt
        system_prompt = prompt_result.get("system_prompt", "")
        print(f"\n   First 500 chars of system prompt:")
        print(f"   {system_prompt[:500]}...")

        # Show tool definition
        tool_def = prompt_result.get("tool_definition")
        if tool_def:
            print(f"\n   Tool definition:")
            print(f"   {json.dumps(tool_def, indent=2)[:500]}...")
        else:
            print("   ⚠️  WARNING: No tool definition found!")

    except Exception as e:
        print(f"❌ FAILED to load prompt: {e}")
        logger.exception("debug.prompt_load_failed", error=str(e))
        return

    # Step 2: Get sample data
    print("\n[STEP 2] Retrieving sample Q&A data...")
    try:
        categories = load_categories()
        print(f"✅ Loaded {len(categories)} categories")

        # Map bank name to bank_id
        bank_map = {
            "Royal Bank of Canada": 1,
            "Toronto-Dominion Bank": 2,
            "Bank of Nova Scotia": 3,
            "Bank of Montreal": 4,
            "Canadian Imperial Bank of Commerce": 5,
        }
        bank_id = bank_map.get(args.bank)
        if not bank_id:
            print(f"❌ Unknown bank: {args.bank}")
            return

        chunks = await retrieve_full_sections(
            engine=engine,
            bank_id=bank_id,
            fiscal_year=args.year,
            quarter=args.quarter,
            sections=["QA"],
            context=context,
        )
        print(f"✅ Retrieved {len(chunks)} Q&A chunks")

        if not chunks:
            print("❌ No data found for this bank/period")
            return

        # Classify and format (take first 3 for testing)
        classified = await classify_qa_blocks(
            chunks[:3], categories, context, limit=3  # Limit to 3 for faster testing
        )
        print(f"✅ Classified {len(classified)} Q&A blocks")

        formatted = await format_qa_blocks(classified, context)
        print(f"✅ Formatted {len(formatted)} Q&A blocks")

    except Exception as e:
        print(f"❌ FAILED to get sample data: {e}")
        logger.exception("debug.data_retrieval_failed", error=str(e))
        return

    # Step 3: Build the grouping request
    print("\n[STEP 3] Building theme_grouping LLM request...")
    try:
        # Build user message with formatted Q&A blocks
        user_message = "\n\n---\n\n".join(formatted)
        print(f"✅ Built user message ({len(user_message)} chars)")
        print(f"\n   First 500 chars:")
        print(f"   {user_message[:500]}...")

        messages = [
            {"role": "system", "content": prompt_result["system_prompt"]},
            {"role": "user", "content": user_message},
        ]

        tools = [prompt_result["tool_definition"]] if prompt_result.get("tool_definition") else []

        print(f"\n   Message count: {len(messages)}")
        print(f"   Tool count: {len(tools)}")

    except Exception as e:
        print(f"❌ FAILED to build request: {e}")
        logger.exception("debug.request_build_failed", error=str(e))
        return

    # Step 4: Make LLM request
    print("\n[STEP 4] Making LLM request to theme_grouping...")
    try:
        response = await complete_with_tools(
            messages=messages,
            tools=tools,
            context=context,
            llm_params={"temperature": 0.1, "max_tokens": 32768},
        )
        print("✅ LLM request successful")
        print(f"   Response type: {type(response)}")
        print(f"   Response keys: {list(response.keys()) if isinstance(response, dict) else 'N/A'}")

        # Extract tool calls from OpenAI response structure
        tool_calls = None
        if "choices" in response:
            tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls", [])

        print(f"   Has tool calls: {bool(tool_calls)}")

        if tool_calls:
            # OpenAI returns tool_calls with function.name and function.arguments
            tool_call = tool_calls[0]
            print(f"\n   Tool called: {tool_call.get('function', {}).get('name', 'unknown')}")
            args_str = tool_call.get('function', {}).get('arguments', '')
            print(f"   Arguments length: {len(args_str)}")

            # Try to parse arguments
            try:
                args_dict = json.loads(args_str)
                print(f"   ✅ Arguments are valid JSON")
                print(f"   Keys in response: {list(args_dict.keys())}")

                # Check for theme_groups
                if "theme_groups" in args_dict:
                    theme_groups = args_dict["theme_groups"]
                    print(f"   ✅ Found 'theme_groups' key with {len(theme_groups)} groups")

                    # Show first group structure
                    if theme_groups:
                        first_group = theme_groups[0]
                        print(f"\n   First group structure:")
                        print(f"   Keys: {list(first_group.keys())}")
                        print(
                            f"   Group title: {first_group.get('group_title', 'missing')[:50]}..."
                        )
                        print(f"   QA IDs: {first_group.get('qa_ids', [])}")
                        print(f"   Rationale: {first_group.get('rationale', 'missing')[:100]}...")
                else:
                    print("   ❌ 'theme_groups' key NOT found in response!")
                    print(f"   Available keys: {list(args_dict.keys())}")

                # Print full response for inspection
                print(f"\n   Full arguments (first 1000 chars):")
                print(f"   {args_str[:1000]}...")

            except json.JSONDecodeError as je:
                print(f"   ❌ Arguments are NOT valid JSON: {je}")
                print(f"   Raw arguments: {args_str[:500]}...")

        else:
            print("   ⚠️  No tool calls in response (got text completion instead)")
            print(f"   Content: {response.get('content', '')[:500]}...")

    except Exception as e:
        print(f"❌ FAILED LLM request: {e}")
        logger.exception("debug.llm_request_failed", error=str(e))
        return

    print("\n" + "=" * 80)
    print("DEBUG COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
