#!/usr/bin/env python3
"""
Debug script to check prompt database contents.

This script connects to the database and shows all key_themes_etl prompts,
their versions, and validates their structure.

Usage:
    python scripts/debug_prompt_database.py
"""

import asyncio
import json
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aegis.utils.settings import config
from aegis.utils.logging import setup_logging, get_logger
from aegis.connections.postgres_connector import get_connection
from sqlalchemy import text


async def main():
    """Check all key_themes_etl prompts in database."""
    setup_logging()
    logger = get_logger()

    print("\n" + "=" * 80)
    print("PROMPT DATABASE DEBUG SCRIPT")
    print("=" * 80)

    # Get database connection
    engine = await get_connection()

    print(f"\n[DATABASE CONNECTION]")
    print(f"Host: {config.postgres.host}")
    print(f"Port: {config.postgres.port}")
    print(f"Database: {config.postgres.database}")

    # Query all key_themes_etl prompts
    print("\n[QUERYING PROMPTS]")
    query = text("""
        SELECT
            name,
            version,
            created_at,
            updated_at,
            LENGTH(system_prompt) as system_prompt_length,
            LENGTH(user_prompt) as user_prompt_length,
            tool_definition IS NOT NULL as has_tool_definition,
            tool_definitions IS NOT NULL as has_tool_definitions
        FROM aegis_prompts
        WHERE layer = 'key_themes_etl'
        ORDER BY name, version DESC
    """)

    async with engine.begin() as conn:
        result = await conn.execute(query)
        prompts = result.fetchall()

    if not prompts:
        print("❌ No key_themes_etl prompts found in database!")
        return

    print(f"✅ Found {len(prompts)} prompt versions\n")

    # Group by name
    by_name = {}
    for row in prompts:
        name = row[0]
        if name not in by_name:
            by_name[name] = []
        by_name[name].append(row)

    # Display each prompt
    for name, versions in sorted(by_name.items()):
        print(f"\n{'=' * 80}")
        print(f"PROMPT: {name}")
        print(f"{'=' * 80}")
        print(f"Total versions: {len(versions)}")

        for row in versions:
            (
                _,
                version,
                created_at,
                updated_at,
                sys_len,
                user_len,
                has_tool_def,
                has_tool_defs,
            ) = row

            print(f"\n  Version {version}:")
            print(f"    Created: {created_at}")
            print(f"    Updated: {updated_at}")
            print(f"    System prompt: {sys_len} chars")
            print(f"    User prompt: {user_len} chars")
            print(f"    Has tool_definition: {has_tool_def}")
            print(f"    Has tool_definitions: {has_tool_defs}")

    # Now check the actual content of theme_grouping
    print(f"\n\n{'=' * 80}")
    print("DETAILED CHECK: theme_grouping")
    print(f"{'=' * 80}")

    query = text("""
        SELECT
            version,
            system_prompt,
            tool_definition
        FROM aegis_prompts
        WHERE layer = 'key_themes_etl' AND name = 'theme_grouping'
        ORDER BY version DESC
        LIMIT 1
    """)

    async with engine.begin() as conn:
        result = await conn.execute(query)
        row = result.fetchone()

    if not row:
        print("❌ theme_grouping prompt NOT FOUND!")
    else:
        version, system_prompt, tool_definition = row
        print(f"\n✅ Latest version: {version}")
        print(f"\nSystem prompt (first 500 chars):")
        print("-" * 80)
        print(system_prompt[:500] if system_prompt else "EMPTY")
        print("-" * 80)

        print(f"\nTool definition:")
        print("-" * 80)
        if tool_definition:
            try:
                # Parse and pretty print
                tool_dict = json.loads(tool_definition)
                print(json.dumps(tool_dict, indent=2)[:1000])

                # Check for required fields
                if "name" in tool_dict:
                    print(f"\n✅ Tool name: {tool_dict['name']}")
                else:
                    print("\n❌ Missing 'name' field!")

                if "parameters" in tool_dict:
                    print(f"✅ Has parameters")
                    params = tool_dict["parameters"]
                    if "properties" in params:
                        print(f"✅ Properties: {list(params['properties'].keys())}")
                    if "required" in params:
                        print(f"✅ Required fields: {params['required']}")
                else:
                    print("❌ Missing 'parameters' field!")

            except json.JSONDecodeError as e:
                print(f"❌ Tool definition is NOT valid JSON: {e}")
                print(tool_definition[:500])
        else:
            print("❌ EMPTY - No tool definition!")
        print("-" * 80)

    print("\n" + "=" * 80)
    print("DEBUG COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())
