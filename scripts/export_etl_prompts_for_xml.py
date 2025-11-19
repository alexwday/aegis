"""
Export Call Summary and Key Themes prompts from database for XML conversion.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aegis.connections.postgres_connector import fetch_all


async def export_prompts():
    """Export prompts from database."""

    # First check the table schema
    print("="*80)
    print("PROMPTS TABLE SCHEMA")
    print("="*80)

    schema = await fetch_all(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'prompts'
        ORDER BY ordinal_position
        """
    )

    print("\nColumns in 'prompts' table:")
    for row in schema:
        print(f"  - {row['column_name']}: {row['data_type']}")

    # Check what ETL prompts exist
    print("\n" + "="*80)
    print("ALL PROMPTS IN DATABASE")
    print("="*80)

    all_prompts = await fetch_all(
        """
        SELECT model, name FROM prompts ORDER BY model, name
        """
    )

    print(f"\nFound {len(all_prompts)} prompts:")
    for row in all_prompts:
        print(f"  - Model: {row['model']}, Name: {row['name']}")

    # Export Call Summary prompts
    print("\n" + "="*80)
    print("CALL SUMMARY ETL PROMPTS")
    print("="*80)

    call_summary_prompts = await fetch_all(
        """
        SELECT name, description, system_prompt, user_prompt
        FROM prompts
        WHERE layer = 'call_summary_etl'
        ORDER BY name
        """
    )

    for row in call_summary_prompts:
        print(f"\n{'='*80}")
        print(f"PROMPT: {row['name']}")
        print(f"Description: {row['description']}")
        print(f"{'='*80}")

        if row['system_prompt']:
            print(f"\n--- SYSTEM PROMPT ---")
            print(row['system_prompt'])

        if row['user_prompt']:
            print(f"\n--- USER PROMPT ---")
            print(row['user_prompt'])
        print()

    # Export Key Themes prompts
    print("\n" + "="*80)
    print("KEY THEMES ETL PROMPTS")
    print("="*80)

    key_themes_prompts = await fetch_all(
        """
        SELECT name, description, system_prompt, user_prompt
        FROM prompts
        WHERE layer = 'key_themes_etl'
        ORDER BY name
        """
    )

    for row in key_themes_prompts:
        print(f"\n{'='*80}")
        print(f"PROMPT: {row['name']}")
        print(f"Description: {row['description']}")
        print(f"{'='*80}")

        if row['system_prompt']:
            print(f"\n--- SYSTEM PROMPT ---")
            print(row['system_prompt'])

        if row['user_prompt']:
            print(f"\n--- USER PROMPT ---")
            print(row['user_prompt'])
        print()


if __name__ == "__main__":
    asyncio.run(export_prompts())
