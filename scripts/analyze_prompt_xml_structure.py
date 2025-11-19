"""
Analyze XML structure across all ETL prompts and create comparison table.
"""
import asyncio
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aegis.connections.postgres_connector import fetch_all


def extract_xml_tags(text):
    """Extract all unique opening XML tags from text."""
    if not text:
        return set()

    # Find all opening tags (e.g., <context>, <objective>)
    pattern = r'<([a-zA-Z_][a-zA-Z0-9_-]*)[^>]*>'
    matches = re.findall(pattern, text)

    # Filter out closing tags and common HTML tags
    xml_tags = set()
    for tag in matches:
        # Skip if it's just whitespace or common HTML
        if tag and tag.lower() not in ['br', 'b', 'i', 'u', 'p', 'div', 'span', 'mark']:
            xml_tags.add(tag)

    return xml_tags


async def analyze_prompts():
    """Analyze all ETL prompts and create comparison table."""

    # Fetch all Call Summary prompts
    call_summary_prompts = await fetch_all(
        """
        SELECT name, description, system_prompt, user_prompt
        FROM prompts
        WHERE layer = 'call_summary_etl'
        ORDER BY name
        """
    )

    # Fetch all Key Themes prompts
    key_themes_prompts = await fetch_all(
        """
        SELECT name, description, system_prompt, user_prompt
        FROM prompts
        WHERE layer = 'key_themes_etl'
        ORDER BY name
        """
    )

    # Analyze each prompt
    prompt_analysis = {}

    print("="*100)
    print("ETL PROMPTS XML STRUCTURE ANALYSIS")
    print("="*100)

    print("\n" + "="*100)
    print("CALL SUMMARY ETL PROMPTS")
    print("="*100)

    for row in call_summary_prompts:
        name = row['name']
        system_tags = extract_xml_tags(row['system_prompt'])
        user_tags = extract_xml_tags(row['user_prompt'])

        all_tags = sorted(system_tags | user_tags)
        prompt_analysis[f"call_summary:{name}"] = {
            'etl': 'Call Summary',
            'prompt': name,
            'tags': all_tags,
            'system_tags': sorted(system_tags),
            'user_tags': sorted(user_tags)
        }

        print(f"\n{name}:")
        print(f"  System prompt tags: {', '.join(sorted(system_tags)) if system_tags else 'None'}")
        print(f"  User prompt tags: {', '.join(sorted(user_tags)) if user_tags else 'None'}")

    print("\n" + "="*100)
    print("KEY THEMES ETL PROMPTS")
    print("="*100)

    for row in key_themes_prompts:
        name = row['name']
        system_tags = extract_xml_tags(row['system_prompt'])
        user_tags = extract_xml_tags(row['user_prompt'])

        all_tags = sorted(system_tags | user_tags)
        prompt_analysis[f"key_themes:{name}"] = {
            'etl': 'Key Themes',
            'prompt': name,
            'tags': all_tags,
            'system_tags': sorted(system_tags),
            'user_tags': sorted(user_tags)
        }

        print(f"\n{name}:")
        print(f"  System prompt tags: {', '.join(sorted(system_tags)) if system_tags else 'None'}")
        print(f"  User prompt tags: {', '.join(sorted(user_tags)) if user_tags else 'None'}")

    # Create comprehensive comparison table
    print("\n" + "="*100)
    print("COMPREHENSIVE XML TAG COMPARISON TABLE")
    print("="*100)

    # Get all unique tags across all prompts
    all_unique_tags = set()
    for data in prompt_analysis.values():
        all_unique_tags.update(data['tags'])

    all_unique_tags = sorted(all_unique_tags)

    print(f"\nTotal unique XML tags found: {len(all_unique_tags)}")
    print(f"Tags: {', '.join(all_unique_tags)}")

    # Create table header
    print("\n" + "-"*100)
    print(f"{'ETL':<15} {'Prompt':<25} {'XML Tags Used':<60}")
    print("-"*100)

    # Print each prompt's tags
    for key, data in sorted(prompt_analysis.items()):
        etl = data['etl']
        prompt = data['prompt']
        tags = ', '.join(data['tags']) if data['tags'] else 'None'

        # Wrap long tag lists
        if len(tags) > 55:
            tags_display = tags[:52] + "..."
        else:
            tags_display = tags

        print(f"{etl:<15} {prompt:<25} {tags_display:<60}")

    # Create detailed tag matrix
    print("\n" + "="*100)
    print("DETAILED TAG USAGE MATRIX")
    print("="*100)
    print(f"\nLegend: ✓ = tag present, - = tag absent")

    # Create matrix header
    header = f"{'Tag':<30}"
    for key in sorted(prompt_analysis.keys()):
        data = prompt_analysis[key]
        short_name = f"{data['etl'][:2]}:{data['prompt'][:8]}"
        header += f" {short_name:<12}"
    print("\n" + header)
    print("-" * len(header))

    # Create matrix rows
    for tag in all_unique_tags:
        row = f"{tag:<30}"
        for key in sorted(prompt_analysis.keys()):
            data = prompt_analysis[key]
            marker = "✓" if tag in data['tags'] else "-"
            row += f" {marker:<12}"
        print(row)

    # Summary statistics
    print("\n" + "="*100)
    print("SUMMARY STATISTICS")
    print("="*100)

    for key, data in sorted(prompt_analysis.items()):
        print(f"\n{data['etl']} - {data['prompt']}:")
        print(f"  Total unique tags: {len(data['tags'])}")
        print(f"  Tags: {', '.join(data['tags']) if data['tags'] else 'None'}")


if __name__ == "__main__":
    asyncio.run(analyze_prompts())
