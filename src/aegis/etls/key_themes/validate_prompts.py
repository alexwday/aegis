"""
Validate key_themes ETL prompts in PostgreSQL against markdown templates.

This script:
1. Loads prompts from PostgreSQL for key_themes_etl layer
2. Loads corresponding markdown template files
3. Validates that the stored prompts match the templates
4. Reports any discrepancies

Usage:
    python -m aegis.etls.key_themes.validate_prompts
"""

import asyncio
import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional
from sqlalchemy import text

from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger

setup_logging()
logger = get_logger()


def parse_markdown_prompt(filepath: str) -> Dict[str, Any]:
    """
    Parse a markdown prompt file to extract metadata, system prompt, and tool definition.

    Args:
        filepath: Path to markdown file

    Returns:
        Dictionary with 'metadata', 'system_prompt', 'tool_definition' keys
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    result = {
        'metadata': {},
        'system_prompt': None,
        'tool_definition': None,
    }

    # Extract metadata from header
    lines = content.split('\n')
    in_metadata = False
    for line in lines:
        if line.strip() == '## Metadata':
            in_metadata = True
            continue
        if in_metadata:
            if line.strip().startswith('##') and line.strip() != '## Metadata':
                break
            if line.strip().startswith('- **'):
                # Parse "- **Key**: Value"
                line_content = line.strip()[2:].strip()  # Remove "- " prefix
                if line_content.startswith('**') and '**:' in line_content:
                    # Split on first **: occurrence
                    key_part, value_part = line_content.split('**:', 1)
                    key = key_part.strip('*').strip().lower().replace(' ', '_')
                    value = value_part.strip()
                    result['metadata'][key] = value

    # Extract system prompt
    system_start = content.find('## System Prompt\n\n```')
    if system_start != -1:
        system_content_start = content.find('```', system_start) + 3
        system_content_end = content.find('```', system_content_start)
        if system_content_end != -1:
            result['system_prompt'] = content[system_content_start:system_content_end].strip()

    # Extract tool definition
    tool_start = content.find('## Tool Definition\n\n```json')
    if tool_start != -1:
        tool_content_start = content.find('```json', tool_start) + 7
        tool_content_end = content.find('```', tool_content_start)
        if tool_content_end != -1:
            tool_json = content[tool_content_start:tool_content_end].strip()
            try:
                result['tool_definition'] = json.loads(tool_json)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse tool definition JSON in {filepath}: {e}")
                result['tool_definition'] = None

    return result


async def get_prompt_from_db(layer: str, name: str, model: str = 'aegis') -> Optional[Dict[str, Any]]:
    """
    Retrieve latest prompt from PostgreSQL.

    Args:
        layer: Prompt layer (e.g., 'key_themes_etl')
        name: Prompt name (e.g., 'theme_extraction')
        model: Model name (default: 'aegis')

    Returns:
        Dictionary with prompt data or None if not found
    """
    async with get_connection() as conn:
        result = await conn.execute(
            text("""
                SELECT
                    id,
                    model,
                    name,
                    layer,
                    version,
                    system_prompt,
                    user_prompt,
                    tool_definition,
                    uses_global,
                    created_at,
                    updated_at
                FROM prompts
                WHERE model = :model AND layer = :layer AND name = :name
                ORDER BY updated_at DESC
                LIMIT 1
            """),
            {'model': model, 'layer': layer, 'name': name}
        )
        row = result.fetchone()

        if not row:
            return None

        return {
            'id': row[0],
            'model': row[1],
            'name': row[2],
            'layer': row[3],
            'version': row[4],
            'system_prompt': row[5],
            'user_prompt': row[6],
            'tool_definition': row[7],
            'uses_global': row[8],
            'created_at': row[9],
            'updated_at': row[10],
        }


def compare_system_prompts(template: str, db: str) -> Dict[str, Any]:
    """
    Compare system prompts from template and database.

    Args:
        template: System prompt from markdown template
        db: System prompt from database

    Returns:
        Dictionary with 'matches', 'differences' keys
    """
    if template == db:
        return {'matches': True, 'differences': []}

    # Find differences
    template_lines = template.split('\n')
    db_lines = db.split('\n') if db else []

    differences = []
    max_lines = max(len(template_lines), len(db_lines))

    for i in range(max_lines):
        template_line = template_lines[i] if i < len(template_lines) else '<missing>'
        db_line = db_lines[i] if i < len(db_lines) else '<missing>'

        if template_line != db_line:
            differences.append({
                'line': i + 1,
                'template': template_line[:100],  # First 100 chars
                'db': db_line[:100],
            })

            # Only report first 5 differences to keep output manageable
            if len(differences) >= 5:
                differences.append({'note': f'... and {max_lines - i - 1} more lines'})
                break

    return {
        'matches': False,
        'differences': differences,
        'template_lines': len(template_lines),
        'db_lines': len(db_lines),
    }


def compare_tool_definitions(template: Dict, db: Dict) -> Dict[str, Any]:
    """
    Compare tool definitions from template and database.

    Args:
        template: Tool definition from markdown template
        db: Tool definition from database

    Returns:
        Dictionary with 'matches', 'differences' keys
    """
    if not template and not db:
        return {'matches': True, 'note': 'Both None (no tool definition)'}

    if not template or not db:
        return {
            'matches': False,
            'note': f"Template: {type(template)}, DB: {type(db)}"
        }

    # Normalize JSON for comparison
    template_json = json.dumps(template, sort_keys=True, indent=2)
    db_json = json.dumps(db, sort_keys=True, indent=2)

    if template_json == db_json:
        return {'matches': True}

    return {
        'matches': False,
        'template_preview': template_json[:500],
        'db_preview': db_json[:500],
    }


async def validate_prompt(
    name: str,
    template_file: str,
    layer: str = 'key_themes_etl'
) -> Dict[str, Any]:
    """
    Validate a single prompt against its template.

    Args:
        name: Prompt name in database
        template_file: Markdown template filename
        layer: Prompt layer

    Returns:
        Validation result dictionary
    """
    # Get paths
    etl_dir = Path(__file__).parent
    template_path = etl_dir / 'documentation' / 'prompts' / template_file

    # Load template
    if not template_path.exists():
        return {
            'name': name,
            'status': 'error',
            'message': f'Template file not found: {template_path}',
        }

    template_data = parse_markdown_prompt(str(template_path))

    # Load from database
    db_data = await get_prompt_from_db(layer, name)

    if not db_data:
        return {
            'name': name,
            'status': 'missing',
            'message': f'Prompt not found in database (layer={layer}, name={name})',
            'template_version': template_data['metadata'].get('version'),
        }

    # Compare versions
    template_version = template_data['metadata'].get('version')
    db_version = db_data.get('version')

    version_match = template_version == db_version

    # Compare system prompts
    system_comparison = compare_system_prompts(
        template_data['system_prompt'] or '',
        db_data['system_prompt'] or ''
    )

    # Compare tool definitions
    tool_comparison = compare_tool_definitions(
        template_data['tool_definition'],
        db_data['tool_definition']
    )

    # Determine overall status
    if version_match and system_comparison['matches'] and tool_comparison['matches']:
        status = 'valid'
        message = 'Prompt matches template'
    else:
        status = 'mismatch'
        issues = []
        if not version_match:
            issues.append(f'version mismatch (template: {template_version}, db: {db_version})')
        if not system_comparison['matches']:
            issues.append('system prompt differs')
        if not tool_comparison['matches']:
            issues.append('tool definition differs')
        message = 'Validation failed: ' + ', '.join(issues)

    return {
        'name': name,
        'status': status,
        'message': message,
        'template_version': template_version,
        'db_version': db_version,
        'db_id': db_data.get('id'),
        'db_updated_at': str(db_data.get('updated_at')),
        'system_prompt_comparison': system_comparison,
        'tool_definition_comparison': tool_comparison,
    }


async def validate_all_prompts() -> List[Dict[str, Any]]:
    """
    Validate all key_themes ETL prompts.

    Returns:
        List of validation results
    """
    prompts_to_validate = [
        ('theme_extraction', 'theme_extraction_prompt.md'),
        ('html_formatting', 'html_formatting_prompt.md'),
        ('theme_grouping', 'theme_grouping_prompt.md'),
    ]

    results = []
    for name, template_file in prompts_to_validate:
        logger.info(f"Validating prompt: {name}")
        result = await validate_prompt(name, template_file)
        results.append(result)

    return results


def print_results(results: List[Dict[str, Any]]):
    """
    Print validation results in a readable format.

    Args:
        results: List of validation result dictionaries
    """
    print("\n" + "="*80)
    print("KEY THEMES ETL PROMPT VALIDATION REPORT")
    print("="*80 + "\n")

    valid_count = sum(1 for r in results if r['status'] == 'valid')
    mismatch_count = sum(1 for r in results if r['status'] == 'mismatch')
    missing_count = sum(1 for r in results if r['status'] == 'missing')
    error_count = sum(1 for r in results if r['status'] == 'error')

    print(f"Summary:")
    print(f"  ✓ Valid:    {valid_count}")
    print(f"  ⚠ Mismatch: {mismatch_count}")
    print(f"  ✗ Missing:  {missing_count}")
    print(f"  ⚠ Errors:   {error_count}")
    print(f"  Total:      {len(results)}\n")

    print("-" * 80)

    for result in results:
        status_icon = {
            'valid': '✓',
            'mismatch': '⚠',
            'missing': '✗',
            'error': '✗'
        }.get(result['status'], '?')

        print(f"\n{status_icon} {result['name'].upper()}")
        print(f"  Status: {result['status']}")
        print(f"  Message: {result['message']}")

        if result.get('template_version'):
            print(f"  Template Version: {result['template_version']}")
        if result.get('db_version'):
            print(f"  Database Version: {result['db_version']}")
        if result.get('db_id'):
            print(f"  Database ID: {result['db_id']}")
        if result.get('db_updated_at'):
            print(f"  Last Updated: {result['db_updated_at']}")

        # Show system prompt comparison details for mismatches
        if result['status'] == 'mismatch':
            sys_comp = result.get('system_prompt_comparison', {})
            if not sys_comp.get('matches'):
                print(f"\n  System Prompt Issues:")
                print(f"    Template lines: {sys_comp.get('template_lines', '?')}")
                print(f"    Database lines: {sys_comp.get('db_lines', '?')}")

                if sys_comp.get('differences'):
                    print(f"    First differences:")
                    for diff in sys_comp['differences'][:3]:
                        if 'note' in diff:
                            print(f"      {diff['note']}")
                        else:
                            print(f"      Line {diff['line']}:")
                            print(f"        Template: {diff['template']}")
                            print(f"        Database: {diff['db']}")

            tool_comp = result.get('tool_definition_comparison', {})
            if not tool_comp.get('matches'):
                print(f"\n  Tool Definition Issues:")
                if 'note' in tool_comp:
                    print(f"    {tool_comp['note']}")
                else:
                    print(f"    Tool definitions differ (check JSON structure)")

    print("\n" + "="*80)

    # Overall verdict
    if valid_count == len(results):
        print("✓ All prompts are valid and match templates")
    else:
        print(f"⚠ {mismatch_count + missing_count + error_count} prompt(s) need attention")

    print("="*80 + "\n")


async def main():
    """Main execution function."""
    print("\n🔍 Validating key_themes ETL prompts against templates...\n")

    try:
        results = await validate_all_prompts()
        print_results(results)

        # Exit with error code if any validation failed
        failed = sum(1 for r in results if r['status'] != 'valid')
        if failed > 0:
            exit(1)

    except Exception as e:
        logger.error(f"Validation failed with error: {e}", exc_info=True)
        print(f"\n✗ Validation failed: {e}\n")
        exit(1)


if __name__ == '__main__':
    asyncio.run(main())
