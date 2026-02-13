"""
Push ETL prompt markdown files to the local PostgreSQL prompts table.

Reads each prompt markdown file, parses out the system_prompt, user_prompt,
and tool_definition sections, then INSERTs as a new row in the prompts table.
The prompt_manager uses MAX(updated_at) to select the latest version at runtime.

Usage:
    source venv/bin/activate
    python scripts/push_etl_prompts.py
"""

import json
import re
import sys
from pathlib import Path

# Add project root to path so we can import aegis modules
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from aegis.utils.settings import config  # noqa: E402


# ---------------------------------------------------------------------------
# Prompt file definitions
# ---------------------------------------------------------------------------

PROMPT_FILES = [
    {
        "layer": "call_summary_etl",
        "name": "research_plan",
        "path": "src/aegis/etls/call_summary/documentation/prompts/research_plan_prompt.md",
        "description": "Comprehensive research plan with content mapping and extraction strategy",
    },
    {
        "layer": "call_summary_etl",
        "name": "category_extraction",
        "path": "src/aegis/etls/call_summary/documentation/prompts/category_extraction_prompt.md",
        "description": "Category extraction with chain-of-thought reasoning and research-plan-based dedup",
    },
    {
        "layer": "call_summary_etl",
        "name": "deduplication",
        "path": "src/aegis/etls/call_summary/documentation/prompts/deduplication_prompt.md",
        "description": "Cross-category deduplication of statements and evidence",
    },
    {
        "layer": "key_themes_etl",
        "name": "theme_extraction",
        "path": "src/aegis/etls/key_themes/documentation/prompts/theme_extraction_prompt.md",
        "description": "Q&A validation and classification into predefined categories",
    },
    {
        "layer": "key_themes_etl",
        "name": "html_formatting",
        "path": "src/aegis/etls/key_themes/documentation/prompts/html_formatting_prompt.md",
        "description": "HTML formatting of Q&A content for executive briefing documents",
    },
    {
        "layer": "key_themes_etl",
        "name": "theme_grouping",
        "path": "src/aegis/etls/key_themes/documentation/prompts/theme_grouping_prompt.md",
        "description": "Review category classifications, regroup, and create dynamic titles",
    },
]


# ---------------------------------------------------------------------------
# Markdown parsing
# ---------------------------------------------------------------------------


def parse_prompt_markdown(filepath: str) -> dict:
    """
    Parse a prompt markdown file and extract components.

    Extracts:
    - version from ## Metadata section
    - system_prompt from ## System Prompt section (between ``` delimiters)
    - user_prompt from ## User Prompt section (between ``` delimiters)
    - tool_definition from ## Tool Definition section (between ```json delimiters)

    Args:
        filepath: Path to the markdown file

    Returns:
        Dict with version, system_prompt, user_prompt, tool_definition keys
    """
    text = Path(filepath).read_text(encoding="utf-8")

    # Extract version from metadata
    version_match = re.search(r"\*\*Version\*\*:\s*(\S+)", text)
    if not version_match:
        raise ValueError(f"No version found in {filepath}")
    version = version_match.group(1)

    # Extract system prompt (content between ``` after ## System Prompt)
    system_prompt = _extract_section(text, "System Prompt")
    if not system_prompt:
        raise ValueError(f"No system prompt found in {filepath}")

    # Extract user prompt
    user_prompt = _extract_section(text, "User Prompt")
    if not user_prompt:
        raise ValueError(f"No user prompt found in {filepath}")

    # Extract tool definition (may not exist for all prompts)
    tool_definition = _extract_tool_definition(text)

    return {
        "version": version,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "tool_definition": tool_definition,
    }


def _extract_section(text: str, section_name: str) -> str:
    """Extract content between ``` delimiters after a ## section header."""
    # Find the section header
    pattern = rf"## {re.escape(section_name)}\s*\n"
    match = re.search(pattern, text)
    if not match:
        return None

    # Find the first ``` block after the section header
    rest = text[match.end():]
    # Match ```<optional language>\n ... ```
    block_match = re.search(r"```(?:\w*)\n(.*?)```", rest, re.DOTALL)
    if not block_match:
        return None

    return block_match.group(1).strip()


def _extract_tool_definition(text: str) -> dict:
    """Extract tool definition JSON from ## Tool Definition section."""
    # Find the section
    match = re.search(r"## Tool Definition\s*\n", text)
    if not match:
        return None

    rest = text[match.end():]

    # Check for "does not use a tool definition" or similar
    no_tool_match = re.search(r"does not use a tool definition", rest[:200], re.IGNORECASE)
    if no_tool_match:
        return None

    # Find ```json block
    block_match = re.search(r"```json\n(.*?)```", rest, re.DOTALL)
    if not block_match:
        return None

    json_text = block_match.group(1).strip()

    # Handle {{ and }} escape sequences (used in .format()-compatible prompts)
    # Tool definitions are stored as raw JSON, not processed by .format()
    json_text = json_text.replace("{{", "{").replace("}}", "}")

    try:
        return json.loads(json_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in tool definition: {e}") from e


# ---------------------------------------------------------------------------
# Database insertion
# ---------------------------------------------------------------------------


def insert_prompt(conn, prompt_def: dict, parsed: dict) -> None:
    """
    Insert a prompt row into the prompts table.

    Args:
        conn: psycopg2 connection
        prompt_def: Dict with layer, name, description
        parsed: Dict with version, system_prompt, user_prompt, tool_definition
    """
    sql = """
        INSERT INTO prompts (model, layer, name, description, system_prompt,
                             user_prompt, tool_definition, version,
                             created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
    """
    tool_json = json.dumps(parsed["tool_definition"]) if parsed["tool_definition"] else None

    params = (
        "aegis",
        prompt_def["layer"],
        prompt_def["name"],
        prompt_def["description"],
        parsed["system_prompt"],
        parsed["user_prompt"],
        tool_json,
        parsed["version"],
    )

    cursor = conn.cursor()
    cursor.execute(sql, params)
    cursor.close()


def get_current_db_version(conn, layer: str, name: str) -> str:
    """Get the current version of a prompt in the DB."""
    sql = """
        SELECT version FROM prompts
        WHERE model = 'aegis' AND layer = %s AND name = %s
        ORDER BY updated_at DESC
        LIMIT 1
    """
    cursor = conn.cursor()
    cursor.execute(sql, (layer, name))
    row = cursor.fetchone()
    cursor.close()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    """Parse all prompt markdown files and push to local Postgres."""
    import psycopg2

    db_url = (
        f"host={config.postgres_host} port={config.postgres_port} "
        f"dbname={config.postgres_database} user={config.postgres_user} "
        f"password={config.postgres_password}"
    )

    conn = psycopg2.connect(db_url)
    conn.autocommit = False

    try:
        print("=" * 70)
        print("ETL Prompt Push — Local PostgreSQL")
        print("=" * 70)

        for prompt_def in PROMPT_FILES:
            filepath = PROJECT_ROOT / prompt_def["path"]
            print(f"\n--- {prompt_def['layer']}/{prompt_def['name']} ---")

            # Parse the markdown
            parsed = parse_prompt_markdown(str(filepath))
            print(f"  File version:  {parsed['version']}")
            print(f"  System prompt: {len(parsed['system_prompt'])} chars")
            print(f"  User prompt:   {len(parsed['user_prompt'])} chars")
            print(f"  Tool def:      {'yes' if parsed['tool_definition'] else 'none'}")

            # Check current DB version
            db_version = get_current_db_version(
                conn, prompt_def["layer"], prompt_def["name"]
            )
            print(f"  DB version:    {db_version or 'MISSING'}")

            # Insert
            insert_prompt(conn, prompt_def, parsed)
            print(f"  -> INSERTED v{parsed['version']}")

        conn.commit()
        print("\n" + "=" * 70)
        print("All 6 prompts pushed successfully.")
        print("=" * 70)

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}")
        print("Transaction rolled back. No changes applied.")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
