"""
Push Aegis runtime prompt files to the local PostgreSQL prompts table.

This loads prompts from src/aegis/model/prompts for the live agent workflow:
router, clarifier, planner, response, summarizer, global context prompts, and
subagent prompts used by the model runtime.

Usage:
    source venv/bin/activate
    python scripts/push_aegis_prompts.py --dry-run
    python scripts/push_aegis_prompts.py
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from aegis.utils.settings import config  # noqa: E402


PROMPT_FILES = [
    # Aegis orchestration agents.
    {
        "layer": "aegis",
        "name": "router",
        "path": "src/aegis/model/prompts/aegis/router.yaml",
        "description": "Routes user queries to direct response or research workflow",
    },
    {
        "layer": "aegis",
        "name": "clarifier_banks",
        "path": "src/aegis/model/prompts/aegis/clarifier_banks.yaml",
        "description": "Extracts requested banks and peer groups",
    },
    {
        "layer": "aegis",
        "name": "clarifier_periods",
        "path": "src/aegis/model/prompts/aegis/clarifier_periods.yaml",
        "description": "Extracts fiscal years and quarters",
    },
    {
        "layer": "aegis",
        "name": "planner",
        "path": "src/aegis/model/prompts/aegis/planner.yaml",
        "description": "Selects databases for research workflow",
    },
    {
        "layer": "aegis",
        "name": "response",
        "path": "src/aegis/model/prompts/aegis/response.md",
        "description": "Generates direct responses that do not require data retrieval",
    },
    {
        "layer": "aegis",
        "name": "summarizer",
        "path": "src/aegis/model/prompts/aegis/summarizer.yaml",
        "description": "Synthesizes subagent research outputs",
    },
    # Global prompts. fiscal.py is dynamic code and is intentionally not loaded.
    {
        "layer": "global",
        "name": "project",
        "path": "src/aegis/model/prompts/global/project.md",
        "description": "Aegis project and user context",
    },
    {
        "layer": "global",
        "name": "restrictions",
        "path": "src/aegis/model/prompts/global/restrictions.md",
        "description": "Safety, scope, citation, and response restrictions",
    },
    {
        "layer": "global",
        "name": "database",
        "path": "src/aegis/model/prompts/global/database.yaml",
        "description": "Available database descriptions",
    },
    # Subagent prompts used by the live runtime.
    {
        "layer": "transcripts",
        "name": "method_selection",
        "path": "src/aegis/model/prompts/subagents/transcripts/method_selection.yaml",
        "description": "Selects transcript retrieval method",
    },
    {
        "layer": "transcripts",
        "name": "reranking",
        "path": "src/aegis/model/prompts/subagents/transcripts/reranking.yaml",
        "description": "Filters irrelevant transcript similarity results",
    },
    {
        "layer": "transcripts",
        "name": "research_synthesis",
        "path": "src/aegis/model/prompts/subagents/transcripts/research_synthesis.yaml",
        "description": "Synthesizes transcript retrieval results",
    },
    {
        "layer": "transcripts",
        "name": "transcripts",
        "path": "src/aegis/model/prompts/subagents/transcripts/transcripts.yaml",
        "description": "Legacy transcripts subagent prompt",
    },
    {
        "layer": "supplementary",
        "name": "supplementary",
        "path": "src/aegis/model/prompts/supplementary/supplementary.yaml",
        "description": "Legacy supplementary/IR benchmarking subagent prompt",
    },
    {
        "layer": "rts",
        "name": "rts",
        "path": "src/aegis/model/prompts/rts/rts.yaml",
        "description": "Report to Shareholders subagent prompt",
    },
    {
        "layer": "pillar3",
        "name": "pillar3",
        "path": "src/aegis/model/prompts/pillar3/pillar3.yaml",
        "description": "Pillar 3 subagent prompt",
    },
]


def parse_prompt_file(filepath: Path) -> Dict[str, Any]:
    """Parse a YAML or Markdown prompt file."""

    if filepath.suffix.lower() in {".yaml", ".yml"}:
        return parse_prompt_yaml(filepath)
    if filepath.suffix.lower() == ".md":
        return parse_prompt_markdown(filepath)
    raise ValueError(f"Unsupported prompt file type: {filepath}")


def parse_prompt_yaml(filepath: Path) -> Dict[str, Any]:
    """Parse a YAML prompt file into prompts-table columns."""

    data = yaml.safe_load(filepath.read_text(encoding="utf-8")) or {}

    system_prompt = first_present(
        data,
        ["system_prompt", "system_prompt_template", "system_template", "content"],
    )
    if not system_prompt and "databases" in data:
        system_prompt = yaml.safe_dump(data, sort_keys=False)
    if not system_prompt:
        raise ValueError(f"No system/content prompt found in {filepath}")

    user_prompt = first_present(
        data,
        ["user_prompt", "user_prompt_template", "user_template"],
    )

    tool_definition = data.get("tool_definition")
    if tool_definition is None:
        tool_definition = data.get("tool_definitions")

    return {
        "version": str(data.get("version", "1.0.0")),
        "system_prompt": str(system_prompt).strip(),
        "user_prompt": str(user_prompt).strip() if user_prompt else None,
        "tool_definition": tool_definition,
        "uses_global": data.get("uses_global") or [],
        "description": data.get("description"),
    }


def parse_prompt_markdown(filepath: Path) -> Dict[str, Any]:
    """Parse a Markdown prompt file into prompts-table columns."""

    text = filepath.read_text(encoding="utf-8")

    version_match = re.search(r"\*\*Version\*\*:\s*(\S+)", text)
    uses_global_match = re.search(r"\*\*Uses Global\*\*:\s*([^\n]+)", text)

    system_prompt = extract_markdown_section(text, ["System Prompt"])
    if not system_prompt:
        system_prompt = markdown_body_without_metadata(text)
    if not system_prompt:
        raise ValueError(f"No prompt content found in {filepath}")

    user_prompt = extract_markdown_section(text, ["User Prompt", "User Prompt Template"])
    tool_definition = extract_tool_definition(text)

    return {
        "version": version_match.group(1) if version_match else "1.0.0",
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "tool_definition": tool_definition,
        "uses_global": parse_uses_global(uses_global_match.group(1) if uses_global_match else ""),
        "description": None,
    }


def markdown_body_without_metadata(text: str) -> str:
    """Return plain Markdown body for global docs that are themselves prompts."""

    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("**Version**:"):
            continue
        if stripped.startswith("**Last Updated**:"):
            continue
        if stripped.startswith("**Uses Global**:"):
            continue
        if stripped == "---":
            continue
        lines.append(line)

    return "\n".join(lines).strip()


def first_present(data: Dict[str, Any], keys: List[str]) -> Optional[Any]:
    """Return the first non-empty value for the given keys."""

    for key in keys:
        value = data.get(key)
        if value:
            return value
    return None


def extract_markdown_section(text: str, section_names: List[str]) -> Optional[str]:
    """Extract the first fenced block after one of the given section headings."""

    for section_name in section_names:
        pattern = rf"## {re.escape(section_name)}\s*\n"
        match = re.search(pattern, text)
        if not match:
            continue

        rest = text[match.end():]
        block_match = re.search(r"```(?:\w*)\n(.*?)```", rest, re.DOTALL)
        if block_match:
            return block_match.group(1).strip()

    return None


def extract_tool_definition(text: str) -> Optional[Any]:
    """Extract a JSON tool definition from Markdown, if present."""

    match = re.search(r"## Tool Definition\s*\n", text)
    if not match:
        return None

    rest = text[match.end():]
    if re.search(r"does not use a tool definition", rest[:200], re.IGNORECASE):
        return None

    block_match = re.search(r"```json\n(.*?)```", rest, re.DOTALL)
    if not block_match:
        return None

    json_text = block_match.group(1).strip().replace("{{", "{").replace("}}", "}")
    return json.loads(json_text)


def parse_uses_global(raw_value: str) -> List[str]:
    """Parse a comma-separated Uses Global metadata value."""

    return [item.strip() for item in raw_value.split(",") if item.strip()]


def get_db_connection():
    """Create a psycopg2 connection using Aegis settings."""

    import psycopg2

    db_url = (
        f"host={config.postgres_host} port={config.postgres_port} "
        f"dbname={config.postgres_database} user={config.postgres_user} "
        f"password={config.postgres_password}"
    )
    return psycopg2.connect(db_url)


def get_current_db_version(conn, layer: str, name: str) -> Optional[str]:
    """Return the latest DB version for a prompt, if present."""

    sql = """
        SELECT version
        FROM prompts
        WHERE model = 'aegis' AND layer = %s AND name = %s
        ORDER BY updated_at DESC
        LIMIT 1
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (layer, name))
        row = cursor.fetchone()
    return row[0] if row else None


def insert_prompt(conn, prompt_def: Dict[str, str], parsed: Dict[str, Any]) -> None:
    """Insert a prompt as a new latest row."""

    from psycopg2.extras import Json

    sql = """
        INSERT INTO prompts (
            model,
            layer,
            name,
            description,
            system_prompt,
            user_prompt,
            tool_definition,
            uses_global,
            version,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (model, layer, name, version)
        DO UPDATE SET
            description = EXCLUDED.description,
            system_prompt = EXCLUDED.system_prompt,
            user_prompt = EXCLUDED.user_prompt,
            tool_definition = EXCLUDED.tool_definition,
            uses_global = EXCLUDED.uses_global,
            updated_at = NOW()
    """

    description = parsed.get("description") or prompt_def.get("description")
    tool_definition = parsed.get("tool_definition")
    tool_json = Json(tool_definition) if tool_definition is not None else None

    params = (
        "aegis",
        prompt_def["layer"],
        prompt_def["name"],
        description,
        parsed["system_prompt"],
        parsed.get("user_prompt"),
        tool_json,
        parsed.get("uses_global") or [],
        parsed["version"],
    )

    with conn.cursor() as cursor:
        cursor.execute(sql, params)


def selected_prompt_defs(only: Optional[List[str]]) -> List[Dict[str, str]]:
    """Filter prompt definitions by layer/name values."""

    if not only:
        return PROMPT_FILES

    requested = set(only)
    selected = []
    for prompt_def in PROMPT_FILES:
        key = f"{prompt_def['layer']}/{prompt_def['name']}"
        if key in requested:
            selected.append(prompt_def)

    missing = requested - {f"{item['layer']}/{item['name']}" for item in selected}
    if missing:
        raise ValueError(f"Unknown prompt selector(s): {', '.join(sorted(missing))}")

    return selected


def main() -> None:
    """Load Aegis runtime prompts into the prompts table."""

    parser = argparse.ArgumentParser(description="Push Aegis runtime prompts to PostgreSQL")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and report prompts without inserting rows",
    )
    parser.add_argument(
        "--only",
        action="append",
        help="Only load one layer/name prompt, e.g. --only aegis/response. Can repeat.",
    )
    args = parser.parse_args()

    prompt_defs = selected_prompt_defs(args.only)

    conn = get_db_connection()
    conn.autocommit = False

    try:
        print("=" * 78)
        print("Aegis Runtime Prompt Push - Local PostgreSQL")
        print("=" * 78)

        for prompt_def in prompt_defs:
            filepath = PROJECT_ROOT / prompt_def["path"]
            parsed = parse_prompt_file(filepath)
            current_version = get_current_db_version(
                conn, prompt_def["layer"], prompt_def["name"]
            )

            print(f"\n--- {prompt_def['layer']}/{prompt_def['name']} ---")
            print(f"  Source:        {prompt_def['path']}")
            print(f"  File version:  {parsed['version']}")
            print(f"  DB version:    {current_version or 'MISSING'}")
            print(f"  Uses global:   {', '.join(parsed.get('uses_global') or []) or 'none'}")
            print(f"  System prompt: {len(parsed['system_prompt'])} chars")
            print(f"  User prompt:   {len(parsed.get('user_prompt') or '')} chars")
            print(f"  Tool def:      {'yes' if parsed.get('tool_definition') else 'none'}")

            if args.dry_run:
                print("  -> DRY RUN")
            else:
                insert_prompt(conn, prompt_def, parsed)
                print(f"  -> UPSERTED v{parsed['version']}")

        if args.dry_run:
            conn.rollback()
            print("\nDry run complete. No rows inserted.")
        else:
            conn.commit()
            print(f"\nUpserted {len(prompt_defs)} prompt row(s).")

        print("=" * 78)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
