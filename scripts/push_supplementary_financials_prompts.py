"""
Push only the supplementary financials retriever prompts to PostgreSQL.

This script is intentionally scoped to the new staged supplementary financials
subagent prompts:

- supplementary_financials/query_prep
- supplementary_financials/rerank
- supplementary_financials/research

Usage:
    source venv/bin/activate
    python scripts/push_supplementary_financials_prompts.py --dry-run
    python scripts/push_supplementary_financials_prompts.py
"""

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from aegis.utils.settings import config  # noqa: E402


PROMPT_FILES = [
    {
        "layer": "supplementary_financials",
        "name": "query_prep",
        "path": ("src/aegis/model/subagents/supplementary_financials/prompts/query_prep.yaml"),
        "description": ("Decomposes supplementary financials queries for hybrid retrieval"),
    },
    {
        "layer": "supplementary_financials",
        "name": "rerank",
        "path": "src/aegis/model/subagents/supplementary_financials/prompts/rerank.yaml",
        "description": "Filters clearly irrelevant supplementary financials chunks",
    },
    {
        "layer": "supplementary_financials",
        "name": "research",
        "path": "src/aegis/model/subagents/supplementary_financials/prompts/research.yaml",
        "description": "Extracts cited supplementary financials findings",
    },
]


def parse_prompt_yaml(filepath: Path) -> Dict[str, Any]:
    """Parse one supplementary financials prompt YAML file."""
    data = yaml.safe_load(filepath.read_text(encoding="utf-8")) or {}
    system_prompt = data.get("system_prompt")
    user_prompt = data.get("user_prompt")
    tool_definition = (
        data.get("tools") or data.get("tool_definition") or data.get("tool_definitions")
    )

    if not system_prompt:
        raise ValueError(f"No system_prompt found in {filepath}")
    if not user_prompt:
        raise ValueError(f"No user_prompt found in {filepath}")
    if not tool_definition:
        raise ValueError(f"No tools/tool_definition found in {filepath}")
    if not isinstance(tool_definition, (list, dict)):
        raise ValueError(f"Tool definition must be a list or dict in {filepath}")

    return {
        "version": str(data.get("version", "1.0")),
        "description": data.get("description"),
        "system_prompt": str(system_prompt).strip(),
        "user_prompt": str(user_prompt).strip(),
        "tool_definition": tool_definition,
        "uses_global": data.get("uses_global") or [],
    }


def get_db_connection() -> Any:
    """Create a psycopg2 connection using Aegis settings."""
    import psycopg2

    db_url = (
        f"host={config.postgres_host} port={config.postgres_port} "
        f"dbname={config.postgres_database} user={config.postgres_user} "
        f"password={config.postgres_password}"
    )
    return psycopg2.connect(db_url)


def db_target_label() -> str:
    """Return a password-free database target label for script output."""
    return (
        f"{config.postgres_user}@{config.postgres_host}:"
        f"{config.postgres_port}/{config.postgres_database}"
    )


def get_current_db_version(conn: Any, layer: str, name: str) -> Optional[str]:
    """Return the latest DB version for one prompt, if present."""
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


def upsert_prompt(conn: Any, prompt_def: Dict[str, str], parsed: Dict[str, Any]) -> str:
    """Insert or update one prompt row for the parsed prompt version."""
    from psycopg2.extras import Json

    description = parsed.get("description") or prompt_def.get("description")
    tool_json = Json(parsed["tool_definition"])
    identity = (
        "aegis",
        prompt_def["layer"],
        prompt_def["name"],
        parsed["version"],
    )
    update_sql = """
        UPDATE prompts
        SET
            description = %s,
            system_prompt = %s,
            user_prompt = %s,
            tool_definition = %s,
            uses_global = %s,
            updated_at = NOW()
        WHERE model = %s AND layer = %s AND name = %s AND version = %s
        RETURNING id
    """
    update_params = (
        description,
        parsed["system_prompt"],
        parsed["user_prompt"],
        tool_json,
        parsed.get("uses_global") or [],
        *identity,
    )
    with conn.cursor() as cursor:
        cursor.execute(update_sql, update_params)
        updated_rows = cursor.fetchall()
        if updated_rows:
            return "UPDATED"

    insert_sql = """
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
    """
    insert_params = (
        "aegis",
        prompt_def["layer"],
        prompt_def["name"],
        description,
        parsed["system_prompt"],
        parsed["user_prompt"],
        tool_json,
        parsed.get("uses_global") or [],
        parsed["version"],
    )
    with conn.cursor() as cursor:
        cursor.execute(insert_sql, insert_params)
    return "INSERTED"


def selected_prompt_defs(only: Optional[List[str]]) -> List[Dict[str, str]]:
    """Filter prompt definitions by prompt name or layer/name selector."""
    if not only:
        return PROMPT_FILES

    selected = []
    requested = set(only)
    for prompt_def in PROMPT_FILES:
        name = prompt_def["name"]
        layer_name = f"{prompt_def['layer']}/{name}"
        if name in requested or layer_name in requested:
            selected.append(prompt_def)

    matched = {item["name"] for item in selected}
    matched.update(f"{item['layer']}/{item['name']}" for item in selected)
    missing = requested - matched
    if missing:
        raise ValueError(f"Unknown prompt selector(s): {', '.join(sorted(missing))}")
    return selected


def print_prompt_summary(
    prompt_def: Dict[str, str],
    parsed: Dict[str, Any],
    current_version: Optional[str],
) -> None:
    """Print a compact summary for one prompt file."""
    print(f"\n--- {prompt_def['layer']}/{prompt_def['name']} ---")
    print(f"  Source:        {prompt_def['path']}")
    print(f"  File version:  {parsed['version']}")
    print(f"  DB version:    {current_version or 'MISSING'}")
    print(f"  Uses global:   {', '.join(parsed.get('uses_global') or []) or 'none'}")
    print(f"  System prompt: {len(parsed['system_prompt'])} chars")
    print(f"  User prompt:   {len(parsed['user_prompt'])} chars")
    print("  Tool def:      yes")


def main() -> None:
    """Load supplementary financials prompt files into the prompts table."""
    parser = argparse.ArgumentParser(
        description="Push supplementary financials retriever prompts to PostgreSQL"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Connect and report prompts without inserting or updating rows",
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Parse local YAML and report prompts without opening a DB connection",
    )
    parser.add_argument(
        "--only",
        action="append",
        help=(
            "Only load one prompt, e.g. --only query_prep or "
            "--only supplementary_financials/query_prep. Can repeat."
        ),
    )
    args = parser.parse_args()
    prompt_defs = selected_prompt_defs(args.only)
    parsed_prompts = [
        (prompt_def, parse_prompt_yaml(PROJECT_ROOT / prompt_def["path"]))
        for prompt_def in prompt_defs
    ]

    print("=" * 78)
    print("Supplementary Financials Prompt Push")
    print("=" * 78)
    print(f"Target DB: {db_target_label()}")

    if args.parse_only:
        for prompt_def, parsed in parsed_prompts:
            print_prompt_summary(prompt_def, parsed, current_version=None)
            print("  -> PARSE ONLY")
        print("\nParse-only complete. No DB connection opened.")
        print("=" * 78)
        return

    conn = get_db_connection()
    conn.autocommit = False
    try:
        for prompt_def, parsed in parsed_prompts:
            current_version = get_current_db_version(
                conn,
                prompt_def["layer"],
                prompt_def["name"],
            )
            print_prompt_summary(prompt_def, parsed, current_version)

            if args.dry_run:
                print("  -> DRY RUN")
            else:
                status = upsert_prompt(conn, prompt_def, parsed)
                print(f"  -> {status} v{parsed['version']}")

        if args.dry_run:
            conn.rollback()
            print("\nDry run complete. No rows inserted or updated.")
        else:
            conn.commit()
            print(f"\nPushed {len(parsed_prompts)} prompt row(s).")
        print("=" * 78)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
