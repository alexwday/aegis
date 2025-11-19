"""
Upload ETL Prompts to PostgreSQL Database

This script reads markdown-format prompts from ETL documentation folders
and uploads them to the aegis prompts table.

Usage:
    python scripts/upload_etl_prompts.py                    # Upload all ETLs
    python scripts/upload_etl_prompts.py --etl call_summary # Upload only call_summary
    python scripts/upload_etl_prompts.py --etl key_themes   # Upload only key_themes
"""

import asyncio
import argparse
import re
import json
from pathlib import Path
from sqlalchemy import text
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger

setup_logging()
logger = get_logger()

# ETL configurations
ETL_CONFIGS = {
    "call_summary": {
        "layer": "call_summary_etl",
        "prompts": [
            {
                "file": "src/aegis/etls/call_summary/documentation/prompts/research_plan_prompt.md",
                "name": "research_plan",
            },
            {
                "file": "src/aegis/etls/call_summary/documentation/prompts/category_extraction_prompt.md",
                "name": "category_extraction",
            },
        ],
    },
    "key_themes": {
        "layer": "key_themes_etl",
        "prompts": [
            {
                "file": "src/aegis/etls/key_themes/documentation/prompts/theme_extraction_prompt.md",
                "name": "theme_extraction",
            },
            {
                "file": "src/aegis/etls/key_themes/documentation/prompts/theme_grouping_prompt.md",
                "name": "theme_grouping",
            },
            {
                "file": "src/aegis/etls/key_themes/documentation/prompts/html_formatting_prompt.md",
                "name": "html_formatting",
            },
        ],
    },
}


def parse_markdown_prompt(file_path: Path) -> dict:
    """
    Parse markdown prompt file and extract metadata, system prompt, user prompt, and tool definition.

    Args:
        file_path: Path to markdown file

    Returns:
        Dict with 'metadata', 'system_prompt', 'user_prompt', and 'tool_definition'
    """
    with open(file_path, "r") as f:
        content = f.read()

    # Extract metadata section
    metadata_match = re.search(r"## Metadata\n(.*?)\n---", content, re.DOTALL)
    metadata = {}
    if metadata_match:
        metadata_text = metadata_match.group(1)
        # Parse metadata lines
        for line in metadata_text.strip().split("\n"):
            if line.startswith("- **"):
                match = re.match(r"- \*\*([^*]+)\*\*:\s*(.+)", line)
                if match:
                    key = match.group(1).lower().replace(" ", "_")
                    value = match.group(2)
                    metadata[key] = value

    # Extract system prompt (content between ```triple backticks``` in System Prompt section)
    system_prompt_match = re.search(
        r"## System Prompt\s*\n\s*```\s*\n(.*?)\n```", content, re.DOTALL
    )
    system_prompt = system_prompt_match.group(1) if system_prompt_match else ""

    # Extract user prompt (content between ```triple backticks``` in User Prompt section)
    user_prompt_match = re.search(r"## User Prompt\s*\n\s*```\s*\n(.*?)\n```", content, re.DOTALL)
    user_prompt = user_prompt_match.group(1).strip() if user_prompt_match else None

    # Extract tool definition (JSON content in Tool Definition section)
    tool_def = None
    tool_match = re.search(r"## Tool Definition\s*\n\s*```json\s*\n(.*?)\n```", content, re.DOTALL)
    if tool_match:
        try:
            # Unescape double braces {{  }} to single braces { } for JSON parsing
            json_str = tool_match.group(1).replace("{{", "{").replace("}}", "}")
            tool_def = json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse tool definition: {e}")

    return {
        "metadata": metadata,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "tool_definition": tool_def,
    }


async def upload_etl_prompts(etl_name: str):
    """Upload prompts for a specific ETL."""
    if etl_name not in ETL_CONFIGS:
        logger.error(f"Unknown ETL: {etl_name}")
        return

    config = ETL_CONFIGS[etl_name]
    layer = config["layer"]

    logger.info("=" * 80)
    logger.info(f"UPLOADING {etl_name.upper()} ETL PROMPTS TO DATABASE")
    logger.info("=" * 80)

    async with get_connection() as conn:
        for prompt_config in config["prompts"]:
            file_path = Path(prompt_config["file"])
            name = prompt_config["name"]

            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                continue

            logger.info(f"\nProcessing: {name}")
            logger.info(f"  File: {file_path}")

            # Parse markdown file
            parsed = parse_markdown_prompt(file_path)
            metadata = parsed["metadata"]
            system_prompt = parsed["system_prompt"]
            user_prompt = parsed["user_prompt"]
            tool_definition = parsed["tool_definition"]

            # Check if prompt already exists
            check_query = text(
                """
                SELECT id, version FROM prompts
                WHERE model = 'aegis'
                AND layer = :layer
                AND name = :name
                ORDER BY created_at DESC
                LIMIT 1
            """
            )

            result = await conn.execute(check_query, {"layer": layer, "name": name})
            existing = result.fetchone()

            if existing:
                logger.info(f"  Existing prompt found: v{existing.version} (id={existing.id})")
                logger.info(f"  Deleting old version...")
                await conn.execute(text("DELETE FROM prompts WHERE id = :id"), {"id": existing.id})

            # Insert new prompt
            insert_query = text(
                """
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
                ) VALUES (
                    'aegis',
                    :layer,
                    :name,
                    :description,
                    :system_prompt,
                    :user_prompt,
                    :tool_definition,
                    '{}',
                    :version,
                    NOW(),
                    NOW()
                )
                RETURNING id, version
            """
            )

            result = await conn.execute(
                insert_query,
                {
                    "layer": layer,
                    "name": name,
                    "description": metadata.get("purpose", metadata.get("name", "")),
                    "system_prompt": system_prompt,
                    "user_prompt": user_prompt,
                    "tool_definition": json.dumps(tool_definition) if tool_definition else None,
                    "version": metadata.get("version", "1.0"),
                },
            )

            row = result.fetchone()
            logger.info(f"  ✅ Uploaded: v{row.version} (id={row.id})")

    logger.info("\n" + "=" * 80)
    logger.info(f"{etl_name.upper()} UPLOAD COMPLETE!")
    logger.info("=" * 80)

    # Verify uploads
    logger.info("\nVerifying uploads...")
    async with get_connection() as conn:
        verify_query = text(
            """
            SELECT layer, name, version, created_at
            FROM prompts
            WHERE model = 'aegis'
            AND layer = :layer
            ORDER BY name
        """
        )
        result = await conn.execute(verify_query, {"layer": layer})
        rows = result.fetchall()

        logger.info(f"\nFound {len(rows)} {layer} prompts in database:")
        for row in rows:
            logger.info(f"  • {row.name} (v{row.version}) - {row.created_at}")


async def main(etl_filter: str = None):
    """Upload prompts for all or specific ETL."""
    if etl_filter:
        await upload_etl_prompts(etl_filter)
    else:
        for etl_name in ETL_CONFIGS.keys():
            await upload_etl_prompts(etl_name)
            logger.info("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload ETL prompts to database")
    parser.add_argument(
        "--etl",
        choices=["call_summary", "key_themes"],
        help="Specific ETL to upload (default: all)",
    )
    args = parser.parse_args()

    asyncio.run(main(args.etl))
