"""
Upload Key Themes ETL Prompts to PostgreSQL Database

This script reads the key_themes prompt YAML files and inserts them into
the aegis_prompts table for use by the ETL.
"""

import asyncio
import yaml
from pathlib import Path
from sqlalchemy import text
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger

setup_logging()
logger = get_logger()

# Prompt file mappings
PROMPTS = [
    {
        "file": "src/aegis/etls/key_themes/prompts/theme_extraction_prompt.yaml",
        "layer": "key_themes_etl",
        "name": "theme_extraction"
    },
    {
        "file": "src/aegis/etls/key_themes/prompts/html_formatting_prompt.yaml",
        "layer": "key_themes_etl",
        "name": "html_formatting"
    },
    {
        "file": "src/aegis/etls/key_themes/prompts/theme_grouping_prompt.yaml",
        "layer": "key_themes_etl",
        "name": "grouping"
    }
]


async def upload_prompts():
    """Upload all key_themes prompts to database."""
    logger.info("=" * 80)
    logger.info("UPLOADING KEY THEMES ETL PROMPTS TO DATABASE")
    logger.info("=" * 80)

    async with get_connection() as conn:
        for prompt_config in PROMPTS:
            file_path = Path(prompt_config["file"])
            layer = prompt_config["layer"]
            name = prompt_config["name"]

            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                continue

            logger.info(f"\nProcessing: {name}")
            logger.info(f"  File: {file_path}")

            # Load YAML file
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f)

            # Extract components
            metadata = data.get('metadata', {})
            system_template = data.get('system_template', '')
            tool = data.get('tool')

            # Check if prompt already exists
            check_query = text("""
                SELECT id, version FROM prompts
                WHERE model = 'aegis'
                AND layer = :layer
                AND name = :name
                ORDER BY created_at DESC
                LIMIT 1
            """)

            result = await conn.execute(check_query, {"layer": layer, "name": name})
            existing = result.fetchone()

            if existing:
                logger.info(f"  Existing prompt found: v{existing.version} (id={existing.id})")
                logger.info(f"  Deleting old version...")
                await conn.execute(
                    text("DELETE FROM prompts WHERE id = :id"),
                    {"id": existing.id}
                )

            # Insert new prompt
            insert_query = text("""
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
                    NULL,
                    :tool_definition,
                    '{}',
                    :version,
                    NOW(),
                    NOW()
                )
                RETURNING id, version
            """)

            import json
            result = await conn.execute(insert_query, {
                "layer": layer,
                "name": name,
                "description": metadata.get('purpose', ''),
                "system_prompt": system_template,
                "tool_definition": json.dumps(tool) if tool else None,
                "version": metadata.get('version', '1.0')
            })

            row = result.fetchone()
            logger.info(f"  ✅ Uploaded: v{row.version} (id={row.id})")

    logger.info("\n" + "=" * 80)
    logger.info("UPLOAD COMPLETE!")
    logger.info("=" * 80)

    # Verify uploads
    logger.info("\nVerifying uploads...")
    async with get_connection() as conn:
        verify_query = text("""
            SELECT layer, name, version, created_at
            FROM prompts
            WHERE model = 'aegis'
            AND layer = 'key_themes_etl'
            ORDER BY name
        """)
        result = await conn.execute(verify_query)
        rows = result.fetchall()

        logger.info(f"\nFound {len(rows)} key_themes_etl prompts in database:")
        for row in rows:
            logger.info(f"  • {row.name} (v{row.version}) - {row.created_at}")


if __name__ == "__main__":
    asyncio.run(upload_prompts())
