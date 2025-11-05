"""
Upload call summary ETL prompts to the database.

This script uploads both research_plan and category_extraction prompts
for the call_summary_etl layer, following the same pattern as transcripts prompts.
"""

import asyncio
import json
import yaml
from pathlib import Path
from sqlalchemy import text
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger

setup_logging()
logger = get_logger()


async def load_yaml_prompt(yaml_path: Path) -> dict:
    """Load prompt data from YAML file."""
    with open(yaml_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    return data


async def upload_prompt(
    layer: str,
    name: str,
    description: str,
    system_prompt: str,
    user_prompt: str = None,
    tool_definition: dict = None,
    uses_global: list = None,
    version: str = "1",
    model: str = "gpt-4o-mini"
):
    """Upload a prompt to the database, replacing any existing version."""
    async with get_connection() as conn:
        # Check if prompt exists
        result = await conn.execute(
            text("""
                SELECT id, version FROM prompts
                WHERE layer = :layer AND name = :name
                ORDER BY version DESC
                LIMIT 1
            """),
            {"layer": layer, "name": name}
        )
        existing = result.fetchone()

        if existing:
            # Delete existing prompt
            await conn.execute(
                text("DELETE FROM prompts WHERE layer = :layer AND name = :name"),
                {"layer": layer, "name": name}
            )
            logger.info(
                f"Deleted existing prompt: layer='{layer}', name='{name}', "
                f"old_version={existing.version}"
            )

        # Insert new prompt
        # Convert tool_definition dict to JSON string for JSONB column
        tool_def_json = json.dumps(tool_definition) if tool_definition else None

        await conn.execute(
            text("""
                INSERT INTO prompts (
                    model, layer, name, description, comments,
                    system_prompt, user_prompt, tool_definition,
                    uses_global, version, created_at, updated_at
                )
                VALUES (
                    :model, :layer, :name, :description, :comments,
                    :system_prompt, :user_prompt, CAST(:tool_definition AS JSONB),
                    :uses_global, :version, NOW(), NOW()
                )
            """),
            {
                "model": model,
                "layer": layer,
                "name": name,
                "description": description,
                "comments": "Auto-uploaded from YAML by upload_call_summary_prompts.py",
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "tool_definition": tool_def_json,
                "uses_global": uses_global or [],
                "version": version
            }
        )
        await conn.commit()

        logger.info(
            f"✅ Uploaded prompt: layer='{layer}', name='{name}', version={version}"
        )


async def main():
    """Main function to upload both call summary ETL prompts."""
    # Define base path to prompt files
    etl_dir = Path(__file__).parent.parent / "src" / "aegis" / "etls" / "call_summary" / "prompts"

    # =========================================================================
    # 1. Upload Research Plan Prompt
    # =========================================================================
    logger.info("=" * 60)
    logger.info("Uploading Research Plan Prompt...")
    logger.info("=" * 60)

    research_yaml_path = etl_dir / "research_plan_prompt.yaml"
    research_data = await load_yaml_prompt(research_yaml_path)

    await upload_prompt(
        layer="call_summary_etl",
        name="research_plan",
        description=research_data["metadata"]["purpose"],
        system_prompt=research_data["system_template"],
        user_prompt=None,  # Research plan doesn't have a user prompt template
        tool_definition=research_data["tool"],
        uses_global=[],  # ETL doesn't use global contexts
        version=research_data["metadata"]["version"],  # Keep as string "2.1"
        model="aegis"  # Use aegis model to match prompt_loader
    )

    # =========================================================================
    # 2. Upload Category Extraction Prompt
    # =========================================================================
    logger.info("=" * 60)
    logger.info("Uploading Category Extraction Prompt...")
    logger.info("=" * 60)

    extraction_yaml_path = etl_dir / "category_extraction_prompt.yaml"
    extraction_data = await load_yaml_prompt(extraction_yaml_path)

    await upload_prompt(
        layer="call_summary_etl",
        name="category_extraction",
        description=extraction_data["metadata"]["purpose"],
        system_prompt=extraction_data["system_template"],
        user_prompt=None,  # Category extraction doesn't have a user prompt template
        tool_definition=extraction_data["tool"],
        uses_global=[],  # ETL doesn't use global contexts
        version=extraction_data["metadata"]["version"],  # Keep as string "2.1"
        model="aegis"  # Use aegis model to match prompt_loader
    )

    logger.info("=" * 60)
    logger.info("✅ All call summary ETL prompts uploaded successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
