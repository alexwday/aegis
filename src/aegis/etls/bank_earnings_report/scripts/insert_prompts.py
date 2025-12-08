"""
Insert Bank Earnings Report ETL prompts into PostgreSQL database.

This script reads prompt definitions from markdown files in the documentation/prompts/
directory and inserts them into the prompts table. It uses the environment variables
from .env for database connection.

Usage:
    python -m aegis.etls.bank_earnings_report.scripts.insert_prompts
    python -m aegis.etls.bank_earnings_report.scripts.insert_prompts --dry-run

The script is idempotent - running it multiple times will update existing prompts
to the latest version based on (model, layer, name) combination.
"""

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from aegis.utils.settings import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Database connection
DB_URL = (
    f"postgresql://{config.postgres_user}:{config.postgres_password}"
    f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}"
)

# Path to prompts directory
PROMPTS_DIR = Path(__file__).parent.parent / "documentation" / "prompts"


# =============================================================================
# MARKDOWN PARSING
# =============================================================================


def parse_markdown_prompt(file_path: Path) -> Optional[Dict[str, Any]]:
    """
    Parse a prompt markdown file and extract prompt components.

    Expected markdown structure:
    - Metadata section with Model, Layer, Name, Version, Description
    - System Prompt section with code block
    - User Prompt section with code block
    - Tool Definition section with JSON code block

    Args:
        file_path: Path to the markdown file

    Returns:
        Dict with prompt data, or None if parsing fails
    """
    try:
        content = file_path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error(f"Failed to read {file_path}: {e}")
        return None

    prompt_data = {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "uses_global": None,
    }

    # Extract metadata
    # Look for: - **Name**: value or - **Version**: value
    name_match = re.search(r"\*\*Name\*\*:\s*(\S+)", content)
    if name_match:
        prompt_data["name"] = name_match.group(1)
    else:
        logger.error(f"No name found in {file_path}")
        return None

    version_match = re.search(r"\*\*Version\*\*:\s*(\S+)", content)
    if version_match:
        prompt_data["version"] = version_match.group(1)
    else:
        prompt_data["version"] = "2.0.0"  # Default version

    desc_match = re.search(r"\*\*Description\*\*:\s*(.+)", content)
    if desc_match:
        prompt_data["description"] = desc_match.group(1).strip()
    else:
        prompt_data["description"] = ""

    # Extract System Prompt (content between ```  after "## System Prompt")
    system_prompt_match = re.search(
        r"## System Prompt\s*\n+```[^\n]*\n(.*?)```",
        content,
        re.DOTALL,
    )
    if system_prompt_match:
        prompt_data["system_prompt"] = system_prompt_match.group(1).strip()
    else:
        logger.warning(f"No system prompt found in {file_path}")
        prompt_data["system_prompt"] = ""

    # Extract User Prompt
    user_prompt_match = re.search(
        r"## User Prompt\s*\n+```[^\n]*\n(.*?)```",
        content,
        re.DOTALL,
    )
    if user_prompt_match:
        prompt_data["user_prompt"] = user_prompt_match.group(1).strip()
    else:
        logger.warning(f"No user prompt found in {file_path}")
        prompt_data["user_prompt"] = ""

    # Extract Tool Definition (JSON)
    tool_def_match = re.search(
        r"## Tool Definition\s*\n+```json\s*\n(.*?)```",
        content,
        re.DOTALL,
    )
    if tool_def_match:
        try:
            tool_json = tool_def_match.group(1).strip()
            prompt_data["tool_definition"] = json.loads(tool_json)
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON in tool definition for {file_path}: {e}")
            prompt_data["tool_definition"] = None
    else:
        prompt_data["tool_definition"] = None

    return prompt_data


def load_prompts_from_markdown() -> List[Dict[str, Any]]:
    """
    Load all prompts from markdown files in the prompts directory.

    Returns:
        List of prompt data dictionaries
    """
    prompts = []

    if not PROMPTS_DIR.exists():
        logger.error(f"Prompts directory not found: {PROMPTS_DIR}")
        return prompts

    # Find all markdown files ending in _prompt.md
    md_files = sorted(PROMPTS_DIR.glob("*_prompt.md"))

    logger.info(f"Found {len(md_files)} prompt markdown files")

    for md_file in md_files:
        logger.info(f"  Parsing: {md_file.name}")
        prompt_data = parse_markdown_prompt(md_file)
        if prompt_data:
            prompts.append(prompt_data)
        else:
            logger.error(f"  Failed to parse: {md_file.name}")

    return prompts


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================


def insert_or_update_prompt(
    engine,
    prompt: Dict[str, Any],
    dry_run: bool = False,
) -> bool:
    """
    Insert or update a single prompt in the database.

    Uses upsert logic: if (model, layer, name) exists, update; otherwise insert.

    Args:
        engine: SQLAlchemy engine
        prompt: Prompt data dictionary
        dry_run: If True, log but don't execute

    Returns:
        True if successful, False otherwise
    """
    model = prompt["model"]
    layer = prompt["layer"]
    name = prompt["name"]

    try:
        with engine.connect() as conn:
            # Check if prompt exists
            check_sql = text("""
                SELECT id FROM prompts
                WHERE model = :model AND layer = :layer AND name = :name
                ORDER BY updated_at DESC
                LIMIT 1
            """)
            result = conn.execute(
                check_sql,
                {"model": model, "layer": layer, "name": name},
            )
            existing = result.fetchone()

            # Prepare tool_definition as JSON
            tool_def = prompt.get("tool_definition")
            tool_def_json = json.dumps(tool_def) if tool_def else None

            # Prepare uses_global as array
            uses_global = prompt.get("uses_global")

            if dry_run:
                action = "UPDATE" if existing else "INSERT"
                logger.info(f"[DRY RUN] {action}: {layer}/{name} v{prompt['version']}")
                return True

            if existing:
                # Update existing prompt
                update_sql = text("""
                    UPDATE prompts SET
                        description = :description,
                        system_prompt = :system_prompt,
                        user_prompt = :user_prompt,
                        tool_definition = CAST(:tool_definition AS jsonb),
                        uses_global = :uses_global,
                        version = :version,
                        updated_at = :updated_at
                    WHERE model = :model AND layer = :layer AND name = :name
                """)
                conn.execute(
                    update_sql,
                    {
                        "model": model,
                        "layer": layer,
                        "name": name,
                        "description": prompt.get("description"),
                        "system_prompt": prompt.get("system_prompt"),
                        "user_prompt": prompt.get("user_prompt"),
                        "tool_definition": tool_def_json,
                        "uses_global": uses_global,
                        "version": prompt.get("version"),
                        "updated_at": datetime.now(timezone.utc),
                    },
                )
                conn.commit()
                logger.info(f"UPDATED: {layer}/{name} v{prompt['version']}")
            else:
                # Insert new prompt
                insert_sql = text("""
                    INSERT INTO prompts (
                        model, layer, name, description,
                        system_prompt, user_prompt, tool_definition,
                        uses_global, version, created_at, updated_at
                    ) VALUES (
                        :model, :layer, :name, :description,
                        :system_prompt, :user_prompt, CAST(:tool_definition AS jsonb),
                        :uses_global, :version, :created_at, :updated_at
                    )
                """)
                now = datetime.now(timezone.utc)
                conn.execute(
                    insert_sql,
                    {
                        "model": model,
                        "layer": layer,
                        "name": name,
                        "description": prompt.get("description"),
                        "system_prompt": prompt.get("system_prompt"),
                        "user_prompt": prompt.get("user_prompt"),
                        "tool_definition": tool_def_json,
                        "uses_global": uses_global,
                        "version": prompt.get("version"),
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                conn.commit()
                logger.info(f"INSERTED: {layer}/{name} v{prompt['version']}")

            return True

    except SQLAlchemyError as e:
        logger.error(f"Database error for {layer}/{name}: {e}")
        return False


def verify_connection(engine) -> bool:
    """
    Verify database connection and show existing prompt counts.

    Args:
        engine: SQLAlchemy engine

    Returns:
        True if connection successful, False otherwise
    """
    try:
        with engine.connect() as conn:
            # Get total prompt count
            result = conn.execute(text("SELECT COUNT(*) FROM prompts"))
            total_count = result.scalar()

            # Get count by layer
            result = conn.execute(
                text("""
                    SELECT layer, COUNT(*) as count
                    FROM prompts
                    GROUP BY layer
                    ORDER BY layer
                """)
            )
            layer_counts = result.fetchall()

            # Check for existing bank_earnings_report_etl prompts
            result = conn.execute(
                text("""
                    SELECT COUNT(*) FROM prompts
                    WHERE layer = 'bank_earnings_report_etl'
                """)
            )
            etl_count = result.scalar()

            logger.info("")
            logger.info("DATABASE CONNECTION VERIFIED")
            logger.info(f"  Total prompts in table: {total_count}")
            logger.info(f"  Existing bank_earnings_report_etl prompts: {etl_count}")
            if layer_counts:
                logger.info("  Prompts by layer:")
                for layer, count in layer_counts:
                    logger.info(f"    - {layer}: {count}")
            logger.info("")

            return True

    except SQLAlchemyError as e:
        logger.error(f"Database connection failed: {e}")
        return False


def main(dry_run: bool = False):
    """
    Main function to insert all prompts into the database.

    Args:
        dry_run: If True, log actions but don't modify database
    """
    logger.info("=" * 60)
    logger.info("Bank Earnings Report ETL - Prompt Insertion Script")
    logger.info("=" * 60)

    if dry_run:
        logger.info("DRY RUN MODE - No database changes will be made")

    logger.info(f"Database: {config.postgres_host}:{config.postgres_port}/{config.postgres_database}")
    logger.info(f"Prompts directory: {PROMPTS_DIR}")

    # Load prompts from markdown files
    logger.info("")
    logger.info("Loading prompts from markdown files...")
    prompts = load_prompts_from_markdown()

    if not prompts:
        logger.error("No prompts loaded. Aborting.")
        return 1

    logger.info(f"Total prompts loaded: {len(prompts)}")
    logger.info("")

    # Create engine
    engine = create_engine(DB_URL)

    # Verify connection and show existing counts
    if not verify_connection(engine):
        logger.error("Failed to connect to database. Aborting.")
        return 1

    # Process each prompt
    success_count = 0
    error_count = 0

    for prompt in prompts:
        if insert_or_update_prompt(engine, prompt, dry_run=dry_run):
            success_count += 1
        else:
            error_count += 1

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Successful: {success_count}")
    logger.info(f"Errors: {error_count}")
    logger.info(f"Total: {len(prompts)}")

    if error_count > 0:
        logger.warning("Some prompts failed to insert. Check logs above.")
        return 1
    else:
        logger.info("All prompts inserted successfully!")
        return 0


if __name__ == "__main__":
    import sys

    # Check for --dry-run flag
    dry_run_mode = "--dry-run" in sys.argv

    exit_code = main(dry_run=dry_run_mode)
    sys.exit(exit_code)
