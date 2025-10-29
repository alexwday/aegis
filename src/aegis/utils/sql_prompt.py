"""
SQL-based prompt management - retrieves prompts from PostgreSQL database.
"""

from typing import Dict, Any, Optional
from sqlalchemy import text
from aegis.connections.postgres_connector import get_sync_engine
from aegis.utils.logging import get_logger

logger = get_logger()


class PromptManager:
    """Manages prompt retrieval from PostgreSQL database."""

    def __init__(self):
        """Initialize prompt manager."""
        self.engine = get_sync_engine()

    def get_latest_prompt(
        self,
        layer: str,
        name: str,
        system_prompt: bool = False
    ) -> Dict[str, Any]:
        """
        Retrieve the latest version of a prompt from database.

        Args:
            layer: Prompt layer (aegis, transcripts, reports, global, etc.)
            name: Prompt name
            system_prompt: If True, return only system_prompt string. If False, return full dict.

        Returns:
            If system_prompt=True: Just the system_prompt string
            If system_prompt=False: Full prompt dictionary with all fields

        Raises:
            FileNotFoundError: If prompt doesn't exist
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT
                            id, model, layer, name, description, comments,
                            system_prompt, user_prompt, tool_definition, tool_definitions,
                            uses_global, version, created_at, updated_at
                        FROM prompts
                        WHERE layer = :layer AND name = :name
                        ORDER BY version DESC
                        LIMIT 1
                    """),
                    {"layer": layer, "name": name}
                ).fetchone()

                if not result:
                    raise FileNotFoundError(
                        f"No prompt found for layer='{layer}', name='{name}'"
                    )

                # Convert Row to dict
                prompt_data = {
                    "id": result[0],
                    "model": result[1],
                    "layer": result[2],
                    "name": result[3],
                    "description": result[4],
                    "comments": result[5],
                    "system_prompt": result[6],
                    "user_prompt": result[7],
                    "tool_definition": result[8],
                    "tool_definitions": result[9],
                    "uses_global": result[10] or [],
                    "version": result[11],
                    "created_at": result[12],
                    "updated_at": result[13],
                }

                if system_prompt:
                    return prompt_data.get("system_prompt", "")
                else:
                    return prompt_data

        except Exception as e:
            logger.error(f"Error retrieving prompt: {e}")
            raise


# Singleton instance
prompt_manager = PromptManager()
