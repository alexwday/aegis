#!/usr/bin/env python3
"""
Prompt Editor - Standalone web interface for viewing and editing prompts table.

Usage:
    python scripts/prompt_editor.py

Then open browser to: http://localhost:5001
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any
import json
from contextlib import contextmanager

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from flask import Flask, render_template, jsonify, request
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.pool import QueuePool
from aegis.utils.settings import config
from aegis.connections.llm_connector import complete
from aegis.connections.oauth_connector import setup_authentication
from aegis.utils.ssl import setup_ssl
import asyncio

app = Flask(__name__, template_folder="templates")

# Create synchronous database engine for Flask
sync_engine = create_engine(
    f"postgresql://{config.postgres_user}:{config.postgres_password}"
    f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}",
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
)


@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    conn = sync_engine.connect()
    try:
        yield conn
    finally:
        conn.close()


def increment_version(version: str) -> str:
    """
    Increment version string (e.g., "1.0.0" -> "2.0.0").

    Always increments the major version and resets minor/patch to 0.

    Args:
        version: Current version string

    Returns:
        Incremented version string (X.0.0)
    """
    try:
        parts = version.split(".")
        if len(parts) >= 1:
            major = int(parts[0])
            # Increment major version, reset rest to 0
            return f"{major + 1}.0.0"
        else:
            # Default if parsing fails
            return "2.0.0"
    except (ValueError, AttributeError):
        # If parsing fails, default to 2.0.0
        return "2.0.0"


@app.route("/")
def index():
    """Render main prompt editor page."""
    return render_template("prompt_editor.html")


@app.route("/api/prompts", methods=["GET"])
def get_prompts():
    """Get all prompts from database."""
    try:
        with get_db_connection() as conn:
            result = conn.execute(
                text("""
                    SELECT
                        id, model, layer, name, description, comments,
                        system_prompt, user_prompt, tool_definition, uses_global,
                        version, created_at, updated_at
                    FROM prompts
                    ORDER BY model, layer, name, version DESC
                """)
            )

            rows = result.fetchall()
            prompts = []

            for row in rows:
                prompts.append({
                    "id": row[0],
                    "model": row[1],
                    "layer": row[2],
                    "name": row[3],
                    "description": row[4],
                    "comments": row[5],
                    "system_prompt": row[6],
                    "user_prompt": row[7],
                    "tool_definition": row[8],
                    "uses_global": row[9],
                    "version": row[10],
                    "created_at": row[11].isoformat() if row[11] else None,
                    "updated_at": row[12].isoformat() if row[12] else None,
                })

            return jsonify({"success": True, "prompts": prompts})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/prompt/<int:prompt_id>", methods=["GET"])
def get_prompt(prompt_id: int):
    """Get single prompt by ID."""
    try:
        with get_db_connection() as conn:
            result = conn.execute(
                text("""
                    SELECT
                        id, model, layer, name, description, comments,
                        system_prompt, user_prompt, tool_definition, uses_global,
                        version, created_at, updated_at
                    FROM prompts
                    WHERE id = :id
                """),
                {"id": prompt_id}
            )

            row = result.fetchone()

            if not row:
                return jsonify({"success": False, "error": "Prompt not found"}), 404

            prompt = {
                "id": row[0],
                "model": row[1],
                "layer": row[2],
                "name": row[3],
                "description": row[4],
                "comments": row[5],
                "system_prompt": row[6],
                "user_prompt": row[7],
                "tool_definition": row[8],
                "uses_global": row[9],
                "version": row[10],
                "created_at": row[11].isoformat() if row[11] else None,
                "updated_at": row[12].isoformat() if row[12] else None,
            }

            return jsonify({"success": True, "prompt": prompt})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/prompt", methods=["POST"])
def create_prompt():
    """Create new prompt."""
    try:
        data = request.json

        # Validate required fields
        if not data.get("model") or not data.get("layer") or not data.get("name"):
            return jsonify({
                "success": False,
                "error": "Model, layer, and name are required fields"
            }), 400

        with get_db_connection() as conn:
            # Convert tool_definition to JSON string for JSONB
            tool_def_json = None
            if data.get("tool_definition"):
                tool_def_json = json.dumps(data.get("tool_definition"))

            # Set default version if not provided
            version = data.get("version", "1.0.0")

            # Insert new record
            conn.execute(
                text("""
                    INSERT INTO prompts (
                        model, layer, name, description, comments,
                        system_prompt, user_prompt, tool_definition, uses_global,
                        version, created_at, updated_at
                    ) VALUES (
                        :model, :layer, :name, :description, :comments,
                        :system_prompt, :user_prompt, CAST(:tool_definition AS JSONB), :uses_global,
                        :version, :created_at, :updated_at
                    )
                """),
                {
                    "model": data.get("model"),
                    "layer": data.get("layer"),
                    "name": data.get("name"),
                    "description": data.get("description"),
                    "comments": data.get("comments"),
                    "system_prompt": data.get("system_prompt"),
                    "user_prompt": data.get("user_prompt"),
                    "tool_definition": tool_def_json,
                    "uses_global": data.get("uses_global"),
                    "version": version,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }
            )

            conn.commit()

            return jsonify({
                "success": True,
                "message": f"New prompt created successfully: {data.get('name')} v{version}"
            })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/prompt/<int:prompt_id>", methods=["PUT"])
def update_prompt(prompt_id: int):
    """Update existing prompt (overwrite)."""
    try:
        data = request.json

        with get_db_connection() as conn:
            # Convert tool_definition to JSON string for JSONB
            tool_def_json = None
            if data.get("tool_definition"):
                tool_def_json = json.dumps(data.get("tool_definition"))

            # Update the record
            conn.execute(
                text("""
                    UPDATE prompts
                    SET
                        model = :model,
                        layer = :layer,
                        name = :name,
                        description = :description,
                        comments = :comments,
                        system_prompt = :system_prompt,
                        user_prompt = :user_prompt,
                        tool_definition = CAST(:tool_definition AS JSONB),
                        uses_global = :uses_global,
                        version = :version,
                        updated_at = :updated_at
                    WHERE id = :id
                """),
                {
                    "id": prompt_id,
                    "model": data.get("model"),
                    "layer": data.get("layer"),
                    "name": data.get("name"),
                    "description": data.get("description"),
                    "comments": data.get("comments"),
                    "system_prompt": data.get("system_prompt"),
                    "user_prompt": data.get("user_prompt"),
                    "tool_definition": tool_def_json,
                    "uses_global": data.get("uses_global"),
                    "version": data.get("version"),
                    "updated_at": datetime.utcnow(),
                }
            )

            conn.commit()

            return jsonify({"success": True, "message": "Prompt updated successfully"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/prompt/<int:prompt_id>/new-version", methods=["POST"])
def create_new_version(prompt_id: int):
    """Create new version of prompt (increment version, new record)."""
    try:
        data = request.json

        # Increment version
        current_version = data.get("version", "1.0.0")
        new_version = increment_version(current_version)

        with get_db_connection() as conn:
            # Convert tool_definition to JSON string for JSONB
            tool_def_json = None
            if data.get("tool_definition"):
                tool_def_json = json.dumps(data.get("tool_definition"))

            # Insert new record with incremented version
            conn.execute(
                text("""
                    INSERT INTO prompts (
                        model, layer, name, description, comments,
                        system_prompt, user_prompt, tool_definition, uses_global,
                        version, created_at, updated_at
                    ) VALUES (
                        :model, :layer, :name, :description, :comments,
                        :system_prompt, :user_prompt, CAST(:tool_definition AS JSONB), :uses_global,
                        :version, :created_at, :updated_at
                    )
                """),
                {
                    "model": data.get("model"),
                    "layer": data.get("layer"),
                    "name": data.get("name"),
                    "description": data.get("description"),
                    "comments": data.get("comments"),
                    "system_prompt": data.get("system_prompt"),
                    "user_prompt": data.get("user_prompt"),
                    "tool_definition": tool_def_json,
                    "uses_global": data.get("uses_global"),
                    "version": new_version,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }
            )

            conn.commit()

            return jsonify({
                "success": True,
                "message": f"New version created: {new_version}",
                "version": new_version
            })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/prompt/<int:prompt_id>/assist", methods=["POST"])
def prompt_assistant(prompt_id: int):
    """AI assistant to help improve prompts."""
    try:
        data = request.json
        user_question = data.get("question", "")
        prompt_data = data.get("prompt", {})

        if not user_question:
            return jsonify({"success": False, "error": "No question provided"}), 400

        # Build context for the AI assistant
        system_prompt = f"""You are an expert AI prompt engineer helping to improve prompts for the Aegis financial assistant system.

You are currently reviewing a prompt with the following details:

**Layer**: {prompt_data.get('layer', 'N/A')}
**Name**: {prompt_data.get('name', 'N/A')}
**Description**: {prompt_data.get('description', 'N/A')}

**System Prompt**:
```
{prompt_data.get('system_prompt', 'N/A')}
```

**User Prompt Template**:
```
{prompt_data.get('user_prompt', 'N/A')}
```

**Tool Definition**:
```json
{json.dumps(prompt_data.get('tool_definition'), indent=2) if prompt_data.get('tool_definition') else 'N/A'}
```

**Uses Global Contexts**: {', '.join(prompt_data.get('uses_global', [])) if prompt_data.get('uses_global') else 'None'}

Provide specific, actionable advice on how to improve this prompt. Consider:
- Clarity and specificity
- Structured output formats
- Error handling instructions
- Edge case coverage
- Consistency with best practices
- Variable usage in user prompt templates

Be concise and practical. Suggest specific improvements with examples when relevant."""

        user_message = user_question

        # Setup async context for LLM call
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Setup authentication
            ssl_config = setup_ssl()
            auth_result = loop.run_until_complete(
                setup_authentication(execution_id="prompt_editor", ssl_config=ssl_config)
            )

            if not auth_result["success"]:
                return jsonify({
                    "success": False,
                    "error": "Authentication failed"
                }), 500

            # Call LLM
            context = {
                "execution_id": "prompt_editor",
                "auth_config": auth_result,
                "ssl_config": ssl_config
            }

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message}
            ]

            llm_response = loop.run_until_complete(
                complete(
                    messages=messages,
                    context=context,
                    llm_params={"model": config.llm.large.model}
                )
            )

            # Extract the assistant's response
            if llm_response.get("choices") and len(llm_response["choices"]) > 0:
                assistant_message = llm_response["choices"][0]["message"]["content"]

                return jsonify({
                    "success": True,
                    "response": assistant_message
                })
            else:
                return jsonify({
                    "success": False,
                    "error": "No response from LLM"
                }), 500

        finally:
            loop.close()

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


if __name__ == "__main__":
    print("\n" + "="*60)
    print("🚀 Prompt Editor Starting...")
    print("="*60)
    print(f"\n📊 Database: {config.postgres_host}:{config.postgres_port}/{config.postgres_database}")
    print(f"🌐 Web Interface: http://localhost:5001")
    print("\n💡 Press Ctrl+C to stop\n")
    print("="*60 + "\n")

    app.run(host="0.0.0.0", port=5001, debug=True)
