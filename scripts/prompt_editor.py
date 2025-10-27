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


if __name__ == "__main__":
    print("\n" + "="*60)
    print("🚀 Prompt Editor Starting...")
    print("="*60)
    print(f"\n📊 Database: {config.postgres_host}:{config.postgres_port}/{config.postgres_database}")
    print(f"🌐 Web Interface: http://localhost:5001")
    print("\n💡 Press Ctrl+C to stop\n")
    print("="*60 + "\n")

    app.run(host="0.0.0.0", port=5001, debug=True)
