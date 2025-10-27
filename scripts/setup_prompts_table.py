#!/usr/bin/env python3
"""
Setup script to create prompts table and add test data.

Run this once before using the prompt editor.
"""

import sys
import asyncio
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aegis.connections.postgres_connector import get_connection
from sqlalchemy import text


async def setup_prompts_table():
    """Create prompts table and add test data."""

    print("\n" + "="*60)
    print("Setting up prompts table...")
    print("="*60 + "\n")

    async with get_connection() as conn:
        # Drop table if exists (for clean slate)
        print("🗑️  Dropping existing table (if any)...")
        await conn.execute(text("DROP TABLE IF EXISTS prompts CASCADE"))

        # Create table
        print("📦 Creating prompts table...")
        await conn.execute(text("""
            CREATE TABLE prompts (
                id SERIAL PRIMARY KEY,
                model TEXT NOT NULL,
                layer TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                comments TEXT,
                system_prompt TEXT,
                user_prompt TEXT,
                tool_definition JSONB,
                uses_global TEXT[],
                version TEXT NOT NULL DEFAULT '1.0.0',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(model, layer, name, version)
            )
        """))

        print("✅ Table created successfully\n")

        # Add test data
        print("📝 Inserting test prompts...")

        test_prompts = [
            {
                "model": "aegis",
                "layer": "aegis",
                "name": "router",
                "description": "Routes queries to direct response or research workflow",
                "comments": "Binary classification agent",
                "system_prompt": "You are the router agent. Classify queries as direct_response (0) or research_workflow (1).",
                "user_prompt": "User query: {user_query}",
                "tool_definition": {
                    "type": "function",
                    "function": {
                        "name": "route",
                        "description": "Route the query",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "route": {"type": "integer", "enum": [0, 1]}
                            }
                        }
                    }
                },
                "uses_global": ["fiscal", "project"],
                "version": "1.0.0"
            },
            {
                "model": "aegis",
                "layer": "aegis",
                "name": "clarifier_banks",
                "description": "Extracts banks from user query",
                "comments": "First stage of clarification",
                "system_prompt": "You are the bank clarifier. Extract bank names and IDs from queries.",
                "user_prompt": "Extract banks from: {user_query}",
                "tool_definition": {
                    "type": "function",
                    "function": {
                        "name": "extract_banks",
                        "description": "Extract bank information"
                    }
                },
                "uses_global": ["project"],
                "version": "1.0.0"
            },
            {
                "model": "aegis",
                "layer": "aegis",
                "name": "clarifier_periods",
                "description": "Extracts fiscal periods from user query",
                "comments": "Second stage of clarification",
                "system_prompt": "You are the period clarifier. Extract fiscal years and quarters.",
                "user_prompt": "Extract periods from: {user_query}",
                "tool_definition": None,
                "uses_global": ["fiscal", "project"],
                "version": "1.0.0"
            },
            {
                "model": "aegis",
                "layer": "transcripts",
                "name": "method_selection",
                "description": "Selects retrieval method for transcripts",
                "comments": "Chooses full/category/similarity search",
                "system_prompt": "You select the best retrieval method for transcript queries.\n\nCategories: {category_mapping}",
                "user_prompt": "Bank: {bank_name}\nQuery: {full_intent}",
                "tool_definition": {
                    "type": "function",
                    "function": {
                        "name": "select_method",
                        "description": "Select retrieval method",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "method": {"type": "integer", "enum": [0, 1, 2]}
                            }
                        }
                    }
                },
                "uses_global": ["fiscal", "project"],
                "version": "1.0.0"
            },
            {
                "model": "aegis",
                "layer": "transcripts",
                "name": "reranking",
                "description": "Reranks similarity search results",
                "comments": "Improves relevance of retrieved chunks",
                "system_prompt": "You rerank transcript chunks by relevance to the query.",
                "user_prompt": "Query: {search_phrase}\nChunks to rank: {chunks}",
                "tool_definition": None,
                "uses_global": ["project"],
                "version": "1.0.0"
            },
            {
                "model": "aegis",
                "layer": "transcripts",
                "name": "research_synthesis",
                "description": "Synthesizes transcript research statement",
                "comments": "Generates final research output",
                "system_prompt": "You synthesize transcript data into research statements.",
                "user_prompt": "Bank: {bank_name} {quarter} {fiscal_year}\nQuery: {query_intent}\n\nContent:\n{formatted_content}",
                "tool_definition": None,
                "uses_global": ["fiscal", "project", "restrictions"],
                "version": "1.0.0"
            },
        ]

        for prompt in test_prompts:
            # Convert tool_definition to JSON string for JSONB
            tool_def_json = None
            if prompt["tool_definition"]:
                import json
                tool_def_json = json.dumps(prompt["tool_definition"])

            await conn.execute(
                text("""
                    INSERT INTO prompts (
                        model, layer, name, description, comments,
                        system_prompt, user_prompt, tool_definition, uses_global, version
                    ) VALUES (
                        :model, :layer, :name, :description, :comments,
                        :system_prompt, :user_prompt, CAST(:tool_definition AS JSONB), :uses_global, :version
                    )
                """),
                {
                    "model": prompt["model"],
                    "layer": prompt["layer"],
                    "name": prompt["name"],
                    "description": prompt["description"],
                    "comments": prompt["comments"],
                    "system_prompt": prompt["system_prompt"],
                    "user_prompt": prompt["user_prompt"],
                    "tool_definition": tool_def_json,
                    "uses_global": prompt["uses_global"],
                    "version": prompt["version"]
                }
            )

            print(f"  ✓ {prompt['layer']}.{prompt['name']}")

        await conn.commit()

        print(f"\n✅ Inserted {len(test_prompts)} test prompts\n")

    # Verify in a new connection context
    async with get_connection() as conn:
        result = await conn.execute(text("SELECT COUNT(*) FROM prompts"))
        count = result.scalar()

    print("="*60)
    print(f"✅ Setup complete! Total prompts: {count}")
    print("="*60 + "\n")
    print("You can now run: python scripts/prompt_editor.py\n")


if __name__ == "__main__":
    asyncio.run(setup_prompts_table())
