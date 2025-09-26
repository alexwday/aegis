#!/usr/bin/env python
"""
FastAPI application with WebSocket support for Aegis.

This replaces the Flask application with a fully async FastAPI implementation
featuring WebSocket streaming for real-time responses, monitoring dashboard,
and database viewer.
"""

import argparse
import os
import asyncio
import json
from typing import Dict, Any, Optional, List
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
import uvicorn

from src.aegis.model.main import model
from src.aegis.utils.logging import setup_logging, get_logger
from src.aegis.connections.llm_connector import close_all_clients
from src.aegis.connections.postgres_connector import close_all_connections, fetch_all
from src.aegis.utils.settings import config

# Import monitoring utilities (will use async postgres_connector for database)
from interfaces.monitoring import get_monitoring_summary, get_run_details, get_stage_trends

# Initialize logging
setup_logging()
logger = get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle - startup and shutdown.

    This ensures proper initialization and cleanup of resources
    like database connections and LLM clients.
    """
    # Startup
    logger.info("fastapi.startup", message="Aegis FastAPI server starting up")
    setup_logging()

    # Initialize database connection pool (pre-warm)
    try:
        from src.aegis.connections.postgres_connector import _get_async_engine
        engine = await _get_async_engine()
        logger.info("fastapi.startup.database", message="Database connection pool initialized")
    except Exception as e:
        logger.error("fastapi.startup.database_error", error=str(e))
        # Don't prevent startup, but log the error

    yield  # Application runs

    # Shutdown
    logger.info("fastapi.shutdown", message="Aegis FastAPI server shutting down")

    # Close all async clients
    try:
        await close_all_clients()
        logger.info("fastapi.shutdown.llm_clients_closed")
    except Exception as e:
        logger.error("fastapi.shutdown.llm_error", error=str(e))

    try:
        await close_all_connections()
        logger.info("fastapi.shutdown.db_connections_closed")
    except Exception as e:
        logger.error("fastapi.shutdown.db_error", error=str(e))


# Create FastAPI app with lifespan manager
app = FastAPI(
    title="Aegis AI Financial Assistant",
    description="WebSocket-based streaming interface for Aegis model with monitoring and database viewer",
    version="2.1.0",
    lifespan=lifespan
)

# Add CORS middleware for browser compatibility
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files and templates
if os.path.exists("templates"):
    # Serve CSS, JS, and other static assets from templates
    app.mount("/static", StaticFiles(directory="templates"), name="static")


@app.get("/")
async def root():
    """Serve the main chat interface."""
    template_path = "templates/chat.html"
    if os.path.exists(template_path):
        return FileResponse(template_path)
    # Fallback if template doesn't exist
    return HTMLResponse("""
    <html>
        <head>
            <title>Aegis AI Assistant</title>
            <style>
                body { font-family: Arial, sans-serif; padding: 20px; }
                #messages { height: 400px; overflow-y: auto; border: 1px solid #ccc; padding: 10px; margin-bottom: 10px; }
                #input-area { display: flex; gap: 10px; }
                #message-input { flex: 1; padding: 10px; }
                button { padding: 10px 20px; }
                .message { margin: 10px 0; padding: 10px; border-radius: 5px; }
                .user { background: #e3f2fd; text-align: right; }
                .assistant { background: #f5f5f5; }
                .error { background: #ffebee; color: #c62828; }
                .status { color: #666; font-style: italic; }
            </style>
        </head>
        <body>
            <h1>Aegis AI Financial Assistant</h1>
            <div id="messages"></div>
            <div id="input-area">
                <input type="text" id="message-input" placeholder="Ask me about financial data..." />
                <button onclick="sendMessage()">Send</button>
            </div>
            <div id="status"></div>

            <script>
                let ws = null;
                let currentMessageDiv = null;

                function connect() {
                    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                    ws = new WebSocket(`${protocol}//${window.location.host}/ws`);

                    ws.onopen = () => {
                        document.getElementById('status').textContent = 'Connected';
                        console.log('WebSocket connected');
                    };

                    ws.onmessage = (event) => {
                        const data = JSON.parse(event.data);
                        handleMessage(data);
                    };

                    ws.onerror = (error) => {
                        console.error('WebSocket error:', error);
                        document.getElementById('status').textContent = 'Error: ' + error;
                    };

                    ws.onclose = () => {
                        document.getElementById('status').textContent = 'Disconnected. Reconnecting...';
                        setTimeout(connect, 3000);
                    };
                }

                function handleMessage(data) {
                    const messagesDiv = document.getElementById('messages');

                    if (data.type === 'agent' || data.type === 'subagent') {
                        // Find or create the appropriate message container
                        let targetDiv = document.querySelector(`[data-source="${data.name}"]`);

                        if (!targetDiv) {
                            targetDiv = document.createElement('div');
                            targetDiv.className = 'message assistant';
                            targetDiv.setAttribute('data-source', data.name);

                            // Add header for subagents
                            if (data.type === 'subagent') {
                                const header = document.createElement('strong');
                                header.textContent = data.name.charAt(0).toUpperCase() + data.name.slice(1) + ': ';
                                targetDiv.appendChild(header);
                            }

                            messagesDiv.appendChild(targetDiv);
                        }

                        // Append content
                        const contentSpan = document.createElement('span');
                        contentSpan.innerHTML = data.content;  // Use innerHTML to support markdown/HTML
                        targetDiv.appendChild(contentSpan);
                    } else if (data.type === 'subagent_start') {
                        // Create placeholder for subagent
                        const subagentDiv = document.createElement('div');
                        subagentDiv.className = 'message assistant';
                        subagentDiv.setAttribute('data-source', data.name);
                        subagentDiv.innerHTML = `<strong>${data.name.charAt(0).toUpperCase() + data.name.slice(1)}:</strong> <span class="status">Loading...</span>`;
                        messagesDiv.appendChild(subagentDiv);
                    } else if (data.type === 'error') {
                        const errorDiv = document.createElement('div');
                        errorDiv.className = 'message error';
                        errorDiv.textContent = 'Error: ' + data.content;
                        messagesDiv.appendChild(errorDiv);
                    } else if (data.type === 'status') {
                        document.getElementById('status').textContent = data.content;
                    }

                    messagesDiv.scrollTop = messagesDiv.scrollHeight;
                }

                function sendMessage() {
                    const input = document.getElementById('message-input');
                    const message = input.value.trim();

                    if (!message) return;

                    // Display user message
                    const messagesDiv = document.getElementById('messages');
                    const userDiv = document.createElement('div');
                    userDiv.className = 'message user';
                    userDiv.textContent = message;
                    messagesDiv.appendChild(userDiv);

                    // Send to WebSocket
                    if (ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(JSON.stringify({
                            type: 'message',
                            content: message
                        }));
                    } else {
                        handleMessage({
                            type: 'error',
                            content: 'Not connected to server'
                        });
                    }

                    input.value = '';
                }

                // Handle Enter key
                document.getElementById('message-input').addEventListener('keypress', (e) => {
                    if (e.key === 'Enter') {
                        sendMessage();
                    }
                });

                // Connect on load
                connect();
            </script>
        </body>
    </html>
    """)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for streaming Aegis responses.

    Handles bidirectional communication with the client:
    - Receives user messages
    - Streams model responses in real-time
    - Manages conversation state per connection
    """
    await websocket.accept()

    # Initialize conversation state for this connection
    conversation_state = {
        "messages": [],
        "connection_id": str(uuid.uuid4()),
    }

    logger.info(
        "websocket.connected",
        connection_id=conversation_state["connection_id"],
    )

    try:
        while True:
            # Receive message from client
            data = await websocket.receive_text()
            message_data = json.loads(data)

            if message_data.get("type") == "message":
                user_message = message_data.get("content", "")
                selected_databases = message_data.get("databases", [])

                # Add user message to conversation
                conversation_state["messages"].append({
                    "role": "user",
                    "content": user_message
                })

                logger.info(
                    "websocket.message_received",
                    connection_id=conversation_state["connection_id"],
                    message_preview=user_message[:100] if user_message else "",
                    databases=selected_databases,
                )

                # Send status update
                await websocket.send_json({
                    "type": "status",
                    "content": "Processing your request..."
                })

                try:
                    # Prepare model kwargs with database filters if provided
                    model_kwargs = {}
                    if selected_databases:
                        model_kwargs["db_names"] = selected_databases

                    # Stream responses from the model
                    async for chunk in model(conversation_state, **model_kwargs):
                        # Send each chunk immediately to the client
                        await websocket.send_json(chunk)

                        # Track assistant responses in conversation
                        if chunk.get("type") == "agent" and chunk.get("name") == "aegis":
                            # Accumulate agent responses
                            if not conversation_state["messages"] or \
                               conversation_state["messages"][-1]["role"] != "assistant":
                                conversation_state["messages"].append({
                                    "role": "assistant",
                                    "content": chunk.get("content", "")
                                })
                            else:
                                conversation_state["messages"][-1]["content"] += chunk.get("content", "")

                    # Send completion status
                    await websocket.send_json({
                        "type": "status",
                        "content": "Ready"
                    })

                except Exception as e:
                    logger.error(
                        "websocket.model_error",
                        connection_id=conversation_state["connection_id"],
                        error=str(e),
                    )
                    await websocket.send_json({
                        "type": "error",
                        "content": f"Model error: {str(e)}"
                    })

    except WebSocketDisconnect:
        logger.info(
            "websocket.disconnected",
            connection_id=conversation_state["connection_id"],
        )
    except Exception as e:
        logger.error(
            "websocket.error",
            connection_id=conversation_state["connection_id"],
            error=str(e),
        )
        try:
            await websocket.send_json({
                "type": "error",
                "content": f"Connection error: {str(e)}"
            })
        except:
            pass  # Client may already be disconnected


@app.get("/monitoring")
async def monitoring_page():
    """Serve the monitoring dashboard interface."""
    template_path = "templates/monitoring.html"
    if os.path.exists(template_path):
        return FileResponse(template_path)
    return HTMLResponse("<h1>Monitoring dashboard not available</h1>")


@app.get("/database")
async def database_page():
    """Serve the database viewer interface."""
    template_path = "templates/database.html"
    if os.path.exists(template_path):
        return FileResponse(template_path)
    return HTMLResponse("<h1>Database viewer not available</h1>")


# Monitoring API endpoints
@app.get("/api/monitoring/summary")
async def monitoring_summary(
    hours: int = Query(default=24, description="Hours to look back"),
    limit: int = Query(default=100, description="Maximum number of runs")
):
    """Get monitoring summary data."""
    try:
        from src.aegis.connections.postgres_connector import fetch_all
        from datetime import datetime, timedelta, timezone

        # Calculate the time threshold
        threshold = datetime.now(timezone.utc) - timedelta(hours=hours)

        # Query for recent runs summary
        recent_runs_query = """
        WITH run_summary AS (
            SELECT
                run_uuid,
                MIN(stage_start_time) as start_time,
                MAX(stage_end_time) as end_time,
                COUNT(*) as stage_count,
                SUM(duration_ms) as total_duration_ms,
                SUM(total_tokens) as total_tokens,
                SUM(total_cost) as total_cost,
                STRING_AGG(DISTINCT status, ', ') as statuses,
                BOOL_OR(status != 'Success') as has_errors
            FROM process_monitor_logs
            WHERE stage_start_time >= :threshold
            GROUP BY run_uuid
            ORDER BY start_time DESC
            LIMIT :limit
        )
        SELECT * FROM run_summary
        """

        # Properly await the async function
        recent_runs = await fetch_all(
            recent_runs_query,
            params={"threshold": threshold, "limit": limit},
            execution_id="monitoring"
        )

        # Process the data for JSON serialization
        runs_list = recent_runs if recent_runs else []

        # Calculate overall stats
        total_runs = len(runs_list)
        successful_runs = sum(1 for r in runs_list if not r.get("has_errors", False))
        total_cost = sum(r.get("total_cost", 0) or 0 for r in runs_list)
        total_tokens = sum(r.get("total_tokens", 0) or 0 for r in runs_list)

        # Calculate average duration
        durations = [r.get("total_duration_ms", 0) for r in runs_list if r.get("total_duration_ms")]
        avg_duration = sum(durations) / len(durations) if durations else 0

        # Get stage statistics
        stage_stats_query = """
        SELECT
            stage_name,
            COUNT(*) as count,
            AVG(duration_ms) as avg_duration,
            SUM(total_tokens) as total_tokens,
            SUM(total_cost) as total_cost
        FROM process_monitor_logs
        WHERE stage_start_time >= :threshold
        GROUP BY stage_name
        ORDER BY count DESC
        """

        stage_stats = await fetch_all(
            stage_stats_query,
            params={"threshold": threshold},
            execution_id="monitoring"
        )

        summary = {
            "overall_stats": {
                "total_runs": total_runs,
                "successful_runs": successful_runs,
                "success_rate": (successful_runs / total_runs * 100) if total_runs > 0 else 0,
                "total_cost": total_cost,
                "total_tokens": total_tokens,
                "avg_duration_ms": avg_duration
            },
            "stage_statistics": stage_stats if stage_stats else [],
            "recent_runs": runs_list,
            "hours": hours
        }

        return summary
    except Exception as e:
        logger.error(f"Error getting monitoring summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/monitoring/runs/{run_uuid}")
async def monitoring_run_details(run_uuid: str):
    """Get detailed information about a specific run."""
    try:
        details = get_run_details(run_uuid)
        return JSONResponse(content=details, media_type="application/json")
    except Exception as e:
        logger.error(f"Error getting run details: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/monitoring/stages/{stage_name}")
async def monitoring_stage_trends(
    stage_name: str,
    hours: int = Query(default=24, description="Hours to look back")
):
    """Get stage trends."""
    try:
        trends = get_stage_trends(stage_name, hours)
        return JSONResponse(content=trends, media_type="application/json")
    except Exception as e:
        logger.error(f"Error getting stage trends: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Database viewer API endpoints
@app.get("/api/database/tables")
async def database_tables():
    """Get list of all tables in the database."""
    try:
        from src.aegis.connections.postgres_connector import fetch_all

        # Query to get all table names from information_schema
        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name
        """

        result = await fetch_all(query, execution_id="database_viewer")
        tables = [row["table_name"] for row in result] if result else []

        return {"tables": tables}
    except Exception as e:
        logger.error(f"Error getting tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/database/table/{table_name}/schema")
async def database_table_schema(table_name: str):
    """Get schema information for a specific table."""
    try:
        from src.aegis.connections.postgres_connector import get_table_schema, fetch_one

        # Get column information
        columns_raw = await get_table_schema(table_name, execution_id="database_viewer")

        # Transform column data to match frontend expectations
        columns = []
        for col in columns_raw:
            columns.append({
                "name": col.get("column_name"),
                "type": col.get("data_type"),
                "nullable": col.get("is_nullable") == "YES",
                "default": col.get("column_default"),
                "max_length": col.get("character_maximum_length"),
                "precision": col.get("numeric_precision"),
                "scale": col.get("numeric_scale")
            })

        # Get row count
        count_query = f"SELECT COUNT(*) as count FROM {table_name}"
        count_result = await fetch_one(count_query, execution_id="database_viewer")
        row_count = count_result["count"] if count_result else 0

        # Get primary key constraint
        # Using format string for table name since asyncpg doesn't handle ::regclass cast with parameters well
        pk_query = f"""
        SELECT a.attname as column_name
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = '{table_name}'::regclass AND i.indisprimary
        """
        pk_result = await fetch_all(pk_query, execution_id="database_viewer")
        pk_columns = [row["column_name"] for row in pk_result] if pk_result else []

        info = {
            "columns": columns,
            "row_count": row_count,
            "primary_key": pk_columns,
            "table_name": table_name
        }

        return info
    except Exception as e:
        logger.error(f"Error getting table info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/database/table/{table_name}/data")
async def database_table_data(
    table_name: str,
    limit: int = Query(default=100, description="Maximum rows to return"),
    offset: int = Query(default=0, description="Offset for pagination"),
    order_by: Optional[str] = Query(default=None, description="Column to order by"),
    filter: Optional[str] = Query(default=None, description="Filter condition")
):
    """Get data from a specific table."""
    try:
        from src.aegis.connections.postgres_connector import fetch_all
        import json
        from decimal import Decimal
        from datetime import datetime, date
        import uuid

        # Build query
        query = f"SELECT * FROM {table_name}"
        params = {}

        if filter:
            query += f" WHERE {filter}"

        if order_by:
            query += f" ORDER BY {order_by}"

        query += " LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset

        # Fetch data using async postgres_connector
        rows = await fetch_all(query, params=params, execution_id="database_viewer")

        if not rows:
            return {"data": [], "total_rows": 0}

        # Custom JSON encoder for PostgreSQL types
        def clean_for_json(obj):
            """Convert PostgreSQL types to JSON-serializable types."""
            if obj is None:
                return None
            elif isinstance(obj, (Decimal, float)):
                # Handle NaN and infinity
                import math
                if math.isnan(obj) or math.isinf(obj):
                    return None
                return float(obj)
            elif isinstance(obj, (datetime, date)):
                return obj.isoformat()
            elif isinstance(obj, uuid.UUID):
                return str(obj)
            elif isinstance(obj, bytes):
                try:
                    return obj.decode('utf-8', errors='ignore')
                except:
                    return str(obj)
            elif isinstance(obj, dict):
                # Recursively clean nested dicts (for JSONB columns)
                return {k: clean_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                # Recursively clean lists (for array columns)
                return [clean_for_json(item) for item in obj]
            else:
                return obj

        # Clean all rows
        cleaned_data = []
        for row in rows:
            cleaned_row = {key: clean_for_json(value) for key, value in row.items()}
            cleaned_data.append(cleaned_row)

        result = {
            "data": cleaned_data,
            "total_rows": len(cleaned_data)
        }

        return result
    except Exception as e:
        logger.error(f"Error getting table data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/database/query")
async def database_query(request: Dict[str, Any]):
    """Execute a custom SQL query."""
    try:
        from src.aegis.connections.postgres_connector import fetch_all
        from decimal import Decimal
        from datetime import datetime, date
        import uuid

        query = request.get("query", "")
        if not query:
            raise HTTPException(status_code=400, detail="No query provided")

        # Execute query using async postgres_connector
        rows = await fetch_all(query, execution_id="database_viewer")

        if not rows:
            return {"data": [], "total_rows": 0, "columns": []}

        # Custom JSON encoder for PostgreSQL types
        def clean_for_json(obj):
            """Convert PostgreSQL types to JSON-serializable types."""
            if obj is None:
                return None
            elif isinstance(obj, (Decimal, float)):
                import math
                if math.isnan(obj) or math.isinf(obj):
                    return None
                return float(obj)
            elif isinstance(obj, (datetime, date)):
                return obj.isoformat()
            elif isinstance(obj, uuid.UUID):
                return str(obj)
            elif isinstance(obj, bytes):
                try:
                    return obj.decode('utf-8', errors='ignore')
                except:
                    return str(obj)
            elif isinstance(obj, dict):
                return {k: clean_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [clean_for_json(item) for item in obj]
            else:
                return obj

        # Clean all rows
        cleaned_data = []
        for row in rows:
            cleaned_row = {key: clean_for_json(value) for key, value in row.items()}
            cleaned_data.append(cleaned_row)

        result = {
            "data": cleaned_data,
            "total_rows": len(cleaned_data),
            "columns": list(rows[0].keys()) if rows else []
        }

        return result
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/database/samples")
async def database_samples():
    """Get sample queries for the database interface."""
    try:
        # Return predefined sample queries
        samples = [
            {
                "name": "Recent Process Monitor Logs",
                "query": """SELECT run_uuid, stage_name, status, duration_ms, stage_start_time
FROM process_monitor_logs
ORDER BY stage_start_time DESC
LIMIT 20"""
            },
            {
                "name": "Data Availability Summary",
                "query": """SELECT bank_name, bank_symbol, fiscal_year, quarter,
       array_length(database_names, 1) as database_count
FROM aegis_data_availability
ORDER BY fiscal_year DESC, quarter DESC
LIMIT 20"""
            },
            {
                "name": "Failed Processes",
                "query": """SELECT run_uuid, stage_name, error_message, stage_start_time
FROM process_monitor_logs
WHERE status != 'Success'
ORDER BY stage_start_time DESC
LIMIT 20"""
            },
            {
                "name": "Average Stage Duration",
                "query": """SELECT stage_name,
       COUNT(*) as execution_count,
       AVG(duration_ms) as avg_duration_ms,
       MIN(duration_ms) as min_duration_ms,
       MAX(duration_ms) as max_duration_ms
FROM process_monitor_logs
GROUP BY stage_name
ORDER BY avg_duration_ms DESC"""
            },
            {
                "name": "Banks with Most Data",
                "query": """SELECT bank_name, bank_symbol,
       COUNT(*) as quarters_available,
       MIN(fiscal_year || '-Q' || quarter) as earliest_period,
       MAX(fiscal_year || '-Q' || quarter) as latest_period
FROM aegis_data_availability
GROUP BY bank_name, bank_symbol
ORDER BY quarters_available DESC"""
            }
        ]

        return {"samples": samples}
    except Exception as e:
        logger.error(f"Error getting sample queries: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/database/overview")
async def database_overview():
    """Get database overview statistics."""
    try:
        from src.aegis.connections.postgres_connector import fetch_all, fetch_one

        # Query to get all table names and their details
        tables_query = """
        WITH table_info AS (
            SELECT
                t.table_name,
                COUNT(DISTINCT c.column_name) as column_count
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c
                ON t.table_name = c.table_name AND t.table_schema = c.table_schema
            WHERE t.table_schema = 'public'
            GROUP BY t.table_name
        ),
        index_info AS (
            SELECT
                tablename as table_name,
                COUNT(*) as index_count
            FROM pg_indexes
            WHERE schemaname = 'public'
            GROUP BY tablename
        ),
        pk_info AS (
            SELECT
                tc.table_name,
                string_agg(kcu.column_name, ', ') as primary_key
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = 'public'
            GROUP BY tc.table_name
        ),
        fk_info AS (
            SELECT
                tc.table_name,
                COUNT(*) as foreign_key_count
            FROM information_schema.table_constraints tc
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'public'
            GROUP BY tc.table_name
        )
        SELECT
            ti.table_name as table,
            ti.column_count as columns,
            COALESCE(ii.index_count, 0) as indexes,
            pk.primary_key,
            COALESCE(fk.foreign_key_count, 0) as foreign_keys
        FROM table_info ti
        LEFT JOIN index_info ii ON ti.table_name = ii.table_name
        LEFT JOIN pk_info pk ON ti.table_name = pk.table_name
        LEFT JOIN fk_info fk ON ti.table_name = fk.table_name
        ORDER BY ti.table_name
        """

        tables_result = await fetch_all(tables_query, execution_id="database_viewer")

        # Get row counts for each table (separate query for performance)
        table_details = []
        total_rows = 0
        total_columns = 0
        total_indexes = 0

        for table in tables_result if tables_result else []:
            # Get row count for this table
            count_query = f"SELECT COUNT(*) as count FROM {table['table']}"
            count_result = await fetch_one(count_query, execution_id="database_viewer")
            row_count = count_result["count"] if count_result else 0

            total_rows += row_count
            total_columns += table["columns"]
            total_indexes += table["indexes"]

            table_details.append({
                "table": table["table"],
                "rows": row_count,
                "columns": table["columns"],
                "indexes": table["indexes"],
                "primary_key": table.get("primary_key", ""),
                "foreign_keys": table.get("foreign_keys", 0)
            })

        # Calculate averages
        num_tables = len(table_details)
        avg_columns = round(total_columns / num_tables) if num_tables > 0 else 0

        overview = {
            "summary": {
                "total_tables": num_tables,
                "total_rows": total_rows,
                "avg_columns": avg_columns,
                "total_indexes": total_indexes
            },
            "tables": table_details,
            "database_name": config.postgres_database,
            "host": config.postgres_host
        }
        return overview
    except Exception as e:
        logger.error(f"Error getting database overview: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Conversation management endpoints
@app.post("/api/reset")
async def reset_conversation():
    """Reset the conversation history (for session-based management)."""
    # Note: In WebSocket implementation, each connection has its own state
    # This endpoint is kept for compatibility with the original interface
    return {"status": "Conversation reset", "conversation_length": 0}


@app.get("/api/history")
async def get_history():
    """Get conversation history (placeholder for compatibility)."""
    # Note: In WebSocket implementation, history is managed per connection
    return []


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "2.1.0"
    }


# Note: Startup and shutdown are handled by the lifespan context manager above


def main():
    """Main entry point for the FastAPI application."""
    parser = argparse.ArgumentParser(description="Run Aegis FastAPI with WebSockets")
    parser.add_argument(
        "--host",
        default=os.getenv("SERVER_HOST", "127.0.0.1"),
        help="Host to run the server on (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("SERVER_PORT", "8000")),
        help="Port to run the server on (default: 8000)"
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        default=os.getenv("DEBUG", "false").lower() == "true",
        help="Enable auto-reload for development"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes (default: 1)"
    )

    args = parser.parse_args()

    print(f"Starting Aegis FastAPI server on http://{args.host}:{args.port}")
    print("Available endpoints:")
    print(f"  - WebSocket: ws://{args.host}:{args.port}/ws")
    print(f"  - Chat UI:   http://{args.host}:{args.port}/")
    print(f"  - Health:    http://{args.host}:{args.port}/health")
    print("\nWebSocket features:")
    print("  - Real-time streaming responses")
    print("  - Concurrent request handling")
    print("  - Automatic reconnection")
    print("  - Per-connection conversation state")

    # Run with uvicorn
    uvicorn.run(
        "run_fastapi:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        workers=args.workers if not args.reload else 1,  # Can't use multiple workers with reload
        log_level="info"
    )


if __name__ == "__main__":
    main()