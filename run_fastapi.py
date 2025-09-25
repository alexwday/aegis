#!/usr/bin/env python
"""
FastAPI application with WebSocket support for Aegis.

This replaces the Flask application with a fully async FastAPI implementation
featuring WebSocket streaming for real-time responses.
"""

import argparse
import os
import asyncio
import json
from typing import Dict, Any, Optional
import uuid
from datetime import datetime

from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
import uvicorn

from src.aegis.model.main import model
from src.aegis.utils.logging import setup_logging, get_logger
from src.aegis.connections.llm_connector import close_all_clients
from src.aegis.connections.postgres_connector import close_all_connections

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
    description="WebSocket-based streaming interface for Aegis model",
    version="2.0.0",
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

# Mount static files (if templates directory exists)
if os.path.exists("templates"):
    app.mount("/static", StaticFiles(directory="templates"), name="static")


@app.get("/")
async def root():
    """Serve the main chat interface."""
    template_path = "templates/chat.html"
    if os.path.exists(template_path):
        return FileResponse(template_path)
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

                # Add user message to conversation
                conversation_state["messages"].append({
                    "role": "user",
                    "content": user_message
                })

                logger.info(
                    "websocket.message_received",
                    connection_id=conversation_state["connection_id"],
                    message_preview=user_message[:100] if user_message else "",
                )

                # Send status update
                await websocket.send_json({
                    "type": "status",
                    "content": "Processing your request..."
                })

                try:
                    # Stream responses from the model
                    async for chunk in model(conversation_state):
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


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "2.0.0"
    }


@app.on_event("startup")
async def startup_event():
    """Initialize resources on startup."""
    logger.info("fastapi.startup", message="Aegis FastAPI server starting up")
    setup_logging()


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown."""
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