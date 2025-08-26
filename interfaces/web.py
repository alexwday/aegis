"""
Web interface for Aegis model interaction.

This module provides a Flask-based web server with HTML chat interface
for interacting with the Aegis model.
"""

import json
import os
from pathlib import Path
from typing import Generator

from flask import Flask, render_template, request, jsonify, Response

from src.aegis.model.main import model
from src.aegis.utils.logging import get_logger
from interfaces.monitoring import register_monitoring_routes
from interfaces.database import register_database_routes

# Set up the Flask app with the correct template directory
# Templates are in the project root
template_dir = Path(__file__).parent.parent / "templates"
app = Flask(__name__, template_folder=str(template_dir))
logger = get_logger()

# Register monitoring routes
register_monitoring_routes(app)

# Register database viewer routes
register_database_routes(app)

# Store conversation history in memory (for simplicity)
# In production, you'd want to use a session-based storage
conversation_history = []


@app.route("/")
def index():
    """
    Serve the main chat interface.

    Returns:
        HTML template for the chat interface
    """
    return render_template("chat.html")


@app.route("/monitoring")
def monitoring():
    """
    Serve the monitoring dashboard interface.

    Returns:
        HTML template for the monitoring dashboard
    """
    return render_template("monitoring.html")


@app.route("/api/chat", methods=["POST"])
def chat():
    """
    Handle chat requests and interact with the Aegis model.

    Returns:
        JSON response with the model's reply or error message
    """
    try:
        data = request.json
        user_message = data.get("message", "").strip()
        selected_databases = data.get("databases", [])

        if not user_message:
            return jsonify({"error": "No message provided"}), 400

        # Add user message to conversation history
        conversation_history.append({"role": "user", "content": user_message})

        # Prepare input for the model
        model_input = {"messages": conversation_history.copy()}

        # Call the model with database filters if provided
        model_kwargs = {}
        if selected_databases:
            model_kwargs["db_names"] = selected_databases

        # Call the model and collect the response
        assistant_response = ""
        for chunk in model(model_input, **model_kwargs):
            # Handle different response types from the model
            if chunk.get("type") in ["agent", "chunk"]:
                assistant_response += chunk.get("content", "")

        # Add assistant response to conversation history
        if assistant_response:
            conversation_history.append({"role": "assistant", "content": assistant_response})

        return jsonify(
            {"response": assistant_response, "conversation_length": len(conversation_history)}
        )

    except Exception as e:
        logger.error(f"Error in chat endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/chat/stream", methods=["POST"])
def chat_stream():
    """
    Handle streaming chat requests for real-time responses.

    Returns:
        Server-sent events stream with model responses
    """
    try:
        data = request.json
        user_message = data.get("message", "").strip()
        selected_databases = data.get("databases", [])

        if not user_message:
            return jsonify({"error": "No message provided"}), 400

        # Add user message to conversation history
        conversation_history.append({"role": "user", "content": user_message})

        def generate() -> Generator[str, None, None]:
            """
            Generate streaming response from the model.

            Yields:
                Server-sent event formatted strings
            """
            model_input = {"messages": conversation_history.copy()}

            # Add database filters if provided
            model_kwargs = {}
            if selected_databases:
                model_kwargs["db_names"] = selected_databases

            assistant_response = ""
            subagent_responses = {}  # Track responses from each subagent

            try:
                for chunk in model(model_input, **model_kwargs):
                    msg_type = chunk.get("type")
                    msg_name = chunk.get("name", "")
                    content = chunk.get("content", "")

                    if msg_type == "agent":
                        # Main agent response
                        assistant_response += content
                        # Send as main agent chunk
                        response_data = {"type": "agent", "name": "aegis", "content": content}
                        yield f"data: {json.dumps(response_data)}\n\n"

                    elif msg_type == "subagent_start":
                        # Forward subagent start signal directly
                        response_data = {"type": "subagent_start", "name": msg_name}
                        yield f"data: {json.dumps(response_data)}\n\n"
                        if msg_name not in subagent_responses:
                            subagent_responses[msg_name] = ""

                    elif msg_type == "subagent":
                        # Subagent response - track separately
                        if msg_name not in subagent_responses:
                            subagent_responses[msg_name] = ""

                        subagent_responses[msg_name] += content
                        # Send as subagent chunk with name
                        response_data = {"type": "subagent", "name": msg_name, "content": content}
                        yield f"data: {json.dumps(response_data)}\n\n"

                    elif msg_type == "summarizer_start":
                        # Forward summarizer start signal
                        response_data = {"type": "summarizer_start", "name": msg_name}
                        yield f"data: {json.dumps(response_data)}\n\n"

                    elif msg_type == "chunk":
                        # Legacy support for old chunk format
                        assistant_response += content
                        response_data = {"type": "agent", "name": "aegis", "content": content}
                        yield f"data: {json.dumps(response_data)}\n\n"

                # Add complete response to conversation history
                full_response = assistant_response
                if subagent_responses:
                    # Include subagent responses in history
                    full_response += "\n\n---\n**Database Responses:**\n"
                    for name, response in subagent_responses.items():
                        full_response += f"\n[{name.upper()}]:\n{response}\n"

                if full_response:
                    conversation_history.append({"role": "assistant", "content": full_response})

                # Send completion signal
                yield f"data: {json.dumps({'done': True})}\n\n"

            except Exception as e:
                logger.error(f"Error in streaming: {str(e)}")
                yield f"data: {json.dumps({'error': str(e)})}\n\n"

        return Response(generate(), mimetype="text/event-stream")

    except Exception as e:
        logger.error(f"Error in stream endpoint: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/reset", methods=["POST"])
def reset_conversation():
    """
    Reset the conversation history.

    Returns:
        JSON confirmation of reset
    """
    global conversation_history
    conversation_history = []
    return jsonify({"status": "Conversation reset", "conversation_length": 0})


@app.route("/api/history", methods=["GET"])
def get_history():
    """
    Get the current conversation history.

    Returns:
        JSON array of conversation messages
    """
    return jsonify(conversation_history)

