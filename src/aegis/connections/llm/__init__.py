"""
LLM connector module for OpenAI integration.

This module provides functions to interact with OpenAI's API using
configurable models and authentication methods.
"""

from aegis.connections.llm.llm_connector import (
    complete,
    stream,
    complete_with_tools,
    check_connection,
    embed,
    embed_batch,
)

__all__ = [
    "complete",
    "stream",
    "complete_with_tools",
    "check_connection",
    "embed",
    "embed_batch",
]
