"""
Configuration for Call Summary ETL.

This config allows overriding the default model names used in the ETL
without modifying the main code.
"""

import os

# Model configuration for different LLM calls
MODELS = {
    "summarization": os.getenv("CALL_SUMMARY_MODEL", "gpt-4o"),
}

# Optional: Fall back to main config if needed
try:
    from aegis.utils.settings import config
    # Use main config models as defaults if environment variables not set
    if not os.getenv("CALL_SUMMARY_MODEL"):
        MODELS["summarization"] = config.llm.large.model
except ImportError:
    pass

# Other configuration options
TEMPERATURE = float(os.getenv("CALL_SUMMARY_TEMPERATURE", "0.7"))
MAX_TOKENS = int(os.getenv("CALL_SUMMARY_MAX_TOKENS", "4000"))