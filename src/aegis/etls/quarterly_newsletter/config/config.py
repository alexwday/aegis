"""
Configuration for Quarterly Newsletter ETL.

This config allows overriding the default model names used in the ETL
without modifying the main code.
"""

import os

# Model configuration for LLM calls
MODELS = {
    "summary": os.getenv("NEWSLETTER_SUMMARY_MODEL", "gpt-4o-mini"),
}

# Optional: Fall back to main config if needed
try:
    from aegis.utils.settings import config
    # Use main config models as defaults if environment variables not set
    if not os.getenv("NEWSLETTER_SUMMARY_MODEL"):
        MODELS["summary"] = config.llm.medium.model
except ImportError:
    pass

# Other configuration options
TEMPERATURE = float(os.getenv("NEWSLETTER_TEMPERATURE", "0.7"))
MAX_TOKENS = int(os.getenv("NEWSLETTER_MAX_TOKENS", "32768"))  # Standard max tokens for all ETLs