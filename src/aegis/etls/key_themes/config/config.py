"""
Configuration for Key Themes ETL.

This config allows overriding the default model names used in the ETL
without modifying the main code.
"""

import os

# Model configuration for different LLM calls
MODELS = {
    "theme_extraction": os.getenv("KEY_THEMES_MODEL_EXTRACTION", "gpt-4o-mini"),
    "formatting": os.getenv("KEY_THEMES_MODEL_FORMATTING", "gpt-4o"),
    "grouping": os.getenv("KEY_THEMES_MODEL_GROUPING", "gpt-4o-mini"),
}

# Optional: Fall back to main config if needed
try:
    from aegis.utils.settings import config
    # Use main config models as defaults if environment variables not set
    if not os.getenv("KEY_THEMES_MODEL_EXTRACTION"):
        MODELS["theme_extraction"] = config.llm.medium.model
    if not os.getenv("KEY_THEMES_MODEL_FORMATTING"):
        MODELS["formatting"] = config.llm.large.model
    if not os.getenv("KEY_THEMES_MODEL_GROUPING"):
        MODELS["grouping"] = config.llm.medium.model
except ImportError:
    pass

# Other configuration options
MAX_PARALLEL_REQUESTS = int(os.getenv("KEY_THEMES_MAX_PARALLEL", "12"))
TEMPERATURE = float(os.getenv("KEY_THEMES_TEMPERATURE", "0.5"))
MAX_TOKENS = int(os.getenv("KEY_THEMES_MAX_TOKENS", "16384"))  # OpenAI model limit