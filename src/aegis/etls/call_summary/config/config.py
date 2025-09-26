"""
Configuration for Call Summary ETL.

This config allows overriding the default model names used in the ETL
without modifying the main code. The ETL uses different models for different
stages to optimize quality and performance.
"""

import os

# Model configuration for different LLM calls
# The ETL uses three distinct model tiers:
# 1. Base model for general summarization
# 2. Research plan model for comprehensive content mapping
# 3. Category extraction model for synthesis and insights
MODELS = {
    # Base model for general summarization tasks
    "summarization": os.getenv("CALL_SUMMARY_MODEL", "gpt-4o"),

    # Model for research planning stage (comprehensive analysis)
    "research_plan": os.getenv("CALL_SUMMARY_RESEARCH_MODEL", "gpt-4o"),

    # Model for category extraction (maximum quality synthesis)
    "category_extraction": os.getenv("CALL_SUMMARY_EXTRACTION_MODEL", "gpt-4o"),
}

# Optional: Fall back to main config if needed
try:
    from aegis.utils.settings import config
    # Use main config models as defaults if environment variables not set
    if not os.getenv("CALL_SUMMARY_MODEL"):
        MODELS["summarization"] = config.llm.large.model
    if not os.getenv("CALL_SUMMARY_RESEARCH_MODEL"):
        MODELS["research_plan"] = config.llm.large.model
    if not os.getenv("CALL_SUMMARY_EXTRACTION_MODEL"):
        MODELS["category_extraction"] = config.llm.large.model
except ImportError:
    pass

# Other configuration options
TEMPERATURE = float(os.getenv("CALL_SUMMARY_TEMPERATURE", "0.7"))
MAX_TOKENS = int(os.getenv("CALL_SUMMARY_MAX_TOKENS", "4000"))