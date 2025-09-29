"""
Configuration for Key Themes ETL.

This config provides model defaults that can be overridden:
- If MODELS[key] is None, uses the default from main config
- If MODELS[key] is a string, uses that specific model
- Can be overridden via environment variables
"""

import os

def get_model(task_type: str, override_model: str = None) -> str:
    """
    Get the appropriate model for a task.

    Args:
        task_type: One of 'theme_extraction', 'formatting', 'grouping'
        override_model: Optional model override

    Returns:
        Model name to use
    """
    # If override provided, use it
    if override_model:
        return override_model

    # Check environment variable override
    env_var = f"KEY_THEMES_MODEL_{task_type.upper()}"
    env_model = os.getenv(env_var)
    if env_model:
        return env_model

    # Use configured model or fall back to defaults
    model = MODELS.get(task_type)
    if model:
        return model

    # Fall back to main config defaults
    try:
        from aegis.utils.settings import config
        if task_type == "formatting":
            return config.llm.large.model
        else:  # theme_extraction, grouping
            return config.llm.medium.model
    except ImportError:
        # Ultimate fallbacks if main config not available
        if task_type == "formatting":
            return "gpt-4o"
        else:
            return "gpt-4o-mini"

# Model configuration - set to None to use defaults, or specify a model name
MODELS = {
    "theme_extraction": None,  # Will use config.llm.medium.model
    "formatting": None,         # Will use config.llm.large.model
    "grouping": None,          # Will use config.llm.medium.model
}

# You can override specific models here if needed:
# MODELS["theme_extraction"] = "gpt-4o-mini"  # Force specific model
# MODELS["formatting"] = "gpt-4o"             # Force specific model

# Other configuration options
MAX_PARALLEL_REQUESTS = int(os.getenv("KEY_THEMES_MAX_PARALLEL", "12"))
TEMPERATURE = float(os.getenv("KEY_THEMES_TEMPERATURE", "0.5"))
MAX_TOKENS = int(os.getenv("KEY_THEMES_MAX_TOKENS", "32768"))  # Standard max tokens for all ETLs