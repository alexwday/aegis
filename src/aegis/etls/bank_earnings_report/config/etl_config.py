"""
ETL Configuration loader for Bank Earnings Report.

This module provides configuration management for the bank earnings report ETL,
kept separate from main.py to avoid circular imports when extraction modules
need to access configuration settings.
"""

import os
from typing import Any, Dict

import yaml

from aegis.utils.settings import config


class ETLConfig:
    """
    Configuration loader for the Bank Earnings Report ETL.

    Reads YAML configuration files and resolves model tier references to actual
    model names from the global aegis settings.

    Attributes:
        config_path: Path to the YAML configuration file.
    """

    def __init__(self, config_path: str):
        """
        Initialize the ETL configuration loader.

        Args:
            config_path: Absolute or relative path to the YAML configuration file.

        Raises:
            FileNotFoundError: If the configuration file does not exist.
        """
        self.config_path = config_path
        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """
        Load configuration from YAML file.

        Returns:
            Dictionary containing the parsed YAML configuration.

        Raises:
            FileNotFoundError: If the configuration file does not exist.
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def get_model(self, model_key: str) -> str:
        """
        Get the actual model name for a given model key.

        Resolves model tier references (small/medium/large) to actual model names
        from the global settings configuration.

        Args:
            model_key: The model key defined in the YAML configuration.

        Returns:
            The actual model name string from global settings.

        Raises:
            KeyError: If the model key is not found in configuration.
            ValueError: If no tier is specified or tier is invalid.
        """
        if "models" not in self._config or model_key not in self._config["models"]:
            raise KeyError(f"Model key '{model_key}' not found in configuration")

        tier = self._config["models"][model_key].get("tier")
        if not tier:
            raise ValueError(f"No tier specified for model '{model_key}'")

        tier_map = {
            "small": config.llm.small.model,
            "medium": config.llm.medium.model,
            "large": config.llm.large.model,
        }

        if tier not in tier_map:
            raise ValueError(
                f"Invalid tier '{tier}' for model '{model_key}'. "
                f"Valid tiers: {list(tier_map.keys())}"
            )

        return tier_map[tier]

    @property
    def temperature(self) -> float:
        """
        Get the LLM temperature parameter.

        Returns:
            Temperature value for LLM calls, defaults to 0.1 if not configured.
        """
        return self._config.get("llm", {}).get("temperature", 0.1)

    @property
    def max_tokens(self) -> int:
        """
        Get the LLM max_tokens parameter.

        Returns:
            Maximum token limit for LLM responses, defaults to 32768 if not configured.
        """
        return self._config.get("llm", {}).get("max_tokens", 32768)


etl_config = ETLConfig(os.path.join(os.path.dirname(__file__), "config.yaml"))
