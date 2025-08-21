"""
Configuration management module.

This module handles loading and accessing environment variables
from .env files and system environment.
"""

import os
from dataclasses import dataclass
from typing import List, Optional

from dotenv import load_dotenv


@dataclass
class OAuthConfig:
    """OAuth configuration settings."""

    endpoint: str
    client_id: str
    client_secret: str
    grant_type: str
    max_retries: int
    retry_delay: int


@dataclass
class SSLConfig:
    """SSL configuration settings."""

    verify: bool
    cert_path: str


@dataclass
class ConversationConfig:
    """Conversation processing configuration."""

    include_system_messages: bool
    allowed_roles: List[str]
    max_history_length: int


@dataclass
class LLMModelConfig:
    """Configuration for a single LLM model tier."""

    model: str
    temperature: float
    max_tokens: int
    timeout: int
    max_retries: int


@dataclass
class LLMEmbeddingConfig:
    """Configuration for embedding model."""

    model: str
    dimensions: int
    timeout: int
    max_retries: int


@dataclass
class LLMConfig:
    """LLM service configuration with model tiers."""

    base_url: str
    small: LLMModelConfig
    medium: LLMModelConfig
    large: LLMModelConfig
    embedding: LLMEmbeddingConfig


class Config:  # pylint: disable=too-many-instance-attributes
    """
    Centralized configuration management from environment variables.

    Implements singleton pattern to ensure only one configuration
    instance exists throughout the application lifecycle. Loads from
    .env file if present, otherwise uses system environment variables.

    Attributes:
        log_level: Logging level (DEBUG/INFO/WARNING/ERROR/CRITICAL). Default: INFO
        auth_method: Authentication method ("oauth" or "api_key"). Default: "api_key"
        api_key: Direct API key for LLM. Default: ""
        oauth: OAuth configuration settings
        ssl: SSL configuration settings
        conversation: Conversation processing settings

    Environment Variables:
        LOG_LEVEL: Set logging verbosity
        AUTH_METHOD: Authentication method ("oauth" or "api_key")
        API_KEY: Direct API key for LLM (used when AUTH_METHOD=api_key)
        INCLUDE_SYSTEM_MESSAGES: "true"/"false" to include/exclude system messages
        ALLOWED_ROLES: Comma-separated list of allowed message roles
        MAX_HISTORY_LENGTH: Number of recent messages to keep
        SSL_VERIFY: "true"/"false" to enable/disable SSL verification
        SSL_CERT_PATH: Path to certificate file when SSL_VERIFY=true
        OAUTH_ENDPOINT: OAuth token endpoint URL
        OAUTH_CLIENT_ID: OAuth client ID for authentication
        OAUTH_CLIENT_SECRET: OAuth client secret for authentication
        OAUTH_GRANT_TYPE: OAuth grant type (typically client_credentials)
        OAUTH_MAX_RETRIES: Maximum retry attempts for token generation
        OAUTH_RETRY_DELAY: Initial retry delay in seconds
    """

    _instance = None
    _loaded = False

    def __new__(cls):
        """
        Create or return the singleton instance.

        Returns:
            The single Config instance.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize configuration by loading environment variables."""
        if not Config._loaded:
            self.load_config()
            Config._loaded = True

    def load_config(self) -> None:
        """
        Load configuration from .env file and environment variables.

        Loads .env file if present and sets configuration attributes
        from environment variables with appropriate defaults.
        """
        # Load .env file if it exists
        load_dotenv()

        # Top-level Configuration
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.auth_method = os.getenv("AUTH_METHOD", "api_key").lower()
        self.api_key = os.getenv("API_KEY", "")

        # Conversation Configuration
        self.conversation = ConversationConfig(
            include_system_messages=os.getenv("INCLUDE_SYSTEM_MESSAGES", "false").lower() == "true",
            allowed_roles=[
                role.strip() for role in os.getenv("ALLOWED_ROLES", "user,assistant").split(",")
            ],
            max_history_length=int(os.getenv("MAX_HISTORY_LENGTH", "10")),
        )

        # SSL Configuration
        self.ssl = SSLConfig(
            verify=os.getenv("SSL_VERIFY", "false").lower() == "true",
            cert_path=os.getenv("SSL_CERT_PATH", ""),
        )

        # OAuth Configuration
        self.oauth = OAuthConfig(
            endpoint=os.getenv("OAUTH_ENDPOINT", ""),
            client_id=os.getenv("OAUTH_CLIENT_ID", ""),
            client_secret=os.getenv("OAUTH_CLIENT_SECRET", ""),
            grant_type=os.getenv("OAUTH_GRANT_TYPE", "client_credentials"),
            max_retries=int(os.getenv("OAUTH_MAX_RETRIES", "3")),
            retry_delay=int(os.getenv("OAUTH_RETRY_DELAY", "1")),
        )

        # LLM Configuration
        self.llm = LLMConfig(
            base_url=os.getenv("LLM_BASE_URL", "https://api.openai.com/v1"),
            small=LLMModelConfig(
                model=os.getenv("LLM_MODEL_SMALL", "gpt-4.1-nano-2025-04-14"),
                temperature=float(os.getenv("LLM_TEMPERATURE_SMALL", "0.3")),
                max_tokens=int(os.getenv("LLM_MAX_TOKENS_SMALL", "1000")),
                timeout=int(os.getenv("LLM_TIMEOUT_SMALL", "30")),
                max_retries=int(os.getenv("LLM_MAX_RETRIES_SMALL", "3")),
            ),
            medium=LLMModelConfig(
                model=os.getenv("LLM_MODEL_MEDIUM", "gpt-4.1-mini-2025-04-14"),
                temperature=float(os.getenv("LLM_TEMPERATURE_MEDIUM", "0.5")),
                max_tokens=int(os.getenv("LLM_MAX_TOKENS_MEDIUM", "2000")),
                timeout=int(os.getenv("LLM_TIMEOUT_MEDIUM", "60")),
                max_retries=int(os.getenv("LLM_MAX_RETRIES_MEDIUM", "3")),
            ),
            large=LLMModelConfig(
                model=os.getenv("LLM_MODEL_LARGE", "gpt-4.1-2025-04-14"),
                temperature=float(os.getenv("LLM_TEMPERATURE_LARGE", "0.7")),
                max_tokens=int(os.getenv("LLM_MAX_TOKENS_LARGE", "4000")),
                timeout=int(os.getenv("LLM_TIMEOUT_LARGE", "120")),
                max_retries=int(os.getenv("LLM_MAX_RETRIES_LARGE", "3")),
            ),
            embedding=LLMEmbeddingConfig(
                model=os.getenv("LLM_EMBEDDING_MODEL", "text-embedding-3-large"),
                dimensions=int(os.getenv("LLM_EMBEDDING_DIMENSIONS", "3072")),
                timeout=int(os.getenv("LLM_EMBEDDING_TIMEOUT", "30")),
                max_retries=int(os.getenv("LLM_EMBEDDING_MAX_RETRIES", "3")),
            ),
        )

        # Create legacy attributes for backward compatibility
        self._create_legacy_attributes()

    def _create_legacy_attributes(self) -> None:
        """Create legacy attributes for backward compatibility."""
        # pylint: disable=attribute-defined-outside-init
        # OAuth attributes
        self.oauth_endpoint = self.oauth.endpoint
        self.oauth_client_id = self.oauth.client_id
        self.oauth_client_secret = self.oauth.client_secret
        self.oauth_grant_type = self.oauth.grant_type
        self.oauth_max_retries = self.oauth.max_retries
        self.oauth_retry_delay = self.oauth.retry_delay

        # SSL attributes
        self.ssl_verify = self.ssl.verify
        self.ssl_cert_path = self.ssl.cert_path

        # Conversation attributes
        self.include_system_messages = self.conversation.include_system_messages
        self.allowed_roles = self.conversation.allowed_roles
        self.max_history_length = self.conversation.max_history_length
        # pylint: enable=attribute-defined-outside-init

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a configuration value by key.

        Args:
            key: Configuration key to retrieve.
            default: Default value if key doesn't exist.

        Returns:
            Configuration value or default if not found.
        """
        return getattr(self, key, default)


# Global config instance
config = Config()
