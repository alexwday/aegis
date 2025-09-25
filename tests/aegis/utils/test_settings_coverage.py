"""
Comprehensive tests for settings.py module.
"""

import os
from unittest import mock
import pytest
from aegis.utils.settings import config, Config


def test_config_get_method():
    """Test the get method of Config class."""
    # Test getting an existing attribute
    result = config.get("log_level")
    assert result is not None

    # Test getting a non-existent attribute with default
    result = config.get("non_existent_key", "default_value")
    assert result == "default_value"

    # Test getting a non-existent attribute without default
    result = config.get("another_non_existent_key")
    assert result is None


def test_config_singleton_pattern():
    """Test that Config follows singleton pattern."""
    # Get the config instance
    config1 = Config()
    config2 = Config()

    # Both should be the same instance
    assert config1 is config2
    assert config1 is config


def test_config_dataclass_attributes():
    """Test that all dataclass attributes are accessible."""
    # Test OAuth config
    assert hasattr(config, 'oauth')
    assert hasattr(config.oauth, 'endpoint')
    assert hasattr(config.oauth, 'client_id')
    assert hasattr(config.oauth, 'max_retries')

    # Test SSL config
    assert hasattr(config, 'ssl')
    assert hasattr(config.ssl, 'verify')
    assert hasattr(config.ssl, 'cert_path')

    # Test LLM configs
    assert hasattr(config, 'llm')
    assert hasattr(config.llm, 'small')
    assert hasattr(config.llm.small, 'model')
    assert hasattr(config.llm.small, 'temperature')
    assert hasattr(config.llm.small, 'max_tokens')

    # Test Embedding config (under llm)
    assert hasattr(config.llm, 'embedding')
    assert hasattr(config.llm.embedding, 'model')
    assert hasattr(config.llm.embedding, 'dimensions')


def test_config_environment_variable_types():
    """Test that environment variables are properly typed."""
    # Test string types
    assert isinstance(config.log_level, str)
    assert isinstance(config.auth_method, str)

    # Test boolean types
    assert isinstance(config.ssl.verify, bool)
    assert isinstance(config.include_system_messages, bool)

    # Test integer types
    assert isinstance(config.oauth.max_retries, int)
    assert isinstance(config.max_history_length, int)
    # postgres_port is stored as string but should be valid integer
    assert isinstance(config.postgres_port, str)
    assert config.postgres_port.isdigit()

    # Test float types
    assert isinstance(config.llm.small.temperature, float)
    assert isinstance(config.llm.small.cost_per_1k_input, float)

    # Test list types
    assert isinstance(config.allowed_roles, list)
    assert all(isinstance(role, str) for role in config.allowed_roles)


def test_config_defaults():
    """Test that config has sensible defaults."""
    # These should have defaults even if not in env
    assert config.log_level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    assert config.auth_method in ['oauth', 'api_key']
    assert config.environment in ['local', 'dev', 'sai', 'prod', 'test']

    # OAuth defaults
    assert config.oauth.grant_type == 'client_credentials'
    assert config.oauth.max_retries >= 1
    assert config.oauth.retry_delay >= 1

    # LLM defaults
    assert config.llm.small.temperature >= 0.0
    assert config.llm.small.temperature <= 2.0
    assert config.llm.small.max_tokens > 0
    assert config.llm.small.timeout > 0


def test_config_postgres_settings():
    """Test PostgreSQL configuration."""
    assert hasattr(config, 'postgres_host')
    assert hasattr(config, 'postgres_port')
    assert hasattr(config, 'postgres_database')
    assert hasattr(config, 'postgres_user')
    assert hasattr(config, 'postgres_password')

    # Port should be valid (it's stored as string)
    assert 1 <= int(config.postgres_port) <= 65535


def test_config_llm_tiers():
    """Test that all LLM tiers are configured."""
    tiers = ['small', 'medium', 'large']

    for tier in tiers:
        tier_config = getattr(config.llm, tier)
        assert tier_config is not None
        assert tier_config.model is not None
        assert tier_config.model != ''
        assert 0.0 <= tier_config.temperature <= 2.0
        assert tier_config.max_tokens > 0
        assert tier_config.cost_per_1k_input >= 0
        assert tier_config.cost_per_1k_output >= 0