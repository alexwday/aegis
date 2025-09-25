"""
Shared pytest fixtures and configuration for all tests.

This module provides common fixtures used across multiple test files to reduce
duplication and ensure consistent test isolation.
"""

import asyncio
import pytest
import pytest_asyncio

from aegis.utils.settings import config
from aegis.utils.monitor import clear_monitor_entries

# Configure pytest-asyncio to auto-detect async tests
pytest_plugins = ("pytest_asyncio",)


@pytest.fixture(autouse=True)
def reset_config():
    """
    Save and restore config values for test isolation.

    This fixture runs automatically for all tests to ensure environment
    configuration doesn't leak between tests.
    """
    # Save original values
    original_values = {
        "ssl_verify": config.ssl_verify,
        "ssl_cert_path": config.ssl_cert_path,
        "include_system_messages": config.include_system_messages,
        "allowed_roles": config.allowed_roles[:] if config.allowed_roles else [],
        "max_history_length": config.max_history_length,
        "auth_method": config.auth_method,
        "api_key": config.api_key,
        "oauth_endpoint": config.oauth_endpoint,
        "oauth_client_id": config.oauth_client_id,
        "oauth_client_secret": config.oauth_client_secret,
        "log_level": config.log_level,
        "environment": config.environment,
    }

    # Set test defaults
    config.ssl_verify = False
    config.ssl_cert_path = "src/aegis/utils/ssl/rbc-ca-bundle.cer"
    config.include_system_messages = False
    config.allowed_roles = ["user", "assistant"]
    config.max_history_length = 10
    config.auth_method = "api_key"
    config.api_key = "test-api-key"
    config.oauth_endpoint = "https://test.example.com/oauth/token"
    config.oauth_client_id = "test-client-id"
    config.oauth_client_secret = "test-client-secret"
    config.log_level = "INFO"
    config.environment = "test"

    yield

    # Restore original values
    for key, value in original_values.items():
        setattr(config, key, value)


@pytest.fixture(autouse=True)
def cleanup_monitor():
    """
    Clean up monitor entries after each test.

    Ensures process monitoring data doesn't leak between tests.
    """
    yield
    clear_monitor_entries()


@pytest.fixture
def sample_conversation():
    """
    Provide a standard conversation for testing.

    Returns:
        Dict with messages list containing user/assistant exchanges.
    """
    return {
        "messages": [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there!"},
            {"role": "user", "content": "How are you?"},
        ]
    }


@pytest.fixture
def mock_oauth_token():
    """
    Mock OAuth token response for testing.

    Returns:
        Dict with OAuth token data structure.
    """
    return {"access_token": "mock_token_123", "token_type": "Bearer", "expires_in": 3600}


@pytest.fixture
def mock_ssl_config():
    """
    Mock SSL configuration for testing.

    Returns:
        Dict with standard SSL config structure.
    """
    return {
        "success": True,
        "verify": False,
        "cert_path": None,
        "status": "Success",
        "error": None,
        "decision_details": "SSL verification: disabled",
    }


@pytest.fixture
def mock_auth_config():
    """
    Mock authentication configuration for testing.

    Returns:
        Dict with standard auth config structure.
    """
    return {
        "success": True,
        "status": "Success",
        "method": "api_key",
        "token": "test-token",
        "header": {"Authorization": "Bearer test-token"},
        "error": None,
        "decision_details": "Authentication method: api_key",
    }


@pytest.fixture
def execution_id():
    """
    Provide a consistent execution ID for testing.

    Returns:
        String UUID for execution tracking.
    """
    return "test-exec-1234-5678-9abc-def012345678"


@pytest_asyncio.fixture
async def async_test_context(execution_id, mock_auth_config, mock_ssl_config):
    """
    Provide a standard async context for testing LLM and database operations.

    Returns:
        Dict with execution_id, auth_config, and ssl_config.
    """
    return {
        "execution_id": execution_id,
        "auth_config": mock_auth_config,
        "ssl_config": mock_ssl_config
    }


@pytest_asyncio.fixture
async def async_mock_oauth_response():
    """
    Mock OAuth token response for async testing.

    Returns:
        Dict with OAuth token data structure.
    """
    return {"access_token": "mock_async_token_123", "token_type": "Bearer", "expires_in": 3600}


@pytest.fixture
def event_loop():
    """
    Create an instance of the default event loop for the test session.
    This ensures all async tests use the same event loop.
    """
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
