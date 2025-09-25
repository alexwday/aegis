"""
Additional tests for OAuth connector to achieve 90%+ coverage.
"""

from unittest import mock
import pytest
import pytest_asyncio
import httpx
from aegis.connections.oauth_connector import get_oauth_token, setup_authentication
from aegis.utils.settings import config


@pytest.fixture(autouse=True)
def reset_config():
    """Reset config to defaults before each test."""
    original_values = {
        "auth_method": config.auth_method,
        "api_key": config.api_key,
        "oauth_endpoint": config.oauth_endpoint,
        "oauth_client_id": config.oauth_client_id,
        "oauth_client_secret": config.oauth_client_secret,
        "oauth_grant_type": config.oauth_grant_type,
        "oauth_max_retries": config.oauth_max_retries,
        "oauth_retry_delay": config.oauth_retry_delay,
    }
    yield
    for key, value in original_values.items():
        setattr(config, key, value)


class TestOAuthErrorHandling:
    """Test error handling in OAuth connector."""
    
    @pytest.mark.asyncio
    @mock.patch("aegis.connections.oauth_connector.httpx.AsyncClient")
    async def test_get_oauth_token_request_exception(self, mock_client_class):
        """Test handling of request exceptions during token generation."""
        # Setup
        config.oauth_endpoint = "https://test.com/oauth"
        config.oauth_client_id = "client"
        config.oauth_client_secret = "secret"
        config.oauth_grant_type = "client_credentials"
        config.oauth_max_retries = 1
        config.oauth_retry_delay = 0

        # Mock async client to raise RequestError
        mock_client = mock.AsyncMock()
        mock_client.post.side_effect = httpx.RequestError("Network error")
        mock_client_class.return_value.__aenter__.return_value = mock_client

        # Test - should raise the exception after retries
        ssl_config = {"verify": True, "cert_path": None}
        with pytest.raises(httpx.RequestError):
            await get_oauth_token(execution_id="test-123", ssl_config=ssl_config)

        # Verify retry was attempted
        assert mock_client.post.call_count == 1
    

    @mock.patch("aegis.connections.oauth_connector.httpx.AsyncClient")
    @mock.patch("aegis.connections.oauth_connector.get_logger")
    @pytest.mark.asyncio
    async def test_setup_authentication_oauth_value_error(self, mock_logger, mock_client_class):
        """Test handling of ValueError during OAuth authentication."""
        # Setup
        config.auth_method = "oauth"
        config.test_mode = False
        config.oauth_endpoint = "https://test.com/oauth"
        config.oauth_client_id = "client"
        config.oauth_client_secret = "secret"
        config.oauth_max_retries = 1
        mock_logger_instance = mock.Mock()
        mock_logger.return_value = mock_logger_instance

        # Mock async client to return invalid JSON (will cause ValueError)
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_response.raise_for_status = mock.Mock()
        mock_client = mock.AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client

        # Test
        ssl_config = {"verify": True, "cert_path": None}
        result = await setup_authentication(execution_id="test-789", ssl_config=ssl_config)

        # Should return error status
        assert result["status"] == "Failure"
        assert "OAuth authentication error" in result["error"]

        # Should log the error
        mock_logger_instance.error.assert_called()
    
    @mock.patch("aegis.connections.oauth_connector.httpx.AsyncClient")
    @mock.patch("aegis.connections.oauth_connector.get_logger")
    @pytest.mark.asyncio
    async def test_setup_authentication_oauth_request_exception(self, mock_logger, mock_client_class):
        """Test handling of RequestException during OAuth authentication."""
        # Setup
        config.auth_method = "oauth"
        config.test_mode = False
        config.oauth_endpoint = "https://test.com/oauth"
        config.oauth_client_id = "client"
        config.oauth_client_secret = "secret"
        config.oauth_max_retries = 1
        config.oauth_retry_delay = 0
        mock_logger_instance = mock.Mock()
        mock_logger.return_value = mock_logger_instance

        # Mock async client to raise RequestError
        mock_client = mock.AsyncMock()
        mock_client.post.side_effect = httpx.RequestError("Connection failed")
        mock_client_class.return_value.__aenter__.return_value = mock_client

        # Test
        ssl_config = {"verify": True, "cert_path": None}
        result = await setup_authentication(execution_id="test-abc", ssl_config=ssl_config)

        # Should return error status
        assert result["status"] == "Failure"
        assert "OAuth authentication error" in result["error"]
        assert "Connection failed" in result["error"]

        # Should log the error
        mock_logger_instance.error.assert_called()
    
    @mock.patch("aegis.connections.oauth_connector.get_oauth_token")
    @mock.patch("aegis.connections.oauth_connector.get_logger")
    @pytest.mark.asyncio
    async def test_setup_authentication_oauth_test_mode_with_error(self, mock_logger, mock_get_token):
        """Test OAuth in test mode when token generation fails."""
        # Setup
        config.auth_method = "oauth"
        config.test_mode = True
        mock_logger_instance = mock.Mock()
        mock_logger.return_value = mock_logger_instance
        
        # Make get_oauth_token fail
        mock_get_token.side_effect = Exception("Token generation failed")
        
        # Test
        ssl_config = {"verify": True, "cert_path": None}
        result = await setup_authentication(execution_id="test-def", ssl_config=ssl_config)
        
        # Should return placeholder token despite error
        assert result["method"] == "placeholder"
        assert result["token"] == "oauth-failed"
        assert result["header"]["Authorization"] == "Bearer oauth-failed"
        
        # Should log warning
        mock_logger_instance.warning.assert_called_with(
            "Failed to obtain OAuth token - using placeholder",
            execution_id="test-def",
            error="Token generation failed"
        )