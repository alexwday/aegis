"""
Tests for OAuth connector module.
"""

from unittest import mock
from unittest.mock import AsyncMock, Mock, patch

import pytest
import httpx

from aegis.connections.oauth_connector import get_oauth_token, setup_authentication
from aegis.utils.settings import config


@pytest.fixture(autouse=True)
def reset_config():
    """Reset config to defaults before each test to ensure test isolation."""
    # Save original values
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

    # Let test run
    yield

    # Restore original values
    for key, value in original_values.items():
        setattr(config, key, value)


class TestOAuthToken:
    """Test cases for OAuth token generation."""

    @pytest.mark.asyncio
    @mock.patch("aegis.connections.oauth_connector.httpx.AsyncClient")
    async def test_get_oauth_token_success(self, mock_client_class):
        """Test successful OAuth token generation."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "test_token_123",
            "token_type": "Bearer",
            "expires_in": 3600,
        }
        mock_response.raise_for_status = Mock()

        # Setup mock client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client

        # Configure OAuth settings
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "test_client_id"
        config.oauth_client_secret = "test_secret"
        config.oauth_grant_type = "client_credentials"

        # SSL config from workflow
        ssl_config = {"verify": False, "cert_path": None}

        # Get token
        result = await get_oauth_token("test-execution-id", ssl_config)

        # Verify result
        assert result["access_token"] == "test_token_123"
        assert result["token_type"] == "Bearer"
        assert result["expires_in"] == 3600

        # Verify request was made with Basic Auth
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert call_args.kwargs["auth"] == ("test_client_id", "test_secret")
        assert call_args.kwargs["data"] == {"grant_type": "client_credentials"}

    @pytest.mark.asyncio
    async def test_get_oauth_token_not_configured(self):
        """Test OAuth token returns None when not configured."""
        # Clear OAuth configuration
        config.oauth_endpoint = ""
        config.oauth_client_id = ""
        config.oauth_client_secret = ""

        # Should return None instead of raising error
        ssl_config = {"verify": False, "cert_path": None}
        result = await get_oauth_token("test-execution-id", ssl_config)
        assert result is None

    @pytest.mark.asyncio
    @mock.patch("aegis.connections.oauth_connector.httpx.AsyncClient")
    async def test_get_oauth_token_http_error(self, mock_client_class):
        """Test OAuth token generation handles HTTP errors."""
        # Setup mock error response
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "401 Unauthorized", request=Mock(), response=mock_response
        )

        # Setup mock client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client

        # Configure OAuth settings
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "test_client_id"
        config.oauth_client_secret = "test_secret"

        # Should raise HTTPError
        ssl_config = {"verify": False, "cert_path": None}
        with pytest.raises(httpx.HTTPStatusError):
            await get_oauth_token("test-execution-id", ssl_config)

    @mock.patch("aegis.connections.oauth_connector.httpx.AsyncClient")
    @pytest.mark.asyncio

    async def test_get_oauth_token_connection_error(self, mock_client_class):
        """Test OAuth token generation handles connection errors."""
        # Setup mock session with connection error
        mock_client = AsyncMock()
        mock_client.post.side_effect = httpx.ConnectError("Failed to connect")
        mock_client_class.return_value.__aenter__.return_value = mock_client

        # Configure OAuth settings
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "test_client_id"
        config.oauth_client_secret = "test_secret"

        # Should raise ConnectionError
        ssl_config = {"verify": False, "cert_path": None}
        with pytest.raises(httpx.ConnectError):
            await get_oauth_token("test-execution-id", ssl_config)

    @mock.patch("aegis.connections.oauth_connector.httpx.AsyncClient")
    @pytest.mark.asyncio

    async def test_get_oauth_token_timeout(self, mock_client_class):
        """Test OAuth token generation handles timeouts."""
        # Setup mock session with timeout
        mock_client = AsyncMock()
        mock_client.post.side_effect = httpx.TimeoutException("Request timed out")
        mock_client_class.return_value.__aenter__.return_value = mock_client

        # Configure OAuth settings
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "test_client_id"
        config.oauth_client_secret = "test_secret"

        # Should raise Timeout
        ssl_config = {"verify": False, "cert_path": None}
        with pytest.raises(httpx.TimeoutException):
            await get_oauth_token("test-execution-id", ssl_config)

    @mock.patch("aegis.connections.oauth_connector.httpx.AsyncClient")
    @pytest.mark.asyncio

    async def test_get_oauth_token_invalid_response(self, mock_client_class):
        """Test OAuth token generation handles invalid response."""
        # Setup mock response without access_token
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"error": "invalid_grant"}
        mock_response.raise_for_status = Mock()

        # Setup mock client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client

        # Configure OAuth settings
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "test_client_id"
        config.oauth_client_secret = "test_secret"

        # Should raise ValueError
        ssl_config = {"verify": False, "cert_path": None}
        with pytest.raises(ValueError, match="missing 'access_token'"):
            await get_oauth_token("test-execution-id", ssl_config)

    @mock.patch("aegis.connections.oauth_connector.httpx.AsyncClient")
    @pytest.mark.asyncio

    async def test_get_oauth_token_json_decode_error(self, mock_client_class):
        """Test OAuth token generation handles JSON decode errors."""
        # Setup mock response with invalid JSON
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_response.text = "<html>Error page</html>"
        mock_response.raise_for_status = Mock()

        # Setup mock client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client

        # Configure OAuth settings
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "test_client_id"
        config.oauth_client_secret = "test_secret"

        # Should raise original ValueError
        ssl_config = {"verify": False, "cert_path": None}
        with pytest.raises(ValueError, match="Invalid JSON"):
            await get_oauth_token("test-execution-id", ssl_config)

    @mock.patch("aegis.connections.oauth_connector.httpx.AsyncClient")
    @pytest.mark.asyncio

    async def test_get_oauth_token_connection_error_duplicate(self, mock_client_class):
        """Test OAuth token generation handles connection errors."""
        # Setup mock client to raise ConnectError
        mock_client = AsyncMock()
        mock_client.post.side_effect = httpx.ConnectError("Connection failed")
        mock_client_class.return_value.__aenter__.return_value = mock_client

        # Configure OAuth settings
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "test_client_id"
        config.oauth_client_secret = "test_secret"

        # Should raise ConnectionError
        ssl_config = {"verify": False, "cert_path": None}
        with pytest.raises(httpx.ConnectError):
            await get_oauth_token("test-execution-id", ssl_config)


class TestAuthenticationSetup:
    """Test cases for authentication setup."""

    @pytest.mark.asyncio
    @mock.patch("aegis.connections.oauth_connector.get_oauth_token")
    async def test_setup_authentication_with_oauth(self, mock_get_oauth):
        """Test authentication setup with OAuth method."""
        # Setup config
        config.auth_method = "oauth"
        config.oauth_endpoint = "https://test.com/oauth"
        config.oauth_client_id = "client"
        config.oauth_client_secret = "secret"

        # Mock OAuth token response
        mock_get_oauth.return_value = {
            "access_token": "test_oauth_token",
            "token_type": "Bearer",
            "expires_in": 3600,
        }

        ssl_config = {"verify": False, "cert_path": None}
        result = await setup_authentication("test-id", ssl_config)

        assert result["method"] == "oauth"
        assert result["token"] == "test_oauth_token"
        assert result["header"] == {"Authorization": "Bearer test_oauth_token"}

    @pytest.mark.asyncio
    async def test_setup_authentication_with_api_key(self):
        """Test authentication setup with API key method."""
        # Setup config
        config.auth_method = "api_key"
        config.api_key = "test_api_key_123"

        ssl_config = {"verify": False, "cert_path": None}
        result = await setup_authentication("test-id", ssl_config)

        assert result["method"] == "api_key"
        assert result["token"] == "test_api_key_123"
        assert result["header"] == {"Authorization": "Bearer test_api_key_123"}

    @pytest.mark.asyncio
    async def test_setup_authentication_oauth_missing_config(self):
        """Test OAuth returns placeholder when credentials not configured."""
        config.auth_method = "oauth"
        config.oauth_endpoint = ""
        config.oauth_client_id = ""
        config.oauth_client_secret = ""

        ssl_config = {"verify": False, "cert_path": None}
        result = await setup_authentication("test-id", ssl_config)

        # Should return placeholder config, not raise error
        assert result["method"] == "placeholder"
        assert result["token"] == "no-oauth-configured"
        assert result["header"] == {"Authorization": "Bearer no-oauth-configured"}

    @pytest.mark.asyncio
    async def test_setup_authentication_api_key_missing(self):
        """Test API key returns error when not configured."""
        config.auth_method = "api_key"
        config.api_key = ""

        ssl_config = {"verify": False, "cert_path": None}
        result = await setup_authentication("test-id", ssl_config)

        # Should return error status with api_key method
        assert result["success"] is False
        assert result["method"] == "api_key"
        assert result["token"] is None
        assert result["header"] == {}
        assert result["error"] == "API_KEY not configured"

    @pytest.mark.asyncio
    async def test_setup_authentication_invalid_method(self):
        """Test invalid auth method returns error."""
        config.auth_method = "invalid"

        ssl_config = {"verify": False, "cert_path": None}
        result = await setup_authentication("test-id", ssl_config)

        # Should return error status with invalid method
        assert result["success"] is False
        assert result["method"] == "invalid"
        assert result["token"] is None
        assert result["header"] == {}
        assert "Invalid AUTH_METHOD" in result["error"]


# TestRetryLogic removed - not applicable to async httpx implementation

class TestIntegration:
    """Integration tests for OAuth connector."""

    @mock.patch("aegis.connections.oauth_connector.httpx.AsyncClient")
    @pytest.mark.asyncio

    async def test_oauth_with_ssl_enabled(self, mock_client_class):
        """Test OAuth uses SSL configuration when enabled."""
        # Setup SSL config
        ssl_config = {"verify": True, "cert_path": "/path/to/cert.pem"}

        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "token"}
        mock_response.raise_for_status = Mock()

        # Setup mock client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client

        # Configure OAuth
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "client"
        config.oauth_client_secret = "secret"

        # Get token with SSL config
        await get_oauth_token("test-id", ssl_config)

        # Verify SSL cert was used in client constructor
        mock_client_class.assert_called_once()
        call_args = mock_client_class.call_args
        assert call_args.kwargs["verify"] == "/path/to/cert.pem"

    @mock.patch("aegis.connections.oauth_connector.httpx.AsyncClient")
    @pytest.mark.asyncio

    async def test_oauth_with_ssl_disabled(self, mock_client_class):
        """Test OAuth with SSL verification disabled."""
        # Setup SSL config
        ssl_config = {"verify": False, "cert_path": None}

        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "token"}
        mock_response.raise_for_status = Mock()

        # Setup mock client
        mock_client = AsyncMock()
        mock_client.post.return_value = mock_response
        mock_client_class.return_value.__aenter__.return_value = mock_client

        # Configure OAuth
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "client"
        config.oauth_client_secret = "secret"

        # Get token with SSL config
        await get_oauth_token("test-id", ssl_config)

        # Verify SSL was disabled in client constructor
        mock_client_class.assert_called_once()
        call_args = mock_client_class.call_args
        assert call_args.kwargs["verify"] is False
