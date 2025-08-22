"""
Tests for OAuth connector module.
"""

from unittest import mock

import pytest
import requests

from aegis.connections import get_oauth_token, setup_authentication
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

    @mock.patch("aegis.connections.oauth_connector.requests.Session")
    def test_get_oauth_token_success(self, mock_session_class):
        """Test successful OAuth token generation."""
        # Setup mock response
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "test_token_123",
            "token_type": "Bearer",
            "expires_in": 3600,
        }
        mock_response.raise_for_status = mock.Mock()

        # Setup mock session
        mock_session = mock.Mock()
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        # Configure OAuth settings
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "test_client_id"
        config.oauth_client_secret = "test_secret"
        config.oauth_grant_type = "client_credentials"

        # SSL config from workflow
        ssl_config = {"verify": False, "cert_path": None}

        # Get token
        result = get_oauth_token("test-execution-id", ssl_config)

        # Verify result
        assert result["access_token"] == "test_token_123"
        assert result["token_type"] == "Bearer"
        assert result["expires_in"] == 3600

        # Verify request was made with Basic Auth
        mock_session.post.assert_called_once()
        call_args = mock_session.post.call_args
        assert call_args.kwargs["auth"] == ("test_client_id", "test_secret")
        assert call_args.kwargs["data"] == {"grant_type": "client_credentials"}

    def test_get_oauth_token_not_configured(self):
        """Test OAuth token returns None when not configured."""
        # Clear OAuth configuration
        config.oauth_endpoint = ""
        config.oauth_client_id = ""
        config.oauth_client_secret = ""

        # Should return None instead of raising error
        ssl_config = {"verify": False, "cert_path": None}
        result = get_oauth_token("test-execution-id", ssl_config)
        assert result is None

    @mock.patch("aegis.connections.oauth_connector.requests.Session")
    def test_get_oauth_token_http_error(self, mock_session_class):
        """Test OAuth token generation handles HTTP errors."""
        # Setup mock error response
        mock_response = mock.Mock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "401 Unauthorized"
        )

        # Setup mock session
        mock_session = mock.Mock()
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        # Configure OAuth settings
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "test_client_id"
        config.oauth_client_secret = "test_secret"

        # Should raise HTTPError
        ssl_config = {"verify": False, "cert_path": None}
        with pytest.raises(requests.exceptions.HTTPError):
            get_oauth_token("test-execution-id", ssl_config)

    @mock.patch("aegis.connections.oauth_connector.requests.Session")
    def test_get_oauth_token_connection_error(self, mock_session_class):
        """Test OAuth token generation handles connection errors."""
        # Setup mock session with connection error
        mock_session = mock.Mock()
        mock_session.post.side_effect = requests.exceptions.ConnectionError("Failed to connect")
        mock_session_class.return_value = mock_session

        # Configure OAuth settings
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "test_client_id"
        config.oauth_client_secret = "test_secret"

        # Should raise ConnectionError
        ssl_config = {"verify": False, "cert_path": None}
        with pytest.raises(requests.exceptions.ConnectionError):
            get_oauth_token("test-execution-id", ssl_config)

    @mock.patch("aegis.connections.oauth_connector.requests.Session")
    def test_get_oauth_token_timeout(self, mock_session_class):
        """Test OAuth token generation handles timeouts."""
        # Setup mock session with timeout
        mock_session = mock.Mock()
        mock_session.post.side_effect = requests.exceptions.Timeout("Request timed out")
        mock_session_class.return_value = mock_session

        # Configure OAuth settings
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "test_client_id"
        config.oauth_client_secret = "test_secret"

        # Should raise Timeout
        ssl_config = {"verify": False, "cert_path": None}
        with pytest.raises(requests.exceptions.Timeout):
            get_oauth_token("test-execution-id", ssl_config)

    @mock.patch("aegis.connections.oauth_connector.requests.Session")
    def test_get_oauth_token_invalid_response(self, mock_session_class):
        """Test OAuth token generation handles invalid response."""
        # Setup mock response without access_token
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"error": "invalid_grant"}
        mock_response.raise_for_status = mock.Mock()

        # Setup mock session
        mock_session = mock.Mock()
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        # Configure OAuth settings
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "test_client_id"
        config.oauth_client_secret = "test_secret"

        # Should raise ValueError
        ssl_config = {"verify": False, "cert_path": None}
        with pytest.raises(ValueError, match="missing 'access_token'"):
            get_oauth_token("test-execution-id", ssl_config)

    @mock.patch("aegis.connections.oauth_connector.requests.Session")
    def test_get_oauth_token_json_decode_error(self, mock_session_class):
        """Test OAuth token generation handles JSON decode errors."""
        # Setup mock response with invalid JSON
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_response.text = "<html>Error page</html>"
        mock_response.raise_for_status = mock.Mock()

        # Setup mock session
        mock_session = mock.Mock()
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        # Configure OAuth settings
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "test_client_id"
        config.oauth_client_secret = "test_secret"

        # Should raise original ValueError
        ssl_config = {"verify": False, "cert_path": None}
        with pytest.raises(ValueError, match="Invalid JSON"):
            get_oauth_token("test-execution-id", ssl_config)

    @mock.patch("aegis.connections.oauth_connector.requests.Session")
    def test_get_oauth_token_connection_error_duplicate(self, mock_session_class):
        """Test OAuth token generation handles connection errors."""
        # Setup mock session to raise ConnectionError
        mock_session = mock.Mock()
        mock_session.post.side_effect = requests.exceptions.ConnectionError("Connection failed")
        mock_session_class.return_value = mock_session

        # Configure OAuth settings
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "test_client_id"
        config.oauth_client_secret = "test_secret"

        # Should raise ConnectionError
        ssl_config = {"verify": False, "cert_path": None}
        with pytest.raises(requests.exceptions.ConnectionError):
            get_oauth_token("test-execution-id", ssl_config)


class TestAuthenticationSetup:
    """Test cases for authentication setup."""

    @mock.patch("aegis.connections.oauth_connector.get_oauth_token")
    def test_setup_authentication_with_oauth(self, mock_get_oauth):
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
        result = setup_authentication("test-id", ssl_config)

        assert result["method"] == "oauth"
        assert result["token"] == "test_oauth_token"
        assert result["header"] == {"Authorization": "Bearer test_oauth_token"}

    def test_setup_authentication_with_api_key(self):
        """Test authentication setup with API key method."""
        # Setup config
        config.auth_method = "api_key"
        config.api_key = "test_api_key_123"

        ssl_config = {"verify": False, "cert_path": None}
        result = setup_authentication("test-id", ssl_config)

        assert result["method"] == "api_key"
        assert result["token"] == "test_api_key_123"
        assert result["header"] == {"Authorization": "Bearer test_api_key_123"}

    def test_setup_authentication_oauth_missing_config(self):
        """Test OAuth returns placeholder when credentials not configured."""
        config.auth_method = "oauth"
        config.oauth_endpoint = ""
        config.oauth_client_id = ""
        config.oauth_client_secret = ""

        ssl_config = {"verify": False, "cert_path": None}
        result = setup_authentication("test-id", ssl_config)

        # Should return placeholder config, not raise error
        assert result["method"] == "placeholder"
        assert result["token"] == "no-oauth-configured"
        assert result["header"] == {"Authorization": "Bearer no-oauth-configured"}

    def test_setup_authentication_api_key_missing(self):
        """Test API key returns error when not configured."""
        config.auth_method = "api_key"
        config.api_key = ""

        ssl_config = {"verify": False, "cert_path": None}
        result = setup_authentication("test-id", ssl_config)

        # Should return error status with api_key method
        assert result["success"] is False
        assert result["method"] == "api_key"
        assert result["token"] is None
        assert result["header"] == {}
        assert result["error"] == "API_KEY not configured"

    def test_setup_authentication_invalid_method(self):
        """Test invalid auth method returns error."""
        config.auth_method = "invalid"

        ssl_config = {"verify": False, "cert_path": None}
        result = setup_authentication("test-id", ssl_config)

        # Should return error status with invalid method
        assert result["success"] is False
        assert result["method"] == "invalid"
        assert result["token"] is None
        assert result["header"] == {}
        assert "Invalid AUTH_METHOD" in result["error"]


class TestRetryLogic:
    """Test cases for retry logic configuration."""

    @mock.patch("aegis.connections.oauth_connector.requests.Session")
    @mock.patch("aegis.connections.oauth_connector.HTTPAdapter")
    @mock.patch("aegis.connections.oauth_connector.Retry")
    def test_session_retry_configuration(
        self, mock_retry_class, mock_adapter_class, mock_session_class
    ):
        """Test session is configured with proper retry settings."""
        from aegis.connections.oauth_connector import _create_session_with_retry

        # Setup mock session
        mock_session = mock.Mock()
        mock_session_class.return_value = mock_session

        # Configure retry settings
        config.oauth_max_retries = 3
        config.oauth_retry_delay = 1

        # Create session
        _ = _create_session_with_retry()

        # Verify retry configuration
        mock_retry_class.assert_called_once_with(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["POST"],
            raise_on_status=False,
        )

        # Verify adapter was mounted
        assert mock_session.mount.call_count == 2  # http:// and https://


class TestIntegration:
    """Integration tests for OAuth connector."""

    @mock.patch("aegis.connections.oauth_connector.requests.Session")
    def test_oauth_with_ssl_enabled(self, mock_session_class):
        """Test OAuth uses SSL configuration when enabled."""
        # Setup SSL config
        ssl_config = {"verify": True, "cert_path": "/path/to/cert.pem"}

        # Setup mock response
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "token"}
        mock_response.raise_for_status = mock.Mock()

        # Setup mock session
        mock_session = mock.Mock()
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        # Configure OAuth
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "client"
        config.oauth_client_secret = "secret"

        # Get token with SSL config
        get_oauth_token("test-id", ssl_config)

        # Verify SSL cert was used
        call_args = mock_session.post.call_args
        assert call_args.kwargs["verify"] == "/path/to/cert.pem"

    @mock.patch("aegis.connections.oauth_connector.requests.Session")
    def test_oauth_with_ssl_disabled(self, mock_session_class):
        """Test OAuth with SSL verification disabled."""
        # Setup SSL config
        ssl_config = {"verify": False, "cert_path": None}

        # Setup mock response
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"access_token": "token"}
        mock_response.raise_for_status = mock.Mock()

        # Setup mock session
        mock_session = mock.Mock()
        mock_session.post.return_value = mock_response
        mock_session_class.return_value = mock_session

        # Configure OAuth
        config.oauth_endpoint = "https://test.com/oauth/token"
        config.oauth_client_id = "client"
        config.oauth_client_secret = "secret"

        # Get token with SSL config
        get_oauth_token("test-id", ssl_config)

        # Verify SSL was disabled
        call_args = mock_session.post.call_args
        assert call_args.kwargs["verify"] is False
