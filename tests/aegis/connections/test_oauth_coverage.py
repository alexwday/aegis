"""
Additional tests to achieve 100% coverage for oauth_connector.py.

Focuses on edge cases not covered by test_oauth_expanded.py.
"""

import pytest
from unittest.mock import Mock, patch
from aegis.connections.oauth_connector import setup_authentication
from aegis.utils.settings import config


class TestOAuthCoverage:
    """
    Test edge cases in OAuth connector for 100% coverage.
    """
    
    def setup_method(self):
        """Set up test fixtures."""
        # Save original config values
        self.orig_auth_method = config.auth_method
        self.orig_oauth_endpoint = config.oauth_endpoint
        self.orig_oauth_client_id = config.oauth_client_id
        self.orig_oauth_client_secret = config.oauth_client_secret
    
    def teardown_method(self):
        """Restore original config values."""
        config.auth_method = self.orig_auth_method
        config.oauth_endpoint = self.orig_oauth_endpoint
        config.oauth_client_id = self.orig_oauth_client_id
        config.oauth_client_secret = self.orig_oauth_client_secret
    
    @patch("aegis.connections.oauth_connector.get_oauth_token")
    @patch("aegis.connections.oauth_connector.get_logger")
    def test_oauth_returns_none_token(self, mock_logger, mock_get_token):
        """Test when get_oauth_token returns None."""
        # Setup
        config.auth_method = "oauth"
        config.oauth_endpoint = "https://test.example.com/oauth"
        config.oauth_client_id = "test_client"
        config.oauth_client_secret = "test_secret"
        
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        # Make get_oauth_token return None
        mock_get_token.return_value = None
        
        # Test
        ssl_config = {"verify": True, "cert_path": None}
        result = setup_authentication(execution_id="test-none", ssl_config=ssl_config)
        
        # Should return placeholder due to None token
        assert result["method"] == "placeholder"
        assert result["token"] == "oauth-failed"
        assert result["header"]["Authorization"] == "Bearer oauth-failed"
        
        # Should log warning
        mock_logger_instance.warning.assert_called_with(
            "Failed to obtain OAuth token - using placeholder",
            execution_id="test-none"
        )
    
    @patch("aegis.connections.oauth_connector.get_oauth_token")
    @patch("aegis.connections.oauth_connector.get_logger")
    def test_oauth_returns_token_without_access_token(self, mock_logger, mock_get_token):
        """Test when get_oauth_token returns a dict without access_token."""
        # Setup
        config.auth_method = "oauth"
        config.oauth_endpoint = "https://test.example.com/oauth"
        config.oauth_client_id = "test_client"
        config.oauth_client_secret = "test_secret"
        
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        # Make get_oauth_token return dict without access_token
        mock_get_token.return_value = {"token_type": "Bearer"}
        
        # Test
        ssl_config = {"verify": True, "cert_path": None}
        result = setup_authentication(execution_id="test-no-access", ssl_config=ssl_config)
        
        # Should return placeholder due to missing access_token
        assert result["method"] == "placeholder"
        assert result["token"] == "oauth-failed"
        assert result["header"]["Authorization"] == "Bearer oauth-failed"
        
        # Should log warning
        mock_logger_instance.warning.assert_called_with(
            "Failed to obtain OAuth token - using placeholder",
            execution_id="test-no-access"
        )
    
    @patch("aegis.connections.oauth_connector._handle_oauth_auth")
    @patch("aegis.connections.oauth_connector.get_logger")
    def test_setup_authentication_unexpected_exception(self, mock_logger, mock_handle_oauth):
        """Test when an unexpected exception occurs during setup."""
        # Setup
        config.auth_method = "oauth"
        
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        # Make _handle_oauth_auth raise an unexpected exception
        mock_handle_oauth.side_effect = RuntimeError("Unexpected error")
        
        # Test
        ssl_config = {"verify": True, "cert_path": None}
        result = setup_authentication(execution_id="test-unexpected", ssl_config=ssl_config)
        
        # Should return failure with error details
        assert result["success"] is False
        assert result["status"] == "Failure"
        assert result["method"] == "oauth"
        assert result["token"] is None
        assert result["header"] == {}
        assert "Unexpected error during authentication" in result["error"]
        
        # Should log error
        mock_logger_instance.error.assert_called()
        error_call = mock_logger_instance.error.call_args
        assert "Unexpected error during authentication" in error_call[0][0]