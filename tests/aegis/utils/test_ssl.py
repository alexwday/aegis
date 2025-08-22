"""
Tests for SSL setup module.

Tests the SSL certificate configuration and verification settings.
"""

import os
import tempfile
from unittest import mock

from aegis.utils.ssl import setup_ssl
from aegis.utils.settings import config


class TestSSLSetup:
    """Test cases for SSL setup."""

    def setup_method(self):
        """Reset config to defaults before each test."""
        config.ssl_verify = False
        config.ssl_cert_path = "src/aegis/utils/ssl/rbc-ca-bundle.cer"

    def test_ssl_disabled(self):
        """Test SSL setup when verification is disabled."""
        config.ssl_verify = False

        result = setup_ssl()

        assert result["success"] is True
        assert result["verify"] is False
        assert result["cert_path"] is None
        assert result["status"] == "Success"
        assert result["error"] is None

    def test_ssl_enabled_with_cert(self):
        """Test SSL setup with custom certificate."""
        config.ssl_verify = True

        # Create a temporary cert file
        with tempfile.NamedTemporaryFile(suffix=".cer", delete=False) as tmp:
            tmp.write(b"test certificate content")
            cert_path = tmp.name

        try:
            config.ssl_cert_path = cert_path

            result = setup_ssl()

            assert result["success"] is True
            assert result["verify"] is True
            assert result["cert_path"] == cert_path
            assert result["status"] == "Success"
            assert result["error"] is None
        finally:
            # Clean up
            os.unlink(cert_path)

    def test_ssl_enabled_without_cert(self):
        """Test SSL setup without custom certificate (use system certs)."""
        config.ssl_verify = True
        config.ssl_cert_path = ""

        result = setup_ssl()

        assert result["success"] is True
        assert result["verify"] is True
        assert result["cert_path"] is None
        assert result["status"] == "enabled"
        assert result["error"] is None

    def test_ssl_enabled_cert_not_found(self):
        """Test SSL setup with missing certificate file."""
        config.ssl_verify = True
        config.ssl_cert_path = "/nonexistent/path/to/cert.cer"

        result = setup_ssl()

        assert result["success"] is False
        assert result["verify"] is False
        assert result["cert_path"] is None
        assert result["status"] == "Failure"
        assert "SSL certificate file not found" in result["error"]

    def test_ssl_path_expansion(self):
        """Test that user home path is expanded."""
        config.ssl_verify = True

        # Create temp file in temp directory
        with tempfile.NamedTemporaryFile(suffix=".cer", delete=False) as tmp:
            tmp.write(b"test certificate")
            cert_path = tmp.name

        try:
            # Use ~ path notation
            config.ssl_cert_path = f"~/{os.path.basename(cert_path)}"

            # Mock expanduser to return our temp file
            with mock.patch("os.path.expanduser", return_value=cert_path):
                result = setup_ssl()
                assert result["success"] is True
                assert result["verify"] is True
                assert result["cert_path"] == cert_path
                assert result["status"] == "Success"
                assert result["error"] is None
        finally:
            os.unlink(cert_path)

    def test_ssl_setup_with_exception(self):
        """Test SSL setup handles unexpected exceptions."""
        config.ssl_verify = True
        config.ssl_cert_path = "test.cer"

        # Mock os.path.exists to raise an exception
        with mock.patch("os.path.exists", side_effect=Exception("Unexpected error")):
            result = setup_ssl()

            # Should handle exception gracefully
            assert result["success"] is False
            assert result["verify"] is False
            assert result["cert_path"] is None
            assert result["status"] == "Failure"
            assert "Unexpected error" in result["error"]


class TestLogging:
    """Test cases for SSL logging."""

    @mock.patch("aegis.utils.ssl.get_logger")
    def test_logs_ssl_disabled(self, mock_get_logger):
        """Test that SSL disabled state is logged."""
        mock_logger = mock.Mock()
        mock_get_logger.return_value = mock_logger

        config.ssl_verify = False

        setup_ssl()

        mock_logger.debug.assert_called_once_with("SSL verification disabled")

    @mock.patch("aegis.utils.ssl.get_logger")
    def test_logs_ssl_with_cert(self, mock_get_logger):
        """Test that custom certificate usage is logged."""
        mock_logger = mock.Mock()
        mock_get_logger.return_value = mock_logger

        config.ssl_verify = True

        with tempfile.NamedTemporaryFile(suffix=".cer", delete=False) as tmp:
            tmp.write(b"test")
            cert_path = tmp.name

        try:
            config.ssl_cert_path = cert_path

            setup_ssl()

            mock_logger.info.assert_called_once_with(
                "SSL verification enabled with certificate", cert_path=cert_path
            )
        finally:
            os.unlink(cert_path)
