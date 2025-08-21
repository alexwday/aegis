"""
Tests for logging setup module.

Tests the logging configuration and custom renderer functionality.
"""

import logging
import sys
from io import StringIO
from unittest import mock

import pytest
import structlog

from aegis.utils.logging import setup_logging, get_logger, custom_renderer


class TestLoggingSetup:
    """Test cases for logging configuration."""

    def test_setup_logging_default_level(self):
        """Test that setup_logging uses INFO as default level."""
        # Reset logging configuration
        logging.getLogger().handlers = []
        
        with mock.patch("aegis.utils.settings.config.log_level", "INFO"):
            setup_logging()
            
            # Check that root logger is set to INFO
            assert logging.getLogger().level == logging.INFO

    def test_setup_logging_custom_level(self):
        """Test that setup_logging accepts custom log level."""
        # Reset logging configuration
        logging.getLogger().handlers = []
        
        setup_logging("DEBUG")
        
        # Check that root logger is set to DEBUG
        assert logging.getLogger().level == logging.DEBUG

    def test_setup_logging_from_env(self):
        """Test that setup_logging reads from environment config."""
        # Reset logging configuration
        logging.getLogger().handlers = []
        
        with mock.patch("aegis.utils.settings.config.log_level", "WARNING"):
            setup_logging()
            
            # Check that root logger is set to WARNING
            assert logging.getLogger().level == logging.WARNING

    def test_get_logger_returns_structlog_instance(self):
        """Test that get_logger returns a structlog BoundLogger."""
        setup_logging()  # Ensure logging is configured
        logger = get_logger()
        # Check it's a structlog logger (BoundLoggerLazyProxy is also valid)
        assert hasattr(logger, "info")
        assert hasattr(logger, "debug")
        assert hasattr(logger, "error")


class TestCustomRenderer:
    """Test cases for custom log renderer."""

    def test_custom_renderer_basic_formatting(self):
        """Test custom renderer formats messages correctly with various log levels."""
        # Test INFO level with context
        event_dict = {
            "timestamp": "2024-01-01 12:00:00",
            "level": "info",
            "event": "test message",
            "key1": "value1"
        }
        
        output = custom_renderer(None, None, event_dict)
        
        # Check basic formatting works
        assert "2024-01-01 12:00:00" in output
        assert "INFO" in output
        assert "test message" in output
        assert "key1=" in output
        assert "✓" in output  # INFO icon
        
        # Test ERROR level
        error_dict = {
            "timestamp": "2024-01-01 12:00:00",
            "level": "error",
            "event": "error occurred",
        }
        
        error_output = custom_renderer(None, None, error_dict)
        assert "ERROR" in error_output
        assert "✗" in error_output


class TestIntegration:
    """Integration tests for logging system."""

    def test_logging_workflow(self):
        """Test complete logging workflow with execution_id."""
        # Capture stdout
        captured_output = StringIO()
        
        with mock.patch("sys.stdout", captured_output):
            setup_logging("INFO")
            logger = get_logger()
            
            # Simulate workflow logging
            execution_id = "test-exec-123"
            logger.info("workflow.started", execution_id=execution_id)
            
            output = captured_output.getvalue()
            
            # Verify output contains expected elements
            assert "workflow.started" in output
            assert "test-exec-123" in output
            assert "INFO" in output or "✓" in output