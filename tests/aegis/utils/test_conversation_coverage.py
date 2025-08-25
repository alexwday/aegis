"""
Test to achieve 100% coverage for conversation.py.
"""

import pytest
from unittest.mock import patch, Mock
from aegis.utils.conversation import process_conversation


class TestConversationCoverage:
    """Test edge cases in conversation processing for 100% coverage."""
    
    @patch("aegis.utils.conversation.get_logger")
    def test_empty_messages_after_filtering(self, mock_logger):
        """Test when all messages are filtered out."""
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        # All messages have roles that will be filtered
        conversation = [
            {"role": "system", "content": "System message"}
        ]
        
        with patch("aegis.utils.conversation.config") as mock_config:
            # Configure to filter out system messages
            mock_config.allowed_roles = ["user", "assistant"]  # system not in allowed_roles
            mock_config.include_system_messages = False
            mock_config.max_history_length = 10
            
            result = process_conversation(conversation, "test-exec")
            
            # Should return error when no valid messages
            assert result["success"] is False
            assert result["status"] == "Failure"
            assert "No valid messages after filtering" in result["error"]
    
    @patch("aegis.utils.conversation.get_logger")
    def test_content_preview_not_truncated(self, mock_logger):
        """Test when message content is exactly 50 characters or less."""
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        # Message with content exactly 50 chars
        short_content = "This is a short message under fifty characters."
        conversation = [
            {"role": "user", "content": short_content}
        ]
        
        with patch("aegis.utils.conversation.config") as mock_config:
            mock_config.allowed_roles = ["user", "assistant"]
            mock_config.max_history_length = 10
            mock_config.include_system_messages = False
            
            result = process_conversation(conversation, "test-exec")
            
            # Should succeed
            assert result["success"] is True
            # Check decision_details doesn't have ellipsis
            assert "..." not in result["decision_details"]
            assert short_content in result["decision_details"]
    
    @patch("aegis.utils.conversation.get_logger")
    def test_exception_with_dict_input(self, mock_logger):
        """Test exception handling when input is a dict."""
        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance
        
        # Dict input that will cause an error
        conversation = {
            "messages": [{"invalid": "structure"}]
        }
        
        with patch("aegis.utils.conversation.config") as mock_config:
            mock_config.allowed_roles = ["user", "assistant"]
            
            result = process_conversation(conversation, "test-exec")
            
            # Should handle the exception and return error
            assert result["success"] is False
            assert result["status"] == "Failure"
            assert result["error"] is not None
            
            # Should have original_count for dict input
            assert "original_message_count" in result
            assert result["original_message_count"] == 1