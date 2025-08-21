"""
Tests for conversation processing module.

Tests the validation and processing of conversation input data.
"""

from unittest import mock

import pytest

from aegis.utils.conversation import process_conversation
from aegis.utils.settings import config


@pytest.fixture(autouse=True)
def reset_config():
    """Save and restore config values for test isolation."""
    # Save original values
    original_values = {
        "include_system_messages": config.include_system_messages,
        "allowed_roles": config.allowed_roles[:],  # Copy list
        "max_history_length": config.max_history_length,
    }

    # Set test defaults
    config.include_system_messages = False
    config.allowed_roles = ["user", "assistant"]
    config.max_history_length = 10

    yield

    # Restore original values
    for key, value in original_values.items():
        setattr(config, key, value)


class TestProcessConversation:
    """Test cases for conversation processing."""

    def test_process_valid_conversation(self):
        """Test processing of valid conversation data."""
        conversation_input = {
            "messages": [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi there!"},
                {"role": "user", "content": "How are you?"},
            ]
        }
        execution_id = "test-123"

        result = process_conversation(conversation_input, execution_id)

        # Verify structure
        assert "messages" in result
        assert "latest_message" in result
        assert "message_count" in result
        assert "execution_id" in result

        # Verify content (system message filtered by default)
        assert len(result["messages"]) == 3  # System message filtered out
        assert result["message_count"] == 3
        assert result["latest_message"]["role"] == "user"
        assert result["latest_message"]["content"] == "How are you?"
        assert result["execution_id"] == "test-123"

    def test_process_single_message(self):
        """Test processing conversation with single message."""
        conversation_input = {"messages": [{"role": "user", "content": "Hello world"}]}
        execution_id = "test-456"

        result = process_conversation(conversation_input, execution_id)

        assert result["message_count"] == 1
        assert result["latest_message"]["content"] == "Hello world"

    def test_process_list_format(self):
        """Test processing conversation provided as a list (auto-wrapped)."""
        conversation_input = [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there!"},
        ]
        execution_id = "test-list"

        result = process_conversation(conversation_input, execution_id)

        assert result["message_count"] == 2
        assert result["messages"][0]["content"] == "Hello"
        assert result["messages"][1]["content"] == "Hi there!"
        assert result["latest_message"]["role"] == "assistant"

    def test_process_strips_extra_fields(self):
        """Test that processing only keeps role and content fields."""
        conversation_input = {
            "messages": [
                {
                    "role": "user",
                    "content": "Test message",
                    "timestamp": "2024-01-01",
                    "user_id": "12345",
                    "extra": "data",
                }
            ]
        }
        execution_id = "test-789"

        result = process_conversation(conversation_input, execution_id)

        # Only role and content should remain
        message = result["messages"][0]
        assert list(message.keys()) == ["role", "content"]
        assert message["role"] == "user"
        assert message["content"] == "Test message"

    def test_filter_system_messages(self):
        """Test filtering out system messages when configured."""
        config.include_system_messages = False

        conversation_input = {
            "messages": [
                {"role": "system", "content": "System prompt"},
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi"},
            ]
        }
        execution_id = "test-filter-system"

        result = process_conversation(conversation_input, execution_id)

        # System message should be filtered out
        assert result["message_count"] == 2
        assert all(msg["role"] != "system" for msg in result["messages"])

    def test_filter_by_allowed_roles(self):
        """Test filtering messages by allowed roles."""
        config.allowed_roles = ["user"]  # Only allow user messages
        config.include_system_messages = False

        conversation_input = {
            "messages": [
                {"role": "user", "content": "Question"},
                {"role": "assistant", "content": "Answer"},
                {"role": "user", "content": "Follow up"},
            ]
        }
        execution_id = "test-filter-roles"

        result = process_conversation(conversation_input, execution_id)

        # Only user messages should remain
        assert result["message_count"] == 2
        assert all(msg["role"] == "user" for msg in result["messages"])

    def test_max_history_length(self):
        """Test limiting conversation history length."""
        config.max_history_length = 3

        conversation_input = {
            "messages": [
                {"role": "user", "content": "Message 1"},
                {"role": "assistant", "content": "Response 1"},
                {"role": "user", "content": "Message 2"},
                {"role": "assistant", "content": "Response 2"},
                {"role": "user", "content": "Message 3"},
            ]
        }
        execution_id = "test-max-history"

        result = process_conversation(conversation_input, execution_id)

        # Should only keep last 3 messages
        assert result["message_count"] == 3
        assert result["messages"][0]["content"] == "Message 2"
        assert result["messages"][-1]["content"] == "Message 3"


class TestValidationErrors:
    """Test cases for validation error handling."""

    @pytest.mark.parametrize(
        "invalid_input,error_match",
        [
            ("not a dict or list", "Expected dict or list"),
            ([], "Messages list cannot be empty"),
            ({"other": "data"}, "Missing required 'messages' field"),
            ({"messages": "not a list"}, "Messages must be a list"),
            ({"messages": []}, "Messages list cannot be empty"),
        ],
    )
    def test_input_validation_errors(self, invalid_input, error_match):
        """Test various input validation errors."""
        with pytest.raises(ValueError, match=error_match):
            process_conversation(invalid_input, "test-id")

    @pytest.mark.parametrize(
        "invalid_message,error_match",
        [
            (["not a dict"], "Message at index 0 must be a dict"),
            ([{"content": "test"}], "Message at index 0 missing 'role' field"),
            ([{"role": "user"}], "Message at index 0 missing 'content' field"),
            ([{"role": "invalid", "content": "test"}], "invalid role 'invalid'"),
            ([{"role": "user", "content": 123}], "content must be a string"),
            ([{"role": "user", "content": "   "}], "content cannot be empty"),
        ],
    )
    def test_message_validation_errors(self, invalid_message, error_match):
        """Test various message validation errors."""
        conversation_input = {"messages": invalid_message}
        with pytest.raises(ValueError, match=error_match):
            process_conversation(conversation_input, "test-id")


class TestLogging:
    """Test cases for logging in conversation processing."""

    @mock.patch("aegis.utils.conversation.conversation_setup.get_logger")
    def test_logs_processing_success(self, mock_get_logger):
        """Test that successful processing is logged."""
        mock_logger = mock.Mock()
        mock_get_logger.return_value = mock_logger

        conversation_input = {"messages": [{"role": "user", "content": "test"}]}
        execution_id = "test-log-123"

        process_conversation(conversation_input, execution_id)

        # Verify debug and info logs were called
        mock_logger.debug.assert_called_once_with("Processing conversation")
        mock_logger.info.assert_called_once_with(
            "Conversation processed", message_count=1, latest_role="user"
        )

    @mock.patch("aegis.utils.conversation.conversation_setup.get_logger")
    def test_logs_processing_error(self, mock_get_logger):
        """Test that processing errors are logged."""
        mock_logger = mock.Mock()
        mock_get_logger.return_value = mock_logger

        conversation_input = {"messages": "invalid"}
        execution_id = "test-error-123"

        with pytest.raises(ValueError):
            process_conversation(conversation_input, execution_id)

        # Verify error was logged
        mock_logger.error.assert_called_once()
        error_call = mock_logger.error.call_args
        assert error_call[0][0] == "Failed to process conversation"
        assert "error" in error_call[1]
