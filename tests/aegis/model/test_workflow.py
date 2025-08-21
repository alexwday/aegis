"""
Tests for model workflow module.

Tests the main workflow orchestration including UUID generation,
SSL setup, and conversation processing.
"""

from unittest import mock

import pytest

from aegis.model.workflow.model_workflow import execute_workflow
from aegis.utils.settings import config


# Mock OAuth token for tests
MOCK_OAUTH_TOKEN = {"access_token": "mock_token_123", "token_type": "Bearer", "expires_in": 3600}


@pytest.fixture(autouse=True)
def reset_config():
    """Save and restore config values for test isolation."""
    # Save original values
    original_values = {
        "ssl_verify": config.ssl_verify,
        "ssl_cert_path": config.ssl_cert_path,
        "include_system_messages": config.include_system_messages,
        "allowed_roles": config.allowed_roles[:],  # Copy list
        "max_history_length": config.max_history_length,
        "auth_method": config.auth_method,
        "api_key": config.api_key,
        "oauth_endpoint": config.oauth_endpoint,
        "oauth_client_id": config.oauth_client_id,
        "oauth_client_secret": config.oauth_client_secret,
    }

    # Set test defaults
    config.ssl_verify = False
    config.ssl_cert_path = "src/aegis/utils/ssl/rbc-ca-bundle.cer"
    config.include_system_messages = False
    config.allowed_roles = ["user", "assistant"]
    config.max_history_length = 10
    config.auth_method = "api_key"
    config.api_key = ""
    config.oauth_endpoint = ""
    config.oauth_client_id = ""
    config.oauth_client_secret = ""

    yield

    # Restore original values
    for key, value in original_values.items():
        setattr(config, key, value)


class TestWorkflow:
    """Test cases for workflow execution."""

    @mock.patch("aegis.model.workflow.model_workflow.setup_authentication")
    def test_basic_workflow_execution(self, mock_get_oauth):
        """Test basic workflow execution with valid input."""
        # Mock auth to return empty config (not configured)
        mock_get_oauth.return_value = {"method": None, "token": None, "header": {}}

        conversation_input = {
            "messages": [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi there!"},
                {"role": "user", "content": "How are you?"},
            ]
        }

        result = execute_workflow(conversation_input)

        # Check all expected keys are present
        assert "execution_id" in result
        assert "auth_config" in result
        assert "ssl_config" in result
        assert "processed_conversation" in result

        # Check execution_id format (UUID)
        assert len(result["execution_id"]) == 36
        assert result["execution_id"].count("-") == 4

        # Check SSL config
        assert result["ssl_config"] == {"verify": False, "cert_path": None}

        # Check processed conversation
        assert result["processed_conversation"]["message_count"] == 3
        assert result["processed_conversation"]["latest_message"]["content"] == "How are you?"

    @mock.patch("aegis.model.workflow.model_workflow.setup_authentication")
    def test_workflow_with_list_input(self, mock_get_oauth):
        """Test workflow with list format conversation."""
        mock_get_oauth.return_value = {"method": None, "token": None, "header": {}}
        conversation_input = [{"role": "user", "content": "Test message"}]

        result = execute_workflow(conversation_input)

        assert result["processed_conversation"]["message_count"] == 1
        assert result["processed_conversation"]["latest_message"]["content"] == "Test message"

    @mock.patch("aegis.model.workflow.model_workflow.setup_authentication")
    def test_workflow_with_ssl_enabled(self, mock_get_oauth):
        """Test workflow with SSL verification enabled."""
        mock_get_oauth.return_value = {"method": None, "token": None, "header": {}}
        config.ssl_verify = True
        config.ssl_cert_path = ""  # Use system certs

        conversation_input = {"messages": [{"role": "user", "content": "Test"}]}

        result = execute_workflow(conversation_input)

        # Check SSL is enabled
        assert result["ssl_config"]["verify"] is True
        assert result["ssl_config"]["cert_path"] is None

    @mock.patch("aegis.model.workflow.model_workflow.setup_authentication")
    def test_workflow_filters_system_messages(self, mock_get_oauth):
        """Test that system messages are filtered by default."""
        mock_get_oauth.return_value = {"method": None, "token": None, "header": {}}
        conversation_input = {
            "messages": [
                {"role": "system", "content": "You are helpful"},
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi"},
            ]
        }

        result = execute_workflow(conversation_input)

        # System message should be filtered (default config)
        assert result["processed_conversation"]["message_count"] == 2
        messages = result["processed_conversation"]["messages"]
        assert all(msg["role"] != "system" for msg in messages)

    @mock.patch("aegis.model.workflow.model_workflow.setup_authentication")
    def test_workflow_respects_max_history(self, mock_get_oauth):
        """Test that conversation history is limited."""
        mock_get_oauth.return_value = {"method": None, "token": None, "header": {}}
        config.max_history_length = 2

        # Create conversation with more messages than limit
        messages = []
        for i in range(5):
            messages.append({"role": "user", "content": f"Message {i}"})
            messages.append({"role": "assistant", "content": f"Response {i}"})

        conversation_input = {"messages": messages}

        result = execute_workflow(conversation_input)

        # Should only keep last 2 messages
        assert result["processed_conversation"]["message_count"] == 2
        assert result["processed_conversation"]["messages"][0]["content"] == "Message 4"
        assert result["processed_conversation"]["messages"][1]["content"] == "Response 4"

    @mock.patch("aegis.model.workflow.model_workflow.setup_authentication")
    def test_workflow_invalid_input(self, mock_get_oauth):
        """Test workflow with invalid input."""
        mock_get_oauth.return_value = {"method": None, "token": None, "header": {}}
        # Invalid input type
        with pytest.raises(ValueError, match="Expected dict or list"):
            execute_workflow("invalid input")

        # Empty messages
        with pytest.raises(ValueError, match="Messages list cannot be empty"):
            execute_workflow({"messages": []})

        # Missing required fields
        with pytest.raises(ValueError, match="missing 'content' field"):
            execute_workflow({"messages": [{"role": "user"}]})

    @mock.patch("aegis.model.workflow.model_workflow.setup_authentication")
    @mock.patch("uuid.uuid4")
    def test_workflow_generates_unique_id(self, mock_uuid, mock_get_oauth):
        """Test that each workflow execution gets unique ID."""
        mock_get_oauth.return_value = {"method": None, "token": None, "header": {}}
        mock_uuid.return_value.hex = "12345678901234567890123456789012"
        mock_uuid.return_value.__str__ = lambda x: "test-uuid-1234"

        conversation_input = {"messages": [{"role": "user", "content": "Test"}]}

        result = execute_workflow(conversation_input)

        assert result["execution_id"] == "test-uuid-1234"
        mock_uuid.assert_called_once()


class TestWorkflowLogging:
    """Test cases for workflow logging."""

    @mock.patch("aegis.model.workflow.model_workflow.setup_authentication")
    @mock.patch("aegis.model.workflow.model_workflow.get_logger")
    def test_workflow_logs_all_steps(self, mock_get_logger, mock_get_oauth):
        """Test that workflow logs all major steps."""
        mock_get_oauth.return_value = {"method": None, "token": None, "header": {}}
        mock_logger = mock.Mock()
        mock_get_logger.return_value = mock_logger

        conversation_input = {"messages": [{"role": "user", "content": "Test"}]}

        execute_workflow(conversation_input)

        # Check that workflow start is logged at INFO level
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]

        assert "workflow.started" in info_calls

        # Verify execution_id is logged at workflow start
        workflow_start_call = mock_logger.info.call_args_list[0]
        assert "execution_id" in workflow_start_call[1]

        # We should have no warnings since auth returns empty config (handled gracefully)
        # Auth errors would be logged as errors, not warnings


class TestWorkflowIntegration:
    """Integration tests for complete workflow."""

    @mock.patch("aegis.model.workflow.model_workflow.setup_authentication")
    def test_full_workflow_with_all_features(self, mock_get_oauth):
        """Test complete workflow with all features enabled."""
        mock_get_oauth.return_value = {"method": None, "token": None, "header": {}}
        # Configure all features
        config.ssl_verify = False
        config.include_system_messages = True
        config.allowed_roles = ["user", "assistant", "system"]
        config.max_history_length = 5

        conversation_input = {
            "messages": [
                {"role": "system", "content": "You are a helpful assistant"},
                {"role": "user", "content": "What is 2+2?"},
                {"role": "assistant", "content": "2+2 equals 4"},
                {"role": "user", "content": "Thanks!"},
            ]
        }

        result = execute_workflow(conversation_input)

        # Verify complete result structure
        assert "execution_id" in result
        assert "ssl_config" in result
        assert "processed_conversation" in result

        # All messages should be kept (system messages included)
        assert result["processed_conversation"]["message_count"] == 4

        # Check message types
        messages = result["processed_conversation"]["messages"]
        assert messages[0]["role"] == "system"
        assert messages[-1]["role"] == "user"
        assert messages[-1]["content"] == "Thanks!"

        # Verify execution_id is consistent
        assert result["processed_conversation"]["execution_id"] == result["execution_id"]
