"""
Tests for model streaming functionality.

Tests the main model() generator function with various inputs
and verifies the streaming output format.
"""

from unittest import mock

from aegis.model.main import model


class TestModelStreaming:
    """Test cases for the model streaming generator."""

    @mock.patch("aegis.model.main.setup_authentication")
    def test_basic_streaming(self, mock_auth):
        """Test basic streaming with dictionary input."""
        mock_auth.return_value = {
            "success": True,
            "status": "Success",
            "method": "api_key",
            "token": "test-token",
            "header": {"Authorization": "Bearer test-token"},
            "error": None,
        }

        conversation = {"messages": [{"role": "user", "content": "What is Q3 revenue?"}]}

        messages = list(model(conversation))

        # Should have generated at least one message
        assert len(messages) >= 1

        # Check message structure
        for msg in messages:
            assert "type" in msg
            assert "name" in msg
            assert "content" in msg
            assert msg["type"] in ["agent", "subagent"]

        # Currently just returns placeholder message
        agent_messages = [m for m in messages if m["type"] == "agent"]
        assert len(agent_messages) >= 1

        # Verify agent name is always "aegis"
        for msg in agent_messages:
            assert msg["name"] == "aegis"

    @mock.patch("aegis.model.main.setup_authentication")
    def test_streaming_with_list_input(self, mock_auth):
        """Test streaming with list format input."""
        mock_auth.return_value = {
            "success": True,
            "status": "Success",
            "method": "api_key",
            "token": "test-token",
            "header": {"Authorization": "Bearer test-token"},
            "error": None,
        }

        conversation = [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there!"},
            {"role": "user", "content": "Tell me about Q3"},
        ]

        messages = list(model(conversation))

        assert len(messages) >= 1

        # Should have valid message structure
        for msg in messages:
            assert set(msg.keys()) == {"type", "name", "content"}

    @mock.patch("aegis.model.main.setup_authentication")
    def test_streaming_with_db_filters(self, mock_auth):
        """Test streaming with database filters."""
        mock_auth.return_value = {
            "success": True,
            "status": "Success",
            "method": "api_key",
            "token": "test-token",
            "header": {"Authorization": "Bearer test-token"},
            "error": None,
        }

        conversation = {"messages": [{"role": "user", "content": "Test"}]}
        db_names = ["internal_capm", "internal_wiki", "external_ey"]

        messages = list(model(conversation, db_names))

        assert len(messages) > 0

        # All messages should have the unified schema
        for msg in messages:
            assert set(msg.keys()) == {"type", "name", "content"}

    @mock.patch("aegis.model.main.post_monitor_entries")
    @mock.patch("aegis.model.main.add_monitor_entry")
    @mock.patch("aegis.model.main.initialize_monitor")
    @mock.patch("aegis.model.main.setup_authentication")
    def test_monitoring_integration(self, mock_auth, mock_init, mock_add, mock_post):
        """Test that monitoring is properly integrated."""
        mock_auth.return_value = {
            "success": True,
            "status": "Success",
            "method": "api_key",
            "token": "test-token",
            "header": {"Authorization": "Bearer test-token"},
            "error": None,
        }
        mock_post.return_value = 4  # Number of entries posted

        conversation = {"messages": [{"role": "user", "content": "Test"}]}
        db_names = ["internal_capm"]

        list(model(conversation, db_names))

        # Verify monitoring was initialized
        mock_init.assert_called_once()
        call_args = mock_init.call_args
        assert call_args[0][1] == "aegis"  # model_name

        # Verify monitor entries were added
        assert mock_add.call_count >= 4  # SSL, Auth, Conversation, Filter stages

        # Verify entries were posted
        mock_post.assert_called_once()

        # Check that Filter_Processing stage was logged with db_names
        filter_calls = [
            call
            for call in mock_add.call_args_list
            if call[1].get("stage_name") == "Filter_Processing"
        ]
        assert len(filter_calls) == 1
        filter_call = filter_calls[0]
        metadata = filter_call[1].get("custom_metadata", {})
        assert metadata.get("db_names_requested") == db_names
        assert metadata.get("filter_count") == 1
