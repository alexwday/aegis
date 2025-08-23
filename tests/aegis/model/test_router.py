"""
Tests for the router agent.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.aegis.model.agents.router import route_query


class TestRouterAgent:
    """Test the router agent functionality."""

    @patch("src.aegis.model.agents.router.complete_with_tools")
    @patch("src.aegis.model.agents.router.load_yaml")
    def test_route_to_direct_response(self, mock_load_yaml, mock_complete):
        """Test routing to direct response when data exists in conversation."""
        # Setup
        mock_load_yaml.return_value = {"content": "Test prompt content"}
        mock_complete.return_value = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "arguments": json.dumps({"r": 0})  # Binary 0 for direct_response
                                }
                            }
                        ]
                    }
                }
            ],
            "usage": {"total_tokens": 100},
            "metrics": {"total_cost": 0.001, "response_time": 0.5}
        }

        context = {"execution_id": "test-123", "auth_config": {}, "ssl_config": {}}
        conversation_history = [
            {"role": "user", "content": "What is RBC's NIM?"},
            {"role": "assistant", "content": "RBC's NIM is 1.65%"},
        ]
        latest_message = "Can you show that in a table?"

        # Execute
        result = route_query(conversation_history, latest_message, context)

        # Assert
        assert result["status"] == "Success"
        assert result["route"] == "direct_response"
        assert result["tokens_used"] == 100
        assert result["cost"] == 0.001
        assert "Direct response" in result["rationale"]

    @patch("src.aegis.model.agents.router.complete_with_tools")
    @patch("src.aegis.model.agents.router.load_yaml")
    def test_route_to_research_workflow(self, mock_load_yaml, mock_complete):
        """Test routing to research workflow when new data is needed."""
        # Setup
        mock_load_yaml.return_value = {"content": "Test prompt content"}
        mock_complete.return_value = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "arguments": json.dumps({"r": 1})  # Binary 1 for research_workflow
                                }
                            }
                        ]
                    }
                }
            ],
            "usage": {"total_tokens": 150},
            "metrics": {"total_cost": 0.002, "response_time": 0.6}
        }

        context = {"execution_id": "test-456", "auth_config": {}, "ssl_config": {}}
        conversation_history = []
        latest_message = "Show me Canadian bank efficiency ratios"

        # Execute
        result = route_query(conversation_history, latest_message, context)

        # Assert
        assert result["status"] == "Success"
        assert result["route"] == "research_workflow"
        assert result["tokens_used"] == 150
        assert result["cost"] == 0.002
        assert "Data retrieval" in result["rationale"]

    @patch("src.aegis.model.agents.router.complete_with_tools")
    @patch("src.aegis.model.agents.router.load_yaml")
    def test_router_error_defaults_to_research(self, mock_load_yaml, mock_complete):
        """Test that router defaults to research workflow on error."""
        # Setup
        mock_load_yaml.return_value = {"content": "Test prompt content"}
        mock_complete.side_effect = Exception("API Error")

        context = {"execution_id": "test-789", "auth_config": {}, "ssl_config": {}}
        conversation_history = []
        latest_message = "Test query"

        # Execute
        result = route_query(conversation_history, latest_message, context)

        # Assert
        assert result["status"] == "Error"
        assert result["route"] == "research_workflow"
        assert result["tokens_used"] == 0
        assert result["cost"] == 0
        assert "error" in result["rationale"].lower()
        assert result["error"] == "API Error"

    @patch("src.aegis.model.agents.router.complete_with_tools")
    @patch("src.aegis.model.agents.router.load_yaml")
    def test_router_no_tool_response_defaults(self, mock_load_yaml, mock_complete):
        """Test router defaults when no tool response is received."""
        # Setup
        mock_load_yaml.return_value = {"content": "Test prompt content"}
        mock_complete.return_value = {
            "choices": [{"message": {}}],  # No tool_calls
            "usage": {"total_tokens": 50},
            "metrics": {"total_cost": 0.0005, "response_time": 0.3}
        }

        context = {"execution_id": "test-000", "auth_config": {}, "ssl_config": {}}
        conversation_history = []
        latest_message = "Test query"

        # Execute
        result = route_query(conversation_history, latest_message, context)

        # Assert
        assert result["status"] == "Success"
        assert result["route"] == "research_workflow"
        assert result["tokens_used"] == 50
        assert result["cost"] == 0.0005
        assert "no clear routing decision" in result["rationale"].lower()

    @patch("src.aegis.model.agents.router.complete_with_tools")
    @patch("src.aegis.model.agents.router.load_yaml")
    def test_router_with_database_filters(self, mock_load_yaml, mock_complete):
        """Test router with database filters provided."""
        # Setup
        mock_load_yaml.return_value = {"content": "Test prompt content"}
        mock_complete.return_value = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "arguments": json.dumps({"r": 1})  # Binary 1 for research
                                }
                            }
                        ]
                    }
                }
            ],
            "usage": {"total_tokens": 200},
            "metrics": {"total_cost": 0.003, "response_time": 0.7}
        }

        context = {
            "execution_id": "test-111", 
            "auth_config": {}, 
            "ssl_config": {},
            "available_databases": ["benchmarking", "reports"],
            "database_prompt": "Test database prompt"
        }
        conversation_history = []
        latest_message = "Compare efficiency ratios"

        # Execute
        result = route_query(conversation_history, latest_message, context)

        # Assert
        assert result["status"] == "Success"
        assert result["route"] == "research_workflow"
        # Verify db_names was passed to the prompt formatting
        mock_complete.assert_called_once()
        call_args = mock_complete.call_args
        assert "benchmarking" in str(call_args)

    def test_router_conversation_history_limit(self):
        """Test that router only uses last 10 messages from conversation."""
        with patch("src.aegis.model.agents.router.load_yaml") as mock_load:
            with patch("src.aegis.model.agents.router.complete_with_tools") as mock_complete:
                mock_load.return_value = {"content": "Test prompt"}
                mock_complete.return_value = {
                    "choices": [
                        {
                            "message": {
                                "tool_calls": [
                                    {
                                        "function": {
                                            "arguments": json.dumps({"r": 0})  # Binary 0
                                        }
                                    }
                                ]
                            }
                        }
                    ],
                    "usage": {"total_tokens": 80},
                    "metrics": {"total_cost": 0.0008, "response_time": 0.4}
                }

                # Create long conversation history
                long_history = [
                    {"role": "user" if i % 2 == 0 else "assistant", "content": f"Message {i}"}
                    for i in range(20)
                ]

                context = {"execution_id": "test-long", "auth_config": {}, "ssl_config": {}}
                result = route_query(long_history, "Latest query", context)

                # Check that only last 10 messages were included
                call_args = mock_complete.call_args[1]["messages"][1]["content"]
                # Should contain messages 10-19 (last 10)
                assert "Message 10" in call_args
                assert "Message 19" in call_args
                assert "Message 9" not in call_args  # Should not contain earlier messages