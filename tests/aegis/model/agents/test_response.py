"""
Tests for the response agent.
"""

import pytest
from unittest.mock import patch, MagicMock
from src.aegis.model.agents.response import generate_response


class TestResponseAgent:
    """Test suite for the response agent."""

    @pytest.fixture
    def mock_context(self):
        """Mock context for testing."""
        return {
            "execution_id": "test-exec-123",
            "auth_config": {"method": "api_key", "credentials": {"api_key": "test"}},
            "ssl_config": {"verify": False},
        }

    @pytest.fixture
    def mock_prompts(self):
        """Mock prompt data."""
        return {
            "aegis/response.yaml": {
                "version": "1.0.0",
                "last_updated": "2025-01-24",
                "content": "You are the Response Agent. Be helpful and accurate.",
            },
            "global/project.yaml": {
                "content": "Aegis is a financial data assistant.",
            },
            "global/fiscal.yaml": {
                "content": "Fiscal year information.",
            },
        }

    @patch("src.aegis.model.agents.response.load_yaml")
    @patch("src.aegis.model.agents.response.complete")
    def test_generate_greeting_response(self, mock_complete, mock_load_yaml, mock_context, mock_prompts):
        """Test response generation for greetings."""
        # Setup mocks
        mock_load_yaml.side_effect = lambda path: mock_prompts.get(path, {})
        mock_complete.return_value = {
            "choices": [{"message": {"content": "Hello! How can I help you today?"}}],
            "usage": {"total_tokens": 50},
            "metrics": {"total_cost": 0.001, "response_time": 0.5},
        }

        # Test greeting response
        result = generate_response(
            conversation_history=[],
            latest_message="Hello",
            context=mock_context,
            streaming=False,
        )

        # Assertions
        assert result["status"] == "Success"
        assert "Hello" in result["response"]
        assert result["tokens_used"] == 50
        assert result["cost"] == 0.001
        assert result["response_time_ms"] == 500
        assert result["prompt_version"] == "1.0.0"

    @patch("src.aegis.model.agents.response.load_yaml")
    @patch("src.aegis.model.agents.response.complete")
    def test_generate_concept_explanation(self, mock_complete, mock_load_yaml, mock_context, mock_prompts):
        """Test response generation for financial concept explanations."""
        # Setup mocks
        mock_load_yaml.side_effect = lambda path: mock_prompts.get(path, {})
        mock_complete.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "ROE (Return on Equity) measures profitability relative to equity."
                    }
                }
            ],
            "usage": {"total_tokens": 100},
            "metrics": {"total_cost": 0.002, "response_time": 0.8},
        }

        # Test concept explanation
        result = generate_response(
            conversation_history=[],
            latest_message="What is ROE?",
            context=mock_context,
            streaming=False,
        )

        # Assertions
        assert result["status"] == "Success"
        assert "ROE" in result["response"]
        assert "equity" in result["response"].lower()
        assert result["tokens_used"] == 100

    @patch("src.aegis.model.agents.response.load_yaml")
    @patch("src.aegis.model.agents.response.complete")
    def test_conversation_context_awareness(self, mock_complete, mock_load_yaml, mock_context, mock_prompts):
        """Test that response agent maintains conversation context."""
        # Setup mocks
        mock_load_yaml.side_effect = lambda path: mock_prompts.get(path, {})
        mock_complete.return_value = {
            "choices": [{"message": {"content": "Based on the RBC data I showed earlier..."}}],
            "usage": {"total_tokens": 75},
            "metrics": {"total_cost": 0.0015, "response_time": 0.6},
        }

        # Test with conversation history
        conversation_history = [
            {"role": "user", "content": "Show me RBC efficiency ratio"},
            {"role": "assistant", "content": "RBC's efficiency ratio is 54.2%"},
            {"role": "user", "content": "Format that as a table"},
        ]

        result = generate_response(
            conversation_history=conversation_history,
            latest_message="Format that as a table",
            context=mock_context,
            streaming=False,
        )

        # Verify conversation history was included in messages
        mock_complete.assert_called_once()
        call_args = mock_complete.call_args
        messages = call_args[1]["messages"]
        
        # Should have system + 3 history + 1 current = 5 messages
        assert len(messages) >= 4
        assert any("RBC" in msg["content"] for msg in messages)

    @patch("src.aegis.model.agents.response.load_yaml")
    @patch("src.aegis.model.agents.response.stream")
    def test_streaming_response(self, mock_stream, mock_load_yaml, mock_context, mock_prompts):
        """Test streaming response generation."""
        # Setup mocks
        mock_load_yaml.side_effect = lambda path: mock_prompts.get(path, {})
        
        # Mock streaming chunks
        def stream_generator():
            yield {"choices": [{"delta": {"content": "Hello! "}}]}
            yield {"choices": [{"delta": {"content": "How can I help?"}}]}
            yield {
                "choices": [{"delta": {}}],
                "usage": {"total_tokens": 50},
                "metrics": {"total_cost": 0.001, "response_time": 0.5},
            }
        
        mock_stream.return_value = stream_generator()

        # Test streaming
        result_gen = generate_response(
            conversation_history=[],
            latest_message="Hello",
            context=mock_context,
            streaming=True,
        )

        # Collect chunks
        chunks = list(result_gen)
        
        # Assertions
        assert len(chunks) == 3
        assert chunks[0]["type"] == "chunk"
        assert chunks[0]["content"] == "Hello! "
        assert chunks[1]["type"] == "chunk"
        assert chunks[1]["content"] == "How can I help?"
        assert chunks[2]["type"] == "final"
        assert chunks[2]["status"] == "Success"
        assert chunks[2]["response"] == "Hello! How can I help?"
        assert chunks[2]["tokens_used"] == 50

    @patch("src.aegis.model.agents.response.load_yaml")
    @patch("src.aegis.model.agents.response.complete")
    def test_model_tier_override(self, mock_complete, mock_load_yaml, mock_context, mock_prompts):
        """Test that model tier can be overridden."""
        # Setup mocks
        mock_load_yaml.side_effect = lambda path: mock_prompts.get(path, {})
        mock_complete.return_value = {
            "choices": [{"message": {"content": "Response"}}],
            "usage": {"total_tokens": 50},
            "metrics": {"total_cost": 0.001, "response_time": 0.5},
        }

        # Test with medium model override
        context_with_override = {**mock_context, "model_tier_override": "medium"}
        
        result = generate_response(
            conversation_history=[],
            latest_message="Hello",
            context=context_with_override,
            streaming=False,
        )

        # Check that medium model was used
        call_args = mock_complete.call_args
        llm_params = call_args[1]["llm_params"]
        
        # Import config to check model names
        from src.aegis.utils.settings import config
        assert llm_params["model"] == config.llm.medium.model

    @patch("src.aegis.model.agents.response.load_yaml")
    @patch("src.aegis.model.agents.response.complete")
    def test_error_handling(self, mock_complete, mock_load_yaml, mock_context, mock_prompts):
        """Test error handling in response generation."""
        # Setup mocks
        mock_load_yaml.side_effect = lambda path: mock_prompts.get(path, {})
        mock_complete.side_effect = Exception("LLM API Error")

        # Test error handling
        result = generate_response(
            conversation_history=[],
            latest_message="Hello",
            context=mock_context,
            streaming=False,
        )

        # Assertions
        assert result["status"] == "Error"
        assert "apologize" in result["response"].lower()
        assert result["error"] == "LLM API Error"
        assert result["tokens_used"] == 0
        assert result["cost"] == 0

    @patch("src.aegis.model.agents.response.load_yaml")
    @patch("src.aegis.model.agents.response.stream")
    def test_streaming_error_handling(self, mock_stream, mock_load_yaml, mock_context, mock_prompts):
        """Test error handling in streaming response."""
        # Setup mocks
        mock_load_yaml.side_effect = lambda path: mock_prompts.get(path, {})
        mock_stream.side_effect = Exception("Stream Error")

        # Test streaming error
        result_gen = generate_response(
            conversation_history=[],
            latest_message="Hello",
            context=mock_context,
            streaming=True,
        )

        # Collect chunks
        chunks = list(result_gen)
        
        # Should have one final error chunk
        assert len(chunks) == 1
        assert chunks[0]["type"] == "final"
        assert chunks[0]["status"] == "Error"
        assert "apologize" in chunks[0]["response"].lower()
        assert chunks[0]["error"] == "Stream Error"

    @patch("src.aegis.model.agents.response.load_yaml")
    @patch("src.aegis.model.agents.response.complete")
    def test_conversation_history_limit(self, mock_complete, mock_load_yaml, mock_context, mock_prompts):
        """Test that conversation history is limited to last 10 messages."""
        # Setup mocks
        mock_load_yaml.side_effect = lambda path: mock_prompts.get(path, {})
        mock_complete.return_value = {
            "choices": [{"message": {"content": "Response"}}],
            "usage": {"total_tokens": 50},
            "metrics": {"total_cost": 0.001, "response_time": 0.5},
        }

        # Create long conversation history
        long_history = [
            {"role": "user" if i % 2 == 0 else "assistant", "content": f"Message {i}"}
            for i in range(20)
        ]

        result = generate_response(
            conversation_history=long_history,
            latest_message="Current query",
            context=mock_context,
            streaming=False,
        )

        # Check that only last 10 messages were included
        call_args = mock_complete.call_args
        messages = call_args[1]["messages"]
        
        # Count history messages (excluding system and current)
        history_messages = [m for m in messages if m["content"] not in ["Current query"] and m["role"] != "system"]
        assert len(history_messages) <= 10

    @patch("src.aegis.model.agents.response.load_yaml")
    @patch("src.aegis.model.agents.response.complete")
    def test_acknowledgment_response(self, mock_complete, mock_load_yaml, mock_context, mock_prompts):
        """Test response generation for acknowledgments."""
        # Setup mocks
        mock_load_yaml.side_effect = lambda path: mock_prompts.get(path, {})
        mock_complete.return_value = {
            "choices": [{"message": {"content": "You're welcome! Let me know if you need anything else."}}],
            "usage": {"total_tokens": 40},
            "metrics": {"total_cost": 0.0008, "response_time": 0.4},
        }

        # Test acknowledgment
        result = generate_response(
            conversation_history=[
                {"role": "user", "content": "Show me TD's revenue"},
                {"role": "assistant", "content": "TD's revenue is $45.2B"},
            ],
            latest_message="Thanks",
            context=mock_context,
            streaming=False,
        )

        # Assertions
        assert result["status"] == "Success"
        assert "welcome" in result["response"].lower() or "you're" in result["response"].lower()

    @patch("src.aegis.model.agents.response.load_yaml")
    @patch("src.aegis.model.agents.response.complete")
    def test_aegis_capabilities_response(self, mock_complete, mock_load_yaml, mock_context, mock_prompts):
        """Test response about Aegis capabilities."""
        # Setup mocks
        mock_load_yaml.side_effect = lambda path: mock_prompts.get(path, {})
        mock_complete.return_value = {
            "choices": [
                {
                    "message": {
                        "content": "I can help you access financial data for Canadian banks..."
                    }
                }
            ],
            "usage": {"total_tokens": 120},
            "metrics": {"total_cost": 0.0024, "response_time": 0.9},
        }

        # Test capabilities question
        result = generate_response(
            conversation_history=[],
            latest_message="What can you do?",
            context=mock_context,
            streaming=False,
        )

        # Assertions
        assert result["status"] == "Success"
        assert "financial" in result["response"].lower() or "banks" in result["response"].lower()