"""
Tests for the summarizer agent module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, AsyncMock, call

from aegis.model.agents.summarizer import synthesize_responses


async def async_generator(items):
    """Helper to create async generator from list."""
    for item in items:
        yield item


class TestSynthesizeResponses:
    """
    Tests for the synthesize_responses function.
    """

    @pytest.mark.asyncio
    @patch('aegis.model.agents.summarizer.stream')
    @patch('aegis.model.agents.summarizer.load_yaml')
    @patch('aegis.utils.settings.config')
    async def test_synthesize_responses_success(self, mock_config, mock_load_yaml, mock_stream):
        """
        Test successful synthesis of multiple database responses.
        """
        # Setup mocks
        mock_config.llm.large.model = "gpt-4"

        mock_load_yaml.side_effect = [
            {"content": "Summarizer prompt", "version": "1.0", "last_updated": "2024-01-01"},
            {"content": "Project context"},
            {"content": "Restrictions"}
        ]

        # Mock streaming response
        mock_stream.return_value = async_generator([
            {"choices": [{"delta": {"content": "Based on the data, "}}]},
            {"choices": [{"delta": {"content": "RBC's efficiency ratio is 55%."}}]},
            {"choices": [{"delta": {}}]},  # Empty delta
            {"usage": {"total_tokens": 1000}}
        ])

        # Test inputs
        conversation_history = [
            {"role": "user", "content": "Show me RBC data"}
        ]
        latest_message = "What is RBC's efficiency ratio?"
        database_responses = [
            {
                "database_id": "benchmarking",
                "full_intent": "Get RBC efficiency ratio Q1 2024",
                "response": "RBC efficiency ratio: 55% in Q1 2024"
            },
            {
                "database_id": "transcripts",
                "full_intent": "Get RBC management commentary on efficiency",
                "response": "Management discussed improving efficiency"
            }
        ]
        context = {"execution_id": "test-123"}

        # Execute and collect results
        results = []
        async for item in synthesize_responses(
            conversation_history,
            latest_message,
            database_responses,
            context
        ):
            results.append(item)

        # Verify results
        assert len(results) == 2  # Two content chunks
        assert results[0]["type"] == "agent"
        assert results[0]["name"] == "aegis"
        assert results[0]["content"] == "Based on the data, "
        assert results[1]["content"] == "RBC's efficiency ratio is 55%."

        # Verify stream was called with correct parameters
        mock_stream.assert_called_once()
        call_args = mock_stream.call_args
        messages = call_args[1]["messages"]

        # Check system message contains synthesis instructions
        assert "synthesis_instructions" in messages[0]["content"]
        assert "dropdown" in messages[0]["content"].lower()
        assert "below" in messages[0]["content"]

        # Check conversation history is included
        assert any("Show me RBC data" in msg["content"] for msg in messages)

        # Check final user message
        assert messages[-1]["role"] == "user"
        assert "BRIEF summary" in messages[-1]["content"]
        assert "below" in messages[-1]["content"]

    @pytest.mark.asyncio
    @patch('aegis.model.agents.summarizer.stream')
    @patch('aegis.model.agents.summarizer.load_yaml')
    @patch('aegis.utils.settings.config')
    async def test_synthesize_responses_single_database(self, mock_config, mock_load_yaml, mock_stream):
        """
        Test synthesis with single database response.
        """
        mock_config.llm.large.model = "gpt-4"
        mock_load_yaml.return_value = {"content": "Summarizer prompt"}

        mock_stream.return_value = async_generator([
            {"choices": [{"delta": {"content": "Summary content"}}]},
            {"usage": {"total_tokens": 500}}
        ])

        conversation_history = []
        latest_message = "Query"
        database_responses = [
            {
                "database_id": "benchmarking",
                "full_intent": "Get data",
                "response": "Response data"
            }
        ]
        context = {"execution_id": "test-123"}

        results = []
        async for item in synthesize_responses(
            conversation_history,
            latest_message,
            database_responses,
            context
        ):
            results.append(item)

        assert len(results) == 1
        assert results[0]["content"] == "Summary content"

    @pytest.mark.asyncio
    @patch('aegis.model.agents.summarizer.stream')
    @patch('aegis.model.agents.summarizer.load_yaml')
    @patch('aegis.utils.settings.config')
    async def test_synthesize_responses_with_placeholder_removal(self, mock_config, mock_load_yaml, mock_stream):
        """
        Test that test mode placeholders are removed from responses.
        """
        mock_config.llm.large.model = "gpt-4"
        mock_load_yaml.return_value = {"content": "Summarizer prompt"}

        mock_stream.return_value = async_generator([
            {"choices": [{"delta": {"content": "Clean summary"}}]}
        ])

        conversation_history = []
        latest_message = "Query"
        database_responses = [
            {
                "database_id": "benchmarking",
                "full_intent": "Get data",
                "response": "*[55% placeholder data - test mode]* actual content"
            }
        ]
        context = {"execution_id": "test-123"}

        # Execute
        results = []
        async for item in synthesize_responses(
            conversation_history,
            latest_message,
            database_responses,
            context
        ):
            results.append(item)

        # Verify placeholder was removed
        call_args = mock_stream.call_args
        messages = call_args[1]["messages"]
        system_content = messages[0]["content"]

        assert "*[" not in system_content
        assert "placeholder data - test mode]*" not in system_content
        assert "55% actual content" in system_content

    @pytest.mark.asyncio
    @patch('aegis.model.agents.summarizer.stream')
    @patch('aegis.model.agents.summarizer.load_yaml')
    @patch('aegis.utils.settings.config')
    async def test_synthesize_responses_limited_conversation_history(self, mock_config, mock_load_yaml, mock_stream):
        """
        Test that only last 5 messages from conversation history are included.
        """
        mock_config.llm.large.model = "gpt-4"
        mock_load_yaml.return_value = {"content": "Summarizer prompt"}

        mock_stream.return_value = async_generator([
            {"choices": [{"delta": {"content": "Summary"}}]}
        ])

        # Create 10 messages in history
        conversation_history = [
            {"role": "user", "content": f"Message {i}"} for i in range(10)
        ]
        latest_message = "Query"
        database_responses = [
            {"database_id": "benchmarking", "full_intent": "Get data", "response": "Data"}
        ]
        context = {"execution_id": "test-123"}

        results = []
        async for item in synthesize_responses(
            conversation_history,
            latest_message,
            database_responses,
            context
        ):
            results.append(item)

        # Verify only last 5 messages are included
        call_args = mock_stream.call_args
        messages = call_args[1]["messages"]

        # Count user messages (excluding system and final synthesis request)
        user_messages = [m for m in messages if m["role"] == "user" and "Message" in m["content"]]
        assert len(user_messages) == 5
        assert "Message 5" in user_messages[0]["content"]
        assert "Message 9" in user_messages[-1]["content"]

    @pytest.mark.asyncio
    @patch('aegis.model.agents.summarizer.stream')
    @patch('aegis.model.agents.summarizer.load_yaml')
    @patch('aegis.utils.settings.config')
    @patch('aegis.model.agents.summarizer.get_logger')
    async def test_synthesize_responses_exception_handling(self, mock_logger, mock_config, mock_load_yaml, mock_stream):
        """
        Test exception handling during synthesis.
        """
        mock_config.llm.large.model = "gpt-4"
        mock_load_yaml.return_value = {"content": "Summarizer prompt"}
        mock_stream.side_effect = Exception("LLM error")

        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance

        conversation_history = []
        latest_message = "Query"
        database_responses = [
            {"database_id": "benchmarking", "full_intent": "Get data", "response": "Data"}
        ]
        context = {"execution_id": "test-123"}

        results = []
        async for item in synthesize_responses(
            conversation_history,
            latest_message,
            database_responses,
            context
        ):
            results.append(item)

        # Should yield error message
        assert len(results) == 1
        assert results[0]["type"] == "agent"
        assert results[0]["name"] == "aegis"
        assert "Error in summarizer" in results[0]["content"]
        assert "LLM error" in results[0]["content"]

        # Should log error
        mock_logger_instance.error.assert_called_once()
        error_call = mock_logger_instance.error.call_args
        assert error_call[0][0] == "summarizer.error"
        assert "LLM error" in str(error_call[1]["error"])

    @pytest.mark.asyncio
    @patch('aegis.model.agents.summarizer.stream')
    @patch('aegis.model.agents.summarizer.load_yaml')
    @patch('aegis.utils.settings.config')
    async def test_synthesize_responses_load_yaml_exceptions(self, mock_config, mock_load_yaml, mock_stream):
        """
        Test that YAML loading exceptions are handled gracefully.
        """
        mock_config.llm.large.model = "gpt-4"

        # First call succeeds (summarizer.yaml), others fail
        mock_load_yaml.side_effect = [
            {"content": "Summarizer prompt"},
            Exception("Project YAML error"),
            Exception("Restrictions YAML error")
        ]

        mock_stream.return_value = async_generator([
            {"choices": [{"delta": {"content": "Summary"}}]}
        ])

        conversation_history = []
        latest_message = "Query"
        database_responses = [
            {"database_id": "benchmarking", "full_intent": "Get data", "response": "Data"}
        ]
        context = {"execution_id": "test-123"}

        # Should not raise exception
        results = []
        async for item in synthesize_responses(
            conversation_history,
            latest_message,
            database_responses,
            context
        ):
            results.append(item)

        assert len(results) == 1
        assert results[0]["content"] == "Summary"

    @pytest.mark.asyncio
    @patch('aegis.model.agents.summarizer.stream')
    @patch('aegis.model.agents.summarizer.load_yaml')
    @patch('aegis.utils.settings.config')
    @patch('aegis.model.agents.summarizer.get_logger')
    async def test_synthesize_responses_logging(self, mock_logger, mock_config, mock_load_yaml, mock_stream):
        """
        Test that appropriate logging occurs during synthesis.
        """
        mock_config.llm.large.model = "gpt-4"
        mock_load_yaml.return_value = {"content": "Summarizer prompt"}

        mock_stream.return_value = async_generator([
            {"choices": [{"delta": {"content": "Part1"}}]},
            {"choices": [{"delta": {"content": "Part2"}}]},
            {"usage": {"total_tokens": 750}}
        ])

        mock_logger_instance = Mock()
        mock_logger.return_value = mock_logger_instance

        conversation_history = []
        latest_message = "Query"
        database_responses = [
            {"database_id": "benchmarking", "full_intent": "Get data", "response": "Data1"},
            {"database_id": "reports", "full_intent": "Get reports", "response": "Data2"}
        ]
        context = {"execution_id": "test-123"}

        results = []
        async for item in synthesize_responses(
            conversation_history,
            latest_message,
            database_responses,
            context
        ):
            results.append(item)

        # Check start logging
        start_call = mock_logger_instance.info.call_args_list[0]
        assert start_call[0][0] == "summarizer.starting"
        assert start_call[1]["database_count"] == 2
        assert start_call[1]["databases"] == ["benchmarking", "reports"]

        # Check completion logging
        complete_call = mock_logger_instance.info.call_args_list[1]
        assert complete_call[0][0] == "summarizer.completed"
        assert complete_call[1]["tokens_used"] == 750
        assert complete_call[1]["total_chars"] == 10  # "Part1Part2"
        assert complete_call[1]["chunk_count"] == 2
        assert complete_call[1]["databases_synthesized"] == 2

    @pytest.mark.asyncio
    @patch('aegis.model.agents.summarizer.stream')
    @patch('aegis.model.agents.summarizer.load_yaml')
    @patch('aegis.utils.settings.config')
    async def test_synthesize_responses_empty_database_responses(self, mock_config, mock_load_yaml, mock_stream):
        """
        Test synthesis with empty database responses.
        """
        mock_config.llm.large.model = "gpt-4"
        mock_load_yaml.return_value = {"content": "Summarizer prompt"}

        mock_stream.return_value = async_generator([
            {"choices": [{"delta": {"content": "No data available"}}]}
        ])

        conversation_history = []
        latest_message = "Query"
        database_responses = []  # Empty list
        context = {"execution_id": "test-123"}

        results = []
        async for item in synthesize_responses(
            conversation_history,
            latest_message,
            database_responses,
            context
        ):
            results.append(item)

        assert len(results) == 1
        assert results[0]["content"] == "No data available"

    @pytest.mark.asyncio
    @patch('aegis.model.agents.summarizer.stream')
    @patch('aegis.model.agents.summarizer.load_yaml')
    @patch('aegis.utils.settings.config')
    async def test_synthesize_responses_llm_params(self, mock_config, mock_load_yaml, mock_stream):
        """
        Test that correct LLM parameters are used.
        """
        mock_config.llm.large.model = "gpt-4-turbo"
        mock_load_yaml.return_value = {"content": "Summarizer prompt"}

        mock_stream.return_value = async_generator([
            {"choices": [{"delta": {"content": "Summary"}}]}
        ])

        conversation_history = []
        latest_message = "Query"
        database_responses = [
            {"database_id": "benchmarking", "full_intent": "Get data", "response": "Data"}
        ]
        context = {"execution_id": "test-123"}

        results = []
        async for item in synthesize_responses(
            conversation_history,
            latest_message,
            database_responses,
            context
        ):
            results.append(item)

        # Verify LLM parameters
        call_args = mock_stream.call_args
        llm_params = call_args[1]["llm_params"]

        assert llm_params["model"] == "gpt-4-turbo"
        assert llm_params["temperature"] == 0.3
        assert llm_params["max_tokens"] == 300