"""
Tests for LLM connector module.
"""

from unittest import mock
from unittest.mock import MagicMock

import pytest
import httpx
from openai import OpenAI

from aegis.connections.llm import complete, stream, complete_with_tools, check_connection
from aegis.utils.settings import config


@pytest.fixture(autouse=True)
def reset_llm_cache():
    """Clear the client cache before each test."""
    from aegis.connections.llm import llm_connector
    llm_connector._client_cache.clear()
    yield
    llm_connector._client_cache.clear()


class TestLLMComplete:
    """Test cases for non-streaming completion."""

    @mock.patch("aegis.connections.llm.llm_connector.OpenAI")
    def test_complete_success(self, mock_openai_class):
        """Test successful completion request."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.model_dump.return_value = {
            "id": "chatcmpl-123",
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "Test response"
                    },
                    "index": 0,
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15
            }
        }
        
        # Setup mock client
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        # Test data
        messages = [{"role": "user", "content": "Hello"}]
        auth_config = {"token": "test-token", "method": "api_key"}
        ssl_config = {"verify": False, "cert_path": None}
        
        # Make request
        result = complete(
            messages=messages,
            auth_config=auth_config,
            ssl_config=ssl_config,
            execution_id="test-exec-id",
            model=config.llm.small.model,
            temperature=0.5,
            max_tokens=100
        )
        
        # Verify response
        assert result["id"] == "chatcmpl-123"
        assert result["choices"][0]["message"]["content"] == "Test response"
        assert result["usage"]["total_tokens"] == 15
        
        # Verify client was called correctly
        mock_client.chat.completions.create.assert_called_once_with(
            model=config.llm.small.model,
            messages=messages,
            temperature=0.5,
            max_tokens=100
        )

    @mock.patch("aegis.connections.llm.llm_connector.OpenAI")
    def test_complete_uses_default_model(self, mock_openai_class):
        """Test completion uses medium model by default."""
        # Setup mock
        mock_response = MagicMock()
        mock_response.model_dump.return_value = {
            "id": "chatcmpl-123",
            "choices": [{"message": {"role": "assistant", "content": "Test"}}],
        }
        
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        # Test without specifying model
        messages = [{"role": "user", "content": "Hello"}]
        auth_config = {"token": "test-token", "method": "api_key"}
        ssl_config = {"verify": False, "cert_path": None}
        
        complete(
            messages=messages,
            auth_config=auth_config,
            ssl_config=ssl_config,
            execution_id="test-exec-id"
        )
        
        # Should use medium model by default
        mock_client.chat.completions.create.assert_called_once()
        call_args = mock_client.chat.completions.create.call_args
        assert call_args.kwargs["model"] == config.llm.medium.model
        assert call_args.kwargs["temperature"] == config.llm.medium.temperature
        assert call_args.kwargs["max_tokens"] == config.llm.medium.max_tokens

    @mock.patch("aegis.connections.llm.llm_connector.OpenAI")
    def test_complete_error_handling(self, mock_openai_class):
        """Test completion handles API errors properly."""
        # Setup mock to raise error
        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = Exception("API Error")
        mock_openai_class.return_value = mock_client
        
        # Test data
        messages = [{"role": "user", "content": "Hello"}]
        auth_config = {"token": "test-token", "method": "api_key"}
        ssl_config = {"verify": False, "cert_path": None}
        
        # Should raise exception
        with pytest.raises(Exception, match="API Error"):
            complete(
                messages=messages,
                auth_config=auth_config,
                ssl_config=ssl_config,
                execution_id="test-exec-id"
            )


class TestLLMStream:
    """Test cases for streaming completion."""

    @mock.patch("aegis.connections.llm.llm_connector.OpenAI")
    def test_stream_success(self, mock_openai_class):
        """Test successful streaming request."""
        # Setup mock streaming response
        mock_chunk1 = MagicMock()
        mock_chunk1.model_dump.return_value = {
            "id": "chatcmpl-123",
            "choices": [{"delta": {"content": "Hello"}, "index": 0}]
        }
        
        mock_chunk2 = MagicMock()
        mock_chunk2.model_dump.return_value = {
            "id": "chatcmpl-123",
            "choices": [{"delta": {"content": " world"}, "index": 0}]
        }
        
        # Setup mock client
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = iter([mock_chunk1, mock_chunk2])
        mock_openai_class.return_value = mock_client
        
        # Test data
        messages = [{"role": "user", "content": "Hello"}]
        auth_config = {"token": "test-token", "method": "api_key"}
        ssl_config = {"verify": False, "cert_path": None}
        
        # Collect streamed chunks
        chunks = list(stream(
            messages=messages,
            auth_config=auth_config,
            ssl_config=ssl_config,
            execution_id="test-exec-id",
            model=config.llm.large.model
        ))
        
        # Verify chunks
        assert len(chunks) == 2
        assert chunks[0]["choices"][0]["delta"]["content"] == "Hello"
        assert chunks[1]["choices"][0]["delta"]["content"] == " world"
        
        # Verify stream=True was passed
        mock_client.chat.completions.create.assert_called_once()
        call_args = mock_client.chat.completions.create.call_args
        assert call_args.kwargs["stream"] is True


class TestLLMTools:
    """Test cases for tool/function calling."""

    @mock.patch("aegis.connections.llm.llm_connector.OpenAI")
    def test_complete_with_tools_success(self, mock_openai_class):
        """Test successful completion with tools."""
        # Setup mock response with tool call
        mock_response = MagicMock()
        mock_response.model_dump.return_value = {
            "id": "chatcmpl-123",
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": "tool-1",
                                "function": {
                                    "name": "test_function",
                                    "arguments": '{"arg": "value"}'
                                }
                            }
                        ]
                    },
                    "finish_reason": "tool_calls"
                }
            ],
            "usage": {"prompt_tokens": 20, "completion_tokens": 10, "total_tokens": 30}
        }
        
        # Setup mock client
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        # Test data
        messages = [{"role": "user", "content": "Call a function"}]
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "test_function",
                    "description": "A test function",
                    "parameters": {"type": "object", "properties": {}}
                }
            }
        ]
        auth_config = {"token": "test-token", "method": "api_key"}
        ssl_config = {"verify": False, "cert_path": None}
        
        # Make request
        result = complete_with_tools(
            messages=messages,
            tools=tools,
            auth_config=auth_config,
            ssl_config=ssl_config,
            execution_id="test-exec-id"
        )
        
        # Verify response has tool calls
        assert result["choices"][0]["message"]["tool_calls"][0]["function"]["name"] == "test_function"
        
        # Verify tools were passed to API
        mock_client.chat.completions.create.assert_called_once()
        call_args = mock_client.chat.completions.create.call_args
        assert call_args.kwargs["tools"] == tools

    @mock.patch("aegis.connections.llm.llm_connector.OpenAI")  
    def test_complete_with_tools_defaults_to_large_model(self, mock_openai_class):
        """Test tool completion defaults to large model."""
        # Setup mock
        mock_response = MagicMock()
        mock_response.model_dump.return_value = {
            "id": "chatcmpl-123",
            "choices": [{"message": {"role": "assistant", "content": "Test"}}],
        }
        
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        # Test without specifying model
        messages = [{"role": "user", "content": "Hello"}]
        tools = []
        auth_config = {"token": "test-token", "method": "api_key"}
        ssl_config = {"verify": False, "cert_path": None}
        
        complete_with_tools(
            messages=messages,
            tools=tools,
            auth_config=auth_config,
            ssl_config=ssl_config,
            execution_id="test-exec-id"
        )
        
        # Should use large model for tools by default
        call_args = mock_client.chat.completions.create.call_args
        assert call_args.kwargs["model"] == config.llm.large.model


class TestLLMConnection:
    """Test cases for connection testing."""

    @mock.patch("aegis.connections.llm.llm_connector.complete")
    def test_connection_success(self, mock_complete):
        """Test successful connection test."""
        # Setup mock response
        mock_complete.return_value = {
            "choices": [
                {"message": {"content": "Hello! I'm working properly."}}
            ]
        }
        
        auth_config = {"token": "test-token", "method": "api_key"}
        ssl_config = {"verify": False, "cert_path": None}
        
        result = check_connection(auth_config, ssl_config, "test-exec-id")
        
        assert result["status"] == "success"
        assert result["response"] == "Hello! I'm working properly."
        assert result["auth_method"] == "api_key"
        assert result["model"] == config.llm.small.model

    @mock.patch("aegis.connections.llm.llm_connector.complete")
    def test_connection_failure(self, mock_complete):
        """Test failed connection test."""
        # Setup mock to raise error
        mock_complete.side_effect = Exception("Connection failed")
        
        auth_config = {"token": "test-token", "method": "oauth"}
        ssl_config = {"verify": True, "cert_path": "/path/to/cert"}
        
        result = check_connection(auth_config, ssl_config, "test-exec-id")
        
        assert result["status"] == "failed"
        assert "Connection failed" in result["error"]
        assert result["auth_method"] == "oauth"


class TestClientCaching:
    """Test cases for client caching."""

    @mock.patch("aegis.connections.llm.llm_connector.OpenAI")
    @mock.patch("aegis.connections.llm.llm_connector.httpx.Client")
    def test_client_caching(self, mock_httpx_client, mock_openai_class):
        """Test that clients are cached and reused."""
        # Setup mocks
        mock_response = MagicMock()
        mock_response.model_dump.return_value = {
            "id": "chatcmpl-123",
            "choices": [{"message": {"role": "assistant", "content": "Test"}}],
        }
        
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        messages = [{"role": "user", "content": "Hello"}]
        auth_config = {"token": "same-token", "method": "api_key"}
        ssl_config = {"verify": False, "cert_path": None}
        
        # First call - should create client
        complete(messages, auth_config, ssl_config, "exec-1")
        assert mock_openai_class.call_count == 1
        
        # Second call with same token - should reuse client
        complete(messages, auth_config, ssl_config, "exec-2")
        assert mock_openai_class.call_count == 1  # Still 1, client was reused
        
        # Third call with different token - should create new client
        auth_config2 = {"token": "different-token", "method": "api_key"}
        complete(messages, auth_config2, ssl_config, "exec-3")
        assert mock_openai_class.call_count == 2  # New client created


class TestSSLConfiguration:
    """Test cases for SSL configuration."""

    @mock.patch("aegis.connections.llm.llm_connector.OpenAI")
    @mock.patch("aegis.connections.llm.llm_connector.httpx.Client")
    def test_ssl_with_custom_cert(self, mock_httpx_client, mock_openai_class):
        """Test SSL configuration with custom certificate."""
        # Setup mocks
        mock_response = MagicMock()
        mock_response.model_dump.return_value = {
            "id": "chatcmpl-123",
            "choices": [{"message": {"role": "assistant", "content": "Test"}}],
        }
        
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        messages = [{"role": "user", "content": "Hello"}]
        auth_config = {"token": "test-token", "method": "api_key"}
        ssl_config = {"verify": True, "cert_path": "/path/to/cert.pem"}
        
        complete(messages, auth_config, ssl_config, "test-exec-id")
        
        # Verify httpx.Client was created with cert path
        mock_httpx_client.assert_called_once()
        call_args = mock_httpx_client.call_args
        assert call_args.kwargs["verify"] == "/path/to/cert.pem"

    @mock.patch("aegis.connections.llm.llm_connector.OpenAI")
    @mock.patch("aegis.connections.llm.llm_connector.httpx.Client")
    def test_ssl_disabled(self, mock_httpx_client, mock_openai_class):
        """Test SSL configuration when disabled."""
        # Setup mocks
        mock_response = MagicMock()
        mock_response.model_dump.return_value = {
            "id": "chatcmpl-123",
            "choices": [{"message": {"role": "assistant", "content": "Test"}}],
        }
        
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        messages = [{"role": "user", "content": "Hello"}]
        auth_config = {"token": "test-token", "method": "api_key"}
        ssl_config = {"verify": False, "cert_path": None}
        
        complete(messages, auth_config, ssl_config, "test-exec-id")
        
        # Verify httpx.Client was created with verify=False
        mock_httpx_client.assert_called_once()
        call_args = mock_httpx_client.call_args
        assert call_args.kwargs["verify"] is False