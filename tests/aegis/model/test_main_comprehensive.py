"""
Comprehensive tests for the main model orchestrator.

This test suite provides complete coverage for the main.py model function
with proper mocking to avoid external dependencies.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call
import uuid
from datetime import datetime, timezone

from aegis.model.main import model, extract_s3_info


class TestExtractS3Info:
    """Test S3 file extraction utility function."""

    def test_extract_s3_info_with_single_file(self):
        """Test extracting single S3 file marker."""
        content = "Here is a report {{S3_LINK:download:docx:report.docx:Download Report}}"

        result = extract_s3_info(content)

        assert len(result) == 1
        assert result[0]["action"] == "download"
        assert result[0]["file_type"] == "docx"
        assert result[0]["s3_key"] == "report.docx"
        assert result[0]["display_text"] == "Download Report"

    def test_extract_s3_info_with_multiple_files(self):
        """Test extracting multiple S3 file markers."""
        content = """
        First file: {{S3_LINK:open:pdf:doc1.pdf:View PDF}}
        Second file: {{S3_LINK:download:docx:doc2.docx:Get DOCX}}
        """

        result = extract_s3_info(content)

        assert len(result) == 2
        assert result[0]["action"] == "open"
        assert result[0]["file_type"] == "pdf"
        assert result[1]["action"] == "download"
        assert result[1]["file_type"] == "docx"

    def test_extract_s3_info_no_markers(self):
        """Test content without S3 markers."""
        content = "This is regular content without any special markers."

        result = extract_s3_info(content)

        assert result == []

    def test_extract_s3_info_empty_content(self):
        """Test empty content."""
        result = extract_s3_info("")
        assert result == []


class TestModelOrchestrator:
    """Comprehensive tests for the main model function."""

    @pytest.fixture
    def mock_context(self):
        """Mock context dictionary for testing."""
        return {
            "execution_id": "test-exec-123",
            "auth_config": {"method": "api_key", "credentials": {"api_key": "test"}},
            "ssl_config": {"verify": False},
            "available_databases": ["benchmarking", "reports", "rts"],
            "database_prompt": "Test database prompt"
        }

    @pytest.fixture
    def sample_conversation(self):
        """Sample conversation for testing."""
        return {
            "messages": [
                {"role": "user", "content": "What is RBC's revenue?"}
            ]
        }

    @pytest.mark.asyncio
    @patch("aegis.model.main.setup_ssl")
    @patch("aegis.model.main.setup_authentication", new_callable=AsyncMock)
    @patch("aegis.model.main.process_conversation")
    @patch("aegis.model.main.filter_databases")
    @patch("aegis.model.main.get_database_prompt")
    @patch("aegis.model.main.route_query", new_callable=AsyncMock)
    @patch("aegis.model.main.generate_response", new_callable=AsyncMock)
    @patch("aegis.model.main.initialize_monitor")
    @patch("aegis.model.main.post_monitor_entries_async", new_callable=AsyncMock)
    async def test_direct_response_path_complete(
        self,
        mock_post_monitor,
        mock_init_monitor,
        mock_generate_response,
        mock_route_query,
        mock_get_db_prompt,
        mock_filter_dbs,
        mock_process_conv,
        mock_setup_auth,
        mock_setup_ssl,
        sample_conversation,
        mock_context
    ):
        """Test complete direct response path with all stages."""

        # Setup mocks for initialization
        mock_setup_ssl.return_value = {"verify": False, "success": True}
        mock_setup_auth.return_value = {"method": "api_key", "credentials": {"api_key": "test"}}
        mock_process_conv.return_value = {
            "success": True,
            "messages": sample_conversation["messages"],
            "latest_message": {"role": "user", "content": "What is RBC's revenue?"},
            "message_count": 1
        }
        mock_filter_dbs.return_value = {
            "benchmarking": {"name": "Benchmarking", "content": "Benchmarking data"},
            "reports": {"name": "Reports", "content": "Reports data"}
        }
        mock_get_db_prompt.return_value = "Database prompt"

        # Setup router to return direct_response
        mock_route_query.return_value = {
            "status": "Success",
            "route": "direct_response",
            "rationale": "Simple definition question"
        }

        # Setup response generator
        async def mock_response_gen():
            yield {"type": "agent", "name": "aegis", "content": "RBC's revenue is $12B"}

        mock_generate_response.return_value = mock_response_gen()
        mock_post_monitor.return_value = 5  # 5 entries posted

        # Execute
        responses = []
        async for chunk in model(sample_conversation):
            responses.append(chunk)

        # Verify workflow stages were called
        mock_setup_ssl.assert_called_once()
        mock_setup_auth.assert_called_once()
        mock_process_conv.assert_called_once()
        mock_route_query.assert_called_once()
        mock_generate_response.assert_called_once()
        mock_post_monitor.assert_called_once()

        # Verify response structure
        assert len(responses) == 1
        assert responses[0]["type"] == "agent"
        assert responses[0]["name"] == "aegis"
        assert "revenue" in responses[0]["content"]

    @pytest.mark.asyncio
    @patch("aegis.model.main.setup_ssl")
    @patch("aegis.model.main.setup_authentication", new_callable=AsyncMock)
    @patch("aegis.model.main.process_conversation")
    @patch("aegis.model.main.filter_databases")
    @patch("aegis.model.main.get_database_prompt")
    @patch("aegis.model.main.route_query", new_callable=AsyncMock)
    @patch("aegis.model.main.clarify_query", new_callable=AsyncMock)
    @patch("aegis.model.main.plan_database_queries", new_callable=AsyncMock)
    @patch("aegis.model.main.run_subagent", new_callable=AsyncMock)
    @patch("aegis.model.main.synthesize_responses", new_callable=AsyncMock)
    @patch("aegis.model.main.initialize_monitor")
    @patch("aegis.model.main.post_monitor_entries_async", new_callable=AsyncMock)
    async def test_research_workflow_complete(
        self,
        mock_post_monitor,
        mock_init_monitor,
        mock_synthesize,
        mock_run_subagent,
        mock_planner,
        mock_clarifier,
        mock_route_query,
        mock_get_db_prompt,
        mock_filter_dbs,
        mock_process_conv,
        mock_setup_auth,
        mock_setup_ssl,
        sample_conversation
    ):
        """Test complete research workflow with subagents."""

        # Setup initialization mocks
        mock_setup_ssl.return_value = {"verify": False, "success": True}
        mock_setup_auth.return_value = {"method": "api_key", "credentials": {"api_key": "test"}}
        mock_process_conv.return_value = {
            "success": True,
            "messages": sample_conversation["messages"],
            "latest_message": {"role": "user", "content": "What is RBC's revenue?"},
            "message_count": 1
        }
        mock_filter_dbs.return_value = {
            "benchmarking": {"name": "Benchmarking", "content": "Benchmarking data"},
            "reports": {"name": "Reports", "content": "Reports data"}
        }
        mock_get_db_prompt.return_value = "Database prompt"

        # Setup router for research workflow
        mock_route_query.return_value = {
            "status": "Success",
            "route": "research_workflow",
            "rationale": "Data request"
        }

        # Setup clarifier success
        mock_clarifier.return_value = {
            "status": "success",
            "bank_period_combinations": [
                {"bank_id": 1, "bank_name": "RBC", "fiscal_year": 2024, "quarter": "Q3"}
            ],
            "query_intent": "Get RBC revenue data"
        }

        # Setup planner success
        mock_planner.return_value = {
            "status": "success",
            "databases_to_query": ["benchmarking", "reports"],
            "query_intent": "Revenue analysis"
        }

        # Setup subagent responses
        async def mock_subagent_gen():
            yield {"type": "subagent", "name": "benchmarking", "content": "Revenue: $12B"}

        mock_run_subagent.return_value = mock_subagent_gen()

        # Setup synthesizer
        async def mock_synthesis_gen():
            yield {"type": "agent", "name": "aegis", "content": "Based on the data, RBC's revenue is $12B"}

        mock_synthesize.return_value = mock_synthesis_gen()
        mock_post_monitor.return_value = 8  # 8 entries posted

        # Execute
        responses = []
        async for chunk in model(sample_conversation):
            responses.append(chunk)

        # Verify all stages were called
        mock_route_query.assert_called_once()
        mock_clarifier.assert_called_once()
        mock_planner.assert_called_once()
        mock_synthesize.assert_called_once()

        # Verify we got responses from both subagent and synthesizer
        subagent_responses = [r for r in responses if r["type"] == "subagent"]
        agent_responses = [r for r in responses if r["type"] == "agent"]

        assert len(subagent_responses) >= 1
        assert len(agent_responses) >= 1

    @pytest.mark.asyncio
    @patch("aegis.model.main.setup_ssl")
    async def test_ssl_setup_failure(self, mock_setup_ssl):
        """Test handling of SSL setup failure."""
        mock_setup_ssl.return_value = {"success": False, "error": "SSL failed"}

        responses = []
        async for chunk in model({"messages": [{"role": "user", "content": "test"}]}):
            responses.append(chunk)

        assert len(responses) == 1
        assert "SSL setup failed" in responses[0]["content"]

    @pytest.mark.asyncio
    @patch("aegis.model.main.setup_ssl")
    @patch("aegis.model.main.setup_authentication", new_callable=AsyncMock)
    async def test_auth_failure(self, mock_setup_auth, mock_setup_ssl):
        """Test handling of authentication failure."""
        mock_setup_ssl.return_value = {"success": True, "verify": False}
        mock_setup_auth.side_effect = Exception("Auth failed")

        responses = []
        async for chunk in model({"messages": [{"role": "user", "content": "test"}]}):
            responses.append(chunk)

        assert len(responses) == 1
        assert "Authentication failed" in responses[0]["content"]

    @pytest.mark.asyncio
    @patch("aegis.model.main.setup_ssl")
    @patch("aegis.model.main.setup_authentication", new_callable=AsyncMock)
    @patch("aegis.model.main.process_conversation")
    async def test_conversation_processing_failure(
        self, mock_process_conv, mock_setup_auth, mock_setup_ssl
    ):
        """Test handling of conversation processing failure."""
        mock_setup_ssl.return_value = {"success": True, "verify": False}
        mock_setup_auth.return_value = {"method": "api_key"}
        mock_process_conv.return_value = {
            "success": False,
            "error": "Invalid conversation format"
        }

        responses = []
        async for chunk in model({"messages": [{"role": "user", "content": "test"}]}):
            responses.append(chunk)

        assert len(responses) == 1
        assert "conversation processing failed" in responses[0]["content"].lower()

    @pytest.mark.asyncio
    @patch("aegis.model.main.setup_ssl")
    @patch("aegis.model.main.setup_authentication", new_callable=AsyncMock)
    @patch("aegis.model.main.process_conversation")
    @patch("aegis.model.main.filter_databases")
    @patch("aegis.model.main.get_database_prompt")
    @patch("aegis.model.main.route_query", new_callable=AsyncMock)
    @patch("aegis.model.main.clarify_query", new_callable=AsyncMock)
    async def test_clarifier_needs_clarification(
        self,
        mock_clarifier,
        mock_route_query,
        mock_get_db_prompt,
        mock_filter_dbs,
        mock_process_conv,
        mock_setup_auth,
        mock_setup_ssl,
        sample_conversation
    ):
        """Test handling when clarifier needs user clarification."""

        # Setup successful initialization
        mock_setup_ssl.return_value = {"success": True, "verify": False}
        mock_setup_auth.return_value = {"method": "api_key"}
        mock_process_conv.return_value = {
            "success": True,
            "messages": sample_conversation["messages"],
            "latest_message": {"role": "user", "content": "Show me data"},
            "message_count": 1
        }
        mock_filter_dbs.return_value = {
            "benchmarking": {"name": "Benchmarking", "content": "Benchmarking data"}
        }
        mock_get_db_prompt.return_value = "Database prompt"

        # Setup router for research
        mock_route_query.return_value = {
            "status": "Success",
            "route": "research_workflow"
        }

        # Setup clarifier to need clarification
        mock_clarifier.return_value = {
            "status": "needs_clarification",
            "clarification": {
                "questions": [
                    "Which bank are you interested in?",
                    "What time period?"
                ]
            }
        }

        responses = []
        async for chunk in model(sample_conversation):
            responses.append(chunk)

        # Should get clarification questions
        content = " ".join(r["content"] for r in responses)
        assert "additional information" in content.lower()
        assert any("bank" in r["content"].lower() for r in responses)

    @pytest.mark.asyncio
    async def test_empty_conversation_handling(self):
        """Test handling of None/empty conversation."""
        responses = []
        async for chunk in model(None):
            responses.append(chunk)

        assert len(responses) >= 1
        # Should handle gracefully and provide appropriate response

    @pytest.mark.asyncio
    async def test_db_names_filtering(self):
        """Test database filtering with db_names parameter."""
        with patch("aegis.model.main.setup_ssl") as mock_ssl:
            with patch("aegis.model.main.setup_authentication", new_callable=AsyncMock) as mock_auth:
                with patch("aegis.model.main.process_conversation") as mock_conv:
                    with patch("aegis.model.main.filter_databases") as mock_filter:

                        mock_ssl.return_value = {"success": True, "verify": False}
                        mock_auth.return_value = {"method": "api_key"}
                        mock_conv.return_value = {
                            "success": True,
                            "messages": [{"role": "user", "content": "test"}],
                            "latest_message": {"role": "user", "content": "test"},
                            "message_count": 1
                        }
                        mock_filter.return_value = ["benchmarking"]

                        # Call with specific db_names
                        responses = []
                        async for chunk in model(
                            {"messages": [{"role": "user", "content": "test"}]},
                            db_names=["benchmarking", "reports"]
                        ):
                            responses.append(chunk)
                            if len(responses) > 5:  # Prevent infinite loops
                                break

                        # Verify filter_databases was called with the specified names
                        mock_filter.assert_called_with(["benchmarking", "reports"])

    @pytest.mark.asyncio
    @patch("aegis.model.main.initialize_monitor")
    @patch("aegis.model.main.setup_ssl")
    async def test_monitoring_integration(self, mock_setup_ssl, mock_init_monitor):
        """Test that monitoring is properly initialized and used."""
        mock_setup_ssl.return_value = {"success": False, "error": "SSL failed"}

        responses = []
        async for chunk in model({"messages": [{"role": "user", "content": "test"}]}):
            responses.append(chunk)

        # Verify monitor was initialized with execution_id
        mock_init_monitor.assert_called_once()
        call_args = mock_init_monitor.call_args[0]
        assert len(call_args[0]) > 0  # execution_id should be non-empty
        assert call_args[1] == "aegis"  # model name