"""
Comprehensive tests for reports/main.py to achieve 100% coverage.
"""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock, call
from datetime import datetime, timezone
import json


class TestReportsAgentCoverage:
    """Complete test coverage for reports_agent function."""

    @pytest.fixture
    def mock_context(self):
        """Mock context for testing."""
        return {
            "execution_id": "test-exec-123",
            "auth_config": {"method": "api_key", "credentials": {"api_key": "test"}},
            "ssl_config": {"verify": False}
        }

    @pytest.fixture
    def mock_bank_combinations(self):
        """Mock bank-period combinations."""
        return [
            {
                "bank_id": 1,
                "bank_name": "Royal Bank of Canada",
                "bank_symbol": "RY",
                "fiscal_year": 2024,
                "quarter": "Q3",
                "query_intent": "Get call summary report"
            }
        ]

    @pytest.fixture
    def mock_report(self):
        """Mock report structure."""
        return {
            "id": 1,
            "bank_id": 1,
            "bank_name": "Royal Bank of Canada",
            "fiscal_year": 2024,
            "quarter": "Q3",
            "report_type": "call_summary",
            "report_name": "Q3 2024 Earnings Call Summary",
            "report_description": "Summary of Q3 2024 earnings call",
            "markdown_content": "# Call Summary\nRevenue increased 5%",
            "s3_document_name": "RY_Q3_2024_summary.docx",
            "s3_pdf_name": "RY_Q3_2024_summary.pdf",
            "metadata": {"generated_at": "2024-09-24"}
        }

    @patch("aegis.model.subagents.reports.main.get_unique_report_types")
    @patch("aegis.model.subagents.reports.main.format_no_data_message")
    @patch("aegis.model.subagents.reports.main.add_monitor_entry")
    @pytest.mark.asyncio
    async def test_no_reports_available(
        self,
        mock_monitor,
        mock_format_no_data,
        mock_unique_types,
        mock_context,
        mock_bank_combinations
    ):
        """Test when no reports are available (lines 90-118)."""
        from aegis.model.subagents.reports.main import reports_agent

        # No report types available
        mock_unique_types.return_value = []
        mock_format_no_data.return_value = "No reports available for the requested banks and periods."

        # Execute
        chunks = []
        async for chunk in reports_agent(
            conversation=[],
            latest_message="Get reports",
            bank_period_combinations=mock_bank_combinations,
            basic_intent="reports",
            full_intent="Get reports for RBC",
            database_id="reports",
            context=mock_context
        ):
            chunks.append(chunk)

        # Assertions
        assert len(chunks) == 1
        assert chunks[0]["type"] == "subagent"
        assert chunks[0]["name"] == "reports"
        assert "No reports available" in chunks[0]["content"]

        # Verify monitoring
        mock_monitor.assert_called_once()
        monitor_call = mock_monitor.call_args
        assert monitor_call.kwargs["status"] == "Success"
        assert monitor_call.kwargs["decision_details"] == "No reports available for requested combinations"

    @patch("aegis.model.subagents.reports.main.get_unique_report_types")
    @patch("aegis.model.subagents.reports.main.retrieve_reports_by_type")
    @patch("aegis.model.subagents.reports.main.add_monitor_entry")
    @pytest.mark.asyncio
    async def test_single_report_type_auto_select(
        self,
        mock_monitor,
        mock_retrieve,
        mock_unique_types,
        mock_context,
        mock_bank_combinations,
        mock_report
    ):
        """Test auto-selection when only one report type exists (lines 130-137)."""
        from aegis.model.subagents.reports.main import reports_agent

        # Single report type available
        mock_unique_types.return_value = [
            {"report_type": "call_summary", "report_name": "Call Summary", "report_description": "Earnings call summary"}
        ]

        # Mock retrieval returns empty (to test that path)
        mock_retrieve.return_value = []

        # Execute
        chunks = []
        async for chunk in reports_agent(
            conversation=[],
            latest_message="Get call summary",
            bank_period_combinations=mock_bank_combinations,
            basic_intent="call summary",
            full_intent="Get call summary for RBC Q3 2024",
            database_id="reports",
            context=mock_context
        ):
            chunks.append(chunk)

        # Assertions
        assert len(chunks) == 1
        assert "No call_summary reports found" in chunks[0]["content"]

        # Verify auto-selection path was taken
        mock_retrieve.assert_called_once_with(
            mock_bank_combinations,
            "call_summary",
            mock_context
        )

    @patch("aegis.model.subagents.reports.main.get_unique_report_types")
    @patch("aegis.model.subagents.reports.main.retrieve_reports_by_type")
    @patch("aegis.model.subagents.reports.main.add_monitor_entry")
    @pytest.mark.asyncio
    async def test_multiple_report_types_default_selection(
        self,
        mock_monitor,
        mock_retrieve,
        mock_unique_types,
        mock_context,
        mock_bank_combinations
    ):
        """Test default selection when multiple report types exist (lines 138-148)."""
        from aegis.model.subagents.reports.main import reports_agent

        # Multiple report types available
        mock_unique_types.return_value = [
            {"report_type": "call_summary", "report_name": "Call Summary", "report_description": "Earnings call"},
            {"report_type": "key_themes", "report_name": "Key Themes", "report_description": "Thematic analysis"}
        ]

        # Mock empty retrieval
        mock_retrieve.return_value = []

        # Execute
        chunks = []
        async for chunk in reports_agent(
            conversation=[],
            latest_message="Get reports",
            bank_period_combinations=mock_bank_combinations,
            basic_intent="reports",
            full_intent="Get reports",
            database_id="reports",
            context=mock_context
        ):
            chunks.append(chunk)

        # Should default to first type (call_summary)
        mock_retrieve.assert_called_once_with(
            mock_bank_combinations,
            "call_summary",  # First in list
            mock_context
        )

    @patch("aegis.model.subagents.reports.main.get_unique_report_types")
    @patch("aegis.model.subagents.reports.main.retrieve_reports_by_type")
    @patch("aegis.model.subagents.reports.main.format_report_content")
    @patch("aegis.model.subagents.reports.main.add_monitor_entry")
    @pytest.mark.asyncio
    async def test_single_report_formatting(
        self,
        mock_monitor,
        mock_format_content,
        mock_retrieve,
        mock_unique_types,
        mock_context,
        mock_bank_combinations,
        mock_report
    ):
        """Test single report formatting path (lines 193-195)."""
        from aegis.model.subagents.reports.main import reports_agent

        # Setup mocks
        mock_unique_types.return_value = [
            {"report_type": "call_summary", "report_name": "Call Summary", "report_description": "Summary"}
        ]
        mock_retrieve.return_value = [mock_report]  # Single report
        mock_format_content.return_value = "Formatted single report content\nLine 2"

        # Execute
        chunks = []
        async for chunk in reports_agent(
            conversation=[],
            latest_message="Get report",
            bank_period_combinations=mock_bank_combinations,
            basic_intent="report",
            full_intent="Get report",
            database_id="reports",
            context=mock_context
        ):
            chunks.append(chunk)

        # Assertions
        assert len(chunks) == 2  # Two lines of content
        assert chunks[0]["content"] == "Formatted single report content\n"
        assert chunks[1]["content"] == "Line 2\n"

        # Verify single report formatting was called
        mock_format_content.assert_called_once_with(mock_report, include_links=True, context=mock_context)

    @patch("aegis.model.subagents.reports.main.get_unique_report_types")
    @patch("aegis.model.subagents.reports.main.retrieve_reports_by_type")
    @patch("aegis.model.subagents.reports.main.format_multiple_reports")
    @patch("aegis.model.subagents.reports.main.add_monitor_entry")
    @pytest.mark.asyncio
    async def test_multiple_reports_formatting(
        self,
        mock_monitor,
        mock_format_multiple,
        mock_retrieve,
        mock_unique_types,
        mock_context,
        mock_bank_combinations,
        mock_report
    ):
        """Test multiple reports formatting path (lines 197-198)."""
        from aegis.model.subagents.reports.main import reports_agent

        # Setup mocks
        mock_unique_types.return_value = [
            {"report_type": "call_summary", "report_name": "Call Summary", "report_description": "Summary"}
        ]

        # Multiple reports
        report2 = {**mock_report, "id": 2, "quarter": "Q2"}
        mock_retrieve.return_value = [mock_report, report2]
        mock_format_multiple.return_value = "Multiple reports formatted\nReport 1\nReport 2"

        # Execute
        chunks = []
        async for chunk in reports_agent(
            conversation=[],
            latest_message="Get reports",
            bank_period_combinations=mock_bank_combinations,
            basic_intent="reports",
            full_intent="Get reports",
            database_id="reports",
            context=mock_context
        ):
            chunks.append(chunk)

        # Assertions
        assert len(chunks) == 3  # Three lines

        # Verify multiple report formatting was called
        mock_format_multiple.assert_called_once_with(
            [mock_report, report2],
            mock_context,
            mock_bank_combinations
        )

    @patch("aegis.model.subagents.reports.main.get_unique_report_types")
    @patch("aegis.model.subagents.reports.main.retrieve_reports_by_type")
    @patch("aegis.model.subagents.reports.main.add_monitor_entry")
    @pytest.mark.asyncio
    async def test_no_reports_found_for_type(
        self,
        mock_monitor,
        mock_retrieve,
        mock_unique_types,
        mock_context,
        mock_bank_combinations
    ):
        """Test when no reports found for selected type (lines 157-183)."""
        from aegis.model.subagents.reports.main import reports_agent

        # Report type exists but no reports found
        mock_unique_types.return_value = [
            {"report_type": "themes", "report_name": "Key Themes", "report_description": "Themes"}
        ]
        mock_retrieve.return_value = []  # No reports found

        # Execute
        chunks = []
        async for chunk in reports_agent(
            conversation=[],
            latest_message="Get themes",
            bank_period_combinations=mock_bank_combinations,
            basic_intent="themes",
            full_intent="Get themes",
            database_id="reports",
            context=mock_context
        ):
            chunks.append(chunk)

        # Assertions
        assert len(chunks) == 1
        assert "No themes reports found" in chunks[0]["content"]

        # Verify monitoring
        mock_monitor.assert_called_once()
        monitor_call = mock_monitor.call_args
        assert monitor_call.kwargs["status"] == "Success"
        assert "No themes reports found" in monitor_call.kwargs["decision_details"]

    @patch("aegis.model.subagents.reports.main.get_unique_report_types")
    @patch("aegis.model.subagents.reports.main.format_error_message")
    @patch("aegis.model.subagents.reports.main.add_monitor_entry")
    @pytest.mark.asyncio
    async def test_error_handling(
        self,
        mock_monitor,
        mock_format_error,
        mock_unique_types,
        mock_context,
        mock_bank_combinations
    ):
        """Test error handling (lines 234-262)."""
        from aegis.model.subagents.reports.main import reports_agent

        # Mock an exception
        mock_unique_types.side_effect = Exception("Database connection error")
        mock_format_error.return_value = "An error occurred: Database connection error"

        # Execute
        chunks = []
        async for chunk in reports_agent(
            conversation=[],
            latest_message="Get reports",
            bank_period_combinations=mock_bank_combinations,
            basic_intent="reports",
            full_intent="Get reports",
            database_id="reports",
            context=mock_context
        ):
            chunks.append(chunk)

        # Assertions
        assert len(chunks) == 1
        assert "An error occurred" in chunks[0]["content"]

        # Verify error monitoring
        mock_monitor.assert_called_once()
        monitor_call = mock_monitor.call_args
        assert monitor_call.kwargs["status"] == "Failure"
        assert monitor_call.kwargs["error_message"] == "Database connection error"

    @patch("aegis.model.subagents.reports.main.get_unique_report_types")
    @patch("aegis.model.subagents.reports.main.retrieve_reports_by_type")
    @patch("aegis.model.subagents.reports.main.format_report_content")
    @patch("aegis.model.subagents.reports.main.add_monitor_entry")
    @pytest.mark.asyncio
    async def test_empty_line_filtering(
        self,
        mock_monitor,
        mock_format_content,
        mock_retrieve,
        mock_unique_types,
        mock_context,
        mock_bank_combinations,
        mock_report
    ):
        """Test that empty lines are filtered out (line 202)."""
        from aegis.model.subagents.reports.main import reports_agent

        # Setup mocks
        mock_unique_types.return_value = [
            {"report_type": "call_summary", "report_name": "Call Summary", "report_description": "Summary"}
        ]
        mock_retrieve.return_value = [mock_report]
        # Content with empty lines
        mock_format_content.return_value = "Line 1\n\nLine 2\n\n\nLine 3"

        # Execute
        chunks = []
        async for chunk in reports_agent(
            conversation=[],
            latest_message="Get report",
            bank_period_combinations=mock_bank_combinations,
            basic_intent="report",
            full_intent="Get report",
            database_id="reports",
            context=mock_context
        ):
            chunks.append(chunk)

        # Should only have 3 chunks (empty lines filtered)
        assert len(chunks) == 3
        assert chunks[0]["content"] == "Line 1\n"
        assert chunks[1]["content"] == "Line 2\n"
        assert chunks[2]["content"] == "Line 3\n"


class TestSelectReportType:
    """Test coverage for select_report_type function."""

    @pytest.fixture
    def mock_context(self):
        return {"execution_id": "test-456"}

    @pytest.fixture
    def mock_report_types(self):
        return [
            {
                "report_type": "call_summary",
                "report_name": "Earnings Call Summary",
                "report_description": "Detailed summary of earnings call"
            },
            {
                "report_type": "key_themes",
                "report_name": "Key Themes",
                "report_description": "Major themes from the call"
            }
        ]

    @patch("aegis.model.subagents.reports.main.complete_with_tools")
    @pytest.mark.asyncio
    async def test_select_report_type_success(
        self,
        mock_complete,
        mock_context,
        mock_report_types
    ):
        """Test successful report type selection (lines 341-355)."""
        from aegis.model.subagents.reports.main import select_report_type

        # Mock successful tool call
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "arguments": json.dumps({
                                "report_type": "key_themes",
                                "reasoning": "User asked for themes"
                            })
                        }
                    }]
                }
            }]
        }

        # Execute
        result = await select_report_type(
            mock_report_types,
            "What are the main themes?",
            mock_context
        )

        # Assertions
        assert result == "key_themes"
        mock_complete.assert_called_once()

    @patch("aegis.model.subagents.reports.main.complete_with_tools")
    @pytest.mark.asyncio
    async def test_select_report_type_no_tool_calls(
        self,
        mock_complete,
        mock_context,
        mock_report_types
    ):
        """Test fallback when no tool calls returned (line 365)."""
        from aegis.model.subagents.reports.main import select_report_type

        # Mock response with no tool calls
        mock_complete.return_value = {
            "choices": [{
                "message": {}  # No tool_calls
            }]
        }

        # Execute
        result = await select_report_type(
            mock_report_types,
            "Get reports",
            mock_context
        )

        # Should fallback to first type
        assert result == "call_summary"

    @patch("aegis.model.subagents.reports.main.complete_with_tools")
    @pytest.mark.asyncio
    async def test_select_report_type_exception(
        self,
        mock_complete,
        mock_context,
        mock_report_types
    ):
        """Test exception handling in selection (lines 357-362)."""
        from aegis.model.subagents.reports.main import select_report_type

        # Mock exception
        mock_complete.side_effect = Exception("LLM error")

        # Execute
        result = await select_report_type(
            mock_report_types,
            "Get reports",
            mock_context
        )

        # Should fallback to first type
        assert result == "call_summary"

    @patch("aegis.model.subagents.reports.main.complete_with_tools")
    @pytest.mark.asyncio
    async def test_select_report_type_invalid_json(
        self,
        mock_complete,
        mock_context,
        mock_report_types
    ):
        """Test handling of invalid JSON in tool call."""
        from aegis.model.subagents.reports.main import select_report_type

        # Mock response with invalid JSON
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "arguments": "invalid json"
                        }
                    }]
                }
            }]
        }

        # Execute
        result = await select_report_type(
            mock_report_types,
            "Get reports",
            mock_context
        )

        # Should fallback to first type due to JSON error
        assert result == "call_summary"