"""
Tests for reports subagent main.py to achieve coverage.
"""

import pytest
import json
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timezone


class TestReportsAgent:
    """Tests for reports_agent function."""

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

    @patch("aegis.model.subagents.reports.main.add_monitor_entry")
    @patch("aegis.model.subagents.reports.main.load_subagent_prompt")
    @patch("aegis.model.subagents.reports.main.complete_with_tools")
    @patch("aegis.model.subagents.reports.main.retrieve_reports_by_type", new_callable=AsyncMock)
    @patch("aegis.model.subagents.reports.main.get_unique_report_types", new_callable=AsyncMock)
    @patch("aegis.model.subagents.reports.main.get_available_reports", new_callable=AsyncMock)
    @pytest.mark.asyncio
    async def test_reports_agent_success_with_data(
        self,
        mock_available,
        mock_unique_types,
        mock_retrieve,
        mock_complete,
        mock_load_prompt,
        mock_monitor,
        mock_context,
        mock_bank_combinations
    ):
        """Test successful report retrieval with data."""
        from aegis.model.subagents.reports.main import reports_agent

        # Setup async mocks for database functions
        mock_available.return_value = [
            {"report_type": "call_summary", "bank_id": 1, "fiscal_year": 2024, "quarter": "Q3"}
        ]

        mock_unique_types.return_value = [
            {"report_type": "call_summary", "report_name": "Call Summary", "report_description": "Earnings call summary"}
        ]

        # Mock LLM response for report type selection
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "name": "select_report",
                            "arguments": json.dumps({
                                "report_type": "call_summary",
                                "reasoning": "User wants call summary"
                            })
                        }
                    }]
                }
            }],
            "usage": {"total_tokens": 100, "prompt_tokens": 80, "completion_tokens": 20}
        }

        # Mock retrieved reports with all required fields
        mock_retrieve.return_value = [
            {
                "report_id": 1,
                "bank_id": 1,
                "bank_name": "Royal Bank of Canada",
                "bank_symbol": "RY",
                "fiscal_year": 2024,
                "quarter": "Q3",
                "report_type": "call_summary",
                "report_name": "Call Summary",
                "markdown_content": "# Call Summary\nRevenue increased 5%",
                "generation_date": datetime.now(timezone.utc),
                "date_last_modified": datetime.now(timezone.utc),
                "generated_by": "ETL Process",
                "s3_document_name": None,
                "s3_pdf_name": None,
                "metadata": {"generated_at": "2024-09-24"}
            }
        ]

        # Mock prompt
        mock_load_prompt.return_value = "You are the reports subagent"

        # Execute
        chunks = []
        async for chunk in reports_agent(
            conversation=[],
            latest_message="Get me the call summary",
            bank_period_combinations=mock_bank_combinations,
            basic_intent="call summary",
            full_intent="Get call summary for RBC Q3 2024",
            database_id="reports",
            context=mock_context
        ):
            chunks.append(chunk)

        # Assertions
        assert len(chunks) > 0
        assert all(chunk["type"] == "subagent" for chunk in chunks)
        assert all(chunk["name"] == "reports" for chunk in chunks)

        # Check content was yielded
        full_content = "".join(chunk["content"] for chunk in chunks)
        assert "Call Summary" in full_content or "Revenue" in full_content

        # Verify monitoring was called
        mock_monitor.assert_called()

    @patch("aegis.model.subagents.reports.main.add_monitor_entry")
    @patch("aegis.model.subagents.reports.main.load_subagent_prompt")
    @patch("aegis.model.subagents.reports.main.get_unique_report_types", new_callable=AsyncMock)
    @pytest.mark.asyncio
    async def test_reports_agent_no_available_reports(
        self,
        mock_unique_types,
        mock_load_prompt,
        mock_monitor,
        mock_context,
        mock_bank_combinations
    ):
        """Test when no reports are available."""
        from aegis.model.subagents.reports.main import reports_agent

        # Mock no unique report types (empty list)
        mock_unique_types.return_value = []

        # Mock prompt
        mock_load_prompt.return_value = "You are the reports subagent"

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
        assert len(chunks) > 0
        full_content = "".join(chunk["content"] for chunk in chunks)
        assert "No pre-generated reports are available" in full_content or "no reports" in full_content.lower()

        # Verify monitoring was called
        mock_monitor.assert_called()

    @patch("aegis.model.subagents.reports.main.add_monitor_entry")
    @patch("aegis.model.subagents.reports.main.load_subagent_prompt")
    @patch("aegis.model.subagents.reports.main.retrieve_reports_by_type", new_callable=AsyncMock)
    @patch("aegis.model.subagents.reports.main.get_unique_report_types", new_callable=AsyncMock)
    @patch("aegis.model.subagents.reports.main.complete_with_tools")
    @pytest.mark.asyncio
    async def test_reports_agent_no_reports_found(
        self,
        mock_complete,
        mock_unique_types,
        mock_retrieve,
        mock_load_prompt,
        mock_monitor,
        mock_context,
        mock_bank_combinations
    ):
        """Test when reports exist but none match the query."""
        from aegis.model.subagents.reports.main import reports_agent

        # Setup async mocks
        mock_unique_types.return_value = [
            {"report_type": "call_summary", "report_name": "Call Summary", "report_description": "Earnings call summary"}
        ]

        # Mock LLM response for selection
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "name": "select_report",
                            "arguments": json.dumps({
                                "report_type": "call_summary",
                                "reasoning": "User query"
                            })
                        }
                    }]
                }
            }],
            "usage": {"total_tokens": 100, "prompt_tokens": 80, "completion_tokens": 20}
        }

        # Mock no retrieved reports (empty result)
        mock_retrieve.return_value = []

        # Mock prompt
        mock_load_prompt.return_value = "You are the reports subagent"

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
        assert len(chunks) > 0
        full_content = "".join(chunk["content"] for chunk in chunks)
        assert "No" in full_content and "reports found" in full_content

        # Verify monitoring was called
        mock_monitor.assert_called()

    @patch("aegis.model.subagents.reports.main.add_monitor_entry")
    @patch("aegis.model.subagents.reports.main.load_subagent_prompt")
    @patch("aegis.model.subagents.reports.main.get_unique_report_types", new_callable=AsyncMock)
    @pytest.mark.asyncio
    async def test_reports_agent_error_handling(
        self,
        mock_unique_types,
        mock_load_prompt,
        mock_monitor,
        mock_context,
        mock_bank_combinations
    ):
        """Test error handling in reports agent."""
        from aegis.model.subagents.reports.main import reports_agent

        # Mock exception
        mock_unique_types.side_effect = Exception("Database connection error")

        # Mock prompt
        mock_load_prompt.return_value = "You are the reports subagent"

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
        assert len(chunks) > 0
        full_content = "".join(chunk["content"] for chunk in chunks)
        assert "Error" in full_content or "error" in full_content

        # Verify error monitoring was called
        mock_monitor.assert_called()
        call_args = mock_monitor.call_args
        assert call_args.kwargs.get("status") == "Failure"

    @patch("aegis.model.subagents.reports.main.add_monitor_entry")
    @patch("aegis.model.subagents.reports.main.load_subagent_prompt")
    @patch("aegis.model.subagents.reports.main.complete_with_tools")
    @patch("aegis.model.subagents.reports.main.retrieve_reports_by_type", new_callable=AsyncMock)
    @patch("aegis.model.subagents.reports.main.get_unique_report_types", new_callable=AsyncMock)
    @patch("aegis.model.subagents.reports.main.get_available_reports", new_callable=AsyncMock)
    @pytest.mark.asyncio
    async def test_reports_agent_multiple_reports(
        self,
        mock_available,
        mock_unique_types,
        mock_retrieve,
        mock_complete,
        mock_load_prompt,
        mock_monitor,
        mock_context
    ):
        """Test handling multiple reports."""
        from aegis.model.subagents.reports.main import reports_agent

        # Multiple bank combinations
        bank_combinations = [
            {
                "bank_id": 1,
                "bank_name": "RBC",
                "bank_symbol": "RY",
                "fiscal_year": 2024,
                "quarter": "Q2"
            },
            {
                "bank_id": 1,
                "bank_name": "RBC",
                "bank_symbol": "RY",
                "fiscal_year": 2024,
                "quarter": "Q3"
            }
        ]

        # Setup async mocks
        mock_available.return_value = [
            {"report_type": "themes", "bank_id": 1, "fiscal_year": 2024, "quarter": "Q2"},
            {"report_type": "themes", "bank_id": 1, "fiscal_year": 2024, "quarter": "Q3"}
        ]

        mock_unique_types.return_value = [
            {"report_type": "themes", "report_name": "Key Themes", "report_description": "Key themes analysis"}
        ]

        # Mock LLM response
        mock_complete.return_value = {
            "choices": [{
                "message": {
                    "tool_calls": [{
                        "function": {
                            "name": "select_report",
                            "arguments": json.dumps({
                                "report_type": "themes",
                                "reasoning": "User wants themes"
                            })
                        }
                    }]
                }
            }],
            "usage": {"total_tokens": 100, "prompt_tokens": 80, "completion_tokens": 20}
        }

        # Mock multiple retrieved reports with all required fields
        mock_retrieve.return_value = [
            {
                "report_id": 1,
                "bank_id": 1,
                "bank_name": "RBC",
                "bank_symbol": "RY",
                "fiscal_year": 2024,
                "quarter": "Q2",
                "report_type": "themes",
                "report_name": "Key Themes",
                "markdown_content": "Q2 Themes: Growth",
                "generation_date": datetime.now(timezone.utc),
                "date_last_modified": datetime.now(timezone.utc),
                "generated_by": "ETL Process",
                "metadata": {}
            },
            {
                "report_id": 2,
                "bank_id": 1,
                "bank_name": "RBC",
                "bank_symbol": "RY",
                "fiscal_year": 2024,
                "quarter": "Q3",
                "report_type": "themes",
                "report_name": "Key Themes",
                "markdown_content": "Q3 Themes: Innovation",
                "generation_date": datetime.now(timezone.utc),
                "date_last_modified": datetime.now(timezone.utc),
                "generated_by": "ETL Process",
                "metadata": {}
            }
        ]

        # Mock prompt
        mock_load_prompt.return_value = "You are the reports subagent"

        # Execute
        chunks = []
        async for chunk in reports_agent(
            conversation=[],
            latest_message="Get themes",
            bank_period_combinations=bank_combinations,
            basic_intent="themes",
            full_intent="Get themes for RBC",
            database_id="reports",
            context=mock_context
        ):
            chunks.append(chunk)

        # Assertions
        assert len(chunks) > 0
        full_content = "".join(chunk["content"] for chunk in chunks)

        # Should have both quarters
        assert "Q2" in full_content
        assert "Q3" in full_content

        # Verify monitoring was called
        mock_monitor.assert_called()