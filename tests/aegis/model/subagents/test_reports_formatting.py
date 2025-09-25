"""
Tests for reports formatting module.

Provides coverage for report formatting functions including list formatting,
single report formatting, multiple report formatting, and error handling.
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from aegis.model.subagents.reports.formatting import (
    format_reports_list,
    format_report_content,
    format_multiple_reports,
    format_no_data_message,
    format_error_message
)


class TestFormatReportsList:
    """Tests for formatting reports list display."""

    @pytest.mark.asyncio
    async def test_format_reports_list_empty(self):
        """Test formatting with no reports."""
        context = {"execution_id": "test-123"}

        result = await format_reports_list([], context)

        assert result == "No reports available for the specified criteria."

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.formatting.get_logger")
    async def test_format_reports_list_with_reports(self, mock_get_logger):
        """Test formatting with available reports."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        reports = [
            {
                "report_name": "Q3 2024 Call Summary",
                "bank_symbol": "RBC",
                "bank_name": "Royal Bank of Canada",
                "quarter": "Q3",
                "fiscal_year": 2024,
                "report_description": "Comprehensive earnings call analysis",
                "generation_date": datetime(2024, 11, 1, 14, 30),
                "s3_document_name": "RBC_Q3_2024_summary.docx",
                "s3_pdf_name": "RBC_Q3_2024_summary.pdf"
            },
            {
                "report_name": "Q2 2024 Call Summary",
                "bank_symbol": "TD",
                "bank_name": "Toronto-Dominion Bank",
                "quarter": "Q2",
                "fiscal_year": 2024,
                "report_description": "Q2 earnings analysis",
                "generation_date": datetime(2024, 8, 15, 9, 45)
                # No S3 names - testing without download links
            }
        ]

        context = {"execution_id": "test-123"}

        result = await format_reports_list(reports, context)

        # Verify header
        assert "**Available Reports:**" in result

        # Verify first report with download links
        assert "**Q3 2024 Call Summary** - RBC Q3 2024" in result
        assert "Comprehensive earnings call analysis" in result
        assert "Generated: 2024-11-01 14:30" in result
        assert "[Download Word Document](RBC_Q3_2024_summary.docx)" in result
        assert "[Download PDF](RBC_Q3_2024_summary.pdf)" in result

        # Verify second report without download links
        assert "**Q2 2024 Call Summary** - TD Q2 2024" in result
        assert "Q2 earnings analysis" in result
        assert "Generated: 2024-08-15 09:45" in result

        # Should not have download links for second report
        assert "TD_Q2_2024" not in result


class TestFormatReportContent:
    """Tests for formatting single report content."""

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.formatting.get_logger")
    async def test_format_report_content_complete(self, mock_get_logger):
        """Test formatting complete report with all components."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        report = {
            "report_name": "Q3 2024 Call Summary",
            "bank_symbol": "RBC",
            "quarter": "Q3",
            "fiscal_year": 2024,
            "generation_date": datetime(2024, 11, 1, 14, 30),
            "markdown_content": "# Key Findings\n\nRevenue increased 5% year-over-year.",
            "s3_document_name": "RBC_Q3_2024.docx",
            "s3_pdf_name": "RBC_Q3_2024.pdf",
            "generated_by": "ETL System v2.1",
            "date_last_modified": datetime(2024, 11, 2, 10, 15)
        }

        context = {"execution_id": "test-123"}

        result = await format_report_content(report, include_links=True, context=context)

        # Verify header
        assert "# Q3 2024 Call Summary - RBC Q3 2024" in result

        # Verify generation metadata
        assert "Generated on November 01, 2024 at 02:30 PM" in result

        # Verify S3 link markers (for main agent processing)
        assert "{{S3_LINK:download:docx:RBC_Q3_2024.docx:Download Q3 2024 Call Summary Document (RBC Q3 2024)}}" in result
        assert "{{S3_LINK:open:pdf:RBC_Q3_2024.pdf:Open Q3 2024 Call Summary PDF (RBC Q3 2024)}}" in result

        # Verify content
        assert "# Key Findings" in result
        assert "Revenue increased 5% year-over-year." in result

        # Verify footer
        assert "Source: ETL System v2.1" in result
        assert "Last Modified: 2024-11-02 10:15" in result

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.formatting.get_logger")
    async def test_format_report_content_no_links(self, mock_get_logger):
        """Test formatting report without including links."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        report = {
            "report_name": "Test Report",
            "bank_symbol": "TD",
            "quarter": "Q2",
            "fiscal_year": 2024,
            "generation_date": datetime(2024, 8, 1, 12, 0),
            "markdown_content": "Report content here.",
            "s3_document_name": "TD_Q2_2024.docx",
            "date_last_modified": datetime(2024, 8, 1, 12, 0),
            "generated_by": "System"
        }

        context = {"execution_id": "test-123"}

        result = await format_report_content(report, include_links=False, context=context)

        # Should not include S3 link markers
        assert "{{S3_LINK:" not in result

        # Should still include content
        assert "Report content here." in result

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.formatting.get_logger")
    async def test_format_report_content_no_markdown(self, mock_get_logger):
        """Test formatting report with no markdown content but with links."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        report = {
            "report_name": "Document Only Report",
            "bank_symbol": "BMO",
            "quarter": "Q1",
            "fiscal_year": 2024,
            "generation_date": datetime(2024, 5, 1, 10, 0),
            "markdown_content": "",  # Empty content
            "s3_document_name": "BMO_Q1_2024.docx",
            "date_last_modified": datetime(2024, 5, 1, 10, 0),
            "generated_by": "System"
        }

        context = {"execution_id": "test-123"}

        result = await format_report_content(report, include_links=True, context=context)

        # Should include helpful message about document
        assert "Report content is available in the document above" in result
        assert "Please download to view the full report" in result

        # Should not show "no content" message since we have links
        assert "*No content available for this report.*" not in result

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.formatting.get_logger")
    async def test_format_report_content_no_content_no_links(self, mock_get_logger):
        """Test formatting report with no content and no links."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        report = {
            "report_name": "Empty Report",
            "bank_symbol": "CIBC",
            "quarter": "Q4",
            "fiscal_year": 2023,
            "generation_date": datetime(2024, 2, 1, 15, 30),
            "markdown_content": None,  # No content
            "date_last_modified": datetime(2024, 2, 1, 15, 30)
        }

        context = {"execution_id": "test-123"}

        result = await format_report_content(report, include_links=True, context=context)

        # Should show no content message
        assert "*No content available for this report.*" in result

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.formatting.get_logger")
    async def test_format_report_content_no_context(self, mock_get_logger):
        """Test formatting report without context."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        report = {
            "report_name": "Test Report",
            "bank_symbol": "RBC",
            "quarter": "Q3",
            "fiscal_year": 2024,
            "generation_date": datetime(2024, 11, 1, 14, 30),
            "markdown_content": "Test content",
            "date_last_modified": datetime(2024, 11, 1, 14, 30)
        }

        result = await format_report_content(report, context=None)

        # Should still format correctly without context
        assert "# Test Report - RBC Q3 2024" in result
        assert "Test content" in result


class TestFormatMultipleReports:
    """Tests for formatting multiple reports."""

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.formatting.get_logger")
    async def test_format_multiple_reports_empty(self, mock_get_logger):
        """Test formatting with no reports."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        context = {"execution_id": "test-123"}

        result = await format_multiple_reports([], context)

        assert result == "No reports found for the specified banks and periods."

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.formatting.get_logger")
    async def test_format_multiple_reports_with_data(self, mock_get_logger):
        """Test formatting multiple reports with grouping by bank."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        reports = [
            {
                "report_name": "Call Summary",
                "bank_symbol": "RBC",
                "bank_name": "Royal Bank",
                "quarter": "Q3",
                "fiscal_year": 2024,
                "markdown_content": "RBC Q3 content here",
                "s3_document_name": "RBC_Q3_2024.docx"
            },
            {
                "report_name": "Call Summary",
                "bank_symbol": "RBC",
                "bank_name": "Royal Bank",
                "quarter": "Q2",
                "fiscal_year": 2024,
                "markdown_content": "RBC Q2 content here",
                "s3_pdf_name": "RBC_Q2_2024.pdf"
            },
            {
                "report_name": "Call Summary",
                "bank_symbol": "TD",
                "bank_name": "Toronto-Dominion",
                "quarter": "Q3",
                "fiscal_year": 2024,
                "markdown_content": "Long content " + "x" * 5000,  # Long content to test truncation
                "s3_document_name": "TD_Q3_2024.docx"
            }
        ]

        context = {"execution_id": "test-123"}

        result = await format_multiple_reports(reports, context, None)

        # Verify bank grouping headers
        assert "## Royal Bank (RBC)" in result
        assert "## Toronto-Dominion (TD)" in result

        # Verify period headers
        assert "### Q3 2024 - Call Summary" in result
        assert "### Q2 2024 - Call Summary" in result

        # Verify S3 link markers
        assert "{{S3_LINK:download:docx:RBC_Q3_2024.docx:" in result
        assert "{{S3_LINK:open:pdf:RBC_Q2_2024.pdf:" in result

        # Verify content (including truncation for long content)
        assert "RBC Q3 content here" in result
        assert "RBC Q2 content here" in result
        assert "Long content x" in result
        assert "..." in result  # Truncation indicator

        # Verify summary footer
        assert "*Total reports available: 3*" in result

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.formatting.get_logger")
    async def test_format_multiple_reports_with_missing_combinations(self, mock_get_logger):
        """Test formatting when some requested combinations are missing."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        reports = [
            {
                "report_name": "Call Summary",
                "bank_symbol": "RBC",
                "bank_name": "Royal Bank",
                "quarter": "Q3",
                "fiscal_year": 2024,
                "markdown_content": "Available report"
            }
        ]

        # Requested more combinations than available
        requested_combinations = [
            {"bank_symbol": "RBC", "bank_name": "Royal Bank", "quarter": "Q3", "fiscal_year": 2024},  # Available
            {"bank_symbol": "TD", "bank_name": "Toronto-Dominion", "quarter": "Q3", "fiscal_year": 2024},  # Missing
            {"bank_symbol": "BMO", "bank_name": "Bank of Montreal", "quarter": "Q2", "fiscal_year": 2024}  # Missing
        ]

        context = {"execution_id": "test-123"}

        result = await format_multiple_reports(reports, context, requested_combinations)

        # Verify missing combinations are shown first
        assert "## Toronto-Dominion (TD)" in result
        assert "*No call summary report available for Toronto-Dominion Q3 2024.*" in result
        assert "## Bank of Montreal (BMO)" in result
        assert "*No call summary report available for Bank of Montreal Q2 2024.*" in result

        # Verify available report is shown
        assert "## Royal Bank (RBC)" in result
        assert "Available report" in result

        # Verify summary shows correct counts
        assert "*Total reports available: 1*" in result
        assert "*Reports unavailable: 2*" in result

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.formatting.get_logger")
    async def test_format_multiple_reports_no_content_with_links(self, mock_get_logger):
        """Test formatting reports with no markdown content but with links."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        reports = [
            {
                "report_name": "Document Only",
                "bank_symbol": "CIBC",
                "bank_name": "CIBC",
                "quarter": "Q1",
                "fiscal_year": 2024,
                "markdown_content": None,  # No content
                "s3_document_name": "CIBC_Q1_2024.docx"
            }
        ]

        context = {"execution_id": "test-123"}

        result = await format_multiple_reports(reports, context)

        # Should show helpful message about documents
        assert "Report content is available in the documents above" in result
        assert "Please download to view the full report" in result


class TestFormatNoDataMessage:
    """Tests for formatting no data messages."""

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.formatting.get_logger")
    async def test_format_no_data_message_empty_combinations(self, mock_get_logger):
        """Test no data message with empty combinations."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        context = {"execution_id": "test-123"}

        result = await format_no_data_message([], context)

        assert result == "No bank and period combinations were specified."

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.formatting.get_logger")
    async def test_format_no_data_message_with_combinations(self, mock_get_logger):
        """Test no data message with requested combinations."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        combinations = [
            {
                "bank_name": "Royal Bank of Canada",
                "bank_symbol": "RBC",
                "quarter": "Q3",
                "fiscal_year": 2024
            },
            {
                "bank_name": "Toronto-Dominion Bank",
                "bank_symbol": "TD",
                "quarter": "Q2",
                "fiscal_year": 2024
            }
        ]

        context = {"execution_id": "test-123"}

        result = await format_no_data_message(combinations, context)

        # Verify message content
        assert "No pre-generated reports are available for:" in result
        assert "• Royal Bank of Canada (RBC) - Q3 2024" in result
        assert "• Toronto-Dominion Bank (TD) - Q2 2024" in result
        assert "Reports are generated periodically through ETL processes" in result

        # Verify logging occurred
        mock_logger.info.assert_called_once()


class TestFormatErrorMessage:
    """Tests for formatting error messages."""

    @pytest.mark.asyncio
    @patch("aegis.model.subagents.reports.formatting.get_logger")
    async def test_format_error_message(self, mock_get_logger):
        """Test error message formatting."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        error_text = "Database connection failed"
        context = {"execution_id": "test-123"}

        result = await format_error_message(error_text, context)

        # Verify error message format
        assert "⚠️ An error occurred while retrieving reports: Database connection failed" in result
        assert "Please try again or contact support" in result

        # Verify error was logged
        mock_logger.error.assert_called_once()
        error_call = mock_logger.error.call_args[0][0]
        assert "subagent.reports.formatting_error" in error_call