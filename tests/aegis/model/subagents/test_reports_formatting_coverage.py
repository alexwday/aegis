"""
Test to achieve 100% coverage for reports/formatting.py.
"""

import pytest
from unittest.mock import patch, MagicMock


class TestReportsFormattingCoverage:
    """Test to cover missing line 211 in formatting.py."""

    @pytest.mark.asyncio
    async def test_format_multiple_reports_no_content_no_links(self):
        """Test formatting when report has no content and no S3 links (line 211)."""
        from aegis.model.subagents.reports.formatting import format_multiple_reports

        # Mock context
        context = {"execution_id": "test-123"}

        # Report with no markdown_content and no S3 links
        reports = [
            {
                "id": 1,
                "bank_id": 1,
                "bank_name": "Test Bank",
                "bank_symbol": "TB",
                "fiscal_year": 2024,
                "quarter": "Q1",
                "report_type": "summary",
                "report_name": "Q1 Summary",
                "report_description": "Test summary",
                # No markdown_content
                # No s3_document_name
                # No s3_pdf_name
            }
        ]

        # Requested combinations
        requested_combinations = [
            {
                "bank_id": 1,
                "bank_name": "Test Bank",
                "bank_symbol": "TB",
                "fiscal_year": 2024,
                "quarter": "Q1"
            }
        ]

        # Execute
        result = await format_multiple_reports(reports, context, requested_combinations)

        # Should contain the "No content available" message
        assert "*No content available for this report.*" in result
        assert "Test Bank" in result
        assert "Q1 2024" in result