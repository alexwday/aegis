"""
Test coverage for aegis.etls document converter utility functions.

These tests target simple utility functions in the ETL document converters
to expand coverage of the ETL modules.
"""

import pytest
from aegis.etls.call_summary.document_converter import (
    get_standard_report_metadata,
    structured_data_to_markdown
)
from aegis.etls.key_themes.document_converter import get_standard_report_metadata as get_key_themes_metadata


class TestCallSummaryDocumentConverter:
    """Test call summary document converter functions."""

    def test_get_standard_report_metadata(self):
        """Test get_standard_report_metadata function."""
        metadata = get_standard_report_metadata()

        # Should return a dictionary with expected keys
        assert isinstance(metadata, dict)
        assert "report_name" in metadata
        assert "report_description" in metadata
        assert "report_type" in metadata

        # Check expected values
        assert metadata["report_name"] == "Earnings Call Summary"
        assert metadata["report_type"] == "call_summary"
        assert isinstance(metadata["report_description"], str)
        assert len(metadata["report_description"]) > 50  # Should be a substantial description


class TestKeyThemesDocumentConverter:
    """Test key themes document converter functions."""

    def test_get_standard_report_metadata(self):
        """Test get_standard_report_metadata function for key themes."""
        metadata = get_key_themes_metadata(
            bank_name="Royal Bank of Canada",
            fiscal_year=2024,
            quarter="Q3",
            report_type="key_themes",
            theme_count=5
        )

        # Should return a dictionary with expected keys
        assert isinstance(metadata, dict)
        assert "report_type" in metadata
        assert "bank_name" in metadata
        assert "fiscal_year" in metadata
        assert "quarter" in metadata
        assert "generated_at" in metadata
        assert "theme_count" in metadata
        assert "version" in metadata

        # Check expected values
        assert metadata["report_type"] == "key_themes"
        assert metadata["bank_name"] == "Royal Bank of Canada"
        assert metadata["fiscal_year"] == 2024
        assert metadata["quarter"] == "Q3"
        assert metadata["theme_count"] == 5
        assert metadata["version"] == "1.0"

        # generated_at should be a timestamp string
        assert isinstance(metadata["generated_at"], str)
        assert "T" in metadata["generated_at"]  # ISO format should have T