"""Compatibility checks for the call_summary direct replacement package."""

from aegis.etls.call_summary.main import INTERACTIVE_REPORT_METADATA, LEGACY_REPORT_METADATA


def test_call_summary_replacement_writes_legacy_and_editor_report_types() -> None:
    """The old ETL name should now emit both old and editor report rows."""
    assert LEGACY_REPORT_METADATA["report_type"] == "call_summary"
    assert INTERACTIVE_REPORT_METADATA["report_type"] == "call_summary_editor"
