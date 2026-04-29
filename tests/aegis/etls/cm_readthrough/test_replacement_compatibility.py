"""Compatibility checks for the cm_readthrough direct replacement package."""

import pytest

from aegis.etls.cm_readthrough import main


def test_cm_readthrough_replacement_writes_legacy_and_editor_report_types() -> None:
    """The old ETL name should now emit both old and editor report rows."""
    assert main.LEGACY_REPORT_METADATA["report_type"] == "cm_readthrough"
    assert main.INTERACTIVE_REPORT_METADATA["report_type"] == "cm_readthrough_editor"


def test_cm_readthrough_counts_question_and_answer_findings() -> None:
    """CM counters should include both Q&A question and answer report findings."""
    bank_data = {
        "md_blocks": [],
        "qa_conversations": [
            {
                "render_mode": "question",
                "question_sentences": [{"status": "candidate"}],
                "answer_sentences": [{"status": "selected"}],
            }
        ],
    }

    assert main._count_bank_findings(bank_data) == 2
    assert main._count_banks_with_selected_findings({"RY": bank_data}) == 1


@pytest.mark.asyncio
async def test_generate_cm_readthrough_wrapper_preserves_legacy_api(monkeypatch) -> None:
    """Legacy wrapper should pass old arguments through and return the DOCX path."""
    captured = {}

    async def fake_generate_cm_readthrough_editor(**kwargs):
        captured.update(kwargs)
        return main.CMReadthroughEditorResult(
            filepath="/tmp/report.html",
            html_filepath="/tmp/report.html",
            docx_filepath="/tmp/report.docx",
            total_categories=3,
            included_categories=2,
            execution_id="exec-1",
            banks_requested=4,
            banks_included=3,
            banks_with_findings=2,
            skipped_banks=1,
            total_cost=1.25,
            total_tokens=42,
        )

    monkeypatch.setattr(
        main,
        "generate_cm_readthrough_editor",
        fake_generate_cm_readthrough_editor,
    )

    result = await main.generate_cm_readthrough(
        fiscal_year=2025,
        quarter="Q2",
        use_latest=True,
        output_path="/tmp/report.docx",
        output_dir="/tmp",
    )

    assert captured == {
        "bank_name": None,
        "fiscal_year": 2025,
        "quarter": "Q2",
        "use_latest": True,
        "output_path": "/tmp/report.docx",
        "output_dir": "/tmp",
    }
    assert result.filepath == "/tmp/report.docx"
    assert result.html_filepath == "/tmp/report.html"
    assert result.docx_filepath == "/tmp/report.docx"
    assert result.banks_processed == 4
    assert result.banks_with_outlook == 2


@pytest.mark.asyncio
async def test_find_latest_available_quarter_uses_transcript_availability(monkeypatch) -> None:
    """use_latest should resolve through aegis_data_availability transcript rows."""

    class FakeRow:
        fiscal_year = 2025
        quarter = "Q4"

    class FakeResult:
        def fetchone(self):
            return FakeRow()

    class FakeConnection:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def execute(self, query, params):
            assert "aegis_data_availability" in str(query)
            assert "'transcripts' = ANY(database_names)" in str(query)
            assert params == {"bank_id": 7, "min_year": 2025, "min_quarter": 2}
            return FakeResult()

    monkeypatch.setattr(main, "get_connection", lambda: FakeConnection())

    assert await main.find_latest_available_quarter(7, 2025, "Q2", "Test Bank") == (
        2025,
        "Q4",
    )
