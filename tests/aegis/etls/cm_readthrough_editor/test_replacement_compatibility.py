"""Compatibility checks for the cm_readthrough direct replacement package."""

import pytest

from aegis.etls.cm_readthrough_editor import main


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
async def test_use_latest_preflight_queries_nas_without_availability_check(monkeypatch) -> None:
    """use_latest remains accepted but must not gate CM readthrough on availability rows."""
    requested_period = {}

    class FakeNasConnection:
        def close(self) -> None:
            pass

    class FakeXmlResult:
        file_path = "/nas/2026/Q1/test.xml"
        xml_bytes = b"<xml />"

    def fail_get_connection():
        raise AssertionError("cm_readthrough should not query aegis_data_availability")

    def fake_find_transcript_xml(conn, bank_info, fiscal_year, quarter):
        del conn, bank_info
        requested_period["fiscal_year"] = fiscal_year
        requested_period["quarter"] = quarter
        return FakeXmlResult()

    monkeypatch.setattr(
        main,
        "_resolve_requested_banks",
        lambda bank_name: [
            {
                "bank_id": 7,
                "bank_name": "Test Bank",
                "bank_symbol": "TB",
                "bank_type": "US_Banks",
                "path_safe_name": "TB_Test_Bank",
            }
        ],
    )
    monkeypatch.setattr(main, "load_categories_from_xlsx", lambda: [{"category": "x"}])
    monkeypatch.setattr(main, "get_nas_connection", lambda: FakeNasConnection())
    monkeypatch.setattr(main, "get_connection", fail_get_connection)
    monkeypatch.setattr(main, "find_transcript_xml", fake_find_transcript_xml)
    monkeypatch.setattr(main, "parse_transcript_xml", lambda xml_bytes: {"xml": xml_bytes})
    monkeypatch.setattr(main, "extract_raw_blocks", lambda transcript, ticker: ([{}], []))

    result = await main.preflight_cm_readthrough_editor(
        bank_name=None,
        fiscal_year=2026,
        quarter="Q1",
        use_latest=True,
    )

    assert result["ok_banks"] == 1
    assert result["statuses"][0]["source_fiscal_year"] == 2026
    assert result["statuses"][0]["source_quarter"] == "Q1"
    assert requested_period == {"fiscal_year": 2026, "quarter": "Q1"}
