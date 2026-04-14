"""Integration tests for the interactive call_summary_editor ETL."""

import os
from unittest.mock import AsyncMock, patch

import pytest

from aegis.etls.call_summary_editor.main import (
    CallSummaryResult,
    CallSummarySystemError,
    CallSummaryUserError,
    generate_call_summary,
)


MOCK_SINGLE_CATEGORY = [
    {
        "transcript_sections": "ALL",
        "report_section": "Results Summary",
        "category_name": "Revenue",
        "category_description": "Revenue and income analysis.",
        "example_1": "",
        "example_2": "",
        "example_3": "",
    }
]

MOCK_XML_RESULT = type(
    "MockXmlResult",
    (),
    {
        "file_path": "Data/2024/Q3/Canadian_Banks/RY-CA_Royal_Bank_of_Canada/RY-CA_Q3_2024_E1_123_5.xml",
        "xml_bytes": b"<transcript />",
    },
)()

MOCK_PARSED_TRANSCRIPT = {
    "title": "Royal Bank of Canada Q3 2024 Earnings Call",
    "participants": {},
    "sections": [],
}

MOCK_MD_RAW_BLOCKS = [
    {
        "id": "RY-CA_MD_1",
        "speaker": "Chief Executive Officer",
        "speaker_title": "",
        "speaker_affiliation": "",
        "paragraphs": ["Revenue grew 8% year-over-year to $14.5 billion."],
    }
]

MOCK_QA_RAW_BLOCKS = []

MOCK_BANK_DATA = {
    "ticker": "RY-CA",
    "company_name": "Royal Bank of Canada",
    "transcript_title": "Royal Bank of Canada Q3 2024 Earnings Call",
    "fiscal_year": 2024,
    "fiscal_quarter": "Q3",
    "md_blocks": [
        {
            "id": "RY-CA_MD_1",
            "speaker": "Chief Executive Officer",
            "speaker_title": "",
            "speaker_affiliation": "",
            "sentences": [
                {
                    "sid": "s1",
                    "text": "Revenue grew 8% year-over-year to $14.5 billion.",
                    "primary": "bucket_0",
                    "scores": {"bucket_0": 8.5},
                    "importance_score": 8.0,
                    "condensed": "Revenue grew 8% to $14.5 billion.",
                    "summary": "Revenue grew 8% year-over-year.",
                    "paraphrase": "Management noted revenue grew 8% year-over-year.",
                    "para_idx": 0,
                }
            ],
        }
    ],
    "qa_conversations": [],
}


def _setup_mocks():
    patches = {}
    patches["auth"] = patch(
        "aegis.etls.call_summary_editor.main.setup_authentication",
        new_callable=AsyncMock,
        return_value={"success": True, "method": "api_key"},
    )
    patches["ssl"] = patch(
        "aegis.etls.call_summary_editor.main.setup_ssl",
        return_value={"verify": False},
    )
    patches["availability"] = patch(
        "aegis.etls.call_summary_editor.main.verify_and_get_availability",
        new_callable=AsyncMock,
        return_value=None,
    )
    patches["nas_conn"] = patch(
        "aegis.etls.call_summary_editor.main.get_nas_connection",
        return_value=object(),
    )
    patches["find_xml"] = patch(
        "aegis.etls.call_summary_editor.main.find_transcript_xml",
        return_value=MOCK_XML_RESULT,
    )
    patches["parse_xml"] = patch(
        "aegis.etls.call_summary_editor.main.parse_transcript_xml",
        return_value=MOCK_PARSED_TRANSCRIPT,
    )
    patches["extract_blocks"] = patch(
        "aegis.etls.call_summary_editor.main.extract_raw_blocks",
        return_value=(MOCK_MD_RAW_BLOCKS, MOCK_QA_RAW_BLOCKS),
    )
    patches["categories"] = patch(
        "aegis.etls.call_summary_editor.main.load_categories_from_xlsx",
        return_value=MOCK_SINGLE_CATEGORY,
    )
    patches["bank_data"] = patch(
        "aegis.etls.call_summary_editor.main.build_interactive_bank_data",
        new_callable=AsyncMock,
        return_value=MOCK_BANK_DATA,
    )
    patches["headlines"] = patch(
        "aegis.etls.call_summary_editor.main.generate_bucket_headlines",
        new_callable=AsyncMock,
        return_value={"bucket_0": "Revenue growth remains strong"},
    )
    patches["save"] = patch(
        "aegis.etls.call_summary_editor.main._save_interactive_report_to_database",
        new_callable=AsyncMock,
        return_value=None,
    )
    return patches


class TestGenerateCallSummaryIntegration:
    """Integration tests for generate_call_summary()."""

    @pytest.mark.asyncio
    async def test_successful_end_to_end(self):
        patches = _setup_mocks()
        managers = {key: patcher.start() for key, patcher in patches.items()}

        try:
            result = await generate_call_summary(bank_name="RY", fiscal_year=2024, quarter="Q3")

            assert isinstance(result, CallSummaryResult)
            assert result.included_categories == 1
            assert result.total_categories == 1
            assert result.filepath.endswith(".html")
            assert os.path.exists(result.filepath)

            managers["save"].assert_called_once()
            managers["find_xml"].assert_called_once()
        finally:
            for patcher in patches.values():
                patcher.stop()
            if "result" in locals() and os.path.exists(result.filepath):
                os.remove(result.filepath)

    @pytest.mark.asyncio
    async def test_auth_failure_returns_error(self):
        patches = _setup_mocks()
        patches["auth"] = patch(
            "aegis.etls.call_summary_editor.main.setup_authentication",
            new_callable=AsyncMock,
            return_value={"success": False, "error": "Invalid credentials"},
        )

        for patcher in patches.values():
            patcher.start()

        try:
            with pytest.raises(CallSummarySystemError, match="Authentication failed"):
                await generate_call_summary(bank_name="RY", fiscal_year=2024, quarter="Q3")
        finally:
            for patcher in patches.values():
                patcher.stop()

    @pytest.mark.asyncio
    async def test_missing_xml_returns_error(self):
        patches = _setup_mocks()
        patches["find_xml"] = patch(
            "aegis.etls.call_summary_editor.main.find_transcript_xml",
            return_value=None,
        )

        for patcher in patches.values():
            patcher.start()

        try:
            with pytest.raises(CallSummaryUserError, match="No transcript XML found"):
                await generate_call_summary(bank_name="RY", fiscal_year=2024, quarter="Q3")
        finally:
            for patcher in patches.values():
                patcher.stop()

    @pytest.mark.asyncio
    async def test_invalid_bank_returns_error(self):
        with pytest.raises(CallSummaryUserError, match="not found"):
            await generate_call_summary(
                bank_name="NONEXISTENT_BANK_XYZ", fiscal_year=2024, quarter="Q3"
            )
