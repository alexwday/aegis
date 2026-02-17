"""Integration tests for generate_cm_readthrough orchestration."""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from aegis.etls.cm_readthrough.main import (
    CMReadthroughResult,
    CMReadthroughSystemError,
    CMReadthroughUserError,
    generate_cm_readthrough,
)


def _minimal_results():
    return {
        "metadata": {
            "banks_processed": 2,
            "banks_with_outlook": 1,
            "banks_with_section2": 1,
            "banks_with_section3": 1,
            "subtitle_section1": "Outlook: Test",
            "subtitle_section2": "Conference calls: Test",
            "subtitle_section3": "Conference calls: Test",
        },
        "outlook": {
            "Bank A": {
                "bank_symbol": "A",
                "statements": [{"category": "C", "statement": "S"}],
            }
        },
        "section2_questions": {
            "Bank A": {
                "bank_symbol": "A",
                "questions": [{"category": "Q", "verbatim_question": "Question"}],
            }
        },
        "section3_questions": {
            "Bank A": {
                "bank_symbol": "A",
                "questions": [{"category": "Q", "verbatim_question": "Question"}],
            }
        },
    }


@pytest.mark.asyncio
async def test_generate_cm_readthrough_success(tmp_path):
    """Happy path returns CMReadthroughResult with filepath and metrics."""
    out_file = tmp_path / "cm_report.docx"

    def fake_create_doc(_results, output_path):
        Path(output_path).write_bytes(b"docx-bytes")

    with (
        patch(
            "aegis.etls.cm_readthrough.main.load_outlook_categories",
            return_value=[{"category_name": "A"}],
        ),
        patch(
            "aegis.etls.cm_readthrough.main.load_qa_market_volatility_regulatory_categories",
            return_value=[{"category_name": "B"}],
        ),
        patch(
            "aegis.etls.cm_readthrough.main.load_qa_pipelines_activity_categories",
            return_value=[{"category_name": "C"}],
        ),
        patch("aegis.etls.cm_readthrough.main.setup_ssl", return_value={"verify": False}),
        patch(
            "aegis.etls.cm_readthrough.main.setup_authentication",
            new_callable=AsyncMock,
            return_value={"success": True, "method": "api_key"},
        ),
        patch("aegis.etls.cm_readthrough.main._load_prompt_bundle", return_value={}),
        patch(
            "aegis.etls.cm_readthrough.main.process_all_banks_parallel",
            new_callable=AsyncMock,
            return_value=_minimal_results(),
        ),
        patch(
            "aegis.etls.cm_readthrough.main.create_combined_document", side_effect=fake_create_doc
        ),
        patch("aegis.etls.cm_readthrough.main.save_to_database", new_callable=AsyncMock),
    ):
        result = await generate_cm_readthrough(2024, "Q3", output_path=str(out_file))

    assert isinstance(result, CMReadthroughResult)
    assert result.filepath == str(out_file)
    assert result.banks_processed == 2
    assert result.banks_with_outlook == 1
    assert result.banks_with_section2 == 1
    assert result.banks_with_section3 == 1


@pytest.mark.asyncio
async def test_generate_cm_readthrough_auth_failure_raises_system_error():
    """Authentication failure should raise CMReadthroughSystemError."""
    with (
        patch(
            "aegis.etls.cm_readthrough.main.load_outlook_categories",
            return_value=[{"category_name": "A"}],
        ),
        patch(
            "aegis.etls.cm_readthrough.main.load_qa_market_volatility_regulatory_categories",
            return_value=[{"category_name": "B"}],
        ),
        patch(
            "aegis.etls.cm_readthrough.main.load_qa_pipelines_activity_categories",
            return_value=[{"category_name": "C"}],
        ),
        patch("aegis.etls.cm_readthrough.main.setup_ssl", return_value={"verify": False}),
        patch(
            "aegis.etls.cm_readthrough.main.setup_authentication",
            new_callable=AsyncMock,
            return_value={"success": False, "error": "bad token"},
        ),
    ):
        with pytest.raises(CMReadthroughSystemError, match="Authentication failed"):
            await generate_cm_readthrough(2024, "Q3")


@pytest.mark.asyncio
async def test_generate_cm_readthrough_no_results_raises_user_error():
    """Empty extraction output should raise CMReadthroughUserError."""
    with (
        patch(
            "aegis.etls.cm_readthrough.main.load_outlook_categories",
            return_value=[{"category_name": "A"}],
        ),
        patch(
            "aegis.etls.cm_readthrough.main.load_qa_market_volatility_regulatory_categories",
            return_value=[{"category_name": "B"}],
        ),
        patch(
            "aegis.etls.cm_readthrough.main.load_qa_pipelines_activity_categories",
            return_value=[{"category_name": "C"}],
        ),
        patch("aegis.etls.cm_readthrough.main.setup_ssl", return_value={"verify": False}),
        patch(
            "aegis.etls.cm_readthrough.main.setup_authentication",
            new_callable=AsyncMock,
            return_value={"success": True, "method": "api_key"},
        ),
        patch("aegis.etls.cm_readthrough.main._load_prompt_bundle", return_value={}),
        patch(
            "aegis.etls.cm_readthrough.main.process_all_banks_parallel",
            new_callable=AsyncMock,
            return_value={"outlook": {}, "section2_questions": {}, "section3_questions": {}},
        ),
    ):
        with pytest.raises(CMReadthroughUserError, match="No results generated"):
            await generate_cm_readthrough(2024, "Q3")


@pytest.mark.asyncio
async def test_generate_cm_readthrough_unexpected_error_raises_system_error():
    """Unexpected low-level errors should surface as CMReadthroughSystemError."""
    with patch(
        "aegis.etls.cm_readthrough.main.load_outlook_categories",
        side_effect=KeyError("bad key"),
    ):
        with pytest.raises(CMReadthroughSystemError, match="Error generating CM readthrough"):
            await generate_cm_readthrough(2024, "Q3")
