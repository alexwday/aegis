"""Integration tests for generate_key_themes() end-to-end flow."""

import json
import os

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from contextlib import asynccontextmanager

from aegis.etls.key_themes.main import (
    generate_key_themes,
    KeyThemesResult,
    KeyThemesUserError,
    KeyThemesSystemError,
    _load_monitored_institutions,
)


MOCK_INSTITUTIONS = {
    1: {
        "id": 1,
        "name": "Royal Bank of Canada",
        "symbol": "RY",
        "type": "Canadian_Banks",
    },
}


def _make_classification_response(is_valid=True, category="Revenue", summary="Summary."):
    """Build a classification LLM response."""
    return {
        "choices": [
            {
                "message": {
                    "tool_calls": [
                        {
                            "function": {
                                "arguments": json.dumps(
                                    {
                                        "is_valid": is_valid,
                                        "completion_status": "complete" if is_valid else "",
                                        "category_name": category if is_valid else "",
                                        "summary": summary if is_valid else "",
                                        "rejection_reason": (
                                            "" if is_valid else "Operator transition"
                                        ),
                                    }
                                )
                            }
                        }
                    ]
                }
            }
        ],
        "metrics": {
            "prompt_tokens": 1000,
            "completion_tokens": 100,
            "total_cost": 0.01,
            "response_time": 1.0,
        },
    }


def _make_formatting_response(content="<b>Formatted</b> content"):
    """Build a formatting LLM response."""
    return {
        "choices": [{"message": {"content": content}}],
        "metrics": {"prompt_tokens": 500, "completion_tokens": 200},
    }


def _make_grouping_response(qa_ids_per_group):
    """Build a grouping LLM response."""
    groups = []
    for i, qa_ids in enumerate(qa_ids_per_group):
        groups.append(
            {
                "group_title": f"Theme Group {i + 1}",
                "qa_ids": qa_ids,
                "rationale": f"Group {i + 1}",
            }
        )
    return {
        "choices": [
            {
                "message": {
                    "tool_calls": [
                        {"function": {"arguments": json.dumps({"theme_groups": groups})}}
                    ]
                }
            }
        ],
        "metrics": {
            "prompt_tokens": 2000,
            "completion_tokens": 200,
            "total_cost": 0.02,
            "response_time": 2.0,
        },
    }


def _setup_mocks():
    """Create all the mocks needed for end-to-end testing."""
    # Mock transcript chunks (2 valid Q&A groups + 1 operator transition)
    mock_chunks = [
        {
            "id": 1,
            "section_name": "Q&A",
            "speaker_block_id": None,
            "qa_group_id": 1,
            "chunk_id": 1,
            "content": (
                "John Smith, Goldman Sachs: Can you discuss NIM trends?\n\n"
                "Jane Doe, CFO: NIM is at 1.65% and we expect expansion to 1.75%."
            ),
            "block_summary": "NIM discussion",
            "classification_ids": None,
            "classification_names": None,
            "title": "Q3 2024 Earnings Call",
        },
        {
            "id": 2,
            "section_name": "Q&A",
            "speaker_block_id": None,
            "qa_group_id": 2,
            "chunk_id": 1,
            "content": (
                "Mike Johnson, JP Morgan: How about credit quality?\n\n"
                "Bob Smith, CRO: PCL was $850 million with strong coverage."
            ),
            "block_summary": "Credit quality",
            "classification_ids": None,
            "classification_names": None,
            "title": "Q3 2024 Earnings Call",
        },
    ]

    mock_categories = [
        {
            "transcript_sections": "QA",
            "category_name": "Revenue",
            "category_description": "Revenue and NII trends.",
            "example_1": "",
            "example_2": "",
            "example_3": "",
        },
        {
            "transcript_sections": "QA",
            "category_name": "Credit",
            "category_description": "Credit quality and provisions.",
            "example_1": "",
            "example_2": "",
            "example_3": "",
        },
    ]

    # LLM call tracking
    classification_call_count = 0

    async def mock_complete_with_tools(*args, **kwargs):
        nonlocal classification_call_count
        # Determine which LLM call this is based on the tool definition
        tools = kwargs.get("tools", args[1] if len(args) > 1 else [])
        if tools and tools[0].get("function", {}).get("name") == "extract_qa_theme":
            classification_call_count += 1
            if classification_call_count == 1:
                return _make_classification_response(True, "Revenue", "NIM discussion.")
            else:
                return _make_classification_response(True, "Credit", "Credit quality discussion.")
        else:
            # Grouping call
            return _make_grouping_response([["qa_1"], ["qa_2"]])

    async def mock_complete(*args, **kwargs):
        return _make_formatting_response("<b>Formatted</b> Q&A content here.")

    # Different prompt data for each LLM call type
    classification_prompt = {
        "system_prompt": (
            "Analyze {bank_name} {quarter} {fiscal_year}. "
            "Categories: {categories_list} ({num_categories}). "
            "Previous: {previous_classifications}"
        ),
        "user_prompt": "Classify.",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "extract_qa_theme",
                "parameters": {"type": "object", "properties": {}},
            },
        },
    }

    formatting_prompt = {
        "system_prompt": "Format {bank_name} {quarter} {fiscal_year} Q&A.",
        "user_prompt": "Format this Q&A.",
    }

    grouping_prompt = {
        "system_prompt": (
            "Group {bank_name} ({bank_symbol}) {quarter} {fiscal_year}. "
            "Total: {total_qa_blocks}. QA: {qa_blocks_info}. "
            "Categories: {categories_list} ({num_categories})."
        ),
        "user_prompt": "Review category assignments, regroup if needed, and create final titles.",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "group_themes",
                "parameters": {"type": "object", "properties": {}},
            },
        },
    }

    def mock_load_prompt_from_db(**kwargs):
        name = kwargs.get("name", "")
        if name == "theme_extraction":
            # Return a copy so .format() doesn't corrupt the template
            return {k: v for k, v in classification_prompt.items()}
        elif name == "html_formatting":
            return {k: v for k, v in formatting_prompt.items()}
        elif name == "theme_grouping":
            return {k: v for k, v in grouping_prompt.items()}
        return classification_prompt

    @asynccontextmanager
    async def mock_get_connection():
        mock_conn = AsyncMock()
        # verify_and_get_availability
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (["transcripts"],)
        mock_conn.execute.return_value = mock_result
        yield mock_conn

    return {
        "chunks": mock_chunks,
        "categories": mock_categories,
        "complete_with_tools": mock_complete_with_tools,
        "complete": mock_complete,
        "load_prompt_from_db": mock_load_prompt_from_db,
        "get_connection": mock_get_connection,
    }


class TestGenerateKeyThemes:
    """Integration tests for generate_key_themes()."""

    @pytest.fixture(autouse=True)
    def clear_cache(self):
        """Clear institution cache before each test."""
        _load_monitored_institutions.cache_clear()
        yield
        _load_monitored_institutions.cache_clear()

    @pytest.mark.asyncio
    async def test_end_to_end_success(self, tmp_path):
        """Full end-to-end flow produces KeyThemesResult."""
        mocks = _setup_mocks()

        with (
            patch(
                "aegis.etls.key_themes.main._load_monitored_institutions",
                return_value=MOCK_INSTITUTIONS,
            ),
            patch(
                "aegis.etls.key_themes.main.verify_and_get_availability",
                new_callable=AsyncMock,
            ),
            patch(
                "aegis.etls.key_themes.main.setup_ssl",
                return_value={"verify": False},
            ),
            patch(
                "aegis.etls.key_themes.main.setup_authentication",
                new_callable=AsyncMock,
                return_value={"success": True, "method": "api_key"},
            ),
            patch(
                "aegis.etls.key_themes.main.load_categories_from_xlsx",
                return_value=mocks["categories"],
            ),
            patch(
                "aegis.etls.key_themes.main.retrieve_full_section",
                new_callable=AsyncMock,
                return_value=mocks["chunks"],
            ),
            patch(
                "aegis.etls.key_themes.main.load_prompt_from_db",
                side_effect=mocks["load_prompt_from_db"],
            ),
            patch(
                "aegis.etls.key_themes.main.complete_with_tools",
                side_effect=mocks["complete_with_tools"],
            ),
            patch(
                "aegis.etls.key_themes.main.complete",
                side_effect=mocks["complete"],
            ),
            patch(
                "aegis.etls.key_themes.main.get_connection",
                mocks["get_connection"],
            ),
            patch(
                "aegis.etls.key_themes.main.os.path.dirname",
                return_value=str(tmp_path),
            ),
        ):
            # Create output directory
            os.makedirs(str(tmp_path / "output"), exist_ok=True)

            result = await generate_key_themes(bank_name="RY", fiscal_year=2024, quarter="Q3")

        assert isinstance(result, KeyThemesResult)
        assert result.theme_groups == 2
        assert result.valid_qa == 2
        assert result.invalid_qa_filtered == 0
        assert result.filepath.endswith(".docx")
        assert result.total_cost >= 0
        assert result.total_tokens >= 0

    @pytest.mark.asyncio
    async def test_no_data_raises_user_error(self):
        """No Q&A data raises KeyThemesUserError."""
        with (
            patch(
                "aegis.etls.key_themes.main._load_monitored_institutions",
                return_value=MOCK_INSTITUTIONS,
            ),
            patch(
                "aegis.etls.key_themes.main.verify_and_get_availability",
                new_callable=AsyncMock,
            ),
            patch(
                "aegis.etls.key_themes.main.setup_ssl",
                return_value={"verify": False},
            ),
            patch(
                "aegis.etls.key_themes.main.setup_authentication",
                new_callable=AsyncMock,
                return_value={"success": True},
            ),
            patch(
                "aegis.etls.key_themes.main.load_categories_from_xlsx",
                return_value=[{"category_name": "Test", "transcript_sections": "QA"}],
            ),
            patch(
                "aegis.etls.key_themes.main.retrieve_full_section",
                new_callable=AsyncMock,
                return_value=[],  # No chunks
            ),
        ):
            with pytest.raises(KeyThemesUserError, match="No Q&A data found"):
                await generate_key_themes(bank_name="RY", fiscal_year=2024, quarter="Q3")

    @pytest.mark.asyncio
    async def test_bad_bank_raises_user_error(self):
        """Invalid bank identifier raises KeyThemesUserError."""
        with patch(
            "aegis.etls.key_themes.main._load_monitored_institutions",
            return_value=MOCK_INSTITUTIONS,
        ):
            with pytest.raises(KeyThemesUserError, match="not found"):
                await generate_key_themes(bank_name="NONEXISTENT", fiscal_year=2024, quarter="Q3")

    @pytest.mark.asyncio
    async def test_auth_failure_raises_system_error(self):
        """Authentication failure raises KeyThemesSystemError."""
        with (
            patch(
                "aegis.etls.key_themes.main._load_monitored_institutions",
                return_value=MOCK_INSTITUTIONS,
            ),
            patch(
                "aegis.etls.key_themes.main.verify_and_get_availability",
                new_callable=AsyncMock,
            ),
            patch(
                "aegis.etls.key_themes.main.setup_ssl",
                return_value={"verify": False},
            ),
            patch(
                "aegis.etls.key_themes.main.setup_authentication",
                new_callable=AsyncMock,
                return_value={"success": False, "error": "Invalid credentials"},
            ),
        ):
            with pytest.raises(KeyThemesSystemError, match="Authentication failed"):
                await generate_key_themes(bank_name="RY", fiscal_year=2024, quarter="Q3")
