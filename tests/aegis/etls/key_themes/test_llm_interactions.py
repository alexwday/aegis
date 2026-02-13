"""Tests for LLM interaction paths in key_themes ETL.

Tests classify_qa_block, format_qa_html, and determine_comprehensive_grouping
with mocked LLM responses, covering happy path, parse-error retries,
and transport-error backoff.
"""

import json

import pytest
from unittest.mock import AsyncMock, patch

from aegis.etls.key_themes.main import (
    classify_qa_block,
    format_qa_html,
    determine_comprehensive_grouping,
    QABlock,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_context():
    """Minimal context dict for LLM calls."""
    return {
        "execution_id": "test-exec-123",
        "auth_config": {"method": "api_key", "api_key": "test", "success": True},
        "ssl_config": {"verify": False},
        "bank_name": "Royal Bank of Canada",
        "bank_symbol": "RY",
        "quarter": "Q3",
        "fiscal_year": 2024,
    }


@pytest.fixture
def mock_prompt_data():
    """Minimal prompt data dict from load_prompt_from_db."""
    return {
        "system_prompt": (
            "Analyze {bank_name} {quarter} {fiscal_year}. "
            "Categories: {categories_list} ({num_categories}). "
            "Previous: {previous_classifications}"
        ),
        "user_prompt": "Classify this Q&A.",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "extract_qa_theme",
                "parameters": {"type": "object", "properties": {}},
            },
        },
    }


@pytest.fixture
def valid_classification_response():
    """Valid classification LLM response."""
    return {
        "choices": [
            {
                "message": {
                    "tool_calls": [
                        {
                            "function": {
                                "arguments": json.dumps(
                                    {
                                        "is_valid": True,
                                        "completion_status": "complete",
                                        "category_name": "Revenue Trends & Net Interest Income",
                                        "summary": "NIM outlook discussion.",
                                        "rejection_reason": "",
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


@pytest.fixture
def invalid_classification_response():
    """Invalid (rejected) classification LLM response."""
    return {
        "choices": [
            {
                "message": {
                    "tool_calls": [
                        {
                            "function": {
                                "arguments": json.dumps(
                                    {
                                        "is_valid": False,
                                        "completion_status": "",
                                        "category_name": "",
                                        "summary": "",
                                        "rejection_reason": "Operator transition only",
                                    }
                                )
                            }
                        }
                    ]
                }
            }
        ],
        "metrics": {},
    }


@pytest.fixture
def valid_grouping_response():
    """Valid grouping LLM response."""
    return {
        "choices": [
            {
                "message": {
                    "tool_calls": [
                        {
                            "function": {
                                "arguments": json.dumps(
                                    {
                                        "theme_groups": [
                                            {
                                                "group_title": "NIM & Revenue Trends",
                                                "qa_ids": ["qa_1"],
                                                "rationale": "NIM discussion",
                                            },
                                            {
                                                "group_title": "Credit Quality",
                                                "qa_ids": ["qa_2"],
                                                "rationale": "Credit discussion",
                                            },
                                        ]
                                    }
                                )
                            }
                        }
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


# ---------------------------------------------------------------------------
# classify_qa_block
# ---------------------------------------------------------------------------
class TestClassifyQaBlock:
    """Tests for classify_qa_block()."""

    @pytest.mark.asyncio
    async def test_happy_path_valid(
        self, mock_context, mock_prompt_data, valid_classification_response
    ):
        """Successful classification of a valid Q&A block."""
        qa_block = QABlock(qa_id="qa_1", position=1, original_content="Q&A about NIM")
        categories = [
            {
                "transcript_sections": "QA",
                "category_name": "Revenue Trends & Net Interest Income",
                "category_description": "NII and margin trends.",
                "example_1": "",
                "example_2": "",
                "example_3": "",
            }
        ]

        with patch(
            "aegis.etls.key_themes.main.complete_with_tools",
            new_callable=AsyncMock,
            return_value=valid_classification_response,
        ):
            await classify_qa_block(
                qa_block, categories, [], mock_prompt_data, mock_context
            )

        assert qa_block.is_valid is True
        assert qa_block.category_name == "Revenue Trends & Net Interest Income"
        assert qa_block.summary == "NIM outlook discussion."
        assert qa_block.completion_status == "complete"

    @pytest.mark.asyncio
    async def test_rejected_qa(
        self, mock_context, mock_prompt_data, invalid_classification_response
    ):
        """Classification correctly marks invalid Q&A blocks."""
        qa_block = QABlock(qa_id="qa_1", position=1, original_content="Operator transition")

        with patch(
            "aegis.etls.key_themes.main.complete_with_tools",
            new_callable=AsyncMock,
            return_value=invalid_classification_response,
        ):
            await classify_qa_block(qa_block, [], [], mock_prompt_data, mock_context)

        assert qa_block.is_valid is False
        assert qa_block.category_name is None

    @pytest.mark.asyncio
    async def test_retries_on_parse_error(self, mock_context, mock_prompt_data):
        """Parse errors (bad JSON) trigger immediate retry."""
        qa_block = QABlock(qa_id="qa_1", position=1, original_content="Q&A content")

        bad_response = {
            "choices": [
                {"message": {"tool_calls": [{"function": {"arguments": "not valid json"}}]}}
            ],
            "metrics": {},
        }
        good_response = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "arguments": json.dumps(
                                        {
                                            "is_valid": True,
                                            "completion_status": "complete",
                                            "category_name": "Revenue",
                                            "summary": "S",
                                            "rejection_reason": "",
                                        }
                                    )
                                }
                            }
                        ]
                    }
                }
            ],
            "metrics": {},
        }

        mock_llm = AsyncMock(side_effect=[bad_response, good_response])
        with patch("aegis.etls.key_themes.main.complete_with_tools", mock_llm):
            await classify_qa_block(
                qa_block, [], [], mock_prompt_data, mock_context
            )

        assert mock_llm.call_count == 2
        assert qa_block.is_valid is True

    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self, mock_context, mock_prompt_data):
        """Exhausted retries on parse errors raises RuntimeError."""
        qa_block = QABlock(qa_id="qa_1", position=1, original_content="Q&A content")

        bad_response = {
            "choices": [{"message": {"tool_calls": [{"function": {"arguments": "bad json"}}]}}],
            "metrics": {},
        }

        mock_llm = AsyncMock(return_value=bad_response)
        with patch("aegis.etls.key_themes.main.complete_with_tools", mock_llm):
            with pytest.raises(RuntimeError, match="Failed to parse classification"):
                await classify_qa_block(
                    qa_block, [], [], mock_prompt_data, mock_context
                )

    @pytest.mark.asyncio
    async def test_transport_error_retries_with_backoff(
        self, mock_context, mock_prompt_data, valid_classification_response
    ):
        """Transport errors trigger retry with backoff."""
        qa_block = QABlock(qa_id="qa_1", position=1, original_content="Q&A content")

        mock_llm = AsyncMock(
            side_effect=[ConnectionError("timeout"), valid_classification_response]
        )
        with (
            patch("aegis.etls.key_themes.main.complete_with_tools", mock_llm),
            patch("aegis.etls.key_themes.main.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            await classify_qa_block(
                qa_block, [], [], mock_prompt_data, mock_context
            )

        assert mock_llm.call_count == 2
        assert mock_sleep.call_count == 1
        assert qa_block.is_valid is True

    @pytest.mark.asyncio
    async def test_no_tool_calls_raises(self, mock_context, mock_prompt_data):
        """LLM returning no tool_calls raises RuntimeError."""
        qa_block = QABlock(qa_id="qa_1", position=1, original_content="Q&A content")

        no_tool_response = {
            "choices": [{"message": {"content": "Some text, no tool calls."}}],
            "metrics": {},
        }

        mock_llm = AsyncMock(return_value=no_tool_response)
        with patch("aegis.etls.key_themes.main.complete_with_tools", mock_llm):
            with pytest.raises(RuntimeError, match="Classification failed"):
                await classify_qa_block(
                    qa_block, [], [], mock_prompt_data, mock_context
                )


# ---------------------------------------------------------------------------
# format_qa_html
# ---------------------------------------------------------------------------
class TestFormatQaHtml:
    """Tests for format_qa_html()."""

    @pytest.fixture
    def formatting_prompt_data(self):
        """Minimal formatting prompt data."""
        return {
            "system_prompt": "Format {bank_name} {quarter} {fiscal_year} Q&A.",
            "user_prompt": "Format this Q&A:\n\n{completion_note}{qa_content}",
        }

    @pytest.mark.asyncio
    async def test_happy_path(self, mock_context, formatting_prompt_data):
        """Successful formatting returns HTML content."""
        qa_block = QABlock(qa_id="qa_1", position=1, original_content="Raw Q&A content")
        qa_block.is_valid = True
        qa_block.completion_status = "complete"

        response = {
            "choices": [
                {"message": {"content": "<b>John Smith</b> (Goldman): Key question content."}}
            ],
            "metrics": {"prompt_tokens": 500, "completion_tokens": 200},
        }

        with patch(
            "aegis.etls.key_themes.main.complete",
            new_callable=AsyncMock,
            return_value=response,
        ):
            await format_qa_html(qa_block, formatting_prompt_data, mock_context)

        assert qa_block.formatted_content is not None
        assert "<b>John Smith</b>" in qa_block.formatted_content

    @pytest.mark.asyncio
    async def test_skips_invalid_block(self, mock_context, formatting_prompt_data):
        """Invalid blocks get None formatted_content."""
        qa_block = QABlock(qa_id="qa_1", position=1, original_content="Invalid")
        qa_block.is_valid = False

        await format_qa_html(qa_block, formatting_prompt_data, mock_context)
        assert qa_block.formatted_content is None

    @pytest.mark.asyncio
    async def test_raises_on_error(self, mock_context, formatting_prompt_data):
        """Raises RuntimeError when all retries exhausted."""
        qa_block = QABlock(qa_id="qa_1", position=1, original_content="Original content")
        qa_block.is_valid = True
        qa_block.completion_status = "complete"

        mock_llm = AsyncMock(side_effect=ConnectionError("timeout"))
        with (
            patch("aegis.etls.key_themes.main.complete", mock_llm),
            patch("aegis.etls.key_themes.main.asyncio.sleep", new_callable=AsyncMock),
        ):
            with pytest.raises(RuntimeError, match="HTML formatting failed"):
                await format_qa_html(qa_block, formatting_prompt_data, mock_context)

    @pytest.mark.asyncio
    async def test_content_with_curly_braces(self, mock_context, formatting_prompt_data):
        """Content containing {braces} does not crash .format() call."""
        qa_block = QABlock(
            qa_id="qa_1",
            position=1,
            original_content="Revenue breakdown {see table 3} showed growth of 5%.",
        )
        qa_block.is_valid = True
        qa_block.completion_status = "complete"

        response = {
            "choices": [{"message": {"content": "<b>Revenue</b> breakdown showed growth."}}],
            "metrics": {},
        }

        with patch(
            "aegis.etls.key_themes.main.complete",
            new_callable=AsyncMock,
            return_value=response,
        ):
            await format_qa_html(qa_block, formatting_prompt_data, mock_context)

        assert qa_block.formatted_content is not None


# ---------------------------------------------------------------------------
# determine_comprehensive_grouping
# ---------------------------------------------------------------------------
class TestDetermineComprehensiveGrouping:
    """Tests for determine_comprehensive_grouping()."""

    @pytest.fixture
    def grouping_prompt_data(self):
        """Minimal grouping prompt data."""
        return {
            "system_prompt": (
                "Group {bank_name} ({bank_symbol}) {quarter} {fiscal_year}. "
                "Total: {total_qa_blocks}. QA info: {qa_blocks_info}. "
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

    @pytest.mark.asyncio
    async def test_happy_path(
        self,
        mock_context,
        sample_qa_blocks,
        sample_categories,
        grouping_prompt_data,
        valid_grouping_response,
    ):
        """Successful grouping returns ThemeGroup list."""
        with patch(
            "aegis.etls.key_themes.main.complete_with_tools",
            new_callable=AsyncMock,
            return_value=valid_grouping_response,
        ):
            groups = await determine_comprehensive_grouping(
                sample_qa_blocks, sample_categories, grouping_prompt_data, mock_context
            )

        assert len(groups) == 2
        assert groups[0].group_title == "NIM & Revenue Trends"
        assert groups[0].qa_ids == ["qa_1"]
        assert groups[1].group_title == "Credit Quality"
        assert groups[1].qa_ids == ["qa_2"]

    @pytest.mark.asyncio
    async def test_empty_qa_returns_empty(
        self, mock_context, sample_categories, grouping_prompt_data
    ):
        """No valid Q&A blocks returns empty list."""
        qa_index = {
            "qa_1": QABlock(qa_id="qa_1", position=1, original_content="invalid"),
        }
        qa_index["qa_1"].is_valid = False

        groups = await determine_comprehensive_grouping(
            qa_index, sample_categories, grouping_prompt_data, mock_context
        )
        assert groups == []

    @pytest.mark.asyncio
    async def test_pydantic_validation_catches_bad_schema(
        self, mock_context, sample_qa_blocks, sample_categories, grouping_prompt_data
    ):
        """Pydantic validation catches missing required fields."""
        bad_schema_response = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "arguments": json.dumps({"theme_groups": [{"no_title": True}]})
                                }
                            }
                        ]
                    }
                }
            ],
            "metrics": {},
        }

        mock_llm = AsyncMock(return_value=bad_schema_response)
        with patch("aegis.etls.key_themes.main.complete_with_tools", mock_llm):
            with pytest.raises(RuntimeError, match="Failed to parse theme regrouping"):
                await determine_comprehensive_grouping(
                    sample_qa_blocks, sample_categories, grouping_prompt_data, mock_context
                )

    @pytest.mark.asyncio
    async def test_raises_on_no_tool_calls(
        self, mock_context, sample_qa_blocks, sample_categories, grouping_prompt_data
    ):
        """Raises RuntimeError when LLM returns no tool calls after all retries."""
        no_tool_response = {
            "choices": [{"message": {"content": "Some text but no tool calls."}}],
            "metrics": {},
        }

        mock_llm = AsyncMock(return_value=no_tool_response)
        with patch("aegis.etls.key_themes.main.complete_with_tools", mock_llm):
            with pytest.raises(RuntimeError, match="Theme grouping failed"):
                await determine_comprehensive_grouping(
                    sample_qa_blocks, sample_categories, grouping_prompt_data, mock_context
                )
