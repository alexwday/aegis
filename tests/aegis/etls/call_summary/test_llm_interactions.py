"""Tests for LLM interaction paths in call_summary ETL (D3.1).

Tests _generate_research_plan and _extract_single_category with mocked LLM
responses, covering happy path, parse-error retries, and transport-error backoff.
"""

import asyncio
import json

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from aegis.etls.call_summary.main import (
    _generate_research_plan,
    _extract_single_category,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_context():
    """Minimal context dict for LLM calls."""
    return {
        "execution_id": "test-exec-123",
        "auth_config": {"method": "api_key", "api_key": "test"},
        "ssl_config": {"verify": False},
    }


@pytest.fixture
def research_prompts():
    """Minimal research prompts dict."""
    return {
        "system_prompt": "You are a financial analyst.",
        "user_prompt_template": "Analyze: {transcript_text}",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "generate_research_plan",
                "parameters": {"type": "object", "properties": {}},
            },
        },
    }


@pytest.fixture
def extraction_prompts():
    """Minimal extraction prompts dict."""
    return {
        "system_prompt": "Extract content for {category_name}. Desc: {category_description}. "
        "Plan: {research_plan}. Notes: {cross_category_notes}. "
        "Idx {category_index}/{total_categories}. "
        "{bank_name} ({bank_symbol}) {quarter} {fiscal_year}. "
        "Section: {transcripts_section}.",
        "user_prompt": "Extract from: {formatted_section}",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "extract_category_content",
                "parameters": {"type": "object", "properties": {}},
            },
        },
    }


@pytest.fixture
def sample_research_plan_response():
    """Valid research plan LLM response."""
    return {
        "choices": [
            {
                "message": {
                    "tool_calls": [
                        {
                            "function": {
                                "arguments": json.dumps(
                                    {
                                        "category_plans": [
                                            {
                                                "index": 1,
                                                "name": "Revenue",
                                                "extraction_strategy": "Focus on NII and fees",
                                                "cross_category_notes": "",
                                                "relevant_qa_groups": [1, 2],
                                            }
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
            "prompt_tokens": 1000,
            "completion_tokens": 200,
            "total_cost": 0.01,
            "response_time": 1.5,
        },
    }


@pytest.fixture
def sample_extraction_response():
    """Valid category extraction LLM response."""
    return {
        "choices": [
            {
                "message": {
                    "tool_calls": [
                        {
                            "function": {
                                "arguments": json.dumps(
                                    {
                                        "rejected": False,
                                        "title": "Revenue: Strong NII growth",
                                        "summary_statements": [
                                            {
                                                "statement": "NII grew **8%** to **$5.2 BN**.",
                                                "evidence": [
                                                    {
                                                        "type": "paraphrase",
                                                        "content": "Strong NII growth.",
                                                        "speaker": "CFO",
                                                    }
                                                ],
                                            }
                                        ],
                                    }
                                )
                            }
                        }
                    ]
                }
            }
        ],
        "metrics": {
            "prompt_tokens": 5000,
            "completion_tokens": 300,
            "total_cost": 0.05,
            "response_time": 3.0,
        },
    }


@pytest.fixture
def etl_context_for_extraction(mock_context):
    """ETL context dict for _extract_single_category."""
    return {
        "context": mock_context,
        "execution_id": "test-exec-123",
        "retrieval_params": {
            "bank_id": 1,
            "bank_name": "Royal Bank of Canada",
            "bank_symbol": "RY",
            "fiscal_year": 2024,
            "quarter": "Q3",
        },
        "bank_info": {
            "bank_id": 1,
            "bank_name": "Royal Bank of Canada",
            "bank_symbol": "RY",
        },
        "quarter": "Q3",
        "fiscal_year": 2024,
        "section_cache": {
            "ALL": [
                {
                    "id": 1,
                    "section_name": "MANAGEMENT DISCUSSION SECTION",
                    "content": "Revenue grew strongly.",
                    "speaker_block_id": 1,
                    "qa_group_id": None,
                    "block_summary": "Revenue discussion",
                }
            ],
            "MD": [
                {
                    "id": 1,
                    "section_name": "MANAGEMENT DISCUSSION SECTION",
                    "content": "Revenue grew strongly.",
                    "speaker_block_id": 1,
                    "qa_group_id": None,
                    "block_summary": "Revenue discussion",
                }
            ],
            "QA": [],
        },
    }


# ---------------------------------------------------------------------------
# _generate_research_plan
# ---------------------------------------------------------------------------
class TestGenerateResearchPlan:
    """Tests for _generate_research_plan()."""

    @pytest.mark.asyncio
    async def test_happy_path(
        self, mock_context, research_prompts, sample_research_plan_response
    ):
        """Successful research plan generation returns validated data."""
        with patch(
            "aegis.etls.call_summary.main.complete_with_tools",
            new_callable=AsyncMock,
            return_value=sample_research_plan_response,
        ):
            result = await _generate_research_plan(
                context=mock_context,
                research_prompts=research_prompts,
                transcript_text="Test transcript content.",
                execution_id="test-exec-123",
            )

        assert "category_plans" in result
        assert len(result["category_plans"]) == 1
        assert result["category_plans"][0]["name"] == "Revenue"
        assert result["category_plans"][0]["relevant_qa_groups"] == [1, 2]

    @pytest.mark.asyncio
    async def test_retries_on_parse_error(self, mock_context, research_prompts):
        """Parse errors (bad JSON) trigger immediate retry."""
        bad_response = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {"function": {"arguments": "not valid json"}}
                        ]
                    }
                }
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
                                            "category_plans": [
                                                {
                                                    "index": 1,
                                                    "name": "Test",
                                                    "extraction_strategy": "s",
                                                    "cross_category_notes": "",
                                                    "relevant_qa_groups": [],
                                                }
                                            ]
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
        with patch("aegis.etls.call_summary.main.complete_with_tools", mock_llm):
            result = await _generate_research_plan(
                context=mock_context,
                research_prompts=research_prompts,
                transcript_text="Test",
                execution_id="test-exec-123",
            )

        assert mock_llm.call_count == 2
        assert "category_plans" in result

    @pytest.mark.asyncio
    async def test_raises_after_max_retries_parse_error(
        self, mock_context, research_prompts
    ):
        """Exhausted retries on parse errors raises RuntimeError."""
        bad_response = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {"function": {"arguments": "bad json"}}
                        ]
                    }
                }
            ],
            "metrics": {},
        }

        mock_llm = AsyncMock(return_value=bad_response)
        with patch("aegis.etls.call_summary.main.complete_with_tools", mock_llm):
            with pytest.raises(RuntimeError, match="Error generating research plan"):
                await _generate_research_plan(
                    context=mock_context,
                    research_prompts=research_prompts,
                    transcript_text="Test",
                    execution_id="test-exec-123",
                )

    @pytest.mark.asyncio
    async def test_retries_on_transport_error_with_backoff(
        self, mock_context, research_prompts, sample_research_plan_response
    ):
        """Transport errors trigger retry with exponential backoff."""
        mock_llm = AsyncMock(
            side_effect=[ConnectionError("timeout"), sample_research_plan_response]
        )

        with (
            patch("aegis.etls.call_summary.main.complete_with_tools", mock_llm),
            patch("aegis.etls.call_summary.main.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            result = await _generate_research_plan(
                context=mock_context,
                research_prompts=research_prompts,
                transcript_text="Test",
                execution_id="test-exec-123",
            )

        assert mock_llm.call_count == 2
        assert mock_sleep.call_count == 1
        assert "category_plans" in result

    @pytest.mark.asyncio
    async def test_pydantic_validation_catches_bad_schema(
        self, mock_context, research_prompts
    ):
        """Pydantic validation catches missing required fields."""
        bad_schema_response = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "arguments": json.dumps(
                                        {
                                            "category_plans": [
                                                {"index": "not_an_int"}
                                            ]
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

        mock_llm = AsyncMock(return_value=bad_schema_response)
        with patch("aegis.etls.call_summary.main.complete_with_tools", mock_llm):
            with pytest.raises(RuntimeError, match="Error generating research plan"):
                await _generate_research_plan(
                    context=mock_context,
                    research_prompts=research_prompts,
                    transcript_text="Test",
                    execution_id="test-exec-123",
                )


# ---------------------------------------------------------------------------
# _extract_single_category
# ---------------------------------------------------------------------------
class TestExtractSingleCategory:
    """Tests for _extract_single_category()."""

    @pytest.mark.asyncio
    async def test_happy_path(
        self, extraction_prompts, sample_extraction_response, etl_context_for_extraction
    ):
        """Successful extraction returns validated data with enriched fields."""
        category = {
            "category_name": "Revenue",
            "category_description": "Revenue analysis",
            "transcript_sections": "ALL",
            "report_section": "Results Summary",
        }
        research_plan_data = {
            "category_plans": [
                {
                    "index": 1,
                    "name": "Revenue",
                    "extraction_strategy": "Focus on NII",
                    "cross_category_notes": "",
                    "relevant_qa_groups": [],
                }
            ]
        }

        with patch(
            "aegis.etls.call_summary.main.complete_with_tools",
            new_callable=AsyncMock,
            return_value=sample_extraction_response,
        ):
            result = await _extract_single_category(
                index=1,
                category=category,
                research_plan_data=research_plan_data,
                extraction_prompts=extraction_prompts,
                etl_context=etl_context_for_extraction,
                semaphore=asyncio.Semaphore(5),
                total_categories=3,
            )

        assert result["rejected"] is False
        assert result["index"] == 1
        assert result["name"] == "Revenue"
        assert result["report_section"] == "Results Summary"
        assert result["title"] == "Revenue: Strong NII growth"
        assert len(result["summary_statements"]) == 1

    @pytest.mark.asyncio
    async def test_fallback_when_not_in_research_plan(
        self, extraction_prompts, sample_extraction_response, etl_context_for_extraction
    ):
        """Category not in research plan uses fallback extraction strategy."""
        category = {
            "category_name": "ESG",
            "category_description": "ESG topics",
            "transcript_sections": "ALL",
        }
        research_plan_data = {"category_plans": []}  # No plan for this category

        with patch(
            "aegis.etls.call_summary.main.complete_with_tools",
            new_callable=AsyncMock,
            return_value=sample_extraction_response,
        ) as mock_llm:
            result = await _extract_single_category(
                index=5,
                category=category,
                research_plan_data=research_plan_data,
                extraction_prompts=extraction_prompts,
                etl_context=etl_context_for_extraction,
                semaphore=asyncio.Semaphore(5),
                total_categories=3,
            )

        assert result["rejected"] is False
        # Should have been called with fallback strategy in the prompt
        call_args = mock_llm.call_args
        messages = call_args.kwargs.get("messages", call_args.args[0] if call_args.args else [])
        system_msg = messages[0]["content"]
        assert "not identified in the research plan" in system_msg

    @pytest.mark.asyncio
    async def test_returns_rejection_on_exhausted_retries(
        self, extraction_prompts, etl_context_for_extraction
    ):
        """Returns rejection result (not exception) after exhausting retries."""
        category = {
            "category_name": "Revenue",
            "category_description": "Revenue analysis",
            "transcript_sections": "ALL",
        }
        research_plan_data = {
            "category_plans": [
                {
                    "index": 1,
                    "name": "Revenue",
                    "extraction_strategy": "s",
                    "cross_category_notes": "",
                    "relevant_qa_groups": [],
                }
            ]
        }

        mock_llm = AsyncMock(side_effect=ConnectionError("timeout"))
        with (
            patch("aegis.etls.call_summary.main.complete_with_tools", mock_llm),
            patch("aegis.etls.call_summary.main.asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await _extract_single_category(
                index=1,
                category=category,
                research_plan_data=research_plan_data,
                extraction_prompts=extraction_prompts,
                etl_context=etl_context_for_extraction,
                semaphore=asyncio.Semaphore(5),
                total_categories=3,
            )

        # Should return rejection, not raise
        assert result["rejected"] is True
        assert "Error after" in result["rejection_reason"]

    @pytest.mark.asyncio
    async def test_title_defaults_to_category_name(
        self, extraction_prompts, etl_context_for_extraction
    ):
        """When LLM returns no title, defaults to category_name."""
        no_title_response = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "arguments": json.dumps(
                                        {
                                            "rejected": False,
                                            "summary_statements": [
                                                {"statement": "Test."}
                                            ],
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
        category = {
            "category_name": "Revenue",
            "category_description": "Revenue analysis",
            "transcript_sections": "ALL",
        }
        research_plan_data = {
            "category_plans": [
                {
                    "index": 1,
                    "name": "Revenue",
                    "extraction_strategy": "s",
                    "cross_category_notes": "",
                    "relevant_qa_groups": [],
                }
            ]
        }

        with patch(
            "aegis.etls.call_summary.main.complete_with_tools",
            new_callable=AsyncMock,
            return_value=no_title_response,
        ):
            result = await _extract_single_category(
                index=1,
                category=category,
                research_plan_data=research_plan_data,
                extraction_prompts=extraction_prompts,
                etl_context=etl_context_for_extraction,
                semaphore=asyncio.Semaphore(5),
                total_categories=3,
            )

        assert result["title"] == "Revenue"
