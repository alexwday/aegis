"""Tests for LLM interaction reliability in CM readthrough ETL."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from aegis.etls.cm_readthrough.main import (
    _complete_with_tools_validated,
    OutlookExtractionResponse,
    QAExtractionResponse,
    SubtitleResponse,
    extract_outlook_from_transcript,
    extract_questions_from_qa,
    format_outlook_batch,
    _deduplicate_qa_results_llm,
)


@pytest.mark.asyncio
async def test_complete_with_tools_validated_success_updates_costs(
    sample_context, valid_tool_response_outlook
):
    """Validated helper should parse tool payload and accumulate metrics."""
    with patch(
        "aegis.etls.cm_readthrough.main.complete_with_tools",
        new_callable=AsyncMock,
        return_value=valid_tool_response_outlook,
    ):
        result = await _complete_with_tools_validated(
            messages=[{"role": "system", "content": "s"}, {"role": "user", "content": "u"}],
            tools=[{"type": "function", "function": {"name": "x"}}],
            context=sample_context,
            llm_params={"model": "m", "temperature": 0, "max_tokens": 100},
            response_model=OutlookExtractionResponse,
            stage="test_stage",
        )

    assert result["has_content"] is True
    assert len(result["statements"]) == 1
    assert len(sample_context["_llm_costs"]) == 1


@pytest.mark.asyncio
async def test_complete_with_tools_validated_retries_on_parse_error(sample_context):
    """Parse errors should retry and then succeed on a valid response."""
    bad_response = {
        "choices": [{"message": {"tool_calls": [{"function": {"arguments": "{bad json"}}]}}],
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
                                        "has_content": True,
                                        "statements": [
                                            {
                                                "category": "C",
                                                "statement": "S",
                                                "relevance_score": 7,
                                                "is_new_category": False,
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
        "metrics": {},
    }

    mock_llm = AsyncMock(side_effect=[bad_response, good_response])
    with patch("aegis.etls.cm_readthrough.main.complete_with_tools", mock_llm):
        result = await _complete_with_tools_validated(
            messages=[{"role": "system", "content": "s"}, {"role": "user", "content": "u"}],
            tools=[{"type": "function", "function": {"name": "x"}}],
            context=sample_context,
            llm_params={"model": "m", "temperature": 0, "max_tokens": 100},
            response_model=OutlookExtractionResponse,
            stage="test_retry_parse",
        )

    assert result["has_content"] is True
    assert mock_llm.call_count == 2


@pytest.mark.asyncio
async def test_complete_with_tools_validated_can_default_on_failure(sample_context):
    """Default value should be returned when failures are allowed."""
    no_tool_call_response = {"choices": [{"message": {"tool_calls": []}}], "metrics": {}}
    mock_llm = AsyncMock(
        side_effect=[no_tool_call_response, no_tool_call_response, no_tool_call_response]
    )

    with patch("aegis.etls.cm_readthrough.main.complete_with_tools", mock_llm):
        result = await _complete_with_tools_validated(
            messages=[{"role": "system", "content": "s"}, {"role": "user", "content": "u"}],
            tools=[{"type": "function", "function": {"name": "x"}}],
            context=sample_context,
            llm_params={"model": "m", "temperature": 0, "max_tokens": 100},
            response_model=SubtitleResponse,
            stage="subtitle_default",
            allow_default_on_failure=True,
            default_value={"subtitle": "Fallback subtitle"},
        )

    assert result["subtitle"] == "Fallback subtitle"


@pytest.mark.asyncio
async def test_extract_outlook_raises_on_llm_pipeline_failure(
    sample_bank_info, sample_outlook_categories, sample_context, outlook_prompt_data
):
    """Outlook extractor should propagate errors (bank-level handler catches them)."""
    with patch(
        "aegis.etls.cm_readthrough.main._complete_with_tools_validated",
        new_callable=AsyncMock,
        side_effect=RuntimeError("transport error"),
    ):
        with pytest.raises(RuntimeError, match="transport error"):
            await extract_outlook_from_transcript(
                sample_bank_info,
                "Transcript text",
                sample_outlook_categories,
                2024,
                "Q3",
                sample_context,
                prompt_data=outlook_prompt_data,
            )


@pytest.mark.asyncio
async def test_extract_questions_no_content_is_not_failure(
    sample_bank_info, sample_qa_categories, sample_context, qa_prompt_data
):
    """No-content response should return failed=False with empty question list."""
    with patch(
        "aegis.etls.cm_readthrough.main._complete_with_tools_validated",
        new_callable=AsyncMock,
        return_value={"has_content": False, "questions": []},
    ):
        result = await extract_questions_from_qa(
            sample_bank_info,
            "Q&A text",
            sample_qa_categories,
            2024,
            "Q3",
            sample_context,
            prompt_data=qa_prompt_data,
        )

    assert result == {
        "has_content": False,
        "questions": [],
        "emerging_categories": [],
        "failed": False,
    }


@pytest.mark.asyncio
async def test_extract_questions_uses_dynamic_categories_in_prompt(
    sample_bank_info, sample_qa_categories, sample_context, qa_prompt_data
):
    """System prompt should be built from runtime category payload (no hardcoded categories)."""
    captured = {}

    async def fake_validated(**kwargs):
        captured["messages"] = kwargs["messages"]
        return {"has_content": False, "questions": []}

    with patch(
        "aegis.etls.cm_readthrough.main._complete_with_tools_validated", side_effect=fake_validated
    ):
        await extract_questions_from_qa(
            sample_bank_info,
            "Q&A text",
            sample_qa_categories,
            2024,
            "Q3",
            sample_context,
            prompt_data=qa_prompt_data,
        )

    system_prompt = captured["messages"][0]["content"]
    assert "Market Volatility" in system_prompt


@pytest.mark.asyncio
async def test_format_outlook_batch_applies_formatting(sample_context, formatting_prompt_data):
    """Batch formatter should add formatted_quote to statements when LLM succeeds."""
    all_outlook = {
        "Bank A": {
            "bank_symbol": "A",
            "statements": [
                {
                    "category": "Pipelines",
                    "statement": "Pipelines are strong.",
                    "relevance_score": 8,
                    "is_new_category": False,
                }
            ],
        }
    }

    formatted_response = {
        "formatted_quotes": {
            "Bank A": [
                {
                    "category": "Pipelines",
                    "statement": "Pipelines are strong.",
                    "relevance_score": 8,
                    "formatted_quote": "<strong><u>Pipelines are strong</u></strong>.",
                    "is_new_category": False,
                }
            ]
        }
    }

    with patch(
        "aegis.etls.cm_readthrough.main._complete_with_tools_validated",
        new_callable=AsyncMock,
        return_value=formatted_response,
    ):
        result = await format_outlook_batch(
            all_outlook, sample_context, prompt_data=formatting_prompt_data
        )

    assert "Bank A" in result
    stmt = result["Bank A"]["statements"][0]
    assert "<strong><u>" in stmt["formatted_quote"]
    assert result["Bank A"]["bank_symbol"] == "A"


@pytest.mark.asyncio
async def test_format_outlook_batch_falls_back_on_failure(sample_context, formatting_prompt_data):
    """Batch formatter should return original data if LLM call fails."""
    all_outlook = {
        "Bank A": {
            "bank_symbol": "A",
            "statements": [
                {"category": "C", "statement": "S", "relevance_score": 5, "is_new_category": False}
            ],
        }
    }

    with patch(
        "aegis.etls.cm_readthrough.main._complete_with_tools_validated",
        new_callable=AsyncMock,
        side_effect=RuntimeError("LLM error"),
    ):
        result = await format_outlook_batch(
            all_outlook, sample_context, prompt_data=formatting_prompt_data
        )

    # Should fall back to original data
    assert result["Bank A"]["statements"][0]["statement"] == "S"


@pytest.mark.asyncio
async def test_format_outlook_batch_preserves_category_group(
    sample_context, formatting_prompt_data
):
    """Batch formatter should preserve category_group from original statements."""
    all_outlook = {
        "Bank A": {
            "bank_symbol": "A",
            "statements": [
                {
                    "category": "Pipelines",
                    "category_group": "Investment Banking",
                    "statement": "Pipelines are strong.",
                    "relevance_score": 8,
                    "is_new_category": False,
                },
                {
                    "category": "Trading",
                    "category_group": "Markets",
                    "statement": "Trading up.",
                    "relevance_score": 7,
                    "is_new_category": False,
                },
            ],
        }
    }

    formatted_response = {
        "formatted_quotes": {
            "Bank A": [
                {
                    "category": "Pipelines",
                    "statement": "Pipelines are strong.",
                    "relevance_score": 8,
                    "formatted_quote": "<strong><u>Pipelines are strong</u></strong>.",
                    "is_new_category": False,
                },
                {
                    "category": "Trading",
                    "statement": "Trading up.",
                    "relevance_score": 7,
                    "formatted_quote": "<strong><u>Trading up</u></strong>.",
                    "is_new_category": False,
                },
            ]
        }
    }

    with patch(
        "aegis.etls.cm_readthrough.main._complete_with_tools_validated",
        new_callable=AsyncMock,
        return_value=formatted_response,
    ):
        result = await format_outlook_batch(
            all_outlook, sample_context, prompt_data=formatting_prompt_data
        )

    stmts = result["Bank A"]["statements"]
    assert stmts[0]["category_group"] == "Investment Banking"
    assert stmts[1]["category_group"] == "Markets"


@pytest.fixture
def dedup_prompt_data():
    """Prompt payload for Q&A deduplication tests."""
    return {
        "system_prompt": "Identify duplicate questions.",
        "user_prompt": "Questions: {questions_xml}",
        "tool_definition": {
            "type": "function",
            "function": {"name": "report_qa_duplicates", "parameters": {}},
        },
    }


@pytest.mark.asyncio
async def test_deduplicate_qa_results_llm_removes_duplicates(sample_context, dedup_prompt_data):
    """LLM dedup should remove questions flagged as duplicates."""
    section2 = {
        "Bank A": {
            "bank_symbol": "A",
            "questions": [
                {
                    "category": "Risk",
                    "verbatim_question": "How is risk managed?",
                    "analyst_name": "J",
                },
                {
                    "category": "Risk",
                    "verbatim_question": "Risk management approach?",
                    "analyst_name": "J",
                },
                {
                    "category": "Volatility",
                    "verbatim_question": "Vol impact on flows?",
                    "analyst_name": "K",
                },
            ],
        }
    }
    section3 = {
        "Bank A": {
            "bank_symbol": "A",
            "questions": [
                {
                    "category": "M&A",
                    "verbatim_question": "M&A pipeline outlook?",
                    "analyst_name": "B",
                },
                {
                    "category": "M&A",
                    "verbatim_question": "Sponsor activity trends?",
                    "analyst_name": "C",
                },
            ],
        }
    }

    dedup_response = {
        "analysis_notes": "1 duplicate found",
        "duplicate_questions": [
            {
                "bank": "Bank A",
                "section": "section2",
                "category": "Risk",
                "question_index": 1,
                "duplicate_of_section": "section2",
                "duplicate_of_category": "Risk",
                "duplicate_of_question_index": 0,
                "reasoning": "Same question about risk management",
            }
        ],
    }

    with patch(
        "aegis.etls.cm_readthrough.main._complete_with_tools_validated",
        new_callable=AsyncMock,
        return_value=dedup_response,
    ):
        result_s2, result_s3 = await _deduplicate_qa_results_llm(
            section2, section3, sample_context, prompt_data=dedup_prompt_data
        )

    assert len(result_s2["Bank A"]["questions"]) == 2
    assert result_s2["Bank A"]["questions"][0]["verbatim_question"] == "How is risk managed?"
    assert result_s2["Bank A"]["questions"][1]["verbatim_question"] == "Vol impact on flows?"
    assert len(result_s3["Bank A"]["questions"]) == 2  # Unchanged


@pytest.mark.asyncio
async def test_deduplicate_qa_results_llm_skips_when_few_questions(
    sample_context, dedup_prompt_data
):
    """Dedup should be skipped when total questions < 5."""
    section2 = {
        "Bank A": {
            "bank_symbol": "A",
            "questions": [
                {"category": "Risk", "verbatim_question": "Q1", "analyst_name": "J"},
            ],
        }
    }
    section3 = {}

    # Should NOT call the LLM at all
    with patch(
        "aegis.etls.cm_readthrough.main._complete_with_tools_validated",
        new_callable=AsyncMock,
    ) as mock_llm:
        result_s2, result_s3 = await _deduplicate_qa_results_llm(
            section2, section3, sample_context, prompt_data=dedup_prompt_data
        )

    mock_llm.assert_not_called()
    assert len(result_s2["Bank A"]["questions"]) == 1


@pytest.mark.asyncio
async def test_extract_outlook_separates_emerging_categories(
    sample_bank_info, sample_outlook_categories, sample_context, outlook_prompt_data
):
    """Emerging categories should be separated from standard results."""
    llm_return = {
        "has_content": True,
        "statements": [
            {
                "category": "Investment Banking Pipelines",
                "statement": "Standard.",
                "relevance_score": 8,
                "is_new_category": False,
            },
            {
                "category": "Stablecoins",
                "statement": "Emerging theme.",
                "relevance_score": 6,
                "is_new_category": True,
            },
        ],
    }

    with patch(
        "aegis.etls.cm_readthrough.main._complete_with_tools_validated",
        new_callable=AsyncMock,
        return_value=llm_return,
    ):
        result = await extract_outlook_from_transcript(
            sample_bank_info,
            "Transcript text",
            sample_outlook_categories,
            2024,
            "Q3",
            sample_context,
            prompt_data=outlook_prompt_data,
        )

    # Standard statements only in main list
    assert len(result["statements"]) == 1
    assert result["statements"][0]["category"] == "Investment Banking Pipelines"

    # Emerging captured separately
    assert len(result["emerging_categories"]) == 1
    assert result["emerging_categories"][0]["category"] == "Stablecoins"
    assert result["has_content"] is True
    assert result["failed"] is False


@pytest.mark.asyncio
async def test_extract_questions_separates_emerging_categories(
    sample_bank_info, sample_qa_categories, sample_context, qa_prompt_data
):
    """Emerging Q&A categories should be separated from standard results."""
    llm_return = {
        "has_content": True,
        "questions": [
            {
                "category": "Market Volatility",
                "verbatim_question": "Standard Q?",
                "analyst_name": "A",
                "analyst_firm": "F",
                "is_new_category": False,
            },
            {
                "category": "Govt Spending",
                "verbatim_question": "New theme Q?",
                "analyst_name": "B",
                "analyst_firm": "G",
                "is_new_category": True,
            },
        ],
    }

    with patch(
        "aegis.etls.cm_readthrough.main._complete_with_tools_validated",
        new_callable=AsyncMock,
        return_value=llm_return,
    ):
        result = await extract_questions_from_qa(
            sample_bank_info,
            "Q&A text",
            sample_qa_categories,
            2024,
            "Q3",
            sample_context,
            prompt_data=qa_prompt_data,
        )

    assert len(result["questions"]) == 1
    assert result["questions"][0]["category"] == "Market Volatility"
    assert len(result["emerging_categories"]) == 1
    assert result["emerging_categories"][0]["category"] == "Govt Spending"


@pytest.mark.asyncio
async def test_extract_outlook_only_emerging_means_no_content(
    sample_bank_info, sample_outlook_categories, sample_context, outlook_prompt_data
):
    """If only emerging categories exist, has_content should be False."""
    llm_return = {
        "has_content": True,
        "statements": [
            {
                "category": "NewTheme",
                "statement": "Only emerging.",
                "relevance_score": 5,
                "is_new_category": True,
            },
        ],
    }

    with patch(
        "aegis.etls.cm_readthrough.main._complete_with_tools_validated",
        new_callable=AsyncMock,
        return_value=llm_return,
    ):
        result = await extract_outlook_from_transcript(
            sample_bank_info,
            "Transcript text",
            sample_outlook_categories,
            2024,
            "Q3",
            sample_context,
            prompt_data=outlook_prompt_data,
        )

    assert result["has_content"] is False
    assert len(result["statements"]) == 0
    assert len(result["emerging_categories"]) == 1
