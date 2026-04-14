"""Tests for the interactive pipeline helpers."""

from unittest.mock import AsyncMock, patch

import pytest

from aegis.etls.call_summary_editor.interactive_pipeline import (
    analyze_config_coverage,
    classify_qa_conversation,
    count_included_categories,
    detect_qa_boundaries,
)


@pytest.mark.asyncio
async def test_detect_qa_boundaries_uses_tool_output():
    qa_raw_blocks = [
        {
            "id": "RY_QA_1",
            "speaker": "Analyst",
            "speaker_title": "",
            "speaker_affiliation": "Big Bank",
            "speaker_type_hint": "q",
            "paragraphs": ["How should we think about NII?"],
        },
        {
            "id": "RY_QA_2",
            "speaker": "Chief Financial Officer",
            "speaker_title": "",
            "speaker_affiliation": "Royal Bank of Canada",
            "speaker_type_hint": "a",
            "paragraphs": ["We expect modest growth."],
        },
    ]

    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=AsyncMock(
            return_value={
                "conversations": [
                    {"conversation_id": "conv_1", "block_indices": [1, 2]}
                ]
            }
        ),
    ):
        conversations = await detect_qa_boundaries(
            qa_raw_blocks=qa_raw_blocks,
            categories_text_qa='<category index="0"><name>NII</name></category>',
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test"},
        )

    assert len(conversations) == 1
    assert [block["id"] for block in conversations[0]] == ["RY_QA_1", "RY_QA_2"]


@pytest.mark.asyncio
async def test_detect_qa_boundaries_retries_until_valid_output():
    qa_raw_blocks = [
        {
            "id": "RY_QA_1",
            "speaker": "Analyst",
            "speaker_type_hint": "q",
            "speaker_title": "",
            "speaker_affiliation": "",
            "paragraphs": ["Question?"],
        },
        {
            "id": "RY_QA_2",
            "speaker": "Chief Financial Officer",
            "speaker_type_hint": "a",
            "speaker_title": "",
            "speaker_affiliation": "",
            "paragraphs": ["Answer."],
        },
    ]

    mock_call_tool = AsyncMock(
        side_effect=[
            {"conversations": [{"conversation_id": "conv_1", "block_indices": [2, 1]}]},
            {"conversations": [{"conversation_id": "conv_1", "block_indices": [1, 2]}]},
        ]
    )

    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        conversations = await detect_qa_boundaries(
            qa_raw_blocks=qa_raw_blocks,
            categories_text_qa="",
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test"},
        )

    assert len(conversations) == 1
    assert [block["id"] for block in conversations[0]] == ["RY_QA_1", "RY_QA_2"]
    assert mock_call_tool.await_count == 2


@pytest.mark.asyncio
async def test_detect_qa_boundaries_uses_last_parseable_version_after_three_invalid_attempts():
    qa_raw_blocks = [
        {
            "id": "RY_QA_1",
            "speaker": "Analyst",
            "speaker_type_hint": "q",
            "speaker_title": "",
            "speaker_affiliation": "",
            "paragraphs": ["Question?"],
        },
        {
            "id": "RY_QA_2",
            "speaker": "Chief Financial Officer",
            "speaker_type_hint": "a",
            "speaker_title": "",
            "speaker_affiliation": "",
            "paragraphs": ["Answer."],
        },
    ]

    mock_call_tool = AsyncMock(
        side_effect=[
            {"conversations": [{"conversation_id": "conv_1", "block_indices": [1]}]},
            {"conversations": [{"conversation_id": "conv_1", "block_indices": [2, 1]}]},
            {"conversations": [{"conversation_id": "conv_1", "block_indices": [2]}]},
        ]
    )

    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        conversations = await detect_qa_boundaries(
            qa_raw_blocks=qa_raw_blocks,
            categories_text_qa="",
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test"},
        )

    assert len(conversations) == 1
    assert [block["id"] for block in conversations[0]] == ["RY_QA_2"]
    assert mock_call_tool.await_count == 3


def test_count_included_categories_excludes_other_bucket():
    banks_data = {
        "RY-CA": {
            "md_blocks": [
                {
                    "sentences": [
                        {"primary": "bucket_0", "importance_score": 8.0, "summary": "Revenue up"},
                        {"primary": "other", "importance_score": 9.0, "summary": "Greeting"},
                    ]
                }
            ],
            "qa_conversations": [],
        }
    }

    assert count_included_categories(banks_data, 4.0) == 1


@pytest.mark.asyncio
async def test_analyze_config_coverage_returns_existing_and_new_rows():
    bank_data = {
        "ticker": "RY-CA",
        "company_name": "Royal Bank of Canada",
        "fiscal_quarter": "Q1",
        "fiscal_year": 2026,
        "md_blocks": [
            {
                "speaker": "Chief Executive Officer",
                "speaker_title": "CEO",
                "sentences": [
                    {
                        "sid": "md_1",
                        "text": "We are scaling agentic AI across operations.",
                        "condensed": "Scaling agentic AI across operations.",
                        "primary": "other",
                        "importance_score": 8.5,
                    }
                ],
            }
        ],
        "qa_conversations": [],
    }
    categories = [
        {
            "transcript_sections": "MD",
            "report_section": "Results Summary",
            "category_name": "Efficiency",
            "category_description": "Productivity and cost discipline.",
            "example_1": "",
            "example_2": "",
            "example_3": "",
        }
    ]

    mock_tool = AsyncMock(
        return_value={
            "existing_section_updates": [
                {
                    "bucket_index": 0,
                    "category_name": "Efficiency",
                    "gap_summary": "The category does not explicitly mention AI-enabled productivity.",
                    "why_update": "New transcript language ties efficiency to agentic AI execution.",
                    "supporting_evidence": ["Scaling agentic AI across operations."],
                    "proposed_config_row": {
                        "transcript_sections": "MD",
                        "report_section": "Results Summary",
                        "category_name": "Efficiency",
                        "category_description": "Productivity, cost discipline, and AI-enabled operating leverage.",
                        "example_1": "Scaling agentic AI across operations.",
                        "example_2": "",
                        "example_3": "",
                    },
                }
            ],
            "new_section_suggestions": [
                {
                    "category_name": "Agentic AI",
                    "why_new_section": "AI now appears as a standalone strategic theme with repeatable importance.",
                    "supporting_evidence": ["Scaling agentic AI across operations."],
                    "suggested_subtitle": "AI moves from pilot to operating model",
                    "proposed_config_row": {
                        "transcript_sections": "MD",
                        "report_section": "Results Summary",
                        "category_name": "Agentic AI",
                        "category_description": "Strategic, operational, or financial commentary on agentic AI deployment.",
                        "example_1": "Scaling agentic AI across operations.",
                        "example_2": "",
                        "example_3": "",
                    },
                }
            ],
        }
    )

    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=mock_tool,
    ):
        review = await analyze_config_coverage(
            bank_data=bank_data,
            categories=categories,
            min_importance=4.0,
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test"},
        )

    assert review["existing_section_updates"][0]["bucket_id"] == "bucket_0"
    assert (
        review["existing_section_updates"][0]["proposed_config_row"]["category_description"]
        == "Productivity, cost discipline, and AI-enabled operating leverage."
    )
    assert review["new_section_suggestions"][0]["category_name"] == "Agentic AI"
    assert (
        review["new_section_suggestions"][0]["proposed_config_row"]["report_section"]
        == "Results Summary"
    )


@pytest.mark.asyncio
async def test_classify_qa_conversation_classifies_question_sentences_individually():
    categories = [
        {
            "transcript_sections": "QA",
            "report_section": "Earnings Call Q&A",
            "category_name": "Capital",
            "category_description": "Capital and CET1 discussion.",
        },
        {
            "transcript_sections": "QA",
            "report_section": "Earnings Call Q&A",
            "category_name": "Expenses",
            "category_description": "Expense outlook and efficiency discussion.",
        },
    ]
    conv_blocks = [
        {
            "speaker": "Analyst",
            "speaker_affiliation": "Big Bank",
            "speaker_title": "",
            "speaker_type_hint": "q",
            "paragraphs": ["How are you thinking about CET1? And what about expenses?"],
        },
        {
            "speaker": "Chief Financial Officer",
            "speaker_affiliation": "Royal Bank of Canada",
            "speaker_title": "CFO",
            "speaker_type_hint": "a",
            "paragraphs": ["CET1 should remain strong. Expenses should moderate."],
        },
    ]

    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=AsyncMock(
            return_value={
                "primary_bucket_index": 0,
                "question_sentences": [
                    {
                        "index": 1,
                        "scores": [{"bucket_index": 0, "score": 8.4}],
                        "importance_score": 7.2,
                        "condensed": "How are you thinking about CET1?",
                    },
                    {
                        "index": 2,
                        "scores": [{"bucket_index": 1, "score": 7.9}],
                        "importance_score": 6.8,
                        "condensed": "What about expenses?",
                    },
                ],
                "answer_sentences": [
                    {
                        "index": 1,
                        "scores": [{"bucket_index": 0, "score": 8.1}],
                        "importance_score": 7.0,
                        "condensed": "CET1 should remain strong.",
                    },
                    {
                        "index": 2,
                        "scores": [{"bucket_index": 1, "score": 7.4}],
                        "importance_score": 6.6,
                        "condensed": "Expenses should moderate.",
                    },
                ],
            }
        ),
    ):
        result = await classify_qa_conversation(
            conv_idx=1,
            conv_blocks=conv_blocks,
            ticker="RY-CA",
            categories=categories,
            categories_text_qa="",
            company_name="Royal Bank of Canada",
            fiscal_year=2026,
            fiscal_quarter="Q1",
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test"},
        )

    assert result["question_sentences"][0]["primary"] == "bucket_0"
    assert result["question_sentences"][0]["importance_score"] == 7.2
    assert result["question_sentences"][1]["primary"] == "bucket_1"
    assert result["question_sentences"][1]["importance_score"] == 6.8
    assert result["answer_sentences"][0]["primary"] == "bucket_0"
    assert result["answer_sentences"][1]["primary"] == "bucket_1"
