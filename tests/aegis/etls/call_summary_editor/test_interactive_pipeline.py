"""Tests for the interactive pipeline helpers."""

from unittest.mock import AsyncMock, patch

import pytest

from aegis.etls.call_summary_editor.interactive_pipeline import (
    _primary_from_scores,
    _seed_selected_report_sentences,
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
            return_value={"conversations": [{"conversation_id": "conv_1", "block_indices": [1, 2]}]}
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
    first_call_messages = mock_call_tool.await_args_list[0].kwargs["messages"]
    first_user_prompt = next(
        message["content"] for message in first_call_messages if message["role"] == "user"
    )
    assert "<index>1</index>" in first_user_prompt
    assert "block=RY_QA_1" not in first_user_prompt
    second_call_messages = mock_call_tool.await_args_list[1].kwargs["messages"]
    retry_prompt = second_call_messages[-1]["content"]
    assert "The previous grouping was invalid" in retry_prompt
    assert "The only valid block indices are integers 1 through 2 inclusive." in retry_prompt
    assert "Only the integers inside `<index>` tags are valid block indices." in retry_prompt


@pytest.mark.asyncio
async def test_detect_qa_boundaries_raises_after_three_invalid_attempts():
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
        with pytest.raises(RuntimeError, match="failed validation after 3 attempts"):
            await detect_qa_boundaries(
                qa_raw_blocks=qa_raw_blocks,
                categories_text_qa="",
                context={"execution_id": "test-exec"},
                llm_params={"model": "gpt-test"},
            )

    assert mock_call_tool.await_count == 3


def test_count_included_categories_counts_auto_included_buckets():
    banks_data = {
        "RY-CA": {
            "md_blocks": [
                {
                    "sentences": [
                        {
                            "primary": "bucket_0",
                            "selected_bucket_id": "bucket_0",
                            "scores": {"bucket_0": 8.0},
                            "importance_score": 8.0,
                            "status": "selected",
                            "verbatim_text": "Revenue up",
                        },
                        {
                            "primary": "bucket_1",
                            "selected_bucket_id": "bucket_1",
                            "scores": {"bucket_1": 7.1},
                            "importance_score": 7.0,
                            "status": "selected",
                            "verbatim_text": "Expenses down",
                        },
                    ]
                }
            ],
            "qa_conversations": [],
        }
    }

    assert count_included_categories(banks_data, 4.0) == 2


def test_primary_from_scores_uses_best_positive_score_without_legacy_threshold():
    scores = {
        "bucket_0": 0.8,
        "bucket_1": 0.35,
        "bucket_2": 0.1,
    }

    assert _primary_from_scores(scores, ["bucket_0", "bucket_1", "bucket_2"]) == "bucket_0"


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
                        "verbatim_text": "We are scaling agentic AI across operations.",
                        "primary": "bucket_0",
                        "selected_bucket_id": "bucket_0",
                        "candidate_bucket_ids": ["bucket_0"],
                        "scores": {"bucket_0": 6.0},
                        "importance_score": 8.5,
                        "status": "selected",
                        "source_block_id": "RY-CA_MD_1",
                        "parent_record_id": "RY-CA_MD_1",
                        "transcript_section": "MD",
                    },
                    {
                        "sid": "md_2",
                        "text": "Agentic AI is now a standalone operating priority.",
                        "verbatim_text": "Agentic AI is now a standalone operating priority.",
                        "primary": "",
                        "selected_bucket_id": "",
                        "candidate_bucket_ids": ["bucket_0"],
                        "scores": {"bucket_0": 5.2},
                        "importance_score": 8.1,
                        "status": "candidate",
                        "source_block_id": "RY-CA_MD_1",
                        "parent_record_id": "RY-CA_MD_1",
                        "transcript_section": "MD",
                        "emerging_topic": True,
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
        side_effect=[
            {
                "proposals": [
                    {
                        "change_type": "update_existing",
                        "change_summary": (
                            "Expand the Efficiency row to cover AI-enabled productivity."
                        ),
                        "target_bucket_index": 0,
                        "target_category_name": "Efficiency",
                        "suggested_subtitle": "",
                        "linked_evidence_ids": ["md_1"],
                        "current_row": {
                            "transcript_sections": "MD",
                            "report_section": "Results Summary",
                            "category_name": "Efficiency",
                            "category_description": "Productivity and cost discipline.",
                            "example_1": "",
                            "example_2": "",
                            "example_3": "",
                        },
                        "proposed_row": {
                            "transcript_sections": "MD",
                            "report_section": "Results Summary",
                            "category_name": "Efficiency",
                            "category_description": (
                                "Productivity, cost discipline, and AI-enabled operating leverage."
                            ),
                            "example_1": "Scaling agentic AI across operations.",
                            "example_2": "",
                            "example_3": "",
                        },
                        "supporting_quotes": [
                            {
                                "evidence_id": "md_1",
                                "quote": "We are scaling agentic AI across operations.",
                                "speaker": "Chief Executive Officer, CEO",
                                "transcript_section": "MD",
                            }
                        ],
                    }
                ],
            },
            {
                "proposals": [
                    {
                        "change_type": "new_category",
                        "change_summary": (
                            "Create a new category for standalone agentic AI commentary."
                        ),
                        "target_bucket_index": -1,
                        "target_category_name": "Agentic AI",
                        "suggested_subtitle": "AI moves from pilot to operating model",
                        "linked_evidence_ids": ["md_2"],
                        "current_row": {
                            "transcript_sections": "ALL",
                            "report_section": "Results Summary",
                            "category_name": "",
                            "category_description": "",
                            "example_1": "",
                            "example_2": "",
                            "example_3": "",
                        },
                        "proposed_row": {
                            "transcript_sections": "MD",
                            "report_section": "Results Summary",
                            "category_name": "Agentic AI",
                            "category_description": (
                                "Strategic, operational, or financial commentary on "
                                "agentic AI deployment."
                            ),
                            "example_1": "Agentic AI is now a standalone operating priority.",
                            "example_2": "",
                            "example_3": "",
                        },
                        "supporting_quotes": [
                            {
                                "evidence_id": "md_2",
                                "quote": "Agentic AI is now a standalone operating priority.",
                                "speaker": "Chief Executive Officer, CEO",
                                "transcript_section": "MD",
                            }
                        ],
                    }
                ],
            },
        ]
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

    assert review["config_change_proposals"][0]["target_bucket_id"] == "bucket_0"
    assert (
        review["config_change_proposals"][0]["proposed_row"]["category_description"]
        == "Productivity, cost discipline, and AI-enabled operating leverage."
    )
    assert review["config_change_proposals"][1]["target_category_name"] == "Agentic AI"
    assert (
        review["config_change_proposals"][1]["proposed_row"]["report_section"]
        == "Results Summary"
    )
    assert review["config_change_proposals"][1]["linked_evidence_ids"] == ["md_2"]
    assert mock_tool.await_count == 2
    first_prompt = mock_tool.await_args_list[0].kwargs["messages"][-1]["content"]
    second_prompt = mock_tool.await_args_list[1].kwargs["messages"][-1]["content"]
    assert "<mapped_evidence>" in first_prompt
    assert "<id>md_1</id>" in first_prompt
    assert "<id>md_2</id>" not in first_prompt
    assert "<uncovered_evidence>" in second_prompt
    assert "<id>md_2</id>" in second_prompt


@pytest.mark.asyncio
async def test_analyze_config_coverage_uses_full_verbatim_evidence_without_importance_filter():
    long_quote = " ".join(["AI rollout remains broader than initially planned."] * 12)
    long_question = " ".join(["Can you unpack the AI rollout in more detail?"] * 10)
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
                        "sid": "md_low",
                        "text": long_quote,
                        "verbatim_text": long_quote,
                        "primary": "",
                        "selected_bucket_id": "",
                        "candidate_bucket_ids": [],
                        "scores": {},
                        "importance_score": 1.0,
                        "status": "candidate",
                        "source_block_id": "RY-CA_MD_1",
                        "parent_record_id": "RY-CA_MD_1",
                        "transcript_section": "MD",
                        "emerging_topic": True,
                    },
                    {
                        "sid": "md_rejected",
                        "text": "Legacy sentence classification missed this evidence.",
                        "verbatim_text": "Legacy sentence classification missed this evidence.",
                        "primary": "",
                        "selected_bucket_id": "",
                        "candidate_bucket_ids": [],
                        "scores": {},
                        "importance_score": 0.0,
                        "status": "rejected",
                        "source_block_id": "RY-CA_MD_1",
                        "parent_record_id": "RY-CA_MD_1",
                        "transcript_section": "MD",
                        "emerging_topic": False,
                    },
                ],
            }
        ],
        "qa_conversations": [
            {
                "id": "RY-CA_QA_1",
                "executive_name": "Chief Financial Officer",
                "executive_title": "CFO",
                "question_sentences": [
                    {
                        "sid": "qa_q1",
                        "text": long_question,
                        "verbatim_text": long_question,
                    }
                ],
                "answer_sentences": [
                    {
                        "sid": "qa_a1",
                        "text": long_quote,
                        "verbatim_text": long_quote,
                        "primary": "",
                        "selected_bucket_id": "",
                        "candidate_bucket_ids": [],
                        "scores": {},
                        "importance_score": 1.5,
                        "status": "candidate",
                        "source_block_id": "RY-CA_QA_1",
                        "parent_record_id": "RY-CA_QA_1",
                        "transcript_section": "QA",
                        "emerging_topic": True,
                    }
                ],
            }
        ],
    }
    categories = [
        {
            "transcript_sections": "ALL",
            "report_section": "Results Summary",
            "category_name": "Efficiency",
            "category_description": "Productivity and cost discipline.",
            "example_1": "",
            "example_2": "",
            "example_3": "",
        }
    ]

    mock_tool = AsyncMock(return_value={"proposals": []})

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

    assert review == {"config_change_proposals": []}
    prompt_messages = mock_tool.await_args.kwargs["messages"]
    user_prompt = next(message["content"] for message in prompt_messages if message["role"] == "user")
    assert "<id>md_low</id>" in user_prompt
    assert "<id>md_rejected</id>" in user_prompt
    assert f"<quote>{long_quote}</quote>" in user_prompt
    assert f"<question_context>{long_question}</question_context>" in user_prompt


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
            report_inclusion_threshold=4.0,
            selected_importance_threshold=6.5,
            candidate_importance_threshold=4.0,
            min_bucket_score_for_assignment=6.0,
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test"},
        )

    assert result["question_sentences"][0]["primary"] == "bucket_0"
    assert result["question_sentences"][0]["importance_score"] == 7.2
    assert result["question_sentences"][0]["source_block_id"] == "RY-CA_QA_1"
    assert result["question_sentences"][0]["status"] == "selected"
    assert result["question_sentences"][1]["primary"] == "bucket_1"
    assert result["question_sentences"][1]["importance_score"] == 6.8
    assert result["answer_sentences"][0]["primary"] == "bucket_0"
    assert result["answer_sentences"][0]["verbatim_text"] == "CET1 should remain strong."
    assert result["answer_sentences"][1]["primary"] == "bucket_1"


@pytest.mark.asyncio
async def test_classify_qa_conversation_prompt_mentions_auto_include_threshold():
    categories = [
        {
            "transcript_sections": "QA",
            "report_section": "Earnings Call Q&A",
            "category_name": "Capital",
            "category_description": "Capital and CET1 discussion.",
        }
    ]
    conv_blocks = [
        {
            "speaker": "Analyst",
            "speaker_affiliation": "Big Bank",
            "speaker_title": "",
            "speaker_type_hint": "q",
            "paragraphs": ["How are you thinking about CET1?"],
        },
        {
            "speaker": "Chief Financial Officer",
            "speaker_affiliation": "Royal Bank of Canada",
            "speaker_title": "CFO",
            "speaker_type_hint": "a",
            "paragraphs": ["CET1 should remain strong."],
        },
    ]

    mock_call_tool = AsyncMock(
        return_value={
            "primary_bucket_index": 0,
            "question_sentences": [],
            "answer_sentences": [],
        }
    )

    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        await classify_qa_conversation(
            conv_idx=1,
            conv_blocks=conv_blocks,
            ticker="RY-CA",
            categories=categories,
            categories_text_qa="",
            company_name="Royal Bank of Canada",
            fiscal_year=2026,
            fiscal_quarter="Q1",
            report_inclusion_threshold=4.0,
            selected_importance_threshold=6.5,
            candidate_importance_threshold=4.0,
            min_bucket_score_for_assignment=6.0,
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test"},
        )

    prompt_messages = mock_call_tool.await_args.kwargs["messages"]
    user_prompt = next(
        message["content"] for message in prompt_messages if message["role"] == "user"
    )
    assert "Use bucket `score` on a 0-10 relevance scale." in user_prompt
    assert "Scores >= 4.0 should remain visible for analyst review at minimum." in user_prompt


@pytest.mark.asyncio
async def test_classify_qa_conversation_rescales_normalized_bucket_scores():
    categories = [
        {
            "transcript_sections": "QA",
            "report_section": "Earnings Call Q&A",
            "category_name": "Capital",
            "category_description": "Capital and CET1 discussion.",
        }
    ]
    conv_blocks = [
        {
            "speaker": "Analyst",
            "speaker_affiliation": "Big Bank",
            "speaker_title": "",
            "speaker_type_hint": "q",
            "paragraphs": ["How are you thinking about CET1?"],
        },
        {
            "speaker": "Chief Financial Officer",
            "speaker_affiliation": "Royal Bank of Canada",
            "speaker_title": "CFO",
            "speaker_type_hint": "a",
            "paragraphs": ["CET1 should remain strong."],
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
                        "scores": [{"bucket_index": 0, "score": 0.82}],
                        "importance_score": 7.0,
                        "condensed": "How are you thinking about CET1?",
                    }
                ],
                "answer_sentences": [
                    {
                        "index": 1,
                        "scores": [{"bucket_index": 0, "score": 0.84}],
                        "importance_score": 7.1,
                        "condensed": "CET1 should remain strong.",
                    }
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
            report_inclusion_threshold=4.0,
            selected_importance_threshold=6.5,
            candidate_importance_threshold=4.0,
            min_bucket_score_for_assignment=6.0,
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test"},
        )

    assert result["primary_bucket"] == "bucket_0"
    assert result["answer_sentences"][0]["scores"]["bucket_0"] == 8.4
    assert result["answer_sentences"][0]["selected_bucket_id"] == "bucket_0"
    assert result["answer_sentences"][0]["status"] == "selected"


def test_seed_selected_report_sentences_promotes_mapped_candidates_when_preview_is_blank():
    processed_md = [
        {
            "sentences": [
                {
                    "sid": "md_1",
                    "status": "candidate",
                    "selected_bucket_id": "bucket_0",
                    "primary": "bucket_0",
                },
                {
                    "sid": "md_2",
                    "status": "candidate",
                    "selected_bucket_id": "",
                    "primary": "",
                },
            ]
        }
    ]
    processed_qa = [
        {
            "answer_sentences": [
                {
                    "sid": "qa_1",
                    "status": "candidate",
                    "selected_bucket_id": "bucket_1",
                    "primary": "bucket_1",
                }
            ]
        }
    ]

    summary = _seed_selected_report_sentences(processed_md, processed_qa)

    assert summary == {"promoted": 2, "md": 1, "qa": 1}
    assert processed_md[0]["sentences"][0]["status"] == "selected"
    assert processed_md[0]["sentences"][1]["status"] == "candidate"
    assert processed_qa[0]["answer_sentences"][0]["status"] == "selected"


@pytest.mark.asyncio
async def test_classify_qa_conversation_keeps_weak_bucket_match_as_unmapped_candidate():
    categories = [
        {
            "transcript_sections": "QA",
            "report_section": "Earnings Call Q&A",
            "category_name": "Capital",
            "category_description": "Capital and CET1 discussion.",
        }
    ]
    conv_blocks = [
        {
            "speaker": "Analyst",
            "speaker_affiliation": "Big Bank",
            "speaker_title": "",
            "speaker_type_hint": "q",
            "paragraphs": ["Can you talk about the AI rollout?"],
        },
        {
            "speaker": "Chief Financial Officer",
            "speaker_affiliation": "Royal Bank of Canada",
            "speaker_title": "CFO",
            "speaker_type_hint": "a",
            "paragraphs": ["We are scaling AI assistants across operations."],
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
                        "scores": [{"bucket_index": 0, "score": 4.2}],
                        "importance_score": 6.0,
                        "condensed": "Can you talk about the AI rollout?",
                    }
                ],
                "answer_sentences": [
                    {
                        "index": 1,
                        "scores": [{"bucket_index": 0, "score": 4.3}],
                        "importance_score": 6.4,
                        "condensed": "We are scaling AI assistants across operations.",
                    }
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
            report_inclusion_threshold=4.0,
            selected_importance_threshold=6.5,
            candidate_importance_threshold=4.0,
            min_bucket_score_for_assignment=6.0,
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test"},
        )

    assert result["answer_sentences"][0]["selected_bucket_id"] == ""
    assert result["answer_sentences"][0]["candidate_bucket_ids"] == ["bucket_0"]
    assert result["answer_sentences"][0]["status"] == "candidate"
    assert result["answer_sentences"][0]["emerging_topic"] is True
    assert result["primary_bucket"] == ""


@pytest.mark.asyncio
async def test_classify_qa_conversation_missing_sentence_results_are_rejected_not_emerging():
    categories = [
        {
            "transcript_sections": "QA",
            "report_section": "Earnings Call Q&A",
            "category_name": "Capital",
            "category_description": "Capital and CET1 discussion.",
        }
    ]
    conv_blocks = [
        {
            "speaker": "Analyst",
            "speaker_affiliation": "Big Bank",
            "speaker_title": "",
            "speaker_type_hint": "q",
            "paragraphs": ["How are you thinking about CET1?"],
        },
        {
            "speaker": "Chief Financial Officer",
            "speaker_affiliation": "Royal Bank of Canada",
            "speaker_title": "CFO",
            "speaker_type_hint": "a",
            "paragraphs": ["CET1 should remain strong."],
        },
    ]

    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=AsyncMock(
            return_value={
                "primary_bucket_index": 0,
                "question_sentences": [],
                "answer_sentences": [],
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
            report_inclusion_threshold=4.0,
            selected_importance_threshold=6.5,
            candidate_importance_threshold=4.0,
            min_bucket_score_for_assignment=6.0,
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test"},
        )

    assert result["primary_bucket"] == ""
    assert result["question_sentences"][0]["status"] == "rejected"
    assert result["question_sentences"][0]["emerging_topic"] is False
    assert (
        result["question_sentences"][0]["classification_error"]
        == "missing_sentence_classification"
    )
    assert result["answer_sentences"][0]["status"] == "rejected"
    assert result["answer_sentences"][0]["emerging_topic"] is False
    assert (
        result["answer_sentences"][0]["classification_error"]
        == "missing_sentence_classification"
    )
