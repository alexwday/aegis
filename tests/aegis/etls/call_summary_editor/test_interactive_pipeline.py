"""Tests for the interactive pipeline helpers."""

import re
from unittest.mock import AsyncMock, patch

import pytest

from aegis.etls.call_summary_editor.interactive_pipeline import (
    FindingGroup,
    _primary_from_scores,
    _seed_selected_report_sentences,
    analyze_config_coverage,
    build_md_grouping_context,
    classify_md_block,
    classify_qa_conversation,
    count_included_categories,
    detect_qa_boundaries,
    format_categories_for_prompt,
    group_md_block_findings,
    group_qa_block_findings,
    repair_finding_groups,
)


def _qa_call_tool_dispatcher(classification_response):
    """Build an AsyncMock that returns singleton-group findings for grouping
    calls and ``classification_response`` for the exchange classification.

    Grouping calls are identified by tool.function.name. Grouping returns one
    finding per sentence so tests exercise the classification path with a
    1:1 sentence-to-finding mapping (matching the old sentence-level flow).
    """
    async def _dispatch(**kwargs):
        tool_name = kwargs["tool"]["function"]["name"]
        if tool_name in {"group_md_block_findings", "group_qa_block_findings"}:
            user_msg = next(m for m in kwargs["messages"] if m["role"] == "user")
            current_block = re.search(
                r"<current_block>(.*?)</current_block>", user_msg["content"], re.DOTALL
            )
            body = current_block.group(1) if current_block else user_msg["content"]
            count = len(re.findall(r"^\s*S\d+:", body, re.MULTILINE))
            return {"findings": [{"sentence_indices": [i]} for i in range(1, count + 1)]}
        if tool_name == "classify_qa_exchange":
            return classification_response
        return None
    return AsyncMock(side_effect=_dispatch)


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


def test_format_categories_for_prompt_keeps_legacy_single_cell_description():
    categories = [
        {
            "transcript_sections": "QA",
            "report_section": "Earnings Call Q&A",
            "category_name": "Capital",
            "category_description": "Capital and CET1 discussion.",
        }
    ]

    prompt_text = format_categories_for_prompt(categories, "QA")

    assert "<description_format>legacy_free_text</description_format>" in prompt_text
    assert "<description>Capital and CET1 discussion.</description>" in prompt_text
    assert "<topics>" not in prompt_text
    assert "<additional_sections>" not in prompt_text


def test_format_categories_for_prompt_supports_sectioned_single_cell_descriptions():
    categories = [
        {
            "transcript_sections": "ALL",
            "report_section": "Results Summary",
            "category_name": "Revenue",
            "category_description": (
                "Topics:\n"
                "- net interest income\n"
                "- fee income\n"
                "Keywords: NII, spread income, advisory fees\n"
                "Instructions:\n"
                "- Use when the main point is revenue drivers or revenue mix.\n"
                "Notes:\n"
                "- If expenses dominate, leave it in Efficiency."
            ),
        }
    ]

    prompt_text = format_categories_for_prompt(categories, "ALL")

    assert "<description_format>sectioned_lists</description_format>" in prompt_text
    assert "<topic>net interest income</topic>" in prompt_text
    assert "<keyword>NII</keyword>" in prompt_text
    assert "<keyword>spread income</keyword>" in prompt_text
    assert (
        "<instruction>Use when the main point is revenue drivers or revenue mix.</instruction>"
        in prompt_text
    )
    assert "<additional_sections>" in prompt_text
    assert '<section name="Notes">' in prompt_text
    assert "<item>If expenses dominate, leave it in Efficiency.</item>" in prompt_text


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

    # Pass 1 returns description updates; pass 2 returns emerging topics.
    # Both passes see the same findings digest so `md_1` and `md_2` both
    # appear in each prompt.
    mock_tool = AsyncMock(
        side_effect=[
            {
                "proposals": [
                    {
                        "target_category_name": "Efficiency",
                        "change_summary": (
                            "Expand the Efficiency row to cover AI-enabled productivity."
                        ),
                        "proposed_description": (
                            "Productivity, cost discipline, and AI-enabled operating leverage."
                        ),
                    }
                ],
            },
            {
                "proposals": [
                    {
                        "change_summary": (
                            "Create a new category for standalone agentic AI commentary."
                        ),
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
                        "linked_finding_ids": ["md_2"],
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

    proposals = review["config_change_proposals"]
    assert len(proposals) == 2
    # Pass 1: description update preserves the existing row and swaps only the description.
    assert proposals[0]["change_type"] == "update_existing"
    assert proposals[0]["target_bucket_id"] == "bucket_0"
    assert proposals[0]["target_category_name"] == "Efficiency"
    assert (
        proposals[0]["proposed_row"]["category_description"]
        == "Productivity, cost discipline, and AI-enabled operating leverage."
    )
    assert proposals[0]["proposed_row"]["category_name"] == "Efficiency"
    assert "supporting_quotes" not in proposals[0]
    # Pass 2: emerging topic carries linked finding ids for the UI to reassign.
    assert proposals[1]["change_type"] == "new_category"
    assert proposals[1]["target_category_name"] == "Agentic AI"
    assert proposals[1]["proposed_row"]["report_section"] == "Results Summary"
    assert proposals[1]["linked_evidence_ids"] == ["md_2"]
    assert mock_tool.await_count == 2
    first_prompt = mock_tool.await_args_list[0].kwargs["messages"][-1]["content"]
    second_prompt = mock_tool.await_args_list[1].kwargs["messages"][-1]["content"]
    # Both passes see the same findings digest — pass 2 needs to consider
    # findings that are already mapped too, in case they should move under
    # a new emerging topic.
    assert "<findings>" in first_prompt
    assert "<id>md_1</id>" in first_prompt
    assert "<id>md_2</id>" in first_prompt
    assert "<findings>" in second_prompt
    assert "<id>md_1</id>" in second_prompt
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
async def test_analyze_config_coverage_prompt_requests_sectioned_single_cell_descriptions():
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
                        "primary": "",
                        "selected_bucket_id": "",
                        "candidate_bucket_ids": [],
                        "scores": {},
                        "importance_score": 8.5,
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

    mock_tool = AsyncMock(side_effect=[{"proposals": []}, {"proposals": []}])

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
    update_prompt = mock_tool.await_args_list[0].kwargs["messages"][-1]["content"]
    emerging_prompt = mock_tool.await_args_list[1].kwargs["messages"][-1]["content"]
    for prompt_text in (update_prompt, emerging_prompt):
        assert "Keep `category_description` as one multiline cell" in prompt_text
        assert "Topics:" in prompt_text
        assert "Keywords:" in prompt_text
        assert "Instructions:" in prompt_text
        assert "Optional extra headings like `Notes:` or `Overrides:` are allowed" in prompt_text
        assert "Use section headings and short list items, not paragraph prose." in prompt_text
    assert "The proposal must be additive only" in update_prompt
    assert "Never remove, narrow, or overwrite" in update_prompt
    assert "`Keywords` are hint fields for non-exhaustive strong phrases" in update_prompt
    assert "## Category Name Guidance" in emerging_prompt
    assert "Treat `category_name` as a reusable taxonomy label" in emerging_prompt
    assert "Use a generalized, bank-agnostic title" in emerging_prompt
    assert "Do not use sentence-like titles" in emerging_prompt
    assert "Example existing titles:" in emerging_prompt
    assert "- Efficiency" in emerging_prompt


@pytest.mark.asyncio
async def test_analyze_config_coverage_merges_structured_updates_additively():
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
                        "text": "Deposit migration is picking up.",
                        "verbatim_text": "Deposit migration is picking up.",
                        "primary": "bucket_0",
                        "selected_bucket_id": "bucket_0",
                        "candidate_bucket_ids": ["bucket_0"],
                        "scores": {"bucket_0": 7.1},
                        "importance_score": 7.3,
                        "status": "selected",
                        "source_block_id": "RY-CA_MD_1",
                        "parent_record_id": "RY-CA_MD_1",
                        "transcript_section": "MD",
                        "emerging_topic": False,
                    }
                ],
            }
        ],
        "qa_conversations": [],
    }
    categories = [
        {
            "transcript_sections": "ALL",
            "report_section": "Results Summary",
            "category_name": "Deposits & Funding",
            "category_description": (
                "Topics:\n"
                "- deposit growth\n"
                "Keywords:\n"
                "- deposit beta\n"
                "Instructions:\n"
                "- Use when the main point is deposit economics."
            ),
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
                        "target_category_name": "Deposits & Funding",
                        "change_summary": "Add migration commentary to the keyword field.",
                        "proposed_description": (
                            "Topics:\n"
                            "- deposit growth\n"
                            "Keywords:\n"
                            "- migration\n"
                            "Instructions:\n"
                            "- Use when the main point is deposit economics."
                        ),
                    }
                ]
            },
            {"proposals": []},
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

    proposals = review["config_change_proposals"]
    assert len(proposals) == 1
    merged_description = proposals[0]["proposed_row"]["category_description"]
    assert "- deposit growth" in merged_description
    assert "- deposit beta" in merged_description
    assert "- migration" in merged_description
    assert merged_description.count("Keywords:") == 1


@pytest.mark.asyncio
async def test_classify_qa_conversation_treats_question_sentences_as_context_only():
    """Analyst question sentences must never receive a bucket assignment.

    Analysts say arbitrary things (greetings, unrelated questions, follow-ups)
    and we deliberately do not summarise them as standalone findings. Question
    records exist so they still render in the transcript, but they carry no
    primary bucket, no scores, no importance, and a dedicated ``"context"``
    status that distinguishes deliberate omission from a parse failure.
    """
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
        new=_qa_call_tool_dispatcher(
            {
                "primary_bucket_index": 0,
                "analyst_question_summary": "on CET1 capital and expense outlook",
                "answer_findings": [
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

    # Question sentences are context-only: no bucket, no scores, dedicated status.
    for question_record in result["question_sentences"]:
        assert question_record["primary"] == ""
        assert question_record["selected_bucket_id"] == ""
        assert question_record["candidate_bucket_ids"] == []
        assert question_record["scores"] == {}
        assert question_record["importance_score"] == 0.0
        assert question_record["status"] == "context"
        assert question_record["emerging_topic"] is False
    # Answer sentences continue to be classified normally.
    assert result["answer_sentences"][0]["primary"] == "bucket_0"
    assert result["answer_sentences"][0]["verbatim_text"] == "CET1 should remain strong."
    assert result["answer_sentences"][1]["primary"] == "bucket_1"
    # Analyst question summary is surfaced for the report card prefix.
    assert result["analyst_question_summary"] == "on CET1 capital and expense outlook"
    # Turns preserve back-and-forth order with role tags.
    assert [turn["role"] for turn in result["turns"]] == ["q", "a"]
    assert result["turns"][0]["sentences"][0]["status"] == "context"
    assert result["turns"][1]["sentences"][0]["primary"] == "bucket_0"


@pytest.mark.asyncio
async def test_classify_qa_conversation_prompt_supports_sectioned_description_cells():
    categories = [
        {
            "transcript_sections": "QA",
            "report_section": "Earnings Call Q&A",
            "category_name": "Revenue",
            "category_description": (
                "Topics:\n"
                "- net interest income\n"
                "Keywords: NII, margin\n"
                "Instructions:\n"
                "- Use for revenue drivers.\n"
                "Overrides:\n"
                "- If the point is purely about expenses, do not use this category."
            ),
        }
    ]
    conv_blocks = [
        {
            "speaker": "Analyst",
            "speaker_affiliation": "Big Bank",
            "speaker_title": "",
            "speaker_type_hint": "q",
            "paragraphs": ["How should we think about NII?"],
        },
        {
            "speaker": "Chief Financial Officer",
            "speaker_affiliation": "Royal Bank of Canada",
            "speaker_title": "CFO",
            "speaker_type_hint": "a",
            "paragraphs": ["NII should remain resilient through the year."],
        },
    ]

    mock_call_tool = _qa_call_tool_dispatcher(
        {
            "primary_bucket_index": 0,
            "analyst_question_summary": "on NII outlook",
            "answer_findings": [
                {
                    "index": 1,
                    "scores": [{"bucket_index": 0, "score": 8.2}],
                    "importance_score": 7.4,
                    "condensed": "NII should remain resilient through the year.",
                }
            ],
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
            categories_text_qa=format_categories_for_prompt(categories, "QA"),
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

    classification_call = next(
        call
        for call in mock_call_tool.await_args_list
        if call.kwargs["tool"]["function"]["name"] == "classify_qa_exchange"
    )
    user_prompt = next(
        message["content"]
        for message in classification_call.kwargs["messages"]
        if message["role"] == "user"
    )
    assert "The main headings to expect are `<topics>`, `<keywords>`, and `<instructions>`." in user_prompt
    assert "<keyword>NII</keyword>" in user_prompt
    assert "<additional_sections>" in user_prompt
    assert '<section name="Overrides">' in user_prompt


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

    mock_call_tool = _qa_call_tool_dispatcher(
        {
            "primary_bucket_index": 0,
            "analyst_question_summary": "on CET1 outlook",
            "answer_findings": [],
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

    classification_call = next(
        call
        for call in mock_call_tool.await_args_list
        if call.kwargs["tool"]["function"]["name"] == "classify_qa_exchange"
    )
    user_prompt = next(
        message["content"]
        for message in classification_call.kwargs["messages"]
        if message["role"] == "user"
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
        new=_qa_call_tool_dispatcher(
            {
                "primary_bucket_index": 0,
                "analyst_question_summary": "on CET1 outlook",
                "answer_findings": [
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
        new=_qa_call_tool_dispatcher(
            {
                "primary_bucket_index": 0,
                "analyst_question_summary": "on the AI rollout",
                "answer_findings": [
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
async def test_classify_qa_conversation_missing_finding_results_are_rejected_not_emerging():
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
        new=_qa_call_tool_dispatcher(
            {
                "primary_bucket_index": 0,
                "analyst_question_summary": "on CET1 outlook",
                "answer_findings": [],
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
    # Question records are context-only regardless of LLM response shape \u2014
    # no classification_error, no rejected status.
    assert result["question_sentences"][0]["status"] == "context"
    assert result["question_sentences"][0]["emerging_topic"] is False
    assert "classification_error" not in result["question_sentences"][0]
    # Answer records that the model failed to address still fall back to
    # rejected, since the LLM was supposed to classify them.
    assert result["answer_sentences"][0]["status"] == "rejected"
    assert result["answer_sentences"][0]["emerging_topic"] is False
    assert (
        result["answer_sentences"][0]["classification_error"]
        == "missing_sentence_classification"
    )


# ── Finding grouping helpers ───────────────────────────────────────────────


def _make_groups(*index_lists: list) -> list:
    return [FindingGroup(sentence_indices=list(indices)) for indices in index_lists]


def test_repair_finding_groups_accepts_valid_contiguous_groups():
    groups = _make_groups([1, 2, 3], [4], [5, 6])
    repaired = repair_finding_groups(groups, total_sentences=6)
    assert [group.sentence_indices for group in repaired] == [[1, 2, 3], [4], [5, 6]]


def test_repair_finding_groups_fills_gaps_with_singletons():
    groups = _make_groups([1, 2], [5])
    repaired = repair_finding_groups(groups, total_sentences=6)
    assert [group.sentence_indices for group in repaired] == [[1, 2], [3], [4], [5], [6]]


def test_repair_finding_groups_rejects_overlapping_groups_keeping_first():
    groups = _make_groups([1, 2, 3], [2, 3, 4])
    repaired = repair_finding_groups(groups, total_sentences=4)
    assert [group.sentence_indices for group in repaired] == [[1, 2, 3], [4]]


def test_repair_finding_groups_rejects_non_contiguous_groups():
    groups = _make_groups([1, 3], [2])
    repaired = repair_finding_groups(groups, total_sentences=3)
    # Non-contiguous [1,3] rejected; [2] accepted; 1 and 3 filled as singletons.
    # Ordering is by minimum index ascending: [1], [2], [3].
    assert [group.sentence_indices for group in repaired] == [[1], [2], [3]]


def test_repair_finding_groups_rejects_out_of_range_indices():
    groups = _make_groups([1, 2], [5, 6])
    repaired = repair_finding_groups(groups, total_sentences=3)
    assert [group.sentence_indices for group in repaired] == [[1, 2], [3]]


def test_repair_finding_groups_handles_empty_llm_output():
    repaired = repair_finding_groups([], total_sentences=3)
    assert [group.sentence_indices for group in repaired] == [[1], [2], [3]]


def _make_md_block(speaker: str, text: str, **extra) -> dict:
    return {
        "id": extra.get("id", f"block_{speaker.lower().replace(' ', '_')}"),
        "speaker": speaker,
        "speaker_title": extra.get("speaker_title", ""),
        "speaker_affiliation": extra.get("speaker_affiliation", ""),
        "paragraphs": [text] if text else [],
    }


def test_build_md_grouping_context_returns_empty_for_first_block():
    blocks = [_make_md_block("CEO", "Opening remarks for the quarter.")]
    assert build_md_grouping_context(0, blocks) == ""


def test_build_md_grouping_context_returns_immediate_prior_block_when_long_enough():
    long_text = "Lorem ipsum " * 30  # well over 200 chars
    blocks = [
        _make_md_block("CEO", long_text),
        _make_md_block("CFO", "Thanks."),
    ]
    context = build_md_grouping_context(1, blocks, min_chars=200, max_blocks_back=3)
    assert "CEO" in context
    assert long_text.strip() in context
    # Shouldn't include the current block.
    assert "CFO" not in context


def test_build_md_grouping_context_extends_backward_for_short_preceding_blocks():
    blocks = [
        _make_md_block("Operator", "Please welcome today's speakers."),
        _make_md_block("CEO", "Thanks."),  # short
        _make_md_block("CFO", "Let me add."),  # short
        _make_md_block("Analyst", "Next speaker."),  # current
    ]
    context = build_md_grouping_context(3, blocks, min_chars=200, max_blocks_back=3)
    # Should include multiple prior blocks to accumulate context.
    assert "CFO" in context
    assert "CEO" in context
    # Analyst (current block) excluded.
    assert "Next speaker." not in context


def test_build_md_grouping_context_caps_at_three_blocks_back():
    blocks = [
        _make_md_block("A", "One."),
        _make_md_block("B", "Two."),
        _make_md_block("C", "Three."),
        _make_md_block("D", "Four."),
        _make_md_block("E", "Five."),
    ]
    context = build_md_grouping_context(4, blocks, min_chars=10_000, max_blocks_back=3)
    # Despite not hitting min_chars, should stop after 3 blocks back: B, C, D.
    assert "A: One." not in context
    assert "B: Two." in context
    assert "C: Three." in context
    assert "D: Four." in context
    assert "E: Five." not in context


def test_build_md_grouping_context_orders_oldest_to_newest():
    blocks = [
        _make_md_block("A", "first."),
        _make_md_block("B", "second."),
        _make_md_block("C", "third."),
    ]
    context = build_md_grouping_context(2, blocks, min_chars=10_000, max_blocks_back=3)
    # Prior blocks A then B, in transcript order.
    assert context.find("A: first.") < context.find("B: second.")


@pytest.mark.asyncio
async def test_group_md_block_findings_returns_findings_on_happy_path():
    mock_call_tool = AsyncMock(
        return_value={"findings": [{"sentence_indices": [1, 2]}, {"sentence_indices": [3]}]}
    )
    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        groups = await group_md_block_findings(
            block_id="RY_MD_1",
            speaker_line="CEO",
            sentences=["S1 text.", "S2 text.", "S3 text."],
            prior_context="",
            categories_text_md="",
            context={"execution_id": "test"},
            llm_params={"model": "gpt-test"},
        )

    assert [group.sentence_indices for group in groups] == [[1, 2], [3]]
    assert mock_call_tool.await_count == 1


@pytest.mark.asyncio
async def test_group_md_block_findings_retries_on_invalid_then_repairs_on_final_failure():
    mock_call_tool = AsyncMock(
        side_effect=[
            # Attempt 1: non-contiguous (invalid)
            {"findings": [{"sentence_indices": [1, 3]}, {"sentence_indices": [2]}]},
            # Attempt 2: missing sentence 3 (invalid coverage)
            {"findings": [{"sentence_indices": [1, 2]}]},
        ]
    )
    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        groups = await group_md_block_findings(
            block_id="RY_MD_1",
            speaker_line="CEO",
            sentences=["S1.", "S2.", "S3."],
            prior_context="",
            categories_text_md="",
            context={"execution_id": "test"},
            llm_params={"model": "gpt-test"},
            max_retries=1,
        )

    # After retries fail, repair falls back to keeping accepted groups + filling gaps.
    # Final LLM groups were [[1,2]]; repair fills [3] as singleton.
    assert [group.sentence_indices for group in groups] == [[1, 2], [3]]
    assert mock_call_tool.await_count == 2


@pytest.mark.asyncio
async def test_group_md_block_findings_passes_prior_context_to_prompt():
    mock_call_tool = AsyncMock(
        return_value={"findings": [{"sentence_indices": [1]}]}
    )
    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        await group_md_block_findings(
            block_id="RY_MD_2",
            speaker_line="CFO",
            sentences=["Only one sentence."],
            prior_context="CEO: Opening remarks for the quarter.",
            categories_text_md="",
            context={"execution_id": "test"},
            llm_params={"model": "gpt-test"},
        )

    user_prompt = next(
        message["content"]
        for message in mock_call_tool.await_args.kwargs["messages"]
        if message["role"] == "user"
    )
    assert "CEO: Opening remarks for the quarter." in user_prompt
    assert "prior_speaker_context" in user_prompt
    assert 'S1: "Only one sentence."' in user_prompt


@pytest.mark.asyncio
async def test_group_qa_block_findings_includes_exchange_context():
    mock_call_tool = AsyncMock(
        return_value={"findings": [{"sentence_indices": [1, 2]}]}
    )
    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        await group_qa_block_findings(
            conversation_id="RY_QA_conv_1",
            block_id="RY_QA_2",
            speaker_role="a",
            speaker_line="CFO",
            sentences=["We expect modest growth.", "That reflects our baseline."],
            exchange_context="ANALYST (Big Bank):\n  \"How should we think about NII?\"",
            context={"execution_id": "test"},
            llm_params={"model": "gpt-test"},
        )

    user_prompt = next(
        message["content"]
        for message in mock_call_tool.await_args.kwargs["messages"]
        if message["role"] == "user"
    )
    assert "How should we think about NII?" in user_prompt
    assert "EXECUTIVE (CFO)" in user_prompt
    assert 'S1: "We expect modest growth."' in user_prompt


@pytest.mark.asyncio
async def test_group_md_block_findings_empty_sentences_returns_no_groups():
    # Shouldn't call the LLM if there are no sentences to group.
    mock_call_tool = AsyncMock(return_value=None)
    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        groups = await group_md_block_findings(
            block_id="RY_MD_1",
            speaker_line="CEO",
            sentences=[],
            prior_context="",
            categories_text_md="",
            context={"execution_id": "test"},
            llm_params={"model": "gpt-test"},
        )

    assert groups == []
    assert mock_call_tool.await_count == 0


@pytest.mark.asyncio
async def test_classify_md_block_makes_grouping_then_classification_call():
    """classify_md_block issues a grouping call followed by a classification call."""
    categories = [
        {
            "transcript_sections": "MD",
            "report_section": "Results Summary",
            "category_name": "Capital",
            "category_description": "Capital discussion.",
        },
    ]
    block_raw = {
        "id": "RY_MD_1",
        "speaker": "CEO",
        "speaker_title": "CEO",
        "speaker_affiliation": "Royal Bank of Canada",
        "paragraphs": ["CET1 was strong. We remain well capitalized."],
    }

    mock_call_tool = AsyncMock(
        side_effect=[
            {"findings": [{"sentence_indices": [1, 2]}]},
            {
                "findings": [
                    {
                        "index": 1,
                        "scores": [{"bucket_index": 0, "score": 8.1}],
                        "importance_score": 7.5,
                        "condensed": "CET1 strong; well capitalized.",
                    }
                ]
            },
        ]
    )
    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        result = await classify_md_block(
            block_raw=block_raw,
            block_index=0,
            all_md_blocks=[block_raw],
            categories=categories,
            categories_text_md="",
            company_name="RBC",
            fiscal_year=2026,
            fiscal_quarter="Q1",
            report_inclusion_threshold=4.0,
            selected_importance_threshold=6.5,
            candidate_importance_threshold=4.0,
            min_bucket_score_for_assignment=3.0,
            context={"execution_id": "test"},
            llm_params={"model": "gpt-test"},
        )

    assert mock_call_tool.await_count == 2
    grouping_label = mock_call_tool.await_args_list[0].kwargs["label"]
    classification_label = mock_call_tool.await_args_list[1].kwargs["label"]
    assert grouping_label.startswith("md_group:RY_MD_1")
    assert classification_label == "md_block:RY_MD_1"

    findings = result["sentences"]
    assert len(findings) == 1
    assert findings[0]["sid"] == "RY_MD_1_f0"
    assert findings[0]["sentence_ids"] == ["RY_MD_1_s0", "RY_MD_1_s1"]
    assert findings[0]["text"] == "CET1 was strong. We remain well capitalized."
    assert findings[0]["condensed"] == "CET1 strong; well capitalized."
    assert findings[0]["primary"] == "bucket_0"


@pytest.mark.asyncio
async def test_classify_md_block_returns_empty_for_block_with_no_sentences():
    """Empty blocks skip both the grouping and classification LLM calls."""
    block_raw = {
        "id": "RY_MD_1",
        "speaker": "CEO",
        "speaker_title": "CEO",
        "speaker_affiliation": "RBC",
        "paragraphs": [""],
    }
    mock_call_tool = AsyncMock(return_value=None)
    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        result = await classify_md_block(
            block_raw=block_raw,
            block_index=0,
            all_md_blocks=[block_raw],
            categories=[],
            categories_text_md="",
            company_name="RBC",
            fiscal_year=2026,
            fiscal_quarter="Q1",
            report_inclusion_threshold=4.0,
            selected_importance_threshold=6.5,
            candidate_importance_threshold=4.0,
            min_bucket_score_for_assignment=3.0,
            context={"execution_id": "test"},
            llm_params={"model": "gpt-test"},
        )

    assert result["sentences"] == []
    assert mock_call_tool.await_count == 0


@pytest.mark.asyncio
async def test_classify_md_block_passes_prior_block_context_into_prompt():
    """The grouping prompt should carry the preceding speaker block as context."""
    long_prior = "A " * 250
    prior_block = {
        "id": "RY_MD_1",
        "speaker": "Operator",
        "speaker_title": "",
        "speaker_affiliation": "",
        "paragraphs": [long_prior],
    }
    current_block = {
        "id": "RY_MD_2",
        "speaker": "CEO",
        "speaker_title": "CEO",
        "speaker_affiliation": "RBC",
        "paragraphs": ["Let me begin."],
    }

    mock_call_tool = AsyncMock(
        side_effect=[
            {"findings": [{"sentence_indices": [1]}]},
            {
                "findings": [
                    {
                        "index": 1,
                        "scores": [],
                        "importance_score": 2.0,
                        "condensed": "Begin.",
                    }
                ]
            },
        ]
    )
    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        await classify_md_block(
            block_raw=current_block,
            block_index=1,
            all_md_blocks=[prior_block, current_block],
            categories=[],
            categories_text_md="",
            company_name="RBC",
            fiscal_year=2026,
            fiscal_quarter="Q1",
            report_inclusion_threshold=4.0,
            selected_importance_threshold=6.5,
            candidate_importance_threshold=4.0,
            min_bucket_score_for_assignment=3.0,
            context={"execution_id": "test"},
            llm_params={"model": "gpt-test"},
        )

    grouping_prompt = next(
        message["content"]
        for message in mock_call_tool.await_args_list[0].kwargs["messages"]
        if message["role"] == "user"
    )
    assert "Operator" in grouping_prompt
    assert "A A A" in grouping_prompt


@pytest.mark.asyncio
async def test_classify_qa_conversation_groups_each_block_in_parallel():
    """Grouping calls fire once per block; classification then runs once."""
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
            "paragraphs": ["First analyst sentence.", "Second analyst sentence."],
        },
        {
            "speaker": "Chief Financial Officer",
            "speaker_affiliation": "RBC",
            "speaker_title": "CFO",
            "speaker_type_hint": "a",
            "paragraphs": ["Exec reply A1.", "Exec reply A2."],
        },
    ]

    mock_call_tool = _qa_call_tool_dispatcher(
        {
            "primary_bucket_index": 0,
            "analyst_question_summary": "on the opening question",
            "answer_findings": [
                {
                    "index": 1,
                    "scores": [{"bucket_index": 0, "score": 7.5}],
                    "importance_score": 7.0,
                    "condensed": "Exec reply A1.",
                },
                {
                    "index": 2,
                    "scores": [{"bucket_index": 0, "score": 6.2}],
                    "importance_score": 5.5,
                    "condensed": "Exec reply A2.",
                },
            ],
        }
    )

    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        result = await classify_qa_conversation(
            conv_idx=1,
            conv_blocks=conv_blocks,
            ticker="RY-CA",
            categories=categories,
            categories_text_qa="",
            company_name="RBC",
            fiscal_year=2026,
            fiscal_quarter="Q1",
            report_inclusion_threshold=4.0,
            selected_importance_threshold=6.5,
            candidate_importance_threshold=4.0,
            min_bucket_score_for_assignment=3.0,
            context={"execution_id": "test"},
            llm_params={"model": "gpt-test"},
        )

    tool_names = [
        call.kwargs["tool"]["function"]["name"] for call in mock_call_tool.await_args_list
    ]
    assert tool_names.count("group_qa_block_findings") == 2
    assert tool_names.count("classify_qa_exchange") == 1
    assert len(result["question_sentences"]) == 2
    assert len(result["answer_sentences"]) == 2
    assert result["answer_sentences"][0]["sid"] == "RY-CA_QA_1_af0"
    assert result["answer_sentences"][0]["sentence_ids"] == ["RY-CA_QA_1_as0"]
    assert result["question_sentences"][0]["sid"] == "RY-CA_QA_1_qf0"
    assert result["question_sentences"][0]["status"] == "context"


@pytest.mark.asyncio
async def test_classify_qa_conversation_skips_operator_for_executive_attribution():
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
            "paragraphs": ["Can you walk through capital?"],
        },
        {
            "speaker": "Operator",
            "speaker_affiliation": "",
            "speaker_title": "",
            "speaker_type_hint": "a",
            "paragraphs": ["Our next question comes from Jane Analyst."],
        },
        {
            "speaker": "Chief Financial Officer",
            "speaker_affiliation": "RBC",
            "speaker_title": "CFO",
            "speaker_type_hint": "a",
            "paragraphs": ["Capital remains strong and earnings drove the build."],
        },
    ]

    mock_call_tool = _qa_call_tool_dispatcher(
        {
            "primary_bucket_index": 0,
            "analyst_question_summary": "on capital generation",
            "answer_findings": [
                {
                    "index": 1,
                    "scores": [],
                    "importance_score": 1.0,
                    "condensed": "Operator handoff.",
                },
                {
                    "index": 2,
                    "scores": [{"bucket_index": 0, "score": 8.4}],
                    "importance_score": 7.4,
                    "condensed": "Capital remains strong.",
                },
            ],
        }
    )

    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        result = await classify_qa_conversation(
            conv_idx=1,
            conv_blocks=conv_blocks,
            ticker="RY-CA",
            categories=categories,
            categories_text_qa="",
            company_name="RBC",
            fiscal_year=2026,
            fiscal_quarter="Q1",
            report_inclusion_threshold=4.0,
            selected_importance_threshold=6.5,
            candidate_importance_threshold=4.0,
            min_bucket_score_for_assignment=6.0,
            context={"execution_id": "test"},
            llm_params={"model": "gpt-test"},
        )

    assert result["executive_name"] == "Chief Financial Officer"
    assert result["executive_title"] == "CFO"
    assert result["executive_affiliation"] == "RBC"
    assert result["answer_sentences"][0]["speaker"] == "Operator"
    assert result["answer_sentences"][0]["speaker_title"] == ""
    assert result["answer_sentences"][1]["speaker"] == "Chief Financial Officer"
    assert result["answer_sentences"][1]["speaker_title"] == "CFO"
    assert result["answer_sentences"][1]["speaker_affiliation"] == "RBC"
    assert result["answer_sentences"][1]["selected_bucket_id"] == "bucket_0"
