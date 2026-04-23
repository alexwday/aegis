"""Tests for CM readthrough editor pipeline helpers."""

from unittest.mock import AsyncMock, patch

import pytest

from aegis.etls.cm_readthrough_editor.interactive_pipeline import _speaker_role
from aegis.etls.cm_readthrough_editor.interactive_pipeline import (
    QAQuestion,
    _call_tool,
    _extract_outlook_for_bank,
    _format_categories_for_prompt,
    _mark_qa_question_findings,
    generate_section_subtitle,
)
from aegis.etls.cm_readthrough_editor.nas_source import (
    extract_raw_blocks,
    parse_transcript_xml,
)


def test_speaker_role_keeps_bank_capital_markets_executive_as_answer() -> None:
    """A bank executive title containing Capital Markets is still management."""
    block = {
        "speaker": "Jane Executive",
        "speaker_title": "Group Head, Capital Markets",
        "speaker_affiliation": "Royal Bank of Canada",
        "bank_name": "Royal Bank of Canada",
        "speaker_type_hint": "",
        "participant_type": "",
    }

    assert _speaker_role(block) == "a"


def test_speaker_role_uses_participant_type_before_title_heuristics() -> None:
    """Structured participant role metadata should override weak title cues."""
    analyst_block = {
        "speaker": "Analyst Name",
        "speaker_title": "Managing Director",
        "speaker_affiliation": "Example Securities",
        "speaker_type_hint": "",
        "participant_type": "analyst",
    }
    executive_block = {
        "speaker": "Executive Name",
        "speaker_title": "Capital Markets",
        "speaker_affiliation": "Bank of Montreal",
        "speaker_type_hint": "",
        "participant_type": "corporate",
    }

    assert _speaker_role(analyst_block) == "q"
    assert _speaker_role(executive_block) == "a"


def test_extract_raw_blocks_preserves_participant_type() -> None:
    """FactSet participant type is retained for downstream role detection."""
    xml = b"""
    <transcript>
      <meta>
        <title>Sample Call</title>
        <participants>
          <participant id="p1" name="Jane Executive" type="corporate" title="Group Head, Capital Markets" affiliation="Royal Bank of Canada" />
          <participant id="p2" name="John Analyst" type="analyst" title="Managing Director" affiliation="Example Securities" />
        </participants>
      </meta>
      <body>
        <section name="Q&amp;A">
          <speaker id="p2" type="q"><plist><p>Can you discuss advisory pipelines?</p></plist></speaker>
          <speaker id="p1" type="a"><plist><p>Pipelines remain active.</p></plist></speaker>
        </section>
      </body>
    </transcript>
    """

    parsed = parse_transcript_xml(xml)
    assert parsed is not None
    _, qa_blocks = extract_raw_blocks(parsed, "RY-CA")

    assert qa_blocks[0]["participant_type"] == "analyst"
    assert qa_blocks[1]["participant_type"] == "corporate"


def test_qa_question_findings_use_relevance_thresholds() -> None:
    """Q&A findings should not all become selected by default."""
    conversation = {
        "id": "RY-CA_QA_1",
        "question_sentences": [
            {
                "sid": "RY-CA_QA_1_qs0",
                "text": "Can you discuss advisory pipelines?",
                "source_block_id": "RY-CA_QA_1",
                "status": "context",
                "scores": {},
                "candidate_bucket_ids": [],
            },
            {
                "sid": "RY-CA_QA_1_qs1",
                "text": "And how much of that converts this quarter?",
                "source_block_id": "RY-CA_QA_1",
                "status": "context",
                "scores": {},
                "candidate_bucket_ids": [],
            },
        ],
        "answer_sentences": [],
    }
    categories = [
        {
            "category_index": 0,
            "report_section": "Q&A",
            "category_name": "Pipelines",
        }
    ]

    _mark_qa_question_findings(
        conversation,
        [
            QAQuestion(
                category_index=0,
                source_block_id="RY-CA_QA_1",
                source_sentence_ids=["RY-CA_QA_1_qs0"],
                relevance_score=8,
                capital_markets_linkage="explicit advisory pipeline question",
                is_new_category=False,
            ),
            QAQuestion(
                category_index=0,
                source_block_id="RY-CA_QA_1",
                source_sentence_ids=["RY-CA_QA_1_qs1"],
                relevance_score=5,
                capital_markets_linkage="follow-up conversion timing",
                is_new_category=False,
            ),
        ],
        categories,
        selected_importance_threshold=6.5,
        candidate_importance_threshold=4.0,
    )

    assert conversation["question_sentences"][0]["status"] == "selected"
    assert conversation["question_sentences"][0]["importance_score"] == 8
    assert conversation["question_sentences"][1]["status"] == "candidate"
    assert conversation["question_sentences"][1]["importance_score"] == 5


def test_format_categories_for_prompt_escapes_xml_fields() -> None:
    """Category config values should not create accidental prompt tags."""
    rendered = _format_categories_for_prompt(
        [
            {
                "category_index": 0,
                "report_section": "Q&A",
                "transcript_sections": "QA",
                "category_name": "M&A <pipeline>",
                "category_group": "IB & Markets",
                "category_description": "Track <deals> & sponsor activity.",
                "example_1": "Does M&A <activity> improve?",
                "example_2": "",
                "example_3": "",
            }
        ]
    )

    assert "<name>M&amp;A &lt;pipeline&gt;</name>" in rendered
    assert "<group>IB &amp; Markets</group>" in rendered
    assert "<description>Track &lt;deals&gt; &amp; sponsor activity.</description>" in rendered
    assert "<example>Does M&amp;A &lt;activity&gt; improve?</example>" in rendered


@pytest.mark.asyncio
async def test_call_tool_forces_named_tool_choice() -> None:
    """CM editor calls should force the exact function tool, not inherit auto mode."""

    async def fake_complete_with_tools(**kwargs):
        assert kwargs["llm_params"]["tool_choice"] == {
            "type": "function",
            "function": {"name": "generate_subtitle"},
        }
        return {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "arguments": '{"subtitle": "Outlook: Pipelines improve"}'
                                }
                            }
                        ]
                    }
                }
            ],
            "metrics": {},
        }

    with patch(
        "aegis.etls.cm_readthrough_editor.interactive_pipeline.complete_with_tools",
        new=AsyncMock(side_effect=fake_complete_with_tools),
    ):
        result = await _call_tool(
            messages=[{"role": "user", "content": "Generate a subtitle"}],
            tool={
                "type": "function",
                "function": {
                    "name": "generate_subtitle",
                    "parameters": {"type": "object", "properties": {}},
                },
            },
            label="subtitle:test",
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test", "tool_choice": "auto"},
        )

    assert result == {"subtitle": "Outlook: Pipelines improve"}


@pytest.mark.asyncio
async def test_generate_section_subtitle_retries_and_normalizes_prefix() -> None:
    """Subtitle generation should retry missing tool output and enforce prefixes."""
    mock_call_tool = AsyncMock(
        side_effect=[
            None,
            {"subtitle": "trading activity and advisory pipelines stabilize"},
        ]
    )

    with patch(
        "aegis.etls.cm_readthrough_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        subtitle = await generate_section_subtitle(
            content_json=[{"bank": "RY", "findings": ["Pipelines remain constructive."]}],
            content_type="outlook",
            section_context="Forward-looking outlook statements",
            fallback="Outlook: Capital markets activity",
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test"},
        )

    assert subtitle == "Outlook: trading activity and advisory pipelines stabilize"
    assert mock_call_tool.await_count == 2


@pytest.mark.asyncio
async def test_generate_section_subtitle_falls_back_after_invalid_outputs() -> None:
    """Repeated bad subtitle tool responses should use the configured fallback."""
    mock_call_tool = AsyncMock(return_value={"subtitle": ""})

    with patch(
        "aegis.etls.cm_readthrough_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        subtitle = await generate_section_subtitle(
            content_json=[{"bank": "RY", "questions": ["Can you discuss ECM?"]}],
            content_type="questions",
            section_context="Analyst capital-markets questions",
            fallback="Conference calls: Capital markets questions",
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test"},
        )

    assert subtitle == "Conference calls: Capital markets questions"
    assert mock_call_tool.await_count == 3


@pytest.mark.asyncio
async def test_outlook_extraction_retries_missing_tool_output() -> None:
    """Missing structured output is retried instead of treated as no content."""
    mock_call_tool = AsyncMock(
        side_effect=[
            None,
            {
                "has_content": True,
                "statements": [
                    {
                        "category_index": 0,
                        "source_block_id": "RY-CA_MD_1",
                        "source_sentence_ids": ["RY-CA_MD_1_s0"],
                        "relevance_score": 8,
                        "is_new_category": False,
                    }
                ],
            },
        ]
    )

    with patch(
        "aegis.etls.cm_readthrough_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        result = await _extract_outlook_for_bank(
            md_blocks=[
                {
                    "id": "RY-CA_MD_1",
                    "speaker": "Executive",
                    "speaker_title": "CFO",
                    "speaker_affiliation": "Royal Bank of Canada",
                    "sentences": [
                        {
                            "sid": "RY-CA_MD_1_s0",
                            "text": "Advisory pipelines remain constructive.",
                            "source_block_id": "RY-CA_MD_1",
                        }
                    ],
                }
            ],
            qa_conversations=[],
            bank_info={"bank_name": "Royal Bank of Canada", "bank_symbol": "RY"},
            categories=[
                {
                    "category_index": 0,
                    "report_section": "Outlook",
                    "transcript_sections": "MD",
                    "category_name": "Pipelines",
                    "category_group": "Investment Banking",
                    "category_description": "Advisory and underwriting pipelines.",
                }
            ],
            fiscal_year=2025,
            fiscal_quarter="Q3",
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test"},
        )

    assert len(result) == 1
    assert mock_call_tool.await_count == 2
    retry_prompt = mock_call_tool.await_args_list[1].kwargs["messages"][-1]["content"]
    assert "previous Outlook extraction" in retry_prompt
    assert "No parseable tool response returned" in retry_prompt


@pytest.mark.asyncio
async def test_outlook_extraction_raises_after_repeated_invalid_output() -> None:
    """Repeated schema failures surface as ETL errors rather than empty results."""
    mock_call_tool = AsyncMock(return_value={"has_content": True, "statements": [{}]})

    with patch(
        "aegis.etls.cm_readthrough_editor.interactive_pipeline._call_tool",
        new=mock_call_tool,
    ):
        with pytest.raises(RuntimeError, match="Outlook extraction.*failed validation"):
            await _extract_outlook_for_bank(
                md_blocks=[
                    {
                        "id": "RY-CA_MD_1",
                        "speaker": "Executive",
                        "speaker_title": "CFO",
                        "speaker_affiliation": "Royal Bank of Canada",
                        "sentences": [
                            {
                                "sid": "RY-CA_MD_1_s0",
                                "text": "Advisory pipelines remain constructive.",
                                "source_block_id": "RY-CA_MD_1",
                            }
                        ],
                    }
                ],
                qa_conversations=[],
                bank_info={"bank_name": "Royal Bank of Canada", "bank_symbol": "RY"},
                categories=[
                    {
                        "category_index": 0,
                        "report_section": "Outlook",
                        "transcript_sections": "MD",
                        "category_name": "Pipelines",
                        "category_group": "Investment Banking",
                        "category_description": "Advisory and underwriting pipelines.",
                    }
                ],
                fiscal_year=2025,
                fiscal_quarter="Q3",
                context={"execution_id": "test-exec"},
                llm_params={"model": "gpt-test"},
            )

    assert mock_call_tool.await_count == 3
