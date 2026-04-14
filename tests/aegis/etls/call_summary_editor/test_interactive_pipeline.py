"""Tests for the interactive pipeline helpers."""

from unittest.mock import AsyncMock, patch

import pytest

from aegis.etls.call_summary_editor.interactive_pipeline import (
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
                    {"conversation_id": "conv_1", "block_ids": ["RY_QA_1", "RY_QA_2"]}
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
async def test_detect_qa_boundaries_falls_back_to_type_hints():
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

    with patch(
        "aegis.etls.call_summary_editor.interactive_pipeline._call_tool",
        new=AsyncMock(return_value=None),
    ):
        conversations = await detect_qa_boundaries(
            qa_raw_blocks=qa_raw_blocks,
            categories_text_qa="",
            context={"execution_id": "test-exec"},
            llm_params={"model": "gpt-test"},
        )

    assert len(conversations) == 1
    assert [block["id"] for block in conversations[0]] == ["RY_QA_1", "RY_QA_2"]


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
