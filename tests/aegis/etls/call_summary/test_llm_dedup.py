"""Tests for LLM-based deduplication in call_summary main.py."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from aegis.etls.call_summary.main import _deduplicate_results_llm


def _make_category_results():
    """Build sample category results with 2 non-rejected categories."""
    return [
        {
            "name": "Revenue",
            "summary_statements": [
                {
                    "statement": "Revenue grew 5%.",
                    "evidence": [{"content": "Strong quarter.", "type": "paraphrase"}],
                },
            ],
        },
        {
            "name": "Capital",
            "summary_statements": [
                {
                    "statement": "CET1 improved to 13%.",
                    "evidence": [{"content": "Capital strong.", "type": "paraphrase"}],
                },
                {
                    "statement": "Revenue grew 5%.",
                    "evidence": [{"content": "Strong quarter.", "type": "paraphrase"}],
                },
            ],
        },
    ]


def _make_dedup_prompts():
    """Build minimal dedup prompt dict."""
    return {
        "system_prompt": "You are a dedup assistant.",
        "user_prompt": "Review: {categories_xml}",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "report_deduplication",
                "parameters": {"type": "object", "properties": {}},
            },
        },
    }


def _make_context():
    """Build minimal context dict."""
    return {
        "execution_id": "test-exec",
        "auth_config": {"success": True},
        "ssl_config": {"verify": False},
    }


def _mock_llm_response(dedup_data: dict) -> dict:
    """Build a mock LLM response wrapping dedup tool call data."""
    return {
        "choices": [
            {
                "message": {
                    "tool_calls": [
                        {
                            "function": {
                                "name": "report_deduplication",
                                "arguments": json.dumps(dedup_data),
                            }
                        }
                    ]
                }
            }
        ],
        "metrics": {
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "total_cost": 0.001,
            "response_time": 0.5,
        },
    }


class TestDeduplicateResultsLlm:
    """Tests for _deduplicate_results_llm()."""

    @pytest.mark.asyncio
    async def test_happy_path_with_removals(self):
        """LLM identifies and removes a duplicate statement."""
        results = _make_category_results()
        dedup_data = {
            "analysis_notes": "Found one duplicate",
            "duplicate_statements": [
                {
                    "category_index": 1,
                    "statement_index": 1,
                    "duplicate_of_category_index": 0,
                    "duplicate_of_statement_index": 0,
                    "reasoning": "Same revenue insight",
                }
            ],
            "duplicate_evidence": [],
        }

        with patch(
            "aegis.etls.call_summary.main.complete_with_tools",
            new_callable=AsyncMock,
            return_value=_mock_llm_response(dedup_data),
        ):
            result = await _deduplicate_results_llm(
                results, _make_dedup_prompts(), _make_context(), "test-exec"
            )

        # Cat2 should have 1 statement remaining (duplicate removed)
        assert len(result[1]["summary_statements"]) == 1
        assert "CET1" in result[1]["summary_statements"][0]["statement"]

    @pytest.mark.asyncio
    async def test_no_duplicates_returns_unchanged(self):
        """LLM finds no duplicates — results unchanged."""
        results = _make_category_results()
        original_lengths = [len(r["summary_statements"]) for r in results]

        dedup_data = {
            "analysis_notes": "No duplicates found",
            "duplicate_statements": [],
            "duplicate_evidence": [],
        }

        with patch(
            "aegis.etls.call_summary.main.complete_with_tools",
            new_callable=AsyncMock,
            return_value=_mock_llm_response(dedup_data),
        ):
            result = await _deduplicate_results_llm(
                results, _make_dedup_prompts(), _make_context(), "test-exec"
            )

        for i, r in enumerate(result):
            assert len(r["summary_statements"]) == original_lengths[i]

    @pytest.mark.asyncio
    async def test_raises_on_llm_failure(self):
        """LLM raises exception — propagates as RuntimeError after retries."""
        results = _make_category_results()

        with patch(
            "aegis.etls.call_summary.main.complete_with_tools",
            new_callable=AsyncMock,
            side_effect=RuntimeError("LLM connection failed"),
        ):
            with pytest.raises(RuntimeError, match="Deduplication failed"):
                await _deduplicate_results_llm(
                    results, _make_dedup_prompts(), _make_context(), "test-exec"
                )

    @pytest.mark.asyncio
    async def test_fewer_than_2_categories_skips_dedup(self):
        """With only 1 non-rejected category, dedup is skipped entirely."""
        results = [
            {"name": "Cat1", "rejected": True, "rejection_reason": "no data"},
            {
                "name": "Cat2",
                "summary_statements": [{"statement": "S1", "evidence": []}],
            },
        ]

        # complete_with_tools should NOT be called
        with patch(
            "aegis.etls.call_summary.main.complete_with_tools",
            new_callable=AsyncMock,
        ) as mock_llm:
            result = await _deduplicate_results_llm(
                results, _make_dedup_prompts(), _make_context(), "test-exec"
            )
            mock_llm.assert_not_called()

        assert len(result[1]["summary_statements"]) == 1

    @pytest.mark.asyncio
    async def test_invalid_json_from_llm_raises(self):
        """LLM returns invalid JSON — raises RuntimeError after retries."""
        results = _make_category_results()

        bad_response = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "name": "report_deduplication",
                                    "arguments": "not valid json{{{",
                                }
                            }
                        ]
                    }
                }
            ],
            "metrics": {},
        }

        with patch(
            "aegis.etls.call_summary.main.complete_with_tools",
            new_callable=AsyncMock,
            return_value=bad_response,
        ):
            with pytest.raises(RuntimeError, match="Deduplication failed"):
                await _deduplicate_results_llm(
                    results, _make_dedup_prompts(), _make_context(), "test-exec"
                )

    @pytest.mark.asyncio
    async def test_evidence_removal(self):
        """LLM identifies duplicate evidence across categories."""
        results = _make_category_results()
        dedup_data = {
            "analysis_notes": "Duplicate evidence found",
            "duplicate_statements": [],
            "duplicate_evidence": [
                {
                    "category_index": 1,
                    "statement_index": 1,
                    "evidence_index": 0,
                    "duplicate_of_category_index": 0,
                    "duplicate_of_statement_index": 0,
                    "duplicate_of_evidence_index": 0,
                    "reasoning": "Same quote",
                }
            ],
        }

        with patch(
            "aegis.etls.call_summary.main.complete_with_tools",
            new_callable=AsyncMock,
            return_value=_mock_llm_response(dedup_data),
        ):
            result = await _deduplicate_results_llm(
                results, _make_dedup_prompts(), _make_context(), "test-exec"
            )

        # Evidence removed from Cat2/stmt1
        assert len(result[1]["summary_statements"][1]["evidence"]) == 0
        # Cat1 evidence untouched
        assert len(result[0]["summary_statements"][0]["evidence"]) == 1
