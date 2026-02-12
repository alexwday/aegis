"""Integration tests for generate_call_summary (D3.3).

Tests the end-to-end flow with all external dependencies mocked:
database, LLM, authentication, and file system.
"""

import json
import os

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from aegis.etls.call_summary.main import (
    generate_call_summary,
    CallSummaryResult,
    CallSummarySystemError,
    CallSummaryUserError,
)


# ---------------------------------------------------------------------------
# Shared mock data
# ---------------------------------------------------------------------------

MOCK_CHUNKS = [
    {
        "id": 1,
        "section_name": "MANAGEMENT DISCUSSION SECTION",
        "speaker_block_id": 1,
        "qa_group_id": None,
        "chunk_id": 1,
        "content": "Revenue grew 8% year-over-year to $14.5 billion.",
        "block_summary": "Revenue discussion",
        "classification_ids": None,
        "classification_names": None,
        "title": None,
    },
    {
        "id": 2,
        "section_name": "Q&A",
        "speaker_block_id": None,
        "qa_group_id": 1,
        "chunk_id": 2,
        "content": "Analyst: Can you discuss NII trends? CFO: NII was strong.",
        "block_summary": "NII Q&A",
        "classification_ids": None,
        "classification_names": None,
        "title": None,
    },
]

MOCK_RESEARCH_PLAN_RESPONSE = {
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
                                            "name": "Revenue & Income Breakdown",
                                            "extraction_strategy": "Focus on NII growth.",
                                            "cross_category_notes": "",
                                            "relevant_qa_groups": [1],
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
    "metrics": {"prompt_tokens": 100, "completion_tokens": 50, "total_cost": 0.01, "response_time": 1.0},
}

MOCK_EXTRACTION_RESPONSE = {
    "choices": [
        {
            "message": {
                "tool_calls": [
                    {
                        "function": {
                            "arguments": json.dumps(
                                {
                                    "rejected": False,
                                    "title": "Revenue: Strong NII performance",
                                    "summary_statements": [
                                        {
                                            "statement": "NII grew **8%** to **$14.5 BN**.",
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
    "metrics": {"prompt_tokens": 500, "completion_tokens": 100, "total_cost": 0.05, "response_time": 2.0},
}

MOCK_RESEARCH_PROMPT = {
    "system_prompt": (
        "You are a financial analyst for {bank_name} ({bank_symbol}) "
        "{quarter} {fiscal_year}.\n{categories_list}"
    ),
    "user_prompt": "Analyze: {transcript_text}",
    "tool_definition": {
        "type": "function",
        "function": {
            "name": "generate_research_plan",
            "parameters": {"type": "object", "properties": {}},
        },
    },
}

MOCK_EXTRACTION_PROMPT = {
    "system_prompt": (
        "Extract for {category_name}. {category_description}. "
        "Plan: {research_plan}. Notes: {cross_category_notes}. "
        "Idx {category_index}/{total_categories}. "
        "{bank_name} ({bank_symbol}) {quarter} {fiscal_year}. "
        "Section: {transcripts_section}."
    ),
    "user_prompt": "Extract: {formatted_section}",
    "tool_definition": {
        "type": "function",
        "function": {
            "name": "extract_category_content",
            "parameters": {"type": "object", "properties": {}},
        },
    },
}


# ---------------------------------------------------------------------------
# Helper to set up all mocks
# ---------------------------------------------------------------------------


MOCK_SINGLE_CATEGORY = [
    {
        "transcript_sections": "ALL",
        "report_section": "Results Summary",
        "category_name": "Revenue",
        "category_description": "Revenue and income analysis.",
        "example_1": "",
        "example_2": "",
        "example_3": "",
    }
]


def _setup_mocks():
    """Create all mock patches for the integration test."""
    patches = {}

    # Auth
    mock_auth = AsyncMock(return_value={"success": True, "method": "api_key"})
    patches["auth"] = patch("aegis.etls.call_summary.main.setup_authentication", mock_auth)

    # SSL
    patches["ssl"] = patch(
        "aegis.etls.call_summary.main.setup_ssl",
        return_value={"verify": False},
    )

    # Data availability
    mock_availability = AsyncMock()
    patches["availability"] = patch(
        "aegis.etls.call_summary.main.verify_and_get_availability", mock_availability
    )

    # Transcript retrieval
    mock_retrieve = AsyncMock(return_value=MOCK_CHUNKS)
    patches["retrieve"] = patch(
        "aegis.etls.call_summary.main.retrieve_full_section", mock_retrieve
    )

    # Transcript formatting (synchronous — no I/O)
    mock_format = MagicMock(return_value="Formatted transcript text.")
    patches["format"] = patch(
        "aegis.etls.call_summary.main.format_full_section_chunks", mock_format
    )

    # Categories - return single category to match mock LLM response count
    patches["categories"] = patch(
        "aegis.etls.call_summary.main.load_categories_from_xlsx",
        return_value=MOCK_SINGLE_CATEGORY,
    )

    # Prompt loading - return different prompts based on name
    def mock_load_prompt(**kwargs):
        if kwargs.get("name") == "research_plan":
            return dict(MOCK_RESEARCH_PROMPT)
        return dict(MOCK_EXTRACTION_PROMPT)

    patches["prompts"] = patch(
        "aegis.etls.call_summary.main.load_prompt_from_db",
        side_effect=mock_load_prompt,
    )

    # LLM calls - first call is research plan, subsequent are extractions
    mock_llm = AsyncMock(
        side_effect=[MOCK_RESEARCH_PLAN_RESPONSE, MOCK_EXTRACTION_RESPONSE]
    )
    patches["llm"] = patch("aegis.etls.call_summary.main.complete_with_tools", mock_llm)

    # Database save
    mock_save = AsyncMock()
    patches["save"] = patch("aegis.etls.call_summary.main._save_to_database", mock_save)

    return patches


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGenerateCallSummaryIntegration:
    """Integration tests for generate_call_summary()."""

    @pytest.mark.asyncio
    async def test_successful_end_to_end(self):
        """Full pipeline succeeds with single category and produces document."""
        patches = _setup_mocks()
        managers = {k: p.start() for k, p in patches.items()}

        try:
            result = await generate_call_summary(
                bank_name="RY", fiscal_year=2024, quarter="Q3"
            )

            assert isinstance(result, CallSummaryResult)
            assert result.included_categories == 1
            assert result.total_categories == 1

            # Verify save was called
            managers["save"].assert_called_once()

        finally:
            for p in patches.values():
                p.stop()

            # Clean up generated document
            output_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "../../../../src/aegis/etls/call_summary/output",
            )
            for f in os.listdir(output_dir) if os.path.isdir(output_dir) else []:
                if f.startswith("RY_2024_Q3") and f.endswith(".docx"):
                    os.remove(os.path.join(output_dir, f))

    @pytest.mark.asyncio
    async def test_auth_failure_returns_error(self):
        """Authentication failure returns user-friendly error."""
        patches = _setup_mocks()
        # Override auth to fail
        patches["auth"] = patch(
            "aegis.etls.call_summary.main.setup_authentication",
            new_callable=AsyncMock,
            return_value={"success": False, "error": "Invalid credentials"},
        )

        for p in patches.values():
            p.start()

        try:
            with pytest.raises(CallSummarySystemError, match="Authentication failed"):
                await generate_call_summary(
                    bank_name="RY", fiscal_year=2024, quarter="Q3"
                )
        finally:
            for p in patches.values():
                p.stop()

    @pytest.mark.asyncio
    async def test_no_chunks_returns_error(self):
        """Empty transcript returns user-friendly error."""
        patches = _setup_mocks()
        patches["retrieve"] = patch(
            "aegis.etls.call_summary.main.retrieve_full_section",
            new_callable=AsyncMock,
            return_value=[],
        )

        for p in patches.values():
            p.start()

        try:
            with pytest.raises(CallSummaryUserError, match="No transcript chunks found"):
                await generate_call_summary(
                    bank_name="RY", fiscal_year=2024, quarter="Q3"
                )
        finally:
            for p in patches.values():
                p.stop()

    @pytest.mark.asyncio
    async def test_invalid_bank_returns_error(self):
        """Invalid bank name raises user-friendly error."""
        with pytest.raises(CallSummaryUserError, match="not found"):
            await generate_call_summary(
                bank_name="NONEXISTENT_BANK_XYZ", fiscal_year=2024, quarter="Q3"
            )

    @pytest.mark.asyncio
    async def test_all_categories_rejected_returns_error(self):
        """When all categories are rejected, returns error."""
        patches = _setup_mocks()

        # Override extraction to return rejected
        rejected_response = {
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "function": {
                                    "arguments": json.dumps(
                                        {
                                            "rejected": True,
                                            "rejection_reason": "No content.",
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
        patches["llm"] = patch(
            "aegis.etls.call_summary.main.complete_with_tools",
            new_callable=AsyncMock,
            side_effect=[MOCK_RESEARCH_PLAN_RESPONSE, rejected_response],
        )

        for p in patches.values():
            p.start()

        try:
            with pytest.raises(CallSummaryUserError, match="rejected"):
                await generate_call_summary(
                    bank_name="RY", fiscal_year=2024, quarter="Q3"
                )
        finally:
            for p in patches.values():
                p.stop()
