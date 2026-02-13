"""Tests for pure functions in call_summary main.py."""

import pytest

from aegis.etls.call_summary.main import (
    _sanitize_for_prompt,
    _build_rejection_result,
    _filter_chunks_for_category,
    _timing_summary,
    format_categories_for_prompt,
    _format_categories_for_dedup,
    _apply_dedup_removals,
    DeduplicationResponse,
    DuplicateStatement,
    DuplicateEvidence,
    _generate_document,
)


# ---------------------------------------------------------------------------
# _sanitize_for_prompt
# ---------------------------------------------------------------------------
class TestSanitizeForPrompt:
    """Tests for _sanitize_for_prompt()."""

    def test_escapes_curly_braces(self):
        assert _sanitize_for_prompt("value is {x}") == "value is {{x}}"

    def test_no_braces_unchanged(self):
        assert _sanitize_for_prompt("plain text") == "plain text"

    def test_empty_string(self):
        assert _sanitize_for_prompt("") == ""

    def test_already_escaped(self):
        assert _sanitize_for_prompt("{{x}}") == "{{{{x}}}}"


# ---------------------------------------------------------------------------
# _build_rejection_result
# ---------------------------------------------------------------------------
class TestBuildRejectionResult:
    """Tests for _build_rejection_result()."""

    def test_basic_rejection(self, sample_category):
        result = _build_rejection_result(1, sample_category, "No data found")
        assert result["index"] == 1
        assert result["name"] == "Revenue & Income Breakdown"
        assert result["rejected"] is True
        assert result["rejection_reason"] == "No data found"

    def test_report_section_default(self):
        cat = {"category_name": "Test", "transcript_sections": "ALL"}
        result = _build_rejection_result(5, cat, "reason")
        assert result["report_section"] == "Results Summary"

    def test_report_section_from_category(self):
        cat = {
            "category_name": "Outlook",
            "report_section": "Strategic Outlook",
            "transcript_sections": "MD",
        }
        result = _build_rejection_result(3, cat, "reason")
        assert result["report_section"] == "Strategic Outlook"


# ---------------------------------------------------------------------------
# _filter_chunks_for_category
# ---------------------------------------------------------------------------
class TestFilterChunksForCategory:
    """Tests for _filter_chunks_for_category()."""

    @pytest.fixture
    def section_cache(self):
        """Build a section cache with MD and QA chunks."""
        md_chunks = [
            {"id": 1, "section_name": "MANAGEMENT DISCUSSION SECTION", "content": "MD1"},
            {"id": 2, "section_name": "MANAGEMENT DISCUSSION SECTION", "content": "MD2"},
        ]
        qa_chunks = [
            {"id": 3, "section_name": "Q&A", "qa_group_id": 1, "content": "QA-G1-A"},
            {"id": 4, "section_name": "Q&A", "qa_group_id": 1, "content": "QA-G1-B"},
            {"id": 5, "section_name": "Q&A", "qa_group_id": 2, "content": "QA-G2"},
            {"id": 6, "section_name": "Q&A", "qa_group_id": 3, "content": "QA-G3"},
        ]
        return {"MD": md_chunks, "QA": qa_chunks, "ALL": md_chunks + qa_chunks}

    def test_md_only(self, section_cache):
        result = _filter_chunks_for_category(section_cache, "MD", [])
        assert len(result) == 2
        assert all("MD" in c["content"] for c in result)

    def test_qa_only_with_filter(self, section_cache):
        result = _filter_chunks_for_category(section_cache, "QA", [1, 3])
        assert len(result) == 3  # G1 has 2 chunks + G3 has 1
        assert all(c.get("qa_group_id") in {1, 3} for c in result)

    def test_qa_no_filter_returns_all(self, section_cache):
        result = _filter_chunks_for_category(section_cache, "QA", [])
        assert len(result) == 4

    def test_all_sections_with_filter(self, section_cache):
        result = _filter_chunks_for_category(section_cache, "ALL", [2])
        # MD chunks (2) + filtered QA (1 chunk from group 2)
        assert len(result) == 3

    def test_all_sections_no_filter(self, section_cache):
        result = _filter_chunks_for_category(section_cache, "ALL", [])
        assert len(result) == 6  # All MD + all QA

    def test_empty_cache(self):
        result = _filter_chunks_for_category({}, "ALL", [1])
        assert result == []

    def test_missing_section_key(self):
        cache = {"MD": [{"id": 1}]}
        result = _filter_chunks_for_category(cache, "QA", [])
        assert result == []


# ---------------------------------------------------------------------------
# _timing_summary
# ---------------------------------------------------------------------------
class TestTimingSummary:
    """Tests for _timing_summary()."""

    def test_basic_timing(self):
        marks = [("start", 100.0), ("setup", 101.5), ("end", 103.0)]
        result = _timing_summary(marks)
        assert result["setup_s"] == 1.5
        assert result["end_s"] == 1.5
        assert result["total_s"] == 3.0

    def test_single_mark_returns_empty(self):
        assert _timing_summary([("start", 100.0)]) == {}

    def test_empty_marks_returns_empty(self):
        assert _timing_summary([]) == {}

    def test_rounding(self):
        marks = [("start", 0.0), ("mid", 1.333333), ("end", 2.666666)]
        result = _timing_summary(marks)
        assert result["mid_s"] == 1.33
        assert result["end_s"] == 1.33
        assert result["total_s"] == 2.67


# ---------------------------------------------------------------------------
# format_categories_for_prompt
# ---------------------------------------------------------------------------
class TestFormatCategoriesForPrompt:
    """Tests for format_categories_for_prompt()."""

    def test_single_category(self, sample_category):
        result = format_categories_for_prompt([sample_category])
        assert "<category>" in result
        assert (
            "<name>Revenue &amp; Income Breakdown</name>" in result
            or "<name>Revenue & Income Breakdown</name>" in result
        )
        assert "Both Management Discussion and Q&A sections" in result

    def test_examples_included(self, sample_category):
        result = format_categories_for_prompt([sample_category])
        assert "<examples>" in result
        assert "Net interest income rose 5%" in result

    def test_empty_examples_excluded(self):
        cat = {
            "transcript_sections": "MD",
            "category_name": "Test",
            "category_description": "Desc",
            "example_1": "",
            "example_2": "",
            "example_3": "",
        }
        result = format_categories_for_prompt([cat])
        assert "<examples>" not in result

    def test_multiple_categories(self, sample_categories):
        result = format_categories_for_prompt(sample_categories)
        assert result.count("<category>") == 3
        assert "Q&A section only" in result
        assert "Management Discussion section only" in result


# ---------------------------------------------------------------------------
# _format_categories_for_dedup
# ---------------------------------------------------------------------------
class TestFormatCategoriesForDedup:
    """Tests for _format_categories_for_dedup()."""

    def test_basic_formatting(self):
        results = [
            {
                "name": "Revenue",
                "summary_statements": [
                    {
                        "statement": "Revenue grew 5%.",
                        "evidence": [{"content": "Strong growth this quarter."}],
                    }
                ],
            },
        ]
        output = _format_categories_for_dedup(results)
        assert 'category index="0"' in output
        assert 'statement index="0"' in output
        assert 'evidence index="0"' in output
        assert "Revenue grew 5%." in output

    def test_skips_rejected(self):
        results = [
            {"name": "Cat1", "rejected": True, "rejection_reason": "no data"},
            {
                "name": "Cat2",
                "summary_statements": [{"statement": "S1", "evidence": []}],
            },
        ]
        output = _format_categories_for_dedup(results)
        assert "Cat1" not in output
        assert "Cat2" in output

    def test_empty_results(self):
        output = _format_categories_for_dedup([])
        assert output == ""

    def test_multiple_categories_and_indices(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [
                    {"statement": "S1", "evidence": [{"content": "E1"}, {"content": "E2"}]},
                    {"statement": "S2", "evidence": []},
                ],
            },
            {
                "name": "Cat2",
                "summary_statements": [
                    {"statement": "S3", "evidence": [{"content": "E3"}]},
                ],
            },
        ]
        output = _format_categories_for_dedup(results)
        assert 'category index="0"' in output
        assert 'category index="1"' in output
        assert 'statement index="0"' in output
        assert 'statement index="1"' in output
        assert 'evidence index="0"' in output
        assert 'evidence index="1"' in output


# ---------------------------------------------------------------------------
# _apply_dedup_removals
# ---------------------------------------------------------------------------
class TestApplyDedupRemovals:
    """Tests for _apply_dedup_removals()."""

    def test_removes_duplicate_statement(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [
                    {"statement": "Revenue grew 5%.", "evidence": []},
                ],
            },
            {
                "name": "Cat2",
                "summary_statements": [
                    {"statement": "Revenue increased 5%.", "evidence": []},
                    {"statement": "Credit improved.", "evidence": []},
                ],
            },
        ]
        dedup = DeduplicationResponse(
            analysis_notes="Found duplicate",
            duplicate_statements=[
                DuplicateStatement(
                    category_index=1,
                    statement_index=0,
                    duplicate_of_category_index=0,
                    duplicate_of_statement_index=0,
                    reasoning="Same revenue insight",
                )
            ],
        )
        stmts_removed, ev_removed = _apply_dedup_removals(results, dedup, "test-id")
        assert stmts_removed == 1
        assert ev_removed == 0
        assert len(results[1]["summary_statements"]) == 1
        assert "Credit" in results[1]["summary_statements"][0]["statement"]

    def test_removes_duplicate_evidence(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [
                    {
                        "statement": "S1",
                        "evidence": [{"content": "CFO said revenue grew."}],
                    },
                ],
            },
            {
                "name": "Cat2",
                "summary_statements": [
                    {
                        "statement": "S2",
                        "evidence": [
                            {"content": "CFO said revenue grew."},
                            {"content": "Unique evidence."},
                        ],
                    },
                ],
            },
        ]
        dedup = DeduplicationResponse(
            analysis_notes="Duplicate evidence",
            duplicate_evidence=[
                DuplicateEvidence(
                    category_index=1,
                    statement_index=0,
                    evidence_index=0,
                    duplicate_of_category_index=0,
                    duplicate_of_statement_index=0,
                    duplicate_of_evidence_index=0,
                    reasoning="Same CFO quote",
                )
            ],
        )
        stmts_removed, ev_removed = _apply_dedup_removals(results, dedup, "test-id")
        assert stmts_removed == 0
        assert ev_removed == 1
        assert len(results[1]["summary_statements"][0]["evidence"]) == 1
        assert "Unique" in results[1]["summary_statements"][0]["evidence"][0]["content"]

    def test_no_duplicates_returns_zeros(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [{"statement": "S1", "evidence": []}],
            },
        ]
        dedup = DeduplicationResponse(analysis_notes="No duplicates found")
        stmts_removed, ev_removed = _apply_dedup_removals(results, dedup, "test-id")
        assert stmts_removed == 0
        assert ev_removed == 0

    def test_invalid_category_index_skipped(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [{"statement": "S1", "evidence": []}],
            },
        ]
        dedup = DeduplicationResponse(
            duplicate_statements=[
                DuplicateStatement(
                    category_index=99,
                    statement_index=0,
                    duplicate_of_category_index=0,
                    duplicate_of_statement_index=0,
                )
            ],
        )
        stmts_removed, ev_removed = _apply_dedup_removals(results, dedup, "test-id")
        assert stmts_removed == 0
        assert len(results[0]["summary_statements"]) == 1

    def test_invalid_statement_index_skipped(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [{"statement": "S1", "evidence": []}],
            },
        ]
        dedup = DeduplicationResponse(
            duplicate_statements=[
                DuplicateStatement(
                    category_index=0,
                    statement_index=99,
                    duplicate_of_category_index=0,
                    duplicate_of_statement_index=0,
                )
            ],
        )
        stmts_removed, ev_removed = _apply_dedup_removals(results, dedup, "test-id")
        assert stmts_removed == 0

    def test_invalid_evidence_index_skipped(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [
                    {"statement": "S1", "evidence": [{"content": "E1"}]},
                ],
            },
        ]
        dedup = DeduplicationResponse(
            duplicate_evidence=[
                DuplicateEvidence(
                    category_index=0,
                    statement_index=0,
                    evidence_index=99,
                    duplicate_of_category_index=0,
                    duplicate_of_statement_index=0,
                    duplicate_of_evidence_index=0,
                )
            ],
        )
        stmts_removed, ev_removed = _apply_dedup_removals(results, dedup, "test-id")
        assert ev_removed == 0
        assert len(results[0]["summary_statements"][0]["evidence"]) == 1

    def test_skips_rejected_categories(self):
        results = [
            {"name": "Cat1", "rejected": True, "rejection_reason": "no data"},
        ]
        dedup = DeduplicationResponse(
            duplicate_statements=[
                DuplicateStatement(
                    category_index=0,
                    statement_index=0,
                    duplicate_of_category_index=0,
                    duplicate_of_statement_index=0,
                )
            ],
        )
        stmts_removed, ev_removed = _apply_dedup_removals(results, dedup, "test-id")
        assert stmts_removed == 0

    def test_reverse_order_removal_correctness(self):
        """Verify that removing multiple items from same category works correctly."""
        results = [
            {
                "name": "Cat1",
                "summary_statements": [
                    {"statement": "Keep this.", "evidence": []},
                    {"statement": "Remove this.", "evidence": []},
                    {"statement": "Also remove.", "evidence": []},
                    {"statement": "Keep this too.", "evidence": []},
                ],
            },
        ]
        dedup = DeduplicationResponse(
            duplicate_statements=[
                DuplicateStatement(
                    category_index=0,
                    statement_index=1,
                    duplicate_of_category_index=0,
                    duplicate_of_statement_index=0,
                ),
                DuplicateStatement(
                    category_index=0,
                    statement_index=2,
                    duplicate_of_category_index=0,
                    duplicate_of_statement_index=0,
                ),
            ],
        )
        stmts_removed, _ = _apply_dedup_removals(results, dedup, "test-id")
        assert stmts_removed == 2
        assert len(results[0]["summary_statements"]) == 2
        assert results[0]["summary_statements"][0]["statement"] == "Keep this."
        assert results[0]["summary_statements"][1]["statement"] == "Keep this too."


# ---------------------------------------------------------------------------
# _generate_document — report section grouping (A1.1 / D3.1)
# ---------------------------------------------------------------------------
class TestGenerateDocumentSections:
    """Tests for _generate_document() report section grouping."""

    def test_three_report_sections_not_fragmented(self, sample_etl_context):
        """Categories with 3 distinct report_sections produce one heading per section."""
        import os

        valid_categories = [
            {
                "index": 1,
                "name": "Revenue",
                "title": "Revenue",
                "report_section": "Results Summary",
                "rejected": False,
                "summary_statements": [
                    {
                        "statement": "Revenue grew **5%**.",
                        "evidence": [
                            {"content": "Revenue strong.", "type": "paraphrase", "speaker": "CFO"}
                        ],
                    }
                ],
            },
            {
                "index": 2,
                "name": "Strategy",
                "title": "Strategy",
                "report_section": "Strategic Outlook",
                "rejected": False,
                "summary_statements": [
                    {
                        "statement": "Strategy is clear.",
                        "evidence": [
                            {"content": "We have a plan.", "type": "quote", "speaker": "CEO"}
                        ],
                    }
                ],
            },
            {
                "index": 3,
                "name": "Credit",
                "title": "Credit Quality",
                "report_section": "Results Summary",
                "rejected": False,
                "summary_statements": [
                    {
                        "statement": "Credit improved.",
                        "evidence": [
                            {"content": "PCL was lower.", "type": "paraphrase", "speaker": "CRO"}
                        ],
                    }
                ],
            },
            {
                "index": 4,
                "name": "Risk",
                "title": "Risk Analysis",
                "report_section": "Risk Analysis",
                "rejected": False,
                "summary_statements": [
                    {
                        "statement": "Risk is managed.",
                        "evidence": [
                            {
                                "content": "Provisions stable.",
                                "type": "paraphrase",
                                "speaker": "CRO",
                            }
                        ],
                    }
                ],
            },
            {
                "index": 5,
                "name": "Guidance",
                "title": "Forward Guidance",
                "report_section": "Strategic Outlook",
                "rejected": False,
                "summary_statements": [
                    {
                        "statement": "Outlook positive.",
                        "evidence": [
                            {"content": "We expect growth.", "type": "quote", "speaker": "CEO"}
                        ],
                    }
                ],
            },
        ]

        filepath, docx_filename = _generate_document(valid_categories, sample_etl_context)

        # Read back and check section headings
        from docx import Document

        doc = Document(filepath)
        h1_headings = [p.text for p in doc.paragraphs if p.style and p.style.name == "Heading 1"]

        # Should have exactly 3 unique section headings (no duplicates)
        assert len(h1_headings) == 3
        assert h1_headings[0] == "Results Summary"
        # The other two should each appear once
        assert h1_headings.count("Strategic Outlook") == 1
        assert h1_headings.count("Risk Analysis") == 1

        # Clean up
        os.remove(filepath)
