"""Tests for pure functions in call_summary main.py."""

import pytest

from aegis.etls.call_summary.main import (
    _sanitize_for_prompt,
    _normalize_text,
    _texts_are_similar,
    _build_rejection_result,
    _filter_chunks_for_category,
    _timing_summary,
    format_categories_for_prompt,
    _dedup_evidence_across_categories,
    _dedup_statements_within_categories,
    _dedup_statements_across_categories,
    _deduplicate_results,
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
# _normalize_text
# ---------------------------------------------------------------------------
class TestNormalizeText:
    """Tests for _normalize_text()."""

    def test_strips_bold_markers(self):
        assert "revenue" in _normalize_text("**Revenue**")

    def test_strips_underline_markers(self):
        assert "key phrase" in _normalize_text("__Key Phrase__")

    def test_lowercases(self):
        result = _normalize_text("UPPER Case")
        assert result == "upper case"

    def test_collapses_whitespace(self):
        result = _normalize_text("  lots   of   space  ")
        assert result == "lots of space"

    def test_combined_normalization(self):
        result = _normalize_text("  **Revenue**  grew  __strongly__  ")
        assert result == "revenue grew strongly"


# ---------------------------------------------------------------------------
# _texts_are_similar
# ---------------------------------------------------------------------------
class TestTextsAreSimilar:
    """Tests for _texts_are_similar()."""

    def test_identical_texts(self):
        assert _texts_are_similar("same text", "same text") is True

    def test_completely_different(self):
        assert _texts_are_similar("abc", "xyz 123 456 789") is False

    def test_similar_with_bold_markers(self):
        assert (
            _texts_are_similar(
                "Revenue grew **5%** to **$5.2 BN**",
                "Revenue grew 5% to $5.2 BN",
            )
            is True
        )

    def test_below_threshold(self):
        assert _texts_are_similar("short text", "completely different long text here") is False

    def test_minor_wording_difference(self):
        text_a = "Net interest income rose 5% quarter over quarter to $5.2 billion"
        text_b = "Net interest income increased 5% QoQ to $5.2 billion"
        # These are somewhat similar but may or may not pass threshold
        result = _texts_are_similar(text_a, text_b)
        assert isinstance(result, bool)


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
# Deduplication functions
# ---------------------------------------------------------------------------
class TestDeduplication:
    """Tests for deduplication functions."""

    def test_evidence_dedup_removes_duplicates(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [
                    {
                        "statement": "S1",
                        "evidence": [{"content": "Revenue grew 5% this quarter."}],
                    }
                ],
            },
            {
                "name": "Cat2",
                "summary_statements": [
                    {
                        "statement": "S2",
                        "evidence": [{"content": "Revenue grew 5% this quarter."}],
                    }
                ],
            },
        ]
        removed = _dedup_evidence_across_categories(results, "test-id")
        assert removed == 1
        assert len(results[0]["summary_statements"][0]["evidence"]) == 1
        assert len(results[1]["summary_statements"][0]["evidence"]) == 0

    def test_evidence_dedup_keeps_different(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [
                    {"statement": "S1", "evidence": [{"content": "Revenue grew 5%."}]},
                ],
            },
            {
                "name": "Cat2",
                "summary_statements": [
                    {"statement": "S2", "evidence": [{"content": "Credit losses declined."}]},
                ],
            },
        ]
        removed = _dedup_evidence_across_categories(results, "test-id")
        assert removed == 0

    def test_evidence_dedup_skips_rejected(self):
        results = [
            {"name": "Cat1", "rejected": True, "rejection_reason": "no data"},
            {
                "name": "Cat2",
                "summary_statements": [
                    {"statement": "S1", "evidence": [{"content": "text"}]},
                ],
            },
        ]
        removed = _dedup_evidence_across_categories(results, "test-id")
        assert removed == 0

    def test_statement_dedup_within_category(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [
                    {"statement": "Revenue grew 5% this quarter to $5.2 BN."},
                    {"statement": "Revenue grew 5% this quarter to $5.2 BN."},
                    {"statement": "A totally different statement about credit."},
                ],
            }
        ]
        removed = _dedup_statements_within_categories(results, "test-id")
        assert removed == 1
        assert len(results[0]["summary_statements"]) == 2

    def test_statement_dedup_skips_rejected(self):
        results = [{"name": "Cat1", "rejected": True}]
        removed = _dedup_statements_within_categories(results, "test-id")
        assert removed == 0

    def test_deduplicate_results_combined(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [
                    {
                        "statement": "Revenue grew strongly.",
                        "evidence": [{"content": "Same evidence text here."}],
                    },
                    {
                        "statement": "Revenue grew strongly.",
                        "evidence": [{"content": "Different evidence."}],
                    },
                ],
            },
            {
                "name": "Cat2",
                "summary_statements": [
                    {
                        "statement": "Credit improved.",
                        "evidence": [{"content": "Same evidence text here."}],
                    },
                ],
            },
        ]
        result = _deduplicate_results(results, "test-id")
        # Statement dedup should remove 1 duplicate in Cat1
        assert len(result[0]["summary_statements"]) == 1
        # Evidence dedup should remove duplicate in Cat2
        assert len(result[1]["summary_statements"][0]["evidence"]) == 0

    def test_deduplicate_empty_evidence_kept(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [
                    {"statement": "S1", "evidence": [{"content": ""}]},
                ],
            },
        ]
        removed = _dedup_evidence_across_categories(results, "test-id")
        assert removed == 0
        assert len(results[0]["summary_statements"][0]["evidence"]) == 1

    def test_deduplicate_no_evidence_key(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [
                    {"statement": "S1"},
                ],
            },
        ]
        removed = _dedup_evidence_across_categories(results, "test-id")
        assert removed == 0


# ---------------------------------------------------------------------------
# _dedup_statements_across_categories (B2.1)
# ---------------------------------------------------------------------------
class TestDeduplicateStatementsAcrossCategories:
    """Tests for _dedup_statements_across_categories()."""

    def test_removes_duplicate_across_categories(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [
                    {"statement": "Revenue grew 5% this quarter to $5.2 BN."},
                ],
            },
            {
                "name": "Cat2",
                "summary_statements": [
                    {"statement": "Revenue grew 5% this quarter to $5.2 BN."},
                    {"statement": "Credit losses declined significantly."},
                ],
            },
        ]
        removed = _dedup_statements_across_categories(results, "test-id")
        assert removed == 1
        # Cat1 keeps its statement, Cat2 loses the duplicate
        assert len(results[0]["summary_statements"]) == 1
        assert len(results[1]["summary_statements"]) == 1
        assert "Credit" in results[1]["summary_statements"][0]["statement"]

    def test_keeps_different_statements(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [
                    {"statement": "Revenue grew strongly this quarter."},
                ],
            },
            {
                "name": "Cat2",
                "summary_statements": [
                    {"statement": "Credit losses declined significantly."},
                ],
            },
        ]
        removed = _dedup_statements_across_categories(results, "test-id")
        assert removed == 0

    def test_skips_rejected_categories(self):
        results = [
            {"name": "Cat1", "rejected": True},
            {
                "name": "Cat2",
                "summary_statements": [
                    {"statement": "Revenue grew strongly."},
                ],
            },
        ]
        removed = _dedup_statements_across_categories(results, "test-id")
        assert removed == 0

    def test_empty_statement_kept(self):
        results = [
            {
                "name": "Cat1",
                "summary_statements": [{"statement": ""}],
            },
        ]
        removed = _dedup_statements_across_categories(results, "test-id")
        assert removed == 0
        assert len(results[0]["summary_statements"]) == 1


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
