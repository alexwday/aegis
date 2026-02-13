"""Tests for pure functions in key_themes main.py."""

import pytest

from aegis.etls.key_themes.main import (
    _sanitize_for_prompt,
    _timing_summary,
    format_categories_for_prompt,
    validate_grouping_assignments,
    apply_grouping_to_index,
    ThemeGroup,
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

    def test_single_brace(self):
        assert _sanitize_for_prompt("{") == "{{"
        assert _sanitize_for_prompt("}") == "}}"


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

    def test_many_stages(self):
        marks = [
            ("start", 0.0),
            ("setup", 1.0),
            ("retrieval", 3.0),
            ("classification", 8.0),
            ("formatting", 10.0),
            ("grouping", 12.0),
            ("document", 13.0),
            ("save", 14.0),
        ]
        result = _timing_summary(marks)
        assert result["total_s"] == 14.0
        assert "setup_s" in result
        assert "save_s" in result


# ---------------------------------------------------------------------------
# format_categories_for_prompt
# ---------------------------------------------------------------------------
class TestFormatCategoriesForPrompt:
    """Tests for format_categories_for_prompt()."""

    def test_single_category(self, sample_category):
        result = format_categories_for_prompt([sample_category])
        assert "<category>" in result
        assert "<name>" in result
        assert "Revenue Trends" in result
        assert "Q&A section only" in result

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

    def test_curly_braces_escaped(self):
        cat = {
            "transcript_sections": "ALL",
            "category_name": "Test {Category}",
            "category_description": "Has {braces} in it",
            "example_1": "",
            "example_2": "",
            "example_3": "",
        }
        result = format_categories_for_prompt([cat])
        assert "{{Category}}" in result
        assert "{{braces}}" in result

    def test_section_descriptions(self):
        cats = [
            {
                "transcript_sections": "MD",
                "category_name": "A",
                "category_description": "D",
                "example_1": "",
                "example_2": "",
                "example_3": "",
            },
            {
                "transcript_sections": "QA",
                "category_name": "B",
                "category_description": "D",
                "example_1": "",
                "example_2": "",
                "example_3": "",
            },
            {
                "transcript_sections": "ALL",
                "category_name": "C",
                "category_description": "D",
                "example_1": "",
                "example_2": "",
                "example_3": "",
            },
        ]
        result = format_categories_for_prompt(cats)
        assert "Management Discussion section only" in result
        assert "Q&A section only" in result
        assert "Both Management Discussion and Q&A sections" in result


# ---------------------------------------------------------------------------
# validate_grouping_assignments
# ---------------------------------------------------------------------------
class TestValidateGroupingAssignments:
    """Tests for validate_grouping_assignments()."""

    def test_valid_assignment(self, sample_qa_blocks, sample_theme_groups):
        # Should not raise
        validate_grouping_assignments(sample_qa_blocks, sample_theme_groups, "test-id")

    def test_missing_qa_raises(self, sample_qa_blocks):
        groups = [
            ThemeGroup(group_title="G1", qa_ids=["qa_1"]),
            # qa_2 is missing
        ]
        with pytest.raises(ValueError, match="validation"):
            validate_grouping_assignments(sample_qa_blocks, groups, "test-id")

    def test_duplicate_qa_raises(self, sample_qa_blocks):
        groups = [
            ThemeGroup(group_title="G1", qa_ids=["qa_1", "qa_2"]),
            ThemeGroup(group_title="G2", qa_ids=["qa_1"]),  # duplicate
        ]
        with pytest.raises(ValueError, match="validation"):
            validate_grouping_assignments(sample_qa_blocks, groups, "test-id")

    def test_unknown_qa_raises(self, sample_qa_blocks):
        groups = [
            ThemeGroup(group_title="G1", qa_ids=["qa_1", "qa_2", "qa_99"]),
        ]
        with pytest.raises(ValueError, match="validation"):
            validate_grouping_assignments(sample_qa_blocks, groups, "test-id")

    def test_ignores_invalid_blocks(self, sample_qa_blocks):
        # qa_3 is invalid, should not be expected in groups
        groups = [
            ThemeGroup(group_title="G1", qa_ids=["qa_1"]),
            ThemeGroup(group_title="G2", qa_ids=["qa_2"]),
        ]
        # Should not raise (qa_3 is invalid, not expected)
        validate_grouping_assignments(sample_qa_blocks, groups, "test-id")


# ---------------------------------------------------------------------------
# apply_grouping_to_index
# ---------------------------------------------------------------------------
class TestApplyGroupingToIndex:
    """Tests for apply_grouping_to_index()."""

    def test_assigns_groups(self, sample_qa_blocks, sample_theme_groups):
        apply_grouping_to_index(sample_qa_blocks, sample_theme_groups)

        assert sample_qa_blocks["qa_1"].assigned_group == sample_theme_groups[0]
        assert sample_qa_blocks["qa_2"].assigned_group == sample_theme_groups[1]
        assert sample_qa_blocks["qa_3"].assigned_group is None

    def test_populates_qa_blocks_list(self, sample_qa_blocks, sample_theme_groups):
        apply_grouping_to_index(sample_qa_blocks, sample_theme_groups)

        assert len(sample_theme_groups[0].qa_blocks) == 1
        assert sample_theme_groups[0].qa_blocks[0].qa_id == "qa_1"
        assert len(sample_theme_groups[1].qa_blocks) == 1
        assert sample_theme_groups[1].qa_blocks[0].qa_id == "qa_2"

    def test_clears_previous_assignments(self, sample_qa_blocks, sample_theme_groups):
        # First assignment
        apply_grouping_to_index(sample_qa_blocks, sample_theme_groups)
        assert sample_qa_blocks["qa_1"].assigned_group is not None

        # Re-assign with empty groups should clear
        empty_groups = [ThemeGroup(group_title="New", qa_ids=["qa_1", "qa_2"])]
        apply_grouping_to_index(sample_qa_blocks, empty_groups)
        assert sample_qa_blocks["qa_1"].assigned_group == empty_groups[0]
