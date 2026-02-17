"""Tests for pure helper functions in CM readthrough ETL."""

from aegis.etls.cm_readthrough.main import (
    _sanitize_for_prompt,
    _timing_summary,
    _accumulate_llm_cost,
    _get_total_llm_cost,
    format_categories_for_prompt,
    aggregate_results,
    OutlookStatement,
    _format_qa_for_dedup,
    _apply_qa_dedup_removals,
)


def test_sanitize_for_prompt_escapes_braces():
    """Curly braces should be escaped for safe .format() injection."""
    text = "Revenue {up} and risk {down}"
    assert _sanitize_for_prompt(text) == "Revenue {{up}} and risk {{down}}"


def test_timing_summary_calculates_stage_and_total_seconds():
    """Timing summary should include per-stage and total elapsed values."""
    marks = [("start", 0.0), ("extract", 1.25), ("persist", 3.75)]
    summary = _timing_summary(marks)
    assert summary["extract_s"] == 1.25
    assert summary["persist_s"] == 2.5
    assert summary["total_s"] == 3.75


def test_cost_accumulation_and_aggregation():
    """Cost accumulator should aggregate totals across calls."""
    context = {}
    _accumulate_llm_cost(context, {"prompt_tokens": 10, "completion_tokens": 5, "total_cost": 0.01})
    _accumulate_llm_cost(context, {"prompt_tokens": 20, "completion_tokens": 7, "total_cost": 0.02})
    summary = _get_total_llm_cost(context)
    assert summary["total_prompt_tokens"] == 30
    assert summary["total_completion_tokens"] == 12
    assert summary["total_tokens"] == 42
    assert summary["total_cost"] == 0.03
    assert summary["llm_calls"] == 2


def test_format_categories_for_prompt_uses_sanitized_fields(sample_outlook_categories):
    """Category formatter should emit expected XML with escaped unsafe content."""
    categories = [
        {
            **sample_outlook_categories[0],
            "category_name": "Pipelines {M&A}",
            "category_description": "Track {conversion} and backlog",
        }
    ]
    rendered = format_categories_for_prompt(categories)
    assert "<category>" in rendered
    assert "<name>Pipelines {{M&A}}</name>" in rendered
    assert "<description>Track {{conversion}} and backlog</description>" in rendered


def test_aggregate_results_filters_non_content_and_keeps_symbols():
    """Aggregation should include only has_content items with payload arrays."""
    outlook_in = [
        ("Bank A", "A", {"has_content": True, "statements": [{"category": "C1"}]}),
        ("Bank B", "B", {"has_content": False, "statements": []}),
    ]
    section2_in = [
        ("Bank A", "A", {"has_content": True, "questions": [{"category": "Q1"}]}),
        ("Bank C", "C", {"has_content": False, "questions": []}),
    ]
    section3_in = [
        ("Bank D", "D", {"has_content": True, "questions": [{"category": "Q2"}]}),
    ]

    outlook, section2, section3 = aggregate_results(outlook_in, section2_in, section3_in)
    assert list(outlook.keys()) == ["Bank A"]
    assert outlook["Bank A"]["bank_symbol"] == "A"
    assert list(section2.keys()) == ["Bank A"]
    assert list(section3.keys()) == ["Bank D"]


def test_outlook_statement_relevance_score_default():
    """OutlookStatement should default relevance_score to 5."""
    stmt = OutlookStatement(category="M&A", statement="Pipeline is strong.")
    assert stmt.relevance_score == 5


def test_outlook_statement_relevance_score_validation():
    """OutlookStatement should reject scores outside 1-10 range."""
    from pydantic import ValidationError

    try:
        OutlookStatement(category="M&A", statement="S", relevance_score=0)
        assert False, "Should have raised ValidationError"
    except ValidationError:
        pass

    try:
        OutlookStatement(category="M&A", statement="S", relevance_score=11)
        assert False, "Should have raised ValidationError"
    except ValidationError:
        pass

    stmt = OutlookStatement(category="M&A", statement="S", relevance_score=10)
    assert stmt.relevance_score == 10


def test_format_categories_for_prompt_includes_group_tag():
    """Category formatter should include <group> tag when category_group is present."""
    categories = [
        {
            "transcript_sections": "ALL",
            "category_name": "M&A Activity",
            "category_description": "M&A pipeline and deal flow.",
            "category_group": "Investment Banking",
            "example_1": "",
            "example_2": "",
            "example_3": "",
        },
        {
            "transcript_sections": "ALL",
            "category_name": "Trading",
            "category_description": "Trading performance.",
            "category_group": "",
            "example_1": "",
            "example_2": "",
            "example_3": "",
        },
    ]
    rendered = format_categories_for_prompt(categories)
    assert "<group>Investment Banking</group>" in rendered
    # Empty group should not produce a <group> tag
    assert rendered.count("<group>") == 1


def test_format_qa_for_dedup_produces_indexed_xml():
    """Dedup formatter should produce XML with section, bank, category, and index tags."""
    section2 = {
        "Bank A": {
            "bank_symbol": "A",
            "questions": [
                {
                    "category": "Risk",
                    "verbatim_question": "How are you managing risk?",
                    "analyst_name": "Jane",
                },
                {
                    "category": "Risk",
                    "verbatim_question": "VaR exposure details?",
                    "analyst_name": "Bob",
                },
            ],
        }
    }
    section3 = {
        "Bank A": {
            "bank_symbol": "A",
            "questions": [
                {
                    "category": "M&A",
                    "verbatim_question": "M&A pipeline outlook?",
                    "analyst_name": "Jane",
                },
            ],
        }
    }
    xml = _format_qa_for_dedup(section2, section3)
    assert "<section>section2</section>" in xml
    assert "<section>section3</section>" in xml
    assert "<question_index>0</question_index>" in xml
    assert "<question_index>1</question_index>" in xml
    assert "How are you managing risk?" in xml
    assert "M&amp;A pipeline outlook?" in xml or "M&A pipeline outlook?" in xml


def test_apply_qa_dedup_removals_removes_correct_questions():
    """Dedup removals should remove the specified questions by index."""
    section2 = {
        "Bank A": {
            "bank_symbol": "A",
            "questions": [
                {"category": "Risk", "verbatim_question": "Q1"},
                {"category": "Risk", "verbatim_question": "Q2 (duplicate)"},
                {"category": "Risk", "verbatim_question": "Q3"},
            ],
        }
    }
    section3 = {}

    dedup_response = {
        "analysis_notes": "Found 1 duplicate",
        "duplicate_questions": [
            {
                "bank": "Bank A",
                "section": "section2",
                "category": "Risk",
                "question_index": 1,
                "duplicate_of_section": "section2",
                "duplicate_of_category": "Risk",
                "duplicate_of_question_index": 0,
                "reasoning": "Same question",
            }
        ],
    }

    removed = _apply_qa_dedup_removals(section2, section3, dedup_response, "test-exec")
    assert removed == 1
    remaining = [q["verbatim_question"] for q in section2["Bank A"]["questions"]]
    assert remaining == ["Q1", "Q3"]


def test_apply_qa_dedup_removals_reverse_index_safety():
    """Removing multiple indices from same bank should work in reverse order."""
    section2 = {
        "Bank A": {
            "bank_symbol": "A",
            "questions": [
                {"category": "Risk", "verbatim_question": "Keep"},
                {"category": "Risk", "verbatim_question": "Remove1"},
                {"category": "Risk", "verbatim_question": "Remove2"},
                {"category": "Risk", "verbatim_question": "Keep2"},
            ],
        }
    }
    section3 = {}

    dedup_response = {
        "duplicate_questions": [
            {
                "bank": "Bank A",
                "section": "section2",
                "category": "Risk",
                "question_index": 1,
                "duplicate_of_section": "section2",
                "duplicate_of_category": "Risk",
                "duplicate_of_question_index": 0,
            },
            {
                "bank": "Bank A",
                "section": "section2",
                "category": "Risk",
                "question_index": 2,
                "duplicate_of_section": "section2",
                "duplicate_of_category": "Risk",
                "duplicate_of_question_index": 0,
            },
        ],
    }

    removed = _apply_qa_dedup_removals(section2, section3, dedup_response, "test-exec")
    assert removed == 2
    remaining = [q["verbatim_question"] for q in section2["Bank A"]["questions"]]
    assert remaining == ["Keep", "Keep2"]


def test_apply_qa_dedup_removals_empty_duplicates():
    """No duplicates should result in zero removals."""
    section2 = {"Bank A": {"questions": [{"category": "C", "verbatim_question": "Q"}]}}
    removed = _apply_qa_dedup_removals(section2, {}, {"duplicate_questions": []}, "test")
    assert removed == 0


def test_apply_qa_dedup_removals_scoped_to_bank():
    """A removal targeting Bank A should NOT affect Bank B with same category/index."""
    section2 = {
        "Bank A": {
            "bank_symbol": "A",
            "questions": [
                {"category": "Risk", "verbatim_question": "Q-A-0"},
                {"category": "Risk", "verbatim_question": "Q-A-1 (dup)"},
            ],
        },
        "Bank B": {
            "bank_symbol": "B",
            "questions": [
                {"category": "Risk", "verbatim_question": "Q-B-0"},
                {"category": "Risk", "verbatim_question": "Q-B-1 (keep)"},
            ],
        },
    }
    section3 = {}

    dedup_response = {
        "duplicate_questions": [
            {
                "bank": "Bank A",
                "section": "section2",
                "category": "Risk",
                "question_index": 1,
                "duplicate_of_section": "section2",
                "duplicate_of_category": "Risk",
                "duplicate_of_question_index": 0,
                "reasoning": "Duplicate within Bank A",
            }
        ],
    }

    removed = _apply_qa_dedup_removals(section2, section3, dedup_response, "test-exec")
    assert removed == 1
    # Bank A should have 1 question remaining
    assert len(section2["Bank A"]["questions"]) == 1
    assert section2["Bank A"]["questions"][0]["verbatim_question"] == "Q-A-0"
    # Bank B should be untouched
    assert len(section2["Bank B"]["questions"]) == 2
    assert section2["Bank B"]["questions"][1]["verbatim_question"] == "Q-B-1 (keep)"
