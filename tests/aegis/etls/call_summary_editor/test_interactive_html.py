"""Tests for interactive HTML rendering helpers."""

from aegis.etls.call_summary_editor.interactive_html import build_report_state, generate_html


def test_generate_html_injects_period_and_state():
    banks_data = {
        "RY-CA": {
            "ticker": "RY-CA",
            "company_name": "Royal Bank of Canada",
            "transcript_title": "Royal Bank of Canada Q3 2024 Earnings Call",
            "fiscal_year": 2024,
            "fiscal_quarter": "Q3",
            "md_blocks": [],
            "qa_conversations": [],
        }
    }
    categories = [
        {
            "transcript_sections": "ALL",
            "report_section": "Results Summary",
            "category_name": "Revenue",
            "category_description": "Revenue and income analysis.",
        }
    ]
    config_review = {
        "RY-CA": {
            "existing_section_updates": [
                {
                    "bucket_index": 0,
                    "bucket_id": "bucket_0",
                    "category_name": "Revenue",
                    "gap_summary": "The current description misses fee mix changes.",
                    "why_update": "The transcript highlights fee mix shifts that fit the existing bucket.",
                    "supporting_evidence": ["Fee mix improved year over year."],
                    "proposed_config_row": {
                        "transcript_sections": "ALL",
                        "report_section": "Results Summary",
                        "category_name": "Revenue",
                        "category_description": "Revenue, income, and fee mix analysis.",
                        "example_1": "Fee mix improved year over year.",
                        "example_2": "",
                        "example_3": "",
                    },
                }
            ],
            "new_section_suggestions": [
                {
                    "category_name": "AI",
                    "why_new_section": "AI became a standalone management theme.",
                    "supporting_evidence": ["Management discussed AI deployment."],
                    "suggested_subtitle": "AI shifts from pilot to execution",
                    "proposed_config_row": {
                        "transcript_sections": "MD",
                        "report_section": "Results Summary",
                        "category_name": "AI",
                        "category_description": "AI strategy and deployment commentary.",
                        "example_1": "Management discussed AI deployment.",
                        "example_2": "",
                        "example_3": "",
                    },
                }
            ],
        }
    }

    state = build_report_state(
        banks_data=banks_data,
        categories=categories,
        fiscal_year=2024,
        fiscal_quarter="Q3",
        min_importance=4.0,
        bucket_headlines={"bucket_0": "Revenue growth remains strong"},
        config_review_by_bank=config_review,
    )
    html = generate_html(
        state=state,
        fiscal_year=2024,
        fiscal_quarter="Q3",
        min_importance=4.0,
    )

    assert "Q3 2024" in html
    assert "Royal Bank of Canada" in html
    assert "Revenue growth remains strong" in html
    assert state["config_review"]["by_bank"]["RY-CA"]["new_section_suggestions"][0]["category_name"] == "AI"
    assert state["next_bucket_seq"] == 1
    assert len(state["buckets"]) == 1
    assert state["bank_states"]["RY-CA"]["force_included_sentences"] == []
    assert 'id="config-review-shell"' in html
    assert "Config Analysis Review" in html
    assert "No description provided." not in html
