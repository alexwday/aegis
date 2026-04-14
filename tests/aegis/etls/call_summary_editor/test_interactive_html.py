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

    state = build_report_state(
        banks_data=banks_data,
        categories=categories,
        fiscal_year=2024,
        fiscal_quarter="Q3",
        min_importance=4.0,
        bucket_headlines={"bucket_0": "Revenue growth remains strong"},
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
