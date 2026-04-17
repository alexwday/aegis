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
            "config_change_proposals": [
                {
                    "change_type": "update_existing",
                    "change_summary": "The current description misses fee mix changes.",
                    "target_bucket_index": 0,
                    "target_bucket_id": "bucket_0",
                    "target_category_name": "Revenue",
                    "linked_evidence_ids": ["ev_1"],
                    "current_row": {
                        "transcript_sections": "ALL",
                        "report_section": "Results Summary",
                        "category_name": "Revenue",
                        "category_description": "Revenue and income analysis.",
                        "example_1": "",
                        "example_2": "",
                        "example_3": "",
                    },
                    "proposed_row": {
                        "transcript_sections": "ALL",
                        "report_section": "Results Summary",
                        "category_name": "Revenue",
                        "category_description": "Revenue, income, and fee mix analysis.",
                        "example_1": "Fee mix improved year over year.",
                        "example_2": "",
                        "example_3": "",
                    },
                    "supporting_quotes": [
                        {
                            "evidence_id": "ev_1",
                            "quote": "Fee mix improved year over year.",
                            "speaker": "Chief Financial Officer",
                            "transcript_section": "MD",
                        }
                    ],
                },
                {
                    "change_type": "new_category",
                    "change_summary": "AI became a standalone management theme.",
                    "target_bucket_index": -1,
                    "target_category_name": "AI",
                    "linked_evidence_ids": ["ev_2"],
                    "suggested_subtitle": "AI shifts from pilot to execution",
                    "current_row": {
                        "transcript_sections": "ALL",
                        "report_section": "Results Summary",
                        "category_name": "",
                        "category_description": "",
                        "example_1": "",
                        "example_2": "",
                        "example_3": "",
                    },
                    "proposed_row": {
                        "transcript_sections": "MD",
                        "report_section": "Results Summary",
                        "category_name": "AI",
                        "category_description": "AI strategy and deployment commentary.",
                        "example_1": "Management discussed AI deployment.",
                        "example_2": "",
                        "example_3": "",
                    },
                    "supporting_quotes": [
                        {
                            "evidence_id": "ev_2",
                            "quote": "Management discussed AI deployment.",
                            "speaker": "Chief Executive Officer",
                            "transcript_section": "MD",
                        }
                    ],
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
    assert (
        state["config_change_proposals"]["by_bank"]["RY-CA"]["proposals"][1]["target_category_name"]
        == "AI"
    )
    assert state["next_bucket_seq"] == 1
    assert len(state["buckets"]) == 1
    assert state["bank_states"]["RY-CA"]["sentence_status_overrides"] == {}
    assert 'id="config-changes-btn"' in html
    assert 'id="emerging-topics-btn"' in html
    assert 'id="proposal-modal"' in html
    assert "Config Change Proposals" in html
    assert "No description provided." not in html
    assert "function renderEmergingTopicsModalBody()" in html
    assert "function renderEmergingTopicsBar()" in html
    assert "function disableEmergingCategory(" in html
    # The Configured/Suggested/Emerging source badge row was removed from the
    # bucket header in favor of an inline `<bucket name>: <generated headline>`
    # layout — the source provenance is no longer surfaced to users.
    assert "bucketSourceLabel(source)" not in html
    assert "bkt-source-badge" not in html
    assert "const context = getSentenceContext(_activeSid);" in html
    assert "const {context} = getSentenceContext(_activeSid);" not in html
    assert "conv.primary_bucket ||" not in html
    assert "source: options.source || 'suggested'" in html
    assert "source: 'suggested'" in html
    assert "source: 'custom'" not in html
