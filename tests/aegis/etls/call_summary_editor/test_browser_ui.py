"""Browser-level UI tests for the interactive call_summary_editor HTML."""

from __future__ import annotations

import os
import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator

import pytest

playwright = pytest.importorskip("playwright.sync_api")
from playwright.sync_api import Page, expect, sync_playwright

from aegis.etls.call_summary_editor.interactive_html import build_report_state, generate_html

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_CALL_SUMMARY_EDITOR_BROWSER_TESTS") != "1",
    reason="Set RUN_CALL_SUMMARY_EDITOR_BROWSER_TESTS=1 to run browser UI tests.",
)

_SORTABLE_STUB = """
window.Sortable = {
  create: function(element, options) {
    return {
      el: element,
      options: options || {},
      destroy: function() {}
    };
  }
};
"""


class _SilentSimpleHTTPRequestHandler(SimpleHTTPRequestHandler):
    """Quiet HTTP handler for serving local browser-test fixtures."""

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        del format, args


@pytest.fixture
def browser_page() -> Iterator[Page]:
    """Launch a browser page with downloads enabled and local Sortable stubbed."""
    with sync_playwright() as playwright_context:
        browser = playwright_context.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.route(
            "https://cdn.jsdelivr.net/npm/sortablejs@1.15.6/Sortable.min.js",
            lambda route: route.fulfill(
                status=200,
                content_type="application/javascript",
                body=_SORTABLE_STUB,
            ),
        )
        yield page
        context.close()
        browser.close()


@pytest.fixture
def served_report(tmp_path: Path) -> Iterator[str]:
    """Write a realistic report HTML fixture and serve it over localhost."""
    banks_data = {
        "RY-CA": {
            "ticker": "RY-CA",
            "company_name": "Royal Bank of Canada",
            "transcript_title": "Royal Bank of Canada Q1 2026 Earnings Call",
            "fiscal_year": 2026,
            "fiscal_quarter": "Q1",
            "md_blocks": [
                {
                    "id": "RY-CA_MD_1",
                    "speaker": "Chief Financial Officer",
                    "speaker_title": "CFO",
                    "speaker_affiliation": "Royal Bank of Canada",
                    "sentences": [
                        {
                            "sid": "md_1",
                            "text": "Revenue remained resilient across client segments.",
                            "verbatim_text": "Revenue remained resilient across client segments.",
                            "primary": "bucket_0",
                            "selected_bucket_id": "bucket_0",
                            "candidate_bucket_ids": ["bucket_0"],
                            "scores": {"bucket_0": 8.8},
                            "importance_score": 8.2,
                            "status": "selected",
                            "source_block_id": "RY-CA_MD_1",
                            "parent_record_id": "RY-CA_MD_1",
                            "transcript_section": "MD",
                            "para_idx": 0,
                            "emerging_topic": False,
                            "condensed": "Revenue remained resilient across client segments.",
                        },
                        {
                            "sid": "md_2",
                            "text": "Expenses are stabilizing and we expect continued discipline.",
                            "verbatim_text": (
                                "Expenses are stabilizing and we expect continued discipline."
                            ),
                            "primary": "",
                            "selected_bucket_id": "",
                            "candidate_bucket_ids": ["bucket_0"],
                            "scores": {"bucket_0": 6.2},
                            "importance_score": 5.8,
                            "status": "candidate",
                            "source_block_id": "RY-CA_MD_1",
                            "parent_record_id": "RY-CA_MD_1",
                            "transcript_section": "MD",
                            "para_idx": 0,
                            "emerging_topic": True,
                            "condensed": "Expenses are stabilizing with continued discipline.",
                        },
                        {
                            "sid": "md_3",
                            "text": "Agentic AI is now a standalone operating priority.",
                            "verbatim_text": "Agentic AI is now a standalone operating priority.",
                            "primary": "",
                            "selected_bucket_id": "",
                            "candidate_bucket_ids": ["bucket_0"],
                            "scores": {"bucket_0": 5.1},
                            "importance_score": 8.1,
                            "status": "candidate",
                            "source_block_id": "RY-CA_MD_1",
                            "parent_record_id": "RY-CA_MD_1",
                            "transcript_section": "MD",
                            "para_idx": 0,
                            "emerging_topic": True,
                            "condensed": "Agentic AI is a standalone operating priority.",
                        },
                    ],
                },
                {
                    "id": "RY-CA_MD_2",
                    "speaker": "Chief Executive Officer",
                    "speaker_title": "CEO",
                    "speaker_affiliation": "Royal Bank of Canada",
                    "sentences": [
                        {
                            "sid": "md_4",
                            "text": "Capital generation remains strong across the franchise.",
                            "verbatim_text": "Capital generation remains strong across the franchise.",
                            "primary": "bucket_1",
                            "selected_bucket_id": "bucket_1",
                            "candidate_bucket_ids": ["bucket_1"],
                            "scores": {"bucket_1": 8.6},
                            "importance_score": 8.0,
                            "status": "selected",
                            "source_block_id": "RY-CA_MD_2",
                            "parent_record_id": "RY-CA_MD_2",
                            "transcript_section": "MD",
                            "para_idx": 0,
                            "emerging_topic": False,
                            "condensed": "Capital generation remains strong.",
                        }
                    ],
                }
            ],
            "qa_conversations": [
                {
                    "id": "RY-CA_QA_1",
                    "analyst_name": "Jane Analyst",
                    "analyst_affiliation": "Big Bank Securities",
                    "executive_name": "Chief Financial Officer",
                    "executive_title": "CFO",
                    "question_sentences": [
                        {
                            "sid": "qa_q1",
                            "text": "Can you walk through what drove the capital build this quarter?",
                            "verbatim_text": (
                                "Can you walk through what drove the capital build this quarter?"
                            ),
                            "primary": "bucket_1",
                            "selected_bucket_id": "bucket_1",
                            "candidate_bucket_ids": ["bucket_1"],
                            "scores": {"bucket_1": 7.4},
                            "importance_score": 6.6,
                            "status": "selected",
                            "source_block_id": "RY-CA_QA_1",
                            "parent_record_id": "RY-CA_QA_1",
                            "transcript_section": "QA",
                            "para_idx": 0,
                            "emerging_topic": False,
                            "condensed": "Question on capital build drivers.",
                        }
                    ],
                    "answer_sentences": [
                        {
                            "sid": "qa_a1",
                            "text": "We generated capital through earnings and lower RWA intensity.",
                            "verbatim_text": (
                                "We generated capital through earnings and lower RWA intensity."
                            ),
                            "primary": "",
                            "selected_bucket_id": "",
                            "candidate_bucket_ids": ["bucket_1"],
                            "scores": {"bucket_1": 7.9},
                            "importance_score": 7.1,
                            "status": "candidate",
                            "source_block_id": "RY-CA_QA_1",
                            "parent_record_id": "RY-CA_QA_1",
                            "transcript_section": "QA",
                            "para_idx": 0,
                            "emerging_topic": False,
                            "condensed": "Capital came from earnings and lower RWA intensity.",
                        }
                    ],
                }
            ],
        }
    }
    categories = [
        {
            "transcript_sections": "ALL",
            "report_section": "Results Summary",
            "category_name": "Revenue",
            "category_description": "Revenue and income analysis.",
            "example_1": "",
            "example_2": "",
            "example_3": "",
        },
        {
            "transcript_sections": "ALL",
            "report_section": "Results Summary",
            "category_name": "Capital",
            "category_description": "Capital generation and CET1 commentary.",
            "example_1": "",
            "example_2": "",
            "example_3": "",
        }
    ]
    config_review = {
        "RY-CA": {
            "config_change_proposals": [
                {
                    "id": "RY-CA_emerging_1",
                    "change_type": "new_category",
                    "change_summary": "Agentic AI became a standalone management theme.",
                    "target_bucket_index": -1,
                    "target_category_name": "Agentic AI",
                    "linked_evidence_ids": ["md_3"],
                    "suggested_subtitle": "AI moves from pilot to operating priority",
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
                        "category_name": "Agentic AI",
                        "category_description": "AI deployment, execution, and operating-model commentary.",
                        "example_1": "Agentic AI is now a standalone operating priority.",
                        "example_2": "",
                        "example_3": "",
                    },
                    "supporting_quotes": [
                        {
                            "evidence_id": "md_3",
                            "quote": "Agentic AI is now a standalone operating priority.",
                            "speaker": "Chief Financial Officer, CFO",
                            "transcript_section": "MD",
                        }
                    ],
                }
            ]
        }
    }

    state = build_report_state(
        banks_data=banks_data,
        categories=categories,
        fiscal_year=2026,
        fiscal_quarter="Q1",
        min_importance=4.0,
        bucket_headlines={"bucket_0": "Revenue remains resilient"},
        config_review_by_bank=config_review,
    )
    html = generate_html(
        state=state,
        fiscal_year=2026,
        fiscal_quarter="Q1",
        min_importance=4.0,
    )
    report_path = tmp_path / "report.html"
    report_path.write_text(html, encoding="utf-8")

    handler = partial(_SilentSimpleHTTPRequestHandler, directory=str(tmp_path))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/report.html"
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def _open_transcript(page: Page) -> None:
    page.locator("#tp-rail").click()
    expect(page.locator("#transcript-body")).to_be_visible()


def _open_saved_report(page: Page, saved_path: Path) -> None:
    page.goto(saved_path.as_uri())
    expect(page.locator("#report-page")).to_be_visible()


def test_browser_candidate_sentence_can_be_selected_and_added_to_report(
    browser_page: Page,
    served_report: str,
) -> None:
    browser_page.goto(served_report)
    _open_transcript(browser_page)

    browser_page.get_by_text(
        "Expenses are stabilizing and we expect continued discipline.",
        exact=True,
    ).click()
    browser_page.locator("#s-popover").get_by_text("Selected for report", exact=True).click()

    expect(browser_page.locator("#bkc_bucket_0")).to_have_text("1")
    expect(browser_page.locator("#report-page")).to_contain_text(
        "Expenses are stabilizing and we expect continued discipline."
    )


def test_browser_selected_sentence_can_be_rejected_and_removed_from_report(
    browser_page: Page,
    served_report: str,
) -> None:
    browser_page.goto(served_report)
    _open_transcript(browser_page)

    browser_page.locator("#transcript-body").get_by_text(
        "Revenue remained resilient across client segments.",
        exact=True,
    ).click()
    browser_page.locator("#s-popover").get_by_text("Rejected", exact=True).click()

    expect(browser_page.locator("#report-page")).not_to_contain_text(
        "Revenue remained resilient across client segments."
    )
    expect(browser_page.locator("#report-page")).to_contain_text(
        "Capital generation remains strong across the franchise."
    )


def test_browser_qa_answer_can_be_selected_and_renders_question_context(
    browser_page: Page,
    served_report: str,
) -> None:
    browser_page.goto(served_report)
    _open_transcript(browser_page)

    browser_page.get_by_text(
        "We generated capital through earnings and lower RWA intensity.",
        exact=True,
    ).click()
    browser_page.locator("#s-popover").get_by_text("Selected for report", exact=True).click()

    expect(browser_page.locator("#report-page")).to_contain_text(
        "Can you walk through what drove the capital build this quarter?"
    )
    expect(browser_page.locator("#report-page")).to_contain_text(
        "We generated capital through earnings and lower RWA intensity."
    )


def test_browser_adopt_emerging_topic_creates_new_bucket_with_linked_evidence(
    browser_page: Page,
    served_report: str,
) -> None:
    browser_page.goto(served_report)

    # Emerging topics are auto-enabled on load, so the new section appears
    # in the draft without any user interaction. The top-of-report bar shows
    # a row per topic with an on/off toggle.
    expect(browser_page.locator("#report-page .emg-bar")).to_contain_text("Emerging Topics")

    emerging_section = browser_page.locator("#report-page .bkt-section").filter(
        has=browser_page.locator('input.bkt-name-input[value="Agentic AI"]')
    )
    expect(emerging_section).to_contain_text("Agentic AI is now a standalone operating priority.")


def test_browser_dragging_subquote_to_new_bucket_updates_report(
    browser_page: Page,
    served_report: str,
) -> None:
    browser_page.goto(served_report)

    expect(browser_page.locator("#bs_bucket_0")).to_contain_text(
        "Revenue remained resilient across client segments."
    )
    expect(browser_page.locator("#bs_bucket_1")).to_contain_text(
        "Capital generation remains strong across the franchise."
    )

    browser_page.evaluate(
        """
        () => {
          const card = document.querySelector('[data-subquote-id="SQ_md_1"]');
          const target = document.getElementById('bq_bucket_1');
          if (!card || !target) throw new Error('Missing drag/drop fixture nodes');
          target.appendChild(card);
          handleSubquoteDrop('SQ_md_1', 'bucket_0', 'bucket_1');
        }
        """
    )

    expect(browser_page.locator("#bs_bucket_0")).to_have_count(0)
    expect(browser_page.locator("#bs_bucket_1")).to_contain_text(
        "Revenue remained resilient across client segments."
    )
    expect(browser_page.locator("#bkc_bucket_1")).to_have_text("2")


def test_browser_save_download_persists_updated_state(
    browser_page: Page,
    served_report: str,
    tmp_path: Path,
) -> None:
    browser_page.goto(served_report)
    # Emerging topics auto-enable on load, so the save captures the adopted
    # bucket + linked-evidence state without extra clicks.

    with browser_page.expect_download() as download_info:
        browser_page.get_by_role("button", name="Save").click()

    download = download_info.value
    download_path = tmp_path / "saved_report.html"
    download.save_as(str(download_path))
    saved_html = download_path.read_text(encoding="utf-8")

    assert '"category_name": "Agentic AI"' in saved_html
    assert '"adopted_bucket_id"' in saved_html


def test_browser_saved_report_can_be_reopened_with_persisted_editor_state(
    browser_page: Page,
    served_report: str,
    tmp_path: Path,
) -> None:
    browser_page.goto(served_report)
    _open_transcript(browser_page)

    browser_page.get_by_text(
        "We generated capital through earnings and lower RWA intensity.",
        exact=True,
    ).click()
    browser_page.locator("#s-popover").get_by_text("Selected for report", exact=True).click()
    # Emerging topic adoption now happens automatically on load.

    with browser_page.expect_download() as download_info:
        browser_page.get_by_role("button", name="Save").click()

    download = download_info.value
    download_path = tmp_path / "reopened_report.html"
    download.save_as(str(download_path))

    _open_saved_report(browser_page, download_path)

    expect(browser_page.locator("#report-page")).to_contain_text(
        "We generated capital through earnings and lower RWA intensity."
    )
    expect(browser_page.locator("#report-page")).to_contain_text(
        "Can you walk through what drove the capital build this quarter?"
    )
    expect(
        browser_page.locator("#report-page .bkt-section").filter(
            has=browser_page.locator('input.bkt-name-input[value="Agentic AI"]')
        )
    ).to_contain_text("Agentic AI is now a standalone operating priority.")
