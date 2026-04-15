"""Tests for recall benchmark helpers."""

import json

from aegis.etls.call_summary_editor.benchmark import (
    benchmark_recall,
    extract_state_from_html,
    flatten_predicted_items,
    load_predicted_items,
    render_benchmark_report,
)


def test_benchmark_recall_tracks_capture_and_miss_reasons():
    predicted_items = [
        {
            "sid": "md_1",
            "selected_bucket_id": "bucket_0",
            "status": "selected",
            "parent_record_id": "RY-CA_MD_1",
        },
        {
            "sid": "qa_1",
            "selected_bucket_id": "",
            "status": "candidate",
            "emerging_topic": True,
            "parent_record_id": "RY-CA_QA_1",
        },
        {
            "sid": "qa_2",
            "selected_bucket_id": "bucket_2",
            "status": "selected",
            "parent_record_id": "RY-CA_QA_2",
        },
    ]
    expected_items = [
        {"evidence_id": "md_1", "expected_bucket_id": "bucket_0"},
        {"evidence_id": "qa_1", "expected_bucket_id": "bucket_1"},
        {"evidence_id": "qa_2", "expected_bucket_id": "bucket_1"},
        {
            "evidence_id": "qa_missing",
            "transcript_section": "QA",
            "parent_record_id": "RY-CA_QA_MISSING",
        },
    ]

    result = benchmark_recall(predicted_items, expected_items)

    assert result["captured"] == 1
    assert result["wrong_category"] == 1
    assert result["miss_reason_counts"]["emerging_topic_miss"] == 1
    assert result["miss_reason_counts"]["wrong_category"] == 1
    assert result["miss_reason_counts"]["qa_boundary_loss"] == 1
    assert result["recall"] == 0.25


def test_flatten_predicted_items_applies_saved_editor_overrides():
    payload = {
        "banks": {
            "RY-CA": {
                "md_blocks": [
                    {
                        "sentences": [
                            {
                                "sid": "md_1",
                                "selected_bucket_id": "",
                                "primary": "",
                                "status": "candidate",
                                "parent_record_id": "RY-CA_MD_1",
                                "transcript_section": "MD",
                            }
                        ]
                    }
                ],
                "qa_conversations": [],
            }
        },
        "bank_states": {
            "RY-CA": {
                "sentence_user_primary": {"md_1": "bucket_3"},
                "sentence_status_overrides": {"md_1": "selected"},
            }
        },
    }

    items = flatten_predicted_items(payload)

    assert items == [
        {
            "sid": "md_1",
            "selected_bucket_id": "bucket_3",
            "status": "selected",
            "emerging_topic": False,
            "parent_record_id": "RY-CA_MD_1",
            "transcript_section": "MD",
        }
    ]


def test_load_predicted_items_reads_embedded_state_from_html(tmp_path):
    state = {
        "banks": {
            "RY-CA": {
                "md_blocks": [],
                "qa_conversations": [
                    {
                        "question_sentences": [],
                        "answer_sentences": [
                            {
                                "sid": "qa_1",
                                "selected_bucket_id": "bucket_1",
                                "primary": "bucket_1",
                                "status": "selected",
                                "parent_record_id": "RY-CA_QA_1",
                                "transcript_section": "QA",
                            }
                        ],
                    }
                ],
            }
        },
        "bank_states": {},
    }
    html_path = tmp_path / "report.html"
    html_path.write_text(
        "/* __BEGIN_STATE__ */\n"
        f"{json.dumps(state)}\n"
        "/* __END_STATE__ */\n",
        encoding="utf-8",
    )

    extracted_state = extract_state_from_html(html_path.read_text(encoding="utf-8"))
    items = load_predicted_items(str(html_path))

    assert extracted_state["banks"]["RY-CA"]["qa_conversations"][0]["answer_sentences"][0]["sid"] == "qa_1"
    assert items[0]["sid"] == "qa_1"
    assert items[0]["selected_bucket_id"] == "bucket_1"


def test_render_benchmark_report_includes_summary_and_misses():
    report = render_benchmark_report(
        {
            "total_expected": 2,
            "captured": 1,
            "recall": 0.5,
            "wrong_category": 1,
            "miss_reason_counts": {"wrong_category": 1},
            "misses": [
                {
                    "evidence_id": "qa_2",
                    "expected_bucket_id": "bucket_1",
                    "predicted_bucket_id": "bucket_2",
                    "predicted_status": "selected",
                    "miss_reason": "wrong_category",
                }
            ],
        }
    )

    assert "# Recall Benchmark" in report
    assert "- Recall: 0.5000" in report
    assert "- wrong_category: 1" in report
    assert "qa_2: expected=bucket_1, predicted=bucket_2, status=selected, reason=wrong_category" in report
