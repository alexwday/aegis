"""Benchmark helpers for analyst-reviewed recall evaluation."""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
import re
from typing import Any, Dict, List, Optional

ALLOWED_MISS_REASONS = {
    "parser_loss",
    "qa_boundary_loss",
    "extraction_miss",
    "wrong_category",
    "emerging_topic_miss",
}
_STATE_BLOCK_RE = re.compile(
    r"/\*\s*__BEGIN_STATE__\s*\*/\s*(.*?)\s*/\*\s*__END_STATE__\s*\*/",
    re.DOTALL,
)


def _derive_miss_reason(
    expected_item: Dict[str, Any],
    predicted_item: Optional[Dict[str, Any]],
    parent_ids: set[str],
) -> str:
    """Infer a miss reason when an explicit override is not provided."""
    explicit_reason = str(expected_item.get("miss_reason", "")).strip()
    if explicit_reason in ALLOWED_MISS_REASONS:
        return explicit_reason

    expected_bucket_id = str(expected_item.get("expected_bucket_id", "")).strip()
    if predicted_item is None:
        if expected_item.get("transcript_section") == "QA":
            parent_record_id = str(expected_item.get("parent_record_id", "")).strip()
            if parent_record_id and parent_record_id not in parent_ids:
                return "qa_boundary_loss"
        return "parser_loss"

    selected_bucket_id = str(predicted_item.get("selected_bucket_id", "")).strip()
    status = str(predicted_item.get("status", "")).strip()
    emerging_topic = bool(predicted_item.get("emerging_topic"))

    if expected_bucket_id and selected_bucket_id and selected_bucket_id != expected_bucket_id:
        return "wrong_category"
    if emerging_topic and status != "selected":
        return "emerging_topic_miss"
    return "extraction_miss"


def benchmark_recall(
    predicted_items: List[Dict[str, Any]],
    expected_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Compare predicted evidence against analyst-reviewed expectations.

    Args:
        predicted_items: Evidence records emitted by the editor ETL.
        expected_items: Analyst-reviewed records keyed by `evidence_id`. Optional
            fields include `expected_bucket_id`, `transcript_section`,
            `parent_record_id`, and `miss_reason`.

    Returns:
        Summary metrics with captured counts and miss-reason breakdown.
    """
    predicted_by_id = {
        str(item.get("sid") or item.get("evidence_id")): item
        for item in predicted_items
        if item.get("sid") or item.get("evidence_id")
    }
    parent_ids = {
        str(item.get("parent_record_id", "")).strip()
        for item in predicted_items
        if item.get("parent_record_id")
    }

    captured = 0
    wrong_category = 0
    misses: List[Dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()

    for expected_item in expected_items:
        evidence_id = str(expected_item.get("evidence_id", "")).strip()
        if not evidence_id:
            continue

        predicted_item = predicted_by_id.get(evidence_id)
        expected_bucket_id = str(expected_item.get("expected_bucket_id", "")).strip()
        selected_bucket_id = (
            str(predicted_item.get("selected_bucket_id", "")).strip() if predicted_item else ""
        )
        selected = bool(predicted_item) and str(predicted_item.get("status", "")).strip() == "selected"

        if selected and (not expected_bucket_id or selected_bucket_id == expected_bucket_id):
            captured += 1
            continue

        if selected and expected_bucket_id and selected_bucket_id != expected_bucket_id:
            wrong_category += 1

        miss_reason = _derive_miss_reason(expected_item, predicted_item, parent_ids)
        reason_counts[miss_reason] += 1
        misses.append(
            {
                "evidence_id": evidence_id,
                "expected_bucket_id": expected_bucket_id,
                "predicted_bucket_id": selected_bucket_id,
                "predicted_status": predicted_item.get("status", "") if predicted_item else "",
                "miss_reason": miss_reason,
            }
        )

    total_expected = len([item for item in expected_items if item.get("evidence_id")])
    recall = (captured / total_expected) if total_expected else 1.0
    return {
        "total_expected": total_expected,
        "captured": captured,
        "recall": round(recall, 4),
        "wrong_category": wrong_category,
        "miss_reason_counts": dict(reason_counts),
        "misses": misses,
    }


def extract_state_from_html(html_text: str) -> Dict[str, Any]:
    """Extract embedded report state JSON from a saved interactive HTML report."""
    match = _STATE_BLOCK_RE.search(html_text)
    if not match:
        raise ValueError("Could not find embedded report state markers in HTML file.")
    return json.loads(match.group(1).strip())


def flatten_predicted_items(payload: Any) -> List[Dict[str, Any]]:
    """Flatten report-state or bank-data payloads into benchmark evidence records."""
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]

    bank_states: Dict[str, Dict[str, Any]] = {}
    banks_data: Dict[str, Dict[str, Any]] = {}

    if isinstance(payload, dict):
        if isinstance(payload.get("banks"), dict):
            banks_data = payload.get("banks", {})
            bank_states = payload.get("bank_states", {}) or {}
        elif "md_blocks" in payload or "qa_conversations" in payload:
            ticker = str(payload.get("ticker") or payload.get("bank_id") or "bank")
            banks_data = {ticker: payload}
        elif all(
            isinstance(value, dict) and ("md_blocks" in value or "qa_conversations" in value)
            for value in payload.values()
        ):
            banks_data = payload

    if not banks_data:
        raise ValueError("Unsupported predicted payload format for benchmark flattening.")

    predicted_items: List[Dict[str, Any]] = []
    for bank_id, bank_data in banks_data.items():
        bank_state = bank_states.get(bank_id, {}) if isinstance(bank_states, dict) else {}
        status_overrides = bank_state.get("sentence_status_overrides", {}) or {}
        primary_overrides = bank_state.get("sentence_user_primary", {}) or {}

        def _effective_item(sentence: Dict[str, Any]) -> Dict[str, Any]:
            sid = str(sentence.get("sid") or sentence.get("evidence_id") or "").strip()
            status = str(status_overrides.get(sid) or sentence.get("status") or "").strip()
            selected_bucket_id = str(
                primary_overrides.get(sid)
                or sentence.get("selected_bucket_id")
                or sentence.get("primary")
                or ""
            ).strip()
            return {
                "sid": sid,
                "selected_bucket_id": selected_bucket_id,
                "status": status,
                "emerging_topic": not bool(selected_bucket_id),
                "parent_record_id": str(sentence.get("parent_record_id", "")).strip(),
                "transcript_section": str(sentence.get("transcript_section", "")).strip(),
            }

        for block in bank_data.get("md_blocks", []):
            for sentence in block.get("sentences", []):
                predicted_items.append(_effective_item(sentence))

        for conversation in bank_data.get("qa_conversations", []):
            for sentence in conversation.get("question_sentences", []):
                predicted_items.append(_effective_item(sentence))
            for sentence in conversation.get("answer_sentences", []):
                predicted_items.append(_effective_item(sentence))

    return [item for item in predicted_items if item.get("sid")]


def load_predicted_items(path: str) -> List[Dict[str, Any]]:
    """Load benchmark predicted items from JSON payloads or saved HTML reports."""
    file_path = Path(path)
    raw_text = file_path.read_text(encoding="utf-8")

    if file_path.suffix.lower() == ".html":
        payload = extract_state_from_html(raw_text)
    else:
        payload = json.loads(raw_text)

    return flatten_predicted_items(payload)


def load_expected_items(path: str) -> List[Dict[str, Any]]:
    """Load analyst-reviewed benchmark expectations from JSON."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("expected_items"), list):
        return payload["expected_items"]
    raise ValueError("Expected benchmark file must be a list or contain `expected_items`.")


def render_benchmark_report(result: Dict[str, Any]) -> str:
    """Render a compact markdown benchmark report."""
    lines = [
        "# Recall Benchmark",
        "",
        f"- Total expected: {result.get('total_expected', 0)}",
        f"- Captured: {result.get('captured', 0)}",
        f"- Recall: {result.get('recall', 0):.4f}",
        f"- Wrong category: {result.get('wrong_category', 0)}",
        "",
        "## Miss Reasons",
    ]

    reason_counts = result.get("miss_reason_counts", {}) or {}
    if reason_counts:
        for reason, count in sorted(reason_counts.items()):
            lines.append(f"- {reason}: {count}")
    else:
        lines.append("- none")

    misses = result.get("misses", []) or []
    lines.extend(["", "## Misses"])
    if misses:
        for miss in misses:
            lines.append(
                "- "
                f"{miss.get('evidence_id', '')}: expected={miss.get('expected_bucket_id', '') or '—'}, "
                f"predicted={miss.get('predicted_bucket_id', '') or '—'}, "
                f"status={miss.get('predicted_status', '') or '—'}, "
                f"reason={miss.get('miss_reason', '') or '—'}"
            )
    else:
        lines.append("- none")

    return "\n".join(lines)
