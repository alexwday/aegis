"""HTML rendering helpers for call_summary_editor interactive output."""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BUCKET_COLORS: List[Tuple[str, str]] = [
    ("#E3F2FD", "#1565C0"),
    ("#F3E5F5", "#6A1B9A"),
    ("#E8F5E9", "#2E7D32"),
    ("#FFF3E0", "#E65100"),
    ("#FCE4EC", "#880E4F"),
    ("#E0F7FA", "#00695C"),
    ("#FFF8E1", "#F57F17"),
    ("#E8EAF6", "#283593"),
    ("#F1F8E9", "#558B2F"),
    ("#FBE9E7", "#BF360C"),
    ("#E0F2F1", "#004D40"),
    ("#EDE7F6", "#4527A0"),
    ("#F9FBE7", "#827717"),
    ("#FCE4EC", "#AD1457"),
    ("#E8EAF6", "#1A237E"),
]
OTHER_COLOR: Tuple[str, str] = ("#FAFAFA", "#9E9E9E")
DEFAULT_BANNER_PATH = (
    Path(__file__).resolve().parent.parent / "call_summary_editor_mock" / "banner.svg"
)
MOCK_MAIN_PATH = (
    Path(__file__).resolve().parent.parent
    / "call_summary_editor_mock"
    / "main_call_summary.py"
)


@lru_cache(maxsize=1)
def _load_html_template() -> str:
    text = MOCK_MAIN_PATH.read_text(encoding="utf-8")
    start_marker = 'HTML_TEMPLATE = r"""'
    end_marker = '\n\n# ============================================================\n# HTML GENERATION'
    start = text.find(start_marker)
    end = text.find(end_marker, start)
    if start == -1 or end == -1:
        raise ValueError(f"Could not extract HTML_TEMPLATE from {MOCK_MAIN_PATH}")
    return text[start + len(start_marker) : end].rstrip().removesuffix('"""')


def load_banner_b64(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    data = base64.b64encode(path.read_bytes()).decode()
    ext = path.suffix.lower().lstrip(".")
    mime = {
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "svg": "image/svg+xml",
    }.get(ext, "image/png")
    return f"data:{mime};base64,{data}"


def build_report_state(
    *,
    banks_data: Dict[str, Dict[str, Any]],
    categories: List[Dict[str, Any]],
    fiscal_year: int,
    fiscal_quarter: str,
    min_importance: float,
    bucket_headlines: Optional[Dict[str, str]] = None,
    config_review_by_bank: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Build the mock-compatible report state JSON."""
    headlines = bucket_headlines or {}
    raw_config_reviews = config_review_by_bank or {}
    buckets = []
    for idx, category in enumerate(categories):
        bg, accent = BUCKET_COLORS[idx % len(BUCKET_COLORS)]
        buckets.append(
            {
                "id": f"bucket_{idx}",
                "name": category["category_name"],
                "report_section": category.get("report_section", "Results Summary"),
                "transcript_sections": category["transcript_sections"],
                "description": category["category_description"],
                "color_bg": bg,
                "color_accent": accent,
                "generated_headline": headlines.get(f"bucket_{idx}", ""),
                "source": "config",
            }
        )

    buckets.append(
        {
            "id": "other",
            "name": "Other",
            "report_section": "Other",
            "transcript_sections": "ALL",
            "description": "Quotes not strongly matching any defined bucket.",
            "color_bg": OTHER_COLOR[0],
            "color_accent": OTHER_COLOR[1],
            "generated_headline": "",
            "source": "system",
        }
    )

    bank_states = {
        ticker: {
            "sentence_user_primary": {},
            "excluded_sentences": [],
            "subquote_bucket_overrides": {},
            "bucket_subquote_order": {},
            "subquote_formats": {},
        }
        for ticker in banks_data
    }

    config_review_state = {"by_bank": {}}
    for ticker in banks_data:
        bank_review = raw_config_reviews.get(ticker, {})
        existing_updates = []
        for idx, suggestion in enumerate(bank_review.get("existing_section_updates", []), start=1):
            proposed_row = suggestion.get("proposed_config_row", {})
            existing_updates.append(
                {
                    "id": suggestion.get("id") or f"{ticker}_existing_{idx}",
                    "bucket_index": suggestion.get("bucket_index"),
                    "bucket_id": suggestion.get("bucket_id"),
                    "category_name": suggestion.get("category_name")
                    or proposed_row.get("category_name", ""),
                    "gap_summary": suggestion.get("gap_summary", ""),
                    "why_update": suggestion.get("why_update", ""),
                    "supporting_evidence": [
                        evidence
                        for evidence in suggestion.get("supporting_evidence", [])
                        if evidence
                    ],
                    "proposed_config_row": {
                        "transcript_sections": proposed_row.get("transcript_sections", "ALL"),
                        "report_section": proposed_row.get("report_section", "Results Summary"),
                        "category_name": proposed_row.get("category_name", ""),
                        "category_description": proposed_row.get("category_description", ""),
                        "example_1": proposed_row.get("example_1", ""),
                        "example_2": proposed_row.get("example_2", ""),
                        "example_3": proposed_row.get("example_3", ""),
                    },
                }
            )

        new_section_suggestions = []
        for idx, suggestion in enumerate(bank_review.get("new_section_suggestions", []), start=1):
            proposed_row = suggestion.get("proposed_config_row", {})
            new_section_suggestions.append(
                {
                    "id": suggestion.get("id") or f"{ticker}_new_{idx}",
                    "category_name": suggestion.get("category_name")
                    or proposed_row.get("category_name", ""),
                    "why_new_section": suggestion.get("why_new_section", ""),
                    "supporting_evidence": [
                        evidence
                        for evidence in suggestion.get("supporting_evidence", [])
                        if evidence
                    ],
                    "suggested_subtitle": suggestion.get("suggested_subtitle", ""),
                    "adopted_bucket_id": suggestion.get("adopted_bucket_id"),
                    "proposed_config_row": {
                        "transcript_sections": proposed_row.get("transcript_sections", "ALL"),
                        "report_section": proposed_row.get("report_section", "Results Summary"),
                        "category_name": proposed_row.get("category_name", ""),
                        "category_description": proposed_row.get("category_description", ""),
                        "example_1": proposed_row.get("example_1", ""),
                        "example_2": proposed_row.get("example_2", ""),
                        "example_3": proposed_row.get("example_3", ""),
                    },
                }
            )

        config_review_state["by_bank"][ticker] = {
            "existing_section_updates": existing_updates,
            "new_section_suggestions": new_section_suggestions,
        }

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fiscal_quarter,
            "min_importance": min_importance,
            "version": "2.0",
        },
        "buckets": buckets,
        "banks": banks_data,
        "bank_states": bank_states,
        "current_bank": next(iter(banks_data)) if banks_data else None,
        "banner_visible": True,
        "banner_src": None,
        "bucket_user_titles": {},
        "config_review": config_review_state,
        "next_bucket_seq": len(categories),
    }


def generate_html(
    *,
    state: Dict[str, Any],
    fiscal_year: int,
    fiscal_quarter: str,
    min_importance: float,
    banner_path: Optional[Path] = None,
) -> str:
    """Render the mock HTML shell with injected report state."""
    state_json = json.dumps(state, ensure_ascii=False, indent=2).replace("</script>", "<\\/script>")
    html = _load_html_template()
    html = html.replace("__PERIOD__", f"{fiscal_quarter} {fiscal_year}")
    html = html.replace("__MIN_IMPORTANCE__", str(min_importance))
    html = html.replace("__STATE_JSON__", state_json)

    banner_b64 = load_banner_b64(banner_path or DEFAULT_BANNER_PATH)
    if banner_b64 and '"banner_src": null' in html:
        html = html.replace('"banner_src": null', f'"banner_src": "{banner_b64}"')
    return html
