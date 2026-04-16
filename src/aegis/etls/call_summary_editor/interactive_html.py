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
_MODULE_DIR = Path(__file__).resolve().parent
DEFAULT_BANNER_PATH = _MODULE_DIR / "templates" / "banner.svg"
TEMPLATE_PATH = _MODULE_DIR / "templates" / "report.html"


@lru_cache(maxsize=1)
def _load_html_template() -> str:
    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"HTML template not found at {TEMPLATE_PATH}")
    return TEMPLATE_PATH.read_text(encoding="utf-8")


def load_banner_b64(path: Path) -> Optional[str]:
    """Return the banner file encoded as a `data:` URL, or None if missing."""
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

    bank_states = {
        ticker: {
            "sentence_user_primary": {},
            "sentence_status_overrides": {},
            "subquote_bucket_overrides": {},
            "bucket_subquote_order": {},
            "subquote_formats": {},
        }
        for ticker in banks_data
    }

    config_change_proposals_state = {"by_bank": {}}
    for ticker in banks_data:
        bank_review = raw_config_reviews.get(ticker, {})
        proposals = []
        for idx, proposal in enumerate(bank_review.get("config_change_proposals", []), start=1):
            current_row = proposal.get("current_row", {})
            proposed_row = proposal.get("proposed_row", {})
            proposals.append(
                {
                    "id": proposal.get("id") or f"{ticker}_proposal_{idx}",
                    "change_type": proposal.get("change_type", "update_existing"),
                    "change_summary": proposal.get("change_summary", ""),
                    "target_bucket_index": proposal.get("target_bucket_index", -1),
                    "target_bucket_id": proposal.get("target_bucket_id"),
                    "target_category_name": proposal.get("target_category_name", ""),
                    "suggested_subtitle": proposal.get("suggested_subtitle", ""),
                    "linked_evidence_ids": [
                        evidence_id
                        for evidence_id in proposal.get("linked_evidence_ids", [])
                        if evidence_id
                    ],
                    "adopted_bucket_id": proposal.get("adopted_bucket_id"),
                    "current_row": {
                        "transcript_sections": current_row.get("transcript_sections", "ALL"),
                        "report_section": current_row.get("report_section", "Results Summary"),
                        "category_name": current_row.get("category_name", ""),
                        "category_description": current_row.get("category_description", ""),
                        "example_1": current_row.get("example_1", ""),
                        "example_2": current_row.get("example_2", ""),
                        "example_3": current_row.get("example_3", ""),
                    },
                    "proposed_row": {
                        "transcript_sections": proposed_row.get("transcript_sections", "ALL"),
                        "report_section": proposed_row.get("report_section", "Results Summary"),
                        "category_name": proposed_row.get("category_name", ""),
                        "category_description": proposed_row.get("category_description", ""),
                        "example_1": proposed_row.get("example_1", ""),
                        "example_2": proposed_row.get("example_2", ""),
                        "example_3": proposed_row.get("example_3", ""),
                    },
                    "supporting_quotes": [
                        {
                            "evidence_id": quote.get("evidence_id", ""),
                            "quote": quote.get("quote", ""),
                            "speaker": quote.get("speaker", ""),
                            "transcript_section": quote.get("transcript_section", ""),
                        }
                        for quote in proposal.get("supporting_quotes", [])
                        if quote
                    ],
                }
            )

        config_change_proposals_state["by_bank"][ticker] = {"proposals": proposals}

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
        "config_change_proposals": config_change_proposals_state,
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
    # Resolve the banner before serialization so the data: URL travels through
    # the same JSON-escaping path as the rest of the state. The previous
    # implementation post-patched the serialized string by string-replacing
    # `"banner_src": null`, which silently failed if json.dumps formatting
    # changed or if any other field happened to serialize the same way.
    state_with_banner = dict(state)
    banner_b64 = load_banner_b64(banner_path or DEFAULT_BANNER_PATH)
    if banner_b64 and not state_with_banner.get("banner_src"):
        state_with_banner["banner_src"] = banner_b64

    state_json = json.dumps(state_with_banner, ensure_ascii=False, indent=2).translate(
        {
            ord("<"): "\\u003c",
            ord(">"): "\\u003e",
            ord("&"): "\\u0026",
            ord("\u2028"): "\\u2028",
            ord("\u2029"): "\\u2029",
        }
    )
    html = _load_html_template()
    html = html.replace("__PERIOD__", f"{fiscal_quarter} {fiscal_year}")
    html = html.replace("__MIN_IMPORTANCE__", str(min_importance))
    html = html.replace("__STATE_JSON__", state_json)
    return html
