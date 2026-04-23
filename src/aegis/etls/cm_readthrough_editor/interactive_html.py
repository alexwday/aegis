"""HTML rendering helpers for cm_readthrough_editor interactive output."""

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
_TEMPLATE_DIR = _MODULE_DIR / "templates"
DEFAULT_BANNER_PATH = _TEMPLATE_DIR / "banner.svg"
BANNER_EXTENSIONS: Tuple[str, ...] = ("svg", "png", "jpg", "jpeg")
TEMPLATE_PATH = _MODULE_DIR / "templates" / "report.html"
SECTION_OUTLOOK = "Outlook"
SECTION_QA = "Q&A"


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


def resolve_banner_path(banner_path: Optional[Path] = None) -> Optional[Path]:
    """Resolve an explicit or default banner path with extension fallback.

    When no explicit path is supplied, the editor checks for
    `templates/banner.svg`, `templates/banner.png`, `templates/banner.jpg`, and
    `templates/banner.jpeg` in that order. This lets users replace the banner
    asset by file type without changing call sites.
    """
    if banner_path is not None:
        return banner_path

    for ext in BANNER_EXTENSIONS:
        candidate = _TEMPLATE_DIR / f"banner.{ext}"
        if candidate.exists():
            return candidate
    return None


def _normalize_report_section(value: Optional[str]) -> str:
    """Validate one CM report section label."""
    normalized = str(value or "").strip()
    if normalized in {SECTION_OUTLOOK, SECTION_QA}:
        return normalized
    if not normalized:
        return SECTION_QA
    raise ValueError(f"Unsupported CM report section: {normalized}")


def _normalize_bank_payload(bank_id: str, bank_data: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure each bank payload exposes the fields the CM shell expects."""
    payload = dict(bank_data)
    ticker = (
        payload.get("ticker")
        or payload.get("symbol")
        or payload.get("bank_symbol")
        or payload.get("full_ticker")
        or bank_id
    )
    company_name = payload.get("company_name") or payload.get("name") or bank_id
    payload.setdefault("ticker", ticker)
    payload.setdefault("company_name", company_name)
    payload.setdefault("selector_label", ticker)
    payload.setdefault("report_group", company_name)
    return payload


def build_report_state(
    *,
    banks_data: Dict[str, Dict[str, Any]],
    categories: List[Dict[str, Any]],
    fiscal_year: int,
    fiscal_quarter: str,
    min_importance: float,
    bucket_headlines: Optional[Dict[str, str]] = None,
    config_review_by_bank: Optional[Dict[str, Dict[str, Any]]] = None,
    section_subtitles: Optional[Dict[str, str]] = None,
    cm_main_title: Optional[str] = None,
    report_title: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the CM editor report state JSON."""
    headlines = bucket_headlines or {}
    raw_config_reviews = config_review_by_bank or {}
    normalized_section_subtitles = {
        "outlook": (
            (section_subtitles or {}).get("outlook")
            or "Outlook: Capital markets activity"
        ),
        "qa": (
            (section_subtitles or {}).get("qa")
            or "Conference calls: Capital markets questions"
        ),
    }
    normalized_banks = {
        bank_id: _normalize_bank_payload(bank_id, bank_payload)
        for bank_id, bank_payload in banks_data.items()
    }
    bank_ids = list(normalized_banks.keys())
    first_bank = normalized_banks[bank_ids[0]] if bank_ids else {}
    cover_entity = (
        f"{len(bank_ids)} Banks"
        if len(bank_ids) > 1
        else (first_bank.get("company_name") or first_bank.get("name") or "")
    )
    buckets = []
    for idx, category in enumerate(categories):
        bg, accent = BUCKET_COLORS[idx % len(BUCKET_COLORS)]
        normalized_report_section = _normalize_report_section(category.get("report_section"))
        transcript_scope = (
            "MD_QA_ANSWER" if normalized_report_section == SECTION_OUTLOOK else "QA_QUESTION"
        )
        buckets.append(
            {
                "id": f"bucket_{idx}",
                "name": category["category_name"],
                "report_section": normalized_report_section,
                "transcript_sections": transcript_scope,
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
            change_type = proposal.get("change_type", "update_existing")
            current_row = proposal.get("current_row", {}) or {}
            proposed_row = proposal.get("proposed_row", {}) or {}
            serialized = {
                "id": proposal.get("id") or f"{ticker}_proposal_{idx}",
                "change_type": change_type,
                "change_summary": proposal.get("change_summary", ""),
                "target_bucket_index": proposal.get("target_bucket_index", -1),
                "target_bucket_id": proposal.get("target_bucket_id"),
                "target_category_name": proposal.get("target_category_name", ""),
                "current_row": {
                    "transcript_sections": current_row.get("transcript_sections", "ALL"),
                    "report_section": current_row.get("report_section", SECTION_OUTLOOK),
                    "category_name": current_row.get("category_name", ""),
                    "category_description": current_row.get("category_description", ""),
                    "example_1": current_row.get("example_1", ""),
                    "example_2": current_row.get("example_2", ""),
                    "example_3": current_row.get("example_3", ""),
                },
                "proposed_row": {
                    "transcript_sections": proposed_row.get("transcript_sections", "ALL"),
                    "report_section": proposed_row.get("report_section", SECTION_OUTLOOK),
                    "category_name": proposed_row.get("category_name", ""),
                    "category_description": proposed_row.get("category_description", ""),
                    "example_1": proposed_row.get("example_1", ""),
                    "example_2": proposed_row.get("example_2", ""),
                    "example_3": proposed_row.get("example_3", ""),
                },
            }
            # Emerging topics carry the finding ids the UI will reassign on enable.
            # Description-only updates never carry evidence — that's the point of
            # the slim pass-1 schema.
            if change_type == "new_category":
                serialized["linked_evidence_ids"] = [
                    evidence_id
                    for evidence_id in proposal.get("linked_evidence_ids", [])
                    if evidence_id
                ]
                serialized["adopted_bucket_id"] = proposal.get("adopted_bucket_id")
            proposals.append(serialized)

        config_change_proposals_state["by_bank"][ticker] = {"proposals": proposals}

    return {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fiscal_quarter,
            "min_importance": min_importance,
            "version": "2.0",
            "report_title": report_title or "Capital Markets Readthrough",
            "cover_entity": cover_entity,
            "cm_main_title": cm_main_title
            or f"Read Through For Capital Markets: {fiscal_quarter}/{str(fiscal_year)[2:]} Select Banks",
            "section_subtitles": normalized_section_subtitles,
        },
        "buckets": buckets,
        "banks": normalized_banks,
        "bank_states": bank_states,
        "current_bank": bank_ids[0] if bank_ids else None,
        "report_bank_order": bank_ids,
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
    resolved_banner_path = resolve_banner_path(banner_path)
    banner_b64 = load_banner_b64(resolved_banner_path) if resolved_banner_path else None
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
