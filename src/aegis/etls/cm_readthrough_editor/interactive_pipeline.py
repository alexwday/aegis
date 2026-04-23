"""Interactive CM readthrough processing helpers.

This module keeps the call-summary editor's browser state shape, but the
processing flow is capital-markets specific:

- Management Discussion speaker blocks and management Q&A turns feed Outlook.
- Q&A speaker turns are grouped into analyst/executive conversations using the
  same LLM boundary method as call_summary_editor.
- Analyst questions, not executive answers, are the reportable Q&A findings.
- Transcript blocks retain all sentence text so the UI can show filtered
  speaker blocks by default and expand to the full transcript later.
"""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from xml.sax.saxutils import escape as _xml_escape

from pydantic import BaseModel, Field, ValidationError

from aegis.connections.llm_connector import complete_with_tools
from aegis.etls.prompt_schema import load_prompt_bundle
from aegis.utils.logging import get_logger

logger = get_logger()

MODULE_DIR = Path(__file__).resolve().parent
PROMPTS_DIR = MODULE_DIR / "documentation" / "prompts"
STATUS_PRIORITY = {
    "rejected": 0,
    "context": 1,
    "candidate": 2,
    "selected": 3,
}
SENTENCE_BOUNDARY_RE = re.compile(r"(?<=[.!?])\s+(?=(?:[\"'“‘(\[]?[A-Z0-9]))")
NORMALIZE_RE = re.compile(r"[^a-z0-9]+")


class OutlookStatement(BaseModel):
    """Structured Outlook finding returned by the LLM tool."""

    category_index: int = Field(ge=0)
    source_block_id: str = Field(min_length=1)
    source_sentence_ids: List[str] = Field(min_length=1)
    relevance_score: int = Field(ge=1, le=10)
    is_new_category: bool


class OutlookExtractionResponse(BaseModel):
    """Validated Outlook extraction response."""

    has_content: bool
    statements: List[OutlookStatement]


class QAQuestion(BaseModel):
    """Structured analyst question returned by the LLM tool."""

    category_index: int = Field(ge=0)
    source_block_id: str = Field(min_length=1)
    source_sentence_ids: List[str] = Field(min_length=1)
    relevance_score: int = Field(ge=1, le=10)
    capital_markets_linkage: str = Field(min_length=1)
    is_new_category: bool


class QAExtractionResponse(BaseModel):
    """Validated Q&A extraction response."""

    has_content: bool
    questions: List[QAQuestion]


class QAConversationGroup(BaseModel):
    """One Q&A conversation grouping emitted by the boundary tool."""

    conversation_id: str
    block_indices: List[int] = Field(
        description="1-based speaker block indices from the QA boundary prompt.",
    )


class QABoundaryResult(BaseModel):
    """Boundary grouping response for raw QA speaker blocks."""

    conversations: List[QAConversationGroup]


class SubtitleResponse(BaseModel):
    """Validated section-subtitle response."""

    subtitle: str = Field(min_length=1)


TOOL_QA_BOUNDARY = {
    "type": "function",
    "function": {
        "name": "group_qa_conversations",
        "strict": True,
        "description": (
            "Call this tool when an indexed Q&A speaker-block list needs conversation "
            "boundaries. Group the blocks into contiguous analyst-to-executive exchanges "
            "and return a non-empty ordered `block_indices` array for every conversation. "
            "Every indexed block must appear exactly once across the returned conversations."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "conversations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "conversation_id": {"type": "string"},
                            "block_indices": {
                                "type": "array",
                                "items": {"type": "integer"},
                                "description": (
                                    "1-based speaker block indices from the prompt, listed "
                                    "in transcript order."
                                ),
                            },
                        },
                        "required": ["conversation_id", "block_indices"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["conversations"],
            "additionalProperties": False,
        },
    },
}


def _append_metrics(context: Dict[str, Any], metrics: Optional[Dict[str, Any]]) -> None:
    """Accumulate LLM metrics on the shared ETL context."""
    if not metrics:
        return
    context.setdefault("_llm_costs", []).append(metrics)


def _load_prompt(name: str) -> Dict[str, Any]:
    """Load one local YAML prompt bundle."""
    return load_prompt_bundle(PROMPTS_DIR / f"{name}.yaml")


async def _call_tool(
    *,
    messages: List[Dict[str, str]],
    tool: Dict[str, Any],
    label: str,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
    warn_on_missing: bool = True,
) -> Optional[Dict[str, Any]]:
    """Run one structured LLM tool call and parse its arguments."""
    call_llm_params = dict(llm_params)
    call_llm_params["tool_choice"] = {
        "type": "function",
        "function": {"name": tool["function"]["name"]},
    }
    response = await complete_with_tools(
        messages=messages,
        tools=[tool],
        context=context,
        llm_params=call_llm_params,
    )
    _append_metrics(context, response.get("metrics"))

    choices = response.get("choices") or []
    message = choices[0].get("message", {}) if choices else {}
    tool_calls = message.get("tool_calls") or []
    if not tool_calls:
        if warn_on_missing:
            logger.warning(
                "LLM tool call missing structured output",
                stage=label,
                finish_reason=choices[0].get("finish_reason") if choices else None,
                content_preview=_preview_text(message.get("content", ""), 300),
            )
        return None

    arguments = tool_calls[0].get("function", {}).get("arguments", "{}")
    if isinstance(arguments, dict):
        return arguments
    try:
        return json.loads(arguments)
    except json.JSONDecodeError as exc:
        logger.warning("LLM tool call returned invalid JSON", stage=label, error=str(exc))
        return None


def _clean_text(text: str) -> str:
    """Collapse whitespace and trim."""
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _escape_for_prompt(value: Any) -> str:
    """Escape a value for safe interpolation inside XML-style prompt blocks."""
    return _xml_escape(str(value or ""), entities={'"': "&quot;"})


def _xml_block(tag: str, content: str) -> str:
    """Wrap dynamic prompt content in an XML block."""
    body = (content or "").strip()
    return f"<{tag}>\n{body}\n</{tag}>"


def _summarise_validation_errors(errors: List[str], limit: int = 2) -> str:
    """Collapse validation errors into a short log-friendly preview."""
    preview = "; ".join(errors[:limit])
    if len(errors) > limit:
        preview += f" (+{len(errors) - limit} more)"
    return preview


def split_sentences(text: str) -> List[str]:
    """Split a paragraph into sentence-like units."""
    cleaned = _clean_text(text)
    if not cleaned:
        return []
    parts = SENTENCE_BOUNDARY_RE.split(cleaned)
    return [part.strip() for part in parts if part and part.strip()]


def _normalize_match_text(text: str) -> str:
    """Normalize text for loose verbatim matching."""
    lowered = _clean_text(text).lower()
    return NORMALIZE_RE.sub(" ", lowered).strip()


def _bucket_id_for_category(
    category_name: str,
    categories: Sequence[Dict[str, Any]],
    *,
    report_section: str,
) -> str:
    """Resolve a category name to the UI bucket id."""
    target = _clean_text(category_name).lower()
    for idx, category in enumerate(categories):
        if str(category.get("report_section", "")).strip() != report_section:
            continue
        if _clean_text(category.get("category_name", "")).lower() == target:
            return f"bucket_{idx}"
    return ""


def _bucket_id_for_category_index(
    category_index: int,
    categories: Sequence[Dict[str, Any]],
    *,
    report_section: str,
) -> str:
    """Resolve a category index to the UI bucket id."""
    for category in categories:
        if int(category.get("category_index", -1)) != int(category_index):
            continue
        if str(category.get("report_section", "")).strip() != report_section:
            continue
        return f"bucket_{category_index}"
    return ""


def _categories_for_section(
    categories: Sequence[Dict[str, Any]],
    *,
    report_section: str,
    transcript_section: str,
) -> List[Dict[str, Any]]:
    """Filter categories for one report/transcript section pair."""
    allowed = []
    for category in categories:
        if str(category.get("report_section", "")).strip() != report_section:
            continue
        if report_section == "Outlook":
            allowed.append(category)
            continue
        transcript_sections = str(category.get("transcript_sections", "")).strip()
        if transcript_sections not in {transcript_section, "ALL"}:
            continue
        allowed.append(category)
    return allowed


def _format_categories_for_prompt(categories: Sequence[Dict[str, Any]]) -> str:
    """Render category config rows into prompt-friendly XML."""
    lines = ["<categories>"]
    for category in categories:
        lines.append("  <category>")
        lines.append(f"    <index>{int(category.get('category_index', -1))}</index>")
        lines.append(
            f"    <name>{_escape_for_prompt(_clean_text(category.get('category_name', '')))}</name>"
        )
        lines.append(
            f"    <group>{_escape_for_prompt(_clean_text(category.get('category_group', '')))}</group>"
        )
        lines.append(
            "    <transcript_sections>"
            f"{_escape_for_prompt(_clean_text(category.get('transcript_sections', '')))}"
            "</transcript_sections>"
        )
        lines.append(
            "    <description>"
            f"{_escape_for_prompt(_clean_text(category.get('category_description', '')))}"
            "</description>"
        )
        examples = [
            _clean_text(category.get("example_1", "")),
            _clean_text(category.get("example_2", "")),
            _clean_text(category.get("example_3", "")),
        ]
        example_lines = [example for example in examples if example]
        if example_lines:
            lines.append("    <examples>")
            for example in example_lines:
                lines.append(f"      <example>{_escape_for_prompt(example)}</example>")
            lines.append("    </examples>")
        lines.append("  </category>")
    lines.append("</categories>")
    return "\n".join(lines)


def _make_sentence_record(
    *,
    sid: str,
    text: str,
    para_idx: int,
    transcript_section: str,
    source_block_id: str,
    parent_record_id: str,
    speaker: str = "",
    speaker_title: str = "",
    speaker_affiliation: str = "",
) -> Dict[str, Any]:
    """Create one transcript sentence record in the shell's expected shape."""
    cleaned = _clean_text(text)
    return {
        "sid": sid,
        "text": cleaned,
        "verbatim_text": cleaned,
        "condensed": cleaned,
        "para_idx": para_idx,
        "status": "context",
        "importance_score": 0.0,
        "primary": "",
        "selected_bucket_id": "",
        "candidate_bucket_ids": [],
        "scores": {},
        "transcript_section": transcript_section,
        "source_block_id": source_block_id,
        "parent_record_id": parent_record_id,
        "speaker": speaker,
        "speaker_title": speaker_title,
        "speaker_affiliation": speaker_affiliation,
    }


def _status_for_score(
    score: float,
    selected_threshold: float,
    candidate_threshold: float,
) -> str:
    """Map a score onto the editor's review-status buckets."""
    if score >= selected_threshold:
        return "selected"
    if score >= candidate_threshold:
        return "candidate"
    return "rejected"


def _merge_sentence_assignment(
    sentence: Dict[str, Any],
    *,
    bucket_id: str,
    bucket_score: float,
    importance_score: float,
    status: str,
) -> None:
    """Merge one category assignment onto a sentence record."""
    scores = sentence.setdefault("scores", {})
    scores[bucket_id] = max(float(scores.get(bucket_id, 0.0)), float(bucket_score))
    sentence["importance_score"] = max(
        float(sentence.get("importance_score", 0.0)),
        float(importance_score),
    )

    current_primary = sentence.get("selected_bucket_id") or sentence.get("primary") or ""
    current_primary_score = float(scores.get(current_primary, 0.0)) if current_primary else -1.0
    if not current_primary or float(bucket_score) >= current_primary_score:
        sentence["primary"] = bucket_id
        sentence["selected_bucket_id"] = bucket_id

    candidate_ids = set(sentence.get("candidate_bucket_ids") or [])
    candidate_ids.add(bucket_id)
    sentence["candidate_bucket_ids"] = sorted(candidate_ids)

    current_status = sentence.get("status", "context")
    if STATUS_PRIORITY.get(status, 0) > STATUS_PRIORITY.get(current_status, 0):
        sentence["status"] = status


def _find_matching_sentence_indices(
    sentences: Sequence[Dict[str, Any]],
    excerpt: str,
    *,
    max_span: int = 4,
) -> List[int]:
    """Find the best contiguous sentence span for an extracted verbatim quote."""
    target = _normalize_match_text(excerpt)
    if not target:
        return []

    sentence_norms = [_normalize_match_text(sentence.get("text", "")) for sentence in sentences]
    for idx, sentence_norm in enumerate(sentence_norms):
        if sentence_norm and (
            target == sentence_norm or target in sentence_norm or sentence_norm in target
        ):
            return [idx]

    best: List[int] = []
    best_delta: Optional[int] = None
    for start in range(len(sentence_norms)):
        combined = ""
        for end in range(start, min(len(sentence_norms), start + max_span)):
            combined = (combined + " " + sentence_norms[end]).strip()
            if not combined:
                continue
            if target in combined or combined in target:
                delta = abs(len(combined) - len(target))
                if best_delta is None or delta < best_delta:
                    best = list(range(start, end + 1))
                    best_delta = delta
    return best


def _apply_verbatim_assignment(
    *,
    sentences: List[Dict[str, Any]],
    excerpt: str,
    bucket_id: str,
    bucket_score: float,
    importance_score: float,
    status: str,
) -> bool:
    """Assign one extracted verbatim quote onto one or more sentence records."""
    indices = _find_matching_sentence_indices(sentences, excerpt)
    if not indices:
        return False
    for idx in indices:
        _merge_sentence_assignment(
            sentences[idx],
            bucket_id=bucket_id,
            bucket_score=bucket_score,
            importance_score=importance_score,
            status=status,
        )
    return True


def _apply_sentence_id_assignment(
    *,
    sentence_lookup: Dict[str, Dict[str, Any]],
    source_sentence_ids: Sequence[str],
    source_block_id: str,
    bucket_id: str,
    bucket_score: float,
    importance_score: float,
    status: str,
) -> bool:
    """Assign a finding directly onto transcript sentences by sentence id."""
    matched = False
    for sentence_id in source_sentence_ids:
        sentence = sentence_lookup.get(sentence_id)
        if not sentence:
            continue
        if source_block_id and sentence.get("source_block_id") != source_block_id:
            continue
        _merge_sentence_assignment(
            sentence,
            bucket_id=bucket_id,
            bucket_score=bucket_score,
            importance_score=importance_score,
            status=status,
        )
        matched = True
    return matched


def _speaker_role(block: Dict[str, Any]) -> str:
    """Best-effort analyst vs executive role detection for QA turns."""
    hint = _clean_text(block.get("speaker_type_hint", "")).lower()
    if hint in {"q", "question", "analyst"}:
        return "q"
    if hint in {"a", "answer", "management", "executive", "company"}:
        return "a"

    participant_type = _clean_text(block.get("participant_type", "")).lower()
    if (
        any(marker in participant_type for marker in ("analyst", "question"))
        or participant_type == "q"
    ):
        return "q"
    if (
        any(
            marker in participant_type
            for marker in ("company", "corporate", "management", "executive", "answer")
        )
        or participant_type == "a"
    ):
        return "a"

    speaker_title = _clean_text(block.get("speaker_title", "")).lower()
    speaker_affiliation = _clean_text(block.get("speaker_affiliation", "")).lower()
    speaker_name = _clean_text(block.get("speaker", "")).lower()
    company_name = _clean_text(
        block.get("bank_name") or block.get("company_name") or block.get("issuer_name") or ""
    )
    if speaker_name.startswith("operator"):
        return "a"
    if company_name:
        normalized_company = _normalize_match_text(company_name)
        normalized_affiliation = _normalize_match_text(speaker_affiliation)
        if (
            normalized_company
            and normalized_affiliation
            and (
                normalized_company in normalized_affiliation
                or normalized_affiliation in normalized_company
            )
        ):
            return "a"

    executive_markers = (
        "chief ",
        "ceo",
        "cfo",
        "coo",
        "cro",
        "president",
        "group head",
        "head of",
        "treasurer",
        "controller",
    )
    if any(marker in speaker_title for marker in executive_markers):
        return "a"

    analyst_markers = ("analyst", "research analyst", "equity research")
    if any(marker in speaker_title for marker in analyst_markers):
        return "q"
    analyst_affiliation_markers = ("securities", "research", "capital markets", "equity markets")
    if any(marker in speaker_affiliation for marker in analyst_affiliation_markers):
        return "q"
    if "analyst" in speaker_affiliation:
        return "q"
    return "a"


def _group_qa_conversations_fallback(
    qa_raw_blocks: Sequence[Dict[str, Any]],
) -> List[List[Dict[str, Any]]]:
    """Fallback heuristic grouping when QA boundary detection fails."""
    conversations: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    saw_answer = False

    for block in qa_raw_blocks:
        role = _speaker_role(block)
        if role == "q" and current and saw_answer:
            conversations.append(current)
            current = [block]
            saw_answer = False
            continue

        current.append(block)
        if role == "a":
            saw_answer = True

    if current:
        conversations.append(current)
    return conversations


def _build_qa_block_index(
    qa_raw_blocks: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Build a 1-based index of QA speaker blocks for boundary detection."""
    return [
        {
            "block_index": block_index,
            "block_id": block["id"],
            "speaker": block.get("speaker", "Unknown Speaker"),
            "speaker_title": block.get("speaker_title", ""),
            "speaker_affiliation": block.get("speaker_affiliation", ""),
            "participant_type": block.get("participant_type", ""),
            "speaker_type_hint": block.get("speaker_type_hint", ""),
            "paragraphs": block.get("paragraphs", []),
        }
        for block_index, block in enumerate(qa_raw_blocks, start=1)
    ]


def _preview_text(text: str, limit: int = 1200) -> str:
    """Return a large preview, which is effectively full text for most paragraphs."""
    clean = (text or "").strip()
    if len(clean) <= limit:
        return clean
    return clean[:limit].rstrip() + "..."


def _format_block_preview(paragraphs: List[str], max_paragraph_chars: int = 900) -> str:
    """Format a large preview for a QA speaker block without numeric labels."""
    preview_parts = []
    for paragraph in paragraphs:
        preview_parts.append(
            "<paragraph>"
            f"{_escape_for_prompt(_preview_text(paragraph, max_paragraph_chars))}"
            "</paragraph>"
        )
    return "\n".join(preview_parts)


def _format_qa_block_prompt_entry(entry: Dict[str, Any]) -> str:
    """Format one QA speaker block as XML for boundary detection."""
    hint = (entry.get("speaker_type_hint") or "?").upper()
    return (
        "<qa_block>\n"
        f"  <index>{entry['block_index']}</index>\n"
        f"  <speaker_type_hint>{_escape_for_prompt(hint)}</speaker_type_hint>\n"
        f"  <speaker>{_escape_for_prompt(entry.get('speaker', 'Unknown Speaker'))}</speaker>\n"
        f"  <speaker_title>{_escape_for_prompt(entry.get('speaker_title', ''))}</speaker_title>\n"
        "  <speaker_affiliation>"
        f"{_escape_for_prompt(entry.get('speaker_affiliation', ''))}"
        "</speaker_affiliation>\n"
        f"  <participant_type>{_escape_for_prompt(entry.get('participant_type', ''))}</participant_type>\n"
        "  <preview>\n"
        f"{entry.get('preview', '')}\n"
        "  </preview>\n"
        "</qa_block>"
    )


def _qa_boundary_retry_message(total_blocks: int, errors: List[str]) -> str:
    """Build corrective retry guidance for QA boundary re-attempts."""
    error_lines = "\n".join(f"- {error}" for error in errors) if errors else "- Unknown error"
    return (
        "## Retry Correction\n"
        "The previous grouping was invalid. Correct it and try again.\n\n"
        "## Hard Constraints\n"
        f"1. The only valid block indices are integers 1 through {total_blocks} inclusive.\n"
        "2. Only the integers inside `<index>` tags are valid block indices.\n"
        "3. Ignore any numbers inside speaker names, titles, affiliations, or preview text.\n"
        "4. Every valid block index must appear exactly once across all conversations.\n"
        "5. Each conversation must remain a contiguous run of block indices.\n\n"
        "## Validation Errors To Fix\n"
        f"{error_lines}\n\n"
        "Return a corrected grouping with the provided tool."
    )


def _validated_tool_retry_message(stage_name: str, errors: List[str]) -> str:
    """Build corrective retry guidance for extraction tool validation failures."""
    error_lines = "\n".join(f"- {error}" for error in errors) if errors else "- Unknown error"
    return (
        "## Retry Correction\n"
        f"The previous {stage_name} tool response was invalid. Correct it and try again.\n\n"
        "## Hard Constraints\n"
        "1. Use the provided tool and return arguments that match the tool schema exactly.\n"
        "2. Use only category indices, block ids, and sentence ids from the prompt.\n"
        "3. Do not invent ids, categories, or additional response fields.\n"
        "4. If no qualifying content exists, return `has_content: false` with an empty array.\n\n"
        "## Validation Errors To Fix\n"
        f"{error_lines}\n\n"
        "Return a corrected tool call."
    )


async def _call_validated_tool(
    *,
    messages: List[Dict[str, str]],
    tool: Dict[str, Any],
    label: str,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
    response_model: Any,
    stage_name: str,
    max_attempts: int = 3,
) -> Any:
    """Run a structured LLM tool call with bounded schema-validation retries."""
    base_messages = list(messages)
    retry_messages = list(base_messages)
    last_validation_errors: List[str] = []

    for attempt in range(max_attempts):
        raw = await _call_tool(
            messages=retry_messages,
            tool=tool,
            label=label,
            context=context,
            llm_params=llm_params,
            warn_on_missing=False,
        )
        if not raw:
            last_validation_errors = ["No parseable tool response returned"]
        else:
            try:
                return response_model.model_validate(raw)
            except ValidationError as exc:
                last_validation_errors = [f"Tool output schema validation failed: {exc}"]
                logger.warning(
                    "LLM extraction response could not be validated",
                    stage=label,
                    attempt=attempt + 1,
                    max_attempts=max_attempts,
                    error=str(exc),
                )

        if attempt < max_attempts - 1:
            retry_messages = base_messages + [
                {
                    "role": "user",
                    "content": _validated_tool_retry_message(stage_name, last_validation_errors),
                }
            ]

    raise RuntimeError(
        f"{stage_name} failed validation after {max_attempts} attempts: "
        + "; ".join(last_validation_errors or ["No parseable tool response returned"])
    )


def _resolve_block_indices(
    conversation: QAConversationGroup,
) -> List[int]:
    """Resolve tool output into 1-based block indices."""
    return list(conversation.block_indices)


def _validate_qa_boundary_indices(
    conversation_indices: List[List[int]],
    total_blocks: int,
) -> List[str]:
    """Validate that grouped QA block indices cover the transcript exactly once in order."""
    errors: List[str] = []
    if not conversation_indices:
        return ["No conversations returned"]

    flat_indices = [idx for indices in conversation_indices for idx in indices]
    if not flat_indices:
        return ["No block indices returned"]

    if any(not indices for indices in conversation_indices):
        errors.append("One or more conversations are empty")

    expected = list(range(1, total_blocks + 1))
    if flat_indices != expected:
        seen = set()
        duplicates = []
        for idx in flat_indices:
            if idx in seen:
                duplicates.append(idx)
            seen.add(idx)

        missing = [idx for idx in expected if idx not in seen]
        out_of_range = [idx for idx in flat_indices if idx < 1 or idx > total_blocks]

        if missing:
            errors.append(f"Missing block indices: {missing}")
        if duplicates:
            errors.append(f"Duplicate block indices: {duplicates}")
        if out_of_range:
            errors.append(f"Out-of-range block indices: {out_of_range}")
        if not missing and not duplicates and not out_of_range:
            errors.append(
                f"Block order must remain sequential. Got {flat_indices}, expected {expected}"
            )

    return errors


def _materialize_qa_conversations(
    conversation_indices: List[List[int]],
    block_by_index: Dict[int, Dict[str, Any]],
) -> List[List[Dict[str, Any]]]:
    """Map validated or last-attempt block indices back to QA block records."""
    conversations = []
    for indices in conversation_indices:
        blocks = [block_by_index[idx] for idx in indices if idx in block_by_index]
        if blocks:
            conversations.append(blocks)
    return conversations


async def detect_qa_boundaries(
    *,
    qa_raw_blocks: List[Dict[str, Any]],
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> List[List[Dict[str, Any]]]:
    """Group QA speaker blocks into analyst-to-management conversations."""
    if not qa_raw_blocks:
        return []

    logger.info("Finding Q&A conversation boundaries", qa_speaker_blocks=len(qa_raw_blocks))

    block_entries = _build_qa_block_index(qa_raw_blocks)
    block_lines = []
    for entry in block_entries:
        prompt_entry = dict(entry)
        prompt_entry["preview"] = _format_block_preview(entry.get("paragraphs", []))
        block_lines.append(_format_qa_block_prompt_entry(prompt_entry))
    indexed_blocks_xml = "\n\n".join(block_lines)

    system_prompt = (
        "You are a transcript-structure analyst for earnings call Q&A sections. "
        "Group indexed speaker blocks into complete analyst-to-executive conversations using the "
        "speaker metadata and block content. Speaker type hints can be imperfect in raw XML. "
        "Always use the provided tool."
    )
    user_prompt = (
        "## Task\n"
        "Group the indexed Q&A speaker blocks into ordered conversation exchanges.\n\n"
        "## Decision Criteria\n"
        "Each conversation starts with an analyst question block and includes the executive "
        "response blocks that follow until the next analyst question block.\n\n"
        "## Rules\n"
        f"1. The only valid block indices are integers 1 through {len(qa_raw_blocks)} inclusive.\n"
        "2. Preserve transcript order and cover the indexed blocks exactly once.\n"
        "3. Use the block content and speaker metadata when a type hint is ambiguous.\n"
        "4. Keep each conversation as a contiguous run of block indices.\n"
        "5. Only the integers inside `<index>` tags are valid block indices.\n"
        "6. Ignore any numbers that appear inside speaker names, titles, affiliations, "
        "or preview text.\n"
        "7. Every conversation object must include a non-empty `block_indices` array.\n"
        "8. Do not return empty conversations.\n"
        "9. Return the grouped indices with the provided tool.\n\n"
        "## Indexed Blocks\n"
        f"{_xml_block('qa_block_index', indexed_blocks_xml)}"
    )
    block_by_index = dict(enumerate(qa_raw_blocks, start=1))
    base_messages = [
        {"role": "developer", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    messages = list(base_messages)
    max_attempts = 3
    last_validation_errors: List[str] = []

    for attempt in range(max_attempts):
        raw = await _call_tool(
            messages=messages,
            tool=TOOL_QA_BOUNDARY,
            label="qa_boundary",
            context=context,
            llm_params=llm_params,
        )
        if not raw:
            last_validation_errors = ["No parseable tool response returned"]
            if attempt < max_attempts - 1:
                messages = base_messages + [
                    {
                        "role": "user",
                        "content": _qa_boundary_retry_message(
                            len(qa_raw_blocks),
                            last_validation_errors,
                        ),
                    }
                ]
                continue
            break

        try:
            result = QABoundaryResult.model_validate(raw)
        except Exception as exc:  # pylint: disable=broad-except
            last_validation_errors = [f"Tool output schema validation failed: {exc}"]
            logger.warning(
                "Q&A boundary response could not be validated",
                error=str(exc),
                attempt=attempt + 1,
                max_attempts=max_attempts,
            )
            if attempt < max_attempts - 1:
                messages = base_messages + [
                    {
                        "role": "user",
                        "content": _qa_boundary_retry_message(
                            len(qa_raw_blocks),
                            last_validation_errors,
                        ),
                    }
                ]
                continue
            break

        conversation_indices = [
            _resolve_block_indices(conversation) for conversation in result.conversations
        ]
        validation_errors = _validate_qa_boundary_indices(
            conversation_indices,
            len(qa_raw_blocks),
        )
        if not validation_errors:
            logger.info(
                "Q&A conversation boundaries resolved",
                conversations=len(conversation_indices),
                qa_speaker_blocks=len(qa_raw_blocks),
            )
            return _materialize_qa_conversations(conversation_indices, block_by_index)

        logger.warning(
            "Q&A boundary response failed validation",
            attempt=attempt + 1,
            max_attempts=max_attempts,
            issues=_summarise_validation_errors(validation_errors),
        )
        last_validation_errors = validation_errors
        if attempt < max_attempts - 1:
            messages = base_messages + [
                {
                    "role": "user",
                    "content": _qa_boundary_retry_message(
                        len(qa_raw_blocks),
                        validation_errors,
                    ),
                }
            ]

    raise RuntimeError(
        "Q&A boundary detection failed validation after 3 attempts: "
        + "; ".join(last_validation_errors or ["No parseable tool response returned"])
    )


def _build_md_block(
    block: Dict[str, Any],
    *,
    fiscal_year: int,
    fiscal_quarter: str,
) -> Dict[str, Any]:
    """Convert one raw MD speaker block into transcript sentences."""
    sentences: List[Dict[str, Any]] = []
    sentence_index = 0
    for para_idx, paragraph in enumerate(block.get("paragraphs", [])):
        for sentence in split_sentences(paragraph):
            sentences.append(
                _make_sentence_record(
                    sid=f"{block['id']}_s{sentence_index}",
                    text=sentence,
                    para_idx=para_idx,
                    transcript_section="MD",
                    source_block_id=block["id"],
                    parent_record_id=block["id"],
                    speaker=block.get("speaker", ""),
                    speaker_title=block.get("speaker_title", ""),
                    speaker_affiliation=block.get("speaker_affiliation", ""),
                )
            )
            sentence_index += 1

    return {
        "id": block["id"],
        "speaker": block.get("speaker", ""),
        "speaker_title": block.get("speaker_title", ""),
        "speaker_affiliation": block.get("speaker_affiliation", ""),
        "sentences": sentences,
        "has_findings": False,
        "fiscal_year": fiscal_year,
        "fiscal_quarter": fiscal_quarter,
    }


def _build_qa_conversation(
    conv_blocks: Sequence[Dict[str, Any]],
    *,
    ticker: str,
    conv_idx: int,
) -> Dict[str, Any]:
    """Convert one grouped QA conversation into transcript turns."""
    conv_id = f"{ticker}_QA_{conv_idx}"
    turns: List[Dict[str, Any]] = []
    question_sentences: List[Dict[str, Any]] = []
    answer_sentences: List[Dict[str, Any]] = []
    q_idx = 0
    a_idx = 0

    explicit_question = any(_speaker_role(block) == "q" for block in conv_blocks)
    for position, block in enumerate(conv_blocks):
        role = _speaker_role(block)
        if not explicit_question and position == 0:
            role = "q"

        turn_sentences: List[Dict[str, Any]] = []
        for para_idx, paragraph in enumerate(block.get("paragraphs", [])):
            for sentence in split_sentences(paragraph):
                if role == "q":
                    sid = f"{conv_id}_qs{q_idx}"
                    q_idx += 1
                else:
                    sid = f"{conv_id}_as{a_idx}"
                    a_idx += 1
                turn_sentences.append(
                    _make_sentence_record(
                        sid=sid,
                        text=sentence,
                        para_idx=para_idx,
                        transcript_section="QA",
                        source_block_id=block["id"],
                        parent_record_id=conv_id,
                        speaker=block.get("speaker", ""),
                        speaker_title=block.get("speaker_title", ""),
                        speaker_affiliation=block.get("speaker_affiliation", ""),
                    )
                )

        turn = {
            "id": block["id"],
            "role": role,
            "speaker": block.get("speaker", ""),
            "speaker_title": block.get("speaker_title", ""),
            "speaker_affiliation": block.get("speaker_affiliation", ""),
            "block_id": block["id"],
            "sentences": turn_sentences,
        }
        turns.append(turn)
        if role == "q":
            question_sentences.extend(turn_sentences)
        else:
            answer_sentences.extend(turn_sentences)

    first_question_turn = next((turn for turn in turns if turn["role"] == "q"), None)
    first_answer_turn = next((turn for turn in turns if turn["role"] == "a"), None)
    analyst_question_summary = " ".join(
        sentence.get("text", "") for sentence in question_sentences
    ).strip()
    return {
        "id": conv_id,
        "render_mode": "question",
        "primary_bucket": "",
        "analyst_name": (first_question_turn or {}).get("speaker", "Analyst"),
        "analyst_affiliation": (first_question_turn or {}).get("speaker_affiliation", ""),
        "executive_name": (first_answer_turn or {}).get("speaker", "Executive"),
        "executive_title": (first_answer_turn or {}).get("speaker_title", ""),
        "executive_affiliation": (first_answer_turn or {}).get("speaker_affiliation", ""),
        "analyst_question_summary": analyst_question_summary,
        "turns": turns,
        "question_sentences": question_sentences,
        "answer_sentences": answer_sentences,
        "has_findings": False,
        "source_block_ids": [block["id"] for block in conv_blocks],
    }


def _render_sentence_prompt_entry(sentence: Dict[str, Any]) -> str:
    """Format one indexed sentence for prompt use."""
    return (
        f'    <sentence id="{_escape_for_prompt(sentence.get("sid", ""))}">'
        f"{_escape_for_prompt(sentence.get('text', ''))}"
        "</sentence>"
    )


def _render_md_block_prompt(block: Dict[str, Any]) -> str:
    """Format one indexed MD block for the Outlook prompt."""
    sentence_lines = [
        _render_sentence_prompt_entry(sentence) for sentence in block.get("sentences", [])
    ]
    return (
        f'<md_block id="{_escape_for_prompt(block.get("id", ""))}">\n'
        f"  <speaker>{_escape_for_prompt(block.get('speaker', 'Unknown Speaker'))}</speaker>\n"
        f"  <speaker_title>{_escape_for_prompt(block.get('speaker_title', ''))}</speaker_title>\n"
        f"  <speaker_affiliation>{_escape_for_prompt(block.get('speaker_affiliation', ''))}</speaker_affiliation>\n"
        "  <sentences>\n"
        f"{chr(10).join(sentence_lines)}\n"
        "  </sentences>\n"
        "</md_block>"
    )


def _render_qa_turn_prompt(turn: Dict[str, Any]) -> str:
    """Format one indexed QA turn for prompt use."""
    sentence_lines = [
        _render_sentence_prompt_entry(sentence) for sentence in turn.get("sentences", [])
    ]
    return (
        f'  <turn role="{_escape_for_prompt(turn.get("role", ""))}" '
        f'block_id="{_escape_for_prompt(turn.get("block_id", turn.get("id", "")))}">\n'
        f"    <speaker>{_escape_for_prompt(turn.get('speaker', 'Speaker'))}</speaker>\n"
        f"    <speaker_title>{_escape_for_prompt(turn.get('speaker_title', ''))}</speaker_title>\n"
        f"    <speaker_affiliation>{_escape_for_prompt(turn.get('speaker_affiliation', ''))}</speaker_affiliation>\n"
        "    <sentences>\n"
        f"{chr(10).join(sentence_lines)}\n"
        "    </sentences>\n"
        "  </turn>"
    )


def _render_qa_conversation_prompt(conversation: Dict[str, Any]) -> str:
    """Format one indexed QA conversation for prompt use."""
    turn_lines = [_render_qa_turn_prompt(turn) for turn in conversation.get("turns", [])]
    return (
        f'<conversation id="{_escape_for_prompt(conversation.get("id", ""))}">\n'
        f"{chr(10).join(turn_lines)}\n"
        "</conversation>"
    )


def _render_outlook_transcript_prompt(
    md_blocks: Sequence[Dict[str, Any]],
    qa_conversations: Sequence[Dict[str, Any]],
) -> str:
    """Format the full bank transcript for bank-level Outlook extraction."""
    md_xml = "\n\n".join(_render_md_block_prompt(block) for block in md_blocks)
    qa_xml = "\n\n".join(_render_qa_conversation_prompt(conv) for conv in qa_conversations)
    parts = []
    if md_xml:
        parts.append(_xml_block("management_discussion", md_xml))
    if qa_xml:
        parts.append(_xml_block("qa_conversations", qa_xml))
    return "\n\n".join(parts)


def _render_qa_bank_prompt(qa_conversations: Sequence[Dict[str, Any]]) -> str:
    """Format all QA conversations for bank-level Q&A extraction."""
    return _xml_block(
        "qa_conversations",
        "\n\n".join(_render_qa_conversation_prompt(conv) for conv in qa_conversations),
    )


def _resolve_source_block_id(value: str, allowed_block_ids: Sequence[str]) -> str:
    """Resolve a model-returned block id against the allowed source ids."""
    allowed = {str(block_id): str(block_id) for block_id in allowed_block_ids if block_id}
    raw = _clean_text(value).strip("[]")
    if raw in allowed:
        return allowed[raw]
    lowered = raw.lower()
    for block_id in allowed_block_ids:
        candidate = str(block_id)
        if candidate.lower() == lowered:
            return candidate
    return ""


def _resolve_source_sentence_ids(
    values: Sequence[str],
    allowed_sentence_ids: Sequence[str],
) -> List[str]:
    """Resolve model-returned sentence ids against the allowed sentence ids."""
    allowed = {
        str(sentence_id): str(sentence_id) for sentence_id in allowed_sentence_ids if sentence_id
    }
    resolved: List[str] = []
    seen = set()
    for value in values or []:
        raw = _clean_text(value).strip("[]")
        if raw in allowed and raw not in seen:
            resolved.append(allowed[raw])
            seen.add(raw)
            continue
        lowered = raw.lower()
        for sentence_id in allowed_sentence_ids:
            candidate = str(sentence_id)
            if candidate.lower() == lowered and candidate not in seen:
                resolved.append(candidate)
                seen.add(candidate)
                break
    return resolved


async def _extract_outlook_for_bank(
    *,
    md_blocks: Sequence[Dict[str, Any]],
    qa_conversations: Sequence[Dict[str, Any]],
    bank_info: Dict[str, Any],
    categories: Sequence[Dict[str, Any]],
    fiscal_year: int,
    fiscal_quarter: str,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> List[OutlookStatement]:
    """Extract CM Outlook findings from the full bank transcript."""
    eligible_sentences = [
        sentence for block in md_blocks for sentence in block.get("sentences", [])
    ] + [
        sentence
        for conversation in qa_conversations
        for sentence in conversation.get("answer_sentences", [])
    ]
    if not eligible_sentences:
        return []

    prompt = _load_prompt("outlook_extraction")
    categories_text = _format_categories_for_prompt(categories)
    sentence_to_block = {
        sentence.get("sid", ""): sentence.get("source_block_id", "")
        for sentence in eligible_sentences
        if sentence.get("sid")
    }
    allowed_sentence_ids = list(sentence_to_block.keys())
    allowed_block_ids = sorted({block_id for block_id in sentence_to_block.values() if block_id})
    messages = [
        {
            "role": "developer",
            "content": prompt["system_prompt"].format(categories_list=categories_text),
        },
        {
            "role": "user",
            "content": prompt["user_prompt"].format(
                bank_name=bank_info["bank_name"],
                fiscal_year=fiscal_year,
                quarter=fiscal_quarter,
                categories_list=categories_text,
                transcript_content=_render_outlook_transcript_prompt(md_blocks, qa_conversations),
            ),
        },
    ]
    result = await _call_validated_tool(
        messages=messages,
        tool=prompt["tool_definition"],
        label=f"outlook_bank:{bank_info['bank_symbol']}",
        context=context,
        llm_params=llm_params,
        response_model=OutlookExtractionResponse,
        stage_name=f"Outlook extraction for {bank_info['bank_symbol']}",
    )

    if not result.has_content:
        return []

    resolved: List[OutlookStatement] = []
    for statement in result.statements:
        if statement.is_new_category:
            continue
        statement.source_sentence_ids = _resolve_source_sentence_ids(
            statement.source_sentence_ids,
            allowed_sentence_ids,
        )
        derived_block_id = (
            sentence_to_block.get(statement.source_sentence_ids[0], "")
            if statement.source_sentence_ids
            else ""
        )
        statement.source_block_id = derived_block_id or _resolve_source_block_id(
            statement.source_block_id, allowed_block_ids
        )
        resolved.append(statement)
    return resolved


async def _extract_questions_for_bank(
    *,
    qa_conversations: Sequence[Dict[str, Any]],
    bank_info: Dict[str, Any],
    categories: Sequence[Dict[str, Any]],
    fiscal_year: int,
    fiscal_quarter: str,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> List[QAQuestion]:
    """Extract CM analyst questions from the full bank Q&A section."""
    eligible_sentences = [
        sentence
        for conversation in qa_conversations
        for sentence in conversation.get("question_sentences", [])
    ]
    if not eligible_sentences:
        return []

    prompt = _load_prompt("qa_extraction")
    categories_text = _format_categories_for_prompt(categories)
    sentence_to_block = {
        sentence.get("sid", ""): sentence.get("source_block_id", "")
        for sentence in eligible_sentences
        if sentence.get("sid")
    }
    allowed_sentence_ids = list(sentence_to_block.keys())
    allowed_block_ids = sorted({block_id for block_id in sentence_to_block.values() if block_id})
    messages = [
        {
            "role": "developer",
            "content": prompt["system_prompt"].format(categories_list=categories_text),
        },
        {
            "role": "user",
            "content": prompt["user_prompt"].format(
                bank_name=bank_info["bank_name"],
                fiscal_year=fiscal_year,
                quarter=fiscal_quarter,
                categories_list=categories_text,
                qa_content=_render_qa_bank_prompt(qa_conversations),
            ),
        },
    ]
    result = await _call_validated_tool(
        messages=messages,
        tool=prompt["tool_definition"],
        label=f"qa_bank:{bank_info['bank_symbol']}",
        context=context,
        llm_params=llm_params,
        response_model=QAExtractionResponse,
        stage_name=f"Q&A extraction for {bank_info['bank_symbol']}",
    )

    if not result.has_content:
        return []

    resolved: List[QAQuestion] = []
    for question in result.questions:
        if question.is_new_category:
            continue
        question.source_sentence_ids = _resolve_source_sentence_ids(
            question.source_sentence_ids,
            allowed_sentence_ids,
        )
        derived_block_id = (
            sentence_to_block.get(question.source_sentence_ids[0], "")
            if question.source_sentence_ids
            else ""
        )
        question.source_block_id = derived_block_id or _resolve_source_block_id(
            question.source_block_id, allowed_block_ids
        )
        resolved.append(question)
    return resolved


def _mark_outlook_findings_on_sentences(
    *,
    sentences: Sequence[Dict[str, Any]],
    statements: Sequence[OutlookStatement],
    categories: Sequence[Dict[str, Any]],
    selected_importance_threshold: float,
    candidate_importance_threshold: float,
) -> None:
    """Map extracted Outlook findings back onto sentence records."""
    sentence_lookup = {
        sentence.get("sid", ""): sentence for sentence in sentences if sentence.get("sid")
    }
    for statement in statements:
        bucket_id = _bucket_id_for_category_index(
            statement.category_index,
            categories,
            report_section="Outlook",
        )
        if not bucket_id:
            continue
        score = float(statement.relevance_score)
        status = _status_for_score(
            score, selected_importance_threshold, candidate_importance_threshold
        )
        matched = _apply_sentence_id_assignment(
            sentence_lookup=sentence_lookup,
            source_sentence_ids=statement.source_sentence_ids,
            source_block_id=statement.source_block_id,
            bucket_id=bucket_id,
            bucket_score=score,
            importance_score=score,
            status=status,
        )
        if not matched:
            logger.info(
                "Outlook finding could not be aligned to transcript sentence ids",
                source_block_id=statement.source_block_id,
                category_index=statement.category_index,
                sentence_ids=statement.source_sentence_ids,
            )


def _mark_md_outlook_findings(
    block: Dict[str, Any],
    statements: Sequence[OutlookStatement],
    categories: Sequence[Dict[str, Any]],
    *,
    selected_importance_threshold: float,
    candidate_importance_threshold: float,
) -> None:
    """Map extracted Outlook findings back onto MD block sentences."""
    _mark_outlook_findings_on_sentences(
        sentences=block["sentences"],
        statements=statements,
        categories=categories,
        selected_importance_threshold=selected_importance_threshold,
        candidate_importance_threshold=candidate_importance_threshold,
    )
    block["has_findings"] = any(
        sentence.get("status") in {"selected", "candidate"}
        for sentence in block.get("sentences", [])
    )


def _refresh_qa_conversation_has_findings(conversation: Dict[str, Any]) -> None:
    """Set the QA conversation visibility flag from both question and answer findings."""
    conversation["has_findings"] = any(
        sentence.get("status") in {"selected", "candidate"}
        for sentence in conversation.get("question_sentences", [])
        + conversation.get("answer_sentences", [])
    )


def _mark_qa_outlook_findings(
    conversation: Dict[str, Any],
    statements: Sequence[OutlookStatement],
    categories: Sequence[Dict[str, Any]],
    *,
    selected_importance_threshold: float,
    candidate_importance_threshold: float,
) -> None:
    """Map extracted Outlook findings onto management answer sentences in one QA conversation."""
    _mark_outlook_findings_on_sentences(
        sentences=conversation.get("answer_sentences", []),
        statements=statements,
        categories=categories,
        selected_importance_threshold=selected_importance_threshold,
        candidate_importance_threshold=candidate_importance_threshold,
    )
    _refresh_qa_conversation_has_findings(conversation)


def _mark_qa_question_findings(
    conversation: Dict[str, Any],
    questions: Sequence[QAQuestion],
    categories: Sequence[Dict[str, Any]],
    *,
    selected_importance_threshold: float,
    candidate_importance_threshold: float,
) -> None:
    """Map extracted analyst questions back onto question-turn sentences."""
    primary_bucket = ""
    primary_score = -1.0
    sentence_lookup = {
        sentence.get("sid", ""): sentence
        for sentence in conversation["question_sentences"]
        if sentence.get("sid")
    }
    for question in questions:
        bucket_id = _bucket_id_for_category_index(
            question.category_index,
            categories,
            report_section="Q&A",
        )
        if not bucket_id:
            continue
        score = float(question.relevance_score)
        status = _status_for_score(
            score, selected_importance_threshold, candidate_importance_threshold
        )
        matched = _apply_sentence_id_assignment(
            sentence_lookup=sentence_lookup,
            source_sentence_ids=question.source_sentence_ids,
            source_block_id=question.source_block_id,
            bucket_id=bucket_id,
            bucket_score=score,
            importance_score=score,
            status=status,
        )
        if matched and status in {"selected", "candidate"} and score >= primary_score:
            primary_bucket = bucket_id
            primary_score = score
        if not matched:
            logger.info(
                "Q&A finding could not be aligned to transcript sentence ids",
                conversation_id=conversation["id"],
                source_block_id=question.source_block_id,
                category_index=question.category_index,
                sentence_ids=question.source_sentence_ids,
            )

    conversation["primary_bucket"] = primary_bucket
    _refresh_qa_conversation_has_findings(conversation)


async def build_interactive_bank_data(
    *,
    md_raw_blocks: List[Dict[str, Any]],
    qa_raw_blocks: List[Dict[str, Any]],
    categories: List[Dict[str, Any]],
    bank_info: Dict[str, Any],
    fiscal_year: int,
    fiscal_quarter: str,
    transcript_title: str,
    context: Dict[str, Any],
    qa_boundary_llm_params: Dict[str, Any],
    md_llm_params: Dict[str, Any],
    qa_llm_params: Dict[str, Any],
    selected_importance_threshold: float,
    candidate_importance_threshold: float,
) -> Dict[str, Any]:
    """Build one bank payload in the call-summary-shell shape.

    Args:
        md_raw_blocks: Parsed MD speaker blocks from NAS XML.
        qa_raw_blocks: Parsed QA speaker blocks from NAS XML.
        categories: Flat CM category config rows.
        bank_info: Monitored-institutions metadata for the current bank.
        fiscal_year: Transcript fiscal year.
        fiscal_quarter: Transcript fiscal quarter.
        transcript_title: Parsed transcript title.
        context: Shared ETL runtime context.
        qa_boundary_llm_params: LLM params for QA boundary detection.
        md_llm_params: LLM params for bank-level Outlook extraction.
        qa_llm_params: LLM params for bank-level Q&A extraction.
        selected_importance_threshold: Selected-status threshold.
        candidate_importance_threshold: Candidate-status threshold.

    Returns:
        One bank payload ready for `interactive_html.build_report_state()`.
    """
    categories = [
        {**category, "category_index": int(category.get("category_index", idx))}
        for idx, category in enumerate(categories)
    ]
    ticker = bank_info.get("full_ticker") or bank_info["bank_symbol"]
    bank_identity = {
        "bank_name": bank_info["bank_name"],
        "bank_symbol": bank_info["bank_symbol"],
        "full_ticker": ticker,
    }
    md_raw_blocks = [{**block, **bank_identity} for block in md_raw_blocks]
    qa_raw_blocks = [{**block, **bank_identity} for block in qa_raw_blocks]
    outlook_categories = _categories_for_section(
        categories,
        report_section="Outlook",
        transcript_section="MD",
    )
    qa_categories = _categories_for_section(
        categories,
        report_section="Q&A",
        transcript_section="QA",
    )

    logger.info(
        "Starting CM transcript processing",
        ticker=ticker,
        md_blocks=len(md_raw_blocks),
        qa_speaker_blocks=len(qa_raw_blocks),
        outlook_categories=len(outlook_categories),
        qa_categories=len(qa_categories),
    )

    processed_md = [
        _build_md_block(block, fiscal_year=fiscal_year, fiscal_quarter=fiscal_quarter)
        for block in md_raw_blocks
    ]
    try:
        raw_qa_conversations = await detect_qa_boundaries(
            qa_raw_blocks=qa_raw_blocks,
            context=context,
            llm_params=qa_boundary_llm_params,
        )
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning(
            "Q&A boundary detection failed; using heuristic fallback",
            ticker=ticker,
            error=str(exc),
        )
        raw_qa_conversations = _group_qa_conversations_fallback(qa_raw_blocks)

    qa_conversations = [
        _build_qa_conversation(conversation, ticker=ticker, conv_idx=index)
        for index, conversation in enumerate(raw_qa_conversations, start=1)
    ]
    try:
        outlook_findings, qa_findings = await asyncio.gather(
            _extract_outlook_for_bank(
                md_blocks=processed_md,
                qa_conversations=qa_conversations,
                bank_info=bank_info,
                categories=outlook_categories,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                context=context,
                llm_params=md_llm_params,
            ),
            _extract_questions_for_bank(
                qa_conversations=qa_conversations,
                bank_info=bank_info,
                categories=qa_categories,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                context=context,
                llm_params=qa_llm_params,
            ),
        )
    except Exception as exc:  # pylint: disable=broad-except
        logger.error(
            "Bank-level CM extraction failed",
            ticker=ticker,
            error=str(exc),
            exc_info=True,
        )
        raise

    for block in processed_md:
        md_block_findings = [
            finding for finding in outlook_findings if finding.source_block_id == block["id"]
        ]
        _mark_md_outlook_findings(
            block,
            md_block_findings,
            categories,
            selected_importance_threshold=selected_importance_threshold,
            candidate_importance_threshold=candidate_importance_threshold,
        )

    for conversation in qa_conversations:
        qa_outlook_findings = [
            finding
            for finding in outlook_findings
            if finding.source_block_id in conversation.get("source_block_ids", [])
        ]
        if qa_outlook_findings:
            _mark_qa_outlook_findings(
                conversation,
                qa_outlook_findings,
                categories,
                selected_importance_threshold=selected_importance_threshold,
                candidate_importance_threshold=candidate_importance_threshold,
            )

        conversation_questions = [
            question
            for question in qa_findings
            if question.source_block_id in conversation.get("source_block_ids", [])
        ]
        if conversation_questions:
            _mark_qa_question_findings(
                conversation,
                conversation_questions,
                categories,
                selected_importance_threshold=selected_importance_threshold,
                candidate_importance_threshold=candidate_importance_threshold,
            )

    selected_md = sum(
        1
        for block in processed_md
        for sentence in block.get("sentences", [])
        if sentence.get("status") == "selected"
    )
    selected_qa_outlook = sum(
        1
        for conversation in qa_conversations
        for sentence in conversation.get("answer_sentences", [])
        if sentence.get("status") == "selected"
    )
    selected_qa = sum(
        1
        for conversation in qa_conversations
        for sentence in conversation.get("question_sentences", [])
        if sentence.get("status") == "selected"
    )
    logger.info(
        "CM transcript processing complete",
        ticker=ticker,
        processed_md_blocks=len(processed_md),
        processed_qa_conversations=len(qa_conversations),
        selected_outlook_md_findings=selected_md,
        selected_outlook_qa_findings=selected_qa_outlook,
        selected_qa_findings=selected_qa,
    )

    return {
        "ticker": ticker,
        "symbol": bank_info["bank_symbol"],
        "type": bank_info["bank_type"],
        "company_name": bank_info["bank_name"],
        "selector_label": ticker,
        "report_group": bank_info["bank_name"],
        "transcript_title": transcript_title or f"{fiscal_quarter} {fiscal_year} Earnings Call",
        "fiscal_year": fiscal_year,
        "fiscal_quarter": fiscal_quarter,
        "md_blocks": processed_md,
        "qa_conversations": qa_conversations,
    }


async def analyze_config_coverage(
    *,
    bank_data: Dict[str, Any],
    categories: List[Dict[str, Any]],
    min_importance: float,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> Dict[str, Any]:
    """Return an empty config-review payload for the CM editor shell.

    The copied call-summary shell expects this key to exist, but CM editor v1
    is intentionally not carrying forward the config-maintenance workflow.
    """
    del bank_data, categories, min_importance, context, llm_params
    return {"config_change_proposals": []}


async def generate_bucket_headlines(
    *,
    banks_data: Dict[str, Dict[str, Any]],
    categories: List[Dict[str, Any]],
    min_importance: float,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
    sample_size: int,
) -> Dict[str, str]:
    """Return no generated headlines for the CM editor shell."""
    del banks_data, categories, min_importance, context, llm_params, sample_size
    return {}


def _collect_outlook_subtitle_content(
    banks_data: Dict[str, Dict[str, Any]],
    *,
    max_banks: int = 10,
    max_items_per_bank: int = 3,
) -> List[Dict[str, Any]]:
    """Collect selected Outlook findings for subtitle generation."""
    content: List[Dict[str, Any]] = []
    for bank_id, bank in list(banks_data.items())[:max_banks]:
        findings: List[str] = []
        for block in bank.get("md_blocks", []):
            selected = [
                sentence.get("text", "")
                for sentence in block.get("sentences", [])
                if sentence.get("status") == "selected"
            ]
            if not selected:
                continue
            findings.append(" ".join(selected).strip())
            if len(findings) >= max_items_per_bank:
                break
        if len(findings) < max_items_per_bank:
            for conversation in bank.get("qa_conversations", []):
                selected = [
                    sentence.get("text", "")
                    for sentence in conversation.get("answer_sentences", [])
                    if sentence.get("status") == "selected"
                ]
                if not selected:
                    continue
                findings.append(" ".join(selected).strip())
                if len(findings) >= max_items_per_bank:
                    break
        if findings:
            content.append(
                {
                    "bank": bank.get("symbol") or bank.get("ticker") or bank_id,
                    "findings": findings,
                }
            )
    return content


def _collect_qa_subtitle_content(
    banks_data: Dict[str, Dict[str, Any]],
    *,
    max_banks: int = 10,
    max_items_per_bank: int = 3,
) -> List[Dict[str, Any]]:
    """Collect selected merged-Q&A findings for subtitle generation."""
    content: List[Dict[str, Any]] = []
    for bank_id, bank in list(banks_data.items())[:max_banks]:
        questions: List[str] = []
        for conversation in bank.get("qa_conversations", []):
            selected = [
                sentence.get("text", "")
                for sentence in conversation.get("question_sentences", [])
                if sentence.get("status") == "selected"
            ]
            if not selected:
                continue
            questions.append(" ".join(selected).strip())
            if len(questions) >= max_items_per_bank:
                break
        if questions:
            content.append(
                {
                    "bank": bank.get("symbol") or bank.get("ticker") or bank_id,
                    "questions": questions,
                }
            )
    return content


def _ensure_subtitle_prefix(subtitle: str, content_type: str, fallback: str) -> str:
    """Normalize generated subtitles to the report's expected section prefixes."""
    clean = _clean_text(subtitle)
    if not clean:
        return fallback
    if content_type == "outlook" and not clean.lower().startswith("outlook:"):
        return f"Outlook: {clean}"
    if content_type == "questions" and not clean.lower().startswith("conference calls:"):
        return f"Conference calls: {clean}"
    return clean


async def generate_section_subtitle(
    *,
    content_json: List[Dict[str, Any]],
    content_type: str,
    section_context: str,
    fallback: str,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> str:
    """Generate one report-section subtitle from aggregated CM content."""
    if not content_json:
        return fallback

    prompt = _load_prompt("subtitle_generation")
    messages = [
        {"role": "developer", "content": prompt["system_prompt"]},
        {
            "role": "user",
            "content": prompt["user_prompt"].format(
                content_type=content_type,
                section_context=section_context,
                content_json=json.dumps(content_json, ensure_ascii=False, indent=2),
            ),
        },
    ]
    try:
        result = await _call_validated_tool(
            messages=messages,
            tool=prompt["tool_definition"],
            label=f"subtitle:{content_type}",
            context=context,
            llm_params=llm_params,
            response_model=SubtitleResponse,
            stage_name=f"Subtitle generation for {content_type}",
            max_attempts=3,
        )
    except RuntimeError as exc:
        logger.warning(
            "Subtitle generation failed; using fallback",
            content_type=content_type,
            error=str(exc),
        )
        return fallback
    return _ensure_subtitle_prefix(result.subtitle, content_type, fallback)


async def generate_report_section_subtitles(
    *,
    banks_data: Dict[str, Dict[str, Any]],
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> Dict[str, str]:
    """Generate merged CM editor subtitles for Outlook and Q&A."""
    outlook_content = _collect_outlook_subtitle_content(banks_data)
    qa_content = _collect_qa_subtitle_content(banks_data)
    outlook_subtitle, qa_subtitle = await asyncio.gather(
        generate_section_subtitle(
            content_json=outlook_content,
            content_type="outlook",
            section_context="Forward-looking outlook statements on capital markets activity",
            fallback="Outlook: Capital markets activity",
            context=context,
            llm_params=llm_params,
        ),
        generate_section_subtitle(
            content_json=qa_content,
            content_type="questions",
            section_context=(
                "Analyst questions on capital markets themes including market activity, "
                "pipelines, confidence, regulation, and funding"
            ),
            fallback="Conference calls: Capital markets questions",
            context=context,
            llm_params=llm_params,
        ),
    )
    return {
        "outlook": outlook_subtitle,
        "qa": qa_subtitle,
    }


def count_included_categories(
    banks_data: Dict[str, Dict[str, Any]],
    min_importance: float,
) -> int:
    """Count report buckets that have at least one selected finding."""
    del min_importance
    included: set[str] = set()
    for bank in banks_data.values():
        for block in bank.get("md_blocks", []):
            for sentence in block.get("sentences", []):
                if sentence.get("status") != "selected":
                    continue
                bucket_id = sentence.get("selected_bucket_id") or sentence.get("primary")
                if bucket_id:
                    included.add(bucket_id)
        for conversation in bank.get("qa_conversations", []):
            for sentence in conversation.get("question_sentences", []) + conversation.get(
                "answer_sentences", []
            ):
                if sentence.get("status") != "selected":
                    continue
                bucket_id = sentence.get("selected_bucket_id") or sentence.get("primary")
                if bucket_id:
                    included.add(bucket_id)
    return len(included)
