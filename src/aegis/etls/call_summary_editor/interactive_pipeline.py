"""Interactive HTML report pipeline for call_summary_editor.

This ports the mock editor workflow onto Aegis transcript data:
- Q&A boundary detection over raw XML speaker blocks
- Per-paragraph MD sentence classification
- Per-exchange QA classification
- Bucket headline generation for the interactive HTML report
"""

from __future__ import annotations

import asyncio
import json
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from aegis.connections.llm_connector import complete_with_tools
from aegis.utils.logging import get_logger

logger = get_logger()

try:
    import spacy

    try:
        _NLP = spacy.load("en_core_web_sm", exclude=["ner", "attribute_ruler", "lemmatizer"])
    except OSError:
        _NLP = spacy.blank("en")
        if "sentencizer" not in _NLP.pipe_names:
            _NLP.add_pipe("sentencizer")
except ImportError:
    _NLP = None


class SentenceResult(BaseModel):
    """Sentence-level classification output."""

    index: int = Field(description="1-based sentence index matching S1, S2, ... in the prompt")
    scores: List[Any] = Field(
        description=(
            "Up to the top 3 bucket-score pairs for this sentence. Each item should include "
            "bucket_index and score."
        )
    )
    importance_score: float = Field(description="IR quotability 0-10")
    condensed: str = Field(description="~70% length, filler removed, all facts kept")


class QAConversationGroup(BaseModel):
    """One Q&A conversation grouping emitted by the boundary tool."""

    conversation_id: str
    block_indices: List[int] = Field(
        default_factory=list,
        description="1-based speaker block indices from the QA boundary prompt.",
    )
    block_ids: List[str] = Field(
        default_factory=list,
        description="Legacy block-level grouping output retained for compatibility.",
    )


class QABoundaryResult(BaseModel):
    """Boundary grouping response for raw QA speaker blocks."""

    conversations: List[QAConversationGroup]


class QAExchangeClassification(BaseModel):
    """Whole-exchange classification for one QA conversation."""

    primary_bucket_index: int = Field(
        description="0-based index of the best existing bucket for the whole exchange."
    )
    question_sentences: List[SentenceResult]
    answer_sentences: List[SentenceResult]


class ProposedConfigRow(BaseModel):
    """Copy-ready config sheet row."""

    transcript_sections: str = Field(description="MD, QA, or ALL")
    report_section: str = Field(description="Results Summary or Earnings Call Q&A")
    category_name: str
    category_description: str
    example_1: str = ""
    example_2: str = ""
    example_3: str = ""


class ExistingSectionCoverageSuggestion(BaseModel):
    """Suggestion to strengthen an existing config category."""

    bucket_index: int = Field(description="0-based existing category index")
    category_name: str
    gap_summary: str
    why_update: str
    supporting_evidence: List[str] = Field(default_factory=list)
    proposed_config_row: ProposedConfigRow


class NewSectionSuggestion(BaseModel):
    """Suggestion to add a new config category."""

    category_name: str
    why_new_section: str
    supporting_evidence: List[str] = Field(default_factory=list)
    suggested_subtitle: str = ""
    proposed_config_row: ProposedConfigRow


class ConfigReviewResult(BaseModel):
    """Coverage review output for the current transcript against the config sheet."""

    existing_section_updates: List[ExistingSectionCoverageSuggestion] = Field(default_factory=list)
    new_section_suggestions: List[NewSectionSuggestion] = Field(default_factory=list)


_SENTENCE_RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "index": {
            "type": "integer",
            "description": "1-based sentence index matching S1, S2, etc. in the prompt",
        },
        "scores": {
            "type": "array",
            "description": (
                "Return up to the top 3 bucket-score pairs for this sentence, ordered by "
                "score descending."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "bucket_index": {"type": "integer"},
                    "score": {"type": "number"},
                },
                "required": ["bucket_index", "score"],
                "additionalProperties": False,
            },
        },
        "importance_score": {"type": "number", "description": "IR quotability score 0-10"},
        "condensed": {"type": "string"},
    },
    "required": ["index", "scores", "importance_score", "condensed"],
    "additionalProperties": False,
}

TOOL_QA_BOUNDARY = {
    "type": "function",
    "function": {
        "name": "group_qa_conversations",
        "strict": True,
        "description": (
            "Call this tool when an indexed Q&A speaker-block list needs conversation boundaries. "
            "It groups the blocks into contiguous analyst-to-executive exchanges and returns the "
            "ordered block indices for each conversation. Use it once per indexed Q&A section."
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

TOOL_MD_PARAGRAPH = {
    "type": "function",
    "function": {
        "name": "classify_paragraph_sentences",
        "strict": True,
        "description": (
            "Call this tool when the current Management Discussion paragraph has indexed sentences "
            "that need bucket scores and importance scoring. It returns one structured result per "
            "sentence in the paragraph. Use it only for the S-numbered sentences shown in the "
            "current paragraph context."
        ),
        "parameters": {
            "type": "object",
            "properties": {"sentences": {"type": "array", "items": _SENTENCE_RESULT_SCHEMA}},
            "required": ["sentences"],
            "additionalProperties": False,
        },
    },
}

TOOL_QA_EXCHANGE = {
    "type": "function",
    "function": {
        "name": "classify_qa_exchange",
        "strict": True,
        "description": (
            "Call this tool when one grouped Q&A exchange needs sentence-level classification. "
            "It returns the best whole-exchange bucket plus one structured result for every "
            "indexed analyst and executive sentence. Use it once per completed Q&A conversation."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "primary_bucket_index": {"type": "integer"},
                "question_sentences": {"type": "array", "items": _SENTENCE_RESULT_SCHEMA},
                "answer_sentences": {"type": "array", "items": _SENTENCE_RESULT_SCHEMA},
            },
            "required": [
                "primary_bucket_index",
                "question_sentences",
                "answer_sentences",
            ],
            "additionalProperties": False,
        },
    },
}

TOOL_HEADLINE = {
    "type": "function",
    "function": {
        "name": "set_headline",
        "strict": True,
        "description": (
            "Call this tool when a populated report bucket needs a short factual headline. "
            "It returns one 5-10 word headline that captures what management actually said in the "
            "sample content. Use it after the bucket content has already been selected."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "headline": {
                    "type": "string",
                    "description": (
                        "A specific, factual 5-10 word headline capturing what management said."
                    ),
                }
            },
            "required": ["headline"],
            "additionalProperties": False,
        },
    },
}

_CONFIG_ROW_SCHEMA = {
    "type": "object",
    "properties": {
        "transcript_sections": {
            "type": "string",
            "description": "Use MD, QA, or ALL exactly.",
        },
        "report_section": {
            "type": "string",
            "description": "Use Results Summary or Earnings Call Q&A exactly.",
        },
        "category_name": {"type": "string"},
        "category_description": {"type": "string"},
        "example_1": {"type": "string"},
        "example_2": {"type": "string"},
        "example_3": {"type": "string"},
    },
    "required": [
        "transcript_sections",
        "report_section",
        "category_name",
        "category_description",
        "example_1",
        "example_2",
        "example_3",
    ],
    "additionalProperties": False,
}

TOOL_CONFIG_REVIEW = {
    "type": "function",
    "function": {
        "name": "review_category_config_coverage",
        "strict": True,
        "description": (
            "Call this tool when a classified transcript needs a config-sheet coverage review. "
            "It returns suggested updates to existing rows and proposed net-new category rows in a "
            "copy-ready config-sheet format. Use it after transcript classification, not before."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "existing_section_updates": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "bucket_index": {"type": "integer"},
                            "category_name": {"type": "string"},
                            "gap_summary": {"type": "string"},
                            "why_update": {"type": "string"},
                            "supporting_evidence": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "proposed_config_row": _CONFIG_ROW_SCHEMA,
                        },
                        "required": [
                            "bucket_index",
                            "category_name",
                            "gap_summary",
                            "why_update",
                            "supporting_evidence",
                            "proposed_config_row",
                        ],
                        "additionalProperties": False,
                    },
                },
                "new_section_suggestions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "category_name": {"type": "string"},
                            "why_new_section": {"type": "string"},
                            "supporting_evidence": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "suggested_subtitle": {"type": "string"},
                            "proposed_config_row": _CONFIG_ROW_SCHEMA,
                        },
                        "required": [
                            "category_name",
                            "why_new_section",
                            "supporting_evidence",
                            "suggested_subtitle",
                            "proposed_config_row",
                        ],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["existing_section_updates", "new_section_suggestions"],
            "additionalProperties": False,
        },
    },
}


def split_sentences(text: str) -> List[str]:
    """Split text into sentences with a spaCy-first fallback."""
    clean = re.sub(r"\s+", " ", (text or "")).strip()
    if not clean:
        return []

    if _NLP is not None:
        doc = _NLP(clean)
        sentences = [sent.text.strip() for sent in doc.sents if sent.text.strip()]
        if sentences:
            return sentences

    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9\"'])", clean)
    return [part.strip() for part in parts if part.strip()]


def format_categories_for_prompt(categories: List[Dict[str, Any]], section_filter: str = "ALL") -> str:
    """Format categories as XML for prompt injection."""
    parts = []
    for idx, category in enumerate(categories):
        section = category.get("transcript_sections", "ALL")
        if section_filter != "ALL" and section not in ("ALL", section_filter):
            continue

        applies = {
            "MD": "Management Discussion only",
            "QA": "Q&A only",
            "ALL": "Both Management Discussion and Q&A",
        }.get(section, "Both Management Discussion and Q&A")

        lines = [
            f'<category index="{idx}">',
            f'  <name>{category["category_name"]}</name>',
            f"  <applies_to>{applies}</applies_to>",
            f'  <description>{category["category_description"]}</description>',
        ]

        examples = [
            category.get(f"example_{example_idx}", "").strip()
            for example_idx in (1, 2, 3)
            if category.get(f"example_{example_idx}", "").strip()
        ]
        if examples:
            lines.append("  <examples>")
            for example in examples:
                lines.append(f"    <example>{example}</example>")
            lines.append("  </examples>")

        lines.append("</category>")
        parts.append("\n".join(lines))

    return "\n\n".join(parts)


def _xml_block(tag: str, content: str) -> str:
    """Wrap dynamic prompt content in an XML block."""
    body = (content or "").strip()
    return f"<{tag}>\n{body}\n</{tag}>"


def _importance_scale_guidance(report_inclusion_threshold: float) -> str:
    """Return prompt guidance for the 0-10 importance scale."""
    threshold = f"{float(report_inclusion_threshold):.1f}"
    return (
        "Use `importance_score` as the draft auto-inclusion score for the report editor.\n"
        f"- Scores >= {threshold} are auto-included in the draft report for analyst review.\n"
        f"- Scores just below {threshold} should be reserved for content that is mostly low-value, "
        "procedural, repetitive, or not worth surfacing by default.\n"
        f"- If a sentence is at least semi-important and you want the user to review it in the "
        f"draft, score it at or above {threshold}.\n"
        "- 0-1: ceremonial, legal boilerplate, operator instructions, or procedural remarks.\n"
        "- 2-3: low-signal detail that usually does not belong in the draft by default.\n"
        "- 4-6: meaningful report-worthy content that should usually appear for review.\n"
        "- 7-8: clearly important takeaway.\n"
        "- 9-10: headline-level or must-keep takeaway."
    )


def applicable_bucket_ids(categories: List[Dict[str, Any]], section: str) -> List[str]:
    """Return bucket ids applicable to the given transcript section."""
    return [
        f"bucket_{idx}"
        for idx, category in enumerate(categories)
        if category.get("transcript_sections") in ("ALL", section)
    ]


def _append_metrics(context: Dict[str, Any], metrics: Dict[str, Any]) -> None:
    if "_llm_costs" not in context:
        context["_llm_costs"] = []
    context["_llm_costs"].append(
        {
            "prompt_tokens": metrics.get("prompt_tokens", 0),
            "completion_tokens": metrics.get("completion_tokens", 0),
            "total_cost": metrics.get("total_cost", 0),
        }
    )


async def _call_tool(
    *,
    messages: List[Dict[str, str]],
    tool: Dict[str, Any],
    label: str,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Run one structured LLM call and return parsed tool arguments."""
    call_llm_params = dict(llm_params)
    call_llm_params.setdefault(
        "tool_choice",
        {"type": "function", "function": {"name": tool["function"]["name"]}},
    )
    response = await complete_with_tools(
        messages=messages,
        tools=[tool],
        context=context,
        llm_params=call_llm_params,
    )

    metrics = response.get("metrics", {})
    if metrics:
        _append_metrics(context, metrics)
        logger.info(
            "etl.call_summary_editor.llm_usage",
            execution_id=context["execution_id"],
            stage=label,
            prompt_tokens=metrics.get("prompt_tokens", 0),
            completion_tokens=metrics.get("completion_tokens", 0),
            total_cost=metrics.get("total_cost", 0),
            response_time=metrics.get("response_time", 0),
        )

    tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls", [])
    if not tool_calls:
        logger.warning(
            "etl.call_summary_editor.tool_call_missing",
            execution_id=context["execution_id"],
            stage=label,
        )
        return None

    arguments = tool_calls[0].get("function", {}).get("arguments", "{}")
    if isinstance(arguments, dict):
        return arguments

    try:
        return json.loads(arguments)
    except json.JSONDecodeError as exc:
        logger.warning(
            "etl.call_summary_editor.tool_call_parse_error",
            execution_id=context["execution_id"],
            stage=label,
            error=str(exc),
        )
        return None


def _bucket_name(bucket_id: str, categories: List[Dict[str, Any]]) -> str:
    if not bucket_id:
        return "Unassigned"
    try:
        idx = int(bucket_id.split("_")[1])
        return categories[idx]["category_name"]
    except Exception:
        return bucket_id


def _fallback_bucket_id(applicable_ids: List[str]) -> str:
    """Return a deterministic fallback bucket when score output is unavailable."""
    return applicable_ids[0] if applicable_ids else ""


def _primary_from_scores(scores: Dict[str, float], applicable_ids: List[str]) -> str:
    """Pick the highest-scoring applicable bucket, else the first applicable bucket."""
    best_id = ""
    best_score: Optional[float] = None
    for bucket_id in applicable_ids:
        score = scores.get(bucket_id)
        if score is None:
            continue
        if best_score is None or score > best_score:
            best_score = score
            best_id = bucket_id
    if best_id and best_score is not None:
        return best_id
    return _fallback_bucket_id(applicable_ids)


def _bucket_score(scores: Dict[str, float], bucket_id: str) -> float:
    """Return the recorded score for a bucket from sparse top-3 score output."""
    if not bucket_id:
        return 0.0
    return float(scores.get(bucket_id, 0.0))


def _normalise_scores(raw_scores: Any, categories: List[Dict[str, Any]]) -> Dict[str, float]:
    """Convert model output into sparse `bucket_N` score mapping."""
    output: Dict[str, float] = {}
    if isinstance(raw_scores, list):
        for idx, value in enumerate(raw_scores):
            if isinstance(value, dict):
                bucket_index = value.get("bucket_index")
                score = value.get("score")
                if (
                    isinstance(bucket_index, int)
                    and 0 <= bucket_index < len(categories)
                    and score is not None
                ):
                    output[f"bucket_{bucket_index}"] = round(float(score), 2)
            elif idx < len(categories):
                output[f"bucket_{idx}"] = round(float(value), 2)
    elif isinstance(raw_scores, dict):
        for key, value in raw_scores.items():
            normalized = key.replace("bucket_", "")
            if normalized.isdigit():
                output[f"bucket_{normalized}"] = round(float(value), 2)
    return output


def _normalise_transcript_section(value: str, default: str = "ALL") -> str:
    """Clamp transcript section values to the supported sheet keys."""
    normalized = str(value or "").strip().upper()
    return normalized if normalized in {"MD", "QA", "ALL"} else default


def _normalise_report_section(value: str, transcript_sections: str) -> str:
    """Clamp report section values to the editor's supported L1 sections."""
    normalized = str(value or "").strip()
    if normalized in {"Results Summary", "Earnings Call Q&A"}:
        return normalized
    return "Earnings Call Q&A" if transcript_sections == "QA" else "Results Summary"


def _normalise_config_row(raw_row: Any) -> Dict[str, str]:
    """Convert tool output into a copy-ready config sheet row."""
    if isinstance(raw_row, ProposedConfigRow):
        row = raw_row.model_dump()
    elif isinstance(raw_row, dict):
        row = dict(raw_row)
    else:
        row = {}

    transcript_sections = _normalise_transcript_section(row.get("transcript_sections", "ALL"))
    report_section = _normalise_report_section(row.get("report_section", ""), transcript_sections)
    return {
        "transcript_sections": transcript_sections,
        "report_section": report_section,
        "category_name": str(row.get("category_name", "")).strip(),
        "category_description": str(row.get("category_description", "")).strip(),
        "example_1": str(row.get("example_1", "")).strip(),
        "example_2": str(row.get("example_2", "")).strip(),
        "example_3": str(row.get("example_3", "")).strip(),
    }


def _make_sentence_record(
    sentence_id: str,
    text: str,
    llm_result: Optional[SentenceResult],
    categories: List[Dict[str, Any]],
    applicable_ids: List[str],
) -> Dict[str, Any]:
    if llm_result is None:
        fallback_bucket = _fallback_bucket_id(applicable_ids)
        return {
            "sid": sentence_id,
            "text": text,
            "primary": fallback_bucket,
            "scores": {},
            "importance_score": 0.0,
            "condensed": text,
        }

    scores = _normalise_scores(llm_result.scores, categories)
    return {
        "sid": sentence_id,
        "text": text,
        "primary": _primary_from_scores(scores, applicable_ids),
        "scores": scores,
        "importance_score": round(float(llm_result.importance_score), 1),
        "condensed": llm_result.condensed or text,
    }


def _build_qa_block_index(
    qa_raw_blocks: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Build a 1-based index of QA speaker blocks for boundary detection."""
    block_entries: List[Dict[str, Any]] = []

    for block_index, block in enumerate(qa_raw_blocks, start=1):
        block_entries.append(
            {
                "block_index": block_index,
                "block_id": block["id"],
                "speaker": block.get("speaker", "Unknown Speaker"),
                "speaker_title": block.get("speaker_title", ""),
                "speaker_affiliation": block.get("speaker_affiliation", ""),
                "speaker_type_hint": block.get("speaker_type_hint", ""),
                "paragraphs": block.get("paragraphs", []),
            }
        )

    return block_entries


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
        preview_parts.append(f"<paragraph>{_preview_text(paragraph, max_paragraph_chars)}</paragraph>")
    return "\n".join(preview_parts)


def _format_qa_block_prompt_entry(entry: Dict[str, Any]) -> str:
    """Format one QA speaker block as XML for boundary detection."""
    hint = (entry.get("speaker_type_hint") or "?").upper()
    return (
        "<qa_block>\n"
        f"  <index>{entry['block_index']}</index>\n"
        f"  <speaker_type_hint>{hint}</speaker_type_hint>\n"
        f"  <speaker>{entry.get('speaker', 'Unknown Speaker')}</speaker>\n"
        f"  <speaker_title>{entry.get('speaker_title', '')}</speaker_title>\n"
        f"  <speaker_affiliation>{entry.get('speaker_affiliation', '')}</speaker_affiliation>\n"
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


def _resolve_block_indices(
    conversation: QAConversationGroup,
    block_id_to_index: Dict[str, int],
) -> List[int]:
    """Resolve tool output into 1-based block indices."""
    if conversation.block_indices:
        return list(conversation.block_indices)

    resolved = []
    for block_id in conversation.block_ids:
        if block_id in block_id_to_index:
            resolved.append(block_id_to_index[block_id])
    return resolved


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
    categories_text_qa: str,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> List[List[Dict[str, Any]]]:
    """Use the mock boundary-detection tool call to group QA speaker blocks."""
    if not qa_raw_blocks:
        return []

    del categories_text_qa  # Boundary detection is transcript-structure work, not topic work.

    block_entries = _build_qa_block_index(qa_raw_blocks)
    block_lines = []
    for entry in block_entries:
        entry = dict(entry)
        entry["preview"] = _format_block_preview(entry.get("paragraphs", []))
        block_lines.append(_format_qa_block_prompt_entry(entry))

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
        "6. Ignore any numbers that appear inside speaker names, titles, affiliations, or preview text.\n"
        "7. Return the grouped indices with the provided tool.\n\n"
        "## Indexed Blocks\n"
        f"{_xml_block('qa_block_index', '\n\n'.join(block_lines))}"
    )
    block_by_index = {idx: block for idx, block in enumerate(qa_raw_blocks, start=1)}
    block_id_to_index = {
        block["id"]: idx for idx, block in enumerate(qa_raw_blocks, start=1)
    }
    base_messages = [
        {"role": "developer", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    messages = list(base_messages)

    max_attempts = 3
    last_parseable_indices: Optional[List[List[int]]] = None
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
        except Exception as exc:
            logger.warning(
                "etl.call_summary_editor.qa_boundary_parse_error",
                execution_id=context["execution_id"],
                error=str(exc),
                attempt=attempt + 1,
            )
            last_validation_errors = [f"Tool output schema validation failed: {exc}"]
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
            _resolve_block_indices(conversation, block_id_to_index)
            for conversation in result.conversations
        ]
        last_parseable_indices = conversation_indices
        validation_errors = _validate_qa_boundary_indices(
            conversation_indices,
            len(qa_raw_blocks),
        )
        if not validation_errors:
            return _materialize_qa_conversations(conversation_indices, block_by_index)

        logger.warning(
            "etl.call_summary_editor.qa_boundary_validation_failed",
            execution_id=context["execution_id"],
            attempt=attempt + 1,
            errors=validation_errors,
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

    if last_parseable_indices is not None:
        logger.warning(
            "etl.call_summary_editor.qa_boundary_using_last_attempt",
            execution_id=context["execution_id"],
            errors=last_validation_errors,
        )
        return _materialize_qa_conversations(last_parseable_indices, block_by_index)

    raise RuntimeError(
        "Q&A boundary detection failed after 3 attempts with no parseable tool response"
    )


async def classify_md_block(
    *,
    block_raw: Dict[str, Any],
    categories: List[Dict[str, Any]],
    categories_text_md: str,
    company_name: str,
    fiscal_year: int,
    fiscal_quarter: str,
    report_inclusion_threshold: float,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> Dict[str, Any]:
    """Classify one MD speaker block sentence-by-sentence."""
    block_id = block_raw["id"]
    paragraphs = block_raw["paragraphs"]
    applicable_ids = applicable_bucket_ids(categories, "MD")

    speaker_line = block_raw["speaker"]
    if block_raw.get("speaker_title"):
        speaker_line += f", {block_raw['speaker_title']}"
    if block_raw.get("speaker_affiliation"):
        speaker_line += f" ({block_raw['speaker_affiliation']})"

    all_para_sentences: List[List[str]] = [split_sentences(paragraph) for paragraph in paragraphs]
    sentence_records: List[Dict[str, Any]] = []
    prior_para_summaries: List[str] = []
    global_sent_idx = 0

    for para_idx, para_sentences in enumerate(all_para_sentences):
        if not para_sentences:
            continue

        context_lines = [f"SPEAKER: {speaker_line}\n"]
        for idx, (paragraph, paragraph_sentences) in enumerate(zip(paragraphs, all_para_sentences)):
            if idx < para_idx:
                context_lines.append(f"[Paragraph {idx + 1} - previously classified]")
                context_lines.append(
                    prior_para_summaries[idx] if idx < len(prior_para_summaries) else paragraph[:200]
                )
            elif idx == para_idx:
                context_lines.append(f"\n[Paragraph {idx + 1} - CLASSIFY THESE SENTENCES:]")
                for sent_idx, sentence in enumerate(paragraph_sentences, start=1):
                    context_lines.append(f'  S{sent_idx}: "{sentence}"')
            else:
                context_lines.append(f"[Paragraph {idx + 1} - not yet processed]")
                context_lines.append(paragraph[:150] + ("..." if len(paragraph) > 150 else ""))

        system_prompt = (
            "You are a sentence classifier for earnings call Management Discussion sections. "
            "Assign each indexed sentence to the best report buckets and score its investor-relations "
            "importance using the category sheet and speaker context. Always use the provided tool."
        )
        user_prompt = (
            "## Task\n"
            f"Classify the indexed sentences in Management Discussion paragraph {para_idx + 1}.\n\n"
            "## Decision Criteria\n"
            "Choose bucket scores from the category descriptions and examples, then score how "
            "quotable each sentence is for an investor-relations summary.\n"
            f"{_importance_scale_guidance(report_inclusion_threshold)}\n\n"
            "## Rules\n"
            "1. Return one result for every S-numbered sentence in the current paragraph.\n"
            "2. Use up to the top 3 bucket-score pairs for each sentence, ordered by score descending.\n"
            "3. Score importance from 0 to 10 using the inclusion guidance above.\n"
            "4. Keep `condensed` faithful to the sentence while removing filler.\n"
            "5. Keep the `index` aligned to the S-number shown in the current paragraph.\n\n"
            "## Categories\n"
            f"{_xml_block('categories', categories_text_md)}\n\n"
            "## Paragraph Context\n"
            f"{_xml_block('md_paragraph_context', '\n'.join(context_lines))}"
        )
        raw = await _call_tool(
            messages=[
                {"role": "developer", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            tool=TOOL_MD_PARAGRAPH,
            label=f"md_para:{block_id}:p{para_idx + 1}",
            context=context,
            llm_params=llm_params,
        )

        llm_results_by_idx: Dict[int, SentenceResult] = {}
        if raw and "sentences" in raw:
            for sentence_raw in raw["sentences"]:
                try:
                    result = SentenceResult.model_validate(sentence_raw)
                except Exception:
                    try:
                        result = SentenceResult(
                            index=sentence_raw.get("index"),
                            scores=sentence_raw.get("scores", []),
                            importance_score=sentence_raw.get("importance_score", 3.0),
                            condensed=sentence_raw.get("condensed", ""),
                        )
                    except Exception as exc:
                        logger.warning(
                            "etl.call_summary_editor.md_sentence_parse_error",
                            execution_id=context["execution_id"],
                            block_id=block_id,
                            paragraph_index=para_idx,
                            error=str(exc),
                        )
                        continue
                llm_results_by_idx[result.index] = result

        labels = []
        for sent_idx, sentence in enumerate(para_sentences, start=1):
            sentence_id = f"{block_id}_s{global_sent_idx}"
            record = _make_sentence_record(
                sentence_id,
                sentence,
                llm_results_by_idx.get(sent_idx),
                categories,
                applicable_ids,
            )
            record["para_idx"] = para_idx
            sentence_records.append(record)
            labels.append(f"S{sent_idx}->{_bucket_name(record['primary'], categories)}")
            global_sent_idx += 1

        prior_para_summaries.append(f"  [{', '.join(labels)}] {paragraphs[para_idx][:120]}...")

    return {
        "id": block_id,
        "speaker": block_raw["speaker"],
        "speaker_title": block_raw.get("speaker_title", ""),
        "speaker_affiliation": block_raw.get("speaker_affiliation", ""),
        "sentences": sentence_records,
    }


async def classify_qa_conversation(
    *,
    conv_idx: int,
    conv_blocks: List[Dict[str, Any]],
    ticker: str,
    categories: List[Dict[str, Any]],
    categories_text_qa: str,
    company_name: str,
    fiscal_year: int,
    fiscal_quarter: str,
    report_inclusion_threshold: float,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> Dict[str, Any]:
    """Classify one QA exchange at sentence level."""
    conv_id = f"{ticker}_QA_{conv_idx}"
    applicable_ids = applicable_bucket_ids(categories, "QA")

    question_blocks = [block for block in conv_blocks if block.get("speaker_type_hint") == "q"]
    answer_blocks = [block for block in conv_blocks if block.get("speaker_type_hint") != "q"]
    if not question_blocks:
        question_blocks, answer_blocks = conv_blocks[:1], conv_blocks[1:]

    analyst_name = question_blocks[0]["speaker"] if question_blocks else "Analyst"
    analyst_affiliation = question_blocks[0].get("speaker_affiliation", "") if question_blocks else ""
    executive_name = answer_blocks[0]["speaker"] if answer_blocks else "Executive"
    executive_title = answer_blocks[0].get("speaker_title", "") if answer_blocks else ""

    question_text = " ".join(paragraph for block in question_blocks for paragraph in block["paragraphs"])
    question_sentences = split_sentences(question_text)
    question_para_indices: List[int] = []
    para_idx = 0
    for block in question_blocks:
        for paragraph in block["paragraphs"]:
            for _sentence in split_sentences(paragraph):
                question_para_indices.append(para_idx)
            para_idx += 1

    answer_sentences_raw: List[str] = []
    answer_para_indices: List[int] = []
    para_idx = 0
    for block in answer_blocks:
        for paragraph in block["paragraphs"]:
            for sentence in split_sentences(paragraph):
                answer_sentences_raw.append(sentence)
                answer_para_indices.append(para_idx)
            para_idx += 1

    question_lines = "\n".join(
        f'QS{idx + 1}: "{sentence}"' for idx, sentence in enumerate(question_sentences)
    )
    answer_lines = "\n".join(
        f'AS{idx + 1}: "{sentence}"' for idx, sentence in enumerate(answer_sentences_raw)
    )
    analyst_label = analyst_name + (f", {analyst_affiliation}" if analyst_affiliation else "")
    executive_label = executive_name + (f", {executive_title}" if executive_title else "")
    exchange_text = (
        f"ANALYST ({analyst_label}):\n"
        f"{question_lines}\n\n"
        f"EXECUTIVE ({executive_label}):\n"
        f"{answer_lines}"
    )
    system_prompt = (
        "You are a sentence classifier for earnings call Q&A exchanges. "
        "Assign a best-fit bucket for the overall exchange and score every indexed analyst and "
        "executive sentence using the category sheet. Always use the provided tool."
    )
    user_prompt = (
        "## Task\n"
        "Classify this Q&A exchange at the whole-conversation level and at the sentence level.\n\n"
        "## Decision Criteria\n"
        "Use the overall exchange topic for `primary_bucket_index`, then score each analyst and "
        "executive sentence from the category descriptions and examples.\n"
        f"{_importance_scale_guidance(report_inclusion_threshold)}\n\n"
        "## Rules\n"
        "1. Set `primary_bucket_index` to the single best existing bucket for the full exchange.\n"
        "2. Return one result for every `QS` and `AS` sentence shown below.\n"
        "3. Use up to the top 3 bucket-score pairs for each sentence, ordered by score descending.\n"
        "4. Score importance from 0 to 10 using the inclusion guidance above.\n"
        "5. Keep `condensed` faithful to the source sentence while removing filler.\n"
        "6. Keep each `index` aligned to the numbered `QS` or `AS` sentence.\n\n"
        "## Categories\n"
        f"{_xml_block('categories', categories_text_qa)}\n\n"
        "## Exchange\n"
        f"{_xml_block('qa_exchange', exchange_text)}"
    )
    raw = await _call_tool(
        messages=[
            {"role": "developer", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        tool=TOOL_QA_EXCHANGE,
        label=f"qa_conv:{conv_id}",
        context=context,
        llm_params=llm_params,
    )

    primary_bucket = _fallback_bucket_id(applicable_ids)
    question_records: List[Dict[str, Any]] = []
    answer_records: List[Dict[str, Any]] = []
    if raw:
        try:
            result = QAExchangeClassification.model_validate(raw)
            if 0 <= result.primary_bucket_index < len(categories):
                primary_bucket = f"bucket_{result.primary_bucket_index}"

            question_by_idx = {sentence.index: sentence for sentence in result.question_sentences}
            for idx, sentence in enumerate(question_sentences, start=1):
                record = _make_sentence_record(
                    f"{conv_id}_qs{idx - 1}",
                    sentence,
                    question_by_idx.get(idx),
                    categories,
                    applicable_ids,
                )
                record["para_idx"] = (
                    question_para_indices[idx - 1] if idx - 1 < len(question_para_indices) else 0
                )
                question_records.append(record)

            answer_by_idx = {sentence.index: sentence for sentence in result.answer_sentences}
            for idx, sentence in enumerate(answer_sentences_raw, start=1):
                record = _make_sentence_record(
                    f"{conv_id}_as{idx - 1}",
                    sentence,
                    answer_by_idx.get(idx),
                    categories,
                    applicable_ids,
                )
                record["para_idx"] = (
                    answer_para_indices[idx - 1] if idx - 1 < len(answer_para_indices) else 0
                )
                answer_records.append(record)
        except Exception as exc:
            logger.warning(
                "etl.call_summary_editor.qa_parse_error",
                execution_id=context["execution_id"],
                conversation_id=conv_id,
                error=str(exc),
            )

    if not question_records:
        for idx, sentence in enumerate(question_sentences):
            question_records.append(
                {
                    "sid": f"{conv_id}_qs{idx}",
                    "text": sentence,
                    "primary": _fallback_bucket_id(applicable_ids),
                    "scores": {},
                    "importance_score": 0.0,
                    "condensed": sentence,
                    "para_idx": question_para_indices[idx] if idx < len(question_para_indices) else 0,
                }
            )

    if not answer_records:
        for idx, sentence in enumerate(answer_sentences_raw):
            answer_records.append(
                {
                    "sid": f"{conv_id}_as{idx}",
                    "text": sentence,
                    "primary": _fallback_bucket_id(applicable_ids),
                    "scores": {},
                    "importance_score": 0.0,
                    "condensed": sentence,
                    "para_idx": answer_para_indices[idx] if idx < len(answer_para_indices) else 0,
                }
            )

    if primary_bucket not in applicable_ids:
        answer_bucket_totals: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {"count": 0.0, "importance": 0.0, "score": 0.0}
        )
        for sentence in answer_records or question_records:
            bucket_id = sentence.get("primary") or _fallback_bucket_id(applicable_ids)
            if not bucket_id:
                continue
            answer_bucket_totals[bucket_id]["count"] += 1.0
            answer_bucket_totals[bucket_id]["importance"] += float(sentence.get("importance_score", 0.0))
            answer_bucket_totals[bucket_id]["score"] += _bucket_score(
                sentence.get("scores", {}),
                bucket_id,
            )
        if answer_bucket_totals:
            primary_bucket = max(
                answer_bucket_totals.items(),
                key=lambda item: (
                    item[1]["count"],
                    item[1]["importance"],
                    item[1]["score"],
                    item[0],
                ),
            )[0]

    return {
        "id": conv_id,
        "primary_bucket": primary_bucket,
        "analyst_name": analyst_name,
        "analyst_affiliation": analyst_affiliation,
        "executive_name": executive_name,
        "executive_title": executive_title,
        "question_sentences": question_records,
        "answer_sentences": answer_records,
    }


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
    report_inclusion_threshold: float,
    max_concurrent_md_blocks: int = 1,
) -> Dict[str, Any]:
    """Convert one bank's raw XML transcript blocks into mock-style bank state."""
    ticker = bank_info.get("full_ticker") or bank_info["bank_symbol"]
    company_name = bank_info["bank_name"]

    logger.info(
        "etl.call_summary_editor.raw_blocks_extracted",
        execution_id=context["execution_id"],
        ticker=ticker,
        md_blocks=len(md_raw_blocks),
        qa_raw_blocks=len(qa_raw_blocks),
    )

    categories_text_md = format_categories_for_prompt(categories, "MD")
    categories_text_qa = format_categories_for_prompt(categories, "QA")
    qa_conversations_raw = await detect_qa_boundaries(
        qa_raw_blocks=qa_raw_blocks,
        categories_text_qa=categories_text_qa,
        context=context,
        llm_params=qa_boundary_llm_params,
    )

    semaphore = asyncio.Semaphore(max(1, max_concurrent_md_blocks))

    async def _process_md_block(block_index: int, block: Dict[str, Any]) -> Dict[str, Any]:
        async with semaphore:
            logger.info(
                "etl.call_summary_editor.md_block_started",
                execution_id=context["execution_id"],
                ticker=ticker,
                block_index=block_index,
                total_blocks=len(md_raw_blocks),
                block_id=block["id"],
            )
            return await classify_md_block(
                block_raw=block,
                categories=categories,
                categories_text_md=categories_text_md,
                company_name=company_name,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                report_inclusion_threshold=report_inclusion_threshold,
                context=context,
                llm_params=md_llm_params,
            )

    processed_md = await asyncio.gather(
        *[
            _process_md_block(idx, block)
            for idx, block in enumerate(md_raw_blocks, start=1)
        ]
    )

    processed_qa = []
    for idx, conversation in enumerate(qa_conversations_raw, start=1):
        logger.info(
            "etl.call_summary_editor.qa_conversation_started",
            execution_id=context["execution_id"],
            ticker=ticker,
            conversation_index=idx,
            total_conversations=len(qa_conversations_raw),
        )
        processed_qa.append(
            await classify_qa_conversation(
                conv_idx=idx,
                conv_blocks=conversation,
                ticker=ticker,
                categories=categories,
                categories_text_qa=categories_text_qa,
                company_name=company_name,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                report_inclusion_threshold=report_inclusion_threshold,
                context=context,
                llm_params=qa_llm_params,
            )
        )

    return {
        "ticker": ticker,
        "company_name": company_name,
        "transcript_title": transcript_title or f"{fiscal_quarter} {fiscal_year} Earnings Call",
        "fiscal_year": fiscal_year,
        "fiscal_quarter": fiscal_quarter,
        "md_blocks": processed_md,
        "qa_conversations": processed_qa,
    }


def _build_config_review_digest(
    bank_data: Dict[str, Any],
    categories: List[Dict[str, Any]],
    min_importance: float,
    max_items: int = 48,
) -> str:
    """Build a transcript digest for config coverage review."""
    entries: List[str] = []

    for block_index, block in enumerate(bank_data.get("md_blocks", []), start=1):
        notable = [
            sentence
            for sentence in block.get("sentences", [])
            if sentence.get("importance_score", 0) >= min_importance
        ]
        if not notable:
            continue

        speaker_line = block.get("speaker", "Unknown Speaker")
        if block.get("speaker_title"):
            speaker_line += f", {block['speaker_title']}"

        lines = [f"[MD Block {block_index}] {speaker_line}"]
        for sentence in sorted(
            notable,
            key=lambda item: (
                float(item.get("importance_score", 0)),
                _bucket_score(item.get("scores", {}), item.get("primary", "")),
            ),
            reverse=True,
        )[:4]:
            bucket_name = _bucket_name(sentence.get("primary", ""), categories)
            text = sentence.get("condensed") or sentence.get("text", "")
            lines.append(
                f"- importance={float(sentence.get('importance_score', 0)):.1f} | "
                f"bucket={bucket_name} | {_preview_text(text, 320)}"
            )
        entries.append("\n".join(lines))

    for conversation_index, conversation in enumerate(bank_data.get("qa_conversations", []), start=1):
        notable_answers = [
            sentence
            for sentence in conversation.get("answer_sentences", [])
            if sentence.get("importance_score", 0) >= min_importance
        ]
        if not notable_answers and not conversation.get("question_sentences"):
            continue

        question_text = " ".join(
            sentence.get("text", "") for sentence in conversation.get("question_sentences", [])
        ).strip()
        lines = [
            f"[QA Conversation {conversation_index}] "
            f"Q: {_preview_text(question_text, 320) if question_text else 'Question unavailable'}"
        ]
        lines.append(
            f"Current conversation bucket: {_bucket_name(conversation.get('primary_bucket', ''), categories)}"
        )
        for sentence in sorted(
            notable_answers,
            key=lambda item: (
                float(item.get("importance_score", 0)),
                _bucket_score(item.get("scores", {}), item.get("primary", "")),
            ),
            reverse=True,
        )[:4]:
            bucket_name = _bucket_name(sentence.get("primary", ""), categories)
            text = sentence.get("condensed") or sentence.get("text", "")
            lines.append(
                f"- importance={float(sentence.get('importance_score', 0)):.1f} | "
                f"bucket={bucket_name} | {_preview_text(text, 320)}"
            )
        entries.append("\n".join(lines))

    return "\n\n".join(entries[:max_items])


async def analyze_config_coverage(
    *,
    bank_data: Dict[str, Any],
    categories: List[Dict[str, Any]],
    min_importance: float,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> Dict[str, Any]:
    """Review transcript coverage against the current input config sheet."""
    transcript_digest = _build_config_review_digest(bank_data, categories, min_importance)
    if not transcript_digest:
        return {"existing_section_updates": [], "new_section_suggestions": []}

    categories_text = format_categories_for_prompt(categories, "ALL")
    company_name = bank_data.get("company_name", "the company")
    fiscal_quarter = bank_data.get("fiscal_quarter", "")
    fiscal_year = bank_data.get("fiscal_year", "")
    system_prompt = (
        "You are a config-review analyst for investor-relations call summary category sheets. "
        "Review the classified transcript against the current sheet and identify the highest-signal "
        "gaps in existing rows or missing categories. Always use the provided tool."
    )
    user_prompt = (
        "## Task\n"
        f"Review {company_name}'s {fiscal_quarter} {fiscal_year} transcript against the current "
        "category config sheet.\n\n"
        "## Decision Criteria\n"
        "Use `existing_section_updates` when the evidence clearly belongs in an existing category "
        "but the row description or examples need stronger coverage. Use `new_section_suggestions` "
        "when the content is important and should likely become its own category.\n\n"
        "## Rules\n"
        "1. Return only high-signal suggestions that would improve future runs.\n"
        "2. Keep an existing category's identity unchanged when suggesting an update to that row.\n"
        "3. Make every `proposed_config_row` copy-ready for the input XLSX.\n"
        "4. Use `transcript_sections` as MD, QA, or ALL and `report_section` as Results Summary or Earnings Call Q&A.\n"
        "5. Avoid duplicates and cap each suggestion list at 5 items.\n\n"
        "## Current Category Sheet\n"
        f"{_xml_block('categories', categories_text)}\n\n"
        "## Transcript Evidence Digest\n"
        f"{_xml_block('transcript_digest', transcript_digest)}"
    )
    raw = await _call_tool(
        messages=[
            {"role": "developer", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        tool=TOOL_CONFIG_REVIEW,
        label="config_review",
        context=context,
        llm_params=llm_params,
    )
    if not raw:
        return {"existing_section_updates": [], "new_section_suggestions": []}

    try:
        result = ConfigReviewResult.model_validate(raw)
    except Exception as exc:
        logger.warning(
            "etl.call_summary_editor.config_review_parse_error",
            execution_id=context["execution_id"],
            error=str(exc),
        )
        return {"existing_section_updates": [], "new_section_suggestions": []}

    existing_updates: List[Dict[str, Any]] = []
    seen_existing = set()
    for suggestion in result.existing_section_updates[:5]:
        bucket_index = suggestion.bucket_index
        if not 0 <= bucket_index < len(categories):
            bucket_index = next(
                (
                    idx
                    for idx, category in enumerate(categories)
                    if category.get("category_name") == suggestion.category_name
                ),
                -1,
            )
        if not 0 <= bucket_index < len(categories):
            continue

        base_category = categories[bucket_index]
        row = _normalise_config_row(suggestion.proposed_config_row)
        merged_row = {
            "transcript_sections": base_category.get("transcript_sections", "ALL"),
            "report_section": base_category.get("report_section", "Results Summary"),
            "category_name": base_category.get("category_name", row["category_name"]),
            "category_description": row["category_description"]
            or base_category.get("category_description", ""),
            "example_1": row["example_1"] or base_category.get("example_1", ""),
            "example_2": row["example_2"] or base_category.get("example_2", ""),
            "example_3": row["example_3"] or base_category.get("example_3", ""),
        }
        dedupe_key = (
            bucket_index,
            merged_row["category_description"],
            merged_row["example_1"],
            merged_row["example_2"],
            merged_row["example_3"],
        )
        if dedupe_key in seen_existing:
            continue
        seen_existing.add(dedupe_key)
        existing_updates.append(
            {
                "bucket_index": bucket_index,
                "bucket_id": f"bucket_{bucket_index}",
                "category_name": base_category.get("category_name", suggestion.category_name),
                "gap_summary": suggestion.gap_summary.strip(),
                "why_update": suggestion.why_update.strip(),
                "supporting_evidence": [
                    evidence.strip()
                    for evidence in suggestion.supporting_evidence
                    if evidence and evidence.strip()
                ][:3],
                "proposed_config_row": merged_row,
            }
        )

    new_section_suggestions: List[Dict[str, Any]] = []
    seen_new = set()
    existing_names = {category.get("category_name", "").strip().lower() for category in categories}
    for suggestion in result.new_section_suggestions[:5]:
        row = _normalise_config_row(suggestion.proposed_config_row)
        if not row["category_name"] or not row["category_description"]:
            continue
        dedupe_name = row["category_name"].strip().lower()
        dedupe_key = (dedupe_name, row["report_section"], row["transcript_sections"])
        if dedupe_name in existing_names or dedupe_key in seen_new:
            continue
        seen_new.add(dedupe_key)
        new_section_suggestions.append(
            {
                "category_name": row["category_name"],
                "why_new_section": suggestion.why_new_section.strip(),
                "supporting_evidence": [
                    evidence.strip()
                    for evidence in suggestion.supporting_evidence
                    if evidence and evidence.strip()
                ][:3],
                "suggested_subtitle": suggestion.suggested_subtitle.strip(),
                "proposed_config_row": row,
            }
        )

    return {
        "existing_section_updates": existing_updates,
        "new_section_suggestions": new_section_suggestions,
    }


def _build_auto_include_candidates(bank_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Build subquote-like report candidates grouped by assigned bucket."""
    candidates: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for block in bank_data.get("md_blocks", []):
        current: Optional[Dict[str, Any]] = None
        for sentence in block.get("sentences", []):
            bucket_id = sentence.get("primary")
            if not bucket_id:
                continue
            if not current or current["bucket_id"] != bucket_id:
                if current:
                    candidates[current["bucket_id"]].append(current)
                current = {
                    "bucket_id": bucket_id,
                    "importance": float(sentence.get("importance_score", 0.0)),
                    "bucket_score": _bucket_score(sentence.get("scores", {}), bucket_id),
                    "text": sentence.get("condensed") or sentence.get("text", ""),
                }
            else:
                current["importance"] = max(
                    current["importance"],
                    float(sentence.get("importance_score", 0.0)),
                )
                current["bucket_score"] = max(
                    current["bucket_score"],
                    _bucket_score(sentence.get("scores", {}), bucket_id),
                )
                extra_text = sentence.get("condensed") or sentence.get("text", "")
                if extra_text:
                    current["text"] = f"{current['text']} {extra_text}".strip()
        if current:
            candidates[current["bucket_id"]].append(current)

    for conversation in bank_data.get("qa_conversations", []):
        current = None
        for sentence in conversation.get("answer_sentences", []):
            bucket_id = sentence.get("primary")
            if not bucket_id:
                continue
            if not current or current["bucket_id"] != bucket_id:
                if current:
                    candidates[current["bucket_id"]].append(current)
                current = {
                    "bucket_id": bucket_id,
                    "importance": float(sentence.get("importance_score", 0.0)),
                    "bucket_score": _bucket_score(sentence.get("scores", {}), bucket_id),
                    "text": sentence.get("condensed") or sentence.get("text", ""),
                }
            else:
                current["importance"] = max(
                    current["importance"],
                    float(sentence.get("importance_score", 0.0)),
                )
                current["bucket_score"] = max(
                    current["bucket_score"],
                    _bucket_score(sentence.get("scores", {}), bucket_id),
                )
                extra_text = sentence.get("condensed") or sentence.get("text", "")
                if extra_text:
                    current["text"] = f"{current['text']} {extra_text}".strip()
        if current:
            candidates[current["bucket_id"]].append(current)

    return candidates


def collect_headline_samples(
    banks_data: Dict[str, Dict[str, Any]],
    min_importance: float,
) -> Dict[str, List[str]]:
    """Collect all report-included snippets for bucket-level headline generation."""
    samples: Dict[str, List[str]] = defaultdict(list)
    for bank_data in banks_data.values():
        candidates_by_bucket = _build_auto_include_candidates(bank_data)
        for bucket_id, candidates in candidates_by_bucket.items():
            eligible = [
                candidate
                for candidate in candidates
                if candidate["importance"] >= min_importance
            ]
            if not eligible:
                continue

            ranked = sorted(
                eligible,
                key=lambda item: (
                    item["importance"],
                    item["bucket_score"],
                    len(item.get("text", "")),
                ),
                reverse=True,
            )
            for candidate in ranked:
                if candidate.get("text"):
                    samples[bucket_id].append(candidate["text"])
    return samples


async def generate_bucket_headlines(
    *,
    banks_data: Dict[str, Dict[str, Any]],
    categories: List[Dict[str, Any]],
    min_importance: float,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> Dict[str, str]:
    """Generate bucket headlines mirroring the mock editor workflow."""
    samples_by_bucket = collect_headline_samples(banks_data, min_importance)
    headlines: Dict[str, str] = {}

    for idx, category in enumerate(categories):
        bucket_id = f"bucket_{idx}"
        samples = samples_by_bucket.get(bucket_id, [])
        if not samples:
            continue

        sample_text = "\n\n---\n\n".join(samples[:8])
        system_prompt = (
            "You are a headline writer for investor-relations earnings summaries. "
            "Turn already-selected bucket content into a short factual headline that reflects what "
            "management actually said. Always use the provided tool."
        )
        user_prompt = (
            "## Task\n"
            f"Generate a 5-10 word headline for the '{category['category_name']}' bucket.\n\n"
            "## Decision Criteria\n"
            "The headline should be specific, factual, and driven by the sample content rather than "
            "generic financial language.\n\n"
            "## Rules\n"
            "1. Capture the most important shared point across the sample content.\n"
            "2. Keep the wording specific enough to distinguish this bucket from other sections.\n"
            "3. Do not add trailing punctuation.\n"
            "4. Return the headline with the provided tool.\n\n"
            "## Sample Content\n"
            f"{_xml_block('sample_content', sample_text)}"
        )
        raw = await _call_tool(
            messages=[
                {"role": "developer", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            tool=TOOL_HEADLINE,
            label=f"headline:{bucket_id}",
            context=context,
            llm_params=llm_params,
        )
        if raw and raw.get("headline"):
            headlines[bucket_id] = raw["headline"].strip().strip("\"'")

    return headlines


def count_included_categories(
    banks_data: Dict[str, Dict[str, Any]],
    min_importance: float,
) -> int:
    """Count buckets with at least one auto-included report sample."""
    return len(collect_headline_samples(banks_data, min_importance))
