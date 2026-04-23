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
from typing import Any, Dict, List, Optional, Tuple
from xml.sax.saxutils import escape as _xml_escape

from pydantic import BaseModel, Field

from aegis.connections.llm_connector import complete_with_tools
from aegis.utils.logging import get_logger

logger = get_logger()

_GENERALIZED_CATEGORY_NAME_DESCRIPTION = (
    "Generalized, bank-agnostic category title in the same style as the current config "
    "sheet. Use a reusable industry label that could apply next quarter across multiple "
    "banks. Prefer a short noun-phrase title; keep transcript-specific wording, claims, "
    "metrics, bank names, and timing context in the description or examples, not in the "
    "title."
)

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


class FindingGroup(BaseModel):
    """One finding: a contiguous group of sentence indices within a speaker block."""

    sentence_indices: List[int] = Field(
        description=(
            "1-based sentence indices belonging to this finding, in ascending contiguous "
            "order (e.g. [1,2,3] or [4] or [5,6])."
        )
    )


class FindingGroupResult(BaseModel):
    """Grouping tool response: ordered findings covering every sentence in the block."""

    findings: List[FindingGroup]


class FindingResult(BaseModel):
    """Finding-level classification output."""

    index: int = Field(description="1-based finding index matching F1, F2, ... in the prompt")
    scores: List[Any] = Field(
        description=(
            "Up to the top 3 bucket-score pairs for this finding. Each item should include "
            "bucket_index and a 0-10 relevance score."
        )
    )
    importance_score: float = Field(description="IR quotability 0-10")
    condensed: str = Field(
        description=(
            "Condensed form of the full finding (joined sentences) — filler removed, all "
            "facts kept."
        )
    )


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
    """Whole-exchange classification for one QA conversation.

    Analyst question sentences are intentionally excluded from per-sentence
    classification: anything an analyst says is treated as context only and
    never becomes a standalone finding. The model is asked to produce a short
    paraphrase of the analyst's question so that downstream renderings can
    prefix executive findings with "In response to the analyst question: ...".
    """

    primary_bucket_index: int = Field(
        description="0-based index of the best existing bucket for the whole exchange."
    )
    analyst_question_summary: str = Field(
        description=(
            "One-sentence paraphrase of the analyst's question (\u226425 words). "
            "Used as the lead-in line above the executive findings."
        )
    )
    answer_findings: List[FindingResult]


class ProposedConfigRow(BaseModel):
    """Copy-ready config sheet row."""

    transcript_sections: str = Field(description="MD, QA, or ALL")
    report_section: str = Field(description="Results Summary or Earnings Call Q&A")
    category_name: str = Field(description=_GENERALIZED_CATEGORY_NAME_DESCRIPTION)
    category_description: str = Field(
        description=(
            "Business-editable category guidance kept in one multiline cell. Prefer section "
            "headings like `Topics:`, `Keywords:`, and `Instructions:`; optional extra "
            "headings such as `Notes:` or `Overrides:` are allowed."
        )
    )
    example_1: str = ""
    example_2: str = ""
    example_3: str = ""


class DescriptionUpdateProposal(BaseModel):
    """Pass 1: tightened description for one existing category row."""

    target_category_name: str = Field(
        description="Exact `category_name` of the existing row whose description should be edited."
    )
    change_summary: str = Field(
        description="One or two sentence reasoning for why the description needs to be edited."
    )
    proposed_description: str = Field(
        description=(
            "The full replacement `category_description` — copy-paste ready. Keep it as one "
            "multiline cell using section headings. Prefer `Topics:`, `Keywords:`, and "
            "`Instructions:`; optional extra headings are allowed. The other fields "
            "(transcript_sections, report_section, category_name, examples) are preserved "
            "server-side from the current row. This update must be additive: preserve the "
            "existing scope and append or clarify; do not remove prior concepts."
        )
    )


class DescriptionUpdatesResult(BaseModel):
    """Structured pass-1 proposals: existing-category description refinements."""

    proposals: List[DescriptionUpdateProposal] = Field(default_factory=list)


class EmergingTopicProposal(BaseModel):
    """Pass 2: one emerging-topic candidate with linked finding IDs."""

    change_summary: str = Field(
        description=(
            "One or two sentence reasoning for why this is a distinct emerging topic "
            "worth tracking across future quarters."
        )
    )
    proposed_row: ProposedConfigRow = Field(
        description="The full copy-paste ready config row for the new category."
    )
    linked_finding_ids: List[str] = Field(
        default_factory=list,
        description=(
            "Finding ids from the digest that belong under this emerging topic — across "
            "any status (selected/candidate/rejected) and regardless of current bucket."
        ),
    )


class EmergingTopicsResult(BaseModel):
    """Structured pass-2 proposals: brand-new emerging categories."""

    proposals: List[EmergingTopicProposal] = Field(default_factory=list)


_FINDING_GROUP_SCHEMA = {
    "type": "object",
    "properties": {
        "sentence_indices": {
            "type": "array",
            "description": (
                "1-based sentence indices belonging to this finding, in ascending "
                "contiguous order (e.g. [1], [2,3], [4]). Size follows the idea: "
                "include exactly the sentences that express one coherent claim, "
                "metric, or argument — no more."
            ),
            "items": {"type": "integer"},
        },
    },
    "required": ["sentence_indices"],
    "additionalProperties": False,
}

_FINDING_RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "index": {
            "type": "integer",
            "description": "1-based finding index matching F1, F2, etc. in the prompt",
        },
        "scores": {
            "type": "array",
            "description": (
                "Return up to the top 3 bucket-score pairs for this finding, ordered by "
                "score descending. Use a 0-10 relevance scale."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "bucket_index": {"type": "integer"},
                    "score": {"type": "number", "description": "Bucket relevance score from 0 to 10"},
                },
                "required": ["bucket_index", "score"],
                "additionalProperties": False,
            },
        },
        "importance_score": {"type": "number", "description": "IR quotability score 0-10"},
        "condensed": {
            "type": "string",
            "description": (
                "Condensed form of the full finding (joined sentences) — filler removed, "
                "all facts kept."
            ),
        },
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

TOOL_MD_GROUPING = {
    "type": "function",
    "function": {
        "name": "group_md_block_findings",
        "strict": True,
        "description": (
            "Call this tool when an indexed Management Discussion speaker block's sentences "
            "need to be split into findings. A finding is ONE coherent idea — one claim, "
            "one metric with its context, one forward-looking statement — spanning exactly "
            "the sentences required to express it. Size follows the idea, not a target: a "
            "finding may be one sentence or several, but sentences covering distinct "
            "metrics, time horizons, business lines, or independent claims must be split "
            "into separate findings. Every indexed sentence must appear in exactly one "
            "finding, as a contiguous ascending sequence of indices."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "findings": {
                    "type": "array",
                    "description": (
                        "Ordered list of findings covering every sentence in the block. "
                        "Each finding contains exactly the sentences that express one "
                        "coherent idea — split whenever the next sentence starts a "
                        "distinct idea, even inside the same paragraph. Findings are "
                        "listed in sentence order, each with a contiguous ascending list "
                        "of sentence indices."
                    ),
                    "items": _FINDING_GROUP_SCHEMA,
                },
            },
            "required": ["findings"],
            "additionalProperties": False,
        },
    },
}

TOOL_QA_GROUPING = {
    "type": "function",
    "function": {
        "name": "group_qa_block_findings",
        "strict": True,
        "description": (
            "Call this tool when an indexed Q&A speaker block's sentences (analyst or "
            "executive) need to be split into findings. A finding is ONE coherent idea — "
            "one question, one answer point, one claim, one metric with its context — "
            "spanning exactly the sentences required to express it. Size follows the idea, "
            "not a target: a finding may be one sentence or several, but sentences "
            "covering distinct metrics, time horizons, business lines, or independent "
            "claims must be split into separate findings. Every indexed sentence must "
            "appear in exactly one finding, as a contiguous ascending sequence of indices."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "findings": {
                    "type": "array",
                    "description": (
                        "Ordered list of findings covering every sentence in the block. "
                        "Each finding contains exactly the sentences that express one "
                        "coherent idea — split whenever the next sentence starts a "
                        "distinct idea, even inside the same paragraph. Findings are "
                        "listed in sentence order, each with a contiguous ascending list "
                        "of sentence indices."
                    ),
                    "items": _FINDING_GROUP_SCHEMA,
                },
            },
            "required": ["findings"],
            "additionalProperties": False,
        },
    },
}

TOOL_MD_BLOCK = {
    "type": "function",
    "function": {
        "name": "classify_md_block_findings",
        "strict": True,
        "description": (
            "Call this tool when the current Management Discussion speaker block has indexed "
            "findings (F1, F2, ...) that need bucket scores, importance scoring, and a "
            "condensed summary. It returns one structured result per finding in the block. "
            "Use it exactly once per speaker block."
        ),
        "parameters": {
            "type": "object",
            "properties": {"findings": {"type": "array", "items": _FINDING_RESULT_SCHEMA}},
            "required": ["findings"],
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
            "Call this tool when one grouped Q&A exchange needs finding-level classification "
            "of the executive answer. It returns the best whole-exchange bucket, a one-sentence "
            "paraphrase of the analyst's question, and one structured result for every indexed "
            "executive answer finding. Analyst findings are NOT classified individually "
            "\u2014 they are context only. Use this tool once per completed Q&A conversation."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "primary_bucket_index": {"type": "integer"},
                "analyst_question_summary": {
                    "type": "string",
                    "description": (
                        "One-sentence paraphrase of the analyst's question (\u226425 words). "
                        "Reads as a complete clause, e.g. 'on capital deployment plans for "
                        "the back half of the year'."
                    ),
                },
                "answer_findings": {"type": "array", "items": _FINDING_RESULT_SCHEMA},
            },
            "required": [
                "primary_bucket_index",
                "analyst_question_summary",
                "answer_findings",
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
        "category_name": {
            "type": "string",
            "description": _GENERALIZED_CATEGORY_NAME_DESCRIPTION,
        },
        "category_description": {
            "type": "string",
            "description": (
                "Business-editable category guidance kept in one multiline cell. Prefer "
                "headings like Topics, Keywords, and Instructions; optional extra headings "
                "are allowed."
            ),
        },
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

TOOL_DESCRIPTION_UPDATES = {
    "type": "function",
    "function": {
        "name": "propose_description_updates",
        "strict": True,
        "description": (
            "Call this tool when the current category sheet has descriptions that should be "
            "tightened or extended based on indexed findings from the transcript. Return only "
            "the categories whose `category_description` should change, along with a one-to-two "
            "sentence reasoning and the full replacement description ready to paste into the "
            "config sheet. Keep it as one multiline cell with section headings, prioritizing "
            "`Topics`, `Keywords`, and `Instructions`. Do not propose new categories — that "
            "is a separate pass."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "proposals": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "target_category_name": {
                                "type": "string",
                                "description": (
                                    "Exact `category_name` of the existing row to edit."
                                ),
                            },
                            "change_summary": {
                                "type": "string",
                                "description": (
                                    "One or two sentence reasoning for the description change."
                                ),
                            },
                            "proposed_description": {
                                "type": "string",
                                "description": (
                                    "Full replacement `category_description` — copy-paste ready. "
                                    "Keep it as one multiline cell with section headings, "
                                    "preferably Topics, Keywords, and Instructions. The update "
                                    "must be additive and preserve existing scope."
                                ),
                            },
                        },
                        "required": [
                            "target_category_name",
                            "change_summary",
                            "proposed_description",
                        ],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["proposals"],
            "additionalProperties": False,
        },
    },
}

TOOL_EMERGING_TOPICS = {
    "type": "function",
    "function": {
        "name": "propose_emerging_topics",
        "strict": True,
        "description": (
            "Call this tool to identify brand-new emerging topics worth tracking in future "
            "quarters — themes that do not fit any existing category. For each topic return "
            "a copy-ready config row (including `report_section` so it lands in Results "
            "Summary or Earnings Call Q&A) and the finding ids that belong under it. New "
            "`category_name` values must be generalized, industry-reusable taxonomy labels, "
            "not transcript-specific findings or quotes. Finding ids may come from any status "
            "(selected/candidate/rejected) and may currently sit in other buckets — if the "
            "user enables the topic, those findings will be moved."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "proposals": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "change_summary": {
                                "type": "string",
                                "description": (
                                    "One or two sentence reasoning for why this is a distinct "
                                    "emerging topic worth tracking."
                                ),
                            },
                            "proposed_row": _CONFIG_ROW_SCHEMA,
                            "linked_finding_ids": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": (
                                    "Finding ids from the digest that belong under this topic. "
                                    "Use only ids that appear in the digest."
                                ),
                            },
                        },
                        "required": [
                            "change_summary",
                            "proposed_row",
                            "linked_finding_ids",
                        ],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["proposals"],
            "additionalProperties": False,
        },
    },
}


def split_sentences(text: str) -> List[str]:
    """Split text into sentences with a spaCy-first fallback.

    The regex fallback (used when spaCy is unavailable) splits on terminal
    punctuation followed by whitespace and a sentence-starting character.
    Earnings transcripts frequently start sentences with currency symbols
    (`$`, `\u20ac`, `\u00a3`, `\u00a5`), opening parentheses or brackets, em/en
    dashes, or even lowercase words after diarization quirks - all are
    accepted here.
    """
    clean = re.sub(r"\s+", " ", (text or "")).strip()
    if not clean:
        return []

    if _NLP is not None:
        doc = _NLP(clean)
        sentences = [sent.text.strip() for sent in doc.sents if sent.text.strip()]
        if sentences:
            return sentences

    parts = re.split(
        r"(?<=[.!?])\s+(?=[A-Za-z0-9\"'\(\[\$\u20ac\u00a3\u00a5\u2013\u2014])",
        clean,
    )
    return [part.strip() for part in parts if part.strip()]


def _escape_for_prompt(value: Any) -> str:
    """Escape a value for safe interpolation inside an XML-style prompt block.

    Categories, speaker names, and transcript text are interpolated into
    XML tags throughout this module. Without escaping, a stray `<`, `>`, or
    `&` in a category description (or a speaker name like "Smith & Co")
    would break the surrounding XML structure. We escape on interpolation
    rather than at ingest so the original strings are preserved everywhere
    else (logs, HTML state, DB metadata).
    """
    return _xml_escape(str(value or ""), entities={"\"": "&quot;"})


_CATEGORY_DESCRIPTION_SECTION_ALIASES = {
    "topic": "topics",
    "topics": "topics",
    "subtopic": "topics",
    "subtopics": "topics",
    "concept": "topics",
    "concepts": "topics",
    "key topic": "topics",
    "key topics": "topics",
    "key concept": "topics",
    "key concepts": "topics",
    "keyword": "keywords",
    "keywords": "keywords",
    "key word": "keywords",
    "key words": "keywords",
    "instruction": "instructions",
    "instructions": "instructions",
    "rule": "instructions",
    "rules": "instructions",
}
_CATEGORY_DESCRIPTION_SECTION_RE = re.compile(r"^(?P<header>[A-Za-z][A-Za-z _/-]*):\s*(?P<rest>.*)$")
_CATEGORY_DESCRIPTION_BULLET_RE = re.compile(r"^(?:[-*•]+|\d+[.)])\s*")


def _append_unique_items(target: List[str], items: List[str]) -> None:
    """Append non-empty items while preserving order and removing duplicates."""
    for item in items:
        clean = str(item or "").strip()
        if clean and clean not in target:
            target.append(clean)


def _normalise_category_description_header(value: str) -> str:
    """Normalise a structured category-description header for matching."""
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _split_category_description_items(section: str, value: str) -> List[str]:
    """Split inline description content into list items.

    `keywords` and `topics` often appear inline as comma-separated phrases.
    All other sections should remain intact because commas are common inside a
    single note or rule.
    """
    clean = str(value or "").strip()
    if not clean:
        return []
    if section not in {"topics", "keywords"}:
        return [clean]
    return [item.strip() for item in re.split(r"[;,]\s*", clean) if item.strip()]


def parse_category_description(description: str) -> Dict[str, Any]:
    """Parse a business-friendly category description into structured sections.

    Supported structured format inside the existing `category_description`
    cell:

    Topics:
    - topic one
    - topic two
    Keywords:
    - keyword
    Instructions:
    - inclusion, exclusion, or tie-break rules

    Notes:
    - optional extra guidance

    The parser is intentionally permissive:
    - Section headers are case-insensitive.
    - Topics/keywords may be comma-separated on the header line.
    - Extra section names such as `Notes` or `Overrides` are preserved.
    - Existing plain-text descriptions remain valid and are returned as
      `legacy_free_text`.
    """
    raw = str(description or "").strip()
    parsed = {
        "format": "legacy_free_text",
        "raw": raw,
        "brief": raw,
        "topics": [],
        "keywords": [],
        "instructions": [],
        "additional_sections": [],
    }
    if not raw:
        return parsed

    current_section_key: Optional[str] = None
    section_count = 0
    intro_lines: List[str] = []
    structured_items = {"topics": [], "keywords": [], "instructions": []}
    additional_sections: Dict[str, Dict[str, Any]] = {}

    def _get_section_bucket(header_name: str, role: str) -> List[str]:
        if role in structured_items:
            return structured_items[role]

        section_key = _normalise_category_description_header(header_name)
        if section_key not in additional_sections:
            additional_sections[section_key] = {
                "name": header_name.strip() or "Additional",
                "items": [],
            }
        return additional_sections[section_key]["items"]

    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        header_match = _CATEGORY_DESCRIPTION_SECTION_RE.match(line)
        if header_match:
            header_name = header_match.group("header").strip()
            section = _CATEGORY_DESCRIPTION_SECTION_ALIASES.get(
                _normalise_category_description_header(header_match.group("header"))
            )
            current_section_key = section or _normalise_category_description_header(header_name)
            section_count += 1
            _append_unique_items(
                _get_section_bucket(header_name, section or current_section_key),
                _split_category_description_items(section or current_section_key, header_match.group("rest")),
            )
            continue

        if current_section_key:
            clean_item = _CATEGORY_DESCRIPTION_BULLET_RE.sub("", line).strip()
            _append_unique_items(
                _get_section_bucket(current_section_key, current_section_key),
                _split_category_description_items(current_section_key, clean_item),
            )
            continue

        intro_lines.append(line)

    if not section_count:
        return parsed

    return {
        "format": "sectioned_lists",
        "raw": raw,
        "brief": " ".join(intro_lines).strip(),
        **structured_items,
        "additional_sections": list(additional_sections.values()),
    }


def render_sectioned_category_description(parsed_description: Dict[str, Any]) -> str:
    """Render a parsed sectioned description back to the single-cell format."""
    if parsed_description.get("format") != "sectioned_lists":
        return str(parsed_description.get("raw", "") or "").strip()

    lines: List[str] = []
    for heading, key in (
        ("Topics", "topics"),
        ("Keywords", "keywords"),
        ("Instructions", "instructions"),
    ):
        items = [str(item).strip() for item in parsed_description.get(key, []) if str(item).strip()]
        if not items:
            continue
        lines.append(f"{heading}:")
        lines.extend(f"- {item}" for item in items)

    for section in parsed_description.get("additional_sections", []) or []:
        section_name = str(section.get("name", "") or "").strip()
        items = [str(item).strip() for item in section.get("items", []) if str(item).strip()]
        if not section_name or not items:
            continue
        lines.append(f"{section_name}:")
        lines.extend(f"- {item}" for item in items)

    return "\n".join(lines).strip()


def merge_sectioned_category_descriptions(
    existing_description: str,
    proposed_description: str,
) -> str:
    """Merge a proposed structured description additively onto an existing one.

    This safeguard is intentionally narrow: it only auto-merges when both the
    existing row and the proposal already use the sectioned single-cell
    format. Legacy free-text rows still rely on the prompt to preserve and
    restructure the old meaning because there is no reliable local parser for
    arbitrary prose.
    """
    existing = parse_category_description(existing_description)
    proposed = parse_category_description(proposed_description)
    if (
        existing.get("format") != "sectioned_lists"
        or proposed.get("format") != "sectioned_lists"
    ):
        return str(proposed_description or "").strip()

    merged = {
        "format": "sectioned_lists",
        "raw": "",
        "brief": "",
        "topics": [],
        "keywords": [],
        "instructions": [],
        "additional_sections": [],
    }
    for key in ("topics", "keywords", "instructions"):
        _append_unique_items(merged[key], existing.get(key, []))
        _append_unique_items(merged[key], proposed.get(key, []))

    additional_sections: Dict[str, Dict[str, Any]] = {}
    for source in (existing, proposed):
        for section in source.get("additional_sections", []) or []:
            section_name = str(section.get("name", "") or "").strip()
            if not section_name:
                continue
            section_key = _normalise_category_description_header(section_name)
            if section_key not in additional_sections:
                additional_sections[section_key] = {"name": section_name, "items": []}
            _append_unique_items(additional_sections[section_key]["items"], section.get("items", []))
    merged["additional_sections"] = list(additional_sections.values())
    return render_sectioned_category_description(merged)


def _append_prompt_list(
    lines: List[str], *, parent_tag: str, child_tag: str, items: List[str]
) -> None:
    """Append a structured XML list to the category prompt block."""
    if not items:
        return
    lines.append(f"  <{parent_tag}>")
    for item in items:
        lines.append(f"    <{child_tag}>{_escape_for_prompt(item)}</{child_tag}>")
    lines.append(f"  </{parent_tag}>")


def format_categories_for_prompt(
    categories: List[Dict[str, Any]], section_filter: str = "ALL"
) -> str:
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
            f"  <name>{_escape_for_prompt(category['category_name'])}</name>",
            f"  <applies_to>{_escape_for_prompt(applies)}</applies_to>",
        ]
        parsed_description = parse_category_description(category.get("category_description", ""))
        lines.append(
            f"  <description_format>{parsed_description['format']}</description_format>"
        )
        if parsed_description["format"] == "sectioned_lists":
            if parsed_description["brief"]:
                lines.append(
                    "  "
                    f"<description_brief>{_escape_for_prompt(parsed_description['brief'])}"
                    "</description_brief>"
                )
            _append_prompt_list(
                lines,
                parent_tag="topics",
                child_tag="topic",
                items=parsed_description["topics"],
            )
            _append_prompt_list(
                lines,
                parent_tag="keywords",
                child_tag="keyword",
                items=parsed_description["keywords"],
            )
            _append_prompt_list(
                lines,
                parent_tag="instructions",
                child_tag="instruction",
                items=parsed_description["instructions"],
            )
            if parsed_description["additional_sections"]:
                lines.append("  <additional_sections>")
                for section in parsed_description["additional_sections"]:
                    lines.append(
                        "    "
                        f'<section name="{_escape_for_prompt(section["name"])}">'
                    )
                    for item in section["items"]:
                        lines.append(f"      <item>{_escape_for_prompt(item)}</item>")
                    lines.append("    </section>")
                lines.append("  </additional_sections>")
        else:
            lines.append(
                f"  <description>{_escape_for_prompt(parsed_description['raw'])}</description>"
            )

        examples = [
            category.get(f"example_{example_idx}", "").strip()
            for example_idx in (1, 2, 3)
            if category.get(f"example_{example_idx}", "").strip()
        ]
        if examples:
            lines.append("  <examples>")
            for example in examples:
                lines.append(f"    <example>{_escape_for_prompt(example)}</example>")
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
        "Use `importance_score` to rank evidence for the editor's recall-first workflow.\n"
        f"- Scores >= {threshold} should usually stay in the default report draft.\n"
        "- Scores below that can still remain as review candidates when the content is useful "
        "context but not strong enough to auto-keep in the draft.\n"
        "- Lower scores should be reserved for content that is mostly low-value, procedural, "
        "repetitive, or not worth surfacing by default.\n"
        "- Give higher scores when the quote is exact, specific, and likely worth keeping "
        "verbatim in the draft.\n"
        "- 0-1: ceremonial, legal boilerplate, operator instructions, or procedural remarks.\n"
        "- 2-3: low-signal detail that usually does not belong in the draft by default.\n"
        "- 4-5: meaningful context worth keeping visible for review, but not auto-kept.\n"
        "- 6-8: clearly report-worthy takeaway.\n"
        "- 9-10: headline-level or must-keep takeaway."
    )


def _bucket_score_scale_guidance() -> str:
    """Return prompt guidance for the 0-10 bucket relevance scale."""
    return (
        "Use bucket `score` on a 0-10 relevance scale.\n"
        "- 0 means the sentence does not fit the bucket.\n"
        "- 1-3 means weak or tangential overlap.\n"
        "- 4-5 means a plausible but not definitive fit.\n"
        "- 6-8 means a strong fit that can support assignment.\n"
        "- 9-10 means a direct, highly confident fit.\n"
        "- If a finding explicitly includes a business-authored `<keyword>` for a category "
        "(or a very close surface-form variant), that category should score at least 6 unless "
        "an `<instruction>` explicitly excludes it."
    )


def _category_sheet_guidance() -> str:
    """Explain how the category sheet should be interpreted during matching."""
    return (
        "Interpret the category sheet using overall meaning, not literal keyword overlap alone.\n"
        "- The main headings to expect are `<topics>`, `<keywords>`, and `<instructions>`.\n"
        "- `<topics>` define the main semantic scope of the category.\n"
        "- `<keywords>` are non-exhaustive cues and common phrases; exact matches are not "
        "required.\n"
        "- Include findings that are semantically similar to the listed topics or keywords even "
        "when the exact words in the sheet do not appear.\n"
        "- Do not treat the listed keywords as an exhaustive checklist; they indicate what must "
        "clearly fit, while closely related language and adjacent subtopics should also be "
        "included when the meaning matches.\n"
        "- `<instructions>` are category-specific inclusion, exclusion, and tie-break rules.\n"
        "- Any `<additional_sections>` (for example Notes or Overrides) are supplemental "
        "guidance, not separate buckets.\n"
        "- `<examples>` illustrate the intended fit when present.\n"
        "- If a row has only `<description>`, treat that legacy free-text description as the "
        "category brief."
    )


def _structured_description_template_guidance() -> str:
    """Return the preferred business-editable description template."""
    return (
        "Keep `category_description` as one multiline cell and use section headings.\n"
        "Preferred headings for anything new or rewritten:\n"
        "Topics:\n"
        "- short topic or subtopic\n"
        "Keywords:\n"
        "- short keyword or phrase\n"
        "Instructions:\n"
        "- short inclusion, exclusion, or tie-break rule\n\n"
        "Optional extra headings like `Notes:` or `Overrides:` are allowed when helpful. "
        "Keep the list items simple and business-friendly. Do not write a narrative "
        "description paragraph. Treat the headings like field lists inside one cell. "
        "`Topics` define the category's scope, `Keywords` are non-exhaustive hint fields for "
        "strong phrases that should clearly map in, and `Instructions` define boundaries and "
        "tie-breaks."
    )


def _generalized_category_name_guidance(example_names: Optional[List[str]] = None) -> str:
    """Return naming guidance for new taxonomy rows."""
    guidance = (
        "Treat `category_name` as a reusable taxonomy label, not a summary of this one "
        "transcript.\n"
        "- Use a generalized, bank-agnostic title that could plausibly apply to multiple "
        "banks next quarter.\n"
        "- Prefer a short noun-phrase title, typically 1-6 words, in the same style as the "
        "existing config sheet.\n"
        "- Keep company-specific wording, one-off facts, speaker phrasing, metrics, and "
        "timing context in `category_description`, `change_summary`, or examples — not in "
        "`category_name`.\n"
        "- Do not use sentence-like titles, transcript quotes, or bank-specific labels."
    )
    if not example_names:
        return guidance

    compact_examples = [name.strip() for name in example_names if name and name.strip()][:5]
    if not compact_examples:
        return guidance

    return guidance + "\nExample existing titles:\n" + "\n".join(
        f"- {name}" for name in compact_examples
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


def _summarise_sentence_statuses(records: List[Dict[str, Any]]) -> Dict[str, int]:
    """Count review statuses across sentence records."""
    summary = {"selected": 0, "candidate": 0, "rejected": 0, "errors": 0}
    for record in records:
        status = str(record.get("status", "rejected")).lower()
        if status in summary:
            summary[status] += 1
        else:
            summary["rejected"] += 1
        if record.get("classification_error"):
            summary["errors"] += 1
    return summary


def _summarise_md_results(processed_md: List[Dict[str, Any]]) -> Dict[str, int]:
    """Aggregate Management Discussion classification output for logging."""
    sentences = [sentence for block in processed_md for sentence in block.get("sentences", [])]
    summary = _summarise_sentence_statuses(sentences)
    return {
        "blocks": len(processed_md),
        "sentences": len(sentences),
        "errored_blocks": sum(1 for block in processed_md if block.get("classification_error")),
        **summary,
    }


def _summarise_qa_results(processed_qa: List[Dict[str, Any]]) -> Dict[str, int]:
    """Aggregate Q&A classification output for logging."""
    question_sentences = [
        sentence
        for conversation in processed_qa
        for sentence in conversation.get("question_sentences", [])
    ]
    answer_sentences = [
        sentence
        for conversation in processed_qa
        for sentence in conversation.get("answer_sentences", [])
    ]
    answer_summary = _summarise_sentence_statuses(answer_sentences)
    return {
        "conversations": len(processed_qa),
        "question_sentences": len(question_sentences),
        "answer_sentences": len(answer_sentences),
        "errored_conversations": sum(
            1 for conversation in processed_qa if conversation.get("classification_error")
        ),
        "selected": answer_summary["selected"],
        "candidate": answer_summary["candidate"],
        "rejected": answer_summary["rejected"],
        "errors": answer_summary["errors"],
    }


def _summarise_validation_errors(errors: List[str], limit: int = 2) -> str:
    """Collapse validation errors into a short log-friendly preview."""
    preview = "; ".join(errors[:limit])
    if len(errors) > limit:
        preview += f" (+{len(errors) - limit} more)"
    return preview


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

    choices = response.get("choices") or []
    message = choices[0].get("message", {}) if choices else {}
    finish_reason = choices[0].get("finish_reason", "") if choices else ""
    tool_calls = message.get("tool_calls") or []
    if not tool_calls:
        logger.warning(
            "LLM tool response missing structured output",
            stage=label,
            finish_reason=finish_reason or "unknown",
            choices_present=bool(choices),
        )
        return None

    arguments = tool_calls[0].get("function", {}).get("arguments", "{}")
    if isinstance(arguments, dict):
        return arguments

    try:
        return json.loads(arguments)
    except json.JSONDecodeError as exc:
        logger.warning(
            "LLM tool response could not be parsed",
            stage=label,
            error=str(exc),
            finish_reason=finish_reason or "unknown",
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


def _sorted_candidate_bucket_ids(
    scores: Dict[str, float],
    applicable_ids: List[str],
) -> List[str]:
    """Return scored applicable buckets sorted by descending model score."""
    ranked = [
        (bucket_id, float(scores.get(bucket_id, 0.0)))
        for bucket_id in applicable_ids
        if float(scores.get(bucket_id, 0.0)) > 0.0
    ]
    ranked.sort(key=lambda item: (-item[1], item[0]))
    return [bucket_id for bucket_id, _score in ranked]


def _primary_from_scores(
    scores: Dict[str, float],
    applicable_ids: List[str],
    min_bucket_score_for_assignment: float = 0.0,
) -> str:
    """Pick the highest-scoring applicable bucket only when it clears assignment quality."""
    candidate_ids = _sorted_candidate_bucket_ids(scores, applicable_ids)
    if not candidate_ids:
        return ""

    best_id = candidate_ids[0]
    if float(scores.get(best_id, 0.0)) < float(min_bucket_score_for_assignment):
        return ""
    return best_id


def _bucket_score(scores: Dict[str, float], bucket_id: str) -> float:
    """Return the recorded score for a bucket from sparse top-3 score output."""
    if not bucket_id:
        return 0.0
    return float(scores.get(bucket_id, 0.0))


def _normalise_scores(raw_scores: Any, categories: List[Dict[str, Any]]) -> Dict[str, float]:
    """Convert model output into sparse `bucket_N` score mapping.

    Models occasionally ignore the 0-10 scale guidance in the prompt and
    return 0-1 normalized values instead. When every score in the response
    falls in (0, 1], rescale to the documented 0-10 range. (See
    test_classify_qa_conversation_rescales_normalized_bucket_scores.)
    """
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
    if output and 0.0 < max(output.values()) <= 1.0:
        return {bucket_id: round(score * 10.0, 2) for bucket_id, score in output.items()}
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


def _empty_config_row() -> Dict[str, str]:
    """Return an empty config row payload."""
    return {
        "transcript_sections": "ALL",
        "report_section": "Results Summary",
        "category_name": "",
        "category_description": "",
        "example_1": "",
        "example_2": "",
        "example_3": "",
    }


def _initial_finding_status(
    *,
    importance_score: float,
    selected_bucket_id: str,
    candidate_bucket_ids: List[str],
    selected_importance_threshold: float,
    candidate_importance_threshold: float,
) -> str:
    """Assign the initial recall-first review status for one finding."""
    if selected_bucket_id and importance_score >= selected_importance_threshold:
        return "selected"
    if importance_score >= candidate_importance_threshold or candidate_bucket_ids:
        return "candidate"
    return "rejected"


def _make_finding_record(
    finding_id: str,
    finding_text: str,
    sentence_ids: List[str],
    llm_result: Optional[FindingResult],
    categories: List[Dict[str, Any]],
    applicable_ids: List[str],
    *,
    transcript_section: str,
    source_block_id: str,
    parent_record_id: str,
    selected_importance_threshold: float,
    candidate_importance_threshold: float,
    min_bucket_score_for_assignment: float,
) -> Dict[str, Any]:
    if llm_result is None:
        candidate_bucket_ids: List[str] = []
        selected_bucket_id = ""
        return {
            "sid": finding_id,
            "text": finding_text,
            "verbatim_text": finding_text,
            "sentence_ids": list(sentence_ids),
            "span_id": finding_id,
            "source_block_id": source_block_id,
            "parent_record_id": parent_record_id,
            "transcript_section": transcript_section,
            "primary": selected_bucket_id,
            "selected_bucket_id": selected_bucket_id,
            "candidate_bucket_ids": candidate_bucket_ids,
            "scores": {},
            "importance_score": 0.0,
            "status": "rejected",
            "emerging_topic": False,
            "classification_error": "missing_sentence_classification",
            "condensed": finding_text,
        }

    scores = _normalise_scores(llm_result.scores, categories)
    candidate_bucket_ids = _sorted_candidate_bucket_ids(scores, applicable_ids)
    selected_bucket_id = _primary_from_scores(
        scores,
        applicable_ids,
        min_bucket_score_for_assignment=min_bucket_score_for_assignment,
    )
    importance_score = round(float(llm_result.importance_score), 1)
    return {
        "sid": finding_id,
        "text": finding_text,
        "verbatim_text": finding_text,
        "sentence_ids": list(sentence_ids),
        "span_id": finding_id,
        "source_block_id": source_block_id,
        "parent_record_id": parent_record_id,
        "transcript_section": transcript_section,
        "primary": selected_bucket_id,
        "selected_bucket_id": selected_bucket_id,
        "candidate_bucket_ids": candidate_bucket_ids,
        "scores": scores,
        "importance_score": importance_score,
        "status": _initial_finding_status(
            importance_score=importance_score,
            selected_bucket_id=selected_bucket_id,
            candidate_bucket_ids=candidate_bucket_ids,
            selected_importance_threshold=selected_importance_threshold,
            candidate_importance_threshold=candidate_importance_threshold,
        ),
        "emerging_topic": not bool(selected_bucket_id),
        "condensed": llm_result.condensed or finding_text,
    }


def _make_context_finding_record(
    finding_id: str,
    finding_text: str,
    sentence_ids: List[str],
    *,
    transcript_section: str,
    source_block_id: str,
    parent_record_id: str,
) -> Dict[str, Any]:
    """Build a finding record marked as context-only (no bucket assignment).

    Used for analyst findings in QA conversations: they should appear in the
    transcript and provide context for the executive findings, but should
    never be classified into a bucket or selected as a report finding
    themselves. The dedicated ``status="context"`` value distinguishes
    deliberate omission from a parse failure (``status="rejected"``).
    """
    return {
        "sid": finding_id,
        "text": finding_text,
        "verbatim_text": finding_text,
        "sentence_ids": list(sentence_ids),
        "span_id": finding_id,
        "source_block_id": source_block_id,
        "parent_record_id": parent_record_id,
        "transcript_section": transcript_section,
        "primary": "",
        "selected_bucket_id": "",
        "candidate_bucket_ids": [],
        "scores": {},
        "importance_score": 0.0,
        "status": "context",
        "emerging_topic": False,
        "condensed": finding_text,
    }


def _looks_like_operator_turn(turn: Dict[str, Any]) -> bool:
    """Return whether a QA answer turn appears to be operator-only."""
    speaker = str(turn.get("speaker", "")).strip().lower()
    title = str(turn.get("speaker_title", "")).strip().lower()
    affiliation = str(turn.get("speaker_affiliation", "")).strip().lower()
    combined = " ".join(part for part in (speaker, title, affiliation) if part).strip()
    return (
        speaker in {"operator", "conference operator"}
        or title == "operator"
        or combined.startswith("operator")
        or "conference operator" in combined
    )


def _pick_primary_executive_turn(answer_turns: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Choose the best executive turn to represent a QA conversation."""
    for turn in answer_turns:
        if not _looks_like_operator_turn(turn):
            return turn
    return answer_turns[0] if answer_turns else None


def _seed_selected_report_sentences(
    processed_md: List[Dict[str, Any]],
    processed_qa: List[Dict[str, Any]],
) -> Dict[str, int]:
    """Promote mapped candidates when the initial draft would otherwise be blank."""

    report_sentences = [
        sentence
        for block in processed_md
        for sentence in block.get("sentences", [])
    ] + [
        sentence
        for conversation in processed_qa
        for sentence in conversation.get("answer_sentences", [])
    ]

    if any(sentence.get("status") == "selected" for sentence in report_sentences):
        return {"promoted": 0, "md": 0, "qa": 0}

    md_promoted = 0
    for block in processed_md:
        for sentence in block.get("sentences", []):
            if sentence.get("status") != "candidate":
                continue
            if not (sentence.get("selected_bucket_id") or sentence.get("primary")):
                continue
            sentence["status"] = "selected"
            md_promoted += 1

    qa_promoted = 0
    for conversation in processed_qa:
        for sentence in conversation.get("answer_sentences", []):
            if sentence.get("status") != "candidate":
                continue
            if not (sentence.get("selected_bucket_id") or sentence.get("primary")):
                continue
            sentence["status"] = "selected"
            qa_promoted += 1

    return {
        "promoted": md_promoted + qa_promoted,
        "md": md_promoted,
        "qa": qa_promoted,
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
    logger.info("Finding Q&A conversation boundaries", qa_speaker_blocks=len(qa_raw_blocks))

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
        "6. Ignore any numbers that appear inside speaker names, titles, affiliations, "
        "or preview text.\n"
        "7. Return the grouped indices with the provided tool.\n\n"
        "## Indexed Blocks\n"
        f"{_xml_block('qa_block_index', '\n\n'.join(block_lines))}"
    )
    block_by_index = dict(enumerate(qa_raw_blocks, start=1))
    block_id_to_index = {block["id"]: idx for idx, block in enumerate(qa_raw_blocks, start=1)}
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
        except Exception as exc:
            logger.warning(
                "Q&A boundary response could not be validated",
                error=str(exc),
                attempt=attempt + 1,
                max_attempts=max_attempts,
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


# ── Finding grouping ────────────────────────────────────────────────────────
#
# A "finding" is a contiguous ascending sequence of sentence indices within a
# single speaker block that together express one coherent idea. Grouping runs
# before classification so that downstream scoring and summarization work on
# meaningful units instead of spaCy-fragmented sentences. The spaCy splitter
# is left alone — its errors (e.g. splitting "ex. Capital Markets" on the
# abbreviation period) are absorbed by the grouping stage because the two
# halves land in the same finding.


def build_md_grouping_context(
    block_index: int,
    all_md_blocks: List[Dict[str, Any]],
    *,
    min_chars: int = 200,
    max_blocks_back: int = 3,
) -> str:
    """Build the prior-block context string for grouping one MD speaker block.

    Walks backward from ``block_index`` accumulating speaker+text until
    ``min_chars`` of context is reached or ``max_blocks_back`` blocks have
    been consumed. Returns an empty string for the first block in the section.
    """
    if block_index <= 0:
        return ""

    chunks: List[str] = []
    char_count = 0
    start = max(0, block_index - max_blocks_back)
    for prior_idx in range(block_index - 1, start - 1, -1):
        prior = all_md_blocks[prior_idx]
        speaker_line = prior.get("speaker", "Unknown Speaker")
        if prior.get("speaker_title"):
            speaker_line += f", {prior['speaker_title']}"
        if prior.get("speaker_affiliation"):
            speaker_line += f" ({prior['speaker_affiliation']})"
        body = " ".join(
            paragraph for paragraph in prior.get("paragraphs", []) if paragraph
        ).strip()
        if not body:
            continue
        chunk = f"{speaker_line}: {body}"
        chunks.append(chunk)
        char_count += len(chunk)
        if char_count >= min_chars:
            break

    if not chunks:
        return ""
    return "\n\n".join(reversed(chunks))


def build_qa_exchange_context(turns: List[Dict[str, Any]]) -> str:
    """Render a full Q&A exchange as unindexed context for grouping calls.

    Mirrors the in-situ exchange rendering used by
    :func:`classify_qa_conversation` but emits every sentence without AS/QS
    numbering — grouping calls only need the exchange flow for context, not
    per-sentence addressing.
    """
    lines: List[str] = []
    for turn in turns:
        role = turn.get("role", "a")
        if role == "q":
            label_parts = [turn.get("speaker") or "Analyst"]
            if turn.get("speaker_affiliation"):
                label_parts.append(f", {turn['speaker_affiliation']}")
            lines.append(f"ANALYST ({''.join(label_parts)}):")
        else:
            label_parts = [turn.get("speaker") or "Executive"]
            if turn.get("speaker_title"):
                label_parts.append(f", {turn['speaker_title']}")
            lines.append(f"EXECUTIVE ({''.join(label_parts)}):")
        for _, sentence in turn.get("_sentences_raw", []):
            lines.append(f'  "{sentence}"')
        lines.append("")
    return "\n".join(lines).rstrip()


def _is_valid_finding_group(
    indices: List[int],
    total_sentences: int,
    already_placed: set,
) -> bool:
    """Validate a single candidate finding group."""
    if not indices:
        return False
    if any(idx < 1 or idx > total_sentences for idx in indices):
        return False
    if any(idx in already_placed for idx in indices):
        return False
    # Contiguous ascending
    for prev, curr in zip(indices, indices[1:]):
        if curr != prev + 1:
            return False
    return True


def repair_finding_groups(
    llm_groups: List[FindingGroup],
    total_sentences: int,
) -> List[FindingGroup]:
    """Repair a possibly-invalid grouping into a guaranteed-valid covering.

    Walks LLM-provided groups in order of their minimum sentence index and
    accepts each one if it is contiguous, within ``[1, total_sentences]``, and
    does not overlap any already-accepted group. Rejected groups are
    discarded. Sentences not covered by any accepted group become singleton
    findings. The final list is sorted by minimum sentence index and
    collectively covers ``[1, total_sentences]`` exactly once.
    """
    accepted: List[FindingGroup] = []
    placed: set = set()

    ordered_groups = sorted(
        (group for group in llm_groups if group.sentence_indices),
        key=lambda group: min(group.sentence_indices),
    )

    for group in ordered_groups:
        indices = sorted(set(group.sentence_indices))
        if _is_valid_finding_group(indices, total_sentences, placed):
            accepted.append(FindingGroup(sentence_indices=indices))
            placed.update(indices)

    for idx in range(1, total_sentences + 1):
        if idx not in placed:
            accepted.append(FindingGroup(sentence_indices=[idx]))
            placed.add(idx)

    accepted.sort(key=lambda group: group.sentence_indices[0])
    return accepted


def _format_indexed_sentences(sentences: List[str]) -> str:
    """Render sentences as numbered S1, S2, ... lines for grouping prompts."""
    return "\n".join(
        f'  S{idx}: "{sentence}"' for idx, sentence in enumerate(sentences, start=1)
    )


_GROUPING_SYSTEM_PROMPT_MD = (
    "You split indexed sentences from an earnings call Management Discussion "
    "speaker block into findings. A finding is ONE coherent idea — one claim, "
    "one metric with its context, one forward-looking statement, one argument "
    "— covering exactly as many contiguous sentences as it takes to express "
    "that idea and no more. A finding may be one sentence or several; size "
    "follows the idea, not a target. Critically, closely related sentences on "
    "the same topic are NOT the same finding: two sentences both about NII, "
    "both about capital, or both about the macro backdrop are separate "
    "findings whenever each makes its own standalone claim. Downstream the "
    "user reassigns findings between categories and links them to emerging "
    "topics, so a finding that fuses distinct ideas (different metrics, "
    "different time horizons, different business lines, or independent claims "
    "that each stand on their own) blocks that workflow — split those. Every "
    "indexed sentence must belong to exactly one finding, findings are "
    "contiguous ascending sequences of indices, and findings are returned in "
    "sentence order. Always use the provided tool."
)


_GROUPING_SYSTEM_PROMPT_QA = (
    "You split indexed sentences from one Q&A speaker block (analyst or "
    "executive) into findings. A finding is ONE coherent idea — one question, "
    "one answer point, one claim, one metric with its context — covering "
    "exactly as many contiguous sentences as it takes to express that idea and "
    "no more. A finding may be one sentence or several; size follows the idea, "
    "not a target. Critically, closely related sentences on the same topic are "
    "NOT the same finding: two sentences both about NII, both about capital, "
    "or both about the macro backdrop are separate findings whenever each "
    "makes its own standalone claim. Downstream the user reassigns findings "
    "between categories and links them to emerging topics, so a finding that "
    "fuses distinct ideas (different metrics, different time horizons, "
    "different business lines, or independent claims that each stand on their "
    "own) blocks that workflow — split those. Every indexed sentence must "
    "belong to exactly one finding, findings are contiguous ascending "
    "sequences of indices, and findings are returned in sentence order. "
    "Always use the provided tool."
)


_GROUPING_RULES = (
    "## Rules\n"
    "1. Contiguous & complete — each finding is a contiguous ascending run of "
    "sentence indices (e.g. [1], [2,3], [4]); every indexed sentence appears in "
    "exactly one finding; findings are listed in sentence order starting at S1.\n"
    "2. One idea per finding. The sentences inside a finding must all be part "
    "of the same claim, argument, or metric-with-context. A finding can be one "
    "sentence or several — size follows the idea, not a target.\n"
    "3. Closely related does NOT mean the same finding. Two sentences on the "
    "same broad topic (e.g. both about NII, both about capital, both about the "
    "macro backdrop) are separate findings when each makes its own standalone "
    "claim. Same topic ≠ same idea.\n"
    "4. Split whenever the next sentence starts a distinct idea, even within "
    "the same paragraph. Distinct ideas include: different metrics (e.g. "
    "revenue vs. margin vs. ROE), different time horizons (current quarter vs. "
    "forward outlook), different business lines or segments, or independent "
    "claims that each stand on their own.\n"
    "5. Merge adjacent sentences only when they genuinely express the same "
    "idea. Typical merges: a setup sentence plus its punchline; a claim "
    "followed by the number that proves it; a statement followed by a "
    "subordinate clause, a pronoun reference, or a 'this reflects…' / 'that "
    "means…' elaboration of the exact same claim.\n"
    "6. A finding spanning an entire paragraph is only correct when the whole "
    "paragraph is making one coherent argument. If the paragraph moves through "
    "multiple metrics, topics, or standalone claims, split it into multiple "
    "findings.\n"
    "7. When a sentence could plausibly stand on its own and be reclassified "
    "separately, it should be its own finding. When in doubt, split."
)


async def _call_grouping_tool(
    *,
    messages: List[Dict[str, str]],
    tool: Dict[str, Any],
    label: str,
    total_sentences: int,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
    max_retries: int = 1,
) -> List[FindingGroup]:
    """Run a grouping tool call with one retry, falling back to repair."""
    last_llm_groups: List[FindingGroup] = []
    for attempt in range(max_retries + 1):
        raw = await _call_tool(
            messages=messages,
            tool=tool,
            label=f"{label}:attempt{attempt + 1}",
            context=context,
            llm_params=llm_params,
        )
        if raw:
            try:
                parsed = FindingGroupResult.model_validate(raw)
                last_llm_groups = parsed.findings
            except Exception as exc:
                logger.warning(
                    "Grouping tool response could not be parsed",
                    stage=label,
                    attempt=attempt + 1,
                    error=str(exc),
                )
                last_llm_groups = []
            else:
                placed: set = set()
                all_valid = True
                for group in last_llm_groups:
                    indices = sorted(set(group.sentence_indices))
                    if not _is_valid_finding_group(indices, total_sentences, placed):
                        all_valid = False
                        break
                    placed.update(indices)
                if all_valid and placed == set(range(1, total_sentences + 1)):
                    return [
                        FindingGroup(sentence_indices=sorted(set(group.sentence_indices)))
                        for group in last_llm_groups
                    ]
                logger.warning(
                    "Grouping response failed coverage/contiguity validation",
                    stage=label,
                    attempt=attempt + 1,
                    total_sentences=total_sentences,
                    placed=sorted(placed),
                )

    logger.warning(
        "Grouping tool exhausted retries; applying repair fallback",
        stage=label,
        total_sentences=total_sentences,
        llm_groups=len(last_llm_groups),
    )
    return repair_finding_groups(last_llm_groups, total_sentences)


async def group_md_block_findings(
    *,
    block_id: str,
    speaker_line: str,
    sentences: List[str],
    prior_context: str,
    categories_text_md: str,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
    max_retries: int = 1,
) -> List[FindingGroup]:
    """Group sentences from one MD speaker block into findings."""
    if not sentences:
        return []

    indexed = _format_indexed_sentences(sentences)
    prior_block = prior_context or "[This is the first speaker block in the section — no prior context.]"
    user_prompt = (
        "## Task\n"
        "Split the indexed sentences in the current Management Discussion speaker "
        "block into findings, where each finding is one coherent idea. A finding "
        "may be one sentence or several, but split whenever the next sentence "
        "starts a distinct idea — closely related sentences on the same topic are "
        "NOT the same finding if each makes its own standalone claim.\n\n"
        "## Prior Context\n"
        f"{_xml_block('prior_speaker_context', prior_block)}\n\n"
        f"## Current Block — {speaker_line}\n"
        f"{_xml_block('current_block', indexed)}\n\n"
        f"{_GROUPING_RULES}"
    )

    return await _call_grouping_tool(
        messages=[
            {"role": "developer", "content": _GROUPING_SYSTEM_PROMPT_MD},
            {"role": "user", "content": user_prompt},
        ],
        tool=TOOL_MD_GROUPING,
        label=f"md_group:{block_id}",
        total_sentences=len(sentences),
        context=context,
        llm_params=llm_params,
        max_retries=max_retries,
    )


async def group_qa_block_findings(
    *,
    conversation_id: str,
    block_id: str,
    speaker_role: str,
    speaker_line: str,
    sentences: List[str],
    exchange_context: str,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
    max_retries: int = 1,
) -> List[FindingGroup]:
    """Group sentences from one QA speaker block into findings."""
    if not sentences:
        return []

    role_label = "ANALYST" if speaker_role == "q" else "EXECUTIVE"
    indexed = _format_indexed_sentences(sentences)
    user_prompt = (
        "## Task\n"
        "Split the indexed sentences in the current Q&A speaker block into "
        "findings, where each finding is one coherent idea. A finding may be one "
        "sentence or several, but split whenever the next sentence starts a "
        "distinct idea — closely related sentences on the same topic are NOT "
        "the same finding if each makes its own standalone claim.\n\n"
        "## Full Exchange Context\n"
        f"{_xml_block('qa_exchange', exchange_context or '[No exchange context available.]')}\n\n"
        f"## Current Block — {role_label} ({speaker_line})\n"
        f"{_xml_block('current_block', indexed)}\n\n"
        f"{_GROUPING_RULES}"
    )

    return await _call_grouping_tool(
        messages=[
            {"role": "developer", "content": _GROUPING_SYSTEM_PROMPT_QA},
            {"role": "user", "content": user_prompt},
        ],
        tool=TOOL_QA_GROUPING,
        label=f"qa_group:{conversation_id}:{block_id}",
        total_sentences=len(sentences),
        context=context,
        llm_params=llm_params,
        max_retries=max_retries,
    )


async def classify_md_block(  # pylint: disable=unused-argument
    *,
    block_raw: Dict[str, Any],
    block_index: int,
    all_md_blocks: List[Dict[str, Any]],
    categories: List[Dict[str, Any]],
    categories_text_md: str,
    company_name: str,
    fiscal_year: int,
    fiscal_quarter: str,
    report_inclusion_threshold: float,
    selected_importance_threshold: float,
    candidate_importance_threshold: float,
    min_bucket_score_for_assignment: float,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> Dict[str, Any]:
    """Classify one MD speaker block at finding grain.

    Groups the block's sentences into findings via an LLM grouping call, then
    makes one classification call per block (rather than per paragraph).

    `company_name`, `fiscal_year`, and `fiscal_quarter` are currently unused but
    reserved for prompt enrichment (banks + periods in the system message).
    """
    block_id = block_raw["id"]
    paragraphs = block_raw["paragraphs"]
    applicable_ids = applicable_bucket_ids(categories, "MD")

    speaker_line = block_raw["speaker"]
    if block_raw.get("speaker_title"):
        speaker_line += f", {block_raw['speaker_title']}"
    if block_raw.get("speaker_affiliation"):
        speaker_line += f" ({block_raw['speaker_affiliation']})"

    all_para_sentences: List[List[str]] = [split_sentences(paragraph) for paragraph in paragraphs]
    flat_sentences: List[str] = []
    sentence_para_idx: List[int] = []
    for para_idx, para_sentences in enumerate(all_para_sentences):
        for sentence in para_sentences:
            flat_sentences.append(sentence)
            sentence_para_idx.append(para_idx)

    if not flat_sentences:
        return {
            "id": block_id,
            "speaker": block_raw["speaker"],
            "speaker_title": block_raw.get("speaker_title", ""),
            "speaker_affiliation": block_raw.get("speaker_affiliation", ""),
            "sentences": [],
        }

    prior_context = build_md_grouping_context(block_index, all_md_blocks)
    finding_groups = await group_md_block_findings(
        block_id=block_id,
        speaker_line=speaker_line,
        sentences=flat_sentences,
        prior_context=prior_context,
        categories_text_md=categories_text_md,
        context=context,
        llm_params=llm_params,
    )

    finding_texts: List[str] = []
    finding_sentence_ids: List[List[str]] = []
    finding_para_idx: List[int] = []
    sentence_ids_by_global_idx = [
        f"{block_id}_s{global_idx}" for global_idx in range(len(flat_sentences))
    ]
    for group in finding_groups:
        indices = [idx - 1 for idx in group.sentence_indices]
        finding_texts.append(" ".join(flat_sentences[i] for i in indices))
        finding_sentence_ids.append([sentence_ids_by_global_idx[i] for i in indices])
        finding_para_idx.append(sentence_para_idx[indices[0]] if indices else 0)

    indexed_findings_lines = [
        f'  F{idx}: "{text}"' for idx, text in enumerate(finding_texts, start=1)
    ]
    context_lines = [f"SPEAKER: {speaker_line}", ""]
    if prior_context:
        context_lines.append("[Prior speaker context]")
        context_lines.append(prior_context)
        context_lines.append("")
    context_lines.append("[CLASSIFY THESE FINDINGS:]")
    context_lines.extend(indexed_findings_lines)

    system_prompt = (
        "You are a finding classifier for earnings call Management Discussion sections. "
        "Assign each indexed finding to the best report buckets and score its "
        "investor-relations importance using the category sheet and speaker context. "
        "Always use the provided tool."
    )
    user_prompt = (
        "## Task\n"
        f"Classify the indexed findings from Management Discussion speaker block {block_id}.\n\n"
        "## Decision Criteria\n"
        "Choose bucket scores from the category sheet, then score how quotable each finding "
        "is for an investor-relations summary.\n"
        f"{_category_sheet_guidance()}\n"
        f"{_bucket_score_scale_guidance()}\n"
        f"{_importance_scale_guidance(report_inclusion_threshold)}\n\n"
        "## Rules\n"
        "1. Return one result for every F-numbered finding in the current block.\n"
        "2. Use up to the top 3 bucket-score pairs for each finding, "
        "ordered by score descending.\n"
        "3. Score importance from 0 to 10 using the inclusion guidance above.\n"
        "4. Make `condensed` a compact summary of the full finding "
        "— remove filler, keep every fact.\n"
        "5. Keep the `index` aligned to the F-number shown in the current block.\n\n"
        "## Categories\n"
        f"{_xml_block('categories', categories_text_md)}\n\n"
        "## Block Context\n"
        f"{_xml_block('md_block_context', '\n'.join(context_lines))}"
    )
    raw = await _call_tool(
        messages=[
            {"role": "developer", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        tool=TOOL_MD_BLOCK,
        label=f"md_block:{block_id}",
        context=context,
        llm_params=llm_params,
    )

    llm_results_by_idx: Dict[int, FindingResult] = {}
    parse_error_count = 0
    last_parse_error = ""
    if raw and "findings" in raw:
        for finding_raw in raw["findings"]:
            try:
                result = FindingResult.model_validate(finding_raw)
            except Exception:
                try:
                    result = FindingResult(
                        index=finding_raw.get("index"),
                        scores=finding_raw.get("scores", []),
                        importance_score=finding_raw.get("importance_score", 3.0),
                        condensed=finding_raw.get("condensed", ""),
                    )
                except Exception as exc:
                    parse_error_count += 1
                    last_parse_error = str(exc)
                    continue
            llm_results_by_idx[result.index] = result

    finding_records: List[Dict[str, Any]] = []
    for finding_idx, (text, sentence_ids, para_idx) in enumerate(
        zip(finding_texts, finding_sentence_ids, finding_para_idx), start=1
    ):
        finding_id = f"{block_id}_f{finding_idx - 1}"
        record = _make_finding_record(
            finding_id,
            text,
            sentence_ids,
            llm_results_by_idx.get(finding_idx),
            categories,
            applicable_ids,
            transcript_section="MD",
            source_block_id=block_id,
            parent_record_id=block_id,
            selected_importance_threshold=selected_importance_threshold,
            candidate_importance_threshold=candidate_importance_threshold,
            min_bucket_score_for_assignment=min_bucket_score_for_assignment,
        )
        record["para_idx"] = para_idx
        finding_records.append(record)

    if parse_error_count:
        logger.warning(
            "Management discussion block had parse fallbacks",
            block_id=block_id,
            speaker=block_raw["speaker"],
            parse_errors=parse_error_count,
            last_error=last_parse_error,
        )

    return {
        "id": block_id,
        "speaker": block_raw["speaker"],
        "speaker_title": block_raw.get("speaker_title", ""),
        "speaker_affiliation": block_raw.get("speaker_affiliation", ""),
        "sentences": finding_records,
    }


async def classify_qa_conversation(  # pylint: disable=unused-argument
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
    selected_importance_threshold: float,
    candidate_importance_threshold: float,
    min_bucket_score_for_assignment: float,
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> Dict[str, Any]:
    """Classify one QA exchange at finding grain.

    Groups each block's sentences into findings via parallel LLM grouping
    calls, then makes one classification call per exchange. Analyst findings
    are kept as context; executive findings receive bucket scores.

    `company_name`, `fiscal_year`, and `fiscal_quarter` are currently unused but
    reserved for prompt enrichment — keep them in the public signature.
    """
    conv_id = f"{ticker}_QA_{conv_idx}"
    applicable_ids = applicable_bucket_ids(categories, "QA")

    # Build per-turn role assignments while preserving the original speaker
    # block order. Previously the function partitioned blocks by role, which
    # destroyed the back-and-forth interleaving (greeting Q, A, follow-up Q,
    # A, thanks Q) that the transcript view needs to reconstruct.
    has_explicit_question = any(
        block.get("speaker_type_hint") == "q" for block in conv_blocks
    )

    def _role_for_block(block: Dict[str, Any], position: int) -> str:
        hint = block.get("speaker_type_hint")
        if hint == "q":
            return "q"
        if hint:
            return "a"
        # No hint at all: fall back to "first block in conversation is the
        # analyst" so we always have a question turn to summarize.
        if not has_explicit_question and position == 0:
            return "q"
        return "a"

    turns: List[Dict[str, Any]] = []
    for position, block in enumerate(conv_blocks):
        role = _role_for_block(block, position)
        turn_sentences: List[Tuple[int, str]] = []
        for paragraph_idx, paragraph in enumerate(block["paragraphs"]):
            for sentence in split_sentences(paragraph):
                turn_sentences.append((paragraph_idx, sentence))
        turns.append(
            {
                "turn_idx": len(turns),
                "role": role,
                "speaker": block.get("speaker", ""),
                "speaker_title": block.get("speaker_title", ""),
                "speaker_affiliation": block.get("speaker_affiliation", ""),
                "block_id": block.get("id", conv_id),
                "_sentences_raw": turn_sentences,  # populated with records below
            }
        )

    question_turns = [turn for turn in turns if turn["role"] == "q"]
    answer_turns = [turn for turn in turns if turn["role"] == "a"]
    executive_turn = _pick_primary_executive_turn(answer_turns)

    analyst_name = question_turns[0]["speaker"] if question_turns else "Analyst"
    analyst_affiliation = question_turns[0]["speaker_affiliation"] if question_turns else ""
    executive_name = executive_turn["speaker"] if executive_turn else "Executive"
    executive_title = executive_turn["speaker_title"] if executive_turn else ""
    executive_affiliation = (
        executive_turn["speaker_affiliation"] if executive_turn else ""
    )

    # Assign per-role sentence ids and flat sentence text lists on each turn
    # so grouping + record construction can operate on uniform turn-scoped
    # data structures. Sentence ids preserve the legacy `_qs{idx}` / `_as{idx}`
    # naming so transcript highlighting downstream keeps working.
    global_q_idx = 0
    for turn in question_turns:
        turn["_sentence_ids"] = []
        turn["_sentence_texts"] = []
        turn["_sentence_para_idx"] = []
        for para_idx, sentence in turn["_sentences_raw"]:
            turn["_sentence_ids"].append(f"{conv_id}_qs{global_q_idx}")
            turn["_sentence_texts"].append(sentence)
            turn["_sentence_para_idx"].append(para_idx)
            global_q_idx += 1

    global_a_idx = 0
    for turn in answer_turns:
        turn["_sentence_ids"] = []
        turn["_sentence_texts"] = []
        turn["_sentence_para_idx"] = []
        for para_idx, sentence in turn["_sentences_raw"]:
            turn["_sentence_ids"].append(f"{conv_id}_as{global_a_idx}")
            turn["_sentence_texts"].append(sentence)
            turn["_sentence_para_idx"].append(para_idx)
            global_a_idx += 1

    # Offset paragraph indices so every turn's paragraphs have distinct
    # ``para_idx`` values for the JS para-break renderer (preserves the
    # original indexing scheme: question turns and answer turns advance
    # independently).
    q_para_offset = 0
    for turn in question_turns:
        turn["_sentence_global_para_idx"] = [
            para_idx + q_para_offset for para_idx in turn["_sentence_para_idx"]
        ]
        q_para_offset += len({p for p in turn["_sentence_para_idx"]}) or 1

    a_para_offset = 0
    for turn in answer_turns:
        turn["_sentence_global_para_idx"] = [
            para_idx + a_para_offset for para_idx in turn["_sentence_para_idx"]
        ]
        a_para_offset += len({p for p in turn["_sentence_para_idx"]}) or 1

    # Group each turn's sentences into findings in parallel. Exchange context
    # (unindexed rendering of the whole back-and-forth) is shared across all
    # grouping calls.
    exchange_context_for_grouping = build_qa_exchange_context(turns)

    async def _group_turn(turn: Dict[str, Any]) -> List[FindingGroup]:
        if not turn["_sentence_texts"]:
            return []
        speaker_line = turn["speaker"] or (
            "Analyst" if turn["role"] == "q" else "Executive"
        )
        return await group_qa_block_findings(
            conversation_id=conv_id,
            block_id=turn["block_id"],
            speaker_role=turn["role"],
            speaker_line=speaker_line,
            sentences=turn["_sentence_texts"],
            exchange_context=exchange_context_for_grouping,
            context=context,
            llm_params=llm_params,
        )

    grouping_results = await asyncio.gather(
        *[_group_turn(turn) for turn in turns]
    )
    for turn, groups in zip(turns, grouping_results):
        turn["_finding_groups"] = groups

    # Materialise finding-level aggregates on each turn.
    for turn in turns:
        finding_texts: List[str] = []
        finding_sentence_ids: List[List[str]] = []
        finding_para_idx: List[int] = []
        for group in turn["_finding_groups"]:
            indices = [i - 1 for i in group.sentence_indices]
            finding_texts.append(" ".join(turn["_sentence_texts"][i] for i in indices))
            finding_sentence_ids.append([turn["_sentence_ids"][i] for i in indices])
            finding_para_idx.append(
                turn["_sentence_global_para_idx"][indices[0]] if indices else 0
            )
        turn["_finding_texts"] = finding_texts
        turn["_finding_sentence_ids"] = finding_sentence_ids
        turn["_finding_para_idx"] = finding_para_idx

    # Render the classification prompt with the full back-and-forth, but only
    # AF-index executive findings (analyst findings are shown unindexed as
    # context). AF numbering spans all executive turns in exchange order.
    exchange_lines: List[str] = []
    answer_af_index = 0
    for turn in turns:
        if turn["role"] == "q":
            label_parts = [turn["speaker"] or "Analyst"]
            if turn["speaker_affiliation"]:
                label_parts.append(f", {turn['speaker_affiliation']}")
            exchange_lines.append(f"ANALYST ({''.join(label_parts)}):")
            for text in turn["_finding_texts"]:
                exchange_lines.append(f'  "{text}"')
            exchange_lines.append("")
        else:
            label_parts = [turn["speaker"] or "Executive"]
            if turn["speaker_title"]:
                label_parts.append(f", {turn['speaker_title']}")
            exchange_lines.append(f"EXECUTIVE ({''.join(label_parts)}):")
            for text in turn["_finding_texts"]:
                answer_af_index += 1
                exchange_lines.append(f'  AF{answer_af_index}: "{text}"')
            exchange_lines.append("")
    exchange_text = "\n".join(exchange_lines).rstrip()

    system_prompt = (
        "You are a finding classifier for earnings call Q&A exchanges. "
        "For each exchange, paraphrase the analyst's question, choose a single best-fit bucket "
        "for the overall topic, and classify only the executive answer findings. Treat anything "
        "the analyst says as context only and never assign a bucket to it. Always use the "
        "provided tool."
    )
    user_prompt = (
        "## Task\n"
        "Summarise the analyst's question, choose a primary bucket for the overall exchange, "
        "and classify only the executive answer findings (AF).\n\n"
        "## Decision Criteria\n"
        "Use the overall exchange topic for `primary_bucket_index`, then score each executive "
        "answer finding from the category sheet. Analyst findings are shown for context only "
        "\u2014 do not return any QF results.\n"
        f"{_category_sheet_guidance()}\n"
        f"{_bucket_score_scale_guidance()}\n"
        f"{_importance_scale_guidance(report_inclusion_threshold)}\n\n"
        "## Rules\n"
        "1. Set `primary_bucket_index` to the single best existing bucket for the full exchange.\n"
        "2. Return `analyst_question_summary` as a single clause of \u226425 words paraphrasing "
        "what the analyst is asking about. Avoid filler like greetings or thank-yous.\n"
        "3. Return one result for every `AF` finding shown below \u2014 and only AF findings.\n"
        "4. Use up to the top 3 bucket-score pairs for each AF finding, "
        "ordered by score descending.\n"
        "5. Score importance from 0 to 10 using the inclusion guidance above.\n"
        "6. Make `condensed` a compact summary of the full finding "
        "\u2014 remove filler, keep every fact.\n"
        "7. Keep each `index` aligned to the numbered `AF` finding.\n\n"
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

    question_source_block_id = (
        question_turns[0]["block_id"] if question_turns else conv_id
    )
    answer_source_block_id = (
        answer_turns[0]["block_id"] if answer_turns else conv_id
    )

    # Analyst findings are always context-only: no LLM scoring, no bucket
    # assignment, not eligible for the report.
    question_records: List[Dict[str, Any]] = []
    q_finding_counter = 0
    for turn in question_turns:
        turn_records: List[Dict[str, Any]] = []
        for text, sids, para_idx in zip(
            turn["_finding_texts"],
            turn["_finding_sentence_ids"],
            turn["_finding_para_idx"],
        ):
            finding_id = f"{conv_id}_qf{q_finding_counter}"
            record = _make_context_finding_record(
                finding_id,
                text,
                sids,
                transcript_section="QA",
                source_block_id=question_source_block_id,
                parent_record_id=conv_id,
            )
            record["para_idx"] = para_idx
            question_records.append(record)
            turn_records.append(record)
            q_finding_counter += 1
        turn["_finding_records"] = turn_records

    primary_bucket = ""
    analyst_question_summary = ""
    answer_by_idx: Dict[int, FindingResult] = {}
    if raw:
        try:
            result = QAExchangeClassification.model_validate(raw)
            if 0 <= result.primary_bucket_index < len(categories):
                primary_bucket = f"bucket_{result.primary_bucket_index}"
            analyst_question_summary = (result.analyst_question_summary or "").strip()
            answer_by_idx = {finding.index: finding for finding in result.answer_findings}
        except Exception as exc:
            logger.warning(
                "Q&A conversation response could not be parsed",
                conversation=conv_id,
                error=str(exc),
            )

    answer_records: List[Dict[str, Any]] = []
    a_finding_counter = 0
    for turn in answer_turns:
        turn_records = []
        for text, sids, para_idx in zip(
            turn["_finding_texts"],
            turn["_finding_sentence_ids"],
            turn["_finding_para_idx"],
        ):
            finding_id = f"{conv_id}_af{a_finding_counter}"
            af_idx = a_finding_counter + 1
            record = _make_finding_record(
                finding_id,
                text,
                sids,
                answer_by_idx.get(af_idx),
                categories,
                applicable_ids,
                transcript_section="QA",
                source_block_id=answer_source_block_id,
                parent_record_id=conv_id,
                selected_importance_threshold=selected_importance_threshold,
                candidate_importance_threshold=candidate_importance_threshold,
                min_bucket_score_for_assignment=min_bucket_score_for_assignment,
            )
            record["para_idx"] = para_idx
            record["speaker"] = turn.get("speaker", "")
            record["speaker_title"] = turn.get("speaker_title", "")
            record["speaker_affiliation"] = turn.get("speaker_affiliation", "")
            answer_records.append(record)
            turn_records.append(record)
            a_finding_counter += 1
        turn["_finding_records"] = turn_records

    # Honor the LLM's whole-exchange primary bucket only if (a) it is in the
    # set of buckets applicable to QA, and (b) at least one answer sentence
    # also classifies into it. Otherwise fall back to a vote across the
    # answer-sentence buckets. (Previously this was two chained `if` blocks
    # where the second one always ran whenever the first nullified the
    # primary, making the chain hard to reason about.)
    supported_answer_bucket_ids = {
        sentence.get("selected_bucket_id") or sentence.get("primary", "")
        for sentence in answer_records
        if sentence.get("selected_bucket_id") or sentence.get("primary", "")
    }
    primary_supported = (
        primary_bucket
        and primary_bucket in applicable_ids
        and primary_bucket in supported_answer_bucket_ids
    )

    if not primary_supported:
        answer_bucket_totals: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {"count": 0.0, "importance": 0.0, "score": 0.0}
        )
        for sentence in answer_records:
            bucket_id = sentence.get("selected_bucket_id") or sentence.get("primary", "")
            if not bucket_id:
                continue
            answer_bucket_totals[bucket_id]["count"] += 1.0
            answer_bucket_totals[bucket_id]["importance"] += float(
                sentence.get("importance_score", 0.0)
            )
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
        else:
            primary_bucket = ""
            logger.warning(
                "Q&A conversation could not resolve a primary bucket",
                conversation=conv_id,
                reason="no answer records available to derive primary bucket",
            )

    # Stitch records back into the original turn order so the transcript
    # popout can render the real back-and-forth (greeting Q -> A -> follow-up
    # Q -> A -> thanks). The flat ``question_sentences``/``answer_sentences``
    # lists are also kept for callers/UI code that consume the per-role view
    # (report panel, included-sids set, etc.).
    for turn in turns:
        turn["sentences"] = turn.get("_finding_records", [])
        for scratch_key in (
            "_sentences_raw",
            "_sentence_ids",
            "_sentence_texts",
            "_sentence_para_idx",
            "_sentence_global_para_idx",
            "_finding_groups",
            "_finding_texts",
            "_finding_sentence_ids",
            "_finding_para_idx",
            "_finding_records",
        ):
            turn.pop(scratch_key, None)

    return {
        "id": conv_id,
        "primary_bucket": primary_bucket,
        "analyst_name": analyst_name,
        "analyst_affiliation": analyst_affiliation,
        "executive_name": executive_name,
        "executive_title": executive_title,
        "executive_affiliation": executive_affiliation,
        "analyst_question_summary": analyst_question_summary,
        "turns": turns,
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
    selected_importance_threshold: float,
    candidate_importance_threshold: float,
    min_bucket_score_for_assignment: float,
    max_concurrent_md_blocks: int = 1,
) -> Dict[str, Any]:
    """Convert one bank's raw XML transcript blocks into mock-style bank state."""
    ticker = bank_info.get("full_ticker") or bank_info["bank_symbol"]
    company_name = bank_info["bank_name"]

    logger.info(
        "Starting transcript classification",
        ticker=ticker,
        md_blocks=len(md_raw_blocks),
        qa_speaker_blocks=len(qa_raw_blocks),
        categories=len(categories),
        max_concurrent_md_blocks=max(1, max_concurrent_md_blocks),
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
    logger.info(
        "Classifying Management Discussion blocks",
        ticker=ticker,
        blocks=len(md_raw_blocks),
        max_concurrent_md_blocks=max(1, max_concurrent_md_blocks),
    )

    async def _process_md_block(block_index: int, block: Dict[str, Any]) -> Dict[str, Any]:
        async with semaphore:
            try:
                return await classify_md_block(
                    block_raw=block,
                    block_index=block_index,
                    all_md_blocks=md_raw_blocks,
                    categories=categories,
                    categories_text_md=categories_text_md,
                    company_name=company_name,
                    fiscal_year=fiscal_year,
                    fiscal_quarter=fiscal_quarter,
                    report_inclusion_threshold=report_inclusion_threshold,
                    selected_importance_threshold=selected_importance_threshold,
                    candidate_importance_threshold=candidate_importance_threshold,
                    min_bucket_score_for_assignment=min_bucket_score_for_assignment,
                    context=context,
                    llm_params=md_llm_params,
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(
                    "Management Discussion block classification failed",
                    ticker=ticker,
                    block_index=block_index + 1,
                    block_id=block["id"],
                    error=str(exc),
                    exc_info=True,
                )
                return {
                    "id": block["id"],
                    "speaker": block.get("speaker", ""),
                    "speaker_title": block.get("speaker_title", ""),
                    "speaker_affiliation": block.get("speaker_affiliation", ""),
                    "sentences": [],
                    "classification_error": str(exc),
                }

    md_results = await asyncio.gather(
        *[_process_md_block(idx, block) for idx, block in enumerate(md_raw_blocks)],
        return_exceptions=True,
    )

    processed_md: List[Dict[str, Any]] = []
    for idx, result in enumerate(md_results, start=1):
        if isinstance(result, BaseException):
            block = md_raw_blocks[idx - 1]
            logger.error(
                "Management Discussion block raised an unhandled exception",
                ticker=ticker,
                block_index=idx,
                block_id=block.get("id", ""),
                error=str(result),
            )
            processed_md.append(
                {
                    "id": block.get("id", f"{ticker}_MD_{idx}"),
                    "speaker": block.get("speaker", ""),
                    "speaker_title": block.get("speaker_title", ""),
                    "speaker_affiliation": block.get("speaker_affiliation", ""),
                    "sentences": [],
                    "classification_error": str(result),
                }
            )
        else:
            processed_md.append(result)

    md_summary = _summarise_md_results(processed_md)
    logger.info(
        "Management Discussion classification complete",
        ticker=ticker,
        blocks=md_summary["blocks"],
        sentences=md_summary["sentences"],
        selected=md_summary["selected"],
        candidate=md_summary["candidate"],
        rejected=md_summary["rejected"],
        errored_blocks=md_summary["errored_blocks"],
        sentence_errors=md_summary["errors"],
    )

    logger.info(
        "Classifying Q&A conversations",
        ticker=ticker,
        conversations=len(qa_conversations_raw),
    )

    async def _process_qa_conversation(
        conv_idx: int, conversation: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        async with semaphore:
            try:
                return await classify_qa_conversation(
                    conv_idx=conv_idx,
                    conv_blocks=conversation,
                    ticker=ticker,
                    categories=categories,
                    categories_text_qa=categories_text_qa,
                    company_name=company_name,
                    fiscal_year=fiscal_year,
                    fiscal_quarter=fiscal_quarter,
                    report_inclusion_threshold=report_inclusion_threshold,
                    selected_importance_threshold=selected_importance_threshold,
                    candidate_importance_threshold=candidate_importance_threshold,
                    min_bucket_score_for_assignment=min_bucket_score_for_assignment,
                    context=context,
                    llm_params=qa_llm_params,
                )
            except Exception as exc:  # pylint: disable=broad-except
                logger.error(
                    "Q&A conversation classification failed",
                    ticker=ticker,
                    conversation_index=conv_idx,
                    error=str(exc),
                    exc_info=True,
                )
                return {
                    "id": f"{ticker}_QA_{conv_idx}",
                    "primary_bucket": "",
                    "analyst_name": "Analyst",
                    "analyst_affiliation": "",
                    "executive_name": "Executive",
                    "executive_title": "",
                    "question_sentences": [],
                    "answer_sentences": [],
                    "classification_error": str(exc),
                }

    qa_results = await asyncio.gather(
        *[
            _process_qa_conversation(idx, conversation)
            for idx, conversation in enumerate(qa_conversations_raw, start=1)
        ],
        return_exceptions=True,
    )

    processed_qa: List[Dict[str, Any]] = []
    for idx, result in enumerate(qa_results, start=1):
        if isinstance(result, BaseException):
            logger.error(
                "Q&A conversation raised an unhandled exception",
                ticker=ticker,
                conversation_index=idx,
                error=str(result),
            )
            processed_qa.append(
                {
                    "id": f"{ticker}_QA_{idx}",
                    "primary_bucket": "",
                    "analyst_name": "Analyst",
                    "analyst_affiliation": "",
                    "executive_name": "Executive",
                    "executive_title": "",
                    "question_sentences": [],
                    "answer_sentences": [],
                    "classification_error": str(result),
                }
            )
        else:
            processed_qa.append(result)

    qa_summary = _summarise_qa_results(processed_qa)
    seed_summary = _seed_selected_report_sentences(processed_md, processed_qa)
    if seed_summary["promoted"]:
        logger.info(
            "Seeded initial report draft from mapped candidates",
            ticker=ticker,
            promoted_sentences=seed_summary["promoted"],
            md_promoted=seed_summary["md"],
            qa_promoted=seed_summary["qa"],
        )
        qa_summary = _summarise_qa_results(processed_qa)
    logger.info(
        "Q&A classification complete",
        ticker=ticker,
        conversations=qa_summary["conversations"],
        question_sentences=qa_summary["question_sentences"],
        answer_sentences=qa_summary["answer_sentences"],
        selected=qa_summary["selected"],
        candidate=qa_summary["candidate"],
        rejected=qa_summary["rejected"],
        errored_conversations=qa_summary["errored_conversations"],
        sentence_errors=qa_summary["errors"],
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


def _collect_config_review_evidence(
    bank_data: Dict[str, Any],
) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Collect canonical evidence rows used by the config-review passes."""

    evidence_rows: List[Dict[str, Any]] = []
    evidence_index: Dict[str, Dict[str, Any]] = {}
    seen_ids = set()

    for block in bank_data.get("md_blocks", []):
        speaker_line = block.get("speaker", "Unknown Speaker")
        if block.get("speaker_title"):
            speaker_line += f", {block['speaker_title']}"

        for sentence in block.get("sentences", []):
            evidence_id = sentence.get("sid")
            if not evidence_id or evidence_id in seen_ids:
                continue
            seen_ids.add(evidence_id)
            row = {
                "evidence_id": evidence_id,
                "status": sentence.get("status", "candidate"),
                "speaker": speaker_line,
                "transcript_section": "MD",
                "source_block_id": sentence.get("source_block_id") or block.get("id", ""),
                "parent_record_id": sentence.get("parent_record_id") or block.get("id", ""),
                "selected_bucket_id": sentence.get("selected_bucket_id", ""),
                "candidate_bucket_ids": sentence.get("candidate_bucket_ids", []),
                "importance_score": float(sentence.get("importance_score", 0.0)),
                "quote": sentence.get("verbatim_text") or sentence.get("text", ""),
                "question_context": "",
                "emerging_topic": bool(sentence.get("emerging_topic")),
                "classification_error": str(sentence.get("classification_error", "")).strip(),
            }
            evidence_rows.append(row)
            evidence_index[evidence_id] = row

    for conversation in bank_data.get("qa_conversations", []):
        question_text = " ".join(
            sentence.get("verbatim_text") or sentence.get("text", "")
            for sentence in conversation.get("question_sentences", [])
        ).strip()
        executive_line = conversation.get("executive_name", "Executive")
        if conversation.get("executive_title"):
            executive_line += f", {conversation['executive_title']}"

        for sentence in conversation.get("answer_sentences", []):
            evidence_id = sentence.get("sid")
            if not evidence_id or evidence_id in seen_ids:
                continue
            seen_ids.add(evidence_id)
            row = {
                "evidence_id": evidence_id,
                "status": sentence.get("status", "candidate"),
                "speaker": executive_line,
                "transcript_section": "QA",
                "source_block_id": sentence.get("source_block_id") or conversation.get("id", ""),
                "parent_record_id": sentence.get("parent_record_id") or conversation.get("id", ""),
                "selected_bucket_id": sentence.get("selected_bucket_id", ""),
                "candidate_bucket_ids": sentence.get("candidate_bucket_ids", []),
                "importance_score": float(sentence.get("importance_score", 0.0)),
                "quote": sentence.get("verbatim_text") or sentence.get("text", ""),
                "question_context": question_text,
                "emerging_topic": bool(sentence.get("emerging_topic")),
                "classification_error": str(sentence.get("classification_error", "")).strip(),
            }
            evidence_rows.append(row)
            evidence_index[evidence_id] = row

    return evidence_rows, evidence_index


def _sorted_evidence_rows(evidence_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return evidence rows ordered for stable prompt construction."""
    return sorted(
        evidence_rows,
        key=lambda item: (
            0 if item["status"] == "selected" else 1 if item["status"] == "candidate" else 2,
            0 if item["emerging_topic"] else 1,
            -item["importance_score"],
            item["evidence_id"],
        ),
    )


def _serialise_evidence_digest(
    evidence_rows: List[Dict[str, Any]],
    categories: List[Dict[str, Any]],
    *,
    max_items: Optional[int] = None,
) -> str:
    """Serialize evidence rows into an XML-style digest for prompt injection."""
    digest_rows = []
    digest_rows_source = _sorted_evidence_rows(evidence_rows)
    if max_items is not None and max_items > 0:
        digest_rows_source = digest_rows_source[:max_items]

    for row in digest_rows_source:
        selected_bucket = _bucket_name(row["selected_bucket_id"], categories)
        candidate_names = (
            ", ".join(
                _bucket_name(bucket_id, categories) for bucket_id in row["candidate_bucket_ids"]
            )
            or "None"
        )
        lines = [
            "<evidence>",
            f"  <id>{_escape_for_prompt(row['evidence_id'])}</id>",
            f"  <status>{_escape_for_prompt(row['status'])}</status>",
            "  <transcript_section>"
            f"{_escape_for_prompt(row['transcript_section'])}"
            "</transcript_section>",
            f"  <speaker>{_escape_for_prompt(row['speaker'])}</speaker>",
            f"  <source_block_id>{_escape_for_prompt(row['source_block_id'])}</source_block_id>",
            f"  <selected_bucket>{_escape_for_prompt(selected_bucket)}</selected_bucket>",
            f"  <candidate_buckets>{_escape_for_prompt(candidate_names)}</candidate_buckets>",
            f"  <emerging_topic>{str(row['emerging_topic']).lower()}</emerging_topic>",
            f"  <importance_score>{row['importance_score']:.1f}</importance_score>",
        ]
        if row["classification_error"]:
            lines.append(
                "  <classification_error>"
                f"{_escape_for_prompt(row['classification_error'])}"
                "</classification_error>"
            )
        if row["question_context"]:
            lines.append(
                "  <question_context>"
                f"{_escape_for_prompt(row['question_context'])}"
                "</question_context>"
            )
        lines.append(f"  <quote>{_escape_for_prompt(row['quote'])}</quote>")
        lines.append("</evidence>")
        digest_rows.append("\n".join(lines))

    return "\n\n".join(digest_rows)


def _category_to_row(category: Dict[str, Any]) -> Dict[str, str]:
    """Convert one bucket config category into a row payload."""
    return {
        "transcript_sections": str(category.get("transcript_sections", "ALL")).strip(),
        "report_section": str(category.get("report_section", "Results Summary")).strip(),
        "category_name": str(category.get("category_name", "")).strip(),
        "category_description": str(category.get("category_description", "")).strip(),
        "example_1": str(category.get("example_1", "")).strip(),
        "example_2": str(category.get("example_2", "")).strip(),
        "example_3": str(category.get("example_3", "")).strip(),
    }


async def analyze_config_coverage(
    *,
    bank_data: Dict[str, Any],
    categories: List[Dict[str, Any]],
    min_importance: float,  # pylint: disable=unused-argument
    context: Dict[str, Any],
    llm_params: Dict[str, Any],
) -> Dict[str, Any]:
    """Review the transcript against the current config sheet in two focused passes.

    Pass 1 (`config_review_updates`): asks the model which existing category
    descriptions should be tightened or extended, and returns a replacement
    description per row ready to paste into the config sheet.

    Pass 2 (`config_review_emerging`): asks the model for brand-new emerging
    topics that do not fit any existing category, each with a copy-paste
    ready config row and the finding ids that should move under it when the
    user enables the topic in the UI.

    Both passes share the same indexed-findings digest. Output shape is kept
    as `{"config_change_proposals": [...]}` so downstream rendering (which
    keys off `change_type`) continues to work unchanged.
    """

    evidence_rows, evidence_index = _collect_config_review_evidence(bank_data)
    if not evidence_rows:
        logger.info("Skipping config review because no evidence was captured")
        return {"config_change_proposals": []}

    categories_text = format_categories_for_prompt(categories, "ALL")
    company_name = bank_data.get("company_name", "the company")
    fiscal_quarter = bank_data.get("fiscal_quarter", "")
    fiscal_year = bank_data.get("fiscal_year", "")

    # One shared digest for both passes. Cap at the 150 most-important rows so
    # the prompt stays well under the model's latency ceiling — the previous
    # unbounded digest is what pushed individual calls past the 180s httpx
    # read timeout and triggered silent 3x SDK retries.
    findings_digest = _serialise_evidence_digest(
        evidence_rows,
        categories,
        max_items=150,
    )

    logger.info(
        "Reviewing category coverage",
        ticker=bank_data.get("ticker", ""),
        evidence_rows=len(evidence_rows),
        digest_rows=min(len(evidence_rows), 150),
    )

    existing_category_names = [
        str(category.get("category_name", "")).strip()
        for category in categories
        if str(category.get("category_name", "")).strip()
    ]
    existing_names = {name.lower() for name in existing_category_names}
    update_proposals: List[Dict[str, Any]] = []
    emerging_proposals: List[Dict[str, Any]] = []

    # --- Pass 1: description updates for existing categories ---
    raw_updates = await _call_tool(
        messages=[
            {
                "role": "developer",
                "content": (
                    "You are a config-sheet maintenance analyst for investor-relations "
                    "transcript editors. Given the current category sheet and the indexed "
                    "findings from one transcript, identify existing categories whose "
                    "`category_description` is weak, narrow, or outdated relative to what "
                    "was actually discussed, and return a tightened replacement description "
                    "that the user can copy directly into the config sheet. Keep it as one "
                    "multiline cell with section headings so business users can maintain the "
                    "sheet without writing prompt prose. Prioritize `Topics`, `Keywords`, "
                    "and `Instructions`, but optional extra headings are allowed. This pass "
                    "is ADDITIVE ONLY: build on the current row, preserve existing approved "
                    "scope, and add or clarify missing items; never remove or narrow prior "
                    "topics, keywords, or instructions. If the current row is still legacy "
                    "prose, convert it into section headings while preserving every existing "
                    "concept. Do not propose new categories in this pass. Always use the "
                    "provided tool."
                ),
            },
            {
                "role": "user",
                "content": (
                    "## Task\n"
                    f"Review {company_name}'s {fiscal_quarter} {fiscal_year} indexed findings "
                    "against the current category sheet and return description-only edits "
                    "for existing rows.\n\n"
                    "## Rules\n"
                    "1. Only propose changes to existing categories listed below. Do not "
                    "suggest new categories in this pass.\n"
                    "2. `target_category_name` must match an existing `category_name` exactly.\n"
                    "3. `proposed_description` is the full replacement text for "
                    "`category_description` — do not return deltas or diffs. Keep it as one "
                    "multiline cell and use the preferred sectioned format shown below. The "
                    "other row fields are preserved server-side.\n"
                    "4. The proposal must be additive only: preserve all existing scope and "
                    "append or clarify missing items. Never remove, narrow, or overwrite "
                    "existing topics, keywords, instructions, or optional sections.\n"
                    "5. Use section headings and short list items, not paragraph prose. "
                    "`Topics` are scope fields, `Keywords` are hint fields for non-exhaustive "
                    "strong phrases, and `Instructions` are inclusion/exclusion/tie-break "
                    "rules.\n"
                    "6. Keep `change_summary` to 1-2 sentences of reasoning.\n"
                    "7. Skip rows whose description is already accurate; return nothing "
                    "for them.\n"
                    "8. Cap the response at 6 proposals, one per category at most.\n\n"
                    "## Preferred Description Format\n"
                    f"{_structured_description_template_guidance()}\n\n"
                    "## Current Category Sheet\n"
                    f"{_xml_block('categories', categories_text)}\n\n"
                    "## Indexed Findings Digest\n"
                    f"{_xml_block('findings', findings_digest)}"
                ),
            },
        ],
        tool=TOOL_DESCRIPTION_UPDATES,
        label="config_review_updates",
        context=context,
        llm_params=llm_params,
    )

    if raw_updates:
        try:
            updates_result = DescriptionUpdatesResult.model_validate(raw_updates)
        except Exception as exc:
            logger.warning(
                "Description-update response could not be parsed",
                error=str(exc),
            )
        else:
            seen_targets: set = set()
            for idx, proposal in enumerate(updates_result.proposals[:6], start=1):
                target_name = proposal.target_category_name.strip()
                target_bucket_index = next(
                    (
                        bucket_idx
                        for bucket_idx, category in enumerate(categories)
                        if category.get("category_name", "").strip() == target_name
                    ),
                    -1,
                )
                if target_bucket_index < 0:
                    continue
                base_category = categories[target_bucket_index]
                current_row = _category_to_row(base_category)
                current_description = current_row["category_description"]
                current_parsed = parse_category_description(current_description)
                new_description = proposal.proposed_description.strip()
                if not new_description:
                    continue
                proposed_parsed = parse_category_description(new_description)
                if (
                    current_parsed["format"] == "sectioned_lists"
                    and proposed_parsed["format"] != "sectioned_lists"
                ):
                    logger.warning(
                        "Skipping config proposal that regressed a structured description to prose",
                        target_category_name=target_name,
                    )
                    continue

                merged_description = merge_sectioned_category_descriptions(
                    current_description,
                    new_description,
                )
                dedupe_key = (target_bucket_index, merged_description)
                if dedupe_key in seen_targets:
                    continue
                seen_targets.add(dedupe_key)
                if merged_description == current_description:
                    continue
                proposed_row = dict(current_row)
                proposed_row["category_description"] = merged_description

                update_proposals.append(
                    {
                        "id": f"{bank_data.get('ticker', 'bank')}_update_{idx}",
                        "change_type": "update_existing",
                        "change_summary": proposal.change_summary.strip(),
                        "target_bucket_index": target_bucket_index,
                        "target_bucket_id": f"bucket_{target_bucket_index}",
                        "target_category_name": current_row["category_name"],
                        "current_row": current_row,
                        "proposed_row": proposed_row,
                    }
                )

    # --- Pass 2: emerging topics that deserve a new category ---
    raw_emerging = await _call_tool(
        messages=[
            {
                "role": "developer",
                "content": (
                    "You are an emerging-topic analyst for investor-relations transcript "
                    "editors. Given the current category sheet and the indexed findings from "
                    "one transcript, identify themes that do not fit any existing category "
                    "and are substantial enough to be worth tracking across future quarters "
                    "or across the industry. For each emerging topic return a copy-ready "
                    "config row and the finding ids that belong under it (ids can come from "
                    "any status — selected/candidate/rejected — and may currently sit in "
                    "other buckets; enabling the topic in the UI will reassign them). Keep "
                    "the new row's `category_name` as a generalized, industry-reusable "
                    "taxonomy label in the same style as the current sheet. The title should "
                    "name the reusable theme itself, not the company-specific or "
                    "quarter-specific way it appeared in this transcript. Keep "
                    "the new row's `category_description` as one multiline cell with section "
                    "headings. Prioritize `Topics`, `Keywords`, and `Instructions`, but "
                    "optional extra headings are allowed. Do not write narrative "
                    "descriptions; use short field-like list items. Treat `Keywords` as hint "
                    "fields for strong phrases, not an exhaustive list."
                ),
            },
            {
                "role": "user",
                "content": (
                    "## Task\n"
                    f"Review {company_name}'s {fiscal_quarter} {fiscal_year} indexed findings "
                    "and return brand-new emerging-topic categories worth carrying forward.\n\n"
                    "## Rules\n"
                    "1. Only surface topics that are NOT already covered by an existing "
                    "category below.\n"
                    "2. Each topic must be a genuinely trackable theme, not a one-off mention. "
                    "Skip isolated remarks that are not worth their own category.\n"
                    "3. `proposed_row.report_section` must be exactly `Results Summary` or "
                    "`Earnings Call Q&A`.\n"
                    "4. `proposed_row.transcript_sections` must be exactly `MD`, `QA`, or "
                    "`ALL`.\n"
                    "5. `proposed_row.category_name` must be unique versus the existing sheet "
                    "and must read like a reusable category title from the config, not like a "
                    "transcript-specific finding title.\n"
                    "6. Make `proposed_row.category_name` generalized and bank-agnostic so it "
                    "could be reused to track the same theme across the industry next quarter. "
                    "Keep bank-specific or quarter-specific nuance in `category_description`, "
                    "`change_summary`, or the examples instead.\n"
                    "7. `proposed_row.category_description` should stay as one multiline "
                    "cell and use the preferred sectioned format shown below.\n"
                    "8. Use section headings and short list items, not paragraph prose. "
                    "`Topics` are scope fields, `Keywords` are hint fields for non-exhaustive "
                    "strong phrases, and `Instructions` are inclusion/exclusion/tie-break "
                    "rules.\n"
                    "9. `linked_finding_ids` must use only ids that appear in the findings "
                    "digest below. Include every finding that would genuinely belong under "
                    "the topic, across all statuses.\n"
                    "10. Keep `change_summary` to 1-2 sentences of reasoning.\n"
                    "11. Cap the response at 4 proposals.\n\n"
                    "## Category Name Guidance\n"
                    f"{_generalized_category_name_guidance(existing_category_names)}\n\n"
                    "## Preferred Description Format\n"
                    f"{_structured_description_template_guidance()}\n\n"
                    "## Current Category Sheet\n"
                    f"{_xml_block('categories', categories_text)}\n\n"
                    "## Indexed Findings Digest\n"
                    f"{_xml_block('findings', findings_digest)}"
                ),
            },
        ],
        tool=TOOL_EMERGING_TOPICS,
        label="config_review_emerging",
        context=context,
        llm_params=llm_params,
    )

    if raw_emerging:
        try:
            emerging_result = EmergingTopicsResult.model_validate(raw_emerging)
        except Exception as exc:
            logger.warning(
                "Emerging-topic response could not be parsed",
                error=str(exc),
            )
        else:
            seen_names: set = set()
            for idx, proposal in enumerate(emerging_result.proposals[:4], start=1):
                proposed_row = _normalise_config_row(proposal.proposed_row)
                proposed_parsed = parse_category_description(proposed_row["category_description"])
                if proposed_parsed["format"] == "sectioned_lists":
                    proposed_row["category_description"] = render_sectioned_category_description(
                        proposed_parsed
                    )
                name = proposed_row["category_name"].strip()
                if not name or not proposed_row["category_description"].strip():
                    continue
                name_key = name.lower()
                if name_key in existing_names or name_key in seen_names:
                    continue

                linked_ids = [
                    finding_id
                    for finding_id in proposal.linked_finding_ids
                    if finding_id in evidence_index
                ]
                linked_ids = list(dict.fromkeys(linked_ids))
                if not linked_ids:
                    continue
                seen_names.add(name_key)

                emerging_proposals.append(
                    {
                        "id": f"{bank_data.get('ticker', 'bank')}_emerging_{idx}",
                        "change_type": "new_category",
                        "change_summary": proposal.change_summary.strip(),
                        "target_bucket_index": -1,
                        "target_bucket_id": None,
                        "target_category_name": name,
                        "current_row": _empty_config_row(),
                        "proposed_row": proposed_row,
                        "linked_evidence_ids": linked_ids,
                    }
                )

    # Keep the two pools separate up to their individual caps, then concatenate.
    # Updates come first (description hygiene) followed by emerging topics.
    final_proposals: List[Dict[str, Any]] = update_proposals + emerging_proposals
    logger.info(
        "Config review complete",
        ticker=bank_data.get("ticker", ""),
        proposals=len(final_proposals),
        update_existing=len(update_proposals),
        new_category=len(emerging_proposals),
    )
    return {"config_change_proposals": final_proposals}


def _build_auto_include_candidates(  # pylint: disable=too-many-branches
    bank_data: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    """Build selected evidence groups grouped by assigned bucket."""
    # pylint: disable=unsubscriptable-object,unsupported-assignment-operation
    candidates: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for block in bank_data.get("md_blocks", []):
        current: Optional[Dict[str, Any]] = None
        for sentence in block.get("sentences", []):
            if sentence.get("status") != "selected":
                if current:
                    candidates[current["bucket_id"]].append(current)
                    current = None
                continue
            bucket_id = sentence.get("selected_bucket_id") or sentence.get("primary")
            if not bucket_id:
                continue
            if not current or current["bucket_id"] != bucket_id:
                if current:
                    candidates[current["bucket_id"]].append(current)
                current = {
                    "bucket_id": bucket_id,
                    "importance": float(sentence.get("importance_score", 0.0)),
                    "bucket_score": _bucket_score(sentence.get("scores", {}), bucket_id),
                    "text": sentence.get("verbatim_text") or sentence.get("text", ""),
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
                extra_text = sentence.get("verbatim_text") or sentence.get("text", "")
                if extra_text:
                    current["text"] = f"{current['text']} {extra_text}".strip()
        if current:
            candidates[current["bucket_id"]].append(current)

    for conversation in bank_data.get("qa_conversations", []):
        current = None
        for sentence in conversation.get("answer_sentences", []):
            if sentence.get("status") != "selected":
                if current:
                    candidates[current["bucket_id"]].append(current)
                    current = None
                continue
            bucket_id = sentence.get("selected_bucket_id") or sentence.get("primary")
            if not bucket_id:
                continue
            if not current or current["bucket_id"] != bucket_id:
                if current:
                    candidates[current["bucket_id"]].append(current)
                current = {
                    "bucket_id": bucket_id,
                    "importance": float(sentence.get("importance_score", 0.0)),
                    "bucket_score": _bucket_score(sentence.get("scores", {}), bucket_id),
                    "text": sentence.get("verbatim_text") or sentence.get("text", ""),
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
                extra_text = sentence.get("verbatim_text") or sentence.get("text", "")
                if extra_text:
                    current["text"] = f"{current['text']} {extra_text}".strip()
        if current:
            candidates[current["bucket_id"]].append(current)

    return candidates


def collect_headline_samples(
    banks_data: Dict[str, Dict[str, Any]],
    min_importance: float,
) -> Dict[str, List[str]]:
    """Collect selected report snippets for optional bucket headline generation."""
    samples: Dict[str, List[str]] = defaultdict(list)
    for bank_data in banks_data.values():
        candidates_by_bucket = _build_auto_include_candidates(bank_data)
        for bucket_id, candidates in candidates_by_bucket.items():
            ranked = sorted(
                candidates,
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
    sample_size: int = 8,
) -> Dict[str, str]:
    """Generate bucket headlines mirroring the mock editor workflow."""
    samples_by_bucket = collect_headline_samples(banks_data, min_importance)
    headlines: Dict[str, str] = {}

    if not samples_by_bucket:
        logger.info("Skipping bucket headline generation because no selected content was found")
        return headlines

    logger.info(
        "Generating bucket headlines",
        candidate_buckets=len(samples_by_bucket),
        sample_size=sample_size,
    )

    for idx, category in enumerate(categories):
        bucket_id = f"bucket_{idx}"
        samples = samples_by_bucket.get(bucket_id, [])
        if not samples:
            continue

        sample_text = "\n\n---\n\n".join(samples[:sample_size])
        system_prompt = (
            "You are a headline writer for investor-relations earnings summaries. "
            "Turn already-selected bucket content into a short factual headline that reflects what "
            "management actually said. Always use the provided tool."
        )
        user_prompt = (
            "## Task\n"
            f"Generate a 5-10 word headline for the '{category['category_name']}' bucket.\n\n"
            "## Decision Criteria\n"
            "The headline should be specific, factual, and driven by the sample content "
            "rather than "
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

    logger.info(
        "Bucket headline generation complete",
        candidate_buckets=len(samples_by_bucket),
        generated_headlines=len(headlines),
    )
    return headlines


def count_included_categories(
    banks_data: Dict[str, Dict[str, Any]],
    min_importance: float,
) -> int:
    """Count buckets with at least one selected report sample."""
    return len(collect_headline_samples(banks_data, min_importance))
