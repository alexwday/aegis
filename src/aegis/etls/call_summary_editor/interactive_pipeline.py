"""Interactive HTML report pipeline for call_summary_editor.

This ports the mock editor workflow onto Aegis transcript data:
- Q&A boundary detection over raw XML speaker blocks
- Per-paragraph MD sentence classification
- Per-exchange QA classification
- Bucket headline generation for the interactive HTML report
"""

from __future__ import annotations

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
    scores: List[float] = Field(
        description="Relevance score 0-10 for each bucket in order, one per bucket"
    )
    importance_score: float = Field(description="IR quotability 0-10")
    condensed: str = Field(description="~70% length, filler removed, all facts kept")
    summary: str = Field(description="1-2 sentence summary of the point")
    paraphrase: str = Field(description="Third-person restatement of the sentence")


class QAConversationGroup(BaseModel):
    """One Q&A conversation grouping emitted by the boundary tool."""

    conversation_id: str
    block_ids: List[str]


class QABoundaryResult(BaseModel):
    """Boundary grouping response for raw QA speaker blocks."""

    conversations: List[QAConversationGroup]


class QAExchangeClassification(BaseModel):
    """Whole-exchange classification for one QA conversation."""

    primary_bucket_index: int = Field(
        description="0-based index of the best bucket for the whole exchange. -1 for Other."
    )
    question_scores: List[float] = Field(
        description="Relevance score 0-10 for each bucket in order for the analyst question."
    )
    question_importance: float = Field(description="IR quotability of the analyst question 0-10.")
    answer_sentences: List[SentenceResult]


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
                "Relevance score 0-10 for every bucket in order, one value per bucket "
                "starting from bucket 0."
            ),
            "items": {"type": "number"},
        },
        "importance_score": {"type": "number", "description": "IR quotability score 0-10"},
        "condensed": {"type": "string"},
        "summary": {"type": "string"},
        "paraphrase": {"type": "string"},
    },
    "required": ["index", "scores", "importance_score", "condensed", "summary", "paraphrase"],
}

TOOL_QA_BOUNDARY = {
    "type": "function",
    "function": {
        "name": "group_qa_conversations",
        "description": (
            "Group speaker block IDs into complete Q&A conversation exchanges. "
            "Return only block IDs in order."
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
                            "block_ids": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["conversation_id", "block_ids"],
                    },
                }
            },
            "required": ["conversations"],
        },
    },
}

TOOL_MD_PARAGRAPH = {
    "type": "function",
    "function": {
        "name": "classify_paragraph_sentences",
        "description": "Classify each sentence in the current paragraph.",
        "parameters": {
            "type": "object",
            "properties": {"sentences": {"type": "array", "items": _SENTENCE_RESULT_SCHEMA}},
            "required": ["sentences"],
        },
    },
}

TOOL_QA_EXCHANGE = {
    "type": "function",
    "function": {
        "name": "classify_qa_exchange",
        "description": (
            "Classify the Q&A exchange: whole-question scores and per-sentence answer classification."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "primary_bucket_index": {"type": "integer"},
                "question_scores": {
                    "type": "array",
                    "description": "Relevance score 0-10 for every bucket in order.",
                    "items": {"type": "number"},
                },
                "question_importance": {"type": "number"},
                "answer_sentences": {"type": "array", "items": _SENTENCE_RESULT_SCHEMA},
            },
            "required": [
                "primary_bucket_index",
                "question_scores",
                "question_importance",
                "answer_sentences",
            ],
        },
    },
}

TOOL_HEADLINE = {
    "type": "function",
    "function": {
        "name": "set_headline",
        "description": "Set the headline for a report section.",
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
    response = await complete_with_tools(
        messages=messages,
        tools=[tool],
        context=context,
        llm_params=llm_params,
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
    if bucket_id == "other":
        return "Other"
    try:
        idx = int(bucket_id.split("_")[1])
        return categories[idx]["category_name"]
    except Exception:
        return bucket_id


def _primary_from_scores(scores: Dict[str, float], applicable_ids: List[str]) -> str:
    """Pick the highest-scoring applicable bucket, else Other."""
    best_id, best_score = "other", 0.0
    for bucket_id in applicable_ids:
        score = scores.get(bucket_id, 0.0)
        if score > best_score:
            best_score = score
            best_id = bucket_id
    return best_id if best_score >= 1.5 else "other"


def _normalise_scores(raw_scores: Any, categories: List[Dict[str, Any]]) -> Dict[str, float]:
    """Convert list/dict model output into `bucket_N` score mapping."""
    output = {f"bucket_{idx}": 0.0 for idx in range(len(categories))}
    if isinstance(raw_scores, list):
        for idx, value in enumerate(raw_scores):
            if idx < len(categories):
                output[f"bucket_{idx}"] = round(float(value), 2)
    elif isinstance(raw_scores, dict):
        for key, value in raw_scores.items():
            normalized = key.replace("bucket_", "")
            if normalized.isdigit():
                output[f"bucket_{normalized}"] = round(float(value), 2)
    return output


def _make_sentence_record(
    sentence_id: str,
    text: str,
    llm_result: Optional[SentenceResult],
    categories: List[Dict[str, Any]],
    applicable_ids: List[str],
) -> Dict[str, Any]:
    if llm_result is None:
        return {
            "sid": sentence_id,
            "text": text,
            "primary": "other",
            "scores": {f"bucket_{idx}": 0.0 for idx in range(len(categories))},
            "importance_score": 2.0,
            "condensed": text,
            "summary": text,
            "paraphrase": text,
        }

    scores = _normalise_scores(llm_result.scores, categories)
    return {
        "sid": sentence_id,
        "text": text,
        "primary": _primary_from_scores(scores, applicable_ids),
        "scores": scores,
        "importance_score": round(float(llm_result.importance_score), 1),
        "condensed": llm_result.condensed or text,
        "summary": llm_result.summary or text,
        "paraphrase": llm_result.paraphrase or text,
    }


def _fallback_qa_grouping(qa_raw_blocks: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Fallback grouping by consecutive q->a runs using FactSet speaker hints."""
    conversations: List[List[Dict[str, Any]]] = []
    index = 0
    while index < len(qa_raw_blocks):
        block = qa_raw_blocks[index]
        if block.get("speaker_type_hint") == "q":
            group = [block]
            next_index = index + 1
            while (
                next_index < len(qa_raw_blocks)
                and qa_raw_blocks[next_index].get("speaker_type_hint") == "a"
            ):
                group.append(qa_raw_blocks[next_index])
                next_index += 1
            conversations.append(group)
            index = next_index
            continue
        index += 1
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

    block_lines = []
    for block in qa_raw_blocks:
        hint = (block.get("speaker_type_hint") or "?").upper()
        speaker_line = block.get("speaker", "Unknown Speaker")
        if block.get("speaker_title"):
            speaker_line += f", {block['speaker_title']}"
        if block.get("speaker_affiliation"):
            speaker_line += f" ({block['speaker_affiliation']})"
        preview = block.get("paragraphs", [""])[0][:300] if block.get("paragraphs") else ""
        if block.get("paragraphs") and len(block["paragraphs"][0]) > 300:
            preview += "..."
        block_lines.append(
            f'[{block["id"]}] type_hint={hint} | {speaker_line}\n  "{preview}"'
        )

    system_prompt = (
        "You are grouping speaker blocks from an earnings call Q&A section into complete "
        "conversation exchanges. Each conversation begins with an analyst question and "
        "includes all executive responses that follow until the next analyst question. "
        "Return only block IDs using the group_qa_conversations function. The type_hint "
        "field may be wrong, so use speaker affiliation and content as well.\n\n"
        f"Available categories for downstream QA classification:\n{categories_text_qa}"
    )
    user_prompt = (
        "Group these Q&A speaker blocks into conversation exchanges:\n\n"
        + "\n\n".join(block_lines)
    )
    raw = await _call_tool(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        tool=TOOL_QA_BOUNDARY,
        label="qa_boundary",
        context=context,
        llm_params=llm_params,
    )
    if not raw:
        return _fallback_qa_grouping(qa_raw_blocks)

    try:
        result = QABoundaryResult.model_validate(raw)
    except Exception as exc:
        logger.warning(
            "etl.call_summary_editor.qa_boundary_parse_error",
            execution_id=context["execution_id"],
            error=str(exc),
        )
        return _fallback_qa_grouping(qa_raw_blocks)

    block_by_id = {block["id"]: block for block in qa_raw_blocks}
    conversations = []
    for conversation in result.conversations:
        blocks = [block_by_id[block_id] for block_id in conversation.block_ids if block_id in block_by_id]
        if blocks:
            conversations.append(blocks)

    return conversations or _fallback_qa_grouping(qa_raw_blocks)


async def classify_md_block(
    *,
    block_raw: Dict[str, Any],
    categories: List[Dict[str, Any]],
    categories_text_md: str,
    company_name: str,
    fiscal_year: int,
    fiscal_quarter: str,
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

        n_buckets = len(categories)
        system_prompt = (
            f"You are classifying sentences from an earnings call Management Discussion section "
            f"for {company_name}'s {fiscal_quarter} {fiscal_year} earnings call.\n\n"
            f"Available IR report buckets (MD-applicable):\n{categories_text_md}\n\n"
            f"For each sentence you MUST return ALL of these fields:\n"
            f"- scores: array of {n_buckets} numbers, one per bucket in order [bucket_0_score, bucket_1_score, ...].\n"
            f"- importance_score: 0-10 IR quotability. Use 0 for ceremonial or procedural content.\n"
            f"- condensed: ~70% length version\n"
            f"- summary: 1-2 sentence summary\n"
            f"- paraphrase: 3rd person rewrite\n"
            f"- index: must match the S-number shown (1-based)\n"
            f"Do NOT omit any field."
        )
        user_prompt = "\n".join(context_lines) + f"\n\nClassify the sentences in Paragraph {para_idx + 1} now."
        raw = await _call_tool(
            messages=[
                {"role": "system", "content": system_prompt},
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
                            summary=sentence_raw.get("summary", ""),
                            paraphrase=sentence_raw.get("paraphrase", ""),
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

    answer_sentences_raw: List[str] = []
    answer_para_indices: List[int] = []
    para_idx = 0
    for block in answer_blocks:
        for paragraph in block["paragraphs"]:
            for sentence in split_sentences(paragraph):
                answer_sentences_raw.append(sentence)
                answer_para_indices.append(para_idx)
            para_idx += 1

    n_buckets = len(categories)
    question_lines = "\n".join(
        f'QS{idx + 1}: "{sentence}"' for idx, sentence in enumerate(question_sentences)
    )
    answer_lines = "\n".join(
        f'AS{idx + 1}: "{sentence}"' for idx, sentence in enumerate(answer_sentences_raw)
    )
    system_prompt = (
        f"You are classifying a Q&A exchange from {company_name}'s {fiscal_quarter} {fiscal_year} earnings call.\n\n"
        f"Available IR report buckets (Q&A-applicable):\n{categories_text_qa}\n\n"
        f"Task:\n"
        f"1. primary_bucket_index: which single bucket best describes this whole exchange (-1 for Other)\n"
        f"2. question_scores: array of {n_buckets} numbers [bucket_0_score, ..., bucket_{n_buckets - 1}_score]\n"
        f"3. question_importance: 0-10 IR quotability of the question\n"
        f"4. answer_sentences: for each AS-numbered answer sentence, return index, scores, "
        f"importance_score, condensed, summary, and paraphrase.\n"
        f"Do NOT omit any field."
    )
    user_prompt = (
        f"ANALYST ({analyst_name}{', ' + analyst_affiliation if analyst_affiliation else ''}):\n"
        f"{question_lines}\n\n"
        f"EXECUTIVE ({executive_name}{', ' + executive_title if executive_title else ''}):\n"
        f"{answer_lines}\n\n"
        f"Classify this exchange using the classify_qa_exchange function."
    )
    raw = await _call_tool(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        tool=TOOL_QA_EXCHANGE,
        label=f"qa_conv:{conv_id}",
        context=context,
        llm_params=llm_params,
    )

    primary_bucket = "other"
    question_records: List[Dict[str, Any]] = []
    answer_records: List[Dict[str, Any]] = []
    if raw:
        try:
            result = QAExchangeClassification.model_validate(raw)
            if 0 <= result.primary_bucket_index < len(categories):
                primary_bucket = f"bucket_{result.primary_bucket_index}"

            question_scores = _normalise_scores(result.question_scores, categories)
            question_primary = _primary_from_scores(question_scores, applicable_ids)
            for idx, sentence in enumerate(question_sentences):
                question_records.append(
                    {
                        "sid": f"{conv_id}_qs{idx}",
                        "text": sentence,
                        "primary": question_primary,
                        "scores": question_scores,
                        "importance_score": round(float(result.question_importance), 1),
                        "condensed": sentence,
                        "summary": sentence,
                        "paraphrase": sentence,
                    }
                )

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
                    "primary": "other",
                    "scores": {f"bucket_{bucket_idx}": 0.0 for bucket_idx in range(len(categories))},
                    "importance_score": 3.0,
                    "condensed": sentence,
                    "summary": sentence,
                    "paraphrase": sentence,
                }
            )

    if not answer_records:
        for idx, sentence in enumerate(answer_sentences_raw):
            answer_records.append(
                {
                    "sid": f"{conv_id}_as{idx}",
                    "text": sentence,
                    "primary": "other",
                    "scores": {f"bucket_{bucket_idx}": 0.0 for bucket_idx in range(len(categories))},
                    "importance_score": 3.0,
                    "condensed": sentence,
                    "summary": sentence,
                    "paraphrase": sentence,
                    "para_idx": answer_para_indices[idx] if idx < len(answer_para_indices) else 0,
                }
            )

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

    processed_md = []
    for idx, block in enumerate(md_raw_blocks, start=1):
        logger.info(
            "etl.call_summary_editor.md_block_started",
            execution_id=context["execution_id"],
            ticker=ticker,
            block_index=idx,
            total_blocks=len(md_raw_blocks),
            block_id=block["id"],
        )
        processed_md.append(
            await classify_md_block(
                block_raw=block,
                categories=categories,
                categories_text_md=categories_text_md,
                company_name=company_name,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                context=context,
                llm_params=md_llm_params,
            )
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


def collect_headline_samples(
    banks_data: Dict[str, Dict[str, Any]],
    min_importance: float,
) -> Dict[str, List[str]]:
    """Collect summary snippets for bucket-level headline generation."""
    samples: Dict[str, List[str]] = defaultdict(list)
    for bank_data in banks_data.values():
        for block in bank_data.get("md_blocks", []):
            for sentence in block.get("sentences", []):
                if sentence.get("importance_score", 0) >= min_importance and sentence.get("primary") != "other":
                    if len(samples[sentence["primary"]]) < 8:
                        samples[sentence["primary"]].append(sentence.get("summary") or sentence.get("text", ""))

        for conversation in bank_data.get("qa_conversations", []):
            for sentence in conversation.get("answer_sentences", []):
                if sentence.get("importance_score", 0) >= min_importance and sentence.get("primary") != "other":
                    primary_bucket = conversation.get("primary_bucket") or sentence["primary"]
                    if primary_bucket != "other" and len(samples[primary_bucket]) < 8:
                        samples[primary_bucket].append(sentence.get("summary") or sentence.get("text", ""))
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
            f"Generate a SPECIFIC, FACTUAL 5-10 word headline for the "
            f"'{category['category_name']}' section of an IR earnings summary. "
            f"Capture what management actually said. Return the headline using the "
            f"set_headline function and do not add trailing punctuation."
        )
        raw = await _call_tool(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Sample content:\n\n{sample_text}"},
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
    """Count non-Other buckets with at least one included high-importance sentence."""
    return len(collect_headline_samples(banks_data, min_importance))
