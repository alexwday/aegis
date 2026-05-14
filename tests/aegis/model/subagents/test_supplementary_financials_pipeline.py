"""Tests for the supplementary financials retrieval pipeline."""

import asyncio

import pytest

from aegis.model.subagents.supplementary_financials import pipeline


def _candidate(chunk_id: str, score: float = 0.0) -> dict:
    """Build a minimal retrieval candidate."""
    return {
        "file_id": "financial-supp_2026_Q1_CM",
        "chunk_id": chunk_id,
        "bank": "CM",
        "quarter": "Q1",
        "fiscal_year": "2026",
        "page_number": int(chunk_id.split("_")[1].split(".")[0]),
        "name": chunk_id,
        "filename": "supp.xlsx",
        "file_path": "2026_Q1/CM/supp.xlsx",
        "file_type": "xlsx",
        "summary": "",
        "score": score,
    }


def _combo(symbol: str, quarter: str = "Q1", fiscal_year: str = "2026") -> dict:
    """Build a minimal bank-period combination."""
    return {
        "bank_id": symbol,
        "bank_name": f"{symbol} Bank",
        "bank_symbol": symbol,
        "quarter": quarter,
        "fiscal_year": fiscal_year,
        "query_intent": "test query",
    }


def test_normalize_prepared_query_caps_and_merges_terms() -> None:
    """Query prep caps LLM output and merges deterministic metric terms."""
    parsed = {
        "rewritten_query": "CM Q1 2026 net income and revenue",
        "sub_queries": ["a", "a", "b", "c", "d"],
        "keywords": ["Revenue", "Revenue", "NIM"],
        "metrics": ["net income"],
        "hyde_answer": "Net income was 13.7% and revenue was $1,000 million.",
    }

    prepared = pipeline.normalize_prepared_query(
        parsed,
        "CM Q1 2026 net income and revenue",
    )

    assert prepared["sub_queries"] == ["a", "b", "c"]
    assert prepared["keywords"][:2] == ["Revenue", "NIM"]
    assert "net income" in prepared["metrics"]
    assert "revenue" in prepared["metrics"]
    assert "13.7%" not in prepared["hyde_answer"]
    assert "$1,000" not in prepared["hyde_answer"]


def test_group_combinations_by_period_preserves_period_order() -> None:
    """Combinations are grouped by period without reordering banks inside each period."""
    combos = [
        _combo("RY", "Q1"),
        _combo("TD", "Q2"),
        _combo("BMO", "Q1"),
        _combo("CM", "Q2"),
    ]

    grouped = pipeline.group_combinations_by_period(combos)

    assert [[combo["bank_symbol"] for combo in group] for group in grouped] == [
        ["RY", "BMO"],
        ["TD", "CM"],
    ]


def test_combo_params_uses_base_ticker_for_supplementary_sql() -> None:
    """Supplementary SQL filters use base tickers, not country-suffixed IDs."""
    assert pipeline.combo_params(
        {
            "bank_symbol": "BMO-CA",
            "fiscal_year": "FY2026",
            "quarter": "q1",
        }
    ) == {"bank_symbol": "BMO", "fiscal_year": "2026", "quarter": "Q1"}


@pytest.mark.asyncio
async def test_run_retrieval_pipeline_parallelizes_combos_by_period(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pipeline runs up to six combos in parallel, one period group at a time."""
    combos = [_combo(f"BANK{index}", "Q1") for index in range(7)]
    combos.extend([_combo("RY", "Q2"), _combo("TD", "Q2")])
    active_combos = 0
    max_active_combos = 0
    active_period_counts = {}
    mixed_periods = False

    async def fake_prepare_query(**_kwargs: object) -> dict:
        return {
            "rewritten_query": "test",
            "sub_queries": [],
            "keywords": [],
            "metrics": [],
            "embeddings": {},
        }

    async def fake_process_combo_retrieval(
        combo: dict,
        prepared: dict,
        context: dict,
        search_top_k: int,
        search_semaphore: asyncio.Semaphore,
    ) -> dict:
        nonlocal active_combos, max_active_combos, mixed_periods
        _ = prepared, context, search_top_k, search_semaphore
        period = (combo["fiscal_year"], combo["quarter"])
        active_combos += 1
        max_active_combos = max(max_active_combos, active_combos)
        active_period_counts[period] = active_period_counts.get(period, 0) + 1
        if len([count for count in active_period_counts.values() if count]) > 1:
            mixed_periods = True
        await asyncio.sleep(0.01)
        active_period_counts[period] -= 1
        active_combos -= 1
        return {
            "combo": combo,
            "expanded_chunks": [_candidate("sheet_1.1", score=0.5)],
            "findings": [],
        }

    monkeypatch.setattr(pipeline, "prepare_query", fake_prepare_query)
    monkeypatch.setattr(pipeline, "process_combo_retrieval", fake_process_combo_retrieval)

    results = await pipeline.run_retrieval_pipeline(
        query_text="test",
        latest_message="test",
        bank_period_combinations=combos,
        context={"execution_id": "test"},
    )

    assert results["metrics"]["combo_count"] == len(combos)
    assert max_active_combos == pipeline.MAX_PARALLEL_COMBOS_PER_PERIOD
    assert mixed_periods is False


@pytest.mark.asyncio
async def test_multi_strategy_search_uses_shared_search_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Independent search strategies run concurrently behind a shared semaphore."""
    active_searches = 0
    max_active_searches = 0

    async def record_search(chunk_id: str) -> list[dict]:
        nonlocal active_searches, max_active_searches
        active_searches += 1
        max_active_searches = max(max_active_searches, active_searches)
        await asyncio.sleep(0.01)
        active_searches -= 1
        candidate = _candidate(chunk_id, score=0.5)
        candidate["raw_score"] = 0.5
        return [candidate]

    async def fake_search_embedding_type(
        _combo: dict,
        embedding_vector: list[float],
        embedding_type: str,
        _top_k: int,
    ) -> list[dict]:
        return await record_search(f"sheet_{int(embedding_vector[0])}.{len(embedding_type)}")

    async def fake_search_section_summary(
        _combo: dict,
        _embedding_vector: list[float],
        _top_k: int,
    ) -> list[dict]:
        return await record_search("sheet_20.1")

    async def fake_bm25_search(_combo: dict, _query_text: str, _top_k: int) -> list[dict]:
        return await record_search("sheet_21.1")

    async def fake_jsonb_containment_search(
        _combo: dict,
        column_name: str,
        _terms: list[str],
        _limit: int,
    ) -> list[dict]:
        return await record_search(f"sheet_22.{len(column_name)}")

    monkeypatch.setattr(pipeline, "search_embedding_type", fake_search_embedding_type)
    monkeypatch.setattr(pipeline, "search_section_summary", fake_search_section_summary)
    monkeypatch.setattr(pipeline, "bm25_search", fake_bm25_search)
    monkeypatch.setattr(pipeline, "jsonb_containment_search", fake_jsonb_containment_search)

    candidates = await pipeline.multi_strategy_search(
        combo=_combo("RY"),
        prepared={
            "rewritten_query": "revenue",
            "sub_queries": ["sub"],
            "keywords": ["revenue"],
            "metrics": ["net income"],
            "embeddings": {
                "rewritten": [1.0],
                "hyde": [2.0],
                "sub_query_0": [3.0],
                "keywords": [4.0],
                "metrics": [5.0],
            },
        },
        top_k=20,
        search_semaphore=asyncio.Semaphore(2),
    )

    assert candidates
    assert max_active_searches == 2


@pytest.mark.asyncio
async def test_process_combo_caps_fused_results_after_rerank(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Combo retrieval keeps a global top-k after matrix fusion and rerank."""
    candidates = [_candidate(f"sheet_{index}.1", score=100 - index) for index in range(1, 26)]
    rerank_inputs = []
    research_inputs = []

    async def fake_multi_strategy_search(**kwargs: object) -> list[dict]:
        assert kwargs["top_k"] == 20
        return candidates

    async def fake_rerank_candidates(**kwargs: object) -> list[dict]:
        rerank_inputs.append(list(kwargs["candidates"]))
        return list(kwargs["candidates"])

    async def fake_gap_fill_one_sheet_gaps(
        chunks: list[dict],
        search_semaphore: asyncio.Semaphore | None = None,
    ) -> list[dict]:
        _ = search_semaphore
        return chunks

    async def fake_run_research_loop(**kwargs: object) -> dict:
        research_inputs.append(list(kwargs["initial_chunks"]))
        return {"chunks": kwargs["initial_chunks"], "findings": [], "iterations": []}

    monkeypatch.setattr(pipeline, "multi_strategy_search", fake_multi_strategy_search)
    monkeypatch.setattr(pipeline, "rerank_candidates", fake_rerank_candidates)
    monkeypatch.setattr(pipeline, "gap_fill_one_sheet_gaps", fake_gap_fill_one_sheet_gaps)
    monkeypatch.setattr(pipeline, "run_research_loop", fake_run_research_loop)

    result = await pipeline.process_combo_retrieval(
        combo=_combo("CM"),
        prepared={"rewritten_query": "revenue"},
        context={"execution_id": "test"},
        search_top_k=20,
        search_semaphore=asyncio.Semaphore(10),
    )

    assert len(rerank_inputs) == 1
    assert len(rerank_inputs[0]) == 25
    assert len(result["reranked_chunks"]) == 20
    assert len(research_inputs[0]) == 20
    assert [chunk["chunk_id"] for chunk in result["reranked_chunks"]] == [
        candidate["chunk_id"] for candidate in candidates[:20]
    ]


@pytest.mark.asyncio
async def test_process_combo_skips_rerank_when_fused_results_are_within_top_k(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rerank is skipped when fused retrieval already fits inside the global cap."""
    candidates = [_candidate(f"sheet_{index}.1", score=100 - index) for index in range(1, 6)]

    async def fake_multi_strategy_search(**_kwargs: object) -> list[dict]:
        return candidates

    async def fake_rerank_candidates(**_kwargs: object) -> list[dict]:
        raise AssertionError("rerank should not run for small fused candidate sets")

    async def fake_gap_fill_one_sheet_gaps(
        chunks: list[dict],
        search_semaphore: asyncio.Semaphore | None = None,
    ) -> list[dict]:
        _ = search_semaphore
        return chunks

    async def fake_run_research_loop(**kwargs: object) -> dict:
        return {"chunks": kwargs["initial_chunks"], "findings": [], "iterations": []}

    monkeypatch.setattr(pipeline, "multi_strategy_search", fake_multi_strategy_search)
    monkeypatch.setattr(pipeline, "rerank_candidates", fake_rerank_candidates)
    monkeypatch.setattr(pipeline, "gap_fill_one_sheet_gaps", fake_gap_fill_one_sheet_gaps)
    monkeypatch.setattr(pipeline, "run_research_loop", fake_run_research_loop)

    result = await pipeline.process_combo_retrieval(
        combo=_combo("CM"),
        prepared={"rewritten_query": "revenue"},
        context={"execution_id": "test"},
        search_top_k=20,
        search_semaphore=asyncio.Semaphore(10),
    )

    assert [chunk["chunk_id"] for chunk in result["reranked_chunks"]] == [
        candidate["chunk_id"] for candidate in candidates
    ]


@pytest.mark.asyncio
async def test_research_loop_stops_on_high_confidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """High-confidence research does not spend another embedding/search pass."""

    async def fake_call_research_iteration(**kwargs: object) -> dict:
        assert kwargs["iteration_number"] == 1
        return {
            "iteration": 1,
            "findings": [],
            "additional_queries": ["revenue details"],
            "confidence": pipeline.RESEARCH_CONFIDENCE_STOP_THRESHOLD,
            "usage": {},
        }

    async def fake_search_additional_queries(**_kwargs: object) -> list[dict]:
        raise AssertionError("additional searches should not run after high confidence")

    monkeypatch.setattr(pipeline, "call_research_iteration", fake_call_research_iteration)
    monkeypatch.setattr(pipeline, "search_additional_queries", fake_search_additional_queries)

    result = await pipeline.run_research_loop(
        prepared={"original_query": "revenue"},
        combo=_combo("CM"),
        initial_chunks=[_candidate("sheet_1.1")],
        context={"execution_id": "test"},
        search_semaphore=asyncio.Semaphore(10),
    )

    assert result["stopping_reason"] == "high_confidence"
    assert len(result["iterations"]) == 1


def test_apply_min_keep_floor_restores_highest_scoring_removals() -> None:
    """Rerank cannot remove below the configured keep floor."""
    candidates = [_candidate(f"sheet_{index}.1", score=index) for index in range(12)]
    remove_set = set(range(12))

    adjusted = pipeline.apply_min_keep_floor(candidates, remove_set)

    kept = [index for index in range(12) if index not in adjusted]
    assert len(kept) == pipeline.RERANK_MIN_KEEP
    assert kept == list(range(2, 12))


def test_cap_gap_filled_chunks_preserves_global_limit_and_anchor_chunks() -> None:
    """Gap fill can use spare evidence slots but cannot exceed the top-k cap."""
    anchors = [_candidate(f"sheet_{index}.1", score=index) for index in (1, 3, 5)]
    gap_filled = [
        anchors[0],
        _candidate("sheet_2.1", score=0.0),
        anchors[1],
        _candidate("sheet_4.1", score=0.0),
        anchors[2],
    ]

    capped = pipeline.cap_gap_filled_chunks(gap_filled, anchors, limit=4)

    assert len(capped) == 4
    assert {chunk["chunk_id"] for chunk in anchors}.issubset(
        {chunk["chunk_id"] for chunk in capped}
    )


def test_normalize_remove_indices_accepts_stringified_indices() -> None:
    """Rerank removal parsing accepts common integer-like tool outputs."""
    parsed = pipeline.normalize_remove_indices(
        [1, "2", " 3 ", 4.0, True, False, "x", 99, -1],
        candidate_count=12,
    )

    assert parsed == {1, 2, 3, 4}


def test_normalize_db_stage_prompt_accepts_prompt_table_row() -> None:
    """DB prompt rows are normalized into the local stage-prompt schema."""
    prompt = pipeline.normalize_db_stage_prompt(
        {
            "system_prompt": "System",
            "user_prompt": "User {input}",
            "tool_definition": {"type": "function", "function": {"name": "tool"}},
            "version": "1.2",
            "description": "test",
        },
        "query_prep",
    )

    assert prompt["stage"] == "query_prep"
    assert prompt["version"] == "1.2"
    assert prompt["tool_choice"] == "required"
    assert prompt["tools"] == [{"type": "function", "function": {"name": "tool"}}]


def test_load_stage_prompt_falls_back_to_yaml_when_db_prompt_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing DB prompts fall back to the checked-in YAML prompt."""

    def fake_load_prompt_from_db(**_kwargs: object) -> str:
        return "Blank"

    monkeypatch.setattr(pipeline, "load_prompt_from_db", fake_load_prompt_from_db)

    prompt = pipeline.load_stage_prompt("query_prep", execution_id="test")

    assert prompt["stage"] == "query_prep"
    assert prompt["tool_choice"] == "required"
    assert prompt["tools"][0]["function"]["name"] == "prepare_query"


def test_resolve_tool_choice_forces_single_named_tool() -> None:
    """Single-tool prompts force that exact function instead of generic required mode."""
    prompt = {
        "tool_choice": "required",
        "tools": [{"type": "function", "function": {"name": "filter_chunks"}}],
    }

    assert pipeline.resolve_tool_choice(prompt) == {
        "type": "function",
        "function": {"name": "filter_chunks"},
    }


@pytest.mark.asyncio
async def test_call_tool_prompt_passes_named_tool_choice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stage tool prompts pass an explicit function choice to the LLM connector."""

    def fake_load_stage_prompt(_prompt_name: str, execution_id: str | None = None) -> dict:
        assert execution_id == "test"
        return {
            "system_prompt": "Use the tool.",
            "user_prompt": "Rank {candidates}",
            "tool_choice": "required",
            "tools": [{"type": "function", "function": {"name": "filter_chunks"}}],
        }

    async def fake_complete_with_tools(**kwargs: object) -> dict:
        assert kwargs["llm_params"]["model"] == pipeline.config.llm.small.model
        assert kwargs["llm_params"]["temperature"] == 0
        assert kwargs["llm_params"]["tool_choice"] == {
            "type": "function",
            "function": {"name": "filter_chunks"},
        }
        return {
            "choices": [
                {
                    "message": {
                        "tool_calls": [{"function": {"arguments": '{"remove_indices": [1]}'}}]
                    }
                }
            ],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
        }

    monkeypatch.setattr(pipeline, "load_stage_prompt", fake_load_stage_prompt)
    monkeypatch.setattr(pipeline, "complete_with_tools", fake_complete_with_tools)

    parsed, usage = await pipeline.call_tool_prompt(
        prompt_name="rerank",
        replacements={"candidates": "candidate list"},
        context={"execution_id": "test"},
        max_tokens=800,
    )

    assert parsed == {"remove_indices": [1]}
    assert usage == {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}


def test_extract_tool_arguments_accepts_json_content_fallback() -> None:
    """Plain JSON content is accepted when a model omits the forced tool call."""
    parsed = pipeline.extract_tool_arguments(
        {
            "choices": [
                {
                    "message": {
                        "content": '```json\n{"remove_indices": ["1", 2]}\n```',
                    }
                }
            ]
        }
    )

    assert parsed == {"remove_indices": ["1", 2]}


@pytest.mark.asyncio
async def test_rerank_candidates_coerces_string_indices(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rerank candidates applies valid stringified removal indices."""
    candidates = [_candidate(f"sheet_{index}.1", score=index) for index in range(12)]

    async def fake_call_tool_prompt(**_kwargs: object) -> tuple[dict, dict]:
        return {"remove_indices": ["1", "2", True, "99", "bad"]}, {}

    monkeypatch.setattr(pipeline, "call_tool_prompt", fake_call_tool_prompt)

    reranked = await pipeline.rerank_candidates(
        query="revenue",
        combo=_combo("CM"),
        candidates=candidates,
        context={"execution_id": "test"},
    )

    assert [candidate["chunk_id"] for candidate in reranked] == [
        candidate["chunk_id"] for index, candidate in enumerate(candidates) if index not in {1, 2}
    ]


@pytest.mark.asyncio
async def test_gap_fill_one_sheet_gap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Gap fill inserts the only missing sheet between selected chunks."""

    async def fake_load_sheet_chunks(file_id: str, sheet_number: int) -> list[dict]:
        assert file_id == "financial-supp_2026_Q1_CM"
        assert sheet_number == 11
        return [_candidate("sheet_11.1")]

    monkeypatch.setattr(pipeline, "load_sheet_chunks", fake_load_sheet_chunks)

    expanded = await pipeline.gap_fill_one_sheet_gaps(
        [_candidate("sheet_10.1", score=0.8), _candidate("sheet_12.1", score=0.7)]
    )

    assert [chunk["chunk_id"] for chunk in expanded] == [
        "sheet_10.1",
        "sheet_11.1",
        "sheet_12.1",
    ]
    assert expanded[1]["is_gap_fill"] is True
    assert expanded[1]["match_sources"] == ["gap_fill"]


def test_parse_research_findings_enriches_source_references() -> None:
    """Findings get deterministic file references from source_ref_ids."""
    catalog = pipeline.build_source_catalog([_candidate("sheet_11.1")])
    findings = pipeline.parse_research_findings(
        [
            {
                "finding": "Revenue increased.",
                "page": 11,
                "location_detail": "sheet_11.1",
                "source_ref_ids": ["S1"],
                "metric_name": "Revenue",
                "metric_value": "100",
                "unit": "$MM",
                "period": "Q1 2026",
                "segment": "Enterprise",
            }
        ],
        catalog,
    )

    assert findings[0]["source_ref_ids"] == ["S1"]
    assert findings[0]["references"][0]["filename"] == "supp.xlsx"
    assert findings[0]["references"][0]["s3_key"] == "supp.xlsx"
    assert findings[0]["references"][0]["link_marker"] == (
        "{{S3_LINK:download:xlsx:supp.xlsx:CM Q1 2026 source}}"
    )


def test_parse_research_findings_falls_back_to_page_reference() -> None:
    """Missing source_ref_ids fall back to the cited page when possible."""
    catalog = pipeline.build_source_catalog([_candidate("sheet_11.1")])
    findings = pipeline.parse_research_findings(
        [
            {
                "finding": "Revenue increased.",
                "page": 11,
                "location_detail": "unmatched sheet label",
                "metric_name": "",
                "metric_value": "",
                "unit": "",
                "period": "",
                "segment": "",
            }
        ],
        catalog,
    )

    assert findings[0]["source_ref_ids"] == ["S1"]
    assert findings[0]["references"][0]["chunk_id"] == "sheet_11.1"


def test_format_retrieval_response_only_shows_findings_and_sources() -> None:
    """Dropdown output is research-focused and includes S3 source markers."""
    catalog = pipeline.build_source_catalog([_candidate("sheet_11.1")])
    finding = pipeline.parse_research_findings(
        [
            {
                "finding": "Revenue increased.",
                "page": 11,
                "location_detail": "sheet_11.1",
                "source_ref_ids": ["S1"],
                "metric_name": "Revenue",
                "metric_value": "100",
                "unit": "$MM",
                "period": "Q1 2026",
                "segment": "Enterprise",
            }
        ],
        catalog,
    )[0]
    output = pipeline.format_retrieval_response(
        {
            "combo_results": [
                {
                    "combo": {
                        "bank_symbol": "CM",
                        "bank_name": "CIBC",
                        "quarter": "Q1",
                        "fiscal_year": "2026",
                    },
                    "findings": [finding],
                }
            ]
        }
    )

    assert "## Research Findings" in output
    assert "Revenue increased." in output
    assert "{{S3_LINK:download:xlsx:supp.xlsx:CM Q1 2026 source}}" in output
    assert "Sheet: sheet_11.1" in output
    assert "Page 11" not in output
    assert "Chunk:" not in output
    assert "Query preparation" not in output
    assert "Evidence catalog" not in output
    assert "Pipeline counts" not in output


def test_format_retrieval_response_reports_no_content_for_empty_search() -> None:
    """No search candidates render as no available supplementary content."""
    output = pipeline.format_retrieval_response(
        {
            "combo_results": [
                {
                    "combo": {
                        "bank_symbol": "CM",
                        "bank_name": "CIBC",
                        "quarter": "Q1",
                        "fiscal_year": "2026",
                    },
                    "findings": [],
                    "metrics": {"skipped": "no_search_candidates"},
                }
            ]
        }
    )

    assert "No supplementary financials content was found for this bank/period." in output
