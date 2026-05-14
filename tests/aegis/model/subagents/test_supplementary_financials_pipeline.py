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

    async def fake_count_available_chunks(combo: dict) -> int:
        nonlocal active_combos, max_active_combos, mixed_periods
        period = (combo["fiscal_year"], combo["quarter"])
        active_combos += 1
        max_active_combos = max(max_active_combos, active_combos)
        active_period_counts[period] = active_period_counts.get(period, 0) + 1
        if len([count for count in active_period_counts.values() if count]) > 1:
            mixed_periods = True
        await asyncio.sleep(0.01)
        active_period_counts[period] -= 1
        active_combos -= 1
        return 1

    async def fake_multi_strategy_search(**_kwargs: object) -> list[dict]:
        return [_candidate("sheet_1.1", score=0.5)]

    async def fake_rerank_candidates(**kwargs: object) -> list[dict]:
        return list(kwargs["candidates"])

    async def fake_gap_fill_one_sheet_gaps(
        chunks: list[dict],
        search_semaphore: asyncio.Semaphore | None = None,
    ) -> list[dict]:
        _ = search_semaphore
        return chunks

    async def fake_run_research_loop(**kwargs: object) -> dict:
        return {"chunks": kwargs["initial_chunks"], "findings": [], "iterations": []}

    monkeypatch.setattr(pipeline, "prepare_query", fake_prepare_query)
    monkeypatch.setattr(pipeline, "count_available_chunks", fake_count_available_chunks)
    monkeypatch.setattr(pipeline, "multi_strategy_search", fake_multi_strategy_search)
    monkeypatch.setattr(pipeline, "rerank_candidates", fake_rerank_candidates)
    monkeypatch.setattr(pipeline, "gap_fill_one_sheet_gaps", fake_gap_fill_one_sheet_gaps)
    monkeypatch.setattr(pipeline, "run_research_loop", fake_run_research_loop)

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


def test_apply_min_keep_floor_restores_highest_scoring_removals() -> None:
    """Rerank cannot remove below the configured keep floor."""
    candidates = [_candidate(f"sheet_{index}.1", score=index) for index in range(12)]
    remove_set = set(range(12))

    adjusted = pipeline.apply_min_keep_floor(candidates, remove_set)

    kept = [index for index in range(12) if index not in adjusted]
    assert len(kept) == pipeline.RERANK_MIN_KEEP
    assert kept == list(range(2, 12))


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
    assert findings[0]["references"][0]["link_marker"].startswith("{{S3_LINK:download:xlsx:")


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
    assert "{{S3_LINK:download:xlsx:supp.xlsx:" in output
    assert "Query preparation" not in output
    assert "Evidence catalog" not in output
    assert "Pipeline counts" not in output
