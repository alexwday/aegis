# Call Summary ETL - Findings

## Status Key
- `[ ]` = Not started
- `[~]` = In progress
- `[x]` = Complete
- `[--]` = Won't fix / Deferred

---

## Section A: Critical Severity

### A1. `groupby` Produces Fragmented Report Sections

- [x] **A1.1 - `itertools.groupby` splits non-consecutive report sections into duplicate headings**
  - **Severity**: Critical
  - **File**: `main.py:1125-1135`
  - `itertools.groupby` only groups *consecutive* elements with the same key. The sort key is `(report_section_priority, index)`, where priority is 0 for "Results Summary" and 1 for everything else. When there are 3+ distinct `report_section` values, non-"Results Summary" sections are sorted by index alone, which interleaves different sections.
  - **Example**: Categories with report_section/index values:
    ```
    Cat1: Results Summary, idx=1
    Cat2: Strategic Outlook, idx=2
    Cat3: Results Summary, idx=3
    Cat4: Risk Analysis, idx=4
    Cat5: Strategic Outlook, idx=5
    ```
    After sorting by `(priority, index)`: `(0,1) (0,3) (1,2) (1,4) (1,5)` produces section order: `Results, Results, Strategic, Risk, Strategic`. `groupby` outputs **two** "Strategic Outlook" sections.
  - **Fix**: Add `report_section_name` to sort tuple:
    ```python
    sorted_categories = sorted(
        valid_categories,
        key=lambda x: (
            0 if x.get("report_section", "Results Summary") == "Results Summary" else 1,
            x.get("report_section", "Results Summary"),
            x.get("index", 0),
        ),
    )
    ```

---

### A2. Unsanitized Transcript Text in `.format()` Calls

- [x] **A2.1 - Transcript content not escaped before `.format()`, causing crash on `{` or `}` characters**
  - **Severity**: Critical
  - **File**: `main.py:439`, `main.py:910`
  - Transcript content from the database is injected into prompt templates via `.format()` without escaping curly braces. `_sanitize_for_prompt()` exists for this exact purpose but is only applied to XLSX-sourced category data, not to transcript text.
  - **Code paths**:
    ```python
    # main.py:439 - research plan
    user_prompt = research_prompts["user_prompt_template"].format(
        transcript_text=transcript_text  # NOT sanitized
    )

    # main.py:910 - category extraction
    user_prompt = extraction_prompts["user_prompt"].format(
        formatted_section=formatted_section  # NOT sanitized
    )
    ```
  - If any transcript chunk contains `{variable_name}` or a lone `{`, `.format()` raises `KeyError` or `ValueError`, crashing the entire ETL run.
  - **Fix**: Wrap transcript text in `_sanitize_for_prompt()` before `.format()`.

---

## Section B: Medium Severity

### B1. Performance

- [x] **B1.1 - Diagnostic queries run on every invocation, not just on empty results**
  - **Severity**: Medium
  - **File**: `transcript_utils.py:225-241`
  - `get_filter_diagnostics()` executes 7 separate `COUNT(*)` queries against `aegis_transcripts` on **every** ETL run, regardless of whether the main query returns results. Adds ~0.5-2s of latency per run.
  - **Fix**: Only call `get_filter_diagnostics()` when the main query returns 0 results:
    ```python
    if len(chunks) == 0:
        diagnostics = await get_filter_diagnostics(combo, context)
    ```

---

### B2. Deduplication

- [x] **B2.1 - Similarity threshold of 0.75 not validated against financial text**
  - **Severity**: Medium
  - **File**: `main.py:70`
  - **Validated**: Tested against 10 realistic financial text pairs (5 true duplicates, 5 true distinct). Results:
    - At 0.75: 0/5 paraphrased duplicates caught, 0/5 false positives. Only catches near-verbatim text.
    - No threshold works: at 0.50 precision=40%, recall=40%. At 0.60+ false positives appear before any true duplicates are caught.
    - Root cause: `SequenceMatcher` string similarity cannot distinguish structurally similar financial statements (`"X revenue increased $Y to $Z"`) from actual paraphrased duplicates.
  - **Conclusion**: The 0.75 threshold is appropriate as a safety net for near-verbatim copies. Primary deduplication relies on the research plan's `cross_category_notes` guiding the LLM to avoid overlap at extraction time. No change needed.

---

### B3. Error Handling

- [x] **B3.1 - `retrieve_full_section` returns empty list on database error, masking failures**
  - **Severity**: Medium
  - **File**: `transcript_utils.py:254-256`
  - Database errors (connection timeout, query failure) are caught and return an empty list. A transient database failure produces the same result as "no data found" - the ETL raises `ValueError: No transcript chunks found` with no indication the database was unreachable.
  - **Code**:
    ```python
    except Exception as e:
        logger.error("etl.call_summary.full_section_error", ...)
        return []  # Database error looks identical to "no data"
    ```
  - **Fix**: Let database exceptions propagate:
    ```python
    except Exception as e:
        raise RuntimeError(f"Database error retrieving transcripts: {e}") from e
    ```

- [x] **B3.2 - `_insert_toc_at_position` silently swallows all exceptions**
  - **Severity**: Medium
  - **File**: `main.py:1087-1092`
  - A bare `except Exception` catches all errors during TOC XML manipulation and returns `False`. Programming errors (AttributeError, TypeError) in the XML code never surface - they just produce a document with the TOC appended at the end instead of inserted at the correct position.
  - **Status**: Already has `logger.warning()` with error details. Graceful degradation (TOC at end) is acceptable for a non-critical formatting operation.

---

### B4. Configuration

- [x] **B4.1 - `ETLConfig.get_model()` has no validation that referenced tiers exist**
  - **Severity**: Medium
  - **File**: `main.py:73-151`
  - `ETLConfig.get_model()` expects a tier string in YAML but hardcodes tier_map. No validation that YAML model tiers exist in the global config. Silent failure if config is malformed.
  - **Fix**: Add tier validation during config loading.

- [x] **B4.2 - Similarity threshold and retry parameters hardcoded**
  - **Severity**: Medium
  - **Files**: `main.py:70` (SIMILARITY_THRESHOLD=0.75), `main.py` (MAX_RETRIES=3, backoff formula)
  - These operational parameters require code changes to adjust. Should be configurable via `config.yaml`.
  - **Fix**: Move to config.yaml with sensible defaults.

---

## Section C: Low Severity

### C1. Code Quality

- [x] **C1.1 - Config YAML comment contradicts tier value**
  - **Severity**: Low
  - **File**: `config/config.yaml:8`
  - Comment says "Uses config.llm.large.model" but tier is "medium".
  - **Fix**: Change comment to `# Uses config.llm.medium.model`.

- [x] **C1.2 - `_extract_single_category` has excessive parameter count (10+ arguments)**
  - **Severity**: Low
  - **File**: `main.py:814-822`
  - **Status**: Already reasonable — function has 7 arguments with `etl_context` dict bundling related values. No change needed.

- [x] **C1.3 - `_insert_toc_at_position` hardcodes "last 3 paragraphs" assumption**
  - **Severity**: Low
  - **File**: `main.py:1070-1079`
  - **Status**: Already fixed. Uses `paras_before` count and `all_paras[paras_before:]` slice to dynamically determine which paragraphs were added by `add_table_of_contents`.

- [x] **C1.4 - `auto_bold_metrics` misses negative dollar amounts**
  - **Severity**: Low
  - **File**: `document_converter.py:240-245`
  - Metric patterns don't account for negative signs. `-$1.2 BN` is matched as `$1.2 BN` (without the minus), producing `-**$1.2 BN**` instead of `**-$1.2 BN**`.
  - **Fix**: Add optional `-?` prefix to dollar amount patterns.

---

## Section D: Architecture / SOTA Improvements

### D1. Pipeline Architecture

- [--] **D1.1 - Extractive-then-abstractive pipeline**
  - Current system asks the LLM to both identify relevant content AND synthesize it in one call. SOTA extraction systems separate these: first extract exact spans from the source (high recall), then synthesize extracted spans into statements (high precision). Would solve the deduplication problem at source - extract spans first, deduplicate spans, then synthesize.
  - **Assessment**: Deferred. The current prompt-level dedup via `cross_category_notes` handles cross-category overlap effectively. An extractive step would add 15 extra LLM calls with unclear quality improvement — and B2.1 validation showed string-level dedup is fundamentally limited for financial text regardless of when it's applied. Revisit if output quality audits reveal systematic cross-category duplication.

---

### D2. Output Quality

- [x] **D2.1 - Structured output validation with Pydantic**
  - Instead of parsing raw JSON from tool calls and hoping for correct schema, use OpenAI's structured output mode or validate with Pydantic models. Catches schema violations (missing fields, wrong types) at parse time instead of at document generation time.
  - **Implemented**: Added `ResearchPlanResponse`, `CategoryExtractionResponse`, `CategoryPlan`, `SummaryStatement`, and `Evidence` Pydantic models. Both LLM response parse points now validate via `model_validate()` before proceeding. `ValidationError` added to retry-eligible exceptions.

---

### D3. Test Coverage Gaps

- [x] **D3.1 - No mock-based tests for LLM interaction paths**
  - `_generate_research_plan`, `_extract_single_category`, `_process_categories` have no test coverage. LLM response parsing, retry logic, and error handling are untested.
  - **Implemented**: `test_llm_interactions.py` with 9 tests covering happy path, parse-error retries, transport-error backoff, Pydantic validation, fallback extraction, rejection on exhausted retries, and title defaults.

- [x] **D3.2 - No tests for `_save_to_database`**
  - Database transaction logic, DELETE+INSERT atomicity, and error handling are untested.
  - **Implemented**: `test_save_to_database.py` with 4 tests covering successful DELETE+INSERT, parameter correctness, existing report replacement, and SQLAlchemy error propagation.

- [x] **D3.3 - No integration test for `generate_call_summary`**
  - The end-to-end flow including error handling paths is untested.
  - **Implemented**: `test_integration.py` with 5 tests covering successful end-to-end, auth failure, empty transcript, invalid bank, and all-categories-rejected error paths.

---

## Priority Order

### Phase 1: Critical Bug Fixes
1. **A1.1** - Fix `groupby` fragmentation (1-line fix)
2. **A2.1** - Sanitize transcript text in `.format()` calls (2-line fix)

### Phase 2: Medium Severity
3. **B3.1** - Propagate database errors instead of swallowing
4. **B1.1** - Move diagnostic queries to after empty-result check
5. **B3.2** - Stop swallowing TOC insertion exceptions
6. **B4.1** - Add config tier validation
7. **B4.2** - Move operational parameters to config.yaml
8. **B2.1** - Validate similarity threshold against real data

### Phase 3: Low Severity & Code Quality
9. **C1.1** - Fix config YAML comment
10. **C1.2** - Reduce `_extract_single_category` complexity
11. **C1.3** - Fix hardcoded TOC paragraph count
12. **C1.4** - Handle negative dollar amounts in auto-bold

### Phase 4: SOTA Improvements
13. **D1.1** - Extractive-then-abstractive pipeline
14. **D2.1** - Structured output validation

### Phase 5: Test Coverage
15. **D3.1** - Mock-based LLM interaction tests
16. **D3.2** - Database save tests
17. **D3.3** - End-to-end integration test
