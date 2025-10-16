# Supplementary Subagent Issue: Complete Analysis & Fix

## 🚨 Executive Summary

**Your Question:** "Can we change the queue timeout from 0.1 to 5.0 seconds because supplementary takes time to reach the queue?"

**Answer:** ❌ **NO** - This won't fix your problem and is based on a misunderstanding of the architecture.

**The Real Issue:** Your supplementary subagent violates Aegis's async generator pattern. It blocks for the entire pipeline duration (potentially 100+ seconds) and only yields ONE message at the very end, causing the UI to see nothing during execution.

---

## 🔍 Root Cause Analysis

### Issue #1: Wrapper Blocks Instead of Streaming
**Location:** `main.py:114-147`

```python
# ❌ WRONG: Your current implementation
async def supplementary_agent(...) -> AsyncGenerator[Dict[str, str], None]:
    # ... setup code ...

    # Blocks waiting for entire pipeline (100+ seconds)
    result = await run_pipeline(
        user_query=user_query,
        db=db,
        chat_model=chat_model,
        # ... all params
    )

    # Only yields AFTER pipeline completes
    yield {
        "type": "subagent",
        "name": database_id,
        "content": f"✓ Analysis complete! {result['final_assessments']}"
    }
```

**Problem:**
- UI sees **nothing** for 100+ seconds
- Then suddenly gets **one message** at the end
- If pipeline takes >200s, it gets cancelled and **no message ever arrives**

---

### Issue #2: Pipeline Returns Dict Instead of Yielding
**Location:** `Benchmarking_Pipeline.py:303-491`

```python
# ❌ WRONG: Returns dict at end
async def run_pipeline(...) -> Dict[str, Any]:
    results_dict = {
        "final_assessments": [],
        "errors": []
    }

    # ... 100+ seconds of processing ...

    return results_dict  # ❌ Returns once at the end
```

**Problem:**
- Should be an **async generator** that yields chunks as work progresses
- Not a function that returns once at the end

---

### Issue #3: Non-Streaming LLM Calls
**Location:** `Benchmarking_Pipeline.py:250`

```python
# ❌ WRONG: Non-streaming LLM call
responses_final = await chat_model.client.general_concurrent_call(
    final_calls,
    prompt_context + prompts[config.final_call_prompt_key],
    model_name,
    conversation_id,
    stage,
    query_id,
    use_response_format=False  # Not streaming!
)
```

**Critical Finding:** You imported Aegis's `stream()` function (main.py:33) but **never use it**!

```python
from ....connections.llm_connector import stream  # ← Imported but NEVER USED!
```

You even left a note about it (main.py:229):
```python
# NOTES FOR REPLACING THIS IMPLEMENTATION:
# ...
# — stream() for LLM calls
```

---

## 📊 Evidence Table

| File | Line | Issue | Impact |
|------|------|-------|--------|
| main.py | 33 | Import `stream` but don't use | Have tool, don't use it |
| main.py | 114-141 | `await run_pipeline()` blocks | No streaming to UI |
| main.py | 147 | Single yield at end | Only 1 message after 100s |
| main.py | 229 | Note about using `stream()` | Knew what to do, didn't implement |
| Benchmarking_Pipeline.py | 303 | `-> Dict[str, Any]` return type | Not async generator |
| Benchmarking_Pipeline.py | 491 | `return results_dict` | Returns once at end |
| Benchmarking_Pipeline.py | 250 | `general_concurrent_call()` | Non-streaming LLM call |
| llm_handler.py | 20-38 | Custom ChatModelService | Not using Aegis LLM connector |

---

## ❌ Why Your Proposed Fix Won't Work

**Your proposal:**
```python
if all(task.done() for task in tasks) and output_queue.empty():
    break
```

**Why it fails:**
1. ⚠️ **Race condition:** `queue.empty()` can become false immediately after checking
2. ❌ **Doesn't address root cause:** Messages aren't being put in queue incrementally - they're only yielded once at the very end
3. 🐌 **No performance gain:** The 0.1s timeout is just polling frequency, not execution timeout
4. 🐛 **Creates bugs:** Could exit loop before processing all messages

**The current Aegis code is correct.** The issue is in your subagent implementation, not in Aegis's queue processing logic.

---

## ✅ The Complete Fix

### Fix 1: Convert Wrapper to Stream from Pipeline

**File:** `main.py`

```python
async def supplementary_agent(
    bank_period_combinations: List[Dict[str, Any]],
    context: Dict[str, Any],
    conversation: List[Dict[str, str]],
    latest_message: str,
    available_databases: Optional[List[str]] = None,
    database_id: str = "supplementary",
    db_names: Optional[List[str]] = None,
) -> AsyncGenerator[Dict[str, str], None]:
    """
    Supplementary subagent - streams results incrementally.
    """
    try:
        # Setup (same as before)
        execution_id = context.get("execution_id")
        stage_start = datetime.now(timezone.utc)
        logger = get_logger(__name__)

        # Log start
        logger.info(
            f"subagent.{database_id}.started",
            execution_id=execution_id,
            num_banks=len(bank_period_combinations),
        )

        # ✅ NEW: Stream results from pipeline as they arrive
        async for chunk in run_pipeline_streaming(
            user_query=latest_message,
            db=db,
            chat_model=chat_model,
            user_id=config.user_id,
            model_name=config.model_name,
            model_name_extract=config.model_name_extract,
            prompt_dir=config.prompt_dir,
            conversation=conversation,
            bank_period_combinations=bank_period_combinations,
            context=context,
            conversation_context=conversation_context,
            bank_period_context=bank_period_context
        ):
            # Yield each chunk immediately
            yield {
                "type": "subagent",
                "name": database_id,
                "content": chunk
            }

        # Success monitoring
        stage_end = datetime.now(timezone.utc)
        add_monitor_entry(
            stage_name="Subagent_Supplementary",
            stage_start_time=stage_start,
            stage_end_time=stage_end,
            status="Success",
            decision_details=f"Generated supplementary data for {len(bank_period_combinations)} bank-period combinations",
            custom_metadata={
                "subagent": database_id,
                "banks": [combo["bank_id"] for combo in bank_period_combinations],
            },
        )

        logger.info(
            f"subagent.{database_id}.completed",
            execution_id=execution_id,
            total_duration_ms=int((stage_end - stage_start).total_seconds() * 1000),
        )

    except Exception as e:
        error_msg = str(e)
        logger.error(
            f"subagent.{database_id}.error",
            execution_id=execution_id,
            error=error_msg,
            exc_info=True,
        )

        # Log failure
        add_monitor_entry(
            stage_name="Subagent_Supplementary",
            stage_start_time=stage_start,
            stage_end_time=datetime.now(timezone.utc),
            status="Failure",
            error_message=error_msg,
            custom_metadata={
                "subagent": database_id,
                "error_type": type(e).__name__,
            },
        )

        # Yield error
        yield {
            "type": "subagent",
            "name": database_id,
            "content": f"⚠️ Error in Supplementary subagent: {error_msg}\n",
        }
```

---

### Fix 2: Create Streaming Version of Pipeline

**File:** `Benchmarking_Pipeline.py`

Add this new function (keep the old `run_pipeline` for now for backwards compatibility):

```python
async def run_pipeline_streaming(
    user_query: str,
    db: PostgresHelper,
    chat_model: ChatModelService,
    user_id: str,
    model_name: str,
    model_name_extract: str,
    prompt_dir: str,
    conversation: List[Dict[str, str]],
    bank_period_combinations: List[Dict[str, Any]],
    context: Dict[str, Any],
    conversation_context: str,
    bank_period_context: str
) -> AsyncGenerator[str, None]:
    """
    Streaming version of pipeline that yields incremental results.

    This function follows Aegis's async generator pattern, yielding
    progress updates and results as they become available instead of
    blocking until completion.

    Yields:
        str: Progress updates and final analysis chunks
    """
    try:
        # Load resources
        yield "🔍 Initializing supplementary analysis...\n"

        prompt_dir = os.path.join(os.path.dirname(__file__), "prompts", "prompts")
        BANK_MAPPING_PATH = os.path.join(prompt_dir, "bank_mapping.yaml")
        symbol_to_canonical = load_bank_mapping(BANK_MAPPING_PATH)
        normalized_bank_period_combinations = normalize_bank_period_combinations(
            bank_period_combinations, symbol_to_canonical
        )
        prompts = load_prompts(prompt_dir, chat_model)
        prompt_context = build_prompt_context(user_query, conversation_context, bank_period_context)
        df_local = db.fetch_data(config.kpi_metadata_query)
        kpi_metadata = df_to_kpi_metadata(df_local)

        # Conversation start
        query_id = str(uuid.uuid4())
        results_dict = {"query_id": query_id}
        logger_db.log_user_message(conversation_id, user_id, user_query)
        logger_db.log_query_start(query_id, conversation_id, user_query)

        # ✅ Step 1: Initial filter
        yield "\n📋 Validating query scope...\n"
        r = await step_initial_filter(
            chat_model, user_query, prompt_context, prompts,
            model_name, conversation_id, query_id, logger_db
        )
        if r is None:
            yield "⚠️ This query cannot be answered with supplementary data.\n"
            results_dict["final_assessments"].append(
                "Sorry, This query can't be answered! Please check the Bank, Parameter, Platform to get answer"
            )
            logger_db.log_conversation_end(conversation_id)
            return

        if not r:
            yield "⚠️ Cannot answer. Please check the Bank, Parameter, Platform.\n"
            results_dict["final_assessments"].append("cannot answer, Please check the Bank, Parameter, Platform to get answer")
            logger_db.log_conversation_end(conversation_id)
            return

        yield f"✓ Query validated ({len(r)} sub-queries identified)\n"

        # ✅ Step 2: Parameter extraction
        yield "\n📊 Extracting KPIs from query...\n"
        results = await step_parameter_extraction(
            chat_model, r, model_name, conversation_id, query_id, logger_db
        )
        default_kpi_mapping = prompts['default_kpis']

        if not results or all(not r.get('kpis') for r in results):
            # Try generic terms
            generic_terms = list(default_kpi_mapping.keys())
            selected_defaults = []
            for term in generic_terms:
                if term in user_query.lower():
                    selected_defaults = default_kpi_mapping.get(term, [])
                    break

            if not selected_defaults:
                selected_defaults = ["Net Income", "Return on Equity", "Provisions for Credit Losses", "Core Cash Diluted EPS"]

            results = [{
                "User Query": r_item,
                "KPIs": selected_defaults,
                "Platforms": [],
                "bank_period_quarters": quarters_str
            } for r_item in r]

        yield f"✓ Found {len(results)} KPI groups to analyze\n"

        # ✅ Step 3: KPI metadata
        yield "\n📋 Retrieving KPI metadata from database...\n"
        responses = await step_kpi_response(results, kpi_metadata, chat_model, prompts, model_name, conversation_id, query_id, logger_db)

        all_kpi_meta = [kpi_meta_dict.get(query_item, []) for query_item in r]
        yield f"✓ Retrieved metadata for {sum(len(meta) for meta in all_kpi_meta)} KPIs\n"

        # ✅ Step 4: SQL generation and execution
        yield "\n🔎 Generating and executing database queries...\n"
        questions = []
        for query_item, result in zip(r, results):
            kpi_sections = []
            for kpi_name, candidates in kpi_meta_dict.items():
                kpi_sections.append(f"{kpi_name}:\n{json.dumps(candidates, indent=2)}")
            questions.append(
                f"- User Query: {query_item}\n"
                f"- Extracted Platform: {result['platforms']}\n"
                f"- The KPIs and their candidate matches:\n{chr(10).join(kpi_sections)}"
            )

        results_db = await step_sql_generation(
            chat_model, questions_sql, prompts, model_name, conversation_id,
            query_id, logger_db, db
        )

        if not results_db:
            yield "⚠️ No data found in database\n"
            logger_db.log_conversation_end(conversation_id)
            return

        yield f"✓ Retrieved data for {len(results_db)} metrics\n"

        # ✅ Step 5: STREAMING final LLM call
        yield "\n💬 Synthesizing comprehensive analysis...\n\n"

        # Use streaming LLM call
        async for assessment_chunk in step_final_llm_call_streaming(
            chat_model, r, all_kpi_meta, results_db, all_kpi_alternatives,
            prompt_context, prompts, model_name, conversation_id, query_id,
            logger_db, context
        ):
            yield assessment_chunk  # Stream LLM response in real-time

        yield "\n\n✓ Analysis complete!\n"

        logger_db.log_conversation_end(conversation_id)

    except Exception as e:
        logger_db.log_error(conversation_id, query_id, str(e))
        yield f"\n⚠️ Error during analysis: {str(e)}\n"
```

---

### Fix 3: Create Streaming LLM Call

**File:** `Benchmarking_Pipeline.py`

Add this new function:

```python
async def step_final_llm_call_streaming(
    chat_model,
    r: List[str],
    all_kpi_meta: List[List[Dict]],
    results_db: List[Any],
    all_kpi_alternatives: List[List[Dict]],
    prompt_context: str,
    prompts: Dict,
    model_name: str,
    conversation_id: str,
    query_id: str,
    logger_db,
    context: Dict[str, Any]
) -> AsyncGenerator[str, None]:
    """
    Streaming version of final LLM call.

    Uses Aegis's stream() function to yield LLM response chunks
    as they arrive instead of waiting for the complete response.

    Yields:
        str: Chunks of the LLM response as they arrive
    """
    # Build prompts (same as before)
    final_calls = []
    all_top_alts = []

    for i, (r_item, kpi_meta, db_result) in enumerate(zip(r, all_kpi_meta, results_db)):
        final_calls.append(f"""
- User Query: {r_item}
- KPI Metadata:
{json.dumps([kpi_meta], indent=2)}
- Dataset to answer user query:
{db_result.to_csv(index=False)}
""")

        if all_kpi_alternatives[i]:
            top_alts = [alt for alt in all_kpi_alternatives[i] if alt.get('Score', 0) >= 0.7]
            all_top_alts.extend(top_alts)

    # Build alternative KPIs text
    if all_top_alts:
        # Deduplicate by (KPI, Platform)
        seen = set()
        unique_alts = []
        for alt in all_top_alts:
            key = (alt.get('KPI'), alt.get('Platform'))
            if key not in seen:
                seen.add(key)
                unique_alts.append(alt)

        alt_text = (
            "\n<br/>**Related KPIs**<br/>\n"
            "Other KPIs you may be interested in:<br/>\n"
            + "<br/>\n".join([
                f"- {alt['KPI']} (Platform: {alt.get('Platform','')}, Score: {alt['Score']})"
                for alt in unique_alts
            ])
            + "<br/><br/>If you are interested in these, please ask!"
        )
    else:
        alt_text = ""

    # ✅ NEW: Use Aegis's stream() function
    from ....connections.llm_connector import stream
    from ....utils.settings import config as aegis_config

    # Build messages for streaming call
    system_prompt = prompt_context + prompts[config.final_call_prompt_key]
    user_prompt = "\n\n".join(final_calls)

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]

    # Context for Aegis LLM connector
    llm_context = {
        "execution_id": context.get("execution_id"),
        "auth_config": context.get("auth_config"),
        "ssl_config": context.get("ssl_config")
    }

    # LLM parameters
    llm_params = {
        "model": model_name,
        "temperature": chat_model.temperature,
        "max_tokens": 4000
    }

    # ✅ Stream the response
    stage = "final_call"
    accumulated_response = ""

    try:
        # Start timing for monitoring
        start_time = datetime.now()

        async for chunk in stream(messages, llm_context, llm_params):
            accumulated_response += chunk
            yield chunk  # Yield each token as it arrives

        # Add alternative KPIs at the end
        if alt_text:
            yield alt_text
            accumulated_response += alt_text

        # End timing
        end_time = datetime.now()
        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        # Log the LLM call
        logger_db.log_llm_step(
            query_id=query_id,
            step_name=stage,
            prompt=user_prompt[:1000],  # Truncate for logging
            llm_response=accumulated_response,
            parsed_response=accumulated_response,
            model_name=model_name,
            duration=duration_ms,
            tokens={'prompt': None, 'completion': None, 'total': None},
            cost=None,
            error=None
        )

    except Exception as e:
        error_msg = str(e)
        logger_db.log_error(conversation_id, query_id, error_msg)
        yield f"\n⚠️ Error in LLM synthesis: {error_msg}\n"
```

---

## 📋 Implementation Checklist

Tell your developer to:

- [ ] **DO NOT** change the timeout from 0.1 to 5.0 - this won't fix the issue
- [ ] **DO NOT** modify Aegis's queue processing logic - it's working correctly
- [ ] Create `run_pipeline_streaming()` function as shown above
- [ ] Create `step_final_llm_call_streaming()` function as shown above
- [ ] Modify wrapper to use `async for chunk in run_pipeline_streaming(...)`
- [ ] Use Aegis's `stream()` function (already imported on line 33!)
- [ ] Test incrementally - should see messages arrive every few seconds, not all at once
- [ ] Verify monitoring - should see "Success" status in database after completion
- [ ] Remove `print()` debug statements from wrapper (lines 79-87, 139, 146, 162, 212)

---

## 🎯 Expected Behavior After Fix

| Before (Current) | After (Fixed) |
|------------------|---------------|
| 🔴 100+ seconds of silence | ✅ Updates every 2-3 seconds |
| 🔴 ONE message at end | ✅ 10+ incremental messages |
| 🔴 May timeout after 200s | ✅ Streams within timeout |
| 🔴 User sees nothing during work | ✅ User sees: "Validating...", "Extracting KPIs...", "Querying database...", "Synthesizing...", then streaming analysis |
| 🔴 "Lost" messages | ✅ All messages arrive immediately |

---

## 🔬 Understanding the 0.1s Timeout (Not Your Issue)

The 0.1s timeout in Aegis's main.py is the **queue polling interval**, not an execution timeout:

```python
# In Aegis main.py:791
try:
    msg = await asyncio.wait_for(output_queue.get(), timeout=0.1)
    # Process message
except asyncio.TimeoutError:
    # Queue empty - check if all tasks done, then loop again
    if all(task.done() for task in tasks):
        break
```

**What this does:**
- Every 0.1 seconds, check if any subagent put a message in the queue
- If queue is empty, timeout and loop back to check again
- This creates a responsive streaming experience (100ms latency)

**What happens in your case:**
1. Your subagent doesn't put anything in queue for 100+ seconds (because it's blocking)
2. Aegis loops checking queue every 0.1s (finds nothing)
3. After 100+ seconds, you finally yield one message
4. Aegis immediately processes it (within 0.1s)
5. But user has been staring at blank screen for 100+ seconds!

**If you changed it to 5.0s:**
- Still wouldn't help (your subagent still not yielding anything)
- Would make ALL subagents sluggish (5 second delay between chunks)
- Would hurt UX for properly-implemented subagents (transcripts, reports)

---

## 🔑 Key Takeaways

1. **The 0.1s timeout is NOT your problem** - it's just polling frequency
2. **Your subagent architecture is incompatible with Aegis** - must use async generator pattern
3. **You already have the tools you need** - `stream()` is imported but unused
4. **The fix is straightforward** - convert blocking calls to yielding generators
5. **Aegis's code is correct** - don't modify the queue processing logic

---

## 📞 Questions?

If you need help implementing these changes, please provide:
- Your specific error messages (if any)
- Performance metrics (how long each step takes)
- Whether you're seeing partial output or no output at all

The Aegis architecture is designed to support exactly what you're trying to do - you just need to follow the async generator pattern consistently throughout your code.

---

**Document Version:** 1.0
**Created:** 2025-10-16
**Author:** Aegis Architecture Team
