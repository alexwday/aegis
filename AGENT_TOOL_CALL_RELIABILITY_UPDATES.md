# Agent Tool-Call Reliability Updates

This document captures the before/after code changes and prompt updates to improve tool-call reliability across all Aegis agents.

## Summary of Changes

All agents that use `complete_with_tools()` have been updated to:
1. **Require tool calls** via `tool_choice: "required"`
2. **Retry failed tool calls** up to 3 attempts (initial + 2 retries)
3. **Gracefully fallback** instead of returning errors
4. **Handle JSON parse errors** for malformed tool arguments

---

## 1. Router Agent (`src/aegis/model/agents/router.py`)

### Before

```python
        # Call LLM with tools
        response = await complete_with_tools(
            messages=messages,
            tools=tools,
            context=context,
            llm_params={
                "model": model,
                "temperature": 0.1,  # Very low temperature for deterministic binary routing
                "max_tokens": max_tokens,
            },
        )

        # Extract metrics from response
        metrics = response.get("metrics", {})
        usage = response.get("usage", {})

        # Extract tool call response
        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]
            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_args = json.loads(tool_call["function"]["arguments"])

                # Handle binary response (property name: routing_decision)
                binary_route = function_args.get("routing_decision")
                route = "direct_response" if binary_route == 0 else "research_workflow"

                # Generate simple rationale based on route
                if binary_route == 0:
                    rationale = "Direct response from conversation history"
                else:
                    rationale = "Data retrieval required"

                logger.info(
                    "router.decision",
                    execution_id=execution_id,
                    route=route,
                    binary=binary_route,
                    rationale=rationale,
                    tokens_used=usage.get("total_tokens", 0),
                    cost=metrics.get("total_cost", 0),
                )

                return {
                    "status": "Success",
                    "route": route,
                    "rationale": rationale,
                    "tokens_used": usage.get("total_tokens", 0),
                    "cost": metrics.get("total_cost", 0),
                    "response_time_ms": metrics.get("response_time", 0) * 1000,
                    "model_used": model,
                    "prompt_version": prompt_version,
                    "prompt_last_updated": prompt_last_updated,
                }

    # ... exception handling ...

    # Default to research workflow if no tool response
    logger.warning("router.default", execution_id=execution_id)
    return {
        "status": "Success",
        "route": "research_workflow",
        "rationale": "Defaulting to research workflow - no clear routing decision",
        ...
    }
```

### After

```python
        # Attempt tool call up to 3 times (initial + 2 retries)
        max_attempts = 3
        tokens_used = 0
        cost = 0
        last_error_reason = None

        for attempt in range(1, max_attempts + 1):
            response = await complete_with_tools(
                messages=messages,
                tools=tools,
                context=context,
                llm_params={
                    "model": model,
                    "temperature": 0.1,  # Very low temperature for deterministic routing
                    "max_tokens": max_tokens,
                    "tool_choice": "required",  # Force tool use
                },
            )

            # Extract metrics from response
            metrics = response.get("metrics", {})
            usage = response.get("usage", {})
            tokens_used = usage.get("total_tokens", 0)
            cost = metrics.get("total_cost", 0)

            # Extract tool call response
            if response.get("choices") and response["choices"][0].get("message"):
                message = response["choices"][0]["message"]
                if message.get("tool_calls"):
                    tool_call = message["tool_calls"][0]
                    try:
                        function_args = json.loads(tool_call["function"]["arguments"])
                    except (TypeError, ValueError) as e:
                        last_error_reason = f"tool_args_parse_error: {e}"
                    else:
                        # Handle binary response (property name: routing_decision)
                        binary_route = function_args.get("routing_decision")
                        route = "direct_response" if binary_route == 0 else "research_workflow"

                        # Generate simple rationale based on route
                        if binary_route == 0:
                            rationale = "Direct response from conversation history"
                        else:
                            rationale = "Data retrieval required"

                        logger.info(
                            "router.decision",
                            execution_id=execution_id,
                            route=route,
                            binary=binary_route,
                            rationale=rationale,
                            tokens_used=tokens_used,
                            cost=cost,
                        )

                        return {
                            "status": "Success",
                            "route": route,
                            "rationale": rationale,
                            "tokens_used": tokens_used,
                            "cost": cost,
                            "response_time_ms": metrics.get("response_time", 0) * 1000,
                            "model_used": model,
                            "prompt_version": prompt_version,
                            "prompt_last_updated": prompt_last_updated,
                        }
                else:
                    last_error_reason = "no_tool_calls"
            else:
                last_error_reason = "no_message"

            if attempt < max_attempts:
                logger.warning(
                    "router.retrying_tool_call",
                    execution_id=execution_id,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    reason=last_error_reason,
                )
                continue

        # Fallback to research workflow after retries (safe default)
        logger.warning(
            "router.fallback_to_research",
            execution_id=execution_id,
            reason=last_error_reason,
        )
        return {
            "status": "Success",
            "route": "research_workflow",
            "rationale": "Defaulting to research workflow - no clear routing decision",
            "tokens_used": tokens_used,
            "cost": cost,
            "response_time_ms": (
                metrics.get("response_time", 0) * 1000 if "metrics" in locals() else 0
            ),
            "model_used": model,
            "prompt_version": prompt_version,
            "prompt_last_updated": prompt_last_updated,
        }
```

---

## 2. Clarifier Agent - extract_banks (`src/aegis/model/agents/clarifier.py`)

### Before

```python
        response = await complete_with_tools(
            messages=llm_messages,
            tools=tools,
            context=context,
            llm_params={
                "model": model,
                "temperature": 0.1,  # Low temperature for consistent extraction
                "max_tokens": max_tokens,
            },
        )

        # Extract metrics
        metrics = response.get("metrics", {})
        usage = response.get("usage", {})
        tokens_used = usage.get("total_tokens", 0)
        cost = metrics.get("total_cost", 0)

        # Process tool response
        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]

            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_name = tool_call["function"]["name"]
                function_args = json.loads(tool_call["function"]["arguments"])

                if function_name == "banks_found":
                    bank_ids = function_args.get("bank_ids", [])
                    query_intent = function_args.get("query_intent", "")

                    # Validate query_intent is provided (required field in tool)
                    if not query_intent:
                        logger.error(...)
                        return {
                            "status": "error",
                            "error": "LLM failed to provide query_intent in banks_found tool call",
                            ...
                        }

                    # ... validation and success logic ...

                elif function_name == "clarification_needed":
                    # ... clarification logic ...

        # No tool response - this is an error (LLM should have called a tool)
        logger.error(
            "clarifier.banks.no_tool_response",
            execution_id=execution_id,
            error="LLM did not call any tool",
        )
        return {
            "status": "error",
            "error": "LLM failed to call banks_found or clarification_needed tool",
            "tokens_used": tokens_used,
            "cost": cost,
        }
```

### After

```python
        # Attempt tool call up to 3 times (initial + 2 retries)
        max_attempts = 3
        tokens_used = 0
        cost = 0
        last_error_reason = None

        for attempt in range(1, max_attempts + 1):
            response = await complete_with_tools(
                messages=llm_messages,
                tools=tools,
                context=context,
                llm_params={
                    "model": model,
                    "temperature": 0.1,  # Low temperature for consistent extraction
                    "max_tokens": max_tokens,
                    "tool_choice": "required",  # Force tool use
                },
            )

            # Extract metrics
            metrics = response.get("metrics", {})
            usage = response.get("usage", {})
            tokens_used = usage.get("total_tokens", 0)
            cost = metrics.get("total_cost", 0)

            # Process tool response
            if response.get("choices") and response["choices"][0].get("message"):
                message = response["choices"][0]["message"]

                if message.get("tool_calls"):
                    tool_call = message["tool_calls"][0]
                    function_name = tool_call["function"]["name"]
                    try:
                        function_args = json.loads(tool_call["function"]["arguments"])
                    except (TypeError, ValueError) as e:
                        last_error_reason = f"tool_args_parse_error: {e}"
                    else:

                        if function_name == "banks_found":
                            bank_ids = function_args.get("bank_ids", [])
                            query_intent = function_args.get("query_intent", "")

                            # Validate query_intent is provided (required field in tool)
                            if not query_intent:
                                last_error_reason = "missing_query_intent"
                                # Continue to retry - LLM may provide it on next attempt
                            else:
                                # Validate bank IDs exist in our data (filter to valid only)
                                valid_ids = [
                                    bid for bid in bank_ids if bid in banks_data["banks"]
                                ]
                                invalid_ids = [
                                    bid for bid in bank_ids if bid not in banks_data["banks"]
                                ]

                                if invalid_ids:
                                    logger.warning(...)

                                # If no valid banks after filtering, retry
                                if not valid_ids:
                                    last_error_reason = "no_valid_banks_hallucination"
                                else:
                                    # Success - return validated banks and intent
                                    banks_detail = {
                                        bid: banks_data["banks"][bid] for bid in valid_ids
                                    }

                                    logger.info(...)

                                    return {
                                        "status": "success",
                                        "decision": "banks_selected",
                                        "bank_ids": valid_ids,
                                        "banks_detail": banks_detail,
                                        "query_intent": query_intent,
                                        "tokens_used": tokens_used,
                                        "cost": cost,
                                    }

                        elif function_name == "clarification_needed":
                            question = function_args.get("question", "")
                            possible_banks = function_args.get("possible_banks", [])

                            logger.info(...)

                            return {
                                "status": "needs_clarification",
                                "decision": "clarification_needed",
                                "clarification": question,
                                "possible_banks": possible_banks,
                                "tokens_used": tokens_used,
                                "cost": cost,
                            }

                        else:
                            last_error_reason = f"unexpected_tool:{function_name}"
                else:
                    last_error_reason = "no_tool_calls"
            else:
                last_error_reason = "no_message"

            if attempt < max_attempts:
                logger.warning(
                    "clarifier.banks.retrying_tool_call",
                    execution_id=execution_id,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    reason=last_error_reason,
                )
                continue

        # Fallback clarification after retries
        fallback_question = (
            "Which bank(s) would you like information about? "
            "For example: RBC, TD, or all Big Six banks."
        )
        logger.warning(
            "clarifier.banks.fallback_clarification",
            execution_id=execution_id,
            reason=last_error_reason,
        )
        return {
            "status": "needs_clarification",
            "decision": "clarification_needed",
            "clarification": fallback_question,
            "possible_banks": [],
            "tokens_used": tokens_used,
            "cost": cost,
        }
```

---

## 3. Clarifier Agent - extract_periods (`src/aegis/model/agents/clarifier.py`)

### Before

```python
        response = await complete_with_tools(
            messages=llm_messages,
            tools=tools,
            context=context,
            llm_params={
                "model": model,
                "temperature": 0.1,
                "max_tokens": max_tokens,
            },
        )

        # ... metrics extraction ...

        # Process tool response
        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]

            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_name = tool_call["function"]["name"]
                function_args = json.loads(tool_call["function"]["arguments"])

                # ... tool handling for periods_all, periods_specific, etc. ...

                elif function_name == "periods_valid":
                    periods_clear = function_args.get("periods_clear", False)

                    if periods_clear:
                        # Success
                        return {...}
                    else:
                        # periods_clear=False means LLM should have called
                        # period_clarification instead
                        logger.error(...)
                        return {
                            "status": "error",
                            "error": (
                                "LLM called periods_valid with False "
                                "(should use period_clarification tool)"
                            ),
                            ...
                        }

        # No tool response - this is an error (LLM should have called a tool)
        logger.error(
            "clarifier.periods.no_tool_response",
            execution_id=execution_id,
            error="LLM did not call any tool",
        )
        return {
            "status": "error",
            "error": "LLM failed to call a period extraction tool",
            "tokens_used": tokens_used,
            "cost": cost,
        }
```

### After

```python
        # Attempt tool call up to 3 times (initial + 2 retries)
        max_attempts = 3
        tokens_used = 0
        cost = 0
        last_error_reason = None

        for attempt in range(1, max_attempts + 1):
            response = await complete_with_tools(
                messages=llm_messages,
                tools=tools,
                context=context,
                llm_params={
                    "model": model,
                    "temperature": 0.1,
                    "max_tokens": max_tokens,
                    "tool_choice": "required",  # Force tool use
                },
            )

            # ... metrics extraction ...

            # Process tool response
            if response.get("choices") and response["choices"][0].get("message"):
                message = response["choices"][0]["message"]

                if message.get("tool_calls"):
                    tool_call = message["tool_calls"][0]
                    function_name = tool_call["function"]["name"]
                    try:
                        function_args = json.loads(tool_call["function"]["arguments"])
                    except (TypeError, ValueError) as e:
                        last_error_reason = f"tool_args_parse_error: {e}"
                    else:

                        if function_name == "periods_all":
                            # ... success logic ...
                            return {...}

                        elif function_name == "periods_specific":
                            # ... success logic ...
                            return {...}

                        elif function_name == "periods_valid":
                            periods_clear = function_args.get("periods_clear", False)

                            if periods_clear:
                                return {...}

                            # periods_clear=False means LLM should have called
                            # period_clarification instead -> retry/fallback
                            last_error_reason = "periods_valid_false"

                        elif function_name == "period_clarification":
                            # ... success logic ...
                            return {...}

                        else:
                            last_error_reason = f"unexpected_tool:{function_name}"
                else:
                    last_error_reason = "no_tool_calls"
            else:
                last_error_reason = "no_message"

            if attempt < max_attempts:
                logger.warning(
                    "clarifier.periods.retrying_tool_call",
                    execution_id=execution_id,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    reason=last_error_reason,
                )
                continue

        # Fallback clarification after retries
        fallback_question = (
            "Could you confirm the fiscal year(s) and quarter(s) you want "
            "(for example, FY2024 Q3)?"
        )
        logger.warning(
            "clarifier.periods.fallback_clarification",
            execution_id=execution_id,
            reason=last_error_reason,
        )
        return {
            "status": "needs_clarification",
            "decision": "clarification_needed",
            "clarification": fallback_question,
            "tokens_used": tokens_used,
            "cost": cost,
        }
```

---

## 4. Planner Agent (`src/aegis/model/agents/planner.py`)

### Before

```python
        response = await complete_with_tools(
            messages=messages,
            tools=tools,
            context=context,
            llm_params={
                "model": model,
                "temperature": 0.1,  # Low temperature for consistent rule following
                "max_tokens": max_tokens,
            },
        )

        # ... metrics extraction ...

        # Process tool response
        if response.get("choices") and response["choices"][0].get("message"):
            message = response["choices"][0]["message"]

            if message.get("tool_calls"):
                tool_call = message["tool_calls"][0]
                function_name = tool_call["function"]["name"]
                function_args = json.loads(tool_call["function"]["arguments"])

                if function_name == "databases_selected":
                    databases = function_args.get("databases", [])
                    rationale = function_args.get("rationale", "")

                    # ... filtering and success logic ...

                    return {
                        "status": "success",
                        "databases": filtered_databases,
                        "query_intent": query_intent,
                        "tokens_used": tokens_used,
                        "cost": cost,
                    }

        # Fallback if no tool response
        return {
            "status": "error",
            "error": "Failed to determine databases to query",
            "tokens_used": tokens_used,
            "cost": cost,
        }
```

### After

```python
        # Attempt tool call up to 3 times (initial + 2 retries)
        max_attempts = 3
        tokens_used = 0
        cost = 0
        last_error_reason = None

        for attempt in range(1, max_attempts + 1):
            response = await complete_with_tools(
                messages=messages,
                tools=tools,
                context=context,
                llm_params={
                    "model": model,
                    "temperature": 0.1,  # Low temperature for consistent rule following
                    "max_tokens": max_tokens,
                    "tool_choice": "required",  # Force tool use
                },
            )

            # ... metrics extraction ...

            # Process tool response
            if response.get("choices") and response["choices"][0].get("message"):
                message = response["choices"][0]["message"]

                if message.get("tool_calls"):
                    tool_call = message["tool_calls"][0]
                    function_name = tool_call["function"]["name"]
                    try:
                        function_args = json.loads(tool_call["function"]["arguments"])
                    except (TypeError, ValueError) as e:
                        last_error_reason = f"tool_args_parse_error: {e}"
                    else:

                        if function_name == "databases_selected":
                            databases = function_args.get("databases", [])
                            rationale = function_args.get("rationale", "")

                            # ... filtering logic ...

                            return {
                                "status": "success",
                                "databases": filtered_databases,
                                "query_intent": query_intent,
                                "tokens_used": tokens_used,
                                "cost": cost,
                            }

                        else:
                            last_error_reason = f"unexpected_tool:{function_name}"
                else:
                    last_error_reason = "no_tool_calls"
            else:
                last_error_reason = "no_message"

            if attempt < max_attempts:
                logger.warning(
                    "planner.retrying_tool_call",
                    execution_id=execution_id,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    reason=last_error_reason,
                )
                continue

        # Fallback after retries - return empty databases (will trigger clarification upstream)
        logger.warning(
            "planner.fallback_no_databases",
            execution_id=execution_id,
            reason=last_error_reason,
        )
        return {
            "status": "success",
            "databases": [],
            "query_intent": query_intent,
            "tokens_used": tokens_used,
            "cost": cost,
            "fallback_reason": "Could not determine appropriate databases after retries",
        }
```

---

## 5. Prompt Updates (PostgreSQL)

### Target: `clarifier_periods` prompt

**Database location:**
- model = `aegis`
- layer = `aegis`
- name = `clarifier_periods`

### Changes to `<tool_usage>` section

**Before:**

```xml
<tool_usage>
Choose the appropriate tool:

1. periods_all: When the same period applies to all banks
   - Use for single period queries
   - Use for comparisons where all banks have same periods
   - Only use when period is CLEARLY specified

2. periods_specific: When different banks need different periods
   - Use only when explicitly stated
   - Example: "RBC Q3 2024 and TD Q2 2024"

3. period_clarification: When the time period is unclear or not specified
   - No period mentioned in query
   - Ambiguous references
   - Conflicting period information
   - ALWAYS use this when uncertain

4. periods_valid: (Only available when banks need clarification)
   - Confirms that periods mentioned are clear
   - Used to avoid redundant clarification
</tool_usage>
```

**After:**

```xml
<tool_usage>
CRITICAL: You MUST respond with a tool call. Do not answer in plain text.

Choose the appropriate tool:

1. periods_all: When the same period applies to all banks
   - Use for single period queries
   - Use for comparisons where all banks have same periods
   - Only use when period is CLEARLY specified

2. periods_specific: When different banks need different periods
   - Use only when explicitly stated
   - Example: "RBC Q3 2024 and TD Q2 2024"

3. period_clarification: When the time period is unclear or not specified
   - No period mentioned in query
   - Ambiguous references
   - Conflicting period information
   - ALWAYS use this when uncertain
   - If you cannot determine a period after analysis, ask:
     "Could you confirm the fiscal year(s) and quarter(s) you want (for example, FY2024 Q3)?"

4. periods_valid: (Only available when banks need clarification)
   - Use only when periods are clear
   - If periods are NOT clear, call period_clarification instead
</tool_usage>
```

### Changes to `<important>` section

**Add this line at the beginning:**

```xml
<important>
- You MUST call exactly one tool in every response.
- NEVER default to any period - always clarify when uncertain
...
</important>
```

---

### Target: `clarifier_banks` prompt (version 1.1.0)

**Database location:**
- model = `aegis`
- layer = `aegis`
- name = `clarifier_banks`

**Add to `<tool_usage>` section:**

```xml
<tool_usage>
CRITICAL: You MUST respond with a tool call. Do not answer in plain text.

Use the appropriate tool based on your analysis:
...
</tool_usage>
```

**Add to `<important>` section:**

```xml
<important>
- You MUST call exactly one tool in every response.
- Return ONLY bank ID numbers, not names or symbols
...
</important>
```

---

### Target: `router` prompt (version 2.1.0)

**Database location:**
- model = `aegis`
- layer = `aegis`
- name = `router`

**Add to `<objective>` section:**

```xml
<objective>
  CRITICAL: You MUST respond with a tool call. Do not answer in plain text.

  Return a single binary decision: 0 or 1
  - Use the route tool to return your decision
  - No additional text or explanation needed
  - When uncertain, default to 1 (research_workflow)
</objective>
```

---

### Target: `planner` prompt (version 1.1.0)

**Database location:**
- model = `aegis`
- layer = `aegis`
- name = `planner`

**Add to start of `system_prompt`:**

```
CRITICAL: You MUST respond with a tool call. Do not answer in plain text.

You are the Planner Agent responsible for selecting which databases to query.
...
```

---

## Notes for IT

1. **All code changes** are in `src/aegis/model/agents/`:
   - `router.py`
   - `clarifier.py` (both `extract_banks` and `extract_periods` functions)
   - `planner.py`

2. **Tool forcing** is implemented via `tool_choice="required"` in `llm_params`

3. **Retry logic** uses `max_attempts = 3` with warning logs on each retry

4. **Fallback behavior** varies by agent:
   - Router: Returns `research_workflow` (safe default)
   - Clarifier (banks): Returns clarification question asking which bank(s)
   - Clarifier (periods): Returns clarification question asking which period(s)
   - Planner: Returns empty databases list (triggers clarification upstream)

5. **JSON parse errors** are caught with `try/except (TypeError, ValueError)` and trigger retry

6. **Prompt updates** must be applied in PostgreSQL for ALL agent prompts:
   - `clarifier_periods` (version 1.1.0)
   - `clarifier_banks` (version 1.1.0)
   - `router` (version 2.1.0)
   - `planner` (version 1.1.0)

7. **New log events** added:
   - `router.retrying_tool_call`
   - `router.fallback_to_research`
   - `clarifier.banks.retrying_tool_call`
   - `clarifier.banks.fallback_clarification`
   - `clarifier.periods.retrying_tool_call`
   - `clarifier.periods.fallback_clarification`
   - `planner.retrying_tool_call`
   - `planner.fallback_no_databases`
