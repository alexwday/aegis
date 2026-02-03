# Clarifier Agent - Tool Call Reliability Updates

## Summary

The clarifier agent has been updated to:
1. **Require tool calls** via `tool_choice: "required"`
2. **Retry failed tool calls** up to 3 attempts (initial + 2 retries)
3. **Gracefully fallback** to clarification questions instead of returning errors
4. **Handle JSON parse errors** for malformed tool arguments

## Code Changes

**File:** `src/aegis/model/agents/clarifier.py`

---

## Function 1: `extract_banks`

### BEFORE

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
                        logger.error(
                            "clarifier.banks.missing_intent",
                            execution_id=execution_id,
                            error="LLM called banks_found without query_intent (required field)",
                        )
                        return {
                            "status": "error",
                            "error": "LLM failed to provide query_intent in banks_found tool call",
                            "tokens_used": tokens_used,
                            "cost": cost,
                        }

                    # Validate bank IDs exist in our data (filter to valid only)
                    valid_ids = [bid for bid in bank_ids if bid in banks_data["banks"]]
                    invalid_ids = [bid for bid in bank_ids if bid not in banks_data["banks"]]

                    if invalid_ids:
                        logger.warning(
                            "clarifier.banks.invalid_ids",
                            execution_id=execution_id,
                            invalid_ids=invalid_ids,
                        )

                    # If no valid banks after filtering, return error (LLM hallucinated)
                    if not valid_ids:
                        logger.error(
                            "clarifier.banks.no_valid_banks",
                            execution_id=execution_id,
                            attempted_ids=bank_ids,
                            error="All bank IDs were invalid (LLM hallucination)",
                        )
                        return {
                            "status": "error",
                            "error": "No valid banks found (LLM hallucination)",
                            "tokens_used": tokens_used,
                            "cost": cost,
                        }

                    # Success - return validated banks and intent
                    banks_detail = {bid: banks_data["banks"][bid] for bid in valid_ids}

                    logger.info(
                        "clarifier.banks.extracted",
                        execution_id=execution_id,
                        bank_ids=valid_ids,
                        count=len(valid_ids),
                        query_intent=query_intent[:100],
                    )

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

                    logger.info(
                        "clarifier.banks.needs_clarification",
                        execution_id=execution_id,
                        possible_banks=possible_banks,
                    )

                    return {
                        "status": "needs_clarification",
                        "decision": "clarification_needed",
                        "clarification": question,
                        "possible_banks": possible_banks,
                        "tokens_used": tokens_used,
                        "cost": cost,
                    }

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

    except Exception as e:
        logger.error("clarifier.banks.error", execution_id=execution_id, error=str(e))
        return {
            "status": "error",
            "error": str(e),
        }
```

### AFTER

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
                                valid_ids = [bid for bid in bank_ids if bid in banks_data["banks"]]
                                invalid_ids = [
                                    bid for bid in bank_ids if bid not in banks_data["banks"]
                                ]

                                if invalid_ids:
                                    logger.warning(
                                        "clarifier.banks.invalid_ids",
                                        execution_id=execution_id,
                                        invalid_ids=invalid_ids,
                                    )

                                # If no valid banks after filtering, retry
                                if not valid_ids:
                                    last_error_reason = "no_valid_banks_hallucination"
                                else:
                                    # Success - return validated banks and intent
                                    banks_detail = {
                                        bid: banks_data["banks"][bid] for bid in valid_ids
                                    }

                                    logger.info(
                                        "clarifier.banks.extracted",
                                        execution_id=execution_id,
                                        bank_ids=valid_ids,
                                        count=len(valid_ids),
                                        query_intent=query_intent[:100],
                                    )

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

                            logger.info(
                                "clarifier.banks.needs_clarification",
                                execution_id=execution_id,
                                possible_banks=possible_banks,
                            )

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

    except Exception as e:
        logger.error("clarifier.banks.error", execution_id=execution_id, error=str(e))
        return {
            "status": "error",
            "error": str(e),
        }
```

---

## Function 2: `extract_periods`

### BEFORE

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

                if function_name == "periods_all":
                    fiscal_year = function_args.get("fiscal_year")
                    quarters = function_args.get("quarters", [])

                    logger.info(
                        "clarifier.periods.apply_all",
                        execution_id=execution_id,
                        fiscal_year=fiscal_year,
                        quarters=quarters,
                    )

                    return {
                        "status": "success",
                        "decision": "periods_selected",
                        "periods": {
                            "apply_all": {
                                "fiscal_year": fiscal_year,
                                "quarters": quarters,
                            }
                        },
                        "tokens_used": tokens_used,
                        "cost": cost,
                    }

                elif function_name == "periods_specific":
                    bank_periods = function_args.get("bank_periods", [])

                    # Convert to dictionary format
                    # FIX: Use composite key (bank_id + fiscal_year) to prevent
                    # multiple years for same bank from overwriting each other
                    periods = {}
                    for bp in bank_periods:
                        bank_id = str(bp["bank_id"])
                        fiscal_year = bp["fiscal_year"]
                        # Create composite key to support multiple years per bank
                        composite_key = f"{bank_id}_{fiscal_year}"
                        periods[composite_key] = {
                            "bank_id": bank_id,
                            "fiscal_year": fiscal_year,
                            "quarters": bp["quarters"],
                        }

                    logger.info(
                        "clarifier.periods.bank_specific",
                        execution_id=execution_id,
                        periods=periods,
                    )

                    return {
                        "status": "success",
                        "decision": "periods_selected",
                        "periods": periods,
                    }

                elif function_name == "periods_valid":
                    periods_clear = function_args.get("periods_clear", False)

                    if periods_clear:
                        # Periods are clear, no clarification needed
                        logger.info(
                            "clarifier.periods.valid",
                            execution_id=execution_id,
                        )
                        return {
                            "status": "success",
                            "decision": "periods_clear",
                            "periods": None,  # Will be extracted after bank clarification
                            "tokens_used": tokens_used,
                            "cost": cost,
                        }
                    else:
                        # periods_clear=False means LLM should have called
                        # period_clarification instead
                        logger.error(
                            "clarifier.periods.invalid_tool_use",
                            execution_id=execution_id,
                            error=(
                                "LLM called periods_valid with False "
                                "(should use period_clarification)"
                            ),
                        )
                        return {
                            "status": "error",
                            "error": (
                                "LLM called periods_valid with False "
                                "(should use period_clarification tool)"
                            ),
                            "tokens_used": tokens_used,
                            "cost": cost,
                        }

                elif function_name == "period_clarification":
                    question = function_args.get("question", "")

                    logger.info(
                        "clarifier.periods.needs_clarification",
                        execution_id=execution_id,
                    )

                    return {
                        "status": "needs_clarification",
                        "decision": "clarification_needed",
                        "clarification": question,
                        "tokens_used": tokens_used,
                        "cost": cost,
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

    except Exception as e:
        logger.error("clarifier.periods.error", execution_id=execution_id, error=str(e))
        return {
            "status": "error",
            "error": str(e),
        }
```

### AFTER

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

                        if function_name == "periods_all":
                            fiscal_year = function_args.get("fiscal_year")
                            quarters = function_args.get("quarters", [])

                            logger.info(
                                "clarifier.periods.apply_all",
                                execution_id=execution_id,
                                fiscal_year=fiscal_year,
                                quarters=quarters,
                            )

                            return {
                                "status": "success",
                                "decision": "periods_selected",
                                "periods": {
                                    "apply_all": {
                                        "fiscal_year": fiscal_year,
                                        "quarters": quarters,
                                    }
                                },
                                "tokens_used": tokens_used,
                                "cost": cost,
                            }

                        elif function_name == "periods_specific":
                            bank_periods = function_args.get("bank_periods", [])

                            # Convert to dictionary format
                            # FIX: Use composite key (bank_id + fiscal_year) to prevent
                            # multiple years for same bank from overwriting each other
                            periods = {}
                            for bp in bank_periods:
                                bank_id = str(bp["bank_id"])
                                fiscal_year = bp["fiscal_year"]
                                # Create composite key to support multiple years per bank
                                composite_key = f"{bank_id}_{fiscal_year}"
                                periods[composite_key] = {
                                    "bank_id": bank_id,
                                    "fiscal_year": fiscal_year,
                                    "quarters": bp["quarters"],
                                }

                            logger.info(
                                "clarifier.periods.bank_specific",
                                execution_id=execution_id,
                                periods=periods,
                            )

                            return {
                                "status": "success",
                                "decision": "periods_selected",
                                "periods": periods,
                            }

                        elif function_name == "periods_valid":
                            periods_clear = function_args.get("periods_clear", False)

                            if periods_clear:
                                # Periods are clear, no clarification needed
                                logger.info(
                                    "clarifier.periods.valid",
                                    execution_id=execution_id,
                                )
                                return {
                                    "status": "success",
                                    "decision": "periods_clear",
                                    "periods": None,  # Will be extracted after bank clarification
                                    "tokens_used": tokens_used,
                                    "cost": cost,
                                }

                            # periods_clear=False means LLM should have called
                            # period_clarification instead -> retry/fallback
                            last_error_reason = "periods_valid_false"

                        elif function_name == "period_clarification":
                            question = function_args.get("question", "")

                            logger.info(
                                "clarifier.periods.needs_clarification",
                                execution_id=execution_id,
                            )

                            return {
                                "status": "needs_clarification",
                                "decision": "clarification_needed",
                                "clarification": question,
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

    except Exception as e:
        logger.error("clarifier.periods.error", execution_id=execution_id, error=str(e))
        return {
            "status": "error",
            "error": str(e),
        }
```

---

## Prompt Changes

### File: `src/aegis/model/prompts/aegis/clarifier_banks.yaml`

**Version:** 1.0.0 → 1.1.0

**Change to `<tool_usage>` section:**

**BEFORE:**
```xml
    <tool_usage>
      You MUST respond using one of the provided tools - never plain text
      - banks_found: When you can identify specific banks
      - clarification_needed: When the query is ambiguous
    </tool_usage>
```

**AFTER:**
```xml
    <tool_usage>
      CRITICAL: You MUST respond with a tool call. Do not answer in plain text.

      You MUST respond using one of the provided tools - never plain text
      - banks_found: When you can identify specific banks
      - clarification_needed: When the query is ambiguous
    </tool_usage>
```

**Change to `<important>` section:**

**BEFORE:**
```xml
    <important>
      - NEVER assume banks - always clarify if uncertain
      - query_intent is REQUIRED in banks_found - describe what user wants
      - Use clarification_needed when multiple interpretations possible
    </important>
```

**AFTER:**
```xml
    <important>
      - You MUST call exactly one tool in every response
      - NEVER assume banks - always clarify if uncertain
      - query_intent is REQUIRED in banks_found - describe what user wants
      - Use clarification_needed when multiple interpretations possible
    </important>
```

---

### File: `src/aegis/model/prompts/aegis/clarifier_periods.yaml`

**Version:** 1.0.0 → 1.1.0

**Change to `<tool_usage>` section:**

**BEFORE:**
```xml
    <tool_usage>
      You MUST respond using one of the provided tools - never plain text
      - periods_all: Same period for all banks
      - periods_specific: Different periods per bank
      - periods_valid: (no banks yet) Confirm periods are clear
      - period_clarification: When periods are ambiguous
    </tool_usage>
```

**AFTER:**
```xml
    <tool_usage>
      CRITICAL: You MUST respond with a tool call. Do not answer in plain text.

      You MUST respond using one of the provided tools - never plain text
      - periods_all: Same period for all banks
      - periods_specific: Different periods per bank
      - periods_valid: (no banks yet) Confirm periods are clear
      - period_clarification: When periods are ambiguous
    </tool_usage>
```

**Change to `<important>` section:**

**BEFORE:**
```xml
    <important>
      - NEVER assume periods - always clarify if uncertain
      - Use period_clarification for any ambiguity
      - Apply fiscal year rules from fiscal context
    </important>
```

**AFTER:**
```xml
    <important>
      - You MUST call exactly one tool in every response
      - NEVER assume periods - always clarify if uncertain
      - Use period_clarification for any ambiguity
      - Apply fiscal year rules from fiscal context
    </important>
```

**Change to `period_clarification` tool description:**

**BEFORE:**
```yaml
description: "Ask user to clarify ambiguous periods"
```

**AFTER:**
```yaml
description: "Ask user to clarify ambiguous periods. Use a fallback question like: Could you confirm the fiscal year(s) and quarter(s) you want (for example, FY2024 Q3)?"
```

---

## New Log Events

- `clarifier.banks.retrying_tool_call` - Logged when retrying after a failed tool call in extract_banks
- `clarifier.banks.fallback_clarification` - Logged when falling back to clarification after all retries exhausted
- `clarifier.periods.retrying_tool_call` - Logged when retrying after a failed tool call in extract_periods
- `clarifier.periods.fallback_clarification` - Logged when falling back to clarification after all retries exhausted
