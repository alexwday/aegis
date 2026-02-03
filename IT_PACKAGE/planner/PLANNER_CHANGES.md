# Planner Agent - Tool Call Reliability Updates

## Summary

The planner agent has been updated to:
1. **Require tool calls** via `tool_choice: "required"`
2. **Retry failed tool calls** up to 3 attempts (initial + 2 retries)
3. **Gracefully fallback** to empty databases list instead of returning errors
4. **Handle JSON parse errors** for malformed tool arguments

## Code Changes

**File:** `src/aegis/model/agents/planner.py`

### BEFORE

```python
        # Call LLM with tools
        model_tier_override = context.get("model_tier_override")
        if model_tier_override == "small":
            model = config.llm.small.model
            max_tokens = config.llm.small.max_tokens
        elif model_tier_override == "large":
            model = config.llm.large.model
            max_tokens = config.llm.large.max_tokens
        else:
            model = config.llm.medium.model  # Default to medium for planning
            max_tokens = config.llm.medium.max_tokens

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

                if function_name == "databases_selected":
                    databases = function_args.get("databases", [])
                    rationale = function_args.get("rationale", "")

                    # Now databases is just a list of strings (database IDs)
                    # CRITICAL: Filter to only available databases
                    available_set = set(availability_data["available_databases"])
                    filtered_databases = []
                    rejected_databases = []

                    for db_id in databases:
                        if db_id in available_set:
                            filtered_databases.append(db_id)
                        else:
                            rejected_databases.append(db_id)

                    if rejected_databases:
                        logger.warning(
                            "planner.databases_rejected",
                            execution_id=execution_id,
                            rejected=rejected_databases,
                            reason="Not in available databases",
                            available=list(available_set),
                        )

                    logger.info(
                        "planner.databases_selected",
                        execution_id=execution_id,
                        database_count=len(filtered_databases),
                        databases=filtered_databases,
                        rationale=rationale[:200] if rationale else "No rationale provided",
                    )

                    # Log the clarifier's comprehensive intent that will be used
                    if query_intent:
                        logger.info(
                            "planner.using_clarifier_intent",
                            execution_id=execution_id,
                            comprehensive_intent=query_intent,
                        )

                    return {
                        "status": "success",
                        "databases": filtered_databases,
                        "query_intent": query_intent,  # Pass through the clarifier's intent
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

    except Exception as e:
        import traceback

        logger.error(
            "planner.error",
            execution_id=execution_id,
            error=str(e),
            traceback=traceback.format_exc(),
        )
        return {"status": "error", "error": str(e)}
```

### AFTER

```python
        # Call LLM with tools
        model_tier_override = context.get("model_tier_override")
        if model_tier_override == "small":
            model = config.llm.small.model
            max_tokens = config.llm.small.max_tokens
        elif model_tier_override == "large":
            model = config.llm.large.model
            max_tokens = config.llm.large.max_tokens
        else:
            model = config.llm.medium.model  # Default to medium for planning
            max_tokens = config.llm.medium.max_tokens

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

                        if function_name == "databases_selected":
                            databases = function_args.get("databases", [])
                            rationale = function_args.get("rationale", "")

                            # Now databases is just a list of strings (database IDs)
                            # CRITICAL: Filter to only available databases
                            available_set = set(availability_data["available_databases"])
                            filtered_databases = []
                            rejected_databases = []

                            for db_id in databases:
                                if db_id in available_set:
                                    filtered_databases.append(db_id)
                                else:
                                    rejected_databases.append(db_id)

                            if rejected_databases:
                                logger.warning(
                                    "planner.databases_rejected",
                                    execution_id=execution_id,
                                    rejected=rejected_databases,
                                    reason="Not in available databases",
                                    available=list(available_set),
                                )

                            logger.info(
                                "planner.databases_selected",
                                execution_id=execution_id,
                                database_count=len(filtered_databases),
                                databases=filtered_databases,
                                rationale=(
                                    rationale[:200] if rationale else "No rationale provided"
                                ),
                            )

                            # Log the clarifier's comprehensive intent that will be used
                            if query_intent:
                                logger.info(
                                    "planner.using_clarifier_intent",
                                    execution_id=execution_id,
                                    comprehensive_intent=query_intent,
                                )

                            return {
                                "status": "success",
                                "databases": filtered_databases,
                                "query_intent": query_intent,  # Pass through clarifier's intent
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

    except Exception as e:
        import traceback

        logger.error(
            "planner.error",
            execution_id=execution_id,
            error=str(e),
            traceback=traceback.format_exc(),
        )
        return {"status": "error", "error": str(e)}
```

## Prompt Changes

**File:** `src/aegis/model/prompts/aegis/planner.yaml`

**Version:** 1.0.0 → 1.1.0

**Change to `system_prompt` opening:**

**BEFORE:**
```yaml
system_prompt: |
  You are a database routing specialist for the Aegis financial intelligence system.
```

**AFTER:**
```yaml
system_prompt: |
  CRITICAL: You MUST respond with a tool call. Do not answer in plain text.

  You are a database routing specialist for the Aegis financial intelligence system.
```

## New Log Events

- `planner.retrying_tool_call` - Logged when retrying after a failed tool call
- `planner.fallback_no_databases` - Logged when falling back to empty databases after all retries exhausted

## Fallback Behavior

When all 3 attempts fail, the planner returns:
```python
{
    "status": "success",
    "databases": [],
    "query_intent": query_intent,
    "tokens_used": tokens_used,
    "cost": cost,
    "fallback_reason": "Could not determine appropriate databases after retries",
}
```

This allows upstream orchestration to handle the empty databases case gracefully (e.g., asking the user for clarification about what data they need).
