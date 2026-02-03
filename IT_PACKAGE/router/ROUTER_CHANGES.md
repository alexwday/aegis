# Router Agent - Tool Call Reliability Updates

## Summary

The router agent has been updated to:
1. **Require tool calls** via `tool_choice: "required"`
2. **Retry failed tool calls** up to 3 attempts (initial + 2 retries)
3. **Gracefully fallback** to research_workflow instead of failing
4. **Handle JSON parse errors** for malformed tool arguments

## Code Changes

**File:** `src/aegis/model/agents/router.py`

### BEFORE

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

    except Exception as e:
        logger.error("router.error", execution_id=execution_id, error=str(e))
        return {
            "status": "Error",
            "route": "research_workflow",
            "rationale": "Defaulting to research workflow due to router error",
            "tokens_used": 0,
            "cost": 0,
            "response_time_ms": 0,
            "error": str(e),
            "prompt_version": prompt_version if "prompt_version" in locals() else "unknown",
            "prompt_last_updated": (
                prompt_last_updated if "prompt_last_updated" in locals() else "unknown"
            ),
        }

    # Default to research workflow if no tool response
    logger.warning("router.default", execution_id=execution_id)
    return {
        "status": "Success",
        "route": "research_workflow",
        "rationale": "Defaulting to research workflow - no clear routing decision",
        "tokens_used": usage.get("total_tokens", 0) if "usage" in locals() else 0,
        "cost": metrics.get("total_cost", 0) if "metrics" in locals() else 0,
        "response_time_ms": metrics.get("response_time", 0) * 1000 if "metrics" in locals() else 0,
        "prompt_version": prompt_version,
        "prompt_last_updated": prompt_last_updated,
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

    except Exception as e:
        logger.error("router.error", execution_id=execution_id, error=str(e))
        return {
            "status": "Error",
            "route": "research_workflow",
            "rationale": "Defaulting to research workflow due to router error",
            "tokens_used": 0,
            "cost": 0,
            "response_time_ms": 0,
            "error": str(e),
            "prompt_version": prompt_version if "prompt_version" in locals() else "unknown",
            "prompt_last_updated": (
                prompt_last_updated if "prompt_last_updated" in locals() else "unknown"
            ),
        }
```

## Prompt Changes

**File:** `src/aegis/model/prompts/aegis/router.yaml`

**Version:** 2.0.0 → 2.1.0

### Change to `<objective>` section

**BEFORE:**

```xml
    <objective>
      Return a single binary decision: 0 or 1
      - Use the route tool to return your decision
      - No additional text or explanation needed
      - When uncertain, default to 1 (research_workflow)
    </objective>
```

**AFTER:**

```xml
    <objective>
      CRITICAL: You MUST respond with a tool call. Do not answer in plain text.

      Return a single binary decision: 0 or 1
      - Use the route tool to return your decision
      - No additional text or explanation needed
      - When uncertain, default to 1 (research_workflow)
    </objective>
```

## New Log Events

- `router.retrying_tool_call` - Logged when retrying after a failed tool call
- `router.fallback_to_research` - Logged when falling back to research workflow after all retries exhausted
