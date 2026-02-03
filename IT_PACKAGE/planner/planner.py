"""
Planner agent for determining which databases to query based on user requests.

The planner analyzes the user's query and clarifier outputs to:
1. Determine relevant databases for the query
2. Create complete, self-contained query intents for each database
3. Validate database availability for the requested banks/periods
"""

import json
from typing import Any, Dict, List, Optional

from ...connections.postgres_connector import fetch_all
from ...connections.llm_connector import complete_with_tools
from ...utils.logging import get_logger
from ...utils.prompt_loader import load_prompt_from_db
from ...utils.settings import config


async def get_filtered_availability_table(
    bank_ids: List[int], periods: Dict[str, Any], available_databases: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Get filtered availability table based on clarifier outputs.

    This creates a table showing which databases have data for the
    specified banks and periods, similar to the clarifier's period table.

    Args:
        bank_ids: List of bank IDs from clarifier
        periods: Period structure from clarifier
        available_databases: Optional list of database IDs to filter (from model input)

    Returns:
        Dictionary with availability information and formatted table
    """
    logger = get_logger()

    # Debug what we're receiving
    logger.debug(
        "get_filtered_availability_table params",
        bank_ids=bank_ids,
        periods=periods,
        available_databases=available_databases,
    )

    try:
        # Build query to get availability for specific banks and periods
        # Using SQLAlchemy text() with named parameters for asyncpg
        query = """
        WITH unnested AS (
            SELECT
                bank_id,
                bank_name,
                bank_symbol,
                fiscal_year,
                quarter,
                unnest(database_names) as database_name
            FROM aegis_data_availability
            WHERE bank_id = ANY(:bank_ids)
        )
        SELECT
            bank_id,
            bank_name,
            bank_symbol,
            fiscal_year,
            quarter,
            array_agg(DISTINCT database_name) as databases
        FROM unnested
        GROUP BY bank_id, bank_name, bank_symbol, fiscal_year, quarter
        ORDER BY bank_id, fiscal_year DESC, quarter DESC
        """

        # SQLAlchemy text() with asyncpg uses :param named parameters
        params = {"bank_ids": bank_ids}

        # Use async database connection
        rows = await fetch_all(query, params)

        # Build availability structure
        availability = {}
        available_dbs_set = set()

        for row in rows:
            bank_id = str(row["bank_id"])
            bank_name = row["bank_name"]
            bank_symbol = row["bank_symbol"]
            fiscal_year = row["fiscal_year"]
            quarter = row["quarter"]
            databases = row["databases"] or []

            # Filter databases if specified
            if available_databases:
                databases = [db for db in databases if db in available_databases]

            # Check if this row matches the requested periods
            period_match = False

            if "apply_all" in periods:
                # Same period for all banks
                period_info = periods["apply_all"]
                # Debug logging
                logger.debug(
                    "Checking apply_all period_info",
                    period_info=period_info,
                    fiscal_year=fiscal_year,
                    quarter=quarter,
                )
                if fiscal_year == period_info.get("fiscal_year") and quarter in period_info.get(
                    "quarters", []
                ):
                    period_match = True
            else:
                # Bank-specific periods
                # FIX: After clarifier fix, periods dict uses composite keys (bank_id_fiscal_year)
                # Build composite key to match clarifier's new format
                composite_key = f"{bank_id}_{fiscal_year}"
                if composite_key in periods:
                    period_info = periods[composite_key]
                    if quarter in period_info["quarters"]:
                        period_match = True

            # Only include rows that match the requested periods
            if period_match and databases:
                if bank_id not in availability:
                    availability[bank_id] = {
                        "name": bank_name,
                        "symbol": bank_symbol,
                        "periods": [],
                    }

                availability[bank_id]["periods"].append(
                    {"fiscal_year": fiscal_year, "quarter": quarter, "databases": databases}
                )

                # Track all available databases (already filtered)
                available_dbs_set.update(databases)

        # Format as table
        table_lines = []
        table_lines.append("\n<availability_table>")
        table_lines.append("Filtered by requested banks and periods:")
        table_lines.append(
            "\nBank | Name                         | Year | Quarter | Available Databases"
        )
        table_lines.append(
            "-----|------------------------------|------|---------|--------------------"
        )

        for bank_id, bank_data in availability.items():
            bank_name = bank_data["name"]
            bank_symbol = bank_data["symbol"]
            display_name = f"{bank_name} ({bank_symbol})"

            for period in bank_data["periods"]:
                year = period["fiscal_year"]
                quarter = period["quarter"]
                dbs = ", ".join(sorted(period["databases"]))
                table_lines.append(
                    f" {bank_id:^3} | {display_name:<28} | {year:^4} | {quarter:^7} | {dbs}"
                )

        # Apply final filter to ensure we only return databases that were requested
        final_available_dbs = available_dbs_set
        if available_databases:
            final_available_dbs = available_dbs_set.intersection(set(available_databases))

        table_lines.append("\nSummary of available databases across all requested banks/periods:")
        table_lines.append(", ".join(sorted(final_available_dbs)))
        table_lines.append("</availability_table>\n")

        return {
            "availability": availability,
            "available_databases": sorted(list(final_available_dbs)),
            "table": "\n".join(table_lines),
        }

    except Exception as e:
        import traceback

        logger.error(
            "Failed to get filtered availability",
            error=str(e),
            error_type=type(e).__name__,
            traceback=traceback.format_exc(),
        )
        return {
            "availability": {},
            "available_databases": [],
            "table": "<availability_table>No data available</availability_table>",
        }


# Removed: get_filtered_database_descriptions()
# Now using load_global_prompts_for_agent() which internally uses
# database_filter.get_database_prompt() for proper filtering


async def plan_database_queries(
    query: str,
    conversation: List[Dict[str, str]],
    bank_period_combinations: List[Dict[str, Any]],
    context: Dict[str, Any],
    available_databases: Optional[List[str]] = None,
    query_intent: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Plan which databases to query based on user request and clarifier outputs.

    Args:
        query: User's latest message
        conversation: Full conversation history
        bank_period_combinations: List of bank-period combinations from clarifier
        context: Runtime context with auth, SSL config, execution_id
        available_databases: Optional list of database IDs to filter (from model input)
        query_intent: Optional query intent extracted by the clarifier
            (e.g., 'revenue', 'efficiency ratio')

    Returns:
        Dictionary with planned database queries and intents
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    try:
        # Log what we received with details
        logger.info(
            "planner.starting",
            execution_id=execution_id,
            combination_count=len(bank_period_combinations),
            available_databases=available_databases,
            query_intent=query_intent if query_intent else "not_specified",
        )

        # Log sample of combinations for visibility
        if bank_period_combinations:
            sample_combos = bank_period_combinations[:3]  # First 3
            logger.debug(
                "planner.received_combinations",
                execution_id=execution_id,
                sample=[
                    f"{c['bank_symbol']} {c['quarter']} {c['fiscal_year']}" for c in sample_combos
                ],
                showing=f"{len(sample_combos)} of {len(bank_period_combinations)} total",
            )

        # Validate we have combinations
        if not bank_period_combinations:
            logger.error(
                "planner.no_combinations",
                execution_id=execution_id,
                error="No bank-period combinations provided from clarifier",
            )
            return {
                "status": "error",
                "error": "No bank-period combinations provided from clarifier",
            }

        # Extract unique bank IDs for availability check
        bank_ids = list(set(combo["bank_id"] for combo in bank_period_combinations))

        # Convert combinations back to period format for availability check
        # Group by apply_all vs bank-specific
        unique_periods = set()
        bank_specific_periods = {}

        for combo in bank_period_combinations:
            bank_id = combo["bank_id"]
            fiscal_year = combo["fiscal_year"]
            quarter = combo["quarter"]

            # Track all unique periods
            unique_periods.add((fiscal_year, quarter))

            # Track per-bank periods
            # FIX: Use composite key (bank_id + fiscal_year) to prevent
            # multiple years for same bank from overwriting each other
            composite_key = f"{bank_id}_{fiscal_year}"
            if composite_key not in bank_specific_periods:
                bank_specific_periods[composite_key] = {
                    "bank_id": bank_id,
                    "fiscal_year": fiscal_year,
                    "quarters": [],
                }
            if quarter not in bank_specific_periods[composite_key]["quarters"]:
                bank_specific_periods[composite_key]["quarters"].append(quarter)

        # Check if all banks have same periods (apply_all case)
        period_info = {}
        all_same = (
            len(
                set(
                    tuple(sorted(p["quarters"])) + (p["fiscal_year"],)
                    for p in bank_specific_periods.values()
                )
            )
            == 1
        )

        if all_same and bank_specific_periods:
            # All banks have same periods - use apply_all format
            first_bank = next(iter(bank_specific_periods.values()))
            period_info = {"apply_all": first_bank}
            logger.debug(
                "planner.period_structure",
                execution_id=execution_id,
                type="uniform_periods",
                fiscal_year=first_bank["fiscal_year"],
                quarters=first_bank["quarters"],
                applying_to_banks=len(bank_ids),
            )
        else:
            # Different periods per bank
            period_info = {
                str(bid): period_data for bid, period_data in bank_specific_periods.items()
            }
            logger.debug(
                "planner.period_structure",
                execution_id=execution_id,
                type="bank_specific_periods",
                unique_period_sets=len(
                    set(
                        tuple(sorted(p["quarters"])) + (p["fiscal_year"],)
                        for p in bank_specific_periods.values()
                    )
                ),
            )

        # Get filtered availability table
        availability_data = await get_filtered_availability_table(
            bank_ids=bank_ids, periods=period_info, available_databases=available_databases
        )

        # Load planner prompt from database with global composition
        planner_data = load_prompt_from_db(
            layer="aegis",
            name="planner",
            compose_with_globals=True,
            available_databases=availability_data["available_databases"],
            execution_id=execution_id,
        )

        # Get the composed prompt (globals + planner prompt)
        base_prompt = planner_data.get("composed_prompt", planner_data.get("system_prompt", ""))

        # Add availability table (dynamic data at END)
        system_prompt = "\n\n".join([base_prompt, availability_data["table"]])

        # Build conversation context for the planner
        conversation_context = "Previous conversation:\n"
        for msg in conversation[-5:]:  # Last 5 messages for context
            conversation_context += f"{msg['role']}: {msg['content']}\n"

        # Load and format user prompt template
        user_prompt_template = planner_data.get("user_prompt", "")
        user_message = user_prompt_template.format(
            conversation_context=conversation_context,
            query=query,
            query_intent=query_intent if query_intent else "not_specified",
        )

        # Create messages
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ]

        # Load tools from database record
        tool_definition = planner_data.get("tool_definition")
        if tool_definition:
            tools = [tool_definition] if isinstance(tool_definition, dict) else tool_definition
        else:
            tools = planner_data.get("tool_definitions", [])

        if not tools:
            logger.error(
                "planner.tools_missing",
                execution_id=execution_id,
                error="Failed to load tools from database",
            )
            return {
                "status": "error",
                "databases": [],
                "error": "Planner tools not found in database",
            }

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
