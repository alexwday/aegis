"""
Clarifier agent for extracting banks and periods from user queries.

The clarifier performs two main tasks:
1. Bank identification - extracts bank IDs from user queries
2. Period extraction - identifies fiscal periods for each bank

This agent NEVER defaults - it always clarifies when uncertain.
All bank and period data comes from the aegis_data_availability table.
"""

import json
from typing import Any, Dict, List, Optional

from ...connections.postgres_connector import fetch_all
from ...connections.llm_connector import complete_with_tools
from ...utils.logging import get_logger
from ...utils.prompt_loader import load_yaml, load_tools_from_yaml, _load_fiscal_prompt
from ...utils.settings import config


async def load_banks_from_db(available_databases: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Load bank information from the aegis_data_availability table.

    Args:
        available_databases: Optional list of database IDs to filter banks

    Returns:
        Dictionary containing bank information and availability
    """
    logger = get_logger()

    try:
        # Build query to get unique banks with their info
        base_query = """
        WITH unnested AS (
            SELECT DISTINCT
                bank_id,
                bank_name,
                bank_symbol,
                bank_aliases,
                bank_tags,
                unnest(database_names) as database_name
            FROM aegis_data_availability
        )
        SELECT
            bank_id,
            bank_name,
            bank_symbol,
            bank_aliases,
            bank_tags,
            array_agg(DISTINCT database_name) as all_databases
        FROM unnested
        GROUP BY bank_id, bank_name, bank_symbol, bank_aliases, bank_tags
        ORDER BY bank_id
        """

        result = await fetch_all(base_query, execution_id="clarifier_banks")

        banks_data = {"banks": {}, "categories": {}}

        # Track categories/tags
        tag_to_banks = {}

        for row in result:
            bank_id = row["bank_id"]
            bank_name = row["bank_name"]
            bank_symbol = row["bank_symbol"]
            bank_aliases = row["bank_aliases"] or []
            bank_tags = row["bank_tags"] or []
            all_databases = row["all_databases"] or []

            # Filter by available databases if specified
            if available_databases:
                # Check if bank has data in any of the requested databases
                if not any(db in all_databases for db in available_databases):
                    continue
                # Filter to only show available databases
                bank_databases = [db for db in all_databases if db in available_databases]
            else:
                bank_databases = all_databases

            # Add bank to data structure
            banks_data["banks"][bank_id] = {
                "id": bank_id,
                "name": bank_name,
                "symbol": bank_symbol,
                "aliases": bank_aliases,
                "tags": bank_tags,
                "databases": bank_databases,
            }

            # Track tags for category building
            for tag in bank_tags:
                if tag not in tag_to_banks:
                    tag_to_banks[tag] = []
                tag_to_banks[tag].append(bank_id)

        # Build categories from tags
        for tag, bank_ids in tag_to_banks.items():
            if tag == "canadian_big_six":
                banks_data["categories"]["big_six"] = {
                    "aliases": ["Big Six", "Canadian Big Six", "Big 6"],
                    "bank_ids": sorted(bank_ids),
                }
            elif tag == "us_bank":
                banks_data["categories"]["us_banks"] = {
                    "aliases": ["US banks", "American banks"],
                    "bank_ids": sorted(bank_ids),
                }

        logger.debug(
            "Banks loaded from database",
            bank_count=len(banks_data["banks"]),
            filtered_by=available_databases,
        )

        return banks_data

    except Exception as e:
        logger.error("Failed to load banks from database", error=str(e))
        return {"banks": {}, "categories": {}}


async def get_period_availability_from_db(
    bank_ids: Optional[List[int]] = None, available_databases: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Get period availability data from the aegis_data_availability table.

    Args:
        bank_ids: Optional list of bank IDs to filter
        available_databases: Optional list of database IDs to filter

    Returns:
        Dictionary with period availability by bank and database
    """
    logger = get_logger()

    try:
        # Build query
        query = """
        SELECT
            bank_id,
            bank_name,
            bank_symbol,
            fiscal_year,
            quarter,
            database_names
        FROM aegis_data_availability
        WHERE 1=1
        """

        params = {}

        if bank_ids:
            query += " AND bank_id = ANY(:bank_ids)"
            params["bank_ids"] = bank_ids

        query += " ORDER BY bank_id, fiscal_year DESC, quarter DESC"

        result = await fetch_all(query, params, execution_id="clarifier_periods")

        availability = {}
        latest_year = None
        latest_quarter = None

        for row in result:
            bank_id = str(row["bank_id"])
            bank_name = row["bank_name"]
            bank_symbol = row["bank_symbol"]
            fiscal_year = row["fiscal_year"]
            quarter = row["quarter"]
            database_names = row["database_names"] or []

            # Track latest available period
            if latest_year is None or fiscal_year > latest_year:
                latest_year = fiscal_year
                latest_quarter = quarter
            elif fiscal_year == latest_year and quarter > latest_quarter:
                latest_quarter = quarter

            # Filter databases if specified
            if available_databases:
                filtered_dbs = [db for db in database_names if db in available_databases]
                if not filtered_dbs:
                    continue
                database_names = filtered_dbs

            # Structure: bank_id -> {info, databases}
            if bank_id not in availability:
                availability[bank_id] = {
                    "name": bank_name,
                    "symbol": bank_symbol,
                    "databases": {},
                }

            for db in database_names:
                if db not in availability[bank_id]["databases"]:
                    availability[bank_id]["databases"][db] = {}

                if fiscal_year not in availability[bank_id]["databases"][db]:
                    availability[bank_id]["databases"][db][fiscal_year] = []

                if quarter not in availability[bank_id]["databases"][db][fiscal_year]:
                    availability[bank_id]["databases"][db][fiscal_year].append(quarter)

        # Sort quarters for each bank/db/year
        for bank_id in availability:
            for db in availability[bank_id]["databases"]:
                for year in availability[bank_id]["databases"][db]:
                    availability[bank_id]["databases"][db][year].sort()

        return {
            "latest_reported": (
                {"fiscal_year": latest_year, "quarter": latest_quarter} if latest_year else {}
            ),
            "availability": availability,
        }

    except Exception as e:
        logger.error("Failed to load period availability from database", error=str(e))
        return {"latest_reported": {}, "availability": {}}


def create_bank_prompt(banks_data: Dict[str, Any], available_databases: List[str]) -> str:
    """
    Create a prompt section with bank information from database.

    Args:
        banks_data: Banks data loaded from database
        available_databases: List of available database IDs

    Returns:
        Formatted prompt string with bank index
    """
    lines = ["<available_banks>"]

    if available_databases and available_databases != ["all"]:
        lines.append(f"Based on the selected databases ({', '.join(available_databases)}), ")
    else:
        lines.append("Based on all available databases, ")

    lines.append("the following banks are available:\n")

    for bank_id, bank_info in sorted(banks_data["banks"].items()):
        lines.append(f"{bank_id}. {bank_info['name']} ({bank_info['symbol']})")
        if bank_info.get("aliases"):
            lines.append(f"   Aliases: {', '.join(bank_info['aliases'])}")
        if bank_info.get("tags"):
            lines.append(f"   Tags: {', '.join(bank_info['tags'])}")
        if bank_info.get("databases"):
            lines.append(f"   Available in: {', '.join(bank_info['databases'])}")

    # Add categories if present
    if banks_data.get("categories"):
        lines.append("\nCategories:")
        for category, info in banks_data["categories"].items():
            lines.append(f"  {category}: banks {info['bank_ids']}")
            lines.append(f"    Aliases: {', '.join(info['aliases'])}")

    lines.append("\nNote: Only return the ID numbers of banks from this list.")
    lines.append("</available_banks>")

    return "\n".join(lines)


async def extract_banks(
    query: str,
    context: Dict[str, Any],
    available_databases: Optional[List[str]] = None,
    messages: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """
    Extract bank IDs and query intent from user query using bank data from database.

    This function identifies which banks the user is referring to, handling
    aliases, categories (e.g., "Big Six"), and database filtering.
    Also identifies what the user is asking for (revenue, efficiency ratio, etc.)
    NEVER defaults - always clarifies when uncertain.

    Args:
        query: User's query text (latest message)
        context: Runtime context with auth, SSL config, execution_id
        available_databases: Optional list of database IDs to filter banks
        messages: Optional full conversation history for context

    Returns:
        Dictionary with extraction results including query intent
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    try:
        # Load banks from database
        banks_data = await load_banks_from_db(available_databases)

        if not banks_data.get("banks"):
            return {
                "status": "error",
                "error": "No banks available in the system",
            }

        bank_prompt = create_bank_prompt(
            banks_data, available_databases if available_databases else ["all"]
        )

        logger.info(
            "clarifier.banks.extracting",
            execution_id=execution_id,
            available_banks=len(banks_data.get("banks", {})),
            databases=available_databases,
        )

        # Load clarifier prompt
        clarifier_data = load_yaml("aegis/clarifier_banks.yaml")
        prompt_parts = []

        # Add bank index context
        prompt_parts.append(bank_prompt)

        # Add clarifier system prompt
        system_prompt_template = clarifier_data.get("system_prompt", "")
        prompt_parts.append(system_prompt_template.strip())

        system_prompt = "\n\n".join(prompt_parts)

        # Create messages with conversation history
        llm_messages = [{"role": "system", "content": system_prompt}]

        # Add conversation history if provided
        if messages:
            # Add all messages except the latest (which is the query)
            for msg in messages[:-1]:
                llm_messages.append(msg)

        # Load and format user prompt template
        user_prompt_template = clarifier_data.get("user_prompt_template", "")
        user_content = user_prompt_template.format(query=query)

        llm_messages.append({"role": "user", "content": user_content})

        # Load tools from YAML (no fallback)
        tools = load_tools_from_yaml("clarifier_banks", execution_id=execution_id)

        # Call LLM with tools - using medium model for extraction

        # Get model based on override or default to large
        model_tier_override = context.get("model_tier_override")
        if model_tier_override == "small":
            model = config.llm.small.model
        elif model_tier_override == "medium":
            model = config.llm.medium.model
        else:
            model = config.llm.large.model  # Default to large for better reasoning

        response = await complete_with_tools(
            messages=llm_messages,
            tools=tools,
            context=context,
            llm_params={
                "model": model,
                "temperature": 0.1,  # Low temperature for consistent extraction
                "max_tokens": 200,
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

                    # If no banks specified, need clarification
                    if not bank_ids:
                        return {
                            "status": "needs_clarification",
                            "decision": "clarification_needed",
                            "clarification": (
                                "Which banks would you like to query? "
                                "Please specify the bank names."
                            ),
                            "tokens_used": tokens_used,
                            "cost": cost,
                        }

                    # Validate bank IDs exist in our data
                    valid_ids = [bid for bid in bank_ids if bid in banks_data["banks"]]
                    invalid_ids = [bid for bid in bank_ids if bid not in banks_data["banks"]]

                    if invalid_ids:
                        logger.warning(
                            "clarifier.banks.invalid_ids",
                            execution_id=execution_id,
                            invalid_ids=invalid_ids,
                        )

                    if valid_ids:
                        # Get full bank details
                        banks_detail = {bid: banks_data["banks"][bid] for bid in valid_ids}

                        logger.info(
                            "clarifier.banks.extracted",
                            execution_id=execution_id,
                            bank_ids=valid_ids,
                            count=len(valid_ids),
                        )

                        # Check if we need to clarify the intent
                        if not query_intent:
                            # We have banks but no clear intent
                            intent_clarification = "What would you like to see for "
                            if len(valid_ids) == 1:
                                intent_clarification += f"{banks_detail[valid_ids[0]]['name']}?"
                            else:
                                bank_names = [banks_detail[bid]["name"] for bid in valid_ids[:3]]
                                if len(valid_ids) > 3:
                                    intent_clarification += (
                                        f"{', '.join(bank_names[:2])}, and "
                                        f"{len(valid_ids)-2} other banks?"
                                    )
                                else:
                                    intent_clarification += (
                                        f"{', '.join(bank_names[:-1])} and {bank_names[-1]}?"
                                    )
                            intent_clarification += (
                                " (e.g., revenue, efficiency ratio, expenses, net income)"
                            )
                        else:
                            intent_clarification = None

                        return {
                            "status": "success",
                            "decision": "banks_selected",
                            "bank_ids": valid_ids,
                            "banks_detail": banks_detail,
                            "query_intent": query_intent,
                            "intent_clarification": intent_clarification,
                            "tokens_used": tokens_used,
                            "cost": cost,
                        }
                    else:
                        return {
                            "status": "needs_clarification",
                            "decision": "clarification_needed",
                            "clarification": (
                                "No valid banks found. "
                                "Please specify which banks you're interested in."
                            ),
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

        # No tool response - need clarification
        return {
            "status": "needs_clarification",
            "decision": "clarification_needed",
            "clarification": "Please specify which banks you'd like to query.",
            "tokens_used": tokens_used,
            "cost": cost,
        }

    except Exception as e:
        logger.error("clarifier.banks.error", execution_id=execution_id, error=str(e))
        return {
            "status": "error",
            "error": str(e),
        }


async def extract_periods(
    query: str,
    bank_ids: Optional[List[int]],
    context: Dict[str, Any],
    available_databases: Optional[List[str]] = None,
    messages: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """
    Extract fiscal periods for the identified banks or check if period clarification is needed.

    NEVER defaults - always clarifies when uncertain.

    Args:
        query: User's query text
        bank_ids: List of bank IDs to extract periods for (None if banks need clarification)
        context: Runtime context with auth, SSL config, execution_id
        available_databases: Optional list of available database IDs

    Returns:
        Dictionary with period extraction results
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    try:
        logger.info(
            "clarifier.periods.extracting",
            execution_id=execution_id,
            bank_ids=bank_ids,
            has_banks=bank_ids is not None,
        )

        # Load period availability from database
        period_availability = await get_period_availability_from_db(bank_ids, available_databases)

        # Load clarifier period prompt
        clarifier_data = load_yaml("aegis/clarifier_periods.yaml")

        prompt_parts = []

        # Add dynamic fiscal context from fiscal.py
        prompt_parts.append(_load_fiscal_prompt())

        # Add period availability context if we have banks
        if bank_ids:
            availability_text = "\n<period_availability>\n"

            if period_availability.get("latest_reported"):
                availability_text += (
                    f"Latest reported: {period_availability['latest_reported']['quarter']} "
                )
                availability_text += f"{period_availability['latest_reported']['fiscal_year']}\n\n"

            if period_availability.get("availability"):
                availability_text += "Available periods in the system:\n"
                availability_text += (
                    "VALIDATION RULE: If a period exists in ANY database, it is AVAILABLE.\n\n"
                )
                availability_text += (
                    "Bank | Name                         | Year | Quarter | Databases\n"
                )
                availability_text += (
                    "-----|------------------------------|------|---------|----------\n"
                )

                for bank_id, bank_data in period_availability["availability"].items():
                    bank_name = bank_data.get("name", "Unknown")
                    bank_symbol = bank_data.get("symbol", "")
                    # Show full name with symbol in parentheses
                    display_name = f"{bank_name} ({bank_symbol})"

                    # Reorganize data by year and quarter
                    periods_by_yq = {}
                    for db, years in bank_data["databases"].items():
                        for year, quarters in years.items():
                            for quarter in quarters:
                                key = (year, quarter)
                                if key not in periods_by_yq:
                                    periods_by_yq[key] = []
                                periods_by_yq[key].append(db)

                    # Output in clean table format
                    for year, quarter in sorted(periods_by_yq.keys(), reverse=True):
                        databases = sorted(periods_by_yq[(year, quarter)])
                        availability_text += (
                            f" {bank_id:^3} | {display_name:<28} | {year:^4} | {quarter:^7} | "
                            f"{', '.join(databases)}\n"
                        )
            else:
                availability_text += "WARNING: No periods available for these banks.\n"

            availability_text += "</period_availability>\n"
            prompt_parts.append(availability_text)

        # Add clarifier system prompt
        system_prompt_template = clarifier_data.get("system_prompt", "")
        prompt_parts.append(system_prompt_template.strip())

        # Add context about whether we have banks
        if bank_ids:
            prompt_parts.append(f"Banks to extract periods for (IDs): {bank_ids}")
        else:
            prompt_parts.append(
                "NOTE: Banks are being clarified separately. "
                "Only check if the period mentions in the query are clear and valid."
            )

        system_prompt = "\n\n".join(prompt_parts)

        # Create messages with conversation history
        llm_messages = [{"role": "system", "content": system_prompt}]

        # Add conversation history if provided
        if messages:
            # Add all messages except the latest (which is the query)
            for msg in messages[:-1]:
                llm_messages.append(msg)

        # Load and format user prompt template
        user_prompt_template = clarifier_data.get("user_prompt_template", "")
        user_content = user_prompt_template.format(query=query)

        llm_messages.append({"role": "user", "content": user_content})

        # Load tools from YAML (no fallback)
        all_tools = load_tools_from_yaml("clarifier_periods", execution_id=execution_id)

        # Filter tools based on whether we have banks
        if bank_ids:
            # Full period extraction tools when we have banks
            # Use: periods_all, periods_specific, period_clarification
            tools = [
                tool
                for tool in all_tools
                if tool["function"]["name"] in ["periods_all", "periods_specific", "period_clarification"]
            ]
        else:
            # Limited tools when banks need clarification
            # Use: periods_valid, period_clarification
            tools = [
                tool
                for tool in all_tools
                if tool["function"]["name"] in ["periods_valid", "period_clarification"]
            ]

        # Call LLM with tools - using medium model

        # Get model based on override or default to large
        model_tier_override = context.get("model_tier_override")
        if model_tier_override == "small":
            model = config.llm.small.model
        elif model_tier_override == "medium":
            model = config.llm.medium.model
        else:
            model = config.llm.large.model  # Default to large for complex period validation

        response = await complete_with_tools(
            messages=llm_messages,
            tools=tools,
            context=context,
            llm_params={
                "model": model,
                "temperature": 0.1,
                "max_tokens": 300,
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
                        return {
                            "status": "success",
                            "decision": "periods_clear",
                            "periods": None,  # Will be extracted after bank clarification
                            "tokens_used": tokens_used,
                            "cost": cost,
                        }
                    else:
                        # Need clarification
                        return {
                            "status": "needs_clarification",
                            "decision": "clarification_needed",
                            "clarification": "What time period would you like to query?",
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

        # No tool response - need clarification
        return {
            "status": "needs_clarification",
            "decision": "clarification_needed",
            "clarification": "What time period would you like to query?",
            "tokens_used": tokens_used,
            "cost": cost,
        }

    except Exception as e:
        logger.error("clarifier.periods.error", execution_id=execution_id, error=str(e))
        return {
            "status": "error",
            "error": str(e),
        }


def _create_bank_period_combinations(
    banks_result: Dict[str, Any], periods_result: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Transform banks and periods results into standardized combination records.

    Args:
        banks_result: Output from extract_banks with bank_ids, banks_detail, query_intent
        periods_result: Output from extract_periods with periods data

    Returns:
        List of bank-period combination records
    """
    logger = get_logger()
    combinations = []

    bank_ids = banks_result.get("bank_ids", [])
    banks_detail = banks_result.get("banks_detail", {})
    query_intent = banks_result.get("query_intent", "")

    periods_data = periods_result.get("periods", {})

    # Log what we're transforming
    logger.info(
        "clarifier.transformation.starting",
        bank_count=len(bank_ids),
        banks=[banks_detail.get(bid, {}).get("symbol", f"ID{bid}") for bid in bank_ids],
        period_type="uniform" if "apply_all" in periods_data else "bank-specific",
        query_intent=query_intent if query_intent else "not specified",
    )

    if "apply_all" in periods_data:
        # Same period applies to all banks
        period_info = periods_data["apply_all"]
        fiscal_year = period_info.get("fiscal_year")
        quarters = period_info.get("quarters", [])

        logger.debug(
            "clarifier.transformation.uniform_periods",
            fiscal_year=fiscal_year,
            quarters=quarters,
            applying_to_banks=len(bank_ids),
        )

        for bank_id in bank_ids:
            bank_info = banks_detail.get(bank_id, {})
            for quarter in quarters:
                combinations.append(
                    {
                        "bank_id": bank_id,
                        "bank_name": bank_info.get("name", ""),
                        "bank_symbol": bank_info.get("symbol", ""),
                        "fiscal_year": fiscal_year,
                        "quarter": quarter,
                        "query_intent": query_intent,
                    }
                )
    else:
        # Bank-specific periods - check for periods under bank IDs directly
        logger.debug(
            "clarifier.transformation.bank_specific_periods", processing_banks=len(bank_ids)
        )

        for bank_id in bank_ids:
            bank_info = banks_detail.get(bank_id, {})
            # Period data is stored directly under bank_id as key
            period_data = periods_data.get(str(bank_id))

            if period_data:
                fiscal_year = period_data.get("fiscal_year")
                quarters = period_data.get("quarters", [])

                logger.debug(
                    "clarifier.transformation.bank_periods",
                    bank_symbol=bank_info.get("symbol", ""),
                    fiscal_year=fiscal_year,
                    quarters=quarters,
                )

                for quarter in quarters:
                    combinations.append(
                        {
                            "bank_id": bank_id,
                            "bank_name": bank_info.get("name", ""),
                            "bank_symbol": bank_info.get("symbol", ""),
                            "fiscal_year": fiscal_year,
                            "quarter": quarter,
                            "query_intent": query_intent,
                        }
                    )

    # Log the final transformation result with clear individual combinations
    logger.info(
        "clarifier.transformation.completed",
        total_combinations=len(combinations),
        unique_banks=len(set(c["bank_id"] for c in combinations)),
        unique_periods=len(set((c["fiscal_year"], c["quarter"]) for c in combinations)),
    )

    # Log each combination individually for clarity
    for i, combo in enumerate(combinations[:5], 1):  # Show first 5
        logger.info(
            f"clarifier.combination.{i}",
            bank=combo["bank_symbol"],
            bank_name=combo["bank_name"],
            period=f"{combo['quarter']} {combo['fiscal_year']}",
            intent=combo.get("query_intent", "not specified"),
        )

    if len(combinations) > 5:
        logger.info(
            "clarifier.combinations.additional",
            remaining=len(combinations) - 5,
            message=f"... and {len(combinations) - 5} more combinations",
        )

    return combinations


async def clarify_query(
    query: str,
    context: Dict[str, Any],
    available_databases: Optional[List[str]] = None,
    messages: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """
    Main clarifier function that extracts both banks and periods.

    This is the primary entry point that orchestrates both extraction stages.
    Returns either successful extraction results or clarification questions.

    Args:
        query: User's query text (latest message)
        context: Runtime context
        available_databases: Optional database filter
        messages: Optional full conversation history for context

    Returns:
        Either successful extraction as list of bank-period combinations:
        [
            {
                "bank_id": 1,
                "bank_name": "Royal Bank of Canada",
                "bank_symbol": "RY",
                "fiscal_year": 2024,
                "quarter": "Q1",
                "query_intent": "revenue analysis"
            },
            ...
        ]

        Or clarification needed:
        {
            "status": "needs_clarification",
            "clarifications": ["Bank question", "Period question"]
        }
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    logger.info(
        "clarifier.starting",
        execution_id=execution_id,
        query_preview=query[:100] + "..." if len(query) > 100 else query,
        available_databases=available_databases if available_databases else "all",
    )

    # Stage 1: Extract banks and query intent
    bank_result = await extract_banks(query, context, available_databases, messages)

    # Stage 2: Extract or validate periods
    if bank_result["status"] == "success":
        # We have banks, extract periods for them
        bank_ids = bank_result.get("bank_ids", [])
        period_result = await extract_periods(query, bank_ids, context, available_databases, messages)

        # Check if we need any clarifications
        clarifications = []

        # Check for intent clarification
        if bank_result.get("intent_clarification"):
            clarifications.append(bank_result["intent_clarification"])

        # Check if periods need clarification
        if period_result.get("status") == "needs_clarification":
            clarifications.append(
                period_result.get("clarification", "What time period would you like to query?")
            )

        # If we have any clarifications needed, return them
        if clarifications:
            return {
                "status": "needs_clarification",
                "clarifications": clarifications,
            }

        # Everything successful - create and return standardized combinations
        if period_result.get("status") == "success":
            bank_period_combinations = _create_bank_period_combinations(bank_result, period_result)

            # Log successful completion with summary
            logger.info(
                "clarifier.completed_successfully",
                execution_id=execution_id,
                result_type="bank_period_combinations",
                total_combinations=len(bank_period_combinations),
                banks_extracted=[combo["bank_symbol"] for combo in bank_period_combinations[:2]],
                periods_extracted=[
                    f"{combo['quarter']} {combo['fiscal_year']}"
                    for combo in bank_period_combinations[:2]
                ],
                query_intent=(
                    bank_period_combinations[0].get("query_intent")
                    if bank_period_combinations
                    else None
                ),
            )

            # Return just the list of combinations
            return bank_period_combinations
        else:
            # Period extraction had an error
            return {
                "status": "error",
                "error": period_result.get("error", "Failed to extract periods"),
            }
    else:
        # Banks need clarification, check if periods also need clarification
        period_check = await extract_periods(query, None, context, available_databases, messages)

        # Collect clarification questions
        clarifications = []

        if bank_result.get("clarification"):
            clarifications.append(bank_result["clarification"])

        if period_check["status"] == "needs_clarification" and period_check.get("clarification"):
            clarifications.append(period_check["clarification"])

        return {
            "status": "needs_clarification",
            "clarifications": (
                clarifications
                if clarifications
                else ["Please provide more details about your query."]
            ),
        }
