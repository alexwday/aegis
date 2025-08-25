"""
Planner agent for determining which databases to query based on user requests.

The planner analyzes the user's query and clarifier outputs to:
1. Determine relevant databases for the query
2. Create complete, self-contained query intents for each database
3. Validate database availability for the requested banks/periods
"""

import json
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from ...connections.postgres_connector import _get_engine
from ...connections.llm_connector import complete_with_tools
from ...utils.logging import get_logger
from ...utils.prompt_loader import load_yaml
from ...utils.settings import config


def get_filtered_availability_table(
    bank_ids: List[int],
    periods: Dict[str, Any],
    available_databases: Optional[List[str]] = None
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
    engine = _get_engine()
    
    try:
        # Build query to get availability for specific banks and periods
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
        
        params = {"bank_ids": bank_ids}
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            
            # Build availability structure
            availability = {}
            available_dbs_set = set()
            
            for row in result:
                bank_id = str(row[0])
                bank_name = row[1]
                bank_symbol = row[2]
                fiscal_year = row[3]
                quarter = row[4]
                databases = row[5] or []
                
                # Filter databases if specified
                if available_databases:
                    databases = [db for db in databases if db in available_databases]
                
                # Check if this row matches the requested periods
                period_match = False
                
                if "apply_all" in periods:
                    # Same period for all banks
                    period_info = periods["apply_all"]
                    if fiscal_year == period_info["fiscal_year"] and quarter in period_info["quarters"]:
                        period_match = True
                else:
                    # Bank-specific periods
                    if bank_id in periods:
                        period_info = periods[bank_id]
                        if fiscal_year == period_info["fiscal_year"] and quarter in period_info["quarters"]:
                            period_match = True
                
                # Only include rows that match the requested periods
                if period_match and databases:
                    if bank_id not in availability:
                        availability[bank_id] = {
                            "name": bank_name,
                            "symbol": bank_symbol,
                            "periods": []
                        }
                    
                    availability[bank_id]["periods"].append({
                        "fiscal_year": fiscal_year,
                        "quarter": quarter,
                        "databases": databases
                    })
                    
                    # Track all available databases (already filtered)
                    available_dbs_set.update(databases)
            
            # Format as table
            table_lines = []
            table_lines.append("\n<availability_table>")
            table_lines.append("Filtered by requested banks and periods:")
            table_lines.append("\nBank | Name                         | Year | Quarter | Available Databases")
            table_lines.append("-----|------------------------------|------|---------|--------------------")
            
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
                "table": "\n".join(table_lines)
            }
            
    except Exception as e:
        logger.error("Failed to get filtered availability", error=str(e))
        return {
            "availability": {},
            "available_databases": [],
            "table": "<availability_table>No data available</availability_table>"
        }


def get_filtered_database_descriptions(available_databases: List[str]) -> str:
    """
    Load and filter database descriptions based on available databases.
    
    Args:
        available_databases: List of database IDs that are available
        
    Returns:
        Formatted string with database descriptions
    """
    logger = get_logger()
    
    try:
        # Load the database.yaml file
        database_data = load_yaml("global/database.yaml")
        
        if "databases" not in database_data:
            return ""
        
        # Filter to only include available databases
        filtered_lines = ["\n<database_descriptions>"]
        filtered_lines.append("Available databases for this query:\n")
        
        for db_info in database_data["databases"]:
            if db_info["id"] in available_databases:
                filtered_lines.append(f"Database: {db_info['id']}")
                filtered_lines.append(f"Name: {db_info['name']}")
                filtered_lines.append(db_info["content"])
                filtered_lines.append("")
        
        filtered_lines.append("</database_descriptions>\n")
        
        return "\n".join(filtered_lines)
        
    except Exception as e:
        logger.error("Failed to load database descriptions", error=str(e))
        return ""


def plan_database_queries(
    query: str,
    conversation: List[Dict[str, str]],
    banks: Dict[str, Any],
    periods: Dict[str, Any],
    context: Dict[str, Any],
    available_databases: Optional[List[str]] = None,
    query_intent: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Plan which databases to query based on user request and clarifier outputs.
    
    Args:
        query: User's latest message
        conversation: Full conversation history
        banks: Output from clarifier with bank_ids and details
        periods: Output from clarifier with period information
        context: Runtime context with auth, SSL config, execution_id
        available_databases: Optional list of database IDs to filter (from model input)
        query_intent: Optional query intent extracted by the clarifier (e.g., 'revenue', 'efficiency ratio')
        
    Returns:
        Dictionary with planned database queries and intents
    """
    logger = get_logger()
    execution_id = context.get("execution_id")
    
    try:
        logger.info(
            "planner.starting",
            execution_id=execution_id,
            bank_count=len(banks.get("bank_ids", [])),
            has_periods="apply_all" in periods or len(periods) > 0,
            available_databases=available_databases,
            query_intent=query_intent if query_intent else "not_specified"
        )
        
        # Extract bank IDs from clarifier output
        bank_ids = banks.get("bank_ids", [])
        if not bank_ids:
            return {
                "status": "error",
                "error": "No banks provided from clarifier"
            }
        
        # Get periods from clarifier output
        period_info = periods.get("periods", periods)
        if not period_info:
            return {
                "status": "error",
                "error": "No periods provided from clarifier"
            }
        
        # Get filtered availability table
        availability_data = get_filtered_availability_table(
            bank_ids=bank_ids,
            periods=period_info,
            available_databases=available_databases
        )
        
        if not availability_data["available_databases"]:
            return {
                "status": "no_data",
                "message": "No databases have data for the requested banks and periods",
                "databases": []
            }
        
        # Get filtered database descriptions
        database_descriptions = get_filtered_database_descriptions(
            availability_data["available_databases"]
        )
        
        # Load planner prompt
        planner_data = load_yaml("aegis/planner.yaml")
        
        # Build system prompt
        prompt_parts = []
        
        # Add availability table
        prompt_parts.append(availability_data["table"])
        
        # Add database descriptions
        prompt_parts.append(database_descriptions)
        
        # Add planner instructions
        if "content" in planner_data:
            prompt_parts.append(planner_data["content"].strip())
        
        system_prompt = "\n\n".join(prompt_parts)
        
        # Build conversation context for the planner
        conversation_context = "Previous conversation:\n"
        for msg in conversation[-5:]:  # Last 5 messages for context
            conversation_context += f"{msg['role']}: {msg['content']}\n"
        
        # Build user message with intent if available
        user_message = f"{conversation_context}\n\nLatest query: {query}\n\n"
        if query_intent:
            user_message += f"User is asking for: {query_intent}\n\n"
        user_message += "Determine which databases to query and provide complete, self-contained query intents for each database."
        
        # Create messages
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message}
        ]
        
        # Define tools for database planning
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "databases_selected",
                    "description": "Return the list of databases to query with their intents",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "databases": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "database_id": {
                                            "type": "string",
                                            "description": "The database ID (e.g., 'transcripts', 'benchmarking')"
                                        },
                                        "query_intent": {
                                            "type": "string",
                                            "description": "Complete, self-contained description of what to query"
                                        }
                                    },
                                    "required": ["database_id", "query_intent"]
                                },
                                "description": "List of databases with their query intents"
                            }
                        },
                        "required": ["databases"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "no_databases_needed",
                    "description": "Explain why no databases are needed for this query",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "reason": {
                                "type": "string",
                                "description": "Explanation of why no databases are needed"
                            }
                        },
                        "required": ["reason"]
                    }
                }
            }
        ]
        
        # Call LLM with tools
        model_tier_override = context.get("model_tier_override")
        if model_tier_override == "small":
            model = config.llm.small.model
        elif model_tier_override == "large":
            model = config.llm.large.model
        else:
            model = config.llm.medium.model  # Default to medium for planning
        
        response = complete_with_tools(
            messages=messages,
            tools=tools,
            context=context,
            llm_params={
                "model": model,
                "temperature": 0.1,  # Low temperature for consistent rule following
                "max_tokens": 500,
            }
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
                    
                    # Extract database IDs safely for logging and validation
                    database_ids = []
                    for db in databases:
                        if isinstance(db, dict) and "database_id" in db:
                            database_ids.append(db["database_id"])
                        elif isinstance(db, str):
                            # Handle case where database is just a string ID
                            database_ids.append(db)
                    
                    # CRITICAL: Filter to only available databases
                    available_set = set(availability_data["available_databases"])
                    filtered_databases = []
                    rejected_databases = []
                    
                    for i, db_id in enumerate(database_ids):
                        if db_id in available_set:
                            filtered_databases.append(databases[i])
                        else:
                            rejected_databases.append(db_id)
                    
                    if rejected_databases:
                        logger.warning(
                            "planner.databases_rejected",
                            execution_id=execution_id,
                            rejected=rejected_databases,
                            reason="Not in available databases",
                            available=list(available_set)
                        )
                    
                    # Update database_ids to only include filtered ones
                    filtered_ids = []
                    for db in filtered_databases:
                        if isinstance(db, dict) and "database_id" in db:
                            filtered_ids.append(db["database_id"])
                        elif isinstance(db, str):
                            filtered_ids.append(db)
                    
                    logger.info(
                        "planner.databases_selected",
                        execution_id=execution_id,
                        database_count=len(filtered_databases),
                        databases=filtered_ids
                    )
                    
                    return {
                        "status": "success",
                        "databases": filtered_databases,
                        "tokens_used": tokens_used,
                        "cost": cost
                    }
                
                elif function_name == "no_databases_needed":
                    reason = function_args.get("reason", "")
                    
                    logger.info(
                        "planner.no_databases",
                        execution_id=execution_id,
                        reason=reason
                    )
                    
                    return {
                        "status": "no_databases",
                        "reason": reason,
                        "databases": [],
                        "tokens_used": tokens_used,
                        "cost": cost
                    }
        
        # Fallback if no tool response
        return {
            "status": "error",
            "error": "Failed to determine databases to query",
            "tokens_used": tokens_used,
            "cost": cost
        }
        
    except Exception as e:
        logger.error("planner.error", execution_id=execution_id, error=str(e))
        return {
            "status": "error",
            "error": str(e)
        }