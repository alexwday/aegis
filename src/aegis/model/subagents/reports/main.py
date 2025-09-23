"""
Reports Subagent - Pre-generated Reports Retrieval

This subagent retrieves pre-generated reports from the aegis_reports table.
It provides access to call summaries and other analysis reports that have been
generated through ETL processes.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Generator, List
import json

# Import Aegis utilities
from ....utils.logging import get_logger
from ....utils.prompt_loader import load_subagent_prompt
from ....utils.settings import config
from ....connections.llm_connector import complete_with_tools
from ....utils.monitor import add_monitor_entry, format_llm_call

# Import local retrieval and formatting functions
from .retrieval import (
    get_available_reports,
    get_unique_report_types,
    retrieve_reports_by_type
)
from .formatting import (
    format_report_content,
    format_multiple_reports,
    format_no_data_message,
    format_error_message
)


def reports_agent(
    conversation: List[Dict[str, str]],
    latest_message: str,
    bank_period_combinations: List[Dict[str, Any]],
    basic_intent: str,
    full_intent: str,
    database_id: str,
    context: Dict[str, Any],
) -> Generator[Dict[str, str], None, None]:
    """
    Reports subagent - retrieves pre-generated reports from database.

    This function retrieves reports from the aegis_reports table based on
    the bank and period combinations provided by the planner.

    Parameters:
        conversation: Full chat history
        latest_message: Most recent user message
        bank_period_combinations: Banks/periods to query
        basic_intent: Simple interpretation
        full_intent: Detailed interpretation
        database_id: Always "reports" for this subagent
        context: Contains execution_id, auth_config, ssl_config

    Yields:
        Dictionaries with type="subagent", name="reports", content=text
    """

    # Initialize logging and tracking
    logger = get_logger()
    execution_id = context.get("execution_id")
    stage_start = datetime.now(timezone.utc)

    logger.info(
        f"subagent.{database_id}.started",
        execution_id=execution_id,
        latest_message=(
            latest_message[:100] + "..." if len(latest_message) > 100 else latest_message
        ),
        num_banks=len(bank_period_combinations),
        basic_intent=basic_intent,
    )

    # Log the banks and periods we're working with
    for combo in bank_period_combinations[:3]:  # Show first 3 as preview
        logger.debug(
            f"subagent.{database_id}.bank_period",
            execution_id=execution_id,
            bank=f"{combo['bank_name']} ({combo['bank_symbol']})",
            period=f"{combo['quarter']} {combo['fiscal_year']}",
        )

    try:
        # Step 1: Check what report types are available
        unique_report_types = get_unique_report_types(bank_period_combinations, context)

        if not unique_report_types:
            # No reports available - return informative message
            logger.info(
                f"subagent.{database_id}.no_reports",
                execution_id=execution_id,
                combinations=len(bank_period_combinations)
            )

            no_data_msg = format_no_data_message(bank_period_combinations, context)
            yield {
                "type": "subagent",
                "name": database_id,
                "content": no_data_msg
            }

            # Add monitoring entry
            add_monitor_entry(
                stage_name="Subagent_Reports",
                stage_start_time=stage_start,
                stage_end_time=datetime.now(timezone.utc),
                status="Success",
                decision_details="No reports available for requested combinations",
                custom_metadata={
                    "subagent": database_id,
                    "banks": [combo["bank_id"] for combo in bank_period_combinations],
                    "reports_found": 0
                }
            )
            return

        logger.info(
            f"subagent.{database_id}.types_found",
            execution_id=execution_id,
            report_types=[rt["report_type"] for rt in unique_report_types]
        )

        # Step 2: Determine which report type to retrieve based on user intent
        # For now, if there's only one type, use it. Otherwise, use LLM to select.
        selected_report_type = None

        if len(unique_report_types) == 1:
            # Only one type available, use it
            selected_report_type = unique_report_types[0]["report_type"]
            logger.info(
                f"subagent.{database_id}.auto_selected",
                execution_id=execution_id,
                report_type=selected_report_type
            )
        else:
            # Multiple types available - use LLM to select appropriate one
            # This would involve a tool call to select the best report type
            # For now, default to first available type
            selected_report_type = unique_report_types[0]["report_type"]
            logger.info(
                f"subagent.{database_id}.default_selection",
                execution_id=execution_id,
                report_type=selected_report_type,
                available_types=[rt["report_type"] for rt in unique_report_types]
            )

        # Step 3: Retrieve the selected reports
        reports = retrieve_reports_by_type(
            bank_period_combinations,
            selected_report_type,
            context
        )

        if not reports:
            # No reports found for the selected type
            logger.warning(
                f"subagent.{database_id}.retrieval_empty",
                execution_id=execution_id,
                report_type=selected_report_type
            )

            yield {
                "type": "subagent",
                "name": database_id,
                "content": f"No {selected_report_type} reports found for the specified banks and periods."
            }

            add_monitor_entry(
                stage_name="Subagent_Reports",
                stage_start_time=stage_start,
                stage_end_time=datetime.now(timezone.utc),
                status="Success",
                decision_details=f"No {selected_report_type} reports found",
                custom_metadata={
                    "subagent": database_id,
                    "report_type": selected_report_type,
                    "reports_found": 0
                }
            )
            return

        # Step 4: Format and yield the reports
        logger.info(
            f"subagent.{database_id}.formatting_reports",
            execution_id=execution_id,
            num_reports=len(reports)
        )

        # Check if we're dealing with a single report or multiple
        if len(reports) == 1:
            # Single report - format with full content
            formatted_content = format_report_content(reports[0], include_links=True, context=context)
        else:
            # Multiple reports - format consolidated view
            formatted_content = format_multiple_reports(reports, context, bank_period_combinations)

        # Stream the formatted content
        for line in formatted_content.split('\n'):
            if line.strip():  # Skip empty lines for cleaner output
                yield {
                    "type": "subagent",
                    "name": database_id,
                    "content": line + "\n"
                }

        # Step 5: Add monitoring entry
        stage_end = datetime.now(timezone.utc)
        add_monitor_entry(
            stage_name="Subagent_Reports",
            stage_start_time=stage_start,
            stage_end_time=stage_end,
            status="Success",
            decision_details=f"Retrieved {len(reports)} {selected_report_type} reports",
            custom_metadata={
                "subagent": database_id,
                "banks": [combo["bank_id"] for combo in bank_period_combinations],
                "report_type": selected_report_type,
                "reports_retrieved": len(reports),
                "report_ids": [r["id"] for r in reports],
                "total_content_length": sum(len(r.get("markdown_content") or "") for r in reports)
            }
        )

        logger.info(
            f"subagent.{database_id}.completed",
            execution_id=execution_id,
            total_duration_ms=int((stage_end - stage_start).total_seconds() * 1000),
            reports_delivered=len(reports)
        )

    except Exception as e:
        # Error handling
        error_msg = str(e)
        logger.error(
            f"subagent.{database_id}.error",
            execution_id=execution_id,
            error=error_msg,
            exc_info=True,
        )

        # Add monitoring entry for the failure
        add_monitor_entry(
            stage_name="Subagent_Reports",
            stage_start_time=stage_start,
            stage_end_time=datetime.now(timezone.utc),
            status="Failure",
            error_message=error_msg,
            custom_metadata={
                "subagent": database_id,
                "error_type": type(e).__name__,
            }
        )

        # Yield error message to user
        yield {
            "type": "subagent",
            "name": database_id,
            "content": format_error_message(error_msg, context)
        }


def select_report_type(
    report_types: List[Dict[str, str]],
    user_intent: str,
    context: Dict[str, Any]
) -> str:
    """
    Use LLM to select the most appropriate report type based on user intent.

    Args:
        report_types: List of available report types with descriptions
        user_intent: The user's query intent
        context: Runtime context

    Returns:
        Selected report_type string
    """
    logger = get_logger()
    execution_id = context.get("execution_id")

    # Define tool for report selection
    tool = {
        "type": "function",
        "function": {
            "name": "select_report",
            "description": "Select the most appropriate report type",
            "parameters": {
                "type": "object",
                "properties": {
                    "report_type": {
                        "type": "string",
                        "description": "The selected report type",
                        "enum": [rt["report_type"] for rt in report_types]
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Brief explanation of why this report type was selected"
                    }
                },
                "required": ["report_type", "reasoning"]
            }
        }
    }

    # Build prompt for selection
    report_options = "\n".join([
        f"- {rt['report_type']}: {rt['report_name']} - {rt['report_description']}"
        for rt in report_types
    ])

    messages = [
        {
            "role": "system",
            "content": "You are a report selection assistant. Select the most appropriate report type based on the user's intent."
        },
        {
            "role": "user",
            "content": f"""User Intent: {user_intent}

Available Report Types:
{report_options}

Select the report type that best matches what the user is looking for."""
        }
    ]

    try:
        response = complete_with_tools(
            messages=messages,
            tools=[tool],
            context=context,
            llm_params={
                "model": config.llm.small.model,  # Use small model for simple selection
                "temperature": 0.3
            }
        )

        if response.get("choices") and response["choices"][0].get("message"):
            tool_calls = response["choices"][0]["message"].get("tool_calls", [])
            if tool_calls:
                args = json.loads(tool_calls[0]["function"]["arguments"])
                selected_type = args["report_type"]
                reasoning = args["reasoning"]

                logger.info(
                    f"subagent.reports.type_selected",
                    execution_id=execution_id,
                    selected_type=selected_type,
                    reasoning=reasoning
                )

                return selected_type

    except Exception as e:
        logger.error(
            f"subagent.reports.selection_error",
            execution_id=execution_id,
            error=str(e)
        )

    # Fallback to first available type
    return report_types[0]["report_type"]