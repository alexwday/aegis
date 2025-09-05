"""
Transcripts subagent main entry point.
Retrieves real transcript data from PostgreSQL database.
"""

from typing import Any, Dict, Generator, List

# Import utilities
from .utils import load_financial_categories, get_filter_diagnostics

# Import retrieval functions (for backward compatibility if needed elsewhere)
from .retrieval import (
    retrieve_full_section,
    retrieve_by_categories,
    retrieve_by_similarity
)

# Import the silent version
from .main_silent import transcripts_agent_silent


def transcripts_agent(
    conversation: List[Dict[str, str]],
    latest_message: str,
    bank_period_combinations: List[Dict[str, Any]],
    basic_intent: str,
    full_intent: str,
    database_id: str,
    context: Dict[str, Any],
) -> Generator[Dict[str, str], None, None]:
    """
    Main transcripts agent - uses silent version for cleaner output.
    Only outputs final combined research statements.
    
    Args:
        conversation: Full chat history
        latest_message: Most recent user message
        bank_period_combinations: List of bank-period combos to query
        basic_intent: Simple interpretation of query
        full_intent: Detailed interpretation
        database_id: "transcripts"
        context: Runtime context with auth and execution_id
        
    Yields:
        Dict with type="subagent", name="transcripts", content=research
    """
    return transcripts_agent_silent(
        conversation=conversation,
        latest_message=latest_message,
        bank_period_combinations=bank_period_combinations,
        basic_intent=basic_intent,
        full_intent=full_intent,
        database_id=database_id,
        context=context
    )


# Keep the verbose version for debugging if needed
def transcripts_agent_verbose(
    conversation: List[Dict[str, str]],
    latest_message: str,
    bank_period_combinations: List[Dict[str, Any]],
    basic_intent: str,
    full_intent: str,
    database_id: str,
    context: Dict[str, Any],
) -> Generator[Dict[str, str], None, None]:
    """
    Verbose version of transcripts agent for debugging.
    Shows all intermediate processing steps.
    
    To use this version, import transcripts_agent_verbose instead of transcripts_agent.
    """
    # This would contain the original verbose implementation
    # For now, just use the silent version
    return transcripts_agent_silent(
        conversation=conversation,
        latest_message=latest_message,
        bank_period_combinations=bank_period_combinations,
        basic_intent=basic_intent,
        full_intent=full_intent,
        database_id=database_id,
        context=context
    )