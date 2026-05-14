"""
Subagent modules for Aegis model.

These subagents handle specific database queries after the planner
determines which databases to query.
"""

from typing import Any, AsyncGenerator, Dict, List

from .supplementary.main import supplementary_agent
from .pillar3.main import pillar3_agent
from .rts.main import rts_agent
from .transcripts.main import transcripts_agent
from .supplementary_financials import supplementary_financials_agent

try:
    from .reports.main import reports_agent
except ModuleNotFoundError:
    reports_agent = None


async def supplementary_financials_runtime_adapter(
    conversation: List[Dict[str, str]],
    latest_message: str,
    bank_period_combinations: List[Dict[str, Any]],
    basic_intent: str,
    full_intent: str,
    database_id: str,
    context: Dict[str, Any],
) -> AsyncGenerator[Dict[str, str], None]:
    """Adapt the guide-compatible placeholder to Aegis' current runtime call."""

    async for chunk in supplementary_financials_agent(
        conversation=conversation,
        latest_message=latest_message,
        bank_period_combinations=bank_period_combinations,
        basic_intent=basic_intent,
        full_intent=full_intent,
        database_id=database_id,
        context=context,
        user_req=context.get("user_req"),
    ):
        yield chunk

# Mapping from database_id to subagent function
SUBAGENT_MAPPING = {
    "supplementary": supplementary_agent,
    "supplementary_financials": supplementary_financials_runtime_adapter,
    "pillar3": pillar3_agent,
    "rts": rts_agent,
    "transcripts": transcripts_agent,
}

if reports_agent is not None:
    SUBAGENT_MAPPING["reports"] = reports_agent

__all__ = [
    "SUBAGENT_MAPPING",
    "supplementary_agent",
    "supplementary_financials_agent",
    "pillar3_agent",
    "reports_agent",
    "rts_agent",
    "transcripts_agent",
]
