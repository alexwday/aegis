"""
Subagent modules for Aegis model.

These subagents handle specific database queries after the planner
determines which databases to query.
"""

from .supplementary.main import supplementary_agent
from .pillar3.main import pillar3_agent
from .reports.main import reports_agent
from .rts.main import rts_agent
from .transcripts.main import transcripts_agent

# Mapping from database_id to subagent function
SUBAGENT_MAPPING = {
    "supplementary": supplementary_agent,
    "pillar3": pillar3_agent,
    "reports": reports_agent,
    "rts": rts_agent,
    "transcripts": transcripts_agent,
}

__all__ = [
    "SUBAGENT_MAPPING",
    "supplementary_agent",
    "pillar3_agent",
    "reports_agent",
    "rts_agent",
    "transcripts_agent",
]
