"""
Subagent modules for Aegis model.

These subagents handle specific database queries after the planner
determines which databases to query.
"""

from .benchmarking import benchmarking_agent
from .reports import reports_agent
from .rts import rts_agent
from .transcripts import transcripts_agent
from .pillar3 import pillar3_agent

# Mapping from database_id to subagent function
SUBAGENT_MAPPING = {
    "benchmarking": benchmarking_agent,
    "reports": reports_agent,
    "rts": rts_agent,
    "transcripts": transcripts_agent,
    "pillar3": pillar3_agent,
}

__all__ = [
    "SUBAGENT_MAPPING",
    "benchmarking_agent",
    "reports_agent",
    "rts_agent",
    "transcripts_agent",
    "pillar3_agent",
]
