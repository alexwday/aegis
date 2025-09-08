"""
Call Summary ETL - Generate comprehensive call summaries from transcripts.

This ETL directly calls transcript subagent functions to retrieve and analyze
earnings call transcripts for report generation.
"""

from .main import generate_call_summary, get_bank_info

__all__ = ["generate_call_summary", "get_bank_info"]