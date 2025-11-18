"""
Key Themes ETL - Extract and group earnings call themes from Q&A sections.

This ETL directly calls transcript subagent functions to retrieve Q&A data
and analyze themes for report generation.
"""

from .main import generate_key_themes, get_bank_info

__all__ = ["generate_key_themes", "get_bank_info"]
