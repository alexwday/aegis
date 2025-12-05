"""
Extraction functions for transforming raw data into JSON sections.

Uses LLM to extract and format data according to the report template schemas.

Modules:
- key_metrics.py: Chart and tile metric selection
- segment_metrics.py: Business segment definitions and core metrics
- analyst_focus.py: Q&A theme extraction
- management_narrative.py: Transcript quote extraction
- transcript_insights.py: Overview and items extraction from transcripts
- overview_combination.py: Combine RTS and transcript overviews
- narrative_combination.py: Interleave RTS paragraphs with transcript quotes
- items_deduplication.py: Deduplicate and rank items from multiple sources
- capital_risk.py: Capital ratios, RWA, and credit quality from RTS
"""
