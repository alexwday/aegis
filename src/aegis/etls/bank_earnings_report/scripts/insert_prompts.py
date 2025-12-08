"""
Insert Bank Earnings Report ETL prompts into PostgreSQL database.

This script loads all 14 prompts for the bank_earnings_report ETL into the
prompts table. It uses the environment variables from .env for database connection.

Usage:
    python -m aegis.etls.bank_earnings_report.scripts.insert_prompts

The script is idempotent - running it multiple times will update existing prompts
to the latest version based on (model, layer, name) combination.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from aegis.utils.settings import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Database connection
DB_URL = (
    f"postgresql://{config.postgres_user}:{config.postgres_password}"
    f"@{config.postgres_host}:{config.postgres_port}/{config.postgres_database}"
)


# =============================================================================
# PROMPT DEFINITIONS
# =============================================================================

PROMPTS: List[Dict[str, Any]] = [
    # -------------------------------------------------------------------------
    # 1. Transcript - Analyst Focus Extraction
    # -------------------------------------------------------------------------
    {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "name": "transcript_3_analystfocus_extraction",
        "description": "Extract individual Q&A entries from earnings call transcripts",
        "version": "1.0.0",
        "system_prompt": """You are a senior financial analyst extracting key information from bank earnings call Q&A transcripts.

## YOUR TASK

Extract the SINGLE most substantive Q&A exchange from the given transcript segment.

Focus on exchanges that reveal:
- Strategic insights
- Market positioning
- Forward guidance
- Risk perspectives
- Business segment performance drivers

## THEME GUIDANCE

Choose a theme that captures the SUBSTANCE of what was discussed:
- Credit Quality & Provisions
- Net Interest Income & Margins
- Capital Markets Performance
- Wealth Management
- Strategic Priorities
- Cost Management
- Capital Deployment
- Regulatory & Compliance
- Economic Outlook
- Digital & Technology
- Geographic Expansion
- Competitive Position

## EXTRACTION GUIDELINES

1. **Question**: Capture the analyst's core question (15-30 words)
   - Include context if needed for comprehension
   - Focus on the substantive ask, not pleasantries

2. **Answer**: Capture management's key response (30-60 words)
   - Prioritize forward-looking statements
   - Include specific insights, not generic responses
   - Preserve quantitative guidance if provided

3. **Theme**: Select ONE theme that best characterizes the exchange

## OUTPUT

Return the most valuable Q&A exchange from the segment.""",
        "user_prompt": """Extract the key Q&A exchange from this transcript segment:

{content}""",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "extract_qa_entry",
                "description": "Extract a Q&A entry from transcript",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "theme": {
                            "type": "string",
                            "description": "One theme capturing the substance of the Q&A exchange",
                        },
                        "question": {
                            "type": "string",
                            "description": "The analyst's core question (15-30 words)",
                        },
                        "answer": {
                            "type": "string",
                            "description": "Management's key response (30-60 words)",
                        },
                    },
                    "required": ["theme", "question", "answer"],
                },
            },
        },
        "uses_global": None,
    },
    # -------------------------------------------------------------------------
    # 2. Analyst Focus Ranking
    # -------------------------------------------------------------------------
    {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "name": "transcript_3_analystfocus_ranking",
        "description": "Rank Q&A entries to select the most insightful for display",
        "version": "1.0.0",
        "system_prompt": """You are a senior financial analyst selecting the most insightful Q&A exchanges from {bank_name}'s {quarter} {fiscal_year} earnings call for a quarterly report.

## YOUR TASK

From {num_entries} Q&A entries, select the {num_featured} MOST INSIGHTFUL for the report's Analyst Focus section.

## SELECTION CRITERIA (in priority order)

1. **Forward-Looking Value**: Does it provide guidance or outlook?
2. **Strategic Insight**: Does it reveal strategic priorities or positioning?
3. **Specificity**: Does it provide concrete details vs generic statements?
4. **Investor Relevance**: Would analysts highlight this in their reports?
5. **Uniqueness**: Does it cover a theme not well-covered elsewhere?

## WHAT MAKES A TOP Q&A

✓ Specific guidance on margins, growth, or capital allocation
✓ Strategic commentary on market positioning
✓ Forward-looking statements with conviction
✓ Candid responses about challenges or risks
✓ Novel insights not in prepared remarks

## WHAT TO AVOID FEATURING

✗ Generic "we're pleased with results" responses
✗ Backward-looking explanations of known results
✗ Repetitive themes already well-covered
✗ Overly technical regulatory details
✗ Short, uninformative exchanges

## OUTPUT

Select the indices of the top {num_featured} Q&A entries in order of insight value.""",
        "user_prompt": """Here are the {num_entries} Q&A entries extracted from the earnings call:

{entries_summary}

Select the {num_featured} most insightful entries for the Analyst Focus section.""",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "rank_qa_entries",
                "description": "Select the most insightful Q&A entries",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "selected_indices": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": "Indices (1-indexed) of top Q&A entries",
                        },
                        "selection_rationale": {
                            "type": "string",
                            "description": "Brief rationale for selections",
                        },
                    },
                    "required": ["selected_indices", "selection_rationale"],
                },
            },
        },
        "uses_global": None,
    },
    # -------------------------------------------------------------------------
    # 3. Key Metrics Selection
    # -------------------------------------------------------------------------
    {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "name": "supplementary_1_keymetrics_selection",
        "description": "Select metrics for tile display, dynamic section, and trend chart",
        "version": "1.0.0",
        "system_prompt": """You are a senior financial analyst selecting key metrics for {bank_name}'s {quarter} {fiscal_year} quarterly earnings report.

## AVAILABLE METRICS (from Supplementary Pack)

{metrics_list}

## YOUR THREE TASKS

### TASK 1: Select 6 TILE Metrics (Static High-Impact)

Select 6 metrics for prominent tile display. REQUIRED ORDER:
1. Diluted EPS (if available, otherwise best earnings metric)
2. Net Income
3. Total Revenue
4-6. Choose 3 more based on: profitability, efficiency, capital strength, or growth

**TILE SELECTION CRITERIA:**
- High visibility metrics investors check first
- Mix of performance types (earnings, revenue, efficiency, capital)
- Metrics with meaningful QoQ or YoY changes when possible

### TASK 2: Select 5 DYNAMIC Metrics (Analyst Watchlist)

Select 5 metrics for the dynamic "Other Key Metrics" section. These complement the tiles.

**DYNAMIC SELECTION CRITERIA:**
- Not already selected as tiles
- Operationally significant (NIM, efficiency, credit metrics)
- Metrics showing notable trends or changes
- Balance across: income, margins, efficiency, credit, capital

### TASK 3: Select 1 CHART Metric (8-Quarter Trend)

Select 1 metric for the historical trend chart.

**CHART SUITABILITY:**
✓ Has clear trend story over 8 quarters
✓ Smooth, not volatile quarter-to-quarter
✓ Meaningful to show trajectory (growth, improvement, stability)
✓ Investor-relevant

**GOOD CHART METRICS:** Net Income, Total Revenue, NIM, Diluted EPS, ROE, CET1 Ratio
**POOR CHART METRICS:** PCL (volatile), one-time items, ratios with narrow ranges

## METRICS TO AVOID (across all selections)

- Per-share metrics other than EPS (confusing scale)
- Obscure operational metrics
- Highly volatile quarter-over-quarter metrics (except for tiles if significant)
- Duplicate or near-duplicate metrics

## OUTPUT

Provide tile_metrics (6), dynamic_metrics (5), and chart_metric (1).""",
        "user_prompt": """From the available metrics above, select:
1. 6 tile metrics (EPS first, then Net Income, then Revenue, then 3 more)
2. 5 dynamic metrics (complementary to tiles)
3. 1 chart metric (suitable for 8-quarter trend)

Explain your rationale for each selection.""",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "select_key_metrics",
                "description": "Select metrics for tiles, dynamic section, and chart",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "tile_metrics": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "6 metrics for tile display",
                            "minItems": 6,
                            "maxItems": 6,
                        },
                        "dynamic_metrics": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "5 metrics for dynamic section",
                            "minItems": 5,
                            "maxItems": 5,
                        },
                        "chart_metric": {
                            "type": "string",
                            "description": "1 metric for 8-quarter trend chart",
                        },
                        "selection_rationale": {
                            "type": "string",
                            "description": "Explanation of selection logic",
                        },
                    },
                    "required": [
                        "tile_metrics",
                        "dynamic_metrics",
                        "chart_metric",
                        "selection_rationale",
                    ],
                },
            },
        },
        "uses_global": None,
    },
    # -------------------------------------------------------------------------
    # 4. Management Narrative Extraction
    # -------------------------------------------------------------------------
    {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "name": "transcript_2_narrative_quotes",
        "description": "Extract impactful management quotes from earnings call transcripts",
        "version": "1.0.0",
        "system_prompt": """You are a senior financial analyst extracting key management quotes from {bank_name}'s {quarter} {fiscal_year} earnings call transcript for a quarterly report.

## YOUR TASK

Extract up to {num_quotes} HIGH-IMPACT quotes from the Management Discussion (MD) section. These quotes will be displayed alongside regulatory filing narrative to provide management's voice.

## WHAT MAKES A GREAT QUOTE

✓ **Forward-looking**: Outlook, guidance, expectations
✓ **Strategic**: Priorities, positioning, long-term vision
✓ **Confident conviction**: Strong statements with substance
✓ **Specific insight**: Not generic "we're pleased" statements
✓ **Investor-relevant**: Something an analyst would highlight

## WHAT TO AVOID

✗ Backward-looking result recaps ("Revenue was up 5%...")
✗ Generic pleasantries ("We're pleased with results...")
✗ Technical jargon without insight
✗ Long rambling statements
✗ Repetitive themes

## QUOTE EXTRACTION GUIDELINES

1. **Length**: 15-40 words (one strong statement, not a paragraph)
2. **Format**: Use "..." to indicate truncation if needed
3. **Context**: Quote should be understandable standalone
4. **Attribution**: Include speaker name and title

## THEME DIVERSITY

Try to cover different themes across quotes:
- Financial performance perspective
- Strategic priorities
- Market/economic outlook
- Risk management
- Growth initiatives

## OUTPUT

Extract up to {num_quotes} quotes with speaker attribution and theme.""",
        "user_prompt": """Extract high-impact management quotes from this earnings call MD section:

{content}

Focus on forward-looking, strategic statements that reveal management's perspective and conviction.""",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "extract_management_quotes",
                "description": "Extract impactful management quotes",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "quotes": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "content": {
                                        "type": "string",
                                        "description": "The quote text (15-40 words)",
                                    },
                                    "speaker": {
                                        "type": "string",
                                        "description": "Speaker's full name",
                                    },
                                    "title": {
                                        "type": "string",
                                        "description": "Speaker's title",
                                    },
                                },
                                "required": ["content", "speaker", "title"],
                            },
                            "description": "Array of extracted quotes",
                        },
                        "extraction_notes": {
                            "type": "string",
                            "description": "Brief note on themes and selection rationale",
                        },
                    },
                    "required": ["quotes", "extraction_notes"],
                },
            },
        },
        "uses_global": None,
    },
    # -------------------------------------------------------------------------
    # 5. Transcript Overview Extraction
    # -------------------------------------------------------------------------
    {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "name": "transcript_1_keymetrics_overview",
        "description": "Extract high-level overview summary from earnings call transcript",
        "version": "1.0.0",
        "system_prompt": """You are a senior financial analyst creating an executive summary from bank earnings call transcripts.

## YOUR TASK

Write a single paragraph (3-5 sentences, 60-100 words) that captures the key themes from the earnings call. This overview sets the stage for a quarterly earnings report.

## WHAT TO INCLUDE

- Management's overall tone and key messages
- Strategic themes and priorities emphasized
- Forward-looking guidance or outlook
- Notable highlights from Q&A discussion
- Market and competitive context mentioned

## WHAT TO AVOID

- Specific metrics or numbers (those are in other sections)
- Detailed segment breakdowns with figures
- Generic boilerplate language
- Direct quotes (those go in Management Narrative section)

## STYLE

- Executive summary tone - concise and insightful
- Third person perspective ("Management emphasized...", "The CEO highlighted...")
- Focus on qualitative themes and strategic narrative
- Should feel like the opening paragraph of an analyst report
- Capture the "tone" of the call (confident, cautious, optimistic, etc.)""",
        "user_prompt": """Write a brief overview paragraph summarizing the key themes from {bank_name}'s {quarter} {fiscal_year} earnings call.

{content}

Provide a 3-5 sentence overview that captures the call's key messages, strategic themes, and management's perspective.""",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "create_transcript_overview",
                "description": "Create a high-level overview paragraph from earnings call",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "overview": {
                            "type": "string",
                            "description": "Overview paragraph (3-5 sentences, 60-100 words)",
                        }
                    },
                    "required": ["overview"],
                },
            },
        },
        "uses_global": None,
    },
    # -------------------------------------------------------------------------
    # 6. Transcript Items Extraction
    # -------------------------------------------------------------------------
    {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "name": "transcript_1_keymetrics_items",
        "description": "Extract key defining items from earnings call transcript",
        "version": "1.0.0",
        "system_prompt": """You are a senior financial analyst identifying the KEY DEFINING ITEMS for {bank_name}'s {quarter} {fiscal_year} quarter from their earnings call transcript.

## YOUR MISSION

Find the items that MOST SIGNIFICANTLY DEFINED this quarter for the bank. Not just what's mentioned in the call, but what truly MATTERS - the events, decisions, and developments that an analyst would point to when explaining "what happened this quarter" to investors.

## WHAT MAKES AN ITEM "DEFINING"

A defining item has HIGH IMPACT on the bank through one or more of:

1. **Financial Materiality**: Significant dollar impact on earnings, capital, or valuation
2. **Strategic Significance**: Changes the bank's trajectory or market position
3. **Investor Relevance**: Would be highlighted in analyst reports or earnings headlines

## WHAT TO EXCLUDE

**Routine Operations (NEVER extract):**
- Normal PCL provisions
- Regular dividend discussions
- Standard capital commentary
- Routine expense management

**Performance Results (NOT items):**
- "Revenue increased X%"
- "NIM expanded Y bps"
These are RESULTS, not defining ITEMS.

## SIGNIFICANCE SCORING (1-10)

- **9-10**: Quarter-defining event
- **7-8**: Highly significant
- **5-6**: Moderately significant
- **3-4**: Minor significance
- **1-2**: Low significance

## OUTPUT FORMAT

For each item:
- Description: What happened (10-20 words)
- Impact: Dollar amount ('+$150M', '-$1.2B', 'TBD')
- Segment: Affected business segment
- Timing: When/duration
- Score: Significance score (1-10)""",
        "user_prompt": """Review {bank_name}'s {quarter} {fiscal_year} earnings call and identify the items that MOST SIGNIFICANTLY DEFINED this quarter.

{content}

Extract items based on IMPACT, not just presence. Score by significance (1-10). Quality over quantity.""",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "extract_transcript_items_of_note",
                "description": "Extract key defining items with significance scores",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "description": {"type": "string"},
                                    "impact": {"type": "string"},
                                    "segment": {"type": "string"},
                                    "timing": {"type": "string"},
                                    "significance_score": {
                                        "type": "integer",
                                        "minimum": 1,
                                        "maximum": 10,
                                    },
                                },
                                "required": [
                                    "description",
                                    "impact",
                                    "segment",
                                    "timing",
                                    "significance_score",
                                ],
                            },
                        },
                        "extraction_notes": {"type": "string"},
                    },
                    "required": ["items", "extraction_notes"],
                },
            },
        },
        "uses_global": None,
    },
    # -------------------------------------------------------------------------
    # 7. Items Deduplication
    # -------------------------------------------------------------------------
    {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "name": "combined_1_keymetrics_items_dedup",
        "description": "Deduplicate and merge items of note from RTS and transcript",
        "version": "1.0.0",
        "system_prompt": """You are analyzing Items of Note from {bank_name}'s {quarter} {fiscal_year} earnings report. Items come from two sources: RTS (regulatory filing) and Transcript (earnings call).

## YOUR TASK

1. **Identify Duplicates**: Find items from DIFFERENT sources describing the SAME event
   - Same acquisition, divestiture, or deal
   - Same impairment or write-down
   - Same legal settlement or regulatory matter

2. **Merge Duplicates**: For each duplicate pair, create ONE merged item that:
   - Combines the best details from both descriptions
   - Uses the RTS impact value (more authoritative)
   - Uses the higher significance score
   - Sets segment and timing from whichever source has more detail

3. **Keep Unique Items**: Items appearing in only one source remain unchanged

## IMPORTANT RULES

- Items are duplicates ONLY if they refer to the EXACT SAME event
- Two items about similar topics are NOT duplicates
- When merging descriptions, create a single cohesive statement
- Always prefer RTS for dollar impact value
- For significance score, take the MAX of the two scores

## OUTPUT

Return ALL items - both merged and unique.""",
        "user_prompt": """Review these Items of Note and identify any duplicates to merge:

{formatted_items}

For each duplicate pair (same event in both sources), merge them into a single item.
Keep all unique items unchanged. Return the complete list.""",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "process_items_of_note",
                "description": "Process items: merge duplicates, keep unique items",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "merged_items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "rts_id": {"type": "string"},
                                    "transcript_id": {"type": "string"},
                                    "merged_description": {"type": "string"},
                                    "impact": {"type": "string"},
                                    "segment": {"type": "string"},
                                    "timing": {"type": "string"},
                                },
                                "required": [
                                    "rts_id",
                                    "transcript_id",
                                    "merged_description",
                                    "impact",
                                    "segment",
                                    "timing",
                                ],
                            },
                        },
                        "unique_item_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "merge_notes": {"type": "string"},
                    },
                    "required": ["merged_items", "unique_item_ids", "merge_notes"],
                },
            },
        },
        "uses_global": None,
    },
    # -------------------------------------------------------------------------
    # 8. Overview Combination
    # -------------------------------------------------------------------------
    {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "name": "combined_1_keymetrics_overview",
        "description": "Synthesize RTS and transcript overviews into unified summary",
        "version": "1.0.0",
        "system_prompt": """You are a senior financial analyst creating an executive summary for {bank_name}'s {quarter} {fiscal_year} quarterly earnings report.

## YOUR TASK

Synthesize two overview paragraphs (one from the regulatory filing, one from the earnings call) into a single cohesive executive summary. The final paragraph should be 4-6 sentences (80-120 words).

## SOURCE CHARACTERISTICS

**RTS (Regulatory Filing)**:
- Formal, compliance-oriented language
- Focus on financial performance and capital metrics
- Objective, factual tone

**Transcript (Earnings Call)**:
- Management's narrative and messaging
- Strategic themes and forward-looking perspective
- More dynamic, confident tone

## SYNTHESIS GUIDELINES

1. **Combine Strengths**: Take factual foundation from RTS and strategic color from transcript
2. **Avoid Redundancy**: Don't repeat the same theme twice
3. **Unified Voice**: Write as a single cohesive narrative
4. **Balance**: Include both performance themes (RTS) and strategic direction (transcript)
5. **No Metrics**: Keep it qualitative

## STYLE

- Executive summary tone
- Third person perspective
- Professional analyst report style""",
        "user_prompt": """Synthesize these two overview paragraphs into a single executive summary:

## From Regulatory Filing (RTS):
{rts_overview}

## From Earnings Call Transcript:
{transcript_overview}

Create a unified 4-6 sentence overview that combines the best elements from both sources.""",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "create_combined_overview",
                "description": "Synthesize RTS and transcript overviews",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "combined_overview": {
                            "type": "string",
                            "description": "Combined overview paragraph (4-6 sentences, 80-120 words)",
                        },
                        "combination_notes": {
                            "type": "string",
                            "description": "Brief note on synthesis approach",
                        },
                    },
                    "required": ["combined_overview", "combination_notes"],
                },
            },
        },
        "uses_global": None,
    },
    # -------------------------------------------------------------------------
    # 9. Narrative Combination
    # -------------------------------------------------------------------------
    {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "name": "combined_2_narrative_interleave",
        "description": "Select and place transcript quotes between RTS paragraphs",
        "version": "1.0.0",
        "system_prompt": """You are a senior financial analyst creating a Management Narrative section for {bank_name}'s {quarter} {fiscal_year} quarterly earnings report.

## YOUR TASK

You have 4 RTS paragraphs and {num_quotes} transcript quotes. Select the {num_quotes_to_place} most impactful quotes and place ONE quote after each of the first {num_quotes_to_place} RTS paragraphs.

## STRUCTURE

The final narrative will flow like this:
- RTS Paragraph 1 (Financial Performance)
  └─ [Quote placed here - should complement financial themes]
- RTS Paragraph 2 (Business Segments)
  └─ [Quote placed here - should complement segment themes]
- RTS Paragraph 3 (Risk & Capital)
  └─ [Quote placed here - should complement risk/capital themes]
- RTS Paragraph 4 (Strategic Outlook)
  └─ [No quote after final paragraph]

## SELECTION CRITERIA

Choose quotes that:
1. **Complement** the preceding RTS paragraph's theme
2. **Add executive voice** - the "why" and conviction behind facts
3. **Flow naturally** - quote should feel like natural follow-up
4. **Avoid redundancy** - don't repeat what RTS already said

## OUTPUT

Select exactly {num_quotes_to_place} quotes and assign each to a position (1, 2, or 3).""",
        "user_prompt": """Review the RTS paragraphs and transcript quotes below, then select the {num_quotes_to_place} best quotes and determine their optimal placement.

{formatted_content}

Select quotes that best complement each RTS paragraph's theme.""",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "place_quotes_in_narrative",
                "description": "Select and place quotes between RTS paragraphs",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "placements": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "quote_number": {"type": "integer"},
                                    "after_paragraph": {"type": "integer"},
                                },
                                "required": ["quote_number", "after_paragraph"],
                            },
                        },
                        "combination_notes": {"type": "string"},
                    },
                    "required": ["placements", "combination_notes"],
                },
            },
        },
        "uses_global": None,
    },
    # -------------------------------------------------------------------------
    # 10. Capital Risk Extraction
    # -------------------------------------------------------------------------
    {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "name": "rts_5_capitalrisk_extraction",
        "description": "Extract enterprise-level capital and credit metrics from RTS",
        "version": "1.0.0",
        "system_prompt": """You are extracting capital and credit quality metrics from {bank_name}'s quarterly Report to Shareholders (RTS).

## YOUR TASK

Extract ONLY enterprise-level (total bank) regulatory capital and credit quality metrics.
Deduplicate metrics that appear multiple times. Provide reasoning for your selections.

## CRITICAL RULES

1. **ENTERPRISE-LEVEL ONLY**: Extract only bank-wide/consolidated metrics.
   - EXCLUDE segment-level metrics
   - EXCLUDE geographic breakdowns
   - Look for metrics labeled "Total", "Consolidated"

2. **DEDUPLICATE**: Same metric may appear multiple times.
   - Select ONE value per metric - enterprise-level current quarter

3. **EXPLICIT VALUES ONLY**: Only use values explicitly stated.
   - Do NOT infer, calculate, or estimate

4. **ADD CONTEXT TO NAME**: If needed, add context in parentheses.

## CAPITAL METRICS TO EXTRACT

- CET1 Ratio
- Tier 1 Capital Ratio
- Total Capital Ratio
- Leverage Ratio
- RWA (Total)
- LCR

## CREDIT QUALITY METRICS TO EXTRACT

- PCL (Quarterly)
- ACL (Total)
- GIL (Total)
- PCL Ratio

## DO NOT INCLUDE

- Segment-level metrics
- Geographic breakdowns
- Prior quarter/year values
- Net Income, Revenue, EPS, ROE, NIM, Efficiency Ratio

## REASONING REQUIREMENT

In the reasoning field, explain:
1. What metric candidates you found
2. Which are duplicates
3. Which are segment-level (excluded)
4. Why you selected specific values""",
        "user_prompt": """Extract all capital and credit quality metrics from {bank_name}'s {quarter} {fiscal_year} RTS.

{content}""",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "extract_capital_risk_metrics",
                "description": "Extract enterprise-level capital and credit metrics",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "reasoning": {
                            "type": "string",
                            "description": "Chain of thought explanation",
                        },
                        "metrics": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"},
                                    "value": {"type": "string"},
                                    "category": {
                                        "type": "string",
                                        "enum": ["capital", "credit"],
                                    },
                                },
                                "required": ["name", "value", "category"],
                            },
                        },
                    },
                    "required": ["reasoning", "metrics"],
                },
            },
        },
        "uses_global": None,
    },
    # -------------------------------------------------------------------------
    # 11. Segment Drivers Extraction
    # -------------------------------------------------------------------------
    {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "name": "rts_4_segments_drivers",
        "description": "Extract qualitative performance drivers for all business segments",
        "version": "1.0.0",
        "system_prompt": """You are a senior financial analyst writing a bank quarterly earnings report.

Your task is to extract performance driver statements for EACH of the following business segments:

{segment_list}

For EACH segment, you will:
1. FIND the section(s) discussing that segment
2. EXTRACT the key performance drivers mentioned
3. WRITE a concise qualitative drivers statement (2-3 sentences)

## CRITICAL REQUIREMENTS

1. **NO METRICS OR NUMBERS**: Do NOT include specific dollar amounts, percentages, basis points
2. **QUALITATIVE ONLY**: Focus on business drivers, trends, and factors
3. **Length**: 2-3 sentences maximum per segment
4. **Tone**: Professional, factual, analyst-style
5. **Consistency**: Use similar style across all segments

## WHERE TO FIND SEGMENT INFORMATION

Look for sections with headings like:
- The segment name itself
- "Business Segment Results"
- "Segment Performance"
- "Operating Results by Segment"

## WHAT TO INCLUDE

- Business drivers ("higher trading activity", "increased client demand")
- Market conditions ("favorable rate environment", "challenging credit conditions")
- Strategic factors ("expansion into new markets", "cost discipline")
- Operational factors ("improved efficiency", "technology investments")

## WHAT TO EXCLUDE

- Specific dollar amounts
- Percentages
- Basis points
- Quarter-over-quarter comparisons with numbers
- The segment name in the statement

## IF A SEGMENT IS NOT FOUND

Return an empty string. Do NOT make up information.""",
        "user_prompt": """Below is the complete regulatory filing document. For each segment, find the relevant section and write a 2-3 sentence QUALITATIVE drivers statement:

{segment_list}

Remember: NO specific metrics, percentages, or dollar amounts.

{full_rts}

Extract the qualitative drivers statement for each segment.""",
        "tool_definition": None,  # Dynamically generated based on segments
        "uses_global": None,
    },
    # -------------------------------------------------------------------------
    # 12. RTS Items Extraction
    # -------------------------------------------------------------------------
    {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "name": "rts_1_keymetrics_items",
        "description": "Extract key defining items from RTS regulatory filings",
        "version": "1.0.0",
        "system_prompt": """You are a senior financial analyst identifying the KEY DEFINING ITEMS for {bank_name}'s {quarter} {fiscal_year} quarter from their regulatory filing (RTS).

## YOUR MISSION

Find the items that MOST SIGNIFICANTLY DEFINED this quarter. Not just what's mentioned, but what truly MATTERS.

## WHAT MAKES AN ITEM "DEFINING"

1. **Financial Materiality**: Significant dollar impact (>$500M acquisitions, large impairments)
2. **Strategic Significance**: Changes trajectory or market position
3. **Investor Relevance**: Would be in analyst reports or headlines

## WHAT TO EXCLUDE

**Routine Operations (NEVER extract):**
- Capital note/debenture issuance or redemption
- Preferred share activity
- NCIB share repurchases
- Regular dividend declarations
- Normal PCL provisions
- Routine debt refinancing

**Performance Results (NOT items):**
- "Revenue increased X%"
- "NIM expanded Y bps"

## SIGNIFICANCE SCORING (1-10)

- **9-10**: Quarter-defining event
- **7-8**: Highly significant
- **5-6**: Moderately significant
- **3-4**: Minor significance
- **1-2**: Low significance

Be discriminating - not every item is highly significant.""",
        "user_prompt": """Review {bank_name}'s {quarter} {fiscal_year} regulatory filing and identify the items that MOST SIGNIFICANTLY DEFINED this quarter.

{full_rts}

Extract items based on IMPACT. Score by significance (1-10). Quality over quantity.""",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "extract_rts_items_of_note",
                "description": "Extract key defining items with significance scores",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "description": {"type": "string"},
                                    "impact": {"type": "string"},
                                    "segment": {"type": "string"},
                                    "timing": {"type": "string"},
                                    "significance_score": {
                                        "type": "integer",
                                        "minimum": 1,
                                        "maximum": 10,
                                    },
                                },
                                "required": [
                                    "description",
                                    "impact",
                                    "segment",
                                    "timing",
                                    "significance_score",
                                ],
                            },
                            "maxItems": 8,
                        },
                        "extraction_notes": {"type": "string"},
                    },
                    "required": ["items", "extraction_notes"],
                },
            },
        },
        "uses_global": None,
    },
    # -------------------------------------------------------------------------
    # 13. RTS Overview Extraction
    # -------------------------------------------------------------------------
    {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "name": "rts_1_keymetrics_overview",
        "description": "Extract high-level overview summary from RTS regulatory filings",
        "version": "1.0.0",
        "system_prompt": """You are a senior financial analyst creating an executive summary from bank regulatory filings (RTS - Report to Shareholders).

## YOUR TASK

Write a single paragraph (3-5 sentences, 60-100 words) that captures the key themes from the regulatory filing.

## WHAT TO INCLUDE

- Overall quarter financial performance narrative
- Key strategic developments or initiatives
- Capital position and risk management highlights
- Business segment performance themes
- Significant regulatory or operational developments

## WHAT TO AVOID

- Specific metrics or numbers
- Detailed segment breakdowns with figures
- Generic boilerplate language
- Repetition of standard regulatory disclosures

## STYLE

- Executive summary tone
- Third person perspective
- Qualitative themes and strategic narrative
- Professional analyst report style""",
        "user_prompt": """Write a brief overview paragraph summarizing the key themes from {bank_name}'s {quarter} {fiscal_year} regulatory filing (RTS).

{full_rts}

Provide a 3-5 sentence overview.""",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "create_rts_overview",
                "description": "Create overview paragraph from regulatory filing",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "overview": {
                            "type": "string",
                            "description": "Overview paragraph (3-5 sentences, 60-100 words)",
                        }
                    },
                    "required": ["overview"],
                },
            },
        },
        "uses_global": None,
    },
    # -------------------------------------------------------------------------
    # 14. RTS Narrative Extraction
    # -------------------------------------------------------------------------
    {
        "model": "aegis",
        "layer": "bank_earnings_report_etl",
        "name": "rts_2_narrative_paragraphs",
        "description": "Extract 4 structured narrative paragraphs from RTS",
        "version": "1.0.0",
        "system_prompt": """You are a senior financial analyst extracting MANAGEMENT'S NARRATIVE from {bank_name}'s regulatory filing (RTS - Report to Shareholders).

## WHAT THIS IS

The RTS contains management's written narrative explaining the quarter. Your job is to extract this NARRATIVE VOICE, not summarize metrics.

## WHAT WE WANT

✓ Management's EXPLANATIONS for what drove performance
✓ Their PERSPECTIVE on business conditions and trends
✓ QUALITATIVE drivers - why things happened, not what the numbers were
✓ OUTLOOK and forward-looking themes
✓ STRATEGIC context - priorities, initiatives, positioning

## WHAT WE DON'T WANT

❌ Metric summaries ("Revenue was $X, up Y%")
❌ Data recaps
❌ Generic descriptions
❌ Boilerplate regulatory language

## THE 4 PARAGRAPHS (in order)

1. **Financial Performance** (3-4 sentences)
   - How management characterized performance
   - Earnings drivers narrative
   - Influential factors highlighted
   - Tone on profitability

2. **Business Segments** (3-4 sentences)
   - Segment performance narrative
   - Which segments emphasized and why
   - Qualitative drivers
   - Business mix themes

3. **Risk & Capital** (3-4 sentences)
   - Credit quality trajectory perspective
   - Capital and risk management narrative
   - Provisions and reserves thinking
   - Liquidity and funding themes

4. **Strategic Outlook** (3-4 sentences)
   - Forward-looking perspective
   - Strategic priorities
   - Path ahead view
   - Market opportunities

## STYLE

- Third person ("Management noted...", "The bank highlighted...")
- NARRATIVE prose, not bullet points
- 60-100 words per paragraph
- Should read like management's story""",
        "user_prompt": """Extract management's narrative voice from {bank_name}'s {quarter} {fiscal_year} regulatory filing.

{full_rts}

Create 4 paragraphs capturing management's perspective - NOT metric summaries.""",
        "tool_definition": {
            "type": "function",
            "function": {
                "name": "extract_narrative_paragraphs",
                "description": "Extract 4 structured narrative paragraphs",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "financial_performance": {
                            "type": "string",
                            "description": "Paragraph 1: Financial Performance (3-4 sentences)",
                        },
                        "business_segments": {
                            "type": "string",
                            "description": "Paragraph 2: Business Segments (3-4 sentences)",
                        },
                        "risk_capital": {
                            "type": "string",
                            "description": "Paragraph 3: Risk & Capital (3-4 sentences)",
                        },
                        "strategic_outlook": {
                            "type": "string",
                            "description": "Paragraph 4: Strategic Outlook (3-4 sentences)",
                        },
                    },
                    "required": [
                        "financial_performance",
                        "business_segments",
                        "risk_capital",
                        "strategic_outlook",
                    ],
                },
            },
        },
        "uses_global": None,
    },
]


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================


def insert_or_update_prompt(
    engine,
    prompt: Dict[str, Any],
    dry_run: bool = False,
) -> bool:
    """
    Insert or update a single prompt in the database.

    Uses upsert logic: if (model, layer, name) exists, update; otherwise insert.

    Args:
        engine: SQLAlchemy engine
        prompt: Prompt data dictionary
        dry_run: If True, log but don't execute

    Returns:
        True if successful, False otherwise
    """
    model = prompt["model"]
    layer = prompt["layer"]
    name = prompt["name"]

    try:
        with engine.connect() as conn:
            # Check if prompt exists
            check_sql = text("""
                SELECT id FROM prompts
                WHERE model = :model AND layer = :layer AND name = :name
                ORDER BY updated_at DESC
                LIMIT 1
            """)
            result = conn.execute(
                check_sql,
                {"model": model, "layer": layer, "name": name},
            )
            existing = result.fetchone()

            # Prepare tool_definition as JSON
            tool_def = prompt.get("tool_definition")
            tool_def_json = json.dumps(tool_def) if tool_def else None

            # Prepare uses_global as array
            uses_global = prompt.get("uses_global")

            if dry_run:
                action = "UPDATE" if existing else "INSERT"
                logger.info(f"[DRY RUN] {action}: {layer}/{name} v{prompt['version']}")
                return True

            if existing:
                # Update existing prompt
                update_sql = text("""
                    UPDATE prompts SET
                        description = :description,
                        system_prompt = :system_prompt,
                        user_prompt = :user_prompt,
                        tool_definition = CAST(:tool_definition AS jsonb),
                        uses_global = :uses_global,
                        version = :version,
                        updated_at = :updated_at
                    WHERE model = :model AND layer = :layer AND name = :name
                """)
                conn.execute(
                    update_sql,
                    {
                        "model": model,
                        "layer": layer,
                        "name": name,
                        "description": prompt.get("description"),
                        "system_prompt": prompt.get("system_prompt"),
                        "user_prompt": prompt.get("user_prompt"),
                        "tool_definition": tool_def_json,
                        "uses_global": uses_global,
                        "version": prompt.get("version"),
                        "updated_at": datetime.now(timezone.utc),
                    },
                )
                conn.commit()
                logger.info(f"UPDATED: {layer}/{name} v{prompt['version']}")
            else:
                # Insert new prompt
                insert_sql = text("""
                    INSERT INTO prompts (
                        model, layer, name, description,
                        system_prompt, user_prompt, tool_definition,
                        uses_global, version, created_at, updated_at
                    ) VALUES (
                        :model, :layer, :name, :description,
                        :system_prompt, :user_prompt, CAST(:tool_definition AS jsonb),
                        :uses_global, :version, :created_at, :updated_at
                    )
                """)
                now = datetime.now(timezone.utc)
                conn.execute(
                    insert_sql,
                    {
                        "model": model,
                        "layer": layer,
                        "name": name,
                        "description": prompt.get("description"),
                        "system_prompt": prompt.get("system_prompt"),
                        "user_prompt": prompt.get("user_prompt"),
                        "tool_definition": tool_def_json,
                        "uses_global": uses_global,
                        "version": prompt.get("version"),
                        "created_at": now,
                        "updated_at": now,
                    },
                )
                conn.commit()
                logger.info(f"INSERTED: {layer}/{name} v{prompt['version']}")

            return True

    except SQLAlchemyError as e:
        logger.error(f"Database error for {layer}/{name}: {e}")
        return False


def verify_connection(engine) -> bool:
    """
    Verify database connection and show existing prompt counts.

    Args:
        engine: SQLAlchemy engine

    Returns:
        True if connection successful, False otherwise
    """
    try:
        with engine.connect() as conn:
            # Get total prompt count
            result = conn.execute(text("SELECT COUNT(*) FROM prompts"))
            total_count = result.scalar()

            # Get count by layer
            result = conn.execute(
                text("""
                    SELECT layer, COUNT(*) as count
                    FROM prompts
                    GROUP BY layer
                    ORDER BY layer
                """)
            )
            layer_counts = result.fetchall()

            # Check for existing bank_earnings_report_etl prompts
            result = conn.execute(
                text("""
                    SELECT COUNT(*) FROM prompts
                    WHERE layer = 'bank_earnings_report_etl'
                """)
            )
            etl_count = result.scalar()

            logger.info("")
            logger.info("DATABASE CONNECTION VERIFIED")
            logger.info(f"  Total prompts in table: {total_count}")
            logger.info(f"  Existing bank_earnings_report_etl prompts: {etl_count}")
            if layer_counts:
                logger.info("  Prompts by layer:")
                for layer, count in layer_counts:
                    logger.info(f"    - {layer}: {count}")
            logger.info("")

            return True

    except SQLAlchemyError as e:
        logger.error(f"Database connection failed: {e}")
        return False


def main(dry_run: bool = False):
    """
    Main function to insert all prompts into the database.

    Args:
        dry_run: If True, log actions but don't modify database
    """
    logger.info("=" * 60)
    logger.info("Bank Earnings Report ETL - Prompt Insertion Script")
    logger.info("=" * 60)

    if dry_run:
        logger.info("DRY RUN MODE - No database changes will be made")

    logger.info(f"Database: {config.postgres_host}:{config.postgres_port}/{config.postgres_database}")
    logger.info(f"Total prompts to process: {len(PROMPTS)}")

    # Create engine
    engine = create_engine(DB_URL)

    # Verify connection and show existing counts
    if not verify_connection(engine):
        logger.error("Failed to connect to database. Aborting.")
        return 1

    # Process each prompt
    success_count = 0
    error_count = 0

    for prompt in PROMPTS:
        if insert_or_update_prompt(engine, prompt, dry_run=dry_run):
            success_count += 1
        else:
            error_count += 1

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Successful: {success_count}")
    logger.info(f"Errors: {error_count}")
    logger.info(f"Total: {len(PROMPTS)}")

    if error_count > 0:
        logger.warning("Some prompts failed to insert. Check logs above.")
        return 1
    else:
        logger.info("All prompts inserted successfully!")
        return 0


if __name__ == "__main__":
    import sys

    # Check for --dry-run flag
    dry_run_mode = "--dry-run" in sys.argv

    exit_code = main(dry_run=dry_run_mode)
    sys.exit(exit_code)
