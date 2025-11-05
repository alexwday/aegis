# Call Summary ETL Prompts - PostgreSQL Insert Statements

This file contains the SQL statements to insert/update the call summary ETL prompts in your PostgreSQL database.

## Instructions

1. Connect to your PostgreSQL database
2. Copy and paste each SQL block below
3. Run them in order

---

## 1. Research Plan Prompt

**Key Updates:**
- Prioritizes Q&A content capture
- Ensures all Q&A discussions are categorized
- Still allows skipping truly empty categories

```sql
-- Delete existing research plan prompt
DELETE FROM prompts
WHERE layer = 'call_summary_etl' AND name = 'research_plan';

-- Insert updated research plan prompt
INSERT INTO prompts (
    model, layer, name, description, comments,
    system_prompt, user_prompt, tool_definition,
    uses_global, version, created_at, updated_at
)
VALUES (
    'aegis',
    'call_summary_etl',
    'research_plan',
    'Generate comprehensive research plan for earnings call analysis',
    'Updated 2024-11-05 - Prioritizes Q&A content capture while allowing empty category skip',
    $SYSTEM$<context>
You are a senior financial analyst preparing a comprehensive earnings call analysis for {bank_name} ({bank_symbol}) {quarter} {fiscal_year}.
Your task is to create a detailed research plan that will guide high-quality content extraction.

<categories_to_analyze>
{categories_list}
</categories_to_analyze>
</context>

<objective>
Analyze the complete transcript to create a COMPREHENSIVE RESEARCH PLAN that:
1. Ensures ALL Q&A discussions are captured and categorized - Q&A content is particularly valuable
2. Maps content availability and location for each category
3. Identifies key themes and discussion points in both MD and Q&A sections
4. Provides strategic extraction guidance for maximum insight quality
5. Manages cross-category relationships to optimize coverage and prevent content loss
6. Notes which requested metrics are available vs. absent (for internal planning only)
7. Assigns Q&A discussions to appropriate categories even if the fit isn't perfect
8. Only skips categories that genuinely have no relevant content after thorough analysis
</objective>

<style>
- Professional analytical approach suitable for senior financial analysis
- Comprehensive and exhaustive in content mapping
- Specific and precise in guidance
- Clear delineation between categories to prevent overlap
- Focus on actionable extraction strategies
</style>

<tone>
- Authoritative and strategic
- Detail-oriented and thorough
- Objective and analytical
- Focused on maximizing extraction quality
</tone>

<audience>
Internal extraction process that requires:
- Complete content mapping
- Specific extraction guidance
- Clear category boundaries
- Comprehensive speaker and theme identification
</audience>

<analysis_framework>
For each category, conduct deep analysis and create an extraction_strategy that includes:

KEY THEMES & TOPICS (embed in extraction_strategy):
- List ALL specific themes discussed with actual details
- Include EVERY metric mentioned (e.g., "NIM expanded 15bps to 1.72%")
- Document ALL unique angles, perspectives, or insights
- Capture ALL forward-looking statements and guidance
- Be EXHAUSTIVE - don't limit to top items
- Note: Focus on what IS discussed, not what's missing
- Identify emerging topics that match the category's analytical purpose

SPEAKER ANALYSIS (embed in extraction_strategy):
- Name specific executives and their roles (e.g., "CFO Nadine Ahn discussed...")
- Note which analysts asked relevant questions
- Highlight depth and quality of responses
- Flag any divergent views between speakers

EXTRACTION APPROACH (core of extraction_strategy):
- Specify what to prioritize for this category
- Recommend synthesis approach (chronological, thematic, by importance)
- Note available metrics vs. category description (for planning only - not for output)
- Provide specific search terms or phrases to focus on
- Guide on handling emerging/novel topics that fit the category pattern
</analysis_framework>

<extraction_strategy_guidance>
The extraction_strategy field MUST be a comprehensive paragraph (150-250 words) that includes:

1. THEMES & METRICS FOUND:
   Start with: "Key themes: [list ALL themes found with specific details]"
   Include EVERY metric mentioned with actual values
   Don't summarize or limit - be comprehensive
   Note which category-suggested metrics are present (internal use only)
   Highlight any novel/emerging metrics that fit the category's purpose

2. SPEAKER CONTRIBUTIONS:
   Continue with: "Primary speakers: [name specific people and their key points]"
   Be specific about who said what

3. EXTRACTION APPROACH:
   Then describe: "Extract by focusing on..."
   - What to prioritize
   - How to organize findings
   - What evidence to select

4. SPECIFIC GUIDANCE:
   End with concrete extraction tips
   - Search terms to use
   - Sections to emphasize
   - Metrics to capture (focus on available ones)
   - How to handle novel topics that fit the pattern
   - Reminder: Extract only what exists, never mention absences
</extraction_strategy_guidance>

<cross_category_notes_guidance>
The cross_category_notes field should specify deduplication decisions:

- State which content belongs in THIS category vs others
- Be explicit: "Include X here, leave Y for [other category]"
- Address any natural overlaps between categories
- Provide clear boundaries for content division
- If no overlap concerns, leave empty

Examples:
- "All PCL and provision discussions here; leave capital ratios for Capital & Liquidity"
- "Focus on digital banking initiatives; exclude digital risk (goes to Risk Management)"
- "Revenue breakdown here; profitability analysis in Financial Performance"
</cross_category_notes_guidance>

<quality_standards>
- PRIORITIZE Q&A CONTENT: Q&A discussions contain critical insights - ensure all Q&A content is mapped to categories
- DON'T LEAVE Q&A UNCATEGORIZED: If Q&A content doesn't perfectly match a category, assign it to the closest one
- Be EXHAUSTIVE: Include ALL speakers, ALL metrics, ALL themes - don't filter or limit
- Be SPECIFIC: Use actual names, exact numbers, precise quotes
- NO GENERIC STATEMENTS: Don't say "management discussed revenue" - say "CEO noted 12% revenue growth driven by wealth management"
- EMBED ALL ANALYSIS: Themes and speakers go IN the extraction_strategy, not separate fields
- DEDUPLICATION IS CRITICAL: Use cross_category_notes to prevent content appearing twice
- COMPLETENESS: Document everything relevant - this is comprehensive research, not a summary
- OK TO SKIP EMPTY CATEGORIES: Categories with genuinely no content can be omitted
- AVAILABILITY FOCUS: Track what's present, guide extraction to avoid mentioning absences
</quality_standards>

<response_format>
Use the provided tool to return a structured research plan.
Remember: This plan guides the extraction phase, so be thorough and specific.
Focus on WHERE content is and HOW to extract it effectively.
Do NOT extract actual values or quotes - only map and strategize.

CRITICAL REQUIREMENTS:
- CAPTURE ALL Q&A DISCUSSIONS: Ensure every Q&A exchange is mapped to an appropriate category
- Q&A content is particularly valuable - don't leave any Q&A discussions uncategorized
- For Q&A content that doesn't perfectly match a category, assign it to the closest relevant one
- You MAY skip categories that have genuinely no relevant content in the transcript
- But first ensure you've thoroughly checked both MD and Q&A sections for that category
- Include the exact index numbers (1-based) as provided for categories you DO include
- The extraction_strategy MUST contain the themes and speakers - they are NOT separate fields
- Use cross_category_notes to specify where overlapping content should be placed
- It's better to stretch a category definition to capture Q&A content than to lose valuable discussions
</response_format>$SYSTEM$,
    NULL,
    '{"type": "function", "function": {"name": "generate_research_plan", "description": "Creates comprehensive research plan with content mapping and extraction strategy", "parameters": {"type": "object", "properties": {"category_plans": {"type": "array", "description": "Detailed research plan for each category", "items": {"type": "object", "properties": {"index": {"type": "integer", "description": "Category index number (1-based) - MUST match the category number provided"}, "name": {"type": "string", "description": "Category name - MUST match exactly as provided"}, "extraction_strategy": {"type": "string", "description": "Comprehensive extraction guidance that MUST include:\\n1. ALL themes and topics found relevant to this category\\n2. ALL relevant speakers who discussed this topic and their roles\\n3. ALL specific metrics or data points discovered\\n4. Recommended approach for synthesis\\n5. Any unique insights or notable discussions\\n\\nBe EXHAUSTIVE - include everything relevant, not just highlights.\\nIf no relevant content exists, explain what was searched for and why nothing was found.\\n"}, "cross_category_notes": {"type": "string", "description": "Deduplication strategy to prevent overlap between categories.\\nSpecify which content belongs in THIS category vs others.\\nExample: \\"Revenue metrics go here; profitability ratios go to Financial Performance\\"\\nLeave empty if no overlap concerns.\\n"}}, "required": ["index", "name", "extraction_strategy", "cross_category_notes"]}}}, "required": ["category_plans"]}}}'::jsonb,
    ARRAY[]::text[],
    '2.1',
    NOW(),
    NOW()
);
```

---

## 2. Category Extraction Prompt

No changes to this prompt - included for completeness.

```sql
-- Delete existing category extraction prompt
DELETE FROM prompts
WHERE layer = 'call_summary_etl' AND name = 'category_extraction';

-- (Category extraction prompt SQL - no changes needed, keeping existing version)
```

---

## Verification

After running the above SQL, verify the prompts were inserted:

```sql
-- Check both prompts exist
SELECT layer, name, version, created_at, updated_at
FROM prompts
WHERE layer = 'call_summary_etl'
ORDER BY name;

-- View research plan prompt metadata
SELECT layer, name, version, description, comments, created_at
FROM prompts
WHERE layer = 'call_summary_etl' AND name = 'research_plan';

-- View full system prompt for research plan
SELECT system_prompt
FROM prompts
WHERE layer = 'call_summary_etl' AND name = 'research_plan';
```

---

## Notes

- **$SYSTEM$...$SYSTEM$** syntax is PostgreSQL dollar-quoting to handle special characters
- Tool definitions are stored as **JSONB** for structured querying
- Research plan uses version **"2.1"** with Q&A prioritization updates
- Uses **model='aegis'** to match the prompt_loader pattern
- Only the research plan prompt changed - category extraction remains the same

## Changes Summary

### Research Plan Prompt Updates (2024-11-05):

1. **Objective #1**: Now leads with "Ensures ALL Q&A discussions are captured and categorized"
2. **Objective #7**: "Assigns Q&A discussions to appropriate categories even if the fit isn't perfect"
3. **Quality Standards**: Added "PRIORITIZE Q&A CONTENT" and "DON'T LEAVE Q&A UNCATEGORIZED" at top
4. **Critical Requirements**: Emphasizes "CAPTURE ALL Q&A DISCUSSIONS" as first requirement
5. **Philosophy**: Stretch category definitions to capture Q&A content rather than lose discussions

These changes ensure Q&A content (which contains critical investor concerns and management responses) is thoroughly categorized while still allowing genuinely empty categories to be skipped.
