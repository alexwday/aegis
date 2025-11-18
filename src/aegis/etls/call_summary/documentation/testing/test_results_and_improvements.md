# Call Summary ETL - User Feedback & Implementation Summary

**Version History**: v2.1.0 → v2.2.0 → v2.2.1 → v2.3.0
**Last Updated**: November 6, 2024

---

## Feedback #1: Quote Length

### Verbatim User Feedback
> "when we pull direct quotes from the transcripts we are pulling not only the punchline but also all relevant background commentary that provides context of the discussion. while it might be hard to get to an exact match using the ai tool, the preference would be to longer quotes that can be deleted and cut short rather than just the punchline."

### Problem Identified
The LLM was extracting only key insights without surrounding context, making quotes feel like isolated soundbites rather than complete thoughts with background.

### Solution Applied
**Prompt Changes (category_extraction v2.2.0):**
- Added explicit instruction to include "BOTH the punchline AND relevant background commentary"
- Guidance to "start quotes earlier to capture setup and context"
- Recommendation to extract 3-4 sentences with background rather than single-sentence conclusions

**Practical Result:**
Users receive longer, contextual quotes that can be trimmed down rather than having to search the transcript for missing context.

---

## Feedback #2: Selective Evidence Usage

### Verbatim User Feedback
> "we dont generally require quotes that just defines the performance in the quarter. to the extent quotes are used it is for highlighting key drivers of the result that could be a potential read through for us. most of the other quotes we use are related to either strategic topics or to capture forward guidance/outlook."

### Problem Identified
The system was quoting everything uniformly, including basic performance numbers that don't need attribution (e.g., "Revenue was $13.2 billion").

### Solution Applied
**Prompt Changes (category_extraction v2.2.0):**
- Added new section: "QUOTE SELECTION STRATEGY"
- Prioritize quotes for: drivers, strategic initiatives, outlook, risks, novel insights
- Paraphrase instead for: basic numbers, routine comparisons, standard definitions
- Rule of thumb: "Quote the 'why' and 'what's next', paraphrase the 'what happened'"

**Tool Definition Changes (v2.2.1):**
- Made evidence array optional (removed `minItems: 1` requirement)
- Allows statements with paraphrased metrics to have no evidence section

**Practical Result:**
Reports contain fewer but more valuable quotes focused on strategic content, with basic metrics presented as clean paraphrases.

---

## Feedback #3: Category Duplication & Misclassification

### Verbatim User Feedback
> "the classification of content into categories were off in a few places and were duplicated into multiple categories in a few places (eg. regulatory vs capital)."

### Problem Identified
Content appeared in multiple categories (e.g., capital ratios discussed in both "Regulatory" and "Capital Management") because category boundaries weren't clearly enforced.

### Solution Applied
**Prompt Changes (research_plan v2.2.0):**
- Made `cross_category_notes` field mandatory for deduplication
- Required explicit designation of PRIMARY ownership for cross-cutting themes
- Added specific examples of common overlaps (regulatory vs capital, performance vs strategy)

**Prompt Changes (category_extraction v2.2.0):**
- Strengthened deduplication strategy from "suggested" to "MANDATORY"
- Added semantic similarity examples showing what constitutes duplication
- "ZERO TOLERANCE" language for extracting content already covered

**Code Changes (main.py v2.2.1):**
- Enhanced `extracted_themes` context to include first 3 quote snippets per statement
- Added passive duplicate detection logging (70% similarity threshold)
- Added validation warning when `cross_category_notes` are missing or too brief

**Practical Result:**
Each piece of content appears in only one category with clear ownership boundaries. Logging provides visibility into potential overlaps without disrupting workflow.

---

## Additional Enhancement: Q&A Content Prioritization

### Problem Identified
Q&A sections contain critical investor concerns and management responses but were sometimes underutilized or left uncategorized when they didn't perfectly match category definitions.

### Solution Applied (research_plan v2.3.0)
**Prompt Changes:**
- Added explicit objective: "Ensures ALL Q&A discussions are captured and categorized"
- Guidance to assign Q&A to closest category even if fit isn't perfect
- Priority instruction: "Better to stretch a category definition to capture Q&A content than to lose valuable discussions"
- Allows skipping only genuinely empty categories after thorough MD and Q&A review

**Practical Result:**
No valuable Q&A content is lost due to overly strict category matching. Every investor question and management response gets captured somewhere in the report.

---

## Technical Improvements (v2.2.1)

### Validation Constraints
Added `maxLength` and `maxItems` constraints to all tool definition fields to prevent runaway LLM responses and ensure predictable output sizes.

### Quote Context Enhancement
System now shows up to 3 quote snippets per statement in the deduplication context, giving the LLM better visibility into which quotes have already been used.

### Monitoring Capabilities
Added passive duplicate detection that logs potential overlaps (70%+ similarity) without blocking workflow. Provides visibility for quality review without false positive risk.

---

## Summary of Changes by Version

**v2.1.0 → v2.2.0** (November 5, 2024)
- Longer quotes with context (Feedback #1)
- Selective quote strategy (Feedback #2)
- Mandatory deduplication guidance (Feedback #3)

**v2.2.0 → v2.2.1** (November 5, 2024)
- Made evidence array optional (tool definition fix)
- Added validation constraints
- Enhanced deduplication context with multiple quote snippets
- Added passive duplicate detection logging
- Clarified "longer quotes" applies only to selected quotes

**v2.2.1 → v2.3.0** (November 6, 2024)
- Q&A content prioritization in research plan
- Explicit guidance to capture all Q&A discussions
- Flexibility to stretch category definitions for Q&A
- Permission to skip only genuinely empty categories

---

## Current Status

All user feedback has been addressed through prompt engineering and minimal code changes:
- ✅ Quotes include contextual background
- ✅ Quotes are used selectively for strategic content
- ✅ Duplication is minimized through mandatory boundaries
- ✅ Q&A content is fully captured and categorized

The system prioritizes prompt-based solutions (transparent, debuggable) over automated filtering (risk of false positives).
