# Bank Earnings Report ETL - Prompt Audit

## Summary

This audit reviews all 14 LLM prompts in the bank_earnings_report ETL for consistency, quality, and alignment before migration to database storage.

---

## Prompt Inventory

| # | Module | Prompt Name | Purpose | Config Model Key |
|---|--------|-------------|---------|------------------|
| 1 | analyst_focus.py | analyst_focus_extraction | Extract Q&A entry (theme, question, answer) | analyst_focus_extraction |
| 2 | analyst_focus.py | analyst_focus_ranking | Rank Q&A entries to select featured ones | analyst_focus_extraction |
| 3 | key_metrics.py | key_metrics_selection | Select tile metrics, dynamic metrics, chart | key_metrics_selection |
| 4 | management_narrative.py | management_narrative_extraction | Extract management quotes | management_narrative_extraction |
| 5 | transcript_insights.py | transcript_overview_extraction | Extract overview from transcript | transcript_overview_extraction |
| 6 | transcript_insights.py | transcript_items_extraction | Extract items of note from transcript | transcript_items_extraction |
| 7 | items_deduplication.py | items_deduplication | Deduplicate/merge items from both sources | items_deduplication |
| 8 | overview_combination.py | overview_combination | Combine RTS and transcript overviews | overview_combination |
| 9 | narrative_combination.py | narrative_combination | Interleave RTS paragraphs with quotes | narrative_combination |
| 10 | capital_risk.py | capital_risk_extraction | Extract capital/credit metrics from RTS | capital_risk_extraction |
| 11 | rts.py | segment_drivers_extraction | Extract segment drivers from RTS | segment_drivers_extraction |
| 12 | rts.py | rts_items_extraction | Extract items of note from RTS | rts_items_extraction |
| 13 | rts.py | rts_overview_extraction | Extract overview from RTS | rts_overview_extraction |
| 14 | rts.py | rts_narrative_extraction | Extract 4 narrative paragraphs from RTS | rts_narrative_extraction |

---

## Cross-Prompt Consistency Audit

### 1. Placeholder Consistency

**Current State:**
- `{bank_name}` - Used in most prompts ✓
- `{quarter}` - Consistent ✓
- `{fiscal_year}` - Consistent ✓
- `{num_quotes}` - Used in management_narrative (variable)
- `{num_featured}` - Used in analyst_focus_ranking (variable)
- `{num_quotes_to_place}` - Used in narrative_combination (variable)

**Issue Found:** `capital_risk.py` uses `{bank_name}` but builds it via separate function (`_build_system_prompt(bank_name)`) rather than format string. This is fine but different pattern.

**Recommendation:** Standardize all prompts to use `{bank_name}`, `{quarter}`, `{fiscal_year}` as base placeholders.

### 2. Prompt Structure Consistency

**Expected Structure:**
```
## YOUR TASK
<description>

## GUIDELINES/CRITERIA/REQUIREMENTS
<specific instructions>

## WHAT TO INCLUDE/EXCLUDE
<guidance>

## OUTPUT/STYLE
<format expectations>
```

**Audit Results:**

| Prompt | Has Task Section | Has Guidelines | Has Include/Exclude | Has Output Style |
|--------|-----------------|----------------|---------------------|------------------|
| analyst_focus_extraction | ✓ | ✓ | ✗ (implicit) | ✓ (in tool desc) |
| analyst_focus_ranking | ✓ | ✓ | ✓ | ✓ |
| key_metrics_selection | ✓ (TASK 1,2,3) | ✓ | ✗ | ✓ |
| management_narrative_extraction | ✓ | ✓ | ✓ | ✓ |
| transcript_overview_extraction | ✓ | ✗ | ✓ | ✓ |
| transcript_items_extraction | ✓ | ✓ | ✓ | ✓ |
| items_deduplication | ✓ | ✓ | ✗ | ✓ |
| overview_combination | ✓ | ✓ | ✗ | ✓ |
| narrative_combination | ✓ | ✓ | ✗ | ✓ |
| capital_risk_extraction | ✓ | ✓ | ✓ | ✓ |
| segment_drivers_extraction | ✓ | ✓ | ✓ | ✗ |
| rts_items_extraction | ✓ | ✓ | ✓ | ✓ |
| rts_overview_extraction | ✓ | ✗ | ✓ | ✓ |
| rts_narrative_extraction | ✓ | ✓ | ✗ | ✓ |

### 3. Terminology Consistency

**Items to check:**
- "Quarter" vs "quarter" - Mixed case OK in prose
- "Q&A" vs "QA" vs "Q&A exchange" - **INCONSISTENT**
  - analyst_focus uses "Q&A exchange"
  - transcript_items uses "Q&A discussions"
  - Recommendation: Standardize to "Q&A exchange" or "Q&A"

- "Defining items" vs "Items of note" - Both used appropriately
- "RTS" explanation - Only defined in some prompts

### 4. Line Continuation Style

**Issue:** Many prompts use Python string continuation with `\` which makes them harder to read/edit:
```python
system_prompt = """You are a senior financial analyst extracting key information from bank \
earnings call Q&A transcripts.
```

**Recommendation:** Use triple-quoted strings without continuation for cleaner formatting when migrating to database.

### 5. Tool Definition Consistency

**Schema Pattern Check:**

| Prompt | Uses tool_calls | Has required array | Uses enum where appropriate |
|--------|-----------------|-------------------|---------------------------|
| analyst_focus_extraction | ✓ | ✓ | ✗ (could use for theme) |
| analyst_focus_ranking | ✓ | ✓ | ✗ |
| key_metrics_selection | ✓ | ✓ | ✓ |
| management_narrative_extraction | ✓ | ✓ | ✗ |
| transcript_overview_extraction | ✓ | ✓ | ✗ |
| transcript_items_extraction | ✓ | ✓ | ✗ |
| items_deduplication | ✓ | ✓ | ✓ (dynamic from input) |
| overview_combination | ✓ | ✓ | ✗ |
| narrative_combination | ✓ | ✓ | ✓ (dynamic from input) |
| capital_risk_extraction | ✓ | ✓ | ✓ |
| segment_drivers_extraction | ✓ | ✓ | ✗ (dynamic props) |
| rts_items_extraction | ✓ | ✓ | ✗ |
| rts_overview_extraction | ✓ | ✓ | ✗ |
| rts_narrative_extraction | ✓ | ✓ | ✗ |

---

## Individual Prompt Quality Review

### 1. analyst_focus_extraction (Quality: GOOD)
- Clear task definition
- Good examples of themes
- Length guidelines for question/answer
- **Minor issue:** Could add explicit word count guidance in tool description

### 2. analyst_focus_ranking (Quality: GOOD)
- Clear selection criteria
- Good prioritization guidance
- **Minor issue:** Could add more specific ranking rationale guidance

### 3. key_metrics_selection (Quality: EXCELLENT)
- Very detailed 3-task structure
- Clear chart suitability guidance
- Good metric exclusion list
- **No changes needed**

### 4. management_narrative_extraction (Quality: EXCELLENT)
- Excellent "WHAT TO / NOT TO" structure
- Good examples of good vs bad quotes
- Clear ellipsis usage guidance
- **No changes needed**

### 5. transcript_overview_extraction (Quality: GOOD)
- Clear style guidance
- Word count specified
- **Minor issue:** Missing explicit "GUIDELINES" header section

### 6. transcript_items_extraction (Quality: EXCELLENT)
- Very thorough "WHAT MAKES DEFINING" section
- Excellent scoring rubric
- Clear exclusions
- **No changes needed**

### 7. items_deduplication (Quality: GOOD)
- Clear merge rules
- Good handling of "SAME event" distinction
- **Minor issue:** Could benefit from examples of duplicates vs similar-but-different

### 8. overview_combination (Quality: GOOD)
- Clear source characteristics
- Good synthesis guidelines
- **Minor issue:** Could add example of good synthesis

### 9. narrative_combination (Quality: GOOD)
- Clear structure diagram
- Good selection criteria
- **Minor issue:** Could clarify "complement" vs "repeat"

### 10. capital_risk_extraction (Quality: EXCELLENT)
- Excellent deduplication guidance
- Clear enterprise vs segment distinction
- Reasoning requirement is smart
- **No changes needed**

### 11. segment_drivers_extraction (Quality: EXCELLENT)
- Very clear "NO METRICS" emphasis
- Good examples of what to include/exclude
- Handles not-found case
- **No changes needed**

### 12. rts_items_extraction (Quality: EXCELLENT)
- Nearly identical to transcript_items - good consistency
- Same scoring rubric
- **No changes needed**

### 13. rts_overview_extraction (Quality: GOOD)
- Similar to transcript_overview
- **Minor issue:** Slightly less detailed than transcript version

### 14. rts_narrative_extraction (Quality: EXCELLENT)
- Excellent "WHAT WE WANT / DON'T WANT" with emojis
- Clear 4-paragraph structure
- Good narrative vs data distinction
- **No changes needed**

---

## Issues Requiring Fixes Before Migration

### Critical (Must Fix)
None identified.

### Recommended Improvements

1. **Standardize Q&A terminology** - Use "Q&A exchange" consistently across analyst_focus and transcript prompts

2. **Add word count to tool descriptions** - Several prompts mention word counts in system prompt but not in tool parameter descriptions

3. **Remove Python string continuations** - When migrating to DB, clean up `\` line continuations for readability

4. **Add version headers** - All prompts should have version metadata when stored

### Optional Enhancements

1. Add examples section to items_deduplication showing duplicate vs non-duplicate scenarios

2. Add example synthesis to overview_combination

3. Align transcript_overview and rts_overview prompt detail levels

---

## Database Schema Mapping

| Prompt Name | layer | name |
|-------------|-------|------|
| analyst_focus_extraction | bank_earnings_report_etl | analyst_focus_extraction |
| analyst_focus_ranking | bank_earnings_report_etl | analyst_focus_ranking |
| key_metrics_selection | bank_earnings_report_etl | key_metrics_selection |
| management_narrative_extraction | bank_earnings_report_etl | management_narrative_extraction |
| transcript_overview_extraction | bank_earnings_report_etl | transcript_overview_extraction |
| transcript_items_extraction | bank_earnings_report_etl | transcript_items_extraction |
| items_deduplication | bank_earnings_report_etl | items_deduplication |
| overview_combination | bank_earnings_report_etl | overview_combination |
| narrative_combination | bank_earnings_report_etl | narrative_combination |
| capital_risk_extraction | bank_earnings_report_etl | capital_risk_extraction |
| segment_drivers_extraction | bank_earnings_report_etl | segment_drivers_extraction |
| rts_items_extraction | bank_earnings_report_etl | rts_items_extraction |
| rts_overview_extraction | bank_earnings_report_etl | rts_overview_extraction |
| rts_narrative_extraction | bank_earnings_report_etl | rts_narrative_extraction |

---

## Conclusion

Overall prompt quality is **HIGH**. The bank_earnings_report ETL has well-crafted, detailed prompts with good guidance.

**Proceed with migration after applying recommended improvements:**
1. Clean up line continuations
2. Standardize Q&A terminology
3. Add version metadata

No critical issues blocking migration.
