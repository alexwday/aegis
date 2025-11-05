# Call Summary ETL - User Feedback & Implementation Changes

**Date**: 2025-11-05
**Prompts Affected**: `research_plan` (v2.1.0), `category_extraction` (v2.1.0)
**Code Affected**: `src/aegis/etls/call_summary/main.py`

---

## Feedback #1: Quote Length

### Verbatim Feedback
> "when we pull direct quotes from the transcripts we are pulling not only the punchline but also all relevant background commentary that provides context of the discussion. while it might be hard to get to an exact match using the ai tool, the preference would be to longer quotes that can be deleted and cut short rather than just the punchline."

### Root Cause
- Current prompt guidance: "Evidence that adds depth, context, or nuance to the statement"
- This ambiguous phrasing allows LLM to interpret as "just the key insight"
- No explicit instruction to include background commentary that provides context
- Tool definition similarly ambiguous about including surrounding context

### Changes Made

#### 1. `category_extraction` System Prompt - Section 4: EVIDENCE SELECTION
**Changed from:**
```
4. EVIDENCE SELECTION:
   For each statement, include ALL relevant evidence:
   - Direct quotes that add context or depth
   - Paraphrases of complex explanations
   - Multiple perspectives if available
   - Speaker attribution for credibility
   - Use __underline__ for critical phrases within quotes

   Evidence enriches understanding - be comprehensive, not selective
```

**Changed to:**
```
4. EVIDENCE SELECTION:
   For each statement, include ALL relevant evidence with FULL CONTEXT:
   - Direct quotes should include BOTH the punchline AND relevant background commentary
   - Start quotes earlier to capture the setup and context of the discussion
   - Include explanatory phrases that precede the key insight
   - Paraphrases of complex explanations when quotes would be too long
   - Multiple perspectives if available
   - Speaker attribution for credibility
   - Use __underline__ for critical phrases within quotes

   CRITICAL: Prefer longer quotes with context over short punchlines
   Better to include 3-4 sentences with background than just the conclusion
   Evidence should be rich enough to stand alone without the full transcript
```

#### 2. Tool Definition - `evidence.content` field
**Changed from:**
```json
"content": {
  "type": "string",
  "description": "Evidence that adds depth, context, or nuance to the statement.\nFor quotes: Use __text__ to underline critical phrases (e.g., \"__unprecedented growth__ in wealth management\")"
}
```

**Changed to:**
```json
"content": {
  "type": "string",
  "description": "Evidence with FULL CONTEXT - include background commentary that provides context, not just the punchline.\nFor quotes: Extract 3-4 sentences when needed to capture both setup and conclusion.\nUse __text__ to underline critical phrases (e.g., \"__unprecedented growth__ in wealth management\")\nPrioritize completeness over brevity - longer contextual quotes are preferred."
}
```

### Why This Addresses Feedback
- Explicit instruction to include "BOTH punchline AND background commentary"
- Guidance to "start quotes earlier to capture setup"
- Clear directive: "Prefer longer quotes with context over short punchlines"
- Reinforced at both prompt and tool definition levels

### Trade-offs
- **Pro**: Users can trim quotes down but cannot add missing context
- **Con**: Reports will be 20-30% longer
- **Mitigation**: Better to have too much context that can be edited down

---

## Feedback #2: Selective Evidence Usage

### Verbatim Feedback
> "we dont generally require quotes that just defines the performance in the quarter. to the extent quotes are used it is for highlighting key drivers of the result that could be a potential read through for us. most of the other quotes we use are related to either strategic topics or to capture forward guidance/outlook."

### Root Cause
- Current guidance treats all content equally worthy of direct quotation
- No filtering criteria for what types of content deserve quotes vs paraphrasing
- Prompt says "Include ALL relevant evidence" without distinguishing value levels
- Research plan also emphasizes "Include EVERY metric mentioned" without priority

### Changes Made

#### 1. `category_extraction` System Prompt - NEW Section 5: QUOTE SELECTION STRATEGY
**Added new section (after existing section 4):**
```
5. QUOTE SELECTION STRATEGY:
   Use direct quotes STRATEGICALLY, not uniformly:

   PRIORITIZE QUOTES FOR:
   - Key drivers and root causes of performance ("driven by X, Y, Z")
   - Strategic initiatives and forward-looking plans
   - Management outlook, guidance, and expectations
   - Risk factors and challenges discussed
   - Novel insights or unique perspectives
   - Qualitative context that explains "why" behind results

   PARAPHRASE INSTEAD FOR:
   - Basic performance numbers ("Revenue was $X billion")
   - Simple quarter-over-quarter comparisons without explanation
   - Routine definitions or straightforward metrics
   - Standard regulatory or accounting explanations

   RULE OF THUMB: Quote the "why" and "what's next", paraphrase the "what happened"
```

**Note**: Subsequent sections renumbered accordingly (old 5→6, 6→7, etc.)

#### 2. Tool Definition - `evidence` array description
**Changed from:**
```json
"evidence": {
  "type": "array",
  "description": "ALL relevant supporting quotes and context that enrich the statement",
  "minItems": 1,
```

**Changed to:**
```json
"evidence": {
  "type": "array",
  "description": "Strategic supporting evidence that enriches the statement.\nPrioritize quotes for drivers, strategy, outlook, and risks.\nUse paraphrases for basic performance metrics.\nAll evidence should add analytical value beyond just stating results.",
  "minItems": 1,
```

#### 3. `research_plan` System Prompt - Modified Section 1: THEMES & METRICS FOUND
**Added to existing section:**
```
DISTINGUISH between:
- Basic performance metrics (for paraphrasing in extraction)
- Strategic drivers and outlook (for direct quoting in extraction)
```

### Why This Addresses Feedback
- Clear prioritization: Quote drivers/strategy/outlook, paraphrase basic performance
- Explicit "RULE OF THUMB: Quote the 'why' and 'what's next', paraphrase the 'what happened'"
- Research phase now flags which content is quote-worthy vs basic metrics
- Tool definition reinforces "Strategic supporting evidence"

### Trade-offs
- **Pro**: Reduces quote volume while preserving strategic value
- **Con**: Requires LLM judgment (not 100% consistent)
- **Mitigation**: Clear examples in prompt guide the model effectively

---

## Feedback #3: Category Duplication & Misclassification

### Verbatim Feedback
> "the classification of content into categories were off in a few places and were duplicated into multiple categories in a few places (eg. regulatory vs capital)."

### Root Cause
**Multiple contributing factors:**

1. **research_plan prompt** - cross_category_notes guidance says "If no overlap concerns, leave empty"
   - Makes deduplication OPTIONAL when it should be MANDATORY
   - Research plan may not identify all overlaps

2. **category_extraction prompt** - deduplication strategy says "When in doubt, defer to research plan"
   - Creates circular dependency if research plan missed the overlap
   - Deduplication check happens AFTER extraction, not during

3. **Code** - extracted_themes variable passes prior statements but prompt doesn't strongly enforce checking

4. **Category boundary ambiguity** - When category descriptions overlap (e.g., regulatory vs capital), LLM may legitimately extract same content for both

### Changes Made

#### 1. `research_plan` System Prompt - Strengthened cross_category_notes_guidance

**Changed from:**
```
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
```

**Changed to:**
```
<cross_category_notes_guidance>
The cross_category_notes field is MANDATORY for deduplication and must specify:

- Which specific themes/topics belong in THIS category
- Which overlapping themes belong in OTHER categories (be explicit with category names)
- For cross-cutting themes (e.g., regulatory capital), designate PRIMARY category
- Address ALL potential overlaps, even if minor
- NEVER leave empty - at minimum state "No overlaps with other categories"

CRITICAL OVERLAPS TO ADDRESS:
- Regulatory vs Capital topics → Specify which aspects go where
- Performance metrics vs Strategic initiatives → Distinguish outcomes from drivers
- Risk metrics vs Credit quality → Separate risk types clearly
- Expenses vs Operational efficiency → Distinguish cost reporting from efficiency programs

Examples:
- "Capital RATIOS and liquidity metrics here; regulatory CHANGES go to Risk & Regulatory"
- "Digital transformation STRATEGY here; digital channel PERFORMANCE metrics go to Business Segments"
- "Credit PROVISIONS and PCL here; credit RISK framework and stress testing go to Risk Management"
- "Expense AMOUNTS here; efficiency PROGRAMS and cost initiatives go to Strategic Initiatives"
</cross_category_notes_guidance>
```

#### 2. `category_extraction` System Prompt - Strengthened deduplication_strategy

**Changed from:**
```
<deduplication_strategy>
Strict adherence to category boundaries:

1. FOLLOW RESEARCH PLAN GUIDANCE:
   The research_plan field contains specific instructions about what belongs in THIS category
   Pay special attention to the cross_category_notes

2. CHECK PREVIOUS THEMES:
   Review extracted_themes to see what's already been covered
   If a theme appears there, it's been addressed - don't repeat

3. HONOR BOUNDARIES:
   If cross_category_notes says "Leave X for Category Y", respect that
   When in doubt about where content belongs, defer to the research plan

4. SECTION SPECIFICITY:
   Only extract from the specified transcripts_section (MD or QA)
   Don't pull content from other sections
</deduplication_strategy>
```

**Changed to:**
```
<deduplication_strategy>
MANDATORY deduplication - violations will be rejected:

1. BEFORE EXTRACTING - CHECK ALL PREVIOUS THEMES:
   The extracted_themes field contains EVERYTHING already extracted
   Read through ALL statements from prior categories
   If you find similar/related content, you MUST either:
   a) Skip it entirely if already covered, OR
   b) Extract ONLY the novel aspect not yet mentioned

2. FOLLOW CROSS-CATEGORY BOUNDARIES:
   The cross_category_notes specify PRIMARY ownership of cross-cutting themes
   If a theme belongs primarily in another category, skip it here
   Only extract if this category is designated as primary owner

3. VERIFY NO SEMANTIC OVERLAP:
   Even if wording differs, check if the MEANING was already captured
   Example: Don't extract both "NIM expanded" and "Net interest margin grew"
   Example: Don't extract "Capital ratio strong" if "CET1 above targets" already covered

4. WHEN IN DOUBT - SKIP IT:
   If uncertain whether content overlaps, err on side of skipping
   Better to have one strong instance than duplicate weak ones
   Duplication is worse than minor gaps

5. SECTION SPECIFICITY:
   Only extract from the specified transcripts_section (MD or QA)
   Don't pull content from other sections

ZERO TOLERANCE: If you extract content already in extracted_themes, the category will be rejected
```

#### 3. Tool Definition - Added validation note to summary_statements

**Changed from:**
```json
"summary_statements": {
  "type": "array",
  "description": "ALL key findings with rich supporting evidence - be exhaustive",
  "minItems": 1,
```

**Changed to:**
```json
"summary_statements": {
  "type": "array",
  "description": "ALL key findings with rich supporting evidence - be exhaustive.\nCRITICAL: Each statement must be verified against extracted_themes to ensure no duplication.\nStatements overlapping with prior categories will result in rejection.",
  "minItems": 1,
```

#### 4. Code Changes - Enhanced Context (No Filtering/Rejection)

**Decision**: Enhanced the `extracted_themes` context to include quote snippets, but NO automatic filtering or rejection.

**Change Made** (main.py lines 1043-1066):
```python
# Enhanced to include first quote snippet from evidence
statement_text = f"[{result['name']}] {stmt['statement']}"

# Add evidence snippets to help identify overlapping content
if 'evidence' in stmt and stmt['evidence']:
    first_quote = None
    for ev in stmt['evidence']:
        if ev.get('type') == 'quote' and ev.get('content'):
            first_quote = ev['content'][:100]  # First 100 chars
            if len(ev['content']) > 100:
                first_quote += "..."
            break

    if first_quote:
        statement_text += f"\n  → Quote used: \"{first_quote}\""
```

**What This Provides:**

Before:
```
[Credit Quality] PCL normalized to pre-pandemic levels
[Capital Strategy] Strong capital position maintained
```

After:
```
[Credit Quality] PCL normalized to pre-pandemic levels
  → Quote used: "We've seen provisions return to historical norms of around 25-30 basis points..."

[Capital Strategy] Strong capital position maintained
  → Quote used: "Our CET1 ratio of 13.2% gives us significant flexibility for capital deployment..."
```

**Why This Helps:**
- LLM can see which quotes were already used (avoid quote duplication)
- Better semantic understanding of whether topics truly overlap
- Minimal token cost (only first 100 chars of first quote)
- Falls back gracefully if no evidence exists

**Rationale for Context-Only Approach**:
- Provides information without automated filtering
- LLM makes intelligent decisions based on richer context
- No risk of false positives or silent content dropping
- Transparent to business users
- Easy to debug if issues arise

**What We're NOT Doing**:
- ❌ No similarity threshold filtering
- ❌ No automated content rejection
- ❌ No duplication scoring or detection algorithms

**Future Consideration** (only if prompt changes prove insufficient):
- Add passive logging-only duplication detection for monitoring
- No automatic rejection - just visibility for review

### Why This Addresses Feedback

**Prompt-focused approach:**

1. **Research Plan Phase** - Proactively prevents overlaps
   - Makes cross_category_notes MANDATORY (not optional)
   - Provides explicit examples of common overlaps (regulatory vs capital)
   - Requires designation of PRIMARY category for cross-cutting themes

2. **Extraction Phase** - Enforces strict checking
   - "ZERO TOLERANCE" language makes expectations clear
   - Requires checking extracted_themes BEFORE extracting
   - "When in doubt, skip it" - err on side of caution
   - Code passes complete context: all prior categories and statements

3. **Existing Infrastructure** - Already provides visibility
   - Code passes `extracted_themes` with ALL prior statements
   - Format: `"[Category Name] Statement text"`
   - LLM has complete visibility to check for overlaps

### Trade-offs

**Prompt-Only Approach:**
- **Pro**: Transparent to business users, easy to debug, no risk of false positives
- **Pro**: Addresses root cause (unclear boundaries) rather than symptoms
- **Pro**: LLM can understand semantic overlap better than string similarity
- **Con**: May still have occasional duplicates if LLM misses overlap
- **Mitigation**: Monitor outputs, can add passive logging later if needed

**Why Not Code-Based Filtering:**
- Risk of false positives (similar text, different meaning)
- Unpredictable behavior (content disappearing)
- Hard to tune thresholds across different content types
- Business users prefer transparency over automation

---

## Implementation Strategy

### Single Phase: Prompt Changes Only (Low Risk)
1. ✅ Feedback 1: Longer quotes with context
2. ✅ Feedback 2: Selective quote strategy
3. ✅ Feedback 3: Mandatory deduplication guidance

**Rationale**:
- All three feedback items addressed through prompt improvements
- No code deployment required - prompt changes only
- Transparent and debuggable
- Can be tested immediately

### Validation Plan
1. Test on 3-5 sample transcripts (different banks, quarters)
2. Manually review outputs for:
   - **Quote length**: Verify quotes include 3-4 sentences with background context
   - **Quote selectivity**: Confirm quotes focus on drivers/strategy/outlook, not basic metrics
   - **Duplication instances**: Check for duplicate content across categories
3. If duplicates are found:
   - Review which categories overlapped
   - Adjust cross_category_notes examples in research_plan
   - Consider if specific category descriptions need clarification
4. Document improvements quantitatively

### Future Enhancement (Only If Needed)
If validation reveals persistent duplication issues:
- **Option A**: Add logging-only duplication detection (no rejection)
- **Option B**: Refine category boundary descriptions
- **Option C**: Add manual review workflow for flagged duplicates

Start with prompts, measure results, iterate only if necessary.

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 2.1.0 → 2.2.0 | 2025-11-05 | Applied all feedback changes (Feedback 1, 2, 3 prompt updates - minimal code changes) |
| 2.2.0 → 2.2.1 | 2025-11-05 | Applied fixes from comprehensive analysis (evidence optional fix, validation constraints, enhanced deduplication) - See FIXES_APPLIED_v2.2.1.md |

---

## Contact

For questions about these changes:
- **Business Feedback**: [Business team contact]
- **Technical Implementation**: Alex Day
- **Prompt Engineering**: Alex Day
