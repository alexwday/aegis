# Prompt Changes: v2.2.0 → v2.2.1

Quick reference for what changed between versions.

---

## Research Plan Prompt Changes

### Tool Definition Changes

**Added validation constraints:**
```json
"name": {
  "maxLength": 200  // NEW
}

"extraction_strategy": {
  "maxLength": 3000  // NEW
}

"cross_category_notes": {
  "maxLength": 1000  // NEW
}
```

**System Prompt**: No changes

---

## Category Extraction Prompt Changes

### System Prompt Changes

#### Change 1: Clarified Section 4 (EVIDENCE SELECTION)

**OLD:**
```
CRITICAL: Prefer longer quotes with context over short punchlines
Better to include 3-4 sentences with background than just the conclusion
```

**NEW:**
```
CRITICAL: When you DO use direct quotes (per Section 5 priorities), prefer longer quotes
with context over short punchlines. This doesn't mean quote everything - it means make the
quotes you do use comprehensive and contextual (3-4 sentences with background).
```

**Why**: Removes confusion between "longer quotes" and "selective quotes"

---

#### Change 2: Enhanced Section 3 (SEMANTIC OVERLAP)

**ADDED:**
```
Common semantic duplicates to avoid:
- "NIM expanded 15bps" ≈ "Net interest margin grew 15 basis points" → DUPLICATE
- "CET1 ratio of 13.2%" ≈ "Strong capital position above 13%" → DUPLICATE
- "Revenue growth drivers" ≈ "Factors contributing to revenue increase" → DUPLICATE
- "PCL normalized" ≈ "Provisions returned to historical levels" → DUPLICATE
- "Expense discipline" ≈ "Cost management initiatives" → DUPLICATE

Check MEANING not just WORDING. Different phrasing of same concept = duplicate
```

**Why**: Provides concrete examples for LLM to understand semantic similarity

---

### Tool Definition Changes

#### Change 1: Evidence Now Optional (CRITICAL FIX)

**OLD:**
```json
"evidence": {
  "type": "array",
  "minItems": 1,  // REQUIRED at least one
  "description": "ALL relevant supporting quotes..."
}

"required": ["statement", "evidence"]  // Evidence was REQUIRED
```

**NEW:**
```json
"evidence": {
  "type": "array",
  "maxItems": 5,  // NEW: Limit to 5 items
  // REMOVED: minItems: 1
  "description": "Strategic supporting evidence when appropriate - use per Section 5 guidance.\nFor strategic content (drivers, outlook, risks): Provide rich contextual quotes.\nFor basic metrics: Evidence may be omitted if paraphrased in statement."
}

"required": ["statement"]  // Evidence NOT required
```

**Why**: Fixes conflict with Feedback #2 - allows paraphrasing basic metrics without quotes

---

#### Change 2: Added Validation Constraints

```json
"rejection_reason": {
  "maxLength": 500  // NEW
}

"title": {
  "maxLength": 100  // NEW
}

"summary_statements": {
  "maxItems": 20  // NEW
}

"statement": {
  "maxLength": 500  // NEW
}

"content": {
  "maxLength": 2000  // NEW
}

"speaker": {
  "maxLength": 200  // NEW
}
```

**Why**: Prevents runaway LLM responses and token issues

---

## Code Changes (not in prompts but relevant)

### main.py Enhancements

1. **cross_category_notes validation** (lines 1080-1089)
   - Warns if missing or < 20 characters

2. **Multiple quote snippets** (lines 1051-1064)
   - Shows up to 3 quotes instead of just first
   - Format: `Q1: "..." | Q2: "..." | Q3: "..."`

3. **Passive duplicate detection** (lines 1154-1186)
   - SequenceMatcher with 70% similarity threshold
   - Logs potential duplicates without rejecting
   - Provides visibility for review

---

## Summary of Impact

| Change | Impact | Fixes |
|--------|--------|-------|
| Evidence optional | **CRITICAL** - LLM can now paraphrase without quotes | Feedback #2 |
| Clarified Section 4 | Removes confusion between longer vs selective | Feedback #1 clarity |
| Semantic examples | Better duplicate detection | Feedback #3 |
| Validation constraints | Prevents token issues | System robustness |
| Multiple quote snippets | Better deduplication context | Feedback #3 |
| Passive logging | Visibility without disruption | Monitoring |

---

## How to Use These Files

1. **Open in VS Code**: `PROMPT_FOR_DB_research_plan_v2.2.1.md` and `PROMPT_FOR_DB_category_extraction_v2.2.1.md`

2. **Copy System Prompt**:
   - Find the "System Prompt" section
   - Copy everything between the triple backticks
   - Paste into your prompt editor's system prompt field

3. **Copy Tool Definition**:
   - Find the "Tool Definition" section
   - Copy the entire JSON object (including outer braces)
   - Paste into your prompt editor's tool definition field

4. **Version in DB**: Set version to `2.2.1`
