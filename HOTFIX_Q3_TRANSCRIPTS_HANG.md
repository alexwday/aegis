# Hotfix: Q3 Transcripts Query Hang

## Fix 1: Handle empty databases after rejection

**File:** `src/aegis/model/main.py`

When all selected databases are rejected by the planner validation, the system returns `status="success"` with an empty `databases=[]` list. This causes the UI to hang because no response is yielded.

**Line 627 - Change from:**
```python
                if databases:
```

**To:**
```python
                if not databases:
                    # All selected databases were rejected - nothing to query
                    yield {
                        "type": "agent",
                        "name": "aegis",
                        "content": (
                            "\n⚠️ The requested data is not available for the selected time period. "
                            "Please try a different quarter or check data availability.\n"
                        ),
                    }
                elif databases:
```

---

## Fix 2: Clarify validation wording in clarifier

**File:** `src/aegis/model/agents/clarifier.py`

The phrase "ANY database" is ambiguous when filters are applied, causing the LLM to assume periods exist based on general knowledge rather than checking the filtered availability table.

**Lines 593-594 - Change from:**
```python
                availability_text += (
                    "VALIDATION RULE: If a period exists in ANY database, it is AVAILABLE.\n\n"
                )
```

**To:**
```python
                availability_text += (
                    "VALIDATION RULE: ONLY periods shown in the table below are available. "
                    "If a requested period is NOT in this table, use period_clarification.\n\n"
                )
```

---

## Fix 3: Update clarifier_periods prompt validation rules

**Location:** `prompts` table in PostgreSQL (layer=`aegis`, name=`clarifier_periods`)

Same issue as Fix 2 - the validation rules use ambiguous "ANY database" wording.

**In the `system_prompt` column, lines 152-158 - Change from:**
```yaml
  <validation_rules>
  1. Check if the requested period exists in period_availability for ANY database
  2. If the period IS available in at least one database, return it with periods_all or periods_specific
  3. Only use period_clarification if the period is NOT available in ANY database
  4. Example: If Q3 2025 exists in transcripts but not benchmarking, it's still AVAILABLE
  5. Example: If Q4 2025 doesn't exist in ANY database, then clarify it's not available yet
  </validation_rules>
```

**To:**
```yaml
  <validation_rules>
  1. ONLY periods shown in the period_availability table below are available
  2. If the requested period IS in the table, return it with periods_all or periods_specific
  3. If the requested period is NOT in the table, use period_clarification to inform the user
  4. Do NOT assume periods exist - you must verify against the table
  5. Example: If user asks for Q3 but Q3 is not in the table, clarify it's not available
  </validation_rules>
```
