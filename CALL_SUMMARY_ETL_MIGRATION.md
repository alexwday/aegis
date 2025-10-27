# Call Summary ETL Database Migration - Implementation Guide

## ✅ What Was Completed

### 1. Created Database-Ready Prompt Records
**File**: `scripts/call_summary_prompts_for_db.json`

Contains two prompts properly formatted for the database:
- **research_plan**: Generates research plan for category analysis
- **category_extraction**: Extracts category content with evidence

**Database Schema Mapping**:
```
model → "call_summary_etl"
layer → "call_summary"
name → "research_plan" | "category_extraction"
description → One-sentence purpose
comments → Full metadata (version, framework, purpose)
system_prompt → The prompt template with variables
user_prompt → NULL (ETL builds programmatically)
tool_definition → Function definition as JSONB
uses_global → [] (no globals for ETL)
version → "2.1.0"
```

### 2. Updated ETL Code
**File**: `src/aegis/etls/call_summary/main.py`

**Changes Made**:
1. ✅ Added import: `from aegis.utils.prompt_loader import load_prompt_from_db`
2. ✅ Removed unused import: `import yaml`
3. ✅ Updated `load_research_plan_config()` to load from database
4. ✅ Updated `load_category_extraction_config()` to load from database
5. ✅ Updated both function calls to pass `execution_id` parameter

**New Pattern**:
```python
def load_research_plan_config(execution_id):
    """Load the research plan prompt and tool definition from database."""
    prompt_data = load_prompt_from_db(
        layer="call_summary",
        name="research_plan",
        compose_with_globals=False,  # ETL doesn't use global contexts
        available_databases=None,
        execution_id=execution_id
    )
    return {
        'system_template': prompt_data['system_prompt'],
        'tool': prompt_data['tool_definition']
    }
```

## 🎯 What You Need To Do

### Step 1: Insert Prompts into Database

Use the Prompt Editor at http://localhost:5001 to create both prompts:

**For research_plan:**
1. Click "Create New Prompt"
2. Fill in fields from `scripts/call_summary_prompts_for_db.json`:
   - **Model**: call_summary_etl
   - **Layer**: call_summary
   - **Name**: research_plan
   - **Version**: 2.1.0
   - **Description**: Generates comprehensive research plan for earnings call category analysis
   - **Comments**: Version: 2.1 | Framework: CO-STAR+XML | Purpose: Generate comprehensive research plan for earnings call analysis | Token Target: ~300 tokens | Last Updated: 2024-09-26
   - **System Prompt**: (copy from JSON file)
   - **Tool Definition**: (copy the tool_definition object from JSON)
   - **Uses Global**: (leave empty)
3. Click "Save Changes"

**For category_extraction:**
1. Click "Create New Prompt" again
2. Fill in fields from `scripts/call_summary_prompts_for_db.json`:
   - **Model**: call_summary_etl
   - **Layer**: call_summary
   - **Name**: category_extraction
   - **Version**: 2.1.0
   - **Description**: Extracts comprehensive category content with supporting evidence and quotes
   - **Comments**: Version: 2.1 | Framework: CO-STAR+XML | Purpose: Extract comprehensive category content from earnings calls | Token Target: ~400 tokens | Last Updated: 2024-09-26
   - **System Prompt**: (copy from JSON file)
   - **Tool Definition**: (copy the tool_definition object from JSON)
   - **Uses Global**: (leave empty)
3. Click "Save Changes"

### Step 2: Test the Migration

Run the call_summary ETL from your work computer:

```bash
LOG_LEVEL=INFO python -m aegis.etls.call_summary.main \
  --bank "Royal Bank of Canada" \
  --year 2024 \
  --quarter Q3
```

**Expected Behavior**:
- Script should load prompts from database successfully
- Should generate call summary report as before
- Check logs for: "Loaded prompt from database: call_summary.research_plan"

**If You See Errors**:
- "Prompt not found": Prompts not inserted correctly in database
- "Missing key 'system_prompt'": Database structure issue
- Check logs for detailed error messages

### Step 3: Verify Generated Report

Check that the output DOCX file:
- Has correct structure and formatting
- Categories are properly analyzed
- Evidence and quotes are included
- No missing or corrupted content

## 📊 Migration Benefits

**Before** (YAML-based):
- ❌ Prompts scattered across filesystem
- ❌ No version tracking
- ❌ Manual editing required
- ❌ No central management

**After** (Database-backed):
- ✅ All prompts in database
- ✅ Version control built-in
- ✅ Web UI for editing
- ✅ AI assistant for improvements
- ✅ Easy rollback with versions

## 🔄 Next Steps After Success

Once call_summary works:

1. **Migrate key_themes ETL**:
   - Follow same pattern
   - Create prompts in database
   - Update code to use `load_prompt_from_db()`

2. **Migrate cm_readthrough ETL**:
   - Follow same pattern
   - Create prompts in database
   - Update code to use `load_prompt_from_db()`

## 📝 Notes

- **YAML files preserved**: Old YAML files kept for reference
- **Backward compatible**: Can revert by uncommenting old code
- **Global contexts**: ETL doesn't use them (unlike conversational agents)
- **User prompt**: ETLs build programmatically, so `user_prompt` field is NULL

## 🆘 Troubleshooting

### "Prompt not found" Error
**Solution**: Verify prompts exist in database with correct layer/name:
```sql
SELECT layer, name, version FROM prompts
WHERE model = 'call_summary_etl'
ORDER BY name;
```

### "Missing key" Errors
**Solution**: Check prompt_loader is returning correct structure:
- Should have 'system_prompt' key
- Should have 'tool_definition' key
- tool_definition should be a dict, not string

### Incorrect Output
**Solution**: Compare database prompt with original YAML:
- System prompt should match exactly
- Tool definition structure should be identical
- Variable placeholders like {bank_name} should be intact
