# Key Themes ETL Database Migration - Implementation Guide

## ✅ What Was Completed

### 1. Created Database-Ready Prompt Records
**File**: `scripts/key_themes_prompts_for_db.json`

Contains three prompts properly formatted for the database:
- **theme_extraction**: Validates and extracts theme titles from Q&A exchanges
- **theme_grouping**: Creates optimal thematic groupings for executive reporting
- **html_formatting**: Transforms Q&A content into HTML-formatted documents

**Database Schema Mapping**:
```
model → "aegis"
layer → "key_themes_etl"
name → "theme_extraction" | "theme_grouping" | "html_formatting"
description → One-sentence purpose
comments → Full metadata (version, framework, purpose)
system_prompt → The prompt template with variables
user_prompt → NULL (ETL builds programmatically)
tool_definition → Function definition as JSONB (or NULL for html_formatting)
uses_global → [] (no globals for ETL)
version → "3.1" | "4.0" | "5.0"
```

### 2. Updated ETL Code
**File**: `src/aegis/etls/key_themes/main.py`

**Changes Made**:
1. ✅ Removed import: `import yaml`
2. ✅ Added import: `from aegis.utils.prompt_loader import load_prompt_from_db`
3. ✅ Updated `extract_theme_and_summary()` to load from database
4. ✅ Updated `format_qa_html()` to load from database
5. ✅ Updated `determine_comprehensive_grouping()` to load from database

**New Pattern**:
```python
# Extract execution_id from context
execution_id = context.get('execution_id')

# Load prompt from database
prompt_data = load_prompt_from_db(
    layer="key_themes_etl",
    name="theme_extraction",  # or "theme_grouping" or "html_formatting"
    compose_with_globals=False,  # ETL doesn't use global contexts
    available_databases=None,
    execution_id=execution_id
)

# Use prompt data
system_prompt = prompt_data['system_prompt'].format(...)
tool = prompt_data['tool_definition']  # If prompt has tool
```

## 🎯 What You Need To Do

### Step 1: Insert Prompts into Database

Use the Prompt Editor at http://localhost:5001 or the copy-paste guide:
**File**: `scripts/KEY_THEMES_PROMPTS_COPY_PASTE.md`

**For theme_extraction:**
1. Click "Create New Prompt"
2. Fill in fields from copy-paste guide:
   - **Model**: aegis
   - **Layer**: key_themes_etl
   - **Name**: theme_extraction
   - **Version**: 3.1
   - **Description**: Extracts structured theme titles and summaries from earnings call Q&A exchanges
   - **Comments**: Version: 3.1 | Framework: CO-STAR+XML | Purpose: Extract theme title and summary from earnings Q&A using structured methodology | Token Target: 32768 tokens | Last Updated: 2024-09-26
   - **System Prompt**: (copy from guide)
   - **Tool Definition**: (copy from guide)
   - **Uses Global**: (leave empty)
3. Click "Save Changes"

**For theme_grouping:**
1. Click "Create New Prompt" again
2. Fill in fields from copy-paste guide:
   - **Model**: aegis
   - **Layer**: key_themes_etl
   - **Name**: theme_grouping
   - **Version**: 4.0
   - **Description**: Creates optimal thematic groupings for executive earnings call analysis
   - **Comments**: Version: 4.0 | Framework: CO-STAR+XML | Purpose: Group Q&As into unified themes using banking domain expertise and intelligent reasoning | Token Target: 32768 tokens | Last Updated: 2024-09-26
   - **System Prompt**: (copy from guide)
   - **Tool Definition**: (copy from guide)
   - **Uses Global**: (leave empty)
3. Click "Save Changes"

**For html_formatting:**
1. Click "Create New Prompt" again
2. Fill in fields from copy-paste guide:
   - **Model**: aegis
   - **Layer**: key_themes_etl
   - **Name**: html_formatting
   - **Version**: 5.0
   - **Description**: Transforms Q&A exchanges into executive-ready HTML-formatted documents with strategic emphasis
   - **Comments**: Version: 5.0 | Framework: CO-STAR+XML | Purpose: Format Q&A exchanges for executive document inclusion using HTML tags for emphasis and inline speaker formatting | Token Target: 32768 tokens | Last Updated: 2024-09-26
   - **System Prompt**: (copy from YAML file or JSON - it's very long)
   - **Tool Definition**: Leave empty (html_formatting has no tool)
   - **Uses Global**: (leave empty)
3. Click "Save Changes"

### Step 2: Test the Migration

Run the key_themes ETL from your work computer:

```bash
LOG_LEVEL=INFO python -m aegis.etls.key_themes.main \
  --bank "Royal Bank of Canada" \
  --year 2024 \
  --quarter Q3
```

**Expected Behavior**:
- Script should load prompts from database successfully
- Should generate key themes report as before
- Check logs for: "Loaded prompt from database: key_themes_etl.theme_extraction"

**If You See Errors**:
- "Prompt not found": Prompts not inserted correctly in database
- "Missing key 'system_prompt'": Database structure issue
- Check logs for detailed error messages

### Step 3: Verify Generated Report

Check that the output DOCX file:
- Has correct structure and formatting
- Theme groups are properly created
- Q&A exchanges are formatted with HTML emphasis
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

Once key_themes works:

1. **Migrate cm_readthrough ETL**:
   - Follow same pattern
   - Create prompts in database
   - Update code to use `load_prompt_from_db()`

2. **Clean up old files** (optional):
   - Keep YAML files for reference
   - Or remove them if migration is stable

## 📝 Notes

- **YAML files preserved**: Old YAML files kept for reference
- **Backward compatible**: Can revert by uncommenting old code
- **Global contexts**: ETL doesn't use them (unlike conversational agents)
- **User prompt**: ETLs build programmatically, so `user_prompt` field is NULL
- **Tool definition**: html_formatting has no tool (uses standard completion)

## 🆘 Troubleshooting

### "Prompt not found" Error
**Solution**: Verify prompts exist in database with correct layer/name:
```sql
SELECT layer, name, version FROM prompts
WHERE model = 'aegis' AND layer = 'key_themes_etl'
ORDER BY name;
```

### "Missing key" Errors
**Solution**: Check prompt_loader is returning correct structure:
- Should have 'system_prompt' key
- Should have 'tool_definition' key (or None for html_formatting)
- tool_definition should be a dict, not string

### Incorrect Output
**Solution**: Compare database prompt with original YAML:
- System prompt should match exactly
- Tool definition structure should be identical
- Variable placeholders like {bank_name} should be intact

## 📋 Files Changed

**Modified**:
- `src/aegis/etls/key_themes/main.py` - Updated to use database prompts

**Created**:
- `scripts/key_themes_prompts_for_db.json` - Database-ready prompts
- `scripts/KEY_THEMES_PROMPTS_COPY_PASTE.md` - Copy-paste guide
- `KEY_THEMES_ETL_MIGRATION.md` - This migration guide

**Preserved**:
- `src/aegis/etls/key_themes/prompts/*.yaml` - Original YAML files (reference)
