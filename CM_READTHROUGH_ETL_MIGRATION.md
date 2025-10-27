# CM Readthrough ETL Database Migration - Implementation Guide

## ✅ What Was Completed

### 1. Created Database-Ready Prompt Records
**File**: `scripts/cm_readthrough_prompts_for_db.json`

Contains four prompts properly formatted for the database:
- **outlook_extraction**: Extracts capital markets outlook statements from MD section of earnings calls
- **qa_extraction_dynamic**: Extracts analyst questions from Q&A sections by category
- **subtitle_generation**: Creates concise subtitles that capture overall themes from multiple banks
- **batch_formatting**: Formats capital markets outlook statements with HTML emphasis tags

**Database Schema Mapping**:
```
model → "aegis"
layer → "cm_readthrough_etl"
name → "outlook_extraction" | "qa_extraction_dynamic" | "subtitle_generation" | "batch_formatting"
description → One-sentence purpose
comments → Full metadata (version, purpose)
system_prompt → The system prompt template with variables
user_prompt → The user prompt template with variables (DIFFERENT from other ETLs!)
tool_definition → Function definition as JSONB (or NULL for subtitle_generation)
uses_global → [] (no globals for ETL)
version → "1.0"
```

**IMPORTANT DIFFERENCE**: Unlike call_summary and key_themes ETLs, cm_readthrough prompts have **BOTH system_prompt AND user_prompt** populated. This is intentional and matches the original YAML structure.

### 2. Updated ETL Code
**File**: `src/aegis/etls/cm_readthrough/main.py`

**Changes Made**:
1. ✅ Removed import: `import yaml`
2. ✅ Added import: `from aegis.utils.prompt_loader import load_prompt_from_db`
3. ✅ Replaced `load_prompt_template()` function to load from database
4. ✅ Updated all 4 function calls to pass execution_id parameter

**New Pattern**:
```python
def load_prompt_template(prompt_file: str, execution_id: str = None) -> Dict[str, Any]:
    """
    Load prompt template from database.

    Args:
        prompt_file: Original YAML filename (e.g., "outlook_extraction.yaml")
        execution_id: Execution ID for tracking

    Returns:
        Dict with system_template, user_template, tool_name, tool_description, tool_parameters
    """
    # Convert filename to prompt name (remove .yaml extension)
    prompt_name = prompt_file.replace(".yaml", "")

    # Load from database
    prompt_data = load_prompt_from_db(
        layer="cm_readthrough_etl",
        name=prompt_name,
        compose_with_globals=False,
        available_databases=None,
        execution_id=execution_id
    )

    # Convert database format to cm_readthrough's expected format
    result = {
        'system_template': prompt_data['system_prompt'],
        'user_template': prompt_data.get('user_prompt', '')
    }

    # Extract tool definition components if present
    if prompt_data.get('tool_definition'):
        tool_def = prompt_data['tool_definition']
        result['tool_name'] = tool_def['function']['name']
        result['tool_description'] = tool_def['function']['description']
        result['tool_parameters'] = tool_def['function']['parameters']['properties']

    return result
```

**Updated Function Calls**:
1. Line 432-433: `extract_outlook_from_transcript()` - outlook_extraction
2. Line 548-549: `extract_qa_from_section()` - qa_extraction_dynamic
3. Line 715-716: `generate_subtitle()` - subtitle_generation
4. Line 825-826: `format_outlook_with_batch_call()` - batch_formatting

## 🎯 What You Need To Do

### Step 1: Insert Prompts into Database

Use the Prompt Editor at http://localhost:5001 or the copy-paste guide:
**File**: `scripts/CM_READTHROUGH_PROMPTS_COPY_PASTE.md`

**IMPORTANT**: These prompts have BOTH System Prompt AND User Prompt (unlike previous ETLs)

**For outlook_extraction:**
1. Click "Create New Prompt"
2. Fill in fields from copy-paste guide:
   - **Model**: aegis
   - **Layer**: cm_readthrough_etl
   - **Name**: outlook_extraction
   - **Version**: 1.0
   - **Description**: Extracts capital markets outlook statements from earnings call transcripts by category
   - **Comments**: Purpose: Extract outlook statements from MD section of earnings calls for capital markets readthrough reports | Last Updated: 2024
   - **System Prompt**: (copy from guide)
   - **User Prompt**: (copy from guide - this is populated!)
   - **Tool Definition**: (copy from guide)
   - **Uses Global**: (leave empty)
3. Click "Save Changes"

**For qa_extraction_dynamic:**
1. Click "Create New Prompt" again
2. Fill in fields from copy-paste guide:
   - **Model**: aegis
   - **Layer**: cm_readthrough_etl
   - **Name**: qa_extraction_dynamic
   - **Version**: 1.0
   - **Description**: Extracts analyst questions from Q&A sections by category for capital markets analysis
   - **Comments**: Purpose: Extract verbatim analyst questions from Q&A section of earnings calls for capital markets readthrough reports | Last Updated: 2024
   - **System Prompt**: (copy from guide)
   - **User Prompt**: (copy from guide - this is populated!)
   - **Tool Definition**: (copy from guide)
   - **Uses Global**: (leave empty)
3. Click "Save Changes"

**For subtitle_generation:**
1. Click "Create New Prompt" again
2. Fill in fields from copy-paste guide:
   - **Model**: aegis
   - **Layer**: cm_readthrough_etl
   - **Name**: subtitle_generation
   - **Version**: 1.0
   - **Description**: Creates concise subtitles that capture overall themes from multiple banks
   - **Comments**: Purpose: Generate 8-15 word subtitles that synthesize themes across banks for section headers | Last Updated: 2024
   - **System Prompt**: (copy from guide)
   - **User Prompt**: (copy from guide - this is populated!)
   - **Tool Definition**: (copy from guide)
   - **Uses Global**: (leave empty)
3. Click "Save Changes"

**For batch_formatting:**
1. Click "Create New Prompt" again
2. Fill in fields from copy-paste guide:
   - **Model**: aegis
   - **Layer**: cm_readthrough_etl
   - **Name**: batch_formatting
   - **Version**: 1.0
   - **Description**: Formats capital markets outlook statements with HTML emphasis tags for key phrases
   - **Comments**: Purpose: Add HTML <strong><u> tags to emphasize important phrases in outlook statements | Last Updated: 2024
   - **System Prompt**: (copy from guide)
   - **User Prompt**: (copy from guide - this is populated!)
   - **Tool Definition**: (copy from guide)
   - **Uses Global**: (leave empty)
3. Click "Save Changes"

### Step 2: Test the Migration

Run the cm_readthrough ETL from your work computer:

```bash
LOG_LEVEL=INFO python -m aegis.etls.cm_readthrough.main \
  --year 2025 \
  --quarter Q2 \
  --use-latest \
  --no-pdf
```

**Expected Behavior**:
- Script should load prompts from database successfully
- Should generate capital markets readthrough report as before
- Check logs for: "Loaded prompt from database: cm_readthrough_etl.outlook_extraction"

**If You See Errors**:
- "Prompt not found": Prompts not inserted correctly in database
- "Missing key 'system_prompt'": Database structure issue
- "Missing key 'user_prompt'": Remember cm_readthrough has both system and user prompts!
- Check logs for detailed error messages

### Step 3: Verify Generated Report

Check that the output DOCX file:
- Has correct structure and formatting
- Outlook statements are properly extracted and formatted
- Analyst questions are properly extracted
- Subtitles are generated correctly
- HTML emphasis tags are applied consistently
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

## 🔄 All ETLs Now Migrated

With cm_readthrough complete, all three ETLs are now using database-backed prompts:

1. ✅ **call_summary** - Migrated and tested successfully
2. ✅ **key_themes** - Migrated and tested successfully
3. ✅ **cm_readthrough** - Migrated, ready for testing

## 📝 Notes

- **YAML files preserved**: Old YAML files kept for reference
- **Backward compatible**: Can revert by uncommenting old code
- **Global contexts**: ETL doesn't use them (unlike conversational agents)
- **User prompt**: cm_readthrough uses BOTH system_prompt and user_prompt (different from other ETLs)
- **Tool definition**: All 4 prompts have tools except subtitle_generation (uses standard completion)

## 🆘 Troubleshooting

### "Prompt not found" Error
**Solution**: Verify prompts exist in database with correct layer/name:
```sql
SELECT layer, name, version FROM prompts
WHERE model = 'aegis' AND layer = 'cm_readthrough_etl'
ORDER BY name;
```

### "Missing key" Errors
**Solution**: Check prompt_loader is returning correct structure:
- Should have 'system_prompt' key
- Should have 'user_prompt' key (cm_readthrough needs both!)
- Should have 'tool_definition' key (or None for subtitle_generation)
- tool_definition should be a dict, not string

### Incorrect Output
**Solution**: Compare database prompt with original YAML:
- System prompt should match exactly
- User prompt should match exactly
- Tool definition structure should be identical
- Variable placeholders like {bank_name} should be intact

### "Expected Dict, Got String" Error
**Solution**: If load_prompt_template returns strings instead of dicts:
- Check that the conversion logic correctly maps database format to cm_readthrough format
- Verify tool_definition is parsed as JSON, not left as string

## 📋 Files Changed

**Modified**:
- `src/aegis/etls/cm_readthrough/main.py` - Updated to use database prompts

**Created**:
- `scripts/cm_readthrough_prompts_for_db.json` - Database-ready prompts
- `scripts/CM_READTHROUGH_PROMPTS_COPY_PASTE.md` - Copy-paste guide
- `CM_READTHROUGH_ETL_MIGRATION.md` - This migration guide

**Preserved**:
- `src/aegis/etls/cm_readthrough/prompts/*.yaml` - Original YAML files (reference)
