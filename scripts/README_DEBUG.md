# Key Themes ETL Debug Scripts

This directory contains debugging scripts for troubleshooting the key_themes ETL pipeline, specifically for the `theme_grouping` stage.

## Scripts Overview

### 1. `debug_prompt_database.py`
**Purpose**: Verify prompt database contents and structure

**What it checks**:
- Database connection
- All key_themes_etl prompts and their versions
- Prompt structure (system_prompt, tool_definition, etc.)
- Detailed view of theme_grouping prompt

**Usage**:
```bash
source venv/bin/activate
python scripts/debug_prompt_database.py
```

**When to use**:
- Prompt not found errors
- Verifying prompt versions after uploads
- Checking if prompts match between environments

### 2. `debug_key_themes_grouping.py`
**Purpose**: End-to-end test of theme_grouping with live data

**What it checks**:
- Prompt loading from database
- Data retrieval for specified bank/period
- Theme extraction and formatting
- LLM request/response for theme_grouping
- Response parsing and structure validation

**Usage**:
```bash
source venv/bin/activate
python scripts/debug_key_themes_grouping.py --bank "Royal Bank of Canada" --year 2025 --quarter Q2
```

**When to use**:
- theme_grouping stage failures
- Investigating why LLM isn't returning expected structure
- Testing with real data in different environments

**Output includes**:
- Prompt version and preview
- Tool definition structure
- Sample data statistics
- Full LLM request/response details
- Parsed theme groups

### 3. `debug_llm_response_parsing.py`
**Purpose**: Isolated testing of response parsing logic

**What it checks**:
- JSON parsing of LLM responses
- theme_groups structure validation
- Required field presence (group_title, qa_ids, etc.)

**Usage**:
```bash
# Test with sample data
source venv/bin/activate
python scripts/debug_llm_response_parsing.py

# Test with saved response
python scripts/debug_llm_response_parsing.py --test-response /path/to/response.json
```

**When to use**:
- JSON parsing errors
- Investigating malformed LLM responses
- Testing parsing logic changes

## Common Issues and Solutions

### Issue: "theme_grouping prompt not found"
**Debug script**: `debug_prompt_database.py`
**Likely causes**:
- Prompt not uploaded to database
- Wrong database connection
- Prompt name mismatch

**Solution**: Check output to see available prompts, verify naming

### Issue: "No 'theme_groups' in response"
**Debug script**: `debug_key_themes_grouping.py`
**Likely causes**:
- Tool definition incorrect
- LLM not calling function
- Response structure mismatch

**Solution**: Check Step 4 output for actual LLM response structure

### Issue: "JSON parsing failed"
**Debug script**: `debug_llm_response_parsing.py`
**Likely causes**:
- Malformed JSON from LLM
- Unexpected response format
- Character encoding issues

**Solution**: Examine raw arguments output, look for syntax errors

## Debug Workflow

When experiencing theme_grouping failures:

1. **First**: Run `debug_prompt_database.py`
   - Verify prompt exists and has correct structure
   - Check tool_definition is valid JSON

2. **Second**: Run `debug_key_themes_grouping.py` with same parameters
   - Watch for which step fails
   - Compare local vs work computer outputs

3. **If parsing fails**: Run `debug_llm_response_parsing.py`
   - Save actual response to JSON file
   - Test parsing in isolation

## Environment-Specific Testing

To compare environments:

```bash
# On local machine
python scripts/debug_prompt_database.py > /tmp/prompts_local.txt

# Transfer to work computer, then run:
python scripts/debug_prompt_database.py > /tmp/prompts_work.txt

# Compare
diff /tmp/prompts_local.txt /tmp/prompts_work.txt
```

## Debug Logging

For maximum verbosity, set environment variable before running:

```bash
export LOG_LEVEL=DEBUG
python scripts/debug_key_themes_grouping.py --bank "Royal Bank of Canada" --year 2025 --quarter Q2
```

This enables all debug-level logs in the main ETL code, including:
- Tool definition structure
- Raw LLM responses
- Argument parsing steps
- Error details with context
