# Key Themes ETL - Theme Grouping Debug Guide

## Error You're Experiencing

```
Error generating key themes report: '\n "theme_groups"'
```

This error occurs during the theme grouping step and suggests a `KeyError` when trying to access the `theme_groups` key from the LLM response.

## What I've Added for Debugging

### 1. Enhanced Error Validation (Lines 814-850)

Added validation checks that will retry the LLM call if:
- The parsed result is not a dictionary
- The result doesn't contain the `theme_groups` key

### 2. Comprehensive Debug Logging

When you run with `LOG_LEVEL=DEBUG`, you'll see these log entries:

#### Before LLM Call:
- `regrouping.tool_definition` - Shows the tool definition being sent to the LLM
  - `tool_def_type`: Should be "dict"
  - `tool_def_keys`: Should include "type", "function", etc.
  - `tool_def_preview`: First 500 chars of the tool definition

#### After LLM Call:
- `regrouping.llm_response` - Shows the raw response structure
  - `response_type`: Should be "dict"
  - `response_keys`: Should include "choices", "usage", etc.
  - `has_choices`: Should be True

- `regrouping.tool_calls_structure` - Shows tool calls in response
  - `has_tool_calls`: Should be True
  - `num_tool_calls`: Should be 1
  - `tool_call_preview`: First 300 chars

- `regrouping.function_called` - Shows which function was called
  - `function_name`: Should be "group_themes" (check your prompt!)

- `regrouping.raw_arguments` - Shows the raw arguments string
  - `arg_type`: Type of arguments ("str" or "dict")
  - `arg_length`: Length of arguments string
  - `arg_preview`: First 200 chars of arguments

#### Validation Results:
- `regrouping.invalid_result_type` - If result is not a dict
- `regrouping.missing_theme_groups` - If `theme_groups` key is missing
  - `result_keys`: Shows what keys ARE present
  - `result_repr`: Shows the actual result

- `regrouping.parsed_result` - Success! Shows number of groups

## How to Debug on Your Work Computer

### Option 1: Run the Debug Script

```bash
cd /path/to/aegis
./src/aegis/etls/key_themes/debug_theme_grouping.sh
```

This will:
1. Temporarily enable DEBUG logging
2. Run the ETL
3. Filter for relevant log entries
4. Restore original LOG_LEVEL

### Option 2: Manual Debug Run

```bash
# 1. Edit .env file to enable DEBUG logging
sed -i.bak 's/^LOG_LEVEL=.*/LOG_LEVEL=DEBUG/' .env

# 2. Run the ETL and save full output
source venv/bin/activate
python -m aegis.etls.key_themes.main \
  --bank "Royal Bank of Canada" \
  --year 2025 \
  --quarter Q2 \
  > /tmp/key_themes_debug.log 2>&1

# 3. Check for regrouping errors
grep "regrouping\." /tmp/key_themes_debug.log

# 4. Restore LOG_LEVEL
mv .env.bak .env
```

### Option 3: Check What's in the Database

The tool definition should be stored in PostgreSQL. Check it with:

```bash
source venv/bin/activate
python -c "
from aegis.utils.prompt_loader import load_prompt_from_db
import json

prompt_data = load_prompt_from_db(
    layer='key_themes_etl',
    name='theme_grouping',
    compose_with_globals=False,
    available_databases=None,
    execution_id='debug'
)

print('=== Tool Definition ===')
print(json.dumps(prompt_data.get('tool_definition'), indent=2))
"
```

## What to Look For

### 1. Tool Definition Issues

The tool definition should look like:
```json
{
  "type": "function",
  "function": {
    "name": "group_themes",
    "description": "...",
    "parameters": {
      "type": "object",
      "properties": {
        "theme_groups": {
          "type": "array",
          ...
        }
      },
      "required": ["theme_groups"]
    }
  }
}
```

**Check:**
- Is `function.name` correct? (should match what prompt expects)
- Does `parameters.properties` have `theme_groups`?
- Is `theme_groups` in the `required` array?

### 2. LLM Response Issues

The LLM should return:
```json
{
  "theme_groups": [
    {
      "group_title": "...",
      "qa_ids": ["qa_1", "qa_2"],
      "rationale": "..."
    }
  ]
}
```

**Look for in debug logs:**
- `regrouping.function_called` - Does `function_name` match tool definition?
- `regrouping.raw_arguments` - Does it start with `{` (JSON object)?
- `regrouping.missing_theme_groups` - What keys ARE present?

### 3. Common Issues

#### Issue: `function_name` is wrong
**Cause:** Tool definition has wrong function name
**Fix:** Check database prompt for `theme_grouping`, verify function.name

#### Issue: Arguments is a string like `"\n  \"theme_groups\": ..."`
**Cause:** LLM returned escaped JSON string instead of object
**Fix:** Code should handle this (lines 840-851), but might need adjustment

#### Issue: Result has different keys
**Cause:** LLM didn't follow tool schema
**Fix:** Check system prompt clarity, check if tool definition loaded correctly

## Comparison Check

Compare the debug output on your work computer vs your personal computer:

1. **Tool Definition**: Should be identical
2. **Raw Arguments Type**: Should both be "str" or both be "dict"
3. **Parsed Result Keys**: Should both have `["theme_groups"]`

Any differences will point to the root cause.

## Files Modified

- `src/aegis/etls/key_themes/main.py` (lines 755-850)
  - Added debug logging
  - Added validation and retry logic
  - Added better error messages

## Next Steps If Still Failing

If it still fails on your work computer after adding debug logging:

1. Capture the full debug log
2. Look for `regrouping.missing_theme_groups` entry
3. Check what `result_keys` shows
4. Compare tool definition from database on both computers
5. Check if there are network proxy issues affecting LLM responses

The enhanced logging should pinpoint exactly where the mismatch occurs!
