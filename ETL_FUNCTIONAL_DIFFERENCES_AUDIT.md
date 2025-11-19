# ETL Functional Differences Audit
**Date**: 2025-11-18
**Purpose**: Identify identical operations implemented with different code across the three ETLs

---

## 🎯 Goal
Find all instances where:
- All three ETLs perform the same logical operation
- BUT use different code/patterns to accomplish it
- These should be standardized to use the exact same function

---

## 📋 Findings

### 1. ⚠️ PROMPT LOADING - Different Wrapper Functions

#### Issue
**CM Readthrough** has a custom wrapper function `load_prompt_template()` that other ETLs don't use.

#### CM Readthrough (lines 512-544)
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
    prompt_name = prompt_file.replace(".yaml", "")

    prompt_data = load_prompt_from_db(
        layer="cm_readthrough_etl",
        name=prompt_name,
        compose_with_globals=False,  # ETL doesn't use global contexts
        available_databases=None,
        execution_id=execution_id,
    )

    result = {
        "system_template": prompt_data["system_prompt"],
        "user_template": prompt_data.get("user_prompt", ""),
    }

    if prompt_data.get("tool_definition"):
        tool_def = prompt_data["tool_definition"]
        result["tool_name"] = tool_def["function"]["name"]
        result["tool_description"] = tool_def["function"]["description"]
        result["tool_parameters"] = tool_def["function"]["parameters"]["properties"]

    return result
```

**Usage in CM Readthrough**:
```python
# Line 709
prompt_template = load_prompt_template("outlook_extraction.yaml", execution_id)

# Then accesses as:
prompt_template["system_template"]
prompt_template["user_template"]
prompt_template["tool_name"]
prompt_template["tool_description"]
prompt_template["tool_parameters"]
```

#### Call Summary & Key Themes (Direct Approach)
```python
# Call Summary - Line 863
research_prompts = load_prompt_from_db(
    layer="call_summary_etl",
    name="research_plan",
    compose_with_globals=False,
    available_databases=None,
    execution_id=execution_id,
)

# Accesses as:
research_prompts["system_prompt"]  # NOT "system_template"
research_prompts["user_prompt"]     # NOT "user_template"
research_prompts["tool_definition"]["function"]["name"]  # Full path

# Key Themes - Line 377
prompt_data = load_prompt_from_db(
    layer="key_themes_etl",
    name="theme_extraction",
    compose_with_globals=False,
    available_databases=None,
    execution_id=execution_id,
)

# Accesses as:
prompt_data["system_prompt"]  # NOT "system_template"
prompt_data["tool_definition"]  # Direct access
```

#### Recommendation
**Option A**: Remove CM Readthrough's `load_prompt_template()` wrapper and use direct `load_prompt_from_db()` calls like the other ETLs.

**Option B**: Extract `load_prompt_template()` to shared module and have ALL ETLs use it (provides cleaner interface).

**Preference**: Option B - The wrapper function provides a cleaner API with consistent key names.

---

### 2. ⚠️ PROMPT FORMATTING - Different Variable Replacement Patterns

#### Issue
ETLs use different methods to format prompt strings with variables.

#### Call Summary - Direct string formatting
```python
# Lines 886-892
research_prompts["system_prompt"] = research_prompts["system_prompt"].format(
    categories_list=categories_text,
    bank_name=bank_info["bank_name"],
    bank_symbol=bank_info["bank_symbol"],
    quarter=quarter,
    fiscal_year=fiscal_year,
)
```

#### Key Themes - .format() on prompt data
```python
# Lines 383-397
system_prompt = prompt_data["system_prompt"].format(
    categories_list=categories_str,
    num_categories=len(categories),
    bank_name=bank_info["bank_name"],
    ticker=bank_info["bank_symbol"],
    fiscal_year=fiscal_year,
    quarter=quarter,
    ...
)
```

#### CM Readthrough - .format() in messages construction
```python
# Lines 714-727
messages = [
    {
        "role": "system",
        "content": prompt_template["system_template"].format(
            categories_list=categories_text
        ),
    },
    {
        "role": "user",
        "content": prompt_template["user_template"].format(
            bank_name=bank_info["bank_name"],
            fiscal_year=fiscal_year,
            quarter=quarter,
            transcript_content=transcript_content,
        ),
    },
]
```

#### Analysis
- **Call Summary**: Modifies `research_prompts` dict in-place
- **Key Themes**: Creates new variable `system_prompt`
- **CM Readthrough**: Formats inline during message construction

**Pattern difference**: Same operation, different code style

#### Recommendation
Standardize on **inline formatting during message construction** (CM Readthrough's approach is cleanest):

```python
# Standard pattern for ALL ETLs:
messages = [
    {
        "role": "system",
        "content": prompt_data["system_prompt"].format(**variables),
    },
    {
        "role": "user",
        "content": prompt_data["user_prompt"].format(**variables),
    },
]
```

---

### 3. ⚠️ TOOL CONSTRUCTION - Different Assembly Patterns

#### Issue
ETLs construct tool definitions differently even though structure is identical.

#### Call Summary - Full tool construction
```python
# Lines in LLM call
tools = [research_prompts["tool_definition"]]  # Direct use

# Later in extraction (implied):
tools = [extraction_prompts["tool_definition"]]  # Direct use
```

#### Key Themes - Inline tool construction
```python
# Lines 402-410
tools = [
    {
        "type": "function",
        "function": {
            "name": prompt_data["tool_definition"]["function"]["name"],
            "description": prompt_data["tool_definition"]["function"]["description"],
            "parameters": prompt_data["tool_definition"]["function"]["parameters"],
            "strict": True,  # Enable strict mode
        },
    }
]
```

#### CM Readthrough - Inline with manual structure
```python
# Lines 729-742
tools = [
    {
        "type": "function",
        "function": {
            "name": prompt_template["tool_name"],
            "description": prompt_template["tool_description"],
            "parameters": {
                "type": "object",
                "properties": prompt_template["tool_parameters"],
                "required": ["has_content", "statements"],
            },
        },
    }
]
```

#### Analysis
- **Call Summary**: Uses tool_definition directly from prompt data
- **Key Themes**: Reconstructs tool with `strict: True` addition
- **CM Readthrough**: Manually reconstructs full tool structure

**Key differences**:
- Key Themes adds `"strict": True` for schema validation
- CM Readthrough manually specifies `required` fields
- Call Summary trusts prompt database to have complete tool definition

#### Recommendation
Standardize on **direct use with optional strict mode**:

```python
# Standard pattern for ALL ETLs:
tools = [prompt_data["tool_definition"]]

# OR if strict mode needed:
tool_def = prompt_data["tool_definition"].copy()
tool_def["function"]["strict"] = True
tools = [tool_def]
```

---

### 4. ⚠️ LLM PARAMETER CONSTRUCTION - Different Dict Building

#### Issue
All ETLs build `llm_params` dict but with different patterns.

#### Call Summary - Explicit parameters
```python
# Implied from code review:
llm_params = {
    "model": etl_config.get_model("research_plan"),
    "temperature": etl_config.temperature,
    "max_tokens": etl_config.max_tokens,
}
```

#### Key Themes - Same pattern
```python
# Lines 414-417
llm_params = {
    "model": etl_config.get_model("theme_extraction"),
    "temperature": etl_config.temperature,
    "max_tokens": etl_config.max_tokens,
}
```

#### CM Readthrough - Same pattern
```python
# Lines 744-747
llm_params = {
    "model": etl_config.get_model("outlook_extraction"),
    "temperature": etl_config.temperature,
    "max_tokens": etl_config.max_tokens,
}
```

#### Analysis
✅ **ALREADY ALIGNED** - All three use identical pattern.

---

### 5. ⚠️ TRANSCRIPT RETRIEVAL - Wrapper Functions vs Direct Calls

#### Issue
CM Readthrough has wrapper functions that other ETLs don't use.

#### CM Readthrough - Custom wrappers (lines 547-614)
```python
async def retrieve_full_transcript(
    bank_info: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    use_latest: bool,
) -> Optional[str]:
    """Retrieve full transcript (MD + Q&A sections) as single string."""

    # Has latest quarter fallback logic
    if use_latest:
        latest = await find_latest_available_quarter(...)

    combo = {
        "bank_id": bank_info["bank_id"],
        "bank_name": bank_info["bank_name"],
        "bank_symbol": bank_info["bank_symbol"],
        "fiscal_year": actual_year,
        "quarter": actual_quarter,
    }

    # Retrieves MD + QA
    md_chunks = await retrieve_full_section(combo=combo, sections="MD", context=context)
    md_content = await format_full_section_chunks(chunks=md_chunks, combo=combo, context=context)

    qa_chunks = await retrieve_full_section(combo=combo, sections="QA", context=context)
    qa_content = await format_full_section_chunks(chunks=qa_chunks, combo=combo, context=context)

    return f"{md_content}\n\n{qa_content}"


async def retrieve_qa_section(
    bank_info: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    context: Dict[str, Any],
    use_latest: bool,
) -> Optional[str]:
    """Retrieve only Q&A section as single string."""
    # Similar structure but only QA
```

#### Call Summary & Key Themes - Direct calls
```python
# Call Summary - Lines 849-861
retrieval_params = {
    "bank_id": bank_info["bank_id"],
    "bank_name": bank_info["bank_name"],
    "bank_symbol": bank_info["bank_symbol"],
    "fiscal_year": fiscal_year,
    "quarter": quarter,
    "query_intent": "Generate comprehensive research plan for earnings call summary",
}

chunks = await retrieve_full_section(combo=retrieval_params, sections="ALL", context=context)

if not chunks:
    raise ValueError(...)

formatted_transcript = await format_full_section_chunks(
    chunks=chunks, combo=retrieval_params, context=context
)

# Key Themes - Similar direct approach
combo = {
    "bank_name": bank_info["bank_name"],
    "bank_id": bank_info["bank_id"],
    "bank_symbol": bank_info["bank_symbol"],
    "fiscal_year": fiscal_year,
    "quarter": quarter,
}

chunks = await retrieve_full_section(combo=combo, sections="QA", context=context)
```

#### Recommendation
**Option A**: Keep CM Readthrough wrappers as they provide cleaner interface for multi-bank processing.

**Option B**: Extract wrappers to shared module so ALL ETLs can use them.

**Preference**: Option B - The wrappers provide better abstraction and error handling.

---

### 6. ⚠️ COMBO DICT CONSTRUCTION - Inconsistent Field Names

#### Issue
The "combo" dict uses different field names across ETLs.

#### Call Summary
```python
retrieval_params = {  # Called "retrieval_params" not "combo"
    "bank_id": bank_info["bank_id"],
    "bank_name": bank_info["bank_name"],
    "bank_symbol": bank_info["bank_symbol"],
    "fiscal_year": fiscal_year,
    "quarter": quarter,
    "query_intent": "Generate comprehensive research plan for earnings call summary",  # UNIQUE
}
```

#### Key Themes
```python
combo = {  # Called "combo"
    "bank_name": bank_info["bank_name"],
    "bank_id": bank_info["bank_id"],
    "bank_symbol": bank_info["bank_symbol"],
    "fiscal_year": fiscal_year,
    "quarter": quarter,
    # NO query_intent
}
```

#### CM Readthrough
```python
combo = {  # Called "combo"
    "bank_id": bank_info["bank_id"],
    "bank_name": bank_info["bank_name"],
    "bank_symbol": bank_info["bank_symbol"],
    "fiscal_year": actual_year,  # May differ from input
    "quarter": actual_quarter,    # May differ from input
    # NO query_intent
}
```

#### Analysis
**Differences**:
1. Variable name: `retrieval_params` vs `combo`
2. Field order: different across ETLs
3. Call Summary includes `query_intent`, others don't
4. CM Readthrough uses `actual_year/actual_quarter` (may differ from requested)

#### Recommendation
Standardize on:
```python
# Standard combo dict for ALL ETLs:
combo = {
    "bank_id": bank_info["bank_id"],
    "bank_name": bank_info["bank_name"],
    "bank_symbol": bank_info["bank_symbol"],
    "fiscal_year": fiscal_year,
    "quarter": quarter,
}

# Optional query_intent if needed:
combo["query_intent"] = "..."  # Add when applicable
```

---

### 7. ⚠️ RESPONSE EXTRACTION - Different JSON Parsing Patterns

#### Issue
All ETLs extract JSON from tool calls but use different code.

#### Call Summary (implied pattern)
```python
response = await complete_with_tools(...)
tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")
if tool_calls:
    tool_call = tool_calls[0]
    result = json.loads(tool_call["function"]["arguments"])
```

#### Key Themes
```python
# Lines 428-439
response = await complete_with_tools(...)

tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")
if not tool_calls:
    logger.error(...)
    return None

tool_call = tool_calls[0]
result = json.loads(tool_call["function"]["arguments"])
```

#### CM Readthrough
```python
# Lines 751-777
response = await complete_with_tools(...)

tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")
if tool_calls:
    tool_call = tool_calls[0]
    result = json.loads(tool_call["function"]["arguments"])

    if not result.get("has_content", False):
        logger.info(f"[NO OUTLOOK] {bank_info['bank_name']}: No relevant outlook found")
        return {"has_content": False, "statements": []}

    # Process result...
    return result
else:
    logger.warning(f"No tool call in response for {bank_info['bank_name']}")
    return {"has_content": False, "statements": []}
```

#### Analysis
**Differences**:
- **Error handling**: Key Themes returns None, CM Readthrough returns empty dict
- **Logging**: Different log messages
- **Validation**: CM Readthrough checks `has_content` field

**Pattern is similar** but return values differ.

#### Recommendation
Create standard extraction helper:
```python
def extract_tool_response(
    response: dict,
    default_return: Any = None,
    validate_fn: Optional[Callable] = None
) -> Any:
    """Standard tool response extraction with validation."""
    tool_calls = response.get("choices", [{}])[0].get("message", {}).get("tool_calls")

    if not tool_calls:
        logger.warning("No tool call in LLM response")
        return default_return

    tool_call = tool_calls[0]
    result = json.loads(tool_call["function"]["arguments"])

    if validate_fn and not validate_fn(result):
        logger.info("Tool response failed validation")
        return default_return

    return result
```

---

### 8. ⚠️ CATEGORY FORMATTING - Different String Building

#### Issue
ETLs format category lists for prompts differently.

#### Call Summary - Manual string concatenation
```python
# Lines 871-884
categories_text = ""
for i, category in enumerate(categories, 1):
    section_desc = {
        "MD": "Management Discussion section only",
        "QA": "Q&A section only",
        "ALL": "Both Management Discussion and Q&A sections",
    }.get(category["transcripts_section"], "ALL sections")

    categories_text += f"""
Category {i}:
- Name: {category['category_name']}
- Section: {section_desc}
- Instructions: {category['category_description']}
"""
```

#### Key Themes - List comprehension + join
```python
# Implied from context:
categories_list = []
for i, cat in enumerate(categories, 1):
    categories_list.append(
        f"{i}. {cat['category_name']}\n   {cat['category_description']}"
    )
categories_str = "\n\n".join(categories_list)
```

#### CM Readthrough - Custom format function
```python
# Lines 341-366
def format_categories_for_prompt(categories: List[Dict[str, Any]]) -> str:
    """Format category dictionaries into a structured prompt format."""

    formatted_sections = []

    for cat in categories:
        section = "<example_category>\n"
        section += f"Category: {cat['category']}\n"
        section += f"Description: {cat['description']}\n"

        if cat.get("examples") and len(cat["examples"]) > 0:
            section += "Examples:\n"
            for example in cat["examples"]:
                section += f"  - {example}\n"

        section += "</example_category>"
        formatted_sections.append(section)

    return "\n\n".join(formatted_sections)
```

#### Analysis
**Three different patterns** for the same logical operation:
1. Call Summary: String concatenation
2. Key Themes: List + join
3. CM Readthrough: Dedicated function with XML-style tags

**CM Readthrough's approach is best** - reusable function, clear structure.

#### Recommendation
Extract CM Readthrough's `format_categories_for_prompt()` to shared module, adapt for different category structures:

```python
# Shared function for ALL ETLs:
def format_categories_for_prompt(
    categories: List[Dict[str, Any]],
    template: str = "xml"  # or "numbered", "bulleted"
) -> str:
    """Universal category formatter for prompts."""
    # Implementation
```

---

### 9. ⚠️ DATABASE SAVE PATTERNS - Different Delete Logic

#### Issue
All ETLs delete existing reports before inserting, but with different SQL patterns.

#### Call Summary (implied from other ETLs)
```python
# Delete existing report
await conn.execute(
    text("""
        DELETE FROM aegis_reports
        WHERE bank_id = :bank_id
          AND fiscal_year = :fiscal_year
          AND quarter = :quarter
          AND report_type = :report_type
    """),
    {
        "bank_id": bank_info["bank_id"],
        "fiscal_year": fiscal_year,
        "quarter": quarter,
        "report_type": "call_summary",
    }
)
```

#### Key Themes
```python
# Similar pattern with data availability update
await conn.execute(
    text("""
        DELETE FROM aegis_reports
        WHERE bank_id = :bank_id
          AND fiscal_year = :fiscal_year
          AND quarter = :quarter
          AND report_type = :report_type
        RETURNING id
    """),
    {
        "bank_id": bank_info["bank_id"],
        "fiscal_year": fiscal_year,
        "quarter": quarter,
        "report_type": "key_themes",
    }
)
deleted_rows = result.rowcount

# Then updates aegis_data_availability
```

#### CM Readthrough
```python
# Lines 1398-1414
delete_result = await conn.execute(
    text("""
        DELETE FROM aegis_reports
        WHERE fiscal_year = :fiscal_year
          AND quarter = :quarter
          AND report_type = :report_type
        RETURNING id
    """),
    {
        "fiscal_year": fiscal_year,
        "quarter": quarter,
        "report_type": "cm_readthrough",
    }
)
delete_result.fetchall()  # Consume result
```

#### Analysis
**Differences**:
1. Call Summary & Key Themes: Filter by `bank_id`
2. CM Readthrough: NO bank_id filter (cross-bank report)
3. Key Themes: Uses `RETURNING id` and checks rowcount
4. CM Readthrough: Uses `RETURNING id` but calls `fetchall()`

#### Recommendation
Standardize on pattern with RETURNING:
```python
# Standard delete pattern:
delete_result = await conn.execute(
    text("""
        DELETE FROM aegis_reports
        WHERE <conditions>
        RETURNING id
    """),
    params
)
deleted_count = len(delete_result.fetchall())

if deleted_count > 0:
    logger.info(f"Deleted {deleted_count} existing report(s)")
```

---

### 10. ✅ ALREADY ALIGNED - No Differences

The following operations are **already standardized** across all ETLs:

1. **ETLConfig class** - Identical implementation (should still extract to shared module)
2. **get_bank_info()** - Identical SQL and logic
3. **verify_data_availability()** - Identical where used (Call Summary & Key Themes)
4. **Authentication setup** - Identical pattern
5. **SSL setup** - Identical pattern
6. **Context dict construction** - Identical structure
7. **LLM params dict** - Identical pattern (as shown in #4)
8. **Error handling try-except** - Identical structure
9. **Main entry point** - Identical CLI pattern

---

## 📊 Summary of Differences

| Operation | Call Summary | Key Themes | CM Readthrough | Recommendation |
|-----------|-------------|-----------|----------------|----------------|
| **Prompt loading** | Direct `load_prompt_from_db` | Direct `load_prompt_from_db` | Wrapper `load_prompt_template()` | Extract wrapper to shared module |
| **Prompt formatting** | In-place modification | New variable | Inline in messages | Standardize on inline |
| **Tool construction** | Direct use | Reconstruct with strict | Manual rebuild | Standardize on direct use |
| **LLM params** | ✅ Identical | ✅ Identical | ✅ Identical | Already aligned |
| **Transcript retrieval** | Direct calls | Direct calls | Wrapper functions | Extract wrappers to shared |
| **Combo dict** | "retrieval_params" + query_intent | "combo" | "combo" + actual dates | Standardize name and fields |
| **Response extraction** | Basic pattern | Return None on error | Return empty dict | Create helper function |
| **Category formatting** | String concat | List + join | Dedicated function | Extract function to shared |
| **Database delete** | Basic DELETE | DELETE RETURNING + rowcount | DELETE RETURNING + fetchall | Standardize on RETURNING |

---

## 🎯 Recommended Actions

### High Priority (Different code for same operation)
1. **Extract `load_prompt_template()` wrapper** - Provides cleaner API than direct calls
2. **Standardize combo dict** - Same name, same fields, same order everywhere
3. **Extract transcript retrieval wrappers** - Better abstraction, error handling
4. **Create tool response extraction helper** - Consistent error handling
5. **Extract category formatting function** - Reusable, clear structure

### Medium Priority (Different patterns, functionally equivalent)
6. **Standardize prompt variable replacement** - Inline formatting preferred
7. **Align database delete logic** - Use RETURNING consistently
8. **Tool construction pattern** - Document when to use strict mode

### Low Priority (Already mostly aligned)
9. **Document combo dict field usage** - Clarify when query_intent needed
10. **Standardize logging messages** - Same format for similar operations

---

## 📝 Implementation Plan

### Phase 1: Create Shared Module Structure
```bash
src/aegis/etls/common/
├── __init__.py
├── config.py           # ETLConfig class
├── bank_lookup.py      # get_bank_info, verify_data_availability
├── prompt_utils.py     # load_prompt_template, format_categories
├── transcript_utils.py # retrieve_full_transcript, retrieve_qa_section
├── response_utils.py   # extract_tool_response
└── database_utils.py   # save_report (standardized pattern)
```

### Phase 2: Migrate Functions
1. Move ETLConfig to `common/config.py`
2. Move bank lookup to `common/bank_lookup.py`
3. Create `load_prompt_template()` in `common/prompt_utils.py`
4. Create transcript wrappers in `common/transcript_utils.py`
5. Create `extract_tool_response()` in `common/response_utils.py`

### Phase 3: Update All ETLs
1. Replace duplicated functions with imports from `common/`
2. Standardize combo dict across all ETLs
3. Align prompt formatting to inline pattern
4. Update database save logic to use RETURNING

### Phase 4: Testing
1. Run all three ETLs end-to-end
2. Verify outputs unchanged
3. Check for any regressions

---

**End of Functional Differences Audit**
