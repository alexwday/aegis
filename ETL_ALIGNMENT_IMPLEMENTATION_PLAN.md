# ETL Alignment Implementation Plan
**Date**: 2025-11-18
**Reference**: Call Summary ETL (finalized first, production-ready)
**Target**: Align Key Themes and CM Readthrough to Call Summary patterns

---

## 🎯 Call Summary Reference Patterns

### 1. Prompt Loading Pattern
```python
# Direct call to load_prompt_from_db
research_prompts = load_prompt_from_db(
    layer="call_summary_etl",
    name="research_plan",
    compose_with_globals=False,
    available_databases=None,
    execution_id=execution_id,
)

# Access as:
research_prompts["system_prompt"]
research_prompts["user_prompt"]  # or user_prompt_template
research_prompts["tool_definition"]
```

### 2. Prompt Variable Replacement Pattern
```python
# In-place modification of prompt dict
research_prompts["system_prompt"] = research_prompts["system_prompt"].format(
    categories_list=categories_text,
    bank_name=bank_info["bank_name"],
    bank_symbol=bank_info["bank_symbol"],
    quarter=quarter,
    fiscal_year=fiscal_year,
)
```

### 3. Tool Construction Pattern
```python
# Direct use of tool_definition from prompt data
response = await complete_with_tools(
    messages=messages,
    tools=[research_prompts["tool_definition"]],
    context=context,
    llm_params={
        "model": etl_config.get_model("research_plan"),
        "temperature": etl_config.temperature,
        "max_tokens": etl_config.max_tokens,
    },
)
```

### 4. Response Extraction Pattern
```python
# Direct access without error checking wrapper
tool_call = response["choices"][0]["message"]["tool_calls"][0]
research_plan_data = json.loads(tool_call["function"]["arguments"])
```

### 5. Transcript Retrieval Pattern
```python
# Build params dict
retrieval_params = {
    "bank_id": bank_info["bank_id"],
    "bank_name": bank_info["bank_name"],
    "bank_symbol": bank_info["bank_symbol"],
    "fiscal_year": fiscal_year,
    "quarter": quarter,
    "query_intent": "Generate comprehensive research plan for earnings call summary",
}

# Direct calls to transcript utils
chunks = await retrieve_full_section(
    combo=retrieval_params,
    sections="ALL",  # or "MD", "QA" depending on category
    context=context
)

if not chunks:
    raise ValueError(f"No transcript chunks found...")

formatted_transcript = await format_full_section_chunks(
    chunks=chunks,
    combo=retrieval_params,
    context=context
)
```

---

## 📝 Changes Required

### Key Themes ETL

#### Change 1: Remove Custom Wrapper, Use Direct Prompt Loading
**Current** (Lines 377-381):
```python
prompt_data = load_prompt_from_db(
    layer="key_themes_etl",
    name="theme_extraction",
    compose_with_globals=False,
    available_databases=None,
    execution_id=execution_id,
)
```

**Status**: ✅ Already correct - matches Call Summary pattern

#### Change 2: Align Prompt Variable Replacement
**Current** (Line 383-397):
```python
system_prompt = prompt_data["system_prompt"].format(...)  # Creates new variable
```

**Should be** (In-place modification):
```python
prompt_data["system_prompt"] = prompt_data["system_prompt"].format(
    categories_list=categories_str,
    num_categories=len(categories),
    bank_name=bank_info["bank_name"],
    ticker=bank_info["bank_symbol"],
    fiscal_year=fiscal_year,
    quarter=quarter,
    ...
)
```

#### Change 3: Tool Construction
**Current** (Lines 402-410):
```python
tools = [
    {
        "type": "function",
        "function": {
            "name": prompt_data["tool_definition"]["function"]["name"],
            "description": prompt_data["tool_definition"]["function"]["description"],
            "parameters": prompt_data["tool_definition"]["function"]["parameters"],
            "strict": True,
        },
    }
]
```

**Should be** (Direct use):
```python
tools = [prompt_data["tool_definition"]]
```

#### Change 4: Message Construction
**Current**:
```python
messages = [
    {"role": "system", "content": system_prompt},  # Uses new variable
    {"role": "user", "content": user_prompt},      # Uses new variable
]
```

**Should be**:
```python
messages = [
    {"role": "system", "content": prompt_data["system_prompt"]},  # Direct access
    {"role": "user", "content": prompt_data["user_prompt"]},       # Direct access
]
```

---

### CM Readthrough ETL

#### Change 1: Remove load_prompt_template() Wrapper
**Remove** (Lines 512-544):
```python
def load_prompt_template(prompt_file: str, execution_id: str = None) -> Dict[str, Any]:
    ...
```

**Replace all calls** like:
```python
# OLD:
prompt_template = load_prompt_template("outlook_extraction.yaml", execution_id)

# NEW:
outlook_prompts = load_prompt_from_db(
    layer="cm_readthrough_etl",
    name="outlook_extraction",
    compose_with_globals=False,
    available_databases=None,
    execution_id=execution_id,
)
```

#### Change 2: Update All Prompt Template Accesses
**OLD**:
```python
prompt_template["system_template"]  # Wrong key name
prompt_template["user_template"]     # Wrong key name
prompt_template["tool_name"]         # Extracted from tool_definition
```

**NEW**:
```python
outlook_prompts["system_prompt"]     # Correct key name
outlook_prompts["user_prompt"]       # Correct key name
outlook_prompts["tool_definition"]   # Full definition
```

#### Change 3: Align Tool Construction
**Current** (Lines 729-742):
```python
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

**Should be**:
```python
tools = [outlook_prompts["tool_definition"]]
```

#### Change 4: Align Prompt Variable Replacement
**Current** (Inline in messages):
```python
messages = [
    {
        "role": "system",
        "content": prompt_template["system_template"].format(categories_list=categories_text),
    },
    ...
]
```

**Should be** (In-place modification first):
```python
outlook_prompts["system_prompt"] = outlook_prompts["system_prompt"].format(
    categories_list=categories_text
)

outlook_prompts["user_prompt"] = outlook_prompts["user_prompt"].format(
    bank_name=bank_info["bank_name"],
    fiscal_year=fiscal_year,
    quarter=quarter,
    transcript_content=transcript_content,
)

messages = [
    {"role": "system", "content": outlook_prompts["system_prompt"]},
    {"role": "user", "content": outlook_prompts["user_prompt"]},
]
```

#### Change 5: Remove Custom Transcript Wrappers
**Remove** (Lines 547-614):
```python
async def retrieve_full_transcript(...)
async def retrieve_qa_section(...)
```

**Replace usage** with direct calls like Call Summary:
```python
# OLD:
transcript = await retrieve_full_transcript(bank_info, fiscal_year, quarter, context, use_latest)

# NEW:
combo = {
    "bank_id": bank_info["bank_id"],
    "bank_name": bank_info["bank_name"],
    "bank_symbol": bank_info["bank_symbol"],
    "fiscal_year": fiscal_year,
    "quarter": quarter,
}

md_chunks = await retrieve_full_section(combo=combo, sections="MD", context=context)
md_content = await format_full_section_chunks(chunks=md_chunks, combo=combo, context=context)

qa_chunks = await retrieve_full_section(combo=combo, sections="QA", context=context)
qa_content = await format_full_section_chunks(chunks=qa_chunks, combo=combo, context=context)

transcript = f"{md_content}\n\n{qa_content}"
```

---

## 🔧 Implementation Steps

### Step 1: Key Themes Alignment
1. ✅ Prompt loading already correct
2. Update variable replacement to in-place modification
3. Simplify tool construction to direct use
4. Update message construction to use prompt_data directly
5. No transcript changes needed (already uses direct calls)

### Step 2: CM Readthrough Alignment
1. Remove `load_prompt_template()` function
2. Replace all `load_prompt_template()` calls with `load_prompt_from_db()`
3. Update all `system_template` → `system_prompt`
4. Update all `user_template` → `user_prompt`
5. Update all `tool_name/tool_description/tool_parameters` → `tool_definition`
6. Remove `retrieve_full_transcript()` and `retrieve_qa_section()` wrappers
7. Replace wrapper calls with direct transcript utils calls
8. Align prompt variable replacement to in-place modification

### Step 3: Testing
1. Run Key Themes ETL end-to-end
2. Run CM Readthrough ETL end-to-end
3. Verify outputs unchanged
4. Compare with Call Summary patterns visually

---

## ⚠️ Notes

- **Category formatting**: Acknowledged as different by design (each ETL has different structure needs)
- **Database save patterns**: Already aligned enough (minor variations acceptable)
- **Combo dict naming**: Call Summary uses "retrieval_params", others use "combo" - acceptable variation
- **query_intent field**: Only Call Summary uses this (for research planning context) - acceptable

---

**End of Implementation Plan**
