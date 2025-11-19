# Comprehensive ETL Comparison: Call Summary vs Key Themes vs CM Readthrough

**Date**: 2025-11-18
**Purpose**: Full side-by-side alignment analysis of the three Aegis ETL pipelines

---

## Executive Summary

All three ETLs share a common architectural foundation but diverge significantly in their processing logic, data scope, and output formats:

- **Call Summary**: Single-bank, sequential category processing with research planning
- **Key Themes**: Single-bank, Q&A thematic grouping with sequential then parallel stages
- **CM Readthrough**: Multi-bank, concurrent processing with cross-institution aggregation

---

## 1. ✅ COMPLETE ALIGNMENT - Standardized Components

### 1.1 ETLConfig Class
**Status**: ✅ **IDENTICAL** across all three ETLs

```python
class ETLConfig:
    """ETL configuration loader that reads YAML configs and resolves model references."""

    def __init__(self, config_path: str)
    def _load_config(self) -> Dict[str, Any]
    def get_model(self, model_key: str) -> str  # Resolves small/medium/large tiers

    @property
    def temperature(self) -> float  # Defaults to 0.1

    @property
    def max_tokens(self) -> int  # Defaults to 32768
```

**Location**: Inlined in each `main.py` (lines 53-114 in all three)

**Why this alignment matters**: Single source of truth for config management across all ETLs.

---

### 1.2 Bank Lookup Function
**Status**: ✅ **IDENTICAL** - `get_bank_info()`

All three ETLs use the exact same function to look up bank information:

```python
async def get_bank_info(bank_name: str) -> Dict[str, Any]:
    """Look up bank information from aegis_data_availability table."""
    # Supports: ID lookup, exact name/symbol match, partial name match
    # Returns: {"bank_id": int, "bank_name": str, "bank_symbol": str}
```

**Implementation**:
- Lines 211-279 (call_summary)
- Lines 188-256 (key_themes)
- Lines 369-437 (cm_readthrough)

---

### 1.3 Data Availability Check
**Status**: ✅ **IDENTICAL** - `verify_data_availability()`

```python
async def verify_data_availability(bank_id: int, fiscal_year: int, quarter: str) -> bool:
    """Check if transcript data is available for specified bank and period."""
    # Queries: aegis_data_availability.database_names
    # Returns: True if "transcripts" in database_names array
```

**Implementation**:
- Lines 736-767 (call_summary)
- Lines 258-289 (key_themes)
- Not used in cm_readthrough (uses `find_latest_available_quarter()` instead)

---

### 1.4 Import Structure
**Status**: ✅ **HIGHLY ALIGNED**

All three ETLs import the same core dependencies:

```python
# Standard library
import argparse, asyncio, json, uuid, os, hashlib
from datetime import datetime
from typing import Dict, Any, List

# External libraries
import pandas as pd
import yaml
from sqlalchemy import text
from docx import Document

# Aegis utilities
from aegis.utils.ssl import setup_ssl
from aegis.connections.oauth_connector import setup_authentication
from aegis.connections.llm_connector import complete, complete_with_tools
from aegis.connections.postgres_connector import get_connection
from aegis.utils.logging import setup_logging, get_logger
from aegis.utils.prompt_loader import load_prompt_from_db
from aegis.utils.sql_prompt import postgresql_prompts
from aegis.utils.settings import config
```

**Minor differences**:
- `key_themes` imports `docx.shared.RGBColor` (for HTML formatting)
- `cm_readthrough` imports `docx.enum.section.WD_ORIENTATION` (for landscape orientation)

---

### 1.5 Main Entry Point Pattern
**Status**: ✅ **IDENTICAL STRUCTURE**

All three ETLs follow the same CLI pattern:

```python
def main():
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(...)

    # Call summary / Key themes:
    parser.add_argument("--bank", required=True, help="Bank ID, name, or symbol")
    parser.add_argument("--year", type=int, required=True, help="Fiscal year")
    parser.add_argument("--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"])

    # CM readthrough:
    parser.add_argument("--year", type=int, required=True, help="Fiscal year")
    parser.add_argument("--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"])
    parser.add_argument("--use-latest", action="store_true")
    parser.add_argument("--output", type=str, help="Output file path")

    args = parser.parse_args()
    postgresql_prompts()  # All three initialize prompts

    result = asyncio.run(generate_[etl_name](...))
    print(result)

if __name__ == "__main__":
    main()
```

---

### 1.6 Error Handling Pattern
**Status**: ✅ **IDENTICAL STRUCTURE**

All three ETLs use the same try-except structure with emoji prefixes:

```python
try:
    # ETL processing logic
    ...
    return f"✅ Complete: {filepath}\n   Stats: ..."

except (KeyError, TypeError, AttributeError, json.JSONDecodeError,
        SQLAlchemyError, FileNotFoundError) as e:
    # System errors - unexpected errors (code bugs)
    error_msg = f"Error generating [etl_name]: {str(e)}"
    logger.error("etl.[etl_name].error", execution_id=execution_id,
                 error=error_msg, exc_info=True)
    return f"❌ {error_msg}"

except (ValueError, RuntimeError) as e:
    # User-friendly errors - expected conditions (no data, auth failure)
    logger.error("etl.[etl_name].error", execution_id=execution_id, error=str(e))
    return f"⚠️ {str(e)}"
```

---

### 1.7 Execution Context Structure
**Status**: ✅ **IDENTICAL**

All three ETLs build the same context dictionary:

```python
execution_id = str(uuid.uuid4())
ssl_config = setup_ssl()
auth_config = await setup_authentication(execution_id, ssl_config)

context = {
    "execution_id": execution_id,
    "ssl_config": ssl_config,
    "auth_config": auth_config,
}
```

---

### 1.8 Database Report Metadata
**Status**: ✅ **STANDARDIZED** - `get_standard_report_metadata()`

All three ETLs have this function in their `document_converter.py`:

```python
def get_standard_report_metadata() -> Dict[str, str]:
    return {
        "report_name": str,
        "report_description": str,
        "report_type": str,  # "call_summary" | "key_themes" | "cm_readthrough"
    }
```

---

## 2. ⚙️ CONFIGURATION DIFFERENCES

### 2.1 Model Configuration

#### Call Summary (`config.yaml`)
```yaml
models:
  research_plan:
    tier: large     # Planning phase
  category_extraction:
    tier: medium    # Extraction phase

llm:
  temperature: 0.1
  max_tokens: 32768
```

**Total models**: 2
**Rationale**: Research planning requires large model; category extraction uses medium

---

#### Key Themes (`config.yaml`)
```yaml
models:
  theme_extraction:
    tier: medium    # Classification phase
  formatting:
    tier: medium    # HTML formatting phase
  grouping:
    tier: medium    # Regrouping phase

llm:
  temperature: 0.1
  max_tokens: 32768
```

**Total models**: 3
**Rationale**: All phases use medium model for consistency

---

#### CM Readthrough (`config.yaml`)
```yaml
models:
  outlook_extraction:
    tier: large     # Outlook from full transcript
  qa_extraction:
    tier: large     # Questions from Q&A
  subtitle_generation:
    tier: large     # Subtitle synthesis
  batch_formatting:
    tier: large     # Batch HTML formatting (disabled)

llm:
  temperature: 0.1  # ✅ ALIGNED with other ETLs
  max_tokens: 32768 # ✅ ALIGNED with other ETLs

concurrency:
  max_concurrent_banks: 5  # ✨ UNIQUE TO CM READTHROUGH
```

**Total models**: 4
**Key differences**:
- ✅ **Temperature and max_tokens**: Now aligned with other ETLs (0.1, 32768)
- ✨ **Concurrency control**: Processes 5 banks simultaneously

---

### 2.2 Temperature Analysis

| ETL | Temperature | Implication |
|-----|-------------|-------------|
| Call Summary | 0.1 | Highly deterministic, consistent extraction |
| Key Themes | 0.1 | Highly deterministic, consistent grouping |
| CM Readthrough | **0.1** | ✅ **Now aligned** - Highly deterministic |

**Status**: ✅ **All three ETLs now use temperature 0.1** for consistent, deterministic outputs.

---

### 2.3 Max Tokens Analysis

| ETL | Max Tokens | Typical Use Case |
|-----|------------|------------------|
| Call Summary | 32768 | Long category extractions with evidence |
| Key Themes | 32768 | Full Q&A block classification + HTML |
| CM Readthrough | **32768** | ✅ **Now aligned** - Same capacity as others |

**Status**: ✅ **All three ETLs now use max_tokens 32768** for consistent token budgets.

---

## 3. 🏗️ ARCHITECTURAL DIFFERENCES

### 3.1 Processing Scope

| Dimension | Call Summary | Key Themes | CM Readthrough |
|-----------|-------------|-----------|----------------|
| **Banks per run** | Single | Single | **Multiple (all monitored)** |
| **Transcript sections** | ALL (MD + QA) | QA only | ALL (MD + QA) per bank |
| **Parallel processing** | None (sequential categories) | Partial (parallel formatting) | **Full (concurrent banks)** |
| **Aggregation** | Single bank results | Single bank themes | **Cross-bank aggregation** |

---

### 3.2 Pipeline Architecture

#### Call Summary: Sequential Research Pipeline
```
1. Retrieve full transcript (MD + QA)
2. Generate research plan (1 LLM call)
   └─ Analyzes all categories upfront
3. Process categories sequentially (N LLM calls)
   ├─ Category 1 extraction
   ├─ Category 2 extraction (aware of Category 1)
   ├─ Category 3 extraction (aware of Categories 1-2)
   └─ Category N extraction (aware of all prior)
4. Generate document (structured Word doc)
5. Save to database (single bank report)
```

**Key characteristics**:
- ✅ **Cumulative context**: Later categories see prior results
- ✅ **Research-driven**: Planning phase guides extraction
- ⚠️ **Sequential bottleneck**: Categories processed one at a time

---

#### Key Themes: Hybrid Sequential-Parallel Pipeline
```
1. Load Q&A blocks into index
2. Sequential classification (N LLM calls)
   ├─ Q&A 1 classified (no prior context)
   ├─ Q&A 2 classified (aware of Q&A 1)
   ├─ Q&A 3 classified (aware of Q&As 1-2)
   └─ Q&A N classified (aware of all prior)
3. Parallel HTML formatting (N concurrent LLM calls)
   └─ All Q&As formatted simultaneously
4. Comprehensive regrouping (1 LLM call)
   └─ Reviews all classifications, creates final groups
5. Apply grouping to index (programmatic)
6. Generate document (themed Word doc)
7. Save to database (single bank report)
```

**Key characteristics**:
- ✅ **Cumulative classification**: Later Q&As see prior classifications
- ✅ **Parallel formatting**: Fast HTML generation
- ✅ **Global regrouping**: LLM can reorganize themes across all Q&As
- ⚠️ **Classification bottleneck**: Sequential until formatting stage

---

#### CM Readthrough: Concurrent Multi-Bank Pipeline
```
1. Load monitored institutions (all banks)
2. Concurrent bank processing (max 5 banks at once)
   ├─ Bank 1: Extract outlook + section2 + section3
   ├─ Bank 2: Extract outlook + section2 + section3
   ├─ Bank 3: Extract outlook + section2 + section3
   ├─ Bank 4: Extract outlook + section2 + section3
   └─ Bank 5: Extract outlook + section2 + section3
   (continues for all banks with semaphore control)
3. Aggregate results by section
   ├─ All outlook statements collected
   ├─ All section2 questions collected
   └─ All section3 questions collected
4. Parallel subtitle generation (3 concurrent LLM calls)
   ├─ Outlook subtitle
   ├─ Section2 subtitle
   └─ Section3 subtitle
5. Batch formatting (disabled for performance)
6. Generate document (3-section landscape table)
7. Save to database (cross-bank report)
```

**Key characteristics**:
- ✅ **Massive parallelism**: 5 banks processed simultaneously
- ✅ **Cross-bank insights**: Aggregates themes across institutions
- ✅ **Scalable**: Handles 10-20 banks efficiently
- ⚠️ **No per-bank context**: Banks processed independently

---

### 3.3 Concurrency Patterns

#### Call Summary: Zero Concurrency
```python
for i, category in enumerate(categories, 1):
    # Sequential processing
    category_plan = get_plan_for_category(i)
    chunks = await retrieve_full_section(...)
    result = await extract_category(...)
    category_results.append(result)
```

**Why**: Cumulative context requires seeing prior results.

---

#### Key Themes: Hybrid Concurrency
```python
# Stage 1: Sequential classification
for qa_block in sorted_qa_blocks:
    await classify_qa_block(qa_block, previous_classifications, ...)
    previous_classifications.append(result)

# Stage 2: Parallel formatting
tasks = [format_qa_html(qa_block, context) for qa_block in valid_qa_blocks]
await asyncio.gather(*tasks)

# Stage 3: Single comprehensive regrouping
theme_groups = await determine_comprehensive_grouping(...)
```

**Why**: Classification needs context; formatting doesn't; regrouping needs global view.

---

#### CM Readthrough: Full Concurrency
```python
semaphore = asyncio.Semaphore(5)  # Max 5 concurrent banks

async def process_bank_outlook(bank_data):
    async with semaphore:
        # Process outlook for this bank
        ...

async def process_bank_section2(bank_data):
    async with semaphore:
        # Process section2 for this bank
        ...

# Launch all banks concurrently
outlook_tasks = [process_bank_outlook(bank) for bank in monitored_banks]
section2_tasks = [process_bank_section2(bank) for bank in monitored_banks]
section3_tasks = [process_bank_section3(bank) for bank in monitored_banks]

bank_outlook, bank_section2, bank_section3 = await asyncio.gather(
    asyncio.gather(*outlook_tasks),
    asyncio.gather(*section2_tasks),
    asyncio.gather(*section3_tasks),
)
```

**Why**: Banks are independent; no cross-bank context needed during extraction.

---

## 4. 📊 DATA SOURCE DIFFERENCES

### 4.1 Transcript Section Usage

| ETL | Sections Used | Retrieval Method | Purpose |
|-----|---------------|------------------|---------|
| Call Summary | `ALL` (MD + QA) | Single combined retrieval | Full transcript for research planning |
| | Per-category: `MD`, `QA`, or `ALL` | Category-specific retrieval | Targeted extraction per category |
| Key Themes | `QA` only | Single retrieval | Q&A thematic analysis |
| CM Readthrough | `ALL` (MD + QA) per bank | Per-bank combined retrieval | Outlook extraction |
| | `QA` per bank | Per-bank Q&A retrieval | Question extraction (sections 2 & 3) |

---

### 4.2 Transcript Utility Functions

#### Call Summary & CM Readthrough (Identical)
```python
# Located in: call_summary/transcript_utils.py
# Located in: cm_readthrough/transcript_utils.py

async def retrieve_full_section(combo: dict, sections: str, context: dict) -> list:
    """Retrieve transcript chunks for specified sections."""

async def format_full_section_chunks(chunks: list, combo: dict, context: dict) -> str:
    """Format chunks into single text string."""
```

**Status**: ✅ **IDENTICAL** implementations in both ETLs

---

#### Key Themes (Simplified)
```python
# Located in: key_themes/transcript_utils.py

async def retrieve_full_section(combo: dict, sections: str, context: dict) -> list:
    """Retrieve transcript chunks - SIMPLIFIED VERSION."""
    # Does NOT include format_full_section_chunks()
    # Returns raw chunks, not formatted string
```

**Status**: ⚠️ **DIFFERENT** - Missing formatting function

**Why the difference**:
- Key Themes processes Q&A blocks individually
- Doesn't need full formatted transcript
- Builds Q&A index from raw chunks

---

### 4.3 Data Retrieval Parameters

#### Call Summary
```python
retrieval_params = {
    "bank_id": bank_info["bank_id"],
    "bank_name": bank_info["bank_name"],
    "bank_symbol": bank_info["bank_symbol"],
    "fiscal_year": fiscal_year,
    "quarter": quarter,
    "query_intent": "Generate comprehensive research plan for earnings call summary",
}

# Full transcript for planning
chunks = await retrieve_full_section(combo=retrieval_params, sections="ALL", context=context)

# Per-category retrieval (varies by category)
chunks = await retrieve_full_section(combo=retrieval_params,
                                     sections=category["transcripts_section"],
                                     context=context)
```

---

#### Key Themes
```python
combo = {
    "bank_name": bank_info["bank_name"],
    "bank_id": bank_info["bank_id"],
    "bank_symbol": bank_info["bank_symbol"],
    "fiscal_year": fiscal_year,
    "quarter": quarter,
}

# Q&A only
chunks = await retrieve_full_section(combo=combo, sections="QA", context=context)
```

---

#### CM Readthrough
```python
combo = {
    "bank_id": bank_info["bank_id"],
    "bank_name": bank_info["bank_name"],
    "bank_symbol": bank_info["bank_symbol"],
    "fiscal_year": actual_year,  # May differ from requested (uses latest)
    "quarter": actual_quarter,    # May differ from requested (uses latest)
}

# Full transcript for outlook
md_chunks = await retrieve_full_section(combo=combo, sections="MD", context=context)
qa_chunks = await retrieve_full_section(combo=combo, sections="QA", context=context)
full_transcript = f"{md_content}\n\n{qa_content}"

# Q&A only for questions
qa_chunks = await retrieve_full_section(combo=combo, sections="QA", context=context)
```

**Unique feature**: ✨ **Latest quarter fallback**

```python
async def find_latest_available_quarter(
    bank_id: int, min_fiscal_year: int, min_quarter: str
) -> Optional[Tuple[int, str]]:
    """Find latest available quarter at or after minimum specified."""
```

**Why**: CM Readthrough can use newer data if requested quarter unavailable.

---

## 5. 📂 CATEGORY SYSTEM DIFFERENCES

### 5.1 Category Loading

#### Call Summary: Bank-Type-Specific Categories
```python
def load_categories_from_xlsx(bank_type: str, execution_id: str) -> List[Dict[str, str]]:
    """Load categories from bank-type-specific XLSX file."""

    file_name = (
        "canadian_banks_categories.xlsx" if bank_type == "Canadian_Banks"
        else "us_banks_categories.xlsx"
    )

    xlsx_path = os.path.join(current_dir, "config", "categories", file_name)
    df = pd.read_excel(xlsx_path, sheet_name=0)

    required_columns = ["transcripts_section", "category_name", "category_description"]
    categories = df.to_dict("records")
    return categories
```

**Category structure**:
```python
{
    "transcripts_section": "MD" | "QA" | "ALL",
    "category_name": str,
    "category_description": str,
    "report_section": str,  # Optional: "Results Summary" or other
}
```

**Bank type determination**:
```python
def get_bank_type(bank_id: int) -> str:
    """Look up bank type from monitored institutions config."""
    institutions = _load_monitored_institutions()  # From YAML
    return institutions[bank_id]["type"]  # "Canadian_Banks" | "US_Banks"
```

**File locations**:
- `config/categories/canadian_banks_categories.xlsx`
- `config/categories/us_banks_categories.xlsx`

---

#### Key Themes: Universal Categories
```python
def load_categories_from_xlsx(execution_id: str) -> List[Dict[str, str]]:
    """Load universal categories - same for all banks."""

    file_name = "key_themes_categories.xlsx"
    xlsx_path = os.path.join(current_dir, "config", "categories", file_name)
    df = pd.read_excel(xlsx_path, sheet_name=0)

    required_columns = ["category_name", "category_description"]
    categories = df.to_dict("records")
    return categories
```

**Category structure**:
```python
{
    "category_name": str,
    "category_description": str,
}
```

**File location**:
- `config/categories/key_themes_categories.xlsx`

**Key difference**: ⚠️ **No bank-specific categories** - universal across all banks

---

#### CM Readthrough: Multiple Category Sets
```python
def load_outlook_categories(execution_id: str) -> List[Dict[str, Any]]:
    """Load outlook categories for Section 1."""
    xlsx_path = "config/categories/outlook_categories.xlsx"
    # Parse with examples support
    category = {
        "category": str,
        "description": str,
        "examples": [str, str, str],  # Up to 3 examples
    }
    return categories

def load_qa_market_volatility_regulatory_categories(execution_id: str) -> List[Dict[str, Any]]:
    """Load Q&A categories for Section 2."""
    xlsx_path = "config/categories/qa_market_volatility_regulatory_categories.xlsx"
    # Same structure as outlook categories
    return categories

def load_qa_pipelines_activity_categories(execution_id: str) -> List[Dict[str, Any]]:
    """Load Q&A categories for Section 3."""
    xlsx_path = "config/categories/qa_pipelines_activity_categories.xlsx"
    # Same structure as outlook categories
    return categories
```

**Category structure**:
```python
{
    "category": str,
    "description": str,
    "examples": [str, str, str],  # Optional examples
}
```

**File locations**:
- `config/categories/outlook_categories.xlsx`
- `config/categories/qa_market_volatility_regulatory_categories.xlsx`
- `config/categories/qa_pipelines_activity_categories.xlsx`

**Key difference**: ✨ **Three separate category systems** for different sections

---

### 5.2 Category Usage in Prompts

#### Call Summary
```python
# Categories passed to research planning
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

# Injected into system prompt for research_plan
research_prompts["system_prompt"] = research_prompts["system_prompt"].format(
    categories_list=categories_text,
    ...
)
```

---

#### Key Themes
```python
# Categories formatted for classification prompt
categories_list = []
for i, cat in enumerate(categories, 1):
    categories_list.append(
        f"{i}. {cat['category_name']}\n   {cat['category_description']}"
    )
categories_str = "\n\n".join(categories_list)

# Injected into system prompt for theme_extraction
system_prompt = prompt_data["system_prompt"].format(
    categories_list=categories_str,
    num_categories=len(categories),
    ...
)
```

---

#### CM Readthrough
```python
def format_categories_for_prompt(categories: List[Dict[str, Any]]) -> str:
    """Format categories with examples for prompt injection."""

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

# Injected into system prompt
messages = [{
    "role": "system",
    "content": prompt_template["system_template"].format(
        categories_list=categories_text
    ),
}]
```

**Key difference**: ✨ **XML-style formatting with examples**

---

### 5.3 Category Validation

| ETL | Category Validation | Rejection Mechanism |
|-----|---------------------|---------------------|
| Call Summary | Research plan pre-filters | Categories not in plan marked `rejected=True` |
| Key Themes | Per-Q&A validation | Q&As marked `is_valid=False`, filtered from output |
| CM Readthrough | Per-bank content check | Banks without content return `has_content=False` |

---

## 6. 🤖 LLM USAGE PATTERNS

### 6.1 LLM Call Count Per Run

| ETL | Phase | LLM Calls | Model Tier |
|-----|-------|-----------|------------|
| **Call Summary** | Research plan | 1 | Large |
| | Category extraction | N (sequential) | Medium |
| | **Total** | **1 + N** | Mixed |
| **Key Themes** | Theme extraction | N (sequential) | Medium |
| | HTML formatting | N (parallel) | Medium |
| | Comprehensive regrouping | 1 | Medium |
| | **Total** | **2N + 1** | Medium only |
| **CM Readthrough** | Outlook extraction | M banks (concurrent) | Large |
| | Section 2 extraction | M banks (concurrent) | Large |
| | Section 3 extraction | M banks (concurrent) | Large |
| | Subtitle generation | 3 (parallel) | Large |
| | Batch formatting | ~~1 (disabled)~~ | ~~Large~~ |
| | **Total** | **3M + 3** | Large only |

Where:
- N = Number of categories/Q&As (typically 5-15)
- M = Number of monitored banks (typically 10-20)

---

### 6.2 Prompt Loading Strategy

#### All Three ETLs (Aligned)
```python
# Load prompt from database
prompt_data = load_prompt_from_db(
    layer="call_summary_etl" | "key_themes_etl" | "cm_readthrough_etl",
    name="prompt_name",
    compose_with_globals=False,  # ETLs don't use global contexts
    available_databases=None,
    execution_id=execution_id,
)

# Extract components
system_prompt = prompt_data["system_prompt"]
user_prompt = prompt_data.get("user_prompt", "")
tool_definition = prompt_data.get("tool_definition", {})
```

**Status**: ✅ **IDENTICAL** pattern across all three ETLs

---

### 6.3 Tool Calling vs Completion

#### Call Summary
```python
# Research plan: Tool calling
response = await complete_with_tools(
    messages=messages,
    tools=[research_prompts["tool_definition"]],
    context=context,
    llm_params={"model": etl_config.get_model("research_plan"), ...},
)
research_plan_data = json.loads(tool_call["function"]["arguments"])

# Category extraction: Tool calling
response = await complete_with_tools(
    messages=messages,
    tools=[extraction_prompts["tool_definition"]],
    context=context,
    llm_params={"model": etl_config.get_model("category_extraction"), ...},
)
extracted_data = json.loads(tool_call["function"]["arguments"])
```

**Pattern**: ✅ **100% tool calling** (structured JSON outputs)

---

#### Key Themes
```python
# Theme extraction: Tool calling
response = await complete_with_tools(
    messages=messages,
    tools=[prompt_data["tool_definition"]],
    context=context,
    llm_params={"model": etl_config.get_model("theme_extraction"), ...},
)
result = json.loads(tool_calls[0]["function"]["arguments"])

# HTML formatting: Standard completion (no tools)
response = await complete(
    messages,
    context,
    {"model": etl_config.get_model("formatting"), ...},
)
qa_block.formatted_content = response["choices"][0]["message"]["content"]

# Comprehensive regrouping: Tool calling
response = await complete_with_tools(
    messages=messages,
    tools=[prompt_data["tool_definition"]],
    context=context,
    llm_params={"model": etl_config.get_model("grouping"), ...},
)
result = json.loads(tool_calls[0]["function"]["arguments"])
```

**Pattern**: ⚠️ **Mixed** - Tool calling for structured data, completion for HTML

---

#### CM Readthrough
```python
# Outlook extraction: Tool calling
response = await complete_with_tools(
    messages=messages,
    tools=tools,
    context=context,
    llm_params={"model": etl_config.get_model("outlook_extraction"), ...},
)
result = json.loads(tool_call["function"]["arguments"])

# Q&A extraction: Tool calling
response = await complete_with_tools(
    messages=messages,
    tools=tools,
    context=context,
    llm_params={"model": etl_config.get_model("qa_extraction"), ...},
)
result = json.loads(tool_call["function"]["arguments"])

# Subtitle generation: Tool calling
response = await complete_with_tools(
    messages=messages,
    tools=tools,
    context=context,
    llm_params={"model": etl_config.get_model("subtitle_generation"),
                "tool_choice": "required"},  # Force tool use
)
subtitle = result.get("subtitle", default_subtitle)

# Batch formatting: Tool calling (DISABLED)
# response = await complete_with_tools(...)  # Currently commented out
```

**Pattern**: ✅ **100% tool calling** (all structured JSON outputs)

---

### 6.4 Retry Logic

#### All Three ETLs (Aligned)
```python
max_retries = 3
for attempt in range(max_retries):
    try:
        response = await complete_with_tools(...)
        tool_call = response["choices"][0]["message"]["tool_calls"][0]
        result = json.loads(tool_call["function"]["arguments"])
        break  # Success

    except (KeyError, IndexError, json.JSONDecodeError, TypeError) as e:
        logger.error(f"Error on attempt {attempt + 1}: {e}")
        if attempt < max_retries - 1:
            continue  # Retry
        raise RuntimeError(f"Failed after {max_retries} attempts") from e
```

**Status**: ✅ **IDENTICAL** retry pattern (3 attempts with logging)

---

## 7. 📄 OUTPUT DIFFERENCES

### 7.1 Document Structure

#### Call Summary
**Format**: Portrait orientation, multi-section document

```
┌─────────────────────────────────────┐
│ [Banner Image]                      │
│                                     │
│ Q3/24 Results and Call Summary - RY│
│                                     │
│ Contents                            │
│   Results Summary .............. 3  │
│     Category 1 ................ 3  │
│     Category 2 ................ 4  │
│   Strategic Initiatives ........ 5  │
│     Category 3 ................ 5  │
│                                     │
├─────────────────────────────────────┤
│ ## Results Summary                  │
│                                     │
│ ### Category 1: Financial Results   │
│ • Statement 1 with **bold**         │
│   → Quotes: Q1: "excerpt..." |      │
│              Q2: "excerpt..."       │
│   "Full quote from CEO"             │
│   — John Smith, CEO                 │
│                                     │
│ • Statement 2 with __underline__    │
│   → Quotes: Q1: "excerpt..."        │
│   "Full quote from CFO"             │
│   — Jane Doe, CFO                   │
│                                     │
│ ### Category 2: Credit Quality      │
│ ...                                 │
└─────────────────────────────────────┘

Footer: [Page #]
```

**Key features**:
- ✅ Table of contents with page numbers
- ✅ Banner image
- ✅ Hierarchical sections (report_section groups categories)
- ✅ Bullet points with evidence quotes
- ✅ Speaker attribution
- ✅ Markdown formatting (**bold**, __underline__)

---

#### Key Themes
**Format**: Portrait orientation, themed Q&A document

```
┌─────────────────────────────────────┐
│ [Banner Image]                      │
│                                     │
├─────────────────────────────────────┤
│ Theme 1: Capital Markets Outlook    │
│ [Light blue background, dark blue   │
│  text, bold]                        │
│                                     │
│ Conversation 1:                     │
│ [Underlined]                        │
│                                     │
│    Analyst Name (Firm Name):        │
│    <b>Question with <u>emphasis</u> │
│    and <mark>highlights</mark></b>  │
│                                     │
│    Executive Name (Bank Title):     │
│    Answer with <b>bold</b> and      │
│    <i>italic</i> HTML tags          │
│                                     │
│    _____________________________    │
│                                     │
│ Conversation 2:                     │
│    ...                              │
│                                     │
├─────────────────────────────────────┤
│ Theme 2: Credit Quality             │
│ ...                                 │
└─────────────────────────────────────┘

Footer: RY | Q3/24 | Investor Call - Key Themes | Page #
```

**Key features**:
- ✅ Theme headers with colored background
- ✅ Conversation numbering within themes
- ✅ HTML formatting (<b>, <i>, <u>, <mark>)
- ✅ Separator lines between conversations
- ✅ Custom footer with bank/quarter/title

---

#### CM Readthrough
**Format**: Landscape orientation, 3-section table document

```
┌──────────────────────────────────────────────────────────────────┐
│ Read Through For Capital Markets: Q3/24 Select U.S. & European Banks│
│ Outlook: Capital markets activity across major institutions     │
│                                                                  │
├────────┬─────────────────────────────────────────────────────────┤
│Banks/  │ Investment Banking and Trading Outlook                 │
│Segments│                                                        │
├────────┼─────────────────────────────────────────────────────────┤
│  BAC   │ M&A:                                                   │
│        │ "Strong pipeline with <strong><u>key deals</u></strong>│
│        │  in progress"                                          │
│        │ Trading:                                               │
│        │ "Volatility driving <strong><u>record             │
│        │  volumes</u></strong> in FX"                           │
├────────┼─────────────────────────────────────────────────────────┤
│  JPM   │ M&A:                                                   │
│        │ "Backlog remains elevated..."                          │
├────────┼─────────────────────────────────────────────────────────┤
│  GS    │ ...                                                    │
└────────┴─────────────────────────────────────────────────────────┘

[PAGE BREAK]

┌──────────────────────────────────────────────────────────────────┐
│ Read Through For Capital Markets: Q3/24 Select U.S. & European Banks│
│ Conference calls: Market volatility and regulatory dynamics     │
│                                                                  │
├──────────┬──────┬──────────────────────────────────────────────┤
│ Themes   │Banks │ Relevant Questions                           │
├──────────┼──────┼──────────────────────────────────────────────┤
│ Global   │ BAC  │ • How is volatility impacting trading desk? │
│ Markets  │      │ • Are you seeing sustained client activity?  │
│          ├──────┼──────────────────────────────────────────────┤
│          │ JPM  │ • What's driving the strength in FICC?      │
│          ├──────┼──────────────────────────────────────────────┤
│          │ GS   │ • How long can elevated volumes persist?    │
├──────────┼──────┼──────────────────────────────────────────────┤
│ Risk Mgmt│ BAC  │ • How are you managing counterparty risk?   │
│          ├──────┼──────────────────────────────────────────────┤
│          │ JPM  │ • VaR trending higher - is this concern?    │
└──────────┴──────┴──────────────────────────────────────────────┘

[PAGE BREAK]

┌──────────────────────────────────────────────────────────────────┐
│ Read Through For Capital Markets: Q3/24 Select U.S. & European Banks│
│ Conference calls: Pipeline strength and M&A activity            │
│                                                                  │
├──────────┬──────┬──────────────────────────────────────────────┤
│ Themes   │Banks │ Relevant Questions                           │
├──────────┼──────┼──────────────────────────────────────────────┤
│ IB/M&A   │ BAC  │ • How is the M&A pipeline looking?          │
│          ├──────┼──────────────────────────────────────────────┤
│          │ GS   │ • When do you expect deal closings?         │
└──────────┴──────┴──────────────────────────────────────────────┘

Footer: Source: Company Reports, Transcripts | RBC
```

**Key features**:
- ✅ Landscape orientation
- ✅ 3 separate sections (each on new page)
- ✅ Section 1: 2-column table (ticker + outlook)
- ✅ Sections 2-3: 3-column table (theme + bank + questions)
- ✅ Theme grouping with merged cells
- ✅ Dynamic subtitles per section
- ✅ Custom footer

---

### 7.2 Document Formatting Code

#### Call Summary
**File**: `document_converter.py` (353 lines)

**Key functions**:
```python
def setup_document_formatting(doc) -> None:
    """Configure margins and page numbers."""

def add_banner_image(doc, config_dir: str) -> None:
    """Add banner from config directory."""

def add_document_title(doc, quarter: str, fiscal_year: int, bank_symbol: str) -> None:
    """Add formatted title."""

def add_table_of_contents(doc) -> None:
    """Add real TOC field with styles."""

def add_section_heading(doc, section_name: str, is_first_section: bool) -> None:
    """Add section heading with page break control."""

def add_structured_content_to_doc(doc, category_data: dict, heading_level: int) -> None:
    """Add category with statements and evidence quotes."""

def parse_and_format_text(paragraph, content: str, ...) -> None:
    """Parse markdown (**bold**, __underline__) into Word runs."""
```

---

#### Key Themes
**File**: `document_converter.py` (287 lines)

**Key functions**:
```python
class HTMLToDocx(HTMLParser):
    """Convert HTML tags to Word formatting."""
    # Supports: <b>, <strong>, <i>, <em>, <u>, <mark>, <span style="...">

def add_page_numbers_with_footer(doc, bank_symbol, quarter, fiscal_year):
    """Custom footer with 2-column table."""

def add_theme_header_with_background(doc, theme_number, theme_title):
    """Theme header with light blue background."""

def theme_groups_to_markdown(theme_groups, bank_info, quarter, fiscal_year) -> str:
    """Convert themes to markdown (alternative output format)."""
```

---

#### CM Readthrough
**File**: `document_converter.py` (666 lines)

**Key functions**:
```python
def create_combined_document(results: Dict[str, Any], output_path: str) -> None:
    """Create 3-section landscape document."""

def add_section1_outlook(doc: Document, results: Dict[str, Any]) -> None:
    """2-column table: Banks | Outlook."""
    # Outlook statements grouped by category per bank

def add_section2_qa(doc: Document, results: Dict[str, Any]) -> None:
    """3-column table: Themes | Banks | Questions."""
    # Questions grouped by theme, then by bank

def add_section3_qa(doc: Document, results: Dict[str, Any]) -> None:
    """3-column table: Themes | Banks | Questions."""
    # Questions grouped by theme, then by bank

def add_page_footer(section) -> None:
    """Footer with horizontal line and table layout."""

def _add_formatted_runs(paragraph, text: str, font_size: int) -> None:
    """Process HTML tags: <strong><u>text</u></strong>."""
```

**Unique features**:
- ✨ Landscape orientation
- ✨ Multi-section document (3 pages)
- ✨ Theme-first sorting (categories group banks)
- ✨ Merged cells for theme spanning multiple banks

---

### 7.3 Output File Naming

#### Call Summary
```python
content_hash = hashlib.md5(
    f"{bank_info['bank_id']}_{fiscal_year}_{quarter}_{datetime.now().isoformat()}".encode()
).hexdigest()[:8]

filename = f"{bank_info['bank_symbol']}_{fiscal_year}_{quarter}_{content_hash}.docx"
# Example: RY_2024_Q3_a1b2c3d4.docx
```

---

#### Key Themes
```python
content_hash = hashlib.md5(
    f"{bank_info['bank_id']}_{fiscal_year}_{quarter}_{datetime.now().isoformat()}".encode()
).hexdigest()[:8]

filename = f"{bank_info['bank_symbol']}_{fiscal_year}_{quarter}_{content_hash}.docx"
# Example: RY_2024_Q3_e5f6g7h8.docx
```

**Status**: ✅ **IDENTICAL** to Call Summary

---

#### CM Readthrough
```python
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
content_hash = hashlib.md5(f"{fiscal_year}_{quarter}_{timestamp}".encode()).hexdigest()[:8]

filename = f"CM_Readthrough_{fiscal_year}_{quarter}_{content_hash}.docx"
# Example: CM_Readthrough_2024_Q3_i9j0k1l2.docx
```

**Key difference**: ⚠️ **No bank symbol** (cross-bank report)

---

## 8. 💾 DATABASE INTERACTIONS

### 8.1 Report Storage

#### Call Summary
```python
async def _save_to_database(
    category_results: list,
    valid_categories: list,
    filepath: str,
    docx_filename: str,
    etl_context: dict,
) -> None:
    """Save report metadata to aegis_reports table."""

    # Delete existing report for same bank/period/type
    await conn.execute(
        text("""
            DELETE FROM aegis_reports
            WHERE bank_id = :bank_id
              AND fiscal_year = :fiscal_year
              AND quarter = :quarter
              AND report_type = :report_type
        """),
        {"bank_id": bank_info["bank_id"], ...}
    )

    # Insert new report
    await conn.execute(
        text("""
            INSERT INTO aegis_reports (
                report_name, report_description, report_type,
                bank_id, bank_name, bank_symbol,
                fiscal_year, quarter,
                local_filepath, s3_document_name,
                generation_date, generated_by, execution_id,
                metadata
            ) VALUES (...)
        """),
        {
            "bank_id": bank_info["bank_id"],
            "bank_name": bank_info["bank_name"],
            "bank_symbol": bank_info["bank_symbol"],
            "report_type": "call_summary",
            "generated_by": "call_summary_etl",
            "metadata": json.dumps({
                "bank_type": bank_type,
                "categories_processed": len(category_results),
                "categories_included": len(valid_categories),
                "categories_rejected": len(category_results) - len(valid_categories),
            }),
            ...
        }
    )
```

**Key characteristics**:
- ✅ Bank-specific report (bank_id, bank_name, bank_symbol populated)
- ✅ Metadata includes category statistics
- ✅ report_type = `"call_summary"`

---

#### Key Themes
```python
# Similar structure to Call Summary
async with get_connection() as conn:
    # Delete existing
    await conn.execute(
        text("""
            DELETE FROM aegis_reports
            WHERE bank_id = :bank_id
              AND fiscal_year = :fiscal_year
              AND quarter = :quarter
              AND report_type = :report_type
        """),
        {"bank_id": bank_info["bank_id"], "report_type": "key_themes", ...}
    )

    # Insert new
    await conn.execute(
        text("""INSERT INTO aegis_reports (...) VALUES (...)"""),
        {
            "bank_id": bank_info["bank_id"],
            "report_type": "key_themes",
            "generated_by": "key_themes_etl",
            "metadata": json.dumps({
                "theme_groups": len(theme_groups),
                "total_qa_blocks": sum(len(group.qa_blocks) for group in theme_groups),
                "invalid_qa_filtered": sum(1 for qa in qa_index.values() if not qa.is_valid),
            }),
            ...
        }
    )

    # Update data availability to include "reports"
    await conn.execute(
        text("""
            UPDATE aegis_data_availability
            SET database_names =
                CASE WHEN 'reports' = ANY(database_names)
                     THEN database_names
                     ELSE array_append(database_names, 'reports')
                END
            WHERE bank_id = :bank_id
              AND fiscal_year = :fiscal_year
              AND quarter = :quarter
        """),
        {"bank_id": bank_info["bank_id"], ...}
    )
```

**Key characteristics**:
- ✅ Bank-specific report
- ✅ Metadata includes theme statistics
- ✅ report_type = `"key_themes"`
- ✨ **Updates aegis_data_availability** to add "reports" to database_names

---

#### CM Readthrough
```python
async def save_to_database(
    results: Dict[str, Any],
    fiscal_year: int,
    quarter: str,
    execution_id: str,
    local_filepath: str = None,
    s3_document_name: str = None,
) -> None:
    """Save cross-bank report to database."""

    # Delete existing report for same period/type (no bank filter)
    await conn.execute(
        text("""
            DELETE FROM aegis_reports
            WHERE fiscal_year = :fiscal_year
              AND quarter = :quarter
              AND report_type = :report_type
        """),
        {"fiscal_year": fiscal_year, "quarter": quarter, "report_type": "cm_readthrough"}
    )

    # Insert new report
    await conn.execute(
        text("""INSERT INTO aegis_reports (...) VALUES (...)"""),
        {
            "bank_id": None,           # ⚠️ NULL - cross-bank report
            "bank_name": None,         # ⚠️ NULL - cross-bank report
            "bank_symbol": None,       # ⚠️ NULL - cross-bank report
            "report_type": "cm_readthrough",
            "generated_by": "cm_readthrough_etl",
            "metadata": json.dumps(results),  # ✨ Full results stored
            ...
        }
    )
```

**Key characteristics**:
- ⚠️ **Cross-bank report** (bank_id, bank_name, bank_symbol all NULL)
- ✨ **Full results in metadata** (entire aggregated data structure)
- ✅ report_type = `"cm_readthrough"`
- ⚠️ **No aegis_data_availability update**

---

### 8.2 Database Schema Alignment

#### aegis_reports Table

| Column | Call Summary | Key Themes | CM Readthrough |
|--------|--------------|-----------|----------------|
| report_name | "Earnings Call Summary" | "Key Themes Analysis" | "Capital Markets Readthrough" |
| report_description | (standard text) | (standard text) | (standard text) |
| report_type | `"call_summary"` | `"key_themes"` | `"cm_readthrough"` |
| bank_id | ✅ Populated | ✅ Populated | ⚠️ **NULL** |
| bank_name | ✅ Populated | ✅ Populated | ⚠️ **NULL** |
| bank_symbol | ✅ Populated | ✅ Populated | ⚠️ **NULL** |
| fiscal_year | ✅ | ✅ | ✅ |
| quarter | ✅ | ✅ | ✅ |
| local_filepath | ✅ | ✅ | ✅ |
| s3_document_name | ✅ | ✅ | ✅ |
| generation_date | ✅ | ✅ | ✅ |
| generated_by | `"call_summary_etl"` | `"key_themes_etl"` | `"cm_readthrough_etl"` |
| execution_id | ✅ | ✅ | ✅ |
| metadata | Category stats (JSON) | Theme stats (JSON) | **Full results** (JSON) |

---

### 8.3 Data Availability Updates

| ETL | Updates aegis_data_availability? | Logic |
|-----|----------------------------------|-------|
| Call Summary | ❌ No | Assumes "transcripts" already present |
| Key Themes | ✅ Yes | Adds "reports" to database_names array |
| CM Readthrough | ❌ No | Cross-bank report, no per-bank tracking |

**Key Themes update logic**:
```python
# Add "reports" to database_names if not already present
await conn.execute(
    text("""
        UPDATE aegis_data_availability
        SET database_names =
            CASE WHEN 'reports' = ANY(database_names)
                 THEN database_names
                 ELSE array_append(database_names, 'reports')
            END
        WHERE bank_id = :bank_id
          AND fiscal_year = :fiscal_year
          AND quarter = :quarter
          AND NOT ('reports' = ANY(database_names))
    """)
)

# On report deletion, check if should remove "reports"
if deleted_rows and count_result == 0:
    await conn.execute(
        text("""
            UPDATE aegis_data_availability
            SET database_names = array_remove(database_names, 'reports')
            WHERE bank_id = :bank_id
              AND fiscal_year = :fiscal_year
              AND quarter = :quarter
              AND 'reports' = ANY(database_names)
        """)
    )
```

---

## 9. 🔍 UNIQUE FEATURES

### 9.1 Call Summary Unique Features

#### Research Planning Phase
```python
async def _generate_research_plan(
    context: dict, research_prompts: dict, transcript_text: str, execution_id: str
) -> dict:
    """Generate research plan using LLM before category extraction."""

    # LLM analyzes all categories upfront
    # Returns: {"category_plans": [{"index": 1, "extraction_strategy": "...", ...}, ...]}

    # Categories not in plan are automatically rejected
```

**Why it's unique**: Only Call Summary pre-analyzes all categories to determine relevance.

---

#### Cumulative Context for Categories
```python
def _build_extracted_themes(category_results: list) -> str:
    """Build summary of previously extracted themes."""
    # Shown to LLM for later categories

def _build_previous_summary(category_results: list) -> str:
    """Build summary of completed categories."""
    # Prevents duplication across categories
```

**Why it's unique**: Later categories see prior results to avoid duplication and maintain consistency.

---

#### Bank-Type-Specific Categories
```python
bank_type = get_bank_type(bank_info["bank_id"])  # "Canadian_Banks" | "US_Banks"
categories = load_categories_from_xlsx(bank_type, execution_id)
```

**Why it's unique**: Only Call Summary adapts categories based on bank type.

---

### 9.2 Key Themes Unique Features

#### Sequential Classification with Cumulative Context
```python
async def classify_all_qa_blocks_sequential(
    qa_index: Dict[str, QABlock],
    categories: List[Dict[str, str]],
    context: Dict[str, Any],
):
    """Classify all Q&A blocks sequentially with cumulative context."""

    previous_classifications = []

    for qa_block in sorted_qa_blocks:
        await classify_qa_block(qa_block, categories, previous_classifications, context)

        if qa_block.is_valid:
            previous_classifications.append({
                "qa_id": qa_block.qa_id,
                "category_name": qa_block.category_name,
                "summary": qa_block.summary,
            })
```

**Why it's unique**: Only Key Themes uses sequential classification with cumulative awareness.

---

#### Comprehensive Regrouping Phase
```python
async def determine_comprehensive_grouping(
    qa_index: Dict[str, QABlock], categories: List[Dict[str, str]], context: Dict[str, Any]
) -> List[ThemeGroup]:
    """Make ONE comprehensive grouping decision for all themes."""

    # LLM sees ALL Q&A classifications at once
    # Can regroup Q&As that were initially assigned different categories
    # Creates final theme titles that span multiple initial categories
```

**Why it's unique**: Only Key Themes has a "regrouping" phase that reconsiders initial classifications globally.

---

#### HTML Formatting with Parallel Execution
```python
async def format_all_qa_blocks_parallel(qa_index: Dict[str, QABlock], context: Dict[str, Any]):
    """Format all valid Q&A blocks in parallel with HTML tags."""

    valid_qa_blocks = [qa for qa in qa_index.values() if qa.is_valid]

    # Parallel formatting - all Q&As formatted simultaneously
    tasks = [format_qa_html(qa_block, context) for qa_block in valid_qa_blocks]
    await asyncio.gather(*tasks)
```

**Why it's unique**: Only Key Themes uses parallel LLM calls for formatting.

---

#### QABlock and ThemeGroup Classes
```python
class QABlock:
    """Represents a single Q&A block with its extracted information."""
    def __init__(self, qa_id: str, position: int, original_content: str)
    # Fields: category_name, summary, formatted_content, assigned_group, is_valid

class ThemeGroup:
    """Represents a group of related Q&A blocks under a unified theme."""
    def __init__(self, group_title: str, qa_ids: List[str], rationale: str = "")
    # Fields: group_title, qa_ids, rationale, qa_blocks
```

**Why it's unique**: Only Key Themes uses object-oriented data structures for Q&A management.

---

### 9.3 CM Readthrough Unique Features

#### Multi-Bank Concurrent Processing
```python
semaphore = asyncio.Semaphore(5)  # Max 5 concurrent banks

async def process_bank_outlook(bank_data):
    async with semaphore:
        # Process outlook for this bank independently
        ...

# Launch ALL banks concurrently with semaphore control
outlook_tasks = [process_bank_outlook(bank) for bank in monitored_banks]
bank_outlook = await asyncio.gather(*outlook_tasks, return_exceptions=True)
```

**Why it's unique**: Only CM Readthrough processes multiple banks simultaneously.

---

#### Latest Quarter Fallback
```python
async def find_latest_available_quarter(
    bank_id: int, min_fiscal_year: int, min_quarter: str, bank_name: str = ""
) -> Optional[Tuple[int, str]]:
    """Find the latest available quarter for a bank, at or after the minimum specified."""

    # If Q3 2024 requested but bank only has Q2 2024, use Q2 2024
    # Logs when using more recent data than requested
```

**Why it's unique**: Only CM Readthrough can gracefully use newer/older quarters if exact match unavailable.

---

#### Monitored Institutions Configuration
```python
def _load_monitored_institutions() -> Dict[str, Dict[str, Any]]:
    """Load and cache monitored institutions from YAML."""
    # Returns: {"RY": {"id": 1, "name": "Royal Bank of Canada", "type": "Canadian_Banks"}}

def get_monitored_institutions() -> List[Dict[str, Any]]:
    """Get list of all monitored institutions for processing."""
```

**Why it's unique**: Only CM Readthrough has a configuration file listing all banks to process.

---

#### Three-Section Output Structure
```python
results = {
    "metadata": {
        "fiscal_year": int,
        "quarter": str,
        "banks_processed": int,
        "banks_with_outlook": int,
        "banks_with_section2": int,
        "banks_with_section3": int,
        "subtitle_section1": str,
        "subtitle_section2": str,
        "subtitle_section3": str,
        ...
    },
    "outlook": {  # Section 1: Outlook statements
        "Bank Name": {
            "bank_symbol": str,
            "statements": [{"category": str, "statement": str, "is_new_category": bool}]
        }
    },
    "section2_questions": {  # Section 2: Market volatility/regulatory Q&A
        "Bank Name": {
            "bank_symbol": str,
            "questions": [{"category": str, "verbatim_question": str, "analyst_name": str, ...}]
        }
    },
    "section3_questions": {  # Section 3: Pipelines/activity Q&A
        "Bank Name": {
            "bank_symbol": str,
            "questions": [{"category": str, "verbatim_question": str, "analyst_name": str, ...}]
        }
    },
}
```

**Why it's unique**: Only CM Readthrough generates a 3-section document with different category systems per section.

---

#### Subtitle Generation Phase
```python
async def generate_subtitle(
    content_data: Dict[str, Any],
    content_type: str,
    section_context: str,
    default_subtitle: str,
    context: Dict[str, Any],
) -> str:
    """Universal subtitle generation for any section."""

    # LLM synthesizes content across ALL banks to create descriptive subtitle
    # Used for section 1 (outlook), section 2 (market vol), section 3 (pipelines)
```

**Why it's unique**: Only CM Readthrough auto-generates subtitles based on aggregated content.

---

#### Batch Formatting (Currently Disabled)
```python
async def format_outlook_batch(
    all_outlook: Dict[str, Any], context: Dict[str, Any]
) -> Dict[str, Any]:
    """Single LLM call to format all outlook statements with HTML emphasis."""

    # Would format ALL bank outlook statements in ONE LLM call
    # Currently disabled: formatted_outlook = all_outlook (unformatted)
```

**Why it's unique**: Only CM Readthrough attempts batch formatting (though currently disabled for performance).

---

## 10. 📋 RECOMMENDATIONS

### 10.1 High Priority Standardization

#### 1. Transcript Utils Alignment
**Issue**: Key Themes has simplified transcript_utils without `format_full_section_chunks()`.

**Recommendation**: ✅ **Align all three ETLs** to use identical transcript utility functions.

**Action**:
```bash
# Copy call_summary/transcript_utils.py to key_themes/
cp src/aegis/etls/call_summary/transcript_utils.py \
   src/aegis/etls/key_themes/transcript_utils.py
```

---

#### 2. ETLConfig Consolidation
**Issue**: ETLConfig class duplicated across all three ETLs (identical code).

**Recommendation**: ✅ **Extract to shared module** `aegis.etls.common.config`.

**Action**:
```python
# Create: src/aegis/etls/common/config.py
from aegis.etls.common.config import ETLConfig

etl_config = ETLConfig(os.path.join(os.path.dirname(__file__), "config", "config.yaml"))
```

---

#### 3. Monitored Institutions Alignment
**Issue**: Call Summary and CM Readthrough both use `monitored_institutions.yaml` but with different structures.

**Call Summary**:
```yaml
# Indexed by bank_id
1:
  name: "Royal Bank of Canada"
  type: "Canadian_Banks"
  path_safe_name: "royal_bank_of_canada"
```

**CM Readthrough**:
```yaml
# Indexed by ticker
RY:
  id: 1
  name: "Royal Bank of Canada"
  type: "Canadian_Banks"
  path_safe_name: "royal_bank_of_canada"
```

**Recommendation**: ✅ **Standardize on CM Readthrough structure** (ticker-indexed) and consolidate.

**Action**:
```python
# Create: src/aegis/etls/common/institutions.py
def load_monitored_institutions() -> Dict[str, Dict[str, Any]]:
    """Load universal institution configuration."""

# Use in all three ETLs
from aegis.etls.common.institutions import load_monitored_institutions
```

---

#### 4. get_bank_info() Consolidation
**Issue**: Identical function duplicated in all three ETLs.

**Recommendation**: ✅ **Extract to shared module** `aegis.etls.common.bank_lookup`.

**Action**:
```python
# Create: src/aegis/etls/common/bank_lookup.py
async def get_bank_info(bank_name: str) -> Dict[str, Any]:
    """Universal bank lookup from aegis_data_availability."""

# Use in all three ETLs
from aegis.etls.common.bank_lookup import get_bank_info
```

---

#### 5. verify_data_availability() Consolidation
**Issue**: Identical function in Call Summary and Key Themes; not used in CM Readthrough.

**Recommendation**: ✅ **Extract to shared module** and use in all ETLs.

**Action**:
```python
# Add to: src/aegis/etls/common/bank_lookup.py
async def verify_data_availability(bank_id: int, fiscal_year: int, quarter: str) -> bool:
    """Universal data availability check."""

# Use in all three ETLs (CM Readthrough should use this instead of custom logic)
from aegis.etls.common.bank_lookup import verify_data_availability
```

---

### 10.2 Medium Priority Improvements

#### 6. Document Metadata Alignment
**Issue**: `get_standard_report_metadata()` exists in all three but returns different structures.

**Recommendation**: ⚠️ **Keep separate** but ensure consistent return schema.

**Action**: Document expected schema and add validation.

---

#### 7. Error Handling Consolidation
**Issue**: Identical try-except pattern in all three ETLs.

**Recommendation**: ✅ **Extract to decorator** or context manager.

**Action**:
```python
# Create: src/aegis/etls/common/error_handling.py
def etl_error_handler(etl_name: str):
    """Decorator for consistent ETL error handling."""

# Use in all three ETLs
@etl_error_handler("call_summary")
async def generate_call_summary(...):
    ...
```

---

#### 8. LLM Retry Logic Consolidation
**Issue**: Identical retry logic in all three ETLs.

**Recommendation**: ✅ **Extract to utility function**.

**Action**:
```python
# Create: src/aegis/etls/common/llm_utils.py
async def retry_llm_call(llm_function, max_retries=3, **kwargs):
    """Universal LLM retry wrapper."""

# Use in all three ETLs
result = await retry_llm_call(
    complete_with_tools,
    messages=messages,
    tools=tools,
    context=context,
)
```

---

#### 9. Database Availability Updates
**Issue**: Only Key Themes updates `aegis_data_availability` with "reports".

**Recommendation**: ✅ **All ETLs should update** or ⚠️ **none should update**.

**Action**: Decide on standard practice and implement consistently.

---

#### 10. Configuration Temperature Alignment
**Issue**: ~~CM Readthrough uses temperature=0.7, others use 0.1.~~

**Status**: ✅ **RESOLVED** - CM Readthrough now aligned to temperature=0.1 and max_tokens=32768.

**Action**: ✅ **COMPLETED** - All three ETLs now use identical LLM parameters.

---

### 10.3 Low Priority Enhancements

#### 11. Category Loading Abstraction
**Issue**: Three different category loading patterns.

**Recommendation**: ⚠️ **Keep separate** - ETL-specific requirements differ significantly.

---

#### 12. Output Format Documentation
**Issue**: Document structures vary significantly between ETLs.

**Recommendation**: ✅ **Create visual documentation** showing output examples.

**Action**: Generate sample documents for all three ETLs and document structure.

---

#### 13. Monitoring Integration
**Issue**: All three ETLs generate execution_id but monitoring integration unclear.

**Recommendation**: ✅ **Add consistent monitoring** across all ETLs.

**Action**: Ensure all LLM calls are tracked with execution_id in monitoring database.

---

## 11. 🎯 CONCLUSION

### Summary of Alignment

| Component | Status | Notes |
|-----------|--------|-------|
| ETLConfig class | ✅ Identical | Should extract to shared module |
| get_bank_info() | ✅ Identical | Should extract to shared module |
| verify_data_availability() | ✅ Identical (2/3) | CM Readthrough uses different approach |
| Transcript utils | ⚠️ Mostly aligned | Key Themes missing format function |
| Error handling | ✅ Identical pattern | Should extract to decorator |
| LLM retry logic | ✅ Identical | Should extract to utility function |
| Configuration structure | ✅ Fully aligned | Temperature/max_tokens now identical |
| Processing pipeline | ❌ Different | Intentionally different architectures |
| Category systems | ❌ Different | ETL-specific requirements |
| Output formats | ❌ Different | ETL-specific requirements |
| Database storage | ⚠️ Mostly aligned | CM Readthrough is cross-bank |

### Key Takeaways

1. **✅ Foundation is solid**: Core infrastructure (auth, SSL, context, error handling) is consistent across all three ETLs.

2. **⚠️ Architectural divergence is intentional**: Each ETL has a different processing model suited to its purpose:
   - Call Summary: Sequential with research planning
   - Key Themes: Hybrid sequential-parallel with regrouping
   - CM Readthrough: Fully concurrent multi-bank

3. **✅ Opportunities for consolidation**: 5-6 key functions are duplicated and can be extracted to shared modules.

4. **✅ Configuration now fully aligned**: Temperature (0.1) and max_tokens (32768) standardized across all ETLs.

5. **✅ Output differences are by design**: Each ETL generates a different document format for different use cases.

### Next Steps

1. **Immediate**: Extract duplicated code (ETLConfig, get_bank_info, etc.) to `aegis.etls.common/` module.
2. **Short-term**: Align transcript utils across all three ETLs.
3. **Medium-term**: Standardize monitoring integration and database availability updates.
4. **Long-term**: Consider creating base ETL class with shared methods while preserving unique workflows.

---

**End of Comprehensive ETL Comparison**
