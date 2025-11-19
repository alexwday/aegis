# Call Summary ETL Refactoring Guide
## Applying These Changes to Key Themes ETL

This document explains all the refactoring work done on the Call Summary ETL and how to apply the same patterns to the Key Themes ETL.

---

## Table of Contents
1. [Configuration Management](#1-configuration-management)
2. [Directory Structure](#2-directory-structure)
3. [Prompt Loading System](#3-prompt-loading-system)
4. [Document Generation](#4-document-generation)
5. [Function Decomposition](#5-function-decomposition)
6. [Database Operations](#6-database-operations)
7. [Error Handling](#7-error-handling)
8. [Code Quality](#8-code-quality)

---

## 1. Configuration Management

### What Changed in Call Summary

**Before:**
```python
# config/config.py - Python file with environment variable fallbacks
MODELS = {
    "summarization": os.getenv("CALL_SUMMARY_MODEL", "gpt-4o"),
    "research_plan": os.getenv("CALL_SUMMARY_RESEARCH_MODEL", "gpt-4o"),
    "category_extraction": os.getenv("CALL_SUMMARY_EXTRACTION_MODEL", "gpt-4o"),
}
TEMPERATURE = float(os.getenv("CALL_SUMMARY_TEMPERATURE", "0.7"))
MAX_TOKENS = int(os.getenv("CALL_SUMMARY_MAX_TOKENS", "32768"))
```

**After:**
```yaml
# config/config.yaml - Clean YAML configuration
models:
  research_plan:
    tier: large  # References config.llm.large.model
  category_extraction:
    tier: medium  # References config.llm.medium.model

llm:
  temperature: 0.1
  max_tokens: 32768
```

**New centralized config loader:**
```python
# src/aegis/etls/config_loader.py
from aegis.etls.config_loader import ETLConfig

etl_config = ETLConfig(os.path.join(os.path.dirname(__file__), "config", "config.yaml"))

# Usage throughout ETL:
model = etl_config.get_model("research_plan")  # Returns config.llm.large.model
temp = etl_config.temperature
max_tokens = etl_config.max_tokens
```

### Why This Matters
- **Consistency**: All ETLs use the same configuration pattern
- **Centralization**: Model tier mappings in one place
- **Type Safety**: YAML validation catches errors early
- **No Environment Pollution**: Removes ETL-specific environment variables

### How to Apply to Key Themes

1. **Delete** `src/aegis/etls/key_themes/config/config.py`
2. **Create** `src/aegis/etls/key_themes/config/config.yaml`:
   ```yaml
   # Key Themes ETL Configuration

   models:
     theme_extraction:
       tier: medium
     formatting:
       tier: small
     grouping:
       tier: large

   llm:
     temperature: 0.1
     max_tokens: 32768
   ```
3. **Update** `main.py`:
   ```python
   from aegis.etls.config_loader import ETLConfig

   etl_config = ETLConfig(os.path.join(os.path.dirname(__file__), "config", "config.yaml"))

   # Replace all hardcoded model references:
   response = await complete_with_tools(
       messages=messages,
       tools=[prompt_data['tool_definition']],
       context=context,
       llm_params={
           "model": etl_config.get_model("theme_extraction"),
           "temperature": etl_config.temperature,
           "max_tokens": etl_config.max_tokens
       }
   )
   ```

---

## 2. Directory Structure

### What Changed in Call Summary

**Before:**
```
call_summary/
├── config/
│   ├── config.py
│   ├── canadian_banks_categories.xlsx
│   └── us_banks_categories.xlsx
├── prompts/
│   ├── research_plan.yaml
│   └── category_extraction.yaml
├── main.py
└── document_converter.py
```

**After:**
```
call_summary/
├── config/
│   ├── config.yaml                          # NEW - YAML config
│   ├── categories/                          # NEW - organized subdirectory
│   │   ├── canadian_banks_categories.xlsx
│   │   └── us_banks_categories.xlsx
│   ├── monitored_institutions.yaml          # NEW - bank type mappings
│   └── banner.jpg                           # Branding image
├── documentation/                           # NEW - docs and dependencies
│   ├── README.md
│   ├── requirements.txt
│   └── PROMPTS.md
├── main.py
└── document_converter.py
```

**Key additions:**
- `monitored_institutions.yaml`: Maps bank IDs to types (Canadian/US)
- `documentation/`: Self-contained ETL documentation
- `categories/`: Organized config files by purpose

### How to Apply to Key Themes

1. **Create organized structure:**
   ```bash
   mkdir -p src/aegis/etls/key_themes/config/categories
   mkdir -p src/aegis/etls/key_themes/documentation
   ```

2. **Create `monitored_institutions.yaml`** (if key themes uses bank-specific logic):
   ```yaml
   canadian_banks:
     id: 1
     name: "Royal Bank of Canada"
     type: "Canadian_Banks"
     path_safe_name: "Royal_Bank_of_Canada"
   # ... more banks
   ```

3. **Move theme-related config** to `config/categories/` if applicable

4. **Create `documentation/README.md`** explaining the ETL

---

## 3. Prompt Loading System

### What Changed in Call Summary

**Before:**
```python
# Prompts in local YAML files, manually loaded
with open('prompts/research_plan.yaml', 'r') as f:
    prompt_data = yaml.safe_load(f)
```

**After:**
```python
# Prompts in database, loaded via utility
from aegis.utils.prompt_loader import load_prompt_from_db

research_prompts = load_prompt_from_db(
    layer="call_summary_etl",
    name="research_plan",
    compose_with_globals=False,  # ETLs don't compose with global contexts
    available_databases=None,
    execution_id=execution_id
)

# Returns structured dict with:
# - system_prompt (str)
# - user_prompt_template (str)
# - tool_definition (dict)
```

**Dynamic placeholder population:**
```python
# Format prompts with runtime values
categories_text = ""
for i, category in enumerate(categories, 1):
    categories_text += f"""
Category {i}:
- Name: {category['category_name']}
- Section: {section_desc}
- Instructions: {category['category_description']}
"""

research_prompts["system_prompt"] = research_prompts["system_prompt"].format(
    categories_list=categories_text,
    bank_name=bank_info["bank_name"],
    bank_symbol=bank_info["bank_symbol"],
    quarter=quarter,
    fiscal_year=fiscal_year
)
```

### Why This Matters
- **Centralized prompts**: All prompts in database, version controlled
- **Reusability**: Same prompt infrastructure as main Aegis system
- **Dynamic content**: Placeholders populated at runtime
- **No file I/O**: Faster, cached database access

### How to Apply to Key Themes

1. **Identify all prompts** currently in local YAML files or hardcoded
2. **Upload to database** using the script pattern:
   ```python
   # scripts/upload_key_themes_prompts.py (ALREADY EXISTS)
   from aegis.utils.sql_prompt import postgresql_prompts

   postgresql_prompts()  # Initialize cache

   # Your prompts are uploaded to the database with layer="key_themes_etl"
   ```

3. **Replace all local prompt loading**:
   ```python
   # OLD - key_themes/main.py
   with open('prompts/theme_extraction.yaml', 'r') as f:
       prompt_data = yaml.safe_load(f)

   # NEW
   prompt_data = load_prompt_from_db(
       layer="key_themes_etl",
       name="theme_extraction",
       compose_with_globals=False,
       available_databases=None,
       execution_id=execution_id
   )
   ```

4. **Format prompts with placeholders**:
   ```python
   system_prompt = prompt_data['system_prompt'].format(
       bank_name=context.get('bank_name', 'Bank'),
       quarter=context.get('quarter', 'Q'),
       fiscal_year=context.get('fiscal_year', 'Year')
   )
   ```

---

## 4. Document Generation

### What Changed in Call Summary

**Before:**
```python
# document_converter.py
def structured_data_to_markdown(...) -> str:
    """Convert to markdown string"""
    markdown = f"# Title\n\n"
    # ... build markdown
    return markdown

def convert_docx_to_pdf_native(...):
    """Native PDF conversion with subprocess calls"""
    # 100+ lines of subprocess logic
```

**After:**
```python
# document_converter.py - focused utility functions
def setup_document_formatting(doc) -> None:
    """Configure margins and page numbers"""

def add_banner_image(doc, config_dir: str) -> None:
    """Add banner if found"""

def add_document_title(doc, quarter, fiscal_year, bank_symbol) -> None:
    """Add formatted title"""

def add_section_heading(doc, section_name, is_first_section=False) -> None:
    """Add section header"""

def add_structured_content_to_doc(doc, category_data, heading_level=2) -> None:
    """Add category content directly to document"""

def parse_and_format_text(paragraph, content, base_font_size, base_color, base_italic):
    """Parse markdown formatting (**bold**, __underline__)"""
```

**Direct document creation in main.py:**
```python
def _generate_document(valid_categories: list, etl_context: dict) -> tuple:
    """Generate Word document from category results."""
    doc = Document()
    setup_document_formatting(doc)
    add_banner_image(doc, config_dir)
    add_document_title(doc, quarter, fiscal_year, bank_info["bank_symbol"])
    add_table_of_contents(doc)

    for section_name, section_categories in groupby(...):
        add_section_heading(doc, section_name, is_first_section=idx == 0)
        for category_data in section_categories:
            add_structured_content_to_doc(doc, category_data, heading_level=2)

    doc.save(filepath)
    return filepath, docx_filename
```

### Why This Matters
- **Eliminated intermediate format**: No markdown conversion step
- **Better formatting control**: Direct Word API access
- **Modular functions**: Each does one thing well
- **Removed PDF complexity**: PDF generation is optional and delegated

### How to Apply to Key Themes

**Current key_themes approach is ALREADY GOOD:**
- Uses `HTMLToDocx` parser for HTML formatting
- Direct document creation with `python-docx`
- Clean separation of formatting functions

**However, you can:**

1. **Extract more utilities** from `create_document()` function:
   ```python
   # Currently in main.py, move to document_converter.py:
   def add_page_numbers_with_footer(doc, bank_symbol, quarter, fiscal_year):
       """Add custom footer"""

   def add_theme_header_with_background(doc, theme_number, theme_title):
       """Add styled theme header"""
   ```

2. **Standardize banner handling**:
   ```python
   # Use the same pattern as call_summary
   from aegis.etls.call_summary.document_converter import add_banner_image

   etl_dir = os.path.dirname(os.path.abspath(__file__))
   config_dir = os.path.join(etl_dir, 'config')
   add_banner_image(doc, config_dir)
   ```

3. **Add table of contents** (if desired):
   ```python
   from aegis.etls.call_summary.document_converter import (
       add_table_of_contents,
       mark_document_for_update
   )
   ```

---

## 5. Function Decomposition

### What Changed in Call Summary

**Before:**
```python
async def generate_call_summary(...):
    # 500+ line function doing everything:
    # - Authentication
    # - Data loading
    # - Research plan generation
    # - Category extraction (in loop)
    # - Document generation
    # - Database saving
    # All mixed together
```

**After:**
```python
# Broken into focused functions with clear contracts

async def _generate_research_plan(
    context: dict, research_prompts: dict, transcript_text: str, execution_id: str
) -> dict:
    """Generate research plan using LLM."""
    # Single responsibility: research plan generation
    # Clear inputs/outputs
    # Retry logic isolated here

def _build_extracted_themes(category_results: list) -> str:
    """Build extracted themes summary from completed category results."""
    # Pure function - no side effects

async def _process_categories(
    categories: list, research_plan_data: dict, extraction_prompts: dict, etl_context: dict
) -> list:
    """Process all categories and extract data from transcripts."""
    # All category processing logic
    # Takes structured context dict

def _generate_document(valid_categories: list, etl_context: dict) -> tuple:
    """Generate Word document from category results."""
    # Document creation isolated

async def _save_to_database(
    category_results: list, valid_categories: list, filepath: str,
    docx_filename: str, etl_context: dict
) -> None:
    """Save report metadata to database."""
    # Database operations isolated

# Main function orchestrates:
async def generate_call_summary(bank_name: str, fiscal_year: int, quarter: str) -> str:
    """Generate a call summary by directly calling transcript functions."""
    execution_id = str(uuid.uuid4())

    # Setup
    bank_info = await get_bank_info(bank_name)
    ssl_config = setup_ssl()
    auth_config = await setup_authentication(execution_id, ssl_config)
    context = {...}

    # Step 1: Load data
    bank_type = get_bank_type(bank_info["bank_id"])
    categories = load_categories_from_xlsx(bank_type, execution_id)

    # Step 2: Generate research plan
    research_plan_data = await _generate_research_plan(...)

    # Step 3: Process categories
    category_results = await _process_categories(...)

    # Step 4: Generate document
    filepath, docx_filename = _generate_document(...)

    # Step 5: Save to database
    await _save_to_database(...)
```

### Why This Matters
- **Testability**: Each function can be unit tested
- **Readability**: Main function reads like a workflow
- **Maintainability**: Easy to find and fix bugs
- **Reusability**: Helper functions can be used elsewhere
- **Error isolation**: Failures are easier to trace

### How to Apply to Key Themes

**Key themes is already well-structured**, but you can improve:

1. **Extract helper functions** from `main()`:
   ```python
   # Currently inline in main(), extract to:
   async def _validate_data_availability(bank_info: dict, year: int, quarter: str) -> bool:
       """Check if Q&A data exists."""

   async def _setup_execution_context(execution_id: str) -> dict:
       """Setup SSL, auth, and context."""

   async def _save_report_to_database(
       theme_groups: list, bank_info: dict, docx_path: str,
       pdf_path: str, metadata: dict
   ) -> int:
       """Save report and update availability."""
   ```

2. **Use `etl_context` pattern**:
   ```python
   # Instead of passing many individual parameters:
   def process_qa_blocks(
       qa_index, bank_name, bank_symbol, quarter, fiscal_year,
       execution_id, ssl_config, auth_config
   ):
       ...

   # Pass structured context:
   def process_qa_blocks(qa_index: dict, etl_context: dict):
       bank_name = etl_context["bank_name"]
       quarter = etl_context["quarter"]
       context = etl_context["context"]
       ...
   ```

3. **Separate concerns** clearly:
   ```python
   # Data loading
   qa_index = await load_qa_blocks(...)

   # Processing
   await process_all_qa_blocks(qa_index, context)

   # Grouping
   theme_groups = await determine_comprehensive_grouping(qa_index, context)

   # Document generation
   docx_path = create_document(theme_groups, ...)

   # Persistence
   await save_report(theme_groups, docx_path, ...)
   ```

---

## 6. Database Operations

### What Changed in Call Summary

**Before:**
```python
# Database operations scattered throughout the code
async with get_connection() as conn:
    result = await conn.execute(...)
    # Complex logic inline
```

**After:**
```python
async def _save_to_database(
    category_results: list,
    valid_categories: list,
    filepath: str,
    docx_filename: str,
    etl_context: dict,
) -> None:
    """
    Save report metadata to database.

    Args:
        category_results: All category results
        valid_categories: Accepted category results
        filepath: Local file path
        docx_filename: Document filename
        etl_context: Dict with keys: bank_info, quarter, fiscal_year, bank_type, execution_id
    """
    bank_info = etl_context["bank_info"]
    quarter = etl_context["quarter"]
    fiscal_year = etl_context["fiscal_year"]
    bank_type = etl_context["bank_type"]
    execution_id = etl_context["execution_id"]

    report_metadata = get_standard_report_metadata()
    generation_timestamp = datetime.now()

    try:
        async with get_connection() as conn:
            # Delete existing reports
            delete_result = await conn.execute(
                text("""
                DELETE FROM aegis_reports
                WHERE bank_id = :bank_id
                  AND fiscal_year = :fiscal_year
                  AND quarter = :quarter
                  AND report_type = :report_type
                RETURNING id
                """),
                {...}
            )
            delete_result.fetchall()

            # Insert new report
            result = await conn.execute(
                text("""INSERT INTO aegis_reports ..."""),
                {...}
            )
            result.fetchone()

            await conn.commit()

    except SQLAlchemyError as e:
        logger.error("etl.call_summary.database_error", ...)
```

**Improved bank lookup:**
```python
async def get_bank_info(bank_name: str) -> Dict[str, Any]:
    """
    Look up bank information from the aegis_data_availability table.

    Supports:
    - Bank ID (integer)
    - Exact bank_name or bank_symbol
    - Partial matches
    """
    async with get_connection() as conn:
        # Try as bank_id (integer)
        if bank_name.isdigit():
            result = await conn.execute(
                text("""
                SELECT DISTINCT bank_id, bank_name, bank_symbol
                FROM aegis_data_availability
                WHERE bank_id = :bank_id
                LIMIT 1
                """),
                {"bank_id": int(bank_name)}
            )
            ...

        # Try exact match
        # Try partial match
        # Raise helpful error with available banks
```

### Why This Matters
- **Isolation**: Database logic in dedicated functions
- **Error handling**: Consistent exception handling
- **Helpful errors**: User-friendly messages with suggestions
- **Transaction safety**: Proper commit/rollback

### How to Apply to Key Themes

**Key themes has good database code**, but you can:

1. **Extract `save_report_to_database` function**:
   ```python
   async def _save_report_to_database(
       theme_groups: list,
       qa_index: dict,
       docx_path: str,
       pdf_path: Optional[str],
       markdown_content: str,
       etl_context: dict
   ) -> int:
       """Save report and update data availability."""
       # All database operations in one function
       # Returns report_id
   ```

2. **Use the standardized bank lookup** (already done):
   ```python
   async def resolve_bank_info(bank_input: str, context: Dict[str, Any]) -> Dict[str, Any]:
       # Your function is already good!
       # Supports ID, ticker, partial name
       # Returns helpful error message
   ```

3. **Add data availability check**:
   ```python
   async def verify_data_availability(bank_id: int, fiscal_year: int, quarter: str) -> bool:
       """Check if Q&A data is available."""
       async with get_connection() as conn:
           result = await conn.execute(
               text("""
               SELECT database_names
               FROM aegis_data_availability
               WHERE bank_id = :bank_id
                 AND fiscal_year = :fiscal_year
                 AND quarter = :quarter
               """),
               {"bank_id": bank_id, "fiscal_year": fiscal_year, "quarter": quarter}
           )
           row = result.fetchone()
           if row and row[0]:
               return "transcripts" in row[0]
           return False
   ```

---

## 7. Error Handling

### What Changed in Call Summary

**Before:**
```python
# Errors would crash the ETL or produce unclear messages
try:
    # processing
except Exception as e:
    print(f"Error: {e}")
```

**After:**
```python
# Retry logic for LLM calls
max_retries = 3
for attempt in range(max_retries):
    try:
        response = await complete_with_tools(...)
        # Parse response
        break  # Success
    except (KeyError, IndexError, json.JSONDecodeError, TypeError) as e:
        logger.error(
            "etl.call_summary.research_plan_error",
            execution_id=execution_id,
            error=str(e),
            attempt=attempt + 1
        )
        if attempt < max_retries - 1:
            continue
        raise RuntimeError(
            f"Error generating research plan after {max_retries} attempts: {str(e)}"
        ) from e

# Validation and helpful errors
if not await verify_data_availability(bank_info["bank_id"], fiscal_year, quarter):
    error_msg = f"No transcript data available for {bank_info['bank_name']} {quarter} {fiscal_year}"

    # Show available periods
    result = await conn.execute(
        text("""
        SELECT DISTINCT fiscal_year, quarter
        FROM aegis_data_availability
        WHERE bank_id = :bank_id
          AND 'transcripts' = ANY(database_names)
        ORDER BY fiscal_year DESC, quarter DESC
        LIMIT 10
        """)
    )
    available_periods = await result.fetchall()

    if available_periods:
        period_list = ", ".join([f"{p['quarter']} {p['fiscal_year']}" for p in available_periods])
        error_msg += f"\n\nAvailable periods for {bank_info['bank_name']}: {period_list}"

    raise ValueError(error_msg)

# Different error types for different audiences
except (KeyError, TypeError, AttributeError, json.JSONDecodeError, SQLAlchemyError, FileNotFoundError) as e:
    # System errors with ❌ prefix (unexpected errors)
    error_msg = f"Error generating call summary: {str(e)}"
    logger.error("etl.call_summary.error", execution_id=execution_id, error=error_msg, exc_info=True)
    return f"❌ {error_msg}"
except (ValueError, RuntimeError) as e:
    # User-friendly errors with ⚠️ prefix (expected errors)
    logger.error("etl.call_summary.error", execution_id=execution_id, error=str(e))
    return f"⚠️ {str(e)}"
```

### Why This Matters
- **Reliability**: Retries handle transient failures
- **User experience**: Helpful error messages with suggestions
- **Debugging**: Structured logging with execution IDs
- **Error classification**: System vs user errors

### How to Apply to Key Themes

1. **Add retry logic** to all LLM calls:
   ```python
   # Currently in key_themes:
   response = await complete_with_tools(...)

   # Add retries:
   max_retries = 3
   for attempt in range(max_retries):
       try:
           response = await complete_with_tools(...)
           break
       except Exception as e:
           if attempt < max_retries - 1:
               logger.warning(f"Retry {attempt + 1}/{max_retries}: {str(e)}")
               await asyncio.sleep(2 ** attempt)  # Exponential backoff
               continue
           else:
               raise RuntimeError(f"Failed after {max_retries} attempts") from e
   ```

2. **Add data availability check** with helpful error:
   ```python
   # Before processing
   if not qa_index:
       # Show available periods
       async with get_connection() as conn:
           result = await conn.execute(text("""
               SELECT DISTINCT fiscal_year, quarter
               FROM aegis_transcripts
               WHERE institution_id = :bank_id
               ORDER BY fiscal_year DESC, quarter DESC
               LIMIT 10
           """), {"bank_id": str(bank_info["id"])})
           available = result.fetchall()

       if available:
           periods = ", ".join([f"{p['quarter']} {p['fiscal_year']}" for p in available])
           raise ValueError(
               f"No Q&A data for {bank_info['name']} {quarter} {fiscal_year}\n"
               f"Available periods: {periods}"
           )
       else:
           raise ValueError(f"No Q&A data found for {bank_info['name']}")
   ```

3. **Classify errors** for better UX:
   ```python
   # In main():
   except (ValueError, RuntimeError) as e:
       # Expected errors - user can fix
       logger.error(f"⚠️ {str(e)}")
       return 1
   except Exception as e:
       # Unexpected errors - bug report
       logger.error(f"❌ System error: {str(e)}", exc_info=True)
       return 1
   ```

---

## 8. Code Quality

### What Changed in Call Summary

**Before:**
- Mixed inline logic and function definitions
- Some TODOs and commented code
- Inconsistent error handling
- Limited docstrings

**After:**
- All functions have Google-style docstrings
- Type hints on all parameters
- Consistent naming conventions
- No commented code or TODOs
- Structured logging with execution IDs

**Example:**
```python
async def _process_categories(
    categories: list,
    research_plan_data: dict,
    extraction_prompts: dict,
    etl_context: dict
) -> list:
    """
    Process all categories and extract data from transcripts.

    Args:
        categories: List of category configurations
        research_plan_data: Research plan from LLM
        extraction_prompts: Prompts for extraction
        etl_context: Dict with keys: retrieval_params, bank_info, quarter,
            fiscal_year, context, execution_id

    Returns:
        List of category results (both accepted and rejected)
    """
    retrieval_params = etl_context["retrieval_params"]
    bank_info = etl_context["bank_info"]
    quarter = etl_context["quarter"]
    fiscal_year = etl_context["fiscal_year"]
    context = etl_context["context"]
    execution_id = etl_context["execution_id"]

    category_results = []

    for i, category in enumerate(categories, 1):
        # Clear, commented logic
        ...

    return category_results
```

### How to Apply to Key Themes

1. **Add comprehensive docstrings**:
   ```python
   async def extract_theme_and_summary(qa_block: QABlock, context: Dict[str, Any]):
       """
       Extract theme title and summary for a single Q&A block.

       Validates content and marks invalid Q&A sessions. Retries up to 3 times
       on failure with exponential backoff.

       Args:
           qa_block: Q&A block to process (modified in place)
           context: Execution context with execution_id, auth_config, etc.

       Raises:
           RuntimeError: If extraction fails after all retries
       """
   ```

2. **Add type hints** everywhere:
   ```python
   async def load_qa_blocks(
       bank_name: str,
       fiscal_year: int,
       quarter: str,
       context: Dict[str, Any]
   ) -> Dict[str, QABlock]:
   ```

3. **Use structured logging**:
   ```python
   logger.info(
       "etl.key_themes.theme_extracted",
       execution_id=execution_id,
       qa_id=qa_block.qa_id,
       theme=qa_block.theme_title
   )
   ```

4. **Remove commented code** and TODOs - put them in issues instead

---

## Summary Checklist for Key Themes Refactoring

### Configuration
- [ ] Delete `config/config.py`
- [ ] Create `config/config.yaml` with model tiers
- [ ] Update `main.py` to use `ETLConfig`
- [ ] Test all LLM calls use correct models

### Directory Structure
- [ ] Create `config/categories/` if needed
- [ ] Create `documentation/` folder
- [ ] Add `README.md` in documentation
- [ ] Consider `monitored_institutions.yaml` if needed

### Prompts
- [ ] Verify all prompts uploaded to database
- [ ] Replace local YAML loading with `load_prompt_from_db`
- [ ] Add placeholder formatting where needed
- [ ] Remove local prompt files

### Document Generation
- [ ] Extract document utilities to `document_converter.py`
- [ ] Standardize banner image handling
- [ ] Consider adding table of contents
- [ ] Ensure consistent styling

### Function Decomposition
- [ ] Extract helper functions from `main()`
- [ ] Use `etl_context` dict pattern
- [ ] Separate data loading, processing, persistence
- [ ] Add clear docstrings

### Database Operations
- [ ] Extract database save function
- [ ] Add data availability check
- [ ] Improve error messages with suggestions
- [ ] Use transactions properly

### Error Handling
- [ ] Add retry logic to all LLM calls
- [ ] Classify errors (user vs system)
- [ ] Add helpful error messages
- [ ] Use structured logging

### Code Quality
- [ ] Add Google-style docstrings
- [ ] Add type hints everywhere
- [ ] Use structured logging
- [ ] Remove TODOs and comments

---

## Testing the Refactored ETL

After refactoring, test thoroughly:

```bash
# Activate virtual environment
source venv/bin/activate

# Test with known good data
python -m aegis.etls.key_themes.main --bank RY --year 2025 --quarter Q2

# Test error cases
python -m aegis.etls.key_themes.main --bank INVALID --year 2025 --quarter Q2
python -m aegis.etls.key_themes.main --bank RY --year 1900 --quarter Q1

# Check database was updated
psql -d aegis -c "SELECT * FROM aegis_reports WHERE report_type='key_themes' ORDER BY generation_date DESC LIMIT 1;"
```

---

## Benefits of This Refactoring

1. **Maintainability**: Easier to understand and modify
2. **Testability**: Each function can be unit tested
3. **Consistency**: Both ETLs follow same patterns
4. **Reliability**: Better error handling and retries
5. **User Experience**: Helpful error messages
6. **Performance**: Cached prompts, efficient queries
7. **Scalability**: Easy to add new ETLs using same patterns

---

## Questions?

If you have questions while applying these refactorings, refer to:
- `/Users/alexwday/Projects/aegis/src/aegis/etls/call_summary/main.py` (reference implementation)
- `/Users/alexwday/Projects/aegis/src/aegis/etls/config_loader.py` (config system)
- `/Users/alexwday/Projects/aegis/scripts/upload_key_themes_prompts.py` (prompt upload)
