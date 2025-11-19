# CM Readthrough ETL Alignment Analysis

## Executive Summary

**Status**: call_summary and key_themes ETLs are fully aligned. cm_readthrough needs 8 critical changes to match the standardized pattern.

**Confidence**: call_summary and key_themes share ~95% identical code structure. cm_readthrough diverges in several key areas.

---

## Function-by-Function Comparison

### ✅ ALIGNED: Functions that match across all ETLs

| Function | call_summary | key_themes | cm_readthrough | Status |
|----------|--------------|------------|----------------|--------|
| `ETLConfig.__init__()` | Lines 61-64 | Lines 59-62 | Lines 63-66 | ✅ Identical |
| `ETLConfig._load_config()` | Lines 66-72 | Lines 64-70 | Lines 68-74 | ✅ Identical |
| `ETLConfig.get_model()` | Lines 74-101 | Lines 72-99 | Lines 76-102 | ✅ Identical |
| `ETLConfig.temperature` | Lines 104-106 | Lines 102-104 | Lines 105-107 | ✅ Identical |
| `ETLConfig.max_tokens` | Lines 109-111 | Lines 107-109 | Lines 110-112 | ✅ Identical |
| `verify_data_availability()` | Lines 736-766 | Lines 258-288 | N/A | ✅ Identical (not needed in cm) |

---

### ⚠️ ISSUES FOUND: Critical alignment problems

## Issue 1: ETLConfig Indentation Error

**Location**: `cm_readthrough/main.py:90`

**Problem**: Syntax error - incorrect indentation
```python
# CURRENT (BROKEN):
        if not tier:
            raise ValueError(f"No tier specified for model '{model_key}'")

            tier_map = {  # ❌ Extra indentation
            "small": config.llm.small.model,
```

**Required Fix**:
```python
# SHOULD BE:
        if not tier:
            raise ValueError(f"No tier specified for model '{model_key}'")

        # Resolve tier to actual model from global config
        tier_map = {  # ✅ Correct indentation
            "small": config.llm.small.model,
```

**Impact**: Code won't run - syntax error

---

## Issue 2: get_bank_info() Implementation Mismatch

**Location**: `cm_readthrough/main.py:362-401`

**Problem**: cm_readthrough uses YAML file instead of database query

### call_summary/key_themes Pattern (CORRECT):
```python
async def get_bank_info(bank_name: str) -> Dict[str, Any]:
    """Look up bank information from the aegis_data_availability table."""
    async with get_connection() as conn:
        if bank_name.isdigit():
            result = await conn.execute(
                text("""
                SELECT DISTINCT bank_id, bank_name, bank_symbol
                FROM aegis_data_availability
                WHERE bank_id = :bank_id
                LIMIT 1
                """),
                {"bank_id": int(bank_name)},
            )
            # ... handle result
```

### cm_readthrough Pattern (WRONG):
```python
async def get_bank_info(bank_identifier: Any) -> Dict[str, Any]:
    """Resolve bank identifier from YAML file."""  # ❌ Not using database
    institutions_dict = _load_monitored_institutions()  # ❌ YAML file

    if isinstance(bank_identifier, int):
        bank_id = int(bank_identifier)
        for ticker, info in institutions_dict.items():  # ❌ Searching YAML
            if info["id"] == bank_id:
                return {...}
```

**Required Fix**:
- Replace entire `get_bank_info()` function with database query pattern from call_summary
- Keep `_load_monitored_institutions()` for `get_monitored_institutions()` only
- All bank lookups should query `aegis_data_availability` table

**Impact**: Inconsistent data sources, potential stale data from YAML

---

## Issue 3: main() Function Structure Mismatch

**Location**: `cm_readthrough/main.py:1387-1475`

**Problem**: cm_readthrough has inline logic instead of calling a separate generation function

### call_summary/key_themes Pattern (CORRECT):
```python
def main():
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Generate call summary reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--bank", required=True, help="Bank ID, name, or symbol")
    parser.add_argument("--year", type=int, required=True, help="Fiscal year")
    parser.add_argument("--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"], help="Quarter")

    args = parser.parse_args()

    postgresql_prompts()  # ✅ Initialize SQL prompts

    print(f"\n🔄 Generating report for {args.bank} {args.quarter} {args.year}...\n")

    result = asyncio.run(
        generate_call_summary(bank_name=args.bank, fiscal_year=args.year, quarter=args.quarter)
    )

    print(result)  # ✅ Print final result

if __name__ == "__main__":
    main()
```

### cm_readthrough Pattern (WRONG):
```python
async def main():  # ❌ async main (should be sync)
    """Main entry point for the CM Readthrough ETL."""
    parser = argparse.ArgumentParser(...)
    parser.add_argument("--year", ...)  # ❌ No --bank argument
    parser.add_argument("--quarter", ...)
    parser.add_argument("--use-latest", ...)  # ❌ Extra argument
    parser.add_argument("--output", ...)  # ❌ Extra argument

    args = parser.parse_args()

    # ❌ NO postgresql_prompts() call

    execution_id = uuid.uuid4()
    # ... inline processing logic ...
    results = await process_all_banks_parallel(...)  # ❌ Direct call
    # ... inline document generation ...
    # ❌ No consistent return value or print

if __name__ == "__main__":
    asyncio.run(main())  # ❌ Different pattern
```

**Required Fix**:
1. Rename current `main()` to `generate_cm_readthrough()`
2. Make `generate_cm_readthrough()` return a status string like others
3. Create new sync `main()` function following the standard pattern
4. Add `postgresql_prompts()` call
5. Add consistent print statements

---

## Issue 4: Missing postgresql_prompts() Call

**Location**: `cm_readthrough/main.py:1387-1475`

**Problem**: cm_readthrough doesn't initialize SQL prompts before database operations

### call_summary/key_themes Pattern (CORRECT):
```python
def main():
    # ... argparse setup ...
    args = parser.parse_args()

    postgresql_prompts()  # ✅ Initialize before any DB operations

    print(f"\n🔄 Generating report...")
    result = asyncio.run(generate_xxx(...))
    print(result)
```

### cm_readthrough Pattern (WRONG):
```python
async def main():
    # ... argparse setup ...
    args = parser.parse_args()

    # ❌ NO postgresql_prompts() call

    # Database operations happen without initialization
    results = await process_all_banks_parallel(...)
```

**Required Fix**:
- Add `postgresql_prompts()` call before any database operations
- Should be in the new sync `main()` function, before `asyncio.run()`

**Impact**: Potential database connection issues, missing SQL prompt configurations

---

## Issue 5: Return Value Inconsistency

**Location**: `cm_readthrough/main.py:1387-1475`

**Problem**: cm_readthrough doesn't return a consistent success/error message

### call_summary/key_themes Pattern (CORRECT):
```python
async def generate_call_summary(bank_name: str, fiscal_year: int, quarter: str) -> str:
    """Generate a call summary report."""
    try:
        # ... processing ...
        return (
            f"✅ Complete: {filepath}\n   Categories: "
            f"{len(valid_categories)}/{len(category_results)} included"
        )
    except (KeyError, TypeError, ...) as e:
        return f"❌ {error_msg}"  # System errors
    except (ValueError, RuntimeError) as e:
        return f"⚠️ {str(e)}"  # User-friendly errors
```

### cm_readthrough Pattern (WRONG):
```python
async def main():  # ❌ Should return str
    try:
        # ... processing ...
        logger.info(f"CM Readthrough ETL completed successfully")  # ❌ Just logs
        # ❌ No return value
    except Exception as e:
        logger.error(f"Error in CM Readthrough ETL: {e}", exc_info=True)
        raise  # ❌ Raises instead of returning error message
```

**Required Fix**:
- Rename `main()` to `generate_cm_readthrough()`
- Add return type annotation: `-> str`
- Return success message: `f"✅ Complete: {docx_path}\n   Banks: {metadata}"`
- Return error messages instead of raising exceptions
- Use same error categorization (❌ for system errors, ⚠️ for user errors)

---

## Issue 6: Database Save Pattern Mismatch

**Location**: `cm_readthrough/main.py:1305-1384`

**Problem**: cm_readthrough doesn't follow delete-then-insert pattern

### call_summary/key_themes Pattern (CORRECT):
```python
async with get_connection() as conn:
    # 1. Delete existing report for same bank/period/type
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
    delete_result.fetchall()  # ✅ Fetch to complete

    # 2. Insert new report
    result = await conn.execute(
        text("INSERT INTO aegis_reports ..."),
        {...}
    )
    result.fetchone()  # ✅ Fetch to complete

    await conn.commit()  # ✅ Explicit commit
```

### cm_readthrough Pattern (WRONG):
```python
async with get_connection() as conn:
    # ❌ No delete - just insert (will fail on duplicates)
    query = text("""
        INSERT INTO aegis_reports (...)
        VALUES (...)
    """)

    await conn.execute(query, {...})
    await conn.commit()
```

**Required Fix**:
- Add DELETE query before INSERT
- Use RETURNING id clause
- Fetch results to complete queries
- Handle potential duplicate key errors

**Impact**: Database constraint violations, inability to regenerate reports

---

## Issue 7: Error Handling Pattern Mismatch

**Location**: Various throughout cm_readthrough

**Problem**: Inconsistent error handling compared to other ETLs

### call_summary/key_themes Pattern (CORRECT):
```python
try:
    # ... main processing ...
    return f"✅ Complete: {filepath}..."

except (KeyError, TypeError, AttributeError, json.JSONDecodeError, SQLAlchemyError, FileNotFoundError) as e:
    # System errors - unexpected, likely code bugs
    error_msg = f"Error generating call summary: {str(e)}"
    logger.error("etl.call_summary.error", execution_id=execution_id, error=error_msg, exc_info=True)
    return f"❌ {error_msg}"

except (ValueError, RuntimeError) as e:
    # User-friendly errors - expected conditions (no data, auth failure, etc.)
    logger.error("etl.call_summary.error", execution_id=execution_id, error=str(e))
    return f"⚠️ {str(e)}"
```

### cm_readthrough Pattern (WRONG):
```python
try:
    # ... processing ...
    logger.info(f"CM Readthrough ETL completed successfully")
    # ❌ No return value

except Exception as e:
    # ❌ Catches all exceptions - too broad
    logger.error(f"Error in CM Readthrough ETL: {e}", exc_info=True)
    raise  # ❌ Re-raises instead of returning message
```

**Required Fix**:
- Separate exception handlers for system vs user errors
- Return ❌ messages for system errors
- Return ⚠️ messages for expected errors (no data, auth failure)
- Don't raise - always return a message string

---

## Issue 8: Argument Handling Mismatch

**Location**: `cm_readthrough/main.py:1387-1415`

**Problem**: cm_readthrough has different CLI arguments than other ETLs

### call_summary/key_themes Pattern (CORRECT):
```python
parser.add_argument("--bank", required=True, help="Bank ID, name, or symbol")
parser.add_argument("--year", type=int, required=True, help="Fiscal year")
parser.add_argument("--quarter", required=True, choices=["Q1", "Q2", "Q3", "Q4"], help="Quarter")
# ✅ Only 3 required arguments
```

### cm_readthrough Pattern (DIFFERENT):
```python
parser.add_argument("--year", type=int, required=True, help="Fiscal year (e.g., 2024)")
parser.add_argument("--quarter", type=str, required=True, help="Quarter (e.g., Q3)")
# ❌ No --bank (because it processes all banks)
parser.add_argument("--use-latest", action="store_true", help="Use latest available quarter...")
parser.add_argument("--output", type=str, help="Output file path (optional)")
```

**Analysis**:
- This is **intentionally different** because cm_readthrough is a multi-bank report
- Other ETLs process single banks
- `--use-latest` and `--output` are ETL-specific features

**Required Action**:
- **KEEP AS IS** - this difference is legitimate
- Document this as an intentional variation
- Add comment explaining why arguments differ

---

## Summary Table: Changes Required

| # | Issue | File | Lines | Severity | Action |
|---|-------|------|-------|----------|--------|
| 1 | Indentation error | main.py | 90 | 🔴 Critical | Fix indentation |
| 2 | get_bank_info() uses YAML | main.py | 362-401 | 🔴 Critical | Replace with DB query |
| 3 | main() structure wrong | main.py | 1387-1475 | 🔴 Critical | Refactor to match pattern |
| 4 | Missing postgresql_prompts() | main.py | 1387 | 🟡 High | Add call |
| 5 | No return value | main.py | 1387-1475 | 🟡 High | Return status string |
| 6 | No delete before insert | main.py | 1305-1384 | 🟡 High | Add DELETE query |
| 7 | Wrong error handling | main.py | 1429-1472 | 🟠 Medium | Separate exception types |
| 8 | Different CLI args | main.py | 1392-1413 | ✅ OK | Document as intentional |

---

## Detailed Change Plan

### Change 1: Fix Indentation (5 seconds)
```python
# Line 90 - remove extra indent before tier_map
```

### Change 2: Replace get_bank_info() (10 minutes)
```python
# Replace lines 362-401 with database query pattern
# Copy implementation from call_summary.py:211-278
# Maintain async signature
# Keep same return structure
```

### Change 3: Refactor main() (20 minutes)
```python
# Step 1: Rename current main() -> generate_cm_readthrough()
# Step 2: Add return type: -> str
# Step 3: Return success/error messages
# Step 4: Create new sync main() following standard pattern
# Step 5: Move argparse, postgresql_prompts, print to new main()
```

### Change 4: Add postgresql_prompts() (1 minute)
```python
# Add to new main() function before asyncio.run()
def main():
    # ... argparse ...
    postgresql_prompts()  # ADD THIS
    result = asyncio.run(generate_cm_readthrough(...))
```

### Change 5: Add Return Values (15 minutes)
```python
# In generate_cm_readthrough():
# Success case:
return f"✅ Complete: {docx_path}\n   Banks: {banks_with_data}/{total_banks} with data"

# Error cases:
except (KeyError, TypeError, ...) as e:
    return f"❌ Error generating CM readthrough: {str(e)}"
except (ValueError, RuntimeError) as e:
    return f"⚠️ {str(e)}"
```

### Change 6: Fix Database Save (10 minutes)
```python
# In save_to_database(), add before INSERT:
delete_result = await conn.execute(
    text("""
    DELETE FROM aegis_reports
    WHERE fiscal_year = :fiscal_year
      AND quarter = :quarter
      AND report_type = 'cm_readthrough'
    RETURNING id
    """),
    {"fiscal_year": fiscal_year, "quarter": quarter}
)
delete_result.fetchall()

# Then existing INSERT
# Then await conn.commit()
```

### Change 7: Fix Error Handling (10 minutes)
```python
# Replace generic Exception catch with specific types
except (
    KeyError,
    TypeError,
    AttributeError,
    json.JSONDecodeError,
    FileNotFoundError,
) as e:
    error_msg = f"Error generating CM readthrough: {str(e)}"
    logger.error("etl.cm_readthrough.error", execution_id=execution_id, error=error_msg, exc_info=True)
    return f"❌ {error_msg}"

except (ValueError, RuntimeError) as e:
    logger.error("etl.cm_readthrough.error", execution_id=execution_id, error=str(e))
    return f"⚠️ {str(e)}"
```

### Change 8: Document CLI Arguments (5 minutes)
```python
# Add docstring comment explaining multi-bank processing
"""
CM Readthrough ETL - Multi-bank report generation.

Unlike call_summary and key_themes which process a single bank,
this ETL processes ALL monitored institutions in parallel.

Arguments differ from other ETLs:
- No --bank argument (processes all banks)
- --use-latest flag for flexible quarter selection
- --output for custom file paths
"""
```

---

## Testing Checklist

After making changes, verify:

- [ ] Code has no syntax errors (especially indentation)
- [ ] `get_bank_info()` queries database correctly
- [ ] `postgresql_prompts()` is called before DB operations
- [ ] Returns success message: `"✅ Complete: ..."`
- [ ] Returns system error: `"❌ Error..."`
- [ ] Returns user error: `"⚠️ No data available..."`
- [ ] Database DELETE/INSERT pattern works
- [ ] Can regenerate same report (delete + insert)
- [ ] CLI prints result message
- [ ] Logging follows structured format

---

## Implementation Order

**Priority 1 (Must Fix):**
1. Issue 1: Fix indentation error
2. Issue 3: Refactor main() structure
3. Issue 5: Add return values

**Priority 2 (Should Fix):**
4. Issue 2: Replace get_bank_info()
5. Issue 4: Add postgresql_prompts()
6. Issue 6: Fix database save pattern

**Priority 3 (Nice to Have):**
7. Issue 7: Improve error handling
8. Issue 8: Document CLI differences

---

## Estimated Total Time: 70 minutes
