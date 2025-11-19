# WM Readthrough ETL - Next Steps Guide

**Status**: ✅ Infrastructure Complete & Tested
**Next Phase**: Define Real Document Sections
**Date**: November 3, 2025

---

## 📖 Project Overview

**Purpose**: Generate wealth management (WM) readthrough reports by extracting information from quarterly earnings call transcripts across multiple banks.

**Previous Approach**: 5 hardcoded "page" extraction functions with separate prompts for each page type. Required code changes to add/modify sections.

**New Approach**: Template-driven architecture where sections are defined in a CSV file. No code changes needed to add/modify sections.

---

## 🎯 What We Built

The WM Readthrough ETL has been completely refactored into a **template-driven architecture**:

### Key Changes
- ❌ **Old**: 5 hardcoded "page" functions, separate prompts, code changes for new sections
- ✅ **New**: 1 generic extraction function, template-driven, CSV-based configuration

### Quality Validation
- ✅ **Pylint**: 9.47/10 (main) and 10.00/10 (upload script)
- ✅ **Flake8**: 0 issues
- ✅ **Tests**: 6/6 passed (100%)
- ✅ **Bugs Fixed**: 2 critical bugs found and resolved

### Technology Stack
- **Language**: Python (async/await)
- **LLM**: OpenAI API (gpt-4-turbo)
- **Database**: PostgreSQL (prompts stored in `prompts` table)
- **Transcript Source**: `transcripts` subagent (MD and Q&A sections)
- **Output**: JSON format with structured section results

### Key Architecture Points
1. **Prompt in Database**: Base prompt stored in postgres (layer: `wm_readthrough_etl`, name: `wm_section_extraction`, ID: 9)
2. **Variable Injection**: Template variables from CSV get injected into prompt at runtime
3. **Generic Extraction**: Single `extract_section()` function handles all section types
4. **Concurrent Processing**: Processes multiple banks in parallel (configurable via semaphore)
5. **Bank Filtering**: Each section specifies which bank types to process

---

## 📂 Current File Structure

```
src/aegis/etls/wm_readthrough/
├── main_refactored.py              ← New template-driven ETL (use this)
├── templates/
│   └── section_definitions_MOCKUP.csv  ← Example template (will be replaced)
├── prompts/
│   └── wm_section_extraction.yaml  ← Base prompt (uploaded to DB)
├── test_refactored.py              ← Test suite
├── config/
│   ├── config.py                   ← Settings & bank lists
│   └── monitored_institutions.yaml ← Bank definitions
└── NEXT_STEPS.md                   ← This file

Database:
└── prompts table (ID: 9)           ← Template prompt ready to use
```

---

## 🚀 Next Steps - Define Real Sections

### Phase 1: Section Definition (Current Phase)

You mentioned you'll show me **pictures and examples** from the original WM document. For each section, we'll define:

#### Template Row Structure
```csv
section_id,section_name,section_description,section_instructions,section_notes,section_examples,transcript_parts,institution_types,prompt_name
```

#### What I Need From You

**For Each Section**, please provide:

1. **Picture/Screenshot** - Show me the section from the original document
2. **Section Purpose** - What information should be extracted?
3. **Source Material** - MD only, Q&A only, or both?
4. **Bank Scope** - Which banks? (Monitored US, All US, Canadian AM, etc.)
5. **Key Details** - Any specific formatting, metrics, or structure requirements?

#### Example Workflow (We'll Do This Together)

```
You: "Here's section 1 - WM Narrative" [shows picture]
Me: "I see it needs revenue metrics, AUM growth, and quotes. Let me draft the template row..."

→ Create template row
→ Test extraction
→ Refine based on output quality
→ Move to next section
```

---

## 🎨 How Template Sections Work

Each section in the CSV drives the extraction:

### Template Variables → Prompt Variables

**Section Definition (CSV)**:
```csv
section_id: WM_NARRATIVE
section_description: "Extract revenue and AUM metrics with quotes"
section_instructions: "Focus on MD section, use Q&A for support"
section_notes: "Include YoY comparisons"
section_examples: "Example: Revenue grew 15% to $5.2B"
transcript_parts: BOTH
institution_types: Monitored_US_Banks
```

**Gets Injected Into Prompt**:
```
System Prompt:
  # Section Overview
  Extract revenue and AUM metrics with quotes

  # Instructions
  Focus on MD section, use Q&A for support

  # Notes
  Include YoY comparisons

  # Examples
  Example: Revenue grew 15% to $5.2B

User Prompt:
  Bank: JPMorgan Chase
  Period: 2025 Q1
  Section: WM Narrative
  Transcript: [full transcript content]
```

**Output Structure**:
```json
{
  "section_id": "WM_NARRATIVE",
  "section_name": "WM Narrative",
  "bank_name": "JPMorgan Chase",
  "bank_symbol": "JPM",
  "has_content": true,
  "content": "JPMorgan Chase's wealth management division reported...",
  "metadata": {"source_sections": ["MD", "QA"], "confidence": "high"}
}
```

---

## 🔧 Testing & Running

### 1. Upload Prompt to Database (Already Done)
```bash
source venv/bin/activate
python scripts/upload_wm_section_prompt.py
# ✓ Already uploaded as ID: 9
```

### 2. Run Logic Tests
```bash
source venv/bin/activate
python src/aegis/etls/wm_readthrough/test_refactored.py
# Should show: ✅ ALL TESTS PASSED!
```

### 3. Run ETL with Mockup Template
```bash
source venv/bin/activate

# Test with mockup (won't have perfect output, just tests the flow)
python -m aegis.etls.wm_readthrough.main_refactored \
  --year 2025 \
  --quarter Q1 \
  --output output/test_mockup.json

# Check output
cat output/test_mockup.json | jq '.metadata'
```

### 4. Run ETL with Real Template (After We Define Sections)
```bash
# Once we create section_definitions_REAL.csv:
python -m aegis.etls.wm_readthrough.main_refactored \
  --year 2025 \
  --quarter Q1 \
  --template templates/section_definitions_REAL.csv \
  --output output/wm_readthrough_2025_Q1.json
```

---

## 📋 Template Definition Checklist

When defining each section, we'll validate:

- [ ] **section_id**: Unique, uppercase with underscores (e.g., WM_NARRATIVE)
- [ ] **section_name**: Human-readable (e.g., "Wealth Management Narrative")
- [ ] **section_description**: Clear 1-2 sentence extraction goal
- [ ] **section_instructions**: Specific extraction instructions
- [ ] **section_notes**: Important guidance for the LLM
- [ ] **section_examples**: 1-2 example outputs showing desired format
- [ ] **transcript_parts**: MD, QA, or BOTH (uppercase)
- [ ] **institution_types**: Valid types, comma-separated, no spaces
- [ ] **prompt_name**: Always `wm_section_extraction` (unless we create new prompts)

### Valid Institution Types
- `Monitored_US_Banks` - Subset requiring detailed tracking
- `US_Banks` - All US banking institutions
- `Canadian_Asset_Managers` - Canadian asset management firms

### Valid Transcript Parts
- `MD` - Management Discussion section only
- `QA` - Q&A section only
- `ALL` - Both sections combined

---

## 🎬 How We'll Proceed (Step-by-Step)

### Step 1: Section Discovery
**Your Part**: Share pictures/examples of each section from the WM document

**My Part**: Review and understand what each section needs

### Step 2: Section Definition
**Your Part**: Provide details (purpose, source, scope, requirements)

**My Part**: Draft template rows with:
- Precise section descriptions
- Clear extraction instructions
- Helpful notes and examples
- Correct transcript parts and bank types

### Step 3: Iterative Refinement
**Your Part**: Review initial extraction outputs

**My Part**: Refine template rows based on output quality

**Together**: Iterate until each section produces high-quality results

### Step 4: Final Template
**Your Part**: Approve final section definitions

**My Part**: Create `section_definitions_REAL.csv` and test end-to-end

---

## 💡 Pro Tips

### Good Section Descriptions
- ✅ "Extract revenue metrics, AUM growth, and client flow statistics with YoY comparisons"
- ❌ "Get WM stuff"

### Good Instructions
- ✅ "Focus on quantitative metrics. Include percentage changes and absolute values. Extract from MD section primarily, using Q&A for supporting evidence."
- ❌ "Get the numbers"

### Good Examples
- ✅ "Example: WM revenue grew 15% YoY to $5.2B with net new assets of $75B in the quarter."
- ❌ "Example: Some revenue number"

### CSV Formatting
- ✅ Use double quotes around fields containing commas: `"Extract AUM, flows, and metrics"`
- ❌ Unquoted fields with commas will break parsing

---

## 🔍 Quick Reference

### Key Files
- **ETL Code**: `main_refactored.py`
- **Template**: `templates/section_definitions_MOCKUP.csv` (will become REAL)
- **Tests**: `test_refactored.py`
- **Prompt Upload**: `scripts/upload_wm_section_prompt.py`

### Key Commands
```bash
# Run tests
python src/aegis/etls/wm_readthrough/test_refactored.py

# Run ETL
python -m aegis.etls.wm_readthrough.main_refactored --year 2025 --quarter Q1

# Check output
cat output/*.json | jq '.results[] | {section_id, bank_name, has_content}'
```

### Configuration
- **Bank Lists**: `config/monitored_institutions.yaml`
- **Models**: `config/config.py` (MODELS dict)
- **Prompt in DB**: Table `prompts`, layer `wm_readthrough_etl`, name `wm_section_extraction`

---

## 📞 Ready When You Are!

I'm ready to start defining sections whenever you're ready to share:
1. Pictures/screenshots of each section from the WM document
2. Explanation of what each section should extract
3. Any specific formatting or requirements

We'll work through each section together, test the extraction, and refine until it's perfect.

**Let's build the real template!** 🚀

---

## 📜 What Happened Previously (Context for Fresh Chat)

### Refactoring Work Completed
1. **Analyzed** old page-based architecture with 5 hardcoded extraction functions
2. **Designed** new template-driven architecture with CSV-based section definitions
3. **Created** base prompt template with variable injection (`wm_section_extraction.yaml`)
4. **Uploaded** prompt to postgres database (ID: 9)
5. **Refactored** main ETL code into `main_refactored.py` with generic extraction
6. **Built** comprehensive test suite (`test_refactored.py`) - 6/6 tests passing
7. **Validated** code quality (pylint 9.47-10.00/10, flake8 clean)
8. **Fixed** 2 critical bugs (CSV parsing error, cell-var-from-loop issue)
9. **Cleaned up** project folder (removed 8 old files)

### Files Created
- `main_refactored.py` - New template-driven ETL
- `test_refactored.py` - Test suite
- `templates/section_definitions_MOCKUP.csv` - Example template
- `prompts/wm_section_extraction.yaml` - Base prompt (uploaded to DB)
- `NEXT_STEPS.md` - This guide
- `scripts/upload_wm_section_prompt.py` - DB upload utility

### Files Removed
- `main_OLD_page_based.py` - Old hardcoded approach
- `document_converter.py` - Old page-specific converter
- 6 old page-specific prompt YAML files

### Current State
- ✅ Infrastructure is complete and tested
- ✅ Prompt is in database and ready to use
- ✅ Code quality is excellent (9.47-10.00/10)
- ✅ All tests passing (6/6)
- ⏳ **Next**: Define real sections from WM document (with your input)

### What We Need From You
Pictures/examples of each section from the original WM document, so we can define:
- Section ID and name
- What to extract (description)
- How to extract it (instructions)
- Important notes for the LLM
- Example outputs
- Which transcript parts to use (MD/QA/BOTH)
- Which banks to process (institution types)
