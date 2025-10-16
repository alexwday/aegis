# CM Readthrough ETL - Redesigned Architecture

## Overview
The redesigned CM Readthrough ETL processes earnings call transcripts for 20+ monitored banks to generate Capital Markets readthrough reports. The ETL uses a 5-phase pipeline with concurrent execution and intelligent content filtering.

## Architecture

### **5-Phase Pipeline**

```
Phase 1: Quote Extraction (Parallel)
  └─> Full transcript → LLM categorization → Filtered quotes by bank

Phase 2: Q&A Extraction (Parallel)
  └─> Q&A section → LLM categorization → Filtered questions by bank

Phase 3: Aggregation
  └─> Combine results → Sort by bank → Filter content

Phase 4: Batch Formatting
  └─> All quotes → Single LLM call → HTML emphasis tags

Phase 5: Document Generation
  └─> Formatted data → Word document → PDF (optional)
```

### **Key Improvements**

1. **Performance Optimization**
   - **2 LLM calls per bank** (quote extraction + Q&A extraction)
   - **2 global LLM calls** (batch formatting + optional subtitle)
   - **Total: ~42 LLM calls for 20 banks** (vs 60+ in old design)
   - **Concurrent processing** with semaphore limiting (max 5 concurrent banks)

2. **Intelligent Content Filtering**
   - LLM-based rejection via `has_content` flag
   - Banks with no relevant content automatically filtered out
   - No empty sections in final document

3. **Separate Category Management**
   - `quote_categories.xlsx` - Categories for quote extraction
   - `qa_categories.xlsx` - Categories for Q&A extraction
   - Easy to update without code changes

4. **Batch Formatting**
   - Single LLM call formats all quotes consistently
   - HTML emphasis tags (`<strong><u>`) applied to key phrases
   - Reduces LLM calls from N quotes to 1 total

5. **Robust Error Handling**
   - Bank failure doesn't stop pipeline
   - Graceful degradation on formatting failure
   - Comprehensive logging at each stage

## How to Run

### Basic Usage
```bash
# Activate virtual environment
source venv/bin/activate

# Run for specific period
python -m aegis.etls.cm_readthrough.main --year 2025 --quarter Q2 --no-pdf
```

### Advanced Options
```bash
# Use latest available data for each bank
python -m aegis.etls.cm_readthrough.main --year 2025 --quarter Q2 --use-latest

# Custom output path
python -m aegis.etls.cm_readthrough.main --year 2025 --quarter Q2 --output report.docx

# Generate PDF (requires LibreOffice)
python -m aegis.etls.cm_readthrough.main --year 2025 --quarter Q2
```

## Configuration

### Parameters
- `--year`: Fiscal year (required, e.g., 2025)
- `--quarter`: Quarter (required, Q1-Q4)
- `--use-latest`: Use latest available quarter for each bank (optional)
- `--output`: Custom output file path (optional)
- `--no-pdf`: Skip PDF generation (optional)

### Environment Variables
```bash
# Model configuration
CM_READTHROUGH_QUOTE_MODEL=gpt-4-turbo      # Quote extraction model
CM_READTHROUGH_QA_MODEL=gpt-4-turbo         # Q&A extraction model
CM_READTHROUGH_FORMAT_MODEL=gpt-4-turbo     # Batch formatting model

# Performance tuning
CM_READTHROUGH_TEMPERATURE=0.7              # LLM temperature
CM_READTHROUGH_MAX_TOKENS=4096              # Max tokens per call
CM_READTHROUGH_MAX_CONCURRENT=5             # Max concurrent banks
```

### Category Files
Located in `config/`:
- **quote_categories.xlsx** - Categories for transcript quote extraction
  - First column contains category names
  - Used in Phase 1 (Quote Extraction)

- **qa_categories.xlsx** - Categories for analyst question extraction
  - First column contains category names
  - Used in Phase 2 (Q&A Extraction)

### Monitored Institutions
Located in `config/monitored_institutions.yaml`:
```yaml
RY-CA: {id: 1, name: "Royal Bank of Canada", type: "Canadian_Banks", path_safe_name: "RY-CA_Royal_Bank_of_Canada"}
BMO-CA: {id: 2, name: "Bank of Montreal", type: "Canadian_Banks", path_safe_name: "BMO-CA_Bank_of_Montreal"}
# ... 20+ banks total
```

## Output Format

### Word Document Structure
```
┌─────────────────────────────────────────────────────┐
│ Read Through For Capital Markets: Q2/25            │
│ Select U.S. & European Banks                       │
├─────────────────────────────────────────────────────┤
│                                                     │
│ Section 1: Capital Markets Quotes                  │
│ ┌─────────┬────────────────────────────────────┐   │
│ │ Banks/  │ Investment Banking & Trading       │   │
│ │ Segments│ Outlook                            │   │
│ ├─────────┼────────────────────────────────────┤   │
│ │  JPM    │ • M&A: Pipeline remains strong...  │   │
│ │  BAC    │ • Trading: Record quarter...       │   │
│ └─────────┴────────────────────────────────────┘   │
│                                                     │
│ [Page Break]                                        │
│                                                     │
│ Section 2: Analyst Questions by Category           │
│                                                     │
│ ## M&A Pipeline                                     │
│ (5 questions)                                       │
│                                                     │
│ Q1. [JPMorgan] John Smith (Goldman Sachs)          │
│ "Can you comment on the M&A pipeline..."           │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### Database Storage
- Table: `aegis_reports`
- Fields:
  - `execution_id` - Unique UUID
  - `report_type` - 'cm_readthrough'
  - `fiscal_year`, `quarter`
  - `markdown_content` - Markdown version
  - `metadata` - JSON with full results

## Data Flow

### Phase 1: Quote Extraction
```python
Input:
  - Full transcript (MD + Q&A sections)
  - Quote categories list

Process:
  - LLM analyzes transcript
  - Tool call returns: {has_content: bool, quotes: [...]}
  - Filters banks with no content

Output:
  {
    "Bank of America": {
      "bank_symbol": "BAC-US",
      "quotes": [
        {"category": "M&A", "quote": "..."},
        {"category": "Trading", "quote": "..."}
      ]
    }
  }
```

### Phase 2: Q&A Extraction
```python
Input:
  - Q&A section only
  - Q&A categories list

Process:
  - LLM analyzes Q&A
  - Tool call returns: {has_content: bool, questions: [...]}
  - Extracts verbatim questions with analyst info

Output:
  {
    "Bank of America": {
      "bank_symbol": "BAC-US",
      "questions": [
        {
          "category": "M&A",
          "verbatim_question": "...",
          "analyst_name": "John Smith",
          "analyst_firm": "Goldman Sachs"
        }
      ]
    }
  }
```

### Phase 3: Aggregation
```python
Input:
  - List of (bank_name, bank_symbol, quotes_result)
  - List of (bank_name, bank_symbol, questions_result)

Process:
  - Filter banks with has_content=True
  - Organize by bank name
  - Preserve bank_symbol for ticker display

Output:
  - all_quotes: Dict[str, Dict] (filtered)
  - all_questions: Dict[str, Dict] (filtered)
```

### Phase 4: Batch Formatting
```python
Input:
  - All quotes from all banks (JSON)

Process:
  - Single LLM call with all quotes
  - Tool returns formatted_quotes with HTML tags
  - Merges back with bank_symbol

Output:
  - Same structure, each quote has "formatted_quote" field
  - HTML: <strong><u>key phrase</u></strong>
```

### Phase 5: Document Generation
```python
Input:
  - formatted_quotes
  - formatted_questions
  - metadata

Process:
  - create_combined_document()
  - Landscape orientation
  - Dark blue headers
  - HTML formatting applied

Output:
  - .docx file
  - .pdf file (optional)
  - Database entry
```

## Prompt Templates

### quote_extraction.yaml
- **Purpose**: Extract categorized quotes from full transcript
- **Tool**: `extract_capital_markets_quotes`
- **Output**: `{has_content: bool, quotes: [...]}`
- **Temperature**: 0.7
- **Features**:
  - Rejection mechanism via has_content
  - Category-based extraction
  - Paraphrasing for brevity

### qa_extraction.yaml
- **Purpose**: Extract categorized analyst questions
- **Tool**: `extract_analyst_questions`
- **Output**: `{has_content: bool, questions: [...]}`
- **Temperature**: 0.3 (lower for accuracy)
- **Features**:
  - Verbatim extraction (no paraphrasing)
  - Analyst name + firm capture
  - Question-only extraction (no answers)

### batch_formatting.yaml
- **Purpose**: Format all quotes with HTML emphasis
- **Tool**: `format_quotes_with_emphasis`
- **Output**: `{formatted_quotes: {...}}`
- **Temperature**: 0.3 (consistent formatting)
- **Features**:
  - Batch processing for efficiency
  - HTML tags for key phrases
  - Consistent emphasis across banks

## Performance Metrics

### LLM Call Reduction
```
Old Design (14 banks):
- IB extraction: 14 calls
- Quote formatting: ~50 calls (3-5 per bank)
- Q&A categorization: 14 calls
TOTAL: ~78 calls

New Design (20 banks):
- Quote extraction: 20 calls
- Q&A extraction: 20 calls
- Batch formatting: 2 calls
TOTAL: 42 calls

Improvement: 46% reduction at scale
```

### Concurrency Benefits
```
Sequential processing: ~20 minutes (20 banks × 1 min each)
Concurrent (limit 5): ~4-5 minutes (20 banks / 5 × 1 min)

Speed improvement: 75% faster
```

## Logging

The ETL provides detailed logging at each stage:

```
[PHASE 1 & 2] Starting concurrent extraction for 20 banks...
[TRANSCRIPT] Bank of America 2025 Q2: Retrieved 45000 MD chars + 30000 QA chars
[QUOTES EXTRACTED] Bank of America: 4 quotes
[QUESTIONS EXTRACTED] Bank of America: 7 questions
[NO QUOTES] Small Bank Inc: No relevant quotes found
[AGGREGATION] 18 banks with quotes, 19 banks with questions
[BATCH FORMATTING] Formatting 18 banks with quotes...
[PIPELINE COMPLETE] 18 banks with quotes, 19 banks with questions
```

## Error Handling

### Bank-Level Failures
- Exception during transcript retrieval → Bank skipped, pipeline continues
- Exception during extraction → Bank returns `has_content=False`
- Logged as error with full traceback

### Global Failures
- Formatting failure → Falls back to unformatted quotes
- Document generation failure → Raises exception (critical)
- Database save failure → Raises exception (critical)

## Migration from Old Design

### Breaking Changes
1. **Data Structure**:
   - Old: `ib_trading_outlook`, `categorized_qas`
   - New: `quotes`, `questions`

2. **Quote Structure**:
   - Old: `{theme: str, quote: str, formatted_quote: str}`
   - New: `{category: str, quote: str, formatted_quote: str}`

3. **Question Organization**:
   - Old: Organized by category first
   - New: Organized by bank, then reorganized by category for display

4. **Prompts**:
   - Old: 3 prompts (ib_trading_extraction, qa_categorization, ib_quote_formatting)
   - New: 3 prompts (quote_extraction, qa_extraction, batch_formatting)

### Configuration Updates Needed
1. Create `quote_categories.xlsx` (1-column Excel)
2. Create `qa_categories.xlsx` (1-column Excel)
3. Update environment variables if using custom models
4. Review monitored_institutions.yaml for 20 banks

## Troubleshooting

### "Categories file not found"
```bash
# Ensure Excel files exist:
ls -la config/quote_categories.xlsx
ls -la config/qa_categories.xlsx

# First column must contain category names
```

### "No results generated"
```
Possible causes:
1. No banks have transcript data for specified period
2. All banks rejected (has_content=False)
3. Database connection issues

Check logs for:
[NO DATA] messages
[NO QUOTES] messages
```

### Formatting issues in output
```
If HTML tags appear as text:
1. Check batch_formatting.yaml prompt
2. Verify tool call returns formatted_quote field
3. Check add_html_formatted_runs() in document_converter.py
```

### Concurrency timeouts
```bash
# Reduce concurrent banks if hitting rate limits
export CM_READTHROUGH_MAX_CONCURRENT=3

# Or increase timeout in complete_with_tools calls
```

## Future Enhancements

### Planned Features
- [x] Subtitle generation from aggregated outlook statements (COMPLETED)
- [ ] Executive summary synthesis
- [ ] Quality scoring for extracted statements
- [ ] Caching layer for transcript retrieval
- [ ] Regenerate specific sections without full rerun

### Potential Optimizations
- [ ] Embed categories in system prompt (reduce token usage)
- [ ] Stream formatting results as they arrive
- [ ] Parallel batch formatting (split into chunks)
- [ ] Smart retry logic for transient LLM failures

## Support

For issues or questions:
1. Check logs in terminal output
2. Review this README
3. Check main CLAUDE.md documentation
4. Contact maintainers

---

**Last Updated**: 2025-10-16
**Version**: 2.0 (Redesigned Architecture)
**Maintainer**: RBC CFO Group IT
