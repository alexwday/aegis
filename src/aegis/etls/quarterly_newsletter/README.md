# Quarterly Newsletter ETL

This ETL processes all monitored financial institutions for a given quarter and generates a combined newsletter document containing paragraph summaries for each bank's earnings call.

## Overview

The Quarterly Newsletter ETL differs from single-bank processing ETLs by:

1. **Processing all banks automatically** - iterates through the complete monitored institutions list
2. **Generating simplified summaries** - creates single paragraph summaries rather than detailed categorical analysis
3. **Producing consolidated output** - combines all bank summaries into one Word document
4. **Handling partial failures gracefully** - continues processing if individual banks fail

## Usage

```bash
# Generate newsletter for Q3 2024
python -m aegis.etls.quarterly_newsletter.main --year 2024 --quarter Q3

# Generate newsletter for Q1 2025
python -m aegis.etls.quarterly_newsletter.main --year 2025 --quarter Q1
```

No `--bank` argument is required as the ETL processes all monitored institutions automatically.

## Output

The ETL generates a Word document in the `output/` directory with the following structure:

- **Title page** - Quarter/year and generation timestamp
- **Canadian Banks section** - Individual paragraph summaries for Canadian institutions
- **US Banks section** - Individual paragraph summaries for US institutions
- **Processing notes** - Documentation of any banks that failed to process

## Configuration

The ETL can be customized using environment variables:

```bash
# Use different model (default: gpt-4o-mini)
export NEWSLETTER_SUMMARY_MODEL="gpt-4o"

# Adjust temperature for consistency/creativity (default: 0.7)
export NEWSLETTER_TEMPERATURE="0.3"

# Modify maximum response length (default: 300 tokens ≈ 200 words)
export NEWSLETTER_MAX_TOKENS="400"

# Then execute normally
python -m aegis.etls.quarterly_newsletter.main --year 2024 --quarter Q3
```

## Architecture

### Multi-Bank Processing Pattern
The ETL implements a sequential processing pattern across all monitored institutions:

```python
institutions = load_monitored_institutions()
for symbol, bank_info in institutions.items():
    summary = await generate_bank_summary(...)
    summaries.append(summary)
```

### Infrastructure Reuse
Rather than implementing custom transcript retrieval, the ETL leverages existing transcript subagent functions:

```python
chunks = await retrieve_full_section(combo=combo, sections="ALL", context=context)
formatted_transcript = await format_full_section_chunks(chunks=chunks, combo=combo, context=context)
```

### Error Isolation
Individual bank processing failures are captured but do not halt overall execution:

```python
try:
    summary = await generate_bank_summary(...)
    summaries.append(successful_summary)
except Exception as e:
    summaries.append(failed_summary_with_error)
    # Continue processing remaining banks
```

## File Structure

```
quarterly_newsletter/
├── main.py              # Primary ETL logic and orchestration
├── config/
│   ├── config.py        # Model and parameter configuration
│   └── monitored_institutions.yaml  # Institution definitions
├── prompts/
│   └── newsletter_summary_prompt.yaml  # LLM prompt template
├── output/              # Generated documents
└── README.md           # This documentation
```

## Processing Flow

1. **Initialization** - Load configuration and authenticate with external services
2. **Institution Loading** - Import complete monitored institutions list
3. **Data Availability Check** - Verify transcript data exists for each bank/period
4. **Sequential Processing** - Generate paragraph summary for each available bank
5. **Document Generation** - Combine all summaries into formatted Word document
6. **Results Reporting** - Output processing statistics and document location

## Key Design Decisions

### Sequential vs Parallel Processing
The ETL processes banks sequentially rather than in parallel to:
- Avoid API rate limiting issues
- Simplify error handling and debugging
- Provide predictable resource usage patterns

### Simplified Summary Format
Unlike detailed categorical analysis, this ETL generates single paragraph summaries to:
- Optimize for newsletter readability
- Reduce processing complexity and execution time
- Maintain consistent output format across all institutions

### Graceful Failure Handling
The ETL continues processing when individual banks fail to:
- Maximize useful output even with partial data availability
- Provide comprehensive reporting on both successes and failures
- Avoid total workflow failure due to single institution issues

## Troubleshooting

**"No transcript data available"** - Indicates the specified bank/quarter combination lacks earnings call transcripts in the database. The ETL will document this in the processing notes section.

**"Authentication failed"** - Verify authentication configuration in environment variables or main Aegis settings.

**Empty output document** - Occurs when no banks have available data for the specified period. The document will contain only processing notes explaining the situation.

## Extension Patterns

The architecture patterns demonstrated in this ETL can be adapted for:

- **Multi-entity comparative analysis** - Processing multiple institutions for comparison
- **Time-series batch processing** - Processing the same institutions across multiple periods
- **Regulatory reporting workflows** - Standardized processing across institution portfolios
- **Cross-database aggregation** - Combining data from multiple sources for consolidated reporting