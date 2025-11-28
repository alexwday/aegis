# Bank Earnings Report ETL

Generates HTML quarterly earnings reports for Canadian banks by extracting and synthesizing data from multiple sources (Supplementary Pack, RTS, Transcripts).

## Quick Start

```bash
# Generate a report
python -m aegis.etls.bank_earnings_report.main --bank RY --quarter Q2 --year 2025

# Output saved to: output/{SYMBOL}_{YEAR}_{QUARTER}.html
```

## Project Structure

```
bank_earnings_report/
├── main.py                 # ETL orchestrator and CLI
├── config/
│   ├── etl_config.py       # Configuration management
│   └── etl_config.yaml     # LLM model and parameter settings
├── retrieval/              # Database retrieval functions
│   ├── supplementary.py    # Supp Pack metrics, dividends, segments
│   ├── rts.py              # RTS driver extraction
│   └── transcripts.py      # Earnings call transcript chunks
├── extraction/             # LLM-based extraction and selection
│   ├── key_metrics.py      # Chart/tile metric selection
│   ├── segment_metrics.py  # Segment metric selection
│   ├── analyst_focus.py    # Q&A theme extraction
│   ├── management_narrative.py  # Quote extraction
│   └── transcript_insights.py   # Overview and items of note
├── templates/
│   └── report_template.html    # Jinja2 HTML template
└── output/                 # Generated reports (gitignored)
```

## CLI Options

```bash
python -m aegis.etls.bank_earnings_report.main \
  --bank RY \              # Bank symbol (RY, TD, BMO, BNS, CM, NA)
  --quarter Q2 \           # Fiscal quarter
  --year 2025 \            # Fiscal year
  --output custom.html     # Optional: custom output path
```

## Data Sources

| Source | Data Retrieved |
|--------|----------------|
| **Supp Pack** | Dividend, key metrics, 8Q trends, segment metrics |
| **RTS** | Segment performance drivers, items of note |
| **Transcripts** | Management quotes, analyst Q&A, overview themes |

## Report Sections

1. **Header** - Bank name, period, dividend
2. **Key Metrics** - 6 metric tiles + 8Q trend chart
3. **Overview** - Quarter narrative from transcripts
4. **Items of Note** - Significant $ impact events
5. **Management Narrative** - Executive quotes
6. **Analyst Focus** - Top Q&A themes ranked by importance
7. **Segment Performance** - 5 business segments with metrics and drivers
8. **Capital & Risk** - Regulatory capital metrics (placeholder)

## Configuration

Edit `config/etl_config.yaml` to configure:
- LLM models per extraction task
- Temperature and max_tokens defaults
- Model tier mappings (small/medium/large)
