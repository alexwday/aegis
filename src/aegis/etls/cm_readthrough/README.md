# CM Readthrough ETL

## Overview
Extracts Investment Banking & Trading outlook commentary from earnings call transcripts for monitored financial institutions and generates formatted Capital Markets readthrough reports.

## How to Run

```bash
# Activate virtual environment
source venv/bin/activate

# Run CM readthrough for specific period
python -m aegis.etls.cm_readthrough.main --year 2025 --quarter Q2 --no-pdf
```

## Parameters
- `--year`: Fiscal year (e.g., 2025)
- `--quarter`: Quarter (Q1, Q2, Q3, or Q4)
- `--use-latest`: Use latest available period data (optional)
- `--no-pdf`: Skip PDF generation (optional, requires LibreOffice otherwise)

## Output
Creates a Word document in `src/aegis/etls/cm_readthrough/output/` with:
- Main title: "Read Through For Capital Markets: Qx/2x Select U.S. & European Banks"
- LLM-generated subtitle based on extracted content
- IB & Trading Outlook table with stock tickers and key quotes
- Dark blue headers with white text, selective borders

## Monitored Institutions
Currently monitoring 14 banks (7 Canadian, 7 US) with mock data for 2025 Q1 & Q2.
See `config/monitored_institutions.yaml` for the full list.

## Example
```bash
python -m aegis.etls.cm_readthrough.main --year 2025 --quarter Q2 --no-pdf
```
