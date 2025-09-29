# Call Summary ETL

## How to Run

```bash
# Activate virtual environment
source venv/bin/activate

# Run call summary
python -m aegis.etls.call_summary.main --bank "Royal Bank of Canada" --year 2025 --quarter Q2
```

## Parameters
- `--bank`: Bank name (e.g., "Royal Bank of Canada")
- `--year`: Fiscal year (e.g., 2025)
- `--quarter`: Quarter (Q1, Q2, Q3, or Q4)

## Output
Creates a Word document in `src/aegis/etls/call_summary/output/` with the summary.

## Example
```bash
python -m aegis.etls.call_summary.main --bank "Royal Bank of Canada" --year 2024 --quarter Q3
```