# Key Themes ETL

## How to Run

```bash
# Activate virtual environment
source venv/bin/activate

# Run key themes extraction
python -m aegis.etls.key_themes.main --bank "Royal Bank of Canada" --year 2024 --quarter Q3
```

## Parameters
- `--bank`: Bank name (e.g., "Royal Bank of Canada")
- `--year`: Fiscal year (e.g., 2024)
- `--quarter`: Quarter (Q1, Q2, Q3, or Q4)
- `--no-pdf`: Optional flag to skip PDF generation

## Output
Creates a Word document in `src/aegis/etls/key_themes/output/` with extracted themes.

## Example
```bash
# With PDF generation
python -m aegis.etls.key_themes.main --bank "Royal Bank of Canada" --year 2024 --quarter Q3

# Without PDF generation
python -m aegis.etls.key_themes.main --bank "Royal Bank of Canada" --year 2024 --quarter Q3 --no-pdf
```