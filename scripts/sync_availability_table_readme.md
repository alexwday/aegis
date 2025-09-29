# Data Availability Sync Script

## Overview

The `sync_availability_table.py` script maintains the `aegis_data_availability` table by syncing data from multiple PostgreSQL source tables. It uses the `monitored_institutions.yaml` file as the definitive source for all bank information, ensuring consistency across all data sources.

When the script processes a source table:
1. It reads bank identifiers from your table (ID, name, or symbol)
2. Looks up complete bank details in `monitored_institutions.yaml`
3. Checks if that bank/period exists in `aegis_data_availability`
4. If it exists → adds your tag to the existing record
5. If it doesn't exist → creates a new record with full details from YAML + your tag

## Table Configuration

Configuration is done directly in the script by editing the `TABLE_CONFIGS` section at the top. Each table configuration requires:

- **table_name**: Your PostgreSQL table name
- **bank_id_type**: How your table identifies banks ('id', 'name', or 'symbol')
- **bank_id_field**: The column containing the bank identifier
- **year_field**: The column with fiscal year
- **quarter_field**: The column with quarter (Q1, Q2, Q3, Q4)
- **tag**: Tag to add in aegis_data_availability
- **enabled**: Set to False to skip this table

### Example: aegis_transcripts (uses institution_id)
```python
TableConfig(
    table_name="aegis_transcripts",
    bank_id_type="id",
    bank_id_field="institution_id",
    year_field="fiscal_year",
    quarter_field="fiscal_quarter",
    tag="transcripts",
    enabled=True
)
```

### Example: aegis_reports (uses bank_id)
```python
TableConfig(
    table_name="aegis_reports",
    bank_id_type="id",
    bank_id_field="bank_id",
    year_field="fiscal_year",
    quarter_field="quarter",
    tag="reports",
    enabled=True
)
```

### Example: aegis_rts (uses bank names)
```python
TableConfig(
    table_name="aegis_rts",
    bank_id_type="name",
    bank_id_field="institution_name",
    year_field="report_year",
    quarter_field="report_quarter",
    tag="rts",
    enabled=True
)
```

### Example: aegis_pillar3 (uses bank symbols)
```python
TableConfig(
    table_name="aegis_pillar3",
    bank_id_type="symbol",
    bank_id_field="bank_ticker",
    year_field="fiscal_year",
    quarter_field="fiscal_quarter",
    tag="pillar3",
    enabled=True
)
```

### Example: aegis_supplementary (uses bank symbols)
```python
TableConfig(
    table_name="aegis_supplementary",
    bank_id_type="symbol",
    bank_id_field="ticker",
    year_field="year",
    quarter_field="period",
    tag="supplementary",
    enabled=True
)
```

## Running the Script

The script supports different modes of operation:

- **Update Mode (default)**: Adds/removes tags as needed based on current source data
- **Rebuild Mode**: Clears all instances of configured tags and rebuilds from source
- **Verify Only**: Shows current state without making changes
- **Dry Run**: Preview changes without modifying the database

### Commands
```bash
# Standard sync (update mode)
python scripts/sync_availability_table.py

# Preview changes without making them
python scripts/sync_availability_table.py --dry-run

# Clear and rebuild all configured tags
python scripts/sync_availability_table.py --mode rebuild

# Just display current state
python scripts/sync_availability_table.py --verify-only

# Combine options
python scripts/sync_availability_table.py --mode rebuild --dry-run
```

## Troubleshooting

### Unknown Bank Identifiers
- Check if the bank exists in `monitored_institutions.yaml`
- Banks not in the YAML file will be skipped with a warning

### Database Connection Issues
- Verify `.env` file has correct database credentials
- Check network connectivity to database

### Field Not Found
- Ensure column names exactly match your PostgreSQL table
- Use this query to verify column names:
  ```sql
  SELECT column_name, data_type
  FROM information_schema.columns
  WHERE table_name = 'your_table';
  ```