# ETL Orchestrator - Usage Guide

## Overview

The ETL orchestrator automatically identifies gaps between available transcript data and generated reports, then processes missing reports through the call_summary and key_themes ETL pipelines.

## Basic Usage

### Run with default settings
Processes all gaps across all monitored banks:
```bash
python scripts/etl_orchestrator.py
```

### Dry run (preview only)
See what would be processed without actually running ETLs:
```bash
python scripts/etl_orchestrator.py --dry-run
```

## Filtering Options

### Filter by Year
Only process data from a specific fiscal year forward (excludes historical data):
```bash
# Only process 2024 and later
python scripts/etl_orchestrator.py --from-year 2024

# Only process 2025 and later
python scripts/etl_orchestrator.py --from-year 2025
```

**Use case**: When you have historical transcript data in the database but only want to generate reports for recent years.

### Filter by Bank
Process a specific bank only:
```bash
# Process only Royal Bank of Canada
python scripts/etl_orchestrator.py --bank-symbol RY-CA

# With dry run
python scripts/etl_orchestrator.py --bank-symbol JPM-US --dry-run
```

### Filter by ETL Type
Process only one type of report:
```bash
# Only call summaries
python scripts/etl_orchestrator.py --etl-type call_summary

# Only key themes
python scripts/etl_orchestrator.py --etl-type key_themes
```

## Combined Filters

You can combine multiple filters:

```bash
# Only call summaries for RY from 2024 forward
python scripts/etl_orchestrator.py \
  --etl-type call_summary \
  --bank-symbol RY-CA \
  --from-year 2024

# Dry run: Preview key themes for all banks from 2025 forward
python scripts/etl_orchestrator.py \
  --etl-type key_themes \
  --from-year 2025 \
  --dry-run
```

## Advanced Options

### Control Parallel Execution
Limit the number of banks processed concurrently:
```bash
# Process up to 2 banks in parallel (default is 4)
python scripts/etl_orchestrator.py --max-parallel 2
```

### Disable Execution Lock (Testing Only)
Allows concurrent runs (not recommended for production):
```bash
python scripts/etl_orchestrator.py --no-lock
```

## Production Deployment

### Recommended Cron Schedule
Run every 15 minutes with year filter to only process recent data:
```bash
# /etc/crontab or crontab -e
*/15 * * * * /path/to/venv/bin/python /path/to/aegis/scripts/etl_orchestrator.py --from-year 2024 >> /var/log/aegis/orchestrator.log 2>&1
```

### Environment Variables
The orchestrator inherits all configuration from `.env`:
- Database credentials
- LLM API keys
- Model selections
- Logging levels

## Output Examples

### No Gaps Found
```
================================================================================
ETL ORCHESTRATOR EXECUTION SUMMARY
================================================================================
Timestamp: 2025-11-17T13:34:05.866420
Monitored Institutions: 14 banks
Data Availability: 1 bank-period combinations with transcripts
Existing Reports: 2 reports

✅ NO GAPS FOUND - All reports up to date!

================================================================================
```

### Gaps Identified
```
================================================================================
ETL ORCHESTRATOR EXECUTION SUMMARY
================================================================================
Timestamp: 2025-11-17T13:07:35.718634
Monitored Institutions: 14 banks
Data Availability: 5 bank-period combinations with transcripts
Existing Reports: 8 reports

📋 GAPS IDENTIFIED: 2 missing reports
--------------------------------------------------------------------------------

Royal Bank of Canada (RY):
  • 2025 Q2: call_summary
  • 2025 Q2: key_themes

================================================================================
```

### After Execution
```
--------------------------------------------------------------------------------
EXECUTION RESULTS
--------------------------------------------------------------------------------
Total Jobs: 2
Successful: 2 ✅
Failed: 0 ❌
Duration: 28.6s

================================================================================
```

## Exit Codes

- **0**: Success - All jobs completed successfully
- **1**: Failure - One or more jobs failed

## Troubleshooting

### No data found with --from-year
If you get "No transcript data available" when using `--from-year`, check:
```sql
-- Verify fiscal years in availability table
SELECT DISTINCT fiscal_year, COUNT(*)
FROM aegis_data_availability
WHERE 'transcripts' = ANY(database_names)
GROUP BY fiscal_year
ORDER BY fiscal_year DESC;
```

### Reports already exist
The orchestrator automatically skips existing reports. To reprocess:
```sql
-- Delete specific reports to force regeneration
DELETE FROM aegis_reports
WHERE bank_symbol = 'RY'
  AND fiscal_year = 2025
  AND quarter = 'Q2'
  AND report_type IN ('call_summary', 'key_themes');
```

### Execution lock errors
If the orchestrator hangs on "Attempting to acquire execution lock", another instance may be running:
```bash
# Check for running orchestrator
ps aux | grep etl_orchestrator

# Remove stale lock file (only if no process is running)
rm /tmp/aegis_etl_orchestrator.lock
```

## Help

View all options:
```bash
python scripts/etl_orchestrator.py --help
```

## Monitoring

### Check Logs
```bash
# Real-time monitoring
tail -f /var/log/aegis/orchestrator.log

# Search for errors
grep "ERROR" /var/log/aegis/orchestrator.log
```

### Database Monitoring
```sql
-- Check recent reports
SELECT bank_symbol, fiscal_year, quarter, report_type, generation_date
FROM aegis_reports
ORDER BY generation_date DESC
LIMIT 20;

-- Count reports by type
SELECT report_type, COUNT(*)
FROM aegis_reports
GROUP BY report_type;
```
