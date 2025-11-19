# ETL Orchestrator - Automated Report Generation

## Overview

The ETL Orchestrator (`etl_orchestrator.py`) is an automated scheduler that monitors the `aegis_data_availability` table and generates missing reports for monitored financial institutions. It's designed to run every 15 minutes to catch new transcript data and automatically generate Call Summary and Key Themes reports.

## Key Features

### 1. **Parallel Execution**
- Processes multiple banks in parallel (configurable, default: 4)
- Within each bank, ETLs run sequentially (call_summary → key_themes)
- Maximizes throughput while maintaining data consistency

### 2. **Intelligent Gap Detection**
- Compares `aegis_data_availability` vs `aegis_reports` tables
- Only processes banks from monitored_institutions.yaml (Canadian & US banks)
- Identifies missing reports by (bank_id, fiscal_year, quarter, report_type)

### 3. **Retry Logic with Exponential Backoff**
- Automatic retry on failures (default: 3 attempts)
- Exponential backoff delays: 5s → 10s → 20s (max 5 minutes)
- Continues processing other banks after max retries

### 4. **Execution Lock**
- File-based lock prevents concurrent runs
- If previous run is still executing, next scheduled run waits
- Lock file: `/tmp/aegis_etl_orchestrator.lock`

### 5. **Comprehensive Logging**
- Real-time progress tracking
- Success/failure status for each ETL
- Detailed error messages and retry attempts
- Execution summary with statistics

## Usage

### Basic Commands

```bash
# Full run - process all gaps
python scripts/etl_orchestrator.py

# Dry run - preview gaps without executing
python scripts/etl_orchestrator.py --dry-run

# Process specific bank
python scripts/etl_orchestrator.py --bank-symbol RY

# Process only call summaries
python scripts/etl_orchestrator.py --etl-type call_summary

# Process only key themes
python scripts/etl_orchestrator.py --etl-type key_themes

# Increase parallel workers
python scripts/etl_orchestrator.py --max-parallel 8

# Disable lock (for testing only - allows concurrent runs)
python scripts/etl_orchestrator.py --no-lock
```

### Command-Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--dry-run` | Preview gaps without executing ETLs | False |
| `--bank-symbol` | Process specific bank only (e.g., RY, JPM) | All banks |
| `--etl-type` | Process specific ETL type (call_summary or key_themes) | All types |
| `--max-parallel` | Maximum parallel bank processes | 4 |
| `--no-lock` | Disable execution lock (testing only) | False |

## Architecture

### Workflow

```
1. Acquire Execution Lock
   └─ Blocks if previous run still executing

2. Load Monitored Institutions
   └─ From src/aegis/etls/call_summary/config/monitored_institutions.yaml
   └─ Maps YAML keys (RY-CA) to database symbols (RY)

3. Query Data Availability
   └─ SELECT bank_id, fiscal_year, quarter FROM aegis_data_availability
   └─ WHERE 'transcripts' = ANY(database_names)

4. Query Existing Reports
   └─ SELECT bank_id, fiscal_year, quarter, report_type FROM aegis_reports
   └─ WHERE report_type IN ('Call Summary', 'Key Themes')

5. Identify Gaps
   └─ For each (bank, year, quarter) with transcript data:
       - Missing Call Summary? → Queue
       - Missing Key Themes? → Queue

6. Execute ETLs in Parallel
   └─ Group by bank-period
   └─ Process up to MAX_PARALLEL banks simultaneously
   └─ Within each bank: call_summary → key_themes (sequential)
   └─ Retry failed jobs with exponential backoff

7. Print Execution Summary
   └─ Total jobs, successes, failures
   └─ Duration and error details

8. Release Execution Lock
```

### Database Schema Dependencies

#### aegis_data_availability
```sql
CREATE TABLE aegis_data_availability (
    bank_id INTEGER,
    bank_name VARCHAR,
    bank_symbol VARCHAR,  -- Without country suffix (RY, BMO, JPM, etc.)
    fiscal_year INTEGER,
    quarter VARCHAR,
    database_names TEXT[]  -- Must include 'transcripts'
);
```

#### aegis_reports
```sql
CREATE TABLE aegis_reports (
    bank_id INTEGER,
    fiscal_year INTEGER,
    quarter VARCHAR,
    report_type VARCHAR,  -- 'Call Summary' or 'Key Themes'
    UNIQUE(bank_id, fiscal_year, quarter, report_type)
);
```

### Monitored Institutions YAML

**Location**: `src/aegis/etls/call_summary/config/monitored_institutions.yaml`

**Format**:
```yaml
# Canadian Banks
RY-CA: {id: 1, name: "Royal Bank of Canada", type: "Canadian_Banks"}
BMO-CA: {id: 2, name: "Bank of Montreal", type: "Canadian_Banks"}

# US Banks
JPM-US: {id: 8, name: "JPMorgan Chase & Co.", type: "US_Banks"}
BAC-US: {id: 9, name: "Bank of America Corporation", type: "US_Banks"}
```

**Note**: The orchestrator automatically strips country suffixes (`RY-CA` → `RY`) to match database symbols.

## Scheduling

### Recommended Setup

**Cron job to run every 15 minutes:**

```bash
# Edit crontab
crontab -e

# Add this line (adjust paths)
*/15 * * * * cd /path/to/aegis && source venv/bin/activate && python scripts/etl_orchestrator.py >> /var/log/aegis_orchestrator.log 2>&1
```

**Systemd timer (recommended for production):**

Create `/etc/systemd/system/aegis-orchestrator.service`:
```ini
[Unit]
Description=Aegis ETL Orchestrator
After=network.target postgresql.service

[Service]
Type=oneshot
User=aegis
WorkingDirectory=/path/to/aegis
Environment="PATH=/path/to/aegis/venv/bin:/usr/bin"
ExecStart=/path/to/aegis/venv/bin/python scripts/etl_orchestrator.py
StandardOutput=journal
StandardError=journal
```

Create `/etc/systemd/system/aegis-orchestrator.timer`:
```ini
[Unit]
Description=Run Aegis ETL Orchestrator every 15 minutes

[Timer]
OnBootSec=5min
OnUnitActiveSec=15min
AccuracySec=1min

[Install]
WantedBy=timers.target
```

Enable and start:
```bash
sudo systemctl daemon-reload
sudo systemctl enable aegis-orchestrator.timer
sudo systemctl start aegis-orchestrator.timer
```

### Preventing Concurrent Runs

The orchestrator uses a file-based lock (`/tmp/aegis_etl_orchestrator.lock`) to prevent concurrent execution. If the previous run is still in progress when the next scheduled run starts:

1. The new process will **block** at lock acquisition
2. Once the first run completes and releases the lock
3. The second run proceeds immediately
4. No duplicate processing occurs

**This happens automatically** - no additional configuration needed on the server side.

## Configuration

### Environment Variables

The orchestrator inherits configuration from the main Aegis `.env` file:

```bash
# PostgreSQL connection
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=finance-dev
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

# Logging
LOG_LEVEL=INFO
```

### Script Constants

Edit `etl_orchestrator.py` to adjust:

```python
# Lock file location
LOCK_FILE_PATH = "/tmp/aegis_etl_orchestrator.lock"

# Retry configuration
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 5  # seconds
MAX_RETRY_DELAY = 300    # 5 minutes

# Parallel execution
MAX_PARALLEL_ETLS = 4    # Number of banks to process simultaneously
```

## ETL Coverage

### Included ETLs

| ETL Type | Report Type | Module Path |
|----------|-------------|-------------|
| `call_summary` | "Call Summary" | `aegis.etls.call_summary.main` |
| `key_themes` | "Key Themes" | `aegis.etls.key_themes.main` |

### Excluded ETLs

These are run ad-hoc and not included in automated scheduling:

- **cm_readthrough**: Corporate & Commercial banking readthrough (once per quarter)
- **wm_readthrough**: Wealth Management readthrough (once per quarter)
- **quarterly_newsletter**: De-scoped

## Output Examples

### Dry Run Output

```
================================================================================
ETL ORCHESTRATOR EXECUTION SUMMARY
================================================================================
Timestamp: 2025-11-17T12:58:14.248158
Monitored Institutions: 14 banks
Data Availability: 30 bank-period combinations with transcripts
Existing Reports: 0 reports

📋 GAPS IDENTIFIED: 56 missing reports
--------------------------------------------------------------------------------

Royal Bank of Canada (RY):
  • 2025 Q2: call_summary
  • 2025 Q2: key_themes
  • 2025 Q1: call_summary
  • 2025 Q1: key_themes

Bank of Montreal (BMO):
  • 2025 Q2: call_summary
  • 2025 Q2: key_themes

...

================================================================================
```

### Execution Output

```
[INFO] Starting parallel execution of 56 ETL jobs (max 4 parallel)
[INFO] Grouped into 28 bank-period combinations
[INFO] Executing RY 2025 Q2 [call_summary] (attempt 1/3)
[INFO] ✅ Success RY 2025 Q2 [call_summary] in 127.3s
[INFO] Executing RY 2025 Q2 [key_themes] (attempt 1/3)
[ERROR] ❌ Failed BMO 2025 Q1 [key_themes] (attempt 1): Connection timeout
[INFO] Retrying in 5s...
[INFO] ✅ Success RY 2025 Q2 [key_themes] in 89.1s

--------------------------------------------------------------------------------
EXECUTION RESULTS
--------------------------------------------------------------------------------
Total Jobs: 56
Successful: 54 ✅
Failed: 2 ❌
Duration: 3247.8s

Failed Jobs:
  ❌ BMO 2025 Q1 [key_themes]: Max retries exceeded - Connection timeout
  ❌ GS 2025 Q2 [call_summary]: No transcript data found
================================================================================
```

## Monitoring & Troubleshooting

### Log Files

Logs are output to stdout/stderr and can be redirected:

```bash
# Log to file
python scripts/etl_orchestrator.py >> orchestrator.log 2>&1

# View logs with systemd
sudo journalctl -u aegis-orchestrator.service -f
```

### Check Lock Status

```bash
# Check if orchestrator is running
ls -l /tmp/aegis_etl_orchestrator.lock

# View lock contents (PID and start time)
cat /tmp/aegis_etl_orchestrator.lock
```

### Common Issues

#### 1. No gaps found but reports are missing

**Cause**: Bank symbols don't match between database and YAML

**Solution**: Check symbols in database vs monitored_institutions.yaml
```bash
python -c "
import asyncio
from aegis.connections.postgres_connector import get_connection
from sqlalchemy import text

async def check():
    async with get_connection() as conn:
        result = await conn.execute(text('SELECT DISTINCT bank_symbol FROM aegis_data_availability'))
        print([row.bank_symbol for row in result.fetchall()])

asyncio.run(check())
"
```

#### 2. Orchestrator hangs at lock acquisition

**Cause**: Previous run is still executing or lock file is stale

**Solution**:
```bash
# Check if process is running
cat /tmp/aegis_etl_orchestrator.lock  # Shows PID
ps aux | grep etl_orchestrator

# If process is dead, remove stale lock
rm /tmp/aegis_etl_orchestrator.lock
```

#### 3. All ETLs failing with same error

**Cause**: System-wide issue (database connection, authentication)

**Solution**: Test individual ETL manually:
```bash
python -m aegis.etls.call_summary.main --bank RY --year 2025 --quarter Q2
```

#### 4. High failure rate

**Cause**: May need more retries or longer backoff

**Solution**: Adjust retry configuration in script:
```python
MAX_RETRIES = 5
MAX_RETRY_DELAY = 600  # 10 minutes
```

## Performance Tuning

### Parallel Workers

Adjust based on system resources:

```bash
# Conservative (low CPU/memory)
python scripts/etl_orchestrator.py --max-parallel 2

# Aggressive (high-end server)
python scripts/etl_orchestrator.py --max-parallel 8
```

**Considerations**:
- Each worker processes one bank at a time
- Each ETL spawns its own LLM calls (high memory usage)
- Database connection pool: 20 + 40 overflow
- Recommended: 1 worker per 4GB RAM available

### Database Connection Pool

Edit `src/aegis/connections/postgres_connector.py`:

```python
_async_engine = create_async_engine(
    database_url,
    pool_size=30,      # Increase for more parallel workers
    max_overflow=60,   # 2x pool size
    ...
)
```

## Testing

### Test Orchestrator Logic

```bash
# Dry run - no ETLs executed
python scripts/etl_orchestrator.py --dry-run

# Single bank test
python scripts/etl_orchestrator.py --bank-symbol RY --dry-run

# Single ETL type test
python scripts/etl_orchestrator.py --etl-type call_summary --dry-run
```

### Test Without Lock

Useful for development/testing:

```bash
# Run in one terminal
python scripts/etl_orchestrator.py --no-lock

# Run in another terminal simultaneously
python scripts/etl_orchestrator.py --no-lock
```

### Test Individual ETL

Before adding to orchestrator, test ETL manually:

```bash
# Call Summary
python -m aegis.etls.call_summary.main --bank RY --year 2025 --quarter Q2

# Key Themes
python -m aegis.etls.key_themes.main --bank RY --year 2025 --quarter Q2
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success - all jobs completed successfully |
| 1 | Partial failure - some jobs failed after retries |
| 1 | System error - orchestrator failed to execute |

Use in scripts:

```bash
if python scripts/etl_orchestrator.py; then
    echo "All reports generated successfully"
else
    echo "Some reports failed - check logs"
fi
```

## Future Enhancements

### Potential Improvements

1. **Database-driven configuration**: Store retry params in database
2. **Priority queue**: Prioritize certain banks/periods
3. **Email notifications**: Alert on failures
4. **Metrics dashboard**: Real-time monitoring
5. **Smart scheduling**: Adjust frequency based on data arrival patterns
6. **Partial report handling**: Resume failed reports instead of rerunning
7. **Cost tracking**: Monitor LLM usage and costs

## Support

For issues or questions:

1. Check logs for error details
2. Test individual ETL manually
3. Review database connectivity
4. Verify monitored_institutions.yaml format
5. Ensure transcript data exists in aegis_data_availability

---

**Last Updated**: 2025-11-17
**Version**: 1.0.0
