## How to Run

The CM readthrough editor ETL can be executed directly via command line for a specific bank/quarter, or run across all monitored institutions for a consolidated report.

| Method | Command | Description |
|--------|---------|-------------|
| **Direct Command Line** | `python -m aegis.etls.cm_readthrough_editor --bank RY --year 2025 --quarter Q2` | Run the ETL for one bank and quarter directly from the command line. |
| **Consolidated Run** | `python -m aegis.etls.cm_readthrough_editor --year 2025 --quarter Q2` | Run the ETL across all monitored banks and build one consolidated editor HTML report. |
| **Recall Benchmark** | `python -m aegis.etls.cm_readthrough_editor benchmark --predicted output/CM_Readthrough_Editor_2025_Q2_all_banks.html --expected expected_items.json` | Score predicted evidence against analyst-reviewed expectations and report recall / miss reasons. |
| **Orchestrator Scheduling** | `python scripts/etl_orchestrator.py` | Automatically process all monitored institutions defined in `config/monitored_institutions.yaml`. |

### CLI Options

| Option | Required | Type | Description |
|--------|----------|------|-------------|
| `--bank` | No | string | Optional bank identifier — accepts bank ID, full name (e.g., `"Royal Bank of Canada"`), or symbol (e.g., `RY`) |
| `--year` | Yes | int | Fiscal year |
| `--quarter` | Yes | choice | Quarter: `Q1`, `Q2`, `Q3`, or `Q4` |
| `--preflight` | No | flag | Validate category config, NAS resolution, and XML parseability without calling the LLM |
| `benchmark --predicted` | Benchmark only | path | Saved report HTML or JSON payload containing predicted evidence |
| `benchmark --expected` | Benchmark only | path | Analyst-reviewed JSON expectations |
| `benchmark --format` | No | choice | Benchmark output format: `markdown` or `json` |
| `benchmark --output` | No | path | Optional output file for the benchmark report |

### Examples

```bash
# Run by bank symbol
python -m aegis.etls.cm_readthrough_editor --bank RY --year 2025 --quarter Q3

# Run all monitored banks
python -m aegis.etls.cm_readthrough_editor --year 2025 --quarter Q3

# Run by full bank name
python -m aegis.etls.cm_readthrough_editor --bank "Royal Bank of Canada" --year 2025 --quarter Q3

# Run by bank ID
python -m aegis.etls.cm_readthrough_editor --bank 1 --year 2025 --quarter Q3

# Validate NAS/XML availability without LLM calls
python -m aegis.etls.cm_readthrough_editor --preflight --year 2025 --quarter Q3

# Benchmark a saved report against analyst-reviewed expectations
python -m aegis.etls.cm_readthrough_editor benchmark \
  --predicted output/CM_Readthrough_Editor_2025_Q3_all_banks.html \
  --expected expected_items.json \
  --format markdown
```


## Inputs

The CM readthrough editor ETL resolves transcript XML from NAS, parses raw speaker blocks, and generates an interactive multi-bank HTML review file while still using Aegis tables for final report persistence.

| Input | Location | Description |
|-------|----------|-------------|
| **Transcript XML** | NAS share | Raw FactSet earnings transcript XML resolved from `bank/year/quarter` using institution type and `path_safe_name` |
| **config.yaml** | `config/` | LLM model tiers and parameters (temperature, max_tokens) |
| **monitored_institutions.yaml** | `config/` | Institution metadata including id, name, bank type, full ticker, and NAS `path_safe_name` |
| **outlook_categories.xlsx** | `config/categories/` | Outlook category definitions for the first report section |
| **qa_categories.xlsx** | `config/categories/` | Flat Q&A category definitions for the merged second report section |
| **NAS environment variables** | Runtime env | SMB credentials and base path (`NAS_USERNAME`, `NAS_PASSWORD`, `NAS_SERVER_IP`, `NAS_SERVER_NAME`, `NAS_SHARE_NAME`, `NAS_BASE_PATH`, `CLIENT_MACHINE_NAME`) plus optional transcript-root override `CALL_SUMMARY_NAS_DATA_PATH` |


## Process

The ETL transforms raw transcript XML into an interactive HTML editor through six sequential stages.

| Stage | Purpose | Sub-steps | Output |
|-------|---------|-----------|--------|
| **1. Setup & Validation** | Resolve bank metadata and prepare execution environment before transcript retrieval | • `get_bank_info_from_config()`: Resolve bank by id/name/symbol<br>• `load_categories_from_xlsx()`: Load Outlook + flat Q&A category Excel configs<br>• `setup_authentication()` + `setup_ssl()`: Prepare LLM auth and SSL config | Establishes secure API connections and category config before attempting NAS retrieval |
| **2. Transcript Source Resolution** | Resolve and download the best raw transcript XML for the requested bank/period directly from NAS | • `get_nas_connection()`: Open SMB session<br>• `find_transcript_xml()`: Build NAS path from year/quarter/type/`path_safe_name`, choose best filename version<br>• `nas_download_file()`: Load XML bytes | NAS is the authoritative availability check: if the XML is not present here, the ETL fails for that bank-period |
| **3. XML Parsing & Block Extraction** | Convert XML into structured speaker metadata and ordered transcript blocks | • `parse_transcript_xml()`: Extract title, participants, and section structure<br>• `extract_raw_blocks()`: Build MD speaker blocks and raw QA blocks with speaker/title/affiliation/type hints | Produces source-agnostic structured transcript data that can later come from NAS or S3 |
| **4. QA Boundary Detection** | Group raw Q&A speaker turns into analyst-to-management conversations | • `detect_qa_boundaries()`: Group indexed QA speaker blocks into ordered conversations using the same method as `call_summary_editor` | Produces stable Q&A conversations before extraction |
| **5. CM Extraction** | Extract capital-markets Outlook findings and capital-markets analyst questions | • bank-level Outlook extraction over all MD blocks + full QA conversations in order<br>• bank-level Q&A extraction over all QA conversations<br>• sentence-id + source-block-id linking back to transcript state | Produces transcript-grounded CM findings for each bank |
| **6. HTML Generation & Persistence** | Render the interactive report and persist metadata for downstream retrieval | • `build_report_state()`: Build the CM editor client state JSON<br>• `generate_html()`: Inject state into the HTML template<br>• `_save_interactive_report_to_database()`: Replace existing row and insert HTML report metadata into `aegis_reports` | Generates interactive HTML output and a database record that points downstream consumers to the report |


## Output

The ETL generates an interactive HTML editor file and, for full-scope runs, a corresponding database record stored in `aegis_reports`.

| Output | Location | Description |
|--------|----------|-------------|
| **HTML File** | `output/CM_Readthrough_Editor_[YEAR]_[QUARTER]_[SCOPE].html` | Interactive consolidated CM readthrough editor HTML file |
| **Database Record** | `aegis_reports` table | Report metadata including generation timestamp, execution_id, output format, requested banks, and category counts |
| **Benchmark Report** | stdout or user-supplied `--output` path | Recall / miss-reason report comparing saved editor state with analyst-reviewed expectations |


## Dependencies

The ETL leverages core Aegis infrastructure for database access, LLM operations, configuration management, and logging.

| Module | Import | Description |
|--------|--------|-------------|
| **Connections** | `aegis.connections.postgres_connector` | PostgreSQL database connection and query execution |
| **Connections** | `aegis.connections.llm_connector` | OpenAI API interface with function calling support |
| **Connections** | `aegis.connections.oauth_connector` | OAuth 2.0 authentication for API access |
| **Utils** | `aegis.utils.logging` | Structured logging with execution tracking and colored output |
| **Utils** | `aegis.utils.ssl` | SSL certificate configuration for secure API connections |
| **ETL** | `aegis.etls.prompt_schema` | Standard-schema prompt loader for ETL-local YAML prompt bundles |
| **Utils** | `aegis.utils.settings` | Singleton configuration management with .env file support |
