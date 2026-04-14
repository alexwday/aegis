## How to Run

The Call Summary ETL can be executed directly via command line for a specific bank/quarter, or scheduled via the orchestrator to process all monitored institutions automatically.

| Method | Command | Description |
|--------|---------|-------------|
| **Direct Command Line** | `python -m aegis.etls.call_summary_editor.main --bank RY --year 2025 --quarter Q2` | Run the ETL for a specific bank and quarter directly from the command line. |
| **Orchestrator Scheduling** | `python scripts/etl_orchestrator.py` | Automatically process all monitored institutions defined in `config/monitored_institutions.yaml`. |

### CLI Options

| Option | Required | Type | Description |
|--------|----------|------|-------------|
| `--bank` | Yes | string | Bank identifier — accepts bank ID, full name (e.g., `"Royal Bank of Canada"`), or symbol (e.g., `RY`) |
| `--year` | Yes | int | Fiscal year |
| `--quarter` | Yes | choice | Quarter: `Q1`, `Q2`, `Q3`, or `Q4` |

### Examples

```bash
# Run by bank symbol
python -m aegis.etls.call_summary_editor.main --bank RY --year 2025 --quarter Q3

# Run by full bank name
python -m aegis.etls.call_summary_editor.main --bank "Royal Bank of Canada" --year 2025 --quarter Q3

# Run by bank ID
python -m aegis.etls.call_summary_editor.main --bank 1 --year 2025 --quarter Q3
```


## Inputs

The Call Summary Editor ETL resolves transcript XML from NAS, parses raw speaker blocks, and generates an interactive HTML review file while still using Aegis tables for availability checks and final report persistence.

| Input | Location | Description |
|-------|----------|-------------|
| **Transcript XML** | NAS share | Raw FactSet earnings transcript XML resolved from `bank/year/quarter` using institution type and `path_safe_name` |
| **config.yaml** | `config/` | LLM model tiers and parameters (temperature, max_tokens) |
| **monitored_institutions.yaml** | `config/` | Institution metadata including id, name, bank type, full ticker, and NAS `path_safe_name` |
| **canadian_banks_categories.xlsx** | `config/categories/` | Category definitions for Canadian banks |
| **us_banks_categories.xlsx** | `config/categories/` | Category definitions for US banks |
| **NAS environment variables** | Runtime env | SMB credentials and base path (`NAS_USERNAME`, `NAS_PASSWORD`, `NAS_SERVER_IP`, `NAS_SERVER_NAME`, `NAS_SHARE_NAME`, `NAS_BASE_PATH`, `CLIENT_MACHINE_NAME`) |


## Process

The ETL transforms raw transcript XML into an interactive HTML editor through five sequential stages.

| Stage | Purpose | Sub-steps | Output |
|-------|---------|-----------|--------|
| **1. Setup & Validation** | Resolve bank metadata and prepare execution environment before transcript retrieval | • `get_bank_info_from_config()`: Resolve bank by id/name/symbol<br>• `load_categories_from_xlsx()`: Load Canadian/US category Excel based on bank type<br>• `setup_authentication()` + `setup_ssl()`: Prepare LLM auth and SSL config | Establishes secure API connections and category config before attempting NAS retrieval |
| **2. Transcript Source Resolution** | Resolve and download the best raw transcript XML for the requested bank/period directly from NAS | • `get_nas_connection()`: Open SMB session<br>• `find_transcript_xml()`: Build NAS path from year/quarter/type/`path_safe_name`, choose best filename version<br>• `nas_download_file()`: Load XML bytes | NAS is the authoritative availability check: if the XML is not present here, the ETL fails for that bank-period |
| **3. XML Parsing & Block Extraction** | Convert XML into structured speaker metadata and ordered transcript blocks | • `parse_transcript_xml()`: Extract title, participants, and section structure<br>• `extract_raw_blocks()`: Build MD speaker blocks and raw QA blocks with speaker/title/affiliation/type hints | Produces source-agnostic structured transcript data that can later come from NAS or S3 |
| **4. Interactive Classification** | Apply the mock editor workflow to classify transcript content and build the review state | • `detect_qa_boundaries()`: Group indexed QA speaker blocks into exchanges via a single tool call<br>• `classify_md_block()`: Sentence-level MD classification per paragraph<br>• `classify_qa_conversation()`: Question/answer classification per exchange<br>• `generate_bucket_headlines()`: Create section headlines | Produces mock-compatible bank state and bucket headlines for the interactive HTML editor |
| **5. HTML Generation & Persistence** | Render the interactive report and persist metadata for downstream retrieval | • `build_report_state()`: Build mock-compatible client state JSON<br>• `generate_html()`: Inject state into the HTML template copied from the mock ETL<br>• `_save_interactive_report_to_database()`: Replace existing row and insert HTML report metadata into `aegis_reports` | Generates interactive HTML output and a database record that points downstream consumers to the report |


## Output

The ETL generates an interactive HTML editor file and a corresponding database record stored in `aegis_reports`.

| Output | Location | Description |
|--------|----------|-------------|
| **HTML File** | `output/[FULL_TICKER]_[YEAR]_[QUARTER]_call_summary_editor.html` | Interactive transcript review and report-drafting HTML file using the mock editor UI |
| **Database Record** | `aegis_reports` table | Report metadata including bank info, generation timestamp, execution_id, output format, and category counts |


## Dependencies

The ETL leverages core Aegis infrastructure for database access, LLM operations, configuration management, and logging.

| Module | Import | Description |
|--------|--------|-------------|
| **Connections** | `aegis.connections.postgres_connector` | PostgreSQL database connection and query execution |
| **Connections** | `aegis.connections.llm_connector` | OpenAI API interface with function calling support |
| **Connections** | `aegis.connections.oauth_connector` | OAuth 2.0 authentication for API access |
| **Utils** | `aegis.utils.logging` | Structured logging with execution tracking and colored output |
| **Utils** | `aegis.utils.ssl` | SSL certificate configuration for secure API connections |
| **Utils** | `aegis.utils.prompt_loader` | Database-based prompt retrieval and loading system |
| **Utils** | `aegis.utils.settings` | Singleton configuration management with .env file support |
