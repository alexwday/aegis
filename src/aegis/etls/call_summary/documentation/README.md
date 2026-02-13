## How to Run

The Call Summary ETL can be executed directly via command line for a specific bank/quarter, or scheduled via the orchestrator to process all monitored institutions automatically.

| Method | Command | Description |
|--------|---------|-------------|
| **Direct Command Line** | `python -m aegis.etls.call_summary.main --bank RY --year 2025 --quarter Q2` | Run the ETL for a specific bank and quarter directly from the command line. |
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
python -m aegis.etls.call_summary.main --bank RY --year 2025 --quarter Q3

# Run by full bank name
python -m aegis.etls.call_summary.main --bank "Royal Bank of Canada" --year 2025 --quarter Q3

# Run by bank ID
python -m aegis.etls.call_summary.main --bank 1 --year 2025 --quarter Q3
```


## Inputs

The ETL requires transcript data from the database and configuration files defining categories and LLM parameters.

| Input | Location | Description |
|-------|----------|-------------|
| **aegis_transcripts table** | PostgreSQL | Parsed and chunked earnings call transcripts (MD and Q&A sections) |
| **prompts table** | PostgreSQL | LLM prompts: `research_plan` and `category_extraction` (layer=call_summary_etl) |
| **config.yaml** | `config/` | LLM model tiers and parameters (temperature, max_tokens) |
| **monitored_institutions.yaml** | `config/` | Institution metadata (id, name, type) for bank type classification and orchestrator processing |
| **canadian_banks_categories.xlsx** | `config/categories/` | Category definitions for Canadian banks |
| **us_banks_categories.xlsx** | `config/categories/` | Category definitions for US banks |


## Process

The ETL transforms raw transcript data into structured call summaries through five sequential stages.

| Stage | Purpose | Sub-steps | Output |
|-------|---------|-----------|--------|
| **1. Setup & Validation** | Validate inputs and prepare execution environment before expensive LLM operations | • `get_bank_info()`: Query `aegis_data_availability` by name/symbol/ID<br>• `verify_data_availability()`: Check transcripts exist for bank-period<br>• `get_bank_type()`: Look up bank type from `monitored_institutions.yaml`<br>• `load_categories_from_xlsx()`: Load Canadian/US category Excel based on bank type<br>• `setup_authentication()` + `setup_ssl()`: OAuth token and certificates | Ensures valid bank-period combination exists and establishes secure API connections, preventing wasted compute on invalid requests |
| **2. Transcript Retrieval** | Retrieve and format raw transcript data to provide complete context for LLM analysis | • `retrieve_full_section(sections="ALL")`: Query `aegis_transcripts` WHERE bank_id, fiscal_year, quarter<br>• `format_full_section_chunks()`: Group by speaker_block_id/qa_group_id, concatenate chunks | Provides full MD and Q&A transcript text needed for intelligent category planning and accurate content extraction |
| **3. Research Planning** | Determine which categories apply and create targeted extraction strategies with deduplication guidance | • `load_prompt_from_db(layer="call_summary_etl", name="research_plan")`<br>• `complete_with_tools()`: LLM function call with categories list<br>• Returns `category_plans[]` with extraction_strategy and cross_category_notes (only for applicable categories) | Reduces unnecessary LLM calls by filtering non-applicable categories and establishes cross-category deduplication framework to prevent content overlap |
| **4. Category Extraction** | Transform unstructured transcript text into organized summary statements with supporting evidence | • Loop categories: `retrieve_full_section(sections=category["transcripts_section"])` where section = MD/QA/ALL per category Excel<br>• Build `previous_summary` and `extracted_themes` from prior categories<br>• `complete_with_tools()`: LLM call with deduplication context<br>• Parse tool response: summary_statements[] with evidence[] | Produces structured category insights while dynamically preventing duplication of themes already covered in earlier categories |
| **5. Document Generation** | Create formatted deliverables and persist results for downstream consumption | • `_generate_document()`: Create DOCX with banner, TOC, section headings<br>• Sort by report_section, add content via `add_structured_content_to_doc()`<br>• `_save_to_database()`: DELETE existing, INSERT into `aegis_reports` | Generates both human-readable Word documents for manual review and structured database records for programmatic access by Reports subagent |


## Output

The ETL generates both a formatted Word document and a database record stored in `aegis_reports` for downstream consumption.

| Output | Location | Description |
|--------|----------|-------------|
| **DOCX File** | `output/[SYMBOL]_[YEAR]_[QUARTER].docx` | Formatted Word document with banner, table of contents, and category sections organized by report section |
| **Database Record** | `aegis_reports` table | Full report metadata including bank info, generation timestamp, and JSON metadata (categories processed/included/rejected) |


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
