## How to Run

The Key Themes ETL can be executed directly via command line for a specific bank/quarter, or scheduled via the orchestrator to process all monitored institutions automatically.

| Method | Command | Description |
|--------|---------|-------------|
| **Direct Command Line** | `python -m aegis.etls.key_themes.main --bank RY --year 2025 --quarter Q2` | Run the ETL for a specific bank and quarter directly from the command line. |
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
python -m aegis.etls.key_themes.main --bank RY --year 2025 --quarter Q3

# Run by full bank name
python -m aegis.etls.key_themes.main --bank "Royal Bank of Canada" --year 2025 --quarter Q3

# Run by bank ID
python -m aegis.etls.key_themes.main --bank 1 --year 2025 --quarter Q3
```


## Inputs

The ETL requires transcript data from the database and configuration files defining categories and LLM parameters.

| Input | Location | Description |
|-------|----------|-------------|
| **aegis_transcripts table** | PostgreSQL | Parsed and chunked earnings call transcripts (Q&A section only) |
| **prompts table** | PostgreSQL | LLM prompts: `theme_extraction`, `html_formatting`, and `grouping` (layer=key_themes_etl) |
| **config.yaml** | `config/` | LLM model tiers and parameters (temperature, max_tokens) |
| **monitored_institutions.yaml** | `../call_summary/config/` | Institution metadata (id, name, type) for orchestrator processing |
| **key_themes_categories.xlsx** | `config/categories/` | Category definitions for theme classification |


## Process

The ETL transforms raw Q&A transcript data into organized theme groups through six sequential stages.

| Stage | Purpose | Sub-steps | Output |
|-------|---------|-----------|--------|
| **1. Setup & Validation** | Validate inputs and prepare execution environment before expensive LLM operations | • `get_bank_info()`: Query `aegis_data_availability` by name/symbol/ID<br>• `verify_data_availability()`: Check transcripts exist for bank-period<br>• `load_categories_from_xlsx()`: Load predefined category definitions from Excel<br>• `setup_authentication()` + `setup_ssl()`: OAuth token and certificates | Ensures valid bank-period combination exists and establishes secure API connections, preventing wasted compute on invalid requests |
| **2. Q&A Retrieval** | Retrieve and organize raw Q&A blocks to create indexed structure for sequential processing | • `load_qa_blocks()`: Uses `retrieve_full_section(sections="QA")` from transcript_utils<br>• Group chunks by `qa_group_id`, concatenate into complete Q&A exchanges<br>• Create indexed dictionary with `QABlock` objects with standardized retrieval logic | Provides indexed Q&A content ready for independent classification, enabling efficient sequential processing with context awareness |
| **3. Sequential Classification** | Validate Q&A relevance and classify into predefined categories with cumulative context to ensure consistency | • `classify_all_qa_blocks_sequential()`: Process each Q&A in order<br>• `load_prompt_from_db(layer="key_themes_etl", name="theme_extraction")`<br>• `complete_with_tools()`: LLM function call with categories list and previous classifications<br>• Returns `is_valid`, `category_name`, `summary` for each Q&A<br>• Build cumulative context from prior classifications | Produces validated and categorized Q&A blocks with consistency across sequential classifications, filtering out irrelevant content while maintaining classification coherence |
| **4. Parallel HTML Formatting** | Format valid Q&A blocks with HTML emphasis tags for improved document presentation | • `format_all_qa_blocks_parallel()`: Process all valid Q&As concurrently<br>• `load_prompt_from_db(layer="key_themes_etl", name="html_formatting")`<br>• `complete()`: LLM call to add HTML tags for bold, italic, underline emphasis<br>• Store formatted content in `QABlock.formatted_content` | Generates presentation-ready HTML content for all valid Q&A blocks while maximizing throughput through parallel execution |
| **5. Comprehensive Grouping** | Review all category assignments and create final theme groups with optimized titles | • `determine_comprehensive_grouping()`: Analyze all classifications holistically<br>• `load_prompt_from_db(layer="key_themes_etl", name="grouping")`<br>• `complete_with_tools()`: LLM function call with all Q&A summaries and categories<br>• Returns `theme_groups[]` with `group_title`, `qa_ids[]`, and `rationale`<br>• `apply_grouping_to_index()`: Link Q&A blocks to assigned groups | Creates coherent theme groups by reviewing all classifications simultaneously, enabling intelligent regrouping decisions that optimize for narrative flow and thematic consistency |
| **6. Document Generation** | Create formatted deliverables and persist results for downstream consumption | • `create_document()`: Create DOCX with banner, page numbers, theme section headers<br>• Sort groups, add formatted HTML content via `HTMLToDocx` parser<br>• `_save_to_database()`: DELETE existing, INSERT into `aegis_reports`<br>• `UPDATE aegis_data_availability`: Add 'reports' to database_names array | Generates both human-readable Word documents for manual review and structured database records for programmatic access by Reports subagent |


## Output

The ETL generates both a formatted Word document and a database record stored in `aegis_reports` for downstream consumption.

| Output | Location | Description |
|--------|----------|-------------|
| **DOCX File** | `output/[SYMBOL]_[YEAR]_[QUARTER]_[HASH].docx` | Formatted Word document with banner, page numbers, and theme sections with numbered conversations |
| **Database Record** | `aegis_reports` table | Full report metadata including bank info, generation timestamp, and JSON metadata (theme groups count, valid Q&A count, filtered Q&A count) |


## Dependencies

The ETL leverages core Aegis infrastructure for database access, LLM operations, configuration management, and logging.

| Module | Import | Description |
|--------|--------|-------------|
| **Connections** | `aegis.connections.postgres_connector` | PostgreSQL database connection and query execution |
| **Connections** | `aegis.connections.llm_connector` | OpenAI API interface with function calling and streaming support |
| **Connections** | `aegis.connections.oauth_connector` | OAuth 2.0 authentication for API access |
| **Utils** | `aegis.utils.logging` | Structured logging with execution tracking and colored output |
| **Utils** | `aegis.utils.ssl` | SSL certificate configuration for secure API connections |
| **Utils** | `aegis.utils.prompt_loader` | Database-based prompt retrieval and loading system |
| **Utils** | `aegis.utils.settings` | Singleton configuration management with .env file support |
