## How to Run

The CM Readthrough ETL can be executed directly via command line for a specific quarter, or scheduled via the orchestrator to process all monitored institutions automatically.

| Method | Command | Description |
|--------|---------|-------------|
| **Direct Command Line** | `python -m aegis.etls.cm_readthrough.main --year 2025 --quarter Q2` | Run the ETL for a specific quarter to process all monitored banks. |
| **Orchestrator Scheduling** | `python scripts/etl_orchestrator.py` | Automatically process all monitored institutions defined in `config/monitored_institutions.yaml`. |


## Inputs

The ETL requires transcript data from the database and configuration files defining categories and LLM parameters.

| Input | Location | Description |
|-------|----------|-------------|
| **aegis_transcripts table** | PostgreSQL | Parsed and chunked earnings call transcripts (MD and Q&A sections) |
| **prompts table** | PostgreSQL | LLM prompts: `outlook_extraction`, `qa_extraction_dynamic`, `subtitle_generation`, and `batch_formatting` (layer=cm_readthrough_etl) |
| **config.yaml** | `config/` | LLM model tiers and parameters (temperature, max_tokens, concurrency) |
| **monitored_institutions.yaml** | `config/` | Institution metadata (id, name, type) for multi-bank processing |
| **outlook_categories.xlsx** | `config/categories/` | Category definitions for outlook statement extraction |
| **qa_market_volatility_regulatory_categories.xlsx** | `config/categories/` | Category definitions for Section 2 Q&A (Global Markets, Risk Management, Corporate Banking, Regulatory Changes) |
| **qa_pipelines_activity_categories.xlsx** | `config/categories/` | Category definitions for Section 3 Q&A (Investment Banking/M&A, Transaction Banking) |

## Runtime Standardization Controls

The CM readthrough ETL now follows the same reliability/contract standards used in the other production ETLs.

| Control | Location | Notes |
|--------|----------|-------|
| **Typed result + typed errors** | `main.py` (`CMReadthroughResult`, `CMReadthroughUserError`, `CMReadthroughSystemError`) | Runtime returns structured success metadata and classifies user vs system failures |
| **Per-task token budgets** | `config/config.yaml` (`llm.max_tokens`) | `outlook_extraction`, `qa_extraction`, `subtitle_generation`, `batch_formatting`, plus `default` fallback |
| **Retry/backoff policy** | `config/config.yaml` (`retry`) + `main.py` | Exponential backoff with jitter for tool-call stages |
| **Schema validation for tool outputs** | `main.py` (Pydantic models) | Validates extraction/formatting/subtitle tool responses before use |
| **Prompt safety** | `main.py` (`_sanitize_for_prompt`) | Escapes braces for safe `.format()` prompt injection |
| **Observability** | `main.py` (`_accumulate_llm_cost`, `_timing_summary`) | Stage-level LLM usage and timing logs on completion |
| **Transcript retrieval safeguards** | `transcript_utils.py` | Diagnostics only on misses; retrieval failures raise explicit runtime errors |
| **Document validation** | `document_converter.py` (`validate_document_content`) | Prevents writing invalid/empty report documents |


## Process

The ETL transforms raw transcript data from multiple banks into a structured capital markets readthrough report through six sequential stages with concurrent execution.

| Stage | Purpose | Sub-steps | Output |
|-------|---------|-----------|--------|
| **1. Setup & Validation** | Validate inputs and prepare execution environment before expensive LLM operations | • Load `monitored_institutions.yaml`: Get list of 20+ monitored banks for processing<br>• Load `outlook_categories.xlsx`: Category definitions for outlook extraction<br>• Load `qa_market_volatility_regulatory_categories.xlsx`: 4 category definitions for Section 2<br>• Load `qa_pipelines_activity_categories.xlsx`: 2 category definitions for Section 3<br>• `setup_authentication()` + `setup_ssl()`: OAuth token and certificates | Ensures all category definitions are loaded and establishes secure API connections for multi-bank concurrent processing |
| **2. Parallel Extraction** | Retrieve transcripts and extract content from all monitored banks concurrently across three content types | • **Phase 1 (Parallel)**: Outlook extraction for each bank<br>&nbsp;&nbsp;- `retrieve_full_section(sections="ALL")` for MD+QA per bank<br>&nbsp;&nbsp;- `load_prompt_from_db(layer="cm_readthrough_etl", name="outlook_extraction")`<br>&nbsp;&nbsp;- `complete_with_tools()`: Returns `{has_content: bool, statements: []}`<br>• **Phase 2 (Parallel)**: Section 2 Q&A extraction for each bank<br>&nbsp;&nbsp;- `retrieve_full_section(sections="QA")` per bank<br>&nbsp;&nbsp;- `load_prompt_from_db(layer="cm_readthrough_etl", name="qa_extraction_dynamic")`<br>&nbsp;&nbsp;- `complete_with_tools()`: Returns `{has_content: bool, questions: []}`<br>• **Phase 3 (Parallel)**: Section 3 Q&A extraction for each bank<br>&nbsp;&nbsp;- Same pattern as Phase 2 with different categories<br>• Semaphore limit: max 5 concurrent banks | Produces three result sets (outlook, section2_questions, section3_questions) with automatic bank filtering via `has_content` flag, processing 20+ banks efficiently through concurrent execution |
| **3. Aggregation & Sorting** | Consolidate results from parallel extraction phases and filter banks with no content | • `aggregate_results()`: Process 3 result sets (outlook, section2, section3)<br>• Filter by `has_content=True` to exclude banks without relevant data<br>• Organize by bank name, preserve bank_symbol for ticker display<br>• Returns 3 dictionaries: `all_outlook`, `all_section2`, `all_section3` | Creates structured dictionaries mapping bank names to their statements/questions, automatically excluding banks with no relevant capital markets content |
| **4. Subtitle Generation** | Generate intelligent subtitles for all three sections by analyzing aggregated content | • Concurrent generation for 3 sections (outlook, section2, section3)<br>• `load_prompt_from_db(layer="cm_readthrough_etl", name="subtitle_generation")`<br>• For each section: Summarize first 3 items per bank, format as JSON<br>• `complete_with_tools()`: LLM generates concise 8-15 word subtitle capturing themes<br>• Returns subtitle strings or falls back to default if generation fails | Produces thematically coherent subtitles that reflect actual extracted content rather than generic descriptions |
| **5. Batch Formatting** | Format all statements with HTML emphasis tags in a single LLM call (currently disabled for performance) | • `format_outlook_batch()`: Single LLM call for all banks' outlook statements<br>• `load_prompt_from_db(layer="cm_readthrough_etl", name="batch_formatting")`<br>• `complete_with_tools()`: Returns formatted statements with HTML tags<br>• **Currently disabled for performance** - using unformatted statements | When enabled, provides consistent HTML formatting (`<strong><u>`) across all statements while reducing LLM calls from N banks to 1 total |
| **6. Document Generation** | Create formatted deliverables and persist results for downstream consumption | • `create_combined_document()`: Create DOCX with landscape orientation, dark blue headers<br>• Section 1: 2-column table (Banks/Segments, Outlook statements)<br>• Section 2: 3-column table (Bank, Category, Verbatim question with analyst)<br>• Section 3: 3-column table (Bank, Category, Verbatim question with analyst)<br>• `save_to_database()`: DELETE existing cm_readthrough report, INSERT into `aegis_reports` | Generates both human-readable Word documents for manual review and structured database records for programmatic access by Reports subagent |


## Output

The ETL generates both a formatted Word document and a database record stored in `aegis_reports` for downstream consumption.

| Output | Location | Description |
|--------|----------|-------------|
| **DOCX File** | `output/CM_Readthrough_[YEAR]_[QUARTER].docx` | Formatted Word document with landscape orientation, dark blue headers, and three sections: Outlook (2-column), Section 2 Q&A (3-column), Section 3 Q&A (3-column) |
| **Database Record** | `aegis_reports` table | Cross-bank report metadata with report_type='cm_readthrough', generation timestamp, and JSON metadata (banks_processed, banks_with_outlook/section2/section3, subtitles) |


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
