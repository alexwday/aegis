## How to Run

The Bank Earnings Report ETL can be executed directly via command line for a specific bank/quarter, or scheduled via the orchestrator to process all monitored institutions automatically.

| Method | Command | Description |
|--------|---------|-------------|
| **Direct Command Line** | `python -m aegis.etls.bank_earnings_report.main --bank RY --year 2025 --quarter Q2` | Run the ETL for a specific bank and quarter directly from the command line. |
| **Orchestrator Scheduling** | `python scripts/etl_orchestrator.py` | Automatically process all monitored institutions defined in `config/monitored_institutions.yaml`. Supports two-phase scheduling (RTS/Supp first, then re-run when transcripts available). |


## Inputs

The ETL requires data from multiple database tables and configuration files defining banks and LLM parameters.

| Input | Location | Description |
|-------|----------|-------------|
| **aegis_data_availability table** | PostgreSQL | Tracks which data sources (supplementary, rts, transcripts, pillar3) are available per bank-period |
| **aegis_supplementary table** | PostgreSQL | Financial metrics, 8Q trends, segment metrics, dividend data from Supplementary Packs |
| **aegis_rts table** | PostgreSQL | Report to Shareholders text for drivers, overview, items of note |
| **aegis_transcripts table** | PostgreSQL | Parsed and chunked earnings call transcripts (MD and Q&A sections) |
| **aegis_pillar3 table** | PostgreSQL | Basel III regulatory capital metrics (CET1, RWA, LCR) |
| **prompts table** | PostgreSQL | LLM prompts for extraction tasks (layer=bank_earnings_report_etl) |
| **config.yaml** | `config/` | LLM model tiers and parameters (temperature, max_tokens) |
| **monitored_institutions.yaml** | `config/` | Institution metadata (id, name, type, brand colors, logo) for Canadian banks |


## Process

The ETL transforms data from multiple sources (Supp Pack, RTS, Transcripts, Pillar3) into a structured HTML earnings report through five sequential stages.

| Stage | Purpose | Sub-steps | Output |
|-------|---------|-----------|--------|
| **1. Setup & Validation** | Validate inputs and prepare execution environment before expensive LLM operations | • `get_bank_info_from_config()`: Look up bank from `monitored_institutions.yaml` by ID/symbol/name<br>• `verify_data_availability()`: Query `aegis_data_availability` to check which sources exist for bank-period<br>• `setup_authentication()` + `setup_ssl()`: OAuth token and certificates | Ensures valid bank-period combination exists and establishes secure API connections, preventing wasted compute on invalid requests |
| **2. Data Retrieval** | Retrieve financial data from Supplementary Pack and establish available metrics/segments | • `retrieve_dividend()`: Get dividend amount and change from `aegis_supplementary`<br>• `retrieve_all_metrics()`: Query all metrics for bank-period to establish available set<br>• `retrieve_available_platforms()`: Get list of business segments with data<br>• `retrieve_metric_history()`: Get 8Q historical data for chart metrics | Provides complete financial data foundation (metrics, dividends, segments, historical trends) for LLM extraction and report generation |
| **3. LLM Extraction** | Extract structured insights from multiple sources using parallel LLM calls with source combination | • **Key Metrics**: `select_chart_and_tile_metrics()` - LLM selects 6 tile metrics + 5 dynamic metrics + chart metric from available set<br>• **Overview**: `extract_transcript_overview()` + `extract_rts_overview()` → `combine_overview_narratives()` - merge sources<br>• **Items of Note**: `extract_transcript_items_of_note()` + `extract_rts_items_of_note()` → `process_items_of_note()` - deduplicate and rank<br>• **Narrative**: `extract_rts_narrative_paragraphs()` + `extract_transcript_quotes()` → `combine_narrative_entries()` - interleave paragraphs with quotes<br>• **Analyst Focus**: `extract_analyst_focus()` - extract Q&A themes, questions, answers from transcripts<br>• **Segment Drivers**: `get_all_segment_drivers_from_rts()` - extract performance drivers per segment from RTS<br>• **Capital & Risk**: `extract_capital_risk_section()` - regulatory capital metrics from Pillar3 | Produces structured JSON for each report section: key metrics with selection rationale, combined overview narrative, deduplicated items of note, management narrative with quotes, analyst Q&A themes, segment performance drivers, capital/risk metrics |
| **4. Report Rendering** | Generate formatted HTML report from extracted JSON sections using Jinja2 template | • `render_report()`: Load `report_template.html` Jinja2 template<br>• Inject all section JSON data into template context<br>• Render complete HTML with embedded CSS and JavaScript for interactive charts<br>• Write to `output/[SYMBOL]_[YEAR]_[QUARTER].html` | Generates self-contained HTML report with responsive design, interactive 8Q trend charts, expandable raw data tables, and bank-specific branding (colors, logo) |
| **5. Database Persistence** | Save report metadata and update availability for downstream consumption | • `save_to_database()`: DELETE existing report for bank-period-type<br>• INSERT into `aegis_reports` with filepath, execution_id, metadata<br>• Metadata includes: sources_used array, has_transcript flag for orchestrator | Creates database record for Reports subagent retrieval and enables orchestrator's two-phase detection (re-run when transcripts become available) |


## Output

The ETL generates both a formatted HTML report and a database record stored in `aegis_reports` for downstream consumption.

| Output | Location | Description |
|--------|----------|-------------|
| **HTML File** | `output/[SYMBOL]_[YEAR]_[QUARTER].html` | Self-contained HTML report with embedded CSS, JavaScript charts, and bank logo. Sections: Header with dividend, Key Metrics (6 tiles + dynamic + 8Q chart), Overview narrative, Items of Note, Management Narrative with quotes, Analyst Focus Q&A, Segment Performance (5 segments), Capital & Risk metrics |
| **Database Record** | `aegis_reports` table | Full report metadata including bank info, generation timestamp, execution_id, and JSON metadata (sources_used: rts/supplementary/transcripts, has_transcript flag for two-phase orchestration) |


## Dependencies

The ETL leverages core Aegis infrastructure for database access, LLM operations, configuration management, and logging.

| Module | Import | Description |
|--------|--------|-------------|
| **Connections** | `aegis.connections.postgres_connector` | PostgreSQL database connection and async query execution |
| **Connections** | `aegis.connections.llm_connector` | OpenAI API interface with function calling support |
| **Connections** | `aegis.connections.oauth_connector` | OAuth 2.0 authentication for API access |
| **Utils** | `aegis.utils.logging` | Structured logging with execution tracking and colored output |
| **Utils** | `aegis.utils.ssl` | SSL certificate configuration for secure API connections |
| **Utils** | `aegis.utils.sql_prompt` | Database-based prompt retrieval via `postgresql_prompts()` |
| **Utils** | `aegis.utils.settings` | Singleton configuration management with .env file support |
| **External** | `jinja2` | HTML template rendering engine for report generation |
