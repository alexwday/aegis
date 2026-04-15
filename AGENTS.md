# Aegis

AI-powered financial assistant for RBC's CFO Group. Orchestrates specialized agents to answer natural language queries about financial data from multiple databases.

## Project layout

```
src/aegis/
  model/
    main.py              # Entry point — async generator streaming responses
    agents/              # Router, Clarifier, Planner, Response, Summarizer
    subagents/           # Database-specific retrieval (supplementary, pillar3, rts, transcripts, reports)
    prompts/             # YAML prompt templates (global/, aegis/, per-database/)
  connections/           # OAuth, LLM (OpenAI via async client), PostgreSQL (SQLAlchemy)
  utils/                 # Settings, logging, SSL, conversation processing, monitor, prompt_loader
  etls/                  # Batch report generation (call_summary, key_themes, cm_readthrough, bank_earnings_report)
    etl_orchestrator.py  # Scheduled/gap-based ETL runner
tests/aegis/             # Mirrors src structure
```

## Dev environment

```bash
source venv/bin/activate    # Always activate before any Python command
pip install -r requirements.txt
cp .env.example .env        # Configure credentials
```

## Commands

```bash
# Tests
python -m pytest tests/aegis/ -xvs
python -m pytest tests/aegis/ --cov=aegis --cov-report=html

# Formatting & linting
black src/ tests/ --line-length 100
flake8 src/ tests/ --max-line-length 100
pylint src/

# Run the app
python run_fastapi.py
```

## Code conventions

- **Formatting**: Black, line-length 100. Flake8 clean. Pylint target 10.00/10.
- **Type hints**: Required on all function signatures.
- **Docstrings**: Google style on all public functions and classes.
- **Imports**: PEP 8 ordering (stdlib, third-party, local), separated by blank lines.
- **Config access**: Always `from aegis.utils.settings import config` — never raw `os.getenv()`.
- **Logging**: Always `from aegis.utils.logging import get_logger` — include `execution_id` in workflow logs.
- **Tests**: pytest, mirror source structure under `tests/aegis/`, shared fixtures in `conftest.py`.

## Architecture patterns

**Async streaming pipeline**: The main `model()` function is an `AsyncGenerator` that yields `{"type": "agent"|"subagent", "name": str, "content": str}` dicts. All agents follow this streaming pattern.

**Workflow flow**: `model()` → SSL setup → Auth → Conversation processing → Database filtering → Router → either Direct Response or Research Workflow (Clarifier → Planner → concurrent Subagents → Summarizer).

**Context dict**: Passed through all layers with `execution_id`, `auth_config`, `ssl_config`.

**LLM connector**: Async OpenAI client with three model tiers (small/medium/large) configured via `.env`. Uses `complete()`, `stream()`, `complete_with_tools()`, `create_embedding()`.

**Monitoring**: Every pipeline stage records timing/cost via `add_monitor_entry()`, persisted to `process_monitor_logs` table.

**ETLs**: Bypass the agent pipeline for batch processing. Direct function calls to subagent retrieval with custom prompts. Run via `etl_orchestrator.py` or individually (e.g., `python -m aegis.etls.call_summary.main`).

## Key tables

- `aegis_data_availability` — Bank/period/database coverage matrix
- `process_monitor_logs` — Execution tracking (run_uuid, stage, duration, tokens, cost)

## Things to know

- Python 3.11+ required.
- The LLM backend is OpenAI API (gpt-4.1 family), not Anthropic.
- Prompts are YAML files loaded from DB via `prompt_loader.py` with fallback to local files.
- Subagents run concurrently via `asyncio` tasks.
- S3 link markers in content (`{{S3_LINK:action:type:key:text}}`) are processed into HTML by `process_s3_links()` in `main.py`.
- Clear `__pycache__` if you hit stale import issues: `find . -type d -name __pycache__ -exec rm -rf {} +`
