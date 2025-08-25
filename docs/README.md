# Aegis Documentation - Claude Context Files

## Overview
This documentation provides comprehensive context about the Aegis project structure, designed to help Claude (or any AI assistant) quickly understand the complete project scope when opening new chat sessions.

## Documentation Structure

### Root Level
- **[AEGIS_PROJECT_OVERVIEW.md](./AEGIS_PROJECT_OVERVIEW.md)** - High-level project overview, architecture, and technology stack

### Core Modules

#### Connections (`aegis/connections/`)
- **[CONNECTIONS_CLAUDE.md](./aegis/connections/CONNECTIONS_CLAUDE.md)** - External service integration layer
  - LLM Connector (OpenAI API integration)
  - OAuth Connector (Authentication)
  - PostgreSQL Connector (Database operations)

#### Utils (`aegis/utils/`)
- **[UTILS_CLAUDE.md](./aegis/utils/UTILS_CLAUDE.md)** - Shared utilities and infrastructure
  - Conversation processing
  - Process monitoring
  - Logging setup
  - Settings management
  - SSL configuration
  - Database filtering
  - Prompt loader

#### Model (`aegis/model/`)
- **[MODEL_CLAUDE.md](./aegis/model/MODEL_CLAUDE.md)** - Core AI orchestration and workflow
  - Main workflow execution
  - Agent coordination
  - Streaming response handling

##### Agents (`aegis/model/agents/`)
- **[AGENTS_CLAUDE.md](./aegis/model/agents/AGENTS_CLAUDE.md)** - Specialized AI agents
  - Router (implemented)
  - Clarifier, Planner, Response, Summarizer (planned)

##### Subagents (`aegis/model/subagents/`)
- **[SUBAGENTS_CLAUDE.md](./aegis/model/subagents/SUBAGENTS_CLAUDE.md)** - Domain-specific data retrieval agents
  - Benchmarking, Reports, RTS, Transcripts, Pillar3 (all planned)

##### Prompts (`aegis/model/prompts/`)
- **[PROMPTS_CLAUDE.md](./aegis/model/prompts/PROMPTS_CLAUDE.md)** - YAML prompt templates overview

###### Domain-Specific Prompts
- **[BENCHMARKING_PROMPTS_CLAUDE.md](./aegis/model/prompts/benchmarking/BENCHMARKING_PROMPTS_CLAUDE.md)** - Comparative analysis prompts
- **[REPORTS_PROMPTS_CLAUDE.md](./aegis/model/prompts/reports/REPORTS_PROMPTS_CLAUDE.md)** - Report generation templates
- **[RTS_PROMPTS_CLAUDE.md](./aegis/model/prompts/rts/RTS_PROMPTS_CLAUDE.md)** - Real-time system queries
- **[TRANSCRIPTS_PROMPTS_CLAUDE.md](./aegis/model/prompts/transcripts/TRANSCRIPTS_PROMPTS_CLAUDE.md)** - Earnings call analysis
- **[PILLAR3_PROMPTS_CLAUDE.md](./aegis/model/prompts/pillar3/PILLAR3_PROMPTS_CLAUDE.md)** - Regulatory reporting prompts

## How to Use This Documentation

### For New Chat Sessions
1. Start with **AEGIS_PROJECT_OVERVIEW.md** for project context
2. Reference the specific module documentation based on the task:
   - Working on API integrations? → **CONNECTIONS_CLAUDE.md**
   - Modifying utilities? → **UTILS_CLAUDE.md**
   - Enhancing AI logic? → **MODEL_CLAUDE.md** and **AGENTS_CLAUDE.md**
   - Adding new prompts? → **PROMPTS_CLAUDE.md** and domain-specific docs

### For Specific Tasks

#### Adding a New Agent
1. Review **AGENTS_CLAUDE.md** for patterns and conventions
2. Check **MODEL_CLAUDE.md** for workflow integration
3. Reference **PROMPTS_CLAUDE.md** for prompt structure

#### Modifying Database Connections
1. Read **CONNECTIONS_CLAUDE.md** for connector patterns
2. Review **UTILS_CLAUDE.md** for configuration management
3. Check **MODEL_CLAUDE.md** for context propagation

#### Implementing a Subagent
1. Study **SUBAGENTS_CLAUDE.md** for base patterns
2. Review relevant domain prompt documentation
3. Check **MODEL_CLAUDE.md** for integration points

## Key Implementation Status

### Fully Implemented ✅
- OAuth and API key authentication
- LLM integration with cost tracking
- PostgreSQL database connectivity
- Process monitoring and logging
- Conversation processing pipeline
- Router agent for query routing
- Main workflow orchestration

### Partially Implemented 🚧
- Agent system (only Router completed)
- Prompt templates (core prompts done, domain prompts pending)

### Planned/Not Implemented 📋
- Clarifier, Planner, Response, Summarizer agents
- All subagents (Benchmarking, Reports, RTS, Transcripts, Pillar3)
- Domain-specific prompt implementations
- Async/await support
- Agent collaboration framework

## Quick Reference

### Environment Variables
See `.env.example` for all configuration options

### Testing
- 117+ unit tests with 93% coverage
- Run tests: `python -m pytest tests/`
- Check coverage: `python -m pytest --cov=src tests/`

### Code Quality
- Format: `black src/ --line-length 100`
- Lint: `flake8 src/ --max-line-length 100`
- Analyze: `pylint src/`

### Project Standards
- Pylint: 10.00/10 score
- Type hints on all functions
- Google-style docstrings
- No hardcoded credentials
- Comprehensive error handling

## File Mapping

### Source to Documentation
```
src/aegis/connections/ → docs/aegis/connections/CONNECTIONS_CLAUDE.md
src/aegis/utils/ → docs/aegis/utils/UTILS_CLAUDE.md
src/aegis/model/ → docs/aegis/model/MODEL_CLAUDE.md
src/aegis/model/agents/ → docs/aegis/model/agents/AGENTS_CLAUDE.md
src/aegis/model/subagents/ → docs/aegis/model/subagents/SUBAGENTS_CLAUDE.md
src/aegis/model/prompts/ → docs/aegis/model/prompts/PROMPTS_CLAUDE.md
```

## Contributing
When adding new functionality:
1. Update the relevant Claude documentation file
2. Maintain consistency with existing patterns
3. Include examples and use cases
4. Document any new environment variables
5. Update implementation status in this README