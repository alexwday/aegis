# Aegis Project Overview

## Project Purpose
Aegis is an AI-powered financial analysis system that processes conversations, routes queries to appropriate agents and subagents, and provides intelligent responses using OpenAI's API. The system is designed for enterprise-grade financial data analysis with secure authentication, comprehensive monitoring, and flexible database connectivity.

## High-Level Architecture

### Core Components
1. **Model Orchestration** (`src/aegis/model/`)
   - Main workflow execution and agent routing
   - Multi-agent system with specialized capabilities
   - Prompt management for different domains

2. **Connections Layer** (`src/aegis/connections/`)
   - LLM integration (OpenAI API)
   - OAuth 2.0 authentication
   - PostgreSQL database connectivity

3. **Utilities** (`src/aegis/utils/`)
   - Conversation processing and filtering
   - Structured logging with execution tracking
   - SSL configuration for secure connections
   - Process monitoring and metrics collection
   - Settings management via environment variables

## Directory Structure
```
src/
├── aegis/
│   ├── connections/      # External service connectors
│   ├── model/            # Core AI model and workflow
│   │   ├── agents/       # Specialized agent implementations
│   │   ├── prompts/      # YAML prompt templates
│   │   │   ├── aegis/    # Main agent prompts
│   │   │   ├── global/   # Shared context prompts
│   │   │   └── [domain]/ # Domain-specific prompts
│   │   └── subagents/    # Subagent implementations
│   └── utils/            # Shared utilities and helpers
```

## Key Features
- **Multi-tier LLM Support**: Configurable model tiers (low/medium/high) for cost optimization
- **Streaming Responses**: Real-time streaming of agent and subagent responses
- **Authentication**: Supports both API key and OAuth 2.0 authentication
- **Database Integration**: PostgreSQL with connection pooling and query monitoring
- **Process Monitoring**: Tracks execution stages, costs, and performance metrics
- **Conversation Management**: Message filtering, role-based access, and history trimming
- **SSL/TLS Support**: Configurable certificate verification for secure connections

## Technology Stack
- **Language**: Python 3.x
- **LLM Provider**: OpenAI API (GPT-4, GPT-4-mini)
- **Database**: PostgreSQL with SQLAlchemy ORM
- **Authentication**: OAuth 2.0 (client credentials flow)
- **Logging**: structlog with colored console output
- **HTTP Client**: httpx for async operations
- **Configuration**: python-dotenv for environment management

## Workflow Overview
1. **Request Initialization**: Generate unique execution ID and setup monitoring
2. **SSL Configuration**: Configure secure connections for external services
3. **Authentication**: Setup OAuth or API key authentication
4. **Conversation Processing**: Validate, filter, and trim message history
5. **Database Filtering**: Apply database access restrictions based on user permissions
6. **Query Routing**: Analyze query and route to appropriate agent/subagent
7. **Response Generation**: Stream responses with proper attribution
8. **Monitoring & Logging**: Track all stages with costs and performance metrics

## Environment Configuration
The system uses environment variables for all configuration:
- Authentication credentials (API keys, OAuth settings)
- Database connection parameters
- Model selection and parameters
- Logging levels and formatting
- SSL/TLS certificate paths
- Conversation processing rules

## Testing Strategy
- Comprehensive unit tests (117+ tests)
- 93% code coverage with focus on business logic
- Shared fixtures in `tests/conftest.py`
- Parametrized tests for efficiency
- Isolated test execution without dependencies

## Code Quality Standards
- **Pylint**: 10.00/10 score
- **Black**: Formatted with line-length 100
- **Flake8**: Zero warnings or errors
- **Type Hints**: Full type annotations
- **Documentation**: Google-style docstrings