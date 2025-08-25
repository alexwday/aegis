# Utils Module - Claude Context

## Module Purpose
The utils module provides shared utilities and infrastructure components that support the entire Aegis system. These utilities handle cross-cutting concerns like logging, configuration, monitoring, and data processing.

## Components Overview

### 1. Conversation Processing (`conversation.py`)
**Purpose**: Validates, filters, and processes incoming conversation messages

**Processing Pipeline**:
1. **Input Validation**: Ensures messages have required `role` and `content` fields
2. **Role Filtering**: Applies ALLOWED_ROLES configuration
3. **System Message Handling**: Removes system messages if INCLUDE_SYSTEM_MESSAGES=false
4. **History Trimming**: Keeps only MAX_HISTORY_LENGTH most recent messages
5. **Metadata Extraction**: Identifies latest_message for context

**Key Features**:
- Handles multiple input formats (dict with messages key or direct list)
- Returns detailed metadata including original vs processed counts
- Tracks latest_message (chronologically last after filtering)
- Comprehensive error handling with descriptive messages

**Configuration**:
- `INCLUDE_SYSTEM_MESSAGES`: Include/exclude system role (default: false)
- `ALLOWED_ROLES`: Comma-separated list of allowed roles (default: "user,assistant")
- `MAX_HISTORY_LENGTH`: Number of recent messages to keep (default: 10)

### 2. Process Monitoring (`monitor.py`)
**Purpose**: Tracks workflow execution stages with performance metrics and costs

**Features**:
- **Stage Tracking**: Records start/end times, duration, and status
- **LLM Cost Aggregation**: Calculates total costs from multiple LLM calls
- **Metadata Storage**: Captures custom metadata and decision details
- **Batch Posting**: Accumulates entries and posts to database at workflow end
- **PostgreSQL Integration**: Stores monitoring data in `process_monitor_logs` table

**Monitor Entry Structure**:
```python
{
    "run_uuid": str,           # Workflow execution ID
    "model_name": str,         # Model being executed (e.g., "aegis")
    "stage_name": str,         # Processing stage name
    "stage_start_time": datetime,
    "stage_end_time": datetime,
    "duration_ms": int,        # Stage duration in milliseconds
    "status": str,             # Success/Failure/Error
    "total_tokens": int,       # Aggregate token usage
    "total_cost": Decimal,     # Aggregate cost in USD
    "llm_calls": list,         # Individual LLM call details
    "decision_details": str,   # Human-readable decisions
    "error_message": str,      # Error details if failed
    "custom_metadata": dict,   # Stage-specific metadata
}
```

### 3. Logging Setup (`logging.py`)
**Purpose**: Configures structured logging with colored console output

**Features**:
- **Structlog Integration**: JSON-structured logging with context
- **Color Coding**: Visual indicators for log levels
  - 🔍 DEBUG (gray)
  - ✓ INFO (green)
  - ⚠ WARNING (yellow)
  - ✗ ERROR (red)
  - 🔥 CRITICAL (red)
- **Execution Tracking**: Automatic execution_id propagation
- **Performance Logging**: Timestamps and duration tracking
- **Context Preservation**: Maintains context across log entries

**Usage Pattern**:
```python
logger = get_logger()
logger.info("event.name", execution_id=id, key="value")
```

### 4. Settings Management (`settings.py`)
**Purpose**: Centralized configuration management via environment variables

**Features**:
- **Singleton Pattern**: Single Config instance across application
- **Type Conversion**: Automatic boolean and integer parsing
- **Default Values**: Sensible defaults for all settings
- **Lazy Loading**: Loads .env file on first access
- **Validation**: Ensures required settings are present

**Key Settings Categories**:
- **Authentication**: AUTH_METHOD, API_KEY, OAUTH_*
- **Database**: POSTGRES_* connection parameters
- **LLM**: Model selection, temperature, token limits
- **SSL**: SSL_VERIFY, SSL_CERT_PATH
- **Conversation**: Message filtering and history limits
- **Logging**: LOG_LEVEL configuration

### 5. SSL Configuration (`ssl.py`)
**Purpose**: Manages SSL/TLS settings for secure HTTPS connections

**Features**:
- **Certificate Verification**: Configurable SSL verification
- **Custom CA Bundles**: Support for enterprise certificates
- **Graceful Degradation**: Falls back to no verification if needed
- **Unified Configuration**: Single source of SSL settings

**Configuration Object**:
```python
{
    "verify": bool,           # Whether to verify SSL
    "cert_path": str|None,    # Path to CA bundle
    "status": str,            # Configuration status
    "details": str            # Human-readable details
}
```

### 6. Database Filtering (`database_filter.py`)
**Purpose**: Filters available databases based on user permissions

**Features**:
- **Access Control**: Restricts databases based on db_names parameter
- **Prompt Generation**: Creates filtered database context for LLM
- **Metadata Preservation**: Maintains database descriptions and types
- **Security**: Prevents unauthorized database access

**Database Categories**:
- Internal databases (CAPM, Wiki, etc.)
- External databases (EY, PWC, etc.)
- Specialized databases (transcripts, benchmarking)

### 7. Prompt Loader (`prompt_loader.py`)
**Purpose**: Loads and manages YAML prompt templates

**Features**:
- **YAML Parsing**: Loads prompts from YAML files
- **Template Support**: Variable substitution in prompts
- **Caching**: Caches loaded prompts for performance
- **Error Handling**: Graceful handling of missing prompts

## Design Patterns

### Context Propagation
All utilities support execution context propagation:
```python
context = {
    "execution_id": str,  # Unique request ID
    "auth_config": dict,  # Authentication settings
    "ssl_config": dict    # SSL configuration
}
```

### Error Handling
Consistent error handling across utilities:
- Descriptive error messages
- Structured logging of errors
- Graceful degradation where possible
- Clear status indicators

### Configuration Access
All configuration through centralized Config object:
```python
from .settings import config
value = config.setting_name  # Never use os.getenv()
```

## Performance Considerations
- **Caching**: Prompt templates and configuration cached
- **Batch Operations**: Monitor entries batched for database writes
- **Lazy Loading**: Settings loaded only when needed
- **Connection Reuse**: SSL config created once per workflow

## Security Features
- **No Hardcoded Secrets**: All credentials from environment
- **Certificate Verification**: Proper SSL/TLS validation
- **Access Control**: Database filtering based on permissions
- **Audit Trail**: Complete monitoring of all operations
- **Sensitive Data Protection**: No logging of credentials

## Testing Approach
- Comprehensive unit tests for each utility
- Shared fixtures in conftest.py
- Mock external dependencies
- Parametrized tests for configuration variations
- Edge case coverage for error conditions

## Integration Points
- **Workflow**: All utilities used by main workflow
- **Connectors**: SSL and auth config propagated
- **Agents**: Logging and monitoring integrated
- **Database**: Monitor data persisted to PostgreSQL