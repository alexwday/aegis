# Connections Module - Claude Context

## Module Purpose
The connections module provides the external service integration layer for Aegis, handling all interactions with third-party services including OpenAI's API, OAuth authentication providers, and PostgreSQL databases.

## Components Overview

### 1. LLM Connector (`llm_connector.py`)
**Purpose**: Manages all OpenAI API interactions with multi-tier model support

**Key Features**:
- **Client Caching**: Reuses OpenAI client instances for performance
- **Cost Tracking**: Calculates and logs token usage costs in real-time
- **Model Tiers**: 
  - Low: gpt-4o-mini (fast, cost-effective)
  - Medium: gpt-4o-mini-2024-07-18 (balanced)
  - High: gpt-4o-2025-01-09 (most capable)
- **Response Timing**: Tracks API response times for performance monitoring
- **Streaming Support**: Handles both standard and streaming completions
- **Tool Calling**: Supports OpenAI function calling for structured outputs
- **Embeddings**: Creates text embeddings using text-embedding-3-small model

**API Pattern**:
```python
# Context-based API with unified authentication
context = {
    "execution_id": str,
    "auth_config": dict,  # From setup_authentication()
    "ssl_config": dict     # From setup_ssl()
}

# Available functions
check_connection(context)  # Verify connectivity
complete(messages, context, llm_params, model_tier)  # Standard completion
stream(messages, context, llm_params)  # Streaming response
complete_with_tools(messages, tools, context, llm_params)  # Tool calling
create_embedding(texts, context, llm_params)  # Text embeddings
```

**Cost Tracking**:
- Automatically calculates costs based on token usage
- Logs costs in structured format for monitoring
- Supports different pricing for input/output tokens
- Tracks response times alongside costs

### 2. OAuth Connector (`oauth_connector.py`)
**Purpose**: Handles OAuth 2.0 client credentials flow for secure API authentication

**Key Features**:
- **Client Credentials Flow**: Secure token generation using HTTP Basic Auth
- **Retry Logic**: Exponential backoff with configurable retry strategy
- **SSL Support**: Respects SSL configuration from workflow
- **Token Caching**: Returns cached tokens within validity period
- **Graceful Fallback**: Returns None when OAuth not configured

**Main Functions**:
- `setup_authentication()`: Main entry point returning auth configuration
- `get_oauth_token()`: Internal token generation with retry logic
- Returns either OAuth bearer token or API key configuration

**Authentication Flow**:
1. Check AUTH_METHOD environment variable
2. If "oauth": Generate/retrieve OAuth token
3. If "api_key": Return API key configuration
4. Include SSL configuration in all requests

### 3. PostgreSQL Connector (`postgres_connector.py`)
**Purpose**: Database operations using SQLAlchemy with connection pooling

**Key Features**:
- **Connection Pooling**: QueuePool with 5 connections, 10 overflow
- **Context Managers**: Safe connection handling with automatic cleanup
- **Query Types**:
  - Raw SQL execution
  - Parameterized queries
  - Batch operations (insert_many, update_many)
  - Table introspection
- **Error Handling**: Comprehensive exception handling with logging
- **Singleton Engine**: Global engine instance for connection reuse

**Main Functions**:
```python
execute_query(query, params, context)  # Execute SELECT queries
execute_command(query, params, context)  # Execute INSERT/UPDATE/DELETE
insert_many(table_name, records, context)  # Batch inserts
update_many(table_name, records, context)  # Batch updates
delete_records(table_name, condition, context)  # Conditional deletes
get_table_info(table_name, context)  # Table schema inspection
```

## Context Pattern
All connectors use a unified context pattern for consistency:

```python
context = {
    "execution_id": str,  # UUID for request tracking
    "auth_config": dict,  # Authentication configuration
    "ssl_config": dict    # SSL/TLS settings
}
```

This ensures:
- Consistent logging with execution tracking
- Proper authentication across all services
- SSL/TLS configuration propagation
- Error correlation across components

## Configuration
All connectors read from environment variables via `config` object:

### LLM Configuration
- `AUTH_METHOD`: "oauth" or "api_key"
- `API_KEY`: Direct API key (when AUTH_METHOD=api_key)
- `OPENAI_BASE_URL`: Optional custom endpoint
- `OPENAI_TIMEOUT`: Request timeout (default: 60s)
- `DEFAULT_TEMPERATURE`: Model temperature (default: 0.7)
- `DEFAULT_MAX_TOKENS`: Response limit (default: 2000)

### OAuth Configuration
- `OAUTH_ENDPOINT`: Token endpoint URL
- `OAUTH_CLIENT_ID`: OAuth client identifier
- `OAUTH_CLIENT_SECRET`: OAuth client secret
- `OAUTH_GRANT_TYPE`: Grant type (default: "client_credentials")

### PostgreSQL Configuration
- `POSTGRES_HOST`: Database host
- `POSTGRES_PORT`: Database port
- `POSTGRES_DATABASE`: Database name
- `POSTGRES_USER`: Database user
- `POSTGRES_PASSWORD`: Database password

## Error Handling
All connectors implement robust error handling:
- Retry logic for transient failures
- Graceful degradation when services unavailable
- Comprehensive error logging with context
- Clear error messages for debugging

## Performance Considerations
- **Connection Reuse**: All connectors cache and reuse connections
- **Batch Operations**: PostgreSQL supports batch inserts/updates
- **Streaming**: LLM supports streaming for real-time responses
- **Cost Optimization**: Model tier selection based on query complexity
- **Connection Pooling**: PostgreSQL uses SQLAlchemy pooling

## Security Features
- **OAuth 2.0**: Secure token-based authentication
- **SSL/TLS**: Certificate verification for all HTTPS requests
- **Credential Protection**: No credentials in code or logs
- **Connection Security**: Encrypted database connections
- **Token Expiry**: Automatic token refresh handling

## Testing Approach
- Mock external services for unit tests
- Parametrized tests for different authentication methods
- Connection pool testing for PostgreSQL
- Cost calculation verification for LLM
- SSL configuration testing for all connectors