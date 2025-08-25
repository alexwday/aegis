# Model Module - Claude Context

## Module Purpose
The model module contains the core AI orchestration logic for Aegis, including the main workflow, agent implementations, prompt management, and subagent coordination. This is the brain of the Aegis system that processes queries and generates intelligent responses.

## Architecture Overview

### Workflow Orchestration (`main.py`)
**Purpose**: Main entry point that orchestrates the entire agent pipeline

**Key Function**: `model(conversation, db_names) -> Generator`
- Streams responses with unified message schema
- Generates unique execution_id for tracking
- Coordinates all processing stages
- Yields messages with type/name/content structure

**Processing Stages**:
1. **Initialization**: Setup logging, generate execution_id, initialize monitoring
2. **SSL Setup**: Configure secure connections
3. **Authentication**: Setup OAuth or API key authentication
4. **Conversation Processing**: Validate and filter messages
5. **Database Filtering**: Apply database access restrictions
6. **Query Routing**: Analyze query and determine processing path
7. **Response Generation**: Stream responses from agents/subagents
8. **Monitoring**: Track costs, performance, and decisions

**Message Schema**:
```python
{
    "type": "agent" | "subagent",
    "name": "aegis" | "transcripts" | "rts" | etc,
    "content": "Text content to display"
}
```

### Agent System (`agents/`)

#### Router Agent (`router.py`)
**Purpose**: Determines query processing path (direct response vs research)

**Decision Factors**:
- Query complexity and type
- Available data sources
- Conversation context
- Database requirements

**Routes**:
- **Direct Response**: Simple queries, greetings, clarifications
- **Research Workflow**: Data queries, complex analysis, report generation

**Tool Calling**: Uses OpenAI function calling for structured routing decisions

### Prompt Management (`prompts/`)

#### Structure
```
prompts/
├── aegis/          # Main agent prompts
│   ├── router.yaml      # Query routing logic
│   ├── clarifier.yaml   # Query clarification
│   ├── planner.yaml     # Research planning
│   ├── response.yaml    # Response generation
│   └── summarizer.yaml  # Conversation summarization
├── global/         # Shared context prompts
│   ├── project.yaml     # Project context and capabilities
│   ├── database.yaml    # Database descriptions
│   ├── banks.yaml       # Bank-specific information
│   ├── restrictions.yaml # Access restrictions
│   └── fiscal.py        # Fiscal period calculations
└── [domain]/       # Domain-specific prompts
    ├── benchmarking/    # Benchmarking queries
    ├── reports/         # Report generation
    ├── rts/            # Real-time systems
    └── transcripts/    # Earnings transcripts
```

#### Prompt Features
- **YAML Format**: Human-readable prompt templates
- **Modular Design**: Composable prompt components
- **Context Injection**: Dynamic context insertion
- **Tool Definitions**: Embedded function calling schemas
- **Variable Substitution**: Template variable support

### Subagents (`subagents/`)
**Status**: Placeholder for future specialized agents

**Planned Subagents**:
- **Benchmarking**: Comparative analysis across banks
- **Reports**: Document generation and formatting
- **RTS**: Real-time system integration
- **Transcripts**: Earnings call analysis
- **Pillar3**: Regulatory reporting

## Agent Communication Pattern

### Context Propagation
All agents receive unified context:
```python
context = {
    "execution_id": str,           # Request tracking ID
    "auth_config": dict,          # Authentication settings
    "ssl_config": dict,           # SSL configuration
    "database_prompt": str,       # Filtered database context
    "available_databases": list,  # Accessible databases
}
```

### Streaming Pattern
Agents yield messages for real-time UI updates:
```python
# Main agent message
yield {
    "type": "agent",
    "name": "aegis",
    "content": "Analyzing your query..."
}

# Subagent message
yield {
    "type": "subagent",
    "name": "transcripts",
    "content": "Searching earnings calls..."
}
```

## LLM Integration

### Model Tiers
Agents select appropriate model tier based on task:
- **Low**: Fast, simple queries (gpt-4o-mini)
- **Medium**: Balanced performance (gpt-4o-mini-2024-07-18)
- **High**: Complex analysis (gpt-4o-2025-01-09)

### Cost Optimization
- Router uses low-tier for quick decisions
- Research agents use appropriate tier
- Cost tracking integrated at all levels
- Automatic fallback on errors

### Tool Calling
Agents use OpenAI function calling for:
- Structured routing decisions
- Database query generation
- Research plan creation
- Result formatting

## Monitoring Integration

### Stage Tracking
Each processing stage logs:
- Start/end times and duration
- Token usage and costs
- Decisions and rationale
- Errors and recovery actions

### Metrics Collection
```python
add_monitor_entry(
    stage_name="Query_Routing",
    stage_start_time=start_time,
    status="Success",
    llm_calls=[{
        "model": "gpt-4o-mini",
        "tokens": 500,
        "cost": 0.0015
    }],
    decision_details="Routed to research workflow",
    custom_metadata={"route": "research", "confidence": 0.95}
)
```

## Error Handling

### Graceful Degradation
- Fallback to simpler models on errors
- Default responses for critical failures
- Comprehensive error logging
- User-friendly error messages

### Recovery Strategies
- Retry with exponential backoff
- Alternative routing paths
- Cached response fallbacks
- Manual intervention alerts

## Performance Optimization

### Streaming Response
- Immediate UI feedback
- Chunked response processing
- Parallel subagent execution
- Progressive result refinement

### Caching Strategy
- Prompt template caching
- Database metadata caching
- Authentication token caching
- Connection pooling

## Security Considerations

### Access Control
- Database filtering based on permissions
- Role-based prompt selection
- Audit trail of all decisions
- Sensitive data masking

### Prompt Security
- No credentials in prompts
- Injection attack prevention
- Output validation
- Rate limiting support

## Testing Approach

### Unit Testing
- Individual agent testing
- Prompt loading verification
- Context propagation tests
- Error handling validation

### Integration Testing
- End-to-end workflow tests
- Streaming response validation
- Cost calculation verification
- Monitoring data accuracy

### Mock Strategies
- LLM response mocking
- Database result simulation
- Authentication bypass for tests
- Deterministic routing decisions

## Future Enhancements

### Planned Features
- Multi-agent collaboration
- Advanced planning capabilities
- Memory/context persistence
- Tool use expansion
- Custom agent creation

### Scalability Considerations
- Distributed agent execution
- Result caching layer
- Query optimization
- Load balancing support