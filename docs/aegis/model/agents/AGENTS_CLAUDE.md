# Agents Module - Claude Context

## Module Purpose
The agents module contains the implementation of specialized AI agents that handle different aspects of query processing in the Aegis system. Each agent has a specific responsibility in the workflow pipeline.

## Current Implementation

### Router Agent (`router.py`)
**Status**: Fully Implemented

**Purpose**: First-line decision maker that analyzes incoming queries to determine the appropriate processing path.

**Core Functionality**:
```python
def route_query(
    conversation_history: List[Dict[str, str]],
    latest_message: str,
    context: Dict[str, Any]
) -> Dict[str, Any]
```

**Decision Process**:
1. **Load Prompts**: Combines project, database, and router-specific prompts
2. **Context Analysis**: Evaluates conversation history and current query
3. **Tool Calling**: Uses OpenAI function calling for structured decision
4. **Route Selection**: Returns binary decision (0=direct, 1=research)

**Routing Logic**:
- **Direct Response (0)**:
  - Simple greetings or acknowledgments
  - Questions about Aegis capabilities
  - Reformatting existing conversation data
  - Nonsensical or empty queries

- **Research Workflow (1)**:
  - Financial data requests
  - Entity-specific queries (banks, metrics)
  - Complex analysis requirements
  - Ambiguous queries needing clarification

**Return Structure**:
```python
{
    "route": "direct_response" | "research_workflow",
    "rationale": "Explanation of routing decision",
    "confidence": 0.0-1.0,
    "status": "Success" | "Error",
    "error": Optional[str]
}
```

**Integration Points**:
- Receives filtered database context from main workflow
- Uses LLM connector for decision making
- Logs routing decisions for monitoring
- Handles errors gracefully with fallback routing

## Planned Agents (Not Yet Implemented)

### Clarifier Agent
**Purpose**: Disambiguate vague or incomplete user queries

**Planned Functionality**:
- Entity disambiguation (which bank, time period, metric)
- Intent clarification for multi-interpretation queries
- Option presentation for user selection
- Context gathering for incomplete requests

**Expected Interface**:
```python
def clarify_query(
    query: str,
    conversation_history: List[Dict],
    context: Dict
) -> Dict[str, Any]:
    # Returns clarification questions or options
```

### Planner Agent
**Purpose**: Create execution plans for complex research queries

**Planned Functionality**:
- Query decomposition into sub-tasks
- Data source identification and prioritization
- Execution order optimization
- Resource allocation planning

**Expected Interface**:
```python
def create_plan(
    query: str,
    available_sources: List[str],
    context: Dict
) -> Dict[str, Any]:
    # Returns execution plan with steps
```

### Response Agent
**Purpose**: Generate final responses from collected data

**Planned Functionality**:
- Data synthesis from multiple sources
- Format selection (table, narrative, chart)
- Citation and source attribution
- Confidence scoring

**Expected Interface**:
```python
def generate_response(
    data: Dict,
    query: str,
    format_preference: str,
    context: Dict
) -> Generator[Dict, None, None]:
    # Yields formatted response chunks
```

### Summarizer Agent
**Purpose**: Condense conversation history and results

**Planned Functionality**:
- Conversation compression for context limits
- Key point extraction
- Decision trail documentation
- Report generation

**Expected Interface**:
```python
def summarize_conversation(
    conversation: List[Dict],
    max_tokens: int,
    context: Dict
) -> str:
    # Returns compressed summary
```

## Agent Architecture Patterns

### Common Structure
All agents follow consistent patterns:

1. **Input Validation**: Verify required parameters
2. **Prompt Assembly**: Combine global and specific prompts
3. **Context Injection**: Add runtime context (auth, databases)
4. **LLM Interaction**: Call appropriate model tier
5. **Response Parsing**: Extract structured data
6. **Error Handling**: Graceful degradation
7. **Monitoring**: Log decisions and metrics

### Context Propagation
Every agent receives and maintains context:
```python
context = {
    "execution_id": str,           # Request tracking
    "auth_config": dict,          # Authentication
    "ssl_config": dict,           # SSL settings
    "database_prompt": str,       # Filtered databases
    "available_databases": list,  # Accessible DBs
    "monitoring": dict            # Performance tracking
}
```

### Tool Calling Pattern
Agents use OpenAI function calling for structured outputs:
```python
tools = [{
    "type": "function",
    "function": {
        "name": "agent_action",
        "description": "Agent's structured decision",
        "parameters": {
            "type": "object",
            "properties": {...},
            "required": [...]
        }
    }
}]

response = complete_with_tools(messages, tools, context)
```

## Model Tier Selection

### Tier Strategy by Agent
- **Router**: Low tier (gpt-4o-mini) - Simple binary decisions
- **Clarifier**: Low tier - Template-based questions
- **Planner**: Medium tier - Complex reasoning required
- **Response**: High tier - Quality output generation
- **Summarizer**: Medium tier - Balanced compression

### Dynamic Tier Selection
Agents can adjust tier based on:
- Query complexity assessment
- Error recovery (upgrade tier on failure)
- Cost optimization constraints
- Response time requirements

## Error Handling

### Common Error Scenarios
1. **LLM Timeout**: Retry with exponential backoff
2. **Invalid Response**: Fallback to default behavior
3. **Tool Calling Failure**: Use text parsing backup
4. **Context Missing**: Request required context
5. **Rate Limiting**: Queue and retry

### Recovery Strategies
```python
try:
    # Primary agent logic
    result = primary_processing()
except LLMError:
    # Fallback to simpler model
    result = fallback_processing()
except Exception as e:
    # Return safe default
    result = {
        "status": "Error",
        "error": str(e),
        "fallback": default_response()
    }
```

## Performance Optimization

### Caching Strategies
- Prompt template caching at module level
- Tool definition pre-compilation
- Common response patterns
- Database metadata caching

### Parallel Processing
When implementing multiple agents:
```python
# Future pattern for parallel agent execution
async def process_with_agents(query, context):
    clarify_task = clarifier.process_async(query)
    plan_task = planner.process_async(query)
    
    clarification = await clarify_task
    plan = await plan_task
    
    return combine_results(clarification, plan)
```

## Monitoring Integration

### Metrics Collection
Each agent logs:
- Processing time
- Token usage
- Cost calculation
- Decision rationale
- Error occurrences

### Stage Tracking
```python
add_monitor_entry(
    stage_name=f"Agent_{agent_name}",
    stage_start_time=start,
    stage_end_time=end,
    status="Success",
    llm_calls=[{
        "model": model_used,
        "tokens": token_count,
        "cost": calculated_cost
    }],
    decision_details=decision_rationale,
    custom_metadata=agent_specific_data
)
```

## Testing Strategy

### Unit Testing
- Mock LLM responses for deterministic testing
- Verify prompt assembly logic
- Test error handling paths
- Validate output schemas

### Integration Testing
- End-to-end workflow with all agents
- Context propagation verification
- Performance benchmarking
- Cost tracking accuracy

### Test Fixtures
```python
# Common test fixtures
@pytest.fixture
def mock_router_response():
    return {
        "route": "research_workflow",
        "confidence": 0.95,
        "rationale": "Data request detected"
    }

@pytest.fixture
def test_context():
    return {
        "execution_id": "test-uuid",
        "auth_config": {"type": "api_key"},
        "ssl_config": {"verify": False}
    }
```

## Future Enhancements

### Planned Features
1. **Agent Collaboration**: Inter-agent communication
2. **Learning Loop**: Performance-based prompt tuning
3. **Custom Agents**: User-defined agent creation
4. **Agent Marketplace**: Shareable agent templates
5. **Async Processing**: Full async/await support

### Scalability Considerations
- Stateless agent design for horizontal scaling
- Message queue integration for agent coordination
- Distributed caching for shared context
- Load balancing across agent instances

## Best Practices

### Implementation Guidelines
1. Keep agents focused on single responsibility
2. Use consistent error handling patterns
3. Log all decisions for audit trail
4. Implement graceful degradation
5. Optimize for streaming responses
6. Cache expensive computations
7. Version prompts and logic together
8. Write comprehensive tests
9. Monitor performance metrics
10. Document agent capabilities clearly