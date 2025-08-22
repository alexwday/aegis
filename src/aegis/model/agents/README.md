# Aegis Agents

This directory will contain the main agent implementations for the Aegis model.

## Planned Agents

### Router Agent
- **Purpose**: Analyzes incoming queries and determines the appropriate processing path
- **Decisions**: 
  - Direct response (simple queries)
  - Clarifier (needs more information)
  - Planner (complex queries requiring database searches)
- **Input**: Processed conversation
- **Output**: Routing decision with reasoning

### Clarifier Agent
- **Purpose**: Asks clarifying questions when user intent is ambiguous
- **Input**: Conversation context
- **Output**: Clarifying questions to user

### Planner Agent
- **Purpose**: Creates and executes database query plans for complex questions
- **Process**:
  1. Analyzes query requirements
  2. Determines which databases to search
  3. Creates parallel query plan
  4. Orchestrates subagent execution
  5. Synthesizes results
- **Input**: Conversation + available databases
- **Output**: Coordinated response from multiple sources

### Response Agent
- **Purpose**: Generates final responses to user queries
- **Features**:
  - Markdown formatting
  - Reference linking
  - Source attribution
- **Input**: Aggregated search results
- **Output**: Formatted response with citations

### Summarizer Agent
- **Purpose**: Creates concise summaries of lengthy content
- **Input**: Long-form text or multiple documents
- **Output**: Condensed summary maintaining key points

## Agent Communication Protocol

All agents follow the unified message schema:
```python
{
    "type": "agent",
    "name": "agent_name",
    "content": "Response content"
}
```

## Integration Points

Agents will be integrated into the main workflow at:
- `src/aegis/model/main.py:306-313` - Router decision point
- `src/aegis/model/main.py:315-348` - Agent execution
- `src/aegis/model/main.py:350-370` - Response synthesis