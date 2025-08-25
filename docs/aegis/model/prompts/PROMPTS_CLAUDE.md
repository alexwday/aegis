# Prompts Module - Claude Context

## Module Purpose
The prompts module contains all YAML-based prompt templates that guide the behavior of Aegis agents and subagents. These prompts define the intelligence, decision-making logic, and response patterns of the system.

## Prompt Architecture

### Hierarchical Structure
```
prompts/
├── aegis/          # Core agent prompts
├── global/         # Shared context and knowledge
└── [domains]/      # Domain-specific subagent prompts
```

### Prompt Composition Pattern
Prompts are composed by combining:
1. **Global Context**: Project capabilities, database info
2. **Agent-Specific Logic**: Decision rules and objectives
3. **Dynamic Context**: User permissions, filtered databases
4. **Tool Definitions**: Function calling schemas

## Core Agent Prompts (`aegis/`)

### Router (`router.yaml`)
**Purpose**: Binary routing decision for query processing

**Key Features**:
- Binary decision: 0 (direct) or 1 (research)
- Clear routing rules and examples
- Tool-based structured output
- Bias toward research for uncertainty

**Decision Matrix**:
- **Direct Response (0)**: Greetings, system questions, history reformatting
- **Research Workflow (1)**: Data requests, entity queries, complex analysis

### Clarifier (`clarifier.yaml`)
**Purpose**: Disambiguate vague or incomplete queries

**Capabilities**:
- Entity disambiguation (e.g., "Which bank do you mean?")
- Metric clarification (e.g., "ROE for which period?")
- Intent refinement
- Multi-option presentation

### Planner (`planner.yaml`)
**Purpose**: Create research execution plans

**Planning Process**:
1. Query decomposition
2. Data source identification
3. Execution order determination
4. Resource allocation

### Response (`response.yaml`)
**Purpose**: Generate final user responses

**Response Types**:
- Data presentation with formatting
- Narrative explanations
- Comparative analysis
- Error messages and fallbacks

### Summarizer (`summarizer.yaml`)
**Purpose**: Condense conversation history

**Use Cases**:
- Context compression for token limits
- Key point extraction
- Decision trail documentation
- Report generation

## Global Context Prompts (`global/`)

### Project Context (`project.yaml`)
**Purpose**: Define Aegis capabilities and scope

**Contents**:
- System capabilities and limitations
- Available data sources
- Supported query types
- Integration points

### Database Context (`database.yaml`)
**Purpose**: Describe available data sources

**Database Categories**:
1. **Internal Databases**:
   - `internal_capm`: Risk metrics
   - `internal_wiki`: Documentation
   - `internal_research`: Analysis

2. **External Databases**:
   - `external_ey`: Ernst & Young data
   - `external_pwc`: PwC benchmarks
   - `external_peer`: Peer comparisons

3. **Specialized Sources**:
   - `transcripts`: Earnings calls
   - `benchmarking`: Comparative metrics
   - `rts`: Real-time systems

### Bank Information (`banks.yaml`)
**Purpose**: Bank-specific context and identifiers

**Information Types**:
- Official names and tickers
- Geographic presence
- Business segments
- Key metrics focus

### Restrictions (`restrictions.yaml`)
**Purpose**: Define access controls and limitations

**Restriction Types**:
- Data access permissions
- Query complexity limits
- Rate limiting rules
- Compliance boundaries

### Fiscal Context (`fiscal.py`)
**Purpose**: Dynamic fiscal period calculations

**Features**:
- Quarter/year mapping
- Period comparison logic
- Date range generation
- Reporting calendar alignment

## Domain Prompts (Subagents)

### Benchmarking (`benchmarking/`)
**Purpose**: Comparative analysis prompts

**Capabilities**:
- Peer group selection
- Metric normalization
- Trend analysis
- Ranking generation

### Reports (`reports/`)
**Purpose**: Document generation templates

**Report Types**:
- Executive summaries
- Detailed analysis
- Comparison tables
- Trend reports

### RTS (`rts/`)
**Purpose**: Real-time system integration

**Query Types**:
- Live data retrieval
- Stream processing
- Alert generation
- Threshold monitoring

### Transcripts (`transcripts/`)
**Purpose**: Earnings call analysis

**Analysis Features**:
- Sentiment extraction
- Key topic identification
- Management guidance
- Q&A insights

### Pillar 3 (`pillar3/`)
**Purpose**: Regulatory reporting prompts

**Compliance Areas**:
- Capital requirements
- Risk disclosures
- Liquidity metrics
- Governance reporting

## Prompt Engineering Best Practices

### Structure Guidelines
```yaml
name: agent_name
version: "1.0.0"
last_updated: "2025-01-23"
uses_global:
  - project
  - database

content: |
  <prompt>
    <context>System role and capabilities</context>
    <objective>Clear goal statement</objective>
    <rules>Specific constraints and guidelines</rules>
    <examples>Input/output examples</examples>
    <response>Expected format and structure</response>
  </prompt>

tool_definition: |
  {JSON schema for function calling}
```

### Optimization Techniques
- **Clarity**: Unambiguous instructions
- **Brevity**: Concise without losing context
- **Examples**: Representative input/output pairs
- **Constraints**: Clear boundaries and limitations
- **Fallbacks**: Error handling guidance

## Dynamic Prompt Assembly

### Composition Process
1. Load base agent prompt
2. Inject global context (project, database)
3. Apply user-specific filtering
4. Add runtime context (execution_id, permissions)
5. Include tool definitions

### Context Injection Example
```python
# Load prompts
router_prompt = load_yaml("aegis/router.yaml")
project_context = load_yaml("global/project.yaml")
database_context = filter_databases(db_names)

# Compose full prompt
full_prompt = f"""
{project_context}
---
{database_context}
---
{router_prompt}
"""
```

## Tool Definition Integration

### Function Calling Schema
Prompts include OpenAI function schemas:
```yaml
tool_definition: |
  {
    "name": "route",
    "description": "Routing decision",
    "parameters": {
      "type": "object",
      "properties": {
        "r": {
          "type": "integer",
          "enum": [0, 1]
        }
      },
      "required": ["r"]
    }
  }
```

## Version Management

### Versioning Strategy
- Semantic versioning (major.minor.patch)
- Last updated timestamps
- Backward compatibility notes
- Migration guides for breaking changes

### Change Tracking
- Git history for prompt evolution
- A/B testing support
- Performance metrics per version
- Rollback procedures

## Testing Prompts

### Validation Approaches
- YAML syntax validation
- Schema compliance checking
- Example verification
- Output format testing

### Performance Testing
- Response quality metrics
- Token usage optimization
- Latency measurements
- Cost analysis

## Security Considerations

### Prompt Injection Prevention
- Input sanitization
- Output validation
- Role boundary enforcement
- Instruction hierarchy

### Data Protection
- No PII in prompts
- Credential exclusion
- Access control enforcement
- Audit logging

## Future Enhancements

### Planned Improvements
- Multi-language support
- Dynamic prompt generation
- Contextual adaptation
- Performance auto-tuning
- Prompt chaining optimization