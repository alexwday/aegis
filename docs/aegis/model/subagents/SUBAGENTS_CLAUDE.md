# Subagents Module - Claude Context

## Module Purpose
The subagents module will contain specialized agents that handle domain-specific data retrieval and analysis tasks. These subagents are invoked by the main Aegis agent to gather information from various data sources.

## Architecture Overview

### Subagent Pattern
Each subagent follows a consistent pattern:
1. **Specialized Knowledge**: Domain-specific expertise
2. **Data Source Integration**: Direct connection to specific databases
3. **Streaming Response**: Real-time data retrieval and processing
4. **Cost Optimization**: Appropriate model tier for task complexity

### Communication Protocol
Subagents communicate through the unified message schema:
```python
{
    "type": "subagent",
    "name": "subagent_name",
    "content": "Retrieved data or status update"
}
```

## Planned Subagents

### 1. Benchmarking Subagent
**Purpose**: Comparative analysis across financial institutions

**Capabilities**:
- Peer group comparison
- Metric normalization across banks
- Trend analysis and ranking
- Historical performance tracking

**Data Sources**:
- `benchmarking` database
- `external_ey`, `external_pwc` for third-party data
- `internal_research` for proprietary analysis

**Example Queries**:
- "Compare RBC's ROE with peer banks"
- "Show efficiency ratio rankings for Canadian banks"
- "Benchmark TD's digital adoption metrics"

### 2. Reports Subagent
**Purpose**: Generate formatted reports and documents

**Capabilities**:
- Executive summary generation
- Detailed analysis compilation
- Multi-source data aggregation
- Custom formatting options

**Output Formats**:
- Narrative reports
- Comparison tables
- Trend visualizations
- Key metrics dashboards

**Example Queries**:
- "Generate Q3 performance report for RBC"
- "Create executive summary of banking sector trends"
- "Compile risk metrics report across all banks"

### 3. RTS (Real-Time Systems) Subagent
**Purpose**: Access real-time financial data and metrics

**Capabilities**:
- Live data retrieval
- Streaming updates
- Alert generation
- Threshold monitoring

**Data Sources**:
- Real-time trading systems
- Market data feeds
- Internal monitoring systems
- Risk management platforms

**Example Queries**:
- "Current trading volume for RBC"
- "Real-time risk exposure metrics"
- "Active alerts for capital thresholds"

### 4. Transcripts Subagent
**Purpose**: Analyze earnings calls and investor communications

**Capabilities**:
- Sentiment analysis
- Key topic extraction
- Management guidance tracking
- Q&A insights mining

**Data Sources**:
- `transcripts` database
- Earnings call recordings
- Investor presentation archives
- Analyst meeting notes

**Example Queries**:
- "What did RBC say about digital transformation in Q3?"
- "Management guidance from TD's latest earnings call"
- "Sentiment analysis of BMO's investor day"

### 5. Pillar 3 Subagent
**Purpose**: Handle regulatory reporting and compliance data

**Capabilities**:
- Capital requirement calculations
- Risk disclosure compilation
- Liquidity metric reporting
- Governance documentation

**Regulatory Focus**:
- Basel III compliance
- OSFI requirements
- Stress testing results
- Capital adequacy ratios

**Example Queries**:
- "RBC's Tier 1 capital ratio trends"
- "Liquidity coverage ratio for all banks"
- "Latest stress test results comparison"

## Implementation Pattern

### Base Subagent Class (Planned)
```python
class BaseSubagent:
    """Base class for all subagents."""
    
    def __init__(self, name: str, data_sources: List[str]):
        self.name = name
        self.data_sources = data_sources
        self.logger = get_logger()
    
    async def process(
        self,
        query: str,
        context: Dict[str, Any]
    ) -> AsyncGenerator[Dict[str, str], None]:
        """Process query and yield results."""
        # Validate access to data sources
        # Execute query against sources
        # Stream results back
        pass
    
    def get_model_tier(self, query_complexity: str) -> str:
        """Determine appropriate model tier."""
        pass
```

### Subagent Registry
```python
SUBAGENT_REGISTRY = {
    "benchmarking": BenchmarkingSubagent,
    "reports": ReportsSubagent,
    "rts": RTSSubagent,
    "transcripts": TranscriptsSubagent,
    "pillar3": Pillar3Subagent
}

def get_subagent(name: str) -> BaseSubagent:
    """Factory function to instantiate subagents."""
    return SUBAGENT_REGISTRY[name]()
```

## Integration with Main Workflow

### Invocation Pattern
The main agent invokes subagents based on:
1. Query analysis and routing decision
2. Required data sources identification
3. User permissions and access control
4. Cost and performance optimization

### Parallel Execution
Multiple subagents can run concurrently:
```python
async def gather_from_subagents(
    query: str,
    required_subagents: List[str],
    context: Dict
) -> AsyncGenerator[Dict, None]:
    """Gather data from multiple subagents."""
    
    tasks = []
    for subagent_name in required_subagents:
        subagent = get_subagent(subagent_name)
        task = subagent.process(query, context)
        tasks.append(task)
    
    # Stream results as they arrive
    async for result in merge_streams(tasks):
        yield result
```

## Data Source Mapping

### Subagent to Database Mapping
```python
SUBAGENT_SOURCES = {
    "benchmarking": [
        "benchmarking",
        "external_ey",
        "external_pwc",
        "internal_research"
    ],
    "reports": [
        "internal_wiki",
        "internal_research",
        "*"  # Can access all permitted sources
    ],
    "rts": [
        "rts_trading",
        "rts_risk",
        "market_data"
    ],
    "transcripts": [
        "transcripts",
        "investor_relations"
    ],
    "pillar3": [
        "regulatory",
        "risk_metrics",
        "capital_data"
    ]
}
```

## Performance Optimization

### Caching Strategy
- Query result caching with TTL
- Database metadata caching
- Frequently accessed data preloading
- Connection pooling per data source

### Streaming Optimization
- Chunked response generation
- Progressive result refinement
- Early result yielding
- Backpressure handling

## Error Handling

### Failure Modes
1. **Data Source Unavailable**: Fallback to cached data
2. **Query Timeout**: Return partial results
3. **Permission Denied**: Clear error message
4. **Invalid Query**: Request clarification
5. **Rate Limited**: Queue and retry

### Recovery Pattern
```python
async def safe_subagent_execution(
    subagent: BaseSubagent,
    query: str,
    context: Dict
) -> AsyncGenerator[Dict, None]:
    """Execute subagent with error handling."""
    try:
        async for result in subagent.process(query, context):
            yield result
    except DataSourceError:
        yield {
            "type": "subagent",
            "name": subagent.name,
            "content": "Data source temporarily unavailable"
        }
    except Exception as e:
        logger.error(f"Subagent {subagent.name} failed", error=str(e))
        yield {
            "type": "subagent",
            "name": subagent.name,
            "content": "Unable to retrieve data"
        }
```

## Monitoring and Metrics

### Subagent Metrics
Each subagent tracks:
- Query processing time
- Data source latency
- Result size and quality
- Cache hit rates
- Error frequencies

### Cost Attribution
```python
def track_subagent_costs(
    subagent_name: str,
    llm_calls: List[Dict],
    data_queries: List[Dict]
) -> Dict:
    """Track costs per subagent."""
    return {
        "subagent": subagent_name,
        "llm_cost": sum(call["cost"] for call in llm_calls),
        "data_cost": calculate_data_costs(data_queries),
        "total_cost": llm_cost + data_cost
    }
```

## Testing Approach

### Unit Testing
- Mock data source responses
- Verify query transformation
- Test error handling
- Validate streaming behavior

### Integration Testing
- End-to-end subagent flow
- Multi-subagent coordination
- Performance benchmarking
- Cost calculation verification

## Future Enhancements

### Planned Features
1. **Smart Routing**: ML-based subagent selection
2. **Result Fusion**: Intelligent data combination
3. **Adaptive Caching**: Usage-based cache optimization
4. **Custom Subagents**: User-defined subagents
5. **Federated Queries**: Cross-subagent data joins

### Scalability Roadmap
- Microservice architecture for subagents
- Kubernetes deployment for auto-scaling
- Event-driven subagent triggering
- Distributed caching layer
- GraphQL API for subagent access