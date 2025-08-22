# Aegis Subagents

This directory will contain specialized subagents that perform specific database searches and data retrieval tasks.

## Planned Subagents

### Transcripts Subagent
- **Purpose**: Searches and analyzes earnings call transcripts
- **Databases**: Transcript database systems
- **Capabilities**:
  - Full-text search of transcripts
  - Speaker identification
  - Temporal filtering (by quarter/year)
  - Sentiment analysis of discussions
- **Output**: Relevant transcript excerpts with context

### RTS Subagent (Revenue Tracking System)
- **Purpose**: Queries financial and revenue data
- **Databases**: RTS database
- **Capabilities**:
  - Revenue metrics by period
  - Segment breakdowns
  - Year-over-year comparisons
  - Trend analysis
- **Output**: Structured financial data

### Reports Subagent
- **Purpose**: Searches formal reports and documentation
- **Databases**: Internal report repositories
- **Capabilities**:
  - Document search
  - Section extraction
  - Metadata filtering
- **Output**: Relevant report sections

### Benchmarking Subagent
- **Purpose**: Provides comparative analysis and benchmarks
- **Databases**: Benchmark and comparison databases
- **Capabilities**:
  - Industry comparisons
  - Peer analysis
  - Historical benchmarks
- **Output**: Comparative metrics and insights

## Subagent Communication Protocol

All subagents follow the unified message schema:
```python
{
    "type": "subagent",
    "name": "subagent_name",
    "content": "Search results or status updates"
}
```

## Parallel Execution

Subagents are designed for parallel execution:
1. Planner agent determines which subagents to invoke
2. Multiple subagents launched concurrently
3. Each streams results independently
4. UI displays results in separate dropdowns
5. Main agent synthesizes all results once complete

## Database Filtering

Subagents respect the `db_names` filter parameter:
- Only search databases included in the filter list
- Filter is applied before subagent execution
- Logged in process monitoring for audit trail

## Integration Points

Subagents will be called from:
- `src/aegis/model/main.py:315-348` - Current mock implementation location
- Future: Planner agent will orchestrate subagent execution