# Benchmarking Prompts - Claude Context

## Module Purpose
The benchmarking prompts module will contain YAML templates for comparative analysis queries across financial institutions. These prompts guide the benchmarking subagent in retrieving and analyzing peer comparison data.

## Planned Prompts

### 1. Peer Comparison (`peer_comparison.yaml`)
**Purpose**: Compare metrics across similar institutions
- Peer group identification
- Metric normalization rules
- Ranking methodologies
- Statistical analysis templates

### 2. Trend Analysis (`trend_analysis.yaml`)
**Purpose**: Analyze performance trends over time
- Time series comparisons
- Growth rate calculations
- Volatility assessments
- Forecast generation

### 3. Best Practices (`best_practices.yaml`)
**Purpose**: Identify industry-leading practices
- Top performer identification
- Success factor analysis
- Gap analysis templates
- Improvement recommendations

## Integration Points
- Works with benchmarking subagent
- Accesses external data sources (EY, PwC)
- Provides structured comparison outputs
- Supports multiple output formats

## Data Sources
- `benchmarking` database
- `external_ey` - Ernst & Young benchmarks
- `external_pwc` - PwC industry data
- `internal_research` - Proprietary analysis

## Example Use Cases
- "Compare RBC's efficiency ratio with Big 6 Canadian banks"
- "Benchmark digital adoption metrics across North American banks"
- "Identify best-in-class risk management practices"