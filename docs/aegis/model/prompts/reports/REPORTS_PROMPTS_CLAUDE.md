# Reports Prompts - Claude Context

## Module Purpose
The reports prompts module will contain YAML templates for generating various types of financial reports and documents. These prompts guide report formatting, structure, and content organization.

## Planned Prompts

### 1. Executive Summary (`executive_summary.yaml`)
**Purpose**: Generate high-level summaries for executives
- Key metrics highlighting
- Decision-ready insights
- Risk and opportunity flags
- Action item extraction

### 2. Detailed Analysis (`detailed_analysis.yaml`)
**Purpose**: Create comprehensive analytical reports
- Multi-source data integration
- Statistical analysis presentation
- Trend identification
- Root cause analysis

### 3. Comparison Report (`comparison_report.yaml`)
**Purpose**: Structure comparative analysis documents
- Side-by-side comparisons
- Variance analysis
- Performance gaps
- Competitive positioning

### 4. Dashboard Generation (`dashboard.yaml`)
**Purpose**: Create data visualization specifications
- KPI selection
- Chart type recommendations
- Layout optimization
- Drill-down hierarchies

## Report Formats
- **Narrative**: Long-form written analysis
- **Tabular**: Structured data tables
- **Visual**: Chart and graph specifications
- **Hybrid**: Combined formats

## Customization Options
- Audience-specific language (technical vs. executive)
- Length preferences (brief vs. comprehensive)
- Focus areas (risk, performance, compliance)
- Branding and formatting guidelines

## Integration Points
- Works with reports subagent
- Aggregates data from multiple sources
- Supports various output formats
- Includes citation management

## Example Use Cases
- "Generate Q3 2024 performance report for RBC"
- "Create risk assessment summary for board meeting"
- "Compile regulatory compliance dashboard"