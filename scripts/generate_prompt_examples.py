"""
Generate complete system and user prompts for each agent.

This script loads actual YAML files and generates the prompts
that would be sent to the LLM for an example query.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aegis.utils.prompt_loader import load_yaml, load_global_prompts_for_agent
from aegis.utils.database_filter import get_database_prompt


def format_section(title, content):
    """Format a section with borders."""
    border = "=" * 80
    return f"\n{border}\n{title}\n{border}\n\n{content}\n"


def main():
    """Generate example prompts for all agents."""

    # Example query
    example_query = "What was RBC's Q3 2024 revenue?"
    example_conversation = [
        {"role": "user", "content": example_query}
    ]

    # Available databases for filtering
    available_databases = ["benchmarking", "reports", "rts", "transcripts"]

    output = []
    output.append("# Aegis Agent Prompts - Complete Examples\n")
    output.append(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    output.append(f"**Example Query**: \"{example_query}\"\n")
    output.append("\n---\n")

    # ========================================================================
    # ROUTER AGENT
    # ========================================================================
    output.append("\n## 1. Router Agent\n")
    output.append("**Purpose**: Binary routing decision (direct_response vs research_workflow)\n")

    # Load router YAML
    router_data = load_yaml("aegis/router.yaml")

    # Load globals
    uses_global = router_data.get("uses_global", [])
    globals_prompt = load_global_prompts_for_agent(uses_global, available_databases)

    # Build system prompt
    system_prompt_template = router_data.get("system_prompt", "")
    available_dbs_str = ', '.join(available_databases)
    agent_system_prompt = system_prompt_template.format(available_databases=available_dbs_str)

    prompt_parts = []
    if globals_prompt:
        prompt_parts.append(globals_prompt)
    prompt_parts.append(agent_system_prompt)
    router_system_prompt = "\n\n---\n\n".join(prompt_parts)

    # Build user prompt
    user_prompt_template = router_data.get("user_prompt_template", "")
    conversation_history = "user: What was RBC's Q3 2024 revenue?"
    router_user_prompt = user_prompt_template.format(
        conversation_history=conversation_history,
        current_query=example_query
    )

    output.append(format_section("### System Prompt", router_system_prompt))
    output.append(format_section("### User Prompt", router_user_prompt))

    # ========================================================================
    # CLARIFIER AGENT - BANKS
    # ========================================================================
    output.append("\n## 2. Clarifier Agent - Bank Extraction\n")
    output.append("**Purpose**: Identify banks and create comprehensive query intent\n")

    # Load clarifier banks YAML
    clarifier_banks_data = load_yaml("aegis/clarifier_banks.yaml")

    # Load globals
    uses_global = clarifier_banks_data.get("uses_global", [])
    globals_prompt = load_global_prompts_for_agent(uses_global, available_databases)

    # Build system prompt with bank index (example data)
    bank_index = """
<bank_index>
Available Banks:

ID: 1
Name: Royal Bank of Canada
Symbol: RY
Aliases: RBC, Royal Bank
Categories: Big Six, Canadian Banks
Available in databases: benchmarking, reports, rts, transcripts

ID: 2
Name: Toronto-Dominion Bank
Symbol: TD
Aliases: TD Bank
Categories: Big Six, Canadian Banks
Available in databases: benchmarking, reports, rts, transcripts

ID: 3
Name: Bank of Nova Scotia
Symbol: BNS
Aliases: Scotiabank, BNS
Categories: Big Six, Canadian Banks
Available in databases: benchmarking, reports, rts, transcripts
</bank_index>
"""

    agent_system_prompt = clarifier_banks_data.get("system_prompt", "")

    prompt_parts = []
    if globals_prompt:
        prompt_parts.append(globals_prompt)
    prompt_parts.append(bank_index)
    prompt_parts.append(agent_system_prompt.strip())

    clarifier_banks_system_prompt = "\n\n".join(prompt_parts)

    # Build user prompt
    user_prompt_template = clarifier_banks_data.get("user_prompt_template", "")
    clarifier_banks_user_prompt = user_prompt_template.format(query=example_query)

    output.append(format_section("### System Prompt", clarifier_banks_system_prompt))
    output.append(format_section("### User Prompt", clarifier_banks_user_prompt))

    # ========================================================================
    # CLARIFIER AGENT - PERIODS
    # ========================================================================
    output.append("\n## 3. Clarifier Agent - Period Extraction\n")
    output.append("**Purpose**: Extract and validate fiscal periods\n")

    # Load clarifier periods YAML
    clarifier_periods_data = load_yaml("aegis/clarifier_periods.yaml")

    # Load globals
    uses_global = clarifier_periods_data.get("uses_global", [])
    globals_prompt = load_global_prompts_for_agent(uses_global, available_databases)

    # Build system prompt with period availability (example data)
    period_availability = """
<period_availability>
Bank: Royal Bank of Canada (RY)

FY2024:
  Q1 (Nov 2023 - Jan 2024): benchmarking, reports, rts, transcripts
  Q2 (Feb 2024 - Apr 2024): benchmarking, reports, rts, transcripts
  Q3 (May 2024 - Jul 2024): benchmarking, reports, rts, transcripts
  Q4 (Aug 2024 - Oct 2024): reports (PENDING: benchmarking, rts, transcripts)

FY2023:
  Q1-Q4: All databases available

Latest Reported Period: Q3 2024 (reported September 2024)
Note: Q4 2024 results not yet available (1-month reporting lag)
</period_availability>
"""

    agent_system_prompt = clarifier_periods_data.get("system_prompt", "")

    prompt_parts = []
    if globals_prompt:
        prompt_parts.append(globals_prompt)
    prompt_parts.append(period_availability)
    prompt_parts.append(agent_system_prompt.strip())

    clarifier_periods_system_prompt = "\n\n".join(prompt_parts)

    # Build user prompt
    user_prompt_template = clarifier_periods_data.get("user_prompt_template", "")
    clarifier_periods_user_prompt = user_prompt_template.format(query=example_query)

    output.append(format_section("### System Prompt", clarifier_periods_system_prompt))
    output.append(format_section("### User Prompt", clarifier_periods_user_prompt))

    # ========================================================================
    # PLANNER AGENT
    # ========================================================================
    output.append("\n## 4. Planner Agent\n")
    output.append("**Purpose**: Select databases to query based on intent\n")

    # Load planner YAML
    planner_data = load_yaml("aegis/planner.yaml")

    # Load globals
    uses_global = planner_data.get("uses_global", [])
    globals_prompt = load_global_prompts_for_agent(uses_global, available_databases)

    # Build system prompt with availability table (example data)
    availability_table = """
<availability_table>
Filtered by requested banks and periods:

Bank | Name                         | Year | Quarter | Available Databases
-----|------------------------------|------|---------|--------------------
  1  | Royal Bank of Canada (RY)    | 2024 |   Q3    | benchmarking, reports, rts, transcripts

Summary of available databases across all requested banks/periods:
benchmarking, reports, rts, transcripts
</availability_table>
"""

    agent_system_prompt = planner_data.get("system_prompt", "")

    prompt_parts = []
    if globals_prompt:
        prompt_parts.append(globals_prompt)
    prompt_parts.append(availability_table)
    prompt_parts.append(agent_system_prompt.strip())

    planner_system_prompt = "\n\n".join(prompt_parts)

    # Build user prompt
    user_prompt_template = planner_data.get("user_prompt_template", "")
    conversation_context = "user: What was RBC's Q3 2024 revenue?"
    query_intent = "Retrieve the total revenue for Royal Bank of Canada for Q3 2024 (May-July 2024 fiscal quarter)"
    planner_user_prompt = user_prompt_template.format(
        conversation_context=conversation_context,
        query=example_query,
        query_intent=query_intent
    )

    output.append(format_section("### System Prompt", planner_system_prompt))
    output.append(format_section("### User Prompt", planner_user_prompt))

    # ========================================================================
    # RESPONSE AGENT
    # ========================================================================
    output.append("\n## 5. Response Agent\n")
    output.append("**Purpose**: Direct responses without database retrieval\n")

    # Load response YAML
    response_data = load_yaml("aegis/response.yaml")

    # Load globals
    uses_global = response_data.get("uses_global", [])
    globals_prompt = load_global_prompts_for_agent(uses_global, available_databases)

    # Build system prompt
    agent_system_prompt = response_data.get("system_prompt", "")

    prompt_parts = []
    if globals_prompt:
        prompt_parts.append(globals_prompt)
    if agent_system_prompt:
        prompt_parts.append(agent_system_prompt.strip())
    response_system_prompt = "\n\n---\n\n".join(prompt_parts)

    # Build user prompt - use a greeting example instead
    user_prompt_template = response_data.get("user_prompt_template", "")
    greeting_query = "Hello! Can you explain what Aegis does?"
    response_user_prompt = user_prompt_template.format(latest_message=greeting_query)

    output.append("**Note**: Response agent handles greetings, definitions, and system questions.\n")
    output.append(f"**Example Query for Response Agent**: \"{greeting_query}\"\n")
    output.append(format_section("### System Prompt", response_system_prompt))
    output.append(format_section("### User Prompt", response_user_prompt))

    # ========================================================================
    # SUMMARIZER AGENT
    # ========================================================================
    output.append("\n## 6. Summarizer Agent\n")
    output.append("**Purpose**: Synthesize multiple database responses into concise summary\n")

    # Load summarizer YAML
    summarizer_data = load_yaml("aegis/summarizer.yaml")

    # Load globals
    uses_global = summarizer_data.get("uses_global", [])
    globals_prompt = load_global_prompts_for_agent(uses_global, available_databases)

    # Build system prompt
    agent_system_prompt = summarizer_data.get("system_prompt", "")

    prompt_parts = []
    if globals_prompt:
        prompt_parts.append(globals_prompt)
    if agent_system_prompt:
        prompt_parts.append(agent_system_prompt.strip())
    summarizer_system_prompt = "\n\n---\n\n".join(prompt_parts)

    # Build user prompt with example database responses
    database_responses_example = """
<database_response source="benchmarking">
Query Intent: Retrieve the total revenue for Royal Bank of Canada for Q3 2024
Response:
Royal Bank of Canada reported total revenue of $13.2 billion for Q3 2024 (May-July 2024 fiscal quarter), representing a 6% increase compared to Q3 2023 ($12.5 billion).

Key revenue components:
- Personal & Commercial Banking: $5.8 billion (+5% YoY)
- Wealth Management: $3.4 billion (+8% YoY)
- Insurance: $2.1 billion (+4% YoY)
- Capital Markets: $1.9 billion (+7% YoY)
</database_response>

<database_response source="rts">
Query Intent: Retrieve the total revenue for Royal Bank of Canada for Q3 2024
Response:
According to RBC's Q3 2024 Report to Shareholders (filed August 28, 2024):

Total revenue: $13,152 million (compared to $12,473 million in Q3 2023)

Management noted that the revenue growth was driven by strong performance across all business segments, with particular strength in Wealth Management and Capital Markets.
</database_response>

<database_response source="transcripts">
Query Intent: Retrieve the total revenue for Royal Bank of Canada for Q3 2024
Response:
During the Q3 2024 earnings call (August 28, 2024), CFO Nadine Ahn commented:

"We're very pleased with our Q3 revenue performance of $13.2 billion, up 6% year-over-year. This reflects the strength and diversification of our franchise. We saw particularly strong momentum in Wealth Management, where revenues grew 8%, and Capital Markets, which delivered 7% growth despite challenging market conditions."

CEO Dave McKay added: "Our revenue growth demonstrates the resilience of our diversified business model and our ability to execute across all segments."
</database_response>
"""

    user_prompt_template = summarizer_data.get("user_prompt_template", "")
    summarizer_user_prompt = user_prompt_template.format(
        user_query=example_query,
        database_responses=database_responses_example
    )

    output.append(format_section("### System Prompt", summarizer_system_prompt))
    output.append(format_section("### User Prompt", summarizer_user_prompt))

    # ========================================================================
    # Write to file
    # ========================================================================
    output_file = Path(__file__).parent.parent / "PROMPT_EXAMPLES.md"
    with open(output_file, "w") as f:
        f.write("\n".join(output))

    print(f"✓ Generated prompt examples: {output_file}")
    print(f"  - 6 agents documented")
    print(f"  - Complete system and user prompts")
    print(f"  - Example query: '{example_query}'")


if __name__ == "__main__":
    main()
