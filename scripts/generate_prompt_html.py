"""
Generate HTML documentation for COSTAR-formatted prompts.

This script creates two-column HTML documents that visualize
AI prompts with business explanations.
"""

import re
from pathlib import Path
from typing import Dict, List
from dataclasses import dataclass


@dataclass
class Section:
    """Represents a section of the prompt."""

    name: str
    color: str
    color_rgb: str
    icon: str
    content: str
    explanation: str
    key_points: List[str]


# Color palette for COSTAR sections
SECTION_COLORS = {
    "context": {"hex": "#1e40af", "rgb": "30, 64, 175", "icon": "🎭"},
    "objective": {"hex": "#059669", "rgb": "5, 150, 105", "icon": "🎯"},
    "style": {"hex": "#7c3aed", "rgb": "124, 58, 237", "icon": "✍️"},
    "tone": {"hex": "#8b5cf6", "rgb": "139, 92, 246", "icon": "🎵"},
    "audience": {"hex": "#0d9488", "rgb": "13, 148, 136", "icon": "👥"},
    "analysis_framework": {"hex": "#4f46e5", "rgb": "79, 70, 229", "icon": "🔬"},
    "response_framework": {"hex": "#4f46e5", "rgb": "79, 70, 229", "icon": "🔬"},
    "extraction_strategy_guidance": {
        "hex": "#ea580c",
        "rgb": "234, 88, 12",
        "icon": "📍",
    },
    "cross_category_notes_guidance": {
        "hex": "#ea580c",
        "rgb": "234, 88, 12",
        "icon": "🔗",
    },
    "quote_selection_strategy": {"hex": "#ea580c", "rgb": "234, 88, 12", "icon": "💬"},
    "deduplication_strategy": {"hex": "#d97706", "rgb": "217, 119, 6", "icon": "⚠️"},
    "quality_standards": {"hex": "#e11d48", "rgb": "225, 29, 72", "icon": "⭐"},
    "response_format": {"hex": "#0891b2", "rgb": "8, 145, 178", "icon": "📤"},
}


# Concise business explanations
EXPLANATIONS = {
    "context": {
        "explanation": "Establishes the AI's role, identity, and working environment. This section creates the persona and provides the situational context needed to complete the task, including dynamic variables that get injected at runtime.",
        "key_points": [
            "**Role Assignment**: Defines the AI's professional identity and expertise level",
            "**Dynamic Variables**: Placeholders like `{bank_name}`, `{quarter}` are filled at runtime",
            "**Nested Context**: XML tags organize related information hierarchically",
            "**Scenario Setup**: Provides all necessary background to understand the task",
        ],
    },
    "objective": {
        "explanation": "Defines the specific, measurable goals the AI must achieve. Each numbered item is a success criterion. The objectives are ordered by importance and collectively define what 'complete' and 'correct' output looks like.",
        "key_points": [
            "**Clear Success Criteria**: Enumerated goals provide completion checklist",
            "**Priority Signaling**: Order suggests relative importance of objectives",
            "**Comprehensive Scope**: Multiple objectives ensure nothing is missed",
            "**Actionable Language**: Verbs like 'Ensures', 'Maps', 'Identifies' create clear directives",
        ],
    },
    "style": {
        "explanation": "Specifies the writing approach, formatting characteristics, and structural preferences. This calibrates the AI's output to match organizational standards and ensures consistency across multiple generations.",
        "key_points": [
            "**Writing Approach**: Defines formality level and presentation style",
            "**Comprehensiveness**: Sets expectations for depth and thoroughness",
            "**Precision Standards**: Establishes required level of specificity",
            "**Organizational Patterns**: Guides how to structure information",
        ],
    },
    "tone": {
        "explanation": "Defines the voice characteristics and emotional register of outputs. While style covers structure, tone governs how information is conveyed and the relationship with the reader.",
        "key_points": [
            "**Voice Characteristics**: Authoritative, objective, analytical, detail-oriented",
            "**Confidence Level**: Signals how definitive vs. tentative outputs should be",
            "**Relationship**: Establishes the AI-to-reader dynamic",
            "**Emotional Register**: Professional and measured vs. casual or urgent",
        ],
    },
    "audience": {
        "explanation": "Identifies who will consume the output and what they need. This calibrates complexity, jargon usage, and level of detail based on stakeholder expertise and use case.",
        "key_points": [
            "**Stakeholder Type**: Internal team vs. executives vs. external clients",
            "**Expertise Level**: Determines whether domain terms need explanation",
            "**Information Needs**: Lists specific requirements the audience expects",
            "**Use Case Context**: How the output will be consumed shapes structure",
        ],
    },
    "analysis_framework": {
        "explanation": "Provides a structured methodology for analyzing content. This operationalizes the objectives into a repeatable process with specific steps, ensuring systematic and consistent analysis.",
        "key_points": [
            "**Systematic Process**: Breaks complex analysis into manageable steps",
            "**Consistency**: Standardized approach reduces output variability",
            "**Completeness**: Multi-step framework ensures thorough coverage",
            "**Explicit Guidance**: Removes ambiguity about 'how' to analyze",
        ],
    },
    "response_framework": {
        "explanation": "Defines the detailed requirements for extraction and output construction. This section translates high-level objectives into concrete, operational rules with specific criteria for what to include and how to format it.",
        "key_points": [
            "**Operational Rules**: Converts objectives into actionable requirements",
            "**Quality Criteria**: Specifies what makes a good vs. bad extraction",
            "**Format Specifications**: Title length, statement structure, markup usage",
            "**Decision Logic**: When to reject, when to quote vs. paraphrase",
        ],
    },
    "extraction_strategy_guidance": {
        "explanation": "Specifies the exact structure and content requirements for the extraction_strategy field. This blueprint ensures consistent, comprehensive strategy paragraphs that downstream processes can rely on.",
        "key_points": [
            "**Field Structure**: Four-part format (themes → speakers → approach → guidance)",
            "**Length Requirements**: 150-250 words balances detail with conciseness",
            "**Content Requirements**: Must include ALL themes, speakers, metrics, search terms",
            "**Downstream Use**: Guides the actual extraction phase that follows planning",
        ],
    },
    "cross_category_notes_guidance": {
        "explanation": "Defines how to prevent content duplication across categories by establishing explicit ownership boundaries. This ensures each insight appears exactly once in the final report.",
        "key_points": [
            "**Ownership Assignment**: Declares which category owns which content",
            "**Explicit Boundaries**: 'X goes here, Y goes there' format",
            "**Overlap Resolution**: Addresses predictable cross-category content",
            "**Optional Field**: Can be empty if no overlap concerns exist",
        ],
    },
    "quote_selection_strategy": {
        "explanation": "Establishes rules for when to use direct quotes vs. paraphrases. Strategic quote selection optimizes for insight density while maintaining readability. Quote the 'why' and 'what's next', paraphrase the 'what happened'.",
        "key_points": [
            "**Quote Priorities**: Drivers, strategy, outlook, risks, novel insights get quoted",
            "**Paraphrase Candidates**: Basic metrics and simple comparisons get paraphrased",
            "**Contextual Quotes**: When quoting, include 3-4 sentences with background",
            "**Readability**: Reduces verbosity while preserving high-value commentary",
        ],
    },
    "deduplication_strategy": {
        "explanation": "Establishes zero-tolerance rules for content overlap. This section provides mandatory checks and concrete examples of semantic duplication (different wording, same meaning) to ensure report quality.",
        "key_points": [
            "**Zero Tolerance**: Violations will be rejected - strict enforcement",
            "**Semantic Checking**: Different wording of same concept counts as duplicate",
            "**Concrete Examples**: Provides pairs showing what duplication looks like",
            "**Default Action**: 'When in doubt, skip it' prioritizes quality over coverage",
        ],
    },
    "quality_standards": {
        "explanation": "Lists non-negotiable quality requirements that outputs must meet. These standards act as a final checklist, addressing specific failure modes like generic statements, artificial limits, and duplication.",
        "key_points": [
            "**Comprehensiveness**: Capture all material content, no artificial limits",
            "**Specificity**: Exact figures, precise quotes, actual names required",
            "**Evidence-Rich**: Comprehensive supporting evidence for all statements",
            "**Analytical**: Synthesize insights, don't just report facts",
        ],
    },
    "response_format": {
        "explanation": "Provides final instructions before output generation. This bridges the prompt instructions and technical tool definition, reinforcing critical requirements and reminding the AI to verify completeness.",
        "key_points": [
            "**Output Mechanism**: Use provided function/tool to return structured data",
            "**Critical Reminders**: Reinforces most important rules before submission",
            "**Verification Step**: Final check against all requirements",
            "**Formatting**: Markup usage, field requirements, completeness check",
        ],
    },
}


def extract_system_prompt_content(file_path: Path) -> str:
    """
    Extract only the content within the System Prompt section.

    Args:
        file_path: Path to the markdown file

    Returns:
        Content between the ``` markers in System Prompt section
    """
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Find the System Prompt section
    system_prompt_match = re.search(
        r"## System Prompt\s*```\s*(.*?)\s*```", content, re.DOTALL
    )

    if system_prompt_match:
        return system_prompt_match.group(1)
    return ""


def parse_prompt_sections(content: str) -> List[Section]:
    """
    Parse system prompt content into COSTAR sections.

    Args:
        content: System prompt content

    Returns:
        List of Section objects
    """
    sections = []

    # Find all XML-tagged sections
    section_pattern = r"<(\w+)>(.*?)</\1>"
    matches = re.finditer(section_pattern, content, re.DOTALL)

    for match in matches:
        section_type = match.group(1).lower()
        section_content = match.group(2).strip()

        # Only process sections we have colors and explanations for
        if section_type in SECTION_COLORS and section_type in EXPLANATIONS:
            color_info = SECTION_COLORS[section_type]
            explanation_info = EXPLANATIONS[section_type]

            # Format section name for display
            display_name = section_type.replace("_", " ").title()

            section = Section(
                name=display_name,
                color=color_info["hex"],
                color_rgb=color_info["rgb"],
                icon=color_info["icon"],
                content=section_content,
                explanation=explanation_info["explanation"],
                key_points=explanation_info["key_points"],
            )

            sections.append(section)

    return sections


def highlight_content(content: str) -> str:
    """
    Apply minimal formatting to prompt content.

    Args:
        content: Raw content string

    Returns:
        HTML string with basic formatting
    """
    # Escape HTML
    content = content.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    # Highlight variable placeholders
    content = re.sub(
        r"\{(\w+)\}",
        r'<span class="variable">{\1}</span>',
        content,
    )

    # Preserve line breaks
    content = content.replace("\n", "<br>")

    return content


def markdown_to_html(text: str) -> str:
    """
    Convert basic markdown formatting to HTML.

    Args:
        text: Text with markdown formatting

    Returns:
        HTML string with formatting applied
    """
    # Bold: **text** -> <strong>text</strong>
    text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)

    # Italic: *text* -> <em>text</em>
    text = re.sub(r"\*(.+?)\*", r"<em>\1</em>", text)

    # Inline code: `code` -> <code>code</code>
    text = re.sub(r"`(.+?)`", r"<code>\1</code>", text)

    return text


def generate_html(
    sections: List[Section], main_title: str, subtitle: str, output_path: Path
) -> None:
    """
    Generate complete HTML document.

    Args:
        sections: List of Section objects
        main_title: Main title (e.g., "Call Summary ETL")
        subtitle: Subtitle (e.g., "Research Plan Prompt")
        output_path: Where to save the HTML file
    """
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{main_title} - {subtitle}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            line-height: 1.6;
            color: #1f2937;
            background: #f9fafb;
        }}

        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 2rem;
            text-align: center;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
        }}

        .header h1 {{
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 0.25rem;
        }}

        .header p {{
            font-size: 1.25rem;
            opacity: 0.85;
            font-weight: 300;
        }}

        .container {{
            max-width: 1600px;
            margin: 0 auto;
            padding: 2rem;
        }}

        .section {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 2rem;
            margin-bottom: 3rem;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        }}

        .prompt-column {{
            padding: 2rem;
            border-left: 4px solid var(--section-color);
        }}

        .explanation-column {{
            padding: 2rem;
            background: linear-gradient(to right, rgba(var(--section-color-rgb), 0.03), white 30%);
            border-left: 4px solid var(--section-color);
        }}

        .section-header {{
            display: flex;
            align-items: center;
            margin-bottom: 1.5rem;
            padding-bottom: 1rem;
            border-bottom: 2px solid #e5e7eb;
        }}

        .section-icon {{
            width: 36px;
            height: 36px;
            border-radius: 6px;
            background: var(--section-color);
            color: white;
            display: flex;
            align-items: center;
            justify-content: center;
            margin-right: 0.75rem;
            font-size: 1.25rem;
        }}

        .section-title {{
            font-size: 1.25rem;
            font-weight: 600;
            color: var(--section-color);
        }}

        .prompt-content {{
            font-family: 'Consolas', 'Monaco', monospace;
            font-size: 0.875rem;
            line-height: 1.8;
            color: #1f2937;
            white-space: pre-wrap;
            word-wrap: break-word;
        }}

        .variable {{
            color: #dc2626;
            background: #fef2f2;
            padding: 2px 6px;
            border-radius: 3px;
            font-weight: 600;
        }}

        .explanation-text {{
            font-size: 1rem;
            line-height: 1.7;
            color: #374151;
            margin-bottom: 1.5rem;
        }}

        .key-points {{
            list-style: none;
            margin: 0;
            padding: 0;
        }}

        .key-points li {{
            position: relative;
            padding-left: 1.5rem;
            margin-bottom: 0.75rem;
            color: #4b5563;
            line-height: 1.6;
        }}

        .key-points li::before {{
            content: "▸";
            position: absolute;
            left: 0;
            color: var(--section-color);
            font-weight: bold;
            font-size: 1.1rem;
        }}

        .key-points strong {{
            color: var(--section-color);
            font-weight: 600;
        }}

        .key-points code {{
            font-family: 'Consolas', monospace;
            font-size: 0.875rem;
            background: #f3f4f6;
            padding: 2px 6px;
            border-radius: 3px;
        }}

        @media (max-width: 1200px) {{
            .section {{
                grid-template-columns: 1fr;
            }}
        }}

        @media (max-width: 768px) {{
            .container {{
                padding: 1rem;
            }}

            .prompt-column,
            .explanation-column {{
                padding: 1.5rem;
            }}

            .header h1 {{
                font-size: 1.5rem;
            }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{main_title}</h1>
        <p>{subtitle}</p>
    </div>

    <div class="container">
"""

    # Generate sections
    for section in sections:
        highlighted_content = highlight_content(section.content)
        formatted_explanation = markdown_to_html(section.explanation)
        key_points_html = "\n".join(
            f"<li>{markdown_to_html(point)}</li>" for point in section.key_points
        )

        html_content += f"""
        <div class="section" style="--section-color: {section.color}; --section-color-rgb: {section.color_rgb};">
            <div class="prompt-column">
                <div class="section-header">
                    <div class="section-icon">{section.icon}</div>
                    <h2 class="section-title">{section.name}</h2>
                </div>
                <div class="prompt-content">{highlighted_content}</div>
            </div>

            <div class="explanation-column">
                <div class="section-header">
                    <div class="section-icon">💡</div>
                    <h2 class="section-title">Business Context</h2>
                </div>
                <p class="explanation-text">{formatted_explanation}</p>
                <ul class="key-points">
                    {key_points_html}
                </ul>
            </div>
        </div>
"""

    html_content += """
    </div>
</body>
</html>
"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"✓ Generated: {output_path}")


def main():
    """Main execution function."""
    base_path = Path(__file__).parent.parent
    documentation_path = (
        base_path / "src" / "aegis" / "etls" / "call_summary" / "documentation"
    )

    # Create output directory
    documentation_path.mkdir(exist_ok=True)

    # Process research plan prompt
    print("Parsing research_plan_prompt.md...")
    research_content = extract_system_prompt_content(
        documentation_path / "research_plan_prompt.md"
    )
    research_sections = parse_prompt_sections(research_content)
    generate_html(
        research_sections,
        "Call Summary ETL",
        "Research Plan Prompt",
        documentation_path / "research_plan_analysis.html",
    )

    # Process category extraction prompt
    print("Parsing category_extraction_prompt.md...")
    category_content = extract_system_prompt_content(
        documentation_path / "category_extraction_prompt.md"
    )
    category_sections = parse_prompt_sections(category_content)
    generate_html(
        category_sections,
        "Call Summary ETL",
        "Category Extraction Prompt",
        documentation_path / "category_extraction_analysis.html",
    )

    print(f"\n✓ HTML documentation generated in: {documentation_path}")


if __name__ == "__main__":
    main()
