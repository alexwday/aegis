"""
Map current ETL prompt XML tags to CO-STAR framework.
"""

# Current tag mappings to CO-STAR
tag_mappings = {
    # CONTEXT - Background, data, constraints
    'context': 'CONTEXT',
    'current_category': 'CONTEXT',
    'categories_to_analyze': 'CONTEXT',
    'available_categories': 'CONTEXT',
    'qa_data_with_categories': 'CONTEXT',
    'previous_categories_context': 'CONTEXT',
    'previous_classifications': 'CONTEXT',
    'analysis_framework': 'CONTEXT',

    # OBJECTIVE - What to accomplish
    'objective': 'OBJECTIVE',

    # STYLE - How to write
    'style': 'STYLE',

    # TONE - Attitude/emotion
    'tone': 'TONE',

    # AUDIENCE - Who it's for
    'audience': 'AUDIENCE',

    # RESPONSE - Output format and structure
    'response_format': 'RESPONSE',
    'response_framework': 'RESPONSE',
    'formatting_structure': 'RESPONSE',
    'output_expectations': 'RESPONSE',

    # INSTRUCTIONS - Detailed guidance (CO-STAR extension)
    'research_guidance': 'INSTRUCTIONS',
    'cross_category_notes_guidance': 'INSTRUCTIONS',
    'extraction_strategy_guidance': 'INSTRUCTIONS',
    'deduplication_strategy': 'INSTRUCTIONS',
    'classification_strategy': 'INSTRUCTIONS',
    'regrouping_strategy': 'INSTRUCTIONS',
    'emphasis_strategy': 'INSTRUCTIONS',
    'content_cleanup_rules': 'INSTRUCTIONS',
    'title_creation': 'INSTRUCTIONS',
    'review_criteria': 'INSTRUCTIONS',

    # QUALITY - Standards and validation (CO-STAR extension)
    'quality_standards': 'QUALITY',
    'quality_criteria': 'QUALITY',
    'quality_checklist': 'QUALITY',
    'validation_criteria': 'QUALITY',

    # EXAMPLES - Concrete demonstrations (CO-STAR extension)
    'examples': 'EXAMPLES',
    'example_input': 'EXAMPLES',
    'example_output': 'EXAMPLES',
    'edge_cases': 'EXAMPLES',
    'final_reminder': 'EXAMPLES',
}

# Organize by prompt
prompts = {
    'Call Summary - category_extraction': [
        'audience', 'context', 'current_category', 'deduplication_strategy',
        'objective', 'previous_categories_context', 'quality_standards',
        'research_guidance', 'response_format', 'response_framework', 'style', 'tone'
    ],
    'Call Summary - research_plan': [
        'analysis_framework', 'audience', 'categories_to_analyze', 'context',
        'cross_category_notes_guidance', 'extraction_strategy_guidance',
        'objective', 'quality_standards', 'response_format', 'style', 'tone'
    ],
    'Key Themes - grouping': [
        'available_categories', 'context', 'objective', 'qa_data_with_categories',
        'quality_criteria', 'regrouping_strategy', 'response_format',
        'review_criteria', 'title_creation'
    ],
    'Key Themes - html_formatting': [
        'audience', 'content_cleanup_rules', 'context', 'edge_cases',
        'emphasis_strategy', 'example_input', 'example_output', 'examples',
        'final_reminder', 'formatting_structure', 'objective',
        'output_expectations', 'quality_checklist', 'style', 'tone'
    ],
    'Key Themes - theme_extraction': [
        'available_categories', 'classification_strategy', 'context',
        'objective', 'previous_classifications', 'response_format',
        'validation_criteria'
    ],
}

print("="*100)
print("MAPPING CURRENT XML TAGS TO CO-STAR FRAMEWORK")
print("="*100)

print("\n" + "="*100)
print("CO-STAR FRAMEWORK OVERVIEW")
print("="*100)
print("""
Standard CO-STAR:
  C - CONTEXT:      Background information, data, constraints
  O - OBJECTIVE:    Goal/task to accomplish
  S - STYLE:        Writing style to use
  T - TONE:         Attitude/emotion in response
  A - AUDIENCE:     Who the output is for
  R - RESPONSE:     Format/structure of output

Extended for ETL Prompts:
  I - INSTRUCTIONS: Detailed strategies, rules, guidance
  Q - QUALITY:      Standards, criteria, validation rules
  E - EXAMPLES:     Concrete demonstrations, edge cases
""")

print("\n" + "="*100)
print("CURRENT TAGS MAPPED TO CO-STAR CATEGORIES")
print("="*100)

# Group tags by CO-STAR category
costar_groups = {}
for tag, category in tag_mappings.items():
    if category not in costar_groups:
        costar_groups[category] = []
    costar_groups[category].append(tag)

for category in ['CONTEXT', 'OBJECTIVE', 'STYLE', 'TONE', 'AUDIENCE', 'RESPONSE', 'INSTRUCTIONS', 'QUALITY', 'EXAMPLES']:
    if category in costar_groups:
        print(f"\n{category}:")
        for tag in sorted(costar_groups[category]):
            print(f"  - {tag}")

print("\n" + "="*100)
print("PROMPT-BY-PROMPT CO-STAR STRUCTURE")
print("="*100)

for prompt_name, tags in prompts.items():
    print(f"\n{prompt_name}:")

    # Group by CO-STAR category
    prompt_costar = {}
    for tag in tags:
        category = tag_mappings.get(tag, 'UNKNOWN')
        if category not in prompt_costar:
            prompt_costar[category] = []
        prompt_costar[category].append(tag)

    # Display in CO-STAR order
    for category in ['CONTEXT', 'OBJECTIVE', 'STYLE', 'TONE', 'AUDIENCE', 'RESPONSE', 'INSTRUCTIONS', 'QUALITY', 'EXAMPLES']:
        if category in prompt_costar:
            tags_str = ', '.join(prompt_costar[category])
            print(f"  {category}: {tags_str}")

print("\n" + "="*100)
print("PROPOSED STANDARDIZED STRUCTURE")
print("="*100)
print("""
<prompt>
  <context>
    <!-- Background, current state, available data -->
    <!-- Subsections: current_task, available_data, previous_work -->
  </context>

  <objective>
    <!-- Clear goal statement -->
  </objective>

  <style>
    <!-- Writing style requirements -->
  </style>

  <tone>
    <!-- Attitude and voice -->
  </tone>

  <audience>
    <!-- Who will consume this output -->
  </audience>

  <instructions>
    <!-- Detailed strategies, rules, step-by-step guidance -->
    <!-- Subsections: strategy, rules, workflow -->
  </instructions>

  <quality>
    <!-- Standards, criteria, validation requirements -->
    <!-- Subsections: standards, validation, checklist -->
  </quality>

  <examples>
    <!-- Concrete demonstrations -->
    <!-- Subsections: example_input, example_output, edge_cases -->
  </examples>

  <response>
    <!-- Output format specification -->
    <!-- Subsections: format, structure, expectations -->
  </response>
</prompt>
""")

print("\n" + "="*100)
print("BENEFITS OF STANDARDIZATION")
print("="*100)
print("""
✓ Consistent structure across all 5 prompts
✓ Clear mental model (CO-STAR framework)
✓ Easy to understand where content belongs
✓ Preserves all existing prompt content
✓ Extensible with subsections (e.g., <context><available_data>)
✓ Industry-standard framework
✓ Better for LLM parsing (consistent tag names)
""")

print("\n" + "="*100)
print("COVERAGE ANALYSIS")
print("="*100)

for prompt_name, tags in prompts.items():
    costar_present = set()
    for tag in tags:
        category = tag_mappings.get(tag, 'UNKNOWN')
        costar_present.add(category)

    all_categories = ['CONTEXT', 'OBJECTIVE', 'STYLE', 'TONE', 'AUDIENCE', 'RESPONSE', 'INSTRUCTIONS', 'QUALITY', 'EXAMPLES']
    coverage = len(costar_present) / len(all_categories) * 100

    missing = [c for c in all_categories if c not in costar_present]

    print(f"\n{prompt_name}:")
    print(f"  Coverage: {coverage:.0f}% ({len(costar_present)}/{len(all_categories)} categories)")
    print(f"  Present: {', '.join(sorted(costar_present))}")
    if missing:
        print(f"  Missing: {', '.join(missing)}")
