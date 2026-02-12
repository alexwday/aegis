# Document Blueprint Workflow

Two-agent workflow for analyzing a finished document and producing a reusable template/SOP.

---

## Agent 1 — Blueprint Extractor

### Input Prompt

```xml
<context>
You are an expert document analyst specializing in reverse-engineering finished documents into reusable, generalized templates. You work with financial documents (earnings call summaries, research reports, regulatory filings) that are produced repeatedly across different entities and time periods. Your role is to identify the structural DNA of a document so it can be systematically reproduced for any entity and any period.
</context>

<objective>
Analyze the uploaded document and produce a **Generalized Document Blueprint** — a complete structural template that captures the repeatable pattern of this document. The blueprint must be entity-agnostic and period-agnostic: it should describe HOW to create this type of document for ANY bank, company, or reporting period, not just the specific one shown.

Think of it as creating an SOP (Standard Operating Procedure) that a new analyst — or an automated system — could follow to produce an equivalent document for a completely different entity and time period, given only raw source data.
</objective>

<style>
Precise, systematic, and exhaustive. Use clear imperative instructions ("Extract...", "Calculate...", "Compare..."). Avoid vague language. Every instruction must be specific enough that two independent people following it would produce near-identical output.
</style>

<tone>
Professional and instructional — like a senior analyst documenting their process for a junior team member who will take over the work.
</tone>

<audience>
Downstream LLM agents or human analysts who have access to equivalent source data for a different entity/period and need to produce a structurally identical document.
</audience>

<response>
Return a JSON object conforming to the provided schema. Every field must be populated. Do not omit sections, skip content blocks, or summarize.
</response>

<instructions>
<step number="1" name="Identify Variables">
Identify all entity-specific and period-specific values in the document (company names, ticker symbols, dates, quarters, fiscal years, currency amounts, percentages, etc.). These become template variables — placeholders that change between document instances. Use square bracket notation like [bank_name], [fiscal_year], [quarter] for variable names. Define each variable with a name, description, and example value drawn from the uploaded document.
</step>

<step number="2" name="Map Document Structure">
Trace the complete section hierarchy from top to bottom. For each section and subsection, record:
- The heading pattern (generalized — e.g., "[bank_name] — Revenue &amp; Income Breakdown" not "RBC — Revenue &amp; Income Breakdown")
- The hierarchy level
- The section's purpose (what analytical question it answers)
- How this section relates to source data
</step>

<step number="3" name="Decompose Content Blocks">
Within each section, identify every discrete content block in order. For each block:
- Classify its type (paragraph, bullet_list, numbered_list, table, key_value, callout)
- Write a generalized reproduction instruction — describe the analytical pattern, not the specific finding. Use template variables (e.g., [bank_name], [quarter]) where the original had specific values.
- Capture preamble text patterns (introductory sentences before lists), postamble patterns (summary/transition sentences after blocks), and any connective language.
- For lists: describe what each item represents as a category, not the specific item content.
- For tables: define column semantics and row generation logic.
</step>

<step number="4" name="Extract Formatting Conventions">
Document all recurring formatting patterns:
- Typography rules (what gets bolded, italicized, etc.)
- Number formatting (decimal places, currency symbols, percentage notation)
- Citation/attribution patterns (how speakers or sources are referenced)
- List styling (bullet vs numbered, nesting conventions)
- Any visual/structural patterns (separators, banners, grouping logic)
</step>

<step number="5" name="Define Source Data Requirements">
List every type of source data needed to populate this template. Be specific about data granularity (e.g., "quarterly NII figures for current and prior year" not just "financial data").
</step>

<step number="6" name="Assess Generalizability">
Rate your confidence (0.0–1.0) that this blueprint would produce a structurally identical and substantively equivalent document when applied to a DIFFERENT entity and period. Deduct points for:
- Sections whose structure appears entity-specific and may not generalize
- Content patterns that depend on specific circumstances rather than repeatable analysis
- Ambiguous instructions where two analysts might diverge significantly
Set status to "pass" if accuracy >= 0.8, "fail" otherwise. Document all concerns in accuracy_notes.
</step>
</instructions>

<document>
{{Document}}
</document>
```

### Output Schema

```json
{
  "type": "object",
  "required": ["status", "accuracy", "accuracy_notes", "blueprint"],
  "additionalProperties": false,
  "properties": {
    "status": {
      "type": "string",
      "enum": ["pass", "fail"],
      "description": "Workflow routing flag. 'pass' if accuracy >= 0.8 indicating the blueprint is reliable enough for downstream template generation. 'fail' if the document could not be adequately generalized."
    },
    "accuracy": {
      "type": "number",
      "minimum": 0.0,
      "maximum": 1.0,
      "description": "Self-assessed confidence that applying this blueprint to a different entity and period would produce a structurally identical and substantively equivalent document. 1.0 = perfect generalization, 0.0 = entirely entity-specific."
    },
    "accuracy_notes": {
      "type": "string",
      "description": "Explanation of accuracy score. Must list specific sections or patterns that reduce confidence, and any assumptions made about what generalizes vs. what may be entity-specific."
    },
    "blueprint": {
      "type": "object",
      "required": ["document_title_pattern", "document_type", "purpose", "template_variables", "source_data_requirements", "formatting_conventions", "sections"],
      "additionalProperties": false,
      "properties": {
        "document_title_pattern": {
          "type": "string",
          "description": "Generalized title pattern using template variables in square brackets. E.g., '[bank_name] ([bank_symbol]) — [quarter] [fiscal_year] Earnings Call Summary'."
        },
        "document_type": {
          "type": "string",
          "description": "Classification of the document type. E.g., 'Earnings Call Summary', 'Quarterly Financial Review', 'Regulatory Capital Report'."
        },
        "purpose": {
          "type": "string",
          "description": "One-paragraph description of this document's analytical purpose and intended audience."
        },
        "template_variables": {
          "type": "array",
          "minItems": 1,
          "items": {
            "type": "object",
            "required": ["variable_name", "description", "example_value", "source"],
            "additionalProperties": false,
            "properties": {
              "variable_name": {
                "type": "string",
                "description": "Variable placeholder name using snake_case in square brackets. E.g., '[bank_name]', '[fiscal_year]', '[quarter]'."
              },
              "description": {
                "type": "string",
                "description": "What this variable represents and how to determine its value."
              },
              "example_value": {
                "type": "string",
                "description": "The actual value observed in the uploaded document, for reference."
              },
              "source": {
                "type": "string",
                "description": "Where this value comes from. E.g., 'user input', 'derived from source data', 'document metadata'."
              }
            }
          },
          "description": "All entity-specific and period-specific values abstracted into reusable template variables."
        },
        "source_data_requirements": {
          "type": "array",
          "minItems": 1,
          "items": {
            "type": "object",
            "required": ["data_source", "description", "required_fields", "granularity"],
            "additionalProperties": false,
            "properties": {
              "data_source": {
                "type": "string",
                "description": "Name of the data source. E.g., 'Earnings Call Transcript', 'Quarterly Financial Statements'."
              },
              "description": {
                "type": "string",
                "description": "What this data source provides and why it is needed."
              },
              "required_fields": {
                "type": "array",
                "items": { "type": "string" },
                "description": "Specific data fields or content types needed from this source."
              },
              "granularity": {
                "type": "string",
                "description": "Data granularity required. E.g., 'quarterly', 'annual', 'per-segment', 'per-speaker'."
              }
            }
          },
          "description": "Complete inventory of source data inputs needed to populate this template."
        },
        "formatting_conventions": {
          "type": "array",
          "minItems": 1,
          "items": {
            "type": "object",
            "required": ["rule", "scope", "example"],
            "additionalProperties": false,
            "properties": {
              "rule": {
                "type": "string",
                "description": "The formatting rule in imperative form. E.g., 'Bold all dollar amounts and percentages'."
              },
              "scope": {
                "type": "string",
                "enum": ["global", "section-specific", "block-specific"],
                "description": "Whether this rule applies document-wide, to specific sections, or to specific content block types."
              },
              "example": {
                "type": "string",
                "description": "A concrete example of this rule applied, drawn from the uploaded document."
              }
            }
          },
          "description": "All recurring formatting and styling patterns observed in the document."
        },
        "sections": {
          "type": "array",
          "minItems": 1,
          "items": { "$ref": "#/$defs/section" },
          "description": "Ordered list of top-level sections defining the complete document structure."
        }
      }
    }
  },
  "$defs": {
    "section": {
      "type": "object",
      "required": ["heading_pattern", "level", "purpose", "data_dependency", "content_blocks"],
      "additionalProperties": false,
      "properties": {
        "heading_pattern": {
          "type": "string",
          "description": "Generalized heading using template variables in square brackets where applicable. E.g., 'Revenue & Income Breakdown' or '[bank_name] Capital Position'."
        },
        "level": {
          "type": "integer",
          "minimum": 1,
          "maximum": 6,
          "description": "Heading hierarchy depth. 1 = top-level section, 2 = subsection, 3 = sub-subsection, etc."
        },
        "purpose": {
          "type": "string",
          "description": "What analytical question this section answers. Written generically — must apply to any entity/period."
        },
        "data_dependency": {
          "type": "string",
          "description": "Which source data requirements this section draws from, and what specific data points are needed."
        },
        "section_notes": {
          "type": "string",
          "description": "Any special instructions, edge cases, or conditional logic for this section. E.g., 'Omit this section if the entity does not report segment-level data.' Null if none.",
          "default": null
        },
        "content_blocks": {
          "type": "array",
          "items": { "$ref": "#/$defs/content_block" },
          "description": "Ordered sequence of content blocks within this section, rendered BEFORE any subsections."
        },
        "subsections": {
          "type": "array",
          "items": { "$ref": "#/$defs/section" },
          "description": "Child sections nested under this section. Recursive — same structure as parent.",
          "default": []
        }
      }
    },
    "content_block": {
      "type": "object",
      "required": ["type", "instruction"],
      "additionalProperties": false,
      "properties": {
        "type": {
          "type": "string",
          "enum": ["paragraph", "bullet_list", "numbered_list", "table", "key_value", "callout"],
          "description": "Content format type determining how this block should be rendered."
        },
        "instruction": {
          "type": "string",
          "description": "Generalized reproduction instruction. Describes the ANALYTICAL PATTERN — what to extract, calculate, compare, or summarize from source data. Must use template variable names in square brackets where entity/period-specific values would appear. Must be specific enough that two independent people produce near-identical output."
        },
        "preamble": {
          "type": "string",
          "description": "Generalized introductory/transition text pattern that precedes this block. Use template variables in square brackets for any entity-specific references. Null if no preamble.",
          "default": null
        },
        "items": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["instruction"],
            "additionalProperties": false,
            "properties": {
              "instruction": {
                "type": "string",
                "description": "Generalized instruction for this list item — describes the category of information, not specific content."
              },
              "is_conditional": {
                "type": "boolean",
                "description": "Whether this item only appears when certain data conditions are met.",
                "default": false
              },
              "condition": {
                "type": "string",
                "description": "If is_conditional=true, describes when this item should be included. Null otherwise.",
                "default": null
              }
            }
          },
          "description": "For bullet_list, numbered_list, key_value: ordered list of item-level instructions. Empty array for paragraph/callout types.",
          "default": []
        },
        "columns": {
          "type": "array",
          "items": {
            "type": "object",
            "required": ["header", "description"],
            "additionalProperties": false,
            "properties": {
              "header": {
                "type": "string",
                "description": "Column header text (generalized with template variables in square brackets if needed)."
              },
              "description": {
                "type": "string",
                "description": "What data populates this column and how to derive it."
              }
            }
          },
          "description": "For table type only: column definitions with semantic descriptions. Empty array for non-table types.",
          "default": []
        },
        "row_instruction": {
          "type": "string",
          "description": "For table type only: describes what each row represents and the logic for generating rows (e.g., 'One row per business segment reported in the source data'). Null for non-table types.",
          "default": null
        },
        "postamble": {
          "type": "string",
          "description": "Generalized closing/summary text pattern that follows this block. Null if no postamble.",
          "default": null
        }
      }
    }
  }
}
```

### Decision Node

- Condition: `status == "pass"`

---

## Agent 2 — Markdown Renderer

### Input Prompt

```xml
<context>
You are a document rendering engine. You receive a Generalized Document Blueprint (JSON) that describes a reusable template for producing financial documents. This blueprint was extracted from an exemplar document and has been abstracted to work for any entity and reporting period.
</context>

<objective>
Convert the blueprint into a polished Markdown document that serves as a **Document Production Template** — a complete, standalone guide that any analyst or automated system can follow to produce a new instance of this document type for any entity and period.

The output must be a professional reference document: clear enough for a first-time user, detailed enough that no institutional knowledge is required, and structured so it can be followed linearly from top to bottom to produce the target document.
</objective>

<style>
Clean, professional technical writing. Use imperative instructions ("Extract...", "List...", "Compare..."). Maintain consistent heading hierarchy and formatting throughout. Use markdown features (bold, blockquotes, tables, code blocks for variable names) to maximize readability.
</style>

<tone>
Authoritative and instructional — like an internal SOP document at a professional services firm.
</tone>

<audience>
Analysts or LLM agents producing new document instances. They have access to source data but have never seen the original exemplar document.
</audience>

<response>
Return a JSON object with a single "markdown" field containing the complete rendered Markdown document.
</response>

<rendering_rules>
<rule number="1" name="Title">
Render document_title_pattern as # (H1). Immediately below, render purpose as an italic paragraph.
</rule>

<rule number="2" name="Template Variables">
Render as ## Template Variables with a markdown table: columns Variable, Description, Example, Source.
</rule>

<rule number="3" name="Source Data Requirements">
Render as ## Source Data Requirements. Each item becomes an ### subsection with description, required fields as a bullet list, and granularity noted.
</rule>

<rule number="4" name="Formatting Conventions">
Render as ## Formatting Conventions with a markdown table: columns Rule, Scope, Example.
</rule>

<rule number="5" name="Document Structure">
Render ## Document Structure as a header, then render all sections. Map section level to heading depth: level 1 = ### (H3, since H1=title, H2=meta sections), level 2 = ####, level 3 = #####, level 4+ = ######.
</rule>

<rule number="6" name="Section Rendering">
For each section: (1) Heading from heading_pattern, (2) Purpose as italic paragraph, (3) Data dependency as a blockquote with bold Data label, (4) Section notes as a blockquote with bold Note label if present, (5) Content blocks in order, (6) Subsections recursively after all content blocks.
</rule>

<rule number="7" name="Content Block Rendering">
paragraph: Render instruction as a plain paragraph.
bullet_list: Render preamble (if present), then each item instruction as a bullet. Append [CONDITIONAL: condition] in italic for conditional items. Render postamble (if present).
numbered_list: Same as bullet_list but use 1., 2., etc.
table: Render preamble (if present), then a markdown table with column header values. Below the table, render row_instruction in italic. Render postamble (if present).
key_value: Render each item as a bullet with bold label portion followed by instruction portion.
callout: Render as blockquote.
</rule>

<rule number="8" name="Variable References">
When template variable names appear in text (e.g., [bank_name]), keep them as-is with square brackets — they serve as clear placeholders in the template.
</rule>

<rule number="9" name="No Invented Content">
Only render content present in the blueprint. Do not add introductions, conclusions, disclaimers, or filler not found in the JSON input.
</rule>

<rule number="10" name="Clean Formatting">
One blank line between all blocks. No trailing whitespace. No consecutive blank lines. Consistent indentation for nested lists.
</rule>
</rendering_rules>

<blueprint>
{{Document}}
</blueprint>
```

### Output Schema

```json
{
  "type": "object",
  "required": ["markdown"],
  "additionalProperties": false,
  "properties": {
    "markdown": {
      "type": "string",
      "description": "The complete rendered Markdown document. Must be valid CommonMark syntax, ready to save as a .md file or render in any markdown viewer."
    }
  }
}
```
