# CM Readthrough ETL Prompts - Copy-Paste Guide

**Instructions**: Use the Prompt Editor at http://localhost:5001
Click 'Create New Prompt' and copy-paste each field below.

**IMPORTANT**: These prompts have BOTH System Prompt AND User Prompt (unlike previous ETLs)

================================================================================


## PROMPT 1: OUTLOOK_EXTRACTION

================================================================================

### Model
```
aegis
```

### Layer
```
cm_readthrough_etl
```

### Name
```
outlook_extraction
```

### Version
```
1.0
```

### Description
```
Extracts capital markets outlook statements from earnings call transcripts by category
```

### Comments
```
Purpose: Extract outlook statements from MD section of earnings calls for capital markets readthrough reports | Last Updated: 2024
```

### System Prompt
```
(Copy from scripts/cm_readthrough_prompts_for_db.json - outlook_extraction system_prompt field)
```

**Tip**: Open `scripts/cm_readthrough_prompts_for_db.json`, find the first object, copy the `system_prompt` value

### User Prompt
```
<task>
Analyze the earnings call transcript for {bank_name} for {fiscal_year} {quarter} and extract capital markets outlook statements.
ONLY use the categories provided in the allowed_categories list. Extract the TOP 2-3 most important statements per category.
</task>

<transcript>
{transcript_content}
</transcript>

<instructions>
Use the provided tool to extract and structure the outlook statements.
- ONLY use categories from the allowed_categories list above
- Extract TOP 2-3 statements per category maximum
- Extract VERBATIM quotes - do NOT paraphrase or summarize
- Do NOT include quotation marks in the extracted text
- Set is_new_category to false for ALL statements
- Skip statements that don't fit the provided categories
</instructions>
```

### Tool Definition
```json
{
  "type": "function",
  "function": {
    "name": "extract_capital_markets_outlook",
    "description": "Extract top 2-3 outlook statements per provided category only, or indicate no relevant content exists",
    "parameters": {
      "type": "object",
      "properties": {
        "has_content": {
          "type": "boolean",
          "description": "True if transcript contains substantive capital markets outlook, False otherwise"
        },
        "statements": {
          "type": "array",
          "description": "Array of relevant outlook statements organized by category (empty array if has_content is false). TOP 2-3 per category maximum. ONLY use provided categories.",
          "items": {
            "type": "object",
            "properties": {
              "category": {
                "type": "string",
                "description": "Category name - MUST be an exact match from the allowed categories list (case-sensitive)"
              },
              "statement": {
                "type": "string",
                "description": "EXACT VERBATIM quote from the transcript - no paraphrasing, no quotation marks, just the raw text as spoken"
              },
              "is_new_category": {
                "type": "boolean",
                "description": "MUST always be false - new categories are not allowed"
              }
            },
            "required": ["category", "statement", "is_new_category"]
          }
        }
      },
      "required": ["has_content", "statements"]
    }
  }
}
```

### Uses Global (Array)
```
Leave empty (no global contexts for ETL)
```


================================================================================


## PROMPT 2: QA_EXTRACTION_DYNAMIC

================================================================================

### Model
```
aegis
```

### Layer
```
cm_readthrough_etl
```

### Name
```
qa_extraction_dynamic
```

### Version
```
1.0
```

### Description
```
Extracts analyst questions from Q&A sections by category for capital markets analysis
```

### Comments
```
Purpose: Extract verbatim analyst questions from Q&A section of earnings calls for capital markets readthrough reports | Last Updated: 2024
```

### System Prompt
```
(Copy from scripts/cm_readthrough_prompts_for_db.json - qa_extraction_dynamic system_prompt field)
```

### User Prompt
```
<task>
Analyze the Q&A section from {bank_name}'s earnings call for {fiscal_year} {quarter} and extract analyst questions related to capital markets.
ONLY use the categories provided in the allowed_categories list. Extract the TOP 2-3 most relevant questions per category.
</task>

<qa_section>
{qa_content}
</qa_section>

<instructions>
Use the provided tool to extract and structure the analyst questions.
- Extract just the QUESTION SENTENCE(S) - not the full paragraph or exchange
- Include 1 leading/trailing sentence ONLY IF needed for context
- Do NOT include quotation marks around the question text
- ONLY use categories from the allowed_categories list above
- Extract TOP 2-3 questions per category maximum
- Set is_new_category to false for ALL questions
- Skip questions that don't fit the provided categories
</instructions>
```

### Tool Definition
```json
{
  "type": "function",
  "function": {
    "name": "extract_analyst_questions",
    "description": "Extract top 2-3 analyst questions per provided category only, or indicate no relevant content exists",
    "parameters": {
      "type": "object",
      "properties": {
        "has_content": {
          "type": "boolean",
          "description": "True if Q&A section contains relevant capital markets questions, False otherwise"
        },
        "questions": {
          "type": "array",
          "description": "Array of relevant analyst questions organized by category (empty array if has_content is false). TOP 2-3 per category maximum. ONLY use provided categories.",
          "items": {
            "type": "object",
            "properties": {
              "category": {
                "type": "string",
                "description": "Category name - MUST be an exact match from the allowed categories list (case-sensitive)"
              },
              "verbatim_question": {
                "type": "string",
                "description": "Core question sentence(s) with minimal context if needed - NOT the full paragraph. No quotation marks."
              },
              "analyst_name": {
                "type": "string",
                "description": "Full name of the analyst asking the question"
              },
              "analyst_firm": {
                "type": "string",
                "description": "Firm or institution the analyst represents"
              },
              "is_new_category": {
                "type": "boolean",
                "description": "MUST always be false - new categories are not allowed"
              }
            },
            "required": ["category", "verbatim_question", "analyst_name", "analyst_firm", "is_new_category"]
          }
        }
      },
      "required": ["has_content", "questions"]
    }
  }
}
```

### Uses Global (Array)
```
Leave empty (no global contexts for ETL)
```


================================================================================


## PROMPT 3: SUBTITLE_GENERATION

================================================================================

### Model
```
aegis
```

### Layer
```
cm_readthrough_etl
```

### Name
```
subtitle_generation
```

### Version
```
1.0
```

### Description
```
Creates concise subtitles that capture overall themes from multiple banks
```

### Comments
```
Purpose: Generate 8-15 word subtitles that synthesize themes across banks for section headers | Last Updated: 2024
```

### System Prompt
```
(Copy from scripts/cm_readthrough_prompts_for_db.json - subtitle_generation system_prompt field)
```

### User Prompt
```
<task>
Analyze the following content from multiple banks and generate a concise subtitle that captures the overall theme.

Content type: {content_type}
Section context: {section_context}
</task>

<content>
{content_json}
</content>

<instructions>
IMPORTANT: You MUST use the generate_subtitle tool to return your response. Do not provide the subtitle as text.

Requirements:
- Use the generate_subtitle tool with the "subtitle" parameter
- If content_type is "outlook", the subtitle should start with "Outlook:" and focus on forward-looking themes
- If content_type is "questions", the subtitle should start with "Conference calls:" and focus on what analysts are asking about
- The subtitle must be professional, concise (8-15 words), and capture the essence of the content

Call the tool now with your generated subtitle.
</instructions>
```

### Tool Definition
```json
{
  "type": "function",
  "function": {
    "name": "generate_subtitle",
    "description": "Generate a concise subtitle that captures the overall theme from content across multiple banks",
    "parameters": {
      "type": "object",
      "properties": {
        "subtitle": {
          "type": "string",
          "description": "The generated subtitle (8-15 words) capturing the dominant theme"
        }
      },
      "required": ["subtitle"]
    }
  }
}
```

### Uses Global (Array)
```
Leave empty (no global contexts for ETL)
```


================================================================================


## PROMPT 4: BATCH_FORMATTING

================================================================================

### Model
```
aegis
```

### Layer
```
cm_readthrough_etl
```

### Name
```
batch_formatting
```

### Version
```
1.0
```

### Description
```
Formats capital markets outlook statements with HTML emphasis tags for key phrases
```

### Comments
```
Purpose: Add HTML <strong><u> tags to emphasize important phrases in outlook statements | Last Updated: 2024
```

### System Prompt
```
(Copy from scripts/cm_readthrough_prompts_for_db.json - batch_formatting system_prompt field)
```

### User Prompt
```
<task>
Format all capital markets outlook statements by adding HTML emphasis to the most important phrases in each statement.
Ensure consistent formatting across all banks and statements.
</task>

<statements_to_format>
{quotes_json}
</statements_to_format>

<instructions>
Use the provided tool to return all statements with HTML tags applied consistently.
Return the exact same structure, but with "formatted_statement" (as "formatted_quote" field) added to each statement object.
Maintain the original "statement" field and add the formatted version.
</instructions>
```

### Tool Definition
```json
{
  "type": "function",
  "function": {
    "name": "format_quotes_with_emphasis",
    "description": "Format all capital markets outlook statements with HTML emphasis tags in a single batch",
    "parameters": {
      "type": "object",
      "properties": {
        "formatted_quotes": {
          "type": "object",
          "description": "Dictionary keyed by bank name, containing arrays of statements with formatted_statement field added. Note - use \"formatted_quote\" for backward compatibility with code.",
          "additionalProperties": {
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "category": {
                  "type": "string",
                  "description": "Category of the statement (unchanged from input)"
                },
                "statement": {
                  "type": "string",
                  "description": "Original statement text (unchanged from input)"
                },
                "formatted_statement": {
                  "type": "string",
                  "description": "Statement text with HTML <strong><u> tags applied to emphasize key phrases (return as \"formatted_quote\" field for compatibility)"
                },
                "is_new_category": {
                  "type": "boolean",
                  "description": "Whether this is a new category (unchanged from input)"
                }
              }
            }
          }
        }
      },
      "required": ["formatted_quotes"]
    }
  }
}
```

### Uses Global (Array)
```
Leave empty (no global contexts for ETL)
```


================================================================================
