# Call Summary Editor Category Description Format

This document explains the new `category_description` format for the
`call_summary_editor` ETL and how an agent should migrate an existing config
from long prompt-style descriptions into the new structure.

The goal is to make the config easier for business users to maintain without
losing classification quality.

## What Changed

`category_description` stays as a single spreadsheet cell.

It is no longer expected to be a long natural-language prompt. Instead, it
should be a multiline, sectioned field list inside that one cell.

Preferred headings:

```text
Topics:
- ...

Keywords:
- ...

Instructions:
- ...
```

Optional extra headings are allowed when useful, for example:

```text
Notes:
- ...

Overrides:
- ...
```

The ETL now treats these sections as structured guidance rather than plain
prompt prose.

## Meaning Of Each Section

`Topics`
- The main semantic scope of the category.
- Broad concepts, subtopics, business areas, recurring themes.
- Use these to describe what the category is fundamentally about.

`Keywords`
- Strong phrases, terms, metrics, acronyms, or common wording that should
  clearly map into the category.
- This is a hint field, not an exhaustive checklist.
- The business does not need to list every possible keyword.

`Instructions`
- Inclusion rules, exclusion rules, tie-breaks, or boundary guidance.
- Use this for statements like:
  - include X here
  - leave Y in another category
  - use this only when the main point is Z

Optional headings such as `Notes` or `Overrides`
- Supplemental guidance only.
- Not a separate category and not a replacement for `Topics`, `Keywords`, or
  `Instructions`.

## Intended Classification Behavior

The ETL should not require exact keyword matches.

The config is intended to work like this:

- `Topics` define the category's semantic scope.
- `Keywords` provide strong examples and hints of what definitely belongs.
- Similar wording, adjacent subtopics, and semantically related findings
  should still be included even if the exact listed keyword does not appear.
- `Instructions` define boundaries when content overlaps multiple categories.

In other words:

- keywords are suggestive, not exhaustive
- semantic matches should still classify correctly
- the business can add new keywords or subtopics over time without having to
  rewrite prompt prose

## Required Migration Rule

Migration from the old format must be additive.

Do not remove existing meaning just because the old description is long or
written in prompt language.

When converting an old row:

- preserve all current scope that is intentionally part of the category
- preserve important business terms, metrics, products, and subtopics
- preserve existing inclusion and exclusion logic
- convert prose into sectioned lists
- only clarify or reorganize; do not narrow the category unless the user
  explicitly asks for that

If an old description contains something useful but it does not fit cleanly
into `Topics`, `Keywords`, or `Instructions`, keep it under `Notes` rather
than dropping it.

## Target Format Template

Use this as the default structure for each `category_description` cell:

```text
Topics:
- topic or subtopic
- topic or subtopic

Keywords:
- keyword, acronym, metric, or phrase
- keyword, acronym, metric, or phrase

Instructions:
- short inclusion, exclusion, or tie-break rule
- short inclusion, exclusion, or tie-break rule
```

Optional extension:

```text
Notes:
- extra context that should be preserved

Overrides:
- special case handling
```

## Good Example

```text
Topics:
- deposit growth
- deposit mix
- funding costs
- customer deposit behavior

Keywords:
- deposit beta
- migration
- mix shift
- funding spread
- NII

Instructions:
- Use when the main point is deposit economics, funding mix, or deposit pricing.
- Leave pure expense commentary in Expense Management & Efficiency.
- If the discussion is mainly loan growth rather than funding, leave it in Loan Portfolio & Growth.
```

## Bad Example

Do not write a new prompt paragraph like this:

```text
This category should capture any commentary related to deposits, funding,
deposit betas, customer migration, and funding cost dynamics. Include similar
comments where relevant and be careful not to overlap too much with loans...
```

That is exactly the style this migration is trying to replace.

## Migration Procedure For An Agent

For each existing row:

1. Read the current `category_name`, `category_description`, and example
   columns.
2. Extract the old description's core meaning.
3. Move broad subject matter into `Topics`.
4. Move concrete phrases, acronyms, metrics, and strong lexical cues into
   `Keywords`.
5. Move overlap rules, exclusions, and prioritization logic into
   `Instructions`.
6. Preserve anything useful that does not fit cleanly under an optional
   heading such as `Notes` or `Overrides`.
7. Keep the result in one multiline `category_description` cell.
8. Do not rewrite the row into prose.
9. Do not remove existing scope unless the user explicitly requests it.

## Mapping Guidance

Old prompt text often contains a mix of concepts. Use this mapping:

Narrative content like:
- "overall results"
- "business segment performance"
- "Canadian mortgage portfolio"

Usually belongs in:
- `Topics`

Concrete terms like:
- "CET1"
- "deposit beta"
- "PCL"
- "IB fees"
- "NII"

Usually belongs in:
- `Keywords`

Boundary language like:
- "leave pure expense discussion elsewhere"
- "use only when the main point is..."
- "if discussed in a strategic context, keep it here"

Usually belongs in:
- `Instructions`

## When To Use Optional Sections

Use optional sections only when they preserve useful meaning that would
otherwise be lost.

Examples:

- `Notes` for context the business wants retained but that is not a direct
  scope term or rule
- `Overrides` for special-case instructions

Do not create many custom headings unless there is a real reason. Prefer
keeping most content in `Topics`, `Keywords`, and `Instructions`.

## Suggested Acceptance Criteria

A migrated row is acceptable if:

- it remains a single `category_description` cell
- it uses section headings instead of prose
- it preserves the row's prior meaning
- it is additive, not narrower
- it is easy for a business user to edit
- the keywords are treated as hints rather than a complete list
- the instructions clearly describe category boundaries

## Agent Brief For A New Session

Use the brief below when asking an agent to update an existing config:

```text
Update the call summary editor category config to the new category_description format.

Important rules:
- Keep category_description as a single multiline spreadsheet cell.
- Convert old long prompt-style descriptions into sectioned lists.
- Use these headings by default: Topics, Keywords, Instructions.
- Optional headings like Notes or Overrides are allowed only when needed.
- This migration must be additive: preserve all current intended scope and do not remove or narrow existing concepts.
- Do not rewrite rows as narrative prompt prose.
- Topics = semantic scope.
- Keywords = non-exhaustive hint fields for strong phrases, metrics, acronyms, and wording that clearly belong.
- Instructions = inclusion, exclusion, and tie-break rules.
- Similar or semantically related findings should still match even if the exact listed keyword is absent.

For each row, preserve meaning, convert it into the new sectioned format, and keep the result business-editable.
```

## Notes

This document is specifically about the `call_summary_editor` ETL category
sheet format.

It is not a general prompt-writing guide. The point is to reduce prompt
language in the config and move toward editable business-friendly fields while
keeping semantic classification behavior.
