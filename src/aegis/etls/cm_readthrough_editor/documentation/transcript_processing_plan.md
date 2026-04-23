# CM Readthrough Editor Transcript Processing Workflow

This document locks the intended per-transcript processing flow for
`cm_readthrough_editor`.

The goal is to remove ambiguity around what the unit of processing is, what is
shown in the transcript pane by default, and how extracted CM findings are
turned into the consolidated HTML editor state.

## Locked Decisions

These decisions are now fixed for v1:

- Transcript source is NAS XML, not postgres transcript chunks.
- The editor keeps the `call_summary_editor` shell and interaction model.
- The report has 2 sections only: `Outlook` and merged `Q&A`.
- The report preview is consolidated across banks.
- The transcript pane is scoped to one selected bank.
- The default transcript view shows only CM-selected blocks/conversations.
- The user can expand to the full transcript at any time.
- `Outlook` findings may come from prepared remarks and management answers in Q&A.
- `Q&A` findings are analyst questions only.
- There is no cross-bank deduplication.
- There is no browser-side subtitle regeneration.

## Processing Units

The workflow uses these units:

- `MD speaker block`
  - One continuous management-discussion turn by one speaker.
- `QA turn`
  - One analyst or management turn inside the Q&A section.
- `QA conversation`
  - One analyst question followed by its associated management answer turns.
- `Outlook finding`
  - One capital-markets-relevant forward-looking statement.
- `Q&A finding`
  - One capital-markets-relevant analyst question.

## Stage 0: Inputs And Config

For each requested bank/quarter:

1. Resolve the monitored institution from config.
2. Load the flat editor category config:
   - `outlook_categories.xlsx`
   - merged `qa_categories.xlsx`
3. Resolve thresholds used to initialize editor state.

Outputs:

- bank metadata
- category rows
- ETL thresholds

## Stage 1: Transcript Acquisition

For each bank:

1. Locate the preferred transcript XML on NAS.
2. Download the XML bytes.
3. Fail the bank cleanly if:
   - no XML exists
   - XML cannot be parsed
   - transcript contains no usable MD or QA content

Source of truth:

- [nas_source.py](/Users/alexwday/Projects/aegis/src/aegis/etls/cm_readthrough_editor/nas_source.py)

## Stage 2: XML Parse And Structural Normalization

Parse the XML into:

- transcript title
- participants
- ordered sections
- structured speaker metadata

Then normalize into two raw block streams:

- raw `MD` speaker blocks
- raw `QA` turns

Requirements:

- preserve speaker name
- preserve speaker title
- preserve speaker affiliation
- preserve original transcript order

## Stage 3: Stable Block IDs

Before any LLM extraction:

1. Assign stable IDs to each `MD` block.
2. Assign stable IDs to each raw `QA` turn.
3. After QA grouping, assign stable IDs to each `QA conversation`.

These IDs are the join key between:

- extraction output
- transcript pane navigation
- report-card click-through
- filtered transcript rendering

Minimum requirement:

- every extracted item must carry `source_block_id`

Preferred requirement:

- every extracted item also carries a source excerpt that can be aligned to one
  or more sentence IDs inside that block

## Stage 4: QA Conversation Construction

Raw QA turns are grouped into `QA conversations`.

Primary rule:

- one analyst question starts a conversation
- subsequent management answer turns stay in that conversation
- the next analyst question starts the next conversation

Implementation rule:

- use the same Q&A boundary-detection method as `call_summary_editor`
- deterministic speaker-role structure is the starting input
- if transcript structure is ambiguous, allow the same LLM boundary pass and
  validation pattern used in `call_summary_editor`

The conversation object should preserve:

- analyst question sentences
- management answer sentences
- all original turns
- participating speakers

## Stage 5: Extraction Payload Construction

After structure is normalized, build the content that will be sent to the LLM.

### Outlook payload

Outlook is extracted from:

- all `MD` speaker blocks
- management-answer portions of `QA conversations`

Reason:

- the original CM readthrough Outlook section was extracted from the full
  transcript, not prepared remarks only
- important CM outlook commentary often appears in management answers

### Q&A payload

Merged `Q&A` is extracted from:

- analyst-question portions of `QA conversations` only

Reason:

- the original Q&A sections were based on relevant analyst questions
- the editor transcript view should show the question first, with the answer as
  optional context

## Stage 6: CM-Relevance Extraction

Run two extraction passes.

### 6A. Outlook extraction

Input:

- one `MD` block or one management-answer QA source unit
- category descriptions and examples from the Outlook config

Output per finding:

- `report_section = "Outlook"`
- `category`
- optional `category_group`
- `statement`
- `relevance_score`
- `source_block_id`

### 6B. Q&A extraction

Input:

- one `QA conversation`
- category descriptions and examples from the merged Q&A config

Output per finding:

- `report_section = "Q&A"`
- `category`
- `verbatim_question`
- `analyst_name`
- `analyst_firm`
- `source_block_id`

Prompt behavior:

- extract only capital-markets-relevant content
- exclude wealth, retail, consumer, and unrelated business lines

## Stage 7: Transcript Alignment

Each extracted finding must be mapped back onto transcript content.

Alignment rules:

1. match the extracted text to the sentences inside `source_block_id`
2. assign `source_sentence_ids`
3. mark the owning block/conversation as `has_findings = true`

Result:

- report-card click can jump to the correct bank and block
- transcript highlighting is deterministic
- default filtered transcript view is reliable

## Stage 8: Initial Review-State Assignment

Initialize sentence/finding state for the editor.

### Outlook

Use `relevance_score` to initialize:

- `selected`
- `candidate`
- `rejected`

Thresholds come from ETL config.

### Q&A

In v1, extracted analyst questions start as:

- `selected`

Non-extracted Q&A stays as transcript context only.

Note:

- Outlook has an explicit model score
- merged Q&A does not need to show a score in the report preview

## Stage 9: Bank Payload Assembly

Build one bank payload for the HTML state.

Each bank payload must include:

- bank selector metadata
- transcript title
- full `MD` blocks
- full `QA conversations`
- sentence-level review state
- `has_findings` flags
- stable ordering

The payload must contain the full transcript so the user can switch from
filtered view to full transcript without another backend call.

## Stage 10: Cross-Bank Consolidation

After each bank is processed in parallel:

1. merge banks into one report state
2. preserve stable bank order
3. render the report preview as consolidated across all banks
4. keep transcript navigation bank-local

No cross-bank dedupe is performed.

If multiple banks have questions under the same theme:

- keep them all
- show them as separate bank rows under the same merged Q&A theme

## Stage 11: Section Subtitle Generation

After bank-level findings are assembled:

1. generate one subtitle for `Outlook`
2. generate one subtitle for merged `Q&A`

These are build-time LLM outputs, not browser-regenerated text.

Defaults:

- `Outlook: Capital markets activity`
- `Conference calls: Capital markets questions`

## Final Output Contract

The HTML layer expects:

- `state.meta`
  - `cm_main_title`
  - `report_title`
  - `section_subtitles`
  - period metadata
- `state.buckets[]`
  - category metadata
  - `report_section` normalized to `Outlook` or `Q&A`
- `state.banks[bank_id]`
  - bank metadata
  - `md_blocks`
  - `qa_conversations`
- `state.report_bank_order`
  - stable consolidated report ordering
- `state.current_bank`
  - transcript pane bank only

## Non-Goals

These remain out of scope for this processing flow:

- postgres transcript retrieval
- cross-bank deduplication
- browser-side subtitle regeneration
- a separate custom CM-only shell
- non-CM transcript summarization

## Immediate Implementation Deltas

The current code is close, but these are the remaining processing deltas to
fully match the locked workflow above:

1. extend `Outlook` extraction beyond `MD` blocks so it also evaluates
   management-answer QA content
2. make `source_block_id` explicit in the extraction output contract rather than
   relying only on fuzzy text matching
3. keep the deterministic QA grouping as the primary path, with optional LLM
   boundary fallback only for ambiguous transcripts
