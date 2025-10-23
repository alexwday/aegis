# YAML Refactoring Migration Tracker

**Date**: 2025-10-23
**Branch**: `feature/yaml-refactor-local`
**Purpose**: Refactor agent YAMLs to match transcripts subagent format + fix bank+year key bug

## 📋 Executive Summary

This document tracks ALL changes made during the YAML refactoring migration. Use this to:
1. Understand what changed in each file and why
2. Compare with team's code on work computer
3. Merge team's YAML content into new templates
4. Validate no functionality was lost

---

## 🎯 Changes Overview

### Bug Fixes
- [x] `clarifier.py` - Fix bank+year key bug
- [x] `planner.py` - Fix bank+year key bug (2 locations)

### Infrastructure Changes
- [x] `prompt_loader.py` - Add tool loading support (drop-in compatible)

### YAML Refactoring (complete builds, no fallbacks)
- [x] `router.yaml` - Complete refactor with conversational user template
- [x] `clarifier_banks.yaml` - Complete refactor with conversational user template
- [x] `clarifier_periods.yaml` - Complete refactor with conversational user template
- [x] `planner.yaml` - Complete refactor with conversational user template
- [x] `response.yaml` - Complete refactor with conversational user template
- [ ] `summarizer.yaml` - Pending (team modified this one)

### Agent Code Updates
- [x] `router.py` - Load tools from YAML (no fallback)
- [x] `clarifier.py` - Load tools from YAML (no fallback)
- [x] `planner.py` - Load tools from YAML (no fallback)
- [x] `response.py` - Load tools from YAML (no fallback)
- [ ] `summarizer.py` - Pending

### Architectural Changes
- [x] **Removed `no_databases_needed` tool from planner**: Router already decides if data is needed, so planner shouldn't have a "no databases" option. Cleaner architecture with single-purpose agents.

---

## 📁 File-by-File Changelog

### 1. `/src/aegis/model/agents/clarifier.py`

**Status**: ✅ ALL CHANGES COMPLETE

#### Changes Made:
```
CHANGE #1: Fix bank+year dictionary key bug ✅ COMPLETED
  LOCATION: Lines 873-889 (periods_specific function handling)
  BEFORE: Used only bank_id as dictionary key
  AFTER: Use composite key f"{bank_id}_{fiscal_year}"
  REASON: Multiple years for same bank were overwriting each other

  OLD CODE (lines 873-889):
    elif function_name == "periods_specific":
        bank_periods = function_args.get("bank_periods", [])

        # Convert to dictionary format
        periods = {}
        for bp in bank_periods:
            bank_id = str(bp["bank_id"])
            periods[bank_id] = {
                "fiscal_year": bp["fiscal_year"],
                "quarters": bp["quarters"],
            }

  NEW CODE (lines 873-889):
    elif function_name == "periods_specific":
        bank_periods = function_args.get("bank_periods", [])

        # Convert to dictionary format
        # FIX: Use composite key (bank_id + fiscal_year) to prevent
        # multiple years for same bank from overwriting each other
        periods = {}
        for bp in bank_periods:
            bank_id = str(bp["bank_id"])
            fiscal_year = bp["fiscal_year"]
            # Create composite key to support multiple years per bank
            composite_key = f"{bank_id}_{fiscal_year}"
            periods[composite_key] = {
                "bank_id": bank_id,
                "fiscal_year": fiscal_year,
                "quarters": bp["quarters"],
            }

CHANGE #2: Load tools from YAML instead of hardcoded ✅ COMPLETED
  LOCATIONS:
    - Line 18: Add import load_tools_from_yaml
    - Lines 316-346: extract_banks() - Update system prompt and user template loading
    - Lines 348-349: extract_banks() - Replace hardcoded tools with load_tools_from_yaml
    - Lines 543-634: extract_periods() - Update system prompt and user template loading
    - Lines 636-655: extract_periods() - Replace hardcoded tools with filtered load_tools_from_yaml

  BEFORE: Hardcoded tool definitions and prompt templates in Python
  AFTER: Load from clarifier_banks.yaml and clarifier_periods.yaml with NO FALLBACKS
  REASON: Consolidate prompt management in YAML files

  KEY CHANGES:

  1. Import change (line 18):
     OLD: from ...utils.prompt_loader import load_yaml, _load_fiscal_prompt
     NEW: from ...utils.prompt_loader import load_yaml, load_tools_from_yaml, _load_fiscal_prompt

  2. extract_banks() system prompt (lines 323-325):
     OLD: if "content" in clarifier_data: prompt_parts.append(clarifier_data["content"].strip())
     NEW: system_prompt_template = clarifier_data.get("system_prompt", "")
          prompt_parts.append(system_prompt_template.strip())

  3. extract_banks() user template (lines 338-346):
     OLD: Hardcoded f-strings for with/without conversation history
     NEW: Load user_prompt_template_with_history or user_prompt_template_no_history
          Use .format(query=query) for substitution

  4. extract_banks() tools (lines 348-349):
     OLD: Hardcoded 63-line tool definition array (lines 355-410)
     NEW: tools = load_tools_from_yaml("clarifier_banks", execution_id=execution_id)

  5. extract_periods() system prompt (lines 602-604):
     OLD: if "content" in clarifier_data: prompt_parts.append(clarifier_data["content"].strip())
     NEW: system_prompt_template = clarifier_data.get("system_prompt", "")
          prompt_parts.append(system_prompt_template.strip())

  6. extract_periods() user template (lines 626-634):
     OLD: Hardcoded f-strings for with/without conversation history
     NEW: Load user_prompt_template_with_history or user_prompt_template_no_history
          Use .format(query=query) for substitution

  7. extract_periods() tools (lines 636-655):
     OLD: Hardcoded 106-line tool definition array with conditional logic (lines 704-810)
     NEW: all_tools = load_tools_from_yaml("clarifier_periods", execution_id=execution_id)
          Filter tools based on bank_ids parameter:
            - If bank_ids exists: Use periods_all, periods_specific, period_clarification
            - If bank_ids is None: Use periods_valid, period_clarification
    ]

  NEW CODE:
    from aegis.utils.prompt_loader import load_tools_from_yaml
    tools = load_tools_from_yaml("clarifier_banks") or [fallback...]
```

#### Comparison Notes for Team's Code:
- ✅ Compare how team fixed the bank+year key bug (should be same approach)
- Check if team moved tools to YAML already
- Verify tool definitions match between versions

#### Git Commit for CHANGE #1:
```bash
git add src/aegis/model/agents/clarifier.py
git commit -m "Fix clarifier bank+year dictionary key bug

- Changed dictionary key from bank_id to composite key (bank_id_fiscal_year)
- Prevents multiple years for same bank from overwriting each other
- Added bank_id and fiscal_year fields to dictionary values
- Location: clarifier.py lines 873-889 (periods_specific function)
"
```

---

### 2. `/src/aegis/model/agents/planner.py`

**Status**: ✅ CHANGE #1 Complete | ⏳ CHANGE #2 Pending

#### Changes Made:
```
CHANGE #1A: Fix bank+year dictionary key bug (reading from clarifier) ✅ COMPLETED
  LOCATION: Lines 107-115 (get_filtered_availability_table function)
  BEFORE: Used only bank_id to read from periods dict
  AFTER: Use composite key f"{bank_id}_{fiscal_year}"
  REASON: Clarifier now returns composite keys; this code reads from that dict

  OLD CODE (lines 107-115):
    else:
        # Bank-specific periods
        if bank_id in periods:
            period_info = periods[bank_id]
            if (
                fiscal_year == period_info["fiscal_year"]
                and quarter in period_info["quarters"]
            ):
                period_match = True

  NEW CODE (lines 107-115):
    else:
        # Bank-specific periods
        # FIX: After clarifier fix, periods dict uses composite keys (bank_id_fiscal_year)
        # Build composite key to match clarifier's new format
        composite_key = f"{bank_id}_{fiscal_year}"
        if composite_key in periods:
            period_info = periods[composite_key]
            if quarter in period_info["quarters"]:
                period_match = True

CHANGE #1B: Fix bank+year dictionary key bug (building new dict) ✅ COMPLETED
  LOCATION: Lines 299-313 (plan_database_queries function)
  BEFORE: Used only bank_id as dictionary key when building bank_specific_periods
  AFTER: Use composite key f"{bank_id}_{fiscal_year}"
  REASON: Multiple years for same bank were overwriting each other

  OLD CODE (lines 299-313):
    # Track all unique periods
    unique_periods.add((fiscal_year, quarter))

    # Track per-bank periods
    if bank_id not in bank_specific_periods:
        bank_specific_periods[bank_id] = {"fiscal_year": fiscal_year, "quarters": []}
    if quarter not in bank_specific_periods[bank_id]["quarters"]:
        bank_specific_periods[bank_id]["quarters"].append(quarter)

  NEW CODE (lines 299-313):
    # Track all unique periods
    unique_periods.add((fiscal_year, quarter))

    # Track per-bank periods
    # FIX: Use composite key (bank_id + fiscal_year) to prevent
    # multiple years for same bank from overwriting each other
    composite_key = f"{bank_id}_{fiscal_year}"
    if composite_key not in bank_specific_periods:
        bank_specific_periods[composite_key] = {
            "bank_id": bank_id,
            "fiscal_year": fiscal_year,
            "quarters": []
        }
    if quarter not in bank_specific_periods[composite_key]["quarters"]:
        bank_specific_periods[composite_key]["quarters"].append(quarter)

CHANGE #2: Load tools from YAML instead of hardcoded (PENDING)
  LOCATION: TBD (tool definition section)
  BEFORE: Hardcoded tool definitions in Python
  AFTER: Load from planner.yaml
  REASON: Consolidate prompt management in YAML files

  OLD CODE:
    tools = [
        {
            "type": "function",
            "function": {...}
        }
    ]

  NEW CODE:
    from aegis.utils.prompt_loader import load_tools_from_yaml
    tools = load_tools_from_yaml("planner") or [fallback...]
```

#### Comparison Notes for Team's Code:
- ✅ Compare how team fixed both bank+year key bugs (should be same approach)
- Check if team moved tools to YAML already
- Verify tool definitions match between versions

#### Git Commit for CHANGE #1:
```bash
git add src/aegis/model/agents/planner.py
git commit -m "Fix planner bank+year dictionary key bugs

- Fixed bug #1A: Reading from clarifier's periods dict (lines 107-115)
  - Changed lookup from bank_id to composite key (bank_id_fiscal_year)
  - Matches clarifier's new composite key format

- Fixed bug #1B: Building bank_specific_periods dict (lines 299-313)
  - Changed dictionary key from bank_id to composite key (bank_id_fiscal_year)
  - Prevents multiple years for same bank from overwriting each other
  - Added bank_id and fiscal_year fields to dictionary values
"
```

---

### 3. `/src/aegis/utils/prompt_loader.py`

**Status**: ✅ Complete

#### Changes Made:
```
CHANGE #1: Add imports for new functionality ✅ COMPLETED
  LOCATION: Lines 1-12 (module header)
  BEFORE:
    from typing import Dict, Any
    import yaml

  AFTER:
    from typing import Dict, Any, List, Optional
    import yaml
    from .logging import get_logger
    logger = get_logger()

CHANGE #2: Add load_tools_from_yaml() function ✅ COMPLETED
  LOCATION: Lines 188-280 (new function added before main block)
  BEFORE: Did not exist
  AFTER: Loads tools section from YAML files
  REASON: Support YAML-based tool definitions
  COMPATIBILITY: Drop-in addition, no breaking changes

  NEW FUNCTION SIGNATURE:
    def load_tools_from_yaml(
        prompt_name: str,
        agent_type: str = "agent",
        execution_id: Optional[str] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Load tool definitions from a YAML file.

        Supports the new YAML format with tool_definition or tool_definitions sections.
        This is a drop-in compatible addition that doesn't break existing functionality.

        Args:
            prompt_name: Name of the prompt file (e.g., "router", "clarifier_banks")
            agent_type: Either "agent" or "subagent" (default: "agent")
            execution_id: Optional execution ID for logging

        Returns:
            List of tool definitions in OpenAI format, or None if no tools defined
        """

  KEY FEATURES:
    - Supports both tool_definition (singular) and tool_definitions (plural)
    - Works with agent and subagent YAML files
    - Returns None if no tools found (graceful fallback)
    - Comprehensive logging at debug/warning/error levels
    - Validates YAML structure before returning

CHANGE #3: Add format_tools_for_openai() helper function ✅ COMPLETED
  LOCATION: Lines 283-333 (new function added before main block)
  BEFORE: Did not exist
  AFTER: Validates and formats YAML tools for OpenAI API
  REASON: Ensure tools have correct OpenAI API structure
  COMPATIBILITY: Drop-in addition, no breaking changes

  NEW FUNCTION SIGNATURE:
    def format_tools_for_openai(
        tools: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Format tool definitions for OpenAI API.

        Ensures tools are in the correct format expected by the LLM connector.
        The YAML format should already be OpenAI-compatible, but this function
        validates and normalizes the structure.

        Args:
            tools: Raw tool definitions from YAML

        Returns:
            Formatted tools for OpenAI API
        """

  VALIDATION:
    - Checks each tool is a dictionary
    - Ensures 'type' field exists
    - Validates 'function' field for function tools
    - Skips invalid tools with warnings
    - Returns only validated tools
```

#### Comparison Notes for Team's Code:
- ✅ Check if team added similar tool loading functions
- ✅ Verify function signatures match
- ✅ Test that drop-in replacement works with their code
- ✅ Ensure logging calls work in their environment

#### Git Commit:
```bash
git add src/aegis/utils/prompt_loader.py
git commit -m "Add tool loading support to prompt_loader

- Added load_tools_from_yaml() function for loading tool definitions
  - Supports both tool_definition and tool_definitions sections
  - Works with agent and subagent YAML files
  - Returns None for graceful fallback when no tools present
  - Comprehensive debug/warning/error logging

- Added format_tools_for_openai() helper function
  - Validates tool structure (type, function fields)
  - Normalizes YAML tools to OpenAI API format
  - Skips invalid tools with warnings

- Added necessary imports (List, Optional, logger)
- Drop-in compatible - no breaking changes to existing functionality
"
```

---

### 4. `/src/aegis/model/prompts/aegis/router.yaml`

**Status**: ⏳ Pending

#### Changes Made:
```
STRUCTURE CHANGE: Refactored to match transcripts subagent format

BEFORE:
  content: |
    [Full prompt]

  tool_definition: |
    {JSON tool definition}

AFTER:
  system_prompt: |
    ╔══════════════════════════════════════════════════════════════╗
    ║  🔄 PASTE ZONE: TEAM'S ROUTER SYSTEM PROMPT                 ║
    ║                                                               ║
    ║  Replace everything in this section with the content         ║
    ║  field from the router.yaml file on your work computer.     ║
    ║                                                               ║
    ║  This is the main system message to the LLM.                ║
    ╚══════════════════════════════════════════════════════════════╝

    [Placeholder content - will be replaced with team's actual content]

  tool_definition:
    type: "function"
    function:
      name: "route"
      description: "Binary routing decision: 0=direct_response, 1=research_workflow"
      parameters:
        type: "object"
        properties:
          r:
            type: "integer"
            enum: [0, 1]
            description: "0=direct_response (use conversation history only), 1=research_workflow (needs data retrieval)"
        required: ["r"]
```

#### Merge Instructions:
1. Open team's router.yaml on work computer
2. Copy the `content:` section
3. Paste into the `system_prompt:` section, replacing placeholder
4. Keep `tool_definition:` section unchanged (extracted from current code)
5. Validate YAML syntax

---

### 5. `/src/aegis/model/prompts/aegis/clarifier_banks.yaml`

**Status**: ⏳ Pending

#### Changes Made:
```
STRUCTURE CHANGE: Refactored to match transcripts subagent format

BEFORE:
  content: |
    [Full prompt]

  (No tool_definition section - tools were hardcoded in Python)

AFTER:
  system_prompt: |
    ╔══════════════════════════════════════════════════════════════╗
    ║  🔄 PASTE ZONE: TEAM'S CLARIFIER_BANKS SYSTEM PROMPT        ║
    ║                                                               ║
    ║  Replace everything in this section with the content         ║
    ║  field from clarifier_banks.yaml on your work computer.     ║
    ║                                                               ║
    ║  This is the main system message to the LLM.                ║
    ╚══════════════════════════════════════════════════════════════╝

    [Placeholder content - will be replaced with team's actual content]

  tool_definitions:
    - type: "function"
      function:
        name: "banks_found"
        description: "Use when banks are confidently identified"
        parameters:
          type: "object"
          properties:
            bank_ids:
              type: "array"
              items:
                type: "integer"
              description: "List of bank IDs from the available banks list"
            query_intent:
              type: "string"
              description: "Comprehensive description of what the user wants"
          required: ["bank_ids", "query_intent"]

    - type: "function"
      function:
        name: "clarification_needed"
        description: "Use when banks are ambiguous, unclear, or not specified"
        parameters:
          type: "object"
          properties:
            question:
              type: "string"
              description: "Clear question for the user"
            possible_banks:
              type: "array"
              items:
                type: "integer"
              description: "Possible bank IDs if you have candidates"
          required: ["question", "possible_banks"]
```

#### Merge Instructions:
1. Open team's clarifier_banks.yaml on work computer
2. Copy the `content:` section
3. Paste into the `system_prompt:` section, replacing placeholder
4. Keep `tool_definitions:` section unchanged (extracted from current Python code)
5. Validate YAML syntax

---

### 6. `/src/aegis/model/prompts/aegis/clarifier_periods.yaml`

**Status**: ⏳ Pending

#### Changes Made:
```
STRUCTURE CHANGE: Refactored to match transcripts subagent format

BEFORE:
  content: |
    [Full prompt]

  (No tool_definition section - tools were hardcoded in Python)

AFTER:
  system_prompt: |
    ╔══════════════════════════════════════════════════════════════╗
    ║  🔄 PASTE ZONE: TEAM'S CLARIFIER_PERIODS SYSTEM PROMPT      ║
    ║                                                               ║
    ║  Replace everything in this section with the content         ║
    ║  field from clarifier_periods.yaml on your work computer.   ║
    ║                                                               ║
    ║  This is the main system message to the LLM.                ║
    ╚══════════════════════════════════════════════════════════════╝

    [Placeholder content - will be replaced with team's actual content]

  tool_definitions:
    - type: "function"
      function:
        name: "periods_all"
        description: "Same period(s) for all banks"
        parameters:
          type: "object"
          properties:
            periods:
              type: "array"
              items:
                type: "object"
                properties:
                  fiscal_year:
                    type: "integer"
                  quarter:
                    type: "string"
                    enum: ["Q1", "Q2", "Q3", "Q4", "Annual"]
              description: "Fiscal periods to retrieve"
          required: ["periods"]

    - type: "function"
      function:
        name: "periods_specific"
        description: "Different periods per bank"
        parameters:
          type: "object"
          properties:
            bank_periods:
              type: "array"
              items:
                type: "object"
                properties:
                  bank_id:
                    type: "integer"
                  fiscal_year:
                    type: "integer"
                  quarter:
                    type: "string"
              description: "Bank-specific periods"
          required: ["bank_periods"]

    - type: "function"
      function:
        name: "period_clarification"
        description: "Request clarification about periods"
        parameters:
          type: "object"
          properties:
            question:
              type: "string"
              description: "Question for the user"
          required: ["question"]

    - type: "function"
      function:
        name: "periods_valid"
        description: "Confirm periods are clear and valid"
        parameters:
          type: "object"
          properties:
            periods:
              type: "array"
              items:
                type: "object"
              description: "Validated periods"
          required: ["periods"]
```

#### Merge Instructions:
1. Open team's clarifier_periods.yaml on work computer
2. Copy the `content:` section
3. Paste into the `system_prompt:` section, replacing placeholder
4. Keep `tool_definitions:` section unchanged (extracted from current Python code)
5. Validate YAML syntax

---

### 7. `/src/aegis/model/prompts/aegis/planner.yaml`

**Status**: ⏳ Pending

#### Changes Made:
```
STRUCTURE CHANGE: Refactored to match transcripts subagent format

BEFORE:
  content: |
    [Full prompt]

  (No tool_definition section - tools were hardcoded in Python)

AFTER:
  system_prompt: |
    ╔══════════════════════════════════════════════════════════════╗
    ║  🔄 PASTE ZONE: TEAM'S PLANNER SYSTEM PROMPT                ║
    ║                                                               ║
    ║  Replace everything in this section with the content         ║
    ║  field from planner.yaml on your work computer.             ║
    ║                                                               ║
    ║  This is the main system message to the LLM.                ║
    ╚══════════════════════════════════════════════════════════════╝

    [Placeholder content - will be replaced with team's actual content]

  tool_definition:
    type: "function"
    function:
      name: "plan_database_queries"
      description: "Select appropriate databases for the query"
      parameters:
        type: "object"
        properties:
          databases:
            type: "array"
            items:
              type: "string"
              enum: ["supplementary", "reports", "rts", "transcripts", "pillar3"]
            description: "List of database IDs to query"
          reasoning:
            type: "string"
            description: "Brief explanation of database selection"
        required: ["databases", "reasoning"]
```

#### Merge Instructions:
1. Open team's planner.yaml on work computer
2. Copy the `content:` section
3. Paste into the `system_prompt:` section, replacing placeholder
4. Keep `tool_definition:` section unchanged (extracted from current Python code)
5. Validate YAML syntax

---

### 8. `/src/aegis/model/prompts/aegis/response.yaml`

**Status**: ⏳ Pending

#### Changes Made:
```
STRUCTURE CHANGE: Refactored to match transcripts subagent format

BEFORE:
  content: |
    [Full prompt]

AFTER:
  system_prompt: |
    ╔══════════════════════════════════════════════════════════════╗
    ║  🔄 PASTE ZONE: TEAM'S RESPONSE SYSTEM PROMPT               ║
    ║                                                               ║
    ║  Replace everything in this section with the content         ║
    ║  field from response.yaml on your work computer.            ║
    ║                                                               ║
    ║  This is the main system message to the LLM.                ║
    ╚══════════════════════════════════════════════════════════════╝

    [Placeholder content - will be replaced with team's actual content]

  # NOTE: Response agent does not use tools
```

#### Merge Instructions:
1. Open team's response.yaml on work computer
2. Copy the `content:` section
3. Paste into the `system_prompt:` section, replacing placeholder
4. Validate YAML syntax

---

### 9. `/src/aegis/model/prompts/aegis/summarizer.yaml`

**Status**: ⏳ Pending

#### Changes Made:
```
STRUCTURE CHANGE: Refactored to match transcripts subagent format

BEFORE:
  content: |
    [Full prompt]

  (Possibly had hardcoded synthesis instructions in Python code)

AFTER:
  system_prompt: |
    ╔══════════════════════════════════════════════════════════════╗
    ║  🔄 PASTE ZONE: TEAM'S SUMMARIZER SYSTEM PROMPT             ║
    ║                                                               ║
    ║  Replace everything in this section with the content         ║
    ║  field from summarizer.yaml on your work computer.          ║
    ║                                                               ║
    ║  ⚠️ CRITICAL: Team completely redesigned this YAML!         ║
    ║  Their version has fundamentally different synthesis         ║
    ║  instructions and approach.                                  ║
    ║                                                               ║
    ║  Paste their COMPLETE content here - don't try to merge     ║
    ║  or preserve anything from the local version.               ║
    ╚══════════════════════════════════════════════════════════════╝

    [Placeholder content - will be replaced with team's actual content]

  # NOTE: Summarizer does not use tools
  # NOTE: Team moved hardcoded synthesis instructions from Python to this YAML
```

#### Merge Instructions:
1. Open team's summarizer.yaml on work computer
2. Copy the ENTIRE `content:` section (don't preserve anything local)
3. Paste into the `system_prompt:` section, replacing placeholder
4. **CRITICAL**: Don't try to merge - use team's version completely
5. Validate YAML syntax
6. **IMPORTANT**: Test summarizer output after merge to ensure new format works

---

### 10. `/src/aegis/model/agents/router.py`

**Status**: ⏳ Pending

#### Changes Made:
```
CHANGE #1: Load tools from YAML instead of hardcoded
  LOCATION: route_query() function, tool definition section
  BEFORE: Hardcoded tool definitions in Python
  AFTER: Load from router.yaml using prompt_loader
  REASON: Consolidate prompt management in YAML files

  OLD CODE:
    tools = [
        {
            "type": "function",
            "function": {
                "name": "route_query",
                "description": "...",
                "parameters": {...}
            }
        }
    ]

  NEW CODE:
    from aegis.utils.prompt_loader import load_tools_from_yaml

    # Load tools from YAML with fallback
    tools = load_tools_from_yaml("router", execution_id=execution_id)
    if not tools:
        # Fallback to original hardcoded tools
        tools = [
            {
                "type": "function",
                "function": {
                    "name": "route_query",
                    "description": "...",
                    "parameters": {...}
                }
            }
        ]
```

#### Comparison Notes for Team's Code:
- Check if team moved tools to YAML already
- Verify tool definitions match
- Confirm fallback mechanism is acceptable

---

### 11. `/src/aegis/model/agents/response.py`

**Status**: ⏳ Pending

#### Changes Made:
```
CHANGE #1: Update to match new YAML structure (response.yaml has no tools)
  LOCATION: generate_response() function
  BEFORE: N/A (response agent doesn't use tools)
  AFTER: Still no tools, but updated YAML loading
  REASON: Consistency with other agents

  NOTE: Response agent doesn't use tools, so minimal changes
```

#### Comparison Notes for Team's Code:
- Verify no unexpected changes to response.py
- Check if team modified response agent behavior

---

### 12. `/src/aegis/model/agents/summarizer.py`

**Status**: ⏳ Pending

#### Changes Made:
```
CHANGE #1: Extract hardcoded synthesis instructions to YAML
  LOCATION: synthesize_responses() function, system prompt section
  BEFORE: Hardcoded synthesis instructions in Python
  AFTER: Load from summarizer.yaml
  REASON: Move prompt logic to YAML for easier management

  OLD CODE:
    system_prompt = prompt_content + """

    Additional hardcoded synthesis instructions:
    - Instruction 1
    - Instruction 2
    - Instruction 3
    """

  NEW CODE:
    # All instructions now in summarizer.yaml
    system_prompt = prompt_content
    # No additional hardcoded instructions

CHANGE #2: Update to match new YAML structure
  LOCATION: synthesize_responses() function
  BEFORE: Simple prompt loading
  AFTER: Load from new YAML structure with user/content sections
  REASON: Consistency with new YAML format
```

#### Comparison Notes for Team's Code:
- **CRITICAL**: Team completely redesigned summarizer.yaml
- Check if team extracted hardcoded instructions differently
- Verify synthesis logic matches between versions
- Compare output formatting expectations

---

## 🔄 Merge Workflow

### When You Have Access to Work Computer:

1. **For Each YAML File:**
   - Open the file on work computer
   - Copy the main content section
   - Open the corresponding local file
   - Find the PASTE ZONE marker
   - Replace placeholder with team's content
   - Validate YAML syntax

2. **For Each Python File:**
   - Compare side-by-side with MIGRATION_TRACKER.md
   - Check if team made same changes
   - Merge any differences
   - Test functionality

3. **Special Cases:**
   - **clarifier.py & planner.py**: Verify bank+year key fix matches
   - **summarizer.yaml**: Team's version is completely different - careful merge
   - **prompt_loader.py**: Drop-in replacement - should work as-is

---

## ✅ Validation Checklist

After merging team's content:

- [ ] All YAML files have valid syntax
- [ ] All PASTE ZONE markers removed
- [ ] All Python files have team's bug fixes
- [ ] prompt_loader.py works with new YAML structure
- [ ] All tests pass
- [ ] No hardcoded tools remain in Python files
- [ ] Summarizer outputs match expected format
- [ ] Bank+year key bug is fixed in both clarifier and planner

---

## 📊 Testing Strategy

### Unit Tests:
```bash
python -m pytest tests/aegis/model/agents/ -xvs
python -m pytest tests/aegis/utils/test_prompt_loader.py -xvs
```

### Integration Tests:
```bash
# Test full workflow with multiple years for same bank
python test_multi_year_query.py
```

### Manual Testing Scenarios:
1. Query with multiple years for same bank (tests bug fix)
2. Query with multiple banks and multiple years
3. Direct response query (tests router)
4. Research query (tests full pipeline)

---

## 📝 Git Commit History

Branch: `feature/yaml-refactor-local`

```
Commit 1: Fix clarifier bank+year key bug
  - Modified: clarifier.py
  - Changed dictionary key from bank_id to bank_id_year composite

Commit 2: Fix planner bank+year key bug
  - Modified: planner.py
  - Changed dictionary key from bank_id to bank_id_year composite

Commit 3: Add tool loading support to prompt_loader
  - Modified: prompt_loader.py
  - Added: load_tools_from_yaml()
  - Added: format_tools_for_openai()

Commit 4: Refactor router YAML and code
  - Modified: router.yaml
  - Modified: router.py
  - Extracted tools to YAML with paste zone

Commit 5: Refactor clarifier YAMLs and code
  - Modified: clarifier_banks.yaml
  - Modified: clarifier_periods.yaml
  - Modified: clarifier.py
  - Extracted tools to YAML with paste zones

Commit 6: Refactor planner YAML and code
  - Modified: planner.yaml
  - Modified: planner.py
  - Extracted tools to YAML with paste zone

Commit 7: Refactor response YAML and code
  - Modified: response.yaml
  - Modified: response.py
  - Updated YAML structure (no tools)

Commit 8: Refactor summarizer YAML and code
  - Modified: summarizer.yaml
  - Modified: summarizer.py
  - Extracted hardcoded synthesis instructions to YAML
  - Added paste zone for team's completely redesigned content
```

---

## 🚨 Critical Notes

1. **Summarizer Changes**: Team completely redesigned the summarizer.yaml. Their synthesis approach is fundamentally different. Exercise extreme caution when merging.

2. **Bug Fix**: The bank+year key bug is critical. Ensure it's fixed consistently in both clarifier.py and planner.py.

3. **Drop-in Compatibility**: prompt_loader.py changes are backward compatible. Old code will continue to work.

4. **PASTE ZONES**: Clear markers make it easy to find where to paste team's content. Don't remove the user/tools sections.

5. **Testing**: Test multi-year queries extensively to ensure bug fix works correctly.

---

## 📞 Support

If you encounter issues during merge:
1. Check this document for specific file changes
2. Compare side-by-side with team's code
3. Test each component individually
4. Validate YAML syntax after each paste

**Last Updated**: 2025-10-23
**Status**: Migration in progress
