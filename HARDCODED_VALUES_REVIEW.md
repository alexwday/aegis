# Aegis Hardcoded Values Review

## Overview
This document lists all hardcoded values, defaults, and fallback configurations found in the Aegis agents and subagents that may need review or configuration.

---

## Core Agents (`src/aegis/model/agents/`)

### 1. Router Agent (`router.py`)

#### Hardcoded Values:
- **Temperature**: `0.1` (Line 119)
  - Purpose: Very low temperature for deterministic binary routing
  - Config equivalent: `config.llm.medium.temperature = 0.5`
  - **Status**: ⚠️ Overrides config value

- **Conversation History Limit**: `[-10:]` (Line 71)
  - Purpose: Last 10 messages for routing decision
  - No config equivalent
  - **Status**: ⚠️ Hardcoded

#### Default Model Selection:
- **Default Tier**: Medium (`config.llm.medium.model`)
- **Fallback Logic**: If no `model_tier_override` specified, uses medium
- **Status**: ✅ Configurable via context override

---

### 2. Clarifier Agent (`clarifier.py`)

#### Hardcoded Values:
- **Temperature**: `0.1` (Lines 386, 691)
  - Purpose: Low temperature for consistent extraction
  - Config equivalent: `config.llm.large.temperature = 0.7`
  - **Status**: ⚠️ Overrides config value

- **Conversation History Processing**: `messages[:-1]` (Lines 343, 629)
  - Purpose: All messages except latest (which is the query)
  - No config equivalent
  - **Status**: ✅ Appropriate logic

#### Default Model Selection:
- **Default Tier**: Large (`config.llm.large.model`)
- **Fallback Logic**: Defaults to large for better reasoning
- **Status**: ✅ Configurable via context override

---

### 3. Planner Agent (`planner.py`)

#### Hardcoded Values:
- **Temperature**: `0.1` (Line 410)
  - Purpose: Low temperature for consistent rule following
  - Config equivalent: `config.llm.medium.temperature = 0.5`
  - **Status**: ⚠️ Overrides config value

- **Conversation History Limit**: `[-5:]` (Line 360)
  - Purpose: Last 5 messages for context
  - No config equivalent
  - **Status**: ⚠️ Hardcoded

#### Default Model Selection:
- **Default Tier**: Medium (`config.llm.medium.model`)
- **Fallback Logic**: Defaults to medium for planning efficiency
- **Status**: ✅ Configurable via context override

---

### 4. Response Agent (`response.py`)

#### Hardcoded Values:
- **Temperature**: `0.7` (Line 126)
  - Purpose: Balanced creativity for natural responses
  - Config equivalent: `config.llm.large.temperature = 0.7`
  - **Status**: ✅ Matches config value

- **Conversation History Limit**: `[-10:]` (Line 97)
  - Purpose: Last 10 messages for response generation
  - No config equivalent
  - **Status**: ⚠️ Hardcoded

#### Default Model Selection:
- **Default Tier**: Large (`config.llm.large.model`)
- **Fallback Logic**: Defaults to large for high-quality responses
- **Status**: ✅ Configurable via context override

---

### 5. Summarizer Agent (`summarizer.py`)

#### Hardcoded Values:
- **Temperature**: `0.3` (Line 123)
  - Purpose: Lower temperature for consistency
  - Config equivalent: `config.llm.large.temperature = 0.7`
  - **Status**: ⚠️ Overrides config value

- **Conversation History Limit**: `[-5:]` (Line 108)
  - Purpose: Last 5 messages for context
  - No config equivalent
  - **Status**: ⚠️ Hardcoded

#### Fixed Model Selection:
- **Fixed Tier**: Large (`config.llm.large.model`)
- **Fallback Logic**: Always uses large (no override support)
- **Status**: ⚠️ No context override capability

---

## Subagents (`src/aegis/model/subagents/`)

### 1. Supplementary Subagent (`supplementary/main.py`)

#### Hardcoded Values:
- **Temperature**: `0.7` (Line 181)
  - Config equivalent: `config.llm.medium.temperature = 0.5`
  - **Status**: ⚠️ Overrides config value

- **Max Tokens**: `500` (Line 182)
  - Config equivalent: `config.llm.medium.max_tokens = 2000`
  - **Status**: ⚠️ Overrides config value

#### Fixed Model Selection:
- **Fixed Tier**: Medium (`model_tier = "medium"` on Line 165)
- **Status**: ⚠️ Hardcoded tier selection

---

### 2. Pillar3 Subagent (`pillar3/main.py`)

#### Hardcoded Values:
- **Temperature**: `0.7` (Line 181)
  - Config equivalent: `config.llm.medium.temperature = 0.5`
  - **Status**: ⚠️ Overrides config value

- **Max Tokens**: `500` (Line 182)
  - Config equivalent: `config.llm.medium.max_tokens = 2000`
  - **Status**: ⚠️ Overrides config value

#### Fixed Model Selection:
- **Fixed Tier**: Medium (`model_tier = "medium"` on Line 165)
- **Status**: ⚠️ Hardcoded tier selection

---

### 3. RTS Subagent (`rts/main.py`)

#### Hardcoded Values:
- **Temperature**: `0.7` (Line 181)
  - Config equivalent: `config.llm.medium.temperature = 0.5`
  - **Status**: ⚠️ Overrides config value

- **Max Tokens**: `500` (Line 182)
  - Config equivalent: `config.llm.medium.max_tokens = 2000`
  - **Status**: ⚠️ Overrides config value

#### Fixed Model Selection:
- **Fixed Tier**: Medium (`model_tier = "medium"` on Line 165)
- **Status**: ⚠️ Hardcoded tier selection

---

### 4. Reports Subagent (`reports/main.py`)

#### Hardcoded Values:
- **Temperature**: `0.3` (Line 337)
  - Config equivalent: `config.llm.small.temperature = 0.3`
  - **Status**: ✅ Matches config value

#### Fixed Model Selection:
- **Fixed Tier**: Small (`config.llm.small.model` on Line 336)
- **Status**: ⚠️ Hardcoded tier selection (for report selection only)

---

### 5. Transcripts Subagent (`transcripts/main.py` & `transcripts/formatting.py`)

#### Main Module:
- **Fixed Tier**: Medium (Line 164)
- **Status**: ✅ Uses config defaults (no hardcoded temperature/max_tokens)

#### Formatting Module:
- **Fixed Tier**: Medium (Lines 210, 677)
- **Status**: ✅ Uses config defaults (no hardcoded temperature/max_tokens)

---

## Summary of Issues

### Critical Issues (Should Fix):
1. **Subagents max_tokens**: 3 subagents (supplementary, pillar3, rts) use `max_tokens: 500` instead of config value (2000)
2. **Summarizer context override**: Doesn't support `model_tier_override` like other agents

### Moderate Issues (Consider Configuring):
3. **Temperature overrides**: Multiple agents override config temperatures for specific use cases
4. **Conversation history limits**: Hardcoded at various values (-5, -10) without centralized config

### Minor Issues (Acceptable but Document):
5. **Default model tier selection**: Each agent has a sensible default but could be configurable
6. **Subagent tier selection**: All hardcoded to "medium" without override capability

---

## Recommendations

### High Priority:
1. ✅ **COMPLETED**: Update subagents (supplementary, pillar3, rts) to use `model_config.max_tokens`
2. **Add context override support to Summarizer**: Allow `model_tier_override` in context
3. **Update subagents to use `model_config.temperature`**: Replace hardcoded 0.7 with config values

### Medium Priority:
4. **Add conversation history config**: New env vars:
   - `AGENT_CONVERSATION_HISTORY_ROUTER=10`
   - `AGENT_CONVERSATION_HISTORY_RESPONSE=10`
   - `AGENT_CONVERSATION_HISTORY_PLANNER=5`
   - `AGENT_CONVERSATION_HISTORY_SUMMARIZER=5`

5. **Add per-agent temperature overrides**: New env vars:
   - `AGENT_TEMPERATURE_ROUTER=0.1`
   - `AGENT_TEMPERATURE_CLARIFIER=0.1`
   - `AGENT_TEMPERATURE_PLANNER=0.1`
   - `AGENT_TEMPERATURE_SUMMARIZER=0.3`
   - `SUBAGENT_TEMPERATURE=0.7`

### Low Priority:
6. **Add subagent tier configuration**: Allow override via context or env var
7. **Document intentional overrides**: Add comments explaining why certain values differ from config defaults

---

## Configuration Gaps

### Missing Environment Variables:
- Agent-specific temperatures
- Agent-specific conversation history limits
- Subagent model tier selection
- Per-agent model tier defaults

### Current Behavior:
- Agents use sensible hardcoded defaults
- Some respect `model_tier_override` in context
- Config provides base values that are sometimes overridden
- No centralized way to adjust agent-specific behaviors

---

## Notes

### Why Temperatures Differ:
- **Router (0.1)**: Needs deterministic binary decisions
- **Clarifier (0.1)**: Needs consistent entity extraction
- **Planner (0.1)**: Needs consistent rule application
- **Response (0.7)**: Needs natural conversational flow
- **Summarizer (0.3)**: Balances consistency with readability
- **Subagents (0.7)**: Needs realistic but varied responses

These overrides are **intentional** but could be made **configurable** for flexibility.
