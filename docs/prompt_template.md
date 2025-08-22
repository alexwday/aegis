# Generic Prompt Template - CO-STAR + XML

## Overview
A structured prompting template combining CO-STAR framework with XML tags for optimal LLM performance.

## Template

```xml
<prompt>
    <context>
        <!-- Background information and relevant details about the situation -->
    </context>
    
    <objective>
        <!-- Clear, specific goal or task to accomplish -->
    </objective>
    
    <style>
        <!-- Desired writing or response style (e.g., technical, casual, formal) -->
    </style>
    
    <tone>
        <!-- Emotional tone (e.g., professional, friendly, analytical) -->
    </tone>
    
    <audience>
        <!-- Who the response is intended for -->
    </audience>
    
    <response>
        <!-- Expected format and structure of the output -->
    </response>
    
    <constraints>
        <!-- Limitations, boundaries, or things to avoid -->
    </constraints>
    
    <examples>
        <!-- Input/output examples if helpful -->
        <example>
            <input><!-- Example input --></input>
            <output><!-- Example output --></output>
        </example>
    </examples>
    
    <chain_of_thought>
        <!-- Step-by-step reasoning process for complex tasks -->
        <step>1. First, analyze...</step>
        <step>2. Then, consider...</step>
        <step>3. Finally, conclude...</step>
    </chain_of_thought>
</prompt>
```

## Usage Notes

- **Minimum Required**: Context + Objective
- **Add elements as needed** based on task complexity
- **XML tags** provide clear structure that LLMs parse well
- **CO-STAR** ensures all key aspects are considered

## Why This Structure Works

1. **Context** grounds the LLM in the specific situation
2. **Objective** provides clear direction
3. **Style** ensures appropriate formatting
4. **Tone** sets emotional context
5. **Audience** tailors the response appropriately
6. **Response** specifies output structure

The XML tags create clear boundaries between sections, reducing ambiguity and improving response quality.