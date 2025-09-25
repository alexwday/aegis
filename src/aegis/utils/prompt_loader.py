"""
Prompt loader utility for composing agent and subagent prompts with global contexts.
"""

from pathlib import Path
from typing import Dict, Any

import yaml


# Define the canonical order for global prompts
GLOBAL_ORDER = ["fiscal", "project", "database", "restrictions"]


def load_yaml(file_path: str) -> Dict[str, Any]:
    """
    Load and parse a YAML file.

    Args:
        file_path: Relative path to YAML file from prompts directory

    Returns:
        Parsed YAML content as dictionary
    """
    prompts_dir = Path(__file__).parent.parent / "model" / "prompts"
    full_path = prompts_dir / file_path

    if not full_path.exists():
        raise FileNotFoundError(f"Prompt file not found: {full_path}")

    with open(full_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_fiscal_prompt() -> str:
    """
    Load the dynamic fiscal prompt.

    Returns:
        Fiscal statement string
    """
    # pylint: disable=import-outside-toplevel
    import importlib.util

    fiscal_path = Path(__file__).parent.parent / "model" / "prompts" / "global" / "fiscal.py"
    spec = importlib.util.spec_from_file_location("fiscal", fiscal_path)
    fiscal_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fiscal_module)
    return fiscal_module.get_fiscal_statement()


def _load_global_prompts(uses_global: list) -> list:
    """
    Load global prompts in canonical order.

    Args:
        uses_global: List of global prompt names to use

    Returns:
        List of prompt content strings
    """
    prompt_parts = []

    if not uses_global:
        return prompt_parts

    for global_name in GLOBAL_ORDER:
        if global_name not in uses_global:
            continue

        if global_name == "fiscal":
            prompt_parts.append(_load_fiscal_prompt())
        else:
            try:
                global_data = load_yaml(f"global/{global_name}.yaml")
                if "content" in global_data:
                    prompt_parts.append(global_data["content"].strip())
            except FileNotFoundError:
                print(f"Warning: Global prompt '{global_name}' not found, skipping...")

    return prompt_parts


def load_prompt(agent_type: str, name: str) -> str:
    """
    Load and compose a complete prompt for an agent or subagent.

    Global prompts are added in fixed order: fiscal > project > database > restrictions
    Then the agent-specific content is appended.

    Args:
        agent_type: Either "agent" or "subagent"
        name: Name of the agent (e.g., "router", "benchmarking")

    Returns:
        Fully composed prompt with globals and agent content

    Example:
        >>> prompt = load_prompt("agent", "router")
        >>> prompt = load_prompt("subagent", "benchmarking")
    """
    # Validate agent_type
    if agent_type not in ["agent", "subagent"]:
        raise ValueError(f"agent_type must be 'agent' or 'subagent', got: {agent_type}")

    # Determine YAML path based on type
    if agent_type == "subagent":
        # Subagents have their prompts in individual folders
        yaml_path = f"{name}/{name}.yaml"
    else:
        # Agents are in the agents folder
        yaml_path = f"{agent_type}s/{name}.yaml"

    try:
        agent_data = load_yaml(yaml_path)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"No {agent_type} found with name: {name}") from exc

    # Build prompt parts list
    prompt_parts = []

    # Add global prompts in canonical order
    if "uses_global" in agent_data:
        prompt_parts.extend(_load_global_prompts(agent_data["uses_global"]))

    # Add agent-specific content
    if "content" in agent_data:
        prompt_parts.append(agent_data["content"].strip())
    else:
        raise ValueError(f"No content found in {name}.yaml")

    # Join all parts with clear separation
    return "\n\n---\n\n".join(prompt_parts)


def load_agent_prompt(name: str) -> str:
    """
    Convenience function to load an agent prompt.

    Args:
        name: Name of the agent

    Returns:
        Fully composed agent prompt
    """
    return load_prompt("agent", name)


def load_subagent_prompt(name: str) -> str:
    """
    Convenience function to load a subagent prompt.

    Args:
        name: Name of the subagent

    Returns:
        Fully composed subagent prompt
    """
    return load_prompt("subagent", name)


def list_available_prompts() -> Dict[str, list]:
    """
    List all available agent and subagent prompts.

    Returns:
        Dictionary with 'agents' and 'subagents' lists
    """
    prompts_dir = Path(__file__).parent.parent / "model" / "prompts"

    agents = []
    agents_dir = prompts_dir / "agents"
    if agents_dir.exists():
        agents = [f.stem for f in agents_dir.glob("*.yaml")]

    subagents = []
    subagents_dir = prompts_dir / "subagents"
    if subagents_dir.exists():
        subagents = [f.stem for f in subagents_dir.glob("*.yaml")]

    return {"agents": sorted(agents), "subagents": sorted(subagents)}


if __name__ == "__main__":  # pragma: no cover
    # Test the prompt loader
    available = list_available_prompts()
    print("Available prompts:")
    print(f"  Agents: {available['agents']}")
    print(f"  Subagents: {available['subagents']}")

    # Test loading an agent prompt if any exist
    if available["agents"]:
        TEST_AGENT = available["agents"][0]
        print(f"\nTesting load_agent_prompt('{TEST_AGENT}'):")
        print("-" * 50)
        TEST_PROMPT = load_agent_prompt(TEST_AGENT)
        print(TEST_PROMPT[:500] + "..." if len(TEST_PROMPT) > 500 else TEST_PROMPT)
