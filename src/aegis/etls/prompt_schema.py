"""Shared ETL prompt-schema helpers."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import yaml


def _normalise_parameters_schema(parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Return a strict top-level object schema with a complete required list."""
    schema = dict(parameters or {})
    properties = dict(schema.get("properties") or {})
    schema["type"] = "object"
    schema["properties"] = properties
    schema["required"] = list(schema.get("required") or properties.keys())
    schema["additionalProperties"] = bool(schema.get("additionalProperties", False))
    return schema


def _normalise_tool_definition(tool: Dict[str, Any]) -> Dict[str, Any]:
    """Return one OpenAI-compatible function tool definition."""
    if tool.get("type") != "function":
        raise ValueError("Only function tools are supported by ETL prompt bundles")

    function = dict(tool.get("function") or {})
    name = str(function.get("name") or "").strip()
    if not name:
        raise ValueError("Tool definition is missing function.name")

    return {
        "type": "function",
        "function": {
            "name": name,
            "description": str(function.get("description") or "").strip(),
            "strict": True,
            "parameters": _normalise_parameters_schema(function.get("parameters") or {}),
        },
    }


def _normalise_standard_prompt(raw: Dict[str, Any], path: Path) -> Dict[str, Any]:
    """Normalize a prompt file that follows the standard schema."""
    tools = [_normalise_tool_definition(tool) for tool in raw.get("tools") or []]
    tool_choice = raw.get("tool_choice")
    if tool_choice is None and len(tools) == 1:
        tool_choice = {"type": "function", "function": {"name": tools[0]["function"]["name"]}}
    elif tool_choice is None:
        tool_choice = "required"

    return {
        "stage": str(raw.get("stage") or path.stem).strip(),
        "version": str(raw.get("version") or "1.0.0").strip(),
        "description": str(raw.get("description") or path.stem).strip(),
        "system_prompt": str(raw.get("system_prompt") or "").strip(),
        "user_prompt": str(raw.get("user_prompt") or "").strip(),
        "tool_choice": tool_choice,
        "tool_definitions": tools,
        "tool_definition": tools[0] if tools else None,
    }


def normalise_prompt_bundle(raw: Dict[str, Any], path: Path) -> Dict[str, Any]:
    """Normalize one standard-schema prompt bundle."""
    required_keys = {"stage", "version", "description", "system_prompt", "user_prompt", "tools"}
    missing = sorted(key for key in required_keys if key not in raw)
    if missing:
        raise ValueError(
            f"Prompt file {path} does not follow the standard schema; missing keys: {missing}"
        )
    return _normalise_standard_prompt(raw, path)


@lru_cache(maxsize=None)
def _load_prompt_bundle_cached(path_str: str) -> Dict[str, Any]:
    """Load and normalize one prompt bundle from disk."""
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Prompt file must deserialize to a mapping: {path}")
    return normalise_prompt_bundle(raw, path)


def load_prompt_bundle(path: Path) -> Dict[str, Any]:
    """Public prompt loader used by ETL modules."""
    return _load_prompt_bundle_cached(str(path.resolve()))
