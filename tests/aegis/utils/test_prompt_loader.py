"""
Tests for the prompt loader utility.
"""

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

from aegis.utils.prompt_loader import (
    load_yaml,
    load_prompt,
    load_agent_prompt,
    load_subagent_prompt,
    list_available_prompts,
    _load_fiscal_prompt,
    _load_global_prompts,
)


class TestLoadYaml:
    """Test YAML loading functionality."""

    def test_load_nonexistent_yaml(self):
        """Test loading a non-existent YAML file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_yaml("nonexistent/file.yaml")


class TestLoadFiscalPrompt:
    """Test fiscal prompt loading."""

    @patch("importlib.util")
    def test_load_fiscal_prompt(self, mock_importlib_util):
        """Test loading the fiscal prompt dynamically."""
        # Mock the module loading
        mock_spec = MagicMock()
        mock_module = MagicMock()
        mock_module.get_fiscal_statement.return_value = "Fiscal statement content"
        
        mock_importlib_util.spec_from_file_location.return_value = mock_spec
        mock_importlib_util.module_from_spec.return_value = mock_module
        
        result = _load_fiscal_prompt()
        
        assert result == "Fiscal statement content"
        mock_module.get_fiscal_statement.assert_called_once()


class TestLoadGlobalPrompts:
    """Test global prompt loading."""

    @patch("aegis.utils.prompt_loader.load_yaml")
    @patch("aegis.utils.prompt_loader._load_fiscal_prompt")
    def test_load_global_prompts_with_fiscal(self, mock_fiscal, mock_load_yaml):
        """Test loading global prompts including fiscal."""
        mock_fiscal.return_value = "Fiscal content"
        mock_load_yaml.side_effect = [
            {"content": "Project content"},
            {"content": "Restrictions content"},
        ]
        
        result = _load_global_prompts(["fiscal", "project", "restrictions"])
        
        assert len(result) == 3
        assert "Fiscal content" in result
        assert "Project content" in result
        assert "Restrictions content" in result

    @patch("aegis.utils.prompt_loader.load_yaml")
    def test_load_global_prompts_without_fiscal(self, mock_load_yaml):
        """Test loading global prompts without fiscal."""
        mock_load_yaml.side_effect = [
            {"content": "Project content"},
            {"content": "Database content"},
        ]
        
        result = _load_global_prompts(["project", "database"])
        
        assert len(result) == 2
        assert "Project content" in result
        assert "Database content" in result

    def test_load_global_prompts_empty_list(self):
        """Test loading with empty global list."""
        result = _load_global_prompts([])
        assert result == []

    def test_load_global_prompts_none(self):
        """Test loading with None."""
        result = _load_global_prompts(None)
        assert result == []


class TestLoadPrompt:
    """Test prompt loading functionality."""

    @patch("aegis.utils.prompt_loader.load_yaml")
    @patch("aegis.utils.prompt_loader._load_global_prompts")
    def test_load_agent_prompt_with_globals(self, mock_load_global, mock_load_yaml):
        """Test loading an agent prompt with global dependencies."""
        mock_load_yaml.return_value = {
            "name": "test_agent",
            "uses_global": ["project", "restrictions"],
            "content": "Agent specific content",
        }
        mock_load_global.return_value = ["Project content", "Restrictions content"]

        result = load_prompt("agent", "test_agent")

        assert "Project content" in result
        assert "Restrictions content" in result
        assert "Agent specific content" in result
        assert "---" in result  # Check for separator

    @patch("aegis.utils.prompt_loader.load_yaml")
    def test_load_agent_prompt_without_globals(self, mock_load_yaml):
        """Test loading an agent prompt without global dependencies."""
        mock_load_yaml.return_value = {
            "name": "test_agent",
            "content": "Agent only content",
        }

        result = load_prompt("agent", "test_agent")

        assert result == "Agent only content"
        assert "---" not in result  # No separator when no globals

    def test_invalid_agent_type(self):
        """Test that invalid agent type raises ValueError."""
        with pytest.raises(ValueError, match="agent_type must be"):
            load_prompt("invalid", "test")

    @patch("aegis.utils.prompt_loader.load_yaml")
    def test_missing_content_in_yaml(self, mock_load_yaml):
        """Test that missing content field raises ValueError."""
        mock_load_yaml.return_value = {"name": "test_agent"}  # No content field

        with pytest.raises(ValueError, match="No content found"):
            load_prompt("agent", "test_agent")

    @patch("aegis.utils.prompt_loader.load_yaml")
    def test_nonexistent_agent(self, mock_load_yaml):
        """Test that non-existent agent raises FileNotFoundError."""
        mock_load_yaml.side_effect = FileNotFoundError("Not found")

        with pytest.raises(FileNotFoundError, match="No agent found"):
            load_prompt("agent", "nonexistent")


class TestConvenienceFunctions:
    """Test convenience functions."""

    @patch("aegis.utils.prompt_loader.load_prompt")
    def test_load_agent_prompt(self, mock_load_prompt):
        """Test load_agent_prompt calls load_prompt correctly."""
        mock_load_prompt.return_value = "test content"

        result = load_agent_prompt("router")

        mock_load_prompt.assert_called_once_with("agent", "router")
        assert result == "test content"

    @patch("aegis.utils.prompt_loader.load_prompt")
    def test_load_subagent_prompt(self, mock_load_prompt):
        """Test load_subagent_prompt calls load_prompt correctly."""
        mock_load_prompt.return_value = "test content"

        result = load_subagent_prompt("benchmarking")

        mock_load_prompt.assert_called_once_with("subagent", "benchmarking")
        assert result == "test content"


class TestListAvailablePrompts:
    """Test listing available prompts."""

    @patch("aegis.utils.prompt_loader.Path")
    def test_list_available_prompts(self, mock_path_class):
        """Test listing available agent and subagent prompts."""
        # Create mock path instances
        mock_prompts_dir = MagicMock()
        mock_agents_dir = MagicMock()
        mock_subagents_dir = MagicMock()

        # Setup the __file__ path
        mock_file_path = MagicMock()
        mock_path_class.return_value = mock_file_path
        mock_file_path.parent.parent = MagicMock()
        
        # Create the chain properly
        mock_model = MagicMock()
        mock_file_path.parent.parent.__truediv__.return_value = mock_model
        mock_model.__truediv__.return_value = mock_prompts_dir
        
        # Setup prompts_dir / "agents" and prompts_dir / "subagents"
        mock_prompts_dir.__truediv__.side_effect = [mock_agents_dir, mock_subagents_dir]

        # Setup agents directory
        mock_agents_dir.exists.return_value = True
        mock_agent_files = [
            MagicMock(stem="router"),
            MagicMock(stem="planner"),
            MagicMock(stem="clarifier"),
        ]
        mock_agents_dir.glob.return_value = mock_agent_files

        # Setup subagents directory
        mock_subagents_dir.exists.return_value = True
        mock_subagent_files = [
            MagicMock(stem="benchmarking"),
            MagicMock(stem="reports"),
        ]
        mock_subagents_dir.glob.return_value = mock_subagent_files

        result = list_available_prompts()

        assert result["agents"] == ["clarifier", "planner", "router"]  # Sorted
        assert result["subagents"] == ["benchmarking", "reports"]  # Sorted

    @patch("aegis.utils.prompt_loader.Path")
    def test_list_available_prompts_empty(self, mock_path_class):
        """Test listing prompts when directories don't exist."""
        mock_prompts_dir = MagicMock()
        mock_agents_dir = MagicMock()
        mock_subagents_dir = MagicMock()

        # Setup the path hierarchy
        mock_file_path = MagicMock()
        mock_path_class.return_value = mock_file_path
        mock_file_path.parent.parent = MagicMock()
        
        mock_model = MagicMock()
        mock_file_path.parent.parent.__truediv__.return_value = mock_model
        mock_model.__truediv__.return_value = mock_prompts_dir
        
        mock_prompts_dir.__truediv__.side_effect = [mock_agents_dir, mock_subagents_dir]

        # Directories don't exist
        mock_agents_dir.exists.return_value = False
        mock_subagents_dir.exists.return_value = False

        result = list_available_prompts()

        assert result["agents"] == []
        assert result["subagents"] == []