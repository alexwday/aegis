"""Tests for CM readthrough config and loader behavior."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import yaml

from aegis.etls.cm_readthrough.main import (
    ETLConfig,
    _load_prompt_bundle,
    load_outlook_categories,
)


class TestETLConfig:
    """Tests for ETLConfig configuration loader."""

    @pytest.fixture
    def config_yaml(self, tmp_path):
        """Write a minimal config YAML file and return path."""
        data = {
            "models": {
                "outlook_extraction": {"tier": "medium"},
                "qa_extraction": {"tier": "large"},
            },
            "llm": {
                "temperature": 0.2,
                "max_tokens": {
                    "outlook_extraction": 4096,
                    "qa_extraction": 8192,
                    "default": 16384,
                },
            },
            "retry": {"max_retries": 4, "base_delay": 0.5, "max_delay": 5.0},
            "concurrency": {"max_concurrent_banks": 7, "max_concurrent_subtitle_generation": 2},
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))
        return str(path)

    def test_get_model_resolves_tier(self, config_yaml):
        mock_llm = MagicMock()
        mock_llm.medium.model = "gpt-4.1-mini"
        mock_llm.large.model = "gpt-4.1"

        with patch("aegis.etls.cm_readthrough.main.config") as mock_config:
            mock_config.llm = mock_llm
            cfg = ETLConfig(config_yaml)
            assert cfg.get_model("outlook_extraction") == "gpt-4.1-mini"
            assert cfg.get_model("qa_extraction") == "gpt-4.1"

    def test_get_max_tokens_per_task(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        assert cfg.get_max_tokens("outlook_extraction") == 4096
        assert cfg.get_max_tokens("qa_extraction") == 8192
        assert cfg.get_max_tokens("unknown_task") == 16384

    def test_get_max_tokens_legacy_int(self, tmp_path):
        data = {"llm": {"max_tokens": 2048}}
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))
        cfg = ETLConfig(str(path))
        assert cfg.get_max_tokens("anything") == 2048

    def test_retry_and_concurrency_properties(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        assert cfg.max_retries == 4
        assert cfg.retry_base_delay == 0.5
        assert cfg.retry_max_delay == 5.0
        assert cfg.max_concurrent_banks == 7
        assert cfg.max_concurrent_subtitle_generation == 2

    def test_missing_config_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            ETLConfig(str(tmp_path / "missing.yaml"))


def test_load_prompt_bundle_fetches_all_prompts():
    """Prompt bundle loader should fetch the four prompt artifacts once each."""

    def fake_loader(**kwargs):
        return {"prompt_name": kwargs["name"], "tool_definition": {}}

    with patch(
        "aegis.etls.cm_readthrough.main.load_prompt_from_db", side_effect=fake_loader
    ) as mock:
        bundle = _load_prompt_bundle("exec-123")

    assert set(bundle.keys()) == {
        "outlook_extraction",
        "qa_extraction_dynamic",
        "subtitle_generation",
        "batch_formatting",
        "qa_deduplication",
    }
    assert mock.call_count == 5


def test_load_outlook_categories_missing_required_columns_raises():
    """Missing required columns should surface as RuntimeError."""
    bad_df = pd.DataFrame({"category_name": ["A"], "category_description": ["B"]})

    with (
        patch("aegis.etls.cm_readthrough.main.os.path.exists", return_value=True),
        patch("aegis.etls.cm_readthrough.main.pd.read_excel", return_value=bad_df),
    ):
        with pytest.raises(RuntimeError, match="Failed to load outlook categories"):
            load_outlook_categories("exec-123")
