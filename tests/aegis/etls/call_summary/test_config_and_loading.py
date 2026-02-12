"""Tests for ETLConfig, bank info lookup, and category loading."""

import os
from unittest.mock import patch, MagicMock

import pytest
import yaml

from aegis.etls.call_summary.main import (
    ETLConfig,
    get_bank_info_from_config,
    load_categories_from_xlsx,
    _load_monitored_institutions,
)


# ---------------------------------------------------------------------------
# ETLConfig
# ---------------------------------------------------------------------------
class TestETLConfig:
    """Tests for the ETLConfig configuration loader."""

    @pytest.fixture
    def config_yaml(self, tmp_path):
        """Write a minimal config.yaml and return its path."""
        data = {
            "models": {
                "research_plan": {"tier": "medium"},
                "category_extraction": {"tier": "large"},
            },
            "llm": {
                "temperature": 0.2,
                "max_tokens": {
                    "research_plan": 4096,
                    "category_extraction": 16384,
                    "default": 32768,
                },
            },
            "concurrency": {"max_concurrent_extractions": 3},
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))
        return str(path)

    def test_load_config(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        assert cfg.temperature == 0.2

    def test_get_model_resolves_tier(self, config_yaml):
        mock_llm = MagicMock()
        mock_llm.medium.model = "gpt-4.1-mini"
        mock_llm.large.model = "gpt-4.1"

        with patch("aegis.etls.call_summary.main.config") as mock_config:
            mock_config.llm = mock_llm
            cfg = ETLConfig(config_yaml)
            assert cfg.get_model("research_plan") == "gpt-4.1-mini"
            assert cfg.get_model("category_extraction") == "gpt-4.1"

    def test_get_model_invalid_key(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        with pytest.raises(KeyError, match="nonexistent"):
            cfg.get_model("nonexistent")

    def test_get_max_tokens_per_task(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        assert cfg.get_max_tokens("research_plan") == 4096
        assert cfg.get_max_tokens("category_extraction") == 16384

    def test_get_max_tokens_default_fallback(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        assert cfg.get_max_tokens("unknown_task") == 32768

    def test_get_max_tokens_legacy_int_format(self, tmp_path):
        data = {"llm": {"max_tokens": 8192}}
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))
        cfg = ETLConfig(str(path))
        assert cfg.get_max_tokens("any_task") == 8192

    def test_max_concurrent_extractions(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        assert cfg.max_concurrent_extractions == 3

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            ETLConfig(str(tmp_path / "nonexistent.yaml"))


# ---------------------------------------------------------------------------
# get_bank_info_from_config
# ---------------------------------------------------------------------------
class TestGetBankInfoFromConfig:
    """Tests for get_bank_info_from_config() with mocked institution data."""

    MOCK_INSTITUTIONS = {
        1: {
            "id": 1,
            "name": "Royal Bank of Canada",
            "symbol": "RY",
            "type": "Canadian_Banks",
        },
        2: {
            "id": 2,
            "name": "Toronto-Dominion Bank",
            "symbol": "TD",
            "type": "Canadian_Banks",
        },
        3: {
            "id": 3,
            "name": "JPMorgan Chase & Co.",
            "symbol": "JPM",
            "type": "US_Banks",
        },
    }

    @pytest.fixture(autouse=True)
    def mock_institutions(self):
        """Mock _load_monitored_institutions for all tests in this class."""
        _load_monitored_institutions.cache_clear()
        with patch(
            "aegis.etls.call_summary.main._load_monitored_institutions",
            return_value=self.MOCK_INSTITUTIONS,
        ):
            yield
        _load_monitored_institutions.cache_clear()

    def test_lookup_by_id(self):
        result = get_bank_info_from_config("1")
        assert result["bank_symbol"] == "RY"
        assert result["bank_name"] == "Royal Bank of Canada"

    def test_lookup_by_symbol(self):
        result = get_bank_info_from_config("TD")
        assert result["bank_id"] == 2

    def test_lookup_by_symbol_case_insensitive(self):
        result = get_bank_info_from_config("jpm")
        assert result["bank_symbol"] == "JPM"

    def test_lookup_by_partial_name(self):
        result = get_bank_info_from_config("Royal Bank")
        assert result["bank_id"] == 1

    def test_not_found_raises(self):
        with pytest.raises(ValueError, match="not found"):
            get_bank_info_from_config("NONEXISTENT")


# ---------------------------------------------------------------------------
# load_categories_from_xlsx (integration test with real XLSX files)
# ---------------------------------------------------------------------------
class TestLoadCategoriesFromXlsx:
    """Integration tests for load_categories_from_xlsx using real XLSX files."""

    @pytest.fixture
    def xlsx_dir(self):
        """Path to the real categories XLSX directory."""
        return os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            "..",
            "..",
            "..",
            "src",
            "aegis",
            "etls",
            "call_summary",
            "config",
            "categories",
        )

    def _xlsx_exists(self, xlsx_dir, filename):
        return os.path.exists(os.path.join(xlsx_dir, filename))

    def test_canadian_banks_loads(self, xlsx_dir):
        if not self._xlsx_exists(xlsx_dir, "canadian_banks_categories.xlsx"):
            pytest.skip("Canadian banks XLSX not found")
        categories = load_categories_from_xlsx("Canadian_Banks", "test-id")
        assert len(categories) > 0
        assert all("category_name" in c for c in categories)
        assert all("transcript_sections" in c for c in categories)

    def test_us_banks_loads(self, xlsx_dir):
        if not self._xlsx_exists(xlsx_dir, "us_banks_categories.xlsx"):
            pytest.skip("US banks XLSX not found")
        categories = load_categories_from_xlsx("US_Banks", "test-id")
        assert len(categories) > 0
        assert all("category_name" in c for c in categories)

    def test_categories_have_required_fields(self, xlsx_dir):
        if not self._xlsx_exists(xlsx_dir, "canadian_banks_categories.xlsx"):
            pytest.skip("Canadian banks XLSX not found")
        categories = load_categories_from_xlsx("Canadian_Banks", "test-id")
        required_fields = [
            "transcript_sections",
            "report_section",
            "category_name",
            "category_description",
            "example_1",
            "example_2",
            "example_3",
        ]
        for cat in categories:
            for field in required_fields:
                assert (
                    field in cat
                ), f"Missing field '{field}' in category {cat.get('category_name')}"
