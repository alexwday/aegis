"""Tests for ETLConfig, bank info lookup, and category loading."""

import os
from unittest.mock import patch, MagicMock

import pytest
import yaml

from aegis.etls.key_themes.main import (
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
                "theme_extraction": {"tier": "medium"},
                "html_formatting": {"tier": "medium"},
                "theme_grouping": {"tier": "medium"},
            },
            "llm": {
                "temperature": 0.1,
                "max_tokens": {
                    "theme_extraction": 8192,
                    "html_formatting": 32768,
                    "theme_grouping": 16384,
                    "default": 32768,
                },
            },
            "retry": {
                "max_retries": 3,
                "base_delay": 1.0,
                "max_delay": 10.0,
            },
            "concurrency": {"max_concurrent_formatting": 10},
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))
        return str(path)

    def test_load_config(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        assert cfg.temperature == 0.1

    def test_get_model_resolves_tier(self, config_yaml):
        mock_llm = MagicMock()
        mock_llm.medium.model = "gpt-4.1-mini"
        mock_llm.large.model = "gpt-4.1"

        with patch("aegis.etls.key_themes.main.config") as mock_config:
            mock_config.llm = mock_llm
            cfg = ETLConfig(config_yaml)
            assert cfg.get_model("theme_extraction") == "gpt-4.1-mini"
            assert cfg.get_model("html_formatting") == "gpt-4.1-mini"

    def test_get_model_invalid_key(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        with pytest.raises(KeyError, match="nonexistent"):
            cfg.get_model("nonexistent")

    def test_get_max_tokens_per_task(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        assert cfg.get_max_tokens("theme_extraction") == 8192
        assert cfg.get_max_tokens("html_formatting") == 32768
        assert cfg.get_max_tokens("theme_grouping") == 16384

    def test_get_max_tokens_default_fallback(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        assert cfg.get_max_tokens("unknown_task") == 32768

    def test_get_max_tokens_legacy_int_format(self, tmp_path):
        data = {"llm": {"max_tokens": 8192}}
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))
        cfg = ETLConfig(str(path))
        assert cfg.get_max_tokens("any_task") == 8192

    def test_max_concurrent_formatting(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        assert cfg.max_concurrent_formatting == 10

    def test_max_retries(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        assert cfg.max_retries == 3

    def test_retry_base_delay(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        assert cfg.retry_base_delay == 1.0

    def test_retry_max_delay(self, config_yaml):
        cfg = ETLConfig(config_yaml)
        assert cfg.retry_max_delay == 10.0

    def test_retry_defaults_when_missing(self, tmp_path):
        data = {"llm": {"temperature": 0.1}}
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))
        cfg = ETLConfig(str(path))
        assert cfg.max_retries == 3  # default from module constant
        assert cfg.retry_base_delay == 1.0
        assert cfg.retry_max_delay == 10.0

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
        4: {
            "id": 4,
            "name": "National Bank of Canada",
            "symbol": "NA",
            "type": "Canadian_Banks",
        },
        11: {
            "id": 11,
            "name": "Citigroup Inc.",
            "symbol": "C",
            "type": "US_Banks",
        },
    }

    @pytest.fixture(autouse=True)
    def mock_institutions(self):
        """Mock _load_monitored_institutions for all tests in this class."""
        _load_monitored_institutions.cache_clear()
        with patch(
            "aegis.etls.key_themes.main._load_monitored_institutions",
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

    def test_lookup_symbol_na_not_shadowed_by_name_partial(self):
        result = get_bank_info_from_config("NA")
        assert result["bank_id"] == 4
        assert result["bank_symbol"] == "NA"

    def test_lookup_symbol_c_not_shadowed_by_name_partial(self):
        result = get_bank_info_from_config("C")
        assert result["bank_id"] == 11
        assert result["bank_symbol"] == "C"

    def test_not_found_raises(self):
        with pytest.raises(ValueError, match="not found"):
            get_bank_info_from_config("NONEXISTENT")

    def test_returns_bank_type(self):
        result = get_bank_info_from_config("RY")
        assert result["bank_type"] == "Canadian_Banks"


# ---------------------------------------------------------------------------
# _load_monitored_institutions (cache behavior)
# ---------------------------------------------------------------------------
class TestLoadMonitoredInstitutions:
    """Tests for _load_monitored_institutions() cache behavior."""

    def test_cache_returns_same_object(self):
        _load_monitored_institutions.cache_clear()
        result1 = _load_monitored_institutions()
        result2 = _load_monitored_institutions()
        assert result1 is result2

    def test_cache_clear_reloads(self):
        _load_monitored_institutions.cache_clear()
        result1 = _load_monitored_institutions()
        _load_monitored_institutions.cache_clear()
        result2 = _load_monitored_institutions()
        assert result1 is not result2
        assert result1 == result2


# ---------------------------------------------------------------------------
# load_categories_from_xlsx (integration test with real XLSX file)
# ---------------------------------------------------------------------------
class TestLoadCategoriesFromXlsx:
    """Integration tests for load_categories_from_xlsx using real XLSX file."""

    @pytest.fixture
    def xlsx_exists(self):
        """Check if the real categories XLSX file exists."""
        xlsx_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..",
            "..",
            "..",
            "..",
            "src",
            "aegis",
            "etls",
            "key_themes",
            "config",
            "categories",
            "key_themes_categories.xlsx",
        )
        return os.path.exists(xlsx_path)

    def test_categories_load(self, xlsx_exists):
        if not xlsx_exists:
            pytest.skip("Key themes categories XLSX not found")
        categories = load_categories_from_xlsx("test-id")
        assert len(categories) > 0
        assert all("category_name" in c for c in categories)
        assert all("transcript_sections" in c for c in categories)

    def test_categories_have_required_fields(self, xlsx_exists):
        if not xlsx_exists:
            pytest.skip("Key themes categories XLSX not found")
        categories = load_categories_from_xlsx("test-id")
        required_fields = [
            "transcript_sections",
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
