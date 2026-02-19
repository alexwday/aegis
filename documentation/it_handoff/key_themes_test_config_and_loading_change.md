# `/Users/alexwday/Projects/aegis/tests/aegis/etls/key_themes/test_config_and_loading.py`

## Old Code
```python
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

    def test_not_found_raises(self):
        with pytest.raises(ValueError, match="not found"):
            get_bank_info_from_config("NONEXISTENT")

    def test_returns_bank_type(self):
        result = get_bank_info_from_config("RY")
        assert result["bank_type"] == "Canadian_Banks"
```

## New Code
```python
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
```
