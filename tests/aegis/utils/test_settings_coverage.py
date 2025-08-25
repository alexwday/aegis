"""
Test to achieve 100% coverage for settings.py.
"""

from aegis.utils.settings import config


def test_config_get_method():
    """Test the get method of Config class."""
    # Test getting an existing attribute
    result = config.get("log_level")
    assert result is not None
    
    # Test getting a non-existent attribute with default
    result = config.get("non_existent_key", "default_value")
    assert result == "default_value"
    
    # Test getting a non-existent attribute without default
    result = config.get("another_non_existent_key")
    assert result is None