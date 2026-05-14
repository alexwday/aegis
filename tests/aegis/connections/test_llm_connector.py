"""Tests for LLM connector configuration helpers."""

from aegis.connections.llm_connector import _get_model_config
from aegis.utils.settings import config


def test_get_model_config_preserves_explicit_zero_temperature() -> None:
    """Explicit deterministic temperature overrides must not fall back to tier defaults."""
    model, temperature, max_tokens, tier = _get_model_config(
        model=None,
        temperature=0,
        max_tokens=123,
        default_tier="large",
    )

    assert model == config.llm.large.model
    assert temperature == 0
    assert max_tokens == 123
    assert tier == "large"
