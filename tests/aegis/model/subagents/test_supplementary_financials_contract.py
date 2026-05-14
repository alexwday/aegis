"""Contract tests for the supplementary financials subagent."""

import inspect
from typing import Any, AsyncGenerator, Dict, List

import pytest

from aegis.model.subagents import SUBAGENT_MAPPING
from aegis.model.subagents.supplementary_financials.supplementary_financials import (
    QARequest,
    supplementary_financials_agent,
)


def test_supplementary_financials_agent_matches_replacement_contract() -> None:
    """The exported subagent keeps the legacy drop-in function contract."""
    signature = inspect.signature(supplementary_financials_agent)

    assert list(signature.parameters) == [
        "conversation",
        "latest_message",
        "bank_period_combinations",
        "basic_intent",
        "full_intent",
        "database_id",
        "context",
        "user_req",
    ]
    assert signature.parameters["conversation"].annotation == List[Dict[str, str]]
    assert signature.parameters["latest_message"].annotation is str
    assert signature.parameters["bank_period_combinations"].annotation == List[Dict[str, Any]]
    assert signature.parameters["basic_intent"].annotation is str
    assert signature.parameters["full_intent"].annotation is str
    assert signature.parameters["database_id"].annotation is str
    assert signature.parameters["context"].annotation == Dict[str, Any]
    assert signature.parameters["user_req"].annotation is QARequest
    assert signature.return_annotation == AsyncGenerator[Dict[str, str], None]
    assert inspect.isasyncgenfunction(supplementary_financials_agent)


@pytest.mark.asyncio
async def test_supplementary_financials_agent_yields_required_chunk_shape() -> None:
    """The direct 8-argument entry point yields the required subagent chunk format."""
    chunks = [
        chunk
        async for chunk in supplementary_financials_agent(
            conversation=[],
            latest_message="latest",
            bank_period_combinations=[],
            basic_intent="basic",
            full_intent="full",
            database_id="supplementary_financials",
            context={"execution_id": "contract-test"},
            user_req=QARequest(),
        )
    ]

    assert chunks == [
        {
            "type": "subagent",
            "name": "supplementary_financials",
            "content": "No bank/period combinations were provided.",
        }
    ]


@pytest.mark.asyncio
async def test_supplementary_financials_runtime_mapping_adapter_still_works() -> None:
    """The current Aegis 7-argument runtime call remains compatible."""
    runtime_agent = SUBAGENT_MAPPING["supplementary_financials"]
    chunks = [
        chunk
        async for chunk in runtime_agent(
            conversation=[],
            latest_message="latest",
            bank_period_combinations=[],
            basic_intent="basic",
            full_intent="full",
            database_id="supplementary_financials",
            context={"execution_id": "mapping-test"},
        )
    ]

    assert chunks == [
        {
            "type": "subagent",
            "name": "supplementary_financials",
            "content": "No bank/period combinations were provided.",
        }
    ]
