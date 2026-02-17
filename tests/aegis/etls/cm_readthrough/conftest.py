"""Shared fixtures for cm_readthrough ETL tests."""

import json

import pytest


@pytest.fixture
def sample_bank_info():
    """Bank metadata used across extraction tests."""
    return {
        "bank_id": 1,
        "bank_symbol": "RY-CA",
        "bank_name": "Royal Bank of Canada",
    }


@pytest.fixture
def sample_outlook_categories():
    """Sample outlook category payload in standard ETL format."""
    return [
        {
            "transcript_sections": "ALL",
            "category_name": "Investment Banking Pipelines",
            "category_description": "Pipeline strength and conversion outlook.",
            "example_1": "Pipelines remain robust despite volatility.",
            "example_2": "",
            "example_3": "",
        }
    ]


@pytest.fixture
def sample_qa_categories():
    """Sample Q&A categories payload in standard ETL format."""
    return [
        {
            "transcript_sections": "QA",
            "category_name": "Market Volatility",
            "category_description": "Client activity and volatility themes.",
            "example_1": "",
            "example_2": "",
            "example_3": "",
        }
    ]


@pytest.fixture
def sample_context():
    """Minimal async context for LLM/database calls."""
    return {
        "execution_id": "cm-test-exec-123",
        "ssl_config": {"verify": False},
        "auth_config": {"success": True, "method": "api_key"},
        "_llm_costs": [],
    }


@pytest.fixture
def outlook_prompt_data():
    """Prompt payload for outlook extraction tests."""
    return {
        "system_prompt": "Use these categories: {categories_list}",
        "user_prompt": (
            "Analyze {bank_name} {fiscal_year} {quarter}. Transcript: {transcript_content}"
        ),
        "tool_definition": {
            "type": "function",
            "function": {"name": "extract_capital_markets_outlook", "parameters": {}},
        },
    }


@pytest.fixture
def qa_prompt_data():
    """Prompt payload for Q&A extraction tests."""
    return {
        "system_prompt": "Use these categories: {categories_list}",
        "user_prompt": "Analyze {bank_name} {fiscal_year} {quarter}. Q&A: {qa_content}",
        "tool_definition": {
            "type": "function",
            "function": {"name": "extract_analyst_questions", "parameters": {}},
        },
    }


@pytest.fixture
def subtitle_prompt_data():
    """Prompt payload for subtitle generation tests."""
    return {
        "system_prompt": "Create subtitle.",
        "user_prompt": ("Type: {content_type} Context: {section_context} Content: {content_json}"),
        "tool_definition": {
            "type": "function",
            "function": {"name": "generate_subtitle", "parameters": {}},
        },
    }


@pytest.fixture
def formatting_prompt_data():
    """Prompt payload for batch formatting tests."""
    return {
        "system_prompt": "Format statements.",
        "user_prompt": "Input: {quotes_json}",
        "tool_definition": {
            "type": "function",
            "function": {"name": "format_quotes_with_emphasis", "parameters": {}},
        },
    }


@pytest.fixture
def valid_tool_response_outlook():
    """Valid LLM tool response for outlook extraction."""
    return {
        "choices": [
            {
                "message": {
                    "tool_calls": [
                        {
                            "function": {
                                "arguments": json.dumps(
                                    {
                                        "has_content": True,
                                        "statements": [
                                            {
                                                "category": "Investment Banking Pipelines",
                                                "statement": "Pipelines are up 10% year on year.",
                                                "relevance_score": 8,
                                                "is_new_category": False,
                                            }
                                        ],
                                    }
                                )
                            }
                        }
                    ]
                }
            }
        ],
        "metrics": {
            "prompt_tokens": 1000,
            "completion_tokens": 150,
            "total_cost": 0.02,
            "response_time": 1.1,
        },
    }
