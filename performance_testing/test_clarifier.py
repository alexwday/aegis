#!/usr/bin/env python
"""
Performance testing script for the Clarifier agent.

This script loads and tests the clarifier agent in the same way that main.py does,
using the same SSL, OAuth, and configuration setup.
"""

import sys
import uuid
from pathlib import Path

# Add parent directory to path to import aegis modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from performance_tester import (  # noqa: E402
    MetricsCollector,
    ReportGenerator,
    ScenarioLoader,
    TestResult,
    run_agent_test,
)
from src.aegis.connections.oauth_connector import setup_authentication  # noqa: E402
from src.aegis.model.agents.clarifier import clarify_query  # noqa: E402
from src.aegis.utils.database_filter import filter_databases  # noqa: E402
from src.aegis.utils.logging import setup_logging, get_logger  # noqa: E402
from src.aegis.utils.ssl import setup_ssl  # noqa: E402


def setup_clarifier_context():
    """
    Set up the clarifier context exactly as main.py does.

    Returns:
        Dictionary containing execution_id, auth_config, ssl_config,
        and available_databases
    """
    # Initialize logging
    setup_logging()
    logger = get_logger()

    # Generate execution ID
    execution_id = str(uuid.uuid4())
    logger.info("test_clarifier.context_setup.started", execution_id=execution_id)

    # Setup SSL configuration
    ssl_config = setup_ssl()
    logger.info(
        "test_clarifier.ssl_setup.completed",
        execution_id=execution_id,
        status=ssl_config.get("status", "Unknown"),
    )

    # Setup authentication (requires execution_id and ssl_config)
    auth_config = setup_authentication(execution_id, ssl_config)
    logger.info(
        "test_clarifier.auth_setup.completed",
        execution_id=execution_id,
        auth_method=auth_config.get("auth_method", "Unknown"),
    )

    # Get filtered databases (no filter applied for testing)
    filtered_databases = filter_databases(None)

    logger.info(
        "test_clarifier.database_setup.completed",
        execution_id=execution_id,
        database_count=len(filtered_databases),
    )

    # Build context matching main.py
    context = {
        "execution_id": execution_id,
        "auth_config": auth_config,
        "ssl_config": ssl_config,
        "available_databases": list(filtered_databases.keys()),
    }

    logger.info("test_clarifier.context_setup.completed", execution_id=execution_id)

    return context


def test_clarifier_with_scenario(scenario, context, model_tier=None):
    """
    Test the clarifier with a single scenario.
    
    Note: The clarifier agent has a different signature than router,
    it only takes the query (not conversation history), so we need a wrapper.

    Args:
        scenario: Scenario object to test
        context: Clarifier context from setup_clarifier_context()
        model_tier: Optional model tier (not used by clarifier, but kept for consistency)

    Returns:
        TestResult object with the test outcome
    """
    logger = get_logger()
    logger.info(
        "test_clarifier.scenario.started",
        execution_id=context["execution_id"],
        scenario_id=scenario.id,
        scenario_name=scenario.name,
    )

    # The clarifier expects just the query, not conversation history
    # Extract the latest message from the scenario
    messages = scenario.get_messages()
    query = messages[-1]["content"] if messages else ""
    
    # Create a wrapper function that matches the expected signature
    def clarifier_wrapper(conversation_history, latest_message, context):
        # The clarifier only uses the latest_message as the query
        return clarify_query(
            query=latest_message,
            context=context,
            available_databases=context.get("available_databases"),
        )
    
    # Use the common test runner with our wrapper function
    result = run_agent_test(clarifier_wrapper, scenario, context, model_tier)

    logger.info(
        "test_clarifier.scenario.completed",
        execution_id=context["execution_id"],
        scenario_id=scenario.id,
        success=result.success,
        latency_ms=result.latency_ms,
        tokens_used=result.tokens_used,
        cost=result.cost,
    )

    return result


def main():
    """
    Main entry point for clarifier performance testing.

    Loads scenarios from clarifier_scenarios.yaml and runs tests with small,
    medium, and large models, then generates comprehensive comparison reports.
    """
    # Initialize logging
    setup_logging()
    logger = get_logger()

    print("\n" + "=" * 60)
    print("CLARIFIER AGENT MULTI-MODEL PERFORMANCE TESTING")
    print("=" * 60 + "\n")

    # Load scenarios
    scenario_file = Path(__file__).parent / "scenarios" / "clarifier_scenarios.yaml"

    try:
        print(f"Loading scenarios from: {scenario_file}")
        metadata, scenarios = ScenarioLoader.load_scenarios(str(scenario_file))
        print(f"✓ Loaded {len(scenarios)} test scenarios")
        print(f"  Test Suite: {metadata['name']}")
        print(f"  Version: {metadata['version']}")
        print()
    except Exception as e:
        print(f"✗ Failed to load scenarios: {e}")
        sys.exit(1)

    # Setup clarifier context once (same for all tests)
    print("Setting up clarifier context (SSL, Auth, Databases)...")
    try:
        context = setup_clarifier_context()
        print("✓ Clarifier context initialized")
        print(f"  Execution ID: {context['execution_id']}")
        print(f"  Auth Method: {context['auth_config'].get('auth_method', 'Unknown')}")
        print(f"  Available Databases: {len(context['available_databases'])}")
        print()
    except Exception as e:
        print(f"✗ Failed to setup clarifier context: {e}")
        sys.exit(1)

    # Test with each model tier
    model_tiers = ["small", "medium", "large"]
    all_results = {}
    all_collectors = {}

    for model_tier in model_tiers:
        print(f"\n{'='*60}")
        print(f"TESTING WITH {model_tier.upper()} MODEL")
        print(f"{'='*60}\n")

        # Initialize metrics collector for this model
        collector = MetricsCollector()
        collector.start_test_suite()

        # Run all scenarios with this model
        print(f"Running {len(scenarios)} scenarios with {model_tier} model...")
        print("-" * 40)

        for i, scenario in enumerate(scenarios, 1):
            print(f"[{i}/{len(scenarios)}] {scenario.name}")

            try:
                # Test the scenario with this model
                result = test_clarifier_with_scenario(scenario, context, model_tier)
                collector.add_result(result)

                # Print result with details
                if result.success:
                    print(
                        f"    ✓ PASS (latency: {result.latency_ms/1000:.2f}s, "
                        f"tokens: {result.tokens_used:,})"
                    )
                    # Show what was validated
                    if isinstance(result.expected_output, dict):
                        if 'bank_ids' in result.expected_output:
                            print(f"      Banks: Expected {result.expected_output.get('bank_ids')} ✓")
                        if 'period_year' in result.expected_output:
                            print(f"      Year: Expected {result.expected_output.get('period_year')} ✓")
                        if 'period_quarters' in result.expected_output:
                            print(f"      Quarters: Expected {result.expected_output.get('period_quarters')} ✓")
                else:
                    print(f"    ✗ FAIL: {result.error}")
                    # Show what was expected vs actual
                    if isinstance(result.expected_output, dict) and isinstance(result.actual_output, dict):
                        if 'bank_ids' in result.expected_output:
                            actual_banks = None
                            if result.actual_output.get('banks') and result.actual_output['banks'].get('bank_ids'):
                                actual_banks = result.actual_output['banks']['bank_ids']
                            print(f"      Banks: Expected {result.expected_output.get('bank_ids')}, Got {actual_banks}")
                        if 'period_year' in result.expected_output or 'period_quarters' in result.expected_output:
                            actual_year = None
                            actual_quarters = None
                            if result.actual_output.get('periods') and result.actual_output['periods'].get('periods'):
                                periods_data = result.actual_output['periods']['periods']
                                # Check for apply_all structure
                                if 'apply_all' in periods_data:
                                    actual_year = periods_data['apply_all'].get('fiscal_year')
                                    actual_quarters = periods_data['apply_all'].get('quarters')
                                # Check for bank-specific structure
                                elif periods_data:
                                    # Get first bank's period data
                                    first_bank_id = list(periods_data.keys())[0]
                                    if isinstance(periods_data[first_bank_id], dict):
                                        actual_year = periods_data[first_bank_id].get('fiscal_year')
                                        actual_quarters = periods_data[first_bank_id].get('quarters')
                            if 'period_year' in result.expected_output:
                                print(f"      Year: Expected {result.expected_output.get('period_year')}, Got {actual_year}")
                            if 'period_quarters' in result.expected_output:
                                print(f"      Quarters: Expected {result.expected_output.get('period_quarters')}, Got {actual_quarters}")

            except Exception as e:
                # Create failed result for unexpected errors
                result = TestResult(
                    scenario_id=scenario.id,
                    scenario_name=scenario.name,
                    success=False,
                    actual_output=None,
                    expected_output=scenario.expected,
                    latency_ms=0,
                    error=f"Unexpected error: {str(e)}",
                    model_used=model_tier,
                )
                collector.add_result(result)
                print(f"    ✗ ERROR: {e}")

        # End test suite for this model
        collector.end_test_suite()
        all_collectors[model_tier] = collector
        all_results[model_tier] = collector.results

        # Generate summary for this model
        summary = collector.get_summary()

        print("-" * 40)
        print(f"\n{model_tier.upper()} MODEL SUMMARY")
        print(f"  Success Rate: {summary['success_rate']:.1f}%")
        print(f"  Mean Latency: {summary['latency']['mean']/1000:.2f}s")
        print(f"  Total Tokens: {summary['tokens']['total']:,}")
        print(f"  Total Cost: ${summary['cost']['total']:.4f}")

    # Generate comparison summary
    print(f"\n{'='*60}")
    print("MODEL COMPARISON SUMMARY")
    print(f"{'='*60}\n")

    comparison_data = []
    for model_tier in model_tiers:
        summary = all_collectors[model_tier].get_summary()
        comparison_data.append(
            [
                model_tier.upper(),
                f"{summary['success_rate']:.1f}%",
                f"{summary['latency']['mean']/1000:.2f}s",
                f"{summary['tokens']['total']:,}",
                f"${summary['cost']['total']:.4f}",
            ]
        )

    # Print comparison table
    headers = ["Model", "Success", "Avg Latency", "Tokens", "Cost"]
    col_widths = [
        max(len(str(row[i]) if i < len(row) else "") for row in [headers] + comparison_data) + 2
        for i in range(len(headers))
    ]

    # Print header
    header_line = "|".join(h.center(w) for h, w in zip(headers, col_widths))
    print(header_line)
    print("-" * len(header_line))

    # Print data rows
    for row in comparison_data:
        print("|".join(str(val).center(w) for val, w in zip(row, col_widths)))

    # Generate comprehensive reports
    print("\nGenerating comprehensive reports...")

    # Combine all results for unified report
    combined_results = []
    for model_tier in model_tiers:
        combined_results.extend(all_results[model_tier])

    # Create combined collector for overall statistics
    combined_collector = MetricsCollector()
    combined_collector.results = combined_results
    combined_summary = combined_collector.get_summary()

    # Add model comparison to metadata
    metadata["model_comparison"] = {
        model_tier: all_collectors[model_tier].get_summary() for model_tier in model_tiers
    }

    # Use absolute path for results directory
    import os
    results_dir = os.path.join(os.path.dirname(__file__), "results")
    generator = ReportGenerator("clarifier_multi_model", results_dir=results_dir)

    try:
        report_paths = generator.generate_all_reports(
            metadata=metadata, results=combined_results, summary=combined_summary
        )

        print("✓ Reports generated successfully:")
        for format_type, path in report_paths.items():
            print(f"  {format_type.upper()}: {path}")

    except Exception as e:
        print(f"✗ Failed to generate reports: {e}")
        logger.error("test_clarifier.report_generation.failed", error=str(e))

    # Overall result
    print("\n" + "=" * 60)
    # Check if all models achieved acceptable success rate
    all_success_rates = [
        all_collectors[tier].get_summary()["success_rate"] for tier in model_tiers
    ]
    if all(rate >= 80 for rate in all_success_rates):
        print("✓ MULTI-MODEL TESTING COMPLETED SUCCESSFULLY")
    else:
        print("✗ MULTI-MODEL TESTING COMPLETED WITH FAILURES")
    print("=" * 60 + "\n")

    # Exit with appropriate code
    sys.exit(0 if all(rate >= 80 for rate in all_success_rates) else 1)


if __name__ == "__main__":
    main()