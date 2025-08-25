"""
Performance testing utilities for Aegis agent components.

This module provides shared functionality for loading test scenarios,
collecting performance metrics, and generating reports.
"""

import json
import time
import yaml
import statistics
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
import csv

# PDF generation imports
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.piecharts import Pie
from reportlab.graphics.shapes import String


@dataclass
class Scenario:
    """
    Represents a single test scenario.

    Attributes:
        id: Unique identifier for the scenario
        name: Human-readable name
        description: Detailed description of the test
        message: Single message to test (mutually exclusive with conversation)
        conversation: Full conversation history (mutually exclusive with message)
        expected: Expected outcomes (route, confidence, etc.)
        reasoning: Explanation of why this expectation is correct
        tags: Optional tags for categorization
        thresholds: Optional performance thresholds override
    """

    id: str
    name: str
    description: str
    expected: Dict[str, Any]
    reasoning: str
    message: Optional[str] = None
    conversation: Optional[List[Dict[str, str]]] = None
    tags: List[str] = field(default_factory=list)
    thresholds: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        """Validate that either message or conversation is provided, not both."""
        if not self.message and not self.conversation:
            raise ValueError(f"Scenario {self.id} must have either 'message' or 'conversation'")
        if self.message and self.conversation:
            raise ValueError(f"Scenario {self.id} cannot have both 'message' and 'conversation'")

    def get_messages(self) -> List[Dict[str, str]]:
        """
        Get the messages for this scenario in the format expected by agents.

        Returns:
            List of message dictionaries with 'role' and 'content' keys
        """
        if self.message:
            return [{"role": "user", "content": self.message}]
        return self.conversation


@dataclass
class TestResult:
    """
    Represents the result of running a single test scenario.

    Attributes:
        scenario_id: ID of the scenario that was tested
        scenario_name: Name of the scenario
        success: Whether the test passed
        actual_output: The actual output from the agent
        expected_output: The expected output
        latency_ms: Response time in milliseconds
        tokens_used: Number of tokens consumed
        cost: Estimated cost in dollars
        confidence: Confidence score from the agent (if applicable)
        error: Error message if the test failed
        model_used: The LLM model used for this test
        prompt_version: Version of the prompt used for this test
        prompt_last_updated: Last updated date of the prompt
        test_query: The actual query/message sent to the agent
        timestamp: When the test was run
    """

    scenario_id: str
    scenario_name: str
    success: bool
    actual_output: Any
    expected_output: Any
    latency_ms: float
    tokens_used: int = 0
    cost: float = 0.0
    confidence: Optional[float] = None
    error: Optional[str] = None
    model_used: Optional[str] = None
    prompt_version: Optional[str] = None
    prompt_last_updated: Optional[str] = None
    test_query: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


class ScenarioLoader:
    """Loads and validates test scenarios from YAML files."""

    @staticmethod
    def load_scenarios(yaml_path: str) -> Tuple[Dict[str, Any], List[Scenario]]:
        """
        Load scenarios from a YAML file.

        Args:
            yaml_path: Path to the YAML file containing scenarios

        Returns:
            Tuple of (test_suite_metadata, list of Scenario objects)

        Raises:
            FileNotFoundError: If the YAML file doesn't exist
            yaml.YAMLError: If the YAML is invalid
            ValueError: If scenario validation fails
        """
        path = Path(yaml_path)
        if not path.exists():
            raise FileNotFoundError(f"Scenario file not found: {yaml_path}")

        with open(path, "r") as f:
            data = yaml.safe_load(f)

        if not data or "test_suite" not in data:
            raise ValueError("YAML must contain 'test_suite' root key")

        test_suite = data["test_suite"]
        scenarios_data = test_suite.get("scenarios", [])

        if not scenarios_data:
            raise ValueError("No scenarios found in YAML file")

        # Extract metadata
        metadata = {
            "name": test_suite.get("name", "Unnamed Test Suite"),
            "description": test_suite.get("description", ""),
            "version": test_suite.get("version", "1.0"),
            "default_thresholds": test_suite.get("default_thresholds", {}),
        }

        # Parse scenarios
        scenarios = []
        for scenario_data in scenarios_data:
            # Apply default thresholds if not overridden
            if "thresholds" not in scenario_data and metadata["default_thresholds"]:
                scenario_data["thresholds"] = metadata["default_thresholds"]

            scenario = Scenario(**scenario_data)
            scenarios.append(scenario)

        return metadata, scenarios


class MetricsCollector:
    """Collects and aggregates performance metrics during testing."""

    def __init__(self):
        """Initialize the metrics collector."""
        self.results: List[TestResult] = []
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None

    def start_test_suite(self):
        """Mark the start of a test suite."""
        self.start_time = datetime.now()
        self.results = []

    def end_test_suite(self):
        """Mark the end of a test suite."""
        self.end_time = datetime.now()

    def add_result(self, result: TestResult):
        """
        Add a test result to the collection.

        Args:
            result: TestResult object to add
        """
        self.results.append(result)

    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics for all collected results.

        Returns:
            Dictionary containing summary statistics
        """
        if not self.results:
            return {"total_tests": 0, "passed": 0, "failed": 0, "success_rate": 0.0}

        passed = sum(1 for r in self.results if r.success)
        failed = len(self.results) - passed
        latencies = [r.latency_ms for r in self.results]
        tokens = [r.tokens_used for r in self.results]
        costs = [r.cost for r in self.results]
        confidences = [r.confidence for r in self.results if r.confidence is not None]

        summary = {
            "total_tests": len(self.results),
            "passed": passed,
            "failed": failed,
            "success_rate": (passed / len(self.results)) * 100,
            "latency": {
                "mean": statistics.mean(latencies) if latencies else 0,
                "median": statistics.median(latencies) if latencies else 0,
                "min": min(latencies) if latencies else 0,
                "max": max(latencies) if latencies else 0,
                "stdev": statistics.stdev(latencies) if len(latencies) > 1 else 0,
            },
            "tokens": {"total": sum(tokens), "mean": statistics.mean(tokens) if tokens else 0},
            "cost": {"total": sum(costs), "mean": statistics.mean(costs) if costs else 0},
        }

        if confidences:
            summary["confidence"] = {
                "mean": statistics.mean(confidences),
                "median": statistics.median(confidences),
                "min": min(confidences),
                "max": max(confidences),
            }

        if self.start_time and self.end_time:
            summary["duration_seconds"] = (self.end_time - self.start_time).total_seconds()

        return summary


class ReportGenerator:
    """Generates test reports in various formats (PDF, CSV, JSON)."""

    def __init__(self, agent_name: str, results_dir: str = "results"):
        """
        Initialize the report generator.

        Args:
            agent_name: Name of the agent being tested
            results_dir: Directory to save reports
        """
        self.agent_name = agent_name
        self.results_dir = Path(results_dir)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    def generate_all_reports(
        self, metadata: Dict[str, Any], results: List[TestResult], summary: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Generate reports in all formats.

        Args:
            metadata: Test suite metadata
            results: List of test results
            summary: Summary statistics

        Returns:
            Dictionary mapping format to file path
        """
        paths = {}

        # Generate JSON report
        json_path = self._generate_json(metadata, results, summary)
        paths["json"] = str(json_path)

        # Generate CSV report
        csv_path = self._generate_csv(results, summary)
        paths["csv"] = str(csv_path)

        # Generate PDF report
        pdf_path = self._generate_pdf(metadata, results, summary)
        paths["pdf"] = str(pdf_path)

        return paths

    def _generate_json(
        self, metadata: Dict[str, Any], results: List[TestResult], summary: Dict[str, Any]
    ) -> Path:
        """Generate JSON report."""
        filename = f"{self.agent_name}_{self.timestamp}.json"
        filepath = self.results_dir / filename

        report = {
            "metadata": metadata,
            "summary": summary,
            "results": [
                {
                    "scenario_id": r.scenario_id,
                    "scenario_name": r.scenario_name,
                    "success": r.success,
                    "actual_output": r.actual_output,
                    "expected_output": r.expected_output,
                    "latency_ms": r.latency_ms,
                    "tokens_used": r.tokens_used,
                    "cost": r.cost,
                    "confidence": r.confidence,
                    "error": r.error,
                    "model_used": r.model_used,
                    "prompt_version": r.prompt_version,
                    "prompt_last_updated": r.prompt_last_updated,
                    "test_query": r.test_query,
                    "timestamp": r.timestamp.isoformat(),
                }
                for r in results
            ],
            "generated_at": datetime.now().isoformat(),
        }

        with open(filepath, "w") as f:
            json.dump(report, f, indent=2)

        return filepath

    def _generate_csv(self, results: List[TestResult], summary: Dict[str, Any]) -> Path:
        """Generate CSV report."""
        filename = f"{self.agent_name}_{self.timestamp}.csv"
        filepath = self.results_dir / filename

        with open(filepath, "w", newline="") as f:
            # Write summary section
            writer = csv.writer(f)
            writer.writerow(["SUMMARY"])
            writer.writerow(["Metric", "Value"])
            writer.writerow(["Total Tests", summary["total_tests"]])
            writer.writerow(["Passed", summary["passed"]])
            writer.writerow(["Failed", summary["failed"]])
            writer.writerow(["Success Rate (%)", f"{summary['success_rate']:.2f}"])
            writer.writerow(["Mean Latency (seconds)", f"{summary['latency']['mean']/1000:.2f}"])
            writer.writerow(["Total Tokens", f"{summary['tokens']['total']:,}"])
            writer.writerow(["Total Cost ($)", f"{summary['cost']['total']:.4f}"])
            writer.writerow([])

            # Write detailed results
            writer.writerow(["DETAILED RESULTS"])
            if results:
                # Check if model info is available
                has_models = any(r.model_used for r in results)
                if has_models:
                    fieldnames = [
                        "scenario_id",
                        "scenario_name",
                        "model",
                        "success",
                        "latency_seconds",
                        "tokens_used",
                        "cost",
                        "prompt_version",
                        "prompt_last_updated",
                        "error",
                    ]
                else:
                    fieldnames = [
                        "scenario_id",
                        "scenario_name",
                        "success",
                        "latency_seconds",
                        "tokens_used",
                        "cost",
                        "prompt_version",
                        "prompt_last_updated",
                        "error",
                    ]
                writer.writerow(fieldnames)

                for r in results:
                    if has_models:
                        writer.writerow(
                            [
                                r.scenario_id,
                                r.scenario_name,
                                r.model_used or "N/A",
                                r.success,
                                f"{r.latency_ms/1000:.2f}",  # Convert to seconds
                                f"{r.tokens_used:,}",  # Format with comma
                                f"{r.cost:.4f}",
                                r.prompt_version or "N/A",
                                r.prompt_last_updated or "N/A",
                                r.error or "",
                            ]
                        )
                    else:
                        writer.writerow(
                            [
                                r.scenario_id,
                                r.scenario_name,
                                r.success,
                                f"{r.latency_ms/1000:.2f}",  # Convert to seconds
                                f"{r.tokens_used:,}",  # Format with comma
                                f"{r.cost:.4f}",
                                r.prompt_version or "N/A",
                                r.prompt_last_updated or "N/A",
                                r.error or "",
                            ]
                        )

        return filepath

    def _generate_pdf_old(
        self, metadata: Dict[str, Any], results: List[TestResult], summary: Dict[str, Any]
    ) -> Path:
        """Generate enhanced PDF report with model comparisons and charts."""
        filename = f"{self.agent_name}_{self.timestamp}.pdf"
        filepath = self.results_dir / filename

        # Create PDF document
        doc = SimpleDocTemplate(str(filepath), pagesize=letter)
        story = []
        styles = getSampleStyleSheet()

        # Title style
        title_style = ParagraphStyle(
            "CustomTitle",
            parent=styles["Heading1"],
            fontSize=24,
            textColor=colors.HexColor("#1a73e8"),
            spaceAfter=30,
            alignment=TA_CENTER,
        )

        # Add title
        report_title = metadata.get("name", "Performance Test")
        if "model_comparison" in metadata:
            report_title += " - Multi-Model Analysis"
        title = Paragraph(f"{report_title}", title_style)
        story.append(title)

        # Add metadata
        story.append(Paragraph(f"<b>Agent:</b> {self.agent_name}", styles["Normal"]))
        story.append(
            Paragraph(f"<b>Test Suite Version:</b> {metadata.get('version', 'N/A')}", styles["Normal"])
        )
        story.append(
            Paragraph(f"<b>Description:</b> {metadata.get('description', 'N/A')}", styles["Normal"])
        )
        
        # Add prompt version info if available from results
        if results and results[0].prompt_version:
            story.append(
                Paragraph(f"<b>Prompt Version:</b> {results[0].prompt_version}", styles["Normal"])
            )
            story.append(
                Paragraph(f"<b>Prompt Last Updated:</b> {results[0].prompt_last_updated}", styles["Normal"])
            )
        
        story.append(
            Paragraph(
                f"<b>Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                styles["Normal"],
            )
        )
        story.append(Spacer(1, 20))

        # Executive Summary
        story.append(Paragraph("Executive Summary", styles["Heading2"]))

        # Summary table
        summary_data = [
            ["Metric", "Value"],
            ["Total Tests", str(summary["total_tests"])],
            ["Passed", str(summary["passed"])],
            ["Failed", str(summary["failed"])],
            ["Success Rate", f"{summary['success_rate']:.2f}%"],
            ["Mean Latency", f"{summary['latency']['mean']/1000:.2f}s"],
            ["Median Latency", f"{summary['latency']['median']/1000:.2f}s"],
            ["Total Tokens", f"{summary['tokens']['total']:,}"],
            ["Total Cost", f"${summary['cost']['total']:.4f}"],
        ]

        summary_table = Table(summary_data)
        summary_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 12),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                    ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                    ("GRID", (0, 0), (-1, -1), 1, colors.black),
                ]
            )
        )
        story.append(summary_table)
        story.append(Spacer(1, 20))

        # Add model comparison section if available
        if "model_comparison" in metadata:
            story.append(Paragraph("Model Performance Comparison", styles["Heading2"]))

            model_comp_data = [
                ["Model", "Success Rate", "Avg Latency", "Total Tokens", "Total Cost"]
            ]
            for model_tier, model_summary in metadata["model_comparison"].items():
                model_comp_data.append(
                    [
                        model_tier.upper(),
                        f"{model_summary['success_rate']:.1f}%",
                        f"{model_summary['latency']['mean']/1000:.2f}s",
                        f"{model_summary['tokens']['total']:,}",
                        f"${model_summary['cost']['total']:.4f}",
                    ]
                )

            model_comp_table = Table(model_comp_data)
            model_comp_table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a73e8")),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("FONTSIZE", (0, 0), (-1, 0), 12),
                        ("BOTTOMPADDING", (0, 0), (-1, 0), 12),
                        ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#f0f0f0")),
                        ("GRID", (0, 0), (-1, -1), 1, colors.black),
                        (
                            "ROWBACKGROUNDS",
                            (0, 1),
                            (-1, -1),
                            [colors.white, colors.HexColor("#f0f0f0")],
                        ),
                    ]
                )
            )
            story.append(model_comp_table)
            story.append(Spacer(1, 20))

            # Add model comparison bar chart
            story.append(Paragraph("Success Rate by Model", styles["Heading3"]))

            drawing = Drawing(400, 200)
            bc = VerticalBarChart()
            bc.x = 50
            bc.y = 50
            bc.height = 125
            bc.width = 300

            model_names = []
            success_rates = []
            latencies = []

            for model_tier, model_summary in metadata["model_comparison"].items():
                model_names.append(model_tier.upper())
                success_rates.append(model_summary["success_rate"])
                latencies.append(model_summary["latency"]["mean"] / 1000)

            bc.data = [success_rates]
            bc.categoryAxis.categoryNames = model_names
            bc.valueAxis.valueMin = 0
            bc.valueAxis.valueMax = 100
            bc.valueAxis.valueStep = 20
            bc.bars[0].fillColor = colors.HexColor("#1a73e8")
            bc.categoryAxis.labels.fontSize = 10
            bc.valueAxis.labels.fontSize = 10
            bc.valueAxis.labelTextFormat = "%d%%"

            drawing.add(bc)
            story.append(drawing)
            story.append(Spacer(1, 20))

        # Add pass/fail pie chart
        if summary["total_tests"] > 0:
            story.append(Paragraph("Test Results Distribution", styles["Heading3"]))

            drawing = Drawing(400, 200)
            pie = Pie()
            pie.x = 150
            pie.y = 50
            pie.width = 100
            pie.height = 100
            pie.data = [summary["passed"], summary["failed"]]
            pie.labels = [f"Passed ({summary['passed']})", f"Failed ({summary['failed']})"]
            pie.slices[0].fillColor = colors.green
            pie.slices[1].fillColor = colors.red

            drawing.add(pie)
            story.append(drawing)
            story.append(Spacer(1, 20))

        # Detailed Results
        story.append(PageBreak())
        story.append(Paragraph("Detailed Test Results", styles["Heading2"]))

        # Enhanced results with query and expected/actual outputs
        if results:
            # Group results by scenario for better comparison across models
            scenarios_by_id = {}
            for r in results:
                if r.scenario_id not in scenarios_by_id:
                    scenarios_by_id[r.scenario_id] = []
                scenarios_by_id[r.scenario_id].append(r)
            
            for scenario_id, scenario_results in scenarios_by_id.items():
                # Take first result for scenario details
                first_result = scenario_results[0]
                
                # Scenario header
                story.append(Paragraph(f"<b>{first_result.scenario_name}</b>", styles["Heading3"]))
                
                # Test query
                if first_result.test_query:
                    query_text = first_result.test_query[:200] + ("..." if len(first_result.test_query) > 200 else "")
                    story.append(Paragraph(f"<b>Query:</b> {query_text}", styles["Normal"]))
                
                # Expected output (formatted based on type)
                if first_result.expected_output:
                    expected_str = self._format_output_for_display(first_result.expected_output)
                    story.append(Paragraph(f"<b>Expected:</b> {expected_str}", styles["Normal"]))
                
                # Results by model
                model_results_data = [["Model", "Result", "Actual Output", "Latency", "Cost"]]
                
                for r in scenario_results:
                    actual_str = self._format_output_for_display(r.actual_output)
                    model_results_data.append([
                        r.model_used or "Default",
                        "✓ PASS" if r.success else f"✗ FAIL",
                        actual_str[:100] + ("..." if len(actual_str) > 100 else ""),
                        f"{r.latency_ms/1000:.2f}s",
                        f"${r.cost:.4f}"
                    ])
                
                model_table = Table(model_results_data, colWidths=[60, 60, 200, 60, 60])
                model_table.setStyle(
                    TableStyle([
                        ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                        ("FONTSIZE", (0, 0), (-1, 0), 9),
                        ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
                        ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                        ("GRID", (0, 0), (-1, -1), 1, colors.black),
                        ("FONTSIZE", (0, 1), (-1, -1), 8),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ])
                )
                story.append(model_table)
                story.append(Spacer(1, 15))

        # Failed Tests Details
        failed_tests = [r for r in results if not r.success]
        if failed_tests:
            story.append(PageBreak())
            story.append(Paragraph("Failed Test Details", styles["Heading2"]))

            for r in failed_tests:
                story.append(Paragraph(f"<b>{r.scenario_name}</b>", styles["Heading3"]))
                story.append(Paragraph(f"<b>Scenario ID:</b> {r.scenario_id}", styles["Normal"]))
                if r.model_used:
                    story.append(Paragraph(f"<b>Model:</b> {r.model_used}", styles["Normal"]))
                story.append(Paragraph(f"<b>Expected:</b> {r.expected_output}", styles["Normal"]))
                story.append(Paragraph(f"<b>Actual:</b> {r.actual_output}", styles["Normal"]))
                if r.error:
                    error_style = ParagraphStyle(
                        "ErrorStyle", parent=styles["Normal"], textColor=colors.red
                    )
                    story.append(Paragraph(f"<b>Error:</b> {r.error}", error_style))
                story.append(Spacer(1, 10))

        # Build PDF
        doc.build(story)

        return filepath
    
    def _generate_pdf(self, metadata: Dict[str, Any], results: List[TestResult], summary: Dict[str, Any]) -> Path:
        """Generate PDF report using agent-specific formatting.
        
        Architecture:
        - pdf_router_report.py: Router-specific formatting
        - pdf_report_generic.py: Generic formatting for other agents (clarifier, response, etc.)
        - Future: pdf_planner_report.py for planner agent (when implemented)
        """
        # Use router-specific PDF generator for router tests
        if "router" in self.agent_name.lower():
            from pdf_router_report import RouterPDFGenerator
            generator = RouterPDFGenerator(self.agent_name, self.timestamp, self.results_dir)
        else:
            from pdf_report_generic import GenericPDFGenerator
            generator = GenericPDFGenerator(self.agent_name, self.timestamp, self.results_dir)
        
        return generator.generate(metadata, results, summary)
    
    def _format_output_for_display(self, output: Any) -> str:
        """Format output for display in reports."""
        if output is None:
            return "None"
        elif isinstance(output, dict):
            # Format key parts of dict output
            if "route" in output:
                return f"Route: {output.get('route', 'N/A')}"
            elif "status" in output:
                # Handle clarifier output with banks and periods
                status = output.get('status', 'N/A')
                parts = [f"Status: {status}"]
                
                # Add banks if present
                if "banks" in output and output["banks"]:
                    bank_ids = output.get('banks', {}).get('bank_ids', [])
                    if bank_ids:
                        parts.append(f"Banks: {bank_ids}")
                
                # Add periods if present
                if "periods" in output and output["periods"]:
                    periods_data = output["periods"].get("periods", {})
                    # Check for apply_all structure
                    if "apply_all" in periods_data:
                        year = periods_data["apply_all"].get("fiscal_year")
                        quarters = periods_data["apply_all"].get("quarters")
                        if year:
                            parts.append(f"Year: {year}")
                        if quarters:
                            parts.append(f"Quarters: {quarters}")
                    # Check for bank-specific structure
                    elif periods_data:
                        # Get first bank's period data for display
                        first_bank_id = list(periods_data.keys())[0] if periods_data else None
                        if first_bank_id and isinstance(periods_data[first_bank_id], dict):
                            year = periods_data[first_bank_id].get("fiscal_year")
                            quarters = periods_data[first_bank_id].get("quarters")
                            if year:
                                parts.append(f"Year: {year}")
                            if quarters:
                                parts.append(f"Quarters: {quarters}")
                
                return ", ".join(parts)
            # Handle expected output format for clarifier tests
            elif "bank_ids" in output or "period_year" in output or "period_quarters" in output or "needs_clarification" in output:
                parts = []
                if "status" in output:
                    parts.append(f"Status: {output['status']}")
                if "bank_ids" in output:
                    parts.append(f"Banks: {output['bank_ids']}")
                if "period_year" in output:
                    parts.append(f"Year: {output['period_year']}")
                if "period_quarters" in output:
                    parts.append(f"Quarters: {output['period_quarters']}")
                if "needs_clarification" in output and output["needs_clarification"]:
                    if "status" not in output:
                        parts.append("Needs Clarification")
                return ", ".join(parts) if parts else str(output)
            else:
                # Generic dict formatting
                items = [f"{k}: {v}" for k, v in list(output.items())[:3]]
                return ", ".join(items)
        elif isinstance(output, str):
            return output
        else:
            return str(output)


def run_agent_test(
    agent_function, scenario: Scenario, context: Dict[str, Any], model_tier: Optional[str] = None
) -> TestResult:
    """
    Run a single test scenario against an agent.

    Args:
        agent_function: The agent function to test (should match main.py usage)
        scenario: The scenario to test
        context: Context dictionary with auth_config, ssl_config, etc.
        model_tier: Optional model tier override ('small', 'medium', 'large')

    Returns:
        TestResult object with the test outcome
    """
    start_time = time.time()

    try:
        # Get messages for the scenario
        messages = scenario.get_messages()

        # Extract the latest message (last in the list)
        latest_message = messages[-1]["content"] if messages else ""
        
        # Store the test query for reporting
        test_query = latest_message if scenario.message else json.dumps(messages, indent=2)

        # Add model tier to context if specified
        if model_tier:
            context = dict(context)  # Create a copy to avoid modifying original
            context["model_tier_override"] = model_tier

        # Call the agent function (matching how main.py calls it)
        result = agent_function(
            conversation_history=messages, latest_message=latest_message, context=context
        )

        # Calculate metrics
        latency_ms = (time.time() - start_time) * 1000

        # Extract actual output based on agent response structure
        actual_output = result.get("decision", result.get("route", result))
        confidence = result.get("confidence", result.get("confidence_score"))

        # Check if the result matches expectations
        success = True
        errors = []

        # Check route/decision if specified (for router agent)
        if "route" in scenario.expected:
            if actual_output != scenario.expected["route"]:
                success = False
                errors.append(
                    f"Expected route '{scenario.expected['route']}', got '{actual_output}'"
                )
        
        # Check clarifier-specific validations
        if isinstance(result, dict):
            # Check status
            if "status" in scenario.expected:
                actual_status = result.get("status")
                if actual_status != scenario.expected["status"]:
                    success = False
                    errors.append(
                        f"Expected status '{scenario.expected['status']}', got '{actual_status}'"
                    )
            
            # Check banks if expected
            if "bank_ids" in scenario.expected:
                actual_banks = None
                # For needs_clarification status, banks might be in the result directly or in banks field
                if result.get("status") == "needs_clarification":
                    # When needs clarification, banks might still be extracted
                    if result.get("banks"):
                        if result["banks"].get("status") == "success" and result["banks"].get("bank_ids"):
                            actual_banks = sorted(result["banks"]["bank_ids"])
                        elif result["banks"].get("decision") == "banks_selected" and result["banks"].get("bank_ids"):
                            actual_banks = sorted(result["banks"]["bank_ids"])
                elif result.get("banks") and result["banks"].get("bank_ids"):
                    actual_banks = sorted(result["banks"]["bank_ids"])
                expected_banks = sorted(scenario.expected["bank_ids"])
                
                if actual_banks != expected_banks:
                    success = False
                    errors.append(
                        f"Expected banks {expected_banks}, got {actual_banks}"
                    )
            
            # Check periods if expected
            if "period_year" in scenario.expected or "period_quarters" in scenario.expected:
                # Only check periods if status is success (not needs_clarification)
                # Unless periods are expected to be extracted even with clarification needed
                if result.get("status") == "success" or scenario.expected.get("has_periods"):
                    actual_periods = result.get("periods", {})
                    
                    # Check year
                    if "period_year" in scenario.expected:
                        actual_year = None
                        if actual_periods.get("periods"):
                            periods_data = actual_periods["periods"]
                            # Check for apply_all structure
                            if "apply_all" in periods_data:
                                actual_year = periods_data["apply_all"].get("fiscal_year")
                            # Check for bank-specific structure (e.g., {"1": {fiscal_year: 2024}})
                            elif periods_data:
                                # Get first bank's period data
                                first_bank_id = list(periods_data.keys())[0]
                                if isinstance(periods_data[first_bank_id], dict):
                                    actual_year = periods_data[first_bank_id].get("fiscal_year")
                        
                        if actual_year != scenario.expected["period_year"]:
                            success = False
                            errors.append(
                                f"Expected year {scenario.expected['period_year']}, got {actual_year}"
                            )
                    
                    # Check quarters
                    if "period_quarters" in scenario.expected:
                        actual_quarters = None
                        if actual_periods.get("periods"):
                            periods_data = actual_periods["periods"]
                            # Check for apply_all structure
                            if "apply_all" in periods_data:
                                actual_quarters = sorted(periods_data["apply_all"].get("quarters", []))
                            # Check for bank-specific structure
                            elif periods_data:
                                # Get first bank's period data
                                first_bank_id = list(periods_data.keys())[0]
                                if isinstance(periods_data[first_bank_id], dict):
                                    actual_quarters = sorted(periods_data[first_bank_id].get("quarters", []))
                        expected_quarters = sorted(scenario.expected["period_quarters"])
                        
                        if actual_quarters != expected_quarters:
                            success = False
                            errors.append(
                                f"Expected quarters {expected_quarters}, got {actual_quarters}"
                            )
            
            # Check if clarification was expected
            if "needs_clarification" in scenario.expected:
                actual_needs_clarification = result.get("status") == "needs_clarification"
                if actual_needs_clarification != scenario.expected["needs_clarification"]:
                    success = False
                    errors.append(
                        f"Expected needs_clarification={scenario.expected['needs_clarification']}, "
                        f"got {actual_needs_clarification}"
                    )

        # Check confidence threshold if specified
        if "min_confidence" in scenario.expected and confidence:
            if confidence < scenario.expected["min_confidence"]:
                success = False
                errors.append(
                    f"Confidence {confidence:.2f} below threshold "
                    f"{scenario.expected['min_confidence']}"
                )

        # Note: Latency thresholds are tracked but don't affect success
        # We expect different models to have different latencies
        if scenario.thresholds and "max_latency_ms" in scenario.thresholds:
            if latency_ms > scenario.thresholds["max_latency_ms"]:
                # Log this but don't fail the test
                pass  # Latency is informational only

        # Combine all errors
        error = " | ".join(errors) if errors else None

        # Extract token usage and cost if available
        tokens_used = result.get("tokens_used", 0)
        cost = result.get("cost", 0.0)
        # Always use the model_tier if provided, otherwise fall back to what's in the result
        model_used = model_tier if model_tier else result.get("model_used", "default")
        prompt_version = result.get("prompt_version")
        prompt_last_updated = result.get("prompt_last_updated")

        return TestResult(
            scenario_id=scenario.id,
            scenario_name=scenario.name,
            success=success,
            actual_output=actual_output,
            expected_output=scenario.expected.get("route", scenario.expected),
            latency_ms=latency_ms,
            tokens_used=tokens_used,
            cost=cost,
            confidence=confidence,
            error=error,
            model_used=model_used,
            prompt_version=prompt_version,
            prompt_last_updated=prompt_last_updated,
            test_query=test_query,
        )

    except Exception as e:
        latency_ms = (time.time() - start_time) * 1000
        return TestResult(
            scenario_id=scenario.id,
            scenario_name=scenario.name,
            success=False,
            actual_output=None,
            expected_output=scenario.expected,
            latency_ms=latency_ms,
            error=str(e),
            model_used=model_tier,
            prompt_version=None,
            prompt_last_updated=None,
            test_query=test_query if 'test_query' in locals() else None,
        )
