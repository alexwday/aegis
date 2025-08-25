"""
Specialized PDF report generation for Planner agent testing.
Follows the same format as the Router PDF report for consistency.
"""

from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
)
from reportlab.graphics.shapes import Drawing, String
from reportlab.graphics.charts.barcharts import VerticalBarChart


class PlannerPDFGenerator:
    """Generate PDF reports for planner agent testing matching router format."""
    
    def __init__(self, agent_name: str, timestamp: str, results_dir: Path):
        self.agent_name = agent_name
        self.timestamp = timestamp
        self.results_dir = results_dir
        
    def generate(self, metadata: Dict[str, Any], results: List, summary: Dict[str, Any]) -> Path:
        """Generate the planner-specific PDF report."""
        filename = f"{self.agent_name}_{self.timestamp}.pdf"
        filepath = self.results_dir / filename
        
        # Create PDF document with margins
        doc = SimpleDocTemplate(
            str(filepath), 
            pagesize=letter,
            leftMargin=0.75*inch,
            rightMargin=0.75*inch,
            topMargin=0.75*inch,
            bottomMargin=0.75*inch
        )
        
        story = []
        styles = getSampleStyleSheet()
        
        # Custom styles matching router
        title_style = ParagraphStyle(
            "CustomTitle",
            parent=styles["Heading1"],
            fontSize=22,
            textColor=colors.HexColor("#1a73e8"),
            spaceAfter=20,
            alignment=1  # Center
        )
        
        heading2_style = ParagraphStyle(
            "CustomHeading2",
            parent=styles["Heading2"],
            fontSize=16,
            textColor=colors.HexColor("#333333"),
            spaceBefore=12,
            spaceAfter=8
        )
        
        heading3_style = ParagraphStyle(
            "CustomHeading3",
            parent=styles["Heading3"],
            fontSize=13,
            textColor=colors.HexColor("#555555"),
            spaceBefore=8,
            spaceAfter=6
        )
        
        normal_style = ParagraphStyle(
            "CustomNormal",
            parent=styles["Normal"],
            fontSize=10,
            leading=12
        )
        
        small_style = ParagraphStyle(
            "Small",
            parent=styles["Normal"],
            fontSize=8,
            leading=10
        )
        
        # Title and metadata
        story.append(Paragraph(metadata.get("name", "Planner Performance Test Report"), title_style))
        story.append(Paragraph("Multi-Model Database Selection Analysis", heading3_style))
        story.append(Spacer(1, 12))
        
        # Report metadata
        meta_data = [
            f"<b>Agent:</b> {self.agent_name}",
            f"<b>Version:</b> {metadata.get('version', 'N/A')}",
            f"<b>Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ]
        for item in meta_data:
            story.append(Paragraph(item, normal_style))
        
        story.append(Spacer(1, 20))
        
        # 1. EXECUTIVE SUMMARY - Model comparison and overall stats
        story.append(Paragraph("Executive Summary", heading2_style))
        
        if "model_comparison" in metadata:
            # Model Performance Comparison Table
            model_data = [["Model", "Tests", "Passed", "Success Rate", "Avg Latency", "Total Cost"]]
            
            for model_tier in ["small", "medium", "large"]:
                if model_tier in metadata["model_comparison"]:
                    ms = metadata["model_comparison"][model_tier]
                    model_data.append([
                        model_tier.upper(),
                        str(ms["total_tests"]),
                        f"{ms['passed']}/{ms['total_tests']}",
                        f"{ms['success_rate']:.1f}%",
                        f"{ms['latency']['mean']/1000:.2f}s",
                        f"${ms['cost']['total']:.4f}"
                    ])
            
            # Add overall summary row
            model_data.append([
                "OVERALL",
                str(summary["total_tests"]),
                f"{summary['passed']}/{summary['total_tests']}",
                f"{summary['success_rate']:.1f}%",
                f"{summary['latency']['mean']/1000:.2f}s",
                f"${summary['cost']['total']:.4f}"
            ])
            
            model_table = Table(model_data, colWidths=[80, 50, 60, 80, 80, 80])
            model_table.setStyle(TableStyle([
                # Header row
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a73e8")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 10),
                # Data rows
                ("BACKGROUND", (0, 1), (-1, -2), colors.white),
                ("FONTSIZE", (0, 1), (-1, -2), 9),
                # Overall row
                ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#f0f0f0")),
                ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
                ("LINEABOVE", (0, -1), (-1, -1), 1, colors.black),
                # Grid
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]))
            story.append(model_table)
            story.append(Spacer(1, 20))
        
        # 2. SUCCESS RATE CHART with labels
        if "model_comparison" in metadata:
            story.append(Paragraph("Database Selection Accuracy by Model", heading3_style))
            
            drawing = Drawing(450, 250)
            
            # Create bar chart
            bc = VerticalBarChart()
            bc.x = 60
            bc.y = 60
            bc.height = 140
            bc.width = 350
            
            model_names = []
            success_rates = []
            passed_counts = []
            total_counts = []
            
            for model_tier in ["small", "medium", "large"]:
                if model_tier in metadata["model_comparison"]:
                    ms = metadata["model_comparison"][model_tier]
                    model_names.append(model_tier.upper())
                    success_rates.append(ms["success_rate"])
                    passed_counts.append(ms["passed"])
                    total_counts.append(ms["total_tests"])
            
            bc.data = [success_rates]
            bc.categoryAxis.categoryNames = model_names
            bc.valueAxis.valueMin = 0
            bc.valueAxis.valueMax = 100
            bc.valueAxis.valueStep = 20
            bc.bars[0].fillColor = colors.HexColor("#1a73e8")
            bc.categoryAxis.labels.fontSize = 11
            bc.valueAxis.labels.fontSize = 10
            bc.valueAxis.labelTextFormat = "%d%%"
            bc.barWidth = 60
            
            drawing.add(bc)
            
            # Add value labels on top of bars
            bar_x_positions = [60 + 60 + i * 120 for i in range(len(model_names))]
            for i, (rate, passed, total) in enumerate(zip(success_rates, passed_counts, total_counts)):
                # Percentage label
                y_pos = 60 + (rate * 140 / 100) + 5
                drawing.add(String(
                    bar_x_positions[i], y_pos,
                    f"{rate:.1f}%",
                    fontSize=10,
                    fontName="Helvetica-Bold",
                    textAnchor="middle"
                ))
                # x/x label
                drawing.add(String(
                    bar_x_positions[i], y_pos + 12,
                    f"({passed}/{total})",
                    fontSize=9,
                    fontName="Helvetica",
                    textAnchor="middle"
                ))
            
            story.append(drawing)
            story.append(Spacer(1, 20))
        
        # 3. DETAILED TEST RESULTS - Planner-specific formatting
        story.append(PageBreak())
        story.append(Paragraph("Detailed Test Results", heading2_style))
        
        if results:
            # Group results by scenario
            scenarios_by_id = {}
            for r in results:
                if r.scenario_id not in scenarios_by_id:
                    scenarios_by_id[r.scenario_id] = []
                scenarios_by_id[r.scenario_id].append(r)
            
            for scenario_num, (scenario_id, scenario_results) in enumerate(scenarios_by_id.items(), 1):
                first_result = scenario_results[0]
                
                # Scenario header
                story.append(Paragraph(
                    f"<b>Scenario {scenario_num}: {first_result.scenario_name}</b>",
                    heading3_style
                ))
                
                # Query/Message
                if hasattr(first_result, 'test_query') and first_result.test_query:
                    # Truncate long queries for readability
                    query_text = first_result.test_query
                    if len(query_text) > 200:
                        query_text = query_text[:200] + "..."
                    story.append(Paragraph(f"<b>Query:</b> {query_text}", small_style))
                
                # Expected databases
                if first_result.expected_output:
                    expected_dbs = first_result.expected_output
                    if isinstance(expected_dbs, dict) and "databases" in expected_dbs:
                        expected_dbs = expected_dbs["databases"]
                    if isinstance(expected_dbs, list):
                        expected_dbs_display = ", ".join(expected_dbs)
                    else:
                        expected_dbs_display = str(expected_dbs)
                    story.append(Paragraph(f"<b>Expected Databases:</b> {expected_dbs_display}", small_style))
                
                story.append(Spacer(1, 6))
                
                # Results table for each model
                result_data = [["Model", "Status", "Databases Selected", "Latency", "Cost"]]
                
                for r in scenario_results:
                    # Use the model tier directly
                    model_tier = (r.model_used or "default").upper()
                    status = "✓ Pass" if r.success else "✗ Fail"
                    
                    # Extract databases selected
                    databases = []
                    if isinstance(r.actual_output, dict) and r.actual_output.get("databases"):
                        for db in r.actual_output["databases"]:
                            if isinstance(db, dict) and "database_id" in db:
                                databases.append(db["database_id"])
                            elif isinstance(db, str):
                                databases.append(db)
                    
                    databases_display = ", ".join(databases) if databases else "-"
                    
                    # Format latency and cost
                    latency_str = f"{r.latency_ms/1000:.2f}s" if r.latency_ms else "-"
                    cost_str = f"${r.cost:.5f}" if r.cost else "$0.00000"
                    
                    result_data.append([
                        model_tier,
                        status,
                        databases_display,
                        latency_str,
                        cost_str
                    ])
                
                # Create results table with planner-appropriate column widths
                result_table = Table(
                    result_data,
                    colWidths=[60, 60, 180, 60, 70]
                )
                
                # Style the table
                table_style = [
                    # Header
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#666666")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 9),
                    ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                    ("ALIGN", (3, 0), (4, -1), "CENTER"),  # Center align latency, cost
                    # Data
                    ("FONTSIZE", (0, 1), (-1, -1), 8),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f8f8")]),
                ]
                
                # Color code pass/fail in status column
                for i, r in enumerate(scenario_results, 1):
                    if r.success:
                        table_style.append(("TEXTCOLOR", (1, i), (1, i), colors.green))
                    else:
                        table_style.append(("TEXTCOLOR", (1, i), (1, i), colors.red))
                
                result_table.setStyle(TableStyle(table_style))
                story.append(result_table)
                story.append(Spacer(1, 15))
        
        # Failed test details at the end
        failed_tests = [r for r in results if not r.success]
        if failed_tests:
            story.append(PageBreak())
            story.append(Paragraph("Failed Test Analysis", heading2_style))
            
            # Group failed tests by scenario for better organization
            failed_by_scenario = {}
            for r in failed_tests:
                if r.scenario_name not in failed_by_scenario:
                    failed_by_scenario[r.scenario_name] = []
                failed_by_scenario[r.scenario_name].append(r)
            
            for scenario_name, failures in failed_by_scenario.items():
                story.append(Paragraph(f"<b>{scenario_name}</b>", heading3_style))
                
                for r in failures:
                    model_label = f"{r.model_used.upper()}" if r.model_used else "Default"
                    story.append(Paragraph(f"<b>Model:</b> {model_label}", small_style))
                    
                    if r.error:
                        story.append(Paragraph(f"<b>Error:</b> {r.error}", small_style))
                    
                    # Show database selection mismatch
                    expected_dbs = r.expected_output
                    if isinstance(expected_dbs, dict) and "databases" in expected_dbs:
                        expected_dbs = expected_dbs["databases"]
                    
                    actual_dbs = []
                    if isinstance(r.actual_output, dict) and r.actual_output.get("databases"):
                        for db in r.actual_output["databases"]:
                            if isinstance(db, dict) and "database_id" in db:
                                actual_dbs.append(db["database_id"])
                            elif isinstance(db, str):
                                actual_dbs.append(db)
                    
                    expected_display = ", ".join(expected_dbs) if isinstance(expected_dbs, list) else str(expected_dbs)
                    actual_display = ", ".join(actual_dbs) if actual_dbs else "None"
                    
                    story.append(Paragraph(f"<b>Expected:</b> {expected_display}", small_style))
                    story.append(Paragraph(f"<b>Actual:</b> {actual_display}", small_style))
                    
                    story.append(Spacer(1, 8))
                
                story.append(Spacer(1, 10))
        
        # Build the PDF
        doc.build(story)
        return filepath


def generate_planner_pdf_report(
    filepath: Path,
    metadata: Dict[str, Any],
    results: List,
    summary: Dict[str, Any],
) -> None:
    """
    Generate a PDF report for Planner agent results using the class-based generator.
    
    Args:
        filepath: Path to save the PDF
        metadata: Test suite metadata
        results: List of test results
        summary: Summary statistics
    """
    # Extract agent name and timestamp from filepath
    filename = filepath.stem  # Get filename without extension
    parts = filename.split("_")
    
    # Typically format is "planner_multi_model_TIMESTAMP"
    agent_name = "_".join(parts[:-1]) if len(parts) > 1 else "planner"
    timestamp = parts[-1] if len(parts) > 1 else datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Use the class-based generator
    generator = PlannerPDFGenerator(agent_name, timestamp, filepath.parent)
    generator.generate(metadata, results, summary)