"""
PDF Generator for Bank Earnings Reports.

This module generates multi-page PDF reports from the same data used for HTML reports.
Each page contains a title bar with bank branding and specific content sections.

Pages:
1. Overview + Key Metrics (landscape)
2. Items of Note (landscape)
3. Management Narrative (portrait)
4. Analyst Focus (portrait)
5. Segment Performance (portrait)
6. Capital & Risk Metrics (portrait)
"""

import base64
import io
from typing import Any, Dict, List, Optional, Tuple

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas


# Color definitions matching HTML theme
COLORS = {
    "bg_dark": colors.HexColor("#0a0a0c"),
    "text_white": colors.HexColor("#ffffff"),
    "text_muted": colors.HexColor("#64748b"),
    "text_main": colors.HexColor("#1a202c"),
    "accent": colors.HexColor("#005587"),
    "border": colors.HexColor("#e2e8f0"),
    "positive": colors.HexColor("#16a34a"),
    "negative": colors.HexColor("#dc2626"),
    "card_bg": colors.HexColor("#ffffff"),
    "light_bg": colors.HexColor("#f8fafc"),
    "muted_gray": colors.HexColor("#94a3b8"),
    "chart_fill": colors.HexColor("#e0f2fe"),
    "chart_line": colors.HexColor("#005587"),
    "avg_line": colors.HexColor("#f59e0b"),
}


def _get_direction_color(direction: str, for_dark_bg: bool = False) -> colors.Color:
    """Get color based on direction (positive/negative/neutral)."""
    if direction == "positive":
        return colors.HexColor("#4ade80") if for_dark_bg else COLORS["positive"]
    elif direction == "negative":
        return colors.HexColor("#f87171") if for_dark_bg else COLORS["negative"]
    return COLORS["muted_gray"]


def _format_delta(delta_obj: Dict[str, Any]) -> Tuple[str, colors.Color]:
    """Format a delta object and return (display_text, color)."""
    display = delta_obj.get("display", "—")
    direction = delta_obj.get("direction", "neutral")
    return display, _get_direction_color(direction)


def _clean_value(value: str) -> str:
    """Strip HTML tags from value strings."""
    return value.replace("<span class='unit'>", "").replace("</span>", "")


def _truncate_text(text: str, max_chars: int) -> str:
    """Truncate text to max characters with ellipsis."""
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 2] + ".."


def _draw_gradient_rect(
    c: canvas.Canvas,
    x: float,
    y: float,
    width: float,
    height: float,
    color_left: str,
    color_right: str,
    steps: int = 50,
) -> None:
    """Draw a horizontal gradient rectangle."""
    left_rgb = colors.HexColor(color_left)
    right_rgb = colors.HexColor(color_right)

    step_width = width / steps
    for i in range(steps):
        ratio = i / steps
        r = left_rgb.red + (right_rgb.red - left_rgb.red) * ratio
        g = left_rgb.green + (right_rgb.green - left_rgb.green) * ratio
        b = left_rgb.blue + (right_rgb.blue - left_rgb.blue) * ratio
        c.setFillColor(colors.Color(r, g, b))
        c.rect(x + i * step_width, y, step_width + 1, height, fill=True, stroke=False)


def _draw_title_bar(
    c: canvas.Canvas,
    page_width: float,
    page_height: float,
    header_params: Dict[str, Any],
    dividend_data: Dict[str, Any],
    bar_height: float = 50,
) -> float:
    """Draw the title bar with gradient background and logo."""
    bar_y = page_height - bar_height

    # Draw gradient background
    brand_secondary = header_params.get("brand", {}).get("secondary", "#003366")
    _draw_gradient_rect(c, 0, bar_y, page_width, bar_height, brand_secondary, "#0a0a0c")

    # Left side: Bank name and subtitle
    left_margin = 15
    c.setFillColor(COLORS["text_white"])
    c.setFont("Helvetica-Bold", 16)
    c.drawString(left_margin, bar_y + 28, header_params.get("bank_name", "Bank Name"))

    c.setFillColor(colors.HexColor("#a0a0a0"))
    c.setFont("Helvetica", 8)
    subtitle = (
        f"Fiscal {header_params.get('fiscal_quarter', 'Q?')} "
        f"{header_params.get('fiscal_year', '????')} Report • "
        f"Period Ending {header_params.get('period_ending', 'N/A')}"
    )
    c.drawString(left_margin, bar_y + 12, subtitle)

    # Right side: Logo and dividend
    right_margin = page_width - 15
    dividend = dividend_data.get("dividend", {})

    # Logo (if available) - height-scaled to fit banner
    logo_base64 = header_params.get("logo_base64")
    logo_width = 0
    if logo_base64:
        try:
            image_data = base64.b64decode(logo_base64)
            logo_io = io.BytesIO(image_data)
            img = ImageReader(logo_io)
            # Get original image dimensions
            orig_width, orig_height = img.getSize()
            # Scale to fit banner height (30px target height with padding)
            target_height = 30
            aspect_ratio = orig_width / orig_height if orig_height > 0 else 1
            logo_height = target_height
            logo_width = target_height * aspect_ratio
            logo_x = right_margin - 160
            logo_y = bar_y + 10
            c.drawImage(
                img,
                logo_x,
                logo_y,
                width=logo_width,
                height=logo_height,
                preserveAspectRatio=True,
                mask="auto",
            )
        except Exception:
            pass

    # Dividend section
    div_x = right_margin - 120

    # Dividend label
    c.setFillColor(colors.HexColor("#808080"))
    c.setFont("Helvetica", 6)
    c.drawString(div_x, bar_y + 38, "QUARTERLY DIVIDEND")

    # Dividend amount
    c.setFillColor(COLORS["text_white"])
    c.setFont("Helvetica-Bold", 14)
    c.drawString(div_x, bar_y + 22, dividend.get("amount", "N/A"))

    # QoQ/YoY - with proper spacing between QoQ value and YoY label
    qoq = dividend.get("qoq", {})
    yoy = dividend.get("yoy", {})

    qoq_color = _get_direction_color(qoq.get("direction", "neutral"), for_dark_bg=True)
    yoy_color = _get_direction_color(yoy.get("direction", "neutral"), for_dark_bg=True)

    # QoQ label and value
    c.setFont("Helvetica", 6)
    c.setFillColor(colors.HexColor("#808080"))
    c.drawString(div_x, bar_y + 10, "QoQ")
    c.setFont("Helvetica-Bold", 7)
    c.setFillColor(qoq_color)
    qoq_display = qoq.get("display", "—")
    c.drawString(div_x + 18, bar_y + 10, qoq_display)

    # Calculate QoQ value width for proper YoY spacing
    qoq_value_width = c.stringWidth(qoq_display, "Helvetica-Bold", 7)
    yoy_label_x = div_x + 18 + qoq_value_width + 12  # 12pt gap after QoQ value

    # YoY label and value
    c.setFont("Helvetica", 6)
    c.setFillColor(colors.HexColor("#808080"))
    c.drawString(yoy_label_x, bar_y + 10, "YoY")
    c.setFont("Helvetica-Bold", 7)
    c.setFillColor(yoy_color)
    c.drawString(yoy_label_x + 18, bar_y + 10, yoy.get("display", "—"))

    return bar_y - 18  # More padding below banner before content starts


def _draw_section_header(c: canvas.Canvas, x: float, y: float, text: str) -> float:
    """Draw a section header and return the new Y position."""
    c.setFillColor(COLORS["text_muted"])
    c.setFont("Helvetica-Bold", 8)
    c.drawString(x, y, text.upper())
    return y - 12


def _draw_metric_tile(
    c: canvas.Canvas,
    x: float,
    y: float,
    width: float,
    height: float,
    label: str,
    value: str,
    qoq: Dict[str, Any],
    yoy: Dict[str, Any],
) -> None:
    """Draw a metric tile matching HTML style."""
    # Background
    c.setFillColor(COLORS["light_bg"])
    c.roundRect(x, y, width, height, 3, fill=True, stroke=False)
    c.setStrokeColor(COLORS["border"])
    c.setLineWidth(0.5)
    c.roundRect(x, y, width, height, 3, fill=False, stroke=True)

    center_x = x + width / 2
    clean_val = _clean_value(value)

    # Label (truncate to fit) - with top padding
    max_label_chars = int(width / 4.5)
    display_label = _truncate_text(label, max_label_chars)
    c.setFillColor(COLORS["accent"])
    c.setFont("Helvetica-Bold", 6)
    c.drawCentredString(center_x, y + height - 12, display_label.upper())

    # Value
    c.setFillColor(COLORS["text_main"])
    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(center_x, y + height - 26, clean_val)

    # Horizontal divider below value
    divider_y = y + height - 32
    c.setStrokeColor(COLORS["border"])
    c.setLineWidth(0.3)
    c.line(x + 4, divider_y, x + width - 4, divider_y)

    # QoQ/YoY section with vertical divider
    qoq_x = x + width * 0.25
    yoy_x = x + width * 0.75

    # Vertical divider between QoQ and YoY
    c.setStrokeColor(COLORS["border"])
    c.setLineWidth(0.3)
    c.line(center_x, divider_y - 2, center_x, y + 4)

    # QoQ label and value
    c.setFillColor(COLORS["muted_gray"])
    c.setFont("Helvetica-Bold", 5)
    c.drawCentredString(qoq_x, y + 16, "QOQ")

    qoq_display, qoq_color = _format_delta(qoq)
    c.setFont("Helvetica-Bold", 6)
    c.setFillColor(qoq_color)
    c.drawCentredString(qoq_x, y + 7, qoq_display)

    # YoY label and value
    c.setFillColor(COLORS["muted_gray"])
    c.setFont("Helvetica-Bold", 5)
    c.drawCentredString(yoy_x, y + 16, "YOY")

    yoy_display, yoy_color = _format_delta(yoy)
    c.setFont("Helvetica-Bold", 6)
    c.setFillColor(yoy_color)
    c.drawCentredString(yoy_x, y + 7, yoy_display)


def _draw_slim_tile(
    c: canvas.Canvas,
    x: float,
    y: float,
    width: float,
    height: float,
    label: str,
    value: str,
    qoq: Dict[str, Any],
    yoy: Dict[str, Any],
) -> None:
    """Draw a slim metric tile."""
    # Background
    c.setFillColor(COLORS["light_bg"])
    c.roundRect(x, y, width, height, 3, fill=True, stroke=False)
    c.setStrokeColor(COLORS["border"])
    c.setLineWidth(0.5)
    c.roundRect(x, y, width, height, 3, fill=False, stroke=True)

    center_x = x + width / 2
    clean_val = _clean_value(value)

    # Label - with top padding
    max_label_chars = int(width / 4)
    display_label = _truncate_text(label, max_label_chars)
    c.setFillColor(COLORS["accent"])
    c.setFont("Helvetica-Bold", 5)
    c.drawCentredString(center_x, y + height - 9, display_label.upper())

    # Three columns: Current | QoQ | YoY
    col_width = width / 3
    col1_x = x + col_width * 0.5
    col2_x = x + col_width * 1.5
    col3_x = x + col_width * 2.5

    # Labels
    c.setFillColor(COLORS["muted_gray"])
    c.setFont("Helvetica-Bold", 4)
    c.drawCentredString(col1_x, y + height - 17, "CURRENT")
    c.drawCentredString(col2_x, y + height - 17, "QOQ")
    c.drawCentredString(col3_x, y + height - 17, "YOY")

    # Values - with bottom padding
    c.setFillColor(COLORS["text_main"])
    c.setFont("Helvetica-Bold", 7)
    c.drawCentredString(col1_x, y + 6, clean_val)

    qoq_display, qoq_color = _format_delta(qoq)
    yoy_display, yoy_color = _format_delta(yoy)

    c.setFont("Helvetica-Bold", 6)
    c.setFillColor(qoq_color)
    c.drawCentredString(col2_x, y + 6, qoq_display)
    c.setFillColor(yoy_color)
    c.drawCentredString(col3_x, y + 6, yoy_display)


def _draw_chart(
    c: canvas.Canvas,
    x: float,
    y: float,
    width: float,
    height: float,
    chart_data: Dict[str, Any],
) -> None:
    """Draw a static trend chart matching HTML style."""
    # Background
    c.setFillColor(COLORS["light_bg"])
    c.roundRect(x, y, width, height, 3, fill=True, stroke=False)
    c.setStrokeColor(COLORS["border"])
    c.setLineWidth(0.5)
    c.roundRect(x, y, width, height, 3, fill=False, stroke=True)

    metrics = chart_data.get("metrics", [])
    if not metrics:
        c.setFillColor(COLORS["muted_gray"])
        c.setFont("Helvetica", 7)
        c.drawCentredString(x + width / 2, y + height / 2, "No chart data")
        return

    initial_idx = chart_data.get("initial_index", 0)
    metric = metrics[initial_idx] if initial_idx < len(metrics) else metrics[0]

    label = metric.get("label", metric.get("name", "Metric"))
    quarters = metric.get("quarters", [])
    values = metric.get("values", [])
    unit = metric.get("unit", "")
    decimals = metric.get("decimal_places", 0)

    if not values or not quarters:
        return

    # Title (matching HTML style)
    c.setFillColor(COLORS["accent"])
    c.setFont("Helvetica-Bold", 6)
    display_label = _truncate_text(label, 40)
    c.drawCentredString(x + width / 2, y + height - 10, display_label.upper())

    # Chart area with padding for labels
    chart_left = x + 30  # Space for Y-axis labels
    chart_right = x + width - 8
    chart_bottom = y + 42  # Space for stats bar
    chart_top = y + height - 20
    chart_width = chart_right - chart_left
    chart_height = chart_top - chart_bottom

    # Calculate scales with 15% padding (matching HTML)
    min_val = min(values)
    max_val = max(values)
    val_range = max_val - min_val if max_val != min_val else 1
    padding = val_range * 0.15
    y_min = min_val - padding
    y_max = max_val + padding

    def val_to_y(v: float) -> float:
        return chart_bottom + ((v - y_min) / (y_max - y_min)) * chart_height

    def idx_to_x(i: int) -> float:
        if len(values) == 1:
            return chart_left + chart_width / 2
        return chart_left + (i / (len(values) - 1)) * chart_width

    # Format value helper (matching HTML formatValue)
    def fmt(v: float, short: bool = False) -> str:
        if unit == "%":
            return f"{v:.{decimals}f}%"
        elif unit == "$M":
            if abs(v) >= 1000:
                formatted = f"{v/1000:.2f}".rstrip("0").rstrip(".")
                return f"${formatted}B" if short else f"${formatted} B"
            formatted = f"{v:.2f}".rstrip("0").rstrip(".")
            return f"${formatted}M" if short else f"${formatted} M"
        return f"{v:.{decimals}f}"

    # Draw Y-axis labels
    c.setFillColor(COLORS["muted_gray"])
    c.setFont("Helvetica", 5)
    c.drawRightString(chart_left - 3, chart_top - 3, fmt(y_max, short=True))
    c.drawRightString(
        chart_left - 3, chart_bottom + chart_height / 2 - 2, fmt((y_min + y_max) / 2, short=True)
    )
    c.drawRightString(chart_left - 3, chart_bottom - 3, fmt(y_min, short=True))

    # Draw horizontal grid lines
    c.setStrokeColor(colors.HexColor("#f0f4f8"))
    c.setLineWidth(0.3)
    c.line(chart_left, chart_top, chart_right, chart_top)
    c.line(
        chart_left, chart_bottom + chart_height / 2, chart_right, chart_bottom + chart_height / 2
    )
    c.line(chart_left, chart_bottom, chart_right, chart_bottom)

    # Draw vertical guide lines from points to x-axis (dashed)
    c.setStrokeColor(colors.HexColor("#e2e8f0"))
    c.setLineWidth(0.5)
    c.setDash([2, 2])
    for i, v in enumerate(values):
        px = idx_to_x(i)
        py = val_to_y(v)
        c.line(px, chart_bottom, px, py)
    c.setDash([])

    # Draw average line
    avg_val = sum(values) / len(values)
    avg_y = val_to_y(avg_val)
    c.setStrokeColor(COLORS["avg_line"])
    c.setLineWidth(1)
    c.setDash([4, 3])
    c.line(chart_left, avg_y, chart_right, avg_y)
    c.setDash([])

    # Draw filled area with gradient effect (lighter at bottom)
    path = c.beginPath()
    path.moveTo(idx_to_x(0), chart_bottom)
    for i, v in enumerate(values):
        path.lineTo(idx_to_x(i), val_to_y(v))
    path.lineTo(idx_to_x(len(values) - 1), chart_bottom)
    path.close()
    c.setFillColor(colors.HexColor("#e0f2fe"))  # Light blue fill
    c.drawPath(path, fill=True, stroke=False)

    # Draw line (thicker, matching HTML)
    c.setStrokeColor(COLORS["chart_line"])
    c.setLineWidth(2)
    path = c.beginPath()
    path.moveTo(idx_to_x(0), val_to_y(values[0]))
    for i, v in enumerate(values[1:], 1):
        path.lineTo(idx_to_x(i), val_to_y(v))
    c.drawPath(path, fill=False, stroke=True)

    # Find min/max indices
    min_idx = values.index(min_val)
    max_idx = values.index(max_val)
    current_idx = len(values) - 1

    # Draw points with white stroke (matching HTML)
    for i, v in enumerate(values):
        px, py = idx_to_x(i), val_to_y(v)
        if i == min_idx:
            point_color = COLORS["negative"]
        elif i == max_idx:
            point_color = COLORS["positive"]
        else:
            point_color = COLORS["chart_line"]

        # Larger point for current value
        radius = 3.5 if i == current_idx else 2.5
        stroke_width = 1.5 if i == current_idx else 1

        c.setFillColor(point_color)
        c.setStrokeColor(colors.white)
        c.setLineWidth(stroke_width)
        c.circle(px, py, radius, fill=True, stroke=True)

    # Draw value labels above points
    for i, v in enumerate(values):
        px, py = idx_to_x(i), val_to_y(v)
        if i == min_idx:
            label_color = COLORS["negative"]
            font_size = 6
            font_weight = "Helvetica-Bold"
        elif i == max_idx:
            label_color = COLORS["positive"]
            font_size = 6
            font_weight = "Helvetica-Bold"
        elif i == current_idx:
            label_color = COLORS["chart_line"]
            font_size = 6
            font_weight = "Helvetica-Bold"
        else:
            label_color = COLORS["muted_gray"]
            font_size = 5
            font_weight = "Helvetica"

        c.setFillColor(label_color)
        c.setFont(font_weight, font_size)
        c.drawCentredString(px, py + 6, fmt(v, short=True))

    # X-axis labels (quarters)
    c.setFillColor(COLORS["muted_gray"])
    c.setFont("Helvetica-Bold", 5)
    for i, q in enumerate(quarters):
        qx = idx_to_x(i)
        c.drawCentredString(qx, chart_bottom - 10, q)

    # Stats bar at bottom (matching HTML layout)
    stats_y = y + 8
    divider_y = y + 24
    current_val = values[-1]

    # Divider line above stats
    c.setStrokeColor(colors.HexColor("#f0f4f8"))
    c.setLineWidth(0.5)
    c.line(x + 8, divider_y, x + width - 8, divider_y)

    # Stats layout: Low | Avg | High | Current
    stat_positions = [
        (x + width * 0.15, "Low", min_val, COLORS["negative"]),
        (x + width * 0.38, "Avg", avg_val, COLORS["avg_line"]),
        (x + width * 0.62, "High", max_val, COLORS["positive"]),
        (x + width * 0.85, "Current", current_val, COLORS["chart_line"]),
    ]

    for stat_x, stat_label, stat_val, stat_color in stat_positions:
        # Label
        c.setFillColor(COLORS["muted_gray"])
        c.setFont("Helvetica-Bold", 4)
        c.drawCentredString(stat_x, stats_y + 10, stat_label.upper())
        # Value
        c.setFillColor(stat_color)
        c.setFont("Helvetica-Bold", 6)
        c.drawCentredString(stat_x, stats_y + 2, fmt(stat_val))

    # Vertical dividers between stats
    c.setStrokeColor(colors.HexColor("#e2e8f0"))
    c.setLineWidth(0.3)
    for sep_x in [x + width * 0.27, x + width * 0.50, x + width * 0.73]:
        c.line(sep_x, stats_y, sep_x, stats_y + 16)


def _wrap_text(
    c: canvas.Canvas, text: str, max_width: float, font_name: str, font_size: float
) -> List[str]:
    """Wrap text to fit within max_width."""
    words = text.split()
    lines = []
    current_line = ""

    for word in words:
        test_line = f"{current_line} {word}".strip()
        if c.stringWidth(test_line, font_name, font_size) <= max_width:
            current_line = test_line
        else:
            if current_line:
                lines.append(current_line)
            current_line = word

    if current_line:
        lines.append(current_line)

    return lines


def _draw_items_of_note_table(
    c: canvas.Canvas,
    x: float,
    y: float,
    width: float,
    items_data: Dict[str, Any],
    max_items: int = 6,
) -> float:
    """Draw items of note table and return new Y position."""
    entries = items_data.get("entries", [])[:max_items]

    if not entries:
        c.setFillColor(COLORS["text_muted"])
        c.setFont("Helvetica-Oblique", 6)
        c.drawString(x, y - 10, "No significant items identified.")
        return y - 20

    # Table header
    col_widths = [width * 0.48, width * 0.12, width * 0.16, width * 0.12, width * 0.12]
    headers = ["Description", "Impact", "Segment", "Timing", "Source"]

    c.setFillColor(colors.HexColor("#f1f5f9"))
    c.rect(x, y - 10, width, 10, fill=True, stroke=False)

    c.setFillColor(COLORS["text_muted"])
    c.setFont("Helvetica-Bold", 5)

    hx = x + 2
    for i, header in enumerate(headers):
        c.drawString(hx, y - 7, header.upper())
        hx += col_widths[i]

    y -= 12
    row_height = 11

    for idx, entry in enumerate(entries):
        if y < 25:
            break

        # Alternating row color
        if idx % 2 == 1:
            c.setFillColor(colors.HexColor("#fafbfc"))
            c.rect(x, y - row_height + 2, width, row_height, fill=True, stroke=False)

        col_x = x + 2

        # Description
        c.setFillColor(COLORS["text_main"])
        c.setFont("Helvetica", 5)
        desc = _truncate_text(entry.get("description", ""), 70)
        c.drawString(col_x, y - 6, desc)
        col_x += col_widths[0]

        # Impact
        impact = entry.get("impact", "")
        if impact.startswith("+"):
            c.setFillColor(COLORS["positive"])
        elif impact.startswith("-"):
            c.setFillColor(COLORS["negative"])
        else:
            c.setFillColor(COLORS["text_main"])
        c.setFont("Helvetica-Bold", 5)
        c.drawString(col_x, y - 6, impact)
        col_x += col_widths[1]

        # Segment
        c.setFillColor(COLORS["text_muted"])
        c.setFont("Helvetica", 5)
        segment = _truncate_text(entry.get("segment", ""), 14)
        c.drawString(col_x, y - 6, segment)
        col_x += col_widths[2]

        # Timing
        c.drawString(col_x, y - 6, entry.get("timing", ""))
        col_x += col_widths[3]

        # Source badge
        source = entry.get("source", "")
        badge_color = colors.HexColor("#dbeafe") if source == "RTS" else colors.HexColor("#fef3c7")
        text_color = colors.HexColor("#1e40af") if source == "RTS" else colors.HexColor("#92400e")
        c.setFillColor(badge_color)
        c.roundRect(col_x, y - 8, 22, 8, 2, fill=True, stroke=False)
        c.setFillColor(text_color)
        c.setFont("Helvetica-Bold", 4)
        c.drawString(col_x + 2, y - 5, source)

        y -= row_height

    return y - 4


def generate_page1_overview_metrics(
    c: canvas.Canvas,
    page_width: float,
    page_height: float,
    header_params: Dict[str, Any],
    dividend_data: Dict[str, Any],
    overview_data: Dict[str, Any],
    tiles_data: Dict[str, Any],
    chart_data: Dict[str, Any],
    dynamic_data: Dict[str, Any],
    items_data: Dict[str, Any],
) -> None:
    """Generate Page 1: Overview + Key Metrics + Items of Note (landscape)."""
    content_top = _draw_title_bar(c, page_width, page_height, header_params, dividend_data)

    left_margin = 15
    right_margin = page_width - 15
    content_width = right_margin - left_margin

    # Overview section
    y = content_top
    y = _draw_section_header(c, left_margin, y, "Overview")

    # Overview narrative
    narrative = overview_data.get("narrative", "No overview available.")
    c.setFillColor(COLORS["text_main"])
    c.setFont("Helvetica", 7)

    lines = _wrap_text(c, narrative, content_width, "Helvetica", 7)
    for line in lines[:3]:  # Max 3 lines to save space
        c.drawString(left_margin, y, line)
        y -= 9

    y -= 6

    # Key Metrics section
    y = _draw_section_header(c, left_margin, y, "Key Metrics")

    # Layout: 5 columns grid
    # Row 1: tile, tile, tile, chart (spans 2 cols)
    # Row 2: tile, tile, tile, chart (continues)
    # Row 3: slim, slim, slim, slim, slim

    gap = 6
    tile_height = 50  # Slightly reduced to fit items of note
    slim_height = 32  # Slightly reduced
    chart_col_span = 2

    # Calculate column widths
    num_cols = 5
    total_gap = gap * (num_cols - 1)
    col_width = (content_width - total_gap) / num_cols
    chart_width = col_width * chart_col_span + gap

    metrics = tiles_data.get("metrics", [])[:6]
    dynamic_metrics = dynamic_data.get("metrics", [])[:5]

    # Row 1: First 3 tiles + chart start
    row1_y = y - tile_height
    for i in range(3):
        if i < len(metrics):
            m = metrics[i]
            tile_x = left_margin + i * (col_width + gap)
            _draw_metric_tile(
                c,
                tile_x,
                row1_y,
                col_width,
                tile_height,
                m.get("label", ""),
                m.get("value", ""),
                m.get("qoq", {}),
                m.get("yoy", {}),
            )

    # Chart (spans 2 columns, 2 rows)
    chart_x = left_margin + 3 * (col_width + gap)
    chart_height = tile_height * 2 + gap
    _draw_chart(c, chart_x, row1_y - tile_height - gap, chart_width, chart_height, chart_data)

    # Row 2: Next 3 tiles
    row2_y = row1_y - tile_height - gap
    for i in range(3):
        idx = i + 3
        if idx < len(metrics):
            m = metrics[idx]
            tile_x = left_margin + i * (col_width + gap)
            _draw_metric_tile(
                c,
                tile_x,
                row2_y,
                col_width,
                tile_height,
                m.get("label", ""),
                m.get("value", ""),
                m.get("qoq", {}),
                m.get("yoy", {}),
            )

    # Row 3: 5 slim tiles
    row3_y = row2_y - slim_height - gap
    for i in range(5):
        if i < len(dynamic_metrics):
            m = dynamic_metrics[i]
            tile_x = left_margin + i * (col_width + gap)
            _draw_slim_tile(
                c,
                tile_x,
                row3_y,
                col_width,
                slim_height,
                m.get("label", ""),
                m.get("value", ""),
                m.get("qoq", {}),
                m.get("yoy", {}),
            )

    # Items of Note section (below metrics)
    y = row3_y - 10
    y = _draw_section_header(c, left_margin, y, "Items of Note")
    _draw_items_of_note_table(c, left_margin, y, content_width, items_data, max_items=6)

    # Footer
    c.setFillColor(COLORS["muted_gray"])
    c.setFont("Helvetica-Oblique", 5)
    c.drawString(left_margin, 10, "* Adjusted figures. Source: Supplementary Financial Pack")


def generate_page2_items_of_note(
    c: canvas.Canvas,
    page_width: float,
    page_height: float,
    header_params: Dict[str, Any],
    dividend_data: Dict[str, Any],
    items_data: Dict[str, Any],
) -> None:
    """Generate Page 2: Items of Note (landscape)."""
    content_top = _draw_title_bar(c, page_width, page_height, header_params, dividend_data)

    left_margin = 15
    right_margin = page_width - 15
    content_width = right_margin - left_margin

    y = content_top
    y = _draw_section_header(c, left_margin, y, "Items of Note")

    entries = items_data.get("entries", [])

    if not entries:
        c.setFillColor(COLORS["text_muted"])
        c.setFont("Helvetica-Oblique", 7)
        c.drawString(left_margin, y - 10, "No significant items identified.")
        return

    # Table header
    col_widths = [
        content_width * 0.50,
        content_width * 0.12,
        content_width * 0.15,
        content_width * 0.12,
        content_width * 0.11,
    ]
    headers = ["Description", "Impact", "Segment", "Timing", "Source"]

    c.setFillColor(colors.HexColor("#f1f5f9"))
    c.rect(left_margin, y - 12, content_width, 12, fill=True, stroke=False)

    c.setFillColor(COLORS["text_muted"])
    c.setFont("Helvetica-Bold", 6)

    hx = left_margin + 3
    for i, header in enumerate(headers):
        c.drawString(hx, y - 9, header.upper())
        hx += col_widths[i]

    y -= 14
    row_height = 14

    c.setFont("Helvetica", 6)

    for entry in entries:
        if y < 25:
            break

        # Alternating row color
        if entries.index(entry) % 2 == 1:
            c.setFillColor(colors.HexColor("#fafbfc"))
            c.rect(
                left_margin, y - row_height + 2, content_width, row_height, fill=True, stroke=False
            )

        col_x = left_margin + 3

        # Description
        c.setFillColor(COLORS["text_main"])
        desc = _truncate_text(entry.get("description", ""), 85)
        c.drawString(col_x, y - 8, desc)
        col_x += col_widths[0]

        # Impact
        impact = entry.get("impact", "")
        if impact.startswith("+"):
            c.setFillColor(COLORS["positive"])
        elif impact.startswith("-"):
            c.setFillColor(COLORS["negative"])
        else:
            c.setFillColor(COLORS["text_main"])
        c.setFont("Helvetica-Bold", 6)
        c.drawString(col_x, y - 8, impact)
        col_x += col_widths[1]

        # Segment
        c.setFillColor(COLORS["text_muted"])
        c.setFont("Helvetica", 6)
        segment = _truncate_text(entry.get("segment", ""), 15)
        c.drawString(col_x, y - 8, segment)
        col_x += col_widths[2]

        # Timing
        c.drawString(col_x, y - 8, entry.get("timing", ""))
        col_x += col_widths[3]

        # Source badge
        source = entry.get("source", "")
        badge_color = colors.HexColor("#dbeafe") if source == "RTS" else colors.HexColor("#fef3c7")
        text_color = colors.HexColor("#1e40af") if source == "RTS" else colors.HexColor("#92400e")
        c.setFillColor(badge_color)
        c.roundRect(col_x, y - 10, 28, 10, 2, fill=True, stroke=False)
        c.setFillColor(text_color)
        c.setFont("Helvetica-Bold", 5)
        c.drawString(col_x + 3, y - 7, source)

        y -= row_height

    c.setFillColor(COLORS["muted_gray"])
    c.setFont("Helvetica-Oblique", 5)
    c.drawString(left_margin, 10, "* Items from RTS regulatory filing and earnings transcript.")


def generate_page3_management_narrative(
    c: canvas.Canvas,
    page_width: float,
    page_height: float,
    header_params: Dict[str, Any],
    dividend_data: Dict[str, Any],
    narrative_data: Dict[str, Any],
) -> None:
    """Generate Page 3: Management Narrative (portrait)."""
    content_top = _draw_title_bar(c, page_width, page_height, header_params, dividend_data)

    left_margin = 15
    right_margin = page_width - 15
    content_width = right_margin - left_margin

    y = content_top
    y = _draw_section_header(c, left_margin, y, "Management Narrative")

    entries = narrative_data.get("entries", [])

    for entry in entries:
        if y < 40:
            break

        entry_type = entry.get("type", "rts")
        content = entry.get("content", "")

        if entry_type == "transcript":
            # Quote style
            c.setFillColor(COLORS["text_muted"])
            c.setFont("Helvetica", 16)
            c.drawString(left_margin, y - 5, '"')

            c.setFillColor(COLORS["text_main"])
            c.setFont("Helvetica-Oblique", 7)
            lines = _wrap_text(c, content, content_width - 20, "Helvetica-Oblique", 7)

            quote_y = y - 12
            for line in lines[:6]:
                c.drawString(left_margin + 10, quote_y, line)
                quote_y -= 9

            c.setFillColor(COLORS["text_muted"])
            c.setFont("Helvetica", 16)
            c.drawRightString(right_margin - 5, quote_y + 5, '"')

            # Attribution
            speaker = entry.get("speaker", "")
            title = entry.get("title", "")
            if speaker:
                c.setFillColor(COLORS["text_muted"])
                c.setFont("Helvetica-Bold", 6)
                c.drawRightString(right_margin - 10, quote_y - 5, f"— {speaker}, {title}")
                quote_y -= 12

            y = quote_y - 10

        else:
            # RTS plain paragraph
            c.setFillColor(COLORS["text_main"])
            c.setFont("Helvetica", 7)
            lines = _wrap_text(c, content, content_width, "Helvetica", 7)

            for line in lines[:8]:
                c.drawString(left_margin, y, line)
                y -= 9

            y -= 8

    c.setFillColor(COLORS["muted_gray"])
    c.setFont("Helvetica-Oblique", 5)
    c.drawString(left_margin, 10, "* Quotes condensed from earnings call transcript.")


def generate_page4_analyst_focus(
    c: canvas.Canvas,
    page_width: float,
    page_height: float,
    header_params: Dict[str, Any],
    dividend_data: Dict[str, Any],
    analyst_data: Dict[str, Any],
) -> None:
    """Generate Page 4: Analyst Focus - Top 4 Q&A (portrait)."""
    content_top = _draw_title_bar(c, page_width, page_height, header_params, dividend_data)

    left_margin = 15
    right_margin = page_width - 15
    content_width = right_margin - left_margin

    y = content_top
    y = _draw_section_header(c, left_margin, y, "Analyst Focus")

    featured = analyst_data.get("featured", [])[:4]

    for qa in featured:
        if y < 60:
            break

        # Card background - explicitly set stroke=False for fill, then draw border separately
        card_height = 75
        c.setFillColor(COLORS["light_bg"])
        c.roundRect(
            left_margin, y - card_height, content_width, card_height, 3, fill=True, stroke=False
        )
        c.setStrokeColor(COLORS["border"])
        c.setLineWidth(0.5)
        c.roundRect(
            left_margin, y - card_height, content_width, card_height, 3, fill=False, stroke=True
        )

        inner_left = left_margin + 8
        inner_width = content_width - 16

        # Theme
        c.setFillColor(COLORS["accent"])
        c.setFont("Helvetica-Bold", 6)
        theme = _truncate_text(qa.get("theme", "Topic"), 30)
        c.drawString(inner_left, y - 10, theme.upper())

        # Question
        c.setFillColor(COLORS["text_muted"])
        c.setFont("Helvetica", 10)
        c.drawString(inner_left, y - 20, '"')

        c.setFillColor(colors.HexColor("#475569"))
        c.setFont("Helvetica-Oblique", 6)
        question = qa.get("question", "")
        q_lines = _wrap_text(c, question, inner_width - 15, "Helvetica-Oblique", 6)

        q_y = y - 25
        for line in q_lines[:2]:
            c.drawString(inner_left + 8, q_y, line)
            q_y -= 8

        c.setFillColor(COLORS["text_muted"])
        c.setFont("Helvetica", 10)
        c.drawRightString(left_margin + content_width - 8, q_y + 5, '"')

        # Answer
        c.setFillColor(COLORS["text_main"])
        c.setFont("Helvetica", 6)
        answer = qa.get("answer", "")
        a_lines = _wrap_text(c, answer, inner_width, "Helvetica", 6)

        a_y = q_y - 6
        for line in a_lines[:4]:
            c.drawString(inner_left, a_y, line)
            a_y -= 8

        y = y - card_height - 8

    c.setFillColor(COLORS["muted_gray"])
    c.setFont("Helvetica-Oblique", 5)
    c.drawString(left_margin, 10, "* Q&A summarized from earnings call transcript.")


def _draw_segment_cards(
    c: canvas.Canvas,
    x: float,
    y: float,
    width: float,
    segments_data: Dict[str, Any],
    card_height: int = 55,
) -> float:
    """Draw segment performance cards and return new Y position."""
    entries = segments_data.get("entries", [])

    for segment in entries:
        if y < 70:
            break

        # Segment card - explicitly set stroke=False for fill, then draw border separately
        c.setFillColor(COLORS["light_bg"])
        c.roundRect(x, y - card_height, width, card_height, 3, fill=True, stroke=False)
        c.setStrokeColor(COLORS["border"])
        c.setLineWidth(0.5)
        c.roundRect(x, y - card_height, width, card_height, 3, fill=False, stroke=True)

        inner_left = x + 6

        # Segment name
        c.setFillColor(COLORS["accent"])
        c.setFont("Helvetica-Bold", 7)
        c.drawString(inner_left, y - 10, segment.get("name", "Segment").upper())

        # Two columns: description | metrics
        desc_width = width * 0.45
        metrics_x = x + desc_width + 10

        # Description
        description = segment.get("description", "")
        if description:
            c.setFillColor(COLORS["text_main"])
            c.setFont("Helvetica", 6)
            lines = _wrap_text(c, description, desc_width - 10, "Helvetica", 6)
            desc_y = y - 20
            for line in lines[:4]:
                c.drawString(inner_left, desc_y, line)
                desc_y -= 7

        # Core metrics table
        core_metrics = segment.get("core_metrics", [])[:3]
        if core_metrics:
            # Headers
            c.setFillColor(COLORS["muted_gray"])
            c.setFont("Helvetica-Bold", 5)
            c.drawString(metrics_x, y - 16, "METRIC")
            c.drawString(metrics_x + 55, y - 16, "VALUE")
            c.drawString(metrics_x + 95, y - 16, "QOQ")
            c.drawString(metrics_x + 125, y - 16, "YOY")

            metric_y = y - 26
            for m in core_metrics:
                c.setFillColor(COLORS["text_main"])
                c.setFont("Helvetica", 6)
                label = _truncate_text(m.get("label", ""), 12)
                c.drawString(metrics_x, metric_y, label)

                c.setFont("Helvetica-Bold", 6)
                value = _clean_value(m.get("value", ""))
                c.drawString(metrics_x + 55, metric_y, value)

                qoq = m.get("qoq", {})
                yoy = m.get("yoy", {})
                qoq_display, qoq_color = _format_delta(qoq)
                yoy_display, yoy_color = _format_delta(yoy)

                c.setFillColor(qoq_color)
                c.drawString(metrics_x + 95, metric_y, qoq_display)
                c.setFillColor(yoy_color)
                c.drawString(metrics_x + 125, metric_y, yoy_display)

                metric_y -= 9

        y = y - card_height - 5

    return y


def _draw_capital_risk_compact(
    c: canvas.Canvas,
    x: float,
    y: float,
    width: float,
    capital_risk_data: Dict[str, Any],
) -> float:
    """Draw compact capital & risk section and return new Y position."""
    # Regulatory Capital - 2x2 grid (compact)
    c.setFillColor(COLORS["accent"])
    c.setFont("Helvetica-Bold", 6)
    c.drawString(x, y, "REGULATORY CAPITAL")
    y -= 8

    regulatory_capital = capital_risk_data.get("regulatory_capital", [])[:4]
    tile_width = (width - 8) / 2
    tile_height = 32

    for i, ratio in enumerate(regulatory_capital):
        row = i // 2
        col = i % 2
        tx = x + col * (tile_width + 8)
        ty = y - (row + 1) * (tile_height + 4)

        c.setFillColor(COLORS["light_bg"])
        c.roundRect(tx, ty, tile_width, tile_height, 3, fill=True, stroke=False)
        c.setStrokeColor(COLORS["border"])
        c.roundRect(tx, ty, tile_width, tile_height, 3, fill=False, stroke=True)

        center_x = tx + tile_width / 2

        # Label
        c.setFillColor(COLORS["text_muted"])
        c.setFont("Helvetica-Bold", 5)
        label = ratio.get("label", "")
        min_req = ratio.get("min_requirement", "")
        if min_req:
            label += f" (min {min_req})"
        c.drawCentredString(center_x, ty + tile_height - 7, label)

        # Value
        c.setFillColor(COLORS["text_main"])
        c.setFont("Helvetica-Bold", 9)
        value = _clean_value(ratio.get("value", ""))
        c.drawCentredString(center_x, ty + tile_height - 18, value)

        # QoQ/YoY
        qoq = ratio.get("qoq", {})
        yoy = ratio.get("yoy", {})
        qoq_display, qoq_color = _format_delta(qoq)
        yoy_display, yoy_color = _format_delta(yoy)

        c.setFillColor(COLORS["muted_gray"])
        c.setFont("Helvetica-Bold", 4)
        c.drawString(tx + 10, ty + 5, "QOQ")
        c.drawString(tx + tile_width - 30, ty + 5, "YOY")

        c.setFont("Helvetica-Bold", 5)
        c.setFillColor(qoq_color)
        c.drawString(tx + 22, ty + 5, qoq_display)
        c.setFillColor(yoy_color)
        c.drawString(tx + tile_width - 18, ty + 5, yoy_display)

    y -= (2 * (tile_height + 4)) + 10

    # Liquidity & Credit Quality (compact table)
    c.setFillColor(COLORS["accent"])
    c.setFont("Helvetica-Bold", 6)
    c.drawString(x, y, "LIQUIDITY & CREDIT QUALITY")
    y -= 10

    # Headers
    c.setFillColor(COLORS["muted_gray"])
    c.setFont("Helvetica-Bold", 5)
    c.drawString(x, y, "METRIC")
    c.drawString(x + 50, y, "VALUE")
    c.drawString(x + 95, y, "QOQ")
    c.drawString(x + 130, y, "YOY")

    c.setStrokeColor(COLORS["border"])
    c.line(x, y - 3, x + width, y - 3)
    y -= 10

    liquidity_credit = capital_risk_data.get("liquidity_credit", [])
    for metric in liquidity_credit:
        c.setFillColor(COLORS["text_muted"])
        c.setFont("Helvetica-Bold", 5)
        c.drawString(x, y, metric.get("label", ""))

        c.setFillColor(COLORS["text_main"])
        c.setFont("Helvetica-Bold", 6)
        value = _clean_value(metric.get("value", ""))
        c.drawString(x + 50, y, value)

        qoq = metric.get("qoq", {})
        yoy = metric.get("yoy", {})
        qoq_display, qoq_color = _format_delta(qoq)
        yoy_display, yoy_color = _format_delta(yoy)

        c.setFont("Helvetica-Bold", 5)
        c.setFillColor(qoq_color)
        c.drawString(x + 95, y, qoq_display)
        c.setFillColor(yoy_color)
        c.drawString(x + 130, y, yoy_display)

        y -= 9

    return y


def generate_page5_segment_and_capital(
    c: canvas.Canvas,
    page_width: float,
    page_height: float,
    header_params: Dict[str, Any],
    dividend_data: Dict[str, Any],
    segments_data: Dict[str, Any],
    capital_risk_data: Dict[str, Any],
) -> None:
    """Generate combined Segment Performance + Capital & Risk page (portrait)."""
    content_top = _draw_title_bar(c, page_width, page_height, header_params, dividend_data)

    left_margin = 15
    right_margin = page_width - 15
    content_width = right_margin - left_margin

    y = content_top

    # Segment Performance section
    y = _draw_section_header(c, left_margin, y, "Segment Performance")
    y = _draw_segment_cards(c, left_margin, y, content_width, segments_data, card_height=52)

    # Capital & Risk section (below segments)
    y -= 8
    y = _draw_section_header(c, left_margin, y, "Capital & Risk Metrics")
    _draw_capital_risk_compact(c, left_margin, y, content_width, capital_risk_data)

    # Footer
    c.setFillColor(COLORS["muted_gray"])
    c.setFont("Helvetica-Oblique", 5)
    c.drawString(left_margin, 10, "* Adjusted figures. Source: Supplementary Pack & Pillar 3")


def generate_page5_segment_performance(
    c: canvas.Canvas,
    page_width: float,
    page_height: float,
    header_params: Dict[str, Any],
    dividend_data: Dict[str, Any],
    segments_data: Dict[str, Any],
) -> None:
    """Generate Page 5: Segment Performance (portrait) - legacy function."""
    content_top = _draw_title_bar(c, page_width, page_height, header_params, dividend_data)

    left_margin = 15
    right_margin = page_width - 15
    content_width = right_margin - left_margin

    y = content_top
    y = _draw_section_header(c, left_margin, y, "Segment Performance")

    entries = segments_data.get("entries", [])

    for segment in entries:
        if y < 70:
            break

        # Segment card - explicitly set stroke=False for fill, then draw border separately
        card_height = 60
        c.setFillColor(COLORS["light_bg"])
        c.roundRect(
            left_margin, y - card_height, content_width, card_height, 3, fill=True, stroke=False
        )
        c.setStrokeColor(COLORS["border"])
        c.setLineWidth(0.5)
        c.roundRect(
            left_margin, y - card_height, content_width, card_height, 3, fill=False, stroke=True
        )

        inner_left = left_margin + 6

        # Segment name
        c.setFillColor(COLORS["accent"])
        c.setFont("Helvetica-Bold", 7)
        c.drawString(inner_left, y - 10, segment.get("name", "Segment").upper())

        # Two columns: description | metrics
        desc_width = content_width * 0.45
        metrics_x = left_margin + desc_width + 10

        # Description
        description = segment.get("description", "")
        if description:
            c.setFillColor(COLORS["text_main"])
            c.setFont("Helvetica", 6)
            lines = _wrap_text(c, description, desc_width - 10, "Helvetica", 6)
            desc_y = y - 22
            for line in lines[:4]:
                c.drawString(inner_left, desc_y, line)
                desc_y -= 8

        # Core metrics table
        core_metrics = segment.get("core_metrics", [])[:3]
        if core_metrics:
            # Headers
            c.setFillColor(COLORS["muted_gray"])
            c.setFont("Helvetica-Bold", 5)
            c.drawString(metrics_x, y - 18, "METRIC")
            c.drawString(metrics_x + 55, y - 18, "VALUE")
            c.drawString(metrics_x + 95, y - 18, "QOQ")
            c.drawString(metrics_x + 125, y - 18, "YOY")

            metric_y = y - 28
            for m in core_metrics:
                c.setFillColor(COLORS["text_main"])
                c.setFont("Helvetica", 6)
                label = _truncate_text(m.get("label", ""), 12)
                c.drawString(metrics_x, metric_y, label)

                c.setFont("Helvetica-Bold", 6)
                value = _clean_value(m.get("value", ""))
                c.drawString(metrics_x + 55, metric_y, value)

                qoq = m.get("qoq", {})
                yoy = m.get("yoy", {})
                qoq_display, qoq_color = _format_delta(qoq)
                yoy_display, yoy_color = _format_delta(yoy)

                c.setFillColor(qoq_color)
                c.drawString(metrics_x + 95, metric_y, qoq_display)
                c.setFillColor(yoy_color)
                c.drawString(metrics_x + 125, metric_y, yoy_display)

                metric_y -= 10

        y = y - card_height - 6

    c.setFillColor(COLORS["muted_gray"])
    c.setFont("Helvetica-Oblique", 5)
    c.drawString(left_margin, 10, "* Adjusted figures. Source: Supplementary Pack")


def generate_page6_capital_risk(
    c: canvas.Canvas,
    page_width: float,
    page_height: float,
    header_params: Dict[str, Any],
    dividend_data: Dict[str, Any],
    capital_risk_data: Dict[str, Any],
) -> None:
    """Generate Page 6: Capital & Risk Metrics (portrait)."""
    content_top = _draw_title_bar(c, page_width, page_height, header_params, dividend_data)

    left_margin = 15
    right_margin = page_width - 15
    content_width = right_margin - left_margin

    y = content_top
    y = _draw_section_header(c, left_margin, y, "Capital & Risk Metrics")

    # Regulatory Capital - 2x2 grid
    c.setFillColor(COLORS["accent"])
    c.setFont("Helvetica-Bold", 6)
    c.drawString(left_margin, y, "REGULATORY CAPITAL")
    y -= 10

    regulatory_capital = capital_risk_data.get("regulatory_capital", [])[:4]
    tile_width = (content_width - 10) / 2
    tile_height = 40

    for i, ratio in enumerate(regulatory_capital):
        row = i // 2
        col = i % 2
        tx = left_margin + col * (tile_width + 10)
        ty = y - (row + 1) * (tile_height + 6)

        c.setFillColor(COLORS["light_bg"])
        c.roundRect(tx, ty, tile_width, tile_height, 3, fill=True)
        c.setStrokeColor(COLORS["border"])
        c.roundRect(tx, ty, tile_width, tile_height, 3, stroke=True)

        center_x = tx + tile_width / 2

        # Label
        c.setFillColor(COLORS["text_muted"])
        c.setFont("Helvetica-Bold", 5)
        label = ratio.get("label", "")
        min_req = ratio.get("min_requirement", "")
        if min_req:
            label += f" (min {min_req})"
        c.drawCentredString(center_x, ty + tile_height - 8, label)

        # Value
        c.setFillColor(COLORS["text_main"])
        c.setFont("Helvetica-Bold", 11)
        value = _clean_value(ratio.get("value", ""))
        c.drawCentredString(center_x, ty + tile_height - 22, value)

        # QoQ/YoY
        qoq = ratio.get("qoq", {})
        yoy = ratio.get("yoy", {})
        qoq_display, qoq_color = _format_delta(qoq)
        yoy_display, yoy_color = _format_delta(yoy)

        c.setFillColor(COLORS["muted_gray"])
        c.setFont("Helvetica-Bold", 4)
        c.drawString(tx + 15, ty + 8, "QOQ")
        c.drawString(tx + tile_width - 35, ty + 8, "YOY")

        c.setFont("Helvetica-Bold", 6)
        c.setFillColor(qoq_color)
        c.drawString(tx + 30, ty + 8, qoq_display)
        c.setFillColor(yoy_color)
        c.drawString(tx + tile_width - 20, ty + 8, yoy_display)

    y -= (2 * (tile_height + 6)) + 15

    # RWA Composition
    c.setFillColor(COLORS["accent"])
    c.setFont("Helvetica-Bold", 6)
    c.drawString(left_margin, y, "RWA COMPOSITION")
    y -= 12

    rwa = capital_risk_data.get("rwa", {})
    components = rwa.get("components", [])

    for comp in components:
        label = comp.get("label", "")
        value = _clean_value(comp.get("value", ""))
        pct = comp.get("percentage", 0)
        bar_color = comp.get("color", "#3b82f6")

        c.setFillColor(COLORS["text_muted"])
        c.setFont("Helvetica", 6)
        c.drawString(left_margin, y, label)

        # Bar
        bar_x = left_margin + 55
        bar_width = content_width - 110
        c.setFillColor(COLORS["border"])
        c.roundRect(bar_x, y - 2, bar_width, 6, 2, fill=True, stroke=False)
        c.setFillColor(colors.HexColor(bar_color))
        c.roundRect(bar_x, y - 2, bar_width * pct / 100, 6, 2, fill=True, stroke=False)

        c.setFillColor(COLORS["text_main"])
        c.setFont("Helvetica-Bold", 6)
        c.drawRightString(right_margin, y, value)

        y -= 12

    # Total RWA
    c.setStrokeColor(COLORS["border"])
    c.line(left_margin, y + 3, right_margin, y + 3)
    c.setFillColor(COLORS["text_main"])
    c.setFont("Helvetica-Bold", 7)
    c.drawString(left_margin, y - 6, "Total RWA")
    total = _clean_value(rwa.get("total", ""))
    c.drawRightString(right_margin, y - 6, total)

    y -= 20

    # Liquidity & Credit Quality
    c.setFillColor(COLORS["accent"])
    c.setFont("Helvetica-Bold", 6)
    c.drawString(left_margin, y, "LIQUIDITY & CREDIT QUALITY")
    y -= 12

    # Headers
    c.setFillColor(COLORS["muted_gray"])
    c.setFont("Helvetica-Bold", 5)
    c.drawString(left_margin, y, "METRIC")
    c.drawString(left_margin + 50, y, "VALUE")
    c.drawString(left_margin + 100, y, "QOQ")
    c.drawString(left_margin + 140, y, "YOY")

    c.setStrokeColor(COLORS["border"])
    c.line(left_margin, y - 3, right_margin, y - 3)
    y -= 10

    liquidity_credit = capital_risk_data.get("liquidity_credit", [])
    for metric in liquidity_credit:
        c.setFillColor(COLORS["text_muted"])
        c.setFont("Helvetica-Bold", 6)
        c.drawString(left_margin, y, metric.get("label", ""))

        c.setFillColor(COLORS["text_main"])
        c.setFont("Helvetica-Bold", 7)
        value = _clean_value(metric.get("value", ""))
        c.drawString(left_margin + 50, y, value)

        qoq = metric.get("qoq", {})
        yoy = metric.get("yoy", {})
        qoq_display, qoq_color = _format_delta(qoq)
        yoy_display, yoy_color = _format_delta(yoy)

        c.setFont("Helvetica-Bold", 6)
        c.setFillColor(qoq_color)
        c.drawString(left_margin + 100, y, qoq_display)
        c.setFillColor(yoy_color)
        c.drawString(left_margin + 140, y, yoy_display)

        y -= 10

    c.setFillColor(COLORS["muted_gray"])
    c.setFont("Helvetica-Oblique", 5)
    c.drawString(left_margin, 10, "* Source: Pillar 3 Regulatory Disclosures")


def generate_pdf_report(
    output_path: str,
    header_params: Dict[str, Any],
    dividend_data: Dict[str, Any],
    overview_data: Dict[str, Any],
    tiles_data: Dict[str, Any],
    items_data: Dict[str, Any],
    narrative_data: Dict[str, Any],
    analyst_data: Dict[str, Any],
    segments_data: Dict[str, Any],
    capital_risk_data: Dict[str, Any],
    chart_data: Optional[Dict[str, Any]] = None,
    dynamic_data: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Generate a complete PDF report with 5 pages.

    Page structure:
    1. Overview + Key Metrics + Items of Note (Landscape)
    2. Management Narrative (Portrait)
    3. Analyst Focus (Portrait)
    4. Segment Performance + Capital & Risk (Portrait)

    Args:
        output_path: Path for the output PDF file
        header_params: Bank header information (_0_header_params)
        dividend_data: Dividend information (_0_header_dividend)
        overview_data: Overview narrative (_1_keymetrics_overview)
        tiles_data: Key metrics tiles (_1_keymetrics_tiles)
        items_data: Items of note (_1_keymetrics_items)
        narrative_data: Management narrative (_2_narrative)
        analyst_data: Analyst focus Q&A (_3_analyst_focus)
        segments_data: Segment performance (_4_segments)
        capital_risk_data: Capital & risk metrics (_5_capital_risk)
        chart_data: Chart data (_1_keymetrics_chart)
        dynamic_data: Dynamic metrics (_1_keymetrics_dynamic)

    Returns:
        Path to the generated PDF file
    """
    if chart_data is None:
        chart_data = {"metrics": [], "initial_index": 0}
    if dynamic_data is None:
        dynamic_data = {"metrics": []}
    if items_data is None:
        items_data = {"entries": []}

    c = canvas.Canvas(output_path)

    # Page 1: Overview + Key Metrics + Items of Note (Landscape)
    page_width, page_height = landscape(A4)
    c.setPageSize((page_width, page_height))
    generate_page1_overview_metrics(
        c,
        page_width,
        page_height,
        header_params,
        dividend_data,
        overview_data,
        tiles_data,
        chart_data,
        dynamic_data,
        items_data,
    )
    c.showPage()

    # Page 2: Management Narrative (Portrait)
    page_width, page_height = A4
    c.setPageSize((page_width, page_height))
    generate_page3_management_narrative(
        c, page_width, page_height, header_params, dividend_data, narrative_data
    )
    c.showPage()

    # Page 3: Analyst Focus (Portrait)
    c.setPageSize((page_width, page_height))
    generate_page4_analyst_focus(
        c, page_width, page_height, header_params, dividend_data, analyst_data
    )
    c.showPage()

    # Page 4: Segment Performance + Capital & Risk (Portrait)
    c.setPageSize((page_width, page_height))
    generate_page5_segment_and_capital(
        c, page_width, page_height, header_params, dividend_data, segments_data, capital_risk_data
    )

    c.save()

    return output_path
