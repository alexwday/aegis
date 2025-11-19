"""
Document converter utilities for key themes ETL.

This module provides helper functions for creating Word documents from themed Q&A data
and utilities for markdown processing.
"""

from typing import Dict
from html.parser import HTMLParser
from docx.shared import Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_COLOR_INDEX
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import qn, nsdecls
from aegis.utils.logging import get_logger

logger = get_logger()


class HTMLToDocx(HTMLParser):
    """
    Parser to convert HTML-formatted text to Word document formatting.
    Supports: <b>, <strong>, <i>, <em>, <u>, <mark>, <span style="..."> tags and their nesting.
    """

    def __init__(self, paragraph, font_size=Pt(9)):
        super().__init__()
        self.paragraph = paragraph
        self.font_size = font_size
        self.text_buffer = ""
        self.format_stack = []
        self.style_stack = []

    def handle_starttag(self, tag, attrs):
        self._flush_text()

        if tag in ["b", "strong"]:
            self.format_stack.append("bold")
        elif tag in ["i", "em"]:
            self.format_stack.append("italic")
        elif tag == "u":
            self.format_stack.append("underline")
        elif tag == "mark":
            style_dict = self._parse_style(attrs)
            if style_dict and "background-color" in style_dict:
                self.format_stack.append("highlight_yellow")
            else:
                self.format_stack.append("highlight")
        elif tag == "span":
            style_dict = self._parse_style(attrs)
            self.style_stack.append(style_dict if style_dict else {})

    def handle_endtag(self, tag):
        self._flush_text()

        if tag in ["b", "strong"] and "bold" in self.format_stack:
            self.format_stack.remove("bold")
        elif tag in ["i", "em"] and "italic" in self.format_stack:
            self.format_stack.remove("italic")
        elif tag == "u" and "underline" in self.format_stack:
            self.format_stack.remove("underline")
        elif tag == "mark":
            if "highlight_yellow" in self.format_stack:
                self.format_stack.remove("highlight_yellow")
            elif "highlight" in self.format_stack:
                self.format_stack.remove("highlight")
        elif tag == "span" and self.style_stack:
            self.style_stack.pop()

    def handle_data(self, data):
        self.text_buffer += data

    def _parse_style(self, attrs):
        """Parse style attribute from HTML tags."""
        style_dict = {}
        for attr_name, attr_value in attrs:
            if attr_name == "style":

                styles = attr_value.split(";")
                for style in styles:
                    if ":" in style:
                        prop, value = style.split(":", 1)
                        style_dict[prop.strip().lower()] = value.strip()
        return style_dict

    def _flush_text(self):
        """Apply formatting and add text to document."""
        if not self.text_buffer:
            return

        run = self.paragraph.add_run(self.text_buffer)

        if self.style_stack:
            current_style = self.style_stack[-1]

            if "color" in current_style:
                color_hex = current_style["color"].lstrip("#")
                if color_hex.startswith("1e4d8b"):
                    run.font.color.rgb = RGBColor(0x1E, 0x4D, 0x8B)
                elif color_hex.startswith("4d94ff"):
                    run.font.color.rgb = RGBColor(0x4D, 0x94, 0xFF)

            if "font-size" in current_style:
                size_str = current_style["font-size"]
                if "pt" in size_str:
                    size = float(size_str.replace("pt", "").strip())
                    run.font.size = Pt(size)
                else:
                    run.font.size = self.font_size
            else:
                run.font.size = self.font_size

            if "font-weight" in current_style and current_style["font-weight"] == "bold":
                run.font.bold = True
        else:
            run.font.size = self.font_size

        if "bold" in self.format_stack:
            run.font.bold = True
        if "italic" in self.format_stack:
            run.font.italic = True
        if "underline" in self.format_stack:
            run.font.underline = True
        if "highlight" in self.format_stack or "highlight_yellow" in self.format_stack:
            run.font.highlight_color = WD_COLOR_INDEX.YELLOW

        self.text_buffer = ""

    def close(self):
        """Ensure any remaining text is flushed."""
        self._flush_text()
        super().close()


def add_page_numbers_with_footer(doc, bank_symbol, quarter, fiscal_year):
    """Add custom footer with bank info, document title, and page numbers."""
    for section in doc.sections:
        footer = section.footer

        footer.paragraphs[0].clear()

        tbl = footer.add_table(
            1,
            2,
            width=doc.sections[0].page_width
            - doc.sections[0].left_margin
            - doc.sections[0].right_margin,
        )
        tbl.autofit = False
        tbl.allow_autofit = False

        tbl.style = "Table Grid"
        for row in tbl.rows:
            for cell in row.cells:
                tc = cell._element
                tcPr = tc.get_or_add_tcPr()
                tcBorders = parse_xml(
                    f'<w:tcBorders {nsdecls("w")}>'
                    '<w:top w:val="nil"/><w:left w:val="nil"/>'
                    '<w:bottom w:val="nil"/><w:right w:val="nil"/></w:tcBorders>'
                )
                tcPr.append(tcBorders)

        left_cell = tbl.rows[0].cells[0]
        left_para = left_cell.paragraphs[0]
        left_para.alignment = WD_ALIGN_PARAGRAPH.LEFT
        left_text = (
            f"{bank_symbol} | {quarter}/{str(fiscal_year)[-2:]} | Investor Call - Key Themes"
        )
        run_left = left_para.add_run(left_text)
        run_left.font.size = Pt(9)
        run_left.font.color.rgb = RGBColor(68, 68, 68)

        right_cell = tbl.rows[0].cells[1]
        right_para = right_cell.paragraphs[0]
        right_para.alignment = WD_ALIGN_PARAGRAPH.RIGHT

        run = right_para.add_run()
        fldChar1 = OxmlElement("w:fldChar")
        fldChar1.set(qn("w:fldCharType"), "begin")
        run._element.append(fldChar1)

        instrText = OxmlElement("w:instrText")
        instrText.text = "PAGE"
        run._element.append(instrText)

        fldChar2 = OxmlElement("w:fldChar")
        fldChar2.set(qn("w:fldCharType"), "end")
        run._element.append(fldChar2)

        run_page = right_para.add_run(" | Page")
        run_page.font.size = Pt(9)
        run_page.font.color.rgb = RGBColor(68, 68, 68)


def add_theme_header_with_background(doc, theme_number, theme_title):
    """Add a theme header with dark blue text on light blue background."""
    heading = doc.add_paragraph()
    heading.paragraph_format.space_before = Pt(12)
    heading.paragraph_format.space_after = Pt(8)
    heading.paragraph_format.keep_with_next = True

    full_text = f"Theme {theme_number}: {theme_title}"
    run = heading.add_run(full_text)
    run.font.size = Pt(14)
    run.font.bold = True
    run.font.color.rgb = RGBColor(31, 73, 125)

    shading_elm = OxmlElement("w:shd")
    shading_elm.set(qn("w:val"), "clear")
    shading_elm.set(qn("w:color"), "auto")
    shading_elm.set(qn("w:fill"), "D4E1F5")
    heading._element.get_or_add_pPr().append(shading_elm)

    return heading


def theme_groups_to_markdown(
    theme_groups, bank_info: Dict[str, str], quarter: str, fiscal_year: int
) -> str:
    """
    Convert theme groups to markdown format matching the style of call_summary.

    Args:
        theme_groups: List of ThemeGroup objects with qa_blocks
        bank_info: Dictionary with bank_name, bank_symbol, ticker
        quarter: Quarter string (Q1-Q4)
        fiscal_year: Year

    Returns:
        Markdown-formatted string
    """

    ticker = bank_info.get("ticker", bank_info.get("bank_symbol", "Unknown"))
    markdown = f"# Key Themes Analysis - {ticker} {quarter} {fiscal_year}\n\n"

    for i, group in enumerate(theme_groups, 1):

        markdown += f"## Theme {i}: {group.group_title}\n\n"

        sorted_blocks = sorted(group.qa_blocks, key=lambda x: x.position)

        for j, qa_block in enumerate(sorted_blocks, 1):
            markdown += f"### Conversation {j}\n\n"

            content = qa_block.formatted_content or qa_block.original_content

            for line in content.split("\n"):
                if line.strip():

                    if line.strip() in ["---", "***", "___", "<hr>", "<hr/>", "<hr />"]:
                        continue

                    import re

                    clean_line = re.sub(r"<[^>]+>", "", line)

                    clean_line = (
                        clean_line.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
                    )

                    markdown += f"{clean_line}\n"

            markdown += "\n"

        markdown += "\n---\n\n"

    return markdown


def get_standard_report_metadata() -> Dict[str, str]:
    """
    Get standard metadata for key themes reports.

    Returns:
        Dictionary with report_name, report_description, and report_type
    """
    return {
        "report_name": "Key Themes Analysis",
        "report_description": (
            "AI-generated thematic analysis of earnings call Q&A sessions, "
            "identifying and grouping key discussion topics between analysts "
            "and executives. Provides consolidated insights into major themes "
            "with supporting conversation excerpts."
        ),
        "report_type": "key_themes",
    }
