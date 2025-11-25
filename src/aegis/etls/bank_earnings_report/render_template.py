#!/usr/bin/env python3
"""
Render the report template with sample data.

Usage:
    python render_template.py

Output:
    rendered_report.html (open in browser to preview)
"""

import json
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

def load_json_files(data_dir: Path) -> dict:
    """Load all JSON files from the sample_data directory.

    Filenames starting with numbers get prefixed with underscore
    to create valid Jinja2 variable names.
    e.g., 0_header_params.json -> _0_header_params
    """
    data = {}
    for json_file in sorted(data_dir.glob("*.json")):
        key = json_file.stem  # filename without extension
        # Prefix with underscore if starts with a number (for valid variable name)
        if key[0].isdigit():
            key = f"_{key}"
        with open(json_file, 'r') as f:
            data[key] = json.load(f)
        print(f"  Loaded: {json_file.name} -> {key}")
    return data

def render_template(template_path: Path, data: dict, output_path: Path):
    """Render the Jinja2 template with the provided data."""
    env = Environment(loader=FileSystemLoader(template_path.parent))
    template = env.get_template(template_path.name)

    rendered = template.render(**data)

    with open(output_path, 'w') as f:
        f.write(rendered)

    print(f"\nRendered to: {output_path}")

def main():
    base_dir = Path(__file__).parent
    template_path = base_dir / "report_template.html"
    data_dir = base_dir / "sample_data"
    output_path = base_dir / "rendered_report.html"

    print("Loading sample data...")
    data = load_json_files(data_dir)

    print(f"\nRendering template: {template_path.name}")
    render_template(template_path, data, output_path)

    print(f"\nOpen in browser: file://{output_path.absolute()}")

if __name__ == "__main__":
    main()
