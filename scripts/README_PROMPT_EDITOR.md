# Prompt Editor - User Guide

## Overview
Standalone web interface for viewing and editing prompts in the `prompts` database table.

## Features
- ✅ View all prompts in a searchable, filterable table
- ✅ Click to select and view full prompt details
- ✅ Edit any field (except ID, created_at)
- ✅ Save changes in two ways:
  - **Overwrite**: Update existing record, keeps same version
  - **New Version**: Create new record with incremented version

## Quick Start

### 1. Start the Editor
From the `aegis/` directory:
```bash
python scripts/prompt_editor.py
```

### 2. Open in Browser
Navigate to: **http://localhost:5001**

### 3. Usage

#### View Prompts
- All prompts displayed in main table
- Use search box to filter by name/description
- Use dropdowns to filter by Layer or Model
- Statistics shown at top (total prompts, layers, models)

#### Edit a Prompt
1. **Click any row** in the table to select
2. Details panel opens below with all fields
3. **Edit fields** as needed:
   - Model, Layer, Name (text fields)
   - Description, Comments (text areas)
   - System Prompt, User Prompt (large text areas)
   - Tool Definition (JSON - will validate syntax)
   - Uses Global (tag input - type and press Enter)
4. **Save your changes**:
   - **"Save (Overwrite)"** - Updates existing record, same version
   - **"Save as New Version"** - Creates new record, version increments (e.g., "1.0.0" → "1.1.0")

#### Version Increment Rules
- `1.0.0` → `1.1.0` (minor version +1)
- `2.5.0` → `2.6.0`
- `1.0` → `1.1`
- Non-standard versions: Appends `.1`

## Database Connection
Uses existing Aegis database connection from `.env`:
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DATABASE`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

## Technical Details

### Files
- `scripts/prompt_editor.py` - Flask application
- `scripts/templates/prompt_editor.html` - Web interface

### API Endpoints
- `GET /` - Main page
- `GET /api/prompts` - Get all prompts
- `GET /api/prompt/<id>` - Get single prompt
- `PUT /api/prompt/<id>` - Update existing (overwrite)
- `POST /api/prompt/<id>/new-version` - Create new version

### Dependencies
- Flask (web framework)
- SQLAlchemy (database access)
- Bootstrap 5 (UI styling)
- Font Awesome (icons)

All dependencies already in `requirements.txt` for Aegis.

## Tips
- **JSON Validation**: Tool Definition field will validate JSON syntax before saving
- **Tags Input**: For "Uses Global" field, type each value and press Enter to add as tag
- **Read-Only Fields**: Version and Updated At are read-only (auto-managed)
- **New Version**: Creates entirely new record, doesn't modify original
- **Search**: Searches across Name and Description fields

## Stopping the Editor
Press `Ctrl+C` in the terminal where script is running.

## Troubleshooting

### Can't connect to database
- Check `.env` file has correct database credentials
- Verify PostgreSQL is running
- Ensure you're in the `aegis/` directory when running script

### Port 5001 already in use
Edit `prompt_editor.py` and change port number:
```python
app.run(host="0.0.0.0", port=5002, debug=True)  # Changed to 5002
```

### JSON validation error
- Check Tool Definition field for valid JSON syntax
- Use [jsonlint.com](https://jsonlint.com) to validate JSON structure
