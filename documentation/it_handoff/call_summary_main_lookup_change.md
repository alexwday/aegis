# `/Users/alexwday/Projects/aegis/src/aegis/etls/call_summary/main.py`

## Old Code
```python
def get_bank_info_from_config(bank_identifier: str) -> Dict[str, Any]:
    """
    Look up bank from monitored institutions configuration file.

    Args:
        bank_identifier: Bank ID (as string/int), symbol (e.g., "RY"), or name

    Returns:
        Dictionary with bank_id, bank_name, bank_symbol, bank_type

    Raises:
        ValueError: If bank not found in monitored institutions
    """
    institutions = _load_monitored_institutions()

    # Try lookup by ID
    if bank_identifier.isdigit():
        bank_id = int(bank_identifier)
        if bank_id in institutions:
            inst = institutions[bank_id]
            return {
                "bank_id": inst["id"],
                "bank_name": inst["name"],
                "bank_symbol": inst["symbol"],
                "bank_type": inst["type"],
            }

    # Try lookup by symbol or name
    bank_identifier_upper = bank_identifier.upper()
    bank_identifier_lower = bank_identifier.lower()

    for inst in institutions.values():
        # Match by symbol (case-insensitive)
        if inst["symbol"].upper() == bank_identifier_upper:
            return {
                "bank_id": inst["id"],
                "bank_name": inst["name"],
                "bank_symbol": inst["symbol"],
                "bank_type": inst["type"],
            }

        # Match by name (case-insensitive, partial match)
        if bank_identifier_lower in inst["name"].lower():
            return {
                "bank_id": inst["id"],
                "bank_name": inst["name"],
                "bank_symbol": inst["symbol"],
                "bank_type": inst["type"],
            }

    # Build helpful error message with available banks
    available = [f"{inst['symbol']} ({inst['name']})" for inst in institutions.values()]
    raise ValueError(
        f"Bank '{bank_identifier}' not found in monitored institutions.\n"
        f"Available banks: {', '.join(sorted(available))}"
    )
```

## New Code
```python
def get_bank_info_from_config(bank_identifier: str) -> Dict[str, Any]:
    """
    Look up bank from monitored institutions configuration file.

    Args:
        bank_identifier: Bank ID (as string/int), symbol (e.g., "RY"), or name

    Returns:
        Dictionary with bank_id, bank_name, bank_symbol, bank_type

    Raises:
        ValueError: If bank not found in monitored institutions
    """
    institutions = _load_monitored_institutions()
    bank_identifier = bank_identifier.strip()

    def _to_bank_info(inst: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "bank_id": inst["id"],
            "bank_name": inst["name"],
            "bank_symbol": inst["symbol"],
            "bank_type": inst["type"],
        }

    # Try lookup by ID
    if bank_identifier.isdigit():
        bank_id = int(bank_identifier)
        if bank_id in institutions:
            return _to_bank_info(institutions[bank_id])

    # Try lookup by symbol first (supports full ticker input, e.g. "C-US")
    bank_identifier_upper = bank_identifier.upper()
    bank_identifier_lower = bank_identifier.lower()
    symbol_candidate = bank_identifier_upper.split("-")[0]

    for inst in institutions.values():
        if inst["symbol"].upper() == symbol_candidate:
            return _to_bank_info(inst)

    # Fallback: match by name (case-insensitive, partial match)
    for inst in institutions.values():
        if bank_identifier_lower in inst["name"].lower():
            return _to_bank_info(inst)

    # Build helpful error message with available banks
    available = [f"{inst['symbol']} ({inst['name']})" for inst in institutions.values()]
    raise ValueError(
        f"Bank '{bank_identifier}' not found in monitored institutions.\n"
        f"Available banks: {', '.join(sorted(available))}"
    )
```
