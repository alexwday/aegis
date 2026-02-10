# Hotfix: Dividend Header — Remove QoQ, Fix NaN Display, Fix Layout

**Date:** 2026-02-10
**Scope:** 2 files changed
**Risk:** Low — display-only changes, no database or business logic impact

---

## Summary of Changes

| # | Issue | Fix | File |
|---|-------|-----|------|
| 1 | QoQ not relevant for dividend/share metric | Removed QoQ from dividend header display | `report_template.html` |
| 2 | NaN values display as `— nan%` instead of `—` | Added NaN guard to all delta formatting functions | `supplementary.py` |
| 3 | Up/down indicator renders above the value due to column stacking | Flattened dividend delta to single inline row (consequence of removing QoQ) | `report_template.html` |

---

## Files Changed

### File 1: `src/aegis/etls/bank_earnings_report/retrieval/supplementary.py`

#### Change A — Add `import math` (line 8)

**BEFORE:**
```python
from typing import Any, Dict, List, Optional
from sqlalchemy import text
```

**AFTER:**
```python
import math
from typing import Any, Dict, List, Optional

from sqlalchemy import text
```

**Why:** `math.isnan()` is needed for the NaN guard checks below.

---

#### Change B — NaN guard in `format_dividend_json()` YoY handling (~line 149)

**BEFORE:**
```python
    yoy_value = dividend_data.get("yoy")
    if yoy_value is not None:
```

**AFTER:**
```python
    yoy_value = dividend_data.get("yoy")
    if yoy_value is not None and not (isinstance(yoy_value, float) and math.isnan(yoy_value)):
```

**Why:** When the database returns `NaN` (e.g., 0/0 calculation), Python's `float('nan')` passes the `is not None` check but fails all numeric comparisons, resulting in `"— nan%"` being displayed. This guard treats NaN the same as None → displays `"—"`.

---

#### Change C — NaN guard in `format_delta()` (~line 458)

**BEFORE:**
```python
    if value is None:
        return {"value": 0, "direction": "neutral", "display": "—"}
```

**AFTER:**
```python
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return {"value": 0, "direction": "neutral", "display": "—"}
```

**Why:** `format_delta()` is the shared function used by **all** metric displays across the report — key metrics tiles, segment performance tables, and expandable raw metrics. This single fix prevents `nan%` from appearing anywhere in the report. Without this fix, any metric whose QoQ or YoY delta is NaN in the database would render as `— nan%`.

---

#### Change D — NaN guard in `format_delta_for_llm()` (~line 514)

**BEFORE:**
```python
    if value is None:
        return "—"
```

**AFTER:**
```python
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "—"
```

**Why:** `format_delta_for_llm()` formats delta values for the LLM prompt tables (used during metric selection). A NaN value here would produce `nan%` in the LLM prompt, potentially confusing the model's metric selection. Same fix pattern as `format_delta()`.

---

### File 2: `src/aegis/etls/bank_earnings_report/templates/report_template.html`

#### Change E — Remove QoQ from dividend header, flatten to inline row (~line 572)

**BEFORE:**
```html
                <div style="display: flex; flex-direction: column; gap: 2px; padding-left: 14px; border-left: 1px solid rgba(255,255,255,0.1);">
                    <div style="display: flex; align-items: center; gap: 6px;">
                        <span style="font-size: 8px; color: #94a3b8; text-transform: uppercase; font-weight: 700; width: 24px;">QoQ</span>
                        <span style="font-size: 12px; font-weight: 700;" class="delta-{{ _0_header_dividend.dividend.qoq.direction }}">{{ _0_header_dividend.dividend.qoq.display }}</span>
                    </div>
                    <div style="display: flex; align-items: center; gap: 6px;">
                        <span style="font-size: 8px; color: #94a3b8; text-transform: uppercase; font-weight: 700; width: 24px;">YoY</span>
                        <span style="font-size: 12px; font-weight: 700;" class="delta-{{ _0_header_dividend.dividend.yoy.direction }}">{{ _0_header_dividend.dividend.yoy.display }}</span>
                    </div>
                </div>
```

**AFTER:**
```html
                <div style="display: flex; align-items: center; gap: 6px; padding-left: 14px; border-left: 1px solid rgba(255,255,255,0.1);">
                    <span style="font-size: 8px; color: #94a3b8; text-transform: uppercase; font-weight: 700;">YoY</span>
                    <span style="font-size: 12px; font-weight: 700;" class="delta-{{ _0_header_dividend.dividend.yoy.direction }}">{{ _0_header_dividend.dividend.yoy.display }}</span>
                </div>
```

**Why:** Three issues fixed by this single template change:
1. **QoQ removed** — not meaningful for dividend/share metric (dividends change annually, not quarterly)
2. **Layout fixed** — the old `flex-direction: column` stacked QoQ above YoY in a vertical column, causing the indicator to render above the value when space was tight. Now it's a single horizontal flex row: `[YoY] [▲ 4.8%]`
3. **Space freed** — removing one row eliminates the cramped layout entirely

---

## What Does NOT Change

- The QoQ/YoY display on **key metrics tiles** (Section 1) is unchanged
- The QoQ/YoY display on **segment performance tables** (Section 4) is unchanged
- The raw metrics expandable table still shows both QoQ and YoY columns
- The `format_dividend_json()` Python function still computes and returns QoQ data — the template simply no longer renders it
- No database schema changes
- No new dependencies
