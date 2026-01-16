from __future__ import annotations

"""Minimal, SQLite-friendly migrations.

Why this exists:
- `Base.metadata.create_all()` only creates missing tables; it does NOT add
  missing columns to existing tables.
- Users often keep `app.db` between upgrades, so the app must tolerate schema
  evolution.

Approach:
- Best-effort `ALTER TABLE .. ADD COLUMN ..` for missing columns.
- Designed primarily for SQLite (the default), but also works on other DBs
  that support `ADD COLUMN`.

If you make breaking changes (rename / drop columns), prefer a fresh DB or
introduce a proper migration tool (Alembic).
"""

from typing import Dict, List

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


def _ensure_columns(engine: Engine, table: str, columns_ddl: Dict[str, str]) -> List[str]:
    """Ensure the given columns exist in `table`.

    `columns_ddl` maps column_name -> ddl_fragment.

    Example:
        {"new_col": "new_col INTEGER DEFAULT 0"}

    Returns a list of columns that were added.
    """

    insp = inspect(engine)
    table_names = set(insp.get_table_names())
    if table not in table_names:
        return []

    existing_cols = {c["name"] for c in insp.get_columns(table)}
    to_add = [(name, ddl) for name, ddl in columns_ddl.items() if name not in existing_cols]
    if not to_add:
        return []

    added: List[str] = []
    with engine.begin() as conn:
        for name, ddl in to_add:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {ddl}"))
            added.append(name)

    return added


def ensure_schema(engine: Engine) -> dict[str, List[str]]:
    """Patch DB schema to the current app expectations (best effort)."""

    changes: dict[str, List[str]] = {}

    # orders
    added = _ensure_columns(
        engine,
        "orders",
        {
            # 1C picking feature: numeric code (e.g. 0/1/2)
            "onec_pick_status_code": "onec_pick_status_code INTEGER",
            # Baseline composition capture timestamp
            "baseline_captured_at": "baseline_captured_at DATETIME",
        },
    )
    if added:
        changes["orders"] = added

    # order_lines
    added = _ensure_columns(
        engine,
        "order_lines",
        {
            "sort_index": "sort_index INTEGER DEFAULT 0",
            "qty_collected_remote": "qty_collected_remote REAL",
            "baseline_qty_ordered": "baseline_qty_ordered REAL",
            "is_added": "is_added BOOLEAN DEFAULT 0",
            "is_removed": "is_removed BOOLEAN DEFAULT 0",
            "last_seen_at": "last_seen_at DATETIME",
        },
    )
    if added:
        changes["order_lines"] = added

    return changes
