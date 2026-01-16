from __future__ import annotations

from datetime import date, datetime, timezone
from math import ceil
from typing import Iterable

from app.config import settings
from app.models import Order
from app.schemas import OrderOut


EPS = 1e-9

def _ensure_aware_utc(dt: datetime) -> datetime:
    """Coerce datetime to timezone-aware UTC.

    SQLite may drop tzinfo even if DateTime(timezone=True) is used.
    This helper makes date arithmetic safe and consistent."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _is_line_done(qty_collected: float, qty_ordered: float) -> bool:
    return qty_collected + EPS >= qty_ordered


def _active_lines(order: Order):
    # Lines removed in 1C should NOT block completion; they are shown only for history.
    return [l for l in order.lines if not getattr(l, "is_removed", False)]


def calc_progress(order: Order) -> tuple[int, int, float, float, float]:
    """Return progress as:

    (total_lines, lines_done, total_qty, collected_qty, pct)

    Where qty is the sum of quantities across active lines.
    """

    lines = _active_lines(order)
    total_lines = len(lines)
    if total_lines == 0:
        return 0, 0, 0.0, 0.0, 0.0

    # Count lines done
    lines_done = sum(1 for l in lines if _is_line_done(l.qty_collected or 0.0, l.qty_ordered or 0.0))

    # Quantity-based progress
    total_qty = float(sum((l.qty_ordered or 0.0) for l in lines))
    collected_qty = float(
        sum(
            min((l.qty_collected or 0.0), (l.qty_ordered or 0.0))
            for l in lines
        )
    )
    if total_qty <= EPS:
        pct = 0.0
    else:
        pct = round((collected_qty / total_qty) * 100.0, 1)

    return total_lines, lines_done, total_qty, collected_qty, pct


def determine_column(order: Order) -> str:
    # Column logic:
    # - picked: all lines done OR status == "Собран"
    # - picking: status == "На сборке" OR some progress started
    # - not_started: otherwise
    status_cf = (order.onec_status or "").casefold()
    picked_cf = (settings.onec_status_picked or "").casefold()
    picking_cf = (settings.onec_status_picking or "").casefold()

    if status_cf and status_cf == picked_cf:
        return "picked"

    total_lines, lines_done, total_qty, collected_qty, _ = calc_progress(order)
    if total_qty > EPS and collected_qty + EPS >= total_qty:
        return "picked"

    has_any_progress = any((l.qty_collected or 0.0) > EPS for l in _active_lines(order))
    if (status_cf and status_cf == picking_cf) or has_any_progress:
        return "picking"

    return "not_started"


def determine_urgency(order: Order) -> tuple[str | None, str | None]:
    """Return (urgency_code, urgency_text).

    Rules (as requested):
    - Deadline text is shown only for: today / tomorrow / overdue.
    - All computations are day-based (ignore time-of-day).
    - "Висит Nд" remains for stale orders (rounded down to full days).
    """

    now = datetime.now(timezone.utc)
    today = date.today()

    ship_deadline = order.ship_deadline
    created_at = _ensure_aware_utc(order.created_at) if order.created_at else None

    # Deadline-based urgency (day-level)
    if ship_deadline:
        ddl_day = ship_deadline.date()  # IMPORTANT: do NOT convert tz; keep local business date
        days_to = (ddl_day - today).days
        if days_to < 0:
            # overdue
            return "overdue", f"Дедлайн просрочен на {abs(days_to)}д"
        if days_to == 0:
            return "due_soon", "Дедлайн сегодня"
        if days_to == 1:
            return "due_soon", "Дедлайн завтра"

    # Stale-based urgency (only if not picked)
    col = determine_column(order)
    if col != "picked" and created_at:
        age_days = int((now - created_at).total_seconds() // 86400)
        threshold_days = max(1, ceil(settings.stale_hours / 24))
        if age_days >= threshold_days:
            return "stale", f"Висит {age_days}д"

    return None, None


def sort_orders_for_board(orders: Iterable[Order]) -> list[Order]:
    """Sort orders by:
    1) ship deadline day (if present) ascending
    2) created_at ascending (older first)

    Orders without deadlines are placed after all orders with deadlines.
    """

    def _key(o: Order):
        ddl = o.ship_deadline.date() if o.ship_deadline else None
        has_ddl = 0 if ddl else 1
        # created_at may be None; sort them last within their group
        ca = o.created_at or datetime.max.replace(tzinfo=timezone.utc)
        ca_utc = _ensure_aware_utc(ca) if isinstance(ca, datetime) else ca
        ddl_sort = ddl or date.max
        return (has_ddl, ddl_sort, ca_utc)

    return sorted(list(orders), key=_key)


def to_order_out(order: Order) -> OrderOut:
    total_lines, lines_done, total_qty, collected_qty, pct = calc_progress(order)
    col = determine_column(order)
    urg, urg_text = determine_urgency(order)

    return OrderOut(
        id=order.id,
        onec_id=order.onec_id,
        number=order.number,
        customer_name=order.customer_name,
        created_at=order.created_at,
        ship_deadline=order.ship_deadline,
        comment=order.comment,
        onec_status=order.onec_status,
        is_posted=order.is_posted,
        total_lines=total_lines,
        lines_done=lines_done,
        total_qty=total_qty,
        collected_qty=collected_qty,
        progress_pct=pct,
        column=col,
        urgency=urg,
        urgency_text=urg_text,
    )
