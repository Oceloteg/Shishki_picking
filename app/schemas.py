from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class LoginRequest(BaseModel):
    password: str = Field(min_length=1)


class OrderLineOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    item_id: str
    item_name: str
    unit: str | None = None
    qty_ordered: float
    qty_collected: float
    # Diff/highlight helpers (screen 2)
    baseline_qty_ordered: float | None = None
    is_added: bool = False
    is_removed: bool = False


class OrderOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    onec_id: str
    number: str | None = None
    customer_name: str | None = None
    created_at: datetime | None = None
    ship_deadline: datetime | None = None
    comment: str | None = None
    onec_status: str | None = None
    is_posted: bool

    # Calculated
    total_lines: int
    lines_done: int
    # Quantity-based progress (requested): sum(qty_collected) / sum(qty_ordered)
    total_qty: float = 0.0
    collected_qty: float = 0.0
    progress_pct: float
    column: str  # not_started|picking|picked

    # For UI hints
    urgency: str | None = None  # overdue|due_soon|stale|None
    urgency_text: str | None = None


class OrderDetailOut(BaseModel):
    order: OrderOut
    lines: list[OrderLineOut]


class PatchLineRequest(BaseModel):
    qty_collected: float


class PatchLineResponse(BaseModel):
    line: OrderLineOut
    order: OrderOut
    order_completed_now: bool = False


class ConfigOut(BaseModel):
    due_soon_hours: int
    stale_hours: int
    status_picking: str
    status_picked: str
    status_in_work: str
    status_shipped: str
    status_finished: str
    active_statuses: list[str]


class MeOut(BaseModel):
    ok: bool = True
    user: str = "picker"
