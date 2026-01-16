from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


def utcnow() -> datetime:
    """Timezone-aware UTC now.

    Why:
    - Our OData datetimes come with timezone offsets.
    - SQLite may lose tzinfo; we still prefer to store aware datetimes when
      we create/update local timestamps.
    """
    return datetime.now(timezone.utc)


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    onec_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)

    number: Mapped[str | None] = mapped_column(String(64), nullable=True)
    customer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)

    created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ship_deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    comment: Mapped[str | None] = mapped_column(Text, nullable=True)

    onec_status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    onec_pick_status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_posted: Mapped[bool] = mapped_column(Boolean, default=False)

    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)

    baseline_captured_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    lines: Mapped[list["OrderLine"]] = relationship(
        back_populates="order", cascade="all, delete-orphan"
    )


class OrderLine(Base):
    __tablename__ = "order_lines"
    __table_args__ = (
        UniqueConstraint("order_id", "line_key", name="uq_order_line_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(Integer, ForeignKey("orders.id", ondelete="CASCADE"))

    # Stable key for upsert:
    # - prefer 1C row id if exists,
    # - else build from item_id + row index.
    line_key: Mapped[str] = mapped_column(String(128))

    onec_line_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    item_id: Mapped[str] = mapped_column(String(64))
    item_name: Mapped[str] = mapped_column(String(255))
    unit: Mapped[str | None] = mapped_column(String(32), nullable=True)

    qty_ordered: Mapped[float] = mapped_column(Float, default=0)
    qty_collected: Mapped[float] = mapped_column(Float, default=0)
    qty_collected_remote: Mapped[float | None] = mapped_column(Float, nullable=True)

    sort_index: Mapped[int] = mapped_column(Integer, default=0)
    baseline_qty_ordered: Mapped[float | None] = mapped_column(Float, nullable=True)
    is_added: Mapped[bool] = mapped_column(Boolean, default=False)
    is_removed: Mapped[bool] = mapped_column(Boolean, default=False)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    order: Mapped["Order"] = relationship(back_populates="lines")


class SyncQueue(Base):
    __tablename__ = "sync_queue"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    action_type: Mapped[str] = mapped_column(String(64), index=True)
    payload_json: Mapped[str] = mapped_column(Text)

    status: Mapped[str] = mapped_column(String(16), default="pending", index=True)  # pending|done|failed
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
