from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.config import settings
from app.models import Order, OrderLine, SyncQueue
from app.onec.client import OneCClientBase, OneCOrder, build_onec_client

EPS = 1e-9
LOG_THROTTLE_SECONDS = 60

logger = logging.getLogger("shishki.sync")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def sync_orders_from_onec(db: Session) -> dict[str, Any]:
    """Fetch active orders from 1C and upsert into local DB."""
    onec = build_onec_client()
    try:
        onec_orders = await onec.fetch_active_orders()
    finally:
        # OData client has aclose; mock does not.
        aclose = getattr(onec, "aclose", None)
        if callable(aclose):
            await aclose()

    seen_onec_ids: set[str] = set()
    upserted = 0
    for o in onec_orders:
        seen_onec_ids.add(o.onec_id)
        upserted += _upsert_onec_order(db, o)

    # Mark any previously active orders as inactive if they were not returned.
    # This covers:
    # - orders that became posted
    # - orders that changed status to shipped/finished
    # - orders removed from the active filter
    stmt = update(Order).where(Order.is_active == True)
    if seen_onec_ids:
        stmt = stmt.where(Order.onec_id.notin_(seen_onec_ids))
    db.execute(stmt.values(is_active=False, updated_at=_utcnow()))

    db.commit()
    return {"upserted": upserted, "count": len(onec_orders)}


def _upsert_onec_order(db: Session, o: OneCOrder) -> int:
    existing = db.execute(select(Order).where(Order.onec_id == o.onec_id)).scalar_one_or_none()
    now = _utcnow()

    if existing is None:
        existing = Order(onec_id=o.onec_id)
        db.add(existing)

    existing.number = o.number
    existing.customer_name = o.customer_name
    existing.created_at = o.created_at
    existing.ship_deadline = o.ship_deadline
    existing.comment = o.comment
    existing.onec_status = o.status
    existing.is_posted = bool(o.is_posted)

    # Active filtering:
    # The 1C client already returns only "active" orders (by configured statuses) and only unposted ones.
    # We keep a small safety guard here for shipped/finished and posted.
    status_cf = (existing.onec_status or "").casefold()
    shipped_cf = (settings.onec_status_shipped or "").casefold()
    finished_cf = (settings.onec_status_finished or "").casefold()

    existing.is_active = (not existing.is_posted) and (status_cf not in {shipped_cf, finished_cf})

    existing.last_synced_at = now
    existing.updated_at = now

    db.flush()  # to get existing.id for FK

    # Upsert lines
    _upsert_onec_lines(db, existing, o.lines, now)

    return 1


def _upsert_onec_lines(db: Session, order: Order, lines: list[Any], now: datetime) -> None:
    # Build incoming keys
    incoming: dict[str, Any] = {}
    for idx, l in enumerate(lines):
        line_key = l.onec_line_id or f"{l.item_id}:{idx}"
        sort_index = idx
        if l.onec_line_id is not None:
            try:
                sort_index = int(str(l.onec_line_id))
            except Exception:
                sort_index = idx
        incoming[line_key] = (sort_index, l)

    existing_lines = db.execute(select(OrderLine).where(OrderLine.order_id == order.id)).scalars().all()
    existing_by_key = {l.line_key: l for l in existing_lines}

    baseline_mode = order.baseline_captured_at is None
    if baseline_mode:
        order.baseline_captured_at = now

    # Upsert / update
    for line_key, (_, l) in incoming.items():
        row = existing_by_key.get(line_key)
        created_now = False
        if row is None:
            row = OrderLine(order_id=order.id, line_key=line_key)
            row.qty_collected = 0.0
            row.is_removed = False
            db.add(row)
            created_now = True

        row.onec_line_id = l.onec_line_id
        row.item_id = str(l.item_id)
        row.item_name = str(l.item_name)
        row.unit = l.unit
        row.qty_ordered = float(l.qty_ordered or 0.0)
        row.sort_index = incoming[line_key][0]
        row.last_seen_at = now

        if created_now:
            if baseline_mode:
                row.baseline_qty_ordered = float(l.qty_ordered or 0.0)
                row.is_added = False
            else:
                row.baseline_qty_ordered = 0.0
                row.is_added = True
        elif baseline_mode and row.baseline_qty_ordered is None:
            row.baseline_qty_ordered = float(l.qty_ordered or 0.0)

        if row.is_removed:
            row.is_removed = False

        # If 1C already has picking progress enabled, import it only for brand-new lines (best effort).
        if created_now:
            remote = getattr(l, "qty_collected_remote", None)
            if remote is not None:
                try:
                    row.qty_collected = float(remote)
                except Exception:
                    pass

        # Clamp collected to new ordered qty if ordered decreased
        if row.qty_collected is None:
            row.qty_collected = 0.0
        if row.qty_collected > row.qty_ordered + EPS:
            row.qty_collected = row.qty_ordered

    # Remove lines that disappeared from 1C: keep row but mark as removed.
    incoming_keys = set(incoming.keys())
    for row in existing_lines:
        if row.line_key not in incoming_keys:
            row.is_removed = True
            row.last_seen_at = now


def enqueue_set_status(
    db: Session,
    onec_order_id: str,
    status: str,
    pick_status_code: int | None = None,
    **_: object,
) -> None:
    """Пишем в outbox задачу на смену статуса заказа в 1С.

    Совместимость:
    - раньше enqueue_set_status принимал только (db, onec_order_id, status)
    - в более новых версиях main.py может передавать pick_status_code
      (например, для поля "СтатусСборки" в 1С). Поэтому параметр необязательный.
    """
    payload: dict[str, object] = {"onec_order_id": onec_order_id, "status": status}
    if pick_status_code is not None:
        payload["pick_status_code"] = pick_status_code
    db.add(
        SyncQueue(
            action_type="set_status",
            payload_json=json.dumps(payload, ensure_ascii=False),
        )
    )
    db.commit()


def enqueue_line_progress(
    db: Session, onec_order_id: str, onec_line_id: str | None, item_id: str, qty_collected: float
) -> None:
    payload = {
        "onec_order_id": onec_order_id,
        "onec_line_id": onec_line_id,
        "item_id": item_id,
        "qty_collected": qty_collected,
    }
    db.add(SyncQueue(action_type="line_progress", payload_json=json.dumps(payload, ensure_ascii=False)))
    db.commit()


async def process_sync_queue(db: Session, limit: int = 25) -> dict[str, Any]:
    onec = build_onec_client()
    try:
        now = _utcnow()
        q = (
            select(SyncQueue)
            .where(SyncQueue.status == "pending")
            .where((SyncQueue.next_attempt_at.is_(None)) | (SyncQueue.next_attempt_at <= now))
            .order_by(SyncQueue.id.asc())
            .limit(limit)
        )
        rows = db.execute(q).scalars().all()

        processed = 0
        ok = 0
        for row in rows:
            processed += 1
            try:
                payload = json.loads(row.payload_json)
                if row.action_type == "set_status":
                    onec_order_id = str(payload.get("onec_order_id") or "")
                    status = str(payload.get("status") or "")
                    pick_status_code = payload.get("pick_status_code", None)

                    # Совместимость: set_order_status может быть как (id, status),
                    # так и (id, status, pick_status_code=...).
                    if pick_status_code is None:
                        await onec.set_order_status(onec_order_id, status)
                    else:
                        try:
                            await onec.set_order_status(onec_order_id, status, pick_status_code=pick_status_code)  # type: ignore[arg-type]
                        except TypeError:
                            # fallback: без pick_status_code / или позиционно
                            try:
                                await onec.set_order_status(onec_order_id, status, pick_status_code)  # type: ignore[misc]
                            except TypeError:
                                await onec.set_order_status(onec_order_id, status)
                elif row.action_type == "line_progress":
                    await onec.write_line_progress(
                        payload["onec_order_id"],
                        payload.get("onec_line_id"),
                        payload.get("item_id", ""),
                        float(payload.get("qty_collected", 0.0)),
                    )
                else:
                    raise RuntimeError(f"Unknown action_type: {row.action_type}")

                row.status = "done"
                row.last_error = None
                ok += 1
            except Exception as e:
                row.attempts += 1
                row.last_error = str(e)
                # Backoff in seconds: 2s → 5s → 15s → 30s → 60s → ...
                backoff_steps = [2, 5, 15, 30, 60, 120, 300]
                idx = min(max(row.attempts - 1, 0), len(backoff_steps) - 1)
                row.next_attempt_at = now + timedelta(seconds=backoff_steps[idx])

                # If too many attempts, mark as failed (kept for inspection)
                if row.attempts >= 10:
                    row.status = "failed"

            row.updated_at = now

        db.commit()
        return {"processed": processed, "ok": ok, "pending_left": None}
    finally:
        aclose = getattr(onec, "aclose", None)
        if callable(aclose):
            await aclose()


async def sync_loop(stop_event: asyncio.Event, session_factory) -> None:
    """Background loop: sync orders + process outbox."""
    last_log_at: datetime | None = None
    suppressed = 0
    while not stop_event.is_set():
        try:
            with session_factory() as db:
                await sync_orders_from_onec(db)
                await process_sync_queue(db)
        except Exception:
            now = _utcnow()
            if last_log_at is None or (now - last_log_at).total_seconds() >= LOG_THROTTLE_SECONDS:
                logger.exception("sync_loop error (suppressed=%s)", suppressed)
                last_log_at = now
                suppressed = 0
            else:
                suppressed += 1

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=settings.sync_interval_seconds)
        except asyncio.TimeoutError:
            continue
