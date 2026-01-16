from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from app.auth import create_token, get_current_user
from app.config import settings
from app.db import SessionLocal, engine, get_db
from app.models import Base, Order, OrderLine
from app.schemas import (
    ConfigOut,
    LoginRequest,
    MeOut,
    OrderDetailOut,
    PatchLineRequest,
    PatchLineResponse,
)
from app.migrations import ensure_schema
from app.services.orders_service import calc_progress, to_order_out
from app.services.sync_service import (
    enqueue_line_progress,
    enqueue_set_status,
    process_sync_queue,
    sync_loop,
    sync_orders_from_onec,
)

logger = logging.getLogger("shishki.app")


def _norm(s: str | None) -> str:
    return (s or "").strip().casefold()

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create DB tables
    Base.metadata.create_all(bind=engine)
    changes = ensure_schema(engine)
    if changes:
        logger.info("db schema ensured: %s", changes)

    # Log effective configuration (without secrets)
    try:
        logger.setLevel(getattr(logging, (settings.app_log_level or "INFO").upper(), logging.INFO))
    except Exception:
        pass

    logger.info(
        "startup: debug=%s db=%s onec_mode=%s onec_base_url=%s",
        settings.app_debug,
        settings.database_url,
        settings.onec_mode,
        settings.onec_base_url,
    )
    logger.info(
        "1C entities: orders=%s lines=%s statuses=%s customers=%s items=%s units=%s",
        settings.onec_entity_orders,
        settings.onec_entity_order_lines,
        settings.onec_entity_statuses,
        settings.onec_entity_customers,
        settings.onec_entity_items,
        settings.onec_entity_units,
    )
    logger.info(
        "1C settings: active_statuses=%s orderby=%s top=%s timeout=%ss verify_tls=%s",
        settings.active_statuses_list(),
        settings.onec_orders_orderby,
        settings.onec_orders_top,
        settings.onec_timeout_seconds,
        settings.onec_verify_tls,
    )

    if (settings.onec_mode or "").strip().lower() != "odata" and settings.onec_base_url:
        logger.warning(
            "ONEC_MODE is not 'odata' (current=%s) but ONEC_BASE_URL is set. "
            "The app will use mock data until ONEC_MODE=odata.",
            settings.onec_mode,
        )

    # Start background sync loop
    stop_event = asyncio.Event()
    task = asyncio.create_task(sync_loop(stop_event, SessionLocal))

    app.state.stop_event = stop_event
    app.state.sync_task = task
    yield

    stop_event.set()
    try:
        await task
    except Exception:
        pass


app = FastAPI(title="Шишки — сборка заказов", lifespan=lifespan)

# Static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))

@app.get("/favicon.ico")
def favicon():
    return FileResponse(str(STATIC_DIR / "sibbrend.ico"), media_type="image/x-icon")

@app.get("/api/config", response_model=ConfigOut)
def get_config(_: str = Depends(get_current_user)):
    return ConfigOut(
        due_soon_hours=settings.due_soon_hours,
        stale_hours=settings.stale_hours,
        status_picking=settings.onec_status_picking,
        status_picked=settings.onec_status_picked,
        status_in_work=settings.onec_status_in_work,
        status_shipped=settings.onec_status_shipped,
        status_finished=settings.onec_status_finished,
        active_statuses=settings.active_statuses_list(),
    )


@app.get("/api/me", response_model=MeOut)
def me(_: str = Depends(get_current_user)):
    return MeOut()


@app.post("/api/login")
def login(body: LoginRequest, response: Response):
    if body.password != settings.app_password:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Wrong password")

    token = create_token("picker")
    max_age = settings.token_exp_days * 24 * 60 * 60

    response.set_cookie(
        key=settings.cookie_name,
        value=token,
        httponly=True,
        secure=bool(settings.cookie_secure),
        samesite=settings.cookie_samesite,
        max_age=max_age,
        path="/",
    )
    return {"ok": True}


@app.post("/api/logout")
def logout(response: Response):
    response.delete_cookie(key=settings.cookie_name, path="/")
    return {"ok": True}


@app.get("/api/orders")
def list_orders(
    _: str = Depends(get_current_user),
    db: Session = Depends(get_db),
    limit: int = 200,
):
    limit = max(1, min(limit, 500))
    orders = (
        db.execute(
            select(Order)
            .where(Order.is_active == True)
            .options(selectinload(Order.lines))
            .order_by(Order.id.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )

    if settings.app_debug:
        logger.info("/api/orders: active_orders=%s", len(orders))

    return [
        {"order": to_order_out(o).model_dump(), "lines": [
            {"id": l.id, "item_id": l.item_id, "item_name": l.item_name, "unit": l.unit, "qty_ordered": l.qty_ordered, "qty_collected": l.qty_collected}
            for l in o.lines
        ]}
        for o in orders
    ]


@app.get("/api/orders/{order_id}", response_model=OrderDetailOut)
def get_order(
    order_id: int,
    _: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    order = db.execute(select(Order).where(Order.id == order_id).options(selectinload(Order.lines))).scalar_one_or_none()
    if not order or not order.is_active:
        raise HTTPException(status_code=404, detail="Order not found")

    # Ensure lines loaded
    _ = order.lines
    return OrderDetailOut(
        order=to_order_out(order),
        lines=order.lines,
    )


@app.post("/api/orders/{order_id}/open")
def open_order(
    order_id: int,
    _: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    order = db.execute(select(Order).where(Order.id == order_id).options(selectinload(Order.lines))).scalar_one_or_none()
    if not order or not order.is_active:
        raise HTTPException(status_code=404, detail="Order not found")

    # Set status in 1C to "На сборке" (best effort)
    if _norm(order.onec_status) not in {
        _norm(settings.onec_status_picking),
        _norm(settings.onec_status_picked),
    }:
        order.onec_status = settings.onec_status_picking
        db.commit()
        try:
            enqueue_set_status(db, order.onec_id, settings.onec_status_picking)
        except Exception:
            logger.exception("enqueue_set_status failed (open_order) order_id=%s onec_id=%s", order_id, order.onec_id)

    return {"ok": True}


@app.patch("/api/orders/{order_id}/lines/{line_id}", response_model=PatchLineResponse)
def patch_line(
    order_id: int,
    line_id: int,
    body: PatchLineRequest,
    _: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    line = (
        db.execute(select(OrderLine).where(OrderLine.id == line_id))
        .scalar_one_or_none()
    )
    if not line:
        raise HTTPException(status_code=404, detail="Line not found")

    order = db.execute(select(Order).where(Order.id == line.order_id)).scalar_one()
    if order.id != order_id:
        raise HTTPException(status_code=404, detail="Order not found")
    if not order.is_active:
        raise HTTPException(status_code=404, detail="Order not found")

    # Ensure order in picking status when any interaction happens
    if _norm(order.onec_status) not in {
        _norm(settings.onec_status_picking),
        _norm(settings.onec_status_picked),
    }:
        order.onec_status = settings.onec_status_picking
        db.commit()
        try:
            enqueue_set_status(db, order.onec_id, settings.onec_status_picking)
        except Exception:
            logger.exception("enqueue_set_status failed (patch_line->ensure_picking) order_id=%s line_id=%s onec_id=%s", order_id, line_id, order.onec_id)

    # Was order complete before?
    total_before, done_before, _ = calc_progress(order)
    was_complete = total_before > 0 and done_before == total_before

    # Update with clamp
    new_qty = float(body.qty_collected)
    if new_qty < 0:
        new_qty = 0.0
    if new_qty > (line.qty_ordered or 0.0):
        new_qty = float(line.qty_ordered or 0.0)
    line.qty_collected = new_qty
    db.commit()

    # Enqueue progress sync (optional / depends on 1C)
    enqueue_line_progress(db, order.onec_id, line.onec_line_id, line.item_id, line.qty_collected)

    # Re-load order lines and re-check completion
    order = db.execute(select(Order).where(Order.id == order_id)).scalar_one()
    _ = order.lines
    total_after, done_after, _ = calc_progress(order)
    is_complete = total_after > 0 and done_after == total_after

    completed_now = (not was_complete) and is_complete
    if completed_now:
        # Set status "Собран" (best effort)
        if _norm(order.onec_status) != _norm(settings.onec_status_picked):
            order.onec_status = settings.onec_status_picked
            db.commit()
            try:
                enqueue_set_status(db, order.onec_id, settings.onec_status_picked)
            except Exception:
                logger.exception("enqueue_set_status failed (complete_order) order_id=%s onec_id=%s", order_id, order.onec_id)

    # Prepare response
    line = db.execute(select(OrderLine).where(OrderLine.id == line_id)).scalar_one()
    order = db.execute(select(Order).where(Order.id == order_id)).scalar_one()
    _ = order.lines
    return PatchLineResponse(
        line=line,
        order=to_order_out(order),
        order_completed_now=completed_now,
    )


@app.patch("/api/lines/{line_id}", response_model=PatchLineResponse)
def patch_line_by_id(
    line_id: int,
    body: PatchLineRequest,
    _: str = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Совместимость с более старыми версиями фронта.

    Ранее некоторые версии UI отправляли PATCH на /api/lines/{id}.
    Основной эндпоинт: /api/orders/{order_id}/lines/{line_id}.
    """
    line = db.execute(select(OrderLine).where(OrderLine.id == line_id)).scalar_one_or_none()
    if not line:
        raise HTTPException(status_code=404, detail="Line not found")
    return patch_line(line.order_id, line_id, body, _, db)

@app.post("/api/sync-now")
async def sync_now(_: str = Depends(get_current_user)):
    # Manual button: fetch from 1C + process outbox immediately.
    with SessionLocal() as db:
        res1 = await sync_orders_from_onec(db)
        res2 = await process_sync_queue(db)
        # DB visibility sanity check: how many active orders are available for /api/orders.
        try:
            active_in_db = db.scalar(select(func.count()).select_from(Order).where(Order.is_active == True))
            total_in_db = db.scalar(select(func.count()).select_from(Order))
        except Exception:
            active_in_db = None
            total_in_db = None
        res1["active_in_db"] = active_in_db
        res1["total_in_db"] = total_in_db
        logger.info("sync-now: %s", res1)
        return {"sync": res1, "outbox": res2}


@app.get("/api/debug/db")
def debug_db(_: str = Depends(get_current_user), db: Session = Depends(get_db)):
    """Quick diagnostics endpoint.

    Shows local DB counts and a small sample of recent orders.
    Safe to keep enabled because it requires auth (the same password-based token).
    """

    total_orders = db.scalar(select(func.count()).select_from(Order))
    active_orders = db.scalar(select(func.count()).select_from(Order).where(Order.is_active == True))
    total_lines = db.scalar(select(func.count()).select_from(OrderLine))

    sample_orders = (
        db.execute(select(Order).order_by(Order.id.desc()).limit(10))
        .scalars()
        .all()
    )

    def _sqlite_path(url: str) -> str | None:
        if not url:
            return None
        if url.startswith("sqlite:///"):
            p = url[len("sqlite:///") :]
            try:
                return str(Path(p).resolve())
            except Exception:
                return p
        return None

    return {
        "cwd": str(Path.cwd()),
        "database_url": settings.database_url,
        "database_file": _sqlite_path(settings.database_url),
        "onec_mode": settings.onec_mode,
        "onec_base_url": settings.onec_base_url,
        "active_statuses": settings.active_statuses_list(),
        "counts": {
            "orders_total": total_orders,
            "orders_active": active_orders,
            "lines_total": total_lines,
        },
        "recent_orders": [
            {
                "id": o.id,
                "onec_id": o.onec_id,
                "number": o.number,
                "status": o.onec_status,
                "is_active": o.is_active,
                "is_posted": o.is_posted,
                "created_at": o.created_at.isoformat() if o.created_at else None,
                "ship_deadline": o.ship_deadline.isoformat() if o.ship_deadline else None,
            }
            for o in sample_orders
        ],
    }


@app.get("/api/debug/onec-active")
async def debug_onec_active(_: str = Depends(get_current_user)):
    """Fetch 'active' orders directly from 1C (without touching the local DB)."""
    from app.onec.client import ODataOneCClient, MockOneCClient

    can_use_odata = bool(settings.onec_base_url and settings.onec_username and settings.onec_password)
    using = "odata" if can_use_odata else "mock"
    onec = ODataOneCClient() if can_use_odata else MockOneCClient()

    try:
        orders = await onec.fetch_active_orders()
    finally:
        aclose = getattr(onec, "aclose", None)
        if callable(aclose):
            await aclose()

    return {
        "settings_onec_mode": settings.onec_mode,
        "using": using,
        "fetched": len(orders),
        "sample": [
            {
                "onec_id": o.onec_id,
                "number": o.number,
                "status": o.status,
                "customer": o.customer_name,
                "created_at": o.created_at.isoformat() if o.created_at else None,
                "ship_deadline": o.ship_deadline.isoformat() if o.ship_deadline else None,
                "lines": len(o.lines or []),
            }
            for o in orders[:5]
        ],
    }
