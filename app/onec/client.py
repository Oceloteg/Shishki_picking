from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any, Iterable

import httpx
from dateutil import parser as date_parser

from app.config import settings


@dataclass
class OneCLine:
    item_id: str
    item_name: str
    unit: str | None
    qty_ordered: float
    onec_line_id: str | None = None
    # Optional: if 1C has picking progress fields enabled, we can read them (best effort).
    qty_collected_remote: float | None = None


@dataclass
class OneCOrder:
    onec_id: str
    number: str | None
    customer_name: str | None
    created_at: datetime | None
    ship_deadline: datetime | None
    comment: str | None
    status: str | None
    is_posted: bool
    lines: list[OneCLine]


class OneCClientBase:
    async def fetch_active_orders(self) -> list[OneCOrder]:
        raise NotImplementedError

    async def set_order_status(self, onec_id: str, status: str) -> None:
        raise NotImplementedError

    async def set_order_comment(self, onec_id: str, comment: str) -> None:
        raise NotImplementedError

    async def write_line_progress(
        self, onec_order_id: str, onec_line_id: str | None, item_id: str, qty_collected: float
    ) -> None:
        raise NotImplementedError


class MockOneCClient(OneCClientBase):
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or Path(__file__).with_name("mock_data.json")

    async def fetch_active_orders(self) -> list[OneCOrder]:
        data = json.loads(self.path.read_text(encoding="utf-8"))
        orders: list[OneCOrder] = []
        for raw in data.get("orders", []):
            created = _parse_dt(raw.get("created_at"))
            ship = _parse_dt(raw.get("ship_deadline"))
            lines = [
                OneCLine(
                    item_id=str(l.get("item_id", "")),
                    item_name=str(l.get("item_name", "")),
                    unit=l.get("unit"),
                    qty_ordered=float(l.get("qty_ordered", 0)),
                    onec_line_id=l.get("onec_line_id"),
                    qty_collected_remote=l.get("qty_collected_remote"),
                )
                for l in raw.get("lines", [])
            ]

            orders.append(
                OneCOrder(
                    onec_id=str(raw.get("onec_id", "")),
                    number=raw.get("number"),
                    customer_name=raw.get("customer_name"),
                    created_at=created,
                    ship_deadline=ship,
                    comment=raw.get("comment"),
                    status=raw.get("status"),
                    is_posted=bool(raw.get("is_posted", False)),
                    lines=lines,
                )
            )

        # In mock mode we still apply the "active" filter similar to real mode.
        # IMPORTANT: comparisons must be case-insensitive (see docs/SHISHKI_NOTES.md).
        active_cf = {s.strip().casefold() for s in settings.active_statuses_list() if s and s.strip()}
        shipped_cf = (settings.onec_status_shipped or "").casefold()
        finished_cf = (settings.onec_status_finished or "").casefold()

        filtered: list[OneCOrder] = []
        for o in orders:
            st_cf = (o.status or "").casefold()
            if o.is_posted:
                continue
            if st_cf not in active_cf:
                continue
            if st_cf in {shipped_cf, finished_cf}:
                continue
            filtered.append(o)
        return filtered

    async def set_order_status(self, onec_id: str, status: str) -> None:
        # mock: no-op
        return None

    async def set_order_comment(self, onec_id: str, comment: str) -> None:
        # mock: no-op
        return None

    async def write_line_progress(
        self, onec_order_id: str, onec_line_id: str | None, item_id: str, qty_collected: float
    ) -> None:
        # mock: no-op
        return None


@dataclass
class _FieldGuess:
    """Best-effort inferred field names from live data."""

    # Order fields
    status_field: str | None = None
    # Some 1C OData publications represent references as Edm.String with a
    # companion <FieldName>_Type property (see docs/SHISHKI_NOTES.md).
    # When we detect this for status, we keep both field name and the value
    # so we can write status updates back correctly.
    status_type_field: str | None = None
    status_type_value: str | None = None
    customer_key_field: str | None = None
    ship_deadline_field: str | None = None
    comment_field: str | None = None

    # Line fields
    item_field: str | None = None
    qty_field: str | None = None
    unit_field: str | None = None
    progress_field: str | None = None

    # True when status field is a GUID stored in an Edm.String property
    # (standard OData often exposes references this way, plus a *_Type field).
    status_is_guid_ref: bool = False


class ODataOneCClient(OneCClientBase):
    """1С:Фреш (standard.odata) интеграция через OData.

    Важно:
    - Не полагаемся на точные имена реквизитов заранее (можно переопределить через .env).
    - Статус заказа в разных конфигурациях встречается как:
        * строка (например, "СостояниеЗаказа")
        * ссылка (GUID) на справочник/перечисление (поле с суффиксом _Key)
      Клиент поддерживает оба варианта.
    """

    def __init__(self) -> None:
        if not settings.onec_base_url:
            raise RuntimeError("ONEC_BASE_URL is empty. Set it in .env for ONEC_MODE=odata")

        self.base_url = settings.onec_base_url.strip().rstrip("/") + "/"

        self._status_desc_by_key: dict[str, str] | None = None
        # Casefolded Description -> Ref_Key
        self._status_key_by_desc_cf: dict[str, str] | None = None
        self._field_guess: _FieldGuess | None = None

        self._customer_desc_cache: dict[str, str] = {}
        self._item_desc_cache: dict[str, str] = {}
        self._unit_desc_cache: dict[str, str] = {}

        timeout = httpx.Timeout(float(settings.onec_timeout_seconds))

        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            auth=(settings.onec_username, settings.onec_password),
            verify=bool(settings.onec_verify_tls),
            timeout=timeout,
            headers={"Accept": "application/json"},
        )

    # ------------------------- low-level helpers -------------------------

    def _log(self, msg: str) -> None:
        if settings.onec_http_debug:
            print(f"[1C] {msg}")

    @staticmethod
    def _guid_literal(g: str) -> str:
        return f"guid'{str(g)}'"

    @staticmethod
    def _escape_str(value: str) -> str:
        # OData string literal escaping: single quote is doubled.
        return value.replace("'", "''")

    @staticmethod
    def _extract_items(payload: Any) -> list[dict[str, Any]]:
        """Поддержка разных вариантов JSON у OData 1С."""
        if payload is None:
            return []
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if not isinstance(payload, dict):
            return []

        # OData v4
        if isinstance(payload.get("value"), list):
            return [x for x in payload["value"] if isinstance(x, dict)]

        # OData v2/v3 verbose
        d = payload.get("d")
        if isinstance(d, dict):
            if isinstance(d.get("results"), list):
                return [x for x in d["results"] if isinstance(x, dict)]

        return []

    @staticmethod
    def _extract_single(payload: Any) -> dict[str, Any] | None:
        """Single-entity payload: supports v4 and v2/v3 formats."""
        if not isinstance(payload, dict):
            return None
        if "value" in payload and isinstance(payload["value"], list):
            # sometimes server can return array even for single key
            arr = payload["value"]
            return arr[0] if arr and isinstance(arr[0], dict) else None
        if "d" in payload and isinstance(payload["d"], dict):
            return payload["d"]
        # v4 single entity is a dict with fields (Ref_Key, ...)
        if "Ref_Key" in payload:
            return payload
        return None

    @staticmethod
    def _extract_next(payload: Any) -> str | None:
        if payload is None:
            return None
        if not isinstance(payload, dict):
            return None

        # OData v4
        nxt = payload.get("@odata.nextLink")
        if isinstance(nxt, str) and nxt:
            return nxt

        # OData v2/v3 verbose
        d = payload.get("d")
        if isinstance(d, dict):
            nxt = d.get("__next")
            if isinstance(nxt, str) and nxt:
                return nxt

        return None

    async def _request_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        # IMPORTANT: with httpx base_url, do NOT start url with '/'
        try:
            r = await self.client.request(method, url, params=params, json=json_body, headers=headers)
        except httpx.TimeoutException:
            # Let callers handle timeouts (retry logic lives higher).
            raise
        except httpx.HTTPError as e:
            raise RuntimeError(f"1C request failed: {method} {url}: {e!r}") from e
        except Exception as e:
            raise RuntimeError(f"1C request failed: {method} {url}: {e!r}") from e

        self._log(f"{method} {r.request.url} -> {r.status_code}")

        if settings.onec_http_debug:
            ct = r.headers.get("content-type", "")
            self._log(f"content-type: {ct}")

        if r.status_code >= 400:
            text = None
            try:
                text = r.text
            except Exception:
                text = None
            raise httpx.HTTPStatusError(
                f"1C HTTP {r.status_code} for {method} {r.request.url}: {text[:800] if text else ''}",
                request=r.request,
                response=r,
            )

        # 204 No Content
        if r.status_code == 204:
            return None

        # Parse JSON (force $format=json is used in GET requests)
        try:
            return r.json()
        except Exception as e:
            # Often means auth redirect or server returned HTML/text
            ct = r.headers.get("content-type", "")
            snippet = ""
            try:
                snippet = (r.text or "")[:800]
            except Exception:
                snippet = ""
            raise RuntimeError(
                f"1C returned non-JSON response for {method} {r.request.url}. content-type={ct}. snippet={snippet}"
            ) from e

    async def _paged_get(self, entity_or_url: str, *, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """GET with support for @odata.nextLink / d.__next."""
        items: list[dict[str, Any]] = []
        url = entity_or_url
        first = True

        while True:
            payload = await self._request_json("GET", url, params=params if first else None)
            first = False
            batch = self._extract_items(payload)
            items.extend(batch)
            nxt = self._extract_next(payload)
            if not nxt:
                break

            # nextLink can be absolute; httpx client supports absolute URLs in request()
            url = nxt
            params = None

        return items

    # ------------------------- catalog helpers -------------------------

    async def _load_status_catalog(self) -> None:
        if self._status_desc_by_key is not None:
            return

        self._status_desc_by_key = {}
        self._status_key_by_desc_cf = {}

        if not settings.onec_entity_statuses:
            return

        params = {"$format": "json", "$top": "1000", "$select": "Ref_Key,Description"}
        try:
            rows = await self._paged_get(settings.onec_entity_statuses, params=params)
        except Exception as e:
            self._log(f"Failed to load status catalog {settings.onec_entity_statuses}: {e}")
            return

        for r in rows:
            k = r.get("Ref_Key")
            d = r.get("Description")
            if k is None or d is None:
                continue
            key = str(k)
            desc = str(d)
            self._status_desc_by_key[key] = desc
            self._status_key_by_desc_cf[desc.casefold()] = key

    async def _get_catalog_description_cached(self, entity_set: str, guid: str, cache: dict[str, str]) -> str | None:
        g = str(guid)
        if g in cache:
            return cache[g]

        # Skip obviously non-GUID values
        if len(g) < 32:
            return None

        url = f"{entity_set}({self._guid_literal(g)})"
        params = {"$format": "json", "$select": "Ref_Key,Description"}
        payload = await self._request_json("GET", url, params=params)
        obj = self._extract_single(payload)
        if not isinstance(obj, dict):
            return None
        desc = obj.get("Description")
        if desc is None:
            return None
        cache[g] = str(desc)
        return cache[g]

    async def _warmup_descriptions_batched(
        self,
        *,
        entity_set: str,
        keys: Iterable[str],
        cache: dict[str, str],
        batch_size: int = 40,
    ) -> None:
        """Fetch Description for many GUIDs with fewer requests (OR-filter batches)."""
        uniq = [k for k in set(keys) if k and k not in cache]
        if not uniq:
            return

        # Chunk GUIDs to keep URL length reasonable
        def chunks(xs: list[str], n: int) -> Iterable[list[str]]:
            for i in range(0, len(xs), n):
                yield xs[i : i + n]

        for chunk in chunks(uniq, max(1, batch_size)):
            ors = " or ".join([f"Ref_Key eq {self._guid_literal(k)}" for k in chunk])
            params = {"$format": "json", "$select": "Ref_Key,Description", "$filter": ors}
            try:
                rows = await self._paged_get(entity_set, params=params)
                for r in rows:
                    rk = r.get("Ref_Key")
                    desc = r.get("Description")
                    if rk is not None and desc is not None:
                        cache[str(rk)] = str(desc)
            except Exception as e:
                # Fallback to per-key requests for this chunk
                self._log(f"Batch description fetch failed ({entity_set}): {e}. Falling back to per-key.")
                sem = asyncio.Semaphore(max(1, int(settings.onec_concurrency)))

                async def fetch_one(k: str) -> None:
                    async with sem:
                        try:
                            await self._get_catalog_description_cached(entity_set, k, cache)
                        except Exception as ee:
                            self._log(f"Failed to load description {entity_set}({k}): {ee}")

                await asyncio.gather(*[fetch_one(k) for k in chunk])

    # ------------------------- field detection -------------------------

    async def _ensure_field_guess(self) -> _FieldGuess:
        if self._field_guess is not None:
            return self._field_guess

        guess = _FieldGuess(
            status_field=(settings.onec_order_status_key_field or None),
            customer_key_field=(settings.onec_order_customer_key_field or None),
            ship_deadline_field=(settings.onec_order_ship_deadline_field or None),
            comment_field=(settings.onec_order_comment_field or None),
            item_field=(settings.onec_line_item_key_field or None),
            qty_field=(settings.onec_line_qty_field or None),
            unit_field=(settings.onec_line_unit_field or None),
            progress_field=(settings.onec_line_progress_field or None),
        )

        # Strategy: find ANY order id with a very cheap query (no $orderby),
        # then fetch that single order by key to inspect available fields.
        order_id: str | None = None
        try:
            params = {
                "$format": "json",
                "$top": "1",
                "$select": "Ref_Key",
                "$filter": "Posted eq false and DeletionMark eq false",
            }
            rows = await self._paged_get(settings.onec_entity_orders, params=params)
            if rows and isinstance(rows[0], dict) and rows[0].get("Ref_Key"):
                order_id = str(rows[0]["Ref_Key"])
        except Exception:
            order_id = None

        order_obj: dict[str, Any] | None = None
        if order_id:
            try:
                payload = await self._request_json(
                    "GET",
                    f"{settings.onec_entity_orders}({self._guid_literal(order_id)})",
                    params={"$format": "json"},
                )
                order_obj = self._extract_single(payload)
            except Exception:
                order_obj = None

        if isinstance(order_obj, dict):
            keys = set(order_obj.keys())

            # Status field:
            # In some configurations there are TWO different fields related to "status":
            #   - Order state (for kanban): usually "СостояниеЗаказа" (reference to Catalog_СостоянияЗаказовПокупателей)
            #   - Picking subsystem state: often "СтатусСборки" (numeric code)
            # For the application logic we MUST use the order state field.

            def _type_matches_status_catalog(field: str) -> bool:
                tf = f"{field}_Type"
                tv = order_obj.get(tf)
                if isinstance(tv, str) and tv:
                    # Typically: "StandardODATA.Catalog_СостоянияЗаказовПокупателей"
                    return settings.onec_entity_statuses in tv
                return False

            active_cf = {s.casefold() for s in settings.active_statuses_list() if str(s).strip()}

            if guess.status_field and guess.status_field.casefold() == "статуссборки":
                # Never treat picking subsystem status as order state.
                guess.status_field = None

            # Build candidate list: configured -> common name -> any status-like fields
            candidates: list[str] = []
            if guess.status_field:
                candidates.append(guess.status_field)
            if "СостояниеЗаказа" in keys and "СостояниеЗаказа" not in candidates:
                candidates.append("СостояниеЗаказа")
            for k in sorted(keys):
                lk = k.lower()
                if ("состояни" in lk or "статус" in lk) and not k.endswith("_Type") and k not in candidates:
                    candidates.append(k)

            def _score_status_field(field: str) -> int:
                score = 0
                if field == (settings.onec_order_status_key_field or ""):
                    score += 20
                if field == "СостояниеЗаказа":
                    score += 40
                if field.endswith("_Key"):
                    score += 80
                if _type_matches_status_catalog(field):
                    score += 200

                v = order_obj.get(field)
                if isinstance(v, str):
                    vv = v.strip()
                    if _looks_like_guid(vv):
                        score += 70
                    if vv.casefold() in active_cf:
                        score += 50
                elif isinstance(v, (int, float, bool)):
                    # Numeric / bool fields like "СтатусСборки" are not the order state.
                    score -= 100

                if "сборк" in field.lower():
                    score -= 30
                if field.casefold() == "статуссборки":
                    score -= 500
                return score

            # Pick the best candidate that exists in the payload.
            best_field: str | None = None
            best_score = -10**9
            for c in candidates:
                if c in keys:
                    sc = _score_status_field(c)
                    if sc > best_score:
                        best_score = sc
                        best_field = c

            if best_field:
                if best_field == "СтатусСборки" and "СостояниеЗаказа" in keys:
                    best_field = "СостояниеЗаказа"
                if settings.app_debug and settings.onec_order_status_key_field and best_field != settings.onec_order_status_key_field:
                    # This is a common misconfiguration when picking subsystem fields appear.
                    print(
                        f"[1C] AUTO status field override: configured={settings.onec_order_status_key_field!r} -> detected={best_field!r}"
                    )
                guess.status_field = best_field
            else:
                guess.status_field = None

            # Detect reference pattern for chosen status field.
            guess.status_is_guid_ref = False
            guess.status_type_field = None
            guess.status_type_value = None

            if guess.status_field and not guess.status_field.endswith("_Key"):
                raw_status = order_obj.get(guess.status_field)
                if isinstance(raw_status, str) and _looks_like_guid(raw_status.strip()):
                    guess.status_is_guid_ref = True
                type_field = f"{guess.status_field}_Type"
                if type_field in keys:
                    guess.status_type_field = type_field
                    t = order_obj.get(type_field)
                    if isinstance(t, str) and t:
                        guess.status_type_value = t
                        guess.status_is_guid_ref = True

            if guess.customer_key_field and guess.customer_key_field not in keys:
                guess.customer_key_field = None
            if not guess.customer_key_field:
                cand = _guess_key_field(order_obj, ("Контраг", "Покупател", "Клиент", "Партнер"))
                if cand:
                    guess.customer_key_field = cand

            if guess.ship_deadline_field and guess.ship_deadline_field not in keys:
                guess.ship_deadline_field = None
            if not guess.ship_deadline_field:
                cand = _guess_dt_field(order_obj, ("Отгруз", "Дедлайн", "Доставк"))
                if cand:
                    guess.ship_deadline_field = cand

            if guess.comment_field and guess.comment_field not in keys:
                guess.comment_field = None
            if not guess.comment_field:
                cand = _guess_comment_field(order_obj)
                if cand:
                    guess.comment_field = cand

        # Probe one line for this order (best effort)
        if order_id:
            try:
                params = {
                    "$format": "json",
                    "$top": "1",
                    "$filter": f"Ref_Key eq {self._guid_literal(order_id)}",
                    "$orderby": "LineNumber asc",
                }
                rows = await self._paged_get(settings.onec_entity_order_lines, params=params)
                if rows and isinstance(rows[0], dict):
                    sample_line = rows[0]
                    line_keys = set(sample_line.keys())

                    if guess.item_field and guess.item_field not in line_keys:
                        guess.item_field = None
                    if not guess.item_field:
                        # Prefer "Номенклатура" if present, else any field containing "номенклат"
                        for k in line_keys:
                            if k == "Номенклатура":
                                guess.item_field = k
                                break
                        if not guess.item_field:
                            for k in line_keys:
                                if "номенклат" in k.lower():
                                    guess.item_field = k
                                    break

                    if guess.qty_field and guess.qty_field not in line_keys:
                        guess.qty_field = None
                    if not guess.qty_field:
                        for k in line_keys:
                            if k == "Количество":
                                guess.qty_field = k
                                break
                        if not guess.qty_field:
                            for k in line_keys:
                                if "колич" in k.lower():
                                    guess.qty_field = k
                                    break

                    if guess.progress_field and guess.progress_field not in line_keys:
                        guess.progress_field = None
                    if not guess.progress_field:
                        # Prefer explicit "КоличествоСобрано"
                        for k in line_keys:
                            if k == "КоличествоСобрано":
                                guess.progress_field = k
                                break
                        if not guess.progress_field:
                            for k in line_keys:
                                if "собран" in k.lower():
                                    guess.progress_field = k
                                    break

                    if guess.unit_field and guess.unit_field not in line_keys:
                        guess.unit_field = None
                    if not guess.unit_field:
                        cand = _guess_unit_field(sample_line)
                        if cand:
                            guess.unit_field = cand
            except Exception:
                pass

        self._field_guess = guess
        self._log(
            "Field guess: "
            f"status_field={guess.status_field}, status_type_field={guess.status_type_field}, "
            f"status_is_guid_ref={guess.status_is_guid_ref}, "
            f"customer_key={guess.customer_key_field}, "
            f"ship_deadline={guess.ship_deadline_field}, comment={guess.comment_field}, "
            f"line_item={guess.item_field}, line_qty={guess.qty_field}, "
            f"line_unit={guess.unit_field}, line_progress={guess.progress_field}"
        )
        return guess

    # ------------------------- domain API -------------------------

    async def fetch_active_orders(self) -> list[OneCOrder]:
        fg = await self._ensure_field_guess()

        active_statuses = [s.strip() for s in settings.active_statuses_list() if s.strip()]
        if not active_statuses:
            # Safe default: if not configured, show nothing (to avoid leaking non-target statuses).
            self._log("Active statuses list is empty; returning 0 orders.")
            return []

        active_status_cf = {s.casefold() for s in active_statuses}

        status_field = fg.status_field
        # Status field can be:
        # - Edm.Guid ("*_Key")
        # - Edm.String holding GUID + "*_Type" (standard OData reference representation)
        # - plain string description
        status_is_key = bool(status_field and status_field.endswith("_Key"))
        status_is_guid_ref = bool(status_field and fg.status_is_guid_ref)
        status_is_keylike = status_is_key or status_is_guid_ref

        # If status is a reference (GUID), try to map status names <-> keys via catalog.
        active_status_keys: set[str] = set()
        hidden_status_keys: set[str] = set()
        if status_is_keylike and status_field:
            await self._load_status_catalog()
            if self._status_key_by_desc_cf:
                for s in active_statuses:
                    k = self._status_key_by_desc_cf.get(s.casefold())
                    if k:
                        active_status_keys.add(k)

                for s in [settings.onec_status_shipped, settings.onec_status_finished]:
                    k = self._status_key_by_desc_cf.get((s or "").casefold())
                    if k:
                        hidden_status_keys.add(k)

        base_filter = "Posted eq false and DeletionMark eq false"

        # Request parameters: keep payload small. We'll filter by status in Python.
        base_params: dict[str, Any] = {
            "$format": "json",
            "$top": str(int(settings.onec_orders_top)),
            "$filter": base_filter,
        }
        if settings.onec_orders_orderby and settings.onec_orders_orderby.strip():
            base_params["$orderby"] = settings.onec_orders_orderby.strip()

        select_fields = ["Ref_Key", "Number", "Date", "Posted", "DeletionMark"]
        for opt in [status_field, fg.customer_key_field, fg.ship_deadline_field, fg.comment_field]:
            if opt and opt not in select_fields:
                select_fields.append(opt)
        base_params["$select"] = ",".join(select_fields)

        def build_variants(p: dict[str, Any]) -> list[dict[str, Any]]:
            variants: list[dict[str, Any]] = []
            seen: set[str] = set()

            def add_variant(pp: dict[str, Any]) -> None:
                key = json.dumps(pp, ensure_ascii=False, sort_keys=True)
                if key not in seen:
                    seen.add(key)
                    variants.append(pp)

            for drop_select in (False, True):
                for drop_orderby in (False, True):
                    pv = dict(p)
                    if drop_select:
                        pv.pop("$select", None)
                    if drop_orderby:
                        pv.pop("$orderby", None)
                    add_variant(pv)

            return variants

        async def run_variants(
            p: dict[str, Any],
        ) -> tuple[list[dict[str, Any]], dict[str, Any] | None, Exception | None]:
            last_err: Exception | None = None
            for pv in build_variants(p):
                try:
                    rows = await self._paged_get(settings.onec_entity_orders, params=pv)
                    return rows, pv, None
                except (httpx.TimeoutException, httpx.HTTPStatusError, RuntimeError) as e:
                    last_err = e
                    self._log(f"Orders request variant failed; retrying. err={e}")
                    continue
            return [], None, last_err

        raw_orders: list[dict[str, Any]] = []
        used_params: dict[str, Any] | None = None

        rows, up, err = await run_variants(base_params)
        used_params = up
        if err is not None:
            raise err
        raw_orders = rows

        if settings.onec_http_debug and used_params:
            self._log(f"Orders query params used: {used_params}")

        # Prepare customer descriptions (batched + cached)
        customer_keys: set[str] = set()
        if fg.customer_key_field and fg.customer_key_field.endswith("_Key"):
            for r in raw_orders:
                if r.get(fg.customer_key_field):
                    customer_keys.add(str(r.get(fg.customer_key_field)))

        if customer_keys:
            await self._warmup_descriptions_batched(
                entity_set=settings.onec_entity_customers,
                keys=customer_keys,
                cache=self._customer_desc_cache,
            )

        # Fetch lines per order concurrently
        sem = asyncio.Semaphore(max(1, int(settings.onec_concurrency)))

        async def build_order(r: dict[str, Any]) -> OneCOrder | None:
            onec_id = str(r.get("Ref_Key") or "").strip()
            if not onec_id:
                return None

            posted = bool(r.get("Posted") or False)
            deletion = bool(r.get("DeletionMark") or False)
            if posted or deletion:
                return None

            # Status: resolve and filter client-side
            raw_status = r.get(status_field) if status_field else None
            raw_status_key = str(raw_status).strip() if raw_status is not None else ""

            status_desc: str | None = None
            if status_field and raw_status is not None:
                if status_is_keylike:
                    await self._load_status_catalog()
                    if self._status_desc_by_key:
                        status_desc = self._status_desc_by_key.get(raw_status_key)
                else:
                    status_desc = str(raw_status).strip()

            # Hide shipped/finished
            shipped_cf = (settings.onec_status_shipped or "").casefold()
            finished_cf = (settings.onec_status_finished or "").casefold()

            if status_desc:
                sd_cf = status_desc.casefold()
                if sd_cf in {shipped_cf, finished_cf}:
                    return None
                if sd_cf not in active_status_cf:
                    return None
            else:
                # No description resolved; try filtering by key if we have it.
                if not raw_status_key:
                    return None
                if hidden_status_keys and raw_status_key in hidden_status_keys:
                    return None
                if active_status_keys and raw_status_key not in active_status_keys:
                    return None
                # Keep a readable fallback for UI
                status_desc = raw_status_key

            number = r.get("Number")
            created_at = _parse_dt(r.get("Date"))
            ship_deadline = _parse_dt(r.get(fg.ship_deadline_field)) if fg.ship_deadline_field else None
            comment = str(r.get(fg.comment_field)) if (fg.comment_field and r.get(fg.comment_field) is not None) else None

            cust_name = None
            if fg.customer_key_field and fg.customer_key_field.endswith("_Key"):
                cust_key = r.get(fg.customer_key_field)
                if cust_key is not None:
                    cust_name = self._customer_desc_cache.get(str(cust_key))

            # Lines
            async with sem:
                lines = await self._fetch_order_lines(onec_id, fg)

            return OneCOrder(
                onec_id=onec_id,
                number=str(number) if number is not None else None,
                customer_name=cust_name,
                created_at=created_at,
                ship_deadline=ship_deadline,
                comment=comment,
                status=status_desc,
                is_posted=False,
                lines=lines,
            )

        built = await asyncio.gather(*[build_order(r) for r in raw_orders])
        return [o for o in built if o is not None]

    async def _fetch_order_lines(self, onec_order_id: str, fg: _FieldGuess) -> list[OneCLine]:
        params: dict[str, Any] = {
            "$format": "json",
            "$filter": f"Ref_Key eq {self._guid_literal(onec_order_id)}",
            "$orderby": "LineNumber asc",
        }

        select_fields = ["Ref_Key", "LineNumber"]
        for opt in [fg.item_field, fg.qty_field, fg.unit_field, fg.progress_field]:
            if opt and opt not in select_fields:
                select_fields.append(opt)
        params["$select"] = ",".join(select_fields)

        try:
            raw_lines = await self._paged_get(settings.onec_entity_order_lines, params=params)
        except httpx.HTTPStatusError as e:
            self._log(f"Lines request failed with $select; retry without $select. err={e}")
            params2 = dict(params)
            params2.pop("$select", None)
            raw_lines = await self._paged_get(settings.onec_entity_order_lines, params=params2)

        # Pre-warm item/unit descriptions for this order to avoid per-line requests.
        item_field = fg.item_field
        unit_field = fg.unit_field

        item_keys: list[str] = []
        unit_keys: list[str] = []

        for l in raw_lines:
            if item_field and l.get(item_field) is not None:
                v = l.get(item_field)
                if item_field.endswith("_Key"):
                    item_keys.append(str(v))
                elif isinstance(v, str) and _looks_like_guid(v):
                    item_keys.append(v)
            if unit_field and l.get(unit_field) is not None:
                uv = l.get(unit_field)
                if unit_field.endswith("_Key"):
                    unit_keys.append(str(uv))
                elif isinstance(uv, str) and _looks_like_guid(uv):
                    unit_keys.append(uv)

        if item_keys:
            await self._warmup_descriptions_batched(
                entity_set=settings.onec_entity_items,
                keys=item_keys,
                cache=self._item_desc_cache,
            )

        if unit_keys and getattr(settings, "onec_entity_units", ""):
            if settings.onec_entity_units.strip():
                await self._warmup_descriptions_batched(
                    entity_set=settings.onec_entity_units,
                    keys=unit_keys,
                    cache=self._unit_desc_cache,
                )

        lines: list[OneCLine] = []

        for l in raw_lines:
            line_no = l.get("LineNumber")
            onec_line_id = str(line_no) if line_no is not None else None

            item_name: str | None = None
            item_id: str | None = None

            if item_field and l.get(item_field) is not None:
                v = l.get(item_field)

                if item_field.endswith("_Key"):
                    # Edm.Guid reference
                    item_id = str(v).strip()
                    item_name = self._item_desc_cache.get(item_id)
                else:
                    # Standard OData often exposes references as Edm.String with GUID payload + *_Type.
                    if isinstance(v, str) and _looks_like_guid(v):
                        item_id = v.strip()
                        item_name = self._item_desc_cache.get(item_id)
                        # Some configurations also duplicate presentation in "Содержание".
                        if not item_name:
                            s = l.get("Содержание")
                            if isinstance(s, str) and s.strip():
                                item_name = s.strip()
                    else:
                        # Presentation string
                        item_name = str(v).strip()
                        if item_name:
                            item_id = sha1(item_name.encode("utf-8")).hexdigest()[:16]
            else:
                # Fallback: try common field name
                if isinstance(l.get("Номенклатура"), str) and l.get("Номенклатура"):
                    item_name = str(l.get("Номенклатура")).strip()
                    item_id = sha1(item_name.encode("utf-8")).hexdigest()[:16]

            if not item_name:
                if item_id and _looks_like_guid(item_id):
                    item_name = f"Номенклатура {str(item_id)[:8]}"
                else:
                    item_name = "(без номенклатуры)"
            if not item_id:
                item_id = onec_line_id or sha1(item_name.encode("utf-8")).hexdigest()[:16]

            qty_ordered = 0.0
            if fg.qty_field and l.get(fg.qty_field) is not None:
                try:
                    qty_ordered = float(l.get(fg.qty_field))
                except Exception:
                    qty_ordered = 0.0

            unit = None
            if fg.unit_field and l.get(fg.unit_field) is not None:
                uv = l.get(fg.unit_field)
                if isinstance(uv, str) and _looks_like_guid(uv):
                    unit = self._unit_desc_cache.get(uv.strip())
                else:
                    unit = str(uv)

            qty_collected_remote: float | None = None
            if fg.progress_field and l.get(fg.progress_field) is not None:
                try:
                    qty_collected_remote = float(l.get(fg.progress_field))
                except Exception:
                    qty_collected_remote = None

            lines.append(
                OneCLine(
                    item_id=item_id,
                    item_name=item_name,
                    unit=unit,
                    qty_ordered=qty_ordered,
                    onec_line_id=onec_line_id,
                    qty_collected_remote=qty_collected_remote,
                )
            )

        return lines

    async def set_order_status(self, onec_id: str, status: str) -> None:
        fg = await self._ensure_field_guess()
        status_field = fg.status_field
        if not status_field:
            raise RuntimeError(
                "Не удалось определить поле статуса заказа в OData. "
                "Укажите ONEC_ORDER_STATUS_KEY_FIELD в .env."
            )

        url = f"{settings.onec_entity_orders}({self._guid_literal(onec_id)})"
        headers = {"If-Match": "*"}
        body: dict[str, Any]

        status_is_keylike = status_field.endswith("_Key") or bool(fg.status_is_guid_ref)

        if status_is_keylike:
            await self._load_status_catalog()
            if not self._status_key_by_desc_cf:
                raise RuntimeError("Не удалось загрузить справочник статусов (Catalog_Состояния...).")
            key = self._status_key_by_desc_cf.get(status.strip().casefold())
            if not key:
                raise RuntimeError(
                    f"Статус '{status}' не найден в {settings.onec_entity_statuses}. "
                    "Проверьте, что в 1С есть элемент состояния с таким именем."
                )
            # For both Edm.Guid (*_Key) and Edm.String GUID references, 1C expects plain GUID string.
            body = {status_field: key}

            # If status is a GUID stored in an Edm.String reference field, include the companion
            # *_Type property as well. Without it some 1C publications silently ignore updates.
            if fg.status_is_guid_ref and fg.status_type_field and fg.status_type_value:
                body[fg.status_type_field] = fg.status_type_value
        else:
            body = {status_field: status}

        # OData v2/v3: MERGE is common; 1C часто принимает PATCH.
        try:
            await self._request_json("PATCH", url, json_body=body, headers=headers, params={"$format": "json"})
        except httpx.HTTPStatusError as e:
            self._log(f"PATCH failed, trying MERGE. err={e}")
            await self._request_json("MERGE", url, json_body=body, headers=headers, params={"$format": "json"})

        # Verify write (best-effort): fetch order back and check the raw stored value.
        expected_raw = body.get(status_field)

        async def _read_back_raw() -> str | None:
            # Prefer minimal payload, but fall back to full object if $select fails.
            select = status_field
            if fg.status_is_guid_ref and fg.status_type_field:
                select = f"{select},{fg.status_type_field}"
            try:
                payload = await self._request_json(
                    "GET",
                    url,
                    params={"$format": "json", "$select": select},
                )
            except Exception:
                payload = await self._request_json("GET", url, params={"$format": "json"})
            obj = self._extract_single(payload) or {}
            # Some 1C OData publications may return 200 but omit requested fields
            # in $select (or return null). If so, retry with full object.
            if (status_field not in obj) or (obj.get(status_field) is None):
                try:
                    payload2 = await self._request_json("GET", url, params={"$format": "json"})
                    obj2 = self._extract_single(payload2) or {}
                    if isinstance(obj2, dict) and obj2:
                        obj = obj2
                except Exception:
                    pass

            v = obj.get(status_field)
            return str(v) if v is not None else None

        def _norm_value(v: str) -> str:
            vv = (v or "").strip()
            if _looks_like_guid(vv):
                try:
                    return str(uuid.UUID(vv))
                except Exception:
                    return vv.lower()
            return vv.casefold()

        # 1C может обновлять значение не мгновенно (редко), поэтому делаем пару попыток.
        for _ in range(3):
            got = await _read_back_raw()
            if got is not None and expected_raw is not None:
                if _norm_value(got) == _norm_value(str(expected_raw)):
                    return
            await asyncio.sleep(0.4)

        raise RuntimeError(
            f"Статус не подтвердился после записи через OData. "
            f"expected={expected_raw!r} got={got!r} field={status_field} order={onec_id}"
        )

    async def write_line_progress(
        self, onec_order_id: str, onec_line_id: str | None, item_id: str, qty_collected: float
    ) -> None:
        fg = await self._ensure_field_guess()
        progress_field = fg.progress_field or settings.onec_line_progress_field or ""

        # If we still don't know where to write progress - do nothing (local DB stays master).
        if not progress_field:
            return None

        if not onec_line_id:
            # Without LineNumber we cannot address a specific line in OData key
            return None

        try:
            line_no = int(str(onec_line_id))
        except Exception:
            return None

        url = (
            f"{settings.onec_entity_order_lines}(Ref_Key={self._guid_literal(onec_order_id)},LineNumber={line_no})"
        )
        headers = {"If-Match": "*"}
        body = {progress_field: float(qty_collected)}

        try:
            await self._request_json("PATCH", url, json_body=body, headers=headers, params={"$format": "json"})
        except httpx.HTTPStatusError as e:
            self._log(f"PATCH line progress failed, trying MERGE. err={e}")
            await self._request_json("MERGE", url, json_body=body, headers=headers, params={"$format": "json"})

    async def set_order_comment(self, onec_id: str, comment: str) -> None:
        fg = await self._ensure_field_guess()
        comment_field = fg.comment_field
        if not comment_field:
            raise RuntimeError(
                "Не удалось определить поле комментария заказа в OData. "
                "Укажите ONEC_ORDER_COMMENT_FIELD в .env."
            )

        url = f"{settings.onec_entity_orders}({self._guid_literal(onec_id)})"
        headers = {"If-Match": "*"}
        body = {comment_field: comment}

        try:
            await self._request_json("PATCH", url, json_body=body, headers=headers, params={"$format": "json"})
        except httpx.HTTPStatusError as e:
            self._log(f"PATCH failed, trying MERGE. err={e}")
            await self._request_json("MERGE", url, json_body=body, headers=headers, params={"$format": "json"})

    async def aclose(self) -> None:
        await self.client.aclose()


def build_onec_client() -> OneCClientBase:
    mode = (settings.onec_mode or "mock").lower().strip()
    if mode == "mock":
        return MockOneCClient()
    if mode == "odata":
        return ODataOneCClient()
    raise RuntimeError(f"Unknown ONEC_MODE: {settings.onec_mode}")


def _looks_like_guid(value: str) -> bool:
    """Heuristic: detect GUID strings used by 1C OData in Edm.String reference fields."""
    try:
        uuid.UUID(str(value))
        return True
    except Exception:
        return False


def _parse_dt(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        dt = date_parser.isoparse(str(value))
        # 1C often uses a sentinel "empty" date value 0001-01-01T00:00:00
        # for not-filled fields. Treat it as None so UI doesn't show year 1.
        if dt.year <= 1900:
            return None
        if dt.tzinfo is None:
            # 1C OData often returns naive; assume local timezone is not critical, keep as UTC.
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _guess_key_field(sample: dict[str, Any], needles: Iterable[str]) -> str | None:
    needles_l = [n.lower() for n in needles]
    for k in sample.keys():
        lk = k.lower()
        if not k.endswith("_Key"):
            continue
        if any(n in lk for n in needles_l):
            return k
    return None


def _guess_dt_field(sample: dict[str, Any], needles: Iterable[str]) -> str | None:
    needles_l = [n.lower() for n in needles]
    for k, v in sample.items():
        lk = k.lower()
        if any(n in lk for n in needles_l) and v is not None:
            # Try parse as datetime
            if _parse_dt(v) is not None:
                return k
    # fallback: any field name containing needles
    for k in sample.keys():
        lk = k.lower()
        if any(n in lk for n in needles_l):
            return k
    return None


def _guess_comment_field(sample: dict[str, Any]) -> str | None:
    # Prefer obvious Russian field
    if "Комментарий" in sample:
        return "Комментарий"
    for k in sample.keys():
        lk = k.lower()
        if "коммент" in lk:
            return k
    return None


def _guess_unit_field(sample: dict[str, Any]) -> str | None:
    # Prefer obvious fields
    for k in sample.keys():
        lk = k.lower()
        if "единиц" in lk and not k.endswith("_Key"):
            return k
    return None
