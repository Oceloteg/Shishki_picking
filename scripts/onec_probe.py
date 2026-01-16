"""Пробник OData 1С (standard.odata) для подбора полей и проверки доступа.

Запуск:
  1) заполните .env (ONEC_MODE=odata, ONEC_BASE_URL, ONEC_USERNAME, ONEC_PASSWORD)
  2) python -m scripts.onec_probe

Что делает:
 - скачает $metadata (xml)
 - найдёт 1 любой заказ (самым "лёгким" запросом без сортировок)
 - скачает этот заказ по ключу
 - скачает 1-5 строк табличной части по этому заказу
 - выведет подсказки, какие поля прописать в .env
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import httpx
import uuid

from app.config import settings


def _dump_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _extract_items(payload: Any) -> list[dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("value"), list):
        return [x for x in payload["value"] if isinstance(x, dict)]
    d = payload.get("d")
    if isinstance(d, dict) and isinstance(d.get("results"), list):
        return [x for x in d["results"] if isinstance(x, dict)]
    return []


def _extract_single(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    if "d" in payload and isinstance(payload["d"], dict):
        return payload["d"]
    if "Ref_Key" in payload:
        return payload
    if isinstance(payload.get("value"), list) and payload["value"]:
        v0 = payload["value"][0]
        return v0 if isinstance(v0, dict) else None
    return None


def _guid_literal(g: str) -> str:
    return f"guid'{g}'"


def _looks_like_guid(v: str) -> bool:
    try:
        uuid.UUID(str(v))
        return True
    except Exception:
        return False


def _p(s: str) -> None:
    print(s, flush=True)


def _print_exc(prefix: str, e: BaseException) -> None:
    _p(f"{prefix}{type(e).__name__}: {e!r}")


async def _get_json(client: httpx.AsyncClient, url: str, *, params: dict[str, str] | None = None) -> Any:
    # NOTE: do not start url with '/' when AsyncClient has base_url
    try:
        r = await client.get(url, params=params)
    except Exception as e:
        raise RuntimeError(f"Request error for GET {url}: {e!r}") from e

    ct = r.headers.get("content-type", "")
    _p(f"GET {r.request.url} -> {r.status_code} ({ct})")
    if r.status_code >= 400:
        text = ""
        try:
            text = r.text or ""
        except Exception:
            text = ""
        raise httpx.HTTPStatusError(
            f"HTTP {r.status_code} for {r.request.url}: {text[:1200]}",
            request=r.request,
            response=r,
        )
    try:
        return r.json()
    except Exception as e:
        text = ""
        try:
            text = r.text or ""
        except Exception:
            text = ""
        raise RuntimeError(f"Non-JSON response for {r.request.url}. ct={ct}. snippet={text[:1200]}") from e


def _guess_field(sample: dict[str, Any], needles: tuple[str, ...]) -> str | None:
    needles_l = [n.lower() for n in needles]
    for k in sample.keys():
        lk = k.lower()
        if any(n in lk for n in needles_l):
            return k
    return None


async def main() -> int:
    if not settings.onec_base_url:
        _p("ONEC_BASE_URL пустой. Заполните .env")
        return 2

    base = settings.onec_base_url.strip().rstrip("/") + "/"
    out_dir = Path("onec_probe_out")
    out_dir.mkdir(parents=True, exist_ok=True)

    timeout = httpx.Timeout(float(settings.onec_timeout_seconds))
    async with httpx.AsyncClient(
        base_url=base,
        auth=(settings.onec_username, settings.onec_password),
        verify=bool(settings.onec_verify_tls),
        timeout=timeout,
        headers={"Accept": "application/json"},
    ) as client:
        _p(f"BASE: {base}")
        _p(f"TIMEOUT: {settings.onec_timeout_seconds}s  (можно увеличить ONEC_TIMEOUT_SECONDS)")
        _p(f"VERIFY_TLS: {settings.onec_verify_tls}")

        # 1) Metadata
        try:
            r = await client.get("$metadata")
            ct = r.headers.get("content-type", "")
            _p(f"GET {r.request.url} -> {r.status_code} ({ct})")
            r.raise_for_status()
            (out_dir / "metadata.xml").write_bytes(r.content)
            _p("OK: $metadata -> onec_probe_out/metadata.xml")
        except Exception as e:
            _print_exc("WARN: не удалось скачать $metadata: ", e)

        # 2) Find ANY order id with a "light" query (avoid $orderby on huge tables)
        order_id: str | None = None
        attempts = [
            # Most common & fast: only key, with filter by NOT posted
            {"$format": "json", "$top": "1", "$select": "Ref_Key", "$filter": "Posted eq false and DeletionMark eq false"},
            # If Posted field not available/allowed for filter
            {"$format": "json", "$top": "1", "$select": "Ref_Key", "$filter": "DeletionMark eq false"},
            # The simplest possible
            {"$format": "json", "$top": "1", "$select": "Ref_Key"},
            {"$format": "json", "$top": "1"},
        ]

        for i, params in enumerate(attempts, start=1):
            try:
                _p(f"\n--- Попытка найти заказ #{i}: params={params} ---")
                data = await _get_json(client, settings.onec_entity_orders, params=params)
                items = _extract_items(data)
                if items and isinstance(items[0], dict) and items[0].get("Ref_Key"):
                    order_id = str(items[0]["Ref_Key"]).strip()
                    _p(f"Найден заказ Ref_Key={order_id}")
                    break
                else:
                    _p("Пустой ответ (value/results пустые).")
            except Exception as e:
                _print_exc("ERROR: ", e)

        if not order_id:
            _p("\nНет примера заказа.")
            _p("Возможные причины:")
            _p(" - нет прав на чтение Document_ЗаказПокупателя через OData (в UI заказы видны, но OData может быть ограничен)")
            _p(" - запросы слишком медленные (увеличьте ONEC_TIMEOUT_SECONDS)")
            _p(" - отличается имя entity set (проверьте в metadata.xml наличие Document_ЗаказПокупателя)")
            _p("\nПроверьте вручную запрос (в браузере/через curl):")
            _p(f"  {base}{settings.onec_entity_orders}?$format=json&$top=1&$select=Ref_Key")
            return 0

        # 3) Fetch this order by key (fast, no sorting)
        try:
            order_url = f"{settings.onec_entity_orders}({_guid_literal(order_id)})"
            order_data = await _get_json(client, order_url, params={"$format": "json"})
            _dump_json(out_dir / "orders_top1.json", order_data)
            _p("OK: order by key -> onec_probe_out/orders_top1.json")
        except Exception as e:
            _print_exc("ERROR: не удалось скачать заказ по ключу: ", e)
            return 0

        order_obj = _extract_single(order_data)
        if not isinstance(order_obj, dict):
            _p("Не удалось распарсить объект заказа из ответа.")
            return 0

        _p("\n--- Пример заказа (поля верхнего уровня) ---")
        _p(", ".join(sorted(order_obj.keys())))

        # Guess fields for order
        status_field = None
        if "СостояниеЗаказа" in order_obj:
            status_field = "СостояниеЗаказа"
        else:
            status_field = _guess_field(order_obj, ("Состояние", "Статус"))

        customer_key = None
        if "Контрагент_Key" in order_obj:
            customer_key = "Контрагент_Key"
        else:
            customer_key = _guess_field(order_obj, ("Контрагент", "Покупател", "Клиент", "Партнер"))

        ship_dt = "ДатаОтгрузки" if "ДатаОтгрузки" in order_obj else _guess_field(order_obj, ("Отгруз", "Дедлайн", "Доставк"))
        comment = "Комментарий" if "Комментарий" in order_obj else _guess_field(order_obj, ("Комментар", "Коммент"))

        _p("\n--- Подсказки для .env (заказ) ---")
        _p(f"ONEC_ORDER_STATUS_KEY_FIELD={status_field or '(не найдено)'}")
        _p(f"ONEC_ORDER_CUSTOMER_KEY_FIELD={customer_key or '(не найдено)'}")
        _p(f"ONEC_ORDER_SHIP_DEADLINE_FIELD={ship_dt or '(не найдено)'}")
        _p(f"ONEC_ORDER_COMMENT_FIELD={comment or '(не найдено)'}")

        # Keep status mapping for later diagnostics
        status_desc_by_key: dict[str, str] = {}
        status_key_by_desc_cf: dict[str, str] = {}

        # 3b) Try to resolve status GUID -> Description (helps to verify status names)
        try:
            if status_field and isinstance(order_obj.get(status_field), str) and _looks_like_guid(order_obj.get(status_field)):
                raw_status_key = str(order_obj.get(status_field))
                _p("\n--- Проверка статуса: GUID -> Description ---")
                statuses_payload = await _get_json(
                    client,
                    settings.onec_entity_statuses,
                    params={"$format": "json", "$top": "1000", "$select": "Ref_Key,Description"},
                )
                statuses = _extract_items(statuses_payload)
                status_desc_by_key = {
                    str(s.get("Ref_Key")): str(s.get("Description"))
                    for s in statuses
                    if s.get("Ref_Key") and s.get("Description")
                }
                status_key_by_desc_cf = {v.casefold(): k for k, v in status_desc_by_key.items()}
                desc = status_desc_by_key.get(raw_status_key)
                if desc:
                    _p(f"Текущий статус заказа: {desc}  (key={raw_status_key})")
                else:
                    _p(f"Текущий статус заказа: key={raw_status_key} (не найдено в {settings.onec_entity_statuses})")

                active = settings.active_statuses_list()
                if active:
                    _p("Активные статусы (имя -> key):")
                    for name in active:
                        k = status_key_by_desc_cf.get(str(name).casefold())
                        _p(f" - {name}: {k or '(не найдено)'}")

                # Save for further debugging
                _dump_json(out_dir / "statuses_top.json", statuses_payload)
                _p("OK: statuses -> onec_probe_out/statuses_top.json")
        except Exception as e:
            _print_exc("WARN: не удалось проверить справочник статусов: ", e)

        # 3c) Fetch a few orders WITHOUT any status filter to verify that orders are visible
        try:
            _p("\n--- Проверка: выборка заказов без фильтра статуса (только Posted/DeletionMark) ---")

            # NOTE: keep query very light: no $orderby, small $select, small $top
            list_select = ["Ref_Key", "Number", "Date", "Posted", "DeletionMark"]
            if status_field and status_field not in list_select:
                list_select.append(status_field)
            if ship_dt and ship_dt not in list_select:
                list_select.append(ship_dt)

            list_params = {
                "$format": "json",
                "$top": "10",
                "$select": ",".join(list_select),
                "$filter": "Posted eq false and DeletionMark eq false",
            }
            orders_payload = await _get_json(client, settings.onec_entity_orders, params=list_params)
            orders_list = _extract_items(orders_payload)
            _p(f"Базовая выборка -> {len(orders_list)} заказ(ов)")

            active_cf = {s.casefold() for s in settings.active_statuses_list() if s.strip()}

            for r in orders_list:
                ref = str(r.get("Ref_Key") or "")
                num = str(r.get("Number") or "—")
                dt = str(r.get("Date") or "—")
                raw_st = str(r.get(status_field) or "") if status_field else ""

                st_desc = status_desc_by_key.get(raw_st) if raw_st and status_desc_by_key else None
                st_show = st_desc or raw_st or "—"
                flag = "ACTIVE" if (st_desc and st_desc.casefold() in active_cf) else ""
                _p(f" - №{num} | {dt} | status={st_show} | key={ref} {flag}")

            # Also test status-filter semantics directly (string vs guid literal)
            if status_field and status_key_by_desc_cf:
                # pick first active status that has a key
                pick_name = None
                pick_key = None
                for name in settings.active_statuses_list():
                    k = status_key_by_desc_cf.get(str(name).casefold())
                    if k:
                        pick_name = name
                        pick_key = k
                        break

                if pick_key:
                    _p("\n--- Проверка: фильтр по статусу в OData (как в приложении) ---")
                    # 1) string literal compare (field stores GUID in string)
                    try:
                        p1 = {
                            "$format": "json",
                            "$top": "10",
                            "$select": "Ref_Key,Number," + status_field,
                            "$filter": f"Posted eq false and DeletionMark eq false and ({status_field} eq '{pick_key}')",
                        }
                        d1 = await _get_json(client, settings.onec_entity_orders, params=p1)
                        c1 = len(_extract_items(d1))
                        _p(f"status '{pick_name}' as string literal -> {c1} заказ(ов)")
                    except Exception as e:
                        _print_exc("WARN: статус-фильтр (string) не сработал: ", e)

                    # 2) guid literal compare (может работать в некоторых публикациях)
                    try:
                        p2 = {
                            "$format": "json",
                            "$top": "10",
                            "$select": "Ref_Key,Number," + status_field,
                            "$filter": f"Posted eq false and DeletionMark eq false and ({status_field} eq {_guid_literal(pick_key)})",
                        }
                        d2 = await _get_json(client, settings.onec_entity_orders, params=p2)
                        c2 = len(_extract_items(d2))
                        _p(f"status '{pick_name}' as guid literal -> {c2} заказ(ов)")
                    except Exception as e:
                        _print_exc("WARN: статус-фильтр (guid) не сработал: ", e)

        except Exception as e:
            _print_exc("WARN: не удалось сделать базовую выборку заказов: ", e)

        # 4) One line sample for this order
        try:
            params = {
                "$format": "json",
                "$top": "5",
                "$filter": f"Ref_Key eq {_guid_literal(order_id)}",
                "$orderby": "LineNumber asc",
            }
            line_data = await _get_json(client, settings.onec_entity_order_lines, params=params)
            _dump_json(out_dir / "lines_top1.json", line_data)
            _p("\nOK: lines -> onec_probe_out/lines_top1.json")
        except Exception as e:
            _print_exc("ERROR: не удалось получить строки заказа: ", e)
            line_data = None

        line_obj = None
        if line_data is not None:
            items = _extract_items(line_data)
            if items:
                line_obj = items[0]

        if isinstance(line_obj, dict):
            _p("\n--- Пример строки табличной части (поля) ---")
            _p(", ".join(sorted(line_obj.keys())))

            # Guess line fields
            item_field = "Номенклатура" if "Номенклатура" in line_obj else _guess_field(line_obj, ("Номенклат",))
            qty_field = "Количество" if "Количество" in line_obj else _guess_field(line_obj, ("Колич",))
            progress_field = (
                "КоличествоСобрано"
                if "КоличествоСобрано" in line_obj
                else _guess_field(line_obj, ("Собран", "Собрано"))
            )
            unit_field = (
                "ЕдиницаИзмерения"
                if "ЕдиницаИзмерения" in line_obj
                else _guess_field(line_obj, ("Единиц", "Ед.", "ЕдИзмер"))
            )

            _p("\n--- Подсказки для .env (строки) ---")
            _p(f"ONEC_LINE_ITEM_KEY_FIELD={item_field or '(не найдено)'}")
            _p(f"ONEC_LINE_QTY_FIELD={qty_field or '(не найдено)'}")
            _p(f"ONEC_LINE_PROGRESS_FIELD={progress_field or '(не найдено)'}")
            _p(f"ONEC_LINE_UNIT_FIELD={unit_field or '(не найдено)'}")

            # 4b) Try to resolve one item GUID -> Description (helps to verify access to Catalog_Номенклатура)
            try:
                raw_item = line_obj.get(item_field) if item_field else None
                if isinstance(raw_item, str) and _looks_like_guid(raw_item):
                    item_url = f"{settings.onec_entity_items}({_guid_literal(raw_item)})"
                    item_data = await _get_json(client, item_url, params={"$format": "json", "$select": "Ref_Key,Description"})
                    item_obj = _extract_single(item_data)
                    if isinstance(item_obj, dict) and item_obj.get("Description"):
                        _p(f"\nНоменклатура: {item_obj.get('Description')}  (key={raw_item})")
            except Exception as e:
                _print_exc("WARN: не удалось получить Description номенклатуры: ", e)

        # 5) Final check: run the same `fetch_active_orders()` logic as the app uses.
        _p("\n--- Проверка: логика приложения fetch_active_orders() ---")
        try:
            from app.onec.client import ODataOneCClient

            onec_app = ODataOneCClient()
            try:
                # Show what the application detected as key fields (helps to debug misconfig).
                try:
                    ensure = getattr(onec_app, "_ensure_field_guess", None)
                    if callable(ensure):
                        fg = await ensure()
                        _p(
                            f"app field guess: status_field={getattr(fg,'status_field',None)} "
                            f"guid_ref={getattr(fg,'status_is_guid_ref',None)} "
                            f"line_progress_field={getattr(fg,'progress_field',None)}"
                        )
                except Exception as ee:
                    _p(f"WARN: не удалось вывести field guess приложения: {ee}")

                app_orders = await onec_app.fetch_active_orders()
            finally:
                aclose = getattr(onec_app, "aclose", None)
                if callable(aclose):
                    await aclose()

            _p(f"fetch_active_orders() -> {len(app_orders)} заказ(ов)")
            for o in app_orders[:10]:
                _p(
                    f" - №{o.number or '—'} | {o.status or '—'} | {o.customer_name or '—'} "
                    f"| lines={len(o.lines or [])} | id={o.onec_id}"
                )
        except Exception as e:
            _print_exc("ERROR: fetch_active_orders() не сработал: ", e)

        # 6) Additional diagnostics: count orders (paged) and optional status write test.
        try:
            _p("\n--- Проверка: количество заказов (постранично, без фильтра статуса) ---")
            page_size = int(os.getenv("ONEC_PROBE_PAGE_SIZE", "50"))
            max_total = int(os.getenv("ONEC_PROBE_MAX_TOTAL", "300"))

            count_select = ["Ref_Key", "Number", "Date", "Posted", "DeletionMark"]
            if status_field and status_field not in count_select:
                count_select.append(status_field)

            total = 0
            active_count = 0

            active_cf = {s.casefold() for s in settings.active_statuses_list() if str(s).strip()}
            shipped_cf = (settings.onec_status_shipped or "").casefold()
            finished_cf = (settings.onec_status_finished or "").casefold()

            while total < max_total:
                take = min(page_size, max_total - total)
                params = {
                    "$format": "json",
                    "$top": str(take),
                    "$skip": str(total),
                    "$select": ",".join(count_select),
                    "$filter": "Posted eq false and DeletionMark eq false",
                }
                payload = await _get_json(client, settings.onec_entity_orders, params=params)
                items = _extract_items(payload)
                if not items:
                    break

                for r in items:
                    raw_st = str(r.get(status_field) or "") if status_field else ""
                    st_desc = status_desc_by_key.get(raw_st) if (raw_st and status_desc_by_key) else None
                    st_name = (st_desc or raw_st or "").strip()
                    st_cf = st_name.casefold()
                    if st_cf and (st_cf in active_cf) and (st_cf not in {shipped_cf, finished_cf}):
                        active_count += 1

                total += len(items)
                if len(items) < take:
                    break

            _p(f"Найдено (limit {max_total}) {total} заказ(ов) по базовому фильтру. Из них active≈{active_count}")
            if total >= max_total:
                _p("NOTE: достигнут лимит ONEC_PROBE_MAX_TOTAL; в базе может быть больше заказов.")
        except Exception as e:
            _print_exc("WARN: не удалось посчитать количество заказов: ", e)

        allow_write = os.getenv("ONEC_PROBE_WRITE_STATUS", "1").strip().lower() in {"1", "true", "yes", "y"}
        if allow_write:
            _p("\n--- ПРОБА ЗАПИСИ: изменить статус заказа туда/обратно ---")
            try:
                from app.onec.client import ODataOneCClient

                onec_write = ODataOneCClient()
                try:
                    if not status_field:
                        _p("Не найдено поле статуса (ONEC_ORDER_STATUS_KEY_FIELD); тест записи пропущен.")
                    else:
                        orig_raw = str(order_obj.get(status_field) or "")
                        orig_desc = status_desc_by_key.get(orig_raw) if (orig_raw and status_desc_by_key) else None
                        orig = (orig_desc or orig_raw or "").strip()

                        if not orig:
                            _p("Не удалось определить исходный статус; тест записи пропущен.")
                        else:
                            picking = (settings.onec_status_picking or "").strip()
                            in_work = (settings.onec_status_in_work or "").strip()

                            # Prefer toggling between picking/in_work if both configured.
                            target = None
                            if picking and in_work:
                                target = in_work if orig.casefold() == picking.casefold() else picking
                            else:
                                target = picking or in_work

                            if not target:
                                _p(
                                    "В .env не заданы ONEC_STATUS_PICKING/ONEC_STATUS_IN_WORK; "
                                    "тест записи пропущен."
                                )
                            else:
                                _p(f"Исходный статус: {orig!r} -> ставим {target!r}")
                                await onec_write.set_order_status(order_id, target)
                                _p("OK: статус записан.")

                                _p(f"Возвращаем обратно: {orig!r}")
                                await onec_write.set_order_status(order_id, orig)
                                _p("OK: статус возвращён.")
                finally:
                    aclose = getattr(onec_write, "aclose", None)
                    if callable(aclose):
                        await aclose()
            except Exception as e:
                _print_exc("ERROR: тест записи статуса не прошёл: ", e)
        else:
            _p("\n--- ПРОБА ЗАПИСИ: отключена ---")
            _p("Чтобы протестировать запись статуса (и вернуть обратно), запустите:")
            _p("  ONEC_PROBE_WRITE_STATUS=1 python -m scripts.onec_probe")

        _p("\nГотово. Если нужно — пришлите мне файлы из onec_probe_out (orders_top1.json и lines_top1.json).")
        return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
