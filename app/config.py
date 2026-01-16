from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    # ==== App / logging ====
    app_debug: bool = Field(default=False, alias="APP_DEBUG")
    app_log_level: str = Field(default="INFO", alias="APP_LOG_LEVEL")

    # ==== Auth ====
    app_password: str = Field(default="change-me", alias="APP_PASSWORD")
    secret_key: str = Field(default="change-me", alias="APP_SECRET_KEY")
    jwt_algorithm: str = Field(default="HS256", alias="JWT_ALGORITHM")
    token_exp_days: int = Field(default=30, alias="TOKEN_EXP_DAYS")

    cookie_name: str = Field(default="auth_token", alias="COOKIE_NAME")
    cookie_secure: bool = Field(default=False, alias="APP_COOKIE_SECURE")
    cookie_samesite: str = Field(default="Lax", alias="APP_COOKIE_SAMESITE")  # Lax|Strict|None

    # ==== DB ====
    database_url: str = Field(default="sqlite:///./app.db", alias="DATABASE_URL")

    # ==== Sync ====
    sync_interval_seconds: int = Field(default=60, alias="SYNC_INTERVAL_SECONDS")
    # Outbox (write-back to 1C) processing interval
    outbox_interval_seconds: int = Field(default=5, alias="OUTBOX_INTERVAL_SECONDS")

    # ==== 1C integration ====
    onec_mode: str = Field(default="mock", alias="ONEC_MODE")  # mock|odata
    onec_base_url: str = Field(default="", alias="ONEC_BASE_URL")
    onec_username: str = Field(default="", alias="ONEC_USERNAME")
    onec_password: str = Field(default="", alias="ONEC_PASSWORD")

    # Network / transport options
    onec_verify_tls: bool = Field(default=True, alias="ONEC_VERIFY_TLS")
    onec_timeout_seconds: int = Field(default=30, alias="ONEC_TIMEOUT_SECONDS")
    onec_orders_top: int = Field(default=200, alias="ONEC_ORDERS_TOP")
    onec_orders_orderby: str = Field(default="Date desc", alias="ONEC_ORDERS_ORDERBY")  # set empty to disable
    onec_http_debug: bool = Field(default=False, alias="ONEC_HTTP_DEBUG")
    onec_concurrency: int = Field(default=8, alias="ONEC_CONCURRENCY")

    # Confirm updates by re-reading updated fields (helps diagnose 1C locks / ignored updates)
    onec_confirm_updates: bool = Field(default=True, alias="ONEC_CONFIRM_UPDATES")

    # Entity sets (can be overridden if your OData publication differs)
    onec_entity_orders: str = Field(default="Document_ЗаказПокупателя", alias="ONEC_ENTITY_ORDERS")
    onec_entity_order_lines: str = Field(
        default="Document_ЗаказПокупателя_Запасы", alias="ONEC_ENTITY_ORDER_LINES"
    )
    onec_entity_statuses: str = Field(
        default="Catalog_СостоянияЗаказовПокупателей", alias="ONEC_ENTITY_STATUSES"
    )
    onec_entity_customers: str = Field(default="Catalog_Контрагенты", alias="ONEC_ENTITY_CUSTOMERS")
    onec_entity_items: str = Field(default="Catalog_Номенклатура", alias="ONEC_ENTITY_ITEMS")
    # Units catalog (optional). Set empty to disable unit name resolution.
    onec_entity_units: str = Field(
        default="Catalog_КлассификаторЕдиницИзмерения", alias="ONEC_ENTITY_UNITS"
    )

    # Field mapping (override if names differ in your configuration)
    onec_order_status_key_field: str = Field(default="СостояниеЗаказа", alias="ONEC_ORDER_STATUS_KEY_FIELD")
    onec_order_customer_key_field: str = Field(default="Контрагент_Key", alias="ONEC_ORDER_CUSTOMER_KEY_FIELD")
    onec_order_ship_deadline_field: str = Field(default="ДатаОтгрузки", alias="ONEC_ORDER_SHIP_DEADLINE_FIELD")
    onec_order_comment_field: str = Field(default="Комментарий", alias="ONEC_ORDER_COMMENT_FIELD")

    # Optional: order picking state field/code ("Сборка заказов покупателей")
    # Set ONEC_ORDER_PICK_STATE_FIELD empty to disable.
    onec_order_pick_state_field: str = Field(default="СтатусСборки", alias="ONEC_ORDER_PICK_STATE_FIELD")
    onec_pick_state_not_started: int = Field(default=0, alias="ONEC_PICK_STATE_NOT_STARTED")
    onec_pick_state_picking: int = Field(default=1, alias="ONEC_PICK_STATE_PICKING")
    onec_pick_state_picked: int = Field(default=2, alias="ONEC_PICK_STATE_PICKED")

    onec_line_item_key_field: str = Field(default="Номенклатура", alias="ONEC_LINE_ITEM_KEY_FIELD")
    onec_line_qty_field: str = Field(default="Количество", alias="ONEC_LINE_QTY_FIELD")
    onec_line_unit_field: str = Field(default="ЕдиницаИзмерения", alias="ONEC_LINE_UNIT_FIELD")
    # If empty, app will try to auto-detect a "collected" field in lines.
    onec_line_progress_field: str = Field(default="КоличествоСобрано", alias="ONEC_LINE_PROGRESS_FIELD")

    # Status strings in 1C
    onec_status_picking: str = Field(default="На сборке", alias="ONEC_STATUS_PICKING")
    onec_status_picked: str = Field(default="Собран", alias="ONEC_STATUS_PICKED")
    onec_status_in_work: str = Field(default="В работе", alias="ONEC_STATUS_IN_WORK")
    onec_status_shipped: str = Field(default="Отгружен", alias="ONEC_STATUS_SHIPPED")
    onec_status_finished: str = Field(default="Завершен", alias="ONEC_STATUS_FINISHED")

    # Comma-separated list of statuses considered "active" (orders to show in the board), if NOT posted.
    onec_active_statuses: str = Field(
        default="На сборке,В работе,Собран", alias="ONEC_ACTIVE_STATUSES"
    )

    # ==== UI thresholds ====
    due_soon_hours: int = Field(default=24, alias="DUE_SOON_HOURS")
    stale_hours: int = Field(default=48, alias="STALE_HOURS")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    def active_statuses_list(self) -> list[str]:
        return [s.strip() for s in self.onec_active_statuses.split(",") if s.strip()]


settings = Settings()
