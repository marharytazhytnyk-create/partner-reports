#!/usr/bin/env python3
"""
WBR (Weekly Business Review) — Pinkman Bar / Bella Mozzarella.
Дані з Databricks, HTML українською, UAH.
Автооновлення: щопонеділка о 14:00 (Київ).
"""

from __future__ import annotations

import datetime
import os
import re
import sys
import time
from pathlib import Path

import requests

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://bolt-incentives.cloud.databricks.com")
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "0221-081903-9ag4bh69")


def _load_token() -> str:
    token = os.getenv("DATABRICKS_TOKEN", "")
    if token:
        return token
    base = Path(__file__).parent
    for env_path in (
        base.parent / "databricks-setup" / ".env",
        Path.home() / "Library" / "CloudStorage"
        / "GoogleDrive-marharyta.zhytnyk@bolt.eu" / "My Drive"
        / "Events project" / "databricks-setup" / ".env",
    ):
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("DATABRICKS_TOKEN="):
                    return line.split("=", 1)[1].strip()
    return ""


SCRIPT_DIR = Path(__file__).parent
DATABRICKS_TOKEN = _load_token()

REPORT_CONFIGS: list[dict] = [
    {
        "slug": "pinkman-bar-kharkiv",
        "output": "WBR Pinkman Bar — Kharkiv.html",
        "header_title": "PINKMAN BAR",
        "city_uk": "Харків",
        "provider_ids": [201452, 201453, 201457, 201460, 201467, 201469],
        "name_strip": [r"Pinkman\s+Bar", r"PINKMAN\s+BAR"],
    },
    {
        "slug": "bella-mozzarella-kharkiv",
        "output": "WBR Bella Mozzarella — Kharkiv.html",
        "header_title": "BELLA MOZZARELLA",
        "city_uk": "Харків",
        "provider_ids": [201367, 201375, 201456, 201462, 201465, 201471],
        "name_strip": [r"Bella\s+Mozzarella", r"BELLA\s+MOZZARELLA"],
    },
]

WEEK_COLORS = ["#0d8a52", "#34D186"]

POLL_INTERVAL_S = 5
MAX_POLL_S = 600

HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

# Секції метрик: (заголовок секції, список ключів)
CHART_SECTIONS: list[tuple[str, list[str]]] = [
    ("1. Продажі", ["gross", "orders", "aov"]),
    ("2. Операційні показники", ["avail", "accept", "refunds", "prep_time"]),
    ("3. Клієнти та їх поведінка", [
        "active_users", "freq", "new_users", "sessions", "imp_menu", "menu_prod", "rating",
    ]),
    ("4. Знижки", ["discounts", "camp_bolt", "camp_merch"]),
]

# Українські назви та розшифровки метрик: ключ → (назва, опис, одиниця)
METRIC_UK: dict[str, tuple[str, str, str]] = {
    "gross": (
        "Загальні продажі",
        "Сума вартості доставлених замовлень до застосування знижок",
        "₴",
    ),
    "orders": (
        "Доставлені замовлення",
        "Кількість замовлень, успішно доставлених клієнтам",
        "шт.",
    ),
    "aov": (
        "Середній чек",
        "Середня сума одного доставленого замовлення до знижок",
        "₴",
    ),
    "avail": (
        "Доступність на платформі",
        "Частка часу, коли заклад був онлайн і доступний для замовлення",
        "%",
    ),
    "accept": (
        "Прийняття замовлень",
        "Частка замовлень, які партнер прийняв вчасно",
        "%",
    ),
    "refunds": (
        "Замовлення з компенсаціями",
        "Частка замовлень, за які клієнту надано компенсацію або повернення",
        "%",
    ),
    "prep_time": (
        "Час приготування",
        "Середній час приготування замовлення на кухні",
        "хв",
    ),
    "active_users": (
        "Активні користувачі",
        "Унікальні клієнти з хоча б одним доставленим замовленням за тиждень",
        "осіб",
    ),
    "freq": (
        "Частота замовлень",
        "Середня кількість замовлень на одного активного користувача",
        "зам./корист.",
    ),
    "new_users": (
        "Нові користувачі",
        "Клієнти, які вперше зробили замовлення в цьому бренді",
        "осіб",
    ),
    "sessions": (
        "Перегляди закладу",
        "Сесії, коли клієнт бачив заклад у стрічці, пошуку або на карті",
        "сесій",
    ),
    "imp_menu": (
        "Конверсія: перегляд → меню",
        "Частка переглядів закладу, з яких клієнт відкрив меню",
        "%",
    ),
    "menu_prod": (
        "Конверсія: меню → кошик",
        "Частка переглядів меню, з яких клієнт додав страву в кошик",
        "%",
    ),
    "rating": (
        "Рейтинг закладу",
        "Середня оцінка від клієнтів після доставки",
        "з 5",
    ),
    "discounts": (
        "Загальна сума знижок",
        "Усі знижки для клієнтів за тиждень (Bolt + партнер)",
        "₴",
    ),
    "camp_bolt": (
        "Витрати Bolt на знижки",
        "Сума, яку Bolt профінансував на знижки та промо для клієнтів",
        "₴",
    ),
    "camp_merch": (
        "Витрати партнера на знижки",
        "Сума, яку партнер профінансував на знижки та промо для клієнтів",
        "₴",
    ),
}

# Ключі для гістограм по локаціях (останні 2 тижні)
LOCATION_CHART_SECTIONS: list[tuple[str, list[str]]] = CHART_SECTIONS


# ─── DATE HELPERS ──────────────────────────────────────────────────────────────

def last_complete_week_start(ref: datetime.date | None = None) -> datetime.date:
    """Понеділок останнього повного тижня (Пн–Нд, що завершився перед поточним)."""
    d = ref or datetime.date.today()
    this_monday = d - datetime.timedelta(days=d.weekday())
    return this_monday - datetime.timedelta(days=7)


def week_start_from_iso(dt_key: str) -> datetime.date:
    return datetime.date.fromisoformat(str(dt_key)[:10])


def week_label(week_start: datetime.date) -> str:
    end = week_start + datetime.timedelta(days=6)
    if week_start.year == end.year:
        return f"{week_start.strftime('%d.%m')} – {end.strftime('%d.%m.%Y')}"
    return f"{week_start.strftime('%d.%m.%Y')} – {end.strftime('%d.%m.%Y')}"


def week_key(week_start: datetime.date) -> str:
    return week_start.isoformat()


def week_range_end(week_start: datetime.date) -> datetime.date:
    return week_start + datetime.timedelta(days=7)


def short_location_name(full_name: str, name_strip: list[str] | None = None) -> str:
    n = full_name.strip()
    for pattern in name_strip or []:
        n = re.sub(rf"(?i)^{pattern}\s*", "", n)
    return n.strip() or full_name


def build_week_list(start: datetime.date, end: datetime.date) -> list[datetime.date]:
    weeks: list[datetime.date] = []
    cur = start
    while cur <= end:
        weeks.append(cur)
        cur += datetime.timedelta(days=7)
    return weeks


# ─── DATABRICKS ────────────────────────────────────────────────────────────────

def _post(path: str, payload: dict) -> dict:
    r = requests.post(f"{DATABRICKS_HOST}{path}", headers=HEADERS, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()


def _get(path: str, params: dict) -> dict:
    r = requests.get(f"{DATABRICKS_HOST}{path}", headers=HEADERS, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def create_context() -> str:
    return _post("/api/1.2/contexts/create", {"language": "sql", "clusterId": CLUSTER_ID})["id"]


def run_query(ctx_id: str, sql: str) -> list[list]:
    cmd_id = _post(
        "/api/1.2/commands/execute",
        {"language": "sql", "clusterId": CLUSTER_ID, "contextId": ctx_id, "command": sql},
    )["id"]
    deadline = time.time() + MAX_POLL_S
    while time.time() < deadline:
        time.sleep(POLL_INTERVAL_S)
        resp = _get(
            "/api/1.2/commands/status",
            {"clusterId": CLUSTER_ID, "contextId": ctx_id, "commandId": cmd_id},
        )
        status = resp.get("status")
        if status == "Finished":
            result = resp.get("results", {})
            if result.get("resultType") == "error":
                raise RuntimeError(result.get("summary", "Query error"))
            return result.get("data", [])
        if status in ("Cancelled", "Error"):
            raise RuntimeError(f"Command {status}: {resp}")
    raise TimeoutError(f"Query timed out after {MAX_POLL_S}s")


def destroy_context(ctx_id: str) -> None:
    try:
        _post("/api/1.2/contexts/destroy", {"clusterId": CLUSTER_ID, "contextId": ctx_id})
    except Exception:
        pass


def _parse_week_row(row: list) -> dict:
    ws = week_start_from_iso(str(row[0]))
    delivered = int(row[1] or 0)
    gross = float(row[2] or 0)
    net = float(row[3] or 0)
    new_u = int(row[4] or 0)
    acc_r = round(float(row[5] or 0) * 100, 2)
    avl_r = round(float(row[6] or 0) * 100, 2)
    refund_r = round(float(row[7] or 0) * 100, 2)
    rating = round(float(row[8] or 0), 2)
    del_time = round(float(row[9] or 0), 1)
    acc_time = round(float(row[10] or 0), 1)
    prep_time = round(float(row[11] or 0), 1)
    wait_time = round(float(row[12] or 0), 1)
    c2m = round(float(row[13] or 0), 1)
    c2e = round(float(row[14] or 0), 1)
    discounts = round(float(row[15] or 0), 0)
    camp_bolt = round(float(row[16] or 0), 0)
    camp_merch = round(float(row[17] or 0), 0)
    sessions = int(row[18] or 0)
    menu_view = int(row[19] or 0)
    menu_prod_conv = round(float(row[20] or 0) * 100, 2)
    imp_menu = round(menu_view / sessions * 100, 2) if sessions else 0
    aov = round(gross / delivered, 0) if delivered else 0

    return {
        "week_start": ws,
        "week_key": week_key(ws),
        "label": week_label(ws),
        "gross": round(gross, 0),
        "net": round(net, 0),
        "orders": delivered,
        "aov": aov,
        "avail": avl_r,
        "accept": acc_r,
        "refunds": refund_r,
        "del_time": del_time,
        "acc_time": acc_time,
        "prep_time": prep_time,
        "wait_time": wait_time,
        "c2m_time": c2m,
        "c2e_time": c2e,
        "new_users": new_u,
        "sessions": sessions,
        "imp_menu": imp_menu,
        "menu_prod": menu_prod_conv,
        "rating": rating,
        "discounts": discounts,
        "camp_bolt": camp_bolt,
        "camp_merch": camp_merch,
    }


def _parse_location_row(row: list, active_users: int = 0) -> dict:
    ws = week_start_from_iso(str(row[2]))
    orders = int(row[3] or 0)
    gross = float(row[4] or 0)
    net = float(row[5] or 0)
    avail = round(float(row[6] or 0), 2)
    accept = round(float(row[7] or 0), 2)
    refunds = round(float(row[8] or 0), 2)
    rating = round(float(row[9] or 0), 2)
    prep = round(float(row[10] or 0), 1)
    acc_time = round(float(row[11] or 0), 1)
    discounts = round(float(row[12] or 0), 0)
    camp_bolt = round(float(row[13] or 0), 0)
    camp_merch = round(float(row[14] or 0), 0)
    new_users = int(row[15] or 0)
    sessions = int(row[16] or 0)
    menu_viewed = int(row[17] or 0)
    menu_prod = round(float(row[18] or 0) * 100, 2)
    imp_menu = round(menu_viewed / sessions * 100, 2) if sessions else 0
    aov = round(gross / orders, 0) if orders else 0
    au = active_users or orders
    freq = round(orders / au, 2) if au else 0

    return {
        "week_key": week_key(ws),
        "label": week_label(ws),
        "orders": orders,
        "gross": round(gross, 0),
        "net": round(net, 0),
        "aov": aov,
        "avail": avail,
        "accept": accept,
        "refunds": refunds,
        "rating": rating,
        "prep_time": prep,
        "acc_time": acc_time,
        "discounts": discounts,
        "camp_bolt": camp_bolt,
        "camp_merch": camp_merch,
        "new_users": new_users,
        "active_users": au,
        "freq": freq,
        "sessions": sessions,
        "imp_menu": imp_menu,
        "menu_prod": menu_prod,
    }


# ─── DATA FETCH ────────────────────────────────────────────────────────────────

def fetch_metrics(provider_ids: list[int], name_strip: list[str]) -> dict:
    if not provider_ids:
        raise ValueError("provider_ids is empty")

    pids_sql = ", ".join(str(p) for p in provider_ids)
    pids_str = ", ".join(f"'{p}'" for p in provider_ids)
    last_week = last_complete_week_start()
    global_end = (last_week + datetime.timedelta(days=7)).isoformat()

    ctx = create_context()
    try:
        min_sql = f"""
        SELECT MIN(DATE_TRUNC('week', metric_timestamp_partition)) AS min_week
        FROM ng_delivery_spark.fact_provider_weekly
        WHERE provider_id IN ({pids_sql})
          AND delivered_orders_count > 0
        """
        min_row = run_query(ctx, min_sql)
        if not min_row or not min_row[0][0]:
            min_sql2 = f"""
            SELECT MIN(DATE_TRUNC('week', metric_timestamp_partition)) AS min_week
            FROM ng_delivery_spark.fact_provider_weekly
            WHERE provider_id IN ({pids_sql})
            """
            min_row = run_query(ctx, min_sql2)
        if not min_row or not min_row[0][0]:
            raise RuntimeError("Немає даних для вказаних provider_id")

        global_start = str(min_row[0][0])[:10]
        start_date = week_start_from_iso(global_start)
        weeks_range = build_week_list(start_date, last_week)

        main_sql = f"""
        SELECT
            DATE_TRUNC('week', metric_timestamp_partition) AS week,
            SUM(delivered_orders_count) AS delivered_orders,
            SUM(total_gmv_before_discounts) AS gross_sales,
            SUM(total_gmv_after_discounts) AS net_sales,
            SUM(users_activated_vendor_count) AS new_users,
            AVG(provider_acceptance_rate_value) AS acceptance_rate,
            AVG(provider_active_rate_value) AS availability_rate,
            AVG(customer_refunded_order_rate_value) AS refund_rate,
            AVG(provider_rating_per_order_value) AS rating,
            AVG(order_total_minutes_per_order_value) AS delivery_time,
            AVG(provider_acceptance_minutes_per_order_value) AS acceptance_time,
            AVG(provider_preparation_minutes_per_order_value) AS prep_time,
            AVG(courier_total_wait_minutes_per_order_value) AS courier_wait,
            AVG(courier_to_provider_actual_minutes_per_order_value) AS c2merchant,
            AVG(courier_to_eater_actual_minutes_per_order_value) AS c2eater,
            SUM(total_campaign_discount) AS discounts,
            SUM(total_campaign_spend_bolt) AS camp_bolt,
            SUM(total_campaign_spend_provider) AS camp_merchant,
            SUM(provider_impressions_sessions_count) AS sessions,
            SUM(provider_menu_viewed_sessions_count) AS menu_viewed,
            SUM(provider_product_added_sessions_count) AS prod_added,
            AVG(provider_product_added_from_menu_viewed_rate_value) AS menu_prod_rate
        FROM ng_delivery_spark.fact_provider_weekly
        WHERE provider_id IN ({pids_sql})
          AND metric_timestamp_partition >= '{global_start}'
          AND metric_timestamp_partition < '{global_end}'
        GROUP BY 1 ORDER BY 1
        """
        main_rows = run_query(ctx, main_sql)

        users_sql = f"""
        SELECT DATE_TRUNC('week', metric_timestamp_partition) AS week,
               SUM(provider_deliveries_unique_user_count) AS active_users
        FROM ng_delivery_spark.int_provider_metrics_non_additive
        WHERE entity_id IN ({pids_str}) AND timeframe_name = 'week'
          AND metric_timestamp_partition >= '{global_start}'
          AND metric_timestamp_partition < '{global_end}'
        GROUP BY 1 ORDER BY 1
        """
        users_rows = run_query(ctx, users_sql)

        loc_sql = f"""
        SELECT
            f.provider_id,
            d.provider_name,
            DATE_TRUNC('week', f.metric_timestamp_partition) AS week,
            SUM(f.delivered_orders_count) AS orders,
            SUM(f.total_gmv_before_discounts) AS gross,
            SUM(f.total_gmv_after_discounts) AS net,
            AVG(f.provider_active_rate_value) * 100 AS avail,
            AVG(f.provider_acceptance_rate_value) * 100 AS accept,
            AVG(f.customer_refunded_order_rate_value) * 100 AS refunds,
            AVG(f.provider_rating_per_order_value) AS rating,
            AVG(f.provider_preparation_minutes_per_order_value) AS prep_time,
            AVG(f.provider_acceptance_minutes_per_order_value) AS acc_time,
            SUM(f.total_campaign_discount) AS discounts,
            SUM(f.total_campaign_spend_bolt) AS camp_bolt,
            SUM(f.total_campaign_spend_provider) AS camp_merch,
            SUM(f.users_activated_vendor_count) AS new_users,
            SUM(f.provider_impressions_sessions_count) AS sessions,
            SUM(f.provider_menu_viewed_sessions_count) AS menu_viewed,
            AVG(f.provider_product_added_from_menu_viewed_rate_value) AS menu_prod_rate
        FROM ng_delivery_spark.fact_provider_weekly f
        JOIN ng_delivery_spark.dim_provider_v2 d ON f.provider_id = d.provider_id
        WHERE f.provider_id IN ({pids_sql})
          AND f.metric_timestamp_partition >= '{global_start}'
          AND f.metric_timestamp_partition < '{global_end}'
        GROUP BY 1, 2, 3 ORDER BY d.provider_name, 3
        """
        loc_rows = run_query(ctx, loc_sql)

        loc_users_sql = f"""
        SELECT entity_id AS provider_id,
               DATE_TRUNC('week', metric_timestamp_partition) AS week,
               SUM(provider_deliveries_unique_user_count) AS active_users
        FROM ng_delivery_spark.int_provider_metrics_non_additive
        WHERE entity_id IN ({pids_str}) AND timeframe_name = 'week'
          AND metric_timestamp_partition >= '{global_start}'
          AND metric_timestamp_partition < '{global_end}'
        GROUP BY 1, 2 ORDER BY 1, 2
        """
        loc_users_rows = run_query(ctx, loc_users_sql)

        items_sql = f"""
        SELECT TRIM(bi.name) AS item_name,
               SUM(bi.amount) AS qty,
               SUM(bi.total_price) AS revenue_uah,
               ROUND(AVG(bi.unit_price), 2) AS avg_price
        FROM ng_delivery_spark.delivery_basket_user_basket_item bi
        JOIN ng_delivery_spark.delivery_basket_user_basket ub ON bi.user_basket_id = ub.id
        JOIN ng_delivery_spark.delivery_order_order doo ON ub.master_basket_id = doo.master_basket_id
        WHERE doo.provider_id IN ({pids_sql})
          AND doo.state = 'delivered'
          AND doo.created_date >= '{global_start}' AND doo.created_date < '{global_end}'
          AND bi.name IS NOT NULL AND TRIM(bi.name) != ''
          AND bi.parent_id IS NULL AND bi.type = 'dish'
        GROUP BY TRIM(bi.name) ORDER BY qty DESC LIMIT 10
        """
        top_items = run_query(ctx, items_sql)
    finally:
        destroy_context(ctx)

    active_by_week: dict[str, int] = {}
    for row in users_rows:
        ws = week_start_from_iso(str(row[0]))
        active_by_week[week_key(ws)] = int(row[1] or 0)

    active_by_pid_week: dict[tuple[int, str], int] = {}
    for row in loc_users_rows:
        pid = int(row[0])
        wk = week_key(week_start_from_iso(str(row[1])))
        active_by_pid_week[(pid, wk)] = int(row[2] or 0)

    by_week_key: dict[str, dict] = {}
    for row in main_rows:
        rec = _parse_week_row(row)
        rec["active_users"] = active_by_week.get(rec["week_key"], rec["orders"])
        rec["freq"] = round(rec["orders"] / rec["active_users"], 2) if rec["active_users"] else 0
        by_week_key[rec["week_key"]] = rec

    empty_week = {
        "gross": 0, "net": 0, "orders": 0, "aov": 0,
        "avail": 0, "accept": 0, "refunds": 0, "rating": 0,
        "prep_time": 0, "acc_time": 0, "discounts": 0, "imp_menu": 0,
        "new_users": 0, "active_users": 0, "freq": 0,
        "del_time": 0, "acc_time": 0, "wait_time": 0,
        "sessions": 0, "menu_prod": 0, "camp_bolt": 0, "camp_merch": 0,
    }

    week_data: list[dict] = []
    for ws in weeks_range:
        wk = week_key(ws)
        if wk in by_week_key:
            week_data.append(by_week_key[wk])
        else:
            week_data.append({
                **empty_week,
                "week_start": ws,
                "week_key": wk,
                "label": week_label(ws),
            })

    compare_weeks = weeks_range[-2:] if len(weeks_range) >= 2 else weeks_range
    compare_keys = [week_key(w) for w in compare_weeks]
    compare_labels = [week_label(w) for w in compare_weeks]

    by_pid: dict[int, dict] = {}
    for row in loc_rows:
        pid = int(row[0])
        wk = week_key(week_start_from_iso(str(row[2])))
        au = active_by_pid_week.get((pid, wk), 0)
        rec = _parse_location_row(row, au)
        if pid not in by_pid:
            by_pid[pid] = {
                "provider_id": pid,
                "name": row[1],
                "short_name": short_location_name(str(row[1]), name_strip),
                "by_week": {},
            }
        by_pid[pid]["by_week"][rec["week_key"]] = rec

    locations: list[dict] = []
    for pid in sorted(by_pid.keys(), key=lambda p: by_pid[p]["name"]):
        loc = by_pid[pid]
        weeks_vals = []
        for wk in compare_keys:
            weeks_vals.append(loc["by_week"].get(wk, {
                "week_key": wk,
                "label": compare_labels[compare_keys.index(wk)],
                "orders": 0, "gross": 0, "net": 0, "aov": 0,
                "avail": 0, "accept": 0, "refunds": 0, "rating": 0,
                "prep_time": 0, "acc_time": 0, "discounts": 0,
                "camp_bolt": 0, "camp_merch": 0,
                "new_users": 0, "active_users": 0, "freq": 0,
                "sessions": 0, "imp_menu": 0, "menu_prod": 0,
            }))
        loc["weeks"] = weeks_vals
        locations.append(loc)

    items = [
        {"rank": i + 1, "name": row[0], "qty": int(row[1] or 0),
         "revenue": round(float(row[2] or 0), 0), "avg_price": float(row[3] or 0)}
        for i, row in enumerate(top_items)
    ]

    return {
        "weeks": week_data,
        "locations": locations,
        "week_labels_all": [w["label"] for w in week_data],
        "compare_labels": compare_labels,
        "top_items": items,
        "period_label": f"{week_data[0]['label']} — {week_data[-1]['label']}" if week_data else "",
        "generated_at": datetime.datetime.now().strftime("%d.%m.%Y %H:%M"),
        "total_weeks": len(week_data),
    }


# ─── ANALYSIS ─────────────────────────────────────────────────────────────────

def _pct_change(old: float, new: float) -> float | None:
    if old == 0:
        return None
    return (new - old) / old * 100


def analyze_problem_locations(locations: list[dict]) -> list[dict]:
    problems: list[dict] = []
    for loc in locations:
        if len(loc["weeks"]) < 2:
            continue
        prev_w, last_w = loc["weeks"][0], loc["weeks"][1]
        issues: list[str] = []
        severity = 0

        o_chg = _pct_change(prev_w["orders"], last_w["orders"])
        if last_w["orders"] < 15:
            issues.append(
                f"Дуже мало замовлень у останньому тижні — лише {last_w['orders']} "
                f"(було {prev_w['orders']} тижнем раніше)."
            )
            severity += 3
        elif o_chg is not None and o_chg <= -20:
            issues.append(
                f"Різке падіння замовлень: {prev_w['orders']} → {last_w['orders']} ({o_chg:.0f}%)."
            )
            severity += 2
        elif o_chg is not None and o_chg <= -10:
            issues.append(
                f"Зменшення замовлень: {prev_w['orders']} → {last_w['orders']} ({o_chg:.0f}%)."
            )
            severity += 1

        if last_w["avail"] < 88:
            issues.append(
                f"Низька доступність — {last_w['avail']:.1f}% "
                f"(клієнти часто не бачать заклад онлайн)."
            )
            severity += 2
        elif last_w["avail"] < 92 and prev_w["avail"] < 92:
            issues.append(
                f"Доступність нижче 92% обидва тижні "
                f"({prev_w['avail']:.1f}% → {last_w['avail']:.1f}%)."
            )
            severity += 1

        if last_w["accept"] < 97:
            issues.append(f"Не всі замовлення приймаються вчасно — {last_w['accept']:.1f}%.")
            severity += 2

        if last_w["refunds"] >= 4:
            issues.append(
                f"Висока частка замовлень з компенсаціями — {last_w['refunds']:.1f}%."
            )
            severity += 2
        elif last_w["refunds"] >= 2.5 and last_w["refunds"] > prev_w["refunds"] + 1:
            issues.append(
                f"Зростання повернень: {prev_w['refunds']:.1f}% → {last_w['refunds']:.1f}%."
            )
            severity += 1

        if last_w["rating"] < 4.5 and last_w["rating"] > 0:
            issues.append(f"Низький рейтинг — {last_w['rating']:.2f} з 5.")
            severity += 2

        if last_w["prep_time"] >= 32:
            issues.append(f"Довгий час приготування — {last_w['prep_time']:.1f} хв у середньому.")
            severity += 1

        if last_w["acc_time"] >= 3:
            issues.append(
                f"Повільне прийняття замовлень — {last_w['acc_time']:.1f} хв."
            )
            severity += 2

        if last_w["imp_menu"] < 10 and last_w.get("sessions", 0) > 200:
            issues.append(f"Мало переходів у меню — лише {last_w['imp_menu']:.1f}% переглядів.")
            severity += 1

        if issues and severity >= 1:
            problems.append({
                "name": loc["name"],
                "short_name": loc["short_name"],
                "severity": severity,
                "issues": issues,
                "prev": prev_w,
                "last": last_w,
            })

    problems.sort(key=lambda x: -x["severity"])
    return problems


def build_insights(weeks: list[dict], problem_locations: list[dict]) -> list[dict]:
    if len(weeks) < 2:
        return [{"type": "info", "title": "Недостатньо даних",
                 "text": "Для порівняння потрібні щонайменше два повні тижні."}]

    a, b = weeks[-2], weeks[-1]
    w1, w2 = a["label"], b["label"]
    insights: list[dict] = []

    def add(kind: str, title: str, text: str):
        insights.append({"type": kind, "title": title, "text": text})

    g_chg = _pct_change(a["gross"], b["gross"])
    o_chg = _pct_change(a["orders"], b["orders"])
    if g_chg is not None and g_chg > 5:
        add("positive", "Продажі зростають",
            f"Загальний оборот мережі зріс на {g_chg:.0f}% ({w1} → {w2}). "
            f"Доставлених замовлень: {o_chg:.0f}%.")

    if b["avail"] - a["avail"] >= 3:
        add("positive", "Мережа частіше онлайн",
            f"Середня доступність зросла з {a['avail']:.1f}% до {b['avail']:.1f}%.")

    if b["refunds"] < a["refunds"] - 0.5:
        add("positive", "Менше компенсацій клієнтам",
            f"Частка замовлень з поверненнями: {a['refunds']:.1f}% → {b['refunds']:.1f}%.")

    if b["prep_time"] > a["prep_time"] + 1:
        add("warning", "Час приготування зростає",
            f"Середній час приготування: {a['prep_time']} → {b['prep_time']} хв.")

    if b["rating"] > a["rating"] + 0.1 and b["rating"] > 0:
        add("positive", "Клієнти задоволені якістю",
            f"Середній рейтинг мережі: {a['rating']:.2f} → {b['rating']:.2f} з 5.")

    d_chg = _pct_change(a["discounts"], b["discounts"])
    if d_chg is not None and d_chg > 5:
        add("info", "Співфінансування знижок",
            f"Інвестиції Bolt у знижки зросли на {d_chg:.0f}% "
            f"({a['discounts']:,.0f} → {b['discounts']:,.0f} ₴). ".replace(",", "\u202f")
            + "Для стабільного зростання важливо долучатися до промо з боку партнера.")

    if problem_locations:
        top_names = ", ".join(p["short_name"] for p in problem_locations[:3])
        add("warning", "Є точки, що потребують уваги",
            f"Детальний розбір нижче. Насамперед: {top_names}.")

    if not insights:
        add("info", "Стабільний період",
            "Основні показники без різких змін. Продовжуйте тримати якість та доступність.")

    return insights


# ─── HTML ─────────────────────────────────────────────────────────────────────

def _fmt_chart_value(val: float, opts: dict | None = None) -> str:
    opts = opts or {}
    if opts.get("pct"):
        return f"{val:.2f}%"
    if opts.get("dec"):
        if isinstance(val, float) and val != int(val):
            return f"{val:,.2f}".replace(",", "\u202f")
        return f"{val:,.0f}".replace(",", "\u202f")
    if abs(val) >= 1000:
        return f"{val:,.0f}".replace(",", "\u202f")
    return f"{val:.2f}" if isinstance(val, float) and val != int(val) else str(int(val))


def _metric_opts(key: str) -> dict:
    if key in ("avail", "accept", "refunds", "imp_menu", "menu_prod"):
        return {"pct": True}
    if key in ("rating", "prep_time", "acc_time", "aov", "freq"):
        return {"dec": True}
    return {}


def _metric_label(key: str) -> tuple[str, str, str]:
    return METRIC_UK.get(key, (key, "", ""))


def _brand_histogram_chart(key: str, weeks: list[dict], slug: str = "") -> str:
    title, desc, unit = _metric_label(key)
    opts = _metric_opts(key)
    vals = [float(w.get(key, 0)) for w in weeks]
    max_v = max(vals) if vals and max(vals) > 0 else 1.0

    bars = ""
    for w, val in zip(weeks, vals):
        h_pct = max(4, round(val / max_v * 100))
        short_lbl = w["label"].split(" – ")[0]
        suffix = f" {unit}" if unit in ("₴", "хв", "з 5") else ""
        bars += f"""
        <div class="brand-bar-col">
          <div class="brand-bar-val">{_fmt_chart_value(val, opts)}{suffix}</div>
          <div class="hist-bar brand-bar" style="height:{h_pct}%"></div>
          <div class="brand-bar-week">{short_lbl}</div>
        </div>"""

    cid = f"{slug}-brand-{key}" if slug else f"brand-{key}"
    return f"""
    <div class="chart-card brand-chart-card" id="{cid}">
      <h3>{title}</h3>
      <p class="metric-desc">{desc}</p>
      <p class="unit">Одиниця: {unit} · увесь бренд · по тижнях</p>
      <div class="brand-bars-scroll">
        <div class="brand-bars">{bars}</div>
      </div>
    </div>"""


def _location_grouped_chart(
    key: str,
    locations: list[dict], week_labels: list[str], slug: str = "",
) -> str:
    title, desc, unit = _metric_label(key)
    cid = f"{slug}-loc-{key}" if slug else f"loc-{key}"
    opts = _metric_opts(key)
    all_vals = [
        float(loc["weeks"][i].get(key, 0))
        for loc in locations for i in range(len(week_labels))
    ]
    max_v = max(all_vals) if all_vals and max(all_vals) > 0 else 1.0

    rows_html = ""
    for loc in locations:
        bars = ""
        for i, wlabel in enumerate(week_labels):
            val = float(loc["weeks"][i].get(key, 0))
            h_pct = max(4, round(val / max_v * 100))
            cls = f"m{i + 1}"
            short_lbl = wlabel.split(" – ")[0] if " – " in wlabel else wlabel[:5]
            bars += f"""
            <div class="loc-bar-col">
              <div class="loc-bar-val">{_fmt_chart_value(val, opts)}</div>
              <div class="hist-bar {cls}" style="height:{h_pct}%"></div>
              <div class="loc-bar-month">{short_lbl}</div>
            </div>"""
        problem_cls = " loc-row-problem" if loc.get("_problem") else ""
        rows_html += f"""
        <div class="loc-row{problem_cls}">
          <div class="loc-name" title="{loc['name']}">{loc['short_name']}</div>
          <div class="loc-bars">{bars}</div>
        </div>"""

    legend = ""
    for i, lbl in enumerate(week_labels):
        legend += f'<span><i class="leg m{i+1}"></i> {lbl}</span>'

    return f"""
    <div class="chart-card loc-chart-card">
      <h3>{title}</h3>
      <p class="metric-desc">{desc}</p>
      <p class="unit">Одиниця: {unit} · по кожній локації</p>
      <div class="loc-legend">{legend}</div>
      <div class="loc-chart">{rows_html}</div>
    </div>"""


def _problem_locations_html(problems: list[dict], compare_labels: list[str]) -> str:
    if not problems:
        return """
        <div class="problem-none">
          ✅ За ключовими показниками жодна локація не має критичних відхилень
          у останньому повному тижні. Продовжуйте тримати якість та доступність.
        </div>"""

    cards = ""
    for p in problems:
        issues_li = "".join(f"<li>{issue}</li>" for issue in p["issues"])
        prev_w, last_w = p["prev"], p["last"]
        cards += f"""
        <div class="problem-loc-card">
          <div class="problem-loc-header">
            <span class="problem-badge">⚠️ Потребує уваги</span>
            <h3>{p['name']}</h3>
          </div>
          <div class="problem-kpi-row">
            <span>Замовлення: <strong>{prev_w['orders']}</strong> → <strong>{last_w['orders']}</strong></span>
            <span>Доступність: <strong>{prev_w['avail']:.1f}%</strong> → <strong>{last_w['avail']:.1f}%</strong></span>
            <span>Рейтинг: <strong>{prev_w['rating']:.2f}</strong> → <strong>{last_w['rating']:.2f}</strong></span>
          </div>
          <ul class="problem-list">{issues_li}</ul>
        </div>"""
    hint = " vs ".join(compare_labels) if len(compare_labels) == 2 else "останні 2 тижні"
    return f'<p class="section-hint" style="margin-top:-8px">Порівняння: {hint}</p><div class="problem-locs-grid">{cards}</div>'


def _insight_html(insights: list[dict]) -> str:
    icons = {"positive": "✅", "warning": "⚠️", "info": "ℹ️"}
    return "\n".join(
        f"""<div class="insight-card {ins['type']}">
          <div class="insight-title">{icons.get(ins['type'], '•')} {ins['title']}</div>
          <p>{ins['text']}</p>
        </div>"""
        for ins in insights
    )


def generate_html(data: dict, cfg: dict) -> str:
    weeks = data["weeks"]
    locations = data["locations"]
    compare_labels = data["compare_labels"]
    items = data["top_items"]
    period = data["period_label"]
    gen = data["generated_at"]
    header_title = cfg["header_title"]
    city_uk = cfg["city_uk"]
    slug = cfg["slug"]
    n_locations = len(locations)
    loc_word = "точка" if n_locations == 1 else ("точки" if n_locations <= 4 else "точок")

    problems = analyze_problem_locations(locations)
    problem_names = {p["name"] for p in problems}
    for loc in locations:
        loc["_problem"] = loc["name"] in problem_names

    insights = build_insights(weeks, problems)

    brand_charts_html = ""
    for section_title, metric_keys in CHART_SECTIONS:
        brand_charts_html += f'<div class="section-title">{section_title}</div>'
        brand_charts_html += '<p class="section-hint">Динаміка по тижнях · увесь бренд (усі локації разом)</p>'
        brand_charts_html += '<div class="charts-grid brand-charts-grid">'
        for key in metric_keys:
            brand_charts_html += _brand_histogram_chart(key, weeks, slug)
        brand_charts_html += "</div>"

    loc_charts_html = ""
    cmp_hint = " · ".join(compare_labels) if compare_labels else "останні 2 тижні"
    for section_title, metric_keys in LOCATION_CHART_SECTIONS:
        loc_charts_html += f'<div class="section-title subsection">{section_title}</div>'
        loc_charts_html += (
            f'<p class="section-hint">Порівняння останніх двох повних тижнів ({cmp_hint}) · по кожній локації</p>'
        )
        loc_charts_html += '<div class="charts-grid loc-charts-grid">'
        for key in metric_keys:
            loc_charts_html += _location_grouped_chart(key, locations, compare_labels, slug)
        loc_charts_html += "</div>"

    loc_collapsible = f"""
  <details class="loc-collapse">
    <summary>
      <span class="collapse-icon">▶</span>
      Деталізація по окремих локаціях
      <span class="collapse-hint">натисніть, щоб розгорнути</span>
    </summary>
    <div class="loc-collapse-body">
      <div class="section-title">⚠️ Проблемні локації</div>
      <p class="section-hint">Точки з відхиленнями в останньому повному тижні порівняно з попереднім</p>
      {_problem_locations_html(problems, compare_labels)}
      {loc_charts_html}
    </div>
  </details>"""

    items_rows = "".join(
        f"""<tr>
          <td class="rank">{it['rank']}</td>
          <td><strong>{it['name']}</strong></td>
          <td class="num">{it['qty']:,}</td>
          <td class="num">{it['revenue']:,} ₴</td>
          <td class="num">{it['avg_price']:.0f} ₴</td>
        </tr>""".replace(",", "\u202f")
        for it in items
    )

    last = weeks[-1] if weeks else {}
    kpi_block = f"""
    <div class="kpi-grid">
      <div class="kpi-card"><div class="kpi-label">Загальні продажі (останній тиждень)</div>
        <div class="kpi-value">{last.get('gross',0):,.0f} ₴</div></div>
      <div class="kpi-card"><div class="kpi-label">Доставлені замовлення</div>
        <div class="kpi-value">{last.get('orders',0)}</div></div>
      <div class="kpi-card"><div class="kpi-label">Тижнів на платформі</div>
        <div class="kpi-value">{data['total_weeks']}</div></div>
      <div class="kpi-card"><div class="kpi-label">Локацій / під увагою</div>
        <div class="kpi-value">{n_locations} / {len(problems)}</div></div>
    </div>""".replace(",", "\u202f")

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>{header_title} — WBR · {city_uk} · {period}</title>
  <style>
    :root {{
      --green:#34D186; --green-darker:#0d8a52;
      --black:#0d0d0d; --gray-700:#4a4a4a; --gray-400:#9a9a9a; --gray-100:#f5f5f5;
      --positive:#1aad6a; --warning:#e67e22; --info:#2980b9;
      --problem-bg:#fff8f0;
    }}
    *{{margin:0;padding:0;box-sizing:border-box}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;
      font-size:14px;line-height:1.55;color:#1a1a1a;background:var(--gray-100)}}
    .header{{background:var(--black);padding:28px 40px;display:flex;align-items:center;
      justify-content:space-between;border-bottom:4px solid var(--green);flex-wrap:wrap;gap:16px}}
    .header-logo{{display:flex;align-items:center;gap:14px}}
    .bolt-logo{{width:44px;height:44px;background:var(--green);border-radius:10px;
      display:flex;align-items:center;justify-content:center}}
    .header-title h1{{font-size:22px;font-weight:700;color:#fff}}
    .header-title p{{font-size:11px;color:var(--green);text-transform:uppercase;
      letter-spacing:1.2px;font-weight:600;margin-top:4px}}
    .header-meta{{text-align:right;color:var(--gray-400);font-size:12px;line-height:1.9}}
    .header-meta strong{{color:var(--green)}}
    .container{{max-width:1280px;margin:0 auto;padding:32px 40px}}
    .period-bar{{background:#fff;border-radius:12px;padding:16px 24px;margin-bottom:28px;
      display:flex;align-items:center;gap:12px;flex-wrap:wrap;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .section-title{{font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;
      color:var(--gray-700);padding-bottom:10px;border-bottom:2px solid var(--green);margin:28px 0 10px}}
    .section-hint{{font-size:12px;color:var(--gray-400);margin-bottom:14px}}
    .kpi-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:14px;margin-bottom:8px}}
    .kpi-card{{background:#fff;border-radius:12px;padding:16px 18px;border-top:3px solid var(--green);
      box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .kpi-label{{font-size:10px;font-weight:700;text-transform:uppercase;color:var(--gray-400);margin-bottom:4px}}
    .kpi-value{{font-size:22px;font-weight:700}}
    .charts-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(420px,1fr));gap:20px;margin-bottom:12px}}
    .chart-card{{background:#fff;border-radius:12px;padding:18px 22px;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .chart-card h3{{font-size:13px;font-weight:700;color:var(--gray-700);margin-bottom:4px}}
    .metric-desc{{font-size:12px;color:var(--gray-700);margin-bottom:6px;line-height:1.45}}
    .chart-card .unit{{font-size:10px;color:var(--gray-400);margin-bottom:10px}}
    .brand-bars-scroll{{overflow-x:auto;padding-bottom:6px}}
    .brand-bars{{display:flex;gap:10px;align-items:flex-end;min-height:130px;padding-top:8px}}
    .brand-bar-col{{display:flex;flex-direction:column;align-items:center;min-width:52px;flex-shrink:0;height:120px;justify-content:flex-end}}
    .brand-bar-val{{font-size:9px;font-weight:700;color:var(--gray-700);margin-bottom:4px;text-align:center;max-width:64px;line-height:1.2}}
    .brand-bar-week{{font-size:9px;color:var(--gray-400);margin-top:4px;text-align:center}}
    .hist-bar.brand-bar{{width:36px;background:linear-gradient(180deg,var(--green) 0%,var(--green-darker) 100%)}}
    .loc-collapse{{background:#fff;border-radius:12px;box-shadow:0 1px 4px rgba(0,0,0,.06);margin:28px 0}}
    .loc-collapse summary{{list-style:none;cursor:pointer;padding:18px 24px;font-size:14px;font-weight:700;
      color:var(--gray-700);display:flex;align-items:center;gap:10px;user-select:none}}
    .loc-collapse summary::-webkit-details-marker{{display:none}}
    .loc-collapse[open] summary .collapse-icon{{transform:rotate(90deg)}}
    .collapse-icon{{color:var(--green-darker);transition:transform .2s;font-size:11px}}
    .collapse-hint{{font-size:11px;font-weight:400;color:var(--gray-400);margin-left:auto}}
    .loc-collapse-body{{padding:8px 24px 28px;border-top:1px solid #eee}}
    .subsection{{margin-top:20px}}
    .loc-legend{{display:flex;gap:16px;font-size:11px;color:var(--gray-700);margin-bottom:12px;flex-wrap:wrap}}
    .loc-legend .leg{{display:inline-block;width:12px;height:12px;border-radius:2px;margin-right:4px;vertical-align:middle}}
    .loc-legend .leg.m1{{background:var(--green-darker)}}
    .loc-legend .leg.m2{{background:var(--green)}}
    .loc-chart{{display:flex;flex-direction:column;gap:10px}}
    .loc-row{{display:flex;align-items:flex-end;gap:12px;padding:8px 0;border-bottom:1px solid #f0f0f0}}
    .loc-row-problem{{background:var(--problem-bg);border-radius:8px;padding:8px 10px;
      border-left:3px solid var(--warning)}}
    .loc-name{{width:130px;flex-shrink:0;font-size:11px;font-weight:600;color:var(--gray-700);line-height:1.3}}
    .loc-bars{{display:flex;gap:20px;flex:1;justify-content:flex-start}}
    .loc-bar-col{{display:flex;flex-direction:column;align-items:center;width:56px;height:100px;justify-content:flex-end}}
    .loc-bar-val{{font-size:10px;font-weight:700;color:var(--gray-700);margin-bottom:4px;text-align:center}}
    .loc-bar-month{{font-size:9px;color:var(--gray-400);margin-top:4px}}
    .hist-bar{{width:40px;border-radius:6px 6px 0 0;min-height:6px}}
    .hist-bar.m1{{background:var(--green-darker)}} .hist-bar.m2{{background:var(--green)}}
    .problem-locs-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px;margin-bottom:28px}}
    .problem-loc-card{{background:#fff;border-radius:12px;padding:18px 20px;
      border:1px solid #f0c090;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .problem-loc-header h3{{font-size:14px;margin-top:6px;color:var(--gray-700)}}
    .problem-badge{{font-size:11px;font-weight:700;color:var(--warning)}}
    .problem-kpi-row{{display:flex;flex-wrap:wrap;gap:12px;font-size:11px;color:var(--gray-400);
      margin:10px 0;padding:8px 0;border-top:1px solid #f0f0f0;border-bottom:1px solid #f0f0f0}}
    .problem-kpi-row strong{{color:var(--gray-700)}}
    .problem-list{{margin:10px 0 0 18px;font-size:13px;color:var(--gray-700)}}
    .problem-list li{{margin-bottom:6px}}
    .problem-none{{background:#e6faf2;border-radius:12px;padding:20px;color:var(--positive);font-size:14px}}
    .insights-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:16px;margin-bottom:32px}}
    .insight-card{{background:#fff;border-radius:12px;padding:18px 20px;border-left:4px solid var(--gray-400);
      box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .insight-card.positive{{border-left-color:var(--positive)}}
    .insight-card.warning{{border-left-color:var(--warning)}}
    .insight-card.info{{border-left-color:var(--info)}}
    .insight-title{{font-weight:700;font-size:14px;margin-bottom:8px}}
    .insight-card p{{font-size:13px;color:var(--gray-700)}}
    .table-wrap{{background:#fff;border-radius:12px;overflow:auto;box-shadow:0 1px 4px rgba(0,0,0,.06);margin-bottom:32px}}
    table{{width:100%;border-collapse:collapse;min-width:600px}}
    th{{background:var(--black);color:#fff;font-size:11px;font-weight:700;text-transform:uppercase;padding:12px 16px;text-align:left}}
    th.num,td.num{{text-align:right}}
    td{{padding:8px 16px;border-bottom:1px solid #f0f0f0;font-size:12px}}
    td.week-label{{font-weight:600;white-space:nowrap}}
    td.rank{{color:var(--green-darker);font-weight:700}}
    tr:hover td{{background:#e6faf2}}
    .footer{{background:var(--black);color:var(--gray-400);font-size:11px;padding:22px 40px;text-align:center}}
    .footer span{{color:var(--green)}}
    @media(max-width:700px){{.container{{padding:20px 16px}}.charts-grid{{grid-template-columns:1fr}}.loc-name{{width:90px;font-size:10px}}}}
  </style>
</head>
<body>
<header class="header">
  <div class="header-logo">
    <div class="bolt-logo">
      <svg viewBox="0 0 24 24" width="26" height="26"><path d="M13 2L4.5 13.5H11L10 22L19.5 10.5H13V2Z" fill="#0d0d0d"/></svg>
    </div>
    <div class="header-title">
      <h1>{header_title} · {city_uk}</h1>
      <p>Bolt Food · Щотижневий звіт (WBR)</p>
    </div>
  </div>
  <div class="header-meta">
    <div>Період: <strong>{period}</strong></div>
    <div>Локацій: <strong>{n_locations} {loc_word}</strong></div>
    <div>Оновлено: <strong>{gen}</strong></div>
  </div>
</header>

<div class="container">
  <div class="period-bar">
    <span style="font-size:11px;font-weight:700;text-transform:uppercase;color:var(--gray-700)">Динаміка:</span>
    <span style="font-size:12px;color:var(--gray-700)">{data['total_weeks']} повних тижнів · валюта UAH (₴)</span>
    <span style="margin-left:auto;font-size:11px;color:var(--gray-400)">Останній повний тиждень: {last.get('label','')}</span>
  </div>

  <div class="section-title">Огляд бренду — {last.get('label','')}</div>
  {kpi_block}

  {brand_charts_html}

  {loc_collapsible}

  <div class="section-title">ТОП-10 позицій меню</div>
  <p class="section-hint">За весь період на платформі · найпопулярніші страви</p>
  <div class="table-wrap">
    <table>
      <thead>
        <tr><th>#</th><th>Назва</th><th class="num">Кількість</th>
            <th class="num">Сума</th><th class="num">Сер. ціна</th></tr>
      </thead>
      <tbody>{items_rows or '<tr><td colspan="5">Немає даних</td></tr>'}</tbody>
    </table>
  </div>

  <div class="section-title">Висновки та рекомендації</div>
  <div class="insights-grid">{_insight_html(insights)}</div>
</div>

<footer class="footer">
  <span>Bolt Food</span> · WBR {header_title} · {city_uk} · Автооновлення: щопонеділка о 14:00 (Київ) ·
  <a href="https://github.com/marharytazhytnyk-create/partner-reports/tree/main/WBR%20PINKMAN" style="color:var(--green)">GitHub</a>
</footer>
</body>
</html>"""


def generate_reports_index() -> None:
    links = "".join(
        f'<li><a href="{c["output"]}">{c["header_title"]} · {c["city_uk"]}</a></li>'
        for c in REPORT_CONFIGS
    )
    html = f"""<!DOCTYPE html>
<html lang="uk"><head><meta charset="UTF-8"/>
<title>WBR PINKMAN — навігація</title>
<style>
  body{{font-family:system-ui,sans-serif;max-width:640px;margin:40px auto;padding:0 20px}}
  h1{{font-size:1.4rem}} ul{{line-height:2}} a{{color:#0d8a52;font-weight:600}}
</style></head><body>
<h1>Щотижневі звіти (WBR) — Pinkman / Bella Mozzarella</h1>
<p>Оновлення: щопонеділка о 14:00 (Київ)</p>
<ul>{links}</ul>
</body></html>"""
    (SCRIPT_DIR / "index.html").write_text(html, encoding="utf-8")


def main() -> None:
    if not DATABRICKS_TOKEN:
        print("ERROR: DATABRICKS_TOKEN не задано.", file=sys.stderr)
        sys.exit(1)

    last_w = last_complete_week_start()
    print(f"WBR batch — останній повний тиждень з {last_w} (Пн)\n")

    for cfg in REPORT_CONFIGS:
        title = f"{cfg['header_title']} · {cfg['city_uk']}"
        print(f"▶ {title} ({len(cfg['provider_ids'])} локацій)…")
        try:
            data = fetch_metrics(cfg["provider_ids"], cfg["name_strip"])
            problems = analyze_problem_locations(data["locations"])
            print(f"  тижнів: {data['total_weeks']}, проблемних точок: {len(problems)}")
            out = SCRIPT_DIR / cfg["output"]
            out.write_text(generate_html(data, cfg), encoding="utf-8")
            print(f"  → {out.name}")
        except Exception as e:
            print(f"  ПОМИЛКА: {e}", file=sys.stderr)

    generate_reports_index()
    print("\nНавігація → index.html")


if __name__ == "__main__":
    main()
