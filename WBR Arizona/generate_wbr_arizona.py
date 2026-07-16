#!/usr/bin/env python3
"""
WBR Arizona — щотижневий звіт бренду #ARIZONA (Полтава).
Останні 8 завершених тижнів, HTML українською, валюта UAH.
Автооновлення: щопонеділка о 13:00 (Київ) через GitHub Actions.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import requests

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://bolt-incentives.cloud.databricks.com")
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "0221-081903-9ag4bh69")
BRAND_NAME = "#ARIZONA"
N_WEEKS = 8
SCRIPT_DIR = Path(__file__).parent
OUTPUT_HTML = SCRIPT_DIR / "WBR Arizona.html"
POLL_INTERVAL_S = 4
MAX_POLL_S = 600


def _load_token() -> str:
    token = os.getenv("DATABRICKS_TOKEN", "").strip()
    if token:
        return token

    # OAuth via Databricks CLI (local)
    for profile in ("bolt-incentives-temp", "DEFAULT", "bolt-incentives"):
        try:
            out = subprocess.check_output(
                ["databricks", "auth", "token", "-p", profile],
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=30,
            )
            tok = json.loads(out).get("access_token", "").strip()
            if tok:
                return tok
        except Exception:
            pass

    # PAT from .databrickscfg / .env
    cfg = Path.home() / ".databrickscfg"
    if cfg.exists():
        section = None
        for line in cfg.read_text().splitlines():
            s = line.strip()
            if s.startswith("[") and s.endswith("]"):
                section = s[1:-1]
            elif s.lower().startswith("token") and "=" in s and section:
                tok = s.split("=", 1)[1].strip()
                if tok:
                    return tok

    for env_path in (
        SCRIPT_DIR.parent / "databricks-setup" / ".env",
        Path.home() / "Library" / "CloudStorage"
        / "GoogleDrive-marharyta.zhytnyk@bolt.eu" / "My Drive"
        / "Events project" / "databricks-setup" / ".env",
    ):
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("DATABRICKS_TOKEN="):
                    return line.split("=", 1)[1].strip()
    return ""


DATABRICKS_TOKEN = _load_token()
HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

# Секції: (заголовок, ключі метрик)
CHART_SECTIONS: list[tuple[str, list[str]]] = [
    ("1. Продажі", ["gross", "net", "orders", "aov"]),
    ("2. Операційні показники", [
        "avail", "accept", "refunds",
        "del_time", "acc_time", "prep_time", "wait_time", "c2m_time", "c2e_time",
    ]),
    ("2a. Погані замовлення з вини закладу", ["bad_provider_count", "bad_provider_pct"]),
    ("3. Клієнти та їх поведінка", [
        "active_users", "freq", "new_users", "sessions", "imp_menu", "menu_prod", "rating",
    ]),
    ("4. Знижки", ["discounts", "camp_bolt", "camp_merch"]),
]

SL_ROAS_THRESHOLD = 3.0

PROMO_OFFER_HINTS = {
    "sl": (
        "Щоб отримати більше показів у застосунку Bolt Food, увімкніть спонсоровані оголошення "
        "(Sponsored Listing): у порталі для ресторанів відкрийте «Промоакції» → "
        "«Налаштувати інші акції» → «Запустити платне просування»."
    ),
    "smart": (
        "Щоб залучити більше гостей зі знижкою, підключіть Розумні акції (Smart Promotions): "
        "у порталі для ресторанів відкрийте «Промоакції» → перший блок «Розумні акції». "
        "Bolt ділить витрати на знижку з вами (50/50) для активних, нових і тих, хто давно не замовляв."
    ),
}

BAD_ORDERS_EXPLAIN_UA = (
    "Погані замовлення (Bad Orders) — це доставлені замовлення, з якими у клієнта виникла "
    "проблема: затримка з вини закладу, неповний склад, холодна/неякісна їжа, відмова чи "
    "інша скарга, яку система віднесла до відповідальності ресторану. "
    "Високий показник знижує рейтинг і зменшує шанс, що гість замовить знову."
)

METRIC_UK: dict[str, tuple[str, str, str]] = {
    "gross": ("Gross Sales (загальні продажі)", "Сума вартості доставлених замовлень до знижок", "₴"),
    "net": ("Net Sales (чисті продажі)", "Сума після застосування знижок", "₴"),
    "orders": ("Delivered Orders", "Кількість успішно доставлених замовлень", "шт."),
    "aov": ("AOV (середній чек)", "Середня сума одного доставленого замовлення до знижок", "₴"),
    "avail": ("Availability Rate", "Частка часу, коли заклад був онлайн", "%"),
    "accept": ("Acceptance Rate (прийняття замовлень)", "Частка замовлень, які ви прийняли вчасно", "%"),
    "refunds": ("Orders with Refunds (компенсації)", "Частка замовлень, за які клієнту повернули кошти", "%"),
    "del_time": ("Average Delivery Time", "Середній повний час доставки", "хв"),
    "acc_time": ("Avg. Merchant Acceptance Time", "Середній час прийняття замовлення партнером", "хв"),
    "prep_time": ("Avg. Preparation Time", "Середній час приготування страв", "хв"),
    "wait_time": ("Avg. Courier Wait Time", "Середній час очікування курʼєром видачі", "хв"),
    "c2m_time": ("Avg. Courier to Merchant Time", "Середній час шляху курʼєра до закладу", "хв"),
    "c2e_time": ("Avg. Courier to Eater Time", "Середній час шляху курʼєра до клієнта", "хв"),
    "active_users": ("Active Users", "Унікальні клієнти з доставленим замовленням за тиждень", "осіб"),
    "freq": ("Order Frequency", "Середня кількість замовлень на активного користувача", "зам./корист."),
    "new_users": ("New Users", "Клієнти, які вперше замовили в цьому закладі", "осіб"),
    "sessions": ("Sessions with Impressions", "Перегляди закладу клієнтом у стрічці / пошуку", "сесій"),
    "imp_menu": ("Impression → Menu Viewed", "Частка переглядів, з яких відкрили меню", "%"),
    "menu_prod": ("Menu Viewed → Product Added", "Частка переглядів меню з додаванням у кошик", "%"),
    "rating": ("Average Merchant Rating", "Середня оцінка закладу клієнтами", "з 5"),
    "discounts": ("Total Discounts for Users", "Загальна сума знижок для клієнтів", "₴"),
    "camp_bolt": ("Campaigns Spend by Bolt", "Витрати Bolt на знижки та промо", "₴"),
    "camp_merch": ("Campaigns Spend by Merchant", "Скільки ви вклали у знижки та промо для гостей", "₴"),
    "bad_provider_count": (
        "Погані замовлення з вини закладу",
        "Скільки доставлених замовлень мали проблему, яку система віднесла до відповідальності вашого закладу "
        "(затримка кухні, комплектація, якість тощо)",
        "шт.",
    ),
    "bad_provider_pct": (
        "Погані замовлення з вини закладу (%)",
        "Який відсоток доставлених замовлень був «поганим» через причини з боку вашого закладу. "
        "Чим нижче — тим краще для рейтингу та повторних замовлень",
        "%",
    ),
}

WEEK_BAR_COLORS = [
    "#0a5c38", "#0d8a52", "#12a35f", "#34D186",
    "#5cdb9a", "#7ee0ad", "#a3e8c4", "#c8f0da",
]


# ─── DATES ─────────────────────────────────────────────────────────────────────

def last_n_completed_weeks(n: int = N_WEEKS, ref: datetime.date | None = None) -> list[tuple[datetime.date, datetime.date]]:
    """Останні n повних тижнів Пн–Нд (без поточного незавершеного)."""
    today = ref or datetime.date.today()
    last_sunday = today - datetime.timedelta(days=today.weekday() + 1)
    weeks: list[tuple[datetime.date, datetime.date]] = []
    for i in range(n):
        end = last_sunday - datetime.timedelta(weeks=i)
        start = end - datetime.timedelta(days=6)
        weeks.append((start, end))
    return list(reversed(weeks))


def week_label(start: datetime.date, end: datetime.date) -> str:
    return f"{start.strftime('%d.%m')}–{end.strftime('%d.%m')}"


def week_key(d: datetime.date | str) -> str:
    if isinstance(d, str):
        return str(d)[:10]
    return d.isoformat()


# ─── DATABRICKS ────────────────────────────────────────────────────────────────

def _post(path: str, payload: dict) -> dict:
    r = requests.post(f"{DATABRICKS_HOST}{path}", headers=HEADERS, json=payload, timeout=90)
    r.raise_for_status()
    return r.json()


def _get(path: str, params: dict) -> dict:
    r = requests.get(f"{DATABRICKS_HOST}{path}", headers=HEADERS, params=params, timeout=90)
    r.raise_for_status()
    return r.json()


def ensure_cluster_running() -> None:
    st = _get("/api/2.0/clusters/get", {"cluster_id": CLUSTER_ID})
    state = st.get("state")
    if state == "RUNNING":
        return
    if state in ("TERMINATED", "TERMINATING"):
        _post("/api/2.0/clusters/start", {"cluster_id": CLUSTER_ID})
    deadline = time.time() + 900
    while time.time() < deadline:
        time.sleep(10)
        state = _get("/api/2.0/clusters/get", {"cluster_id": CLUSTER_ID}).get("state")
        print(f"  cluster: {state}")
        if state == "RUNNING":
            return
    raise TimeoutError("Cluster did not start in time")


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


def _sf(v, default=0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _si(v, default=0) -> int:
    return int(round(_sf(v, default)))


def _parse_loc_week(row: list, active_users: int = 0) -> dict:
    orders = _si(row[3])
    gross = _sf(row[4])
    net = _sf(row[5])
    sessions = _si(row[16])
    menu_viewed = _si(row[17])
    au = active_users or orders
    return {
        "orders": orders,
        "gross": round(gross, 0),
        "net": round(net, 0),
        "aov": round(gross / orders, 0) if orders else 0,
        "avail": round(_sf(row[6]), 2),
        "accept": round(_sf(row[7]), 2),
        "refunds": round(_sf(row[8]), 2),
        "del_time": round(_sf(row[9]), 1),
        "acc_time": round(_sf(row[10]), 1),
        "prep_time": round(_sf(row[11]), 1),
        "wait_time": round(_sf(row[12]), 1),
        "c2m_time": round(_sf(row[13]), 1),
        "c2e_time": round(_sf(row[14]), 1),
        "new_users": _si(row[15]),
        "sessions": sessions,
        "imp_menu": round(menu_viewed / sessions * 100, 2) if sessions else 0,
        "menu_prod": round(_sf(row[18]), 2),
        "rating": round(_sf(row[19]), 2),
        "discounts": round(_sf(row[20]), 0),
        "camp_bolt": round(_sf(row[21]), 0),
        "camp_merch": round(_sf(row[22]), 0),
        "active_users": au,
        "freq": round(orders / au, 2) if au else 0,
        "bad_provider_count": 0,
        "bad_provider_pct": 0.0,
    }


EMPTY_WEEK = {
    "orders": 0, "gross": 0, "net": 0, "aov": 0,
    "avail": 0, "accept": 0, "refunds": 0,
    "del_time": 0, "acc_time": 0, "prep_time": 0, "wait_time": 0, "c2m_time": 0, "c2e_time": 0,
    "new_users": 0, "sessions": 0, "imp_menu": 0, "menu_prod": 0, "rating": 0,
    "discounts": 0, "camp_bolt": 0, "camp_merch": 0, "active_users": 0, "freq": 0,
    "bad_provider_count": 0, "bad_provider_pct": 0.0,
}


# ─── FETCH ─────────────────────────────────────────────────────────────────────

def fetch_data() -> dict:
    weeks = last_n_completed_weeks(N_WEEKS)
    global_start = weeks[0][0].isoformat()
    global_end = weeks[-1][1].isoformat()
    week_keys = [w[0].isoformat() for w in weeks]
    week_labels = [week_label(s, e) for s, e in weeks]

    ensure_cluster_running()
    ctx = create_context()
    try:
        loc_rows = run_query(ctx, f"""
        SELECT provider_id, provider_name, brand_name, city_name, zone_name
        FROM ng_delivery_spark.dim_provider_v2
        WHERE brand_name = '{BRAND_NAME}'
        ORDER BY city_name, provider_name
        """)
        if not loc_rows:
            raise RuntimeError(f"Не знайдено локацій для бренду {BRAND_NAME}")

        providers = [
            {
                "provider_id": int(r[0]),
                "name": str(r[1]),
                "city": str(r[3] or ""),
                "zone": str(r[4] or ""),
            }
            for r in loc_rows
        ]
        pids = [p["provider_id"] for p in providers]
        pids_sql = ", ".join(str(p) for p in pids)
        pids_str = ", ".join(f"'{p}'" for p in pids)

        print(f"  локацій: {len(providers)}, період: {global_start} → {global_end}")

        fact_rows = run_query(ctx, f"""
        SELECT
            f.provider_id,
            d.provider_name,
            DATE_FORMAT(DATE_TRUNC('week', f.metric_timestamp_partition), 'yyyy-MM-dd') AS week_start,
            SUM(f.delivered_orders_count) AS orders,
            SUM(f.total_gmv_before_discounts) AS gross,
            SUM(f.total_gmv_after_discounts) AS net,
            AVG(f.provider_active_rate_value) * 100 AS avail,
            AVG(f.provider_acceptance_rate_value) * 100 AS accept,
            AVG(f.customer_refunded_order_rate_value) * 100 AS refunds,
            AVG(f.order_total_minutes_per_order_value) AS del_time,
            AVG(f.provider_acceptance_minutes_per_order_value) AS acc_time,
            AVG(f.provider_preparation_minutes_per_order_value) AS prep_time,
            AVG(f.courier_total_wait_minutes_per_order_value) AS wait_time,
            AVG(f.courier_to_provider_actual_minutes_per_order_value) AS c2m,
            AVG(f.courier_to_eater_actual_minutes_per_order_value) AS c2e,
            SUM(f.users_activated_vendor_count) AS new_users,
            SUM(f.provider_impressions_sessions_count) AS sessions,
            SUM(f.provider_menu_viewed_sessions_count) AS menu_viewed,
            AVG(f.provider_product_added_from_menu_viewed_rate_value) * 100 AS menu_prod,
            AVG(f.provider_rating_per_order_value) AS rating,
            SUM(f.total_campaign_discount) AS discounts,
            SUM(f.total_campaign_spend_bolt) AS camp_bolt,
            SUM(f.total_campaign_spend_provider) AS camp_merch
        FROM ng_delivery_spark.fact_provider_weekly f
        JOIN ng_delivery_spark.dim_provider_v2 d ON f.provider_id = d.provider_id
        WHERE f.provider_id IN ({pids_sql})
          AND f.metric_timestamp_partition >= '{global_start}'
          AND f.metric_timestamp_partition <= '{global_end}'
        GROUP BY 1, 2, 3
        ORDER BY d.provider_name, 3
        """)

        users_rows = run_query(ctx, f"""
        SELECT
            entity_id AS provider_id,
            DATE_FORMAT(DATE_TRUNC('week', metric_timestamp_partition), 'yyyy-MM-dd') AS week_start,
            SUM(provider_deliveries_unique_user_count) AS active_users
        FROM ng_delivery_spark.int_provider_metrics_non_additive
        WHERE entity_id IN ({pids_str})
          AND timeframe_name = 'week'
          AND metric_timestamp_partition >= '{global_start}'
          AND metric_timestamp_partition <= '{global_end}'
        GROUP BY 1, 2
        ORDER BY 1, 2
        """)

        bad_rows = run_query(ctx, f"""
        SELECT
            o.provider_id,
            DATE_FORMAT(DATE_TRUNC('week', o.created_date), 'yyyy-MM-dd') AS week_start,
            SUM(CASE WHEN o.state = 'delivered' THEN 1 ELSE 0 END) AS delivered,
            SUM(CASE
                WHEN f.is_bad_order = true
                 AND LOWER(COALESCE(a.bad_order_actor_at_fault, '')) = 'provider'
                THEN 1 ELSE 0
            END) AS bad_provider
        FROM ng_delivery_spark.delivery_order_order o
        INNER JOIN ng_delivery_spark.fact_order_delivery f ON f.order_id = o.id
        LEFT JOIN ng_delivery_spark.int_order_bad_order_attribution a ON a.order_id = o.id
        WHERE o.provider_id IN ({pids_sql})
          AND o.created_date >= '{global_start}'
          AND o.created_date <= '{global_end}'
        GROUP BY 1, 2
        ORDER BY 1, 2
        """)

        promo_rows = run_query(ctx, f"""
        WITH smart_active AS (
            SELECT DISTINCT provider_id
            FROM ng_delivery_spark.delivery_smart_promotion_log
            WHERE state = 'active'
              AND promotion_type = 'smart_promotion'
              AND (end IS NULL OR end >= current_timestamp())
              AND (start IS NULL OR start <= current_timestamp())
        ),
        sl_active AS (
            SELECT provider_id
            FROM ng_delivery_spark.fact_provider_weekly f
            WHERE CAST(f.metric_timestamp_local AS DATE) >= date_sub(current_date(), 7)
              AND f.provider_id IN ({pids_sql})
            GROUP BY provider_id
            HAVING SUM(COALESCE(f.sponsored_listing_duration_hours, 0)) > 0
        )
        SELECT
            p.provider_id,
            CASE WHEN s.provider_id IS NOT NULL THEN 1 ELSE 0 END AS has_smart_promotion,
            CASE WHEN sl.provider_id IS NOT NULL THEN 1 ELSE 0 END AS has_sponsored_listing
        FROM ng_delivery_spark.dim_provider_v2 p
        LEFT JOIN smart_active s ON p.provider_id = s.provider_id
        LEFT JOIN sl_active sl ON p.provider_id = sl.provider_id
        WHERE p.provider_id IN ({pids_sql})
        """)
    finally:
        destroy_context(ctx)

    active_map: dict[tuple[int, str], int] = {}
    for row in users_rows:
        active_map[(int(row[0]), week_key(str(row[1])))] = _si(row[2])

    bad_map: dict[tuple[int, str], tuple[int, float]] = {}
    for row in bad_rows:
        pid = int(row[0])
        wk = week_key(str(row[1]))
        delivered = _si(row[2])
        bad_n = _si(row[3])
        bad_pct = round(bad_n / delivered * 100, 2) if delivered else 0.0
        bad_map[(pid, wk)] = (bad_n, bad_pct)

    promo_map: dict[int, dict] = {}
    for row in promo_rows:
        promo_map[int(row[0])] = {
            "has_smart_promotion": bool(int(row[1] or 0)),
            "has_sponsored_listing": bool(int(row[2] or 0)),
        }

    by_pid: dict[int, dict] = {
        p["provider_id"]: {**p, "by_week": {}} for p in providers
    }
    for row in fact_rows:
        pid = int(row[0])
        wk = week_key(str(row[2]))
        if pid not in by_pid:
            continue
        au = active_map.get((pid, wk), 0)
        rec = _parse_loc_week(row, au)
        bad_n, bad_pct = bad_map.get((pid, wk), (0, 0.0))
        rec["bad_provider_count"] = bad_n
        rec["bad_provider_pct"] = bad_pct
        by_pid[pid]["by_week"][wk] = rec

    locations: list[dict] = []
    for pid in sorted(by_pid.keys(), key=lambda x: by_pid[x]["name"].lower()):
        loc = by_pid[pid]
        weeks_data = []
        for wk, label in zip(week_keys, week_labels):
            rec = dict(loc["by_week"].get(wk, EMPTY_WEEK))
            rec["week_key"] = wk
            rec["label"] = label
            weeks_data.append(rec)
        loc["weeks"] = weeks_data
        promo = promo_map.get(pid, {})
        loc["has_smart_promotion"] = promo.get("has_smart_promotion", False)
        loc["has_sponsored_listing"] = promo.get("has_sponsored_listing", False)
        locations.append(loc)

    # Медіани міста для рекомендацій промо (Полтава)
    conv_vals = []
    impr_per_loc = []
    for loc in locations:
        recent = loc["weeks"][-4:]
        orders = sum(w["orders"] for w in recent)
        sessions = sum(w["sessions"] for w in recent)
        if sessions:
            conv_vals.append(orders / sessions * 100)  # approx imp→order via sessions
        if recent:
            impr_per_loc.append(sum(w["sessions"] for w in recent) / len(recent))
    city_median_conv = sorted(conv_vals)[len(conv_vals) // 2] if conv_vals else 2.5
    city_median_impr = sorted(impr_per_loc)[len(impr_per_loc) // 2] if impr_per_loc else 1.0

    # Brand totals per week
    brand_weeks = []
    for i, (wk, label) in enumerate(zip(week_keys, week_labels)):
        agg = dict(EMPTY_WEEK)
        for loc in locations:
            w = loc["weeks"][i]
            for k in ("orders", "gross", "net", "new_users", "sessions",
                      "discounts", "camp_bolt", "camp_merch", "active_users",
                      "bad_provider_count"):
                agg[k] += w.get(k, 0)
        agg["bad_provider_pct"] = round(
            agg["bad_provider_count"] / agg["orders"] * 100, 2
        ) if agg["orders"] else 0.0
        # weighted / averaged rates
        weighted_keys = [
            ("avail", "orders"), ("accept", "orders"), ("refunds", "orders"),
            ("del_time", "orders"), ("acc_time", "orders"), ("prep_time", "orders"),
            ("wait_time", "orders"), ("c2m_time", "orders"), ("c2e_time", "orders"),
            ("rating", "orders"), ("imp_menu", "sessions"), ("menu_prod", "sessions"),
        ]
        for metric, weight_key in weighted_keys:
            total_w = sum(loc["weeks"][i].get(weight_key, 0) for loc in locations)
            if total_w:
                agg[metric] = round(
                    sum(loc["weeks"][i].get(metric, 0) * loc["weeks"][i].get(weight_key, 0)
                        for loc in locations) / total_w,
                    2,
                )
        agg["aov"] = round(agg["gross"] / agg["orders"], 0) if agg["orders"] else 0
        agg["freq"] = round(agg["orders"] / agg["active_users"], 2) if agg["active_users"] else 0
        agg["week_key"] = wk
        agg["label"] = label
        brand_weeks.append(agg)

    return {
        "locations": locations,
        "brand_weeks": brand_weeks,
        "week_keys": week_keys,
        "week_labels": week_labels,
        "period_label": f"{week_labels[0]} — {week_labels[-1]}",
        "period_dates": f"{weeks[0][0].strftime('%d.%m.%Y')} — {weeks[-1][1].strftime('%d.%m.%Y')}",
        "generated_at": datetime.datetime.now().strftime("%d.%m.%Y %H:%M"),
        "city": locations[0]["city"] if locations else "",
        "city_median_conv": round(city_median_conv, 2),
        "city_median_impr": round(city_median_impr, 0),
    }


# ─── ANALYSIS ──────────────────────────────────────────────────────────────────

def _loc_recent_metrics(loc: dict, n: int = 4) -> dict:
    recent = loc["weeks"][-n:]
    orders = sum(w["orders"] for w in recent)
    gross = sum(w["gross"] for w in recent)
    net = sum(w["net"] for w in recent)
    sessions = sum(w["sessions"] for w in recent)
    conv = round(orders / sessions * 100, 2) if sessions else 0.0
    impr_per_week = sessions / len(recent) if recent else 0
    return {
        "orders": orders,
        "gross": gross,
        "net": net,
        "sessions": sessions,
        "conv_imp_to_order_pct": conv,
        "impressions_per_week": impr_per_week,
        "cp_margin_pct": round((net / gross * 100) if gross else 0, 1),
    }


def _predicted_roas(conv: float, city_conv: float, impr_per_week: float, city_impr: float) -> float:
    visibility_gap = min(max(city_impr / max(impr_per_week, 1), 1.0), 2.5)
    conv_factor = min(max(conv / max(city_conv, 0.5), 0.7), 1.5)
    return round(min(5.0 * conv_factor * (0.7 + 0.3 * visibility_gap), 15.0), 1)


def recommend_promo(loc: dict, city_median_conv: float, city_median_impr: float) -> tuple[str, str]:
    """Логіка з звіту «Потенційне підключення» / Допоміжні матеріали."""
    m = _loc_recent_metrics(loc)
    has_smart = loc.get("has_smart_promotion", False)
    has_sl = loc.get("has_sponsored_listing", False)
    conv = m["conv_imp_to_order_pct"]
    gross = m["gross"]
    orders = m["orders"]
    cp = m["cp_margin_pct"]
    roas = _predicted_roas(conv, city_median_conv, m["impressions_per_week"], city_median_impr)
    vis_gap = round(city_median_impr / max(m["impressions_per_week"], 1), 2)

    if cp < 0:
        product = "Спочатку якість і доступність"
        reason = (
            "Спочатку варто стабілізувати якість страв і час онлайн у застосунку. "
            "Після цього має сенс спробувати спонсоровані оголошення для більшої видимості."
        )
    elif not has_sl and conv >= max(city_median_conv, 2.5) and vis_gap >= 1.25 and roas >= SL_ROAS_THRESHOLD:
        product = "Спонсоровані оголошення"
        reason = (
            f"Гості часто замовляють після перегляду меню ({conv:.1f}%), але заклад бачать рідше "
            f"за типовий рівень у місті. Платне просування допоможе зʼявитися частіше в пошуку."
        )
    elif conv < 2.0 and gross >= 150000:
        product = "Спонсоровані оголошення → Розумні акції"
        reason = (
            "Продажі хороші, але з показів у стрічці менше замовлень, ніж могло б бути. "
            "Спочатку підсильте видимість спонсорованими оголошеннями, потім підключіть Розумні акції."
        )
    elif conv >= 3.0 and cp >= 10 and vis_gap < 1.2:
        product = "Розумні акції"
        reason = (
            "Гості добре конвертують перегляди в замовлення. "
            "Розумні акції допоможуть залучити більше постійних і нових клієнтів."
        )
    elif gross >= 400000 or orders >= 400:
        product = "Розумні акції + спонсоровані оголошення"
        reason = (
            "У вас сильний обсяг продажів — поєднання більшої видимості та акцій "
            "для окремих груп гостей дасть найкращий ефект."
        )
    elif orders >= 80:
        product = "Розумні акції"
        reason = (
            "Замовлень уже стабільно багато — Розумні акції для активних і нових гостей "
            "допоможуть швидше наростити продажі."
        )
    elif roas >= SL_ROAS_THRESHOLD and vis_gap >= 1.1:
        product = "Спонсоровані оголошення"
        reason = "Варто почати з платного просування в пошуку, щоб більше гостей побачили заклад."
    else:
        product = "Спонсоровані оголошення"
        reason = (
            "Почніть зі спонсорованих оголошень, щоб збільшити покази. "
            "Після зростання видимості додайте Розумні акції."
        )

    if has_smart and has_sl:
        return product, (
            "У вас уже підключені і Розумні акції, і спонсоровані оголошення — "
            "слідкуйте за результатом і тримайте активними вигідні кампанії."
        )
    if has_smart and ("Розумн" in product or "Smart" in product) and "оголошен" not in product.lower():
        return product, f"Розумні акції вже увімкнені. {reason}"
    if has_sl and ("оголошен" in product.lower() or product.startswith("Sponsored")) and "Розумн" not in product:
        return product, f"Спонсоровані оголошення вже увімкнені. {reason}"
    return product, reason


def _promo_advice(loc: dict, city_median_conv: float, city_median_impr: float) -> list[str]:
    product, reason = recommend_promo(loc, city_median_conv, city_median_impr)
    tips: list[str] = [f"Що варто підключити: {product}. {reason}"]
    if not loc.get("has_sponsored_listing") and ("оголошен" in product.lower() or "Sponsored" in product or "Listing" in product):
        tips.append(PROMO_OFFER_HINTS["sl"])
    if not loc.get("has_smart_promotion") and ("Розумн" in product or "Smart" in product):
        tips.append(PROMO_OFFER_HINTS["smart"])
    if loc.get("has_smart_promotion") and loc.get("has_sponsored_listing"):
        tips.append(
            "Тримайте разом спонсоровані оголошення (щоб вас частіше бачили) "
            "і Розумні акції (щоб більше гостей оформлювали замовлення)."
        )
    return tips


def _pct_change(old: float, new: float) -> float | None:
    if old == 0:
        return None
    return (new - old) / old * 100


def analyze_location(loc: dict, city_median_conv: float = 2.5, city_median_impr: float = 1000.0) -> dict:
    weeks = loc["weeks"]
    if len(weeks) < 2:
        return {"name": loc["name"], "severity": 0, "issues": [], "advice": [], "trend": "stable"}

    prev, last = weeks[-2], weeks[-1]
    first = weeks[0]
    issues: list[str] = []
    advice: list[str] = []
    severity = 0

    o_chg = _pct_change(prev["orders"], last["orders"])
    o_trend = _pct_change(first["orders"], last["orders"])

    if last["orders"] < 15:
        issues.append(
            f"За останній тиждень дуже мало замовлень — лише {last['orders']} "
            f"(тиждень раніше було {prev['orders']})."
        )
        advice.append(
            "Перевірте години роботи в застосунку, головне фото та опис меню. "
            "Переконайтеся, що заклад видно в зоні доставки в обід і ввечері."
        )
        severity += 3
    elif o_chg is not None and o_chg <= -25:
        issues.append(
            f"Різке падіння замовлень порівняно з минулим тижнем: "
            f"{prev['orders']} → {last['orders']} ({o_chg:.0f}%)."
        )
        advice.append(
            "Перегляньте, чи не було довгих пауз офлайн, змін у меню чи акцій. "
            "Порівняйте з тижнем, коли замовлень було більше, і поверніть те, що працювало."
        )
        severity += 2
    elif o_chg is not None and o_chg <= -10:
        issues.append(
            f"Замовлень стало менше, ніж тиждень тому: "
            f"{prev['orders']} → {last['orders']} ({o_chg:.0f}%)."
        )
        severity += 1

    if o_chg is not None and o_chg >= 20:
        issues.append(
            f"Гарне зростання замовлень порівняно з минулим тижнем: "
            f"{prev['orders']} → {last['orders']} (+{o_chg:.0f}%)."
        )

    if o_trend is not None and o_trend <= -30:
        issues.append(
            f"За 8 тижнів замовлення зменшилися з {first['orders']} до {last['orders']} ({o_trend:.0f}%)."
        )
        advice.append(
            "Варто посилити видимість і акції (Розумні акції / спонсоровані оголошення) "
            "та перевірити, щоб заклад був онлайн у пікові години."
        )
        severity += 2

    if last["avail"] < 90:
        issues.append(
            f"Заклад був доступний для замовлення лише {last['avail']:.1f}% часу — "
            "гості часто не бачили вас онлайн."
        )
        advice.append(
            "Тримайте заклад увімкненим в обід і ввечері. "
            "Уникайте довгих ручних пауз, якщо кухня може приймати замовлення."
        )
        severity += 2

    if last["accept"] < 97:
        issues.append(f"Не всі замовлення приймаються вчасно — {last['accept']:.1f}%.")
        advice.append(
            "Приймайте замовлення якомога швидше в застосунку для ресторанів "
            "(орієнтир — менше 1 хвилини)."
        )
        severity += 2

    if last["refunds"] >= 5:
        issues.append(
            f"Часто доводиться компенсувати клієнтам кошти — {last['refunds']:.1f}% замовлень."
        )
        advice.append(
            "Перевірте, чи всі позиції в меню актуальні, чи правильно збирають замовлення "
            "і чи встигає кухня в зазначений час приготування."
        )
        severity += 2
    elif last["refunds"] >= 3 and last["refunds"] > prev["refunds"] + 1:
        issues.append(
            f"Компенсацій клієнтам стало більше: {prev['refunds']:.1f}% → {last['refunds']:.1f}%."
        )
        severity += 1

    if last["prep_time"] >= 30:
        issues.append(f"Страви готуються довго — у середньому {last['prep_time']:.1f} хв.")
        advice.append(
            "У пікові години розподіліть навантаження на кухні або оновіть час приготування "
            "у кабінеті, щоб курʼєр не чекав зайве."
        )
        severity += 1

    if last["acc_time"] >= 2.5:
        issues.append(f"Замовлення приймаються повільно — {last['acc_time']:.1f} хв.")
        advice.append(
            "Призначте відповідального за планшет і увімкніть звукові сповіщення, "
            "щоб не пропускати нові замовлення."
        )
        severity += 1

    if last["del_time"] >= 50:
        issues.append(
            f"Повний час доставки для гостя довгий — {last['del_time']:.1f} хв. "
            "Часто це повʼязано з часом приготування або очікуванням на видачі."
        )
        severity += 1

    if last["rating"] and last["rating"] < 4.4:
        issues.append(f"Середня оцінка закладу нижча за комфортну — {last['rating']:.2f} з 5.")
        advice.append(
            "Подивіться останні низькі відгуки: комплектація, температура страв, "
            "запізнення. Виправте найчастіші причини."
        )
        severity += 2

    if last["imp_menu"] < 8 and last["sessions"] > 800:
        issues.append(
            f"З усіх переглядів у стрічці лише {last['imp_menu']:.1f}% відкривають меню "
            f"(переглядів: {last['sessions']})."
        )
        advice.append(
            "Оновіть головне фото, назву та бейджі акцій — щоб гість охочіше натискав на заклад."
        )
        severity += 1

    if last["menu_prod"] < 25 and last["sessions"] > 200:
        issues.append(
            f"Після відкриття меню мало хто додає страви в кошик — {last['menu_prod']:.1f}%."
        )
        advice.append(
            "Підсвітіть хіти та комбо на початку меню, додайте зрозумілі описи й актуальні ціни."
        )
        severity += 1

    disc_chg = _pct_change(prev["discounts"], last["discounts"])
    if o_chg is not None and o_chg < -10 and disc_chg is not None and disc_chg < -20:
        issues.append(
            f"Разом із падінням замовлень зменшилася сума знижок для гостей "
            f"({prev['discounts']:,.0f} → {last['discounts']:,.0f} ₴).".replace(",", "\u202f")
        )
        advice.append(
            "На слабкі дні варто знову запропонувати гостям акцію через Розумні акції "
            "або власну знижку в меню — це часто повертає замовлення."
        )
        severity += 1

    if o_chg is not None and o_chg > 15 and last["camp_bolt"] + last["camp_merch"] > prev["camp_bolt"] + prev["camp_merch"]:
        issues.append("Зростання замовлень збіглося з активнішими акціями для гостей — хороший сигнал.")

    if last["bad_provider_pct"] >= 12:
        issues.append(
            f"Багато поганих замовлень через причини з боку закладу — {last['bad_provider_pct']:.1f}% "
            f"({last['bad_provider_count']} зам.)."
        )
        advice.append(
            "Розіберіть, чому гості залишаються незадоволеними: довге приготування, "
            "неповний склад, відмова від замовлення чи низька оцінка страв. "
            "Виправлення цих причин покращить рейтинг і повторні замовлення."
        )
        severity += 2
    elif last["bad_provider_pct"] >= 8 and last["bad_provider_pct"] > prev["bad_provider_pct"] + 2:
        issues.append(
            f"Поганих замовлень з вини закладу стало більше: "
            f"{prev['bad_provider_pct']:.1f}% → {last['bad_provider_pct']:.1f}%."
        )
        advice.append(
            "Зверніть увагу на якість збірки та час видачі — це найчастіші причини скарг гостей."
        )
        severity += 1

    if prev["bad_provider_count"] and last["bad_provider_count"] == 0:
        issues.append(
            "За останній тиждень не було поганих замовлень з вини закладу — відмінний результат."
        )

    advice.extend(_promo_advice(loc, city_median_conv, city_median_impr))

    if not advice and severity >= 1:
        advice.append(
            "Тримайте заклад онлайн понад 95% часу в робочі години, "
            "приймайте замовлення швидко та слідкуйте за якістю видачі."
        )

    trend = "stable"
    if o_chg is not None:
        if o_chg >= 10:
            trend = "up"
        elif o_chg <= -10:
            trend = "down"

    return {
        "name": loc["name"],
        "provider_id": loc["provider_id"],
        "zone": loc.get("zone", ""),
        "severity": severity,
        "issues": issues,
        "advice": advice,
        "trend": trend,
        "prev": prev,
        "last": last,
        "o_chg": o_chg,
    }


def build_brand_insights(brand_weeks: list[dict], analyses: list[dict]) -> list[dict]:
    if len(brand_weeks) < 2:
        return []
    a, b = brand_weeks[-2], brand_weeks[-1]
    insights: list[dict] = []

    def add(kind: str, title: str, text: str):
        insights.append({"type": kind, "title": title, "text": text})

    g_chg = _pct_change(a["gross"], b["gross"])
    o_chg = _pct_change(a["orders"], b["orders"])
    if g_chg is not None and g_chg > 5:
        add("positive", "Продажі зростають",
            f"Загальні продажі: {a['gross']:,.0f} → {b['gross']:,.0f} ₴ ({g_chg:+.0f}%). "
            f"Замовлень: {o_chg:+.0f}% до минулого тижня.".replace(",", "\u202f"))
    elif g_chg is not None and g_chg < -5:
        add("warning", "Продажі знизилися",
            f"Загальні продажі: {a['gross']:,.0f} → {b['gross']:,.0f} ₴ ({g_chg:.0f}%). "
            f"Замовлень: {o_chg:.0f}% до минулого тижня.".replace(",", "\u202f"))

    if b["avail"] < 92:
        add("warning", "Заклади часто офлайн",
            f"У середньому мережа була доступна для замовлення {b['avail']:.1f}% часу. "
            "Коли заклад вимкнений, гості не можуть замовити.")

    if b["refunds"] > a["refunds"] + 1:
        add("warning", "Більше компенсацій клієнтам",
            f"Частка замовлень із поверненням коштів: {a['refunds']:.1f}% → {b['refunds']:.1f}%.")

    problem = [x for x in analyses if x["severity"] >= 2]
    if problem:
        names = ", ".join(p["name"] for p in problem[:4])
        add("warning", "Локації, на які варто звернути увагу",
            f"Відкрийте деталі по точках: {names}.")

    growing = [x for x in analyses if x["trend"] == "up"]
    if growing:
        add("positive", "Локації з ростом",
            "Замовлень стало більше: " + ", ".join(g["name"] for g in growing[:5]) + ".")

    if not insights:
        add("info", "Стабільний тиждень",
            "Основні показники без різких змін. Продовжуйте тримати якість і доступність.")
    return insights


# ─── HTML ──────────────────────────────────────────────────────────────────────

def _fmt(val: float, key: str) -> str:
    if key in ("avail", "accept", "refunds", "imp_menu", "menu_prod", "bad_provider_pct"):
        return f"{val:.1f}%"
    if key in ("rating", "freq"):
        return f"{val:.2f}"
    if key in ("del_time", "acc_time", "prep_time", "wait_time", "c2m_time", "c2e_time"):
        return f"{val:.1f}"
    if key in ("gross", "net", "aov", "discounts", "camp_bolt", "camp_merch"):
        return f"{val:,.0f}".replace(",", "\u202f")
    return f"{val:,.0f}".replace(",", "\u202f")


def _histogram(key: str, weeks: list[dict], chart_id: str) -> str:
    title, desc, unit = METRIC_UK[key]
    vals = [float(w.get(key, 0)) for w in weeks]
    max_v = max(vals) if vals and max(vals) > 0 else 1.0
    bars = ""
    for i, (w, val) in enumerate(zip(weeks, vals)):
        h = max(4, round(val / max_v * 100))
        color = WEEK_BAR_COLORS[i % len(WEEK_BAR_COLORS)]
        bars += f"""
        <div class="bar-col">
          <div class="bar-val">{_fmt(val, key)}</div>
          <div class="bar" style="height:{h}%;background:{color}"></div>
          <div class="bar-lbl">{w['label']}</div>
        </div>"""
    return f"""
    <div class="chart-card" id="{chart_id}">
      <h3>{title}</h3>
      <p class="metric-desc">{desc}</p>
      <p class="unit">Одиниця: {unit} · 8 тижнів</p>
      <div class="bars-scroll"><div class="bars">{bars}</div></div>
    </div>"""


def _location_analysis_block(analysis: dict) -> str:
    """Блок аналізу та порад для однієї локації (всередині розгорнутої картки)."""
    prev, last = analysis["prev"], analysis["last"]
    sev = "high" if analysis["severity"] >= 3 else ("mid" if analysis["severity"] >= 1 else "ok")
    issues = "".join(f"<li>{i}</li>" for i in analysis["issues"]) or "<li>Критичних відхилень немає.</li>"
    advice = "".join(f"<li>{i}</li>" for i in analysis["advice"]) or "<li>Підтримуйте поточний рівень сервісу — так і далі.</li>"
    badge = "Потребує уваги" if analysis["severity"] >= 2 else "Огляд"
    return f"""
    <div class="loc-analysis sev-{sev}">
      <div class="loc-analysis-head">
        <h3>Що помітили і що зробити</h3>
        <span class="sev-badge">{badge}</span>
      </div>
      <p class="bad-explain">{BAD_ORDERS_EXPLAIN_UA}</p>
      <div class="analysis-kpi">
        <span>Замовлення: <b>{prev['orders']}</b> → <b>{last['orders']}</b></span>
        <span>Доступність: <b>{prev['avail']:.1f}%</b> → <b>{last['avail']:.1f}%</b></span>
        <span>Рейтинг: <b>{prev['rating']:.2f}</b> → <b>{last['rating']:.2f}</b></span>
        <span>Компенсації: <b>{prev['refunds']:.1f}%</b> → <b>{last['refunds']:.1f}%</b></span>
        <span>Погані замовлення (заклад): <b>{prev['bad_provider_count']}</b> → <b>{last['bad_provider_count']}</b> · <b>{last['bad_provider_pct']:.1f}%</b></span>
      </div>
      <h4>Що відбувається</h4>
      <ul>{issues}</ul>
      <h4>Поради для вас</h4>
      <ul class="advice">{advice}</ul>
    </div>"""


def _location_block(loc: dict, analysis: dict) -> str:
    pid = loc["provider_id"]
    search_blob = f"{loc['name']} {loc.get('zone','')} {loc.get('city','')} {pid}".lower()

    charts = ""
    for section_title, keys in CHART_SECTIONS:
        charts += f'<div class="loc-section-title">{section_title}</div>'
        charts += '<div class="charts-grid">'
        for key in keys:
            charts += _histogram(key, loc["weeks"], f"c-{pid}-{key}")
        charts += "</div>"

    promo_tags = ""
    if loc.get("has_smart_promotion"):
        promo_tags += '<span class="promo-tag smart">Розумні акції</span> '
    if loc.get("has_sponsored_listing"):
        promo_tags += '<span class="promo-tag sl">Sponsored Listing</span> '

    return f"""
    <section class="loc-card" data-search="{search_blob}" data-name="{loc['name']}" id="loc-{pid}">
      <div class="loc-row">
        <div class="loc-row-info">
          <h2>{loc['name']}</h2>
          <p class="loc-meta">{loc.get('city','')} · {loc.get('zone','')} · ID {pid}</p>
          <div class="promo-tags">{promo_tags}</div>
        </div>
        <button type="button" class="loc-open-btn" data-loc-id="{pid}" aria-expanded="false" aria-controls="loc-body-{pid}">
          Відкрити інформацію
        </button>
      </div>
      <div class="loc-body" id="loc-body-{pid}" hidden>
        {charts}
        {_location_analysis_block(analysis)}
      </div>
    </section>"""


def _analysis_html(insights: list[dict]) -> str:
    icons = {"positive": "✅", "warning": "⚠️", "info": "ℹ️"}
    insight_cards = "\n".join(
        f"""<div class="insight-card {ins['type']}">
          <div class="insight-title">{icons.get(ins['type'], '•')} {ins['title']}</div>
          <p>{ins['text']}</p>
        </div>"""
        for ins in insights
    )

    return f"""
    <div class="section-title">Висновки по бренду</div>
    <div class="insights-grid">{insight_cards}</div>"""


def generate_html(data: dict) -> str:
    locations = data["locations"]
    brand_weeks = data["brand_weeks"]
    city_median_conv = data.get("city_median_conv", 2.5)
    city_median_impr = data.get("city_median_impr", 1000)
    analyses = [analyze_location(loc, city_median_conv, city_median_impr) for loc in locations]
    insights = build_brand_insights(brand_weeks, analyses)
    last = brand_weeks[-1] if brand_weeks else EMPTY_WEEK
    period = data["period_label"]
    gen = data["generated_at"]
    city = data.get("city") or "Полтава"
    n = len(locations)

    brand_charts = ""
    for section_title, keys in CHART_SECTIONS:
        brand_charts += f'<div class="section-title">{section_title} — увесь бренд</div>'
        brand_charts += '<p class="section-hint">Сума / середнє по всіх локаціях #ARIZONA · 8 тижнів</p>'
        brand_charts += '<div class="charts-grid">'
        for key in keys:
            brand_charts += _histogram(key, brand_weeks, f"brand-{key}")
        brand_charts += "</div>"

    loc_blocks = "\n".join(
        _location_block(loc, next(a for a in analyses if a["provider_id"] == loc["provider_id"]))
        for loc in locations
    )

    search_items = json.dumps(
        [{"id": loc["provider_id"], "name": loc["name"], "zone": loc.get("zone", ""), "city": loc.get("city", "")}
         for loc in locations],
        ensure_ascii=False,
    )

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>WBR Arizona · {BRAND_NAME} · {period}</title>
  <style>
    :root {{
      --green:#34D186; --green-d:#0d8a52; --black:#0d0d0d;
      --gray-700:#4a4a4a; --gray-400:#9a9a9a; --gray-100:#f5f5f5;
      --positive:#1aad6a; --warning:#e67e22; --danger:#c0392b; --info:#2980b9;
    }}
    *{{margin:0;padding:0;box-sizing:border-box}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;
      font-size:14px;line-height:1.55;color:#1a1a1a;background:var(--gray-100)}}
    .header{{background:var(--black);padding:20px 40px;display:flex;align-items:flex-start;
      justify-content:space-between;border-bottom:4px solid var(--green);flex-wrap:wrap;gap:16px}}
    .header-left{{display:flex;align-items:center;gap:14px;flex:1;min-width:240px}}
    .header-right{{display:flex;flex-direction:column;align-items:flex-end;gap:10px}}
    .header-search-wrap{{position:relative;width:min(320px,100%)}}
    .header-search-btn{{
      width:100%;padding:9px 14px;border:1px solid #333;border-radius:8px;background:#1a1a1a;
      color:var(--gray-400);font-size:13px;text-align:left;cursor:pointer;display:flex;
      align-items:center;gap:8px}}
    .header-search-btn:hover{{border-color:var(--green);color:#fff}}
    .header-search-btn svg{{flex-shrink:0;opacity:.8}}
    .search-panel{{
      display:none;position:absolute;top:calc(100% + 6px);right:0;left:0;background:#fff;
      border-radius:10px;box-shadow:0 8px 28px rgba(0,0,0,.18);padding:10px;z-index:50;
      border:1px solid #e8e8e8}}
    .search-panel.open{{display:block}}
    .search-panel input{{
      width:100%;padding:10px 12px;border:1px solid #ddd;border-radius:8px;font-size:14px;outline:none}}
    .search-panel input:focus{{border-color:var(--green-d)}}
    .search-results{{max-height:280px;overflow-y:auto;margin-top:8px}}
    .search-results:empty{{display:none}}
    .search-result-item{{
      display:block;width:100%;text-align:left;padding:9px 10px;border:none;background:transparent;
      border-radius:6px;cursor:pointer;font-size:13px;color:var(--gray-700)}}
    .search-result-item:hover,.search-result-item.active{{background:#e6faf2}}
    .search-result-item small{{display:block;font-size:11px;color:var(--gray-400);margin-top:2px}}
    .search-empty{{font-size:12px;color:var(--gray-400);padding:8px 4px;display:none}}
    .search-empty.visible{{display:block}}
    .bolt-logo{{width:44px;height:44px;background:var(--green);border-radius:10px;
      display:flex;align-items:center;justify-content:center}}
    .header-title h1{{font-size:22px;font-weight:700;color:#fff}}
    .header-title p{{font-size:11px;color:var(--green);text-transform:uppercase;
      letter-spacing:1.2px;font-weight:600;margin-top:4px}}
    .header-meta{{text-align:right;color:var(--gray-400);font-size:12px;line-height:1.9}}
    .header-meta strong{{color:var(--green)}}
    .container{{max-width:1320px;margin:0 auto;padding:28px 40px 48px}}
    .period-bar{{background:#fff;border-radius:12px;padding:14px 20px;margin-bottom:20px;
      display:flex;align-items:center;gap:12px;flex-wrap:wrap;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .promo-tag{{display:inline-block;font-size:10px;font-weight:700;padding:2px 7px;border-radius:6px;margin-top:6px;margin-right:4px}}
    .promo-tag.smart{{background:#f3e8ff;color:#6b21a8}}
    .promo-tag.sl{{background:#dbeafe;color:#1d4ed8}}
    .promo-tags{{min-height:0}}
    .section-title{{font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;
      color:var(--gray-700);padding-bottom:10px;border-bottom:2px solid var(--green);margin:28px 0 10px}}
    .section-hint{{font-size:12px;color:var(--gray-400);margin-bottom:14px}}
    .kpi-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px;margin-bottom:8px}}
    .kpi-card{{background:#fff;border-radius:12px;padding:14px 16px;border-top:3px solid var(--green);
      box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .kpi-label{{font-size:10px;font-weight:700;text-transform:uppercase;color:var(--gray-400);margin-bottom:4px}}
    .kpi-value{{font-size:20px;font-weight:700}}
    .charts-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px;margin-bottom:12px}}
    .chart-card{{background:#fff;border-radius:12px;padding:16px 18px;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .chart-card h3{{font-size:12px;font-weight:700;color:var(--gray-700);margin-bottom:4px}}
    .metric-desc{{font-size:11px;color:var(--gray-700);margin-bottom:4px;line-height:1.4}}
    .unit{{font-size:10px;color:var(--gray-400);margin-bottom:8px}}
    .bars-scroll{{overflow-x:auto;padding-bottom:4px}}
    .bars{{display:flex;gap:6px;align-items:flex-end;min-height:120px;padding-top:6px}}
    .bar-col{{display:flex;flex-direction:column;align-items:center;min-width:42px;flex-shrink:0;
      height:110px;justify-content:flex-end}}
    .bar-val{{font-size:8px;font-weight:700;color:var(--gray-700);margin-bottom:3px;text-align:center;
      max-width:48px;line-height:1.15}}
    .bar{{width:36px;border-radius:5px 5px 0 0;min-height:4px}}
    .bar-lbl{{font-size:8px;color:var(--gray-400);margin-top:3px;text-align:center;line-height:1.2}}
    .loc-card{{background:#fff;border-radius:12px;padding:0;margin:0 0 10px;
      box-shadow:0 1px 4px rgba(0,0,0,.06);border:1px solid #eee;overflow:hidden}}
    .loc-row{{display:flex;align-items:center;justify-content:space-between;gap:16px;
      padding:14px 18px;flex-wrap:wrap}}
    .loc-row-info{{flex:1;min-width:180px}}
    .loc-row-info h2{{font-size:15px;color:var(--black);font-weight:700}}
    .loc-open-btn{{
      flex-shrink:0;padding:9px 16px;border:none;border-radius:8px;background:var(--green-d);
      color:#fff;font-size:13px;font-weight:600;cursor:pointer;white-space:nowrap}}
    .loc-open-btn:hover{{background:var(--green);color:var(--black)}}
    .loc-open-btn[aria-expanded="true"]{{background:#eee;color:var(--gray-700)}}
    .loc-body{{padding:0 18px 20px;border-top:1px solid #f0f0f0}}
    .loc-body[hidden]{{display:none}}
    .loc-analysis{{background:var(--gray-100);border-radius:10px;padding:16px 18px;margin-top:20px;
      border-left:4px solid var(--gray-400)}}
    .loc-analysis.sev-high{{border-left-color:var(--danger);background:#fff8f6}}
    .loc-analysis.sev-mid{{border-left-color:var(--warning);background:#fffaf3}}
    .loc-analysis.sev-ok{{border-left-color:var(--positive)}}
    .loc-analysis-head{{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px}}
    .loc-analysis-head h3{{font-size:14px;color:var(--gray-700)}}
    .loc-analysis h4{{font-size:12px;margin:10px 0 4px;color:var(--gray-700)}}
    .loc-analysis ul{{margin-left:18px;font-size:13px}}
    .loc-analysis ul.advice{{color:var(--green-d)}}
    .bad-explain{{font-size:12px;color:var(--gray-700);line-height:1.45;margin:0 0 12px;
      padding:10px 12px;background:#fff;border-radius:8px;border:1px solid #eee}}
    .loc-meta{{font-size:12px;color:var(--gray-400);margin-top:2px}}
    .loc-list{{display:flex;flex-direction:column;gap:0}}
    .loc-section-title{{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.6px;
      color:var(--green-d);margin:18px 0 10px}}
    .insights-grid,.analysis-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));
      gap:14px;margin-bottom:24px}}
    .insight-card,.analysis-card{{background:#fff;border-radius:12px;padding:16px 18px;
      box-shadow:0 1px 4px rgba(0,0,0,.06);border-left:4px solid var(--gray-400)}}
    .insight-card.positive{{border-left-color:var(--positive)}}
    .insight-card.warning{{border-left-color:var(--warning)}}
    .insight-card.info{{border-left-color:var(--info)}}
    .insight-title{{font-weight:700;margin-bottom:6px}}
    .analysis-card.sev-high{{border-left-color:var(--danger);background:#fff8f6}}
    .analysis-card.sev-mid{{border-left-color:var(--warning);background:#fffaf3}}
    .analysis-card.sev-ok{{border-left-color:var(--positive)}}
    .analysis-head{{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px}}
    .analysis-head h3{{font-size:15px}}
    .sev-badge{{font-size:10px;font-weight:700;text-transform:uppercase;color:var(--warning)}}
    .analysis-kpi{{display:flex;flex-wrap:wrap;gap:10px;font-size:11px;color:var(--gray-400);
      margin-bottom:10px;padding:8px 0;border-top:1px solid #f0f0f0;border-bottom:1px solid #f0f0f0}}
    .analysis-kpi b{{color:var(--gray-700)}}
    .analysis-card h4{{font-size:12px;margin:10px 0 4px;color:var(--gray-700)}}
    .analysis-card ul{{margin-left:18px;font-size:13px}}
    .analysis-card ul.advice{{color:var(--green-d)}}
    .problem-none{{background:#e6faf2;border-radius:12px;padding:18px;color:var(--positive)}}
    .footer{{background:var(--black);color:var(--gray-400);font-size:11px;padding:22px 40px;text-align:center}}
    .footer span{{color:var(--green)}}
    @media(max-width:700px){{
      .container{{padding:16px}} .charts-grid{{grid-template-columns:1fr}}
      .header{{padding:16px}} .header-right{{align-items:stretch;width:100%}}
      .header-search-wrap{{width:100%}}
    }}
  </style>
</head>
<body>
<header class="header">
  <div class="header-left">
    <div class="bolt-logo">
      <svg viewBox="0 0 24 24" width="26" height="26"><path d="M13 2L4.5 13.5H11L10 22L19.5 10.5H13V2Z" fill="#0d0d0d"/></svg>
    </div>
    <div class="header-title">
      <h1>WBR Arizona · {BRAND_NAME}</h1>
      <p>Bolt Food · Щотижневий звіт · {city}</p>
    </div>
  </div>
  <div class="header-right">
    <div class="header-meta">
      <div>Період: <strong>{data['period_dates']}</strong></div>
      <div>Тижнів: <strong>{N_WEEKS} завершених</strong> · Локацій: <strong>{n}</strong></div>
      <div>Оновлено: <strong>{gen}</strong></div>
    </div>
    <div class="header-search-wrap" id="header-search-wrap">
      <button type="button" class="header-search-btn" id="search-open-btn" aria-expanded="false" aria-controls="search-panel">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="M20 20l-3-3"/></svg>
        Пошук локації…
      </button>
      <div class="search-panel" id="search-panel" role="dialog" aria-label="Пошук локації">
        <input id="loc-search" type="search" placeholder="Назва, зона або ID…" autocomplete="off"/>
        <div class="search-empty" id="search-empty">Нічого не знайдено</div>
        <div class="search-results" id="search-results"></div>
      </div>
    </div>
  </div>
</header>

<div class="container">
  <div class="period-bar">
    <span style="font-size:11px;font-weight:700;text-transform:uppercase;color:var(--gray-700)">Період:</span>
    <span style="font-size:12px;color:var(--gray-700)">{period} · валюта UAH (₴)</span>
    <span style="margin-left:auto;font-size:11px;color:var(--gray-400)">Останній тиждень: {last.get('label','')}</span>
  </div>

  <div class="section-title">Огляд бренду — останній тиждень</div>
  <div class="kpi-grid">
    <div class="kpi-card"><div class="kpi-label">Gross Sales</div><div class="kpi-value">{_fmt(last['gross'],'gross')} ₴</div></div>
    <div class="kpi-card"><div class="kpi-label">Net Sales</div><div class="kpi-value">{_fmt(last['net'],'net')} ₴</div></div>
    <div class="kpi-card"><div class="kpi-label">Delivered Orders</div><div class="kpi-value">{last['orders']}</div></div>
    <div class="kpi-card"><div class="kpi-label">AOV</div><div class="kpi-value">{_fmt(last['aov'],'aov')} ₴</div></div>
    <div class="kpi-card"><div class="kpi-label">Availability</div><div class="kpi-value">{last['avail']:.1f}%</div></div>
    <div class="kpi-card"><div class="kpi-label">Acceptance</div><div class="kpi-value">{last['accept']:.1f}%</div></div>
    <div class="kpi-card"><div class="kpi-label">Active Users</div><div class="kpi-value">{last['active_users']}</div></div>
    <div class="kpi-card"><div class="kpi-label">Rating</div><div class="kpi-value">{last['rating']:.2f}</div></div>
    <div class="kpi-card"><div class="kpi-label">Погані замовлення (заклад)</div><div class="kpi-value">{last['bad_provider_count']} · {last['bad_provider_pct']:.1f}%</div></div>
  </div>
  <p class="section-hint" style="margin-top:8px">{BAD_ORDERS_EXPLAIN_UA}</p>

  {brand_charts}

  <div class="section-title">Локації бренду</div>
  <p class="section-hint">Пошук — у шапці · натисніть «Відкрити інформацію», щоб побачити гістограми та поради по локації</p>

  <div id="locations" class="loc-list">
    {loc_blocks}
  </div>

  {_analysis_html(insights)}
</div>

<footer class="footer">
  <span>Bolt Food</span> · WBR Arizona · {BRAND_NAME} · Автооновлення: щопонеділка о 13:00 (Київ) ·
  <a href="https://github.com/marharytazhytnyk-create/partner-reports/tree/main/WBR%20Arizona" style="color:var(--green)">GitHub</a>
</footer>

<script>
(function() {{
  const LOCATIONS = {search_items};

  function toggleLocation(id, forceOpen) {{
    const card = document.getElementById('loc-' + id);
    const body = document.getElementById('loc-body-' + id);
    const btn = card && card.querySelector('.loc-open-btn');
    if (!card || !body || !btn) return;
    const open = forceOpen !== undefined ? forceOpen : body.hidden;
    body.hidden = !open;
    btn.setAttribute('aria-expanded', open ? 'true' : 'false');
    btn.textContent = open ? 'Згорнути' : 'Відкрити інформацію';
    if (open) {{
      card.classList.add('loc-expanded');
      card.scrollIntoView({{behavior: 'smooth', block: 'start'}});
    }} else {{
      card.classList.remove('loc-expanded');
    }}
  }}

  window.openLocation = function(id) {{ toggleLocation(id, true); }};

  document.querySelectorAll('.loc-open-btn').forEach(btn => {{
    btn.addEventListener('click', () => toggleLocation(btn.dataset.locId));
  }});

  const wrap = document.getElementById('header-search-wrap');
  const panel = document.getElementById('search-panel');
  const openBtn = document.getElementById('search-open-btn');
  const input = document.getElementById('loc-search');
  const resultsEl = document.getElementById('search-results');
  const emptyEl = document.getElementById('search-empty');
  let activeIdx = -1;

  function closePanel() {{
    panel.classList.remove('open');
    openBtn.setAttribute('aria-expanded', 'false');
    input.value = '';
    resultsEl.innerHTML = '';
    emptyEl.classList.remove('visible');
    activeIdx = -1;
  }}

  function openPanel() {{
    panel.classList.add('open');
    openBtn.setAttribute('aria-expanded', 'true');
    input.focus();
  }}

  function renderResults(q) {{
    const query = (q || '').trim().toLowerCase();
    resultsEl.innerHTML = '';
    emptyEl.classList.remove('visible');
    activeIdx = -1;
    if (!query) return;

    const matches = LOCATIONS.filter(loc => {{
      const blob = (loc.name + ' ' + loc.zone + ' ' + loc.city + ' ' + loc.id).toLowerCase();
      return blob.includes(query);
    }}).slice(0, 10);

    if (!matches.length) {{
      emptyEl.classList.add('visible');
      return;
    }}

    matches.forEach((loc, i) => {{
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'search-result-item';
      btn.dataset.id = loc.id;
      btn.innerHTML = loc.name + '<small>' + (loc.zone || loc.city) + ' · ID ' + loc.id + '</small>';
      btn.addEventListener('click', () => goTo(loc.id));
      resultsEl.appendChild(btn);
    }});
  }}

  function goTo(id) {{
    closePanel();
    openLocation(id);
  }}

  openBtn.addEventListener('click', (e) => {{
    e.stopPropagation();
    if (panel.classList.contains('open')) closePanel();
    else openPanel();
  }});

  input.addEventListener('input', () => renderResults(input.value));

  input.addEventListener('keydown', (e) => {{
    const items = Array.from(resultsEl.querySelectorAll('.search-result-item'));
    if (e.key === 'Escape') {{ closePanel(); return; }}
    if (!items.length) return;
    if (e.key === 'ArrowDown') {{
      e.preventDefault();
      activeIdx = Math.min(activeIdx + 1, items.length - 1);
    }} else if (e.key === 'ArrowUp') {{
      e.preventDefault();
      activeIdx = Math.max(activeIdx - 1, 0);
    }} else if (e.key === 'Enter' && activeIdx >= 0) {{
      e.preventDefault();
      goTo(items[activeIdx].dataset.id);
      return;
    }} else return;
    items.forEach((it, i) => it.classList.toggle('active', i === activeIdx));
  }});

  document.addEventListener('click', (e) => {{
    if (!wrap.contains(e.target)) closePanel();
  }});
}})();
</script>
</body>
</html>"""


def main() -> None:
    global DATABRICKS_TOKEN, HEADERS
    if not DATABRICKS_TOKEN:
        DATABRICKS_TOKEN = _load_token()
        HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
    if not DATABRICKS_TOKEN:
        print("ERROR: DATABRICKS_TOKEN не задано.", file=sys.stderr)
        sys.exit(1)

    weeks = last_n_completed_weeks(N_WEEKS)
    print(f"WBR Arizona — {N_WEEKS} завершених тижнів: {weeks[0][0]} → {weeks[-1][1]}")
    print(f"Бренд: {BRAND_NAME}\n")

    data = fetch_data()
    html = generate_html(data)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print(f"\n→ {OUTPUT_HTML}")
    print(f"Локацій: {len(data['locations'])}, згенеровано: {data['generated_at']}")


if __name__ == "__main__":
    main()
