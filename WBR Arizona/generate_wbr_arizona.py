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
    ("3. Клієнти та їх поведінка", [
        "active_users", "freq", "new_users", "sessions", "imp_menu", "menu_prod", "rating",
    ]),
    ("4. Знижки", ["discounts", "camp_bolt", "camp_merch"]),
]

METRIC_UK: dict[str, tuple[str, str, str]] = {
    "gross": ("Gross Sales (загальні продажі)", "Сума вартості доставлених замовлень до знижок", "₴"),
    "net": ("Net Sales (чисті продажі)", "Сума після застосування знижок", "₴"),
    "orders": ("Delivered Orders", "Кількість успішно доставлених замовлень", "шт."),
    "aov": ("AOV (середній чек)", "Середня сума одного доставленого замовлення до знижок", "₴"),
    "avail": ("Availability Rate", "Частка часу, коли заклад був онлайн", "%"),
    "accept": ("Acceptance Rate", "Частка замовлень, прийнятих партнером вчасно", "%"),
    "refunds": ("Orders with Refunds", "Частка замовлень із компенсацією клієнту", "%"),
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
    "camp_merch": ("Campaigns Spend by Merchant", "Витрати партнера на знижки та промо", "₴"),
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
    }


EMPTY_WEEK = {
    "orders": 0, "gross": 0, "net": 0, "aov": 0,
    "avail": 0, "accept": 0, "refunds": 0,
    "del_time": 0, "acc_time": 0, "prep_time": 0, "wait_time": 0, "c2m_time": 0, "c2e_time": 0,
    "new_users": 0, "sessions": 0, "imp_menu": 0, "menu_prod": 0, "rating": 0,
    "discounts": 0, "camp_bolt": 0, "camp_merch": 0, "active_users": 0, "freq": 0,
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
    finally:
        destroy_context(ctx)

    active_map: dict[tuple[int, str], int] = {}
    for row in users_rows:
        active_map[(int(row[0]), week_key(str(row[1])))] = _si(row[2])

    by_pid: dict[int, dict] = {
        p["provider_id"]: {**p, "by_week": {}} for p in providers
    }
    for row in fact_rows:
        pid = int(row[0])
        wk = week_key(str(row[2]))
        if pid not in by_pid:
            continue
        au = active_map.get((pid, wk), 0)
        by_pid[pid]["by_week"][wk] = _parse_loc_week(row, au)

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
        locations.append(loc)

    # Brand totals per week
    brand_weeks = []
    for i, (wk, label) in enumerate(zip(week_keys, week_labels)):
        agg = dict(EMPTY_WEEK)
        for loc in locations:
            w = loc["weeks"][i]
            for k in ("orders", "gross", "net", "new_users", "sessions",
                      "discounts", "camp_bolt", "camp_merch", "active_users"):
                agg[k] += w.get(k, 0)
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
    }


# ─── ANALYSIS ──────────────────────────────────────────────────────────────────

def _pct_change(old: float, new: float) -> float | None:
    if old == 0:
        return None
    return (new - old) / old * 100


def analyze_location(loc: dict) -> dict:
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
            f"Дуже мало замовлень за останній тиждень — лише {last['orders']} "
            f"(попередній: {prev['orders']})."
        )
        advice.append("Перевірити години роботи, фото/опис меню та наявність акцій у зоні доставки.")
        severity += 3
    elif o_chg is not None and o_chg <= -25:
        issues.append(
            f"Різке падіння замовлень WoW: {prev['orders']} → {last['orders']} ({o_chg:.0f}%)."
        )
        severity += 2
    elif o_chg is not None and o_chg <= -10:
        issues.append(
            f"Зменшення замовлень WoW: {prev['orders']} → {last['orders']} ({o_chg:.0f}%)."
        )
        severity += 1

    if o_chg is not None and o_chg >= 20:
        issues.append(
            f"Зростання замовлень WoW: {prev['orders']} → {last['orders']} (+{o_chg:.0f}%)."
        )

    if o_trend is not None and o_trend <= -30:
        issues.append(
            f"За 8 тижнів замовлення впали з {first['orders']} до {last['orders']} ({o_trend:.0f}%)."
        )
        advice.append("Порівняти динаміку знижок і доступності з топ-локаціями бренду; посилити промо.")
        severity += 2

    if last["avail"] < 90:
        issues.append(f"Низька доступність — {last['avail']:.1f}%.")
        advice.append("Тримати заклад онлайн у пікові години (обід / вечір), зменшити ручні паузи.")
        severity += 2

    if last["accept"] < 97:
        issues.append(f"Acceptance Rate нижче норми — {last['accept']:.1f}%.")
        advice.append("Прискорити прийняття замовлень у застосунку партнера (ціль < 1 хв).")
        severity += 2

    if last["refunds"] >= 5:
        issues.append(f"Висока частка компенсацій — {last['refunds']:.1f}%.")
        advice.append("Перевірити completeness меню, якість збірки та час приготування.")
        severity += 2
    elif last["refunds"] >= 3 and last["refunds"] > prev["refunds"] + 1:
        issues.append(f"Зростання refunds: {prev['refunds']:.1f}% → {last['refunds']:.1f}%.")
        severity += 1

    if last["prep_time"] >= 30:
        issues.append(f"Довгий час приготування — {last['prep_time']:.1f} хв.")
        advice.append("Оптимізувати кухню в пік або скоригувати cooking time у кабінеті.")
        severity += 1

    if last["acc_time"] >= 2.5:
        issues.append(f"Повільне прийняття — {last['acc_time']:.1f} хв.")
        advice.append("Призначити відповідального за планшет / звукові сповіщення.")
        severity += 1

    if last["del_time"] >= 50:
        issues.append(f"Довга доставка — {last['del_time']:.1f} хв загалом.")
        severity += 1

    if last["rating"] and last["rating"] < 4.4:
        issues.append(f"Низький рейтинг — {last['rating']:.2f} з 5.")
        advice.append("Розібрати останні низькі оцінки: комплектація, температура, запізнення.")
        severity += 2

    if last["imp_menu"] < 8 and last["sessions"] > 800:
        issues.append(f"Слабка конверсія в меню — {last['imp_menu']:.1f}% при {last['sessions']} сесіях.")
        advice.append("Оновити головне фото, рейтинг і бейджі акцій — щоб клієнт клікав у меню.")
        severity += 1

    if last["menu_prod"] < 25 and last["sessions"] > 200:
        issues.append(f"Слабка конверсія меню → кошик — {last['menu_prod']:.1f}%.")
        advice.append("Підсвітити хіти, комбо та зрозумілі описи/ціни позицій.")
        severity += 1

    disc_chg = _pct_change(prev["discounts"], last["discounts"])
    if o_chg is not None and o_chg < -10 and disc_chg is not None and disc_chg < -20:
        issues.append(
            f"Падіння замовлень супроводжується зменшенням знижок "
            f"({prev['discounts']:,.0f} → {last['discounts']:,.0f} ₴).".replace(",", "\u202f")
        )
        advice.append("Повернути співфінансування промо (Bolt + партнер) на слабкі дні тижня.")
        severity += 1

    if o_chg is not None and o_chg > 15 and last["camp_bolt"] + last["camp_merch"] > prev["camp_bolt"] + prev["camp_merch"]:
        issues.append("Зростання замовлень збігається з активнішими кампаніями знижок.")

    if not advice and severity >= 1:
        advice.append("Сфокусуватися на доступності >95%, acceptance >98% і стабільних промо.")

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
        add("positive", "Продажі бренду зростають",
            f"Gross Sales: {a['gross']:,.0f} → {b['gross']:,.0f} ₴ ({g_chg:+.0f}%). "
            f"Замовлення: {o_chg:+.0f}%.".replace(",", "\u202f"))
    elif g_chg is not None and g_chg < -5:
        add("warning", "Продажі бренду падають",
            f"Gross Sales: {a['gross']:,.0f} → {b['gross']:,.0f} ₴ ({g_chg:.0f}%). "
            f"Замовлення: {o_chg:.0f}% WoW.".replace(",", "\u202f"))

    if b["avail"] < 92:
        add("warning", "Доступність мережі",
            f"Середня Availability Rate за останній тиждень — {b['avail']:.1f}%. "
            "Клієнти втрачають можливість замовити.")

    if b["refunds"] > a["refunds"] + 1:
        add("warning", "Зростання компенсацій",
            f"Orders with Refunds: {a['refunds']:.1f}% → {b['refunds']:.1f}%.")

    problem = [x for x in analyses if x["severity"] >= 2]
    if problem:
        names = ", ".join(p["name"] for p in problem[:4])
        add("warning", "Локації під увагою",
            f"Найвищий пріоритет: {names}. Деталі — у блоці аналізу нижче.")

    growing = [x for x in analyses if x["trend"] == "up"]
    if growing:
        add("positive", "Точки росту",
            "Зростання замовлень WoW: " + ", ".join(g["name"] for g in growing[:5]) + ".")

    if not insights:
        add("info", "Стабільний тиждень",
            "Ключові показники бренду без різких змін. Тримайте якість і доступність.")
    return insights


# ─── HTML ──────────────────────────────────────────────────────────────────────

def _fmt(val: float, key: str) -> str:
    if key in ("avail", "accept", "refunds", "imp_menu", "menu_prod"):
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


def _location_block(loc: dict, analysis: dict) -> str:
    pid = loc["provider_id"]
    search_blob = f"{loc['name']} {loc.get('zone','')} {loc.get('city','')} {pid}".lower()
    trend = analysis.get("trend", "stable")
    badge = {"up": "📈 ріст", "down": "📉 падіння", "stable": "→ стабільно"}.get(trend, "")
    o_chg = analysis.get("o_chg")
    chg_txt = f"{o_chg:+.0f}% WoW" if o_chg is not None else "—"

    charts = ""
    for section_title, keys in CHART_SECTIONS:
        charts += f'<div class="loc-section-title">{section_title}</div>'
        charts += '<div class="charts-grid">'
        for key in keys:
            charts += _histogram(key, loc["weeks"], f"c-{pid}-{key}")
        charts += "</div>"

    last = loc["weeks"][-1]
    return f"""
    <section class="loc-card" data-search="{search_blob}" data-name="{loc['name']}" id="loc-{pid}">
      <div class="loc-head">
        <div>
          <h2>{loc['name']}</h2>
          <p class="loc-meta">{loc.get('city','')} · {loc.get('zone','')} · ID {pid}</p>
        </div>
        <div class="loc-kpis">
          <div><span>Замовлення</span><strong>{last['orders']}</strong></div>
          <div><span>Gross</span><strong>{_fmt(last['gross'], 'gross')} ₴</strong></div>
          <div><span>WoW</span><strong class="trend-{trend}">{chg_txt}</strong></div>
          <div><span>Тренд</span><strong>{badge}</strong></div>
        </div>
      </div>
      {charts}
    </section>"""


def _analysis_html(analyses: list[dict], insights: list[dict]) -> str:
    icons = {"positive": "✅", "warning": "⚠️", "info": "ℹ️"}
    insight_cards = "\n".join(
        f"""<div class="insight-card {ins['type']}">
          <div class="insight-title">{icons.get(ins['type'], '•')} {ins['title']}</div>
          <p>{ins['text']}</p>
        </div>"""
        for ins in insights
    )

    cards = ""
    ranked = sorted(analyses, key=lambda x: -x["severity"])
    for a in ranked:
        if not a["issues"] and a["severity"] == 0:
            continue
        issues = "".join(f"<li>{i}</li>" for i in a["issues"]) or "<li>Критичних відхилень немає.</li>"
        advice = "".join(f"<li>{i}</li>" for i in a["advice"]) or "<li>Підтримувати поточний рівень сервісу.</li>"
        prev, last = a["prev"], a["last"]
        sev = "high" if a["severity"] >= 3 else ("mid" if a["severity"] >= 1 else "ok")
        cards += f"""
        <div class="analysis-card sev-{sev}">
          <div class="analysis-head">
            <h3>{a['name']}</h3>
            <span class="sev-badge">{'Пріоритет' if a['severity'] >= 2 else 'Огляд'}</span>
          </div>
          <div class="analysis-kpi">
            <span>Замовлення: <b>{prev['orders']}</b> → <b>{last['orders']}</b></span>
            <span>Доступність: <b>{prev['avail']:.1f}%</b> → <b>{last['avail']:.1f}%</b></span>
            <span>Рейтинг: <b>{prev['rating']:.2f}</b> → <b>{last['rating']:.2f}</b></span>
            <span>Refunds: <b>{prev['refunds']:.1f}%</b> → <b>{last['refunds']:.1f}%</b></span>
          </div>
          <h4>Слабкі місця / динаміка</h4>
          <ul>{issues}</ul>
          <h4>Поради для росту продажів</h4>
          <ul class="advice">{advice}</ul>
        </div>"""

    if not cards:
        cards = '<div class="problem-none">За ключовими метриками критичних відхилень немає.</div>'

    return f"""
    <div class="section-title">Висновки по бренду</div>
    <div class="insights-grid">{insight_cards}</div>
    <div class="section-title">Аналіз локацій — слабкі місця та поради</div>
    <p class="section-hint">Порівняння останнього завершеного тижня з попереднім · причини падіння/росту · рекомендації</p>
    <div class="analysis-grid">{cards}</div>"""


def generate_html(data: dict) -> str:
    locations = data["locations"]
    brand_weeks = data["brand_weeks"]
    analyses = [analyze_location(loc) for loc in locations]
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

    loc_options = "".join(
        f'<option value="{loc["provider_id"]}">{loc["name"]}</option>' for loc in locations
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
    .container{{max-width:1320px;margin:0 auto;padding:28px 40px 48px}}
    .period-bar{{background:#fff;border-radius:12px;padding:14px 20px;margin-bottom:20px;
      display:flex;align-items:center;gap:12px;flex-wrap:wrap;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .search-bar{{background:#fff;border-radius:12px;padding:16px 20px;margin-bottom:24px;
      box-shadow:0 1px 4px rgba(0,0,0,.06);display:flex;flex-wrap:wrap;gap:12px;align-items:center;
      position:sticky;top:0;z-index:20;border-bottom:2px solid var(--green)}}
    .search-bar label{{font-size:11px;font-weight:700;text-transform:uppercase;color:var(--gray-700)}}
    .search-bar input,.search-bar select{{
      flex:1;min-width:200px;padding:10px 14px;border:1px solid #ddd;border-radius:8px;
      font-size:14px;outline:none}}
    .search-bar input:focus,.search-bar select:focus{{border-color:var(--green-d)}}
    .search-count{{font-size:12px;color:var(--gray-400);white-space:nowrap}}
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
    .loc-card{{background:#fff;border-radius:14px;padding:22px 24px;margin:24px 0;
      box-shadow:0 1px 4px rgba(0,0,0,.06);border:1px solid #eee}}
    .loc-card.hidden{{display:none}}
    .loc-head{{display:flex;justify-content:space-between;gap:16px;flex-wrap:wrap;
      margin-bottom:16px;padding-bottom:14px;border-bottom:1px solid #f0f0f0}}
    .loc-head h2{{font-size:18px;color:var(--black)}}
    .loc-meta{{font-size:12px;color:var(--gray-400);margin-top:4px}}
    .loc-kpis{{display:flex;gap:14px;flex-wrap:wrap}}
    .loc-kpis div{{background:var(--gray-100);border-radius:8px;padding:8px 12px;min-width:90px}}
    .loc-kpis span{{display:block;font-size:10px;color:var(--gray-400);text-transform:uppercase}}
    .loc-kpis strong{{font-size:14px}}
    .trend-up{{color:var(--positive)}} .trend-down{{color:var(--danger)}} .trend-stable{{color:var(--gray-700)}}
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
      .header{{padding:20px}} .search-bar{{position:static}}
    }}
  </style>
</head>
<body>
<header class="header">
  <div class="header-logo">
    <div class="bolt-logo">
      <svg viewBox="0 0 24 24" width="26" height="26"><path d="M13 2L4.5 13.5H11L10 22L19.5 10.5H13V2Z" fill="#0d0d0d"/></svg>
    </div>
    <div class="header-title">
      <h1>WBR Arizona · {BRAND_NAME}</h1>
      <p>Bolt Food · Щотижневий звіт · {city}</p>
    </div>
  </div>
  <div class="header-meta">
    <div>Період: <strong>{data['period_dates']}</strong></div>
    <div>Тижнів: <strong>{N_WEEKS} завершених</strong></div>
    <div>Локацій: <strong>{n}</strong></div>
    <div>Оновлено: <strong>{gen}</strong></div>
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
  </div>

  {brand_charts}

  <div class="section-title">Локації бренду</div>
  <p class="section-hint">Пошук і фільтр по назві / зоні · кожна локація містить усі метрики гістограмами по тижнях</p>

  <div class="search-bar">
    <label for="loc-search">Пошук локації</label>
    <input id="loc-search" type="search" placeholder="Назва, зона або ID…" autocomplete="off"/>
    <label for="loc-select">Швидкий вибір</label>
    <select id="loc-select">
      <option value="">Усі локації</option>
      {loc_options}
    </select>
    <span class="search-count" id="search-count">Показано: {n} / {n}</span>
  </div>

  <div id="locations">
    {loc_blocks}
  </div>

  {_analysis_html(analyses, insights)}
</div>

<footer class="footer">
  <span>Bolt Food</span> · WBR Arizona · {BRAND_NAME} · Автооновлення: щопонеділка о 13:00 (Київ) ·
  <a href="https://github.com/marharytazhytnyk-create/partner-reports/tree/main/WBR%20Arizona" style="color:var(--green)">GitHub</a>
</footer>

<script>
(function() {{
  const input = document.getElementById('loc-search');
  const select = document.getElementById('loc-select');
  const cards = Array.from(document.querySelectorAll('.loc-card'));
  const countEl = document.getElementById('search-count');
  const total = cards.length;

  function applyFilter() {{
    const q = (input.value || '').trim().toLowerCase();
    const sel = select.value;
    let shown = 0;
    cards.forEach(card => {{
      const matchText = !q || (card.dataset.search || '').includes(q);
      const matchSel = !sel || card.id === ('loc-' + sel);
      const ok = matchText && matchSel;
      card.classList.toggle('hidden', !ok);
      if (ok) shown++;
    }});
    countEl.textContent = 'Показано: ' + shown + ' / ' + total;
  }}

  input.addEventListener('input', applyFilter);
  select.addEventListener('change', function() {{
    if (select.value) {{
      input.value = '';
      const el = document.getElementById('loc-' + select.value);
      applyFilter();
      if (el) el.scrollIntoView({{behavior:'smooth', block:'start'}});
    }} else {{
      applyFilter();
    }}
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
