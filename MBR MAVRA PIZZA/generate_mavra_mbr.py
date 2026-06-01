#!/usr/bin/env python3
"""
MBR (Monthly Business Review) — MAVRA PIZZA, Запоріжжя.
Дані з Databricks, звіт HTML українською, автооновлення 1-го числа о 15:00 (Київ).
"""

from __future__ import annotations

import datetime
import json
import os
import sys
import time
from pathlib import Path

import requests

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://bolt-incentives.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "0221-081903-9ag4bh69")

BRAND_NAME = "MAVRA PIZZA"
CITY_UK = "Запоріжжя"
VENDOR_ID = 102625
PROVIDER_IDS = [
    138974, 194965, 194972, 194976, 194977,
    194981, 194982, 194984, 194985,
]

SCRIPT_DIR = Path(__file__).parent
OUTPUT_FILE = SCRIPT_DIR / "index.html"

UK_MONTHS = [
    "", "січень", "лютий", "березень", "квітень", "травень", "червень",
    "липень", "серпень", "вересень", "жовтень", "листопад", "грудень",
]

MONTH_COLORS = ["#0d8a52", "#34D186"]
MONTH_BORDERS = ["#066637", "#2ab872"]

POLL_INTERVAL_S = 5
MAX_POLL_S = 600

HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}


# ─── DATE HELPERS ──────────────────────────────────────────────────────────────

def last_n_full_months(n: int = 2, ref: datetime.date | None = None) -> list[tuple[int, int]]:
    """Повертає n останніх повних календарних місяців (рік, місяць), від старого до нового."""
    d = ref or datetime.date.today()
    first_current = d.replace(day=1)
    cur = first_current
    months: list[tuple[int, int]] = []
    for _ in range(n):
        last_day_prev = cur - datetime.timedelta(days=1)
        months.append((last_day_prev.year, last_day_prev.month))
        cur = last_day_prev.replace(day=1)
    return list(reversed(months))


def month_label(year: int, month: int) -> str:
    return f"{UK_MONTHS[month]} {year}"


def month_range_sql(year: int, month: int) -> tuple[str, str]:
    start = datetime.date(year, month, 1)
    if month == 12:
        end = datetime.date(year + 1, 1, 1)
    else:
        end = datetime.date(year, month + 1, 1)
    return start.isoformat(), end.isoformat()


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


# ─── DATA FETCH ────────────────────────────────────────────────────────────────

def fetch_metrics(months: list[tuple[int, int]]) -> dict:
    pids_sql = ", ".join(str(p) for p in PROVIDER_IDS)
    pids_str = ", ".join(f"'{p}'" for p in PROVIDER_IDS)

    y0, m0 = months[0]
    y1, m1 = months[-1]
    global_start, _ = month_range_sql(y0, m0)
    _, global_end = month_range_sql(y1, m1)

    ctx = create_context()
    try:
        main_sql = f"""
        SELECT
            DATE_TRUNC('month', metric_timestamp_partition) AS month,
            SUM(delivered_orders_count)                              AS delivered_orders,
            SUM(total_gmv_before_discounts)                          AS gross_sales,
            SUM(total_gmv_after_discounts)                           AS net_sales,
            SUM(users_activated_vendor_count)                        AS new_users,
            AVG(provider_acceptance_rate_value)                      AS acceptance_rate,
            AVG(provider_active_rate_value)                          AS availability_rate,
            AVG(customer_refunded_order_rate_value)                  AS refund_rate,
            AVG(provider_rating_per_order_value)                     AS rating,
            AVG(order_total_minutes_per_order_value)                 AS delivery_time,
            AVG(provider_acceptance_minutes_per_order_value)         AS acceptance_time,
            AVG(provider_preparation_minutes_per_order_value)        AS prep_time,
            AVG(courier_total_wait_minutes_per_order_value)          AS courier_wait,
            AVG(courier_to_provider_actual_minutes_per_order_value)  AS c2merchant,
            AVG(courier_to_eater_actual_minutes_per_order_value)     AS c2eater,
            SUM(total_campaign_discount)                             AS discounts,
            SUM(total_campaign_spend_bolt)                           AS camp_bolt,
            SUM(total_campaign_spend_provider)                       AS camp_merchant,
            SUM(provider_impressions_sessions_count)                 AS sessions,
            SUM(provider_menu_viewed_sessions_count)                 AS menu_viewed,
            SUM(provider_product_added_sessions_count)               AS prod_added,
            AVG(provider_product_added_from_menu_viewed_rate_value)  AS menu_prod_rate
        FROM ng_delivery_spark.fact_provider_weekly
        WHERE provider_id IN ({pids_sql})
          AND metric_timestamp_partition >= '{global_start}'
          AND metric_timestamp_partition < '{global_end}'
        GROUP BY 1
        ORDER BY 1
        """
        main_rows = run_query(ctx, main_sql)

        users_sql = f"""
        SELECT DATE_TRUNC('month', metric_timestamp_partition) AS month,
               SUM(provider_deliveries_unique_user_count) AS active_users
        FROM ng_delivery_spark.int_provider_metrics_non_additive
        WHERE entity_id IN ({pids_str})
          AND timeframe_name = 'week'
          AND metric_timestamp_partition >= '{global_start}'
          AND metric_timestamp_partition < '{global_end}'
        GROUP BY 1
        ORDER BY 1
        """
        users_rows = run_query(ctx, users_sql)

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
          AND doo.created_date >= '{global_start}'
          AND doo.created_date < '{global_end}'
          AND bi.name IS NOT NULL AND TRIM(bi.name) != ''
          AND bi.parent_id IS NULL
          AND bi.type = 'dish'
        GROUP BY TRIM(bi.name)
        ORDER BY qty DESC
        LIMIT 10
        """
        top_items = run_query(ctx, items_sql)
    finally:
        destroy_context(ctx)

    active_by_month: dict[str, int] = {}
    for row in users_rows:
        key = str(row[0])[:10]
        active_by_month[key] = int(row[1] or 0)

    month_data: list[dict] = []
    for row in main_rows:
        dt_key = str(row[0])[:10]
        y, m = int(dt_key[:4]), int(dt_key[5:7])
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
        prod_add = int(row[20] or 0)
        menu_prod_conv = round(float(row[21] or 0) * 100, 2)
        imp_menu = round(menu_view / sessions * 100, 2) if sessions else 0
        active_users = active_by_month.get(dt_key, delivered)
        aov = round(gross / delivered, 0) if delivered else 0
        freq = round(delivered / active_users, 2) if active_users else 0

        month_data.append({
            "year": y, "month": m,
            "label": month_label(y, m),
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
            "active_users": active_users,
            "freq": freq,
            "new_users": new_u,
            "sessions": sessions,
            "imp_menu": imp_menu,
            "menu_prod": menu_prod_conv,
            "rating": rating,
            "discounts": discounts,
            "camp_bolt": camp_bolt,
            "camp_merch": camp_merch,
        })

    items = []
    for i, row in enumerate(top_items, 1):
        items.append({
            "rank": i,
            "name": row[0],
            "qty": int(row[1] or 0),
            "revenue": round(float(row[2] or 0), 0),
            "avg_price": float(row[3] or 0),
        })

    return {
        "months": month_data,
        "top_items": items,
        "period_label": f"{month_label(*months[0])} — {month_label(*months[-1])}",
        "generated_at": datetime.datetime.now().strftime("%d.%m.%Y %H:%M"),
    }


# ─── INSIGHTS ──────────────────────────────────────────────────────────────────

def _pct_change(old: float, new: float) -> float | None:
    if old == 0:
        return None
    return (new - old) / old * 100


def build_insights(months: list[dict]) -> list[dict]:
    if len(months) < 2:
        return [{"type": "info", "title": "Недостатньо даних", "text": "Для порівняння потрібні щонайменше два повні місяці."}]

    a, b = months[0], months[-1]
    m1, m2 = a["label"], b["label"]
    insights: list[dict] = []

    def add(kind: str, title: str, text: str):
        insights.append({"type": kind, "title": title, "text": text})

    # Продажі
    g_chg = _pct_change(a["gross"], b["gross"])
    o_chg = _pct_change(a["orders"], b["orders"])
    if g_chg is not None and g_chg > 5:
        add("positive", "Продажі зростають",
            f"Загальний оборот зріс на {g_chg:.0f}% ({m1} → {m2}). "
            f"Доставлених замовлень стало на {o_chg:.0f}% більше — це хороший сигнал попиту.")
    elif g_chg is not None and g_chg < -5:
        add("warning", "Продажі знизились",
            f"Оборот упав на {abs(g_chg):.0f}%. Варто перевірити графік роботи точок і акції в застосунку.")

    aov_chg = _pct_change(a["aov"], b["aov"])
    if aov_chg is not None and aov_chg > 3:
        add("positive", "Середній чек підвищився",
            f"Клієнти замовляють на більше: середній чек {a['aov']:.0f} → {b['aov']:.0f} ₴ (+{aov_chg:.0f}%).")

    # Операції
    if b["avail"] - a["avail"] >= 3:
        add("positive", "Заклад частіше онлайн",
            f"Доступність на платформі зросла з {a['avail']:.1f}% до {b['avail']:.1f}%. "
            "Клієнти частіше бачать вас у застосунку.")
    elif b["avail"] < 90:
        add("warning", "Низька доступність",
            f"У {m2} заклад був онлайн лише {b['avail']:.1f}% часу. "
            "Кожна година простою — це втрачені замовлення.")

    if b["refunds"] < a["refunds"] - 0.5:
        add("positive", "Менше компенсацій клієнтам",
            f"Частка замовлень з поверненнями знизилась з {a['refunds']:.1f}% до {b['refunds']:.1f}%.")

    if b["del_time"] > a["del_time"] + 1.5:
        add("warning", "Доставка займає більше часу",
            f"Середній час доставки збільшився: {a['del_time']} → {b['del_time']} хв. "
            "Зверніть увагу на швидкість приготування ({a['prep_time']} → {b['prep_time']} хв) "
            "та видачу курʼєру.")

    if b["prep_time"] > a["prep_time"] + 1:
        add("warning", "Час приготування зростає",
            f"Середній час приготування: {a['prep_time']} → {b['prep_time']} хв. "
            "На пікових годинах варто додати людей на кухню або спростити меню.")

    if b["accept"] < 97:
        add("warning", "Не всі замовлення приймаються вчасно",
            f"Рівень прийняття в {m2}: {b['accept']:.1f}%. "
            "Пропущені замовлення — прямий збиток продажів.")

    # Клієнти
    if b["rating"] > a["rating"] + 0.1:
        add("positive", "Клієнти задоволені якістю",
            f"Середній рейтинг зріс з {a['rating']:.2f} до {b['rating']:.2f} з 5 — продовжуйте тримати якість страв.")

    if b["imp_menu"] < 15:
        add("warning", "Мало переходів у меню",
            f"Лише {b['imp_menu']:.1f}% переглядів закладу переходять у меню. "
            "Оновіть обкладинку, фото страв і акційні позиції на головній сторінці профілю.")

    if b["active_users"] > a["active_users"]:
        add("positive", "Більше постійних клієнтів",
            f"Активних користувачів: {a['active_users']} → {b['active_users']}.")

    # Знижки
    d_chg = _pct_change(a["discounts"], b["discounts"])
    if d_chg is not None and d_chg > 10 and b["camp_merch"] == 0:
        add("info", "Зростають знижки від Bolt",
            f"Витрати на знижки для клієнтів зросли на {d_chg:.0f}%. "
            "Зараз кампанії фінансує Bolt — слідкуйте, щоб знижки не знижували маржу без зростання замовлень.")

    if not insights:
        add("info", "Стабільний період",
            "Основні показники без різких змін. Продовжуйте тримати якість та доступність на платформі.")

    return insights


# ─── HTML ──────────────────────────────────────────────────────────────────────

def _js_arr(months: list[dict], key: str) -> str:
    return "[" + ", ".join(str(m[key]) for m in months) + "]"


def _chart_block(cid: str, title: str, unit: str, key: str, months: list[dict], opts: str = "{}") -> str:
    return f"""
    <div class="chart-card">
      <h3>{title}</h3>
      <p class="unit">{unit}</p>
      <div class="chart-wrap"><canvas id="{cid}"></canvas></div>
    </div>
    <script>
    (function(){{
      makeBar("{cid}", {_js_arr(months, key)}, {opts});
    }})();
    </script>"""


def _insight_html(insights: list[dict]) -> str:
    icons = {"positive": "✅", "warning": "⚠️", "info": "ℹ️"}
    parts = []
    for ins in insights:
        icon = icons.get(ins["type"], "•")
        cls = ins["type"]
        parts.append(f"""
        <div class="insight-card {cls}">
          <div class="insight-title">{icon} {ins['title']}</div>
          <p>{ins['text']}</p>
        </div>""")
    return "\n".join(parts)


def generate_html(data: dict) -> str:
    months = data["months"]
    items = data["top_items"]
    insights = build_insights(months)
    labels_js = json.dumps([m["label"] for m in months], ensure_ascii=False)
    colors_js = json.dumps(MONTH_COLORS[: len(months)])
    borders_js = json.dumps(MONTH_BORDERS[: len(months)])

    last = months[-1] if months else {}
    period = data["period_label"]
    gen = data["generated_at"]

    sections = [
        ("1. Продажі", [
            ("c-gross", "Gross Sales (загальні продажі)", "UAH до знижок", "gross", "{}"),
            ("c-net", "Net Sales (чистий прибуток)", "UAH після знижок", "net", "{}"),
            ("c-orders", "Delivered Orders", "доставлені замовлення", "orders", "{}"),
            ("c-aov", "AOV (середній чек)", "UAH / замовлення", "aov", '{"dec":true}'),
        ]),
        ("2. Операційні показники", [
            ("c-avail", "Availability Rate", "доступність на платформі · %", "avail", '{"pct":true,"ymin":80,"ymax":101}'),
            ("c-accept", "Acceptance Rate", "прийняті замовлення · %", "accept", '{"pct":true,"ymin":90,"ymax":101}'),
            ("c-refunds", "Orders with Refunds", "компенсації клієнту · %", "refunds", '{"pct":true,"dec":true}'),
            ("c-del", "Average Delivery Time", "хвилини", "del_time", '{"dec":true}'),
            ("c-acc-t", "Avg. Merchant Acceptance Time", "хвилини", "acc_time", '{"dec":true}'),
            ("c-prep", "Avg. Preparation Time", "хвилини", "prep_time", '{"dec":true}'),
            ("c-wait", "Avg. Courier Wait Time", "хвилини", "wait_time", '{"dec":true}'),
            ("c-c2m", "Avg. Courier to Merchant Time", "хвилини", "c2m_time", '{"dec":true}'),
            ("c-c2e", "Avg. Courier to Eater Time", "хвилини", "c2e_time", '{"dec":true}'),
        ]),
        ("3. Клієнти та їх поведінка", [
            ("c-active", "Active Users", "унікальні замовники", "active_users", "{}"),
            ("c-freq", "Order Frequency", "замовлень / користувач", "freq", '{"dec":true}'),
            ("c-new", "New Users", "нові клієнти бренду", "new_users", "{}"),
            ("c-sess", "Sessions with Impressions", "перегляди закладу", "sessions", "{}"),
            ("c-imp", "Impression → Menu Viewed", "конверсія · %", "imp_menu", '{"pct":true,"dec":true}'),
            ("c-menu", "Menu Viewed → Product Added", "конверсія · %", "menu_prod", '{"pct":true,"dec":true}'),
            ("c-rating", "Average Merchant Rating", "оцінка 0–5", "rating", '{"dec":true,"ymin":0,"ymax":5.5}'),
        ]),
        ("4. Знижки", [
            ("c-disc", "Total Discounts for Users", "UAH", "discounts", "{}"),
            ("c-bolt", "Campaigns Spend by Bolt", "UAH", "camp_bolt", "{}"),
            ("c-merch", "Campaigns Spend by Merchant", "UAH", "camp_merch", "{}"),
        ]),
    ]

    charts_html = ""
    for section_title, chart_defs in sections:
        charts_html += f'<div class="section-title">{section_title}</div><div class="charts-grid">'
        for cid, title, unit, key, opts in chart_defs:
            charts_html += _chart_block(cid, title, unit, key, months, opts)
        charts_html += "</div>"

    items_rows = ""
    for it in items:
        items_rows += f"""
        <tr>
          <td class="rank">{it['rank']}</td>
          <td><strong>{it['name']}</strong></td>
          <td class="num">{it['qty']:,}</td>
          <td class="num">{it['revenue']:,} ₴</td>
          <td class="num">{it['avg_price']:.0f} ₴</td>
        </tr>""".replace(",", "\u202f")

    month_chips = ""
    for i, m in enumerate(months):
        month_chips += f'<span class="month-chip m{i+1}">{m["label"]}</span>\n'

    kpi = last
    kpi_block = f"""
    <div class="kpi-grid">
      <div class="kpi-card"><div class="kpi-label">Gross Sales</div><div class="kpi-value">{kpi.get('gross',0):,.0f} ₴</div></div>
      <div class="kpi-card"><div class="kpi-label">Delivered Orders</div><div class="kpi-value">{kpi.get('orders',0)}</div></div>
      <div class="kpi-card"><div class="kpi-label">AOV</div><div class="kpi-value">{kpi.get('aov',0):,.0f} ₴</div></div>
      <div class="kpi-card"><div class="kpi-label">Рейтинг</div><div class="kpi-value">{kpi.get('rating',0):.2f}</div></div>
      <div class="kpi-card"><div class="kpi-label">Доступність</div><div class="kpi-value">{kpi.get('avail',0):.1f}%</div></div>
      <div class="kpi-card"><div class="kpi-label">Активні клієнти</div><div class="kpi-value">{kpi.get('active_users',0)}</div></div>
    </div>""".replace(",", "\u202f")

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>MAVRA PIZZA — MBR · {period}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      --green:#34D186; --green-dark:#1aad6a; --green-darker:#0d8a52;
      --black:#0d0d0d; --gray-700:#4a4a4a; --gray-400:#9a9a9a; --gray-100:#f5f5f5; --white:#fff;
      --positive:#1aad6a; --warning:#e67e22; --info:#2980b9;
    }}
    *{{margin:0;padding:0;box-sizing:border-box}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;font-size:14px;line-height:1.55;color:#1a1a1a;background:var(--gray-100)}}
    .header{{background:var(--black);padding:28px 40px;display:flex;align-items:center;justify-content:space-between;border-bottom:4px solid var(--green);flex-wrap:wrap;gap:16px}}
    .header-logo{{display:flex;align-items:center;gap:14px}}
    .bolt-logo{{width:44px;height:44px;background:var(--green);border-radius:10px;display:flex;align-items:center;justify-content:center}}
  .header-title h1{{font-size:22px;font-weight:700;color:#fff}}
    .header-title p{{font-size:11px;color:var(--green);text-transform:uppercase;letter-spacing:1.2px;font-weight:600;margin-top:4px}}
    .header-meta{{text-align:right;color:var(--gray-400);font-size:12px;line-height:1.9}}
    .header-meta strong{{color:var(--green)}}
    .container{{max-width:1280px;margin:0 auto;padding:32px 40px}}
    .period-bar{{background:#fff;border-radius:12px;padding:16px 24px;margin-bottom:28px;display:flex;align-items:center;gap:12px;flex-wrap:wrap;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .period-bar .label{{font-size:11px;font-weight:700;text-transform:uppercase;color:var(--gray-700)}}
    .month-chip{{padding:6px 14px;border-radius:20px;font-size:12px;font-weight:600;color:#fff}}
    .m1{{background:var(--green-darker)}} .m2{{background:var(--green)}}
    .section-title{{font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:var(--gray-700);padding-bottom:10px;border-bottom:2px solid var(--green);margin:28px 0 18px}}
    .kpi-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:14px;margin-bottom:8px}}
    .kpi-card{{background:#fff;border-radius:12px;padding:16px 18px;border-top:3px solid var(--green);box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .kpi-label{{font-size:10px;font-weight:700;text-transform:uppercase;color:var(--gray-400);margin-bottom:4px}}
    .kpi-value{{font-size:22px;font-weight:700}}
    .charts-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(460px,1fr));gap:20px;margin-bottom:12px}}
    .chart-card{{background:#fff;border-radius:12px;padding:18px 22px;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .chart-card h3{{font-size:12px;font-weight:700;color:var(--gray-700);margin-bottom:2px}}
    .chart-card .unit{{font-size:10px;color:var(--gray-400);margin-bottom:12px}}
    .chart-wrap{{height:220px;position:relative}}
    .insights-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:16px;margin-bottom:32px}}
    .insight-card{{background:#fff;border-radius:12px;padding:18px 20px;border-left:4px solid var(--gray-400);box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .insight-card.positive{{border-left-color:var(--positive)}}
    .insight-card.warning{{border-left-color:var(--warning)}}
    .insight-card.info{{border-left-color:var(--info)}}
    .insight-title{{font-weight:700;font-size:14px;margin-bottom:8px}}
    .insight-card p{{font-size:13px;color:var(--gray-700)}}
    .table-wrap{{background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.06);margin-bottom:32px}}
    table{{width:100%;border-collapse:collapse}}
    th{{background:var(--black);color:#fff;font-size:11px;font-weight:700;text-transform:uppercase;padding:12px 16px;text-align:left}}
    th.num, td.num{{text-align:right}}
    td{{padding:10px 16px;border-bottom:1px solid #f0f0f0;font-size:13px}}
    td.rank{{color:var(--green-darker);font-weight:700;width:48px}}
    tr:hover td{{background:#e6faf2}}
    .footer{{background:var(--black);color:var(--gray-400);font-size:11px;padding:22px 40px;text-align:center}}
    .footer span{{color:var(--green)}}
    @media(max-width:700px){{.container{{padding:20px 16px}}.charts-grid{{grid-template-columns:1fr}}}}
  </style>
</head>
<body>
<header class="header">
  <div class="header-logo">
    <div class="bolt-logo">
      <svg viewBox="0 0 24 24" width="26" height="26"><path d="M13 2L4.5 13.5H11L10 22L19.5 10.5H13V2Z" fill="#0d0d0d"/></svg>
    </div>
    <div class="header-title">
      <h1>MAVRA PIZZA · {CITY_UK}</h1>
      <p>Bolt Food · Щомісячний звіт (MBR)</p>
    </div>
  </div>
  <div class="header-meta">
    <div>Період: <strong>{period}</strong></div>
    <div>Локацій: <strong>9 точок</strong></div>
    <div>Оновлено: <strong>{gen}</strong></div>
  </div>
</header>

<div class="container">
  <div class="period-bar">
    <span class="label">Порівняння місяців:</span>
    {month_chips}
    <span style="margin-left:auto;font-size:11px;color:var(--gray-400)">Валюта: UAH · 9 локацій Запоріжжя</span>
  </div>

  <div class="section-title">Ключові показники — {last.get('label','')}</div>
  {kpi_block}

  {charts_html}

  <div class="section-title">ТОП-10 позицій меню (Order Item Report)</div>
  <p style="font-size:12px;color:var(--gray-400);margin:-8px 0 14px">За весь період звіту · найчастіше замовлювані страви</p>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Назва позиції</th>
          <th class="num">Кількість</th>
          <th class="num">Сума продажів</th>
          <th class="num">Сер. ціна</th>
        </tr>
      </thead>
      <tbody>{items_rows}</tbody>
    </table>
  </div>

  <div class="section-title">Висновки та рекомендації для партнера</div>
  <p style="font-size:13px;color:var(--gray-700);margin-bottom:16px">
    Короткий аналіз динаміки між {months[0]['label'] if months else ''} та {months[-1]['label'] if months else ''}.
    Зосередьтесь на пунктах з ⚠️ — там найбільший потенціал для покращення.
  </p>
  <div class="insights-grid">{_insight_html(insights)}</div>
</div>

<footer class="footer">
  <span>Bolt Food</span> · MBR MAVRA PIZZA · Запоріжжя ·
  Дані: Databricks · Автооновлення: 1-го числа кожного місяця о 15:00 (Київ) ·
  <a href="https://github.com/marharytazhytnyk-create/partner-reports/tree/main/MBR%20MAVRA%20PIZZA" style="color:var(--green)">GitHub</a>
</footer>

<script>
const MONTH_LABELS = {labels_js};
const COLORS = {colors_js};
const BORDERS = {borders_js};

function makeBar(id, data, opts) {{
  const el = document.getElementById(id);
  if (!el) return;
  new Chart(el, {{
    type: 'bar',
    data: {{
      labels: MONTH_LABELS,
      datasets: [{{
        data,
        backgroundColor: COLORS,
        borderColor: BORDERS,
        borderWidth: 1.5,
        borderRadius: 6,
      }}]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{
            label: (c) => {{
              const v = c.parsed.y;
              if (opts.pct) return ' ' + v.toFixed(2) + '%';
              if (opts.dec) return ' ' + v.toFixed(2);
              return ' ' + v.toLocaleString('uk-UA');
            }}
          }}
        }}
      }},
      scales: {{
        x: {{ grid: {{ display: false }} }},
        y: {{
          beginAtZero: opts.ymin === undefined,
          min: opts.ymin,
          max: opts.ymax,
          ticks: {{
            callback: (v) => opts.pct ? v + '%' : v.toLocaleString('uk-UA')
          }}
        }}
      }}
    }}
  }});
}}
</script>
</body>
</html>"""


def main() -> None:
    if not DATABRICKS_TOKEN:
        print("ERROR: DATABRICKS_TOKEN не задано.", file=sys.stderr)
        sys.exit(1)

    months_range = last_n_full_months(2)
    print(f"MAVRA PIZZA MBR — період: {month_label(*months_range[0])} — {month_label(*months_range[-1])}")

    data = fetch_metrics(months_range)
    print(f"  Місяців даних: {len(data['months'])}, ТОП позицій: {len(data['top_items'])}")

    html = generate_html(data)
    OUTPUT_FILE.write_text(html, encoding="utf-8")
    print(f"Звіт збережено → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
