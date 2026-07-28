#!/usr/bin/env python3
"""
MBR — Bella Mozzarella / Pinkman Bar (Харків)
Місячний звіт за останні 8 повних місяців.
Автооновлення: 1-го числа кожного місяця о 13:00 (Київ).
"""

from __future__ import annotations

import datetime
import json
import math
import os
import re
import sys
import time
from pathlib import Path

import requests

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DATABRICKS_HOST    = os.getenv("DATABRICKS_HOST", "https://bolt-incentives.cloud.databricks.com")
CLUSTER_ID         = os.getenv("DATABRICKS_CLUSTER_ID", "0221-081903-9ag4bh69")
DATABRICKS_TOKEN   = os.getenv("DATABRICKS_TOKEN", "")
N_MONTHS           = 8
POLL_INTERVAL_S    = 5
MAX_POLL_S         = 600
SCRIPT_DIR         = Path(__file__).parent

HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

BRANDS: list[dict] = [
    {
        "slug":         "bella-mozzarella",
        "title":        "BELLA MOZZARELLA",
        "city_uk":      "Харків",
        "provider_ids": [201375, 201367, 201462, 201471, 201456, 201465],
        "name_strip":   [r"Bella\s+Mozzarella", r"BELLA\s+MOZZARELLA"],
        "color":        "#C62828",
        "emoji":        "🍕",
    },
    {
        "slug":         "pinkman-bar",
        "title":        "PINKMAN BAR",
        "city_uk":      "Харків",
        "provider_ids": [201452, 201469, 201457, 201467, 201460, 201453],
        "name_strip":   [r"Pinkman\s+Bar", r"PINKMAN\s+BAR"],
        "color":        "#880E4F",
        "emoji":        "🍸",
    },
]

UK_MONTHS = [
    "", "Січ", "Лют", "Бер", "Кві", "Тра", "Чер",
    "Лип", "Сер", "Вер", "Жов", "Лис", "Гру",
]
UK_MONTHS_FULL = [
    "", "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
    "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень",
]


# ─── DATE HELPERS ──────────────────────────────────────────────────────────────

def last_n_full_months(n: int = N_MONTHS) -> list[tuple[int, int]]:
    d = datetime.date.today()
    cur = d.replace(day=1)
    months: list[tuple[int, int]] = []
    for _ in range(n):
        last_prev = cur - datetime.timedelta(days=1)
        months.append((last_prev.year, last_prev.month))
        cur = last_prev.replace(day=1)
    return list(reversed(months))


def month_range_sql(year: int, month: int) -> tuple[str, str]:
    start = datetime.date(year, month, 1)
    end = (datetime.date(year + 1, 1, 1) if month == 12
           else datetime.date(year, month + 1, 1))
    return start.isoformat(), end.isoformat()


def month_key(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def month_label(year: int, month: int, short: bool = False) -> str:
    m = UK_MONTHS[month] if short else UK_MONTHS_FULL[month]
    return f"{m} {year}"


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
    return _post("/api/1.2/contexts/create",
                 {"language": "sql", "clusterId": CLUSTER_ID})["id"]


def run_query(ctx_id: str, sql: str) -> list[list]:
    cmd_id = _post(
        "/api/1.2/commands/execute",
        {"language": "sql", "clusterId": CLUSTER_ID,
         "contextId": ctx_id, "command": sql},
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

def fetch_brand_monthly(provider_ids: list[int]) -> dict[str, dict]:
    """Fetch monthly aggregated metrics for a brand (all locations combined)."""
    months = last_n_full_months(N_MONTHS)
    y0, m0 = months[0]
    y1, m1 = months[-1]
    global_start, _ = month_range_sql(y0, m0)
    _, global_end   = month_range_sql(y1, m1)
    pids_sql = ", ".join(str(p) for p in provider_ids)

    sql = f"""
    SELECT
        DATE_FORMAT(DATE_TRUNC('month', f.metric_timestamp_local), 'yyyy-MM-dd') AS month,
        SUM(f.delivered_orders_count)                             AS orders,
        SUM(f.total_gmv_before_discounts)                         AS gross,
        SUM(f.total_gmv_after_discounts)                          AS net,
        SUM(f.users_activated_vendor_count)                       AS new_users,
        SUM(f.delivered_orders_count * f.provider_active_rate_value)
            / NULLIF(SUM(f.delivered_orders_count), 0)            AS avail_rate,
        SUM(f.delivered_orders_count * f.provider_acceptance_rate_value)
            / NULLIF(SUM(f.delivered_orders_count), 0)            AS accept_rate,
        SUM(f.delivered_orders_count * f.customer_refunded_order_rate_value)
            / NULLIF(SUM(f.delivered_orders_count), 0)            AS refund_rate,
        SUM(f.delivered_orders_count * f.provider_rating_per_order_value)
            / NULLIF(SUM(f.delivered_orders_count), 0)            AS rating,
        SUM(f.delivered_orders_count * f.order_total_minutes_per_order_value)
            / NULLIF(SUM(f.delivered_orders_count), 0)            AS total_minutes,
        SUM(f.delivered_orders_count * f.provider_processing_minutes_per_order_value)
            / NULLIF(SUM(f.delivered_orders_count), 0)            AS prep_minutes,
        SUM(f.total_campaign_discount)                            AS discounts,
        SUM(f.total_provider_campaign_spend_bolt)                 AS camp_bolt,
        SUM(f.total_provider_campaign_spend_provider)             AS camp_merch,
        SUM(f.provider_impressions_sessions_count)                AS impressions,
        SUM(f.provider_menu_viewed_sessions_count)                AS menu_views,
        SUM(f.provider_order_placed_sessions_count)               AS order_placed_sessions,
        SUM(f.total_invoiced_demand_refunds_eur)                  AS demand_refunds,
        SUM(f.total_invoiced_supply_refunds_eur)                  AS supply_refunds
    FROM ng_delivery_spark.fact_provider_weekly f
    WHERE f.provider_id IN ({pids_sql})
      AND CAST(f.metric_timestamp_local AS DATE) >= '{global_start}'
      AND CAST(f.metric_timestamp_local AS DATE) < '{global_end}'
    GROUP BY DATE_TRUNC('month', f.metric_timestamp_local)
    ORDER BY month
    """

    ctx = create_context()
    try:
        rows = run_query(ctx, sql)
    finally:
        destroy_context(ctx)

    result: dict[str, dict] = {}
    for row in rows:
        mk = str(row[0])[:7]
        y, m = int(mk[:4]), int(mk[5:7])
        orders    = int(row[1] or 0)
        gross     = float(row[2] or 0)
        net       = float(row[3] or 0)
        new_u     = int(row[4] or 0)
        avail     = round(float(row[5] or 0) * 100, 1)
        accept    = round(float(row[6] or 0) * 100, 1)
        refund    = round(float(row[7] or 0) * 100, 1)
        rating    = round(float(row[8] or 0), 2)
        tot_min   = round(float(row[9] or 0), 1)
        prep_min  = round(float(row[10] or 0), 1)
        discounts = round(float(row[11] or 0), 0)
        camp_bolt = round(float(row[12] or 0), 0)
        camp_merch= round(float(row[13] or 0), 0)
        impressions  = int(row[14] or 0)
        menu_views   = int(row[15] or 0)
        ord_sessions = int(row[16] or 0)
        demand_ref   = round(float(row[17] or 0), 0)
        supply_ref   = round(float(row[18] or 0), 0)

        aov      = round(gross / orders, 0) if orders else 0
        conv_imp = round(menu_views / impressions * 100, 1) if impressions else 0
        conv_menu= round(ord_sessions / menu_views * 100, 1) if menu_views else 0
        conv_ord = round(ord_sessions / impressions * 100, 1) if impressions else 0

        result[mk] = {
            "month_key": mk,
            "label":     month_label(y, m),
            "label_s":   month_label(y, m, short=True),
            "orders":    orders,
            "gross":     round(gross, 0),
            "net":       round(net, 0),
            "aov":       aov,
            "avail":     avail,
            "accept":    accept,
            "refund":    refund,
            "rating":    rating,
            "total_min": tot_min,
            "prep_min":  prep_min,
            "discounts": discounts,
            "camp_bolt": camp_bolt,
            "camp_merch":camp_merch,
            "new_users": new_u,
            "impressions": impressions,
            "menu_views":  menu_views,
            "conv_imp_to_menu": conv_imp,
            "conv_menu_to_order": conv_menu,
            "conv_imp_to_order": conv_ord,
            "demand_refunds": demand_ref,
            "supply_refunds": supply_ref,
        }
    return result


def fetch_locations_monthly(provider_ids: list[int], name_strip: list[str]) -> dict[str, dict[str, dict]]:
    """Fetch monthly metrics per location. Returns {provider_name: {month_key: data}}."""
    months = last_n_full_months(N_MONTHS)
    y0, m0 = months[0]
    y1, m1 = months[-1]
    global_start, _ = month_range_sql(y0, m0)
    _, global_end   = month_range_sql(y1, m1)
    pids_sql = ", ".join(str(p) for p in provider_ids)

    sql = f"""
    SELECT
        p.provider_id,
        p.provider_name,
        DATE_FORMAT(DATE_TRUNC('month', f.metric_timestamp_local), 'yyyy-MM-dd') AS month,
        SUM(f.delivered_orders_count)                             AS orders,
        SUM(f.total_gmv_before_discounts)                         AS gross,
        SUM(f.delivered_orders_count * f.provider_active_rate_value)
            / NULLIF(SUM(f.delivered_orders_count), 0)            AS avail_rate,
        SUM(f.delivered_orders_count * f.provider_acceptance_rate_value)
            / NULLIF(SUM(f.delivered_orders_count), 0)            AS accept_rate,
        SUM(f.delivered_orders_count * f.customer_refunded_order_rate_value)
            / NULLIF(SUM(f.delivered_orders_count), 0)            AS refund_rate,
        SUM(f.delivered_orders_count * f.provider_rating_per_order_value)
            / NULLIF(SUM(f.delivered_orders_count), 0)            AS rating,
        SUM(f.total_provider_campaign_spend_bolt)                 AS camp_bolt,
        SUM(f.total_provider_campaign_spend_provider)             AS camp_merch,
        SUM(f.provider_impressions_sessions_count)                AS impressions,
        SUM(f.provider_menu_viewed_sessions_count)                AS menu_views,
        SUM(f.provider_order_placed_sessions_count)               AS ord_sessions
    FROM ng_delivery_spark.dim_provider_v2 p
    INNER JOIN ng_delivery_spark.fact_provider_weekly f ON p.provider_id = f.provider_id
    WHERE p.provider_id IN ({pids_sql})
      AND CAST(f.metric_timestamp_local AS DATE) >= '{global_start}'
      AND CAST(f.metric_timestamp_local AS DATE) < '{global_end}'
    GROUP BY p.provider_id, p.provider_name, DATE_TRUNC('month', f.metric_timestamp_local)
    ORDER BY p.provider_name, month
    """

    ctx = create_context()
    try:
        rows = run_query(ctx, sql)
    finally:
        destroy_context(ctx)

    locations: dict[str, dict[str, dict]] = {}
    for row in rows:
        pid   = int(row[0])
        pname = str(row[1])
        mk    = str(row[2])[:7]
        y, m  = int(mk[:4]), int(mk[5:7])
        orders= int(row[3] or 0)
        gross = float(row[4] or 0)
        avail = round(float(row[5] or 0) * 100, 1)
        accept= round(float(row[6] or 0) * 100, 1)
        refund= round(float(row[7] or 0) * 100, 1)
        rating= round(float(row[8] or 0), 2)
        bolt  = round(float(row[9] or 0), 0)
        merch = round(float(row[10] or 0), 0)
        impr  = int(row[11] or 0)
        menu  = int(row[12] or 0)
        ords  = int(row[13] or 0)

        # Strip brand name prefix
        short_name = pname
        for pat in name_strip:
            short_name = re.sub(rf"(?i)^{pat}\s*", "", short_name).strip()
        short_name = short_name or pname

        aov       = round(gross / orders, 0) if orders else 0
        conv_menu = round(ords / menu * 100, 1) if menu else 0
        conv_imp  = round(menu / impr * 100, 1) if impr else 0

        if short_name not in locations:
            locations[short_name] = {}
        locations[short_name][mk] = {
            "label":    month_label(y, m),
            "label_s":  month_label(y, m, short=True),
            "orders":   orders,
            "gross":    round(gross, 0),
            "aov":      aov,
            "avail":    avail,
            "accept":   accept,
            "refund":   refund,
            "rating":   rating,
            "camp_bolt":  bolt,
            "camp_merch": merch,
            "impressions": impr,
            "menu_views":  menu,
            "conv_imp_to_menu":    conv_imp,
            "conv_menu_to_order":  conv_menu,
        }
    return locations


# ─── HTML GENERATION ──────────────────────────────────────────────────────────

def safe_val(v) -> float | None:
    try:
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else round(f, 2)
    except (TypeError, ValueError):
        return None


def fmt_num(v, decimals=0) -> str:
    try:
        f = float(v)
        if decimals == 0:
            return f"{int(round(f)):,}".replace(",", "\u00a0")
        return f"{f:,.{decimals}f}".replace(",", "\u00a0")
    except (TypeError, ValueError):
        return "—"


def fmt_pct(v) -> str:
    try:
        return f"{float(v):.1f}%"
    except (TypeError, ValueError):
        return "—"


def pct_change(curr, prev) -> tuple[float | None, str]:
    """Returns (pct_change, formatted_string)."""
    try:
        c, p = float(curr), float(prev)
        if p == 0:
            return None, "—"
        ch = (c / p - 1) * 100
        sign = "▲" if ch >= 0 else "▼"
        color = "#2E7D32" if ch >= 0 else "#C62828"
        return ch, f'<span style="color:{color};font-size:11px">{sign} {abs(ch):.1f}%</span>'
    except (TypeError, ValueError):
        return None, "—"


CHART_METRICS = [
    ("orders",    "📦 Замовлення",          "шт.",   "#1976D2"),
    ("gross",     "💰 GMV (до знижок)",      "₴",     "#1DC462"),
    ("aov",       "🧾 Середній чек",         "₴",     "#00897B"),
    ("avail",     "🟢 Доступність",          "%",     "#43A047"),
    ("accept",    "✅ Acceptance Rate",      "%",     "#3949AB"),
    ("refund",    "🔄 Refund Rate",          "%",     "#E53935"),
    ("rating",    "⭐ Рейтинг",             "з 5",   "#F9A825"),
    ("total_min", "⏱️ Загальний час доставки","хв",   "#7B1FA2"),
    ("prep_min",  "🍳 Час приготування",     "хв",   "#FB8C00"),
    ("discounts", "🏷️ Загальні знижки",     "₴",     "#8D6E63"),
    ("camp_bolt", "⚡ Знижки Bolt",          "₴",     "#1DC462"),
    ("camp_merch","🤝 Знижки партнера",      "₴",     "#FB8C00"),
    ("new_users", "👤 Нові користувачі",     "осіб",  "#0288D1"),
    ("impressions","👁️ Покази закладу",      "сесій", "#6D4C41"),
    ("menu_views","🍽️ Перегляди меню",      "сесій", "#5C6BC0"),
    ("conv_imp_to_menu","📊 Конверсія показ→меню", "%", "#26A69A"),
    ("conv_menu_to_order","📊 Конверсія меню→замов.", "%", "#EC407A"),
    ("demand_refunds","💸 Demand Refunds",   "₴",     "#C62828"),
    ("supply_refunds","📦 Supply Refunds",   "₴",     "#AD1457"),
]

LOC_METRICS = [
    ("orders",   "Замовлення",          "шт.",  "#1976D2"),
    ("gross",    "GMV",                 "₴",    "#1DC462"),
    ("avail",    "Доступність %",       "%",    "#43A047"),
    ("accept",   "Acceptance %",        "%",    "#3949AB"),
    ("refund",   "Refund Rate %",       "%",    "#E53935"),
    ("rating",   "Рейтинг",            "з 5",  "#F9A825"),
    ("camp_bolt","Знижки Bolt ₴",       "₴",    "#1DC462"),
    ("camp_merch","Знижки партнера ₴",  "₴",    "#FB8C00"),
    ("conv_imp_to_menu", "Конв. показ→меню %", "%", "#26A69A"),
    ("conv_menu_to_order","Конв. меню→замов. %","%","#EC407A"),
]


def build_chart_js(canvas_id: str, labels: list[str], data: list,
                   label: str, color: str, chart_type: str = "bar") -> str:
    data_js = json.dumps([safe_val(v) for v in data])
    labels_js = json.dumps(labels)
    return f"""
    (function() {{
      const ctx = document.getElementById('{canvas_id}');
      if (!ctx) return;
      new Chart(ctx, {{
        type: '{chart_type}',
        data: {{
          labels: {labels_js},
          datasets: [{{
            label: '{label}',
            data: {data_js},
            backgroundColor: '{color}33',
            borderColor: '{color}',
            borderWidth: 2,
            borderRadius: 4,
            tension: 0.3,
            fill: {'true' if chart_type == 'line' else 'false'}
          }}]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: true,
          plugins: {{
            legend: {{ display: false }},
            tooltip: {{ callbacks: {{ label: ctx => ctx.parsed.y !== null ? ctx.parsed.y.toLocaleString('uk-UA') : '\u2014' }} }}
          }},
          scales: {{
            x: {{ ticks: {{ font: {{ size: 10 }}, maxRotation: 30 }}, grid: {{ display: false }} }},
            y: {{ ticks: {{ font: {{ size: 10 }} }}, grid: {{ color: '#f0f0f0' }} }}
          }}
        }}
      }});
    }})();"""


def build_brand_tab(brand: dict, monthly: dict[str, dict],
                    locations: dict[str, dict[str, dict]]) -> str:
    months = last_n_full_months(N_MONTHS)
    month_keys   = [month_key(y, m) for y, m in months]
    month_labels = [monthly.get(mk, {}).get("label_s", mk) for mk in month_keys]

    color = brand["color"]
    slug  = brand["slug"]

    # ── KPI summary (last month vs previous) ──────────────────────────────────
    last_mk = month_keys[-1] if month_keys else None
    prev_mk = month_keys[-2] if len(month_keys) > 1 else None
    last_d  = monthly.get(last_mk, {})
    prev_d  = monthly.get(prev_mk, {})

    kpi_html = ""
    for key, label, unit, col in [
        ("orders",    "Замовлень",    "шт.",  color),
        ("gross",     "GMV",          "₴",    color),
        ("avail",     "Доступність",  "%",    color),
        ("accept",    "Acceptance",   "%",    color),
        ("refund",    "Refund Rate",  "%",    "#E53935"),
        ("rating",    "Рейтинг",     "з 5",  "#F9A825"),
    ]:
        curr_v = last_d.get(key, 0)
        prev_v = prev_d.get(key, 0)
        _, ch_html = pct_change(curr_v, prev_v)
        val_str = (fmt_pct(curr_v) if unit == "%" else
                   f"{fmt_num(curr_v)} {unit}" if unit in ("шт.","з 5") else
                   f"₴{fmt_num(curr_v)}")
        kpi_html += f"""
        <div class="kpi-box" style="border-top:3px solid {col}">
          <div class="kpi-label">{label}</div>
          <div class="kpi-value">{val_str}</div>
          <div class="kpi-delta">{ch_html} vs попередній місяць</div>
        </div>"""

    # ── Main brand charts ───────────────────────────────────────────────────────
    charts_html = ""
    charts_js   = ""
    for metric_key, metric_name, unit, col in CHART_METRICS:
        vals = [monthly.get(mk, {}).get(metric_key) for mk in month_keys]
        cid  = f"chart_{slug}_{metric_key}"
        charts_html += f"""
        <div class="chart-card">
          <h4>{metric_name} <span class="unit">({unit})</span></h4>
          <canvas id="{cid}" height="90"></canvas>
        </div>"""
        charts_js += build_chart_js(cid, month_labels, vals, metric_name, col)

    # ── Locations section ───────────────────────────────────────────────────────
    loc_tabs_html  = ""
    loc_panels_html = ""
    loc_js = ""

    for i, (loc_name, loc_data) in enumerate(sorted(locations.items())):
        loc_id = re.sub(r"[^\w]", "_", f"{slug}_{loc_name}")
        active_cls = "active" if i == 0 else ""

        loc_vals: dict[str, list] = {}
        loc_month_labels = [loc_data.get(mk, {}).get("label_s", mk) for mk in month_keys]
        for mk_key, _, _, _ in LOC_METRICS:
            loc_vals[mk_key] = [loc_data.get(mk, {}).get(mk_key) for mk in month_keys]

        last_loc = loc_data.get(last_mk, {})
        loc_orders = fmt_num(last_loc.get("orders", 0))
        loc_gmv    = f'₴{fmt_num(last_loc.get("gross", 0))}'

        loc_charts_html = ""
        for mk_key, mk_label, mk_unit, mk_col in LOC_METRICS:
            lcid = f"chart_{loc_id}_{mk_key}"
            loc_charts_html += f"""
            <div class="chart-card">
              <h4>{mk_label} <span class="unit">({mk_unit})</span></h4>
              <canvas id="{lcid}" height="90"></canvas>
            </div>"""
            loc_js += build_chart_js(lcid, loc_month_labels, loc_vals[mk_key], mk_label, mk_col)

        loc_tabs_html += f"""
        <button class="loc-tab {active_cls}" onclick="showLoc('{slug}', '{loc_id}', this)"
                style="--loc-color:{color}">
          {loc_name}
          <span class="loc-meta">{loc_orders} зам. · {loc_gmv}</span>
        </button>"""

        loc_panels_html += f"""
        <div class="loc-panel {'active' if i == 0 else ''}" id="panel_{loc_id}">
          <div class="charts-grid">{loc_charts_html}</div>
        </div>"""

    period_label = f"{month_labels[0]} — {month_labels[-1]}" if month_labels else ""

    return f"""
    <div class="brand-section" id="brand_{slug}">
      <div class="period-info" style="border-color:{color}">
        📅 Аналіз за {N_MONTHS} місяців: {period_label}
      </div>

      <!-- KPI row -->
      <div class="kpi-row">{kpi_html}</div>

      <!-- Brand-level monthly charts -->
      <div class="section-header">📊 Динаміка бренду по місяцях</div>
      <div class="charts-grid">{charts_html}</div>

      <!-- Locations -->
      <div class="section-header" style="margin-top:24px">📍 Показники по локаціях</div>
      <div class="loc-tabs" id="loctabs_{slug}">{loc_tabs_html}</div>
      <div class="loc-panels">{loc_panels_html}</div>
    </div>

    <script>
    document.addEventListener('DOMContentLoaded', function() {{
      {charts_js}
      {loc_js}
    }});
    </script>"""


def build_html(brand_sections: list[tuple[dict, str]]) -> str:
    today = datetime.date.today()
    months = last_n_full_months(N_MONTHS)
    period = (f"{month_label(months[0][0], months[0][1])} — "
              f"{month_label(months[-1][0], months[-1][1])}")

    brand_tabs = ""
    brand_content = ""
    for i, (brand, section_html) in enumerate(brand_sections):
        active = "active" if i == 0 else ""
        brand_tabs += f"""
        <div class="brand-tab {active}" id="btab_{brand['slug']}"
             onclick="showBrand('{brand['slug']}')"
             style="--brand-color:{brand['color']}">
          {brand['emoji']} {brand['title']}
        </div>"""
        vis = "block" if i == 0 else "none"
        brand_content += f'<div id="bcontent_{brand["slug"]}" style="display:{vis}">{section_html}</div>'

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MBR — Bella Mozzarella / Pinkman Bar — Харків</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --green: #1DC462; --dark: #1A1A1A; --light: #F7F9FC;
    --border: #E0E0E0; --muted: #666;
    --shadow: 0 2px 8px rgba(0,0,0,0.08);
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
         background: var(--light); color: var(--dark); font-size: 14px; line-height: 1.5; }}

  /* Header */
  .header {{ background: var(--dark); color: #fff; padding: 18px 28px;
             display: flex; align-items: center; gap: 16px;
             position: sticky; top: 0; z-index: 100; box-shadow: 0 2px 12px rgba(0,0,0,0.3); }}
  .header-logo {{ font-size: 20px; font-weight: 800; color: var(--green); }}
  .header-title {{ font-size: 15px; font-weight: 700; }}
  .header-sub {{ font-size: 11px; color: #aaa; margin-top: 2px; }}
  .header-meta {{ margin-left: auto; text-align: right; font-size: 11px; color: #aaa; }}
  .header-meta strong {{ color: var(--green); }}

  /* Brand tabs */
  .brand-nav {{ background: #fff; border-bottom: 2px solid var(--border);
                padding: 0 24px; display: flex; gap: 6px;
                position: sticky; top: 64px; z-index: 90; box-shadow: var(--shadow); }}
  .brand-tab {{ padding: 14px 22px; cursor: pointer; font-size: 13px; font-weight: 700;
                color: var(--muted); border-bottom: 3px solid transparent;
                white-space: nowrap; user-select: none; transition: all 0.2s; }}
  .brand-tab:hover {{ color: var(--brand-color, var(--green)); }}
  .brand-tab.active {{ color: var(--brand-color, var(--green));
                       border-bottom-color: var(--brand-color, var(--green)); }}

  /* Content */
  .content {{ padding: 24px 28px; }}

  /* Period info */
  .period-info {{ background: #f0fdf4; border-left: 4px solid var(--green);
                  padding: 8px 16px; font-size: 12px; font-weight: 600;
                  color: #166534; margin-bottom: 20px; border-radius: 0 8px 8px 0; }}

  /* KPI */
  .kpi-row {{ display: grid; grid-template-columns: repeat(6, 1fr);
              gap: 12px; margin-bottom: 24px; }}
  @media (max-width: 1200px) {{ .kpi-row {{ grid-template-columns: repeat(3, 1fr); }} }}
  @media (max-width: 600px)  {{ .kpi-row {{ grid-template-columns: repeat(2, 1fr); }} }}
  .kpi-box {{ background: #fff; border-radius: 10px; box-shadow: var(--shadow);
              padding: 14px 16px; }}
  .kpi-label {{ font-size: 10px; text-transform: uppercase; font-weight: 700;
                color: var(--muted); letter-spacing: .5px; margin-bottom: 4px; }}
  .kpi-value {{ font-size: 20px; font-weight: 800; color: var(--dark); }}
  .kpi-delta {{ font-size: 11px; color: var(--muted); margin-top: 4px; }}

  /* Charts grid */
  .charts-grid {{ display: grid; grid-template-columns: repeat(3, 1fr);
                  gap: 16px; margin-bottom: 24px; }}
  @media (max-width: 1000px) {{ .charts-grid {{ grid-template-columns: repeat(2, 1fr); }} }}
  @media (max-width: 600px)  {{ .charts-grid {{ grid-template-columns: 1fr; }} }}
  .chart-card {{ background: #fff; border-radius: 12px; box-shadow: var(--shadow); padding: 14px 16px; }}
  .chart-card h4 {{ font-size: 11px; text-transform: uppercase; font-weight: 700;
                    color: var(--muted); letter-spacing: .4px; margin-bottom: 10px; }}
  .chart-card canvas {{ max-height: 200px; }}
  .unit {{ font-weight: 400; font-size: 10px; }}

  /* Section header */
  .section-header {{ font-size: 14px; font-weight: 700; color: var(--dark);
                     margin-bottom: 14px; padding-bottom: 8px;
                     border-bottom: 2px solid var(--border); }}

  /* Location tabs */
  .loc-tabs {{ display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 16px; }}
  .loc-tab {{ padding: 8px 14px; border: 1.5px solid var(--border); border-radius: 8px;
              cursor: pointer; font-size: 12px; font-weight: 600; background: #fff;
              color: var(--muted); transition: all 0.15s; }}
  .loc-tab:hover {{ border-color: var(--loc-color, var(--green)); color: var(--loc-color, var(--green)); }}
  .loc-tab.active {{ background: var(--loc-color, var(--green)); color: #fff; border-color: var(--loc-color, var(--green)); }}
  .loc-meta {{ font-size: 10px; font-weight: 400; margin-left: 4px; opacity: .8; }}
  .loc-panel {{ display: none; }}
  .loc-panel.active {{ display: block; }}

  /* Footer */
  .footer {{ background: var(--dark); color: #888; text-align: center;
             padding: 14px; font-size: 11px; margin-top: 40px; }}
  .footer a {{ color: var(--green); text-decoration: none; }}
</style>
</head>
<body>

<div class="header">
  <div class="header-logo">⚡ Bolt</div>
  <div>
    <div class="header-title">MBR — Bella Mozzarella / Pinkman Bar</div>
    <div class="header-sub">Харків · Місячний звіт · {N_MONTHS} місяців</div>
  </div>
  <div class="header-meta">
    <div>Оновлено: <strong>{today.isoformat()}</strong></div>
    <div>Період: {period}</div>
  </div>
</div>

<div class="brand-nav">
  {brand_tabs}
</div>

<div class="content">
  {brand_content}
</div>

<div class="footer">
  Автоматично оновлюється 1-го числа кожного місяця ·
  <a href="https://github.com/marharytazhytnyk-create/partner-reports">GitHub</a>
</div>

<script>
function showBrand(slug) {{
  document.querySelectorAll('.brand-tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('[id^="bcontent_"]').forEach(el => el.style.display = 'none');
  document.getElementById('btab_' + slug).classList.add('active');
  document.getElementById('bcontent_' + slug).style.display = 'block';
}}

function showLoc(brandSlug, locId, btn) {{
  const tabs   = document.getElementById('loctabs_'  + brandSlug);
  const panels = tabs.nextElementSibling;
  tabs.querySelectorAll('.loc-tab').forEach(t => t.classList.remove('active'));
  panels.querySelectorAll('.loc-panel').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('panel_' + locId).classList.add('active');
}}
</script>
</body>
</html>"""


# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    today = datetime.date.today().isoformat()
    print(f"=== MBR Bella Mozzarella / Pinkman Bar [{today}] ===")

    if not DATABRICKS_TOKEN:
        print("ERROR: DATABRICKS_TOKEN not set"); sys.exit(1)

    brand_sections = []
    for brand in BRANDS:
        print(f"\n📊 Fetching data for {brand['title']}...")
        try:
            monthly   = fetch_brand_monthly(brand["provider_ids"])
            locations = fetch_locations_monthly(brand["provider_ids"], brand["name_strip"])
            print(f"  → {len(monthly)} months, {len(locations)} locations")
            section = build_brand_tab(brand, monthly, locations)
            brand_sections.append((brand, section))
        except Exception as exc:
            print(f"  ERROR: {exc}")
            brand_sections.append((brand, f'<div style="padding:40px;color:#E53935">Помилка: {exc}</div>'))

    html = build_html(brand_sections)
    out_path = SCRIPT_DIR / "MBR Bella Mozzarella Pinkman Bar.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"\n✅ Report saved → {out_path}")


if __name__ == "__main__":
    main()
