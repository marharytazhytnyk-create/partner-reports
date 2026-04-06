"""
Bolt Food — SHAVUCHA TEAM Cherkasy Partner Report Generator
============================================================
Queries Databricks fact_provider_weekly for vendor 117729 (Шавуха Team, Черкаси)
and produces an HTML report saved to "Шавуха Team/index.html".

Required env vars:
  DATABRICKS_HOST        e.g. https://bolt-incentives.cloud.databricks.com
  DATABRICKS_TOKEN       Databricks PAT
  DATABRICKS_CLUSTER_ID  Running all-purpose cluster ID
"""

import os
import sys
import json
import time
import math
import datetime
import requests
from pathlib import Path

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DATABRICKS_HOST       = os.getenv("DATABRICKS_HOST", "https://bolt-incentives.cloud.databricks.com")
DATABRICKS_TOKEN      = os.getenv("DATABRICKS_TOKEN", "")
CLUSTER_ID            = os.getenv("DATABRICKS_CLUSTER_ID", "0221-081903-9ag4bh69")
VENDOR_IDS            = [117729, 62587]
# All 3 active locations of the brand in Cherkasy
PROVIDER_IDS          = [157668, 93073, 91332]
# 157668 — Шавуха street food вул. Смілянська (vendor 117729)
# 91332  — Шавуха Team вул. Припортова       (vendor 62587)
# 93073  — Шавуха Team вул. Хрещатик         (vendor 62587)
OUTPUT_DIR            = Path(__file__).parent.parent / "Шавуха Team"
OUTPUT_FILE           = OUTPUT_DIR / "Шавуха_Team.html"
POLL_INTERVAL_S       = 5
MAX_POLL_S            = 300

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type":  "application/json",
}

# ─── DATABRICKS HELPERS ────────────────────────────────────────────────────────

def _post(path: str, payload: dict) -> dict:
    r = requests.post(f"{DATABRICKS_HOST}{path}", headers=HEADERS, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


def _get(path: str, params: dict) -> dict:
    r = requests.get(f"{DATABRICKS_HOST}{path}", headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def create_context(language: str = "sql") -> str:
    return _post("/api/1.2/contexts/create", {"language": language, "clusterId": CLUSTER_ID})["id"]


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
                raise RuntimeError(f"Query error: {result.get('summary')}")
            return result.get("data", [])
        if status in ("Cancelled", "Error"):
            raise RuntimeError(f"Command {status}: {resp}")

    raise TimeoutError(f"Query timed out after {MAX_POLL_S}s")


def destroy_context(ctx_id: str) -> None:
    try:
        _post("/api/1.2/contexts/destroy",
              {"clusterId": CLUSTER_ID, "contextId": ctx_id})
    except Exception:
        pass


# ─── DATA FETCHING ─────────────────────────────────────────────────────────────

def fetch_metrics() -> dict:
    """Return last 4 completed weekly rows for SHAVUCHA TEAM."""
    ctx = create_context()
    try:
        pids_sql = ", ".join(str(p) for p in PROVIDER_IDS)

        # Main metrics query — aggregate across all 3 providers
        main_sql = f"""
        SELECT
            metric_timestamp_partition,
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
            AVG(menu_dish_photo_coverage_rate_value)                 AS photo_coverage,
            SUM(total_campaign_discount)                             AS discounts,
            SUM(total_campaign_spend_bolt)                           AS camp_bolt,
            SUM(total_campaign_spend_provider)                       AS camp_merchant,
            SUM(provider_impressions_sessions_count)                 AS sessions,
            SUM(provider_menu_viewed_sessions_count)                 AS menu_viewed,
            SUM(provider_product_added_sessions_count)               AS prod_added,
            AVG(provider_product_added_from_menu_viewed_rate_value)  AS menu_prod_rate
        FROM ng_delivery_spark.fact_provider_weekly
        WHERE provider_id IN ({pids_sql})
          AND metric_timestamp_partition >= date_sub(current_date(), 28)
        GROUP BY metric_timestamp_partition
        ORDER BY metric_timestamp_partition DESC
        LIMIT 4
        """
        main_rows = run_query(ctx, main_sql)

        # Unique active users across all 3 providers (non-additive, summed)
        pids_str = ", ".join(f"'{p}'" for p in PROVIDER_IDS)
        users_sql = f"""
        SELECT metric_timestamp_partition, SUM(provider_deliveries_unique_user_count) AS active_users
        FROM ng_delivery_spark.int_provider_metrics_non_additive
        WHERE entity_id IN ({pids_str})
          AND timeframe_name = 'week'
          AND metric_timestamp_partition BETWEEN
              date_sub(current_date(), 28) AND current_date()
        GROUP BY metric_timestamp_partition
        LIMIT 8
        """
        users_rows = run_query(ctx, users_sql)
    finally:
        destroy_context(ctx)

    # index unique users by date
    unique_users = {str(r[0]): int(r[1] or 0) for r in users_rows}

    weeks = []
    for row in reversed(main_rows):   # oldest → newest
        dt   = str(row[0])             # "2026-03-09"
        d    = datetime.date.fromisoformat(dt)
        d_end = d + datetime.timedelta(days=6)

        delivered = int(row[1] or 0)
        gross = float(row[2] or 0)
        net   = float(row[3] or 0)
        aov   = round(gross / delivered, 2) if delivered else 0
        new_u = int(row[4] or 0)
        acc_r = round(float(row[5] or 0) * 100, 2)
        avl_r = round(float(row[6] or 0) * 100, 2)
        refund_r = round(float(row[7] or 0) * 100, 2)
        rating = float(row[8] or 0)
        del_time  = round(float(row[9]  or 0), 2)
        acc_time  = round(float(row[10] or 0), 2)
        prep_time = round(float(row[11] or 0), 2)
        wait_time = round(float(row[12] or 0), 2)
        c2m_time  = round(float(row[13] or 0), 2)
        c2e_time  = round(float(row[14] or 0), 2)
        photo_cov = round(float(row[15] or 0) * 100, 2)
        discounts = round(float(row[16] or 0), 2)
        camp_bolt = round(float(row[17] or 0), 2)
        camp_merch = round(float(row[18] or 0), 2)
        sessions  = int(row[19] or 0)
        menu_view = int(row[20] or 0)
        prod_add  = int(row[21] or 0)
        imp_menu_conv  = round(menu_view / sessions * 100, 2) if sessions else 0
        menu_prod_conv = round(float(row[22] or 0) * 100, 2)

        active_users = unique_users.get(dt, delivered)  # fallback to orders count
        freq = round(delivered / active_users, 2) if active_users else 0

        weeks.append({
            "date_start":  dt,
            "date_end":    d_end.isoformat(),
            "label":       f"{d.day} {_uk_month(d.month)}–{d_end.day} {_uk_month(d_end.month)}",
            "gross":       gross,
            "net":         net,
            "orders":      delivered,
            "aov":         aov,
            "avail":       avl_r,
            "accept":      acc_r,
            "active_users": active_users,
            "freq":        freq,
            "new_users":   new_u,
            "discounts":   discounts,
            "camp_bolt":   camp_bolt,
            "camp_merch":  camp_merch,
            "sessions":    sessions,
            "imp_menu":    imp_menu_conv,
            "menu_prod":   menu_prod_conv,
            "photo_cov":   photo_cov,
            "refunds":     refund_r,
            "del_time":    del_time,
            "acc_time":    acc_time,
            "prep_time":   prep_time,
            "wait_time":   wait_time,
            "c2m_time":    c2m_time,
            "c2e_time":    c2e_time,
            "rating":      rating,
        })

    return {"weeks": weeks, "generated_at": datetime.datetime.utcnow().isoformat()}


def _uk_month(m: int) -> str:
    return ["", "січ.", "лют.", "бер.", "квіт.", "трав.", "черв.",
            "лип.", "серп.", "вер.", "жовт.", "лист.", "груд."][m]


# ─── HTML GENERATION ──────────────────────────────────────────────────────────

WEEK_COLORS = ["#34D186", "#1aad6a", "#0d8a52", "#066637"]
WEEK_BORDERS = ["#2ab872", "#1a9a5f", "#0b7545", "#055430"]


def _js_arr(data: list, key: str) -> str:
    return "[" + ", ".join(str(w[key]) for w in data) + "]"


def _js_labels(data: list) -> str:
    return "[" + ", ".join(f'"{w[\"label\"]}"' for w in data) + "]"


def _kpi(label: str, val, sub: str = "") -> str:
    return f"""
    <div class="kpi-card">
      <div class="kpi-label">{label}</div>
      <div class="kpi-value">{val}</div>
      {'<div class="kpi-sub">' + sub + '</div>' if sub else ""}
    </div>"""


def _chart(cid: str, title: str, unit: str, key: str, weeks: list,
           opts: str = "{}") -> str:
    return f"""
    <div class="chart-card">
      <h3>{title}</h3>
      <p class="unit">{unit}</p>
      <div class="chart-wrap"><canvas id="{cid}"></canvas></div>
    </div>
    <script>
    (function(){{
      const d = {_js_arr(weeks, key)};
      const opt = {opts};
      makeBar("{cid}", d, opt);
    }})();
    </script>"""


def _table_row(label: str, weeks: list, key: str, fmt: str = "num") -> str:
    cells = ""
    for i, w in enumerate(weeks):
        v = w[key]
        if fmt == "pct":
            disp = f"{v:.2f}%"
        elif fmt == "dec":
            disp = f"{v:.2f}"
        else:
            disp = f"{v:,.2f}".replace(",", "\u00a0")
        cells += f'<td class="num week-w{i+1}">{disp}</td>'
    return f"<tr><td class='metric-name'>{label}</td>{cells}</tr>"


def _col_headers(weeks: list) -> str:
    hdr = "<tr><th>Метрика</th>"
    for i, w in enumerate(weeks):
        hdr += f'<th class="num week-h{i+1}">W{i+1} · {w["label"]}</th>'
    hdr += "</tr>"
    return hdr


def generate_html(data: dict) -> str:
    weeks = data["weeks"]
    last  = weeks[-1]
    gen   = data["generated_at"][:10]
    week_chips = ""
    chip_cls = ["w1","w2","w3","w4"]
    for i, w in enumerate(weeks):
        week_chips += f'<span class="week-chip {chip_cls[i]}">W{i+1} · {w["label"]}</span>\n'

    kpi_block = "".join([
        _kpi("Валовий продаж",              f'{last["gross"]:,.0f}'.replace(",", "\u00a0"), "UAH"),
        _kpi("Чистий продаж",               f'{last["net"]:,.0f}'.replace(",", "\u00a0"),   "UAH"),
        _kpi("Доставлені замовлення",        last["orders"],      "замовлень"),
        _kpi("Середній чек (AOV)",           f'{last["aov"]:.0f}', "UAH / замовлення"),
        _kpi("Прийняття замовлень",          f'{last["accept"]:.1f}%', "прийнято"),
        _kpi("Доступність ресторану",        f'{last["avail"]:.1f}%',  "онлайн"),
        _kpi("Активні користувачі",          last["active_users"], "унікальних"),
        _kpi("Частота замовлень",            f'{last["freq"]:.2f}', "замовлень / користувач"),
        _kpi("Нові користувачі бренду",      last["new_users"],    "нових клієнтів"),
        _kpi("Середній час доставки",        f'{last["del_time"]:.1f}', "хвилин"),
        _kpi("Середній рейтинг ресторану",   f'{last["rating"]:.1f}', "з 5.0"),
        _kpi("Покриття фото страв меню",     f'{last["photo_cov"]:.0f}%', "страв з фото"),
    ])

    colors_js = json.dumps(WEEK_COLORS)
    borders_js = json.dumps(WEEK_BORDERS)
    labels_js  = _js_labels(weeks)

    charts_block = ""
    chart_defs = [
        ("c-gross",      "Валовий продаж",                              "Сума продажів до знижок · UAH",             "gross",       "{}"),
        ("c-net",        "Чистий продаж",                               "Сума продажів після знижок · UAH",          "net",         "{}"),
        ("c-orders",     "Доставлені замовлення",                       "Кількість виконаних замовлень · шт.",        "orders",      "{}"),
        ("c-aov",        "Середній чек (AOV)",                          "Валовий продаж / замовлення · UAH",          "aov",         '{"dec":true}'),
        ("c-avail",      "Доступність ресторану",                       "Відсоток часу онлайн · %",                   "avail",       '{"pct":true,"ymin":90,"ymax":101}'),
        ("c-accept",     "Прийняття замовлень",                         "Відсоток прийнятих замовлень · %",           "accept",      '{"pct":true,"ymin":90,"ymax":101}'),
        ("c-users",      "Активні користувачі",                         "Унікальні замовники за тиждень · осіб",      "active_users","{}"),
        ("c-freq",       "Частота замовлень",                           "Середня кількість замовлень на 1 користувача","freq",        '{"dec":true}'),
        ("c-new-users",  "Нові користувачі бренду",                     "Вперше замовили у бренду · осіб",            "new_users",   "{}"),
        ("c-discounts",  "Загальні знижки для покупців",                "Різниця між валовим і чистим продажем · UAH","discounts",   "{}"),
        ("c-camp-bolt",  "Витрати Bolt на кампанії",                    "Фінансування кампаній від Bolt · UAH",       "camp_bolt",   "{}"),
        ("c-camp-merch", "Витрати мерчанта на кампанії",                "Фінансування кампаній від партнера · UAH",   "camp_merch",  "{}"),
        ("c-sessions",   "Сесії з показами",                            "Сесії, де ресторан було показано · шт.",     "sessions",    "{}"),
        ("c-imp-menu",   "Конверсія: Показ → Перегляд меню",            "Відсоток переглядів меню від показів · %",   "imp_menu",    '{"pct":true,"dec":true}'),
        ("c-menu-prod",  "Конверсія: Меню → Додавання до кошика",       "Частка додавань у кошик від перегляду · %",  "menu_prod",   '{"pct":true,"dec":true}'),
        ("c-photo",      "Покриття фото страв меню",                    "Частка страв із фотографією · %",            "photo_cov",   '{"pct":true,"ymin":0,"ymax":105}'),
        ("c-refunds",    "Замовлення з поверненнями",                   "Частка замовлень з поверненням коштів · %",  "refunds",     '{"pct":true,"dec":true}'),
        ("c-del-time",   "Середній час доставки",                       "Від підтвердження до вручення · хв.",        "del_time",    '{"dec":true}'),
        ("c-acc-time",   "Час прийняття замовлення",                    "Середній час підтвердження мерчантом · хв.", "acc_time",    '{"dec":true}'),
        ("c-prep-time",  "Час приготування",                            "Середній час готування страв · хв.",         "prep_time",   '{"dec":true}'),
        ("c-cour-wait",  "Час очікування кур'єра",                      "Середній час очікування у ресторані · хв.",  "wait_time",   '{"dec":true}'),
        ("c-cour-merch", "Час кур'єра до ресторану",                    "Від призначення до прибуття в ресторан · хв.","c2m_time",   '{"dec":true}'),
        ("c-cour-eater", "Час кур'єра до покупця",                      "Від ресторану до доставки покупцю · хв.",    "c2e_time",    '{"dec":true}'),
        ("c-rating",     "Середній рейтинг ресторану",                  "Оцінка від покупців · від 0 до 5",           "rating",      '{"dec":true,"ymin":0,"ymax":5.5}'),
    ]
    for cid, title, unit, key, opts in chart_defs:
        charts_block += _chart(cid, title, unit, key, weeks, opts)

    table_rows = "".join([
        _table_row("Валовий продаж (UAH)",                         weeks, "gross"),
        _table_row("Чистий продаж (UAH)",                          weeks, "net"),
        _table_row("Доставлені замовлення",                        weeks, "orders", "dec"),
        _table_row("Середній чек — AOV (UAH)",                     weeks, "aov",    "dec"),
        _table_row("Доступність ресторану (%)",                    weeks, "avail",  "pct"),
        _table_row("Прийняття замовлень (%)",                      weeks, "accept", "pct"),
        _table_row("Активні користувачі",                          weeks, "active_users", "dec"),
        _table_row("Частота замовлень",                            weeks, "freq",   "dec"),
        _table_row("Нові користувачі бренду",                      weeks, "new_users","dec"),
        _table_row("Загальні знижки для покупців (UAH)",           weeks, "discounts"),
        _table_row("Витрати Bolt на кампанії (UAH)",               weeks, "camp_bolt"),
        _table_row("Витрати мерчанта на кампанії (UAH)",           weeks, "camp_merch"),
        _table_row("Сесії з показами",                             weeks, "sessions","dec"),
        _table_row("Конверсія: Показ → Перегляд меню (%)",         weeks, "imp_menu","pct"),
        _table_row("Конверсія: Меню → Додавання до кошика (%)",    weeks, "menu_prod","pct"),
        _table_row("Покриття фото страв меню (%)",                 weeks, "photo_cov","pct"),
        _table_row("Замовлення з поверненнями (%)",                weeks, "refunds", "pct"),
        _table_row("Середній час доставки (хв.)",                  weeks, "del_time","dec"),
        _table_row("Час прийняття замовлення мерчантом (хв.)",     weeks, "acc_time","dec"),
        _table_row("Час приготування (хв.)",                       weeks, "prep_time","dec"),
        _table_row("Час очікування кур'єра (хв.)",                 weeks, "wait_time","dec"),
        _table_row("Час кур'єра до ресторану (хв.)",               weeks, "c2m_time","dec"),
        _table_row("Час кур'єра до покупця (хв.)",                 weeks, "c2e_time","dec"),
        _table_row("Середній рейтинг ресторану (0–5)",             weeks, "rating",  "dec"),
    ])

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>Шавуха Team — Черкаси | Bolt Food Partner Report</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      --green:#34D186; --green-dark:#1aad6a; --green-light:#e6faf2;
      --black:#0d0d0d; --gray-900:#1a1a1a; --gray-700:#4a4a4a;
      --gray-400:#9a9a9a; --gray-100:#f5f5f5; --white:#ffffff;
      --positive:#1aad6a; --negative:#e53935;
      --week1:#34D186; --week2:#1aad6a; --week3:#0d8a52; --week4:#066637;
    }}
    *{{margin:0;padding:0;box-sizing:border-box;}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;font-size:13px;line-height:1.5;color:var(--gray-900);background:var(--gray-100);-webkit-font-smoothing:antialiased;}}
    .header{{background:var(--black);padding:24px 40px;display:flex;align-items:center;justify-content:space-between;border-bottom:3px solid var(--green);}}
    .header-logo{{display:flex;align-items:center;gap:12px;}}
    .bolt-logo{{width:40px;height:40px;background:var(--green);border-radius:8px;display:flex;align-items:center;justify-content:center;}}
    .bolt-logo svg{{width:24px;height:24px;}}
    .header-title h1{{font-size:20px;font-weight:700;color:var(--white);letter-spacing:-0.3px;}}
    .header-title p{{font-size:11px;color:var(--green);text-transform:uppercase;letter-spacing:1px;font-weight:600;}}
    .header-meta{{text-align:right;color:var(--gray-400);font-size:11px;line-height:1.8;}}
    .header-meta strong{{color:var(--green);}}
    .container{{max-width:1280px;margin:0 auto;padding:32px 40px;}}
    .period-bar{{background:var(--white);border-radius:12px;padding:16px 24px;margin-bottom:28px;display:flex;align-items:center;gap:16px;box-shadow:0 1px 4px rgba(0,0,0,.06);flex-wrap:wrap;}}
    .period-bar .label{{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:var(--gray-700);margin-right:4px;}}
    .week-chip{{display:inline-flex;align-items:center;padding:5px 12px;border-radius:20px;font-size:11px;font-weight:600;color:var(--white);}}
    .w1{{background:var(--week1);}} .w2{{background:var(--week2);}} .w3{{background:var(--week3);}} .w4{{background:var(--week4);}}
    .section-title{{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.9px;color:var(--gray-700);padding-bottom:10px;border-bottom:2px solid var(--green);margin-bottom:20px;}}
    .kpi-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:16px;margin-bottom:36px;}}
    .kpi-card{{background:var(--white);border-radius:12px;padding:18px 20px;box-shadow:0 1px 4px rgba(0,0,0,.06);border-top:3px solid var(--green);}}
    .kpi-label{{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.6px;color:var(--gray-400);margin-bottom:6px;}}
    .kpi-value{{font-size:24px;font-weight:700;color:var(--gray-900);letter-spacing:-0.5px;}}
    .kpi-sub{{font-size:10px;color:var(--gray-400);margin-top:4px;}}
    .charts-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(500px,1fr));gap:24px;margin-bottom:36px;}}
    .chart-card{{background:var(--white);border-radius:12px;padding:20px 24px;box-shadow:0 1px 4px rgba(0,0,0,.06);}}
    .chart-card h3{{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.7px;color:var(--gray-700);margin-bottom:4px;}}
    .chart-card p.unit{{font-size:10px;color:var(--gray-400);margin-bottom:14px;}}
    .chart-wrap{{position:relative;height:220px;}}
    .table-wrap{{background:var(--white);border-radius:12px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.06);margin-bottom:36px;}}
    table{{width:100%;border-collapse:collapse;}}
    th{{background:var(--black);color:var(--white);font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.7px;padding:12px 16px;text-align:left;}}
    th.num{{text-align:right;}}
    td{{padding:10px 16px;font-size:12px;border-bottom:1px solid #f0f0f0;color:var(--gray-900);}}
    td.num{{text-align:right;font-variant-numeric:tabular-nums;}}
    tr:last-child td{{border-bottom:none;}}
    tr:hover td{{background:var(--green-light);}}
    .metric-name{{font-weight:600;}}
    td.week-w1{{color:var(--week1);font-weight:600;}}
    td.week-w2{{color:var(--week2);font-weight:600;}}
    td.week-w3{{color:var(--week3);font-weight:600;}}
    td.week-w4{{color:var(--week4);font-weight:600;}}
    .footer{{background:var(--black);color:var(--gray-400);font-size:10px;padding:20px 40px;text-align:center;letter-spacing:.5px;}}
    .footer span{{color:var(--green);}}
    @media(max-width:700px){{.container{{padding:20px 16px;}}.charts-grid{{grid-template-columns:1fr;}}.header{{flex-direction:column;gap:12px;}}}}
  </style>
</head>
<body>

<header class="header">
  <div class="header-logo">
    <div class="bolt-logo">
      <svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
        <path d="M13 2L4.5 13.5H11L10 22L19.5 10.5H13L13 2Z" fill="#0d0d0d"/>
      </svg>
    </div>
    <div class="header-title">
      <h1>Шавуха Team — Черкаси</h1>
      <p>Bolt Food · Звіт по партнеру</p>
    </div>
  </div>
  <div class="header-meta">
    <div>Vendor ID: <strong>62587 / 117729</strong></div>
    <div>Місто: <strong>Черкаси</strong></div>
    <div>Локацій: <strong>3 точки</strong></div>
    <div>Оновлено: <strong>{gen}</strong></div>
  </div>
</header>

<div class="container">
  <div class="period-bar">
    <span class="label">Період:</span>
    {week_chips}
    <span style="margin-left:auto;font-size:10px;color:var(--gray-400);">Останні 4 завершені тижні · Валюта UAH</span>
  </div>

  <div class="section-title">📊 Ключові показники — останній тиждень</div>
  <div class="kpi-grid">{kpi_block}</div>

  <div class="section-title">📈 Динаміка показників по тижнях</div>
  <div class="charts-grid">{charts_block}</div>

  <div class="section-title">📋 Зведена таблиця всіх метрик</div>
  <div class="table-wrap">
    <table>
      <thead>{_col_headers(weeks)}</thead>
      <tbody>{table_rows}</tbody>
    </table>
  </div>
</div>

<footer class="footer">
  <span>Bolt Food</span> · Звіт по партнеру · Шавуха Team — Черкаси ·
  Локації: вул. Смілянська / вул. Припортова / вул. Хрещатик ·
  Дані: <span>ng_delivery_spark.fact_provider_weekly</span> ·
  Автооновлення: кожного понеділка о 13:30
</footer>

<script>
const WEEKS_LABELS = {labels_js};
const COLORS  = {colors_js};
const BORDERS = {borders_js};

function makeBar(id, data, opts) {{
  const ctx = document.getElementById(id);
  if (!ctx) return;
  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: WEEKS_LABELS,
      datasets: [{{
        data,
        backgroundColor: COLORS,
        borderColor: BORDERS,
        borderWidth: 1.5,
        borderRadius: 6,
        borderSkipped: false,
      }}]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{
            label: (c) => {{
              const v = c.parsed.y;
              return ' ' + (opts.pct ? v.toFixed(2) + '%' : opts.dec ? v.toFixed(2) : v.toLocaleString('uk-UA', {{maximumFractionDigits:2}}));
            }}
          }}
        }}
      }},
      scales: {{
        x: {{ grid: {{ display: false }}, ticks: {{ font: {{ size: 10 }}, color: '#6b6b6b' }} }},
        y: {{
          beginAtZero: opts.ymin === undefined,
          min: opts.ymin,
          max: opts.ymax,
          grid: {{ color: '#f0f0f0' }},
          ticks: {{ font: {{ size: 10 }}, color: '#6b6b6b',
            callback: (v) => opts.pct ? v + '%' : v.toLocaleString('uk-UA') }}
        }}
      }}
    }}
  }});
}}
</script>
</body>
</html>"""


# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    if not DATABRICKS_TOKEN:
        print("ERROR: DATABRICKS_TOKEN env var is not set.", file=sys.stderr)
        sys.exit(1)

    print(f"Fetching metrics for Шавуха Team (vendors={VENDOR_IDS}, providers={PROVIDER_IDS})…")
    data = fetch_metrics()
    print(f"  Got {len(data['weeks'])} weeks of data.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    html = generate_html(data)
    OUTPUT_FILE.write_text(html, encoding="utf-8")
    print(f"Report written → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
