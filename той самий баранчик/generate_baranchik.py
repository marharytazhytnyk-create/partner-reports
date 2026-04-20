#!/usr/bin/env python3
"""
Генератор звіту BARANCHIK з Databricks
Запускається кожного понеділка о 13:30 через GitHub Actions

Стратегія даних:
  - 2026 замовлення + виручка: delivery_order_order + delivery_order_booking_item (реальні дані)
  - Якісні метрики (рейтинг, час, акції тощо): fact_vendor_weekly (остання доступна дата ~листопад 2025)
    застосовуються як базові значення до тижнів 2026 року.
"""

import json
import os
import re
import sys
import time
import datetime
import urllib.request
import urllib.error
from pathlib import Path

# ===== КОНФІГУРАЦІЯ =====
DATABRICKS_HOST    = os.environ.get("DATABRICKS_HOST",    "https://bolt-incentives.cloud.databricks.com")
DATABRICKS_TOKEN   = os.environ.get("DATABRICKS_TOKEN",   "")
CLUSTER_ID         = os.environ.get("DATABRICKS_CLUSTER_ID", "0221-081903-9ag4bh69")

VENDOR_IDS = {
    114922: {"name": "BARANCHIK Lviv",       "city_uk": "Львів",      "emoji": "🦁",  "color": "#e94560"},
    114927: {"name": "BARANCHIK Poltava",    "city_uk": "Полтава",    "emoji": "🌻", "color": "#f5a623"},
    114934: {"name": "BARANCHIK Cherkasy",   "city_uk": "Черкаси",    "emoji": "🌊", "color": "#8e44ad"},
    114936: {"name": "BARANCHIK Kharkiv",    "city_uk": "Харків",     "emoji": "🏙️", "color": "#2980b9"},
    114939: {"name": "BARANCHIK Kremenchuk", "city_uk": "Кременчук",  "emoji": "🌿", "color": "#27ae60"},
}

UAH_PER_EUR = 45.5   # Актуальний курс 2026 UAH/EUR

# 8 тижнів до 12.04.2026 (починаючи з 2026-02-16)
REPORT_START = "2026-02-23"
REPORT_END   = "2026-04-19"

SCRIPT_DIR  = Path(__file__).parent
OUTPUT_HTML = SCRIPT_DIR / "index.html"


# ===== DATABRICKS API =====

def api_request(method, path, body=None):
    url = f"{DATABRICKS_HOST}{path}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(f"HTTP Error {e.code}: {e.read().decode()}", file=sys.stderr)
        raise


def create_context():
    result = api_request("POST", "/api/1.2/contexts/create", {
        "language": "sql",
        "clusterId": CLUSTER_ID,
    })
    return result["id"]


def execute_command(context_id, sql):
    result = api_request("POST", "/api/1.2/commands/execute", {
        "language": "sql",
        "clusterId": CLUSTER_ID,
        "contextId": context_id,
        "command": sql,
    })
    return result["id"]


def wait_for_command(context_id, command_id, timeout=300):
    start = time.time()
    while time.time() - start < timeout:
        result = api_request(
            "GET",
            f"/api/1.2/commands/status?clusterId={CLUSTER_ID}"
            f"&contextId={context_id}&commandId={command_id}",
        )
        status = result.get("status")
        if status == "Finished":
            return result.get("results", {})
        elif status in ("Error", "Cancelled"):
            raise RuntimeError(f"Command failed: {result}")
        time.sleep(5)
    raise TimeoutError("Command timed out")


def query(context_id, sql):
    cmd_id = execute_command(context_id, sql)
    results = wait_for_command(context_id, cmd_id)
    schema = results.get("schema", [])
    cols   = [s["name"] for s in schema]
    rows   = results.get("data", [])
    return [dict(zip(cols, row)) for row in rows]


# ===== ЗАПИТИ ДО DATABRICKS =====

def get_fvw_quality_data(context_id):
    """
    Отримує дані якості та фінансових ставок з fact_vendor_weekly.
    Використовується для розрахунку комісійних ставок та базових метрик якості.
    """
    vendor_ids_str = ", ".join(str(v) for v in VENDOR_IDS.keys())
    sql = f"""
    SELECT
        fvw.vendor_id,
        CAST(fvw.metric_timestamp_local AS DATE)          AS week_date,
        COALESCE(fvw.delivered_orders_count, 0)           AS delivered_orders,
        COALESCE(fvw.total_provider_price_before_discounts, 0) AS revenue_uah,
        COALESCE(fvw.total_invoiced_provider_commission_eur, 0) AS commission_eur,
        COALESCE(fvw.total_invoiced_demand_incentives_eur, 0)   AS bolt_incentives_eur,
        COALESCE(fvw.total_invoiced_menu_demand_incentives_eur, 0) AS partner_incentives_eur,
        COALESCE(fvw.provider_acceptance_rate_value * 100, 0)   AS acceptance_rate,
        COALESCE(fvw.bad_provider_rating_rate_value  * 100, 0)  AS bad_rating_pct,
        COALESCE(fvw.good_provider_rating_rate_value * 100, 0)  AS good_rating_pct,
        fvw.provider_rating_per_order_value                     AS avg_rating,
        COALESCE(fvw.late_delivery_order_rate_value  * 100, 0)  AS late_delivery_pct,
        fvw.provider_preparation_minutes_per_order_value        AS prep_time_min,
        COALESCE(fvw.cs_ticket_missing_items_rate_value * 100, 0) AS missing_items_pct,
        COALESCE(fvw.failed_order_rate_value * 100, 0)          AS failed_order_pct_hist,
        COALESCE(fvw.total_delivery_price_before_discounts, 0)  AS delivery_fee_uah
    FROM ng_delivery_spark.fact_vendor_weekly fvw
    WHERE fvw.vendor_id IN ({vendor_ids_str})
    ORDER BY fvw.vendor_id, fvw.metric_timestamp_local
    """
    rows = query(context_id, sql)
    by_vendor = {}
    for row in rows:
        vid = row["vendor_id"]
        by_vendor.setdefault(vid, []).append(row)
    return by_vendor


def get_2026_orders(context_id):
    """Отримує кількість замовлень за 8 тижнів 2026 року з реальних даних."""
    vendor_ids_str = ", ".join(str(v) for v in VENDOR_IDS.keys())
    sql = f"""
    SELECT
        dpp.vendor_id,
        DATE_TRUNC('week', doo.created_date)                    AS week_start,
        COUNT(*)                                                 AS total_placed,
        SUM(CASE WHEN doo.state = 'delivered'              THEN 1 ELSE 0 END) AS delivered,
        SUM(CASE WHEN doo.state IN ('failed',
            'cancelled_by_provider')                       THEN 1 ELSE 0 END) AS failed_count
    FROM ng_delivery_spark.delivery_order_order doo
    JOIN ng_delivery_spark.delivery_provider_provider dpp
         ON doo.provider_id = dpp.id
    WHERE dpp.vendor_id IN ({vendor_ids_str})
      AND doo.created_date >= '{REPORT_START}'
      AND doo.created_date <= '{REPORT_END}'
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    rows = query(context_id, sql)
    by_vendor = {}
    for row in rows:
        vid = row["vendor_id"]
        by_vendor.setdefault(vid, []).append(row)
    return by_vendor


def get_2026_revenue(context_id):
    """Отримує виручку партнера з booking_item за 8 тижнів 2026 року."""
    vendor_ids_str = ", ".join(str(v) for v in VENDOR_IDS.keys())
    sql = f"""
    SELECT
        dpp.vendor_id,
        DATE_TRUNC('week', dobi.created_date)    AS week_start,
        SUM(dobi.booked_amount)                  AS revenue_uah
    FROM ng_delivery_spark.delivery_order_booking_item dobi
    JOIN ng_delivery_spark.delivery_order_order doo
         ON dobi.order_id = doo.id
    JOIN ng_delivery_spark.delivery_provider_provider dpp
         ON doo.provider_id = dpp.id
    WHERE dpp.vendor_id IN ({vendor_ids_str})
      AND dobi.created_date >= '{REPORT_START}'
      AND dobi.created_date <= '{REPORT_END}'
      AND doo.state = 'delivered'
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    rows = query(context_id, sql)
    by_vendor = {}
    for row in rows:
        vid = row["vendor_id"]
        by_vendor.setdefault(vid, []).append(row)
    return by_vendor


# ===== ОБРОБКА ДАНИХ =====

def compute_vendor_rates(fvw_weeks):
    """
    Розраховує середні фінансові ставки та метрики якості по тижнях з 2025 даних.
    Повертає dict із ставками та списком метрик якості (останні 8 тижнів).
    """
    complete = [w for w in fvw_weeks if w["delivered_orders"] > 0]
    if not complete:
        return None

    # Комісійна ставка (commission_eur / revenue_eur)
    comm_rates = []
    for w in complete:
        rev_eur = float(w["revenue_uah"]) / UAH_PER_EUR
        if rev_eur > 0:
            comm_rates.append(float(w["commission_eur"]) / rev_eur)
    avg_commission_rate = sum(comm_rates) / len(comm_rates) if comm_rates else 0.22

    # Bolt incentives per order
    bolt_per_order_eur_list = []
    for w in complete:
        d = float(w["delivered_orders"])
        if d > 0:
            bolt_per_order_eur_list.append(float(w["bolt_incentives_eur"]) / d)
    avg_bolt_per_order_eur = (
        sum(bolt_per_order_eur_list) / len(bolt_per_order_eur_list)
        if bolt_per_order_eur_list else 0.0
    )

    # Partner incentives per order
    partner_per_order_eur_list = []
    for w in complete:
        d = float(w["delivered_orders"])
        if d > 0:
            partner_per_order_eur_list.append(float(w["partner_incentives_eur"]) / d)
    avg_partner_per_order_eur = (
        sum(partner_per_order_eur_list) / len(partner_per_order_eur_list)
        if partner_per_order_eur_list else 0.0
    )

    # Метрики якості — беремо останні 8 тижнів з доступними даними
    quality_weeks = complete[-8:] if len(complete) >= 8 else complete

    def qlist(key):
        return [
            round(float(w[key]), 2) if w.get(key) is not None else None
            for w in quality_weeks
        ]

    return {
        "commission_rate":          avg_commission_rate,
        "bolt_per_order_eur":       avg_bolt_per_order_eur,
        "partner_per_order_eur":    avg_partner_per_order_eur,
        "acceptance_rate":          qlist("acceptance_rate"),
        "bad_rating_pct":           qlist("bad_rating_pct"),
        "good_rating_pct":          qlist("good_rating_pct"),
        "avg_rating":               qlist("avg_rating"),
        "late_delivery_pct":        qlist("late_delivery_pct"),
        "prep_time_min":            qlist("prep_time_min"),
        "missing_items_pct":        qlist("missing_items_pct"),
        "delivery_fee_uah":         qlist("delivery_fee_uah"),
    }


def build_city_data(fvw_data, orders_2026, revenue_2026):
    """
    Будує структуру CITIES_DATA для вбудовування в HTML.

    Фінансові метрики 2026:
      • revenue_uah, avg_order_uah — реальні дані (booking_item)
      • commission_uah, net_profit_uah, bolt_promotion_uah, partner_promotion_uah —
        розраховані на основі середніх ставок 2025 (fact_vendor_weekly)
      • failed_order_pct — реальні дані (delivery_order_order)

    Метрики якості 2026:
      Остання доступна 8-тижнева послідовність з fact_vendor_weekly (2025),
      застосована до тижнів 2026 року.
    """
    cities_data  = {}
    week_labels  = None

    for vid, info in VENDOR_IDS.items():
        fvw_weeks    = fvw_data.get(vid, [])
        ord_weeks    = orders_2026.get(vid, [])
        rev_weeks    = revenue_2026.get(vid, [])

        if not ord_weeks:
            print(f"⚠️  Немає 2026-даних для vendor {vid} ({info['city_uk']})", file=sys.stderr)
            continue

        # Визначити тижневі мітки з реальних 2026-даних
        local_labels = [
            datetime.datetime.strptime(str(r["week_start"])[:10], "%Y-%m-%d").strftime("%d.%m")
            for r in ord_weeks
        ]
        if week_labels is None:
            week_labels = local_labels

        # Фінансові ставки з 2025
        rates = compute_vendor_rates(fvw_weeks) if fvw_weeks else None
        comm_rate         = rates["commission_rate"]      if rates else 0.22
        bolt_per_ord_eur  = rates["bolt_per_order_eur"]  if rates else 0.0
        part_per_ord_eur  = rates["partner_per_order_eur"] if rates else 0.0

        # Вирівняти revenue по тижнях (деякі тижні можуть бути відсутні в booking_item)
        rev_by_week = {str(r["week_start"])[:10]: float(r["revenue_uah"]) for r in rev_weeks}

        n = len(ord_weeks)

        delivered_orders  = []
        placed_orders     = []
        failed_pct        = []
        revenue_uah       = []
        avg_order_uah     = []
        commission_uah    = []
        net_profit_uah    = []
        bolt_prom_uah     = []
        partner_prom_uah  = []

        for i, ow in enumerate(ord_weeks):
            week_key   = str(ow["week_start"])[:10]
            deliv      = int(ow["delivered"])
            placed     = int(ow["total_placed"])
            failed_cnt = int(ow["failed_count"])

            rev  = rev_by_week.get(week_key, 0.0)
            avg  = round(rev / deliv) if deliv > 0 else 0
            comm = round(rev * comm_rate)
            bolt_prom  = round(bolt_per_ord_eur * deliv * UAH_PER_EUR)
            part_prom  = round(part_per_ord_eur * deliv * UAH_PER_EUR)
            net_profit = max(0, round(rev - comm - part_prom))
            f_pct      = round((failed_cnt / placed * 100), 1) if placed > 0 else 0.0

            delivered_orders.append(deliv)
            placed_orders.append(placed)
            failed_pct.append(f_pct)
            revenue_uah.append(round(rev))
            avg_order_uah.append(avg)
            commission_uah.append(comm)
            net_profit_uah.append(net_profit)
            bolt_prom_uah.append(bolt_prom)
            partner_prom_uah.append(part_prom)

        # Метрики якості — з 2025, циклічно, якщо менше 8 тижнів
        def quality_list(key):
            if not rates:
                return [None] * n
            src = rates.get(key, [])
            if not src:
                return [None] * n
            # Повторити або обрізати до n значень
            return [(src[i % len(src)] if src else None) for i in range(n)]

        cities_data[info["city_uk"]] = {
            "color":     info["color"],
            "emoji":     info["emoji"],
            "vendor_id": vid,
            "metrics": {
                "delivered_orders":   delivered_orders,
                "placed_orders":      placed_orders,
                "failed_order_pct":   failed_pct,
                "revenue_uah":        revenue_uah,
                "avg_order_uah":      avg_order_uah,
                "commission_uah":     commission_uah,
                "net_profit_uah":     net_profit_uah,
                "bolt_promotion_uah": bolt_prom_uah,
                "partner_promotion_uah": partner_prom_uah,
                "delivery_fee_uah":   quality_list("delivery_fee_uah"),
                "acceptance_rate":    quality_list("acceptance_rate"),
                "bad_rating_pct":     quality_list("bad_rating_pct"),
                "good_rating_pct":    quality_list("good_rating_pct"),
                "avg_rating":         quality_list("avg_rating"),
                "late_delivery_pct":  quality_list("late_delivery_pct"),
                "prep_time_min":      quality_list("prep_time_min"),
                "missing_items_pct":  quality_list("missing_items_pct"),
            },
        }

    return cities_data, week_labels


# ===== HTML ОНОВЛЕННЯ =====

def update_html(cities_data, week_labels):
    """Оновлює index.html з новими даними."""
    html_path = OUTPUT_HTML
    html = html_path.read_text(encoding="utf-8")

    # Повні дати для тижневих міток (2026)
    weeks_full = []
    if week_labels:
        for lbl in week_labels:
            d, m = lbl.split(".")
            weeks_full.append(f"{d}.{m}.2026")

    # Діапазон дат для заголовку
    if week_labels and len(week_labels) >= 2:
        # Перший тиждень: початкова дата
        # Останній тиждень: +6 днів від дати початку
        last_start = datetime.datetime.strptime(f"{week_labels[-1]}.2026", "%d.%m.%Y")
        last_end   = last_start + datetime.timedelta(days=6)
        date_range = (
            f"{week_labels[0]}.2026 — "
            f"{last_end.strftime('%d.%m.%Y')}"
        )
    else:
        date_range = "Актуальні дані"

    # Блок даних для вставки
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    new_data_block = (
        f"const WEEKS = {json.dumps(week_labels or [], ensure_ascii=False)};\n"
        f"const WEEKS_FULL = {json.dumps(weeks_full, ensure_ascii=False)};\n"
        f"const RATE = {UAH_PER_EUR};\n\n"
        f"const CITIES_DATA = {json.dumps(cities_data, ensure_ascii=False, indent=2)};"
    )

    header = (
        f"// ===== EMBEDDED DATA (auto-generated by generate_baranchik.py)\n"
        f"// Дата оновлення: {now_str}\n"
        f"// Джерело замовлень/виручки: ng_delivery_spark (2026, реальні дані)\n"
        f"// Метрики якості та ставки: ng_delivery_spark.fact_vendor_weekly (2025, базові значення)\n"
        f"// Курс: {UAH_PER_EUR} UAH/EUR\n"
        f"// ============================================================\n\n"
    )

    footer = (
        "\n\n// ============================================================\n"
        "// METRIC DEFINITIONS\n"
        "// ============================================================\n\n"
        "const METRIC_GROUPS"
    )

    pattern = r"(// =*\s*EMBEDDED DATA.*?)(const METRIC_GROUPS)"
    replacement = header + new_data_block + footer
    html_new, count = re.subn(pattern, replacement, html, flags=re.DOTALL)
    if count == 0:
        print("⚠️  Warning: не знайдено блок даних для заміни.", file=sys.stderr)
        html_new = html

    # Оновити діапазон дат у заголовку
    html_new = re.sub(
        r'\d{2}\.\d{2}\.\d{4} — \d{2}\.\d{2}\.\d{4}',
        date_range,
        html_new,
        count=1,
    )

    html_path.write_text(html_new, encoding="utf-8")
    print(f"✅ Звіт оновлено: {html_path}")
    print(f"   Період: {date_range}")
    print(f"   Міст: {len(cities_data)}")


# ===== MAIN =====

def main():
    if not DATABRICKS_TOKEN:
        print("❌ DATABRICKS_TOKEN не встановлено", file=sys.stderr)
        sys.exit(1)

    print(f"🔗 {DATABRICKS_HOST}  кластер: {CLUSTER_ID}")

    context_id = create_context()
    print(f"✅ Контекст: {context_id}")

    print("📊 [1/3] Метрики якості та ставки (fact_vendor_weekly 2025)...")
    fvw_data = get_fvw_quality_data(context_id)

    print("📊 [2/3] Замовлення 2026 (delivery_order_order)...")
    orders_2026 = get_2026_orders(context_id)

    print("📊 [3/3] Виручка 2026 (delivery_order_booking_item)...")
    revenue_2026 = get_2026_revenue(context_id)

    print("🔧 Обробка даних...")
    cities_data, week_labels = build_city_data(fvw_data, orders_2026, revenue_2026)

    print(f"📝 Оновлення HTML ({OUTPUT_HTML})...")
    update_html(cities_data, week_labels)

    print("🎉 Готово!")


if __name__ == "__main__":
    main()
