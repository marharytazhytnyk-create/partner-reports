#!/usr/bin/env python3
"""
Генератор звіту BARANCHIK з Databricks
Запускається кожного понеділка о 13:30 через GitHub Actions
Витягує дані з ng_delivery_spark.fact_vendor_weekly за останні 8 тижнів
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
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://bolt-incentives.cloud.databricks.com")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
CLUSTER_ID = os.environ.get("DATABRICKS_CLUSTER_ID", "0221-081903-9ag4bh69")

VENDOR_IDS = {
    114922: {"name": "BARANCHIK Lviv", "city_uk": "Львів", "emoji": "🦁", "color": "#e94560"},
    114927: {"name": "BARANCHIK Poltava", "city_uk": "Полтава", "emoji": "🌻", "color": "#f5a623"},
    114934: {"name": "BARANCHIK Cherkasy", "city_uk": "Черкаси", "emoji": "🌊", "color": "#8e44ad"},
    114936: {"name": "BARANCHIK Kharkiv", "city_uk": "Харків", "emoji": "🏙️", "color": "#2980b9"},
    114939: {"name": "BARANCHIK Kremenchuk", "city_uk": "Кременчук", "emoji": "🌿", "color": "#27ae60"},
}

UAH_PER_EUR = 48.28  # Exchange rate UAH/EUR

SCRIPT_DIR = Path(__file__).parent
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


def wait_for_command(context_id, command_id, timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        result = api_request(
            "GET",
            f"/api/1.2/commands/status?clusterId={CLUSTER_ID}&contextId={context_id}&commandId={command_id}",
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
    cols = [s["name"] for s in schema]
    rows = results.get("data", [])
    return [dict(zip(cols, row)) for row in rows]


# ===== ДАНІ =====

def get_last_8_weeks_data(context_id):
    """Отримує останні 8 повних тижнів даних для BARANCHIK"""
    vendor_ids_str = ", ".join(str(v) for v in VENDOR_IDS.keys())

    sql = f"""
    SELECT
        fvw.vendor_id,
        CAST(fvw.metric_timestamp_local AS DATE) as week_date,
        COALESCE(fvw.delivered_orders_count, 0)      AS delivered_orders,
        COALESCE(fvw.placed_orders_count, 0)          AS placed_orders,
        COALESCE(fvw.total_gmv_before_discounts, 0)   AS total_gmv_uah,
        fvw.gmv_before_discounts_per_order_eur_value  AS avg_order_eur,
        COALESCE(fvw.provider_acceptance_rate_value * 100, 0)      AS acceptance_rate_pct,
        COALESCE(fvw.bad_provider_rating_rate_value * 100, 0)      AS bad_rating_pct,
        COALESCE(fvw.good_provider_rating_rate_value * 100, 0)     AS good_rating_pct,
        fvw.provider_rating_per_order_value                        AS avg_rating,
        COALESCE(fvw.late_delivery_order_rate_value * 100, 0)      AS late_delivery_pct,
        fvw.provider_preparation_minutes_per_order_value           AS prep_time_min,
        COALESCE(fvw.cs_ticket_missing_items_rate_value * 100, 0)  AS missing_items_pct,
        COALESCE(fvw.failed_order_rate_value * 100, 0)             AS failed_order_pct,
        COALESCE(fvw.total_invoiced_provider_commission_eur, 0)    AS commission_eur,
        COALESCE(fvw.total_delivery_price_before_discounts, 0)     AS delivery_fee_uah,
        COALESCE(fvw.total_provider_price_before_discounts, 0)     AS provider_price_uah
    FROM ng_delivery_spark.fact_vendor_weekly fvw
    WHERE fvw.vendor_id IN ({vendor_ids_str})
    ORDER BY fvw.vendor_id, fvw.metric_timestamp_local
    """
    rows = query(context_id, sql)

    # Group by vendor
    by_vendor = {}
    for row in rows:
        vid = row["vendor_id"]
        if vid not in by_vendor:
            by_vendor[vid] = []
        by_vendor[vid].append(row)

    # Select last 8 complete weeks per vendor
    result = {}
    for vid, weeks in by_vendor.items():
        # Filter out weeks with 0 placed orders (likely partial)
        complete = [w for w in weeks if w["placed_orders"] > 0]
        # Take last 8
        last8 = complete[-8:] if len(complete) >= 8 else complete

        result[vid] = last8

    return result


def build_city_data(raw_data):
    """Будує структуру даних по містах"""
    cities_data = {}
    week_labels = None

    for vid, info in VENDOR_IDS.items():
        weeks = raw_data.get(vid, [])
        if not weeks:
            continue

        if week_labels is None:
            week_labels = [
                datetime.datetime.strptime(str(w["week_date"]), "%Y-%m-%d").strftime("%d.%m")
                for w in weeks
            ]

        def safe_list(key):
            vals = []
            for w in weeks:
                v = w.get(key)
                if v is None:
                    vals.append(None)
                else:
                    vals.append(round(float(v), 2))
            return vals

        def round_list(key, decimals=0):
            vals = []
            for w in weeks:
                v = w.get(key)
                if v is None:
                    vals.append(None)
                elif decimals == 0:
                    vals.append(int(round(float(v))))
                else:
                    vals.append(round(float(v), decimals))
            return vals

        avg_order_eur = safe_list("avg_order_eur")
        avg_order_uah = [round(v * UAH_PER_EUR) if v is not None else None for v in avg_order_eur]
        commission_eur = safe_list("commission_eur")
        commission_uah = [round(v * UAH_PER_EUR) if v is not None else None for v in commission_eur]

        cities_data[info["city_uk"]] = {
            "color": info["color"],
            "emoji": info["emoji"],
            "vendor_id": vid,
            "metrics": {
                "delivered_orders": round_list("delivered_orders"),
                "placed_orders":    round_list("placed_orders"),
                "gmv_uah":          round_list("total_gmv_uah"),
                "avg_order_uah":    avg_order_uah,
                "acceptance_rate":  safe_list("acceptance_rate_pct"),
                "bad_rating_pct":   safe_list("bad_rating_pct"),
                "good_rating_pct":  safe_list("good_rating_pct"),
                "avg_rating":       safe_list("avg_rating"),
                "late_delivery_pct":safe_list("late_delivery_pct"),
                "prep_time_min":    safe_list("prep_time_min"),
                "missing_items_pct":safe_list("missing_items_pct"),
                "failed_order_pct": safe_list("failed_order_pct"),
                "commission_uah":   commission_uah,
                "delivery_fee_uah": round_list("delivery_fee_uah"),
                "provider_price_uah":round_list("provider_price_uah"),
            }
        }

    return cities_data, week_labels


# ===== HTML ГЕНЕРАЦІЯ =====

def update_html(cities_data, week_labels):
    """Оновлює index.html з новими даними"""
    html_path = OUTPUT_HTML
    html = html_path.read_text(encoding="utf-8")

    # Build date range from week labels
    if week_labels and len(week_labels) >= 2:
        date_range = f"{week_labels[0]}.{datetime.date.today().year} — {week_labels[-1]}.{datetime.date.today().year}"
    else:
        date_range = "Актуальні дані"

    # Replace data block in HTML
    weeks_full = []
    if week_labels:
        year = datetime.date.today().year
        for lbl in week_labels:
            d, m = lbl.split(".")
            weeks_full.append(f"{d}.{m}.{year}")

    new_data_block = f"""const WEEKS = {json.dumps(week_labels or [], ensure_ascii=False)};
const WEEKS_FULL = {json.dumps(weeks_full, ensure_ascii=False)};
const RATE = {UAH_PER_EUR};

const CITIES_DATA = {json.dumps(cities_data, ensure_ascii=False, indent=2)};"""

    # Replace the data block between markers
    pattern = r"(// ===== EMBEDDED DATA.*?)(const METRIC_GROUPS)"
    replacement = f"// ===== EMBEDDED DATA (auto-generated by generate_baranchik.py)\n// Дата оновлення: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\n// Джерело: ng_delivery_spark.fact_vendor_weekly\n// Курс: {UAH_PER_EUR} UAH/EUR\n// ============================================================\n\n{new_data_block}\n\n// ============================================================\n// METRIC DEFINITIONS\n// ============================================================\n\nconst METRIC_GROUPS"

    html_new, count = re.subn(pattern, replacement, html, flags=re.DOTALL)
    if count == 0:
        print("Warning: could not find data block to replace. Updating period header only.")
        html_new = html

    # Update period in header
    html_new = re.sub(
        r'(\d{2}\.\d{2}\.\d{4} — \d{2}\.\d{2}\.\d{4})',
        date_range.replace(f".{datetime.date.today().year}", f".{datetime.date.today().year}"),
        html_new,
        count=1
    )

    html_path.write_text(html_new, encoding="utf-8")
    print(f"✅ Звіт оновлено: {html_path}")
    print(f"   Період: {date_range}")
    print(f"   Міст: {len(cities_data)}")


# ===== MAIN =====

def main():
    if not DATABRICKS_TOKEN:
        print("❌ Помилка: DATABRICKS_TOKEN не встановлено", file=sys.stderr)
        sys.exit(1)

    print(f"🔗 Підключення до {DATABRICKS_HOST}")
    print(f"🏃 Кластер: {CLUSTER_ID}")

    context_id = create_context()
    print(f"✅ Контекст створено: {context_id}")

    print("📊 Завантаження даних BARANCHIK...")
    raw_data = get_last_8_weeks_data(context_id)

    print("🔧 Обробка даних...")
    cities_data, week_labels = build_city_data(raw_data)

    print(f"📝 Оновлення HTML звіту ({OUTPUT_HTML})...")
    update_html(cities_data, week_labels)

    print("🎉 Готово!")


if __name__ == "__main__":
    main()
