#!/usr/bin/env python3
"""
MBR — Bella Mozzarella / Pinkman Bar (Харків)
Місячний звіт, останні 8 повних місяців.
Структура: WBR Arizona (бар-чарти, KPI-картки, розкладні локації, аналіз).
Автооновлення: 1-го числа кожного місяця о 13:00 (Київ) через GitHub Actions.
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
CLUSTER_ID      = os.getenv("DATABRICKS_CLUSTER_ID", "0221-081903-9ag4bh69")
N_MONTHS        = 8
SCRIPT_DIR      = Path(__file__).parent
OUTPUT_HTML     = SCRIPT_DIR / "MBR Bella Mozzarella Pinkman Bar.html"
POLL_INTERVAL_S = 5
MAX_POLL_S      = 600

BRANDS_CONFIG = [
    {
        "slug":         "bella",
        "title":        "BELLA MOZZARELLA",
        "emoji":        "🍕",
        "color":        "#C62828",
        "provider_ids": [201375, 201367, 201462, 201471, 201456, 201465],
        "name_strip":   r"Bella\s+Mozzarella\s*",
    },
    {
        "slug":         "pinkman",
        "title":        "PINKMAN BAR",
        "emoji":        "🍸",
        "color":        "#4A148C",
        "provider_ids": [201452, 201469, 201457, 201467, 201460, 201453],
        "name_strip":   r"Pinkman\s+Bar\s*",
    },
]

UK_MONTHS_SHORT = ["","Січ","Лют","Бер","Кві","Тра","Чер","Лип","Сер","Вер","Жов","Лис","Гру"]
UK_MONTHS_FULL  = ["","Січень","Лютий","Березень","Квітень","Травень","Червень",
                    "Липень","Серпень","Вересень","Жовтень","Листопад","Грудень"]

CHART_SECTIONS = [
    ("1. Продажі",                    ["gross","net","orders","aov"]),
    ("2. Операційні показники",       ["avail","accept","refunds","prep_time","acc_time","del_time"]),
    ("3. Клієнти та поведінка",       ["active_users","freq","new_users","sessions","imp_menu","menu_prod","rating"]),
    ("4. Знижки",                     ["discounts","camp_bolt","camp_merch"]),
]

METRIC_UK = {
    "gross":      ("Gross Sales (продажі)",         "Сума вартості доставлених замовлень до знижок",   "₴"),
    "net":        ("Net Sales (чисті продажі)",      "Сума після застосування знижок клієнтам",         "₴"),
    "orders":     ("Delivered Orders",               "Кількість успішно доставлених замовлень",         "шт."),
    "aov":        ("AOV — середній чек",             "Середня сума одного доставленого замовлення",     "₴"),
    "avail":      ("Availability Rate",              "Частка часу, коли заклад був онлайн",             "%"),
    "accept":     ("Acceptance Rate",                "Частка замовлень, прийнятих вчасно",              "%"),
    "refunds":    ("Orders with Refunds",            "Частка замовлень з компенсацією клієнту",         "%"),
    "del_time":   ("Average Delivery Time",          "Середній повний час доставки від закладу до гостя","хв"),
    "acc_time":   ("Merchant Acceptance Time",       "Середній час прийняття замовлення партнером",     "хв"),
    "prep_time":  ("Preparation Time",               "Середній час приготування страв на кухні",        "хв"),
    "active_users":("Active Users",                  "Унікальні клієнти з доставленим замовленням",     "осіб"),
    "freq":       ("Order Frequency",                "Середня кількість замовлень на активного гостя",  "зам./гість"),
    "new_users":  ("New Users",                      "Гості, які вперше замовили в цьому закладі",      "осіб"),
    "sessions":   ("Sessions (покази)",              "Перегляди закладу в стрічці або пошуку",          "сесій"),
    "imp_menu":   ("Impression → Menu",              "Частка переглядів закладу, де відкрили меню",     "%"),
    "menu_prod":  ("Menu → Cart",                    "Частка переглядів меню з додаванням у кошик",     "%"),
    "rating":     ("Average Rating",                 "Середня оцінка закладу від гостей",               "з 5"),
    "discounts":  ("Total Discounts",                "Загальна сума знижок для клієнтів",               "₴"),
    "camp_bolt":  ("Campaigns Spend by Bolt",        "Витрати Bolt на знижки та промо",                 "₴"),
    "camp_merch": ("Campaigns Spend by Merchant",    "Сума, яку партнер вклав у знижки та промо",       "₴"),
}

MONTH_BAR_COLORS = [
    "#3b0f6e","#4a1485","#5e239d","#7b1fa2",
    "#9c27b0","#ab47bc","#ba68c8","#ce93d8",
]
MONTH_BAR_COLORS_BELLA = [
    "#7f0000","#b71c1c","#c62828","#d32f2f",
    "#e53935","#ef5350","#f44336","#ef9a9a",
]

EMPTY_MONTH = {
    "orders":0,"gross":0,"net":0,"aov":0,
    "avail":0,"accept":0,"refunds":0,
    "del_time":0,"acc_time":0,"prep_time":0,
    "new_users":0,"sessions":0,"imp_menu":0,"menu_prod":0,"rating":0,
    "discounts":0,"camp_bolt":0,"camp_merch":0,
    "active_users":0,"freq":0,
}


# ─── DATE HELPERS ──────────────────────────────────────────────────────────────

def last_n_full_months(n: int = N_MONTHS) -> list[tuple[int, int]]:
    d = datetime.date.today().replace(day=1)
    months = []
    for _ in range(n):
        d -= datetime.timedelta(days=1)
        months.append((d.year, d.month))
        d = d.replace(day=1)
    return list(reversed(months))


def month_label(y: int, m: int, short: bool = False) -> str:
    name = UK_MONTHS_SHORT[m] if short else UK_MONTHS_FULL[m]
    return f"{name} {y}"


def month_key(y: int, m: int) -> str:
    return f"{y:04d}-{m:02d}"


def month_range(y: int, m: int) -> tuple[str, str]:
    start = datetime.date(y, m, 1)
    end = (datetime.date(y + 1, 1, 1) if m == 12 else datetime.date(y, m + 1, 1))
    return start.isoformat(), end.isoformat()


# ─── TOKEN ─────────────────────────────────────────────────────────────────────

def _load_token() -> str:
    tok = os.getenv("DATABRICKS_TOKEN", "").strip()
    if tok:
        return tok
    for profile in ("bolt-incentives-temp", "bolt-incentives"):
        try:
            out = subprocess.check_output(
                ["databricks", "auth", "token", "-p", profile],
                text=True, stderr=subprocess.DEVNULL, timeout=30)
            t = json.loads(out).get("access_token", "").strip()
            if t:
                return t
        except Exception:
            pass
    cfg = Path.home() / ".databrickscfg"
    if cfg.exists():
        for line in cfg.read_text().splitlines():
            if line.lower().startswith("token") and "=" in line:
                t = line.split("=", 1)[1].strip()
                if t:
                    return t
    return ""


DATABRICKS_TOKEN = _load_token()
HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}


# ─── DATABRICKS ────────────────────────────────────────────────────────────────

def _post(path, payload):
    r = requests.post(f"{DATABRICKS_HOST}{path}", headers=HEADERS, json=payload, timeout=90)
    r.raise_for_status()
    return r.json()

def _get(path, params):
    r = requests.get(f"{DATABRICKS_HOST}{path}", headers=HEADERS, params=params, timeout=90)
    r.raise_for_status()
    return r.json()

def create_ctx() -> str:
    return _post("/api/1.2/contexts/create", {"language": "sql", "clusterId": CLUSTER_ID})["id"]

def run_query(ctx: str, sql: str) -> list[list]:
    cmd_id = _post("/api/1.2/commands/execute",
        {"language": "sql", "clusterId": CLUSTER_ID, "contextId": ctx, "command": sql})["id"]
    deadline = time.time() + MAX_POLL_S
    while time.time() < deadline:
        time.sleep(POLL_INTERVAL_S)
        resp = _get("/api/1.2/commands/status",
            {"clusterId": CLUSTER_ID, "contextId": ctx, "commandId": cmd_id})
        s = resp.get("status")
        if s == "Finished":
            res = resp.get("results", {})
            if res.get("resultType") == "error":
                raise RuntimeError(res.get("summary", "Query error"))
            return res.get("data", [])
        if s in ("Cancelled", "Error"):
            raise RuntimeError(f"Query {s}")
    raise TimeoutError("Query timed out")

def destroy_ctx(ctx: str):
    try:
        _post("/api/1.2/contexts/destroy", {"clusterId": CLUSTER_ID, "contextId": ctx})
    except Exception:
        pass

def _sf(v, d=0.0):
    try:
        return float(v) if v is not None else d
    except (TypeError, ValueError):
        return d

def _si(v, d=0):
    return int(round(_sf(v, d)))


# ─── DATA FETCH ────────────────────────────────────────────────────────────────

def fetch_brand_data(brand: dict) -> dict:
    months = last_n_full_months(N_MONTHS)
    y0, m0 = months[0]
    y1, m1 = months[-1]
    global_start, _ = month_range(y0, m0)
    _, global_end   = month_range(y1, m1)
    pids_sql = ", ".join(str(p) for p in brand["provider_ids"])
    month_keys   = [month_key(y, m) for y, m in months]
    month_labels = [month_label(y, m) for y, m in months]
    month_labels_s = [month_label(y, m, short=True) for y, m in months]

    print(f"  [{brand['title']}] fetching {N_MONTHS} months {global_start} → {global_end}")
    ctx = create_ctx()
    try:
        # Location names
        loc_rows = run_query(ctx, f"""
            SELECT provider_id, provider_name, city_name, zone_name
            FROM ng_delivery_spark.dim_provider_v2
            WHERE provider_id IN ({pids_sql})
            ORDER BY provider_name
        """)

        # Monthly metrics per location (aggregate weekly → monthly)
        fact_rows = run_query(ctx, f"""
            SELECT
                f.provider_id,
                d.provider_name,
                DATE_FORMAT(DATE_TRUNC('month', f.metric_timestamp_local), 'yyyy-MM-dd') AS mstart,
                SUM(f.delivered_orders_count)                                        AS orders,
                SUM(f.total_gmv_before_discounts)                                    AS gross,
                SUM(f.total_gmv_after_discounts)                                     AS net,
                SUM(f.delivered_orders_count * f.provider_active_rate_value)
                    / NULLIF(SUM(f.delivered_orders_count), 0)                       AS avail,
                SUM(f.delivered_orders_count * f.provider_acceptance_rate_value)
                    / NULLIF(SUM(f.delivered_orders_count), 0)                       AS accept,
                SUM(f.delivered_orders_count * f.customer_refunded_order_rate_value)
                    / NULLIF(SUM(f.delivered_orders_count), 0)                       AS refunds,
                SUM(f.delivered_orders_count * f.order_total_minutes_per_order_value)
                    / NULLIF(SUM(f.delivered_orders_count), 0)                       AS del_time,
                SUM(f.delivered_orders_count * f.provider_acceptance_minutes_per_order_value)
                    / NULLIF(SUM(f.delivered_orders_count), 0)                       AS acc_time,
                SUM(f.delivered_orders_count * f.provider_processing_minutes_per_order_value)
                    / NULLIF(SUM(f.delivered_orders_count), 0)                       AS prep_time,
                SUM(f.users_activated_vendor_count)                                  AS new_users,
                SUM(f.provider_impressions_sessions_count)                           AS sessions,
                SUM(f.provider_menu_viewed_sessions_count)                           AS menu_views,
                SUM(f.delivered_orders_count * f.provider_product_added_from_menu_viewed_rate_value)
                    / NULLIF(SUM(f.delivered_orders_count), 0)                       AS menu_prod,
                SUM(f.delivered_orders_count * f.provider_rating_per_order_value)
                    / NULLIF(SUM(f.delivered_orders_count), 0)                       AS rating,
                SUM(f.total_campaign_discount)                                       AS discounts,
                SUM(f.total_campaign_spend_bolt)                                     AS camp_bolt,
                SUM(f.total_campaign_spend_provider)                                 AS camp_merch,
                SUM(f.users_activated_provider_count)                                AS active_users
            FROM ng_delivery_spark.fact_provider_weekly f
            JOIN ng_delivery_spark.dim_provider_v2 d ON f.provider_id = d.provider_id
            WHERE f.provider_id IN ({pids_sql})
              AND CAST(f.metric_timestamp_local AS DATE) >= '{global_start}'
              AND CAST(f.metric_timestamp_local AS DATE) < '{global_end}'
            GROUP BY 1, 2, 3
            ORDER BY d.provider_name, 3
        """)

    finally:
        destroy_ctx(ctx)

    # Build location dict
    loc_map = {int(r[0]): {"name": str(r[1]), "city": str(r[2] or "Харків"), "zone": str(r[3] or "")}
               for r in loc_rows}

    # Parse fact rows into by_pid structure
    by_pid: dict[int, dict] = {}
    for row in fact_rows:
        pid = int(row[0])
        mk  = str(row[2])[:7]
        orders    = _si(row[3])
        gross     = _sf(row[4])
        net       = _sf(row[5])
        avail     = round(_sf(row[6]) * 100, 1)
        accept    = round(_sf(row[7]) * 100, 1)
        refunds   = round(_sf(row[8]) * 100, 1)
        del_time  = round(_sf(row[9]),  1)
        acc_time  = round(_sf(row[10]), 1)
        prep_time = round(_sf(row[11]), 1)
        new_users = _si(row[12])
        sessions  = _si(row[13])
        menu_views= _si(row[14])
        menu_prod = round(_sf(row[15]) * 100, 1)
        rating    = round(_sf(row[16]), 2)
        discounts = round(_sf(row[17]), 0)
        camp_bolt = round(_sf(row[18]), 0)
        camp_merch= round(_sf(row[19]), 0)
        active_u  = _si(row[20]) or orders
        aov       = round(gross / orders, 0) if orders else 0
        freq      = round(orders / active_u, 2) if active_u else 0
        imp_menu  = round(menu_views / sessions * 100, 1) if sessions else 0

        rec = {
            "orders": orders, "gross": round(gross, 0), "net": round(net, 0),
            "aov": aov, "avail": avail, "accept": accept, "refunds": refunds,
            "del_time": del_time, "acc_time": acc_time, "prep_time": prep_time,
            "new_users": new_users, "sessions": sessions, "imp_menu": imp_menu,
            "menu_prod": menu_prod, "rating": rating,
            "discounts": discounts, "camp_bolt": camp_bolt, "camp_merch": camp_merch,
            "active_users": active_u, "freq": freq,
        }
        if pid not in by_pid:
            by_pid[pid] = {"by_month": {}}
        by_pid[pid]["by_month"][mk] = rec

    # Build locations list with ordered months
    locations = []
    for pid in sorted(by_pid.keys(), key=lambda x: loc_map.get(x, {}).get("name", "")):
        info = loc_map.get(pid, {"name": f"ID {pid}", "city": "Харків", "zone": ""})
        months_data = []
        for mk, lbl, lbls in zip(month_keys, month_labels, month_labels_s):
            rec = dict(by_pid[pid]["by_month"].get(mk, EMPTY_MONTH))
            rec["month_key"] = mk
            rec["label"]     = lbl
            rec["label_s"]   = lbls
            months_data.append(rec)
        # Strip brand prefix from location name
        short_name = re.sub(rf"(?i)^{brand['name_strip']}", "", info["name"]).strip() or info["name"]
        locations.append({
            "provider_id": pid,
            "name": info["name"],
            "short_name": short_name,
            "city": info["city"],
            "zone": info["zone"],
            "months": months_data,
        })

    # Brand totals per month
    brand_months = []
    for i, (mk, lbl, lbls) in enumerate(zip(month_keys, month_labels, month_labels_s)):
        agg = dict(EMPTY_MONTH)
        for loc in locations:
            w = loc["months"][i]
            for k in ("orders","gross","net","new_users","sessions","discounts","camp_bolt","camp_merch","active_users"):
                agg[k] = agg.get(k, 0) + w.get(k, 0)
        # weighted averages
        weighted = [
            ("avail","orders"),("accept","orders"),("refunds","orders"),
            ("del_time","orders"),("acc_time","orders"),("prep_time","orders"),
            ("rating","orders"),("imp_menu","sessions"),("menu_prod","sessions"),
        ]
        for metric, wkey in weighted:
            total_w = sum(loc["months"][i].get(wkey, 0) for loc in locations)
            if total_w:
                agg[metric] = round(
                    sum(loc["months"][i].get(metric, 0) * loc["months"][i].get(wkey, 0)
                        for loc in locations) / total_w, 2)
        agg["aov"]  = round(agg["gross"] / agg["orders"], 0) if agg["orders"] else 0
        agg["freq"] = round(agg["orders"] / agg["active_users"], 2) if agg["active_users"] else 0
        agg["month_key"] = mk
        agg["label"]     = lbl
        agg["label_s"]   = lbls
        brand_months.append(agg)

    return {
        "locations": locations,
        "brand_months": brand_months,
        "month_keys": month_keys,
        "month_labels": month_labels,
        "month_labels_s": month_labels_s,
        "period_label": f"{month_labels[0]} — {month_labels[-1]}" if month_labels else "",
    }


# ─── ANALYSIS ──────────────────────────────────────────────────────────────────

def _pct_chg(old, new):
    if not old:
        return None
    return (new - old) / old * 100

def analyze_location(loc: dict) -> dict:
    months = loc["months"]
    if len(months) < 2:
        return {"severity": 0, "issues": [], "advice": [], "trend": "stable", "prev": {}, "last": {}}
    prev, last = months[-2], months[-1]
    first = months[0]
    issues, advice, severity = [], [], 0

    o_chg = _pct_chg(prev["orders"], last["orders"])
    o_trend = _pct_chg(first["orders"], last["orders"])

    # Orders
    if last["orders"] < 30:
        issues.append(f"Дуже мало замовлень за місяць — {last['orders']} (минулого: {prev['orders']}).")
        advice.append("Перевірте години роботи, фото та опис меню. Переконайтеся, що заклад видно в зоні доставки.")
        severity += 3
    elif o_chg is not None and o_chg <= -20:
        issues.append(f"Різке падіння замовлень: {prev['orders']} → {last['orders']} ({o_chg:.0f}%).")
        advice.append("Перевірте, чи не було довгих пауз офлайн, змін у меню або знижок.")
        severity += 2
    elif o_chg is not None and o_chg <= -10:
        issues.append(f"Замовлень менше, ніж місяць тому: {prev['orders']} → {last['orders']} ({o_chg:.0f}%).")
        severity += 1
    if o_chg is not None and o_chg >= 15:
        issues.append(f"Приємне зростання замовлень: {prev['orders']} → {last['orders']} (+{o_chg:.0f}%).")

    # Long-term decline
    if o_trend is not None and o_trend <= -25:
        issues.append(f"За {N_MONTHS} місяців замовлення впали з {first['orders']} до {last['orders']} ({o_trend:.0f}%).")
        advice.append("Розгляньте підключення Розумних акцій або спонсорованих оголошень для відновлення трафіку.")
        severity += 2

    # Availability
    if last["avail"] < 90:
        issues.append(f"Заклад доступний лише {last['avail']:.1f}% часу — гості часто не знаходять вас онлайн.")
        advice.append("Тримайте заклад увімкненим в обідні та вечірні пікові години.")
        severity += 2

    # Acceptance
    if last["accept"] < 97:
        issues.append(f"Замовлення не завжди приймаються вчасно — {last['accept']:.1f}%.")
        advice.append("Приймайте замовлення в застосунку якомога швидше, орієнтир — до 1 хвилини.")
        severity += 2

    # Refunds
    if last["refunds"] >= 5:
        issues.append(f"Часті компенсації клієнтам — {last['refunds']:.1f}% замовлень.")
        advice.append("Перевірте меню на актуальність, правильність збірки та час приготування.")
        severity += 2
    elif last["refunds"] >= 3 and last["refunds"] > prev["refunds"] + 1:
        issues.append(f"Компенсацій стало більше: {prev['refunds']:.1f}% → {last['refunds']:.1f}%.")
        severity += 1

    # Prep time
    if last["prep_time"] >= 35:
        issues.append(f"Тривалий час приготування — {last['prep_time']:.1f} хв.")
        advice.append("Оновіть час приготування у порталі або оптимізуйте кухонний процес у пікові години.")
        severity += 1

    # Rating
    if last["rating"] and last["rating"] < 4.4:
        issues.append(f"Рейтинг нижчий за комфортний — {last['rating']:.2f} з 5.")
        advice.append("Перегляньте останні негативні відгуки та усуньте часті причини.")
        severity += 2

    # Conversion
    if last["imp_menu"] < 8 and last["sessions"] > 500:
        issues.append(f"Лише {last['imp_menu']:.1f}% переглядів у стрічці переходять до меню.")
        advice.append("Оновіть головне фото та бейджі акцій — щоб гість охочіше натискав на заклад.")
        severity += 1

    trend = "stable"
    if o_chg is not None:
        trend = "up" if o_chg >= 10 else "down" if o_chg <= -10 else "stable"

    return {
        "severity": severity, "issues": issues, "advice": advice,
        "trend": trend, "prev": prev, "last": last, "o_chg": o_chg,
    }


# ─── HTML HELPERS ──────────────────────────────────────────────────────────────

def _fmt(v, unit="₴", decimals=0) -> str:
    try:
        f = float(v)
        if unit == "%":
            return f"{f:.1f}%"
        if unit == "з 5":
            return f"{f:.2f}"
        if unit in ("хв", "зам./гість"):
            return f"{f:.1f}"
        s = f"{int(round(f)):,}".replace(",", "\u202f")
        return f"{s}\u202f{unit}" if unit else s
    except (TypeError, ValueError):
        return "—"


def _pct_badge(old, new) -> str:
    ch = _pct_chg(old, new)
    if ch is None:
        return ""
    sign = "▲" if ch >= 0 else "▼"
    cls  = "positive" if ch >= 0 else "danger"
    return f'<span class="delta {cls}">{sign}\u202f{abs(ch):.1f}%</span>'


def _bar_chart(values: list, labels: list, unit: str, colors: list, max_val=None) -> str:
    nums = [float(v or 0) for v in values]
    m = max_val or (max(nums) if nums else 1) or 1
    bars = ""
    for i, (v, lbl) in enumerate(zip(nums, labels)):
        h = max(4, int(v / m * 110))
        col = colors[i % len(colors)]
        display = _fmt(v, unit)
        bars += (
            f'<div class="bar-col">'
            f'<div class="bar-val">{display}</div>'
            f'<div class="bar" style="height:{h}px;background:{col}"></div>'
            f'<div class="bar-lbl">{lbl}</div>'
            f'</div>'
        )
    return f'<div class="bars-scroll"><div class="bars">{bars}</div></div>'


def _kpi_card(label: str, value: str, delta: str = "", color: str = "var(--green)") -> str:
    return (
        f'<div class="kpi-card" style="border-top-color:{color}">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}{delta}</div>'
        f'</div>'
    )


def _severity_cls(sev: int) -> str:
    if sev >= 4: return "sev-high"
    if sev >= 2: return "sev-mid"
    return "sev-ok"


def _trend_icon(trend: str) -> str:
    return {"up": "↑", "down": "↓", "stable": "→"}.get(trend, "→")


# ─── HTML BUILDERS ─────────────────────────────────────────────────────────────

def build_brand_panel(brand: dict, data: dict, bar_colors: list) -> str:
    brand_months = data["brand_months"]
    locations    = data["locations"]
    labels_s     = data["month_labels_s"]
    month_keys   = data["month_keys"]

    if not brand_months:
        return '<p style="color:#999;padding:40px">Немає даних</p>'

    last = brand_months[-1] if brand_months else {}
    prev = brand_months[-2] if len(brand_months) > 1 else {}

    # ── KPI overview ─────────────────────────────────────────────────────────
    kpi_html = (
        _kpi_card("Delivered Orders", _fmt(last.get("orders"), "шт."),
                  _pct_badge(prev.get("orders",0), last.get("orders",0)), brand["color"]) +
        _kpi_card("Gross Sales", _fmt(last.get("gross"), "₴"),
                  _pct_badge(prev.get("gross",0), last.get("gross",0)), brand["color"]) +
        _kpi_card("Net Sales", _fmt(last.get("net"), "₴"),
                  _pct_badge(prev.get("net",0), last.get("net",0)), brand["color"]) +
        _kpi_card("AOV", _fmt(last.get("aov"), "₴"),
                  _pct_badge(prev.get("aov",0), last.get("aov",0))) +
        _kpi_card("Availability", _fmt(last.get("avail"), "%"),
                  _pct_badge(prev.get("avail",0), last.get("avail",0))) +
        _kpi_card("Acceptance", _fmt(last.get("accept"), "%"),
                  _pct_badge(prev.get("accept",0), last.get("accept",0))) +
        _kpi_card("Refund Rate", _fmt(last.get("refunds"), "%"),
                  _pct_badge(prev.get("refunds",0), last.get("refunds",0)), "#c0392b") +
        _kpi_card("Rating", _fmt(last.get("rating"), "з 5"),
                  _pct_badge(prev.get("rating",0), last.get("rating",0)), "#e67e22") +
        _kpi_card("Active Users", _fmt(last.get("active_users"), "осіб"),
                  _pct_badge(prev.get("active_users",0), last.get("active_users",0))) +
        _kpi_card("Знижки (Bolt)", _fmt(last.get("camp_bolt"), "₴"),
                  _pct_badge(prev.get("camp_bolt",0), last.get("camp_bolt",0))) +
        _kpi_card("Знижки (партнер)", _fmt(last.get("camp_merch"), "₴"),
                  _pct_badge(prev.get("camp_merch",0), last.get("camp_merch",0)))
    )

    # ── Chart sections ────────────────────────────────────────────────────────
    chart_sections_html = ""
    for sec_title, metric_keys in CHART_SECTIONS:
        charts = ""
        for mk in metric_keys:
            name, desc, unit = METRIC_UK.get(mk, (mk, "", ""))
            vals = [bm.get(mk, 0) for bm in brand_months]
            chart = _bar_chart(vals, labels_s, unit, bar_colors)
            charts += (
                f'<div class="chart-card">'
                f'<h3>{name}</h3>'
                f'<div class="metric-desc">{desc}</div>'
                f'<div class="unit">{unit}</div>'
                f'{chart}'
                f'</div>'
            )
        chart_sections_html += (
            f'<div class="section-title">{sec_title}</div>'
            f'<div class="charts-grid">{charts}</div>'
        )

    # ── Locations ─────────────────────────────────────────────────────────────
    analyses = [analyze_location(loc) for loc in locations]
    loc_items = ""
    for loc, anal in zip(locations, analyses):
        sev_cls   = _severity_cls(anal["severity"])
        trend_ico = _trend_icon(anal["trend"])
        last_loc  = anal.get("last", {})
        prev_loc  = anal.get("prev", {})
        loc_id    = re.sub(r"[^\w]", "_", f'{brand["slug"]}_{loc["provider_id"]}')

        loc_charts = ""
        for sec_title, metric_keys in CHART_SECTIONS:
            inner = ""
            for mk in metric_keys:
                name, desc, unit = METRIC_UK.get(mk, (mk, "", ""))
                vals = [m.get(mk, 0) for m in loc["months"]]
                inner += (
                    f'<div class="chart-card">'
                    f'<h3>{name}</h3>'
                    f'<div class="metric-desc">{desc}</div>'
                    f'<div class="unit">{unit}</div>'
                    f'{_bar_chart(vals, labels_s, unit, bar_colors)}'
                    f'</div>'
                )
            loc_charts += f'<div class="section-title">{sec_title}</div><div class="charts-grid">{inner}</div>'

        issues_html = "".join(f"<li>{i}</li>" for i in anal["issues"]) if anal["issues"] else "<li>Без зауважень</li>"
        advice_html = "".join(f"<li>{a}</li>" for a in anal["advice"]) if anal["advice"] else ""

        loc_items += f"""
        <div class="loc-card">
          <div class="loc-row">
            <div class="loc-row-info">
              <h2>{trend_ico} {loc['short_name']}</h2>
              <div class="loc-meta">
                {loc['zone']} &nbsp;·&nbsp; ID {loc['provider_id']} &nbsp;·&nbsp;
                {_fmt(last_loc.get('orders',0),'шт.')} зам. &nbsp;·&nbsp;
                {_fmt(last_loc.get('gross',0),'₴')}
                {_pct_badge(prev_loc.get('orders',0), last_loc.get('orders',0))}
              </div>
            </div>
            <button class="loc-open-btn" aria-expanded="false"
                    onclick="toggleLoc('{loc_id}', this)">Детальніше ▾</button>
          </div>
          <div class="loc-body" id="loc_{loc_id}" hidden>
            {loc_charts}
            <div class="loc-analysis {sev_cls}">
              <div class="loc-analysis-head">
                <h3>Аналіз локації</h3>
                <span class="sev-badge">{['OK','помірно','помірно','увага','увага','критично'][min(anal["severity"],5)]}</span>
              </div>
              <h4>Що варто знати:</h4>
              <ul>{issues_html}</ul>
              {'<h4>Рекомендації:</h4><ul class="advice">' + advice_html + '</ul>' if advice_html else ''}
            </div>
          </div>
        </div>"""

    return f"""
    <div class="period-bar">
      <span class="period-label">Місяці:</span>
      <span>{data['period_label']} &nbsp;·&nbsp; валюта UAH (₴) &nbsp;·&nbsp; Харків</span>
      <span style="margin-left:auto;font-size:11px;color:var(--gray-400)">Останній місяць: {data['month_labels'][-1]}</span>
    </div>

    <div class="section-title">Огляд бренду — останній місяць</div>
    <div class="kpi-grid">{kpi_html}</div>

    {chart_sections_html}

    <div class="section-title">Локації</div>
    <div class="loc-list">{loc_items}</div>
    """


def build_html(brands_data: list[tuple[dict, dict]]) -> str:
    today = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
    months = last_n_full_months(N_MONTHS)
    period = (f"{month_label(months[0][0], months[0][1])} — "
              f"{month_label(months[-1][0], months[-1][1])}") if months else ""

    brand_tabs = ""
    brand_panels = ""
    for i, (brand, data) in enumerate(brands_data):
        active = "active" if i == 0 else ""
        bar_colors = MONTH_BAR_COLORS_BELLA if brand["slug"] == "bella" else MONTH_BAR_COLORS
        panel_html = build_brand_panel(brand, data, bar_colors)
        brand_tabs += (
            f'<button class="brand-tab {active}" id="btab_{brand["slug"]}" '
            f'onclick="switchBrand(\'{brand["slug"]}\')" '
            f'style="--bc:{brand["color"]}">'
            f'{brand["emoji"]} {brand["title"]}</button>'
        )
        vis = "block" if i == 0 else "none"
        brand_panels += (
            f'<div id="bpanel_{brand["slug"]}" style="display:{vis}">{panel_html}</div>'
        )

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>MBR · Bella Mozzarella / Pinkman Bar · Харків</title>
  <style>
    :root{{
      --green:#34D186;--green-d:#0d8a52;--black:#0d0d0d;
      --gray-700:#4a4a4a;--gray-400:#9a9a9a;--gray-100:#f5f5f5;
      --positive:#1aad6a;--warning:#e67e22;--danger:#c0392b;
    }}
    *{{margin:0;padding:0;box-sizing:border-box}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;
      font-size:14px;line-height:1.55;color:#1a1a1a;background:var(--gray-100)}}
    .header{{background:var(--black);padding:20px 40px;display:flex;align-items:flex-start;
      justify-content:space-between;border-bottom:4px solid var(--green);flex-wrap:wrap;gap:16px}}
    .header-left{{display:flex;align-items:center;gap:14px;flex:1;min-width:240px}}
    .bolt-logo{{width:44px;height:44px;background:var(--green);border-radius:10px;
      display:flex;align-items:center;justify-content:center}}
    .header-title h1{{font-size:22px;font-weight:700;color:#fff}}
    .header-title p{{font-size:11px;color:var(--green);text-transform:uppercase;
      letter-spacing:1.2px;font-weight:600;margin-top:4px}}
    .header-meta{{text-align:right;color:var(--gray-400);font-size:12px;line-height:1.9}}
    .header-meta strong{{color:var(--green)}}

    /* Brand tabs */
    .brand-tabs{{background:#fff;padding:0 40px;display:flex;gap:4px;
      border-bottom:2px solid #eee;position:sticky;top:0;z-index:50;box-shadow:0 2px 6px rgba(0,0,0,.06)}}
    .brand-tab{{padding:14px 24px;border:none;background:transparent;cursor:pointer;
      font-size:14px;font-weight:700;color:var(--gray-400);border-bottom:3px solid transparent;
      transition:all .2s;white-space:nowrap}}
    .brand-tab:hover{{color:var(--bc,var(--green-d))}}
    .brand-tab.active{{color:var(--bc,var(--green-d));border-bottom-color:var(--bc,var(--green-d))}}

    .container{{max-width:1320px;margin:0 auto;padding:28px 40px 48px}}
    .period-bar{{background:#fff;border-radius:12px;padding:14px 20px;margin-bottom:20px;
      display:flex;align-items:center;gap:12px;flex-wrap:wrap;
      box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .period-label{{font-size:11px;font-weight:700;text-transform:uppercase;color:var(--gray-700)}}
    .section-title{{font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;
      color:var(--gray-700);padding-bottom:10px;border-bottom:2px solid var(--green);margin:28px 0 10px}}
    .kpi-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px;margin-bottom:8px}}
    .kpi-card{{background:#fff;border-radius:12px;padding:14px 16px;border-top:3px solid var(--green);
      box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .kpi-label{{font-size:10px;font-weight:700;text-transform:uppercase;color:var(--gray-400);margin-bottom:4px}}
    .kpi-value{{font-size:19px;font-weight:700}}
    .delta{{font-size:11px;font-weight:600;margin-left:4px}}
    .delta.positive{{color:var(--positive)}}
    .delta.danger{{color:var(--danger)}}
    .charts-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:16px;margin-bottom:12px}}
    .chart-card{{background:#fff;border-radius:12px;padding:16px 18px;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .chart-card h3{{font-size:12px;font-weight:700;color:var(--gray-700);margin-bottom:4px}}
    .metric-desc{{font-size:11px;color:var(--gray-700);margin-bottom:4px;line-height:1.4}}
    .unit{{font-size:10px;color:var(--gray-400);margin-bottom:8px}}
    .bars-scroll{{overflow-x:auto;padding-bottom:4px}}
    .bars{{display:flex;gap:6px;align-items:flex-end;min-height:120px;padding-top:6px}}
    .bar-col{{display:flex;flex-direction:column;align-items:center;min-width:44px;flex-shrink:0;
      height:110px;justify-content:flex-end}}
    .bar-val{{font-size:8px;font-weight:700;color:var(--gray-700);margin-bottom:3px;
      text-align:center;max-width:52px;line-height:1.15}}
    .bar{{width:36px;border-radius:5px 5px 0 0;min-height:4px}}
    .bar-lbl{{font-size:8px;color:var(--gray-400);margin-top:3px;text-align:center;line-height:1.2}}
    .loc-card{{background:#fff;border-radius:12px;margin:0 0 10px;
      box-shadow:0 1px 4px rgba(0,0,0,.06);border:1px solid #eee;overflow:hidden}}
    .loc-row{{display:flex;align-items:center;justify-content:space-between;gap:16px;
      padding:14px 18px;flex-wrap:wrap}}
    .loc-row-info{{flex:1;min-width:180px}}
    .loc-row-info h2{{font-size:15px;color:var(--black);font-weight:700}}
    .loc-open-btn{{flex-shrink:0;padding:9px 16px;border:none;border-radius:8px;
      background:var(--green-d);color:#fff;font-size:13px;font-weight:600;cursor:pointer}}
    .loc-open-btn:hover{{background:var(--green);color:var(--black)}}
    .loc-body{{padding:0 18px 20px;border-top:1px solid #f0f0f0}}
    .loc-meta{{font-size:12px;color:var(--gray-400);margin-top:2px}}
    .loc-list{{display:flex;flex-direction:column;gap:0}}
    .loc-analysis{{background:var(--gray-100);border-radius:10px;padding:16px 18px;
      margin-top:20px;border-left:4px solid var(--gray-400)}}
    .loc-analysis.sev-high{{border-left-color:var(--danger);background:#fff8f6}}
    .loc-analysis.sev-mid{{border-left-color:var(--warning);background:#fffaf3}}
    .loc-analysis.sev-ok{{border-left-color:var(--positive)}}
    .loc-analysis-head{{display:flex;justify-content:space-between;align-items:center;
      gap:8px;margin-bottom:8px}}
    .loc-analysis-head h3{{font-size:14px;color:var(--gray-700)}}
    .loc-analysis h4{{font-size:12px;margin:10px 0 4px;color:var(--gray-700)}}
    .loc-analysis ul{{margin-left:18px;font-size:13px}}
    .loc-analysis ul.advice{{color:var(--green-d)}}
    .sev-badge{{font-size:10px;font-weight:700;text-transform:uppercase;color:var(--warning)}}
    .footer{{background:var(--black);color:var(--gray-400);font-size:11px;padding:22px 40px;text-align:center}}
    .footer span{{color:var(--green)}}
    @media(max-width:700px){{
      .container{{padding:16px}}.charts-grid{{grid-template-columns:1fr}}
      .header{{padding:16px}}.brand-tabs{{padding:0 16px}}
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
      <h1>MBR · Bella Mozzarella / Pinkman Bar</h1>
      <p>Bolt Food &nbsp;·&nbsp; Місячний звіт &nbsp;·&nbsp; Харків</p>
    </div>
  </div>
  <div class="header-meta">
    <div>Період: <strong>{period}</strong></div>
    <div>Місяців: <strong>{N_MONTHS}</strong> &nbsp;·&nbsp; Оновлено: <strong>{today}</strong></div>
  </div>
</header>

<div class="brand-tabs">
  {brand_tabs}
</div>

<div class="container">
  {brand_panels}
</div>

<div class="footer">
  Автоматично оновлюється 1-го числа кожного місяця &nbsp;·&nbsp;
  <span>Bolt Food MBR</span> &nbsp;·&nbsp; Харків
</div>

<script>
function switchBrand(slug) {{
  document.querySelectorAll('.brand-tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('[id^="bpanel_"]').forEach(p => p.style.display = 'none');
  document.getElementById('btab_' + slug).classList.add('active');
  document.getElementById('bpanel_' + slug).style.display = 'block';
}}

function toggleLoc(id, btn) {{
  const body = document.getElementById('loc_' + id);
  const open = btn.getAttribute('aria-expanded') === 'true';
  body.hidden = open;
  btn.setAttribute('aria-expanded', !open);
  btn.textContent = open ? 'Детальніше ▾' : 'Згорнути ▴';
}}
</script>
</body>
</html>"""


# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    today = datetime.date.today().isoformat()
    print(f"=== MBR Bella Mozzarella / Pinkman Bar [{today}] ===\n")
    if not DATABRICKS_TOKEN:
        print("ERROR: DATABRICKS_TOKEN not set"); sys.exit(1)

    brands_data = []
    for brand in BRANDS_CONFIG:
        print(f"📊 {brand['title']}...")
        try:
            data = fetch_brand_data(brand)
            print(f"  → {len(data['brand_months'])} months, {len(data['locations'])} locations")
            brands_data.append((brand, data))
        except Exception as exc:
            print(f"  ERROR: {exc}")
            brands_data.append((brand, {"brand_months": [], "locations": [],
                                        "month_keys": [], "month_labels": [],
                                        "month_labels_s": [], "period_label": ""}))

    html = build_html(brands_data)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print(f"\n✅ Saved → {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
