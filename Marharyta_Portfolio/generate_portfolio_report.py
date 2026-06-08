"""
Marharyta Portfolio Report Generator
Generates an HTML report for Account Manager Marharyta Zhytnyk's portfolio
with Ukrainian cities navigation, TOP players and problem venues.
"""

import os
import sys
import json
import time
import requests
import pandas as pd
from datetime import date, datetime, timedelta

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://bolt-incentives.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "0221-081903-9ag4bh69")

ACCOUNT_MANAGER = "Marharyta Zhytnyk"
COUNTRY_CODE = "ua"

REPORT_DATE = date.today().isoformat()
OUTPUT_FILE = "Marharyta_Portfolio.html"


# ─── DATE HELPERS ──────────────────────────────────────────────────────────────

def get_last_n_full_weeks(n: int = 4):
    """Return start and end dates for the last N full calendar weeks (Mon–Sun)."""
    today = date.today()
    days_since_sunday = today.weekday() + 1  # Mon=0 → days since last Sun
    last_sunday = today - timedelta(days=days_since_sunday)
    start_monday = last_sunday - timedelta(days=(n * 7) - 1)
    return start_monday.isoformat(), last_sunday.isoformat()


def get_last_4_full_weeks():
    return get_last_n_full_weeks(4)


# ─── DATABRICKS CLUSTER API ───────────────────────────────────────────────────

def _headers() -> dict:
    return {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}


def _create_context() -> str:
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/1.2/contexts/create",
        headers=_headers(),
        json={"language": "sql", "clusterId": CLUSTER_ID},
    )
    resp.raise_for_status()
    return resp.json()["id"]


def _exec_sql(ctx: str, sql: str, timeout: int = 300) -> dict:
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/1.2/commands/execute",
        headers=_headers(),
        json={"language": "sql", "clusterId": CLUSTER_ID, "contextId": ctx, "command": sql},
    )
    resp.raise_for_status()
    cmd_id = resp.json()["id"]

    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(
            f"{DATABRICKS_HOST}/api/1.2/commands/status",
            headers=_headers(),
            params={"clusterId": CLUSTER_ID, "contextId": ctx, "commandId": cmd_id},
        )
        r.raise_for_status()
        data = r.json()
        status = data.get("status")
        if status == "Finished":
            return data
        if status in ("Error", "Cancelled"):
            raise RuntimeError(f"Command failed: {data.get('results', {}).get('summary', data)}")
        time.sleep(5)
    raise TimeoutError(f"Command timed out after {timeout}s")


def _to_df(data: dict) -> pd.DataFrame:
    res = data.get("results", {})
    if res.get("resultType") == "error":
        raise RuntimeError(res.get("summary", "Unknown error"))
    cols = [c["name"] for c in res.get("schema", [])]
    rows = res.get("data", [])
    return pd.DataFrame(rows, columns=cols)


# ─── DATA FETCH ────────────────────────────────────────────────────────────────

def fetch_data() -> pd.DataFrame:
    """Fetch provider metrics for Marharyta Zhytnyk's portfolio for last 4 full weeks."""
    start_date, end_date = get_last_4_full_weeks()
    print(f"Fetching data for period: {start_date} to {end_date}")

    ctx = _create_context()

    sql = f"""
    SELECT
        p.provider_id,
        p.provider_name,
        p.brand_name,
        p.group_name,
        p.city_name,
        p.zone_name,
        p.business_segment_v2,
        p.business_subsegment_v2,
        p.delivery_vertical,
        p.provider_status,
        p.account_manager_name,
        p.is_top_brand,
        p.is_store_1p,
        p.is_store_3p_ent,
        p.is_store_3p_mm_smb,
        p.provider_rating,
        DATE_TRUNC('week', f.metric_timestamp_local) AS week_start,
        SUM(f.delivered_orders_count) AS delivered_orders,
        SUM(f.failed_orders_count) AS failed_orders,
        SUM(f.placed_orders_count) AS placed_orders,
        SUM(f.total_gmv_before_discounts_eur) AS gmv_eur,
        SUM(f.total_contribution_profit_eur) AS contribution_profit_eur,
        AVG(f.bad_order_rate_value) AS bad_order_rate,
        AVG(f.failed_order_rate_value) AS failed_order_rate,
        AVG(f.provider_acceptance_rate_value) AS acceptance_rate,
        AVG(f.late_delivery_order_rate_value) AS late_delivery_rate,
        AVG(f.provider_active_rate_value) AS active_rate
    FROM main.ng_delivery.dim_provider_v2 p
    INNER JOIN main.ng_delivery.fact_provider_weekly f
        ON p.provider_id = f.provider_id
    WHERE
        p.account_manager_name = '{ACCOUNT_MANAGER}'
        AND p.country_code = '{COUNTRY_CODE}'
        AND CAST(f.metric_timestamp_local AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        p.provider_id, p.provider_name, p.brand_name, p.group_name,
        p.city_name, p.zone_name, p.business_segment_v2, p.business_subsegment_v2,
        p.delivery_vertical, p.provider_status, p.account_manager_name,
        p.is_top_brand, p.is_store_1p, p.is_store_3p_ent, p.is_store_3p_mm_smb,
        p.provider_rating,
        DATE_TRUNC('week', f.metric_timestamp_local)
    ORDER BY p.city_name, p.brand_name, week_start
    """

    print("Running main data query...")
    result = _exec_sql(ctx, sql, timeout=300)
    df = _to_df(result)
    print(f"Fetched {len(df):,} rows")
    return df, start_date, end_date


def fetch_provider_summary() -> pd.DataFrame:
    """Fetch brand-level aggregated summary (4-week total), active providers only."""
    start_date, end_date = get_last_4_full_weeks()
    ctx = _create_context()

    # Aggregate at brand + city level so each row = one brand in one city.
    # Rates are weighted by delivered_orders_count so multi-location brands
    # get a meaningful average instead of a simple unweighted AVG.
    sql = f"""
    SELECT
        p.brand_name,
        p.city_name,
        MIN(p.group_name)           AS group_name,
        MIN(p.business_segment_v2)  AS business_segment_v2,
        MIN(p.business_subsegment_v2) AS business_subsegment_v2,
        MIN(p.delivery_vertical)    AS delivery_vertical,
        MAX(CAST(p.is_top_brand AS INT)) AS is_top_brand,
        COUNT(DISTINCT p.provider_id) AS locations_count,
        MIN(p.owner_email)  AS owner_email,
        MIN(p.provider_email) AS provider_email,
        SUM(f.delivered_orders_count)            AS delivered_orders,
        SUM(f.failed_orders_count)               AS failed_orders,
        SUM(f.placed_orders_count)               AS placed_orders,
        SUM(f.total_gmv_before_discounts_eur)    AS gmv_eur,
        SUM(f.total_contribution_profit_eur)     AS contribution_profit_eur,
        CASE
            WHEN SUM(f.total_gmv_before_discounts_eur) > 0
            THEN SUM(f.total_contribution_profit_eur)
                 / SUM(f.total_gmv_before_discounts_eur) * 100
            ELSE NULL
        END AS cp_l2_margin_pct,
        -- Weighted rates: weight = delivered_orders_count per row
        SUM(f.bad_order_rate_value      * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) AS bad_order_rate,
        SUM(f.failed_order_rate_value   * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) AS failed_order_rate,
        SUM(f.provider_acceptance_rate_value * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) AS acceptance_rate,
        SUM(f.late_delivery_order_rate_value * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) AS late_delivery_rate,
        SUM(f.provider_active_rate_value     * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) AS active_rate
    FROM ng_delivery_spark.dim_provider_v2 p
    INNER JOIN ng_delivery_spark.fact_provider_weekly f
        ON p.provider_id = f.provider_id
    WHERE
        p.account_manager_name = '{ACCOUNT_MANAGER}'
        AND p.country_code      = '{COUNTRY_CODE}'
        AND p.provider_status   = 'active'
        AND CAST(f.metric_timestamp_local AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        p.brand_name,
        p.city_name
    ORDER BY p.city_name, gmv_eur DESC
    """

    print("Running brand-level summary query...")
    result = _exec_sql(ctx, sql, timeout=300)
    df = _to_df(result)
    print(f"Fetched {len(df):,} brand-city rows")
    return df


def fetch_weekly_trends(n_weeks: int = 12) -> pd.DataFrame:
    """Fetch week-by-week brand metrics for the last N full weeks (for trend charts)."""
    start_date, end_date = get_last_n_full_weeks(n_weeks)
    ctx = _create_context()

    sql = f"""
    SELECT
        p.brand_name,
        p.city_name,
        DATE_FORMAT(DATE_TRUNC('week', f.metric_timestamp_local), 'yyyy-MM-dd') AS week_start,
        SUM(f.delivered_orders_count)         AS delivered_orders,
        SUM(f.total_gmv_before_discounts_eur) AS gmv_eur,
        SUM(f.total_contribution_profit_eur)  AS contribution_profit_eur,
        CASE
            WHEN SUM(f.total_gmv_before_discounts_eur) > 0
            THEN ROUND(SUM(f.total_contribution_profit_eur)
                 / SUM(f.total_gmv_before_discounts_eur) * 100, 2)
            ELSE NULL
        END AS cp_l2_margin_pct,
        ROUND(SUM(f.failed_order_rate_value   * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) * 100, 2) AS failed_order_rate_pct,
        ROUND(SUM(f.bad_order_rate_value      * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) * 100, 2) AS bad_order_rate_pct,
        ROUND(SUM(f.provider_acceptance_rate_value * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) * 100, 2) AS acceptance_rate_pct,
        ROUND(SUM(f.late_delivery_order_rate_value * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) * 100, 2) AS late_delivery_rate_pct
    FROM ng_delivery_spark.dim_provider_v2 p
    INNER JOIN ng_delivery_spark.fact_provider_weekly f
        ON p.provider_id = f.provider_id
    WHERE
        p.account_manager_name = '{ACCOUNT_MANAGER}'
        AND p.country_code      = '{COUNTRY_CODE}'
        AND p.provider_status   = 'active'
        AND CAST(f.metric_timestamp_local AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        p.brand_name,
        p.city_name,
        DATE_TRUNC('week', f.metric_timestamp_local)
    ORDER BY p.brand_name, p.city_name, week_start
    """

    print(f"Running weekly trends query ({n_weeks} weeks)...")
    result = _exec_sql(ctx, sql, timeout=300)
    df = _to_df(result)
    print(f"Fetched {len(df):,} weekly trend rows")
    return df


# ─── HELPER FUNCTIONS ──────────────────────────────────────────────────────────

def safe_float(val, default=0.0):
    try:
        return float(val) if val is not None and val != "" else default
    except (TypeError, ValueError):
        return default


def fmt_num(val, decimals=0):
    try:
        v = float(val)
        if decimals == 0:
            return f"{int(round(v)):,}".replace(",", "\u00a0")
        return f"{v:,.{decimals}f}".replace(",", "\u00a0")
    except (TypeError, ValueError):
        return "—"


def fmt_eur(val, decimals=0):
    try:
        v = float(val)
        return f"€{v:,.{decimals}f}".replace(",", "\u00a0")
    except (TypeError, ValueError):
        return "—"


def fmt_pct(val):
    try:
        v = float(val)
        return f"{v:.1f}%"
    except (TypeError, ValueError):
        return "—"


# ─── RED FLAG LOGIC ────────────────────────────────────────────────────────────

def cp_l2_diagnosis(row) -> tuple:
    """
    Returns (reasons_list, fixes_list) for a brand with negative CP L2 margin.
    Empty lists if margin is OK.
    """
    cp = safe_float(row.get("cp_l2_margin_pct"))
    if cp >= 0:
        return [], []

    reasons, fixes = [], []

    bad_order_pct   = safe_float(row.get("bad_order_rate")) * 100
    failed_abs      = safe_float(row.get("failed_orders"))
    orders          = safe_float(row.get("delivered_orders"))
    acceptance      = safe_float(row.get("acceptance_rate")) * 100
    late_rate       = safe_float(row.get("late_delivery_rate")) * 100

    if bad_order_pct > 10:
        reasons.append(f"Висока частка поганих замовлень ({bad_order_pct:.1f}%) — витрати на компенсації")
        fixes.append("Перевірити якість пакування та правильність замовлень")
    if failed_abs > 2:
        reasons.append(f"Багато невдалих замовлень ({int(failed_abs)}) — прямі збитки і повернення коштів")
        fixes.append("Стабілізувати прийняття замовлень, перевірити меню та доступність")
    if acceptance < 85 and acceptance > 0:
        reasons.append(f"Низький Acceptance Rate ({acceptance:.1f}%) — часті відмови підвищують витрати")
        fixes.append("Обговорити з рестораном причини відмов і налаштувати робочий графік")
    if late_rate > 20:
        reasons.append(f"Часті запізнення доставки ({late_rate:.1f}%) — знижки клієнтам за затримки")
        fixes.append("Переглянути час приготування у меню (cooking time)")
    if orders < 80:
        reasons.append("Малий обсяг замовлень — фіксовані витрати не покриваються обсягом")
        fixes.append("Підключити акції або ULC-кампанію для зростання замовлень")

    if not reasons:
        reasons.append("Сукупні витрати (доставка + компенсації + знижки) перевищують комісійний дохід")
        fixes.append("Переглянути умови комісії або зменшити частку знижок у GMV")

    return reasons, fixes


def get_red_flags(row, gmv_wow_pct: float = None) -> dict:
    """
    Returns a dict with 4 red flag categories for a brand row.
    Each key maps to a list of issues (empty = no flag).
    """
    flags = {
        "availability": [],
        "failed_orders": [],
        "cp_negative": [],
        "gmv_drop": [],
    }

    # 1. Availability < 95%
    avail = safe_float(row.get("active_rate"))
    if 0 < avail < 0.95:
        flags["availability"].append(
            f"Availability {fmt_pct(avail * 100)} — нижче порогу 95%"
        )

    # 2. Failed orders > 2 (absolute count)
    failed_abs = safe_float(row.get("failed_orders"))
    if failed_abs > 2:
        flags["failed_orders"].append(
            f"{int(failed_abs)} зафейлених замовлень за 4 тижні"
        )

    # 3. Negative CP L2 Margin
    cp = safe_float(row.get("cp_l2_margin_pct"))
    if cp < 0:
        reasons, fixes = cp_l2_diagnosis(row)
        flags["cp_negative"] = {"margin": cp, "reasons": reasons, "fixes": fixes}

    # 4. GMV drop > 1% WoW
    if gmv_wow_pct is not None and gmv_wow_pct < -1.0:
        flags["gmv_drop"].append(
            f"GMV впав на {abs(gmv_wow_pct):.1f}% у порівнянні з попереднім тижнем"
        )

    return flags


def has_any_flag(flags: dict) -> bool:
    for v in flags.values():
        if v:
            return True
    return False


def compute_gmv_wow(df_trends: pd.DataFrame) -> dict:
    """
    Returns dict {(brand, city): wow_pct} for each brand.
    wow_pct = (last_week_gmv / prev_week_gmv - 1) * 100
    """
    result = {}
    if df_trends.empty or "gmv_eur" not in df_trends.columns:
        return result

    df_trends = df_trends.copy()
    df_trends["gmv_eur"] = pd.to_numeric(df_trends["gmv_eur"], errors="coerce")

    for (brand, city), grp in df_trends.groupby(["brand_name", "city_name"]):
        grp = grp.sort_values("week_start")
        if len(grp) < 2:
            continue
        last_gmv = grp["gmv_eur"].iloc[-1]
        prev_gmv = grp["gmv_eur"].iloc[-2]
        if prev_gmv and prev_gmv > 0 and last_gmv is not None:
            import math
            if not (math.isnan(float(last_gmv)) or math.isnan(float(prev_gmv))):
                result[(brand, city)] = (float(last_gmv) / float(prev_gmv) - 1) * 100
    return result


def is_problematic(row, gmv_wow_pct: float = None) -> bool:
    """Returns True if brand has any red flag."""
    return has_any_flag(get_red_flags(row, gmv_wow_pct))


# ─── HTML GENERATION ──────────────────────────────────────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Портфоліо Marharyta Zhytnyk — Bolt Food</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --bolt-green: #1DC462;
    --bolt-dark: #1A1A1A;
    --bolt-gray: #F5F5F5;
    --bolt-light-green: #E8F9EE;
    --bolt-mid-green: #13A350;
    --danger: #E53935;
    --warning: #FB8C00;
    --info: #1976D2;
    --text: #222;
    --muted: #666;
    --border: #E0E0E0;
    --shadow: 0 2px 8px rgba(0,0,0,0.08);
  }}

  * {{ box-sizing: border-box; margin: 0; padding: 0; }}

  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
    background: #F7F9FC;
    color: var(--text);
    font-size: 14px;
    line-height: 1.5;
  }}

  /* ── HEADER ── */
  .header {{
    background: var(--bolt-dark);
    color: #fff;
    padding: 20px 32px;
    display: flex;
    align-items: center;
    gap: 20px;
    position: sticky;
    top: 0;
    z-index: 100;
    box-shadow: 0 2px 12px rgba(0,0,0,0.25);
  }}
  .header-logo {{
    font-size: 22px;
    font-weight: 800;
    color: var(--bolt-green);
    letter-spacing: -0.5px;
  }}
  .header-title {{
    font-size: 15px;
    font-weight: 600;
    color: #fff;
  }}
  .header-sub {{
    font-size: 12px;
    color: #aaa;
    margin-top: 2px;
  }}
  .header-meta {{
    margin-left: auto;
    text-align: right;
    font-size: 12px;
    color: #aaa;
  }}
  .header-meta strong {{ color: var(--bolt-green); font-size: 13px; }}

  /* ── CITY TABS ── */
  .city-nav {{
    background: #fff;
    border-bottom: 2px solid var(--bolt-green);
    padding: 0 20px;
    display: flex;
    gap: 4px;
    overflow-x: auto;
    position: sticky;
    top: 72px;
    z-index: 90;
    box-shadow: var(--shadow);
  }}
  .city-tab {{
    padding: 12px 18px;
    cursor: pointer;
    font-size: 13px;
    font-weight: 600;
    color: var(--muted);
    border-bottom: 3px solid transparent;
    white-space: nowrap;
    transition: color 0.2s, border-color 0.2s;
    user-select: none;
  }}
  .city-tab:hover {{ color: var(--bolt-green); }}
  .city-tab.active {{
    color: var(--bolt-green);
    border-bottom-color: var(--bolt-green);
  }}

  /* ── CONTENT ── */
  .content {{ padding: 24px 28px; }}
  .city-section {{ display: none; }}
  .city-section.active {{ display: block; }}

  /* ── SECTION TITLE ── */
  .section-title {{
    font-size: 20px;
    font-weight: 700;
    color: var(--bolt-dark);
    margin-bottom: 4px;
    display: flex;
    align-items: center;
    gap: 10px;
  }}
  .section-title .badge {{
    font-size: 11px;
    background: var(--bolt-green);
    color: #fff;
    border-radius: 12px;
    padding: 2px 10px;
    font-weight: 600;
  }}
  .section-sub {{
    font-size: 12px;
    color: var(--muted);
    margin-bottom: 20px;
  }}

  /* ── TOP CARDS ── */
  .top-grid {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 16px;
    margin-bottom: 28px;
  }}
  @media (max-width: 900px) {{ .top-grid {{ grid-template-columns: 1fr; }} }}

  .top-card {{
    background: #fff;
    border-radius: 12px;
    box-shadow: var(--shadow);
    padding: 16px 18px;
    border-top: 4px solid var(--bolt-green);
  }}
  .top-card h3 {{
    font-size: 12px;
    text-transform: uppercase;
    color: var(--muted);
    font-weight: 700;
    letter-spacing: 0.5px;
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    gap: 6px;
  }}
  .top-card h3 .icon {{ font-size: 16px; }}
  .top-item {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 6px 0;
    border-bottom: 1px solid var(--border);
    gap: 8px;
  }}
  .top-item:last-child {{ border-bottom: none; }}
  .top-item-rank {{
    width: 22px;
    height: 22px;
    border-radius: 50%;
    background: var(--bolt-light-green);
    color: var(--bolt-mid-green);
    font-size: 11px;
    font-weight: 700;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
  }}
  .top-item-rank.gold {{ background: #FFF8E1; color: #F9A825; }}
  .top-item-rank.silver {{ background: #F5F5F5; color: #757575; }}
  .top-item-rank.bronze {{ background: #FBE9E7; color: #BF360C; }}
  .top-item-name {{
    flex: 1;
    font-size: 13px;
    font-weight: 500;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }}
  .top-item-val {{
    font-size: 13px;
    font-weight: 700;
    color: var(--bolt-mid-green);
    white-space: nowrap;
  }}

  /* ── RED FLAGS PANEL ── */
  .red-flags-wrap {{
    margin-bottom: 28px;
  }}
  .red-flags-title {{
    font-size: 15px;
    font-weight: 700;
    color: var(--danger);
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 12px;
  }}
  .rf-grid {{
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 14px;
    margin-bottom: 16px;
  }}
  @media (max-width: 900px) {{ .rf-grid {{ grid-template-columns: 1fr; }} }}

  .rf-card {{
    background: #fff;
    border-radius: 12px;
    box-shadow: var(--shadow);
    padding: 14px 18px;
    border-top: 4px solid var(--danger);
  }}
  .rf-card.rf-avail  {{ border-top-color: #E53935; }}
  .rf-card.rf-failed {{ border-top-color: #FB8C00; }}
  .rf-card.rf-cp     {{ border-top-color: #7B1FA2; }}
  .rf-card.rf-gmv    {{ border-top-color: #1976D2; }}

  .rf-card h4 {{
    font-size: 11px;
    text-transform: uppercase;
    font-weight: 700;
    letter-spacing: 0.5px;
    margin-bottom: 10px;
    display: flex;
    align-items: center;
    gap: 6px;
  }}
  .rf-card.rf-avail  h4 {{ color: #E53935; }}
  .rf-card.rf-failed h4 {{ color: #FB8C00; }}
  .rf-card.rf-cp     h4 {{ color: #7B1FA2; }}
  .rf-card.rf-gmv    h4 {{ color: #1976D2; }}

  .rf-item {{
    border-bottom: 1px solid var(--border);
    padding: 8px 0;
  }}
  .rf-item:last-child {{ border-bottom: none; }}
  .rf-brand {{
    font-size: 12.5px;
    font-weight: 700;
    color: var(--text);
    margin-bottom: 3px;
  }}
  .rf-detail {{
    font-size: 11.5px;
    color: var(--muted);
  }}
  .rf-reason {{
    font-size: 11px;
    color: #7B1FA2;
    margin-top: 3px;
  }}
  .rf-fix {{
    font-size: 11px;
    color: #2E7D32;
    margin-top: 2px;
    font-style: italic;
  }}
  .rf-empty {{
    font-size: 12px;
    color: #aaa;
    text-align: center;
    padding: 12px 0;
  }}

  /* ── old problem styles kept for table row highlighting ── */
  .badge-problem {{ display: inline-block; width: 8px; height: 8px; background: var(--danger); border-radius: 50%; margin-right: 4px; }}

  /* ── MAIN TABLE ── */
  .table-wrap {{
    background: #fff;
    border-radius: 12px;
    box-shadow: var(--shadow);
    overflow: hidden;
    margin-bottom: 32px;
  }}
  .table-header {{
    padding: 16px 20px 12px;
    border-bottom: 2px solid var(--bolt-green);
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }}
  .table-header h3 {{
    font-size: 14px;
    font-weight: 700;
    color: var(--bolt-dark);
  }}
  .table-search {{
    padding: 6px 12px;
    border: 1px solid var(--border);
    border-radius: 8px;
    font-size: 13px;
    width: 220px;
    outline: none;
  }}
  .table-search:focus {{ border-color: var(--bolt-green); }}

  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 12.5px;
  }}
  thead th {{
    background: var(--bolt-dark);
    color: #fff;
    padding: 9px 12px;
    text-align: left;
    font-weight: 600;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.3px;
    cursor: pointer;
    white-space: nowrap;
    user-select: none;
  }}
  thead th:hover {{ background: #333; }}
  thead th .sort-icon {{ margin-left: 4px; opacity: 0.5; }}
  thead th.sorted .sort-icon {{ opacity: 1; color: var(--bolt-green); }}
  tbody tr {{ border-bottom: 1px solid var(--border); transition: background 0.1s; }}
  tbody tr:hover {{ background: var(--bolt-light-green); }}
  tbody tr.problem-highlight {{ background: #FFF3E0; }}
  tbody tr.problem-highlight:hover {{ background: #FFE0B2; }}
  td {{ padding: 8px 12px; vertical-align: middle; }}
  td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  td.center {{ text-align: center; }}

  .badge-top {{ background: #FFF8E1; color: #F9A825; font-size: 10px; font-weight: 700; padding: 2px 6px; border-radius: 6px; }}
  .badge-status-active {{ background: #E8F5E9; color: #2E7D32; font-size: 10px; font-weight: 700; padding: 2px 6px; border-radius: 6px; }}
  .badge-status-inactive {{ background: #FFEBEE; color: #C62828; font-size: 10px; font-weight: 700; padding: 2px 6px; border-radius: 6px; }}
  .badge-problem {{ display: inline-block; width: 8px; height: 8px; background: var(--danger); border-radius: 50%; margin-right: 4px; }}
  .positive {{ color: #2E7D32; font-weight: 600; }}
  .negative {{ color: var(--danger); font-weight: 600; }}
  .neutral {{ color: var(--muted); }}

  /* ── STATS SUMMARY ── */
  .stats-row {{
    display: flex;
    gap: 12px;
    margin-bottom: 20px;
    flex-wrap: wrap;
  }}
  .stat-box {{
    background: #fff;
    border-radius: 10px;
    padding: 14px 18px;
    box-shadow: var(--shadow);
    flex: 1;
    min-width: 150px;
  }}
  .stat-label {{ font-size: 11px; color: var(--muted); text-transform: uppercase; font-weight: 600; letter-spacing: 0.4px; }}
  .stat-value {{ font-size: 22px; font-weight: 800; color: var(--bolt-dark); margin-top: 4px; }}
  .stat-sub {{ font-size: 11px; color: var(--muted); margin-top: 2px; }}

  /* ── FOOTER ── */
  .footer {{
    background: var(--bolt-dark);
    color: #aaa;
    text-align: center;
    padding: 16px;
    font-size: 11px;
    margin-top: 40px;
  }}
  .footer a {{ color: var(--bolt-green); text-decoration: none; }}

  /* ── PERIOD SELECTOR ── */
  .period-info {{
    background: var(--bolt-light-green);
    border-left: 4px solid var(--bolt-green);
    padding: 8px 16px;
    font-size: 12px;
    color: var(--bolt-mid-green);
    font-weight: 600;
    margin-bottom: 20px;
    border-radius: 0 8px 8px 0;
  }}

  .no-data {{ text-align: center; padding: 40px; color: var(--muted); font-size: 14px; }}

  /* ── DYNAMICS TAB ── */
  .dynamics-wrap {{ padding: 24px 28px; display: none; }}
  .dynamics-wrap.active {{ display: block; }}

  .dyn-controls {{
    display: flex;
    gap: 12px;
    align-items: center;
    flex-wrap: wrap;
    margin-bottom: 24px;
    background: #fff;
    padding: 16px 20px;
    border-radius: 12px;
    box-shadow: var(--shadow);
  }}
  .dyn-controls label {{ font-size: 12px; font-weight: 700; color: var(--muted); text-transform: uppercase; letter-spacing: 0.4px; }}
  .dyn-select {{
    padding: 8px 14px;
    border: 1px solid var(--border);
    border-radius: 8px;
    font-size: 13px;
    outline: none;
    min-width: 180px;
    cursor: pointer;
    background: #fff;
  }}
  .dyn-select:focus {{ border-color: var(--bolt-green); }}
  .dyn-search {{
    padding: 8px 14px;
    border: 1px solid var(--border);
    border-radius: 8px;
    font-size: 13px;
    outline: none;
    width: 240px;
  }}
  .dyn-search:focus {{ border-color: var(--bolt-green); }}
  .dyn-brand-info {{
    margin-left: auto;
    font-size: 12px;
    color: var(--muted);
    background: var(--bolt-light-green);
    border-radius: 8px;
    padding: 6px 14px;
  }}
  .dyn-brand-info strong {{ color: var(--bolt-mid-green); }}

  .charts-grid {{
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 20px;
    margin-bottom: 32px;
  }}
  @media (max-width: 900px) {{ .charts-grid {{ grid-template-columns: 1fr; }} }}

  .chart-card {{
    background: #fff;
    border-radius: 12px;
    box-shadow: var(--shadow);
    padding: 16px 18px;
  }}
  .chart-card h4 {{
    font-size: 12px;
    text-transform: uppercase;
    color: var(--muted);
    font-weight: 700;
    letter-spacing: 0.5px;
    margin-bottom: 12px;
  }}
  .chart-card canvas {{ max-height: 220px; }}

  .dyn-placeholder {{
    text-align: center;
    padding: 60px 20px;
    color: var(--muted);
    font-size: 15px;
  }}
  .dyn-placeholder .icon {{ font-size: 48px; display: block; margin-bottom: 12px; }}

  .city-tab-dynamics {{
    border-left: 2px solid var(--border);
    margin-left: 8px;
    color: var(--bolt-mid-green) !important;
  }}
  .city-tab-dynamics.active {{
    border-bottom-color: var(--bolt-mid-green) !important;
  }}

  /* ── OVERVIEW PAGE ── */
  .ov-kpi-row {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 16px;
    margin-bottom: 24px;
  }}
  @media (max-width: 900px) {{ .ov-kpi-row {{ grid-template-columns: 1fr; }} }}

  .ov-kpi-card {{
    background: #fff;
    border-radius: 12px;
    box-shadow: var(--shadow);
    padding: 20px 22px;
  }}
  .ov-kpi-icon {{ font-size: 24px; margin-bottom: 8px; }}
  .ov-kpi-title {{
    font-size: 11px;
    text-transform: uppercase;
    font-weight: 700;
    color: var(--muted);
    letter-spacing: 0.5px;
    margin-bottom: 6px;
  }}
  .ov-kpi-value {{
    font-size: 28px;
    font-weight: 800;
    color: var(--bolt-dark);
    margin-bottom: 6px;
  }}
  .ov-kpi-delta {{ margin-bottom: 4px; }}
  .ov-kpi-sub {{
    font-size: 11px;
    color: var(--muted);
    margin-bottom: 8px;
  }}
  .ov-analysis {{
    background: #FFF8F0;
    border-left: 3px solid #FB8C00;
    border-radius: 0 6px 6px 0;
    padding: 10px 12px;
    margin-top: 10px;
    font-size: 12px;
    color: #555;
  }}
  .ov-analysis-title {{
    font-weight: 700;
    color: #E65100;
    font-size: 11px;
    margin-bottom: 4px;
  }}

  .ov-charts-row {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 16px;
    margin-bottom: 24px;
  }}
  @media (max-width: 900px) {{ .ov-charts-row {{ grid-template-columns: 1fr; }} }}

  .ov-chart-card {{
    background: #fff;
    border-radius: 12px;
    box-shadow: var(--shadow);
    padding: 16px 18px;
  }}
  .ov-chart-title {{
    font-size: 11px;
    text-transform: uppercase;
    font-weight: 700;
    color: var(--muted);
    letter-spacing: 0.4px;
    margin-bottom: 10px;
  }}
</style>
</head>
<body>

<div class="header">
  <div class="header-logo">⚡ Bolt</div>
  <div>
    <div class="header-title">Портфоліо Акаунт-менеджера</div>
    <div class="header-sub">Marharyta Zhytnyk · Food Delivery · Ukraine</div>
  </div>
  <div class="header-meta">
    <div>Оновлено: <strong>{report_date}</strong></div>
    <div>Період: {period_start} — {period_end}</div>
    <div>Партнерів: <strong>{total_providers}</strong></div>
  </div>
</div>

<!-- CITY TABS -->
<div class="city-nav" id="cityNav">
{city_tabs}
  <div class="city-tab city-tab-dynamics" id="tab-dynamics" onclick="showDynamics()">📊 Динаміка</div>
</div>

<div class="content" id="mainContent">
{city_sections}
</div>

<!-- DYNAMICS PANEL -->
<div class="dynamics-wrap" id="dynamicsPanel">
  <div class="dyn-controls">
    <div>
      <label>Місто</label><br>
      <select class="dyn-select" id="dynCityFilter" onchange="updateBrandList()">
        <option value="">— Всі міста —</option>
        {dyn_city_options}
      </select>
    </div>
    <div>
      <label>Бренд</label><br>
      <input class="dyn-search" id="dynBrandSearch" type="text" placeholder="🔍 Пошук бренду..." oninput="updateBrandList()">
    </div>
    <div>
      <label>&nbsp;</label><br>
      <select class="dyn-select" id="dynBrandSelect" onchange="renderCharts()" size="1" style="min-width:260px;">
        <option value="">— Оберіть бренд —</option>
      </select>
    </div>
    <div class="dyn-brand-info" id="dynBrandInfo">
      Оберіть бренд для перегляду динаміки
    </div>
  </div>

  <div id="dynPlaceholder" class="dyn-placeholder">
    <span class="icon">📊</span>
    Оберіть бренд у фільтрі вище — відобразяться гістограми по тижнях
  </div>

  <div id="dynCharts" style="display:none">
    <div class="charts-grid">
      <div class="chart-card"><h4>📦 Замовлення (доставлені)</h4><canvas id="chartOrders"></canvas></div>
      <div class="chart-card"><h4>💰 GMV (€, до знижок)</h4><canvas id="chartGmv"></canvas></div>
      <div class="chart-card"><h4>📈 Contribution Profit (€)</h4><canvas id="chartCp"></canvas></div>
      <div class="chart-card"><h4>📉 CP L2 Маржа (%)</h4><canvas id="chartCpMargin"></canvas></div>
      <div class="chart-card"><h4>❌ Failed Order Rate (%)</h4><canvas id="chartFailed"></canvas></div>
      <div class="chart-card"><h4>😞 Bad Order Rate (%)</h4><canvas id="chartBad"></canvas></div>
      <div class="chart-card"><h4>✅ Acceptance Rate (%)</h4><canvas id="chartAcceptance"></canvas></div>
      <div class="chart-card"><h4>⏰ Late Delivery Rate (%)</h4><canvas id="chartLate"></canvas></div>
    </div>
  </div>
</div>

<div class="footer">
  Автоматично згенеровано · Bolt Food Partner Reports · <a href="https://github.com/marharytazhytnyk-create/partner-reports">GitHub</a>
</div>

<script>
// ── Embedded trend data ───────────────────────────────────────────────────────
const TRENDS = {trends_json};

// ── City navigation ───────────────────────────────────────────────────────────
function showCity(cityId) {{
  document.querySelectorAll('.city-tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.city-section').forEach(s => s.classList.remove('active'));
  document.getElementById('tab-' + cityId).classList.add('active');
  document.getElementById('city-' + cityId).classList.add('active');
  document.getElementById('mainContent').style.display = '';
  document.getElementById('dynamicsPanel').classList.remove('active');
}}

function showDynamics() {{
  document.querySelectorAll('.city-tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.city-section').forEach(s => s.classList.remove('active'));
  document.getElementById('tab-dynamics').classList.add('active');
  document.getElementById('mainContent').style.display = 'none';
  document.getElementById('dynamicsPanel').classList.add('active');
  updateBrandList();
}}

// ── Table search ──────────────────────────────────────────────────────────────
function filterTable(inputEl, tableId) {{
  const q = inputEl.value.toLowerCase();
  document.querySelectorAll('#' + tableId + ' tbody tr').forEach(row => {{
    row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
  }});
}}

// ── Table sort ────────────────────────────────────────────────────────────────
function sortTable(tableId, col, asc) {{
  const tbody = document.querySelector('#' + tableId + ' tbody');
  const rows = Array.from(tbody.querySelectorAll('tr'));
  rows.sort((a, b) => {{
    const va = a.cells[col]?.dataset?.val ?? a.cells[col]?.textContent ?? '';
    const vb = b.cells[col]?.dataset?.val ?? b.cells[col]?.textContent ?? '';
    const na = parseFloat(va.replace(/[^\d.-]/g, ''));
    const nb = parseFloat(vb.replace(/[^\d.-]/g, ''));
    if (!isNaN(na) && !isNaN(nb)) return asc ? na - nb : nb - na;
    return asc ? va.localeCompare(vb, 'uk') : vb.localeCompare(va, 'uk');
  }});
  rows.forEach(r => tbody.appendChild(r));
}}

// ── Dynamics: brand list ──────────────────────────────────────────────────────
function updateBrandList() {{
  const cityFilter = document.getElementById('dynCityFilter').value;
  const searchQ    = document.getElementById('dynBrandSearch').value.toLowerCase();
  const select     = document.getElementById('dynBrandSelect');
  const prevVal    = select.value;

  const keys = Object.keys(TRENDS).filter(k => {{
    const [brand, city] = k.split('|||');
    const cityOk   = !cityFilter || city === cityFilter;
    const searchOk = !searchQ   || brand.toLowerCase().includes(searchQ);
    return cityOk && searchOk;
  }});

  keys.sort((a, b) => a.split('|||')[0].localeCompare(b.split('|||')[0], 'uk'));

  select.innerHTML = '<option value="">— Оберіть бренд —</option>';
  keys.forEach(k => {{
    const [brand, city] = k.split('|||');
    const opt = document.createElement('option');
    opt.value = k;
    opt.textContent = `${{brand}} (${{city}})`;
    select.appendChild(opt);
  }});

  if (prevVal && keys.includes(prevVal)) {{
    select.value = prevVal;
  }}
  renderCharts();
}}

// ── Chart instances ───────────────────────────────────────────────────────────
const chartInstances = {{}};

function makeOrUpdate(id, labels, data, color, yLabel, isFill) {{
  const ctx = document.getElementById(id).getContext('2d');
  if (chartInstances[id]) {{
    chartInstances[id].destroy();
  }}
  const borderColor = color;
  const bgColor = color + '33';

  chartInstances[id] = new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels,
      datasets: [{{
        label: yLabel,
        data,
        backgroundColor: data.map(v => {{
          if (v === null) return '#e0e0e033';
          // Red tones for "bad" metrics when value is high
          return bgColor;
        }}),
        borderColor,
        borderWidth: 2,
        borderRadius: 4,
      }}]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: true,
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{
            label: ctx => `${{yLabel}}: ${{ctx.parsed.y !== null ? ctx.parsed.y.toLocaleString('uk-UA') : '—'}}`
          }}
        }}
      }},
      scales: {{
        x: {{
          ticks: {{ font: {{ size: 10 }}, maxRotation: 45 }},
          grid: {{ display: false }}
        }},
        y: {{
          beginAtZero: false,
          ticks: {{ font: {{ size: 10 }} }},
          grid: {{ color: '#f0f0f0' }}
        }}
      }}
    }}
  }});
}}

function fmtWeek(w) {{
  // "2026-03-23" → "23 бер"
  const months = ['', 'січ', 'лют', 'бер', 'кві', 'тра', 'чер',
                  'лип', 'сер', 'вер', 'жов', 'лис', 'гру'];
  const [, m, d] = w.split('-');
  return `${{parseInt(d)}} ${{months[parseInt(m)]}}`;
}}

function renderCharts() {{
  const key = document.getElementById('dynBrandSelect').value;
  const placeholder = document.getElementById('dynPlaceholder');
  const chartsDiv   = document.getElementById('dynCharts');
  const infoDiv     = document.getElementById('dynBrandInfo');

  if (!key || !TRENDS[key]) {{
    placeholder.style.display = '';
    chartsDiv.style.display   = 'none';
    infoDiv.innerHTML = 'Оберіть бренд для перегляду динаміки';
    return;
  }}

  placeholder.style.display = 'none';
  chartsDiv.style.display   = '';

  const d = TRENDS[key];
  const labels = d.weeks.map(fmtWeek);

  infoDiv.innerHTML = `<strong>${{d.brand}}</strong> · ${{d.city}} · ${{d.weeks.length}} тижнів даних`;

  const GREEN  = '#1DC462';
  const BLUE   = '#1976D2';
  const PURPLE = '#7B1FA2';
  const TEAL   = '#00897B';
  const RED    = '#E53935';
  const ORANGE = '#FB8C00';
  const INDIGO = '#3949AB';
  const AMBER  = '#F9A825';

  makeOrUpdate('chartOrders',    labels, d.delivered_orders,        GREEN,  'Замовлення', false);
  makeOrUpdate('chartGmv',       labels, d.gmv_eur,                 BLUE,   'GMV, €', false);
  makeOrUpdate('chartCp',        labels, d.contribution_profit_eur, TEAL,   'CP, €', false);
  makeOrUpdate('chartCpMargin',  labels, d.cp_l2_margin_pct,        PURPLE, 'CP L2 Маржа, %', false);
  makeOrUpdate('chartFailed',    labels, d.failed_order_rate_pct,   RED,    'Failed Rate, %', false);
  makeOrUpdate('chartBad',       labels, d.bad_order_rate_pct,      ORANGE, 'Bad Order Rate, %', false);
  makeOrUpdate('chartAcceptance',labels, d.acceptance_rate_pct,     INDIGO, 'Acceptance Rate, %', false);
  makeOrUpdate('chartLate',      labels, d.late_delivery_rate_pct,  AMBER,  'Late Delivery, %', false);
}}

// ── Init ─────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {{
  // Overview tab is already active by default via HTML class
  // but trigger click to ensure consistent state
  const overviewTab = document.getElementById('tab-overview');
  if (overviewTab) overviewTab.click();
}});
</script>
</body>
</html>
"""


def build_trends_json(df_trends: pd.DataFrame) -> str:
    """Convert weekly trends DataFrame into a JSON string for embedding in HTML."""
    import math
    data = {}
    for col in ["delivered_orders", "gmv_eur", "contribution_profit_eur",
                "cp_l2_margin_pct", "failed_order_rate_pct",
                "bad_order_rate_pct", "acceptance_rate_pct", "late_delivery_rate_pct"]:
        if col in df_trends.columns:
            df_trends[col] = pd.to_numeric(df_trends[col], errors="coerce")

    for (brand, city), grp in df_trends.groupby(["brand_name", "city_name"], sort=False):
        key = f"{brand}|||{city}"
        grp = grp.sort_values("week_start")

        def clean(val):
            try:
                v = float(val)
                return None if math.isnan(v) or math.isinf(v) else round(v, 2)
            except (TypeError, ValueError):
                return None

        data[key] = {
            "brand": brand,
            "city": city,
            "weeks": grp["week_start"].tolist(),
            "delivered_orders":        [clean(v) for v in grp["delivered_orders"]],
            "gmv_eur":                 [clean(v) for v in grp["gmv_eur"]],
            "contribution_profit_eur": [clean(v) for v in grp["contribution_profit_eur"]],
            "cp_l2_margin_pct":        [clean(v) for v in grp["cp_l2_margin_pct"]],
            "failed_order_rate_pct":   [clean(v) for v in grp["failed_order_rate_pct"]],
            "bad_order_rate_pct":      [clean(v) for v in grp["bad_order_rate_pct"]],
            "acceptance_rate_pct":     [clean(v) for v in grp["acceptance_rate_pct"]],
            "late_delivery_rate_pct":  [clean(v) for v in grp["late_delivery_rate_pct"]],
        }
    return json.dumps(data, ensure_ascii=False)


def city_slug(name: str) -> str:
    """Create safe HTML ID from city name."""
    import re
    slug = name.lower().replace(" ", "_").replace("-", "_")
    slug = re.sub(r'[^\w]', '', slug)
    return slug or "city"


def build_top_card(providers: pd.DataFrame, metric_col: str, title: str, icon: str, fmt_func) -> str:
    top3 = providers.nlargest(3, metric_col) if len(providers) >= 1 else providers
    rank_classes = ["gold", "silver", "bronze"]
    items_html = ""
    for i, (_, row) in enumerate(top3.iterrows()):
        cls = rank_classes[i] if i < 3 else ""
        name = str(row.get("brand_name") or row.get("provider_name") or "—")
        val = fmt_func(row.get(metric_col))
        items_html += f"""
        <div class="top-item">
          <div class="top-item-rank {cls}">{i+1}</div>
          <div class="top-item-name" title="{name}">{name}</div>
          <div class="top-item-val">{val}</div>
        </div>"""
    if not items_html:
        items_html = '<div class="no-data" style="padding:12px;font-size:12px;">Немає даних</div>'
    return f"""
    <div class="top-card">
      <h3><span class="icon">{icon}</span>{title}</h3>
      {items_html}
    </div>"""


def build_red_flags_panel(city_df: pd.DataFrame, gmv_wow_map: dict) -> str:
    """Build the 4-column red flags panel for a city."""

    # Collect flags per brand
    avail_items, failed_items, cp_items, gmv_items = [], [], [], []

    for _, row in city_df.iterrows():
        brand = str(row.get("brand_name") or "—")
        orders = fmt_num(row.get("delivered_orders"))
        gmv_val = fmt_eur(row.get("gmv_eur"))
        city = str(row.get("city_name") or "")
        wow_pct = gmv_wow_map.get((brand, city))
        flags = get_red_flags(row, wow_pct)

        if flags["availability"]:
            avail = safe_float(row.get("active_rate")) * 100
            avail_items.append(
                f'<div class="rf-item">'
                f'<div class="rf-brand">{brand}</div>'
                f'<div class="rf-detail">Availability: <b>{avail:.1f}%</b> (норма ≥95%) · {orders} зам.</div>'
                f'<div class="rf-fix">→ Перевірити графік роботи та налаштування доступності</div>'
                f'</div>'
            )

        if flags["failed_orders"]:
            failed_abs = int(safe_float(row.get("failed_orders")))
            failed_rate = safe_float(row.get("failed_order_rate")) * 100
            avail_val = safe_float(row.get("active_rate")) * 100
            avail_hint = ""
            if avail_val < 95:
                avail_hint = " · можлива причина — низька доступність"
            accept_val = safe_float(row.get("acceptance_rate")) * 100
            accept_hint = ""
            if accept_val < 90 and accept_val > 0:
                accept_hint = f" · acceptance rate {accept_val:.0f}%"
            failed_items.append(
                f'<div class="rf-item">'
                f'<div class="rf-brand">{brand}</div>'
                f'<div class="rf-detail"><b>{failed_abs} failed замовлень</b> ({failed_rate:.1f}% від розміщених){avail_hint}{accept_hint}</div>'
                f'<div class="rf-fix">→ Перевірити причини відмов і налаштувати меню/доступність</div>'
                f'</div>'
            )

        if flags["cp_negative"]:
            cp_data = flags["cp_negative"]
            margin = cp_data["margin"]
            cp_html = (
                f'<div class="rf-item">'
                f'<div class="rf-brand">{brand}</div>'
                f'<div class="rf-detail">CP L2 Margin: <b>{margin:.1f}%</b> · GMV: {gmv_val}</div>'
            )
            for r in cp_data["reasons"]:
                cp_html += f'<div class="rf-reason">⚠ {r}</div>'
            for f_ in cp_data["fixes"]:
                cp_html += f'<div class="rf-fix">→ {f_}</div>'
            cp_html += '</div>'
            cp_items.append(cp_html)

        if flags["gmv_drop"]:
            wow = wow_pct if wow_pct is not None else 0
            gmv_items.append(
                f'<div class="rf-item">'
                f'<div class="rf-brand">{brand}</div>'
                f'<div class="rf-detail">GMV впав на <b>{abs(wow):.1f}%</b> WoW · Поточний: {gmv_val}</div>'
                f'<div class="rf-fix">→ Перевірити динаміку замовлень та активність акцій</div>'
                f'</div>'
            )

    total_flags = len(avail_items) + len(failed_items) + len(cp_items) + len(gmv_items)
    if total_flags == 0:
        return ""

    def card(cls, icon, title, count, items):
        body = "".join(items[:5]) if items else f'<div class="rf-empty">Немає порушень ✓</div>'
        more = f'<div class="rf-detail" style="text-align:center;padding:6px 0;color:var(--muted)">+ ще {count - 5} бренд(ів)…</div>' if count > 5 else ""
        cnt = f' <span style="background:#fff2;border-radius:10px;padding:1px 7px;font-size:10px">{count}</span>' if count else ""
        return f'<div class="rf-card {cls}"><h4>{icon} {title}{cnt}</h4>{body}{more}</div>'

    return f"""
    <div class="red-flags-wrap">
      <div class="red-flags-title">🚩 Red Flag перформери ({total_flags} випадків)</div>
      <div class="rf-grid">
        {card("rf-avail",  "🔴", "Availability < 95%",         len(avail_items),  avail_items)}
        {card("rf-failed", "🟠", "Failed замовлення > 2",       len(failed_items), failed_items)}
        {card("rf-cp",     "🟣", "Від'ємна CP L2 Margin",       len(cp_items),     cp_items)}
        {card("rf-gmv",    "📉", "Падіння GMV > 1% WoW",        len(gmv_items),    gmv_items)}
      </div>
    </div>"""


def build_provider_table(providers: pd.DataFrame, city_id: str, gmv_wow_map: dict = None) -> str:
    if providers.empty:
        return '<div class="no-data">Немає даних по брендах</div>'

    gmv_wow_map = gmv_wow_map or {}
    table_id = f"tbl_{city_id}"
    rows_html = ""
    for _, row in providers.iterrows():
        brand = str(row.get("brand_name") or "—")
        city  = str(row.get("city_name") or "")
        orders = safe_float(row.get("delivered_orders"))
        gmv = safe_float(row.get("gmv_eur"))
        cp = safe_float(row.get("contribution_profit_eur"))
        cp_margin = safe_float(row.get("cp_l2_margin_pct"))
        locs = int(safe_float(row.get("locations_count"), 1))
        avail = safe_float(row.get("active_rate")) * 100
        failed_abs = safe_float(row.get("failed_orders"))
        wow_pct = gmv_wow_map.get((brand, city))
        flags = get_red_flags(row, wow_pct)
        has_flags = has_any_flag(flags)

        problem_icon = '<span class="badge-problem"></span>' if has_flags else ""
        top_val = str(row.get("is_top_brand"))
        top_badge = '<span class="badge-top">TOP</span>' if top_val in ["1", "1.0", "True", "true"] else ""
        cp_class = "positive" if cp_margin > 0 else "negative" if cp_margin < -1 else "neutral"
        failed_rate = safe_float(row.get("failed_order_rate"))
        failed_class = "negative" if failed_abs > 2 else ""
        avail_class = "negative" if 0 < avail < 95 else ""
        row_class = "problem-highlight" if has_flags else ""
        locs_badge = (
            f'<span style="font-size:10px;background:#E3F2FD;color:#1565C0;'
            f'border-radius:6px;padding:1px 5px;margin-left:4px">{locs} лок.</span>'
        ) if locs > 1 else ""
        # WoW GMV delta
        if wow_pct is not None:
            wow_color = "var(--danger)" if wow_pct < -1 else "#2E7D32"
            wow_arrow = "▼" if wow_pct < 0 else "▲"
            wow_html = f' <span style="font-size:10px;color:{wow_color}">{wow_arrow}{abs(wow_pct):.1f}%</span>'
        else:
            wow_html = ""

        rows_html += f"""
        <tr class="{row_class}">
          <td>{problem_icon}{brand} {top_badge}{locs_badge}</td>
          <td>{str(row.get('business_segment_v2') or '—')}</td>
          <td class="num" data-val="{locs}">{locs}</td>
          <td class="num" data-val="{orders}">{fmt_num(orders)}</td>
          <td class="num" data-val="{gmv}">{fmt_eur(gmv)}{wow_html}</td>
          <td class="num" data-val="{cp}">{fmt_eur(cp)}</td>
          <td class="num {cp_class}" data-val="{cp_margin}">{fmt_pct(cp_margin)}</td>
          <td class="num {avail_class}" data-val="{avail}">{fmt_pct(avail) if avail > 0 else '—'}</td>
          <td class="num {failed_class}" data-val="{failed_abs}">{int(failed_abs)}</td>
          <td class="num" data-val="{safe_float(row.get('bad_order_rate'))}">{fmt_pct(safe_float(row.get('bad_order_rate'))*100)}</td>
          <td class="num" data-val="{safe_float(row.get('acceptance_rate'))}">{fmt_pct(safe_float(row.get('acceptance_rate'))*100)}</td>
        </tr>"""

    search_id = f"search_{city_id}"
    num_cols = 11
    sort_js = "".join(f"let sort_{table_id}_{i}=false;" for i in range(num_cols))
    header_cols = [
        ("Бренд", 0), ("Сегмент", 1), ("Лок.", 2),
        ("Замовлення", 3), ("GMV, €", 4), ("CP, €", 5),
        ("CP L2 %", 6), ("Availability", 7), ("Failed зам.", 8),
        ("Bad Order %", 9), ("Acceptance %", 10),
    ]
    th_html = ""
    for label, idx in header_cols:
        th_html += (
            f'<th onclick="sort_{table_id}_{idx}=!sort_{table_id}_{idx};'
            f'sortTable(\'{table_id}\',{idx},sort_{table_id}_{idx})">'
            f'{label} <span class="sort-icon">⇅</span></th>'
        )

    return f"""
    <div class="table-wrap">
      <div class="table-header">
        <h3>📋 Всі бренди</h3>
        <input class="table-search" id="{search_id}" type="text" placeholder="🔍 Пошук..."
               oninput="filterTable(this, '{table_id}')">
      </div>
      <div style="overflow-x:auto">
        <table id="{table_id}">
          <thead><tr>{th_html}</tr></thead>
          <tbody>{rows_html}</tbody>
        </table>
      </div>
    </div>
    <script>{sort_js}</script>"""


def build_overview_section(df_trends: pd.DataFrame) -> str:
    """
    Build the portfolio overview section comparing last week vs previous week.
    Returns HTML string.
    """
    import math

    if df_trends.empty:
        return '<div class="no-data">Немає трендових даних для аналізу</div>'

    for col in ["delivered_orders", "gmv_eur", "contribution_profit_eur",
                "cp_l2_margin_pct", "failed_order_rate_pct",
                "bad_order_rate_pct", "acceptance_rate_pct", "late_delivery_rate_pct"]:
        if col in df_trends.columns:
            df_trends[col] = pd.to_numeric(df_trends[col], errors="coerce")

    # Aggregate to portfolio level per week
    portfolio_weekly = (
        df_trends.groupby("week_start", sort=True)
        .agg(
            gmv=("gmv_eur", "sum"),
            orders=("delivered_orders", "sum"),
            cp=("contribution_profit_eur", "sum"),
            failed_rate=("failed_order_rate_pct", "mean"),
            bad_rate=("bad_order_rate_pct", "mean"),
            acceptance=("acceptance_rate_pct", "mean"),
            late_rate=("late_delivery_rate_pct", "mean"),
        )
        .reset_index()
        .sort_values("week_start")
    )

    if len(portfolio_weekly) < 2:
        return '<div class="no-data">Недостатньо даних для порівняння тижнів</div>'

    last  = portfolio_weekly.iloc[-1]
    prev  = portfolio_weekly.iloc[-2]
    last_week_label = last["week_start"]
    prev_week_label = prev["week_start"]

    def wow(curr, prev_val):
        try:
            c, p = float(curr), float(prev_val)
            if p == 0 or math.isnan(c) or math.isnan(p):
                return None
            return (c / p - 1) * 100
        except (TypeError, ValueError):
            return None

    gmv_wow    = wow(last["gmv"],    prev["gmv"])
    orders_wow = wow(last["orders"], prev["orders"])
    cp_wow     = wow(last["cp"],     prev["cp"])

    # CP margin %
    cp_margin_last = (float(last["cp"]) / float(last["gmv"]) * 100) if float(last["gmv"]) > 0 else 0
    cp_margin_prev = (float(prev["cp"]) / float(prev["gmv"]) * 100) if float(prev["gmv"]) > 0 else 0

    # ── Trend chart data (all weeks, portfolio level) ──────────────────────────
    weeks_labels = [w for w in portfolio_weekly["week_start"].tolist()]
    weeks_js = json.dumps(weeks_labels)
    gmv_series    = json.dumps([round(float(v), 2) if not math.isnan(float(v)) else None for v in portfolio_weekly["gmv"]])
    orders_series = json.dumps([round(float(v), 2) if not math.isnan(float(v)) else None for v in portfolio_weekly["orders"]])
    cp_series     = json.dumps([round(float(v), 2) if not math.isnan(float(v)) else None for v in portfolio_weekly["cp"]])

    def kpi_card(icon, title, value_str, wow_pct, sub, analysis_html=""):
        if wow_pct is None:
            delta_html = '<span style="color:#aaa;font-size:13px">— WoW</span>'
        elif wow_pct > 0:
            delta_html = f'<span style="color:#2E7D32;font-size:14px;font-weight:700">▲ {wow_pct:.1f}%</span>'
        else:
            delta_html = f'<span style="color:#E53935;font-size:14px;font-weight:700">▼ {abs(wow_pct):.1f}%</span>'

        border = "#E53935" if (wow_pct is not None and wow_pct < 0) else "#1DC462"
        return f"""
        <div class="ov-kpi-card" style="border-top:4px solid {border}">
          <div class="ov-kpi-icon">{icon}</div>
          <div class="ov-kpi-title">{title}</div>
          <div class="ov-kpi-value">{value_str}</div>
          <div class="ov-kpi-delta">{delta_html}</div>
          <div class="ov-kpi-sub">{sub}</div>
          {analysis_html}
        </div>"""

    def drop_analysis(metric: str, wow_pct, last_row, prev_row) -> str:
        """Generate Ukrainian explanation for a metric drop."""
        if wow_pct is None or wow_pct >= 0:
            return ""

        lines = []

        failed  = float(last_row.get("failed_rate", 0) or 0)
        bad     = float(last_row.get("bad_rate",    0) or 0)
        accept  = float(last_row.get("acceptance",  0) or 0)
        late    = float(last_row.get("late_rate",   0) or 0)

        prev_failed  = float(prev_row.get("failed_rate", 0) or 0)
        prev_bad     = float(prev_row.get("bad_rate",    0) or 0)
        prev_accept  = float(prev_row.get("acceptance",  0) or 0)

        if metric == "gmv":
            # Check if orders also dropped
            o_wow = wow(last_row["orders"], prev_row["orders"])
            if o_wow is not None and o_wow < -1:
                lines.append(f"Кількість замовлень також знизилась на {abs(o_wow):.1f}% — менша активність покупців або нижча доступність закладів")
            else:
                lines.append("Кількість замовлень залишилась стабільною — можливо знизився середній чек замовлення")
            if bad > 10:
                lines.append(f"Висока частка поганих замовлень ({bad:.1f}%) — збільшились витрати на компенсації")
            if failed > 3 and failed > prev_failed * 1.1:
                lines.append(f"Зросла кількість зафейлених замовлень ({failed:.1f}%) — прямі втрати GMV")

        elif metric == "orders":
            if accept < 90 and accept < prev_accept - 2:
                lines.append(f"Знизився Acceptance Rate ({accept:.1f}%) — частіші відмови від замовлень")
            if failed > 3 and failed > prev_failed * 1.1:
                lines.append(f"Зросла частка failed замовлень ({failed:.1f}%) — заклади не приймали замовлення")
            if late > 20:
                lines.append(f"Висока частка запізнень ({late:.1f}%) — погіршення досвіду покупців знижує повторні замовлення")
            if not lines:
                lines.append("Можлива сезонність або зниження активності в окремих містах")

        elif metric == "cp":
            if bad > 10 and bad > prev_bad * 1.05:
                lines.append(f"Зросла частка поганих замовлень ({bad:.1f}%) — більші витрати на повернення коштів")
            if failed > 3:
                lines.append(f"Зафейлені замовлення ({failed:.1f}%) — прямі збитки без доходу")
            if accept < 88 and accept < prev_accept - 2:
                lines.append(f"Падіння Acceptance Rate ({accept:.1f}%) — витрати без реалізованого GMV")
            # Check GMV drop
            g_wow = wow(last_row["gmv"], prev_row["gmv"])
            if g_wow is not None and g_wow < -2:
                lines.append(f"Загальний GMV знизився на {abs(g_wow):.1f}% — менший обсяг для покриття постійних витрат")
            if not lines:
                lines.append("Зміна структури замовлень або умов комісії — рекомендується детальний аналіз по брендах")

        if not lines:
            return ""

        items = "".join(f'<li style="margin-bottom:4px">{l}</li>' for l in lines)
        return f"""
        <div class="ov-analysis">
          <div class="ov-analysis-title">🔍 Можливі причини зниження:</div>
          <ul style="margin:6px 0 0 16px;padding:0">{items}</ul>
        </div>"""

    gmv_analysis    = drop_analysis("gmv",    gmv_wow,    last, prev)
    orders_analysis = drop_analysis("orders", orders_wow, last, prev)
    cp_analysis     = drop_analysis("cp",     cp_wow,     last, prev)

    kpi1 = kpi_card("💰", "GMV останній тиждень",
                    fmt_eur(last["gmv"]),
                    gmv_wow,
                    f"Попередній: {fmt_eur(prev['gmv'])}",
                    gmv_analysis)
    kpi2 = kpi_card("📦", "Delivered Orders",
                    fmt_num(last["orders"]),
                    orders_wow,
                    f"Попередній: {fmt_num(prev['orders'])} замовлень",
                    orders_analysis)
    kpi3 = kpi_card("📈", "Contribution Margin",
                    fmt_eur(last["cp"]),
                    cp_wow,
                    f"Маржа: {cp_margin_last:.1f}% (попер. {cp_margin_prev:.1f}%)",
                    cp_analysis)

    # Mini sparkline charts using Chart.js
    charts_html = f"""
    <div class="ov-charts-row">
      <div class="ov-chart-card">
        <div class="ov-chart-title">GMV по тижнях (€)</div>
        <canvas id="ovChartGmv" height="80"></canvas>
      </div>
      <div class="ov-chart-card">
        <div class="ov-chart-title">Замовлення по тижнях</div>
        <canvas id="ovChartOrders" height="80"></canvas>
      </div>
      <div class="ov-chart-card">
        <div class="ov-chart-title">Contribution Profit по тижнях (€)</div>
        <canvas id="ovChartCp" height="80"></canvas>
      </div>
    </div>
    <script>
    (function() {{
      function mkSparkline(id, labels, data, color) {{
        const ctx = document.getElementById(id);
        if (!ctx) return;
        new Chart(ctx, {{
          type: 'line',
          data: {{
            labels: labels.map(w => {{
              const months = ['','січ','лют','бер','кві','тра','чер','лип','сер','вер','жов','лис','гру'];
              const [,m,d] = w.split('-'); return parseInt(d)+' '+months[parseInt(m)];
            }}),
            datasets: [{{
              data,
              borderColor: color,
              backgroundColor: color + '18',
              borderWidth: 2.5,
              pointRadius: data.map((_, i) => i === data.length - 1 ? 5 : 3),
              pointBackgroundColor: data.map((_, i) => i === data.length - 1 ? color : '#fff'),
              tension: 0.3,
              fill: true,
            }}]
          }},
          options: {{
            responsive: true,
            plugins: {{ legend: {{ display: false }}, tooltip: {{ mode: 'index' }} }},
            scales: {{
              x: {{ ticks: {{ font: {{ size: 10 }}, maxRotation: 30 }}, grid: {{ display: false }} }},
              y: {{ ticks: {{ font: {{ size: 10 }} }}, grid: {{ color: '#f0f0f0' }} }}
            }}
          }}
        }});
      }}
      mkSparkline('ovChartGmv',    {weeks_js}, {gmv_series},    '#1DC462');
      mkSparkline('ovChartOrders', {weeks_js}, {orders_series}, '#1976D2');
      mkSparkline('ovChartCp',     {weeks_js}, {cp_series},     '#7B1FA2');
    }})();
    </script>"""

    # Comparison week label
    week_label = f"Тиждень {last_week_label} vs {prev_week_label}"

    return f"""
    <div class="city-section active" id="city-overview">
      <div class="section-title">
        🏠 Огляд портфоліо
        <span class="badge">Marharyta Zhytnyk</span>
      </div>
      <div class="section-sub">{week_label} · порівняння останніх двох повних тижнів</div>
      <div class="period-info">📅 Дані за останні 12 тижнів · Останній тиждень: {last_week_label}</div>

      <div class="ov-kpi-row">
        {kpi1}
        {kpi2}
        {kpi3}
      </div>

      {charts_html}
    </div>"""


def build_html(df: pd.DataFrame, df_trends: pd.DataFrame, start_date: str, end_date: str) -> str:
    """Build full HTML report from DataFrame."""
    if df.empty:
        return "<html><body><h1>Немає даних</h1></body></html>"

    # Numeric conversion
    for col in ["delivered_orders", "failed_orders", "placed_orders", "gmv_eur",
                "contribution_profit_eur", "cp_l2_margin_pct", "bad_order_rate",
                "failed_order_rate", "acceptance_rate", "late_delivery_rate", "active_rate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    cities = sorted(df["city_name"].dropna().unique().tolist())
    total_brands = df["brand_name"].nunique()
    total_locations = int(df["locations_count"].fillna(1).sum()) if "locations_count" in df.columns else total_brands

    # Compute week-over-week GMV change from trends
    gmv_wow_map = compute_gmv_wow(df_trends)

    # Build overview section
    overview_section = build_overview_section(df_trends.copy() if not df_trends.empty else df_trends)

    city_tabs_html = (
        '<div class="city-tab active" id="tab-overview" onclick="showCity(\'overview\')">'
        '🏠 Огляд портфоліо</div>'
    )
    city_sections_html = overview_section

    for city in cities:
        cid = city_slug(city)
        city_df = df[df["city_name"] == city].copy()
        brands_count = len(city_df)
        locs_count = int(city_df["locations_count"].fillna(1).sum()) if "locations_count" in city_df.columns else brands_count

        # Count red flags for tab badge
        n_flags = sum(
            1 for _, row in city_df.iterrows()
            if has_any_flag(get_red_flags(row, gmv_wow_map.get((str(row.get("brand_name") or ""), city))))
        )
        flag_badge = (
            f' <span style="background:#E53935;color:#fff;border-radius:8px;'
            f'padding:1px 6px;font-size:10px;font-weight:700">🚩{n_flags}</span>'
        ) if n_flags > 0 else ""

        city_tabs_html += (
            f'<div class="city-tab" id="tab-{cid}" onclick="showCity(\'{cid}\')">'
            f'{city}{flag_badge} <span style="font-size:11px;color:var(--muted)">({brands_count})</span></div>'
        )

        # Totals for the city
        total_orders = city_df["delivered_orders"].sum()
        total_gmv = city_df["gmv_eur"].sum()
        total_cp = city_df["contribution_profit_eur"].sum()
        avg_cp_margin = (total_cp / total_gmv * 100) if total_gmv > 0 else 0

        stats_html = f"""
        <div class="stats-row">
          <div class="stat-box">
            <div class="stat-label">Брендів</div>
            <div class="stat-value">{brands_count}</div>
            <div class="stat-sub">{locs_count} активних локацій</div>
          </div>
          <div class="stat-box">
            <div class="stat-label">Замовлення (4 тижні)</div>
            <div class="stat-value">{fmt_num(total_orders)}</div>
            <div class="stat-sub">доставлені</div>
          </div>
          <div class="stat-box">
            <div class="stat-label">GMV (4 тижні)</div>
            <div class="stat-value">{fmt_eur(total_gmv)}</div>
            <div class="stat-sub">до знижок</div>
          </div>
          <div class="stat-box">
            <div class="stat-label">Contribution Profit</div>
            <div class="stat-value {'positive' if total_cp >= 0 else 'negative'}">{fmt_eur(total_cp)}</div>
            <div class="stat-sub">маржа: {fmt_pct(avg_cp_margin)}</div>
          </div>
          <div class="stat-box" style="border-top:3px solid #E53935">
            <div class="stat-label" style="color:#E53935">🚩 Red Flags</div>
            <div class="stat-value" style="color:#E53935">{n_flags}</div>
            <div class="stat-sub">брендів з проблемами</div>
          </div>
        </div>"""

        # TOP 3 by each metric
        top_orders_html = build_top_card(city_df, "delivered_orders", "ТОП по замовленнях", "📦", fmt_num)
        top_gmv_html    = build_top_card(city_df, "gmv_eur", "ТОП по GMV", "💰", fmt_eur)
        top_cp_html     = build_top_card(city_df, "contribution_profit_eur", "ТОП по CP L2", "📈", fmt_eur)

        # Red Flags panel
        red_flags_html = build_red_flags_panel(city_df, gmv_wow_map)

        # Brand table
        table_html = build_provider_table(
            city_df.sort_values("gmv_eur", ascending=False), cid, gmv_wow_map
        )

        city_sections_html += f"""
        <div class="city-section" id="city-{cid}">
          <div class="section-title">
            🏙️ {city}
            <span class="badge">{brands_count} брендів · {locs_count} локацій</span>
          </div>
          <div class="section-sub">Період аналізу: {start_date} — {end_date} · Account Manager: Marharyta Zhytnyk</div>
          <div class="period-info">📅 Останні 4 повні тижні: {start_date} — {end_date}</div>

          {stats_html}

          {red_flags_html}

          <div class="top-grid">
            {top_orders_html}
            {top_gmv_html}
            {top_cp_html}
          </div>

          {table_html}
        </div>"""

    # Build city options for dynamics dropdown
    all_cities = sorted(df["city_name"].dropna().unique().tolist())
    dyn_city_options = "\n".join(
        f'        <option value="{c}">{c}</option>' for c in all_cities
    )

    # Build trends JSON
    trends_json_str = build_trends_json(df_trends)

    html = HTML_TEMPLATE.format(
        report_date=REPORT_DATE,
        period_start=start_date,
        period_end=end_date,
        total_providers=f"{total_brands} брендів / {total_locations} локацій",
        city_tabs=city_tabs_html,
        city_sections=city_sections_html,
        dyn_city_options=dyn_city_options,
        trends_json=trends_json_str,
    )
    return html


# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"=== Marharyta Portfolio Report [{REPORT_DATE}] ===\n")

    if not DATABRICKS_TOKEN:
        print("ERROR: DATABRICKS_TOKEN not set. Please set the environment variable.")
        sys.exit(1)

    start_date, end_date = get_last_4_full_weeks()

    try:
        df = fetch_provider_summary()
    except Exception as exc:
        print(f"ERROR fetching summary data: {exc}")
        sys.exit(1)

    try:
        df_trends = fetch_weekly_trends(n_weeks=12)
    except Exception as exc:
        print(f"WARNING: Could not fetch weekly trends: {exc}")
        df_trends = pd.DataFrame()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    out_path = os.path.join(base_dir, OUTPUT_FILE)

    html = build_html(df, df_trends, start_date, end_date)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✅ HTML report saved → {out_path}")
    return out_path


if __name__ == "__main__":
    main()
