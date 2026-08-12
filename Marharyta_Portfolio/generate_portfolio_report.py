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


def _exec_sql_paginated(sql_template: str, timeout: int = 300, page_size: int = 1000) -> pd.DataFrame:
    """
    Execute a SQL query with LIMIT/OFFSET pagination to bypass the 1000-row API limit.
    sql_template must contain {limit} and {offset} placeholders.
    """
    ctx = _create_context()
    all_frames = []
    offset = 0
    while True:
        sql = sql_template.format(limit=page_size, offset=offset)
        result = _exec_sql(ctx, sql, timeout=timeout)
        df_page = _to_df(result)
        if df_page.empty:
            break
        all_frames.append(df_page)
        fetched = len(df_page)
        print(f"    page offset={offset}: {fetched} rows")
        if fetched < page_size:
            break
        offset += page_size
    return pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()


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


def fetch_portfolio_weekly(n_weeks: int = 12) -> pd.DataFrame:
    """
    Fetch portfolio-level weekly aggregation (all brands summed).
    Returns only ~N rows — no pagination needed, used for the Overview tab.
    """
    start_date, end_date = get_last_n_full_weeks(n_weeks)
    ctx = _create_context()

    sql = f"""
    SELECT
        DATE_FORMAT(DATE_TRUNC('week', f.metric_timestamp_local), 'yyyy-MM-dd') AS week_start,
        SUM(f.delivered_orders_count)                          AS delivered_orders,
        SUM(f.failed_orders_count)                             AS failed_orders,
        SUM(f.total_gmv_before_discounts_eur)                  AS gmv_eur,
        SUM(f.total_contribution_profit_eur)                   AS contribution_profit_eur,
        ROUND(SUM(f.failed_order_rate_value   * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) * 100, 2) AS failed_order_rate_pct,
        ROUND(SUM(f.bad_order_rate_value      * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) * 100, 2) AS bad_order_rate_pct,
        ROUND(SUM(f.provider_acceptance_rate_value * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) * 100, 2) AS acceptance_rate_pct,
        ROUND(SUM(f.late_delivery_order_rate_value * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) * 100, 2) AS late_delivery_rate_pct,
        COUNT(DISTINCT p.provider_id)                          AS active_locations
    FROM ng_delivery_spark.dim_provider_v2 p
    INNER JOIN ng_delivery_spark.fact_provider_weekly f
        ON p.provider_id = f.provider_id
    WHERE
        p.account_manager_name = '{ACCOUNT_MANAGER}'
        AND p.country_code      = '{COUNTRY_CODE}'
        AND p.provider_status   = 'active'
        AND CAST(f.metric_timestamp_local AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY DATE_TRUNC('week', f.metric_timestamp_local)
    ORDER BY week_start
    """

    print(f"Running portfolio-weekly query ({n_weeks} weeks)...")
    result = _exec_sql(ctx, sql, timeout=120)
    df = _to_df(result)
    for col in ["delivered_orders", "failed_orders", "gmv_eur", "contribution_profit_eur",
                "failed_order_rate_pct", "bad_order_rate_pct",
                "acceptance_rate_pct", "late_delivery_rate_pct", "active_locations"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    print(f"Fetched {len(df):,} portfolio-week rows")
    return df


def fetch_weekly_trends(n_weeks: int = 12) -> pd.DataFrame:
    """Fetch week-by-week brand metrics for the last N full weeks (for trend charts)."""
    start_date, end_date = get_last_n_full_weeks(n_weeks)

    sql_tpl = f"""
    SELECT
        p.brand_name,
        p.city_name,
        MIN(p.group_name) AS group_name,
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
            / NULLIF(SUM(f.delivered_orders_count), 0) * 100, 2) AS late_delivery_rate_pct,
        ROUND(SUM(f.provider_active_rate_value * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) * 100, 2) AS availability_pct,
        -- Знижки та refunds
        ROUND(SUM(f.total_provider_campaign_spend_provider_eur), 2) AS partner_discount_eur,
        ROUND(SUM(f.total_provider_campaign_spend_bolt_eur), 2)     AS bolt_discount_eur,
        ROUND(SUM(f.total_invoiced_demand_refunds_eur), 2)          AS demand_refunds_eur,
        ROUND(SUM(f.total_invoiced_supply_refunds_eur), 2)          AS supply_refunds_eur,
        ROUND(SUM(COALESCE(f.total_invoiced_demand_refunds_eur,0)
                + COALESCE(f.total_invoiced_supply_refunds_eur,0)), 2) AS total_refunds_eur
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
    LIMIT {{limit}} OFFSET {{offset}}
    """

    print(f"Running weekly trends query ({n_weeks} weeks, paginated)...")
    df = _exec_sql_paginated(sql_tpl, timeout=300)
    print(f"Fetched {len(df):,} weekly trend rows total")
    return df


def fetch_location_trends(n_weeks: int = 12) -> pd.DataFrame:
    """
    Fetch week-by-week per-location (provider_id) data for the Dynamics tab.
    Includes conversion funnel: impressions → menu views → orders placed.
    """
    start_date, end_date = get_last_n_full_weeks(n_weeks)
    ctx = _create_context()

    sql = f"""
    SELECT
        p.provider_id,
        p.provider_name,
        p.brand_name,
        p.city_name,
        p.zone_name,
        DATE_FORMAT(DATE_TRUNC('week', f.metric_timestamp_local), 'yyyy-MM-dd') AS week_start,
        -- Orders & GMV
        SUM(f.delivered_orders_count)                         AS delivered_orders,
        SUM(f.failed_orders_count)                            AS failed_orders,
        SUM(f.total_gmv_before_discounts_eur)                 AS gmv_eur,
        -- Conversion funnel (sum of sessions per week)
        SUM(f.provider_impressions_sessions_count)            AS impressions_sessions,
        SUM(f.provider_menu_viewed_sessions_count)            AS menu_viewed_sessions,
        SUM(f.provider_order_placed_sessions_count)           AS order_placed_sessions,
        -- Conversion rates (calculated)
        ROUND(
            SUM(f.provider_order_placed_sessions_count) * 100.0
            / NULLIF(SUM(f.provider_impressions_sessions_count), 0), 2
        ) AS conversion_impression_to_order_pct,
        ROUND(
            SUM(f.provider_order_placed_sessions_count) * 100.0
            / NULLIF(SUM(f.provider_menu_viewed_sessions_count), 0), 2
        ) AS conversion_menu_to_order_pct,
        ROUND(
            SUM(f.provider_menu_viewed_sessions_count) * 100.0
            / NULLIF(SUM(f.provider_impressions_sessions_count), 0), 2
        ) AS conversion_impression_to_menu_pct,
        -- Quality metrics
        ROUND(SUM(f.bad_order_rate_value * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) * 100, 2) AS bad_order_rate_pct,
        ROUND(SUM(f.failed_order_rate_value * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) * 100, 2) AS failed_order_rate_pct,
        ROUND(SUM(f.provider_acceptance_rate_value * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) * 100, 2) AS acceptance_rate_pct,
        ROUND(SUM(f.provider_active_rate_value * f.delivered_orders_count)
            / NULLIF(SUM(f.delivered_orders_count), 0) * 100, 2) AS availability_pct,
        -- Знижки та refunds
        ROUND(SUM(f.total_provider_campaign_spend_provider_eur), 2) AS partner_discount_eur,
        ROUND(SUM(f.total_provider_campaign_spend_bolt_eur), 2)     AS bolt_discount_eur,
        ROUND(SUM(f.total_invoiced_demand_refunds_eur), 2)          AS demand_refunds_eur,
        ROUND(SUM(f.total_invoiced_supply_refunds_eur), 2)          AS supply_refunds_eur,
        ROUND(SUM(COALESCE(f.total_invoiced_demand_refunds_eur,0)
                + COALESCE(f.total_invoiced_supply_refunds_eur,0)), 2) AS total_refunds_eur
    FROM ng_delivery_spark.dim_provider_v2 p
    INNER JOIN ng_delivery_spark.fact_provider_weekly f
        ON p.provider_id = f.provider_id
    WHERE
        p.account_manager_name = '{ACCOUNT_MANAGER}'
        AND p.country_code      = '{COUNTRY_CODE}'
        AND p.provider_status   = 'active'
        AND CAST(f.metric_timestamp_local AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        p.provider_id, p.provider_name, p.brand_name, p.city_name, p.zone_name,
        DATE_TRUNC('week', f.metric_timestamp_local)
    ORDER BY p.brand_name, p.city_name, p.provider_id, week_start
    """

    print(f"Running per-location trends query ({n_weeks} weeks)...")
    result = _exec_sql(ctx, sql, timeout=360)
    df = _to_df(result)
    print(f"Fetched {len(df):,} location-week rows")
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
    width: 220px;
  }}
  .dyn-search:focus {{ border-color: var(--bolt-green); }}
  .dyn-select-wide {{ min-width: 280px; }}
  .dyn-brand-info {{
    margin-left: auto;
    font-size: 12px;
    color: var(--muted);
    background: var(--bolt-light-green);
    border-radius: 8px;
    padding: 6px 14px;
  }}
  .dyn-brand-info strong {{ color: var(--bolt-mid-green); }}

  .dyn-analysis {{
    background: #fff;
    border-radius: 12px;
    box-shadow: var(--shadow);
    padding: 16px 20px;
    margin-bottom: 20px;
    border-left: 4px solid var(--bolt-green);
  }}
  .dyn-analysis.warning {{ border-left-color: #FB8C00; }}
  .dyn-analysis.danger {{ border-left-color: #E53935; }}
  .dyn-analysis-title {{
    font-size: 13px;
    font-weight: 700;
    color: var(--bolt-dark);
    margin-bottom: 10px;
  }}
  .dyn-analysis-grid {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 14px;
    align-items: start;
  }}
  @media (max-width: 1250px) {{ .dyn-analysis-grid {{ grid-template-columns: 1fr 1fr; }} }}
  @media (max-width: 900px) {{ .dyn-analysis-grid {{ grid-template-columns: 1fr; }} }}
  .dyn-analysis-card {{
    background: #fafafa;
    border-radius: 10px;
    padding: 12px 14px;
  }}
  .dyn-analysis-card h5 {{
    font-size: 12px;
    font-weight: 700;
    margin-bottom: 6px;
    color: #333;
  }}
  .dyn-analysis-card .delta {{
    font-size: 18px;
    font-weight: 800;
    margin-bottom: 8px;
  }}
  .dyn-analysis-card .delta.down {{ color: #E53935; }}
  .dyn-analysis-card .delta.up {{ color: #2E7D32; }}
  .dyn-analysis-card .delta.flat {{ color: #666; }}
  .dyn-analysis-card .delta-sub {{
    font-size: 12px;
    font-weight: 600;
    color: #666;
    margin-left: 6px;
    white-space: nowrap;
  }}
  .dyn-analysis-text {{
    font-size: 12.5px;
    color: #333;
    line-height: 1.55;
    margin: 0 0 8px;
  }}
  .dyn-analysis-list-title {{
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .4px;
    color: var(--muted);
    margin-bottom: 5px;
  }}
  .dyn-analysis-card ul + .dyn-analysis-list-title {{
    margin-top: 10px;
    padding-top: 8px;
    border-top: 1px dashed #e2e2e2;
  }}
  .dyn-analysis-card ul {{
    margin: 0;
    padding-left: 18px;
    font-size: 12px;
    color: #444;
    line-height: 1.55;
  }}
  .dyn-analysis-card li {{ margin-bottom: 4px; }}
  .dyn-analysis-hint {{
    font-size: 11px;
    color: var(--muted);
    margin-top: 10px;
  }}

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
      <select class="dyn-select" id="dynCityFilter" onchange="onDynFiltersChange()">
        <option value="">— Всі міста —</option>
        {dyn_city_options}
      </select>
    </div>
    <div>
      <label>Бренд</label><br>
      <input class="dyn-search" id="dynBrandSearch" type="text" list="dynBrandDatalist"
             placeholder="🔍 Пошук бренду..." oninput="onDynFiltersChange()" autocomplete="off">
      <datalist id="dynBrandDatalist"></datalist>
    </div>
    <div>
      <label>Група</label><br>
      <input class="dyn-search" id="dynGroupSearch" type="text" list="dynGroupDatalist"
             placeholder="🔍 Пошук групи..." oninput="onDynFiltersChange()" autocomplete="off">
      <datalist id="dynGroupDatalist"></datalist>
    </div>
    <div>
      <label>Партнер</label><br>
      <select class="dyn-select dyn-select-wide" id="dynBrandSelect" onchange="renderCharts()">
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
    <!-- Auto analysis -->
    <div id="dynAnalysis" class="dyn-analysis" style="display:none"></div>

    <!-- Brand-level charts -->
    <div style="font-size:13px;font-weight:700;color:var(--bolt-dark);margin-bottom:12px">📊 Загальна динаміка бренду</div>
    <div class="charts-grid">
      <div class="chart-card"><h4>📦 Замовлення (доставлені)</h4><canvas id="chartOrders"></canvas></div>
      <div class="chart-card"><h4>💰 GMV (€, до знижок)</h4><canvas id="chartGmv"></canvas></div>
      <div class="chart-card"><h4>📈 Contribution Profit (€)</h4><canvas id="chartCp"></canvas></div>
      <div class="chart-card"><h4>📉 CP L2 Маржа (%)</h4><canvas id="chartCpMargin"></canvas></div>
      <div class="chart-card"><h4>❌ Failed Order Rate (%)</h4><canvas id="chartFailed"></canvas></div>
      <div class="chart-card"><h4>😞 Bad Order Rate (%)</h4><canvas id="chartBad"></canvas></div>
      <div class="chart-card"><h4>✅ Acceptance Rate (%)</h4><canvas id="chartAcceptance"></canvas></div>
      <div class="chart-card"><h4>⏰ Late Delivery Rate (%)</h4><canvas id="chartLate"></canvas></div>
      <div class="chart-card"><h4>🟢 Availability (%)</h4><canvas id="chartAvailability"></canvas></div>
    </div>

    <!-- Знижки та Refunds -->
    <div style="font-size:13px;font-weight:700;color:var(--bolt-dark);margin:20px 0 12px">💸 Знижки та Refunds</div>
    <div class="charts-grid">
      <div class="chart-card" style="border-top:3px solid #FB8C00">
        <h4 style="color:#E65100">🤝 Знижки партнера (€/тиж)</h4>
        <canvas id="chartPartnerDiscount"></canvas>
      </div>
      <div class="chart-card" style="border-top:3px solid #1DC462">
        <h4 style="color:#1A5E35">⚡ Знижки Bolt Food (€/тиж)</h4>
        <canvas id="chartBoltDiscount"></canvas>
      </div>
      <div class="chart-card" style="border-top:3px solid #E53935">
        <h4 style="color:#B71C1C">🔄 Refunds: покупці (€/тиж)</h4>
        <canvas id="chartDemandRefunds"></canvas>
      </div>
      <div class="chart-card" style="border-top:3px solid #7B1FA2">
        <h4 style="color:#4A148C">📦 Refunds: кур\u2019єри (€/тиж)</h4>
        <canvas id="chartSupplyRefunds"></canvas>
      </div>
    </div>

    <!-- Per-location conversion section -->
    <div id="locSection" style="display:none;margin-top:8px">
      <div style="font-size:13px;font-weight:700;color:var(--bolt-dark);margin-bottom:4px">
        📍 Конверсія по локаціях
      </div>
      <div style="font-size:12px;color:var(--muted);margin-bottom:14px">
        Воронка: покази → перегляди меню → замовлення — по кожній точці окремо
      </div>

      <!-- Location selector tabs -->
      <div id="locTabs" style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px"></div>

      <!-- Location detail -->
      <div id="locDetail">
        <!-- Funnel summary bar -->
        <div id="locFunnelBar" style="background:#fff;border-radius:12px;box-shadow:var(--shadow);
             padding:16px 20px;margin-bottom:16px"></div>

        <!-- Location charts grid -->
        <div class="charts-grid" id="locChartsGrid">
          <div class="chart-card"><h4>🎯 Конверсія: показ → замовлення (%)</h4><canvas id="locChartConvFull"></canvas></div>
          <div class="chart-card"><h4>📋 Конверсія: меню → замовлення (%)</h4><canvas id="locChartConvMenu"></canvas></div>
          <div class="chart-card"><h4>👁️ Конверсія: показ → меню (%)</h4><canvas id="locChartConvImp"></canvas></div>
          <div class="chart-card"><h4>📦 Замовлення (доставлені)</h4><canvas id="locChartOrders"></canvas></div>
          <div class="chart-card"><h4>📱 Покази (сесії)</h4><canvas id="locChartImpressions"></canvas></div>
          <div class="chart-card"><h4>🍽️ Перегляди меню</h4><canvas id="locChartMenuViews"></canvas></div>
          <div class="chart-card"><h4>✅ Acceptance Rate (%)</h4><canvas id="locChartAcceptance"></canvas></div>
          <div class="chart-card"><h4>🟢 Availability (%)</h4><canvas id="locChartAvailability"></canvas></div>
        </div>

        <!-- Знижки та refunds по локації -->
        <div style="font-size:12px;font-weight:700;color:var(--bolt-dark);margin:16px 0 10px">💸 Знижки та Refunds — ця локація</div>
        <div class="charts-grid">
          <div class="chart-card" style="border-top:3px solid #FB8C00">
            <h4 style="color:#E65100">🤝 Знижки партнера (€/тиж)</h4><canvas id="locChartPartnerDiscount"></canvas>
          </div>
          <div class="chart-card" style="border-top:3px solid #1DC462">
            <h4 style="color:#1A5E35">⚡ Знижки Bolt Food (€/тиж)</h4><canvas id="locChartBoltDiscount"></canvas>
          </div>
          <div class="chart-card" style="border-top:3px solid #E53935">
            <h4 style="color:#B71C1C">🔄 Refunds: покупці (€/тиж)</h4><canvas id="locChartDemandRefunds"></canvas>
          </div>
          <div class="chart-card" style="border-top:3px solid #7B1FA2">
            <h4 style="color:#4A148C">📦 Refunds: кур\u2019єри (€/тиж)</h4><canvas id="locChartSupplyRefunds"></canvas>
          </div>
        </div>
      </div>
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
  onDynFiltersChange();
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

// ── Dynamics: filters + datalists ─────────────────────────────────────────────
function getDynEntries() {{
  return Object.keys(TRENDS).map(k => {{
    const d = TRENDS[k];
    return {{
      key: k,
      brand: d.brand || k.split('|||')[0] || '',
      city:  d.city  || k.split('|||')[1] || '',
      group: d.group_name || '',
    }};
  }});
}}

function fillDatalist(datalistId, values) {{
  const el = document.getElementById(datalistId);
  el.innerHTML = '';
  values.forEach(v => {{
    const opt = document.createElement('option');
    opt.value = v;
    el.appendChild(opt);
  }});
}}

function refreshDynDatalists() {{
  const cityFilter = document.getElementById('dynCityFilter').value;
  const brandQ = document.getElementById('dynBrandSearch').value.trim().toLowerCase();
  const groupQ = document.getElementById('dynGroupSearch').value.trim().toLowerCase();
  const entries = getDynEntries().filter(e => !cityFilter || e.city === cityFilter);

  const brands = [...new Set(
    entries
      .filter(e => !groupQ || e.group.toLowerCase().includes(groupQ))
      .map(e => e.brand)
      .filter(Boolean)
  )].sort((a, b) => a.localeCompare(b, 'uk'));

  const groups = [...new Set(
    entries
      .filter(e => !brandQ || e.brand.toLowerCase().includes(brandQ))
      .map(e => e.group)
      .filter(Boolean)
  )].sort((a, b) => a.localeCompare(b, 'uk'));

  fillDatalist('dynBrandDatalist', brands);
  fillDatalist('dynGroupDatalist', groups);
}}

function onDynFiltersChange() {{
  refreshDynDatalists();
  updateBrandList();
}}

function updateBrandList() {{
  const cityFilter = document.getElementById('dynCityFilter').value;
  const brandQ     = document.getElementById('dynBrandSearch').value.trim().toLowerCase();
  const groupQ     = document.getElementById('dynGroupSearch').value.trim().toLowerCase();
  const select     = document.getElementById('dynBrandSelect');
  const prevVal    = select.value;

  const keys = getDynEntries().filter(e => {{
    const cityOk  = !cityFilter || e.city === cityFilter;
    const brandOk = !brandQ || e.brand.toLowerCase().includes(brandQ);
    const groupOk = !groupQ || e.group.toLowerCase().includes(groupQ);
    return cityOk && brandOk && groupOk;
  }}).map(e => e.key);

  keys.sort((a, b) => {{
    const ba = (TRENDS[a].brand || a.split('|||')[0] || '');
    const bb = (TRENDS[b].brand || b.split('|||')[0] || '');
    return ba.localeCompare(bb, 'uk');
  }});

  select.innerHTML = '<option value="">— Оберіть бренд —</option>';
  keys.forEach(k => {{
    const d = TRENDS[k];
    const brand = d.brand || k.split('|||')[0];
    const city  = d.city  || k.split('|||')[1];
    const group = d.group_name || '';
    const opt = document.createElement('option');
    opt.value = k;
    opt.textContent = group && group !== brand
      ? `${{brand}} · ${{group}} (${{city}})`
      : `${{brand}} (${{city}})`;
    select.appendChild(opt);
  }});

  if (prevVal && keys.includes(prevVal)) {{
    select.value = prevVal;
  }} else if (keys.length === 1) {{
    select.value = keys[0];
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

function seriesLastTwo(arr) {{
  if (!arr || arr.length < 2) return [null, null];
  let last = null, prev = null;
  for (let i = arr.length - 1; i >= 0; i--) {{
    if (arr[i] == null || Number.isNaN(arr[i])) continue;
    if (last == null) last = arr[i];
    else {{ prev = arr[i]; break; }}
  }}
  return [prev, last];
}}

function pctChange(prev, last) {{
  if (prev == null || last == null || prev === 0) return null;
  return (last / prev - 1) * 100;
}}

function ppChange(prev, last) {{
  if (prev == null || last == null) return null;
  return last - prev;
}}

function fmtDeltaPct(v) {{
  if (v == null) return '—';
  const r = Math.abs(v) < 0.05 ? 0 : v;
  const sign = r > 0 ? '+' : '';
  return sign + r.toFixed(1) + '%';
}}

function fmtDeltaPp(v) {{
  if (v == null) return '—';
  const r = Math.abs(v) < 0.05 ? 0 : v;
  const sign = r > 0 ? '+' : '';
  return sign + r.toFixed(1) + ' п.п.';
}}

function fmtEurDelta(v) {{
  if (v == null) return '—';
  const r = Math.abs(v) < 0.5 ? 0 : v;
  const sign = r > 0 ? '+' : r < 0 ? '−' : '';
  return sign + '€' + Math.abs(r).toLocaleString('uk-UA', {{ maximumFractionDigits: 0 }});
}}

function fmtEurVal(v) {{
  if (v == null) return '—';
  const abs = Math.abs(v).toLocaleString('uk-UA', {{ maximumFractionDigits: 0 }});
  return (v < 0 ? '−€' : '€') + abs;
}}

function pluralUa(n, one, few, many) {{
  const mod10 = n % 10, mod100 = n % 100;
  if (mod10 === 1 && mod100 !== 11) return `${{n}} ${{one}}`;
  if (mod10 >= 2 && mod10 <= 4 && (mod100 < 12 || mod100 > 14)) return `${{n}} ${{few}}`;
  return `${{n}} ${{many}}`;
}}

function shareOfGmv(v, gmv) {{
  if (v == null || gmv == null || gmv <= 0) return null;
  return v / gmv * 100;
}}

// ── Ринковий контекст: сума по інших брендах міста за той самий тиждень ──────
// Дає змогу відрізнити зовнішній фактор (просів увесь ринок) від проблеми
// конкретного партнера (ринок стабільний, а він падає).
let MARKET_CACHE = null;
function marketByCity() {{
  if (MARKET_CACHE) return MARKET_CACHE;
  const byCity = {{ __all__: {{}} }};
  Object.values(TRENDS).forEach(b => {{
    const city = b.city || '—';
    const cityRec = byCity[city] || (byCity[city] = {{}});
    (b.weeks || []).forEach((w, i) => {{
      const o = (b.delivered_orders || [])[i];
      const g = (b.gmv_eur || [])[i];
      [cityRec, byCity.__all__].forEach(rec => {{
        const wk = rec[w] || (rec[w] = {{ orders: 0, gmv: 0, brands: 0 }});
        if (o != null) wk.orders += o;
        if (g != null) wk.gmv += g;
        wk.brands += 1;
      }});
    }});
  }});
  MARKET_CACHE = byCity;
  return MARKET_CACHE;
}}

// ── Сезонний і календарний контекст для тижня ────────────────────────────────
const UA_HOLIDAYS = [
  [1, 1, 'Новий рік'], [3, 8, 'Міжнародний жіночий день'], [5, 1, 'День праці'],
  [6, 28, 'День Конституції'], [8, 24, 'День Незалежності'],
  [10, 1, 'День захисників і захисниць'], [12, 25, 'Різдво'],
];

function seasonalNotes(weekStart) {{
  if (!weekStart) return [];
  const start = new Date(weekStart + 'T00:00:00');
  const m = start.getMonth() + 1;
  const day = start.getDate();
  const notes = [];

  if (m >= 6 && m <= 8) {{
    notes.push('<b>Сезон відпусток і спека:</b> частина постійних клієнтів виїхала або проводить час поза домом, а в сильну спеку попит на гарячу їжу традиційно нижчий. Це вплив на весь ринок, не лише на цього партнера.');
    if (m === 8 && day >= 15)
      notes.push('<b>Кінець серпня:</b> родини готуються до навчального року — витрати перерозподіляються з доставки на інші категорії.');
  }} else if (m === 9) {{
    notes.push('<b>Вересень:</b> повернення з відпусток і старт навчального року зазвичай відновлюють попит. Якщо цього не сталося — причина радше внутрішня, а не сезонна.');
  }} else if (m === 10 || m === 11) {{
    notes.push('<b>Осінь:</b> холод і дощі зазвичай піднімають доставку. Падіння в цей період важче списати на сезон — шукайте причину в метриках закладу.');
  }} else if (m === 12) {{
    notes.push('<b>Грудень:</b> передсвятковий період і корпоративи зазвичай піднімають попит, але в самі свята він зміщується на домашній стіл.');
  }} else if (m === 1) {{
    notes.push('<b>Січень:</b> після свят попит традиційно просідає — люди економлять після святкових витрат.');
  }} else if (m >= 3 && m <= 5) {{
    notes.push('<b>Весна:</b> тепла погода, травневі вихідні й виїзди на природу переводять частину попиту з доставки в офлайн-заклади.');
  }}

  const end = new Date(start.getTime() + 6 * 86400000);
  UA_HOLIDAYS.forEach(([hm, hd, name]) => {{
    for (let t = new Date(start); t <= end; t = new Date(t.getTime() + 86400000)) {{
      if (t.getMonth() + 1 === hm && t.getDate() === hd) {{
        notes.push(`<b>Святковий тиждень (${{name}}):</b> у свята попит зміщується — частина клієнтів святкує вдома або в закладах, режим роботи кухні теж міг відрізнятися.`);
        break;
      }}
    }}
  }});

  notes.push('<b>Погода:</b> сонячні теплі вихідні зменшують доставку, дощ і різке похолодання — збільшують. Звірте тиждень із фактичною погодою в місті.');
  return notes;
}}

function analyzeBrandDynamics(d) {{
  const [oPrev, oLast]   = seriesLastTwo(d.delivered_orders);
  const [gPrev, gLast]   = seriesLastTwo(d.gmv_eur);
  const [cpPrev, cpLast] = seriesLastTwo(d.contribution_profit_eur);
  const [mPrev, mLast]   = seriesLastTwo(d.cp_l2_margin_pct);
  const [avPrev, avLast] = seriesLastTwo(d.availability_pct);
  const [acPrev, acLast] = seriesLastTwo(d.acceptance_rate_pct);
  const [flPrev, flLast] = seriesLastTwo(d.failed_order_rate_pct);
  const [bdPrev, bdLast] = seriesLastTwo(d.bad_order_rate_pct);
  const [ltPrev, ltLast] = seriesLastTwo(d.late_delivery_rate_pct);
  const [pdPrev, pdLast] = seriesLastTwo(d.partner_discount_eur);
  const [btPrev, btLast] = seriesLastTwo(d.bolt_discount_eur);
  const [drPrev, drLast] = seriesLastTwo(d.demand_refunds_eur);
  const [srPrev, srLast] = seriesLastTwo(d.supply_refunds_eur);
  const [rfPrev, rfLast] = seriesLastTwo(d.total_refunds_eur);

  const ordersWow = pctChange(oPrev, oLast);
  const gmvWow    = pctChange(gPrev, gLast);
  const cpWow     = pctChange(cpPrev, cpLast);
  const marginPp  = ppChange(mPrev, mLast);
  const ordDiff   = (oPrev != null && oLast != null) ? oLast - oPrev : null;
  const gmvDiff   = (gPrev != null && gLast != null) ? gLast - gPrev : null;
  const cpDiff    = (cpPrev != null && cpLast != null) ? cpLast - cpPrev : null;

  const aovPrev = (gPrev != null && oPrev) ? gPrev / oPrev : null;
  const aovLast = (gLast != null && oLast) ? gLast / oLast : null;
  const aovWow  = pctChange(aovPrev, aovLast);

  // Малі обсяги дають шумні відсотки — про це варто попереджати окремо
  const lowVolume = oLast != null && oPrev != null && Math.max(oPrev, oLast) < 20;

  // ── Сигнали по локаціях: покази, конверсія, найбільше падіння замовлень ──
  let impPrev = 0, impLast = 0, convPrevSum = 0, convLastSum = 0, convN = 0;
  const locDrops = [];
  const locs = d.locations || {{}};
  Object.values(locs).forEach(loc => {{
    const [ip, il]   = seriesLastTwo(loc.impressions);
    const [cvp, cvl] = seriesLastTwo(loc.conv_imp_to_order);
    const [lop, lol] = seriesLastTwo(loc.delivered_orders);
    if (ip != null) impPrev += ip;
    if (il != null) impLast += il;
    if (cvp != null && cvl != null) {{ convPrevSum += cvp; convLastSum += cvl; convN += 1; }}
    if (lop != null && lol != null && lol < lop) {{
      locDrops.push({{ name: loc.name, diff: lol - lop, pct: pctChange(lop, lol) }});
    }}
  }});
  locDrops.sort((a, b) => a.diff - b.diff);
  const impWow = (impPrev > 0 && impLast > 0) ? (impLast / impPrev - 1) * 100 : null;
  const convPp = convN ? (convLastSum / convN) - (convPrevSum / convN) : null;

  // ── Ринок міста за ті самі два тижні, без урахування самого партнера ──────
  const weeks = d.weeks || [];
  const wLast = weeks[weeks.length - 1], wPrev = weeks[weeks.length - 2];
  let mktOrdersWow = null, mktGmvWow = null, mktLabel = '', mktBrands = 0, mktIsCity = false;
  if (wLast && wPrev) {{
    const all = marketByCity();
    const scopes = [['city', all[d.city]], ['portfolio', all.__all__]];
    for (const [scope, rec] of scopes) {{
      if (!rec || !rec[wPrev] || !rec[wLast]) continue;
      const oP = rec[wPrev].orders - (oPrev || 0), oL = rec[wLast].orders - (oLast || 0);
      const gP = rec[wPrev].gmv - (gPrev || 0),    gL = rec[wLast].gmv - (gLast || 0);
      const n = rec[wLast].brands - 1;
      const enough = scope === 'city' ? (n >= 2 && oP >= 30) : (n >= 3 && oP >= 50);
      if (!enough) continue;
      mktOrdersWow = pctChange(oP, oL);
      mktGmvWow    = pctChange(gP, gL);
      mktBrands    = n;
      mktIsCity    = scope === 'city';
      mktLabel     = mktIsCity ? `по решті ринку в ${{d.city}}` : 'по решті портфеля (інші міста теж)';
      break;
    }}
  }}

  const hasOrdersDrop = ordersWow != null && ordersWow < -2;
  const hasGmvDrop    = gmvWow != null && gmvWow < -2;
  const cpMaterial    = Math.max(2, Math.abs(cpPrev != null ? cpPrev : 0) * 0.03);
  const cpPctShown    = (cpPrev != null && cpLast != null && cpPrev > 0) ? cpWow : null;
  const hasCpDrop     = cpDiff != null && cpDiff < -cpMaterial;
  const hasCpGrowth   = cpDiff != null && cpDiff > cpMaterial;
  const hasMarginDrop = marginPp != null && marginPp < -0.5;

  // ══ 1. ЧОМУ ВПАЛИ ЗАМОВЛЕННЯ ТА GMV ════════════════════════════════════════
  const orderReasons = [];
  if (avPrev != null && avLast != null && (avLast - avPrev) <= -1)
    orderReasons.push(`<b>Availability</b> ${{avPrev.toFixed(1)}}% → ${{avLast.toFixed(1)}}% (${{fmtDeltaPp(avLast - avPrev)}}): заклад менше часу був онлайн, частина попиту фізично не могла оформити замовлення.`);
  if (acPrev != null && acLast != null && (acLast - acPrev) <= -2)
    orderReasons.push(`<b>Acceptance Rate</b> ${{acPrev.toFixed(1)}}% → ${{acLast.toFixed(1)}}% (${{fmtDeltaPp(acLast - acPrev)}}): частіші відмови від замовлень — оформлені замовлення не доходять до доставлених.`);
  if (flPrev != null && flLast != null && (flLast - flPrev) >= 0.5)
    orderReasons.push(`<b>Failed Rate</b> ${{flPrev.toFixed(1)}}% → ${{flLast.toFixed(1)}}% (${{fmtDeltaPp(flLast - flPrev)}}): більше зірваних замовлень, які не потрапляють у доставлені.`);
  if (impWow != null && impWow < -5)
    orderReasons.push(`<b>Покази</b> ${{fmtDeltaPct(impWow)}} (${{Math.round(impPrev).toLocaleString('uk-UA')}} → ${{Math.round(impLast).toLocaleString('uk-UA')}}): менше видимості в застосунку — менший вхідний трафік. Причини: ранжування, конкуренція в зоні, згорнуті кампанії.`);
  if (convPp != null && convPp <= -0.2)
    orderReasons.push(`<b>Конверсія показ→замовлення</b> ${{fmtDeltaPp(convPp)}}: трафік є, але гірше конвертується — ціни, довгий ETA, вартість доставки для клієнта, вигляд меню чи фото.`);
  if (hasOrdersDrop && impWow != null && impWow > 2 && convPp != null && convPp <= -0.2)
    orderReasons.push(`Показів навіть більше, ніж тижнем раніше — тобто проблема не в трафіку, а саме в конверсії.`);
  if (ltPrev != null && ltLast != null && (ltLast - ltPrev) >= 2)
    orderReasons.push(`<b>Late Delivery</b> ${{fmtDeltaPp(ltLast - ltPrev)}}: затримки псують досвід і знижують повторні замовлення та позицію в списку.`);
  if (bdPrev != null && bdLast != null && (bdLast - bdPrev) >= 1.5)
    orderReasons.push(`<b>Bad Order Rate</b> ${{fmtDeltaPp(bdLast - bdPrev)}}: більше проблемних замовлень — падає повторний попит.`);
  if (hasOrdersDrop && aovWow != null && aovWow > 5)
    orderReasons.push(`<b>Середній чек</b> зріс на ${{aovWow.toFixed(1)}}% (${{fmtEurVal(aovPrev)}} → ${{fmtEurVal(aovLast)}}): можливе підняття цін у меню відлякує частину чутливих до ціни клієнтів.`);
  if (hasOrdersDrop && locDrops.length)
    orderReasons.push(`<b>Де саме падіння:</b> ${{locDrops.slice(0, 3).map(l => `${{l.name}} (${{Math.round(l.diff)}} зам., ${{fmtDeltaPct(l.pct)}})`).join('; ')}}${{locDrops.length > 3 ? ` та ще ${{pluralUa(locDrops.length - 3, 'локація', 'локації', 'локацій')}}` : ''}}.`);

  // GMV = замовлення × середній чек, тож падіння розкладається на два ефекти
  const ordEffect = (ordDiff != null && aovPrev != null) ? ordDiff * aovPrev : null;
  const aovEffect = (oLast != null && aovPrev != null && aovLast != null) ? oLast * (aovLast - aovPrev) : null;
  const gmvReasons = [];
  if (hasGmvDrop && ordEffect != null && aovEffect != null) {{
    if (ordEffect < 0)
      gmvReasons.push(`<b>Менше замовлень: ${{fmtEurDelta(ordEffect)}}</b> — ${{Math.round(oPrev)}} → ${{Math.round(oLast)}} (${{fmtDeltaPct(ordersWow)}}) при старому середньому чеку ${{fmtEurVal(aovPrev)}}.`);
    if (aovEffect < 0)
      gmvReasons.push(`<b>Нижчий середній чек: ${{fmtEurDelta(aovEffect)}}</b> — ${{fmtEurVal(aovPrev)}} → ${{fmtEurVal(aovLast)}} (${{fmtDeltaPct(aovWow)}}). Клієнти беруть менше позицій у кошик (менше напоїв, десертів, додатків), або змінився mix страв чи ціни в меню.`);
    if (ordEffect < 0 && aovEffect < 0) {{
      const share = Math.abs(ordEffect) / ((Math.abs(ordEffect) + Math.abs(aovEffect)) || 1) * 100;
      gmvReasons.push(`Головний драйвер GMV — ${{share >= 55 ? `<b>кількість замовлень</b> (~${{Math.round(share)}}%)` : share <= 45 ? `<b>середній чек</b> (~${{Math.round(100 - share)}}%)` : '<b>обидва фактори порівну</b>'}}.`);
    }}
    if (aovEffect < 0 && ordEffect >= 0)
      gmvReasons.push('Замовлень не стало менше — GMV просів виключно через нижчий середній чек.');
    if (ordEffect < 0 && aovEffect >= 0)
      gmvReasons.push(`Середній чек не падав, навпаки — вищий чек компенсував ${{fmtEurDelta(aovEffect)}}, інакше GMV просів би сильніше. Тобто причина падіння GMV — виключно кількість замовлень.`);
  }}

  // Зовнішній контекст: ринок міста + сезон, погода, тривоги
  const externalReasons = [];
  if (mktOrdersWow != null) {{
    if (hasOrdersDrop || hasGmvDrop) {{
      if (mktOrdersWow <= -3) {{
        const harder = ordersWow != null && ordersWow < mktOrdersWow - 3;
        externalReasons.push(`<b>Ринок теж просів:</b> замовлення ${{mktLabel}} ${{fmtDeltaPct(mktOrdersWow)}}, GMV ${{fmtDeltaPct(mktGmvWow)}} (${{pluralUa(mktBrands, 'бренд', 'бренди', 'брендів')}}). Тобто діє зовнішній фактор, спільний для всіх — погода, тривоги, відпустки. ${{harder ? `Але партнер впав сильніше за ринок (${{fmtDeltaPct(ordersWow)}}), тож частина падіння все одно його власна.` : 'Падіння партнера в межах загальної тенденції.'}}`);
      }} else if (mktOrdersWow >= 2) {{
        externalReasons.push(`<b>Ринок зростав:</b> замовлення ${{mktLabel}} ${{fmtDeltaPct(mktOrdersWow)}} (${{pluralUa(mktBrands, 'бренд', 'бренди', 'брендів')}}), а партнер ${{fmtDeltaPct(ordersWow)}}. ${{mktIsCity ? 'Спільні для міста фактори (погода, тривоги, відпустки) падіння не пояснюють — причина на боці закладу, дивіться метрики вище.' : 'Порівняння тут по інших містах, тож суто локальну причину (тривоги чи погода саме в цьому місті) виключати не можна, але перевірити спершу варто метрики закладу.'}}`);
      }} else {{
        externalReasons.push(`<b>Ринок стабільний:</b> замовлення ${{mktLabel}} ${{fmtDeltaPct(mktOrdersWow)}} (${{pluralUa(mktBrands, 'бренд', 'бренди', 'брендів')}}). Падіння переважно специфічне для партнера${{mktIsCity ? '' : ', хоча бенчмарк тут по інших містах'}}.`);
      }}
    }} else {{
      externalReasons.push(`<b>Для контексту:</b> ${{mktLabel}} замовлення ${{fmtDeltaPct(mktOrdersWow)}}, GMV ${{fmtDeltaPct(mktGmvWow)}} за цей же тиждень.`);
    }}
  }}
  if (hasOrdersDrop || hasGmvDrop) {{
    const alertHint = (avPrev != null && avLast != null && (avLast - avPrev) <= -1)
      ? ' Availability цього тижня теж просіла — це вагома підказка саме на користь такої причини.'
      : ' Якщо тривоги були тривалі, перевірте погодинну динаміку: провал у вечірній пік найпомітніший.';
    externalReasons.push(`<b>Повітряні тривоги та відключення світла:</b> довгі тривоги і блекаути зупиняють кухню й доставку на години.${{alertHint}}`);
    seasonalNotes(wLast).forEach(n => externalReasons.push(n));
  }}

  let ordersTitle = 'Чому впали замовлення та GMV';
  let ordersText;
  if (oPrev == null || oLast == null) {{
    ordersTitle = 'Замовлення та GMV';
    ordersText  = 'Недостатньо даних для порівняння двох тижнів.';
  }} else if (hasOrdersDrop || hasGmvDrop) {{
    const parts = [];
    if (hasOrdersDrop) parts.push(`замовлення впали з ${{Math.round(oPrev)}} до ${{Math.round(oLast)}} (${{fmtDeltaPct(ordersWow)}}, ${{Math.round(ordDiff)}} шт)`);
    else parts.push(`замовлення майже без змін (${{fmtDeltaPct(ordersWow)}})`);
    if (gmvWow != null) parts.push(`GMV ${{hasGmvDrop ? 'просів' : 'змінився'}} з ${{fmtEurVal(gPrev)}} до ${{fmtEurVal(gLast)}} (${{fmtDeltaPct(gmvWow)}}, ${{fmtEurDelta(gmvDiff)}})`);
    ordersText = `За тиждень ${{parts.join('; ')}}.`;
    if (!orderReasons.length)
      orderReasons.push('Операційні метрики (availability, acceptance, failed, покази, конверсія) не погіршились — падіння радше зовнішнє або пов’язане зі структурою кошика.');
  }} else {{
    ordersTitle = ordersWow != null && ordersWow > 2 ? 'Замовлення та GMV: падіння немає' : 'Замовлення та GMV: без змін';
    ordersText  = ordersWow != null && ordersWow > 2
      ? `Замовлення зросли з ${{Math.round(oPrev)}} до ${{Math.round(oLast)}} (${{fmtDeltaPct(ordersWow)}}), GMV ${{fmtDeltaPct(gmvWow)}} — пояснювати падіння немає чого.`
      : `Замовлення стабільні: ${{Math.round(oPrev)}} → ${{Math.round(oLast)}} (${{fmtDeltaPct(ordersWow)}}), GMV ${{fmtDeltaPct(gmvWow)}}.`;
    if (!orderReasons.length) orderReasons.push('Ризиків у попиті та операційній якості не видно.');
  }}
  if (lowVolume) orderReasons.push('Обсяги малі (менше 20 замовлень на тиждень) — відсоткові зміни тут дуже чутливі до кількох замовлень.');

  const ordersListTitle = (hasOrdersDrop || hasGmvDrop)
    ? 'Метрики закладу:'
    : (orderReasons.length && !/Ризиків у попиті/.test(orderReasons[0]) ? 'Метрики під наглядом:' : '');

  // ══ 2. ЧОМУ ПРОСІЛА МАРЖА (CONTRIBUTION PROFIT, €) ═════════════════════════
  // CP = GMV × маржа%, тож ΔCP ≈ ΔGMV × маржа_попередня + GMV_останній × Δмаржа
  const volEffect  = (gPrev != null && gLast != null && mPrev != null) ? (gLast - gPrev) * (mPrev / 100) : null;
  const rateEffect = (gLast != null && mPrev != null && mLast != null) ? gLast * ((mLast - mPrev) / 100) : null;

  const cpReasons = [];
  if (hasCpDrop && volEffect != null && rateEffect != null) {{
    const volShare = Math.abs(volEffect) / ((Math.abs(volEffect) + Math.abs(rateEffect)) || 1) * 100;
    if (volEffect < 0) {{
      const aovDown = aovWow != null && aovWow < -3;
      const src = hasOrdersDrop && aovDown
        ? `менше замовлень (${{fmtDeltaPct(ordersWow)}}) і нижчий середній чек (${{fmtDeltaPct(aovWow)}})`
        : hasOrdersDrop
          ? `менше замовлень (${{fmtDeltaPct(ordersWow)}}) — причини в блоці «Замовлення та GMV»`
          : aovDown
            ? `нижчий середній чек (${{fmtEurVal(aovPrev)}} → ${{fmtEurVal(aovLast)}}, ${{fmtDeltaPct(aovWow)}}) при майже незмінній кількості замовлень`
            : 'менший оборот';
      cpReasons.push(`<b>Ефект обсягу: ${{fmtEurDelta(volEffect)}}</b> — GMV ${{fmtDeltaPct(gmvWow)}} (${{fmtEurVal(gPrev)}} → ${{fmtEurVal(gLast)}}) через ${{src}}. Навіть при незмінній рентабельності це забирає таку суму CP.`);
    }}
    if (rateEffect < 0)
      cpReasons.push(`<b>Ефект рентабельності: ${{fmtEurDelta(rateEffect)}}</b> — CP L2 маржа ${{fmtDeltaPp(marginPp)}}, тобто з кожного євро обороту лишається менше прибутку. Причини — у блоці «CP L2 Маржа».`);
    if (volEffect < 0 && rateEffect < 0)
      cpReasons.push(`Головний драйвер — ${{volShare >= 55 ? `<b>обсяг</b> (~${{Math.round(volShare)}}% падіння CP)` : volShare <= 45 ? `<b>рентабельність</b> (~${{Math.round(100 - volShare)}}% падіння CP)` : '<b>обидва фактори приблизно порівну</b>'}}.`);
    if (volEffect < 0 && rateEffect >= 0)
      cpReasons.push(`Рентабельність не погіршилась (${{fmtDeltaPp(marginPp)}}) — CP просів виключно через менший оборот.`);
    if (rateEffect < 0 && volEffect >= 0)
      cpReasons.push(`Оборот не падав (GMV ${{fmtDeltaPct(gmvWow)}}) — CP просів виключно через нижчу рентабельність.`);

    const pdD = (pdPrev != null && pdLast != null) ? pdLast - pdPrev : null;
    const btD = (btPrev != null && btLast != null) ? btLast - btPrev : null;
    const rfD = (rfPrev != null && rfLast != null) ? rfLast - rfPrev : null;
    const costBits = [];
    if (pdD != null && pdD >= 5) costBits.push(`знижки партнера ${{fmtEurDelta(pdD)}}`);
    if (btD != null && btD >= 5) costBits.push(`знижки Bolt ${{fmtEurDelta(btD)}}`);
    if (rfD != null && rfD >= 3) costBits.push(`refunds ${{fmtEurDelta(rfD)}}`);
    if (costBits.length)
      cpReasons.push(`<b>Витрати, що зросли в грошах:</b> ${{costBits.join(', ')}} — це прямий мінус до CP.`);
  }}

  let cpTitle = 'Чому просіла маржа (CP, €)';
  let cpList  = 'Розклад падіння:';
  let cpText;
  if (cpPrev == null || cpLast == null) {{
    cpTitle = 'Маржа (CP, €)';
    cpList  = '';
    cpText  = 'Недостатньо даних для порівняння двох тижнів.';
  }} else if (hasCpDrop) {{
    cpText = `Contribution Profit просів з ${{fmtEurVal(cpPrev)}} до ${{fmtEurVal(cpLast)}} (${{fmtEurDelta(cpDiff)}}${{cpPctShown != null ? `, ${{fmtDeltaPct(cpPctShown)}}` : ''}}). Падіння розкладається на два ефекти — обсяг і рентабельність.`;
    if (!cpReasons.length) {{
      cpList = '';
      cpReasons.push('Ані GMV, ані маржа% суттєво не змінились — імовірні разові списання чи коригування в цьому тижні.');
    }}
  }} else {{
    cpTitle = hasCpGrowth ? 'Маржа (CP, €): падіння немає' : 'Маржа (CP, €): без змін';
    cpText  = hasCpGrowth
      ? `CP зріс з ${{fmtEurVal(cpPrev)}} до ${{fmtEurVal(cpLast)}} (${{fmtEurDelta(cpDiff)}}${{cpPctShown != null ? `, ${{fmtDeltaPct(cpPctShown)}}` : ''}}) — пояснювати падіння немає чого.`
      : `CP стабільний: ${{fmtEurVal(cpPrev)}} → ${{fmtEurVal(cpLast)}} (${{fmtEurDelta(cpDiff)}}).`;
    cpList = '';
    cpReasons.length = 0;
    cpReasons.push(`Обсяг (GMV ${{gmvWow != null ? fmtDeltaPct(gmvWow) : '—'}}) і рентабельність (${{marginPp != null ? fmtDeltaPp(marginPp) : '—'}}) або працюють на плюс, або компенсують один одного.`);
  }}
  if (cpLast != null && cpLast < 0)
    cpReasons.push(`<b>CP негативний (${{fmtEurVal(cpLast)}})</b> — бренд приносить збиток, потрібен перегляд комерційних умов.`);
  if (lowVolume) cpReasons.push('Обсяги малі — суми в євро невеликі, тож одне-два замовлення сильно змінюють картину.');

  // ══ 3. ЧОМУ ПРОСІЛА CP L2 МАРЖА (%) ════════════════════════════════════════
  // Маржа% = CP / GMV, тож залежить від структури витрат на євро обороту,
  // а не від самого обсягу замовлень.
  const pdShPrev = shareOfGmv(pdPrev, gPrev), pdShLast = shareOfGmv(pdLast, gLast);
  const btShPrev = shareOfGmv(btPrev, gPrev), btShLast = shareOfGmv(btLast, gLast);
  const drShPrev = shareOfGmv(drPrev, gPrev), drShLast = shareOfGmv(drLast, gLast);
  const srShPrev = shareOfGmv(srPrev, gPrev), srShLast = shareOfGmv(srLast, gLast);

  const mReasons = [];
  let explainedPp = 0;
  const addShare = (label, shPrev, shLast, hint) => {{
    if (shPrev == null || shLast == null) return;
    const dpp = shLast - shPrev;
    if (dpp < 0.2) return;
    explainedPp += dpp;
    mReasons.push(`<b>${{label}}</b> ${{shPrev.toFixed(1)}}% → ${{shLast.toFixed(1)}}% від GMV (${{fmtDeltaPp(dpp)}}): забирає приблизно стільки ж п.п. маржі. ${{hint}}`);
  }};
  addShare('Знижки партнера', pdShPrev, pdShLast, 'Кампанія стала дорожчою або охопила більшу частку чеків.');
  addShare('Знижки Bolt', btShPrev, btShLast, 'Вища частка промо-замовлень у міксі.');
  addShare('Refunds покупцям', drShPrev, drShLast, 'Компенсації за помилки в замовленнях і скарги.');
  addShare('Refunds курʼєрам', srShPrev, srShLast, 'Компенсації за очікування, скасування та довгі маршрути.');

  if (aovWow != null && aovWow < -3)
    mReasons.push(`<b>Середній чек</b> ${{fmtEurVal(aovPrev)}} → ${{fmtEurVal(aovLast)}} (${{fmtDeltaPct(aovWow)}}): фіксовані витрати на замовлення (доставка, платіжна комісія) майже не залежать від суми чека, тож на меншому чеку зʼїдають більший % маржі.`);
  if (bdPrev != null && bdLast != null && (bdLast - bdPrev) >= 1)
    mReasons.push(`<b>Bad Order Rate</b> ${{bdPrev.toFixed(1)}}% → ${{bdLast.toFixed(1)}}% (${{fmtDeltaPp(bdLast - bdPrev)}}): кожне проблемне замовлення — це компенсація без виручки.`);
  if (flPrev != null && flLast != null && (flLast - flPrev) >= 0.5)
    mReasons.push(`<b>Failed Rate</b> ${{flPrev.toFixed(1)}}% → ${{flLast.toFixed(1)}}% (${{fmtDeltaPp(flLast - flPrev)}}): витрати на курʼєра понесені, виручки немає.`);
  if (ltPrev != null && ltLast != null && (ltLast - ltPrev) >= 2)
    mReasons.push(`<b>Late Delivery</b> ${{fmtDeltaPp(ltLast - ltPrev)}}: довші доставки — дорожча логістика і більше компенсацій за затримки.`);

  let mTitle = 'Чому просіла CP L2 Маржа (%)';
  let mList  = 'Що зʼїло маржу:';
  let mText;
  if (mPrev == null || mLast == null) {{
    mTitle = 'CP L2 Маржа (%)';
    mList  = '';
    mText  = 'Недостатньо даних для порівняння двох тижнів.';
  }} else if (hasMarginDrop) {{
    mText = `CP L2 маржа просіла з ${{mPrev.toFixed(1)}}% до ${{mLast.toFixed(1)}}% (${{fmtDeltaPp(marginPp)}}). Це рентабельність на євро обороту: обсяг замовлень на неї прямо не впливає, тільки структура витрат.`;
    const residual = Math.abs(marginPp) - explainedPp;
    if (!mReasons.length) {{
      mList = '';
      mReasons.push('Знижки, refunds і якісні метрики не погіршились — перевірте комісійну ставку, mix локацій усередині бренду та вартість доставки на замовлення.');
    }} else if (residual > 1) {{
      mReasons.push(`<b>Непояснений залишок ≈ ${{residual.toFixed(1)}} п.п.</b> — знижки й refunds покривають не все падіння. Найімовірніше: змінилась комісійна ставка, mix локацій чи страв, або подорожчала доставка на замовлення.`);
    }}
  }} else {{
    mTitle = marginPp != null && marginPp > 0.5 ? 'CP L2 Маржа (%): падіння немає' : 'CP L2 Маржа (%): без змін';
    mText  = marginPp != null && marginPp > 0.5
      ? `Маржа зросла з ${{mPrev.toFixed(1)}}% до ${{mLast.toFixed(1)}}% (${{fmtDeltaPp(marginPp)}}) — структура витрат на євро обороту покращилась.`
      : `Маржа стабільна: ${{mPrev.toFixed(1)}}% → ${{mLast.toFixed(1)}}% (${{fmtDeltaPp(marginPp)}}).`;
    mList = mReasons.length ? 'Але ці фактори тиснуть на маржу:' : '';
    if (!mReasons.length) mReasons.push('Частка знижок і refunds у GMV без суттєвих змін.');
  }}
  if (mLast != null && mLast < 0)
    mReasons.push(`<b>Маржа негативна (${{mLast.toFixed(1)}}%)</b> — кожне замовлення приносить збиток, економіка потребує перегляду.`);
  if (lowVolume) mReasons.push('Обсяги малі — на кількох замовленнях маржа% дуже волатильна.');

  // ── Збірка HTML ───────────────────────────────────────────────────────────
  const severity =
    (ordersWow != null && ordersWow < -15) || (marginPp != null && marginPp < -3) ||
    (mLast != null && mLast < 0) || (cpPctShown != null && cpPctShown < -20)
      ? 'danger'
      : (hasOrdersDrop || hasGmvDrop || hasMarginDrop || hasCpDrop) ? 'warning' : '';

  const cls = (v, badBelow, goodAbove) =>
    v == null ? 'flat' : v < badBelow ? 'down' : v > goodAbove ? 'up' : 'flat';

  const card = (icon, title, deltaHtml, deltaCls, sub, text, sections) => `
    <div class="dyn-analysis-card">
      <h5>${{icon}} ${{title}}</h5>
      <div class="delta ${{deltaCls}}">${{deltaHtml}}<span class="delta-sub">${{sub}}</span></div>
      <p class="dyn-analysis-text">${{text}}</p>
      ${{sections.filter(s => s.items && s.items.length).map(s => `
        ${{s.title ? `<div class="dyn-analysis-list-title">${{s.title}}</div>` : ''}}
        <ul>${{s.items.map(i => `<li>${{i}}</li>`).join('')}}</ul>`).join('')}}
    </div>`;

  let html = `<div class="dyn-analysis-title">🔍 Аналіз динаміки · останній тиждень vs попередній</div>`;
  html += `<div class="dyn-analysis-grid">`;
  html += card('📦', ordersTitle,
    fmtDeltaPct(ordersWow), cls(ordersWow, -2, 2),
    (oPrev != null && oLast != null) ? `${{Math.round(oPrev)}} → ${{Math.round(oLast)}} · GMV ${{fmtDeltaPct(gmvWow)}}` : '',
    ordersText,
    [{{ title: ordersListTitle, items: orderReasons }},
     {{ title: 'Розклад падіння GMV:', items: gmvReasons }},
     {{ title: '🌍 Зовнішні фактори:', items: externalReasons }}]);
  html += card('💶', cpTitle,
    fmtEurDelta(cpDiff), cls(cpDiff, -cpMaterial, cpMaterial),
    (cpPrev != null && cpLast != null) ? `${{fmtEurVal(cpPrev)}} → ${{fmtEurVal(cpLast)}}` : '',
    cpText, [{{ title: cpList, items: cpReasons }}]);
  html += card('📉', mTitle,
    fmtDeltaPp(marginPp), cls(marginPp, -0.5, 0.5),
    (mPrev != null && mLast != null) ? `${{mPrev.toFixed(1)}}% → ${{mLast.toFixed(1)}}%` : '',
    mText, [{{ title: mList, items: mReasons }}]);
  html += `</div>`;

  html += `<div class="dyn-analysis-hint">Аналіз побудовано автоматично з WoW-зміни метрик (Availability, Acceptance, Failed/Bad/Late, знижки, refunds, покази та конверсія по локаціях, середній чек) і порівняння з рештою ринку міста за той самий тиждень. Зовнішні фактори (погода, тривоги, сезон) — це підказки для перевірки, а не зафіксовані дані. Це гіпотези для розмови з партнером, не остаточний вердикт.</div>`;
  return {{ html, severity }};
}}

function renderCharts() {{
  const key = document.getElementById('dynBrandSelect').value;
  const placeholder = document.getElementById('dynPlaceholder');
  const chartsDiv   = document.getElementById('dynCharts');
  const infoDiv     = document.getElementById('dynBrandInfo');
  const locSection  = document.getElementById('locSection');
  const analysisEl  = document.getElementById('dynAnalysis');

  if (!key || !TRENDS[key]) {{
    placeholder.style.display = '';
    chartsDiv.style.display   = 'none';
    infoDiv.innerHTML = 'Оберіть бренд для перегляду динаміки';
    if (analysisEl) analysisEl.style.display = 'none';
    return;
  }}

  placeholder.style.display = 'none';
  chartsDiv.style.display   = '';

  const d = TRENDS[key];
  const labels = d.weeks.map(fmtWeek);
  const locs = d.locations || {{}};
  const locCount = Object.keys(locs).length;

  const groupLabel = d.group_name ? ` · <span style="color:var(--muted)">Group: ${{d.group_name}}</span>` : '';
  infoDiv.innerHTML = `<strong>${{d.brand}}</strong>${{groupLabel}} · ${{d.city}} · ${{d.weeks.length}} тижнів · ${{locCount}} локац.`;

  // Auto analysis panel
  if (analysisEl) {{
    const {{ html, severity }} = analyzeBrandDynamics(d);
    analysisEl.className = 'dyn-analysis' + (severity ? ' ' + severity : '');
    analysisEl.innerHTML = html;
    analysisEl.style.display = '';
  }}

  const GREEN  = '#1DC462';
  const BLUE   = '#1976D2';
  const PURPLE = '#7B1FA2';
  const TEAL   = '#00897B';
  const RED    = '#E53935';
  const ORANGE = '#FB8C00';
  const INDIGO = '#3949AB';
  const AMBER  = '#F9A825';

  // Brand-level charts
  makeOrUpdate('chartOrders',    labels, d.delivered_orders,        GREEN,  'Замовлення', false);
  makeOrUpdate('chartGmv',       labels, d.gmv_eur,                 BLUE,   'GMV, €', false);
  makeOrUpdate('chartCp',        labels, d.contribution_profit_eur, TEAL,   'CP, €', false);
  makeOrUpdate('chartCpMargin',  labels, d.cp_l2_margin_pct,        PURPLE, 'CP L2 Маржа, %', false);
  makeOrUpdate('chartFailed',    labels, d.failed_order_rate_pct,   RED,    'Failed Rate, %', false);
  makeOrUpdate('chartBad',       labels, d.bad_order_rate_pct,      ORANGE, 'Bad Order Rate, %', false);
  makeOrUpdate('chartAcceptance',labels, d.acceptance_rate_pct,     INDIGO, 'Acceptance Rate, %', false);
  makeOrUpdate('chartLate',      labels, d.late_delivery_rate_pct,  AMBER,  'Late Delivery, %', false);
  makeOrUpdate('chartAvailability', labels, d.availability_pct,     TEAL,   'Availability, %', false);

  // Знижки та refunds (brand-level)
  if (d.partner_discount_eur && d.partner_discount_eur.length) {{
    makeOrUpdate('chartPartnerDiscount', labels, d.partner_discount_eur, '#FB8C00', 'Знижки партнера, \u20ac', false);
    makeOrUpdate('chartBoltDiscount',    labels, d.bolt_discount_eur,    GREEN,     'Знижки Bolt, \u20ac',     false);
    makeOrUpdate('chartDemandRefunds',   labels, d.demand_refunds_eur,   RED,       'Refunds покупці, \u20ac', false);
    makeOrUpdate('chartSupplyRefunds',   labels, d.supply_refunds_eur,   PURPLE,    'Refunds кур\u2019єри, \u20ac', false);
  }}

  // ── Per-location conversion section ──────────────────────────────────────
  if (locCount === 0) {{
    locSection.style.display = 'none';
    return;
  }}
  locSection.style.display = '';

  // Build location tab buttons
  const tabsEl = document.getElementById('locTabs');
  tabsEl.innerHTML = '';
  const locIds = Object.keys(locs);

  locIds.forEach((pid, idx) => {{
    const loc = locs[pid];
    const btn = document.createElement('button');
    btn.textContent = loc.name + (loc.zone ? ` · ${{loc.zone}}` : '');
    btn.dataset.pid = pid;
    btn.style.cssText = `padding:7px 14px;border:none;border-radius:8px;cursor:pointer;
      font-size:12px;font-weight:600;transition:all 0.15s;
      background:${{idx===0?'var(--bolt-green)':'#f0f0f0'}};
      color:${{idx===0?'#fff':'#444'}};`;
    btn.onclick = () => {{
      tabsEl.querySelectorAll('button').forEach(b => {{
        b.style.background = '#f0f0f0'; b.style.color = '#444';
      }});
      btn.style.background = 'var(--bolt-green)';
      btn.style.color = '#fff';
      renderLocationCharts(locs[pid]);
    }};
    tabsEl.appendChild(btn);
  }});

  // Render first location by default
  renderLocationCharts(locs[locIds[0]]);
}}

function renderLocationCharts(loc) {{
  const labels = loc.weeks.map(fmtWeek);
  const lastIdx = loc.weeks.length - 1;

  // Funnel summary bar
  const lastImp  = loc.impressions  && loc.impressions[lastIdx]  != null ? loc.impressions[lastIdx].toLocaleString('uk-UA')  : '—';
  const lastMenu = loc.menu_views   && loc.menu_views[lastIdx]   != null ? loc.menu_views[lastIdx].toLocaleString('uk-UA')   : '—';
  const lastOrd  = loc.orders_placed&& loc.orders_placed[lastIdx]!= null ? loc.orders_placed[lastIdx].toLocaleString('uk-UA'): '—';
  const convFull = loc.conv_imp_to_order && loc.conv_imp_to_order[lastIdx] != null ? loc.conv_imp_to_order[lastIdx].toFixed(1)+'%' : '—';
  const convMenu = loc.conv_menu_to_order&& loc.conv_menu_to_order[lastIdx]!= null ? loc.conv_menu_to_order[lastIdx].toFixed(1)+'%' : '—';

  document.getElementById('locFunnelBar').innerHTML = `
    <div style="font-size:11px;font-weight:700;text-transform:uppercase;color:var(--muted);
                letter-spacing:.5px;margin-bottom:10px">Воронка — останній тиждень</div>
    <div style="display:flex;align-items:center;gap:0;flex-wrap:wrap">
      ${{funnelStep('👁️ Покази', lastImp, '#1976D2')}}
      ${{funnelArrow(loc.conv_imp_to_menu && loc.conv_imp_to_menu[lastIdx] != null ? loc.conv_imp_to_menu[lastIdx].toFixed(1)+'%' : '—')}}
      ${{funnelStep('🍽️ Меню', lastMenu, '#7B1FA2')}}
      ${{funnelArrow(convMenu)}}
      ${{funnelStep('📦 Замовлення', lastOrd, '#1DC462')}}
      <div style="margin-left:auto;text-align:right;padding:8px 12px;
                  background:#E8F9EE;border-radius:8px">
        <div style="font-size:10px;color:var(--muted)">Загальна конверсія</div>
        <div style="font-size:20px;font-weight:800;color:var(--bolt-mid-green)">${{convFull}}</div>
        <div style="font-size:10px;color:var(--muted)">показ → замовлення</div>
      </div>
    </div>`;

  const RED    = '#E53935';
  const GREEN  = '#1DC462';
  const BLUE   = '#1976D2';
  const PURPLE = '#7B1FA2';
  const TEAL   = '#00897B';
  const INDIGO = '#3949AB';

  makeOrUpdate('locChartConvFull',    labels, loc.conv_imp_to_order,  GREEN,  'Конверсія показ→замов., %', false);
  makeOrUpdate('locChartConvMenu',    labels, loc.conv_menu_to_order, TEAL,   'Конверсія меню→замов., %',  false);
  makeOrUpdate('locChartConvImp',     labels, loc.conv_imp_to_menu,   PURPLE, 'Конверсія показ→меню, %',   false);
  makeOrUpdate('locChartOrders',      labels, loc.delivered_orders,   BLUE,   'Замовлення',                false);
  makeOrUpdate('locChartImpressions', labels, loc.impressions,        INDIGO, 'Покази (сесії)',             false);
  makeOrUpdate('locChartMenuViews',   labels, loc.menu_views,         '#8D6E63', 'Перегляди меню',          false);
  makeOrUpdate('locChartAcceptance',  labels, loc.acceptance,         GREEN,  'Acceptance Rate, %',         false);
  makeOrUpdate('locChartAvailability',labels, loc.availability,       TEAL,   'Availability, %',            false);
  // Знижки та refunds
  makeOrUpdate('locChartPartnerDiscount', labels, loc.partner_discount, '#FB8C00', 'Знижки партнера, \u20ac', false);
  makeOrUpdate('locChartBoltDiscount',    labels, loc.bolt_discount,    GREEN,     'Знижки Bolt, \u20ac',      false);
  makeOrUpdate('locChartDemandRefunds',   labels, loc.demand_refunds,   RED,       'Refunds покупці, \u20ac',  false);
  makeOrUpdate('locChartSupplyRefunds',   labels, loc.supply_refunds,   PURPLE,    'Refunds кур\u2019єри, \u20ac', false);
}}

function funnelStep(label, value, color) {{
  return `<div style="text-align:center;padding:10px 16px;background:#f9f9f9;
                      border-radius:8px;min-width:100px">
    <div style="font-size:11px;color:${{color}};font-weight:700">${{label}}</div>
    <div style="font-size:18px;font-weight:800;color:#1A1A1A">${{value}}</div>
  </div>`;
}}

function funnelArrow(pct) {{
  return `<div style="padding:0 8px;color:#aaa;text-align:center">
    <div style="font-size:18px">→</div>
    <div style="font-size:11px;font-weight:700;color:var(--bolt-mid-green)">${{pct}}</div>
  </div>`;
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


def build_trends_json(df_trends: pd.DataFrame, df_loc: pd.DataFrame,
                      group_map: dict = None) -> str:
    """
    Convert weekly trends DataFrames into a JSON string for embedding in HTML.
    Includes brand-level aggregate data AND per-location conversion funnel data.
    group_map: optional {(brand, city): group_name} fallback from summary.
    """
    import math
    data = {}
    group_map = group_map or {}

    def clean(val):
        try:
            v = float(val)
            return None if math.isnan(v) or math.isinf(v) else round(v, 2)
        except (TypeError, ValueError):
            return None

    # ── Brand-level aggregate trends ──────────────────────────────────────────
    for col in ["delivered_orders", "gmv_eur", "contribution_profit_eur",
                "cp_l2_margin_pct", "failed_order_rate_pct",
                "bad_order_rate_pct", "acceptance_rate_pct", "late_delivery_rate_pct",
                "availability_pct",
                "partner_discount_eur", "bolt_discount_eur",
                "demand_refunds_eur", "supply_refunds_eur", "total_refunds_eur"]:
        if col in df_trends.columns:
            df_trends[col] = pd.to_numeric(df_trends[col], errors="coerce")

    for (brand, city), grp in df_trends.groupby(["brand_name", "city_name"], sort=False):
        key = f"{brand}|||{city}"
        grp = grp.sort_values("week_start")
        def gcol(col_name):
            return [clean(v) for v in grp[col_name]] if col_name in grp.columns else []

        group_name = ""
        if "group_name" in grp.columns:
            for raw in grp["group_name"].tolist():
                if raw is not None and str(raw).strip() and str(raw).lower() != "nan":
                    group_name = str(raw).strip()
                    break
        if not group_name:
            fallback = group_map.get((brand, city)) or group_map.get((str(brand), str(city)))
            if fallback and str(fallback).strip().lower() != "nan":
                group_name = str(fallback).strip()

        data[key] = {
            "brand": brand,
            "city": city,
            "group_name": group_name,
            "weeks": grp["week_start"].tolist(),
            "delivered_orders":        gcol("delivered_orders"),
            "gmv_eur":                 gcol("gmv_eur"),
            "contribution_profit_eur": gcol("contribution_profit_eur"),
            "cp_l2_margin_pct":        gcol("cp_l2_margin_pct"),
            "failed_order_rate_pct":   gcol("failed_order_rate_pct"),
            "bad_order_rate_pct":      gcol("bad_order_rate_pct"),
            "acceptance_rate_pct":     gcol("acceptance_rate_pct"),
            "late_delivery_rate_pct":  gcol("late_delivery_rate_pct"),
            "availability_pct":        gcol("availability_pct"),
            "partner_discount_eur":    gcol("partner_discount_eur"),
            "bolt_discount_eur":       gcol("bolt_discount_eur"),
            "demand_refunds_eur":      gcol("demand_refunds_eur"),
            "supply_refunds_eur":      gcol("supply_refunds_eur"),
            "total_refunds_eur":       gcol("total_refunds_eur"),
            "locations": {},  # filled below
        }

    # ── Per-location conversion data ──────────────────────────────────────────
    if not df_loc.empty:
        for col in ["delivered_orders", "gmv_eur", "impressions_sessions",
                    "menu_viewed_sessions", "order_placed_sessions",
                    "conversion_impression_to_order_pct", "conversion_menu_to_order_pct",
                    "conversion_impression_to_menu_pct",
                    "bad_order_rate_pct", "failed_order_rate_pct",
                    "acceptance_rate_pct", "availability_pct",
                    "partner_discount_eur", "bolt_discount_eur",
                    "demand_refunds_eur", "supply_refunds_eur", "total_refunds_eur"]:
            if col in df_loc.columns:
                df_loc[col] = pd.to_numeric(df_loc[col], errors="coerce")

        for (brand, city, provider_id), grp in df_loc.groupby(
                ["brand_name", "city_name", "provider_id"], sort=False):
            brand_key = f"{brand}|||{city}"
            grp = grp.sort_values("week_start")
            loc_name = str(grp["provider_name"].iloc[0])
            zone     = str(grp["zone_name"].iloc[0]) if "zone_name" in grp.columns else ""

            def lc(col_name):
                return [clean(v) for v in grp[col_name]] if col_name in grp.columns else []

            loc_data = {
                "provider_id":   int(provider_id),
                "name":          loc_name,
                "zone":          zone,
                "weeks":         grp["week_start"].tolist(),
                "delivered_orders":   lc("delivered_orders"),
                "gmv_eur":            lc("gmv_eur"),
                "impressions":        lc("impressions_sessions"),
                "menu_views":         lc("menu_viewed_sessions"),
                "orders_placed":      lc("order_placed_sessions"),
                "conv_imp_to_order":  lc("conversion_impression_to_order_pct"),
                "conv_menu_to_order": lc("conversion_menu_to_order_pct"),
                "conv_imp_to_menu":   lc("conversion_impression_to_menu_pct"),
                "bad_order_rate":     lc("bad_order_rate_pct"),
                "failed_rate":        lc("failed_order_rate_pct"),
                "acceptance":         lc("acceptance_rate_pct"),
                "availability":       lc("availability_pct"),
                "partner_discount":   lc("partner_discount_eur"),
                "bolt_discount":      lc("bolt_discount_eur"),
                "demand_refunds":     lc("demand_refunds_eur"),
                "supply_refunds":     lc("supply_refunds_eur"),
                "total_refunds":      lc("total_refunds_eur"),
            }

            if brand_key in data:
                data[brand_key]["locations"][str(provider_id)] = loc_data
            # If brand not in trends (edge case), create minimal entry
            else:
                data[brand_key] = {
                    "brand": brand, "city": city, "group_name": "",
                    "weeks": [], "locations": {str(provider_id): loc_data}
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


def build_overview_section(df_portfolio: pd.DataFrame) -> str:
    """
    Build the portfolio overview section comparing last week vs previous week.
    Uses pre-aggregated portfolio-level weekly data (no brand-level aggregation needed).
    """
    import math

    if df_portfolio.empty:
        return '<div class="no-data">Немає трендових даних для аналізу</div>'

    portfolio_weekly = df_portfolio.rename(columns={
        "gmv_eur": "gmv",
        "delivered_orders": "orders",
        "contribution_profit_eur": "cp",
        "failed_order_rate_pct": "failed_rate",
        "bad_order_rate_pct": "bad_rate",
        "acceptance_rate_pct": "acceptance",
        "late_delivery_rate_pct": "late_rate",
    }).sort_values("week_start")

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
    def safe_val(v):
        try:
            f = float(v)
            return None if math.isnan(f) or math.isinf(f) else round(f, 2)
        except (TypeError, ValueError):
            return None

    weeks_labels = portfolio_weekly["week_start"].tolist()
    weeks_js = json.dumps(weeks_labels)
    gmv_series    = json.dumps([safe_val(v) for v in portfolio_weekly["gmv"]])
    orders_series = json.dumps([safe_val(v) for v in portfolio_weekly["orders"]])
    cp_series     = json.dumps([safe_val(v) for v in portfolio_weekly["cp"]])

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


def build_html(df: pd.DataFrame, df_trends: pd.DataFrame, df_loc: pd.DataFrame,
               df_portfolio: pd.DataFrame, start_date: str, end_date: str) -> str:
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

    # Build overview section from pre-aggregated portfolio data (accurate, no row limit)
    overview_section = build_overview_section(df_portfolio)

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

    # Build trends JSON (brand-level + per-location conversion)
    group_map = {}
    if "group_name" in df.columns:
        for _, row in df.iterrows():
            b, c = row.get("brand_name"), row.get("city_name")
            g = row.get("group_name")
            if b is not None and c is not None and g is not None and str(g).strip() and str(g).lower() != "nan":
                group_map[(b, c)] = str(g).strip()
    trends_json_str = build_trends_json(
        df_trends,
        df_loc if not df_loc.empty else pd.DataFrame(),
        group_map=group_map,
    )

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

    try:
        df_loc = fetch_location_trends(n_weeks=12)
    except Exception as exc:
        print(f"WARNING: Could not fetch location trends: {exc}")
        df_loc = pd.DataFrame()

    try:
        df_portfolio = fetch_portfolio_weekly(n_weeks=12)
    except Exception as exc:
        print(f"WARNING: Could not fetch portfolio weekly data: {exc}")
        df_portfolio = pd.DataFrame()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    out_path = os.path.join(base_dir, OUTPUT_FILE)

    html = build_html(df, df_trends, df_loc, df_portfolio, start_date, end_date)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✅ HTML report saved → {out_path}")
    return out_path


if __name__ == "__main__":
    main()
