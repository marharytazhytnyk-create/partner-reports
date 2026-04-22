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

def get_last_4_full_weeks():
    """Return start and end dates for the last 4 full calendar weeks (Mon–Sun)."""
    today = date.today()
    # Last full week ends on the most recent Sunday
    days_since_sunday = today.weekday() + 1  # Mon=0 → days since last Sun
    last_sunday = today - timedelta(days=days_since_sunday)
    last_monday = last_sunday - timedelta(days=27)  # 4 weeks back
    return last_monday.isoformat(), last_sunday.isoformat()


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
    """Fetch aggregated (4-week total) provider summary."""
    start_date, end_date = get_last_4_full_weeks()
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
        p.provider_rating,
        SUM(f.delivered_orders_count) AS delivered_orders,
        SUM(f.failed_orders_count) AS failed_orders,
        SUM(f.placed_orders_count) AS placed_orders,
        SUM(f.total_gmv_before_discounts_eur) AS gmv_eur,
        SUM(f.total_contribution_profit_eur) AS contribution_profit_eur,
        CASE WHEN SUM(f.total_gmv_before_discounts_eur) > 0
             THEN SUM(f.total_contribution_profit_eur) / SUM(f.total_gmv_before_discounts_eur) * 100
             ELSE NULL END AS cp_l2_margin_pct,
        AVG(f.bad_order_rate_value) AS bad_order_rate,
        AVG(f.failed_order_rate_value) AS failed_order_rate,
        AVG(f.provider_acceptance_rate_value) AS acceptance_rate,
        AVG(f.late_delivery_order_rate_value) AS late_delivery_rate,
        AVG(f.provider_active_rate_value) AS active_rate,
        COUNT(DISTINCT DATE_TRUNC('week', f.metric_timestamp_local)) AS active_weeks
    FROM ng_delivery_spark.dim_provider_v2 p
    INNER JOIN ng_delivery_spark.fact_provider_weekly f
        ON p.provider_id = f.provider_id
    WHERE
        p.account_manager_name = '{ACCOUNT_MANAGER}'
        AND p.country_code = '{COUNTRY_CODE}'
        AND CAST(f.metric_timestamp_local AS DATE) BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY
        p.provider_id, p.provider_name, p.brand_name, p.group_name,
        p.city_name, p.zone_name, p.business_segment_v2, p.business_subsegment_v2,
        p.delivery_vertical, p.provider_status, p.account_manager_name,
        p.is_top_brand, p.provider_rating
    ORDER BY p.city_name, gmv_eur DESC
    """

    print("Running provider summary query...")
    result = _exec_sql(ctx, sql, timeout=300)
    df = _to_df(result)
    print(f"Fetched {len(df):,} provider summaries")
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


def is_problematic(row) -> list:
    """Returns list of problems for a provider."""
    problems = []
    if safe_float(row.get("failed_order_rate")) > 0.05:
        problems.append(f"Висока частота невдалих замовлень: {fmt_pct(safe_float(row.get('failed_order_rate'))*100)}")
    if safe_float(row.get("bad_order_rate")) > 0.10:
        problems.append(f"Висока частота поганих замовлень: {fmt_pct(safe_float(row.get('bad_order_rate'))*100)}")
    if safe_float(row.get("late_delivery_rate")) > 0.15:
        problems.append(f"Висока частота запізнень: {fmt_pct(safe_float(row.get('late_delivery_rate'))*100)}")
    if safe_float(row.get("acceptance_rate")) < 0.85 and safe_float(row.get("acceptance_rate")) > 0:
        problems.append(f"Низький рівень прийняття замовлень: {fmt_pct(safe_float(row.get('acceptance_rate'))*100)}")
    if safe_float(row.get("cp_l2_margin_pct")) < -5:
        problems.append(f"Від'ємна маржа CP L2: {fmt_pct(safe_float(row.get('cp_l2_margin_pct')))}")
    if safe_float(row.get("delivered_orders")) < 50:
        problems.append(f"Мала кількість замовлень за 4 тижні: {fmt_num(row.get('delivered_orders'))}")
    return problems


# ─── HTML GENERATION ──────────────────────────────────────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Портфоліо Marharyta Zhytnyk — Bolt Food</title>
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

  /* ── PROBLEMS ── */
  .problems-box {{
    background: #fff;
    border-radius: 12px;
    box-shadow: var(--shadow);
    border-left: 5px solid var(--danger);
    padding: 16px 20px;
    margin-bottom: 28px;
  }}
  .problems-box h3 {{
    font-size: 14px;
    font-weight: 700;
    color: var(--danger);
    margin-bottom: 12px;
    display: flex;
    align-items: center;
    gap: 8px;
  }}
  .problem-row {{
    display: flex;
    flex-direction: column;
    padding: 8px 0;
    border-bottom: 1px solid var(--border);
  }}
  .problem-row:last-child {{ border-bottom: none; }}
  .problem-row-header {{
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 4px;
  }}
  .problem-name {{
    font-size: 13px;
    font-weight: 600;
    color: var(--text);
  }}
  .problem-tags {{
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
    margin-left: 6px;
  }}
  .problem-tag {{
    font-size: 11px;
    background: #FFEBEE;
    color: var(--danger);
    border-radius: 8px;
    padding: 2px 8px;
    font-weight: 600;
  }}

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
</div>

<div class="content">
{city_sections}
</div>

<div class="footer">
  Автоматично згенеровано · Bolt Food Partner Reports · <a href="https://github.com/marharytazhytnyk-create/partner-reports">GitHub</a>
</div>

<script>
// City navigation
function showCity(cityId) {{
  document.querySelectorAll('.city-tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.city-section').forEach(s => s.classList.remove('active'));
  document.getElementById('tab-' + cityId).classList.add('active');
  document.getElementById('city-' + cityId).classList.add('active');
}}

// Table search
function filterTable(inputEl, tableId) {{
  const q = inputEl.value.toLowerCase();
  document.querySelectorAll('#' + tableId + ' tbody tr').forEach(row => {{
    row.style.display = row.textContent.toLowerCase().includes(q) ? '' : 'none';
  }});
}}

// Table sort
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

// Initialize first city on load
document.addEventListener('DOMContentLoaded', () => {{
  const firstTab = document.querySelector('.city-tab');
  if (firstTab) firstTab.click();
}});
</script>
</body>
</html>
"""


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


def build_problems_box(problem_providers: list) -> str:
    if not problem_providers:
        return ""
    rows_html = ""
    for item in problem_providers[:20]:
        name = str(item["brand_name"] or item["provider_name"] or "—")
        probs = item["problems"]
        tags = "".join(f'<span class="problem-tag">{p}</span>' for p in probs)
        orders = fmt_num(item.get("delivered_orders"))
        gmv = fmt_eur(item.get("gmv_eur"))
        rows_html += f"""
        <div class="problem-row">
          <div class="problem-row-header">
            <span class="problem-name">⚠️ {name}</span>
            <span style="font-size:11px;color:var(--muted)">({orders} зам. | {gmv})</span>
          </div>
          <div class="problem-tags">{tags}</div>
        </div>"""
    return f"""
    <div class="problems-box">
      <h3>🚨 Заклади з проблемами ({len(problem_providers)})</h3>
      {rows_html}
    </div>"""


def build_provider_table(providers: pd.DataFrame, city_id: str) -> str:
    if providers.empty:
        return '<div class="no-data">Немає даних по закладах</div>'

    table_id = f"tbl_{city_id}"
    rows_html = ""
    for _, row in providers.iterrows():
        orders = safe_float(row.get("delivered_orders"))
        gmv = safe_float(row.get("gmv_eur"))
        cp = safe_float(row.get("contribution_profit_eur"))
        cp_margin = safe_float(row.get("cp_l2_margin_pct"))
        problems = is_problematic(row)
        problem_icon = '<span class="badge-problem"></span>' if problems else ""
        status = str(row.get("provider_status") or "")
        status_badge = (
            '<span class="badge-status-active">Активний</span>' if "active" in status.lower()
            else '<span class="badge-status-inactive">Неактивний</span>'
        )
        top_badge = '<span class="badge-top">TOP</span>' if str(row.get("is_top_brand")) in ["true", "True", "1", True] else ""
        cp_class = "positive" if cp_margin > 0 else "negative" if cp_margin < -1 else "neutral"
        failed_rate = safe_float(row.get("failed_order_rate"))
        failed_class = "negative" if failed_rate > 0.05 else ""
        row_class = "problem-highlight" if problems else ""

        rows_html += f"""
        <tr class="{row_class}">
          <td>{problem_icon}{str(row.get('brand_name') or row.get('provider_name') or '—')} {top_badge}</td>
          <td>{str(row.get('zone_name') or '—')}</td>
          <td>{str(row.get('business_segment_v2') or '—')}</td>
          <td class="center">{status_badge}</td>
          <td class="num" data-val="{orders}">{fmt_num(orders)}</td>
          <td class="num" data-val="{gmv}">{fmt_eur(gmv)}</td>
          <td class="num" data-val="{cp}">{fmt_eur(cp)}</td>
          <td class="num {cp_class}" data-val="{cp_margin}">{fmt_pct(cp_margin)}</td>
          <td class="num {failed_class}" data-val="{failed_rate}">{fmt_pct(failed_rate*100)}</td>
          <td class="num" data-val="{safe_float(row.get('bad_order_rate'))}">{fmt_pct(safe_float(row.get('bad_order_rate'))*100)}</td>
          <td class="num" data-val="{safe_float(row.get('acceptance_rate'))}">{fmt_pct(safe_float(row.get('acceptance_rate'))*100)}</td>
        </tr>"""

    search_id = f"search_{city_id}"
    sort_js = "".join(
        f"let sort_{table_id}_{i}=false;" for i in range(11)
    )
    header_cols = [
        ("Бренд / Заклад", 0), ("Зона", 1), ("Сегмент", 2), ("Статус", 3),
        ("Замовлення", 4), ("GMV, €", 5), ("Contribution Profit, €", 6),
        ("CP L2 Маржа", 7), ("Failed Rate", 8), ("Bad Order Rate", 9), ("Acceptance Rate", 10),
    ]
    th_html = ""
    for label, idx in header_cols:
        th_html += f'<th onclick="sort_{table_id}_{idx}=!sort_{table_id}_{idx};sortTable(\'{table_id}\',{idx},sort_{table_id}_{idx})" class="">  {label} <span class="sort-icon">⇅</span></th>'

    return f"""
    <div class="table-wrap">
      <div class="table-header">
        <h3>📋 Всі заклади</h3>
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


def build_html(df: pd.DataFrame, start_date: str, end_date: str) -> str:
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
    total_providers = df["provider_id"].nunique()

    city_tabs_html = ""
    city_sections_html = ""

    for city in cities:
        cid = city_slug(city)
        city_df = df[df["city_name"] == city].copy()
        providers_count = city_df["provider_id"].nunique()

        city_tabs_html += f'<div class="city-tab" id="tab-{cid}" onclick="showCity(\'{cid}\')">{city} <span style="font-size:11px;color:var(--muted)">({providers_count})</span></div>'

        # Totals for the city
        total_orders = city_df["delivered_orders"].sum()
        total_gmv = city_df["gmv_eur"].sum()
        total_cp = city_df["contribution_profit_eur"].sum()
        avg_cp_margin = (total_cp / total_gmv * 100) if total_gmv > 0 else 0

        stats_html = f"""
        <div class="stats-row">
          <div class="stat-box">
            <div class="stat-label">Закладів</div>
            <div class="stat-value">{providers_count}</div>
            <div class="stat-sub">у портфоліо</div>
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
        </div>"""

        # TOP 3 by each metric
        top_orders_html = build_top_card(city_df, "delivered_orders", "ТОП по замовленнях", "📦", fmt_num)
        top_gmv_html = build_top_card(city_df, "gmv_eur", "ТОП по GMV", "💰", fmt_eur)
        top_cp_html = build_top_card(city_df, "contribution_profit_eur", "ТОП по CP L2", "📈", fmt_eur)

        # Problems
        problem_list = []
        for _, row in city_df.iterrows():
            probs = is_problematic(row)
            if probs:
                problem_list.append({**row.to_dict(), "problems": probs})
        problem_list.sort(key=lambda x: len(x["problems"]), reverse=True)
        problems_html = build_problems_box(problem_list)

        # Provider table
        table_html = build_provider_table(city_df.sort_values("gmv_eur", ascending=False), cid)

        city_sections_html += f"""
        <div class="city-section" id="city-{cid}">
          <div class="section-title">
            🏙️ {city}
            <span class="badge">{providers_count} закладів</span>
          </div>
          <div class="section-sub">Період аналізу: {start_date} — {end_date} · Account Manager: Marharyta Zhytnyk</div>
          <div class="period-info">📅 Останні 4 повні тижні: {start_date} — {end_date}</div>

          {stats_html}

          <div class="top-grid">
            {top_orders_html}
            {top_gmv_html}
            {top_cp_html}
          </div>

          {problems_html}

          {table_html}
        </div>"""

    html = HTML_TEMPLATE.format(
        report_date=REPORT_DATE,
        period_start=start_date,
        period_end=end_date,
        total_providers=total_providers,
        city_tabs=city_tabs_html,
        city_sections=city_sections_html,
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
        print(f"ERROR fetching data: {exc}")
        sys.exit(1)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    out_path = os.path.join(base_dir, OUTPUT_FILE)

    html = build_html(df, start_date, end_date)

    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\n✅ HTML report saved → {out_path}")
    return out_path


if __name__ == "__main__":
    main()
