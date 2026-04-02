"""
Generate provider report: orders count + availability for the last month.
Queries Databricks and outputs providers/index.html
"""

import os
import sys
from pathlib import Path
from datetime import datetime, date

# Allow running from any directory
sys.path.insert(0, str(Path(__file__).parent))

from databricks import sql
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────

SERVER_HOSTNAME = "bolt-incentives.cloud.databricks.com"
HTTP_PATH = "sql/protocolv1/o/2472566184436351/0221-081903-9ag4bh69"

PROVIDER_IDS = [
    36453, 36454, 36455, 36456, 36457,
    36503, 36504, 36505,
    36571, 36572, 36573, 36576,
    46296, 46297,
    52802, 56367,
    70994, 194542,
]

IDS_STR = ", ".join(str(i) for i in PROVIDER_IDS)

OUTPUT_DIR = Path(__file__).parent.parent / "providers"
OUTPUT_DIR.mkdir(exist_ok=True)


def get_token():
    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        return token
    env_path = Path(__file__).parent.parent / "databricks-setup" / ".env"
    if not env_path.exists():
        env_path = Path.home() / "Library" / "CloudStorage" / \
            "GoogleDrive-marharyta.zhytnyk@bolt.eu" / "My Drive" / \
            "Events project" / "databricks-setup" / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("DATABRICKS_TOKEN="):
                return line.split("=", 1)[1].strip()
    raise RuntimeError("DATABRICKS_TOKEN not found")


def run_query(conn, q):
    with conn.cursor() as cur:
        cur.execute(q)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


# ── SQL Queries ───────────────────────────────────────────────────────────────

SQL_PROVIDER_INFO = f"""
SELECT
    provider_id,
    provider_name,
    city_name,
    country_code,
    business_segment,
    provider_status,
    lifecycle_status,
    provider_rating,
    regular_commission_rate
FROM ng_delivery_spark.dim_provider_v2
WHERE provider_id IN ({IDS_STR})
"""

SQL_ORDERS_LAST_MONTH = f"""
SELECT
    provider_id,
    SUM(courier_delivered_orders_count)   AS orders_last_month,
    SUM(total_gmv_before_discounts_eur)   AS gmv_eur
FROM ng_delivery_spark.fact_provider_weekly
WHERE provider_id IN ({IDS_STR})
  AND metric_timestamp_partition >= DATE_TRUNC('month', ADD_MONTHS(CURRENT_DATE, -1))
  AND metric_timestamp_partition <  DATE_TRUNC('month', CURRENT_DATE)
GROUP BY provider_id
"""

SQL_AVAILABILITY = f"""
SELECT
    provider_id,
    status,
    predicted_churn_probability,
    sales_segment,
    account_management_segment
FROM ng_public_spark.etl_incentives_provider_targeting_features
WHERE provider_id IN ({IDS_STR})
  AND date = (
      SELECT MAX(date)
      FROM ng_public_spark.etl_incentives_provider_targeting_features
      WHERE provider_id IN ({IDS_STR})
  )
"""


# ── HTML generation ────────────────────────────────────────────────────────────

def status_badge(status):
    if status is None:
        return '<span class="badge badge-unknown">—</span>'
    s = str(status).lower()
    if s in ("active", "activated"):
        return f'<span class="badge badge-active">{status}</span>'
    if s in ("inactive", "deactivated", "closed"):
        return f'<span class="badge badge-inactive">{status}</span>'
    return f'<span class="badge badge-other">{status}</span>'


def fmt_num(val, decimals=0):
    if val is None or (isinstance(val, float) and val != val):
        return "—"
    try:
        if decimals == 0:
            return f"{int(val):,}".replace(",", "\u202f")
        return f"{val:,.{decimals}f}".replace(",", "\u202f")
    except Exception:
        return str(val)


def build_html(df: pd.DataFrame, generated_at: str) -> str:
    rows_html = ""
    for _, r in df.iterrows():
        churn = r.get("predicted_churn_probability")
        churn_str = f"{float(churn):.0%}" if churn is not None and str(churn) != "nan" else "—"
        rows_html += f"""
        <tr>
          <td class="mono">{int(r['provider_id'])}</td>
          <td><strong>{r.get('provider_name') or '—'}</strong></td>
          <td>{r.get('city_name') or '—'}</td>
          <td>{str(r.get('country_code') or '—').upper()}</td>
          <td>{r.get('business_segment') or '—'}</td>
          <td>{status_badge(r.get('provider_status'))}</td>
          <td>{status_badge(r.get('status'))}</td>
          <td class="num">{fmt_num(r.get('orders_last_month'))}</td>
          <td class="num">{fmt_num(r.get('gmv_eur'), 0)} €</td>
          <td class="num">{churn_str}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Provider Report — Last Month</title>
<style>
  :root {{
    --bg: #0d1117; --surface: #161b22; --border: #30363d;
    --text: #e6edf3; --muted: #8b949e; --accent: #58a6ff;
    --green: #3fb950; --red: #f85149; --yellow: #d29922;
  }}
  * {{ margin:0; padding:0; box-sizing:border-box }}
  body {{ background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; padding:2rem }}
  header {{ margin-bottom:2rem }}
  h1 {{ font-size:1.5rem; font-weight:700; color:var(--accent) }}
  .meta {{ color:var(--muted); font-size:.875rem; margin-top:.35rem }}
  .card {{ background:var(--surface); border:1px solid var(--border); border-radius:12px; overflow:auto }}
  table {{ width:100%; border-collapse:collapse; font-size:.875rem }}
  thead th {{
    background:#1c2128; color:var(--muted); font-weight:600;
    text-align:left; padding:.75rem 1rem; white-space:nowrap;
    border-bottom:1px solid var(--border); position:sticky; top:0;
  }}
  tbody tr:hover {{ background:rgba(88,166,255,.04) }}
  td {{ padding:.65rem 1rem; border-bottom:1px solid #21262d; vertical-align:middle }}
  tbody tr:last-child td {{ border-bottom:none }}
  .mono {{ font-family:'SF Mono',monospace; font-size:.8rem; color:var(--muted) }}
  .num {{ text-align:right; font-variant-numeric:tabular-nums }}
  .badge {{ display:inline-block; padding:2px 10px; border-radius:20px; font-size:.75rem; font-weight:600 }}
  .badge-active   {{ background:rgba(63,185,80,.15);  color:var(--green) }}
  .badge-inactive {{ background:rgba(248,81,73,.15);  color:var(--red) }}
  .badge-other    {{ background:rgba(210,153,34,.15); color:var(--yellow) }}
  .badge-unknown  {{ background:#21262d; color:var(--muted) }}
  .legend {{ display:flex; gap:1.5rem; flex-wrap:wrap; margin-bottom:1.25rem; font-size:.8rem; color:var(--muted) }}
  .legend span {{ display:flex; align-items:center; gap:.4rem }}
  .dot {{ width:8px;height:8px;border-radius:50% }}
  .dot-green {{ background:var(--green) }} .dot-red {{ background:var(--red) }}
</style>
</head>
<body>
<header>
  <h1>📊 Provider Report — Last Month</h1>
  <div class="meta">Generated: {generated_at} &nbsp;·&nbsp; {len(df)} providers</div>
</header>

<div class="legend">
  <span><div class="dot dot-green"></div> Provider Status = active in Bolt platform</span>
  <span><div class="dot dot-green"></div> Availability Status = active in targeting</span>
  <span><div class="dot dot-red"></div> inactive / deactivated</span>
</div>

<div class="card">
  <table>
    <thead>
      <tr>
        <th>ID</th>
        <th>Provider Name</th>
        <th>City</th>
        <th>Country</th>
        <th>Segment</th>
        <th>Platform Status</th>
        <th>Availability</th>
        <th>Orders (last month)</th>
        <th>GMV (last month)</th>
        <th>Churn Risk</th>
      </tr>
    </thead>
    <tbody>
      {rows_html}
    </tbody>
  </table>
</div>
</body>
</html>
"""


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Connecting to Databricks...")
    conn = sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=get_token(),
    )

    try:
        print("Fetching provider info...")
        df_info = run_query(conn, SQL_PROVIDER_INFO)
        print(f"  → {len(df_info)} rows")

        print("Fetching orders for last month...")
        df_orders = run_query(conn, SQL_ORDERS_LAST_MONTH)
        print(f"  → {len(df_orders)} rows")

        print("Fetching availability/targeting features...")
        df_avail = run_query(conn, SQL_AVAILABILITY)
        print(f"  → {len(df_avail)} rows")

    finally:
        conn.close()

    # Merge all dataframes
    all_ids = pd.DataFrame({"provider_id": PROVIDER_IDS})
    df = all_ids.merge(df_info, on="provider_id", how="left")
    df = df.merge(df_orders, on="provider_id", how="left")
    df = df.merge(
        df_avail[["provider_id", "status", "predicted_churn_probability",
                  "sales_segment", "account_management_segment"]],
        on="provider_id", how="left"
    )

    generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    html = build_html(df, generated_at)

    out_path = OUTPUT_DIR / "index.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"\nReport saved → {out_path}")
    print(f"Providers found: {df['provider_name'].notna().sum()}/{len(df)}")


if __name__ == "__main__":
    main()
