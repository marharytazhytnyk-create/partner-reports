#!/usr/bin/env python3
"""Звіт по всіх замовленнях партнера з усіма spend objectives."""

from __future__ import annotations

import html
import json
import subprocess
from collections import defaultdict
from datetime import date
from pathlib import Path

from databricks import sql

HOST = "bolt-incentives.cloud.databricks.com"
HTTP_PATH = "sql/protocolv1/o/2472566184436351/0221-081903-9ag4bh69"
CLUSTER = "0221-081903-9ag4bh69"

PROVIDER_ID = 194993
WEEK_START = "2026-07-06"
WEEK_END = "2026-07-12"
OUTPUT = Path(__file__).parent / "Josper Svintuz Vinnytsia — Orders 06.07-12.07.2026.html"


def get_token() -> str:
    return json.loads(
        subprocess.check_output(
            ["databricks", "auth", "token", "--profile", "bolt-incentives-temp"],
            text=True,
        )
    )["access_token"]


def run_query(conn, sql_text: str):
    with conn.cursor() as cur:
        cur.execute(sql_text)
        cols = [d[0] for d in cur.description]
        return cols, cur.fetchall()


def fmt_num(val, decimals=2):
    if val is None:
        return "—"
    return f"{float(val):,.{decimals}f}".replace(",", "\u202f")


def fmt_uah(val):
    if val is None:
        return "—"
    return f"{float(val):,.0f} ₴".replace(",", "\u202f")


def sp_tag(obj: str | None) -> str:
    if not obj:
        return ""
    if obj.startswith("sp_"):
        return ' <span class="tag tag-sp">Smart Promo</span>'
    return ""


def fetch_rows(conn):
    cols, rows = run_query(
        conn,
        f"""
        SELECT
            o.order_id,
            o.order_created_date,
            o.provider_name,
            o.city_name,
            ROUND(o.provider_price_before_discount_eur, 2) AS order_gross_eur,
            ROUND(o.provider_price_after_discount_eur, 2) AS order_net_eur,
            ROUND(o.provider_price_before_discount, 2) AS order_gross_uah,
            ROUND(o.provider_price_after_discount, 2) AS order_net_uah,
            d.campaign_spend_objective,
            d.campaign_objective_group,
            d.campaign_target_type,
            d.campaign_sponsor_type,
            ROUND(d.campaign_discount_eur, 2) AS discount_eur,
            ROUND(d.campaign_spend_bolt_eur, 2) AS bolt_eur,
            ROUND(d.campaign_spend_provider_eur, 2) AS provider_eur,
            ROUND(d.campaign_discount, 2) AS discount_uah,
            ROUND(d.campaign_spend_bolt, 2) AS bolt_uah,
            ROUND(d.campaign_spend_provider, 2) AS provider_uah
        FROM ng_delivery_spark.fact_order_delivery o
        LEFT JOIN ng_delivery_spark.int_order_campaign_dimensions d
            ON d.order_id = o.order_id
        WHERE o.provider_id = {PROVIDER_ID}
          AND o.order_state = 'delivered'
          AND COALESCE(o.is_bolt_market_order, false) = false
          AND o.order_created_date BETWEEN '{WEEK_START}' AND '{WEEK_END}'
        ORDER BY o.order_created_date, o.order_id, d.campaign_spend_objective
        """,
    )
    return [dict(zip(cols, row)) for row in rows]


def build_summary(rows: list[dict]) -> dict:
    orders: dict[int, dict] = {}
    objectives: dict[str, dict] = defaultdict(
        lambda: {"orders": set(), "discount_uah": 0.0, "bolt_uah": 0.0, "provider_uah": 0.0}
    )

    for row in rows:
        oid = row["order_id"]
        if oid not in orders:
            orders[oid] = {
                "date": row["order_created_date"],
                "provider_name": row["provider_name"],
                "gross_uah": row["order_gross_uah"] or 0,
                "net_uah": row["order_net_uah"] or 0,
                "objectives": set(),
                "has_campaign": False,
            }
        obj = row["campaign_spend_objective"]
        if obj:
            orders[oid]["has_campaign"] = True
            orders[oid]["objectives"].add(obj)
            objectives[obj]["orders"].add(oid)
            objectives[obj]["discount_uah"] += float(row["discount_uah"] or 0)
            objectives[obj]["bolt_uah"] += float(row["bolt_uah"] or 0)
            objectives[obj]["provider_uah"] += float(row["provider_uah"] or 0)

    total_gross = sum(o["gross_uah"] for o in orders.values())
    total_net = sum(o["net_uah"] for o in orders.values())
    with_campaign = sum(1 for o in orders.values() if o["has_campaign"])

    obj_summary = []
    for obj, data in sorted(objectives.items(), key=lambda x: (-len(x[1]["orders"]), x[0])):
        obj_summary.append(
            {
                "objective": obj,
                "orders": len(data["orders"]),
                "discount_uah": data["discount_uah"],
                "bolt_uah": data["bolt_uah"],
                "provider_uah": data["provider_uah"],
            }
        )

    return {
        "total_orders": len(orders),
        "with_campaign": with_campaign,
        "without_campaign": len(orders) - with_campaign,
        "total_rows": len(rows),
        "gross_uah": total_gross,
        "net_uah": total_net,
        "objectives": obj_summary,
    }


def render_table_rows(rows: list[dict]) -> str:
    parts = []
    prev_oid = None
    for row in rows:
        oid = row["order_id"]
        show_order = oid != prev_oid
        prev_oid = oid
        obj = row["campaign_spend_objective"]
        parts.append(
            "<tr>"
            f"<td>{row['order_created_date'] if show_order else ''}</td>"
            f"<td class='num'>{oid if show_order else ''}</td>"
            f"<td class='num'>{fmt_uah(row['order_gross_uah']) if show_order else ''}</td>"
            f"<td class='num'>{fmt_uah(row['order_net_uah']) if show_order else ''}</td>"
            f"<td>{html.escape(obj or '—')}{sp_tag(obj)}</td>"
            f"<td>{html.escape(row['campaign_objective_group'] or '—')}</td>"
            f"<td>{html.escape(row['campaign_target_type'] or '—')}</td>"
            f"<td>{html.escape(row['campaign_sponsor_type'] or '—')}</td>"
            f"<td class='num'>{fmt_uah(row['discount_uah'])}</td>"
            f"<td class='num'>{fmt_uah(row['bolt_uah'])}</td>"
            f"<td class='num'>{fmt_uah(row['provider_uah'])}</td>"
            "</tr>"
        )
    return "\n".join(parts)


def render_objective_rows(objectives: list[dict]) -> str:
    parts = []
    for item in objectives:
        parts.append(
            "<tr>"
            f"<td>{html.escape(item['objective'])}{sp_tag(item['objective'])}</td>"
            f"<td class='num'>{item['orders']}</td>"
            f"<td class='num'>{fmt_uah(item['discount_uah'])}</td>"
            f"<td class='num'>{fmt_uah(item['bolt_uah'])}</td>"
            f"<td class='num'>{fmt_uah(item['provider_uah'])}</td>"
            "</tr>"
        )
    return "\n".join(parts)


def build_html(rows: list[dict], summary: dict, provider_name: str, city: str) -> str:
    generated = date.today().isoformat()
    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
  <title>{html.escape(provider_name)} — Замовлення · {WEEK_START} — {WEEK_END}</title>
  <style>
    :root {{
      --green:#34D186; --green-darker:#0d8a52;
      --black:#0d0d0d; --gray-700:#4a4a4a; --gray-400:#9a9a9a; --gray-100:#f5f5f5;
    }}
    *{{margin:0;padding:0;box-sizing:border-box}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;
      font-size:14px;line-height:1.55;color:#1a1a1a;background:var(--gray-100)}}
    .header{{background:var(--black);padding:28px 40px;display:flex;align-items:center;
      justify-content:space-between;border-bottom:4px solid var(--green);flex-wrap:wrap;gap:16px}}
    .header-title h1{{font-size:22px;font-weight:700;color:#fff}}
    .header-title p{{font-size:11px;color:var(--green);text-transform:uppercase;
      letter-spacing:1.2px;font-weight:600;margin-top:4px}}
    .header-meta{{text-align:right;color:var(--gray-400);font-size:12px;line-height:1.9}}
    .header-meta strong{{color:var(--green)}}
    .container{{max-width:1440px;margin:0 auto;padding:32px 40px}}
    .period-bar{{background:#fff;border-radius:12px;padding:16px 24px;margin-bottom:28px;
      box-shadow:0 1px 4px rgba(0,0,0,.06);font-size:13px;color:var(--gray-700)}}
    .kpi-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:14px;margin-bottom:28px}}
    .kpi-card{{background:#fff;border-radius:12px;padding:16px 18px;border-top:3px solid var(--green);
      box-shadow:0 1px 4px rgba(0,0,0,.06)}}
    .kpi-label{{font-size:10px;font-weight:700;text-transform:uppercase;color:var(--gray-400);margin-bottom:4px}}
    .kpi-value{{font-size:22px;font-weight:700}}
    .section-title{{font-size:13px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;
      color:var(--gray-700);padding-bottom:10px;border-bottom:2px solid var(--green);margin:28px 0 10px}}
    .section-hint{{font-size:12px;color:var(--gray-400);margin-bottom:14px}}
    .table-wrap{{background:#fff;border-radius:12px;overflow:auto;box-shadow:0 1px 4px rgba(0,0,0,.06);margin-bottom:32px}}
    table{{width:100%;border-collapse:collapse;min-width:900px}}
    th{{background:var(--black);color:#fff;font-size:11px;font-weight:700;text-transform:uppercase;
      padding:12px 14px;text-align:left;position:sticky;top:0}}
    td{{padding:10px 14px;border-bottom:1px solid #f0f0f0;font-size:12px;vertical-align:top}}
    tr:hover td{{background:#f9fffc}}
    .num{{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}}
    .tag{{display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700}}
    .tag-sp{{background:#ede7f6;color:#5e35b1}}
    .footer{{text-align:center;color:var(--gray-400);font-size:11px;padding:24px 0 40px}}
    @media print {{
      body{{background:#fff}}
      .table-wrap{{box-shadow:none}}
      th{{position:static}}
    }}
  </style>
</head>
<body>
  <div class="header">
    <div class="header-title">
      <h1>{html.escape(provider_name)}</h1>
      <p>Замовлення зі spend objectives · {html.escape(city)}</p>
    </div>
    <div class="header-meta">
      Provider ID: <strong>{PROVIDER_ID}</strong><br/>
      Період: <strong>{WEEK_START} — {WEEK_END}</strong><br/>
      Оновлено: <strong>{generated}</strong>
    </div>
  </div>

  <div class="container">
    <div class="period-bar">
      Джерело: Databricks <code>fact_order_delivery</code> + <code>int_order_campaign_dimensions</code> ·
      delivered · не Bolt Market · одне замовлення може мати кілька рядків (різні spend objectives)
    </div>

    <div class="kpi-grid">
      <div class="kpi-card"><div class="kpi-label">Доставлені замовлення</div><div class="kpi-value">{summary['total_orders']}</div></div>
      <div class="kpi-card"><div class="kpi-label">З кампаніями</div><div class="kpi-value">{summary['with_campaign']}</div></div>
      <div class="kpi-card"><div class="kpi-label">Без кампаній</div><div class="kpi-value">{summary['without_campaign']}</div></div>
      <div class="kpi-card"><div class="kpi-label">Рядків у звіті</div><div class="kpi-value">{summary['total_rows']}</div></div>
      <div class="kpi-card"><div class="kpi-label">Gross sales</div><div class="kpi-value">{fmt_uah(summary['gross_uah'])}</div></div>
      <div class="kpi-card"><div class="kpi-label">Net sales</div><div class="kpi-value">{fmt_uah(summary['net_uah'])}</div></div>
    </div>

    <div class="section-title">Підсумок по Spend Objective</div>
    <p class="section-hint">Унікальні замовлення та суми знижок по кожному spend objective</p>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Spend Objective</th><th class="num">Замовлення</th>
            <th class="num">Знижка</th><th class="num">Bolt</th><th class="num">Партнер</th>
          </tr>
        </thead>
        <tbody>
          {render_objective_rows(summary['objectives'])}
        </tbody>
      </table>
    </div>

    <div class="section-title">Усі замовлення</div>
    <p class="section-hint">Деталізація: кожен рядок — одна кампанія / spend objective в замовленні</p>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Дата</th><th class="num">Order ID</th>
            <th class="num">Gross</th><th class="num">Net</th>
            <th>Spend Objective</th><th>Objective Group</th>
            <th>Target Type</th><th>Sponsor</th>
            <th class="num">Знижка</th><th class="num">Bolt</th><th class="num">Партнер</th>
          </tr>
        </thead>
        <tbody>
          {render_table_rows(rows)}
        </tbody>
      </table>
    </div>
  </div>

  <div class="footer">Bolt Food · Databricks · {html.escape(provider_name)} · {WEEK_START} — {WEEK_END}</div>
</body>
</html>
"""


def main():
    token = get_token()
    conn = sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=token)
    try:
        rows = fetch_rows(conn)
    finally:
        conn.close()

    if not rows:
        raise SystemExit("No data returned")

    summary = build_summary(rows)
    provider_name = rows[0]["provider_name"]
    city = rows[0]["city_name"]
    html_text = build_html(rows, summary, provider_name, city)
    OUTPUT.write_text(html_text, encoding="utf-8")
    print(f"Saved: {OUTPUT}")
    print(f"Orders: {summary['total_orders']}, rows: {summary['total_rows']}, objectives: {len(summary['objectives'])}")


if __name__ == "__main__":
    main()
