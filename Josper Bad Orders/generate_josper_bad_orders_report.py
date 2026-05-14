#!/usr/bin/env python3
"""
Josper Svintuz (Запоріжжя) — щотижневий звіт по «поганих» замовленнях:
  - не доставлені (failed / rejected) з поясненням причини;
  - доставлені, але з низькою оцінкою клієнта (≤2 зірок).

Джерело: Databricks SQL warehouse (databricks-sql-connector), ті самі змінні, що й інші звіти репо.

Тиждень звіту:
  - за замовчуванням — останній повний календарний тиждень пн–нд,
    де «нд» — остання неділя, не пізніша за сьогодні (на понеділку після
    завершення тижня це вчорашня неділя).
  - Можна передати JOSPER_WEEK_START=YYYY-MM-DD (понеділок тижня).
"""

from __future__ import annotations

import html
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from databricks import sql

# Усі локації бренду Josper Svintuz у Запоріжжі (vendor_id 145072)
PROVIDER_IDS = [
    197909, 195025, 195015, 197892, 195020, 197899, 195021, 195012,
    195006, 195019, 195007, 195024,
]
VENDOR_ID = 145072

SERVER_HOSTNAME = "bolt-incentives.cloud.databricks.com"
HTTP_PATH = "sql/protocolv1/o/2472566184436351/0221-081903-9ag4bh69"

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_HTML = SCRIPT_DIR / "josper_bad_orders_report.html"


def get_token() -> str:
    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        return token
    env_path = Path(__file__).resolve().parent.parent / "databricks-setup" / ".env"
    if not env_path.exists():
        env_path = (
            Path.home()
            / "Library"
            / "CloudStorage"
            / "GoogleDrive-marharyta.zhytnyk@bolt.eu"
            / "My Drive"
            / "Events project"
            / "databricks-setup"
            / ".env"
        )
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("DATABRICKS_TOKEN="):
                return line.split("=", 1)[1].strip()
    raise RuntimeError("DATABRICKS_TOKEN not found (env or databricks-setup/.env)")


def week_bounds() -> tuple[date, date]:
    """Повертає (понеділок, неділя) останнього завершеного тижня пн–нд."""
    if os.environ.get("JOSPER_WEEK_START"):
        start = date.fromisoformat(os.environ["JOSPER_WEEK_START"])
        return start, start + timedelta(days=6)

    today = date.today()
    if today.weekday() == 6:
        end = today
    else:
        end = today - timedelta(days=today.weekday() + 1)
    start = end - timedelta(days=6)
    return start, end


def run_query(conn, q: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(q)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def human_reason(row: pd.Series) -> str:
    """Текстова причина для не доставленого замовлення (укр.)."""
    final = str(row.get("final_state") or "")
    from_st = str(row.get("from_state") or "") or "—"
    rej = row.get("is_rejected_by_provider")
    ncr = int(row.get("number_courier_rejects") or 0)
    ect = row.get("has_eater_cancellation_ticket")

    if final == "rejected":
        return (
            "Не доставлено: замовлення відхилено рестораном на етапі прийняття "
            f"(стан до відхилення: {from_st})."
        )
    if final == "failed":
        bits = []
        if from_st == "waiting_delivery":
            bits.append("зрив після передачі кур'єру (етап доставки)")
        elif from_st == "waiting_preparation":
            bits.append("зрив під час приготування")
        elif from_st == "waiting_acceptance":
            bits.append("не вдалося після прийняття / на старті обробки")
        else:
            bits.append(f"перехід у failed з етапу «{from_st}»")
        if ncr and ncr > 0:
            bits.append(f"відмови кур'єра під час пошуку: {ncr}")
        if ect is True:
            bits.append("є тікет скасування з боку клієнта в даних факту")
        if rej is True:
            bits.append("позначено як відхилення провайдером у факті")
        return "Не доставлено (failed): " + "; ".join(bits) + "."
    return f"Статус: {final}"


def build_html(
    week_start: date,
    week_end: date,
    df_bad: pd.DataFrame,
    df_low: pd.DataFrame,
    df_names: pd.DataFrame,
    generated_at: str,
) -> str:
    pid_to_name = (
        dict(zip(df_names["provider_id"], df_names["provider_name"]))
        if len(df_names)
        else {}
    )

    def esc(x) -> str:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return "—"
        return html.escape(str(x))

    rows_bad = ""
    for _, r in df_bad.iterrows():
        pid = r.get("provider_id")
        pname = pid_to_name.get(pid, "—")
        rows_bad += (
            "<tr>"
            f"<td class='mono'>{int(r['order_id'])}</td>"
            f"<td>{esc(pname)}</td>"
            f"<td><span class='tag tag-fail'>{esc(r.get('final_state'))}</span></td>"
            f"<td>{esc(r.get('from_state'))}</td>"
            f"<td>{esc(r.get('to_state'))}</td>"
            f"<td class='reason'>{esc(human_reason(r))}</td>"
            f"<td class='mono'>{esc(r.get('order_created'))}</td>"
            "</tr>"
        )

    rows_low = ""
    for _, r in df_low.iterrows():
        pid = r.get("provider_id")
        pname = pid_to_name.get(pid, "—")
        c = r.get("comment")
        com = esc(c) if c is not None and str(c).strip() else "—"
        rows_low += (
            "<tr>"
            f"<td class='mono'>{int(r['order_id'])}</td>"
            f"<td>{esc(pname)}</td>"
            f"<td><span class='tag tag-warn'>{int(r['rating_value'])}</span></td>"
            f"<td>{com}</td>"
            f"<td class='mono'>{esc(r.get('order_created'))}</td>"
            f"<td class='mono'>{esc(r.get('rating_created'))}</td>"
            "</tr>"
        )

    if not rows_bad:
        rows_bad = "<tr><td colspan='7' class='muted'>Немає не доставлених за цей період.</td></tr>"
    if not rows_low:
        rows_low = "<tr><td colspan='6' class='muted'>Немає оцінок ≤2 для доставлених замовлень.</td></tr>"

    title = f"Josper Svintuz — погані замовлення, {week_start:%d.%m.%Y}–{week_end:%d.%m.%Y}"

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{esc(title)}</title>
  <style>
    :root {{
      --bg:#0f1419; --card:#1a2332; --border:#2d3a4f; --text:#e8eef7;
      --muted:#8b9cb3; --accent:#4fd1c5; --fail:#f56565; --warn:#ed8936; --ok:#48bb78;
    }}
    * {{ box-sizing:border-box; margin:0; padding:0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: var(--bg); color: var(--text); line-height: 1.5; padding: 2rem 1.25rem 3rem;
    }}
    .wrap {{ max-width: 1100px; margin: 0 auto; }}
    h1 {{ font-size: 1.35rem; font-weight: 700; color: var(--accent); margin-bottom: .35rem; }}
    .meta {{ color: var(--muted); font-size: .85rem; margin-bottom: 1.75rem; }}
    .kpi-row {{ display:flex; flex-wrap:wrap; gap:1rem; margin-bottom:2rem; }}
    .kpi {{
      background: var(--card); border: 1px solid var(--border); border-radius: 12px;
      padding: 1rem 1.25rem; min-width: 140px;
    }}
    .kpi .n {{ font-size: 1.75rem; font-weight: 800; }}
    .kpi .l {{ font-size: .75rem; color: var(--muted); text-transform: uppercase; letter-spacing:.04em; }}
    h2 {{
      font-size: 1.05rem; margin: 2rem 0 .75rem; color: var(--text);
      border-left: 4px solid var(--fail); padding-left: .65rem;
    }}
    h2.sec-deliv {{ border-left-color: var(--warn); }}
    .hint {{ font-size: .8rem; color: var(--muted); margin-bottom: 1rem; }}
    .table-wrap {{
      background: var(--card); border: 1px solid var(--border); border-radius: 12px;
      overflow: auto;
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: .82rem; }}
    th {{
      text-align: left; padding: .65rem .85rem; background: #243044; color: var(--muted);
      font-weight: 600; white-space: nowrap; border-bottom: 1px solid var(--border);
    }}
    td {{
      padding: .55rem .85rem; border-bottom: 1px solid var(--border); vertical-align: top;
    }}
    tr:last-child td {{ border-bottom: none; }}
    tbody tr:hover {{ background: rgba(79,209,197,.06); }}
    .mono {{ font-family: ui-monospace, monospace; font-size: .78rem; }}
    .reason {{ max-width: 420px; }}
    .tag {{
      display:inline-block; padding: 2px 8px; border-radius: 6px; font-weight: 700;
      font-size: .75rem;
    }}
    .tag-fail {{ background: rgba(245,101,101,.2); color: var(--fail); }}
    .tag-warn {{ background: rgba(237,137,54,.2); color: var(--warn); }}
    .muted {{ color: var(--muted); text-align: center; padding: 1.25rem !important; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>{esc(title)}</h1>
    <div class="meta">Запоріжжя · vendor_id {VENDOR_ID} · згенеровано: {esc(generated_at)} UTC</div>

    <div class="kpi-row">
      <div class="kpi"><div class="n">{len(df_bad)}</div><div class="l">Не доставлено</div></div>
      <div class="kpi"><div class="n">{len(df_low)}</div><div class="l">Доставлено, оцінка ≤2</div></div>
    </div>

    <h2>Не доставлені замовлення (failed / rejected)</h2>
    <p class="hint">
      Причина сформована з фінального статусу, останнього переходу в журналі станів
      та полів <code>fact_order_delivery</code> (відмови кур’єра, тікет клієнта).
    </p>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Order ID</th><th>Локація</th><th>Статус</th><th>З етапу</th><th>На етап</th>
            <th>Причина / пояснення</th><th>Час замовлення</th>
          </tr>
        </thead>
        <tbody>{rows_bad}</tbody>
      </table>
    </div>

    <h2 class="sec-deliv">Доставлено, але клієнт незадоволений (оцінка ≤2)</h2>
    <p class="hint">Лише замовлення зі статусом <code>delivered</code>; коментар з опитування, якщо є.</p>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Order ID</th><th>Локація</th><th>Зірки</th><th>Коментар</th>
            <th>Час замовлення</th><th>Час оцінки</th>
          </tr>
        </thead>
        <tbody>{rows_low}</tbody>
      </table>
    </div>
  </div>
</body>
</html>
"""


def main() -> None:
    week_start, week_end = week_bounds()
    d0, d1 = week_start.isoformat(), week_end.isoformat()
    pids = ", ".join(str(p) for p in PROVIDER_IDS)

    sql_names = f"""
    SELECT provider_id, provider_name
    FROM ng_delivery_spark.dim_provider_v2
    WHERE provider_id IN ({pids})
    """

    sql_bad_orders = f"""
    SELECT o.id AS order_id, o.state AS final_state, o.created AS order_created, o.provider_id,
           f.is_rejected_by_provider, f.number_courier_rejects, f.has_eater_cancellation_ticket
    FROM ng_delivery_spark.delivery_order_order o
    LEFT JOIN ng_delivery_spark.fact_order_delivery f ON f.order_id = o.id
    WHERE o.provider_id IN ({pids})
      AND o.created_date BETWEEN '{d0}' AND '{d1}'
      AND o.state IN ('failed', 'rejected')
    ORDER BY o.created
    """

    sql_low = f"""
    SELECT r.order_id, r.provider_id, r.rating_value, r.comment,
           r.created AS rating_created, o.created AS order_created
    FROM ng_delivery_spark.delivery_rating_provider_rating_history r
    INNER JOIN ng_delivery_spark.delivery_order_order o ON o.id = r.order_id
    WHERE r.provider_id IN ({pids})
      AND o.created_date BETWEEN '{d0}' AND '{d1}'
      AND o.state = 'delivered'
      AND r.created_date BETWEEN DATE_SUB('{d0}', 2) AND DATE_ADD('{d1}', 5)
      AND r.rating_value <= 2
      AND (r.ignore_rating IS NULL OR r.ignore_rating = false)
    ORDER BY r.rating_value, r.created
    """

    print(f"Week {d0} .. {d1} — connecting…")
    conn = sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=get_token(),
    )
    try:
        df_names = run_query(conn, sql_names)
        df_bad_base = run_query(conn, sql_bad_orders)
        if len(df_bad_base) == 0:
            df_bad = pd.DataFrame(
                columns=[
                    "order_id",
                    "final_state",
                    "order_created",
                    "provider_id",
                    "from_state",
                    "to_state",
                    "event_state",
                    "is_rejected_by_provider",
                    "number_courier_rejects",
                    "has_eater_cancellation_ticket",
                ]
            )
        else:
            ids = ",".join(str(int(x)) for x in df_bad_base["order_id"].tolist())
            sql_last_log = f"""
            SELECT order_id, from_state, to_state, event_state
            FROM (
              SELECT order_id, from_state, to_state, event_state,
                     ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created DESC) AS rn
              FROM ng_delivery_spark.delivery_order_order_state_log
              WHERE order_id IN ({ids})
                AND created_date >= DATE_SUB('{d0}', 5)
                AND created_date <= DATE_ADD('{d1}', 8)
            ) t
            WHERE rn = 1
            """
            df_log = run_query(conn, sql_last_log)
            df_bad = df_bad_base.merge(df_log, on="order_id", how="left")
        df_low = run_query(conn, sql_low)
        if len(df_low):
            df_low = (
                df_low.sort_values(["order_id", "rating_value", "rating_created"])
                .groupby("order_id", as_index=False)
                .first()
            )
    finally:
        conn.close()

    generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    out = build_html(week_start, week_end, df_bad, df_low, df_names, generated_at)
    OUTPUT_HTML.write_text(out, encoding="utf-8")
    print(f"Written {OUTPUT_HTML} ({len(df_bad)} bad, {len(df_low)} low-rating delivered)")


if __name__ == "__main__":
    main()
