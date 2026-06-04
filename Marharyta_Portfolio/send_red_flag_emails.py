"""
send_red_flag_emails.py
Runs after generate_portfolio_report.py.
Identifies top-5 red-flag brands per city and sends personalised
Ukrainian-language emails to partner contacts via Gmail SMTP.

Required environment variables:
    DATABRICKS_TOKEN          – Databricks personal access token
    GMAIL_USER                – sender address, e.g. marharyta.zhytnyk@gmail.com
    GMAIL_APP_PASSWORD        – Google App Password (16 chars, no spaces)
    AM_EMAIL  (optional)      – if set, BCC every email to the AM herself
"""

import os
import sys
import time
import math
import smtplib
import textwrap
import requests
import pandas as pd
from datetime import date, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://bolt-incentives.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
CLUSTER_ID      = os.getenv("DATABRICKS_CLUSTER_ID", "0221-081903-9ag4bh69")

GMAIL_USER     = os.getenv("GMAIL_USER", "")
GMAIL_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
AM_EMAIL       = os.getenv("AM_EMAIL", "")          # BCC (or TEST recipient)
# When TEST_MODE=true — emails go ONLY to AM_EMAIL, not to actual partners
TEST_MODE      = os.getenv("TEST_MODE", "false").lower() == "true"

ACCOUNT_MANAGER = "Marharyta Zhytnyk"
COUNTRY_CODE    = "ua"
TOP_N           = 5      # max brands per red-flag category to email
REPORT_DATE     = date.today().isoformat()

# ─── DATE HELPERS ──────────────────────────────────────────────────────────────

def get_last_n_full_weeks(n: int = 4):
    today = date.today()
    days_since_sunday = today.weekday() + 1
    last_sunday = today - timedelta(days=days_since_sunday)
    start_monday = last_sunday - timedelta(days=(n * 7) - 1)
    return start_monday.isoformat(), last_sunday.isoformat()


# ─── DATABRICKS ────────────────────────────────────────────────────────────────

def _headers():
    return {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}


def _exec_sql(sql: str, timeout: int = 300) -> pd.DataFrame:
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/1.2/contexts/create",
        headers=_headers(),
        json={"language": "sql", "clusterId": CLUSTER_ID},
    )
    resp.raise_for_status()
    ctx = resp.json()["id"]

    resp2 = requests.post(
        f"{DATABRICKS_HOST}/api/1.2/commands/execute",
        headers=_headers(),
        json={"language": "sql", "clusterId": CLUSTER_ID, "contextId": ctx, "command": sql},
    )
    resp2.raise_for_status()
    cmd_id = resp2.json()["id"]

    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(
            f"{DATABRICKS_HOST}/api/1.2/commands/status",
            headers=_headers(),
            params={"clusterId": CLUSTER_ID, "contextId": ctx, "commandId": cmd_id},
        )
        r.raise_for_status()
        data = r.json()
        if data.get("status") == "Finished":
            res = data.get("results", {})
            cols = [c["name"] for c in res.get("schema", [])]
            return pd.DataFrame(res.get("data", []), columns=cols)
        if data.get("status") in ("Error", "Cancelled"):
            raise RuntimeError(data.get("results", {}).get("summary", "SQL error"))
        time.sleep(5)
    raise TimeoutError("Query timed out")


# ─── DATA FETCH ────────────────────────────────────────────────────────────────

def fetch_summary() -> pd.DataFrame:
    start_date, end_date = get_last_n_full_weeks(4)
    # Also fetch the PREVIOUS 4 weeks for WoW GMV
    prev_start, prev_end = get_last_n_full_weeks(8)

    sql = f"""
    WITH current_period AS (
        SELECT
            p.brand_name, p.city_name,
            MIN(COALESCE(p.owner_email, p.provider_email)) AS contact_email,
            SUM(f.delivered_orders_count)         AS delivered_orders,
            SUM(f.failed_orders_count)             AS failed_orders,
            SUM(f.total_gmv_before_discounts_eur)  AS gmv_eur,
            SUM(f.total_contribution_profit_eur)   AS contribution_profit_eur,
            CASE WHEN SUM(f.total_gmv_before_discounts_eur) > 0
                 THEN SUM(f.total_contribution_profit_eur)
                      / SUM(f.total_gmv_before_discounts_eur) * 100
                 ELSE NULL END AS cp_l2_margin_pct,
            SUM(f.bad_order_rate_value      * f.delivered_orders_count)
                / NULLIF(SUM(f.delivered_orders_count), 0) AS bad_order_rate,
            SUM(f.failed_order_rate_value   * f.delivered_orders_count)
                / NULLIF(SUM(f.delivered_orders_count), 0) AS failed_order_rate,
            SUM(f.provider_acceptance_rate_value * f.delivered_orders_count)
                / NULLIF(SUM(f.delivered_orders_count), 0) AS acceptance_rate,
            SUM(f.late_delivery_order_rate_value * f.delivered_orders_count)
                / NULLIF(SUM(f.delivered_orders_count), 0) AS late_delivery_rate,
            SUM(f.provider_active_rate_value     * f.delivered_orders_count)
                / NULLIF(SUM(f.delivered_orders_count), 0) AS active_rate,
            COUNT(DISTINCT p.provider_id) AS locations_count
        FROM ng_delivery_spark.dim_provider_v2 p
        INNER JOIN ng_delivery_spark.fact_provider_weekly f ON p.provider_id = f.provider_id
        WHERE p.account_manager_name = '{ACCOUNT_MANAGER}'
          AND p.country_code = '{COUNTRY_CODE}'
          AND p.provider_status = 'active'
          AND CAST(f.metric_timestamp_local AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY p.brand_name, p.city_name
    ),
    prev_period AS (
        SELECT p.brand_name, p.city_name,
               SUM(f.total_gmv_before_discounts_eur) AS prev_gmv_eur
        FROM ng_delivery_spark.dim_provider_v2 p
        INNER JOIN ng_delivery_spark.fact_provider_weekly f ON p.provider_id = f.provider_id
        WHERE p.account_manager_name = '{ACCOUNT_MANAGER}'
          AND p.country_code = '{COUNTRY_CODE}'
          AND p.provider_status = 'active'
          AND CAST(f.metric_timestamp_local AS DATE) BETWEEN '{prev_start}' AND '{prev_end}'
          AND CAST(f.metric_timestamp_local AS DATE) < '{start_date}'
        GROUP BY p.brand_name, p.city_name
    )
    SELECT c.*, pp.prev_gmv_eur
    FROM current_period c
    LEFT JOIN prev_period pp ON c.brand_name = pp.brand_name AND c.city_name = pp.city_name
    ORDER BY c.city_name, c.gmv_eur DESC
    """
    print("Fetching brand summary for email notifications...")
    df = _exec_sql(sql)
    for col in ["delivered_orders", "failed_orders", "gmv_eur", "prev_gmv_eur",
                "contribution_profit_eur", "cp_l2_margin_pct", "bad_order_rate",
                "failed_order_rate", "acceptance_rate", "late_delivery_rate",
                "active_rate", "locations_count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    print(f"  → {len(df)} brand-city rows")
    return df


# ─── RED FLAG SCORING ──────────────────────────────────────────────────────────

def score_row(row) -> dict:
    """Returns dict with flags and a severity score (higher = worse)."""
    flags = []
    score = 0

    avail = float(row.get("active_rate") or 0) * 100
    if 0 < avail < 95:
        flags.append(("availability", f"Availability {avail:.1f}% (норма ≥95%)"))
        score += (95 - avail) * 2

    failed_abs = float(row.get("failed_orders") or 0)
    if failed_abs > 2:
        flags.append(("failed", f"{int(failed_abs)} зафейлених замовлень"))
        score += failed_abs * 3

    cp = float(row.get("cp_l2_margin_pct") or 0)
    if cp < 0:
        flags.append(("cp", f"CP L2 Margin {cp:.1f}%"))
        score += abs(cp) * 1.5

    gmv = float(row.get("gmv_eur") or 0)
    prev_gmv = float(row.get("prev_gmv_eur") or 0)
    gmv_wow = None
    if prev_gmv > 0 and not (math.isnan(gmv) or math.isnan(prev_gmv)):
        gmv_wow = (gmv / prev_gmv - 1) * 100
        if gmv_wow < -1:
            flags.append(("gmv_drop", f"GMV впав на {abs(gmv_wow):.1f}% WoW"))
            score += abs(gmv_wow) * 1

    return {"flags": flags, "score": score, "gmv_wow": gmv_wow}


# ─── EMAIL BUILDER ─────────────────────────────────────────────────────────────

EMAIL_COLORS = {
    "availability": "#E53935",
    "failed":       "#FB8C00",
    "cp":           "#7B1FA2",
    "gmv_drop":     "#1976D2",
}

FLAG_LABELS = {
    "availability": "🔴 Низька доступність",
    "failed":       "🟠 Зафейлені замовлення",
    "cp":           "🟣 Від'ємна маржа CP L2",
    "gmv_drop":     "📉 Падіння GMV",
}

FIXES = {
    "availability": "Перевірте налаштування графіку роботи та доступності у Partner Portal.",
    "failed":       "Перевірте причини відмов від замовлень: меню, доступність страв, coverage.",
    "cp":           "Зверніться до Account Manager для обговорення умов та оптимізації показників.",
    "gmv_drop":     "Перевірте активність промо-акцій та видимість закладу на платформі.",
}


def build_email_html(brand: str, city: str, flags: list, row: dict) -> str:
    orders    = int(float(row.get("delivered_orders") or 0))
    gmv       = float(row.get("gmv_eur") or 0)
    cp_margin = float(row.get("cp_l2_margin_pct") or 0)
    avail     = float(row.get("active_rate") or 0) * 100

    flags_html = ""
    for flag_type, flag_text in flags:
        color = EMAIL_COLORS.get(flag_type, "#E53935")
        label = FLAG_LABELS.get(flag_type, flag_type)
        fix   = FIXES.get(flag_type, "")
        flags_html += f"""
        <tr>
          <td style="padding:10px 16px;border-left:4px solid {color};
                     background:#fafafa;border-radius:0 6px 6px 0;margin:6px 0;
                     display:block">
            <div style="font-weight:700;color:{color};font-size:13px">{label}</div>
            <div style="color:#444;font-size:12px;margin-top:3px">{flag_text}</div>
            <div style="color:#2E7D32;font-size:11px;margin-top:4px;font-style:italic">
              → {fix}
            </div>
          </td>
        </tr>
        <tr><td style="height:8px"></td></tr>"""

    return f"""<!DOCTYPE html>
<html lang="uk">
<head><meta charset="UTF-8"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;
             background:#F7F9FC;margin:0;padding:20px">
  <table width="600" cellpadding="0" cellspacing="0"
         style="margin:0 auto;background:#fff;border-radius:12px;
                box-shadow:0 2px 12px rgba(0,0,0,0.08);overflow:hidden">
    <!-- HEADER -->
    <tr>
      <td style="background:#1A1A1A;padding:20px 28px">
        <span style="font-size:20px;font-weight:800;color:#1DC462">⚡ Bolt Food</span>
        <span style="color:#aaa;font-size:12px;margin-left:12px">Щотижневий звіт</span>
      </td>
    </tr>
    <!-- BODY -->
    <tr>
      <td style="padding:24px 28px">
        <h2 style="margin:0 0 6px;font-size:18px;color:#1A1A1A">Показники вашого закладу</h2>
        <p style="margin:0 0 20px;color:#666;font-size:13px">
          Звітний тиждень: {REPORT_DATE} · Місто: {city}
        </p>

        <!-- METRICS SUMMARY -->
        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px">
          <tr>
            <td style="background:#F7F9FC;border-radius:8px;padding:12px 16px;
                       text-align:center;width:33%">
              <div style="font-size:20px;font-weight:800;color:#1A1A1A">{orders:,}</div>
              <div style="font-size:11px;color:#666;text-transform:uppercase">Замовлень</div>
            </td>
            <td width="8"></td>
            <td style="background:#F7F9FC;border-radius:8px;padding:12px 16px;
                       text-align:center;width:33%">
              <div style="font-size:20px;font-weight:800;color:#1A1A1A">€{gmv:,.0f}</div>
              <div style="font-size:11px;color:#666;text-transform:uppercase">GMV</div>
            </td>
            <td width="8"></td>
            <td style="background:#F7F9FC;border-radius:8px;padding:12px 16px;
                       text-align:center;width:33%">
              <div style="font-size:20px;font-weight:800;
                          color:{'#2E7D32' if cp_margin >= 0 else '#E53935'}">
                {cp_margin:.1f}%
              </div>
              <div style="font-size:11px;color:#666;text-transform:uppercase">CP L2 Маржа</div>
            </td>
          </tr>
        </table>

        <!-- FLAGS -->
        <div style="background:#FFF8F8;border:1px solid #FFCDD2;border-radius:8px;
                    padding:14px 16px;margin-bottom:20px">
          <div style="font-weight:700;color:#C62828;font-size:13px;margin-bottom:12px">
            🚩 Виявлені проблеми, що потребують уваги:
          </div>
          <table width="100%" cellpadding="0" cellspacing="0">
            {flags_html}
          </table>
        </div>

        <!-- CTA -->
        <p style="font-size:13px;color:#444;margin:0 0 16px">
          Ваш Account Manager <strong>{ACCOUNT_MANAGER}</strong> готовий допомогти
          з оптимізацією показників. Зверніться, якщо виникнуть запитання.
        </p>
        <a href="https://food.bolt.eu/partner"
           style="display:inline-block;background:#1DC462;color:#fff;
                  padding:10px 22px;border-radius:8px;text-decoration:none;
                  font-weight:700;font-size:13px">
          Відкрити Partner Portal →
        </a>
      </td>
    </tr>
    <!-- FOOTER -->
    <tr>
      <td style="background:#F5F5F5;padding:14px 28px;
                 font-size:11px;color:#999;text-align:center">
        Bolt Food · Автоматичний тижневий звіт · {REPORT_DATE}<br>
        Якщо у вас є запитання — зверніться до вашого AM: {ACCOUNT_MANAGER}
      </td>
    </tr>
  </table>
</body>
</html>"""


def build_email_text(brand: str, flags: list) -> str:
    lines = [
        f"Bolt Food — Тижневий звіт: {brand}",
        f"Дата: {REPORT_DATE}",
        "",
        "Виявлені проблеми:",
    ]
    for flag_type, flag_text in flags:
        label = FLAG_LABELS.get(flag_type, flag_type)
        fix   = FIXES.get(flag_type, "")
        lines.append(f"  {label}: {flag_text}")
        lines.append(f"  → {fix}")
        lines.append("")
    lines += [
        f"Ваш Account Manager {ACCOUNT_MANAGER} готовий допомогти.",
        "Bolt Food Team",
    ]
    return "\n".join(lines)


# ─── EMAIL SENDING ─────────────────────────────────────────────────────────────

def send_email(to_addr: str, subject: str, html_body: str, plain_body: str,
               bcc: str = "") -> bool:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"Bolt Food Notifications <{GMAIL_USER}>"
    msg["To"]      = to_addr
    if bcc:
        msg["Bcc"] = bcc

    msg.attach(MIMEText(plain_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body,  "html",  "utf-8"))

    recipients = [to_addr] + ([bcc] if bcc else [])

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_USER, recipients, msg.as_string())
        return True
    except Exception as exc:
        print(f"    ✗ Failed to send to {to_addr}: {exc}")
        return False


# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n=== Red Flag Email Notifications [{REPORT_DATE}] ===\n")

    if not DATABRICKS_TOKEN:
        print("ERROR: DATABRICKS_TOKEN not set"); sys.exit(1)
    if not GMAIL_USER or not GMAIL_PASSWORD:
        print("ERROR: GMAIL_USER or GMAIL_APP_PASSWORD not set"); sys.exit(1)

    if TEST_MODE:
        if not AM_EMAIL:
            print("ERROR: TEST_MODE=true but AM_EMAIL not set — nowhere to send test emails")
            sys.exit(1)
        print(f"⚠️  TEST MODE — всі листи підуть тільки на {AM_EMAIL} (не реальним партнерам)\n")

    df = fetch_summary()

    # Score every row and pick worst TOP_N per city
    scored_rows = []
    for _, row in df.iterrows():
        s = score_row(row)
        if s["flags"]:
            scored_rows.append({**row.to_dict(), **s})

    if not scored_rows:
        print("✅ No red flags detected — no emails to send.")
        return

    # Sort by severity score (worst first) globally, take TOP_N
    scored_rows.sort(key=lambda x: x["score"], reverse=True)
    to_notify = scored_rows[:TOP_N]

    print(f"Found {len(scored_rows)} brands with red flags. Notifying top {len(to_notify)}:\n")

    sent = 0
    skipped = 0
    for item in to_notify:
        brand  = str(item.get("brand_name") or "—")
        city   = str(item.get("city_name") or "—")
        flags  = item["flags"]
        # Prefer owner_email, fallback to provider_email
        email  = str(item.get("owner_email") or item.get("provider_email") or "").strip()

        flag_summary = ", ".join(f for _, f in flags[:2])
        print(f"  [{brand} / {city}] score={item['score']:.1f} | {flag_summary}")

        if TEST_MODE:
            # In test mode — send to AM only, use partner email only as label
            actual_to = AM_EMAIL
            email_label = f"{email or 'немає email'} → ТЕСТ: надіслано на {AM_EMAIL}"
        else:
            if not email or email.lower() in ("none", "nan", ""):
                print(f"    ⚠ No contact email found — skipping")
                skipped += 1
                continue
            actual_to   = email
            email_label = email

        subject = (
            f"[ТЕСТ] Bolt Food — {brand} / {city} — Red Flag ({REPORT_DATE})"
            if TEST_MODE else
            f"Bolt Food — Увага! Показники закладу потребують уваги ({REPORT_DATE})"
        )
        html_body  = build_email_html(brand, city, flags, item)
        plain_body = build_email_text(brand, flags)

        ok = send_email(actual_to, subject, html_body, plain_body,
                        bcc="" if TEST_MODE else AM_EMAIL)
        if ok:
            print(f"    ✓ Sent to {email_label}")
            sent += 1
        # Small delay between emails
        time.sleep(1)

    print(f"\n✅ Done — sent: {sent}, skipped (no email): {skipped}, total flagged: {len(scored_rows)}")


if __name__ == "__main__":
    main()
