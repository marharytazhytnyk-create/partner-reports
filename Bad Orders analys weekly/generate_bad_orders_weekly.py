#!/usr/bin/env python3
"""
Marharyta Zhytnyk Portfolio — щотижневий аналіз Bad Orders / Failed Orders.

Джерело: Databricks SQL (ng_delivery_spark).
Тиждень за замовчуванням — останній повний календарний тиждень (пн–нд).
Можна передати BAD_ORDERS_WEEK_START=YYYY-MM-DD або згенерувати кілька тижнів.
"""

from __future__ import annotations

import html
import json
import os
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from databricks import sql

ACCOUNT_MANAGER = "Marharyta Zhytnyk"
COUNTRY_CODE = "ua"

SERVER_HOSTNAME = "bolt-incentives.cloud.databricks.com"
HTTP_PATH = "sql/protocolv1/o/2472566184436351/0221-081903-9ag4bh69"

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_HTML = SCRIPT_DIR / "bad_orders_weekly.html"

CITY_UA = {
    "Bila Tserkva": "Біла Церква",
    "Boryspil": "Бориспіль",
    "Cherkasy": "Черкаси",
    "Chernihiv": "Чернігів",
    "Chernivtsi": "Чернівці",
    "Dnipro": "Дніпро",
    "Kharkiv": "Харків",
    "Kremenchuk": "Кременчук",
    "Kryvyi Rih": "Кривий Ріг",
    "Kyiv": "Київ",
    "Lviv": "Львів",
    "Odesa": "Одеса",
    "Oleksandriia": "Олександрія",
    "Pavlohrad": "Павлоград",
    "Poltava": "\u041f\u043e\u043b\u0442\u0430\u0432\u0430",
    "Sumy": "Суми",
    "Vinnytsia": "Вінниця",
    "Zaporizhia": "Запоріжжя",
    "Zhytomyr": "Житомир",
}

ACTOR_UA = {
    "bolt": "Bolt (платформа)",
    "courier": "Кур'єр",
    "provider": "Заклад",
    "eater": "Клієнт",
    "client": "Клієнт",
    "unknown": "Невизначено",
    None: "Невизначено",
    "": "Невизначено",
}

REASON_UA = {
    "bolt_cooking_eta_underestimate_seconds": "Bolt занизив ETA приготування",
    "bolt_assignment_delay_from_supply_starvation_seconds": "Bolt: затримка призначення кур'єра (дефіцит кур'єрів)",
    "bolt_assignment_delay_from_rejections_seconds": "Bolt: затримка через відмови кур'єрів",
    "bolt_batching_delay_seconds": "Bolt: затримка через батчинг замовлень",
    "bolt_dispatch_start_delay_seconds": "Bolt: затримка старту диспетчеризації",
    "bolt_prep_instruction_delay_seconds": "Bolt: затримка інструкцій для закладу",
    "courier_to_provider_eta_error_seconds": "Кур'єр: помилка ETA до закладу",
    "provider_to_eater_eta_error_seconds": "Кур'єр: помилка ETA до клієнта",
    "courier_redispatch_duration_seconds": "Кур'єр: повторна диспетчеризація",
    "pickup_delay_courier_fault_seconds": "Кур'єр: затримка на pickup",
    "courier_dropoff_delay_adjusted_seconds": "Кур'єр: затримка на доставці",
    "order_never_delivered_eater": "Замовлення не доставлено клієнту",
    "provider_preparation_delay_seconds": "Заклад: затримка приготування",
    "provider_preparation_overestimate_seconds": "Заклад: переоцінка часу приготування",
    "pickup_delay_provider_fault_seconds": "Заклад: затримка на видачі",
    "did_not_respond": "Заклад: не відповів на замовлення",
    "missing_item_eater": "Заклад: відсутня позиція в замовленні",
    "items_out_of_stock": "Заклад: позиції немає в наявності",
    "too_many_orders": "Заклад: занадто багато замовлень",
    "closed": "Заклад: закритий",
    "do_not_wish_to_serve_this_client": "Заклад: відмова обслуговувати клієнта",
    "wrong_item_eater": "Заклад: неправильна позиція",
    "item_had_a_spoiled_taste_or_smell_eater": "Поганий смак/запах страви",
    "manually_failed_by_cs": "Скасовано службою підтримки",
    "order_damaged_eater": "Пошкоджене замовлення",
    "order_took_longer_eater": "Замовлення зайняло більше часу",
    "unknown": "Невизначена причина",
    None: "Без деталізації",
}


def get_token() -> str:
    token = os.environ.get("DATABRICKS_TOKEN")
    if token:
        return token
    for env_path in (
        Path(__file__).resolve().parent.parent / "databricks-setup" / ".env",
        Path.home()
        / "Library"
        / "CloudStorage"
        / "GoogleDrive-marharyta.zhytnyk@bolt.eu"
        / "My Drive"
        / "Events project"
        / "databricks-setup"
        / ".env",
    ):
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("DATABRICKS_TOKEN="):
                    return line.split("=", 1)[1].strip()
    raise RuntimeError("DATABRICKS_TOKEN not found")


def week_bounds(week_start: date | None = None) -> tuple[date, date]:
    if week_start:
        return week_start, week_start + timedelta(days=6)
    if os.environ.get("BAD_ORDERS_WEEK_START"):
        start = date.fromisoformat(os.environ["BAD_ORDERS_WEEK_START"])
        return start, start + timedelta(days=6)
    today = date.today()
    end = today if today.weekday() == 6 else today - timedelta(days=today.weekday() + 1)
    return end - timedelta(days=6), end


def city_ua(name: str | None) -> str:
    if not name:
        return "—"
    return CITY_UA.get(name, name)


def reason_ua(code) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return REASON_UA[None]
    return REASON_UA.get(str(code), str(code).replace("_", " "))


def actor_ua(code) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return ACTOR_UA[None]
    return ACTOR_UA.get(str(code).lower(), str(code))


def classify_failed(row: pd.Series) -> str:
    state = str(row.get("final_state") or "")
    if state == "rejected" or row.get("is_rejected_by_provider") is True:
        return "provider"
    ncr = int(row.get("number_courier_rejects") or 0)
    if state == "failed" and ncr > 0:
        return "courier"
    if row.get("has_eater_cancellation_ticket") is True:
        return "client"
    return "bolt"


def failed_detail_ua(row: pd.Series) -> str:
    state = str(row.get("final_state") or "")
    from_st = str(row.get("from_state") or "") or "—"
    ncr = int(row.get("number_courier_rejects") or 0)
    fault = classify_failed(row)
    if state == "rejected":
        return f"Замовлення відхилено закладом (етап: {from_st})"
    if state == "failed":
        parts = []
        stage = {
            "waiting_delivery": "зрив на етапі доставки",
            "waiting_preparation": "зрив під час приготування",
            "waiting_acceptance": "зрив після прийняття",
        }.get(from_st, f"зрив з етапу «{from_st}»")
        parts.append(stage)
        if ncr:
            parts.append(f"відмови кур'єра: {ncr}")
        if row.get("has_eater_cancellation_ticket") is True:
            parts.append("є скасування з боку клієнта")
        if row.get("is_rejected_by_provider") is True:
            parts.append("позначено як відхилення закладом")
        return "Failed: " + "; ".join(parts)
    return state


def bad_comment_ua(row: pd.Series) -> str:
    parts = []
    rating = row.get("order_food_rating_value")
    if rating is not None and not (isinstance(rating, float) and pd.isna(rating)):
        rv = float(rating)
        if rv <= 2:
            parts.append(f"Оцінка їжі: {int(rv)}/5")
    late = row.get("late_delivery_actor_at_fault_reason")
    if late is not None and not (isinstance(late, float) and pd.isna(late)):
        parts.append(reason_ua(str(late).replace("_seconds", "")))
    return "; ".join(parts) if parts else "—"


FAULT_REASON_UA = {
    "provider": "Відхилення або відмова закладу",
    "courier": "Відмова кур'єра під час доставки",
    "client": "Скасування з боку клієнта",
    "bolt": "Зрив на стороні платформи",
}


def run_query(conn, q: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(q)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def fetch_week_data(conn, week_start: date, week_end: date) -> dict:
    d0, d1 = week_start.isoformat(), week_end.isoformat()
    print(f"  Fetching {d0} .. {d1} …")

    sql_summary = f"""
    SELECT
        p.brand_name,
        p.city_name,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN o.state = 'delivered' THEN 1 ELSE 0 END) AS delivered,
        SUM(CASE WHEN f.is_bad_order = true OR o.state IN ('failed','rejected') THEN 1 ELSE 0 END) AS bad_count,
        SUM(CASE WHEN o.state IN ('failed','rejected') THEN 1 ELSE 0 END) AS failed_count
    FROM ng_delivery_spark.delivery_order_order o
    INNER JOIN ng_delivery_spark.fact_order_delivery f ON f.order_id = o.id
    INNER JOIN ng_delivery_spark.dim_provider_v2 p ON p.provider_id = o.provider_id
    WHERE p.account_manager_name = '{ACCOUNT_MANAGER}'
      AND p.country_code = '{COUNTRY_CODE}'
      AND o.created_date BETWEEN '{d0}' AND '{d1}'
    GROUP BY p.brand_name, p.city_name
    """

    sql_orders = f"""
    SELECT
        o.id AS order_id,
        o.reference_id AS order_ref,
        o.state AS final_state,
        o.created AS order_created,
        p.provider_id,
        p.provider_name,
        p.brand_name,
        p.city_name,
        f.is_bad_order,
        f.is_rejected_by_provider,
        f.number_courier_rejects,
        f.has_eater_cancellation_ticket,
        f.order_food_rating_value,
        a.bad_order_actor_at_fault,
        a.bad_order_main_reason,
        a.late_delivery_actor_at_fault_reason
    FROM ng_delivery_spark.delivery_order_order o
    INNER JOIN ng_delivery_spark.fact_order_delivery f ON f.order_id = o.id
    INNER JOIN ng_delivery_spark.dim_provider_v2 p ON p.provider_id = o.provider_id
    LEFT JOIN ng_delivery_spark.int_order_bad_order_attribution a ON a.order_id = o.id
    WHERE p.account_manager_name = '{ACCOUNT_MANAGER}'
      AND p.country_code = '{COUNTRY_CODE}'
      AND o.created_date BETWEEN '{d0}' AND '{d1}'
      AND (
        f.is_bad_order = true
        OR o.state IN ('failed', 'rejected')
      )
    ORDER BY p.city_name, p.brand_name, o.created
    """

    df_summary = run_query(conn, sql_summary)
    df_orders = run_query(conn, sql_orders)

    if len(df_orders):
        ids = ",".join(str(int(x)) for x in df_orders["order_id"].tolist())
        sql_log = f"""
        SELECT order_id, from_state, to_state
        FROM (
          SELECT order_id, from_state, to_state,
                 ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created DESC) AS rn
          FROM ng_delivery_spark.delivery_order_order_state_log
          WHERE order_id IN ({ids})
            AND created_date >= DATE_SUB('{d0}', 5)
            AND created_date <= DATE_ADD('{d1}', 8)
        ) t WHERE rn = 1
        """
        df_log = run_query(conn, sql_log)
        df_orders = df_orders.merge(df_log, on="order_id", how="left")
    else:
        df_orders["from_state"] = None
        df_orders["to_state"] = None

    return build_week_payload(week_start, week_end, df_summary, df_orders)


def prep_time_window() -> tuple[date, date]:
    """Останні 2 повні календарні тижні (пн–нд), закінчуючись минулою неділею."""
    today = date.today()
    last_sunday = today - timedelta(days=today.weekday() + 1)
    return last_sunday - timedelta(days=13), last_sunday


def empty_prep_payload() -> dict:
    return {
        "period_start": "",
        "period_end": "",
        "label": "",
        "rows": [],
    }


def recommend_cooking_time_min(actual: float | None) -> int | None:
    """Рекомендований cooking time у системі: actual, округлений до 5 хв."""
    if actual is None:
        return None
    # Крок 5 хв — типовий для налаштування cooking time в Bolt
    rounded = int(round(max(5.0, float(actual)) / 5.0) * 5)
    return min(rounded, 90)


def fetch_prep_time_data(conn) -> dict:
    """Actual vs estimated preparation time по провайдерах портфоліо за останні 2 тижні."""
    d0, d1 = prep_time_window()
    print(f"  Fetching preparation time {d0} .. {d1} …")

    q = f"""
    SELECT
        p.provider_id,
        p.provider_name,
        p.brand_name,
        p.city_name,
        ROUND(MAX(p.average_cooking_time_minutes) / 60.0, 1) AS cooking_time_min,
        COUNT(*) AS orders,
        ROUND(AVG(f.order_actual_cooking_time_minutes), 1) AS actual_prep_min,
        ROUND(AVG(COALESCE(
            f.provider_ml_estimated_adjusted_cooking_time_minutes,
            f.provider_stated_cooking_time_minutes,
            f.provider_estimated_cooking_time,
            f.order_provider_estimated_cooking_time_seconds / 60.0
        )), 1) AS estimated_prep_min
    FROM ng_delivery_spark.fact_order_delivery f
    INNER JOIN ng_delivery_spark.dim_provider_v2 p
        ON p.provider_id = f.provider_id
    WHERE p.account_manager_name = '{ACCOUNT_MANAGER}'
      AND p.country_code = '{COUNTRY_CODE}'
      AND f.order_created_date_local BETWEEN '{d0.isoformat()}' AND '{d1.isoformat()}'
      AND f.order_actual_cooking_time_minutes IS NOT NULL
      AND f.order_actual_cooking_time_minutes > 0
    GROUP BY
        p.provider_id,
        p.provider_name,
        p.brand_name,
        p.city_name
    HAVING COUNT(*) >= 1
    ORDER BY
        ABS(
            AVG(f.order_actual_cooking_time_minutes)
            - AVG(COALESCE(
                f.provider_ml_estimated_adjusted_cooking_time_minutes,
                f.provider_stated_cooking_time_minutes,
                f.provider_estimated_cooking_time,
                f.order_provider_estimated_cooking_time_seconds / 60.0
            ))
        ) DESC
    """

    df = run_query(conn, q)
    rows: list[dict] = []
    for _, row in df.iterrows():
        actual = float(row["actual_prep_min"]) if row["actual_prep_min"] is not None else None
        estimated = (
            float(row["estimated_prep_min"]) if row["estimated_prep_min"] is not None else None
        )
        cooking = float(row["cooking_time_min"]) if row["cooking_time_min"] is not None else None
        diff = None
        if actual is not None and estimated is not None:
            diff = round(actual - estimated, 1)
        recommended = recommend_cooking_time_min(actual)
        city = str(row["city_name"] or "—")
        rows.append(
            {
                "provider_id": int(row["provider_id"]),
                "provider_name": str(row["provider_name"] or "—"),
                "brand_name": str(row["brand_name"] or "—"),
                "city_name": city,
                "city_ua": CITY_UA.get(city, city),
                "cooking_time_min": cooking,
                "actual_prep_min": actual,
                "estimated_prep_min": estimated,
                "diff_min": diff,
                "recommended_cooking_min": recommended,
                "orders": int(row["orders"]),
            }
        )

    return {
        "period_start": d0.isoformat(),
        "period_end": d1.isoformat(),
        "label": f"{d0:%d.%m.%Y} – {d1:%d.%m.%Y}",
        "rows": rows,
    }


def build_week_payload(
    week_start: date, week_end: date, df_summary: pd.DataFrame, df_orders: pd.DataFrame
) -> dict:
    partners: dict[str, dict] = {}

    for _, row in df_summary.iterrows():
        brand = str(row["brand_name"] or "—")
        city = str(row["city_name"] or "—")
        key = f"{brand}|||{city}"
        delivered = int(row["delivered"] or 0)
        bad = int(row["bad_count"] or 0)
        failed = int(row["failed_count"] or 0)
        partners[key] = {
            "brand": brand,
            "city": city,
            "city_ua": city_ua(city),
            "delivered": delivered,
            "bad_count": bad,
            "failed_count": failed,
            "bad_pct": round(bad / delivered * 100, 2) if delivered else 0,
            "failed_pct": round(failed / delivered * 100, 2) if delivered else 0,
            "failed_by_fault": defaultdict(int),
            "bad_by_actor": defaultdict(int),
            "bad_by_reason": defaultdict(int),
            "failed_orders": [],
            "bad_orders": [],
        }

    for _, r in df_orders.iterrows():
        brand = str(r["brand_name"] or "—")
        city = str(r["city_name"] or "—")
        key = f"{brand}|||{city}"
        if key not in partners:
            partners[key] = {
                "brand": brand,
                "city": city,
                "city_ua": city_ua(city),
                "delivered": 0,
                "bad_count": 0,
                "failed_count": 0,
                "bad_pct": 0,
                "failed_pct": 0,
                "failed_by_fault": defaultdict(int),
                "bad_by_actor": defaultdict(int),
                "bad_by_reason": defaultdict(int),
                "failed_orders": [],
                "bad_orders": [],
            }
        p = partners[key]
        state = str(r.get("final_state") or "")
        order_rec = {
            "order_id": int(r["order_id"]),
            "order_ref": str(r.get("order_ref") or "—"),
            "location": str(r.get("provider_name") or "—"),
            "state": state,
            "created": str(r.get("order_created") or ""),
            "rating": None if pd.isna(r.get("order_food_rating_value")) else float(r["order_food_rating_value"]),
        }

        if state in ("failed", "rejected"):
            fault = classify_failed(r)
            p["failed_by_fault"][fault] += 1
            reason_code = r.get("bad_order_main_reason")
            has_reason = reason_code is not None and not (
                isinstance(reason_code, float) and pd.isna(reason_code)
            )
            order_rec["fault"] = fault
            order_rec["culprit_ua"] = actor_ua(fault if fault != "client" else "eater")
            order_rec["reason_ua"] = (
                reason_ua(reason_code) if has_reason else FAULT_REASON_UA.get(fault, fault)
            )
            order_rec["comment"] = failed_detail_ua(r)
            p["failed_orders"].append(order_rec)

        actor = r.get("bad_order_actor_at_fault")
        actor_key = str(actor).lower() if actor is not None and not (isinstance(actor, float) and pd.isna(actor)) else "unknown"
        reason_code = r.get("bad_order_main_reason")
        reason_key = str(reason_code) if reason_code is not None and not (isinstance(reason_code, float) and pd.isna(reason_code)) else "none"

        p["bad_by_actor"][actor_key] += 1
        p["bad_by_reason"][reason_key] += 1

        bad_rec = {
            **order_rec,
            "actor": actor_key,
            "culprit_ua": actor_ua(actor_key if actor_key != "unknown" else None),
            "reason_code": reason_key,
            "reason_ua": reason_ua(reason_code),
            "comment": bad_comment_ua(r),
        }
        p["bad_orders"].append(bad_rec)

    # Convert defaultdicts to plain dicts for JSON
    out_partners = {}
    for key, p in partners.items():
        if p["bad_count"] == 0 and p["failed_count"] == 0 and not p["bad_orders"]:
            continue
        out_partners[key] = {
            **{k: v for k, v in p.items() if k not in ("failed_by_fault", "bad_by_actor", "bad_by_reason")},
            "failed_by_fault": dict(p["failed_by_fault"]),
            "bad_by_actor": {actor_ua(k): v for k, v in p["bad_by_actor"].items()},
            "bad_by_reason": {reason_ua(k if k != "none" else None): v for k, v in p["bad_by_reason"].items()},
        }

    cities = sorted({p["city_ua"] for p in out_partners.values()}, key=str.lower)
    brands = sorted({p["brand"] for p in out_partners.values()}, key=str.lower)

    total_delivered = int(df_summary["delivered"].sum()) if len(df_summary) else 0
    total_bad = int(df_summary["bad_count"].sum()) if len(df_summary) else 0
    total_failed = int(df_summary["failed_count"].sum()) if len(df_summary) else 0

    return {
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "label": f"{week_start:%d.%m.%Y} – {week_end:%d.%m.%Y}",
        "portfolio": {
            "delivered": total_delivered,
            "bad_count": total_bad,
            "failed_count": total_failed,
            "bad_pct": round(total_bad / total_delivered * 100, 2) if total_delivered else 0,
            "failed_pct": round(total_failed / total_delivered * 100, 2) if total_delivered else 0,
        },
        "cities": cities,
        "brands": brands,
        "partners": out_partners,
    }


def load_existing_weeks(html_path: Path) -> dict[str, dict]:
    if not html_path.exists():
        return {}
    text = html_path.read_text(encoding="utf-8")
    m = re.search(r"const REPORT_WEEKS = (\{.*?\});\s*\n", text, re.DOTALL)
    if not m:
        return {}
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return {}


def load_existing_prep(html_path: Path) -> dict:
    if not html_path.exists():
        return empty_prep_payload()
    text = html_path.read_text(encoding="utf-8")
    m = re.search(r"const PREP_TIME = (\{.*?\});\s*\n", text, re.DOTALL)
    if not m:
        return empty_prep_payload()
    try:
        data = json.loads(m.group(1))
        if not isinstance(data, dict):
            return empty_prep_payload()
        data.setdefault("rows", [])
        return data
    except json.JSONDecodeError:
        return empty_prep_payload()


def build_html(weeks_data: dict[str, dict], prep_data: dict, generated_at: str) -> str:
    weeks_json = json.dumps(weeks_data, ensure_ascii=False, separators=(",", ":"))
    prep_json = json.dumps(prep_data or empty_prep_payload(), ensure_ascii=False, separators=(",", ":"))
    week_keys = sorted(weeks_data.keys(), reverse=True)
    default_week = week_keys[0] if week_keys else ""

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Bad Orders — Marharyta Zhytnyk Portfolio</title>
  <style>
    :root {{
      --green:#1DC462; --dark:#1A1A1A; --bg:#F7F9FC; --card:#fff;
      --border:#E0E0E0; --text:#222; --muted:#666;
      --bolt:#2563EB; --courier:#9333EA; --provider:#EA580C; --client:#64748B;
      --fail:#E53935;
    }}
    * {{ box-sizing:border-box; margin:0; padding:0; }}
    body {{
      font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
      background:var(--bg); color:var(--text); line-height:1.5;
    }}
    .header {{
      background:var(--dark); color:#fff; padding:18px 28px;
      position:sticky; top:0; z-index:100; box-shadow:0 2px 12px rgba(0,0,0,.2);
    }}
    .header h1 {{ font-size:1.2rem; color:var(--green); margin-bottom:4px; }}
    .header p {{ font-size:.82rem; color:#aaa; }}
    .view-tabs {{
      display:flex; gap:8px; margin-top:14px; flex-wrap:wrap;
    }}
    .view-tabs button {{
      background:#2a2a2a; color:#bbb; border:1px solid #444; border-radius:8px;
      padding:8px 16px; font-size:.88rem; cursor:pointer; font-weight:600;
    }}
    .view-tabs button.active {{
      background:var(--green); color:#fff; border-color:var(--green);
    }}
    .filters {{
      display:flex; flex-wrap:wrap; gap:12px; margin-top:14px; align-items:flex-end;
    }}
    .filters label {{ font-size:.72rem; color:#bbb; display:block; margin-bottom:4px; }}
    .filters select, .filters input {{
      background:#2a2a2a; color:#fff; border:1px solid #444; border-radius:8px;
      padding:8px 12px; font-size:.88rem; min-width:180px;
    }}
    .filters input {{ min-width:220px; }}
    .wrap {{ max-width:1200px; margin:0 auto; padding:24px 20px 48px; }}
    .meta {{ color:var(--muted); font-size:.82rem; margin-bottom:20px; }}
    .kpi-row {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:14px; margin-bottom:24px; }}
    .kpi {{
      background:var(--card); border:1px solid var(--border); border-radius:12px;
      padding:16px 18px; box-shadow:0 1px 4px rgba(0,0,0,.04);
    }}
    .kpi .n {{ font-size:1.8rem; font-weight:800; }}
    .kpi .l {{ font-size:.72rem; color:var(--muted); text-transform:uppercase; letter-spacing:.04em; margin-top:4px; }}
    .kpi.bad .n {{ color:var(--fail); }}
    .kpi.fail .n {{ color:#FB8C00; }}
    h2 {{
      font-size:1rem; margin:28px 0 12px; padding-left:10px;
      border-left:4px solid var(--green);
    }}
    h2.fail-h {{ border-left-color:#FB8C00; }}
    h2.bad-h {{ border-left-color:var(--fail); }}
    .grid-2 {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
    @media(max-width:768px) {{ .grid-2 {{ grid-template-columns:1fr; }} }}
    .card {{
      background:var(--card); border:1px solid var(--border); border-radius:12px;
      padding:16px; box-shadow:0 1px 4px rgba(0,0,0,.04);
    }}
    .bar-row {{ display:flex; align-items:center; gap:10px; margin:8px 0; font-size:.85rem; }}
    .bar-label {{ min-width:140px; flex-shrink:0; }}
    .bar-track {{ flex:1; height:8px; background:#eee; border-radius:4px; overflow:hidden; }}
    .bar-fill {{ height:100%; border-radius:4px; }}
    .bar-fill.bolt {{ background:var(--bolt); }}
    .bar-fill.courier {{ background:var(--courier); }}
    .bar-fill.provider {{ background:var(--provider); }}
    .bar-fill.client {{ background:var(--client); }}
    .bar-val {{ min-width:36px; text-align:right; font-weight:700; }}
    .reason-list {{ list-style:none; }}
    .reason-list li {{
      display:flex; justify-content:space-between; gap:12px;
      padding:8px 0; border-bottom:1px solid #f0f0f0; font-size:.84rem;
    }}
    .reason-list li:last-child {{ border-bottom:none; }}
    .btn-detail {{
      background:var(--green); color:#fff; border:none; border-radius:8px;
      padding:10px 20px; font-size:.9rem; font-weight:600; cursor:pointer; margin-top:8px;
    }}
    .btn-detail:hover {{ filter:brightness(1.05); }}
    .btn-partner {{
      background:#fff; color:var(--provider); border:2px solid var(--provider);
      border-radius:8px; padding:10px 20px; font-size:.9rem; font-weight:600;
      cursor:pointer; margin-top:8px; margin-left:10px;
    }}
    .btn-partner:hover {{ background:#FFF7ED; }}
    .modal-overlay {{
      display:none; position:fixed; inset:0; background:rgba(0,0,0,.45);
      z-index:200; align-items:center; justify-content:center; padding:20px;
    }}
    .modal-overlay.open {{ display:flex; }}
    .modal-box {{
      background:#fff; border-radius:14px; width:min(720px,100%);
      max-height:85vh; display:flex; flex-direction:column;
      box-shadow:0 12px 40px rgba(0,0,0,.2);
    }}
    .modal-head {{
      padding:16px 20px; border-bottom:1px solid var(--border);
      display:flex; justify-content:space-between; align-items:center; gap:12px;
    }}
    .modal-head h3 {{ font-size:1rem; }}
    .modal-close {{
      background:none; border:none; font-size:1.4rem; cursor:pointer; color:var(--muted);
    }}
    .modal-body {{ padding:16px 20px; overflow:auto; flex:1; }}
    .modal-body textarea {{
      width:100%; min-height:320px; border:1px solid var(--border); border-radius:10px;
      padding:12px; font-size:.85rem; line-height:1.55; resize:vertical; font-family:inherit;
    }}
    .modal-actions {{
      padding:12px 20px 18px; display:flex; gap:10px; justify-content:flex-end;
      border-top:1px solid var(--border);
    }}
    .btn-copy {{
      background:var(--green); color:#fff; border:none; border-radius:8px;
      padding:9px 18px; font-weight:600; cursor:pointer;
    }}
    .btn-close-modal {{
      background:#f0f0f0; color:var(--text); border:none; border-radius:8px;
      padding:9px 18px; cursor:pointer;
    }}
    .detail-panel {{
      display:none; margin-top:16px; background:var(--card);
      border:1px solid var(--border); border-radius:12px; overflow:hidden;
    }}
    .detail-panel.open {{ display:block; }}
    .detail-tabs {{ display:flex; gap:0; border-bottom:1px solid var(--border); flex-wrap:wrap; }}
    .detail-tabs button {{
      background:none; border:none; padding:10px 16px; cursor:pointer;
      font-size:.82rem; color:var(--muted); border-bottom:2px solid transparent;
    }}
    .detail-tabs button.active {{ color:var(--green); border-bottom-color:var(--green); font-weight:600; }}
    .detail-toolbar {{
      display:flex; flex-wrap:wrap; align-items:flex-end; gap:12px;
      padding:12px 16px; border-bottom:1px solid var(--border); background:#fafafa;
    }}
    .detail-toolbar label {{ font-size:.72rem; color:var(--muted); display:block; margin-bottom:4px; }}
    .detail-toolbar select {{
      border:1px solid var(--border); border-radius:8px; padding:7px 12px;
      font-size:.85rem; min-width:200px; background:#fff;
    }}
    .detail-count {{ font-size:.78rem; color:var(--muted); margin-left:auto; }}
    table {{ width:100%; border-collapse:collapse; font-size:.8rem; }}
    th {{
      text-align:left; padding:10px 12px; background:#f5f5f5;
      color:var(--muted); font-weight:600; border-bottom:1px solid var(--border);
      white-space:nowrap;
    }}
    td {{ padding:8px 12px; border-bottom:1px solid #f0f0f0; vertical-align:top; }}
    tr:hover td {{ background:#fafafa; }}
    .mono {{ font-family:ui-monospace,monospace; }}
    .num {{ font-variant-numeric:tabular-nums; text-align:right; white-space:nowrap; }}
    .tag {{
      display:inline-block; padding:2px 8px; border-radius:6px;
      font-size:.72rem; font-weight:700;
    }}
    .tag-failed {{ background:#FFF3E0; color:#E65100; }}
    .tag-rejected {{ background:#FFEBEE; color:#C62828; }}
    .diff-pos {{
      background:#FFEBEE; color:#C62828; font-weight:700;
      padding:3px 8px; border-radius:6px; display:inline-block;
    }}
    .diff-neg {{
      background:#E8F5E9; color:#2E7D32; font-weight:700;
      padding:3px 8px; border-radius:6px; display:inline-block;
    }}
    .diff-zero {{
      background:#f0f0f0; color:#555; font-weight:600;
      padding:3px 8px; border-radius:6px; display:inline-block;
    }}
    .rec-change {{
      background:#E3F2FD; color:#1565C0; font-weight:700;
      padding:3px 8px; border-radius:6px; display:inline-block;
    }}
    .rec-ok {{
      background:#E8F5E9; color:#2E7D32; font-weight:600;
      padding:3px 8px; border-radius:6px; display:inline-block;
    }}
    .prep-table-wrap {{
      background:var(--card); border:1px solid var(--border); border-radius:12px;
      overflow:auto; box-shadow:0 1px 4px rgba(0,0,0,.04);
    }}
    .prep-legend {{
      display:flex; flex-wrap:wrap; gap:14px; font-size:.8rem; color:var(--muted);
      margin:8px 0 16px;
    }}
    .empty {{ color:var(--muted); text-align:center; padding:40px 20px; }}
    .portfolio-note {{
      background:#E8F9EE; border:1px solid #b8e6c8; border-radius:10px;
      padding:12px 16px; font-size:.84rem; margin-bottom:20px;
    }}
  </style>
</head>
<body>
  <div class="header">
    <h1>Аналіз Bad Orders — Портфоліо Marharyta Zhytnyk</h1>
    <p>Щотижневий розбір поганих та невдалих замовлень · Bolt Food Ukraine</p>
    <div class="view-tabs">
      <button type="button" class="active" data-view="bad">Bad Orders</button>
      <button type="button" data-view="prep">Preparation time</button>
    </div>
    <div class="filters" id="filtersBad">
      <div>
        <label>Тиждень</label>
        <select id="selWeek"></select>
      </div>
      <div>
        <label>Місто</label>
        <select id="selCity"><option value="">— Усі міста —</option></select>
      </div>
      <div>
        <label>Бренд (партнер)</label>
        <select id="selBrand"><option value="">— Оберіть партнера —</option></select>
      </div>
    </div>
    <div class="filters" id="filtersPrep" style="display:none">
      <div>
        <label>Місто</label>
        <select id="selPrepCity"><option value="">— Усі міста —</option></select>
      </div>
      <div>
        <label>Пошук провайдера</label>
        <input id="prepSearch" type="search" placeholder="Назва або Provider ID" />
      </div>
    </div>
  </div>

  <div class="wrap">
    <div class="meta" id="metaLine">Згенеровано: {html.escape(generated_at)} UTC</div>
    <div id="viewBad">
      <div id="portfolioNote" class="portfolio-note" style="display:none"></div>
      <div id="content">
        <div class="empty">Оберіть місто та бренд, щоб переглянути деталі.</div>
      </div>
    </div>
    <div id="viewPrep" style="display:none"></div>
  </div>

  <div class="modal-overlay" id="partnerModal">
    <div class="modal-box">
      <div class="modal-head">
        <h3>Повідомлення для партнера</h3>
        <button type="button" class="modal-close" id="btnClosePartnerModal" title="Закрити">&times;</button>
      </div>
      <div class="modal-body">
        <textarea id="partnerMessageText" readonly></textarea>
      </div>
      <div class="modal-actions">
        <button type="button" class="btn-copy" id="btnCopyPartnerMsg">Скопіювати текст</button>
        <button type="button" class="btn-close-modal" id="btnClosePartnerModal2">Закрити</button>
      </div>
    </div>
  </div>

  <script>
  const REPORT_WEEKS = {weeks_json};
  const PREP_TIME = {prep_json};

  const FAULT_CLASS = {{
    'Bolt (платформа)': 'bolt',
    'Кур\\'єр': 'courier',
    'Заклад': 'provider',
    'Клієнт': 'client',
    'Невизначено': 'client'
  }};

  let activeView = 'bad';

  function $(id) {{ return document.getElementById(id); }}

  function fmtMin(v) {{
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    return Number(v).toFixed(1);
  }}

  function fmtDiff(v) {{
    if (v === null || v === undefined || Number.isNaN(v)) return '—';
    const n = Number(v);
    const sign = n > 0 ? '+' : '';
    const cls = Math.abs(n) < 0.05 ? 'diff-zero' : (n > 0 ? 'diff-pos' : 'diff-neg');
    return `<span class="${{cls}}">${{sign}}${{n.toFixed(1)}} хв</span>`;
  }}

  function recommendCooking(actual) {{
    if (actual === null || actual === undefined || Number.isNaN(Number(actual))) return null;
    const rounded = Math.round(Math.max(5, Number(actual)) / 5) * 5;
    return Math.min(rounded, 90);
  }}

  function fmtRecommended(row) {{
    const rec = (row.recommended_cooking_min != null)
      ? Number(row.recommended_cooking_min)
      : recommendCooking(row.actual_prep_min);
    if (rec === null || rec === undefined || Number.isNaN(rec)) return '—';
    const cooking = Number(row.cooking_time_min);
    const differs = !Number.isNaN(cooking) && Math.abs(cooking - rec) >= 5;
    const cls = differs ? 'rec-change' : 'rec-ok';
    const hint = differs
      ? ` <span style="color:#888;font-weight:500;font-size:.72rem">(зараз ${{fmtMin(cooking)}})</span>`
      : '';
    return `<span class="${{cls}}">${{rec}} хв</span>${{hint}}`;
  }}

  function setView(view) {{
    activeView = view;
    document.querySelectorAll('.view-tabs button').forEach(b => {{
      b.classList.toggle('active', b.dataset.view === view);
    }});
    $('filtersBad').style.display = view === 'bad' ? 'flex' : 'none';
    $('filtersPrep').style.display = view === 'prep' ? 'flex' : 'none';
    $('viewBad').style.display = view === 'bad' ? 'block' : 'none';
    $('viewPrep').style.display = view === 'prep' ? 'block' : 'none';
    if (view === 'prep') renderPrep();
  }}

  function initPrepFilters() {{
    const cities = [...new Set((PREP_TIME.rows || []).map(r => r.city_ua))].sort((a,b) => a.localeCompare(b,'uk'));
    const sel = $('selPrepCity');
    cities.forEach(c => {{
      const o = document.createElement('option');
      o.value = c; o.textContent = c;
      sel.appendChild(o);
    }});
    sel.addEventListener('change', renderPrep);
    $('prepSearch').addEventListener('input', renderPrep);
  }}

  function renderPrep() {{
    const city = $('selPrepCity').value;
    const q = ($('prepSearch').value || '').trim().toLowerCase();
    let rows = [...(PREP_TIME.rows || [])];
    if (city) rows = rows.filter(r => r.city_ua === city);
    if (q) {{
      rows = rows.filter(r =>
        String(r.provider_name || '').toLowerCase().includes(q) ||
        String(r.provider_id || '').includes(q) ||
        String(r.brand_name || '').toLowerCase().includes(q)
      );
    }}

    const total = rows.length;
    const slower = rows.filter(r => (r.diff_min || 0) > 2).length;
    const faster = rows.filter(r => (r.diff_min || 0) < -2).length;
    const avgDiff = total
      ? (rows.reduce((s, r) => s + (r.diff_min || 0), 0) / total)
      : 0;

    const tableRows = rows.map(r => `<tr>
      <td>${{r.provider_name || '—'}}</td>
      <td class="mono">${{r.provider_id}}</td>
      <td class="num">${{fmtMin(r.cooking_time_min)}}</td>
      <td class="num">${{fmtMin(r.actual_prep_min)}}</td>
      <td class="num">${{fmtMin(r.estimated_prep_min)}}</td>
      <td class="num">${{fmtDiff(r.diff_min)}}</td>
      <td class="num">${{fmtRecommended(r)}}</td>
      <td class="num">${{r.orders || 0}}</td>
      <td>${{r.city_ua || r.city_name || '—'}}</td>
    </tr>`).join('');

    $('viewPrep').innerHTML = `
      <div class="portfolio-note">
        <strong>Preparation time</strong> · період <strong>${{PREP_TIME.label || '—'}}</strong>
        (останні 2 повні тижні).<br>
        Cooking time — налаштований час провайдера.
        Actual — фактичний час приготування.
        Estimated — оцінка системи.
        Різниця = Actual − Estimated (плюс = довше за оцінку системи).<br>
        <strong>Рекомендовано виставити</strong> — actual, округлений до 5 хв (макс. 90), щоб партнер підлаштував cooking time у системі під реальний час.
      </div>
      <div class="kpi-row">
        <div class="kpi"><div class="n">${{total}}</div><div class="l">Провайдерів</div></div>
        <div class="kpi bad"><div class="n">${{slower}}</div><div class="l">Повільніше за систему (&gt; +2 хв)</div></div>
        <div class="kpi"><div class="n" style="color:#2E7D32">${{faster}}</div><div class="l">Швидше за систему (&lt; −2 хв)</div></div>
        <div class="kpi"><div class="n">${{avgDiff.toFixed(1)}}</div><div class="l">Середня різниця, хв</div></div>
      </div>
      <div class="prep-legend">
        <span><span class="diff-pos">+x хв</span> актуальний час довший за систему</span>
        <span><span class="diff-neg">−x хв</span> актуальний час коротший за систему</span>
        <span><span class="rec-change">N хв</span> рекомендуємо змінити cooking time</span>
        <span><span class="rec-ok">N хв</span> вже близьке до факту</span>
      </div>
      <div class="prep-table-wrap">
        ${{rows.length ? `<table>
          <thead><tr>
            <th>Provider name</th>
            <th>Provider ID</th>
            <th>Cooking time, хв</th>
            <th>Actual preparation, хв</th>
            <th>Estimated preparation, хв</th>
            <th>Різниця</th>
            <th>Рекомендовано виставити, хв</th>
            <th>Замовлень</th>
            <th>Місто</th>
          </tr></thead>
          <tbody>${{tableRows}}</tbody>
        </table>` : '<p class="empty">Немає даних за обраним фільтром.</p>'}}
      </div>
    `;
  }}

  function initFilters() {{
    const weeks = Object.keys(REPORT_WEEKS).sort().reverse();
    const selW = $('selWeek');
    weeks.forEach(wk => {{
      const o = document.createElement('option');
      o.value = wk;
      o.textContent = REPORT_WEEKS[wk].label;
      selW.appendChild(o);
    }});
    if ('{default_week}') selW.value = '{default_week}';
    selW.addEventListener('change', onFilterChange);
    $('selCity').addEventListener('change', onCityChange);
    $('selBrand').addEventListener('change', render);
    onFilterChange();
  }}

  function currentWeek() {{
    return REPORT_WEEKS[$('selWeek').value];
  }}

  function onFilterChange() {{
    const wk = currentWeek();
    if (!wk) return;
    const selCity = $('selCity');
    const prev = selCity.value;
    selCity.innerHTML = '<option value="">— Усі міста —</option>';
    wk.cities.forEach(c => {{
      const o = document.createElement('option');
      o.value = c; o.textContent = c;
      selCity.appendChild(o);
    }});
    if ([...selCity.options].some(o => o.value === prev)) selCity.value = prev;
    onCityChange();
  }}

  function onCityChange() {{
    const wk = currentWeek();
    const city = $('selCity').value;
    const selBrand = $('selBrand');
    selBrand.innerHTML = '<option value="">— Оберіть партнера —</option>';
    if (!wk) return;
    const brands = new Set();
    Object.values(wk.partners).forEach(p => {{
      if (!city || p.city_ua === city) brands.add(p.brand);
    }});
    [...brands].sort((a,b) => a.localeCompare(b,'uk')).forEach(b => {{
      const o = document.createElement('option');
      o.value = b; o.textContent = b;
      selBrand.appendChild(o);
    }});
    render();
  }}

  function barRows(obj, total) {{
    if (!total) return '<p class="empty" style="padding:12px">Немає даних</p>';
    return Object.entries(obj).sort((a,b) => b[1]-a[1]).map(([label, cnt]) => {{
      const pct = (cnt/total*100).toFixed(1);
      const cls = FAULT_CLASS[label] || 'client';
      return `<div class="bar-row">
        <span class="bar-label">${{label}}</span>
        <div class="bar-track"><div class="bar-fill ${{cls}}" style="width:${{pct}}%"></div></div>
        <span class="bar-val">${{cnt}}</span>
      </div>`;
    }}).join('');
  }}

  function reasonList(obj) {{
    const entries = Object.entries(obj).sort((a,b) => b[1]-a[1]);
    if (!entries.length) return '<p class="empty" style="padding:12px">Немає даних</p>';
    return '<ul class="reason-list">' + entries.map(([r,c]) =>
      `<li><span>${{r}}</span><strong>${{c}}</strong></li>`
    ).join('') + '</ul>';
  }}

  function orderTable(orders) {{
    if (!orders.length) return '<p class="empty">Немає замовлень за обраним фільтром.</p>';
    const rows = orders.map(o => {{
      const tag = o.state === 'rejected'
        ? '<span class="tag tag-rejected">rejected</span>'
        : o.state === 'failed'
        ? '<span class="tag tag-failed">failed</span>'
        : '<span class="tag tag-failed">bad</span>';
      return `<tr>
        <td class="mono">${{o.order_id}}</td>
        <td class="mono">${{o.order_ref || '—'}}</td>
        <td>${{o.location}}</td>
        <td>${{o.reason_ua || '—'}}</td>
        <td>${{o.comment || '—'}}</td>
        <td>${{tag}}</td>
        <td class="mono">${{(o.created||'').slice(0,16)}}</td>
      </tr>`;
    }}).join('');
    const head = '<th>№ замовлення</th><th>Order ref</th><th>Локація</th><th>Причина</th><th>Коментар</th><th>Статус</th><th>Час</th>';
    return `<table><thead><tr>${{head}}</tr></thead><tbody>${{rows}}</tbody></table>`;
  }}

  let detailOrders = {{ failed: [], bad: [] }};
  let activeDetailTab = 'failed';
  let currentPartnerCtx = null;

  function isProviderFault(o) {{
    return o.culprit_ua === 'Заклад' || o.actor === 'provider' || o.fault === 'provider';
  }}

  function providerBadOrders(partner) {{
    return (partner.bad_orders || []).filter(isProviderFault);
  }}

  function buildPartnerMessage(partner, weekLabel, brand, cityLabel) {{
    const orders = providerBadOrders(partner);
    if (!orders.length) return '';

    const byReason = {{}};
    orders.forEach(o => {{
      const reason = o.reason_ua || '—';
      if (!byReason[reason]) byReason[reason] = [];
      byReason[reason].push(o);
    }});

    const lines = [
      'Вітаю!',
      '',
      `Це Marharyta Zhytnyk, ваш акаунт-менеджер Bolt Food.`,
      '',
      `Звертаю вашу увагу на погані замовлення за період ${{weekLabel}} у закладі ${{brand}} (${{cityLabel}}).`,
      '',
      `За результатами тижневого аналізу, ${{orders.length}} замовлень мають причини, пов'язані з роботою закладу. Загальний показник поганих замовлень за тиждень: ${{partner.bad_pct}}% (${{partner.bad_count}} з ${{partner.delivered}} доставлених).`,
      '',
      'Основні причини:',
    ];

    Object.entries(byReason)
      .sort((a, b) => b[1].length - a[1].length)
      .forEach(([reason, list]) => {{
        lines.push(`• ${{reason}} — ${{list.length}} замовлень`);
      }});

    lines.push('', 'Деталі по замовленнях:', '');
    orders.forEach(o => {{
      lines.push(`— Код замовлення: ${{o.order_ref || '—'}} | ${{o.location}}`);
      lines.push(`  Причина: ${{o.reason_ua || '—'}}`);
      if (o.comment && o.comment !== '—') lines.push(`  Коментар: ${{o.comment}}`);
    }});

    lines.push(
      '',
      'Прошу перевірити:',
      '1. Наявність позицій у меню та своєчасне оновлення стоп-листу',
      '2. Якість пакування та правильність комплектації замовлень',
      '3. Час приготування та передачу замовлень курʼєру',
      '4. Стабільність прийняття замовлень протягом робочого дня',
      '',
      'Зниження частки поганих замовлень позитивно вплине на рейтинг закладу, задоволеність клієнтів та обсяг замовлень на платформі.',
      '',
      'Якщо потрібна допомога — напишіть мені, з радістю допоможу.',
      '',
      'З повагою,',
      'Marharyta Zhytnyk',
      'Акаунт-менеджер Bolt Food',
    );
    return lines.join('\\n');
  }}

  function openPartnerModal() {{
    if (!currentPartnerCtx) return;
    const {{ partner, weekLabel, brand, cityLabel }} = currentPartnerCtx;
    const text = buildPartnerMessage(partner, weekLabel, brand, cityLabel);
    $('partnerMessageText').value = text;
    $('partnerModal').classList.add('open');
  }}

  function closePartnerModal() {{
    $('partnerModal').classList.remove('open');
  }}

  function copyPartnerMessage() {{
    const ta = $('partnerMessageText');
    ta.select();
    ta.setSelectionRange(0, 99999);
    navigator.clipboard.writeText(ta.value).then(() => {{
      const btn = $('btnCopyPartnerMsg');
      const prev = btn.textContent;
      btn.textContent = 'Скопійовано!';
      setTimeout(() => {{ btn.textContent = prev; }}, 2000);
    }}).catch(() => document.execCommand('copy'));
  }}

  function updateCulpritFilter() {{
    const sel = $('selCulprit');
    if (!sel) return;
    const prev = sel.value;
    const orders = detailOrders[activeDetailTab] || [];
    const culprits = [...new Set(orders.map(o => o.culprit_ua).filter(Boolean))].sort((a,b) => a.localeCompare(b,'uk'));
    sel.innerHTML = '<option value="">— Усі винуватці —</option>';
    culprits.forEach(c => {{
      const o = document.createElement('option');
      o.value = c; o.textContent = c;
      sel.appendChild(o);
    }});
    if ([...sel.options].some(o => o.value === prev)) sel.value = prev;
    else sel.value = '';
  }}

  function renderDetailTable() {{
    const filter = $('selCulprit')?.value || '';
    const orders = (detailOrders[activeDetailTab] || []).filter(o => !filter || o.culprit_ua === filter);
    const host = activeDetailTab === 'failed' ? $('tabFailed') : $('tabBad');
    if (host) host.innerHTML = orderTable(orders);
    const cnt = $('detailCount');
    if (cnt) cnt.textContent = filter
      ? `Показано ${{orders.length}} з ${{detailOrders[activeDetailTab].length}}`
      : `Всього: ${{orders.length}}`;
  }}

  function initDetailPanel(partner) {{
    detailOrders.failed = (partner.failed_orders || []).map(o => ({{
      ...o,
      order_ref: o.order_ref || '—',
      culprit_ua: o.culprit_ua || o.fault_ua || o.actor_ua || '—',
      reason_ua: o.reason_ua || '—',
      comment: o.comment || o.detail || '—',
    }}));
    detailOrders.bad = (partner.bad_orders || []).map(o => ({{
      ...o,
      order_ref: o.order_ref || '—',
      culprit_ua: o.culprit_ua || o.actor_ua || '—',
      reason_ua: o.reason_ua || '—',
      comment: o.comment || '—',
    }}));
    activeDetailTab = 'failed';
    updateCulpritFilter();
    renderDetailTable();
  }}

  function mergePartners(list) {{
    if (!list.length) return null;
    if (list.length === 1) return list[0];
    const out = {{
      brand: list[0].brand,
      city_ua: 'Усі міста',
      delivered: 0, bad_count: 0, failed_count: 0,
      failed_by_fault: {{}}, bad_by_actor: {{}}, bad_by_reason: {{}},
      failed_orders: [], bad_orders: []
    }};
    list.forEach(p => {{
      out.delivered += p.delivered || 0;
      out.bad_count += p.bad_count || 0;
      out.failed_count += p.failed_count || 0;
      ['failed_by_fault','bad_by_actor','bad_by_reason'].forEach(k => {{
        Object.entries(p[k] || {{}}).forEach(([a, c]) => {{
          out[k][a] = (out[k][a] || 0) + c;
        }});
      }});
      out.failed_orders = out.failed_orders.concat(p.failed_orders || []);
      out.bad_orders = out.bad_orders.concat(p.bad_orders || []);
    }});
    out.bad_pct = out.delivered ? +(out.bad_count / out.delivered * 100).toFixed(2) : 0;
    out.failed_pct = out.delivered ? +(out.failed_count / out.delivered * 100).toFixed(2) : 0;
    return out;
  }}

  function render() {{
    const wk = currentWeek();
    const city = $('selCity').value;
    const brand = $('selBrand').value;
    const el = $('content');
    const note = $('portfolioNote');

    if (!wk) {{ el.innerHTML = '<div class="empty">Немає даних.</div>'; return; }}

    note.style.display = 'block';
    note.innerHTML = `<strong>Портфоліо за тиждень ${{wk.label}}:</strong> `
      + `Bad Orders ${{wk.portfolio.bad_pct}}% (${{wk.portfolio.bad_count}} з ${{wk.portfolio.delivered}} доставлених), `
      + `Failed Orders ${{wk.portfolio.failed_pct}}% (${{wk.portfolio.failed_count}}).`;

    if (!brand) {{
      el.innerHTML = '<div class="empty">Оберіть бренд (партнера) для детального аналізу.</div>';
      return;
    }}

    const matches = Object.values(wk.partners).filter(p =>
      p.brand === brand && (!city || p.city_ua === city)
    );
    const partner = mergePartners(matches);

    if (!partner) {{
      el.innerHTML = '<div class="empty">Немає даних для обраного партнера.</div>';
      return;
    }}

    const failedTotal = Object.values(partner.failed_by_fault).reduce((a,b)=>a+b,0);
    const badTotal = Object.values(partner.bad_by_actor).reduce((a,b)=>a+b,0);
    const titleCity = city || partner.city_ua;
    const providerBadCount = providerBadOrders(partner).length;
    currentPartnerCtx = {{
      partner,
      weekLabel: wk.label,
      brand,
      cityLabel: titleCity,
    }};

    el.innerHTML = `
      <h2 style="border:none;padding:0;margin-bottom:8px;font-size:1.15rem">
        ${{partner.brand}} · ${{titleCity}}
      </h2>
      <div class="kpi-row">
        <div class="kpi bad"><div class="n">${{partner.bad_pct}}%</div><div class="l">Bad Orders</div></div>
        <div class="kpi fail"><div class="n">${{partner.failed_pct}}%</div><div class="l">Failed Orders</div></div>
        <div class="kpi"><div class="n">${{partner.bad_count}}</div><div class="l">Поганих замовлень</div></div>
        <div class="kpi"><div class="n">${{partner.failed_count}}</div><div class="l">Невдалих замовлень</div></div>
        <div class="kpi"><div class="n">${{partner.delivered}}</div><div class="l">Доставлено</div></div>
      </div>

      <h2 class="fail-h">Failed Orders — хто винен</h2>
      <div class="grid-2">
        <div class="card">
          <p style="font-size:.82rem;color:var(--muted);margin-bottom:10px">
            Розподіл невдалих (failed/rejected) замовлень за винуватцем
          </p>
          ${{barRows({{
            'Bolt (платформа)': partner.failed_by_fault.bolt || 0,
            'Кур\\'єр': partner.failed_by_fault.courier || 0,
            'Заклад': partner.failed_by_fault.provider || 0,
            'Клієнт': partner.failed_by_fault.client || 0
          }}, failedTotal)}}
        </div>
        <div class="card">
          <p style="font-size:.82rem;color:var(--muted);margin-bottom:8px"><strong>Пояснення категорій:</strong></p>
          <ul style="font-size:.82rem;color:var(--muted);padding-left:18px">
            <li><strong>Bolt</strong> — зрив без відмови закладу чи кур'єра</li>
            <li><strong>Кур'єр</strong> — відмови кур'єра під час пошуку</li>
            <li><strong>Заклад</strong> — відхилення (rejected) або is_rejected_by_provider</li>
            <li><strong>Клієнт</strong> — скасування з боку клієнта</li>
          </ul>
        </div>
      </div>

      <h2 class="bad-h">Bad Orders — хто винен</h2>
      <div class="card">${{barRows(partner.bad_by_actor, badTotal)}}</div>

      <h2 class="bad-h">Bad Orders — причини</h2>
      <div class="card">${{reasonList(partner.bad_by_reason)}}</div>

      <div style="display:flex;flex-wrap:wrap;align-items:center;gap:4px">
        <button class="btn-detail" id="btnDetail" type="button">Детально — номери замовлень</button>
        ${{providerBadCount > 0 ? `<button class="btn-partner" id="btnPartnerMsg" type="button">Надіслати інформацію партнеру (${{providerBadCount}})</button>` : ''}}
      </div>
      <div class="detail-panel" id="detailPanel">
        <div class="detail-tabs">
          <button type="button" class="active" data-tab="failed">Failed Orders (${{partner.failed_orders.length}})</button>
          <button type="button" data-tab="bad">Bad Orders (${{partner.bad_orders.length}})</button>
        </div>
        <div class="detail-toolbar">
          <div>
            <label for="selCulprit">\u0412\u0438\u043d\u0443\u0432\u0430\u0442\u0435\u0446\u044c</label>
            <select id="selCulprit"><option value="">\u2014 \u0423\u0441\u0456 \u0432\u0438\u043d\u0443\u0432\u0430\u0442\u0446\u0456 \u2014</option></select>
          </div>
          <span class="detail-count" id="detailCount"></span>
        </div>
        <div id="tabFailed" style="overflow:auto"></div>
        <div id="tabBad" style="display:none;overflow:auto"></div>
      </div>
    `;

    initDetailPanel(partner);

    $('btnDetail').addEventListener('click', () => {{
      $('detailPanel').classList.toggle('open');
    }});
    const btnPartner = $('btnPartnerMsg');
    if (btnPartner) btnPartner.addEventListener('click', openPartnerModal);
    $('selCulprit').addEventListener('change', renderDetailTable);
    document.querySelectorAll('.detail-tabs button').forEach(btn => {{
      btn.addEventListener('click', () => {{
        document.querySelectorAll('.detail-tabs button').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        activeDetailTab = btn.dataset.tab;
        $('tabFailed').style.display = activeDetailTab === 'failed' ? 'block' : 'none';
        $('tabBad').style.display = activeDetailTab === 'bad' ? 'block' : 'none';
        updateCulpritFilter();
        renderDetailTable();
      }});
    }});
  }}

  initFilters();
  initPrepFilters();
  document.querySelectorAll('.view-tabs button').forEach(btn => {{
    btn.addEventListener('click', () => setView(btn.dataset.view));
  }});
  $('btnClosePartnerModal').addEventListener('click', closePartnerModal);
  $('btnClosePartnerModal2').addEventListener('click', closePartnerModal);
  $('btnCopyPartnerMsg').addEventListener('click', copyPartnerMessage);
  $('partnerModal').addEventListener('click', e => {{
    if (e.target === $('partnerModal')) closePartnerModal();
  }});
  </script>
</body>
</html>
"""


def weeks_to_fetch(existing: dict[str, dict]) -> list[date]:
    """Усі пропущені повні тижні (пн) від останнього в звіті до останнього завершеного."""
    if os.environ.get("BAD_ORDERS_WEEK_START"):
        return [date.fromisoformat(os.environ["BAD_ORDERS_WEEK_START"])]

    last_complete_start, _ = week_bounds()
    if not existing:
        return [last_complete_start]

    latest_in_report = max(date.fromisoformat(k) for k in existing)
    ws = latest_in_report + timedelta(days=7)
    weeks: list[date] = []
    while ws <= last_complete_start:
        weeks.append(ws)
        ws += timedelta(days=7)
    return weeks


def main() -> None:
    existing = load_existing_weeks(OUTPUT_HTML)
    prep_data = load_existing_prep(OUTPUT_HTML)

    if os.environ.get("BAD_ORDERS_HTML_ONLY"):
        if not existing:
            raise RuntimeError("No existing data in HTML for BAD_ORDERS_HTML_ONLY")
        generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        OUTPUT_HTML.write_text(build_html(existing, prep_data, generated_at), encoding="utf-8")
        print(f"Rebuilt HTML only: {OUTPUT_HTML}")
        return

    print(f"Existing weeks in report: {list(existing.keys())}")

    to_fetch = weeks_to_fetch(existing)
    print(f"Weeks to fetch: {[w.isoformat() for w in to_fetch] if to_fetch else '[] (up to date)'}")

    conn = sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=get_token(),
    )
    try:
        for ws in to_fetch:
            key = ws.isoformat()
            if key in existing and not os.environ.get("BAD_ORDERS_FORCE_REFRESH"):
                print(f"  Week {key} already present — skip (set BAD_ORDERS_FORCE_REFRESH=1 to overwrite)")
                continue
            we = ws + timedelta(days=6)
            existing[key] = fetch_week_data(conn, ws, we)

        prep_data = fetch_prep_time_data(conn)
        print(f"  Preparation time rows: {len(prep_data.get('rows', []))}")
    finally:
        conn.close()

    generated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    OUTPUT_HTML.write_text(build_html(existing, prep_data, generated_at), encoding="utf-8")
    print(f"Written {OUTPUT_HTML} ({len(existing)} week(s))")

    # Also write week-specific snapshot for weeks in report
    for key, data in existing.items():
        ws = date.fromisoformat(key)
        snap = SCRIPT_DIR / f"Bad Orders {ws:%d.%m}-{ws + timedelta(days=6):%d.%m.%Y}.html"
        if not snap.exists() or os.environ.get("BAD_ORDERS_FORCE_REFRESH"):
            single = {key: data}
            snap.write_text(build_html(single, prep_data, generated_at), encoding="utf-8")
            print(f"  Snapshot: {snap.name}")


if __name__ == "__main__":
    main()
