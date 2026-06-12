"""
Потенційне підключення — звіт по Smart Promotions та Sponsored Listing
для портфоліо Account Manager Marharyta Zhytnyk.

Джерела:
  - Smart Promotions (активні): delivery_smart_promotion_log.state = 'active'
  - Sponsored Listing (активні): sponsored_listing_duration_hours > 0 за останні 7 днів
  - Метрики (4 повні тижні): fact_provider_weekly
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import requests

# ─── CONFIG ────────────────────────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://bolt-incentives.cloud.databricks.com")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
CLUSTER_ID = os.getenv("DATABRICKS_CLUSTER_ID", "0221-081903-9ag4bh69")

ACCOUNT_MANAGER = "Marharyta Zhytnyk"
COUNTRY_CODE = "ua"
REPORT_DATE = date.today().isoformat()
OUTPUT_FILE = "Потенційне_підключення_звіт.html"
TOP_RECOMMENDATIONS = 20
TOP_SL_RECOMMENDATIONS = 20
# Мінімальний прогнозований ROAS для рекомендації Sponsored Listing
SL_ROAS_THRESHOLD = 3.0


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
    return ""


def get_last_n_full_weeks(n: int = 4) -> tuple[str, str]:
    today = date.today()
    days_since_sunday = today.weekday() + 1
    last_sunday = today - timedelta(days=days_since_sunday)
    start_monday = last_sunday - timedelta(days=(n * 7) - 1)
    return start_monday.isoformat(), last_sunday.isoformat()


def _headers() -> dict:
    return {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}


def _exec_sql(sql: str, timeout: int = 360) -> pd.DataFrame:
    resp = requests.post(
        f"{DATABRICKS_HOST}/api/1.2/contexts/create",
        headers=_headers(),
        json={"language": "sql", "clusterId": CLUSTER_ID},
    )
    resp.raise_for_status()
    ctx = resp.json()["id"]

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
        if data.get("status") == "Finished":
            res = data.get("results", {})
            if res.get("resultType") == "error":
                raise RuntimeError(res.get("summary", "SQL error"))
            cols = [c["name"] for c in res.get("schema", [])]
            return pd.DataFrame(res.get("data", []), columns=cols)
        if data.get("status") in ("Error", "Cancelled"):
            raise RuntimeError(str(data.get("results", data)))
        time.sleep(4)
    raise TimeoutError(f"Query timed out after {timeout}s")


def fetch_locations() -> tuple[pd.DataFrame, str, str]:
    start_date, end_date = get_last_n_full_weeks(4)
    sql = f"""
    WITH portfolio AS (
        SELECT
            p.provider_id,
            p.provider_name,
            p.brand_name,
            p.city_name,
            p.zone_name,
            p.business_segment_v2,
            CAST(p.is_top_brand AS INT) AS is_top_brand
        FROM ng_delivery_spark.dim_provider_v2 p
        WHERE p.account_manager_name = '{ACCOUNT_MANAGER}'
          AND p.country_code = '{COUNTRY_CODE}'
          AND p.provider_status = 'active'
    ),
    smart_active AS (
        SELECT DISTINCT provider_id
        FROM ng_delivery_spark.delivery_smart_promotion_log
        WHERE state = 'active'
          AND promotion_type = 'smart_promotion'
          AND (end IS NULL OR end >= current_timestamp())
          AND (start IS NULL OR start <= current_timestamp())
    ),
    sl_active AS (
        SELECT provider_id
        FROM ng_delivery_spark.fact_provider_weekly f
        WHERE CAST(f.metric_timestamp_local AS DATE) >= date_sub(current_date(), 7)
        GROUP BY provider_id
        HAVING SUM(COALESCE(f.sponsored_listing_duration_hours, 0)) > 0
    ),
    metrics AS (
        SELECT
            f.provider_id,
            SUM(f.delivered_orders_count) AS delivered_orders,
            SUM(f.total_gmv_before_discounts_eur) AS gmv_eur,
            SUM(f.total_contribution_profit_eur) AS cp_eur,
            SUM(f.total_provider_price_before_discounts) AS gross_uah,
            SUM(f.total_provider_price_after_discounts) AS net_uah,
            CASE
                WHEN SUM(f.total_gmv_before_discounts_eur) > 0
                THEN SUM(f.total_contribution_profit_eur)
                     / SUM(f.total_gmv_before_discounts_eur) * 100
                ELSE NULL
            END AS cp_margin_pct,
            SUM(f.provider_impressions_sessions_count) AS impressions,
            SUM(f.provider_order_placed_sessions_count) AS order_sessions,
            AVG(f.provider_active_rate_value) AS provider_active_rate
        FROM ng_delivery_spark.fact_provider_weekly f
        WHERE CAST(f.metric_timestamp_local AS DATE) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY f.provider_id
    ),
    sl_metrics AS (
        SELECT
            f.provider_id,
            SUM(f.sponsored_listing_duration_hours) AS sl_duration_hours,
            SUM(
                COALESCE(f.sponsored_listing_attributed_gmv_share_value, 0)
                * COALESCE(f.total_gmv_before_discounts_eur, 0)
            ) AS sl_attributed_gmv_eur,
            SUM(COALESCE(f.total_portal_campaign_spend_provider_eur, 0)) AS sl_portal_spend_eur
        FROM ng_delivery_spark.fact_provider_weekly f
        WHERE CAST(f.metric_timestamp_local AS DATE) BETWEEN '{start_date}' AND '{end_date}'
          AND COALESCE(f.sponsored_listing_duration_hours, 0) > 0
        GROUP BY f.provider_id
    )
    SELECT
        p.provider_id,
        p.provider_name,
        p.brand_name,
        p.city_name,
        p.zone_name,
        p.business_segment_v2,
        p.is_top_brand,
        CASE WHEN s.provider_id IS NOT NULL THEN 1 ELSE 0 END AS has_smart_promotion,
        CASE WHEN sl.provider_id IS NOT NULL THEN 1 ELSE 0 END AS has_sponsored_listing,
        COALESCE(m.delivered_orders, 0) AS delivered_orders,
        COALESCE(m.gmv_eur, 0) AS gmv_eur,
        COALESCE(m.cp_eur, 0) AS cp_eur,
        COALESCE(m.gross_uah, 0) AS gross_uah,
        COALESCE(m.net_uah, 0) AS net_uah,
        m.cp_margin_pct,
        COALESCE(m.impressions, 0) AS impressions,
        ROUND(
            COALESCE(m.order_sessions, 0) * 100.0
            / NULLIF(COALESCE(m.impressions, 0), 0),
            2
        ) AS conv_imp_to_order_pct,
        ROUND(COALESCE(m.provider_active_rate, 0) * 100, 1) AS provider_active_rate_pct,
        COALESCE(sl_m.sl_attributed_gmv_eur, 0) AS sl_attributed_gmv_eur,
        COALESCE(sl_m.sl_portal_spend_eur, 0) AS sl_portal_spend_eur,
        COALESCE(sl_m.sl_duration_hours, 0) AS sl_duration_hours
    FROM portfolio p
    LEFT JOIN smart_active s ON p.provider_id = s.provider_id
    LEFT JOIN sl_active sl ON p.provider_id = sl.provider_id
    LEFT JOIN metrics m ON p.provider_id = m.provider_id
    LEFT JOIN sl_metrics sl_m ON p.provider_id = sl_m.provider_id
    ORDER BY p.city_name, p.brand_name, p.provider_name
    """
    print(f"Fetching data ({start_date} — {end_date})…")
    df = _exec_sql(sql)
    numeric_cols = [
        "is_top_brand", "has_smart_promotion", "has_sponsored_listing",
        "delivered_orders", "gmv_eur", "cp_eur", "gross_uah", "net_uah",
        "cp_margin_pct", "impressions", "conv_imp_to_order_pct",
        "provider_active_rate_pct", "sl_attributed_gmv_eur", "sl_portal_spend_eur",
        "sl_duration_hours",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    print(f"  → {len(df):,} locations")
    return df, start_date, end_date


def aggregate_brands(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (brand, city), grp in df.groupby(["brand_name", "city_name"], dropna=False):
        gmv = grp["gmv_eur"].sum()
        cp = grp["cp_eur"].sum()
        gross_uah = grp["gross_uah"].sum()
        net_uah = grp["net_uah"].sum()
        orders = grp["delivered_orders"].sum()
        impressions = grp["impressions"].sum()
        order_sessions = (
            grp["conv_imp_to_order_pct"].fillna(0) * grp["impressions"].fillna(0) / 100
        ).sum()
        conv = (order_sessions * 100 / impressions) if impressions > 0 else 0
        loc_count = len(grp)
        sl_attr_gmv = grp["sl_attributed_gmv_eur"].sum()
        sl_spend = grp["sl_portal_spend_eur"].sum()
        active_rate = grp["provider_active_rate_pct"].mean()
        rows.append({
            "brand_name": brand,
            "city_name": city,
            "business_segment_v2": grp["business_segment_v2"].dropna().iloc[0]
            if grp["business_segment_v2"].notna().any() else "",
            "is_top_brand": int(grp["is_top_brand"].max() or 0),
            "locations_count": loc_count,
            "smart_promo_locations": int((grp["has_smart_promotion"] == 1).sum()),
            "sl_locations": int((grp["has_sponsored_listing"] == 1).sum()),
            "has_smart_promotion": int(grp["has_smart_promotion"].max() or 0),
            "has_sponsored_listing": int(grp["has_sponsored_listing"].max() or 0),
            "delivered_orders": orders,
            "gmv_eur": gmv,
            "cp_eur": cp,
            "gross_uah": gross_uah,
            "net_uah": net_uah,
            "cp_margin_pct": (cp / gmv * 100) if gmv > 0 else None,
            "impressions": impressions,
            "conv_imp_to_order_pct": round(conv, 2),
            "provider_active_rate_pct": round(float(active_rate), 1) if pd.notna(active_rate) else None,
            "impressions_per_loc": impressions / loc_count if loc_count else 0,
            "sl_attributed_gmv_eur": sl_attr_gmv,
            "sl_portal_spend_eur": sl_spend,
            "actual_roas": (sl_attr_gmv / sl_spend) if sl_spend > 0 else None,
        })
    return pd.DataFrame(rows)


def _city_benchmarks(df_brand: pd.DataFrame) -> pd.DataFrame:
    """Медіани по місту для конверсії та видимості (покази на локацію)."""
    bench = (
        df_brand.groupby("city_name", dropna=False)
        .agg(
            city_median_conv=("conv_imp_to_order_pct", "median"),
            city_median_impr_per_loc=("impressions_per_loc", "median"),
        )
        .reset_index()
    )
    return df_brand.merge(bench, on="city_name", how="left")


def _portfolio_sl_benchmark_roas(df_brand: pd.DataFrame) -> float:
    """Еталонний ROAS: сукупний attr. GMV / portal spend по SL-брендах (стабільніше за медіану)."""
    sl = df_brand[df_brand["has_sponsored_listing"] == 1]
    total_gmv = sl["sl_attributed_gmv_eur"].sum()
    total_spend = sl["sl_portal_spend_eur"].sum()
    if total_spend > 0 and total_gmv > 0:
        return min(float(total_gmv / total_spend), 12.0)
    actual = sl.loc[sl["actual_roas"].notna(), "actual_roas"]
    if not actual.empty:
        return min(float(actual.median()), 12.0)
    return 5.0


def compute_predicted_roas(df_brand: pd.DataFrame) -> pd.DataFrame:
    """
    Прогнозований ROAS для Sponsored Listing.

    Окремого поля predicted_roas у Databricks немає — модель будується з:
      - фактичного ROAS (attr. GMV / portal spend) по брендах з активним SL у портфоліо;
      - конверсії показ → замовлення (чим вища — тим краще монетизуються додаткові покази);
      - видимості: низькі покази на локацію відносно медіани міста + низький active rate.
    """
    df = _city_benchmarks(df_brand.copy())
    benchmark = _portfolio_sl_benchmark_roas(df)

    def predict_row(row: pd.Series) -> Tuple[Optional[float], float, str]:
        conv = float(row.get("conv_imp_to_order_pct") or 0)
        impr_loc = float(row.get("impressions_per_loc") or 0)
        city_conv = float(row.get("city_median_conv") or 2.5) or 2.5
        city_impr = float(row.get("city_median_impr_per_loc") or impr_loc) or impr_loc or 1
        active = float(row.get("provider_active_rate_pct") or 95)

        visibility_gap = min(max(city_impr / max(impr_loc, 1), 1.0), 2.5)
        conv_factor = min(max(conv / max(city_conv, 0.5), 0.7), 1.5)
        active_factor = min(max((100 - active) / 20 + 1, 0.9), 1.2)

        if int(row.get("has_sponsored_listing") or 0) and row.get("actual_roas") is not None:
            roas = min(float(row["actual_roas"]), 15.0)
            return round(roas, 2), visibility_gap, "фактичний"

        predicted = benchmark * conv_factor * (0.7 + 0.3 * visibility_gap) * active_factor
        return round(min(predicted, 15.0), 2), visibility_gap, "прогноз"

    results = df.apply(predict_row, axis=1, result_type="expand")
    df["predicted_roas"] = results[0]
    df["visibility_gap"] = results[1].round(2)
    df["roas_source"] = results[2]
    df["benchmark_roas"] = round(benchmark, 2)
    return df


def is_sl_candidate(row: pd.Series) -> bool:
    """Висока конверсія + низька видимість — ідеальний кандидат на SL."""
    if int(row.get("has_sponsored_listing") or 0):
        return False
    conv = float(row.get("conv_imp_to_order_pct") or 0)
    city_conv = float(row.get("city_median_conv") or 2.5)
    visibility_gap = float(row.get("visibility_gap") or 1)
    roas = float(row.get("predicted_roas") or 0)
    cp = float(row.get("cp_margin_pct") or 0)

    good_conv = conv >= max(city_conv, 2.5)
    low_visibility = visibility_gap >= 1.25
    return good_conv and low_visibility and roas >= SL_ROAS_THRESHOLD and cp >= 0


def sl_priority_score(row: pd.Series) -> float:
    roas = float(row.get("predicted_roas") or 0)
    visibility_gap = float(row.get("visibility_gap") or 1)
    conv = float(row.get("conv_imp_to_order_pct") or 0)
    gmv = float(row.get("gmv_eur") or 0)
    return roas * 0.45 + visibility_gap * 0.30 + (conv / 5) * 0.15 + min(gmv / 10000, 1) * 0.10


def recommend_product(row: pd.Series) -> tuple[str, str]:
    gmv = float(row.get("gmv_eur") or 0)
    cp = float(row.get("cp_margin_pct") or 0)
    conv = float(row.get("conv_imp_to_order_pct") or 0)
    orders = float(row.get("delivered_orders") or 0)
    roas = float(row.get("predicted_roas") or 0)
    visibility_gap = float(row.get("visibility_gap") or 1)
    city_conv = float(row.get("city_median_conv") or 2.5)

    if cp < 0:
        return (
            "Спочатку операційні показники",
            "Від'ємна CP L2 маржа — перед промо варто стабілізувати якість/доступність. "
            "Після покращення — легке Sponsored Listing для тесту видимості.",
        )
    if is_sl_candidate(row):
        return (
            "Sponsored Listing",
            f"Сильна конверсія ({conv:.1f}% vs медіана міста {city_conv:.1f}%), але низька видимість "
            f"(покази на локацію нижчі за медіану міста в {visibility_gap:.1f}×). "
            f"Прогнозований ROAS ~{roas:.1f} — платне просування має добре окупитися.",
        )
    if conv < 2.0 and gmv >= 3000:
        return (
            "Sponsored Listing → Smart Promotions",
            "Високий GMV, але низька конверсія з показів — спочатку підсилити видимість "
            "(платне просування / пошук), потім Розумні акції для зростання замовлень.",
        )
    if conv >= 3.0 and cp >= 10 and visibility_gap < 1.2:
        return (
            "Smart Promotions",
            "Сильна конверсія та позитивна маржа — Розумні акції зі співінвестом Bolt "
            "дозволять масштабувати замовлення без надмірного навантаження на маржу.",
        )
    if gmv >= 5000:
        return (
            "Smart Promotions + Sponsored Listing",
            "Топовий обсяг продажів — комбінуйте видимість (Sponsored Listing) "
            "та залучення когорт (Розумні акції) для максимального ефекту.",
        )
    if orders >= 200:
        return (
            "Smart Promotions",
            "Стабільний обсяг замовлень — Розумні акції для активних/нових клієнтів "
            "допоможуть прискорити зростання GMV.",
        )
    if roas >= SL_ROAS_THRESHOLD and visibility_gap >= 1.1:
        return (
            "Sponsored Listing",
            f"Прогнозований ROAS ~{roas:.1f} при потенціалі зростання видимості — "
            "почніть з платного просування в пошуку.",
        )
    return (
        "Sponsored Listing",
        "Середній обсяг — почніть з Sponsored Listing для підвищення показів у пошуку, "
        "далі додайте Розумні акції за результатами.",
    )


def priority_score(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    gmv_max = max(df["gmv_eur"].max(), 1)
    conv_max = max(df["conv_imp_to_order_pct"].max(), 0.1)
    orders_max = max(df["delivered_orders"].max(), 1)

    def score_row(row):
        gmv_n = float(row["gmv_eur"] or 0) / gmv_max
        conv_n = float(row["conv_imp_to_order_pct"] or 0) / conv_max
        orders_n = float(row["delivered_orders"] or 0) / orders_max
        cp = float(row["cp_margin_pct"] or 0)
        cp_n = min(max(cp, 0) / 25, 1)
        top_bonus = 0.08 if int(row.get("is_top_brand") or 0) else 0
        return gmv_n * 0.35 + cp_n * 0.25 + conv_n * 0.25 + orders_n * 0.15 + top_bonus

    return df.apply(score_row, axis=1)


def fmt_num(val, decimals=0) -> str:
    try:
        v = float(val)
        if decimals == 0:
            return f"{int(round(v)):,}".replace(",", "\u00a0")
        return f"{v:,.{decimals}f}".replace(",", "\u00a0")
    except (TypeError, ValueError):
        return "—"


def fmt_eur(val, decimals=0) -> str:
    try:
        v = float(val)
        return f"€{v:,.{decimals}f}".replace(",", "\u00a0")
    except (TypeError, ValueError):
        return "—"


def fmt_pct(val) -> str:
    try:
        return f"{float(val):.1f}%"
    except (TypeError, ValueError):
        return "—"


def fmt_roas(val) -> str:
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return "—"
        return f"{float(val):.1f}×"
    except (TypeError, ValueError):
        return "—"


def brand_key(brand: str, city: str) -> str:
    return f"{brand}|{city}"


def clean_zone(zone: str) -> str:
    return str(zone or "").replace("\xa0", " ").strip()


def build_brand_payload(
    df_loc: pd.DataFrame,
    df_brand: pd.DataFrame,
    start_date: str,
    end_date: str,
) -> dict:
    """JSON-ready payload per brand+city for client-side message generation."""
    payload: dict = {}
    candidates = df_brand[
        (df_brand["has_smart_promotion"] == 0) & (df_brand["has_sponsored_listing"] == 0)
    ]

    for _, brow in candidates.iterrows():
        brand = str(brow["brand_name"] or "")
        city = str(brow["city_name"] or "")
        key = brand_key(brand, city)
        product, reason = recommend_product(brow)

        locs = df_loc[
            (df_loc["brand_name"] == brand)
            & (df_loc["city_name"] == city)
            & (df_loc["has_smart_promotion"] == 0)
            & (df_loc["has_sponsored_listing"] == 0)
        ].sort_values("gross_uah", ascending=False)

        locations = []
        for _, loc in locs.iterrows():
            locations.append({
                "name": str(loc.get("provider_name") or ""),
                "zone": clean_zone(str(loc.get("zone_name") or "")),
                "orders": int(float(loc.get("delivered_orders") or 0)),
                "gross_uah": round(float(loc.get("gross_uah") or 0)),
                "net_uah": round(float(loc.get("net_uah") or 0)),
                "conv_pct": round(float(loc.get("conv_imp_to_order_pct") or 0), 2)
                if loc.get("conv_imp_to_order_pct") is not None and pd.notna(loc.get("conv_imp_to_order_pct"))
                else None,
            })

        gross_uah = round(float(locs["gross_uah"].sum() if not locs.empty else brow.get("gross_uah") or 0))
        net_uah = round(float(locs["net_uah"].sum() if not locs.empty else brow.get("net_uah") or 0))
        payload[key] = {
            "brand": brand,
            "city": city,
            "product": product,
            "reason": reason,
            "is_top_brand": bool(int(brow.get("is_top_brand") or 0)),
            "locations_count": int(brow.get("locations_count") or len(locations)),
            "delivered_orders": int(float(brow.get("delivered_orders") or 0)),
            "gross_uah": gross_uah,
            "net_uah": net_uah,
            "conv_imp_to_order_pct": round(float(brow.get("conv_imp_to_order_pct") or 0), 2),
            "predicted_roas": round(float(brow.get("predicted_roas") or 0), 2)
            if brow.get("predicted_roas") is not None and pd.notna(brow.get("predicted_roas"))
            else None,
            "visibility_gap": round(float(brow.get("visibility_gap") or 1), 2),
            "impressions": int(float(brow.get("impressions") or 0)),
            "period_start": start_date,
            "period_end": end_date,
            "locations": locations,
        }
    return payload


def send_rec_button(key: str) -> str:
    arg = json.dumps(key, ensure_ascii=False)
    return (
        f'<button type="button" class="btn-send-rec" '
        f"onclick='openRecommendation({arg})'>"
        f"📨 Надіслати рекомендацію підключення</button>"
    )


def build_brand_table_rows(
    df: pd.DataFrame,
    show_actual_roas: bool = False,
    show_predicted_roas: bool = False,
) -> str:
    extra_cols = int(show_actual_roas) + int(show_predicted_roas)
    if df.empty:
        return f'<tr><td colspan="{8 + extra_cols}" class="empty">Немає даних</td></tr>'
    html = ""
    for _, row in df.iterrows():
        top = '<span class="badge-top">TOP</span>' if int(row.get("is_top_brand") or 0) else ""
        cp = row.get("cp_margin_pct")
        cp_class = "pos" if cp is not None and float(cp) > 0 else "neg" if cp is not None and float(cp) < 0 else ""
        smart_locs = int(row.get("smart_promo_locations") or row.get("locations_count") or 0)
        sl_locs = int(row.get("sl_locations") or row.get("locations_count") or 0)
        loc_note = ""
        if int(row.get("has_smart_promotion") or 0):
            loc_note = f'<span class="sub">{smart_locs} лок. з Smart Promo</span>'
        elif int(row.get("has_sponsored_listing") or 0):
            loc_note = f'<span class="sub">{sl_locs} лок. з SL</span>'
        roas_cell = ""
        if show_actual_roas:
            roas = row.get("actual_roas") or row.get("predicted_roas")
            src = row.get("roas_source") or ""
            roas_cell = (
                f'<td class="num"><strong>{fmt_roas(roas)}</strong>'
                f'<br><span class="sub">{src}</span></td>'
                if roas is not None and pd.notna(roas) else '<td class="num">—</td>'
            )
        predicted_cell = ""
        if show_predicted_roas:
            if int(row.get("has_sponsored_listing") or 0):
                predicted_cell = '<td class="num"><span class="sub">є SL</span></td>'
            else:
                vis = row.get("visibility_gap")
                predicted_cell = (
                    f'<td class="num"><strong>{fmt_roas(row.get("predicted_roas"))}</strong>'
                    f'<br><span class="sub">vis. {float(vis):.1f}×</span></td>'
                    if row.get("predicted_roas") is not None and pd.notna(row.get("predicted_roas"))
                    else '<td class="num">—</td>'
                )
        html += f"""
        <tr>
          <td><strong>{row.get('brand_name') or '—'}</strong> {top}<br>{loc_note}</td>
          <td>{row.get('city_name') or '—'}</td>
          <td class="num">{int(row.get('locations_count') or 1)}</td>
          <td class="num">{fmt_num(row.get('delivered_orders'))}</td>
          <td class="num">{fmt_eur(row.get('gmv_eur'))}</td>
          <td class="num {cp_class}">{fmt_pct(cp) if cp is not None else '—'}</td>
          <td class="num">{fmt_pct(row.get('conv_imp_to_order_pct'))}</td>
          <td class="num">{fmt_num(row.get('impressions'))}</td>
          {predicted_cell}{roas_cell}
        </tr>"""
    return html


def build_no_sl_roas_rows(df: pd.DataFrame) -> str:
    """Повний список брендів без SL з прогнозованим ROAS."""
    if df.empty:
        return '<tr><td colspan="12" class="empty">Немає даних</td></tr>'
    html = ""
    for _, row in df.iterrows():
        top = '<span class="badge-top">TOP</span>' if int(row.get("is_top_brand") or 0) else ""
        cp = row.get("cp_margin_pct")
        cp_class = "pos" if cp is not None and float(cp) > 0 else "neg" if cp is not None and float(cp) < 0 else ""
        smart_tag = (
            '<span class="tag tag-smart">Smart Promo</span>'
            if int(row.get("has_smart_promotion") or 0) else '<span class="muted">—</span>'
        )
        product, _ = recommend_product(row)
        key = brand_key(str(row.get("brand_name") or ""), str(row.get("city_name") or ""))
        action = (
            send_rec_button(key)
            if int(row.get("has_smart_promotion") or 0) == 0
            else '<span class="muted">вже є Smart Promo</span>'
        )
        html += f"""
        <tr>
          <td><strong>{row.get('brand_name') or '—'}</strong> {top}</td>
          <td>{row.get('city_name') or '—'}</td>
          <td class="num">{int(row.get('locations_count') or 1)}</td>
          <td class="num"><strong>{fmt_roas(row.get('predicted_roas'))}</strong></td>
          <td class="num">{float(row.get('visibility_gap') or 1):.1f}×</td>
          <td class="num">{fmt_pct(row.get('conv_imp_to_order_pct'))}</td>
          <td class="num">{fmt_num(row.get('impressions'))}</td>
          <td class="num">{fmt_eur(row.get('gmv_eur'))}</td>
          <td class="num {cp_class}">{fmt_pct(cp) if cp is not None else '—'}</td>
          <td>{smart_tag}</td>
          <td class="sub">{product}</td>
          <td class="action-cell">{action}</td>
        </tr>"""
    return html


def build_sl_recommendation_rows(df: pd.DataFrame) -> str:
    if df.empty:
        return '<tr><td colspan="10" class="empty">Немає кандидатів</td></tr>'
    html = ""
    for i, (_, row) in enumerate(df.iterrows(), 1):
        _, reason = recommend_product(row)
        key = brand_key(str(row.get("brand_name") or ""), str(row.get("city_name") or ""))
        top = '<span class="badge-top">TOP</span>' if int(row.get("is_top_brand") or 0) else ""
        html += f"""
        <tr>
          <td class="num"><strong>#{i}</strong></td>
          <td><strong>{row.get('brand_name') or '—'}</strong> {top}</td>
          <td>{row.get('city_name') or '—'}</td>
          <td class="num">{fmt_num(row.get('predicted_roas'), 1)}</td>
          <td class="num">{fmt_pct(row.get('conv_imp_to_order_pct'))}</td>
          <td class="num">{fmt_num(row.get('impressions'))}</td>
          <td class="num">{float(row.get('visibility_gap') or 1):.1f}×</td>
          <td class="sub" style="max-width:280px">{reason}</td>
          <td class="action-cell">{send_rec_button(key)}</td>
        </tr>"""
    return html


def build_table_rows(df: pd.DataFrame, show_features: bool = False) -> str:
    if df.empty:
        return '<tr><td colspan="10" class="empty">Немає даних</td></tr>'
    html = ""
    for _, row in df.iterrows():
        top = '<span class="badge-top">TOP</span>' if int(row.get("is_top_brand") or 0) else ""
        feat = ""
        if show_features:
            tags = []
            if int(row.get("has_smart_promotion") or 0):
                tags.append('<span class="tag tag-smart">Smart Promotions</span>')
            if int(row.get("has_sponsored_listing") or 0):
                tags.append('<span class="tag tag-sl">Sponsored Listing</span>')
            feat = " ".join(tags) if tags else '<span class="muted">—</span>'
        cp = row.get("cp_margin_pct")
        cp_class = "pos" if cp is not None and float(cp) > 0 else "neg" if cp is not None and float(cp) < 0 else ""
        html += f"""
        <tr>
          <td><strong>{row.get('brand_name') or '—'}</strong> {top}<br>
              <span class="sub">{row.get('provider_name') or ''}</span></td>
          <td>{row.get('city_name') or '—'}</td>
          <td class="sub">{row.get('zone_name') or '—'}</td>
          <td class="num">{fmt_num(row.get('delivered_orders'))}</td>
          <td class="num">{fmt_eur(row.get('gmv_eur'))}</td>
          <td class="num {cp_class}">{fmt_pct(cp) if cp is not None else '—'}</td>
          <td class="num">{fmt_pct(row.get('conv_imp_to_order_pct')) if row.get('conv_imp_to_order_pct') is not None else '—'}</td>
          {f'<td>{feat}</td>' if show_features else ''}
        </tr>"""
    return html


def build_recommendation_cards(df: pd.DataFrame) -> str:
    if df.empty:
        return '<p class="empty-block">Немає кандидатів для рекомендацій.</p>'
    html = '<div class="rec-grid">'
    for i, (_, row) in enumerate(df.iterrows(), 1):
        product, reason = recommend_product(row)
        top = '<span class="badge-top">TOP</span>' if int(row.get("is_top_brand") or 0) else ""
        key = brand_key(str(row.get("brand_name") or ""), str(row.get("city_name") or ""))
        html += f"""
        <div class="rec-card">
          <div class="rec-rank">#{i}</div>
          <div class="rec-title">{row.get('brand_name') or '—'} {top}</div>
          <div class="rec-city">{row.get('city_name')} · {int(row.get('locations_count') or 1)} лок.</div>
          <div class="rec-metrics">
            <span>GMV {fmt_eur(row.get('gmv_eur'))}</span>
            <span>Зам. {fmt_num(row.get('delivered_orders'))}</span>
            <span>CP {fmt_pct(row.get('cp_margin_pct'))}</span>
            <span>Conv. {fmt_pct(row.get('conv_imp_to_order_pct'))}</span>
            <span>ROAS {fmt_roas(row.get('predicted_roas'))}</span>
            <span>Vis. {float(row.get('visibility_gap') or 1):.1f}×</span>
          </div>
          <div class="rec-product">{product}</div>
          <div class="rec-reason">{reason}</div>
          <div class="rec-actions">{send_rec_button(key)}</div>
        </div>"""
    html += "</div>"
    return html


def build_html(
    df_loc: pd.DataFrame,
    df_brand: pd.DataFrame,
    df_rec: pd.DataFrame,
    df_sl_rec: pd.DataFrame,
    start_date: str,
    end_date: str,
    brand_payload: dict,
    benchmark_roas: float,
) -> str:
    n_loc = len(df_loc)
    n_brands = len(df_brand)
    n_smart_loc = int((df_loc["has_smart_promotion"] == 1).sum())
    n_sl_loc = int((df_loc["has_sponsored_listing"] == 1).sum())
    n_both_loc = int(((df_loc["has_smart_promotion"] == 1) & (df_loc["has_sponsored_listing"] == 1)).sum())
    n_neither_brands = int(
        ((df_brand["has_smart_promotion"] == 0) & (df_brand["has_sponsored_listing"] == 0)).sum()
    )

    df_smart_brands = df_brand[df_brand["has_smart_promotion"] == 1].sort_values(
        ["city_name", "gmv_eur"], ascending=[True, False]
    )
    df_sl_brands = df_brand[df_brand["has_sponsored_listing"] == 1].sort_values(
        ["city_name", "gmv_eur"], ascending=[True, False]
    )
    n_smart_brands = len(df_smart_brands)
    n_sl_brands = len(df_sl_brands)
    df_no_sl = df_brand[df_brand["has_sponsored_listing"] == 0].sort_values(
        "predicted_roas", ascending=False, na_position="last"
    )
    n_no_sl_brands = len(df_no_sl)
    no_sl_roas_rows = build_no_sl_roas_rows(df_no_sl)
    smart_brand_rows = build_brand_table_rows(df_smart_brands, show_predicted_roas=True)
    sl_brand_rows = build_brand_table_rows(df_sl_brands, show_actual_roas=True)
    sl_rec_rows = build_sl_recommendation_rows(df_sl_rec)
    df_neither = df_brand[
        (df_brand["has_smart_promotion"] == 0) & (df_brand["has_sponsored_listing"] == 0)
    ].sort_values("predicted_roas", ascending=False, na_position="last")

    neither_rows = ""
    for _, row in df_neither.iterrows():
        product, _ = recommend_product(row)
        cp = row.get("cp_margin_pct")
        cp_class = "pos" if cp is not None and float(cp) > 0 else "neg" if cp is not None and float(cp) < 0 else ""
        top = '<span class="badge-top">TOP</span>' if int(row.get("is_top_brand") or 0) else ""
        key = brand_key(str(row.get("brand_name") or ""), str(row.get("city_name") or ""))
        neither_rows += f"""
        <tr>
          <td><strong>{row.get('brand_name') or '—'}</strong> {top}</td>
          <td>{row.get('city_name') or '—'}</td>
          <td class="num">{int(row.get('locations_count') or 1)}</td>
          <td class="num">{fmt_num(row.get('delivered_orders'))}</td>
          <td class="num">{fmt_eur(row.get('gmv_eur'))}</td>
          <td class="num {cp_class}">{fmt_pct(cp) if cp is not None else '—'}</td>
          <td class="num">{fmt_pct(row.get('conv_imp_to_order_pct'))}</td>
          <td class="num"><strong>{fmt_roas(row.get('predicted_roas'))}</strong></td>
          <td class="num">{float(row.get('visibility_gap') or 1):.1f}×</td>
          <td><span class="tag tag-rec">{product}</span></td>
          <td class="action-cell">{send_rec_button(key)}</td>
        </tr>"""

    brands_json = json.dumps(brand_payload, ensure_ascii=False)

    return f"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Потенційне підключення — {ACCOUNT_MANAGER}</title>
<style>
  :root {{
    --bolt-green: #1DC462;
    --bolt-dark: #1A1A1A;
    --danger: #E53935;
    --warning: #FB8C00;
    --info: #1976D2;
    --purple: #7B1FA2;
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
  .header-logo {{ font-size: 22px; font-weight: 800; color: var(--bolt-green); }}
  .header-title {{ font-size: 15px; font-weight: 600; }}
  .header-sub {{ font-size: 12px; color: #aaa; margin-top: 2px; }}
  .header-meta {{ margin-left: auto; text-align: right; font-size: 12px; color: #aaa; }}
  .header-meta strong {{ color: var(--bolt-green); }}
  .container {{ max-width: 1280px; margin: 0 auto; padding: 24px 20px 48px; }}
  .stats {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 14px;
    margin-bottom: 28px;
  }}
  .stat {{
    background: #fff;
    border-radius: 12px;
    padding: 16px 18px;
    box-shadow: var(--shadow);
    border-top: 4px solid var(--bolt-green);
  }}
  .stat.warn {{ border-top-color: var(--warning); }}
  .stat.info {{ border-top-color: var(--info); }}
  .stat.purple {{ border-top-color: var(--purple); }}
  .stat-label {{ font-size: 11px; text-transform: uppercase; color: var(--muted); font-weight: 600; }}
  .stat-value {{ font-size: 26px; font-weight: 800; margin: 4px 0; }}
  .stat-sub {{ font-size: 11px; color: var(--muted); }}
  .section {{
    background: #fff;
    border-radius: 12px;
    box-shadow: var(--shadow);
    padding: 20px 22px;
    margin-bottom: 24px;
  }}
  .section h2 {{
    font-size: 16px;
    margin-bottom: 6px;
    display: flex;
    align-items: center;
    gap: 8px;
  }}
  .section-sub {{ font-size: 12px; color: var(--muted); margin-bottom: 16px; }}
  .note {{
    background: #F0FAF4;
    border-left: 4px solid var(--bolt-green);
    padding: 12px 14px;
    border-radius: 0 8px 8px 0;
    font-size: 12px;
    color: #444;
    margin-bottom: 16px;
  }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{
    text-align: left;
    padding: 10px 12px;
    background: #F5F5F5;
    font-size: 11px;
    text-transform: uppercase;
    color: var(--muted);
    border-bottom: 2px solid var(--border);
  }}
  td {{ padding: 10px 12px; border-bottom: 1px solid var(--border); vertical-align: top; }}
  tr:hover td {{ background: #FAFFFE; }}
  .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .sub {{ font-size: 11px; color: var(--muted); }}
  .muted {{ color: var(--muted); }}
  .pos {{ color: #2E7D32; font-weight: 600; }}
  .neg {{ color: var(--danger); font-weight: 600; }}
  .badge-top {{
    font-size: 9px; background: #FFD600; color: #333;
    border-radius: 6px; padding: 1px 5px; font-weight: 700; margin-left: 4px;
  }}
  .tag {{
    display: inline-block; font-size: 10px; font-weight: 700;
    border-radius: 8px; padding: 2px 8px; white-space: nowrap;
  }}
  .tag-smart {{ background: #EDE7F6; color: var(--purple); }}
  .tag-sl {{ background: #E3F2FD; color: var(--info); }}
  .tag-rec {{ background: #E8F9EE; color: #1A9A5A; }}
  .rec-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
    gap: 14px;
  }}
  .rec-card {{
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 14px 16px;
    background: #FAFFFE;
    position: relative;
  }}
  .rec-rank {{
    position: absolute; top: 12px; right: 12px;
    font-size: 11px; font-weight: 800; color: var(--bolt-green);
  }}
  .rec-title {{ font-size: 14px; font-weight: 700; margin-bottom: 2px; padding-right: 28px; }}
  .rec-city {{ font-size: 11px; color: var(--muted); margin-bottom: 10px; }}
  .rec-metrics {{
    display: flex; flex-wrap: wrap; gap: 6px 10px;
    font-size: 11px; margin-bottom: 10px;
  }}
  .rec-product {{ font-size: 12px; font-weight: 700; color: var(--purple); margin-bottom: 6px; }}
  .rec-reason {{ font-size: 11px; color: #444; line-height: 1.45; margin-bottom: 10px; }}
  .rec-actions {{ margin-top: 4px; }}
  .btn-send-rec {{
    display: inline-flex; align-items: center; gap: 4px;
    background: var(--bolt-green); color: #fff; border: none;
    border-radius: 8px; padding: 7px 12px; font-size: 11px; font-weight: 700;
    cursor: pointer; white-space: nowrap; transition: background 0.15s;
  }}
  .btn-send-rec:hover {{ background: #13A350; }}
  .action-cell {{ min-width: 200px; }}
  .modal-overlay {{
    display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.45);
    z-index: 200; align-items: center; justify-content: center; padding: 20px;
  }}
  .modal-overlay.open {{ display: flex; }}
  .modal {{
    background: #fff; border-radius: 14px; width: min(640px, 100%);
    max-height: 90vh; overflow: hidden; box-shadow: 0 12px 40px rgba(0,0,0,0.2);
    display: flex; flex-direction: column;
  }}
  .modal-header {{
    padding: 16px 20px; border-bottom: 1px solid var(--border);
    display: flex; align-items: center; justify-content: space-between; gap: 12px;
  }}
  .modal-header h3 {{ font-size: 15px; }}
  .modal-close {{
    background: none; border: none; font-size: 22px; cursor: pointer; color: var(--muted);
    line-height: 1; padding: 0 4px;
  }}
  .modal-body {{ padding: 16px 20px; overflow-y: auto; flex: 1; }}
  .modal-text {{
    width: 100%; min-height: 320px; border: 1px solid var(--border);
    border-radius: 10px; padding: 14px; font-family: inherit;
    font-size: 13px; line-height: 1.55; resize: vertical; background: #FAFFFE;
  }}
  .modal-footer {{
    padding: 12px 20px 16px; border-top: 1px solid var(--border);
    display: flex; gap: 10px; flex-wrap: wrap;
  }}
  .btn-modal {{
    border: none; border-radius: 8px; padding: 9px 16px;
    font-size: 12px; font-weight: 700; cursor: pointer;
  }}
  .btn-copy {{ background: var(--bolt-green); color: #fff; }}
  .btn-copy:hover {{ background: #13A350; }}
  .btn-copy.copied {{ background: #2E7D32; }}
  .empty, .empty-block {{ text-align: center; color: var(--muted); padding: 24px; }}
  .footer {{
    text-align: center; font-size: 11px; color: #999;
    padding: 20px 0 8px;
  }}
  .footer a {{ color: var(--bolt-green); }}
  @media (max-width: 768px) {{
    .header {{ flex-wrap: wrap; }}
    .header-meta {{ margin-left: 0; text-align: left; width: 100%; }}
  }}
</style>
</head>
<body>
  <header class="header">
    <div class="header-logo">⚡ Bolt Food</div>
    <div>
      <div class="header-title">Потенційне підключення — Smart Promotions & Sponsored Listing</div>
      <div class="header-sub">{ACCOUNT_MANAGER} · Портфоліо Україна</div>
    </div>
    <div class="header-meta">
      Оновлено: <strong>{REPORT_DATE}</strong><br>
      Метрики: {start_date} — {end_date}
    </div>
  </header>

  <div class="container">
    <div class="stats">
      <div class="stat">
        <div class="stat-label">Активних локацій</div>
        <div class="stat-value">{n_loc}</div>
        <div class="stat-sub">{n_brands} брендів</div>
      </div>
      <div class="stat purple">
        <div class="stat-label">Smart Promotions</div>
        <div class="stat-value">{n_smart_loc}</div>
        <div class="stat-sub">активні в порталі зараз</div>
      </div>
      <div class="stat info">
        <div class="stat-label">Sponsored Listing</div>
        <div class="stat-value">{n_sl_loc}</div>
        <div class="stat-sub">активні останні 7 днів</div>
      </div>
      <div class="stat warn">
        <div class="stat-label">Без жодної функції</div>
        <div class="stat-value">{n_neither_brands}</div>
        <div class="stat-sub">брендів-кандидатів</div>
      </div>
    </div>

    <div class="note">
      <strong>Методологія:</strong>
      Smart Promotions — <code>delivery_smart_promotion_log</code> зі статусом <em>active</em>.
      Sponsored Listing — локації з <code>sponsored_listing_duration_hours &gt; 0</code> за останні 7 днів.
      <strong>Прогнозований ROAS</strong> для SL: окремого поля в Databricks немає — розраховується з фактичного ROAS
      (attr. GMV ÷ portal spend) по брендах портфоліо з активним SL (еталон {benchmark_roas:.1f}×), з урахуванням
      конверсії та <em>видимості</em> (покази на локацію vs медіана міста, active rate).
      Рекомендація SL: висока конверсія + низька видимість + прогноз ROAS ≥ {SL_ROAS_THRESHOLD:.0f}.
    </div>

    <section class="section">
      <h2>🎯 Пріоритетні рекомендації — кому пропонувати в першу чергу</h2>
      <div class="section-sub">Топ-{len(df_rec)} брендів без Smart Promotions і Sponsored Listing · скоринг: GMV 35% + CP L2 25% + конверсія 25% + замовлення 15%</div>
      {build_recommendation_cards(df_rec)}
    </section>

    <section class="section">
      <h2>📊 Прогнозований ROAS — усі бренди без Sponsored Listing</h2>
      <div class="section-sub">{n_no_sl_brands} брендів · відсортовано за прогнозованим ROAS (вищий = кращий потенціал SL)</div>
      <table>
        <thead>
          <tr>
            <th>Бренд</th><th>Місто</th><th class="num">Лок.</th>
            <th class="num">Прогноз ROAS</th><th class="num">Visibility gap</th>
            <th class="num">Конверсія</th><th class="num">Покази</th>
            <th class="num">GMV</th><th class="num">CP L2 %</th>
            <th>Smart Promo</th><th>Рекомендація</th><th>Дія</th>
          </tr>
        </thead>
        <tbody>{no_sl_roas_rows}</tbody>
      </table>
    </section>

    <section class="section">
      <h2>🔵 Рекомендації Sponsored Listing — висока конверсія, низька видимість</h2>
      <div class="section-sub">Топ-{len(df_sl_rec)} кандидатів без SL · ранжування: прогнозований ROAS + visibility gap (покази нижче медіани міста)</div>
      <table>
        <thead>
          <tr>
            <th class="num">#</th><th>Бренд</th><th>Місто</th>
            <th class="num">Прогноз ROAS</th><th class="num">Конверсія</th>
            <th class="num">Покази</th><th class="num">Visibility gap</th>
            <th>Обґрунтування</th><th>Дія</th>
          </tr>
        </thead>
        <tbody>{sl_rec_rows}</tbody>
      </table>
    </section>

    <section class="section">
      <h2>🟣 Бренди з активними Smart Promotions</h2>
      <div class="section-sub">{n_smart_brands} брендів · {n_smart_loc} локацій · прогноз ROAS для тих, у кого ще немає SL</div>
      <table>
        <thead>
          <tr>
            <th>Бренд</th><th>Місто</th><th class="num">Лок.</th>
            <th class="num">Замовлення</th><th class="num">GMV</th>
            <th class="num">CP L2 %</th><th class="num">Конверсія</th><th class="num">Покази</th>
            <th class="num">Прогноз ROAS</th>
          </tr>
        </thead>
        <tbody>{smart_brand_rows}</tbody>
      </table>
    </section>

    <section class="section">
      <h2>🔷 Бренди з активним Sponsored Listing</h2>
      <div class="section-sub">{n_sl_brands} брендів · {n_sl_loc} локацій · ROAS = attr. GMV ÷ portal spend (4 тижні)</div>
      <table>
        <thead>
          <tr>
            <th>Бренд</th><th>Місто</th><th class="num">Лок.</th>
            <th class="num">Замовлення</th><th class="num">GMV</th>
            <th class="num">CP L2 %</th><th class="num">Конверсія</th><th class="num">Покази</th>
            <th class="num">ROAS</th>
          </tr>
        </thead>
        <tbody>{sl_brand_rows}</tbody>
      </table>
    </section>

    <section class="section">
      <h2>📋 Бренди без Smart Promotions і Sponsored Listing</h2>
      <div class="section-sub">{len(df_neither)} брендів · повний список з прогнозом ROAS та рекомендованим продуктом</div>
      <table>
        <thead>
          <tr>
            <th>Бренд</th><th>Місто</th><th class="num">Лок.</th>
            <th class="num">Замовлення</th><th class="num">GMV</th>
            <th class="num">CP L2 %</th><th class="num">Конверсія</th>
            <th class="num">Прогноз ROAS</th><th class="num">Visibility gap</th>
            <th>Рекомендація</th><th>Дія</th>
          </tr>
        </thead>
        <tbody>{neither_rows or '<tr><td colspan="11" class="empty">Немає</td></tr>'}</tbody>
      </table>
    </section>
  </div>

  <div id="recModal" class="modal-overlay" onclick="if(event.target===this)closeRecommendation()">
    <div class="modal" role="dialog" aria-labelledby="modalTitle">
      <div class="modal-header">
        <h3 id="modalTitle">Рекомендація підключення</h3>
        <button type="button" class="modal-close" onclick="closeRecommendation()" aria-label="Закрити">&times;</button>
      </div>
      <div class="modal-body">
        <textarea id="modalText" class="modal-text" readonly></textarea>
      </div>
      <div class="modal-footer">
        <button type="button" class="btn-modal btn-copy" id="btnCopy" onclick="copyRecommendation()">📋 Скопіювати текст</button>
        <button type="button" class="btn-modal" style="background:#eee;color:#333" onclick="closeRecommendation()">Закрити</button>
      </div>
    </div>
  </div>

  <div class="footer">
    Автоматично згенеровано · Bolt Food Partner Reports ·
    <a href="https://github.com/marharytazhytnyk-create/partner-reports">GitHub</a>
    · Наступне оновлення: щопонеділка о 14:00 (EEST)
  </div>

<script>
const BRAND_DATA = {brands_json};

function fmtUah(n) {{
  if (n == null || isNaN(n)) return '—';
  return Math.round(n).toLocaleString('uk-UA') + ' грн';
}}

function fmtPct(n) {{
  if (n == null || isNaN(n)) return '—';
  return Number(n).toFixed(1) + '%';
}}

function offerLabel(d) {{
  if (d.product && d.product.includes('операційні')) {{
    return 'Sponsored Listing';
  }}
  return 'Smart Promotions + Sponsored Listing';
}}

function buildWhyReason(d) {{
  const orders = d.delivered_orders || 0;
  const conv = d.conv_imp_to_order_pct || 0;
  const gross = d.gross_uah || 0;
  const roas = d.predicted_roas;
  const visGap = d.visibility_gap || 1;
  const impressions = d.impressions || 0;
  const parts = [];

  if (roas != null && roas >= {SL_ROAS_THRESHOLD} && visGap >= 1.25 && conv >= 2.5) {{
    parts.push(
      'У вас сильна конверсія (' + fmtPct(conv) + '), але покази нижчі за типовий рівень у місті ' +
      '(visibility gap ' + visGap.toFixed(1) + '×) — Sponsored Listing підніме видимість у пошуку. ' +
      'Прогнозований ROAS ~' + roas.toFixed(1) + '×.'
    );
  }}
  if (orders >= 400 || gross >= 400000) {{
    parts.push(
      'Топовий обсяг продажів — комбінуйте видимість (Sponsored Listing) ' +
      'та залучення когорт (Розумні акції) для максимального ефекту.'
    );
  }}
  if (conv < 2.5) {{
    parts.push(
      'Конверсія з показів у замовлення нижча за потенціал — Sponsored Listing підсилить видимість ' +
      'у пошуку та на головному екрані, а Smart Promotions допоможуть перетворити покази на замовлення.'
    );
  }} else if (conv >= 3) {{
    parts.push(
      'Сильна конверсія показує, що гості вже обирають вас — Smart Promotions зі співінвестом Bolt ' +
      'дозволять масштабувати цей попит, а Sponsored Listing додасть нових клієнтів у воронку.'
    );
  }} else {{
    parts.push(
      'Стабільний потік замовлень — поєднання Sponsored Listing та Розумних акцій допоможе ' +
      'наростити обсяг без різкого збільшення витрат з вашого боку.'
    );
  }}
  if (d.net_uah && d.gross_uah && d.net_uah / d.gross_uah >= 0.85) {{
    parts.push(
      'Високий чистий дохід відносно валового — є простір для контрольованого промо ' +
      'з позитивним впливом на підсумковий результат.'
    );
  }}
  return parts.join(' ');
}}

function locLabel(loc, city) {{
  const zone = (loc.zone || '').trim();
  if (zone) return loc.name + ' (' + zone + ')';
  return loc.name + ' (' + city + ')';
}}

function productOfferBlock() {{
  return [
    'Пропоную комбінований підхід:',
    '• Sponsored Listing — платне просування, зайдіть на портал для ресторанів, оберіть вкладку промоакції та оберіть Налаштувати інші акції та блок Запусти платне просування для більшої видимості.',
    '• Smart Promotions (Розумні акції) — співінвестиція 50/50 з Bolt для когорт активних, нових та «давно не замовляли» клієнтів, запускаються теж на порталі в розділі промоакції - перший блок у розділі Розумні акції.',
  ];
}}

function buildRecommendationText(d) {{
  const locs = d.locations || [];
  const lines = [];

  lines.push('Вітаю!');
  lines.push('');
  lines.push(
    'Переглянула показники вашого бренду ' + d.brand + ' (' + d.city + ') ' +
    'за період ' + d.period_start + ' — ' + d.period_end +
    ' і хочу запропонувати підключити ' + offerLabel(d) + '.'
  );
  lines.push('');
  lines.push('Чому саме зараз:');
  lines.push('• Доставлено замовлень: ' + d.delivered_orders.toLocaleString('uk-UA'));
  lines.push('• Валовий дохід: ' + fmtUah(d.gross_uah));
  lines.push('• Чистий прибуток: ' + fmtUah(d.net_uah));
  lines.push('• Конверсія (показ → замовлення): ' + fmtPct(d.conv_imp_to_order_pct));
  if (d.impressions) {{
    lines.push('• Покази (сесії): ' + d.impressions.toLocaleString('uk-UA'));
  }}
  if (d.predicted_roas != null && !isNaN(d.predicted_roas)) {{
    lines.push('• Прогнозований ROAS (Sponsored Listing): ~' + Number(d.predicted_roas).toFixed(1) + '×');
    if (d.visibility_gap != null) {{
      lines.push('• Visibility gap (покази vs медіана міста): ' + Number(d.visibility_gap).toFixed(1) + '×');
    }}
  }}
  lines.push('• ' + buildWhyReason(d));
  lines.push('');
  lines.push('На яких локації рекомендую підключити (' + locs.length + '):');

  locs.forEach((loc, i) => {{
    const stats = [loc.orders.toLocaleString('uk-UA') + ' зам.', fmtUah(loc.gross_uah)];
    if (loc.conv_pct != null) stats.push('conv. ' + fmtPct(loc.conv_pct));
    lines.push((i + 1) + '. ' + locLabel(loc, d.city) + ' — ' + stats.join(', '));
  }});

  if (locs.length > 1) {{
    lines.push('');
    lines.push(
      'Якщо у вас кілька точок, можемо стартувати з найсильнішої за Доходом — «' +
      locs[0].name + '» — і масштабувати на решту після перших результатів.'
    );
  }}

  lines.push('');
  lines.push('Що саме пропоную:');
  productOfferBlock().forEach(line => lines.push(line));
  lines.push('');
  lines.push('Буду рада відповісти на питання та допомогти з підключенням');

  return lines.join('\\n');
}}

function openRecommendation(key) {{
  const d = BRAND_DATA[key];
  if (!d) {{ alert('Дані для цього бренду не знайдено'); return; }}
  document.getElementById('modalTitle').textContent =
    'Рекомендація: ' + d.brand + ' · ' + d.city;
  document.getElementById('modalText').value = buildRecommendationText(d);
  document.getElementById('btnCopy').textContent = '📋 Скопіювати текст';
  document.getElementById('btnCopy').classList.remove('copied');
  document.getElementById('recModal').classList.add('open');
}}

function closeRecommendation() {{
  document.getElementById('recModal').classList.remove('open');
}}

function copyRecommendation() {{
  const ta = document.getElementById('modalText');
  ta.select();
  navigator.clipboard.writeText(ta.value).then(() => {{
    const btn = document.getElementById('btnCopy');
    btn.textContent = '✓ Скопійовано';
    btn.classList.add('copied');
  }}).catch(() => {{
    document.execCommand('copy');
    document.getElementById('btnCopy').textContent = '✓ Скопійовано';
  }});
}}

document.addEventListener('keydown', e => {{
  if (e.key === 'Escape') closeRecommendation();
}});
</script>
</body>
</html>"""


def main() -> None:
    global DATABRICKS_TOKEN
    print(f"=== Потенційне підключення [{REPORT_DATE}] ===\n")
    DATABRICKS_TOKEN = get_token()
    if not DATABRICKS_TOKEN:
        print("ERROR: DATABRICKS_TOKEN not set.", file=sys.stderr)
        sys.exit(1)

    df_loc, start_date, end_date = fetch_locations()
    df_brand = aggregate_brands(df_loc)
    df_brand = compute_predicted_roas(df_brand)
    benchmark_roas = float(df_brand["benchmark_roas"].iloc[0]) if len(df_brand) else 5.0

    mask_neither = (df_brand["has_smart_promotion"] == 0) & (df_brand["has_sponsored_listing"] == 0)
    df_candidates = df_brand[mask_neither].copy()
    df_candidates["priority_score"] = priority_score(df_candidates)
    df_rec = df_candidates.sort_values("priority_score", ascending=False).head(TOP_RECOMMENDATIONS)

    sl_pool = df_brand[mask_neither].copy()
    sl_pool["sl_score"] = sl_pool.apply(sl_priority_score, axis=1)
    sl_pool = sl_pool[sl_pool.apply(is_sl_candidate, axis=1)]
    df_sl_rec = sl_pool.sort_values("sl_score", ascending=False).head(TOP_SL_RECOMMENDATIONS)

    brand_payload = build_brand_payload(df_loc, df_brand, start_date, end_date)
    html = build_html(
        df_loc, df_brand, df_rec, df_sl_rec, start_date, end_date, brand_payload, benchmark_roas
    )
    out_path = Path(__file__).resolve().parent / OUTPUT_FILE
    out_path.write_text(html, encoding="utf-8")
    print(f"\n✅ Report saved → {out_path}")
    print(
        f"   Smart Promo: {(df_loc['has_smart_promotion']==1).sum()} loc | "
        f"Sponsored Listing: {(df_loc['has_sponsored_listing']==1).sum()} loc | "
        f"Candidates: {mask_neither.sum()} brands | "
        f"SL recommendations: {len(df_sl_rec)}"
    )


if __name__ == "__main__":
    main()
