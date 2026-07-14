#!/usr/bin/env python3
"""One-off schema discovery for Preparation time tab. Safe to delete after use."""
from __future__ import annotations

import os
from databricks import sql

SERVER_HOSTNAME = "bolt-incentives.cloud.databricks.com"
HTTP_PATH = "sql/protocolv1/o/2472566184436351/0221-081903-9ag4bh69"

KEYWORDS = (
    "prep", "cook", "estimat", "accept", "wait", "promise",
    "eta", "minute", "duration", "ready", "time",
)


def main() -> None:
    token = os.environ["DATABRICKS_TOKEN"]
    conn = sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=token,
    )
    cur = conn.cursor()

    tables = [
        "ng_delivery_spark.dim_provider_v2",
        "ng_delivery_spark.fact_provider_weekly",
        "ng_delivery_spark.fact_order_delivery",
        "ng_delivery_spark.delivery_order_order",
        "ng_delivery_spark.fact_vendor_weekly",
        "ng_delivery_spark.int_order_bad_order_attribution",
    ]

    for t in tables:
        print(f"\n=== DESCRIBE {t} ===")
        try:
            cur.execute(f"DESCRIBE TABLE {t}")
            cols = cur.fetchall()
            matched = []
            for r in cols:
                name = (r[0] or "")
                if name.startswith("#"):
                    continue
                low = name.lower()
                if any(k in low for k in KEYWORDS):
                    matched.append(f"  {r[0]} | {r[1]}")
            if matched:
                print("\n".join(matched))
            else:
                print("  (no keyword matches)")
                # print all columns for dim_provider only
                if "dim_provider" in t:
                    for r in cols:
                        if r[0] and not str(r[0]).startswith("#"):
                            print(f"  ALL: {r[0]} | {r[1]}")
        except Exception as e:
            print(f"  ERR: {e}")

    # Try SHOW TABLES looking for prep/cooking related tables
    print("\n=== SHOW TABLES like %prep% / %cook% / %menu% ===")
    for pattern in ("%prep%", "%cook%", "%menu%", "%estimat%"):
        try:
            cur.execute(f"SHOW TABLES IN ng_delivery_spark LIKE '{pattern}'")
            rows = cur.fetchall()
            print(f"pattern {pattern}: {rows[:30]}")
        except Exception as e:
            print(f"pattern {pattern} ERR: {e}")

    # Sample probe: try common column names
    print("\n=== probe sample columns ===")
    probes = [
        "SELECT provider_id, provider_name, cooking_time_minutes FROM ng_delivery_spark.dim_provider_v2 LIMIT 1",
        "SELECT provider_id, provider_name, cooking_time FROM ng_delivery_spark.dim_provider_v2 LIMIT 1",
        "SELECT provider_id, provider_name, preparation_time FROM ng_delivery_spark.dim_provider_v2 LIMIT 1",
        "SELECT provider_id, provider_name, default_preparation_time FROM ng_delivery_spark.dim_provider_v2 LIMIT 1",
        "SELECT provider_id, provider_name, preparation_time_minutes FROM ng_delivery_spark.dim_provider_v2 LIMIT 1",
        "SELECT provider_id, estimated_preparation_time_minutes FROM ng_delivery_spark.fact_order_delivery LIMIT 1",
        "SELECT provider_id, estimated_preparation_minutes FROM ng_delivery_spark.fact_order_delivery LIMIT 1",
        "SELECT provider_id, provider_preparation_minutes FROM ng_delivery_spark.fact_order_delivery LIMIT 1",
        "SELECT provider_id, actual_preparation_minutes FROM ng_delivery_spark.fact_order_delivery LIMIT 1",
        "SELECT provider_id, preparation_time_minutes FROM ng_delivery_spark.fact_order_delivery LIMIT 1",
        "SELECT provider_id, cooking_time_seconds FROM ng_delivery_spark.fact_order_delivery LIMIT 1",
        "SELECT provider_id, estimated_cooking_time_seconds FROM ng_delivery_spark.fact_order_delivery LIMIT 1",
        "SELECT provider_id, preparation_duration_seconds FROM ng_delivery_spark.fact_order_delivery LIMIT 1",
        "SELECT provider_id, promised_preparation_time_seconds FROM ng_delivery_spark.fact_order_delivery LIMIT 1",
        "SELECT * FROM ng_delivery_spark.fact_provider_weekly LIMIT 1",
    ]
    for q in probes:
        try:
            cur.execute(q)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
            print(f"OK: {q}")
            print(f"  cols={cols[:40]}")
            if rows:
                print(f"  sample_len={len(rows[0])}")
        except Exception as e:
            msg = str(e).split("\n")[0][:180]
            print(f"FAIL: {q}\n  {msg}")

    conn.close()
    print("\nDONE")


if __name__ == "__main__":
    main()
