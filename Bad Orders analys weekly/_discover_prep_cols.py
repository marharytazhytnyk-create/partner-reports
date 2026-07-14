#!/usr/bin/env python3
"""One-off schema discovery for Preparation time tab. Safe to delete after use."""
from __future__ import annotations

import os
from databricks import sql

SERVER_HOSTNAME = "bolt-incentives.cloud.databricks.com"
HTTP_PATH = "sql/protocolv1/o/2472566184436351/0221-081903-9ag4bh69"

KEYWORDS = (
    "prep", "cook", "estimat", "accept", "wait", "promise",
    "eta", "minute", "duration", "ready", "promise",
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
    ]

    # Also search information_schema for matching column names
    print("=== information_schema search ===")
    cur.execute(
        """
        SELECT table_schema, table_name, column_name, data_type
        FROM system.information_schema.columns
        WHERE lower(table_schema) LIKE '%delivery%'
          AND (
            lower(column_name) LIKE '%prep%'
            OR lower(column_name) LIKE '%cook%'
            OR lower(column_name) LIKE '%estimat%'
            OR lower(column_name) LIKE '%promise%'
          )
        ORDER BY table_schema, table_name, column_name
        LIMIT 300
        """
    )
    rows = cur.fetchall()
    for r in rows:
        print(f"  {r[0]}.{r[1]}.{r[2]} ({r[3]})")
    print(f"  total={len(rows)}")

    for t in tables:
        print(f"\n=== DESCRIBE {t} (filtered) ===")
        try:
            cur.execute(f"DESCRIBE TABLE {t}")
            cols = cur.fetchall()
            for r in cols:
                name = (r[0] or "").lower()
                if any(k in name for k in KEYWORDS):
                    print(f"  {r[0]} | {r[1]}")
        except Exception as e:
            print(f"  ERR: {e}")

    # Sample a few values from likely columns if discovery found them
    print("\n=== sample dim_provider_v2 columns (all) ===")
    try:
        cur.execute("DESCRIBE TABLE ng_delivery_spark.dim_provider_v2")
        for r in cur.fetchall():
            if r[0] and not str(r[0]).startswith("#"):
                print(f"  {r[0]} | {r[1]}")
    except Exception as e:
        print("ERR", e)

    conn.close()


if __name__ == "__main__":
    main()
