#!/usr/bin/env python3
"""Sample prep-time metrics for Marharyta portfolio — last 2 weeks."""
from __future__ import annotations

import os
from datetime import date, timedelta
from databricks import sql

SERVER_HOSTNAME = "bolt-incentives.cloud.databricks.com"
HTTP_PATH = "sql/protocolv1/o/2472566184436351/0221-081903-9ag4bh69"
ACCOUNT_MANAGER = "Marharyta Zhytnyk"
COUNTRY_CODE = "ua"


def main() -> None:
    today = date.today()
    last_sunday = today - timedelta(days=today.weekday() + 1)
    d1 = last_sunday.isoformat()
    d0 = (last_sunday - timedelta(days=13)).isoformat()
    print(f"Window: {d0} .. {d1}")

    conn = sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            p.provider_id,
            p.provider_name,
            p.brand_name,
            p.city_name,
            p.average_cooking_time_minutes AS cooking_time_min,
            COUNT(*) AS orders,
            ROUND(AVG(f.order_actual_cooking_time_minutes), 1) AS actual_prep_min,
            ROUND(AVG(f.order_default_cooking_time_minutes), 1) AS default_cook_min,
            ROUND(AVG(f.order_provider_estimated_cooking_time_seconds) / 60.0, 1) AS provider_est_min,
            ROUND(AVG(f.provider_stated_cooking_time_minutes), 1) AS stated_min,
            ROUND(AVG(f.provider_ml_estimated_adjusted_cooking_time_minutes), 1) AS ml_est_min,
            ROUND(AVG(f.provider_estimated_cooking_time), 1) AS provider_estimated_cooking_time
        FROM ng_delivery_spark.fact_order_delivery f
        INNER JOIN ng_delivery_spark.dim_provider_v2 p
            ON p.provider_id = f.provider_id
        WHERE p.account_manager_name = '{ACCOUNT_MANAGER}'
          AND p.country_code = '{COUNTRY_CODE}'
          AND f.order_created_date_local BETWEEN '{d0}' AND '{d1}'
          AND f.order_actual_cooking_time_minutes IS NOT NULL
          AND f.order_actual_cooking_time_minutes > 0
        GROUP BY 1,2,3,4,5
        HAVING COUNT(*) >= 5
        ORDER BY ABS(AVG(f.order_actual_cooking_time_minutes) - AVG(COALESCE(
            f.provider_ml_estimated_adjusted_cooking_time_minutes,
            f.order_default_cooking_time_minutes,
            f.order_provider_estimated_cooking_time_seconds / 60.0
        ))) DESC
        LIMIT 12
        """
    )
    cols = [d[0] for d in cur.description]
    print("COLS:", cols)
    for row in cur.fetchall():
        print(dict(zip(cols, row)))
    conn.close()
    print("DONE")


if __name__ == "__main__":
    main()
