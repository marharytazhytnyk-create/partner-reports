# Easter 2026 Partners — Ukraine

## Overview

This folder contains data and the interactive map for partners tagged with the **`ua-easter-2026`** Dynamic Segment trait in Bolt's system.

## Data Source

- **Databricks table**: `ng_delivery_spark.delivery_provider_provider_trait`
- **Trait**: `ua-easter-2026` (ID: 179228)
- **Joined with**: `ng_delivery_spark.dim_provider_v2` for coordinates and radius
- **Last updated**: 2026-04-02
- **Country filter**: Ukraine (`ua`)

## Files

| File | Description |
|------|-------------|
| `ukraine_events_map.html` | Interactive map with Easter 2026 partners layer, refresh button, and auto-daily update |
| `ua_easter_2026_partners.csv` | Raw data: provider_id, name, coordinates, delivery radius, city |

## Map Features

- 🐣 **Easter 2026 layer** — 1,383 Ukrainian partners shown as orange markers with delivery radius circles
- 🔄 **Refresh button** — manually refresh partner data from the source
- 📅 **Auto daily refresh** — map automatically refreshes partner data once per day (via `localStorage`)
- 🌙 **Dark/Light mode** toggle
- 📍 **Zone coverage** analysis for Ukrainian delivery zones

## Partner Stats (2026-04-02)

| City | Partners |
|------|----------|
| Kyiv | 768 |
| Lviv | 106 |
| Dnipro | 85 |
| Kharkiv | 79 |
| Vinnytsia | 57 |
| Odesa | 41 |
| Kryvyi Rih | 29 |
| Irpin | 28 |
| Brovary | 21 |
| Chernihiv | 18 |
| **Total** | **1,383** |

## Notes

The tag `global_cal_easter` was searched in Databricks but not found under that exact name.
The closest equivalent is `ua-easter-2026` (slug: `ua-easter-2026`, trait ID: 179228), which is the official Easter 2026 campaign tag for Ukrainian partners.

Partners without a custom delivery radius are assigned a default radius of **3 km**.
