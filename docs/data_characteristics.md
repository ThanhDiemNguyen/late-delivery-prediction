# Data characteristics

What the generated source data looks like: its volume, which columns are skewed and by how much,
which columns have high cardinality, the schema before and after the app v2 change, and the
duplicate and dirty-value rates. How each part is generated is in
[data_generator.md](data_generator.md).

The numbers below were measured on the 1% development slice on 2026-10-23, with
`uv run python -m generator.check_data --scale 0.01`. They are measured, not targets: all 28 checks
passed. The full-scale run (at least 100 GB) is checked again with Spark, and this page is updated
then.

## Volume

| Source system | Dataset | Rows (dev slice) | Size (dev slice) | Full scale (projected by the dry run) |
|---|---|---:|---:|---:|
| Postgres `oms` | `orders` | 808,000 | 234.5 MB | 80.8M rows, 23.4 GB |
| Postgres `oms` | `customers` | 50,000 | 7.3 MB | 5M rows |
| Postgres `oms` | `couriers`, `courier_changes`, `hubs` | 200, 1,093, 40 | 0.3 MB | 20,000, about 104,000, 40 |
| SeaweedFS `app-exports` | `courier_pings` | 49,350,264 | 0.92 GB in 196 files | 4.89B rows, 88.2 GB |
| SeaweedFS `app-exports` | `parcel_scans` | 2,424,000 | 0.04 GB in 366 files | 242M rows, 2.0 GB |
| **Total** | | | **about 1.2 GB** | **about 114 GB** |

### Full-scale parameters, fixed by the dry run

`uv run python -m generator.dry_run --workers 6` builds one v1 day (2024-06-03) and one v2 day
(2024-10-01) at full scale in memory and serialises every file as the export does.

| Measured on one full day | 2024-06-03 (v1) | 2024-10-01 (v2) |
|---|---:|---:|
| Orders | 400,000 in 2.5 s | 400,000 in 2.7 s |
| Couriers working | 16,126 | 18,948 |
| Ping files | 59 | 69 |
| Ping rows | 23,685,732 | 27,830,664 |
| Ping time with 6 workers | 2.8 s (8.5M rows/s) | 2.6 s (10.5M rows/s) |
| Bytes per ping row (zstd Parquet) | 17.0 | 19.3 |
| Bytes per scan row | 8.3 | 8.3 |

`oms.orders` takes 290 bytes per row including its indexes (measured on the slice).

Parameters kept for the full run:

| Parameter | Value |
|---|---|
| Ping interval | 25 s |
| Couriers | 20,000 |
| Customers | 5,000,000 |
| Orders | 400,000 a day, ×2 and ×3 in the peak weeks |
| Workers | 6 |

The projected total is about 114 GB: 88.2 GB of pings, 2.0 GB of scans and 23.4 GB in `oms`.
Ping generation takes about 16 minutes for 183 days; the upload and the `oms` load come on top.

## Skewed columns

| Column | Skew | Measured (dev slice) |
|---|---|---|
| `orders.hub_id` | Downtown LA has 25% of orders; the top 10 of 40 hubs have 60% (a fair share would be 2.5% and 25%) | 24.84%; 59.89% |
| `courier_pings.hub_id` | couriers are spread by the same weights, so Downtown LA sends about 25% of pings | 28.92% (only 200 couriers in the slice) |
| `courier_pings.trip_id` | 30% of pings are idle and NULL. In a join on `trip_id` they all go to one partition | 29.62% NULL |
| `orders.area` | Metropolitan 55%, Urban 35%, Semi-Urban 7%, Other 3% | 55.08%, 35.23%, 6.67%, 3.02% |
| `orders.traffic` | Jam 35%, Low 25%, Medium 25%, High 15% | as generated |
| `orders.weather` | Sunny 62%, Cloudy 14%, Fog 12%, Windy 7%, Stormy 5% | as generated |
| `orders.order_date` | 2× in the mid-July sale week, 3× in Black Friday week | 12,000 orders a day in Black Friday week against 4,000 |

The label is skewed too, by design: 17.56% of orders are late. By traffic it is Low 7.1%,
Medium 15.0%, High 23.1% and Jam 24.4%.

## High-cardinality columns

| Column | Unique / total (dev slice) | Note |
|---|---:|---|
| `orders.order_id` | 1.0000 | primary key |
| `parcel_scans.scan_id` | 1.0000 | 2,424,000 distinct |
| `courier_pings.(courier_id, device_seq)` | 0.9804 | the natural key of a ping. It is below 1 only because of the 2% re-uploads (1 / 1.02 = 0.9804) |
| `orders.customer_id` | 0.0615 | 49,722 customers ordered, of 50,000 (5M at full scale) |
| `orders.(drop_lat, drop_lon)` | 0.0615 | one drop point per customer |
| `courier_pings.trip_id` (non-null) | 0.0134 | one trip id per order, repeated on every ping of the trip |
| `orders.courier_id` | 0.0002 | 200 couriers (20,000 at full scale) |

## Schema before and after the app v2 change

Pings from phones on app v1 and app v2 sit in the same `export_date` folders from 2024-09-01, in
separate files (`part-NNNNN.parquet` and `part-NNNNN-v2.parquet`).

| v1 (`app_version` 3.14.2) | v2 (`app_version` 4.0.0) | Change |
|---|---|---|
| `courier_id` int32 | `courier_id` int32 | |
| `device_seq` int64 | `device_seq` int64 | |
| `hub_id` int16 | `hub_id` int16 | |
| `trip_id` int64 | `trip_id` int64 | |
| `ping_ts` timestamp[ms, UTC] | `ping_ts` timestamp[ms, UTC] | |
| `lat`, `lon` double | `lat`, `lon` double | |
| `speed_kmh` int16 | `speed_mps` float | renamed, new unit and type |
| | `battery_pct` int8 | added |
| | `gps_accuracy_m` float | added |
| `app_version` string | `app_version` string | |
| `uploaded_ts` timestamp[ms, UTC] | `uploaded_ts` timestamp[ms, UTC] | |

The v2 share of pings by local day (dev slice):

| 08-31 | 09-01 | 09-02 | 09-03 | 09-04 | 09-05 | 09-06 | 09-07 | 09-08 | 09-09 | 09-10 | 09-11 | 09-12 | 09-13 | 09-14 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0% | 7.0% | 14.4% | 21.4% | 27.8% | 35.3% | 42.3% | 49.2% | 55.8% | 62.6% | 70.5% | 77.9% | 85.3% | 92.1% | 100% |

## Duplicates

| Dataset | Rate | How it shows |
|---|---|---|
| `courier_pings` | 2.00% extra rows (967,704 on 48,382,560 originals), which is 1.96% of all rows | whole 5-minute upload batches sent twice: same `(courier_id, device_seq)` and content, `uploaded_ts` 10 to 60 minutes later, same `export_date` |
| `orders`, `parcel_scans` | none offline | the stream duplicates scans (1.5%) later |

## Dirty values in `oms.orders`

The rates come from the seed file, so they match it:

| Dirty value | Seed | Dev slice |
|---|---:|---:|
| Null block: `"NaN "` traffic, `"NaN"` weather and `"NaN "` order_time in the same row | 0.208% | 0.203% |
| Store point (0, 0) | 8.013% | 8.010% |
| Store latitude sign-flipped | 0.430% | 0.432% |
| Pickup rollover after midnight (`pickup_time < order_time`) | 1.89% | 1.83% |
| Legacy spellings: trailing spaces, `"Metropolitian "` | every row | every row |

In `oms.courier_changes`, 32.9% of the rows after a courier's hiring change nothing: they are
rating reviews that kept the rating. The SCD2 dimension has to skip them.
