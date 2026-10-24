# Data characteristics

What the generated source data looks like: its volume, which columns are skewed and by how much,
which columns have high cardinality, the schema before and after the app v2 change, and the
duplicate and dirty-value rates. How each part is generated is in
[data_generator.md](data_generator.md).

The numbers below were measured at full scale on 2026-10-24, with
`uv run python -m generator.check_data --scale 1.0`. They are measured, not targets: all 29 checks
passed. The run took 26.9 minutes on the Mac. It reads `oms` with SQL and the exports one file at a
time, so its memory stays at one file. The 1% development slice gave the same rates (28 of 28
checks on 2026-10-23).

## Volume

| Source system | Dataset | Rows | Size | Files |
|---|---|---:|---:|---:|
| Postgres `oms` | `orders` | 80,800,000 | 18.14 GB with its indexes | |
| Postgres `oms` | `customers` | 5,000,000 | 0.73 GB with its indexes | |
| Postgres `oms` | `couriers`, `courier_changes`, `hubs` | 20,000, 104,192, 40 | 0.02 GB | |
| Postgres `oms` | the whole database (`pg_database_size`) | | **18.89 GB** | |
| SeaweedFS `app-exports` | `courier_pings` | 4,888,196,724 (18.2 B/row) | **88.82 GB** | 12,920 |
| SeaweedFS `app-exports` | `parcel_scans` | 242,400,000 (10.2 B/row) | **2.48 GB** | 366 |
| **Total** | | | **110.19 GB** | |

Generation took 1 h 37 min on 2026-10-24, run unattended (`--scale 1.0`, the same seed as the
slice):

- the reference data in 44 s;
- the 80.8 million orders, copied into `oms` day by day, in 49 min;
- the exports, built by 6 workers and uploaded with one PUT per file, in 47 min.

The dry run had projected 23.4 GB for `oms`, from the 290 bytes per order (indexes included)
measured on the slice. At full scale `orders` takes 18,140 MB with its indexes, about 225 bytes
per order, so `oms` is 18.89 GB. The total is still above 100 GB.

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

`oms.orders` took 290 bytes per row including its indexes on the slice, and 225 at full scale.

Parameters kept for the full run:

| Parameter | Value |
|---|---|
| Ping interval | 25 s |
| Couriers | 20,000 |
| Customers | 5,000,000 |
| Orders | 400,000 a day, ×2 and ×3 in the peak weeks |
| Workers | 6 |

The dry run projected about 114 GB: 88.2 GB of pings, 2.0 GB of scans and 23.4 GB in `oms`. The
measured total is in the volume table above.

## Skewed columns

| Column | Skew | Measured (full scale) |
|---|---|---|
| `orders.hub_id` | Downtown LA has 25% of orders; the top 10 of 40 hubs have 60% (a fair share would be 2.5% and 25%) | 25.01%; 60.02% |
| `courier_pings.hub_id` | couriers are spread by the same weights, so Downtown LA sends about 25% of pings | 24.52% |
| `courier_pings.trip_id` | 30% of pings are idle and NULL. In a join on `trip_id` they all go to one partition | 29.55% NULL |
| `orders.area` | Metropolitan 55%, Urban 35%, Semi-Urban 7%, Other 3% | 54.99%, 35.03%, 6.99%, 2.99% |
| `orders.traffic` | Jam 35%, Low 25%, Medium 25%, High 15% | as generated |
| `orders.weather` | Sunny 62%, Cloudy 14%, Fog 12%, Windy 7%, Stormy 5% | as generated |
| `orders.order_date` | 2× in the mid-July sale week, 3× in Black Friday week | 1,200,000 orders a day in Black Friday week against 400,000 |

The label is skewed too, by design: 17.43% of orders are late. By traffic it is Low 7.0%,
Medium 14.9%, High 23.0% and Jam 24.3%.

## High-cardinality columns

| Column | Unique / total (full scale) | Note |
|---|---:|---|
| `orders.order_id` | 1.0000 | primary key |
| `parcel_scans.scan_id` | 1.0000 | 242,400,000 distinct |
| `courier_pings.(courier_id, device_seq)` | 0.9804 | the natural key of a ping. It is below 1 only because of the 2% re-uploads (1 / 1.02 = 0.9804) |
| `orders.customer_id` | 0.0615 | about 4.97 million of the 5 million customers ordered |
| `orders.(drop_lat, drop_lon)` | 0.0615 | one drop point per customer |
| `courier_pings.trip_id` (non-null) | 0.0141 | one trip id per order, repeated on every ping of the trip (counted per file) |
| `orders.courier_id` | 0.0002 | 20,000 couriers |

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

The v2 share of pings by local day (full scale):

| 08-31 | 09-01 | 09-02 | 09-03 | 09-04 | 09-05 | 09-06 | 09-07 | 09-08 | 09-09 | 09-10 | 09-11 | 09-12 | 09-13 | 09-14 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0% | 7.1% | 14.3% | 21.4% | 28.5% | 35.6% | 42.7% | 49.9% | 56.9% | 64.2% | 71.3% | 78.5% | 85.7% | 92.9% | 100% |

## Duplicates

| Dataset | Rate | How it shows |
|---|---|---|
| `courier_pings` | 2.00% extra rows (95,819,124 on 4,792,377,600 originals: 1.999%), which is 1.96% of all rows | whole 5-minute upload batches sent twice: same `(courier_id, device_seq)` and content, `uploaded_ts` 10 to 60 minutes later, same `export_date` |
| `orders`, `parcel_scans` | none offline | the stream duplicates scans (1.5%) later |

## Dirty values in `oms.orders`

The rates come from the seed file, so they match it:

| Dirty value | Seed | Full scale |
|---|---:|---:|
| Null block: `"NaN "` traffic, `"NaN"` weather and `"NaN "` order_time in the same row | 0.208% | 0.208% |
| Store point (0, 0) | 8.013% | 8.009% |
| Store latitude sign-flipped | 0.430% | 0.430% |
| Pickup rollover after midnight (`pickup_time < order_time`) | 1.89% | 1.835% |
| Legacy spellings: trailing spaces, `"Metropolitian "` | every row | every row |

In `oms.courier_changes`, 32.8% of the rows after a courier's hiring change nothing: they are
rating reviews that kept the rating. The SCD2 dimension has to skip them.
