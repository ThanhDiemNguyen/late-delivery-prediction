# Pipelines: DP1, DP2 and DP3

The platform's three data pipelines are Spark jobs in `jobs/`. Airflow runs them later. Each job
starts through `jobs/common.py`, which stops with a clear message if the Spark settings
(`infra/spark/conf/spark-defaults.conf`) were not loaded.

| Pipeline | From | To | Jobs |
|---|---|---|---|
| DP1 | the source systems (`oms`, `app-exports`) | bronze `raw_*` | `dp1_ingest.py` |
| DP2 | bronze | silver `stg_*`, gold `dim_*` / `fact_*`, warehouse `dwh` | later |
| DP3 | gold | feature tables `feat_*` | later |

## DP1: ingest into bronze (`jobs/dp1_ingest.py`)

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/dp1_ingest.py                      # everything new
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/dp1_ingest.py --until 2024-07-31   # up to a local date
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/dp1_ingest.py --part exports       # oms | exports
```

Bronze is a faithful copy. It keeps every source problem: legacy spellings, `NaN` literals, broken
store coordinates, re-uploaded pings and both ping schemas. Each row gets two extra columns:
`_ingested_at`, and `_source` (the `oms` table, or the export file's path).

The tables are written by path in `s3a://lakehouse/bronze/raw_<name>` and registered once with
`CREATE TABLE IF NOT EXISTS … USING DELTA LOCATION`. Schema changes then live in the Delta log,
and the job never asks the Hive Metastore to alter a table (Hive Metastore 3.1 rejects that for
Delta tables).

### From `oms`: incremental by change time

| Source table | Bronze table | Change column |
|---|---|---|
| `hubs` | `raw_hubs` | `updated_ts` |
| `couriers` | `raw_couriers` | `updated_ts` |
| `courier_changes` | `raw_courier_changes` | `changed_ts` (append-only) |
| `customers` | `raw_customers` | `updated_ts` |
| `orders` | `raw_orders` | `updated_ts` |

For each table, the watermark is the newest change time already in bronze. The job reads only rows
changed after it (and before the end of `--until`, a local LA date) with a JDBC query, and appends
them. Bronze therefore keeps every version it pulled, and silver keeps the latest version of each
key. Windows of 1 million rows or more are read in 8 parallel JDBC partitions, split by the key.

### From `app-exports`: incremental by export date

| Export | Bronze table | Partitioned by |
|---|---|---|
| `courier_pings/export_date=*/` | `raw_courier_pings` | `export_date` |
| `parcel_scans/export_date=*/` | `raw_parcel_scans` | `export_date` |

**Which folders it reads.** The job lists the `export_date` folders that are complete (they hold
`_SUCCESS`). It reads only those after the newest `export_date` already in bronze, and appends
them.

**Schema evolution.** It reads with `mergeSchema` and appends with Delta's `mergeSchema`.

- **What it does:** from 2024-09-01 a folder holds v1 and v2 files, and the read takes the union of
  both schemas. The append adds the v2 columns to the table.
- **What it leaves to silver:** v1 rows have NULL `speed_mps`, `battery_pct` and `gps_accuracy_m`;
  v2 rows have NULL `speed_kmh`. Silver unifies them.
- **Without `mergeSchema`:** `--no-merge-schema` reads without it. Spark then takes one file's
  schema and drops the other version's columns. That demo comes with the schema evolution write-up.

### Checks on the dev slice (2026-10-23)

Three runs show the incremental load: up to 2024-07-31, then the rest, then once more.

| Bronze table | Run 1 (`--until 2024-07-31`) | Run 2 | Total | Source |
|---|---:|---:|---:|---:|
| `raw_hubs` | 40 | 0 | 40 | 40 |
| `raw_couriers` | 6 | 194 | 200 | 200 |
| `raw_courier_changes` | 441 | 652 | 1,093 | 1,093 |
| `raw_customers` | 44,975 | 5,025 | 50,000 | 50,000 |
| `raw_orders` | 271,156 | 536,844 | 808,000 | 808,000 |
| `raw_courier_pings` | 61 dates, 15,127,140 | 122 dates | 49,350,264 | 49,350,264 (with the 2% re-uploads) |
| `raw_parcel_scans` | 61 dates, 815,033 | 123 dates | 2,424,000 | 2,424,000 |

- **Bronze matches the sources.** Every total equals its source count from `check_data`.
- **Run times:** run 1 took 42 s and run 2 took 77 s.
- **Rerun:** run 3 found nothing new in any table (28 s).
- **Why only 6 couriers in run 1:** a courier row's `updated_ts` is its last change, and most
  couriers had a rating review after July. Run 2 brought the other 194.

Trino lists all 7 tables in `delta.bronze`. The merged ping schema shows both app versions:

```text
 app_version |   rows   | speed_kmh | speed_mps | battery_pct
-------------+----------+-----------+-----------+-------------
 3.14.2      | 25262268 |  25262268 |         0 |           0
 4.0.0       | 24087996 |         0 |  24087996 |    24087996
```

### Pitfalls

- **Watermark time zone.** The watermark is formatted by Spark in the session time zone (UTC),
  not by Python, whose time zone depends on the container.
- **Equal change times.** The pull uses `>` the watermark. A row written later with exactly the
  watermark's timestamp would be missed. The generator never does that, and a real OMS would need
  a change sequence number.
- **Missing settings.** If the Spark settings are missing, S3A sends requests to Amazon's public
  endpoint, which answers `InvalidAccessKeyId`. `jobs/common.py` stops the job before that
  ([lakehouse.md](lakehouse.md#decisions-and-pitfalls)).
