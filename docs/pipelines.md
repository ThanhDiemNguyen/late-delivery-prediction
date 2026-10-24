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

## DP1: validate bronze (`jobs/validate.py --suite dp1`)

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/validate.py --suite dp1 --ds 2024-09-07 --fail-on-error
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/validate.py --suite dp1 --ds 2024-09-07 --fail-on-error --corrupt
```

**How it runs.** Great Expectations 0.18.19, the course's version, runs on Spark DataFrames through
its legacy `SparkDFDataset` API, so every expectation is a Spark query.

**What it checks.** Each run checks only the slice one DP1 run loads, so the cost does not grow
with the table:

- the export date `ds` of the pings and scans;
- the `oms` rows changed on local day `ds` (orders by `updated_ts`, courier changes by
  `changed_ts`);
- the small reference tables in full.

**What it checks for.** Bronze keeps the source problems on purpose, so the suite checks that the
data is what the sources promise. It does not check that the data is clean.

| Table | Expectations |
|---|---|
| `raw_hubs` | 40 rows, unique `hub_id`, coordinates inside the LA box |
| `raw_couriers` | unique `courier_id`, no NULL attributes, vehicle in {van, car, e-bike}, rating 1 to 5 |
| `raw_courier_changes` | unique `change_id`, no NULL snapshot attributes |
| `raw_customers` | unique `customer_id`, no NULL home point, home latitude inside the LA box |
| `raw_orders` | rows exist, unique `order_id`, keys and timestamps not NULL, `traffic` and `area` only in the legacy spellings, `status` delivered |
| `raw_courier_pings` | rows exist, all 16 columns of the merged v1 + v2 schema exist, keys not NULL, `app_version` 3.14.2 or 4.0.0, positions within a degree of the LA box |
| `raw_parcel_scans` | rows exist, unique `scan_id`, keys not NULL, `scan_type` at_hub, picked_up or delivered |

**Results.** The results go to `s3a://lakehouse/_validation/dp1/ds=<ds>/results.json`: one JSON
document with every expectation, its outcome and the observed counts.

- **Exit code:** with `--fail-on-error` the job exits 1 when any expectation fails.
- **In Airflow:** without the flag it exits 0, so the next task can publish the results to DataHub
  before stopping the pipeline.
- **`--corrupt`:** validates a corrupted copy of the same slice. It changes nothing in the
  lakehouse.

### Checks on the dev slice (2024-09-07, a v2 rollout day)

| Batch | Result | Exit code |
|---|---|---|
| clean | 73 of 73 expectations passed (28 s) | 0 |
| corrupted | 68 of 73 passed (30 s) | 1 |

The five failures are exactly the five injected problems:

| Injected problem | Failed expectation | Observed |
|---|---|---|
| 25 orders duplicated | `order_id` unique | 50 of 3,927 rows not unique |
| 1% of orders with traffic `"Gridlock"` | `traffic` in the legacy set | 44 of 3,927 (1.12%) |
| 1% of pings with NULL `courier_id` | `courier_id` not NULL | 2,791 of 277,608 (1.01%) |
| 0.1% of pings at latitude 0 | `lat` between 32.7 and 35.8 | 248 of 277,608 (0.09%) |
| 1% of scans with type `"lost"` | `scan_type` in set | 121 of 11,855 (1.02%) |

### DP1 pitfalls

- **Watermark time zone.** The watermark is formatted by Spark in the session time zone (UTC),
  not by Python, whose time zone depends on the container.
- **Equal change times.** The pull uses `>` the watermark. A row written later with exactly the
  watermark's timestamp would be missed. The generator never does that, and a real OMS would need
  a change sequence number.
- **Missing settings.** If the Spark settings are missing, S3A sends requests to Amazon's public
  endpoint, which answers `InvalidAccessKeyId`. `jobs/common.py` stops the job before that
  ([lakehouse.md](lakehouse.md#decisions-and-pitfalls)).
