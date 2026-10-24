# Pipelines: DP1, DP2 and DP3

The platform's three data pipelines are Spark jobs in `jobs/`. Airflow runs them later. Each job
starts through `jobs/common.py`, which stops with a clear message if the Spark settings
(`infra/spark/conf/spark-defaults.conf`) were not loaded.

| Pipeline | From | To | Jobs |
|---|---|---|---|
| DP1 | the source systems (`oms`, `app-exports`) | bronze `raw_*` | `dp1_ingest.py` |
| DP2 | bronze | silver `stg_*`, gold `dim_*` / `fact_*`, warehouse `dwh` | `dp2_silver.py`, then later jobs |
| DP3 | gold | feature tables `feat_*` | `dp3_features.py` |

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

## DP2: clean bronze into silver (`jobs/dp2_silver.py`)

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/dp2_silver.py             # what is new in bronze
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/dp2_silver.py --rebuild   # everything again
```

Silver has the same grain as bronze, with one row per real thing: one row per order, ping, scan,
customer and hub.

- **`oms` tables** are merged into silver by key (Delta `MERGE`). Only the bronze rows ingested
  since the last run take part, and the newest version of each key wins.
- **Export tables** process only the export dates silver has not seen. They are written with
  dynamic partition overwrite, so a rerun of a date replaces that date's partition and nothing
  else.

| Silver table | From | Partitioned by |
|---|---|---|
| `stg_hubs`, `stg_customers`, `stg_courier_changes` | the `raw_` table: the latest version of each key | |
| `stg_orders` | `raw_orders`, cleaned as below | |
| `stg_courier_pings` | `raw_courier_pings`, deduplicated and unified | `ping_date` (local) |
| `stg_parcel_scans` | `raw_parcel_scans`, deduplicated by `scan_id` | `scan_date` (local) |

### What silver repairs in orders

| Problem in bronze | Repair |
|---|---|
| trailing spaces (`"Jam "`, `"car "`, `"Urban "`) | `trim` |
| `"Metropolitian"` | corrected to `Metropolitan` |
| `"NaN "` / `"NaN"` in traffic and weather | NULL |
| store point (0, 0) or sign-flipped | replaced by the hub's point; `store_point_repaired` marks the row |
| local `order_date` + `order_time` text | `order_ts` in UTC, read in America/Los_Angeles |
| `order_time` is `"NaN "` (the null block) | rebuilt from the promise rule: `order_ts = promised_ts − (area base + 3 min × km)`; `order_ts_source = 'promise_rule'` |
| the repeated hour 01:00 to 01:59 when daylight saving time ends (2024-11-03) | Spark reads such a time as the first (PDT) occurrence; when the promise rule puts the order exactly one hour later, it was the second (PST) occurrence. `order_ts_source = 'standard_time'` |
| `pickup_time` earlier than `order_time` (picked up after midnight) | `pickup_ts = order_ts + (pickup clock − order clock) mod 24 h`; `pickup_rolled_over` marks the row. A difference over 12 h means the clock went back an hour that night, so 23 h comes off |

Silver also adds `distance_km` (hub to drop point) and `promised_min` (the SLA in minutes).

### What silver repairs in pings

| Problem in bronze | Repair |
|---|---|
| re-uploaded batches (same `courier_id`, `device_seq`) | keep the earliest upload of each key (`row_number` over the key, ordered by `uploaded_ts`) |
| app v1 `speed_kmh` against v2 `speed_mps` | one `speed_kmh` column: `speed_kmh`, or `speed_mps × 3.6`; `schema_version` 1 or 2. v2's `battery_pct` and `gps_accuracy_m` stay, NULL for v1 |

### Checks on the dev slice (2026-10-23)

The first run took 203 s and a rerun 37 s, with nothing new to process. The counts come from
Trino and `spark-sql`:

| Check | Bronze | Silver |
|---|---:|---:|
| orders with a trailing space in traffic | 808,000 | 0 |
| `NaN` traffic / weather literals | 1,639 / 1,639 | 0 / 0 (1,639 NULL each) |
| `Metropolitian`; vehicle or area with a trailing space | every Metropolitan row | 0 |
| store points outside the LA box | 68,210 | 0 (68,210 repaired) |
| `order_ts` missing | 1,639 (`NaN` text) | 0 (1,639 from the promise rule) |
| pickups after midnight | 14,786 | 0 wrong; all rolled over |
| `pickup_ts` equal to the `picked_up` scan | | 808,000 of 808,000 |
| `order_ts` before the `at_hub` scan | | 808,000 of 808,000 |
| pings | 49,350,264 | 48,382,560: **967,704 removed, exactly the injected re-uploads** |
| duplicate ping keys; NULL `speed_kmh` | 967,704; 23,616,000 (v2 rows) | 0; 0 |
| mean speed, v1 / v2 rows | | 18.60 / 18.49 km/h |
| scans | 2,424,000 | 2,424,000 (no offline duplicates) |
| late share | | 17.56%, as in `oms` |

The `picked_up` scans come from a separate source system (the courier-app export). Every rebuilt
pickup time matching them shows that the rollover repair is right.

## DP2: gold dimensions (`jobs/dp2_gold_dims.py`)

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/dp2_gold_dims.py                          # MERGE what is new
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/dp2_gold_dims.py --rebuild --until 2024-08-31
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/check_dim_courier.py                     # SCD2 invariants
```

| Gold table | Grain | Built from |
|---|---|---|
| `dim_courier` | one version of a courier (SCD type 2) | `silver.stg_courier_changes` |
| `dim_hub` | one hub | `silver.stg_hubs` (type 1: current values) |
| `dim_customer` | one customer | `silver.stg_customers` (type 1) |
| `dim_date` | one local date of the period (184 days to 2024-12-01) | generated: weekday, ISO week, the two peak weeks, US holidays |
| `dim_zone` | one H3 cell (resolution 7) with a customer's home | `silver.stg_customers`, cells from Apache Sedona ([novel_h3_sedona.md](novel_h3_sedona.md)) |

### `dim_courier`, SCD type 2

| Column | Meaning |
|---|---|
| `courier_sk` | surrogate key: the `change_id` of the audit row that opened the version, so it never changes |
| `courier_id` | the natural key |
| `hub_id`, `vehicle_type`, `rating`, `employment_type` | the tracked attributes, as they were during the version |
| `valid_from_ts` | when the version started (inclusive) |
| `valid_to_ts` | when the next version started (exclusive); `9999-12-31` while open |
| `is_current` | true for the open version |

How a version is made:

1. **Hash the attributes.** Each audit row gets a SHA-256 hash of its four tracked attributes.
2. **Compare with the previous row.** A row opens a version only when its hash differs from the
   previous row's hash (`lag`). A rating review that left the rating unchanged opens nothing.
3. **Close each version.** `lead` sets each version's end to the start of the next.

Courier 146 has 11 audit rows and 8 versions. Its reviews 376, 576 and 829 changed nothing.

**Incremental load.** New audit rows are merged with one Delta `MERGE`:

- **What is new:** changes after the newest version's opening `change_id`.
- **The comparison:** each courier's open version is put in front of its new changes, so the first
  new change is compared with it.
- **Two kinds of row in the `MERGE` source:**
  - rows with a merge key close the open version: `valid_to_ts` is set to the first new start,
    and `is_current` to false;
  - rows without one are inserted as the new versions.
- **Reruns:** no-op reviews after that `change_id` are compared again on the next run and open
  nothing, so a rerun changes nothing.

### Checks on the dev slice (2026-10-23)

These were run against the dimension built in two ways: up to 2024-08-31 and then merged
incrementally (481 versions, then 799), and as a full rebuild (799).

| Check | Result |
|---|---|
| incremental build against full rebuild (`exceptAll` both ways) | 0 rows differ |
| each courier has exactly one current version | 0 violations |
| `valid_from_ts < valid_to_ts` | 0 violations |
| only current versions are open-ended | 0 violations |
| consecutive versions touch (no gap, no overlap) | 0 violations |
| consecutive versions differ in some attribute | 0 violations |
| `courier_sk` unique | 0 violations |
| every courier in silver is in the dimension | 0 violations |
| the current version equals the courier's last audit row | 0 violations |
| audit rows that opened no version | 294 of 1,093: exactly the no-op reviews the generator wrote |

`dim_hub` has 40 rows, `dim_customer` 50,000 and `dim_date` 184.

## DP2: gold facts (`jobs/dp2_gold_facts.py`)

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/dp2_gold_facts.py
```

| Fact | Grain | Keys | Measures | Partitioned by |
|---|---|---|---|---|
| `fact_delivery` | one order | `order_id`; `courier_sk` → `dim_courier`, `hub_id` → `dim_hub`, `customer_id` → `dim_customer`, `date_key` → `dim_date`, `drop_zone_id` → `dim_zone` | `distance_km`, `delivery_min`, `promised_min`, `is_late`, plus traffic, weather, vehicle, area, category, `order_hour` | `order_month` |
| `fact_courier_activity_hourly` | one courier in one local hour | `courier_id`, `hub_id` → `dim_hub`, `date_key` → `dim_date`, `hour_start` | `ping_count`, `km_moved`, `idle_min`, `late_trip_min`, `avg_speed_kmh`, `trips` | `activity_date` |

**Courier version at pickup.** `fact_delivery` joins each order to the `dim_courier` version
that was valid when the parcel was picked up: `valid_from_ts <= pickup_ts < valid_to_ts`. So a
rating change after the delivery does not change the courier's rating on that delivery. The label
is `is_late = delivered_ts > promised_ts`.

**Activity per courier hour.** `fact_courier_activity_hourly` joins the silver pings to `dim_hub`
for the hub's area type and to `fact_delivery` on `trip_id` for `is_late`, then groups them by
courier and local hour. About 30% of the pings are idle, with a NULL `trip_id`. In a plain join
they would all land in one shuffle partition, so they skip the join and are added back with a
NULL `is_late`. The skew and this fix are measured at 100 GB in
[spark_optimization.md](spark_optimization.md).

| Measure | How it is made |
|---|---|
| `km_moved` | the sum of the distances between consecutive pings |
| `idle_min` | 25 s for each ping without a trip |
| `late_trip_min` | 25 s for each ping on a trip that ended late (`fact_delivery.is_late`) |
| `avg_speed_kmh` | the mean speed of pings on a trip |

**Rebuilds.** `fact_delivery` is merged by `order_id`. An existing order is rewritten only when one
of its values really differs, and `--rebuild` rewrites it from silver. The activity fact adds only
the ping dates it does not have yet, with dynamic partition overwrite. Both facts have Delta's
Change Data Feed on, so DP3 can read just the rows a run changed
([novel_cdf_incremental.md](novel_cdf_incremental.md)).

### Checks on the dev slice (2026-10-23)

The run took 112 s.

| Check | Result |
|---|---|
| `fact_delivery` rows | 808,000, one per silver order |
| foreign-key coverage of `courier_sk`, `hub_id`, `customer_id`, `date_key` | 100.000% each; no NULL `courier_sk` |
| pickups outside the joined courier version | 0 |
| courier versions used | 797 of 799, for 200 couriers |
| `fact_courier_activity_hourly` rows | 335,990 = 33,599 courier-days × 10 shift hours |
| foreign-key coverage of `courier_id`, `hub_id`, `date_key` | 100.000% each |
| pings summed in the activity fact | 48,382,560, equal to silver |
| per courier hour, on average | 18.73 km moved, 17.77 idle minutes, 25.93 km/h on trips |
| late share in `fact_delivery` | 17.56%; mean delivery 120.9 min against 164.8 min promised |

## DP2: load the warehouse (`jobs/dp2_warehouse.py`)

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/dp2_warehouse.py
```

The warehouse is the Postgres database `dwh`: gold's star schema, copied for BI tools and SQL
users, with declared keys (`jobs/sql/dwh_schema.sql`).

| Table | Rows | Key | Foreign keys |
|---|---|---|---|
| `dim_date` | every day of the period | `date_key` | |
| `dim_hub` | every hub | `hub_id` | |
| `dim_zone` | every H3 drop zone | `zone_id` | `main_hub_id` → `dim_hub` |
| `dim_customer` | every customer | `customer_id` | `hub_id` → `dim_hub` |
| `dim_courier` | every SCD2 version | `courier_sk` | `hub_id` → `dim_hub` |
| `fact_delivery` | the last 90 days of the period | `order_id` | `courier_sk` → `dim_courier`, `hub_id` → `dim_hub`, `customer_id` → `dim_customer`, `date_key` → `dim_date`, `drop_zone_id` → `dim_zone` |
| `obt_delivery` | the last 30 days, one wide row per order | `order_id` | none: a one-big-table carries its dimensions' attributes on the row |

**How it loads.** Postgres refuses to `TRUNCATE` one table that another references, and a reader
should never see a half-loaded warehouse. So the job loads in three steps:

1. **Apply the DDL.** `CREATE TABLE IF NOT EXISTS`, with the keys.
2. **Write staging.** Each table's rows go to `staging.<table>` with Spark JDBC.
3. **Swap in one transaction.** It empties all six tables together, inserts dimensions before facts
   (so Postgres checks every foreign key on every row), and drops `staging`. A failure rolls the
   whole swap back.

### Checks on the dev slice (2026-10-23)

| Check | Result |
|---|---|
| backup before the first load | `pg_dump` of the empty `dwh` |
| rows | `dim_date` 184, `dim_hub` 40, `dim_customer` 50,000, `dim_courier` 799, `fact_delivery` 408,000 (2024-09-02 to 2024-11-30), `obt_delivery` 168,000 (2024-11-01 to 2024-11-30) |
| declared keys (`information_schema`) | 6 primary keys; 6 foreign keys: `fact_delivery` → `dim_courier`, `dim_customer`, `dim_date`, `dim_hub`; `dim_courier` → `dim_hub`; `dim_customer` → `dim_hub` |
| `staging` after the load | dropped; only `public` remains |
| time | 30 s; a reload gives the same counts (33 s) |

The fact row counts can be checked by hand:

- **Last 90 days:** 84 normal days × 4,000 orders plus the 6 Black Friday days × 12,000 = 408,000.
- **Last 30 days:** 24 × 4,000 + 6 × 12,000 = 168,000.

**DBeaver:** New connection, then PostgreSQL, with these settings:

| Setting | Value |
|---|---|
| Host | `127.0.0.1` |
| Port | `15432` |
| Database | `dwh` |
| User and password | from `.env` |

The ER diagram of `public` then shows the six foreign keys.

## DP2: validate silver and gold (`jobs/validate.py --suite dp2`)

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/validate.py --suite dp2 --ds 2024-11-29 --fail-on-error
```

DP1's suite accepts the source's dirty values. DP2's suite checks the opposite: silver and gold are
clean, consistent and fully joined. It reads day `ds` of each table:

| Dataset | Expectations |
|---|---|
| `silver.stg_orders` | rows exist; unique `order_id`; no NULL instants, vehicle, area, distance or store point; traffic, weather, vehicle and area only in the clean value sets (NULL traffic and weather are the seed's null block); store point inside the LA box; `pickup_ts > order_ts`; `delivered_ts > pickup_ts`; distance 0 to 40 km |
| `silver.stg_courier_pings` | rows exist; `(courier_id, device_seq)` unique, so no duplicate is left; no NULL `speed_kmh` or `schema_version`; `schema_version` 1 or 2; speed 0 to 200 km/h |
| `silver.stg_parcel_scans` | rows exist; unique `scan_id` |
| `gold.dim_courier` | unique `courier_sk`; no NULL attributes; `valid_to_ts > valid_from_ts` (the full SCD2 invariants are in `jobs/check_dim_courier.py`) |
| `gold.fact_delivery` | rows exist; unique `order_id`; no NULL key, label or measure; delivery 10 to 1,000 min; promised 120 to 300 min |
| `gold.fact_delivery` joined to its four dimensions | every key finds its dimension row (foreign-key coverage) |
| `gold.fact_delivery` labels | the day's late share is between 8% and 30% |
| `gold.fact_courier_activity_hourly` | rows exist; `(courier_id, hour_start)` unique; 1 to 144 pings, 0 to 60 idle minutes and 0 to 120 km per hour |

The two joined or derived datasets are built in the job (`DERIVED` in `jobs/validate.py`) and then
checked like tables. That is how a single-table expectation library checks foreign keys.

### Checks on the dev slice (2026-10-23)

| Day | Result | Exit code |
|---|---|---|
| 2024-09-07 (v1 and v2 pings) | 60 of 60 expectations passed (35 s) | 0 |
| 2024-11-29 (Black Friday, 3× orders) | 60 of 60 passed (43 s) | 0 |

The DP1 suite still passes 73 of 73 on 2024-09-07 after the change.

## DP3: offline feature tables (`jobs/dp3_features.py`)

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/dp3_features.py                 # full recompute
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/dp3_features.py --incremental   # from the facts' CDF
```

| Feature table | Entity and grain | Features | Built from |
|---|---|---|---|
| `gold.feat_courier_daily` | courier, local day (`feature_date`) | `on_time_rate_30d`: share of the courier's deliveries in the last 30 days that were on time; `deliveries_7d`; `avg_speed_kmh_7d`: speed on trips over the last 7 days, weighted by pings | `fact_delivery`, `fact_courier_activity_hourly` |
| `gold.feat_zone_daily` | H3 drop zone, local day | `deliveries_30d`, `late_rate_30d` ([novel_h3_sedona.md](novel_h3_sedona.md)) | `fact_delivery` (`drop_zone_id`), `dim_zone` |
| `gold.feat_hub_hourly` | hub, hour (`hour_start`) | `orders_1h`: orders placed in the hour; `late_rate_7d`: late share of the hub's deliveries in the last 168 hours; `active_couriers_1h` | `fact_delivery`, `fact_courier_activity_hourly` |

Every row has two time columns:

- **`event_timestamp`:** the end of the period the row describes. For a day, that is the next local
  midnight, in UTC: 07:00 UTC in summer and 08:00 UTC after daylight saving time ends. For an hour,
  it is the end of the hour.
- **`created`:** when the job computed the row.

**Point-in-time correctness.** A delivery counts in a feature only once it was delivered, before
the row's `event_timestamp`. The model's training join (in EDAI-2) takes, for each order, the
latest feature row with `event_timestamp <= pickup_ts`. That join therefore never sees the order
being scored or anything after it.

**The grid.** Each table is built on a full grid: every courier from its hiring day, and every
hub for every hour of the period. Days and hours with no activity are present, with 0 or NULL
values, and the rolling windows (`rangeBetween` over a day or hour number) never skip a gap.

### Checks on the dev slice (2026-10-23)

| Check | Result |
|---|---|
| rows | `feat_courier_daily` 33,836 (couriers × days since hiring); `feat_hub_hourly` 175,760 = 40 hubs × 4,394 hours (183 days × 24, plus the closing hour, plus the extra hour when daylight saving ends) |
| `DESCRIBE` | both tables have `event_timestamp timestamp` and `created timestamp` |
| 300 sampled `feat_courier_daily` rows, recomputed directly from `fact_delivery` | 0 mismatches; 0 deliveries at or after `event_timestamp` counted |
| 300 sampled `feat_hub_hourly` rows, recomputed from deliveries in the 168 hours before `event_timestamp` | 0 mismatches |
| example | courier 146 on 2024-11-29: on time 93.4% over 30 days, 300 deliveries in 7 days (Black Friday week), 25.1 km/h; `event_timestamp` 2024-11-30 08:00 UTC |

The job took 51 s.

## DP3: validate the feature tables (`jobs/validate.py --suite dp3`)

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/validate.py --suite dp3 --ds 2024-11-03 --fail-on-error
```

| Dataset (day `ds`) | Expectations |
|---|---|
| `gold.feat_courier_daily` | rows exist; `(courier_id, feature_date)` unique; no NULL key, `deliveries_7d`, `event_timestamp` or `created`; on-time rate 0 to 1; speed 0 to 80 km/h; `created >= event_timestamp` (a row is computed only after its day has ended) |
| `gold.feat_hub_hourly` | 960 to 1,000 rows (40 hubs × 24 hours, or 25 when daylight saving time ends); `(hub_id, hour_start)` unique; no NULL key, counts or time columns; late rate 0 to 1; `event_timestamp > hour_start`; `created >= event_timestamp` |

### Checks on the dev slice (2026-10-23)

| Day | Result | Exit code |
|---|---|---|
| 2024-09-07 | 23 of 23 expectations passed | 0 |
| 2024-11-03 (daylight saving time ends: 25 local hours) | 23 of 23 passed | 0 |

### DP2 pitfalls

- **The repeated hour is not tested by data.** No order falls in the repeated hour of 2024-11-03,
  because the seed has no orders between 01:00 and 07:59 local time. The standard-time rule is in
  place, but no data row exercises it.
- **Exact distinct counts belong in Spark.** Trino's 2 GB limit gives about 500 MB per query, which
  is too little for an exact distinct count over 48 million ping keys.

### DP1 pitfalls

- **Watermark time zone.** The watermark is formatted by Spark in the session time zone (UTC),
  not by Python, whose time zone depends on the container.
- **Equal change times.** The pull uses `>` the watermark. A row written later with exactly the
  watermark's timestamp would be missed. The generator never does that, and a real OMS would need
  a change sequence number.
- **Missing settings.** If the Spark settings are missing, S3A sends requests to Amazon's public
  endpoint, which answers `InvalidAccessKeyId`. `jobs/common.py` stops the job before that
  ([lakehouse.md](lakehouse.md#decisions-and-pitfalls)).
