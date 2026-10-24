# Airflow: orchestration of DP1 to DP3

Airflow runs the three pipelines' Spark jobs (`jobs/`) in order, on a schedule, and records every
run. This page covers the image, the services, where connections and variables come from, and the
DAGs.

## Services (profile `orch`)

| Service | Role | Port on the Mac | Memory |
|---|---|---|---|
| `airflow-init` | one-shot: migrates the `airflow` database (in the shared Postgres), creates the admin user, then the connections, variables and pool | | |
| `airflow-webserver` | the UI, with 1 gunicorn worker | `127.0.0.1:8082` (user `admin`, password `AIRFLOW_ADMIN_PASSWORD` in `.env`) | 784 MiB |
| `airflow-scheduler` | runs the tasks (LocalExecutor). A Spark task's driver runs here, in client mode | | 475 MiB at idle |

`orch` runs on top of `core` and `batch`:

```bash
docker compose --profile core --profile batch --profile orch up -d --wait
```

## The image (`infra/airflow/Dockerfile.naive`)

The image is built on `apache/airflow:2.10.5-python3.10` and is 3.23 GB. This first version is
deliberately naive: everything is copied and installed in one image and nothing is removed. A
multi-stage rewrite comes later.

| Part | Why |
|---|---|
| Java 17 and `/opt/spark`, copied from `lastleg/spark:3.5.9-naive` | a Spark client identical to the cluster, with every lakehouse jar, so the driver Airflow starts matches the executors |
| `/opt/driver-python`, a virtual environment with Great Expectations 0.18.19, Sedona 1.9.0, h3 4.5.0 and delta-spark 3.3.3 | the drivers' Python. GE and Sedona pin versions that clash with Airflow's. PySpark itself comes from `SPARK_HOME` at run time, so it always matches the cluster |
| `apache-airflow-providers-apache-spark` 5.0.0, without its dependencies | `SparkSubmitOperator` only calls the `spark-submit` binary, so it does not need its 340 MB PySpark dependency |
| `acryl-datahub-airflow-plugin` 1.5.0.17 | lineage and assertions to DataHub ([datahub.md](datahub.md)). It is on by default; set `DATAHUB_ENABLED=false` to run Airflow without the `gov` profile |

**Users and hosts.** The containers run as user `185:0`, the Spark user. That way the drivers can
write event logs to the `spark_events` volume, which the history server reads. The Airflow image
adds the user to `/etc/passwd` at start. The scheduler has the fixed hostname `airflow-scheduler`,
so the executors on `spark-worker` can connect back to the driver.

## Connections, variables and pool

**Who sees the credentials.** Only `airflow-init` reads `.env` (`env_file`). `airflow/setup_airflow.py`
stores the credentials as Airflow connections, encrypted with `AIRFLOW_FERNET_KEY`, in the `airflow`
database. The webserver, the scheduler and the DAG code see no password. A DAG reads one through a
connection id, for example `{{ conn.oms_postgres.password }}` in a templated field. Running the init
again updates the same entries.

| Connection | Type | Points to |
|---|---|---|
| `oms_postgres` | postgres | `postgres:5432/oms`, the order system (a source) |
| `dwh_postgres` | postgres | `postgres:5432/dwh`, the warehouse |
| `lakehouse_s3` | aws | SeaweedFS S3, `http://seaweedfs:8333` (`app-exports`, `lakehouse`) |
| `spark_lakehouse` | spark | `spark://spark-master:7077`, client mode |

| Variable | Value |
|---|---|
| `jobs_dir` | `/opt/jobs` |
| `lakehouse_bucket`, `exports_bucket` | `lakehouse`, `app-exports` |
| `period_start`, `period_end` | `2024-06-01`, `2024-11-30` |

**Pool.** `spark` has 1 slot, so only one Spark application runs at a time. The cluster has one
4-core worker, and two drivers would starve each other.

### Checks (2026-10-23)

```text
$ docker compose exec airflow-scheduler airflow connections list -o json   # printed without secrets
connection oms_postgres postgres postgres oms login set password set (hidden)
connection dwh_postgres postgres postgres dwh login set password set (hidden)
connection lakehouse_s3 aws None  login set password set (hidden)
connection spark_lakehouse spark spark://spark-master
$ docker compose exec airflow-scheduler airflow variables list
jobs_dir  lakehouse_bucket  exports_bucket  period_start  period_end
$ docker compose exec airflow-scheduler airflow pools list
default_pool  128    spark  1  one Spark application at a time (one 4-core worker)
```

The installed versions were checked inside the image:

- Airflow 2.10.5, with the Spark provider and the DataHub plugin importable.
- `spark-submit` 3.5.9 on Java 17.0.19.
- The driver Python imports Great Expectations 0.18.19, Sedona, h3 4.5.0, delta and PySpark 3.5.9
  (from `SPARK_HOME`).

Screenshots to add from the UI (Admin → Connections, Admin → Variables, Admin → Pools) are listed
in the project checklist. They show the same entries; the UI masks the passwords.

## DAGs

The DAGs build their Spark tasks with one factory, `airflow/dags/lakehouse_tasks.py`:

- **How it submits:** `SparkSubmitOperator` on connection `spark_lakehouse`, in pool `spark`, with
  the job path from variable `jobs_dir`.
- **Credentials:** they come from connections in templated `env_vars`
  (`{{ conn.oms_postgres.password }}`, `{{ conn.lakehouse_s3.password }}`).
- **Spark settings:** `spark.driver.host` is set to `airflow-scheduler`.

The DAGs run from 2024-06-01 to 2024-12-01. The extra last day loads the parcels ordered on
2024-11-30 and delivered after midnight: their order update and their scans belong to 2024-12-01.

### DP1: `dp1_ingest_validate`

```text
ingest_bronze (jobs/dp1_ingest.py --until {{ ds }})  >>  validate_bronze (jobs/validate.py --suite dp1 --ds {{ ds }} --period-end {{ var.value.period_end }} --fail-on-error)  >>  trigger_dp2
validate_bronze  >>  publish_assertions (the suite's results to DataHub, trigger rule all_done)
```

`validate_bronze` fails the run when any expectation fails, so nothing after it runs on bad
bronze. For a day after `period_end`, the suite skips the slices that are empty then: pings, the
hourly activity, and the orders placed that day.

**Check on the dev slice (2026-10-23).** Bronze's rows from local 2024-11-29 on were deleted with a
Delta `DELETE`, so the DAG had real work to do. (The deleted rows stay in the table's history, and
the sources still hold them.) Then three runs were triggered:

| Run | `ingest_bronze` | `validate_bronze` | Result |
|---|---|---|---|
| 2024-11-29 | +12,012 orders, +9 couriers and courier changes, +43 customers, +1 ping and +1 scan export date (29 s) | 73 of 73 (33 s) | success |
| 2024-11-30 | the next day (31 s) | 73 of 73 (34 s) | success |
| 2024-12-01 | the 2,519 orders and 2,838 scans changed after midnight | first run: failed. 72 of 73, because there are no pings after the period. After the period-end rule: 45 of 45 | success after the fix |

After the runs, bronze equals the sources again: 808,000 orders, 49,350,264 pings, 2,424,000
scans, 200 couriers, 1,093 courier changes and 50,000 customers. The 5.7 MB of task logs contain
neither the Postgres password nor the S3 secret key.

### DP2: `dp2_transform_validate`

DP2 has no schedule. `dp1_ingest_validate` starts it for the same logical date
(`TriggerDagRunOperator`), and only after `validate_bronze` passed.

```text
ingest stage:   silver >> gold_dims >> gold_facts >> warehouse
validate stage: >> [validate_suite (jobs/validate.py --suite dp2 --ds {{ ds }} ...), validate_scd2 (jobs/check_dim_courier.py)]
                >> trigger_dp3
validate_suite >> publish_assertions (the suite's results to DataHub, trigger rule all_done)
```

**Check on the dev slice (2026-10-23).** A run of `dp1_ingest_validate` for 2024-11-30 passed its
three tasks and triggered `dp2_transform_validate` for the same day. All six DP2 tasks succeeded
in 3.5 minutes:

| Task | Time | Output |
|---|---:|---|
| `silver` | 38 s | `stg_orders` 808,000, `stg_courier_changes` 1,093, `stg_customers` 50,000; no new ping or scan export date |
| `gold_dims` | 32 s | `dim_zone` 1,497, `dim_customer` 50,000; `dim_courier`: no new version after change 1093 |
| `gold_facts` | 36 s | `fact_delivery` 808,000 (merged); no new activity hour |
| `warehouse` | 38 s | `dwh` reloaded: `fact_delivery` 408,000, `obt_delivery` 168,000, the dimensions |
| `validate_suite` | 46 s | 61 of 61 expectations passed |
| `validate_scd2` | 23 s | 8 invariants, 0 violations: 799 versions from 1,093 audit rows |

The first try of the DP1 run failed at `trigger_dp2` ("Dag id dp2_transform_validate not found").
The scheduler looks for new DAG files every 300 s by default, and the run started seconds after the
file was added. `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL` is now 30 s. After clearing the task,
the run passed.

### DP3: `dp3_features_validate`

DP3 has no schedule either. `dp2_transform_validate` starts it for the same day once both of its
checks passed.

```text
ingest stage:   features (jobs/dp3_features.py --incremental: only what the facts' Change Data Feed changed)
validate stage: >> [validate_suite (jobs/validate.py --suite dp3 --ds {{ ds }} ...), validate_incremental (jobs/check_cdf.py)]
validate_suite >> publish_assertions (the suite's results to DataHub, trigger rule all_done)
```

`validate_incremental` compares every incrementally maintained feature table with a full
recompute, so each run also checks novel idea 2 ([novel_cdf_incremental.md](novel_cdf_incremental.md)).

### The whole chain

```text
dp1_ingest_validate ──trigger_dp2──► dp2_transform_validate ──trigger_dp3──► dp3_features_validate
  (bronze must pass)                   (silver and gold must pass)
```

**Check on the dev slice (2026-10-23).** One trigger of `dp1_ingest_validate` for 2024-11-28 ran
the whole chain green. DP1 finished after 66 s, DP2 (triggered by DP1) after 5.7 min, and DP3
(triggered by DP2) after 8.1 min. The run for 2024-12-01, the day after the period, is green too:
the DP3 suite passed 19 of 19, skipping only the hub-hour slice, which holds just that day's
closing hour.

### Failure drill: a corrupted batch blocks everything downstream

The drill was run on 2026-10-23 against the order system.

1. **Back up `oms`.** `pg_dump` (35 MB).
2. **Insert a corrupted batch.** 100 orders copied from real 2024-12-01 orders, with new ids
   (9000001 to 9000100), a traffic code the system never used (`"Gridlock "`), status `lost`, and
   an `updated_ts` after bronze's watermark.
3. **Rerun DP1 for 2024-12-01** (clearing its run).
   - `ingest_bronze` succeeded: `bronze.raw_orders: +100 rows`.
   - `validate_bronze` **failed**: 43 of 45. `traffic` and `status` were out of their value sets
     for 100 of 2,619 rows each (3.82%).
   - `trigger_dp2` was `upstream_failed`.
4. **Check downstream.** No new DP2 run started (still 1) and no DP3 run (still 0). Silver,
   gold, the warehouse and the features never saw the batch.
5. **Repair.** The rows were deleted from `oms` and from bronze (Delta keeps the bad version in its
   history). After clearing the DP1 run:
   - DP1 passed and triggered DP2;
   - DP2 passed and triggered DP3;
   - DP3 passed: `features` read 0 changed fact rows, `validate_incremental` found 0 differences,
     and the suite passed 19 of 19.

The failed `validate_bronze` still wrote its results
(`s3a://lakehouse/_validation/dp1/ds=2024-12-01/results.json`) before the run stopped, and
`publish_assertions` sends them to DataHub even when the suite fails
([datahub.md](datahub.md#data-validation-great-expectations-results-as-assertions)).

## Pitfalls

- **A run after `end_date` checks nothing.** A run whose logical date is later than the DAG's
  `end_date` gets no task instances and is marked success. The first drill run (2024-12-01
  20:00 UTC, after the 00:00 `end_date`) did exactly that, so it was deleted and the drill reused
  the 2024-12-01 run.
- **The Change Data Feed of an unchanged fact.** Delta refuses a starting version past the table's
  latest one. DP3 failed once when only `fact_delivery` had a new version. It now reads an empty
  frame for a fact without one.
- **Never print connections as URIs.** `airflow connections list` prints the password in plain
  text unless the output is filtered, as above.
- **New mounted folders after a merge.** Before a feature branch is merged, `git checkout develop`
  deletes a folder that only the feature branch has (here `airflow/`), and the merge creates it
  again as a new folder. Containers that mounted it then see an empty folder until they are
  recreated (`up -d --force-recreate`).
- **A new module in the DAG folder needs a scheduler restart.** The DAG files import
  `datahub_assertions.py`, which was added while the scheduler ran. Its DAG processor did not see
  the new file: every DAG file failed with `ModuleNotFoundError: No module named
  'datahub_assertions'`, the three DAGs were deactivated, and a triggered run stayed `queued`.
  `airflow tasks test` worked because it starts a fresh process. `docker compose restart
  airflow-scheduler airflow-webserver` fixed it.
- **`--profile orch` alone is not enough,** like `batch`: Airflow needs Postgres from `core` and
  the Spark cluster from `batch`.
