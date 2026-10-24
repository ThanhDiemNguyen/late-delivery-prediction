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
| `acryl-datahub-airflow-plugin` 1.5.0.17 | lineage and assertions to DataHub. It is off (`AIRFLOW__DATAHUB__ENABLED=false`) until the governance profile exists |

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
ingest_bronze (jobs/dp1_ingest.py --until {{ ds }})  >>  validate_bronze (jobs/validate.py --suite dp1 --ds {{ ds }} --period-end {{ var.value.period_end }} --fail-on-error)
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

## Pitfalls

- **Never print connections as URIs.** `airflow connections list` prints the password in plain
  text unless the output is filtered, as above.
- **New mounted folders after a merge.** Before a feature branch is merged, `git checkout develop`
  deletes a folder that only the feature branch has (here `airflow/`), and the merge creates it
  again as a new folder. Containers that mounted it then see an empty folder until they are
  recreated (`up -d --force-recreate`).
- **`--profile orch` alone is not enough,** like `batch`: Airflow needs Postgres from `core` and
  the Spark cluster from `batch`.
