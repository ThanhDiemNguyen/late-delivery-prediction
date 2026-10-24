# Lakehouse: Hive Metastore, Spark, Delta Lake and Trino

The lakehouse stores the platform's own tables (bronze, silver, gold) as Delta tables in the
SeaweedFS bucket `lakehouse`.

- **Spark** builds them.
- **The Hive Metastore** catalogs them, so Spark and Trino can find a table by name.

- **Trino** queries them with SQL for DBeaver and DataHub.

This page covers the services, the Spark image, how a job runs, the smoke test that proves the
whole path works, and Trino.

## Services (profile `batch`)

| Service | Image | Role | Port on the Mac | Memory |
|---|---|---|---|---|
| `hive-metastore` | `starburstdata/hive:3.1.2-e.18` | table catalog over Thrift; its schema lives in the Postgres database `metastore`. Also in profile `query`, for Trino | `127.0.0.1:9083` | heap 512 MB, 699 MiB in use |
| `spark-master` | `lastleg/spark:3.5.9-naive` | standalone master; jobs are submitted inside this container, so their driver runs here | `127.0.0.1:8080` (master UI), `:4040` (running job UI) | daemon 512 MB, 272 MiB in use |
| `spark-worker` | same | runs executors: 4 cores and 4 GB for the dev slice (`SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY`) | `127.0.0.1:8081` | daemon 512 MB; executors 2 GB each |
| `spark-history` | same | the Spark UI of finished jobs, from the event logs in the `spark_events` volume | `127.0.0.1:18080` | daemon 512 MB, 272 MiB in use |

The batch profile runs on top of `core`, because the metastore needs Postgres and SeaweedFS:

```bash
docker compose --profile core --profile batch up -d --wait
docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/<job>.py
```

With `core` and `batch` running and no job active, the containers use about 3.1 GB in total
(measured with `docker stats` after the smoke test).

## The Spark image (`infra/spark/Dockerfile.naive`, then `Dockerfile`)

The services now run `lastleg/spark:3.5.9` from the multi-stage `infra/spark/Dockerfile` (1.96 GB
against 2.82 GB, with the same jars and packages;
[environment.md](environment.md#built-images-naive-against-multi-stage)). This section describes
the contents, which both builds share.

The image is built on `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` (Ubuntu 22.04, Python
3.10.12, Java 17) and adds:

| Jar or package | Version | Why |
|---|---|---|
| `delta-spark_2.12`, `delta-storage` | 3.3.3 | Delta Lake tables |
| `hadoop-aws` + `aws-java-sdk-bundle` | 3.3.4 + 1.12.262 | S3A access to SeaweedFS; it must match Spark's Hadoop 3.3.4 |
| `postgresql` JDBC | 42.7.7 | read `oms`, write `dwh` |
| `delta-spark` (Python, `--no-deps`) | 3.3.3 | the `delta.tables` API; `--no-deps` stops pip from installing a second PySpark |
| `great_expectations` | 0.18.19 | the validate stage of DP1 to DP3 ([pipelines.md](pipelines.md)) |
| `sedona-spark-shaded-3.5_2.12` + `geotools-wrapper`, `apache-sedona`, `h3` | 1.9.0, 1.9.0-33.5, 1.9.0, 4.5.0 | H3 zones and spatial SQL ([novel_h3_sedona.md](novel_h3_sedona.md)); `h3` checks Sedona's cells |

This first image is deliberately naive: one stage, jars downloaded at build time, and a `chmod`
in its own layer. It is 2.82 GB (2.05 GB before Great Expectations and Sedona were added), against 1.13 GB for the base
image. The Docker optimization step
rewrites it as a multi-stage build and measures both with the same size measure
([environment.md](environment.md#image-inventory)).

**Shared settings.** `infra/spark/conf/spark-defaults.conf` is mounted into every Spark container.

- **Catalog and Delta:** the Delta SQL extension and catalog, and the Hive Metastore as catalog
  (`thrift://hive-metastore:9083`).
- **Storage:** S3A pointed at `http://seaweedfs:8333` with path-style access, and the warehouse in
  `s3a://lakehouse/warehouse`.
- **Time and logs:** UTC session time, and event logs in `/opt/spark/events`.
- **Keys:** none in the file. S3A reads them from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`,
  which Compose fills from `.env`. Jobs read the Postgres login from `POSTGRES_USER` and
  `POSTGRES_PASSWORD` the same way.

## Smoke test

`jobs/smoke_lakehouse.py` checks every path a pipeline will use:

```text
$ docker compose exec spark-master /opt/spark/bin/spark-submit /opt/jobs/smoke_lakehouse.py
[s3a] pings on 2024-06-01: 239,412 rows, columns ['courier_id', 'device_seq', 'hub_id', 'trip_id', 'ping_ts', 'lat', 'lon', 'speed_kmh', 'app_version', 'uploaded_ts']
[jdbc] oms.orders: 808,000 rows
[delta] smoke.pings_per_hub: 34 hubs, 239,412 pings; format delta, 1 files at s3a://lakehouse/smoke/pings_per_hub
[catalog] tables in smoke: ['pings_per_hub']
```

The metastore database holds the table as `smoke | pings_per_hub | MANAGED_TABLE | delta`. The
history server at `http://127.0.0.1:18080` lists the job as completed. The job took 13 s.

## Trino (profile `query`)

| Service | Image | Role | Port on the Mac | Memory |
|---|---|---|---|---|
| `trino` | `trinodb/trino:479` | SQL over the lakehouse: catalog `delta` (`infra/trino/catalog/delta.properties`) | `127.0.0.1:8090` | `mem_limit: 2g`, 991 MiB in use |

The `delta` catalog uses the `delta_lake` connector:

- **Finding tables:** it asks the Hive Metastore, so every table Spark registers appears in Trino
  under the same schema and name.
- **Reading files:** it reads them from SeaweedFS with Trino's native S3 file system, with the keys
  from the container environment (`${ENV:S3_ACCESS_KEY}`).
- **Memory:** Trino sizes its heap at 80% of the memory it can see, so without `mem_limit` it could
  take about 12.5 GiB of the VM.
- **Port:** host port 8090, because 8080 is the Spark master UI.

```bash
docker compose --profile core --profile batch --profile query up -d --wait
docker compose exec trino trino --execute "SELECT count(*), sum(pings) FROM delta.smoke.pings_per_hub"
```

The smoke table reads back with the same numbers Spark wrote: 34 hubs and 239,412 pings. The top
hub is Downtown LA (hub 1) with 70,548 pings. From the Mac, the Python Trino client on
`127.0.0.1:8090` gets the same result. That is the path DBeaver uses.

**DBeaver:** New connection, then Trino, with these settings:

| Setting | Value |
|---|---|
| Host | `127.0.0.1` |
| Port | `8090` |
| User | any name, for example `lastleg` |
| Password | none |
| Catalog | `delta` |

## Decisions and pitfalls

- **Delta 3.3.3, not 3.3.2, on Spark 3.5.9.** Delta 3.3.2 is built against Spark 3.5.3. On Spark
  3.5.9 these all failed with "Table … does not support truncate in batch mode":
  - `saveAsTable` with mode `overwrite`;
  - `writeTo().createOrReplace()`;
  - `CREATE OR REPLACE TABLE … AS SELECT`.

  Appends and path-based writes worked. Delta 3.3.3 (August 2026, built against Spark 3.5.6) passes
  all five write paths, and the Python package accepts PySpark 3.5.3 to 3.5.x.
- **Profiles stack.** `--profile batch` alone fails ("depends on undefined service postgres"),
  because Postgres and SeaweedFS are in `core`. Always pass `--profile core` as well.
- **Config is mounted as folders.** `infra/spark/conf/` and `infra/trino/catalog/` are mounted as
  whole folders. A single-file bind mount follows the file's inode, so after `git checkout`
  replaced `spark-defaults.conf`, the running containers could no longer find it
  (`FileNotFoundException`). With folder mounts, the containers see a replaced file. They still
  lose the folder if git deletes and recreates it (switching to a branch that does not have it),
  and then a job runs without its settings. Its S3A requests then go to Amazon's public endpoint,
  which rejects them with `InvalidAccessKeyId`. Every job therefore starts through
  `jobs/common.py`, which stops with a clear message when the settings are missing. The fix is to
  recreate the Spark containers.
  The Trino catalog folder replaces the image's demo catalogs (`tpch`, `jmx` and the others), and
  only `delta` is needed.
- **The metastore image wants empty cloud variables.** Its start script stops unless 14
  Azure/GCS variables exist, so the Compose file sets them to `""`, as the course lab did.
- **The metastore's own log.** Its start script traces commands with `set -x`. A check of its log
  found neither the Postgres password nor the S3 secret key. Only the access key id, which is the
  user name `platform`, appears once.
