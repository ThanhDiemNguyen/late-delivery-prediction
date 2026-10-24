# Environment: the machine, the images, and how to stop safely

This document records what the platform runs on and checks, before any service beyond `core` is
built, that every pinned image runs natively on Apple Silicon. It also holds the safe start, stop
and unplug procedure for the external SSD that holds all Docker data.

## Machine and Docker Desktop

| Item | Value |
|---|---|
| Mac | Apple M4, 10 cores, 24 GB RAM |
| Docker Desktop VM | 16 GB memory, 8 CPUs, 2 GB swap, `linux/aarch64`, Docker Engine 29.9.2 |
| Docker disk image | `/Volumes/T9/docker-data/DockerDesktop/Docker.raw` on a Samsung T9 1 TB SSD (APFS), 800 GB limit |
| Image store | `overlay2` (the classic store, not the containerd snapshotter) |

Every image, volume, bucket and table lives in that disk image, so the SSD must be plugged in
before Docker starts.

## Image inventory

Every pinned image was pulled with `docker pull --platform linux/arm64` on 2026-10-23. All of them
publish a `linux/arm64` build, so no service needs Rosetta (`platform: linux/amd64`).

| Service | Image | Arch | Size | Runtime inside | Added in |
|---|---|---|---:|---|---|
| Postgres | `postgres:16.15` | linux/arm64 | 0.47 GB | — | core |
| SeaweedFS | `chrislusf/seaweedfs:4.48` | linux/arm64 | 0.50 GB | Go binary | core |
| Kafka | `apache/kafka:3.9.2` | linux/arm64 | 0.41 GB | JVM | core |
| Spark master, worker, history | `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` | linux/arm64 | 1.13 GB | Ubuntu 22.04.5, Python 3.10.12, Java 17.0.19 | lakehouse |
| Spark (built) | `lastleg/spark:3.5.9-naive` from `infra/spark/Dockerfile.naive` | linux/arm64 | 2.82 GB | the base plus Delta 3.3.3, S3A and JDBC jars, Great Expectations, Sedona ([lakehouse.md](lakehouse.md)) | lakehouse |
| Hive Metastore | `starburstdata/hive:3.1.2-e.18` | linux/arm64 | 1.44 GB | JVM | lakehouse |
| Trino | `trinodb/trino:479` | linux/arm64 | 1.36 GB | Java 25.0.1 | query |
| Flink jobmanager, taskmanager | `flink:1.20.5-scala_2.12-java17` | linux/arm64 | 0.80 GB | Ubuntu 22.04.5, Java 17.0.20.1, no Python | streaming |
| Airflow | `apache/airflow:2.10.5-python3.10` | linux/arm64 | 1.57 GB | Debian 12, Python 3.10.16 | orchestration |
| OpenSearch (DataHub search) | `opensearchproject/opensearch:2.19.7` | linux/arm64 | 1.18 GB | JVM | governance |
| DataHub GMS | `acryldata/datahub-gms:v1.5.0.6` | linux/arm64 | 1.43 GB | JVM | governance |
| DataHub frontend | `acryldata/datahub-frontend-react:v1.5.0.6` | linux/arm64 | 0.81 GB | JVM | governance |
| DataHub upgrade job | `acryldata/datahub-upgrade:v1.5.0.6` | linux/arm64 | 0.87 GB | JVM | governance |
| DataHub actions | `acryldata/datahub-actions:v1.5.0.6` | linux/arm64 | 3.32 GB | Python 3.11.15 | governance |

**Size** is `docker image inspect --format '{{.Size}}'`, the uncompressed size in the `overlay2`
store, in decimal GB. The Docker image optimization step later uses the same measure, so its
before and after numbers compare directly with this table.

**Python 3.10 everywhere a job runs.** The Spark image and the Airflow image both run Python 3.10,
so a Spark driver started from Airflow and the Spark workers share a minor version (PySpark
refuses to run when they differ). The Flink image has no Python; the PyFlink section below shows
the 3.10 that its Ubuntu 22.04 base installs. DataHub actions runs Python 3.11, but it is a
separate service and does not exchange Python objects with any job.

Reproduce the table:

```bash
for img in postgres:16.15 chrislusf/seaweedfs:4.48 apache/kafka:3.9.2 \
    apache/spark:3.5.9-scala2.12-java17-python3-ubuntu starburstdata/hive:3.1.2-e.18 trinodb/trino:479 \
    flink:1.20.5-scala_2.12-java17 apache/airflow:2.10.5-python3.10 opensearchproject/opensearch:2.19.7 \
    acryldata/datahub-gms:v1.5.0.6 acryldata/datahub-frontend-react:v1.5.0.6 \
    acryldata/datahub-upgrade:v1.5.0.6 acryldata/datahub-actions:v1.5.0.6; do
  docker pull -q --platform linux/arm64 "$img"
  docker image inspect "$img" --format "$img {{.Os}}/{{.Architecture}} {{.Size}}"
done
```

## Built images: naive against multi-stage

Each built image has two Dockerfiles in its `infra/` folder:

- `Dockerfile.naive` is the first, simple build, kept as the baseline;
- `Dockerfile` is the multi-stage rewrite that the services run.

Sizes are from `docker images`, and the layers behind them from `docker history`.

| Image | `Dockerfile.naive` | `Dockerfile` | Saved | Where the saving comes from |
|---|---:|---:|---:|---|
| `lastleg/spark:3.5.9` | 2.82 GB | 1.96 GB | 0.86 GB (30%) | The naive build adds the 7 jars with `ADD` and then runs `chmod` on them: that layer (1.28 GB) stores the 405 MB of jars a second time, with the pip packages. The fetch stage downloads them and sets their mode once, and the runtime copies them in one layer. The pip packages are installed with `--target`, without bytecode caches or their test suites. |
| `lastleg/airflow:2.10.5` | 3.23 GB | 2.31 GB | 0.92 GB (28%) | The naive build starts from the full `apache/airflow` image (1.57 GB), which ships every provider. The rewrite starts from `slim` (0.68 GB) and installs only what the DAGs and the metadata database use, at the full image's versions: postgres 6.0.0, amazon 9.2.0, openlineage 2.0.0, apache-spark 5.0.0 and the DataHub plugin. The drivers' Python is built in its own stage without caches or test suites, and only `bin`, `jars` and `python` of `/opt/spark` are copied (`conf` is mounted). |
| `lastleg/flink:1.20.5` | 2.06 GB | 1.68 GB | 0.38 GB (18%) | PyFlink has no arm64 wheel, so it is compiled. The naive build installs the compilers and Python headers next to PyFlink, in one 1.22 GB layer. The rewrite compiles the wheels in a first stage. The runtime installs them through a bind mount from that stage, so the wheels are no layer, and keeps only `python3` and `libpython3.10`. The Kafka connector gets its mode in the same `ADD`. |

**Checks of `lastleg/spark:3.5.9` (2026-10-24),** with the master, the worker and the history
server recreated on it:

- **Imports:** Great Expectations 0.18.19 (`SparkDFDataset`), Sedona 1.9.0, h3 4.5.0 and
  delta-spark 3.3.3 import, and the 7 jars are in `/opt/spark/jars` with mode 644.
- **`smoke_lakehouse.py`:** it read 23,603,484 pings of 2024-06-01 over S3A, counted 80,800,000
  `oms` orders over JDBC, and wrote and registered a Delta table.
- **`validate.py --suite dp1 --ds 2024-11-27`:** 73 of 73 expectations passed. One earlier
  attempt failed before it registered with the master. It started just as a killed application was
  being removed; the cause was not captured, and the rerun passed.
- **`check_h3.py`** (Sedona against h3) passed.

**Checks of `lastleg/airflow:2.10.5` (2026-10-24),** with the webserver and the scheduler
recreated on it:

- **Providers:** `airflow providers list` shows amazon 9.2.0, postgres 6.0.0, apache-spark 5.0.0
  and the DataHub plugin 1.5.0.17.
- **DAGs:** no import errors, and the 3 DAGs are listed.
- **A task through the new client:** `airflow tasks test dp1_ingest_validate validate_bronze`
  for 2024-11-27 ran spark-submit with the drivers' Python and passed 73 of 73.
- **Lineage:** the DataHub listener started with `enabled=True` and the GMS address.

**Checks of `lastleg/flink:1.20.5` (2026-10-24),** with the jobmanager and the taskmanager
recreated on it. The dedup job ran twice in a row on one TaskManager, on the 84,712 events in
`parcel-scans-replay`. That is the second-job case that the pemja fix covers.

- Both jobs finished.
- `KEYED PROCESS` received 84,712 records and sent 83,460.
- The final windows were 32,124, with 83,460 scans, 27,577 delivered and 4,738 late-delivered.
  Those are the truth's numbers in [streaming.md](streaming.md).

**Pitfall: the DataHub plugin needs the OpenLineage provider.** The first slim build had no
`apache-airflow-providers-openlineage`. The scheduler then logged `Failed to import plugin
acryl-datahub-airflow-plugin: No module named 'openlineage'`. The tasks still passed, but Airflow
would have sent no lineage. The image now pins openlineage 2.0.0 and its libraries at 1.27.0, as
in the full image.

## Idle memory

Each image that can start without the rest of the platform ran with its default configuration on
a throwaway network. `docker stats --no-stream` was read 90 s after start, when the Spark worker
and the Flink taskmanager had registered and Trino and OpenSearch had logged that they started.
Airflow was read again 6 min after start, which gives the range in its row.

| Container | Default memory rule in the image | Idle memory |
|---|---|---:|
| Spark master | daemon heap 1 GB (`SPARK_DAEMON_MEMORY`) | 163 MiB |
| Spark worker | daemon heap 1 GB; each executor is a separate JVM with 1 GB by default | 186 MiB |
| Flink jobmanager | `jobmanager.memory.process.size: 1600m` | 293 MiB |
| Flink taskmanager | `taskmanager.memory.process.size: 1728m`, 1 slot | 428 MiB |
| Trino | `-XX:InitialRAMPercentage=80 -XX:MaxRAMPercentage=80` | 697 MiB |
| OpenSearch | `-Xms1g -Xmx1g` | 1.31 GiB |
| Airflow `standalone` (webserver, scheduler and triggerer in one container, SQLite) | 17 Python processes | 1.25 to 1.44 GiB |
| Core: Postgres, SeaweedFS, Kafka | — | 21 MiB, 186 MiB, 330 MiB |

Not measured here, because they need other services configured first:

- **Hive Metastore** needs a JDBC URL, and on first start it creates its schema in the `metastore`
  database. It is measured when the lakehouse services are added. The image sets no heap, so the
  JVM default applies: a maximum heap of 25% of the memory it can see.
- **DataHub** GMS, frontend, upgrade and actions need OpenSearch, Kafka and Postgres configured.
  They are measured when the governance services are added. GMS ships with an empty `JAVA_OPTS`,
  so it also gets the 25% default, about 3.9 GiB in this VM.

Idle memory is a floor, not the budget. Everything above adds up to about 5 GiB, but under load
each JVM grows toward its default limit, and several of those limits are sized from the whole
16 GB VM because no container has a memory limit:

- Trino's 80% would let it take about 12.5 GiB.
- GMS and the Metastore would each take up to 25%.

So every service gets an explicit heap or `mem_limit` in the commit that adds it, and the memory
of each set of profiles that must run together is measured with `docker stats` at that point.

## PyFlink on arm64

This was the one real arm64 risk. PyPI has no Linux arm64 wheel for `apache-flink`, so installing
PyFlink in the Flink image builds it from source. The test below ran on 2026-10-23 as a throwaway
two-stage build. Its Dockerfile is not in the repo: the Flink image is added with the streaming
services.

| Package | arm64 wheel on PyPI? | What `pip wheel` does |
|---|---|---|
| `apache-flink` 1.20.5 | no, source only | compiles 7 Cython modules (`coder_impl_fast`, `beam_operations_fast`, ...) into `apache_flink-1.20.5-cp310-cp310-linux_aarch64.whl` (5.9 MB) |
| `apache-flink-libraries` 1.20.5 | no, source only (the Flink jars) | packs them into a pure-Python wheel (232 MB) |
| `pemja` 0.5.7 | yes (`manylinux_2_17_aarch64`) | downloads it |
| `apache-beam` 2.48.0, `pyarrow` 11.0.0, `numpy` 1.24.4, `pandas` 2.2.3, `grpcio` 1.84.1 | yes | downloads them |

The recipe:

1. **Build stage:** `flink:1.20.5-scala_2.12-java17`, plus `python3 python3-dev python3-pip
   build-essential` from apt. Then
   `pip wheel --wheel-dir /wheels "apache-flink==1.20.5" "setuptools<82"`, which makes 41 wheels
   in 88 s.
2. **Runtime stage:** the same base, plus `python3 python3-pip libpython3.10`. Then
   `pip install --no-index --find-links /wheels "apache-flink==1.20.5"`.

The check ran a Python `map` function (`[1, 2, 3, 4]` to `[1, 4, 9, 16]`) on a local Flink
mini-cluster, on `aarch64` with Python 3.10.12, in both Python execution modes:

| `python.execution-mode` | Without `libpython3.10` | With `libpython3.10` |
|---|---|---|
| `process` (the default: Python runs in a separate Beam worker process) | works | works |
| `thread` (Python runs inside the JVM through pemja) | fails | works, on Java 17 and on Java 11 |

Without `libpython3.10`, thread mode fails differently on each Java version:

- **Java 17:** `NoSuchFieldException: loadedLibraryNames`.
- **Java 11:** `UnsatisfiedLinkError: Expecting an absolute path of the library: None`.

The Java 17 message points at a JDK internal field, but the real cause is the same on both. pemja
loads the shared Python library, and Ubuntu's `python3` package does not install it.

Decisions:

- **PyFlink 1.20.5, the same version as the Flink image.** The earlier plan said 1.20.3. That
  version pins `pemja` 0.5.5, which has no arm64 wheel, so it would also need a JDK in the build
  stage to compile pemja's JNI code. 1.20.5 pins `pemja` 0.5.7, which has one.
- **Keep the Java 17 Flink image** and install `libpython3.10` in the runtime stage.
- **No `platform: linux/amd64` fallback is needed** for Flink.
- **In the session cluster, `flink-python` goes in `lib/` and pemja is parent-first.** The test
  above ran one job per JVM. On a TaskManager, every job loads pemja through its own classloader,
  and the JVM lets a native library load in only one classloader. The second thread-mode job then
  failed with the same `NoSuchFieldException: loadedLibraryNames`, because pemja's fallback for
  that case reads a field Java 17 removed. With the jar copied to `lib/` and
  `classloader.parent-first-patterns.additional: pemja.`, three jobs in a row ran
  ([streaming.md](streaming.md)).

## Start, stop and unplug safely

All Docker data is inside `Docker.raw` on the T9. Pulling the drive while Docker is writing can
corrupt that file and with it every database, bucket and topic.

**Start:**

1. Plug in the T9 and wait until it shows in Finder (`ls /Volumes` lists `T9`).
2. Start Docker Desktop.
3. `docker compose --profile core up -d --wait` (add the other profiles you need).

**Stop and unplug,** every time. `scripts/safe-shutdown.sh` runs these five steps in order. If any
step fails, it stops before ejecting and leaves the drive mounted.

```bash
scripts/safe-shutdown.sh          # asks first whether every job is finished
scripts/safe-shutdown.sh --yes    # no question
```

By hand:

1. Make sure no generator, Spark, Airflow or Flink job is still writing.
2. From the repo: `docker compose --profile "*" stop -t 60`.
3. Quit Docker Desktop.
4. `lsof 2>/dev/null | grep /Volumes/T9 || echo "T9 not in use"` must print `T9 not in use`.
5. Eject the physical disk, then unplug. `diskutil eject /Volumes/T9` ejects only the APFS volume,
   which sits on a synthesized disk. Find the physical disk with
   `diskutil info /Volumes/T9 | grep "APFS Physical Store"` (for example `disk4s2`), then run
   `diskutil eject disk4`. The disk number can change each time the drive is plugged in; the
   script looks it up when it runs.

Stop the containers with Compose (step 2) before quitting Docker Desktop (step 3). Each service
then gets its own grace period, and `docker compose --profile "*" ps -a` shows every exit code
before the VM shuts down.

**If the drive was unplugged by accident:** do not use Docker Desktop's "Reset to factory
defaults", which deletes the disk image. Plug the drive back in, start Docker Desktop, and check
`docker compose --profile core ps -a`. The generator is deterministic, so raw data can be
regenerated, and the lakehouse is rebuilt from raw.

## Disk safety for the 100 GB runs

The full-scale data and the Spark jobs that read it share the T9 with everything else. Two limits
apply: the free space on the SSD, which `Docker.raw` grows into, and the Docker VM's own disk,
which stops at the 800 GB limit. On 2026-10-24 before the full generation, the SSD had 889 GB free
and the VM 705 GB, so the VM is the tighter one.

**The watcher (`scripts/disk-watch.sh`)** checks both every 30 s and takes the smaller number.
Below the threshold (default 50 GB) it does three things:

1. It kills every running Spark application through the master UI
   (`POST /app/kill/?id=<app>&terminate=true`).
2. It stops the generator with `pkill -f`, matching `-m generator.(oms_reference|oms_orders|app_exports)`:
   the `uv` wrapper, its Python process and a shell that runs them.
3. It exits with status 2.

```bash
scripts/disk-watch.sh &                      # beside a 100 GB run
scripts/disk-watch.sh --once                 # one check
GENERATOR_PATTERN= scripts/disk-watch.sh ... # leave the generator alone
```

**Check (2026-10-24).** A test application (`disk-watch-test`, two tasks that sleep for 10
minutes) was running while the full generation loaded `oms`. The watcher ran with
`--once --threshold-gb 100000` and `GENERATOR_PATTERN=`, so the threshold was certain to trip. It
printed `killed Spark application app-20261024083026-0109` and exited 2. The master listed the
application as `KILLED`, and the generator kept running.

**Spark cleanup settings.**

- **Worker** (`SPARK_WORKER_OPTS` in compose): `spark.worker.cleanup.enabled=true`, checked every
  30 minutes (`interval=1800`), deletes the work folders of applications that ended over an hour
  ago (`appDataTtl=3600`).
- **Jobs** (`infra/spark/conf/spark-defaults.conf`): `spark.cleaner.periodicGC.interval 15min`.
  A long job then deletes the shuffle files of data it no longer references every 15 minutes
  instead of every 30.
- **Shuffle files of a finished application:** without an external shuffle service, the
  standalone worker deletes an application's local folders when it ends. That is Spark's
  behaviour; it was not measured here.

## Decisions and pitfalls

- **`pkill` on macOS has no `-E`.** BSD `pgrep` and `pkill` already read the pattern as an extended
  regular expression, and `-E` is an illegal option. Inside `if`, that failure would only mean
  "no process stopped". The watcher's first pattern also missed the generator: `uv run` starts
  `.venv/bin/python3 -m generator...`, which `python -m` does not match. `pgrep -fl` with the final
  pattern lists the `uv` wrapper, its Python process and the shell that runs them.
- **SeaweedFS needs about 26 s to stop.** It handles SIGTERM and shuts its volume server, filer
  and master down in order, which took 26 s and 28 s in two measured stops. Docker's default grace
  period is 10 s, after which it sends SIGKILL (exit 137), so the first shutdown on 2026-10-22
  killed it mid-way. The `seaweedfs` service now has `stop_grace_period: 60s`. A plain
  `docker compose stop seaweedfs` then exits 0, and an object written before the stop reads back
  with the same SHA-256 after the restart.
- **Kafka exits 143 on stop.** That is the normal exit code of a JVM that received SIGTERM and ran
  its shutdown hooks; it is not a failure.
- **`docker compose config` prints secrets.** Without `-q` it prints the file with the values from
  `.env` filled in. Use `docker compose config -q` to validate. `docker inspect` on a running
  container shows its environment too; `docker image inspect` is safe.
