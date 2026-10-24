# Environment: the machine, the images, and how to stop safely

This document records what the platform runs on. Before any service beyond `core` is built, it
checks that every pinned image runs natively on Apple Silicon. It also gives the safe start, stop
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
| Spark (built) | `lastleg/spark:3.5.9-naive` from `infra/spark/Dockerfile.naive` (the services now run `lastleg/spark:3.5.9`, 1.96 GB; see [built images](#built-images-naive-against-multi-stage)) | linux/arm64 | 2.82 GB | the base plus Delta 3.3.3, S3A and JDBC jars, Great Expectations, Sedona ([lakehouse.md](lakehouse.md)) | lakehouse |
| Hive Metastore | `starburstdata/hive:3.1.2-e.18` | linux/arm64 | 1.44 GB | JVM | lakehouse |
| Trino | `trinodb/trino:479` | linux/arm64 | 1.36 GB | Java 25.0.1 | query |
| Flink jobmanager, taskmanager | `flink:1.20.5-scala_2.12-java17` (the services run `lastleg/flink:1.20.5`, 1.68 GB, with PyFlink; see [built images](#built-images-naive-against-multi-stage)) | linux/arm64 | 0.80 GB | Ubuntu 22.04.5, Java 17.0.20.1, no Python | streaming |
| Airflow | `apache/airflow:2.10.5-python3.10` (the services run `lastleg/airflow:2.10.5`, 2.31 GB, built on the slim image; see [built images](#built-images-naive-against-multi-stage)) | linux/arm64 | 1.57 GB | Debian 12, Python 3.10.16 | orchestration |
| OpenSearch (DataHub search) | `opensearchproject/opensearch:2.19.7` | linux/arm64 | 1.18 GB | JVM | governance |
| DataHub GMS | `acryldata/datahub-gms:v1.5.0.6` | linux/arm64 | 1.43 GB | JVM | governance |
| DataHub frontend | `acryldata/datahub-frontend-react:v1.5.0.6` | linux/arm64 | 0.81 GB | JVM | governance |
| DataHub upgrade job | `acryldata/datahub-upgrade:v1.5.0.6` | linux/arm64 | 0.87 GB | JVM | governance |
| DataHub actions | `acryldata/datahub-actions:v1.5.0.6` | linux/arm64 | 3.32 GB | Python 3.11.15 | pulled, no service runs it |

The Size column comes from `docker image inspect --format '{{.Size}}'`. It is the uncompressed size
in the `overlay2` store, in decimal GB. The built images section below uses the same measure, so
you can compare its naive and multi-stage sizes directly with this table.

**Python 3.10 everywhere a job runs.** The Spark image and the Airflow image both run Python 3.10.
So a Spark driver started from Airflow and the Spark workers share a minor version (PySpark refuses
to run when they differ). The Flink base image has no Python. The PyFlink section below shows the
Python 3.10 that Ubuntu 22.04 installs. DataHub actions runs Python 3.11, but no Compose service
runs it.

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

- `Dockerfile.naive` is the first, simple build, kept as the baseline.
- `Dockerfile` is the multi-stage rewrite that the services run.

The sizes come from `docker images`, and the layers behind them come from `docker history`.

| Image | `Dockerfile.naive` | `Dockerfile` | Saved | Where the saving comes from |
|---|---:|---:|---:|---|
| `lastleg/spark:3.5.9` | 2.82 GB | 1.96 GB | 0.86 GB (30%) | The naive build adds the 7 jars with `ADD` and then runs `chmod` on them: that layer (1.28 GB) stores the 405 MB of jars a second time, with the pip packages. The fetch stage downloads them and sets their mode once, and the runtime copies them in one layer. The pip packages are installed with `--target`, without bytecode caches or their test suites. |
| `lastleg/airflow:2.10.5` | 3.23 GB | 2.31 GB | 0.92 GB (28%) | The naive build starts from the full `apache/airflow` image (1.57 GB), which ships every provider. The rewrite starts from `slim` (0.68 GB) and installs only what the DAGs and the metadata database use, at the full image's versions: postgres 6.0.0, amazon 9.2.0, openlineage 2.0.0, apache-spark 5.0.0 and the DataHub plugin. The drivers' Python is built in its own stage without caches or test suites, and only `bin`, `jars` and `python` of `/opt/spark` are copied (`conf` is mounted). |
| `lastleg/flink:1.20.5` | 2.06 GB | 1.68 GB | 0.38 GB (18%) | PyFlink has no arm64 wheel, so it is compiled. The naive build installs the compilers and Python headers next to PyFlink, in one 1.22 GB layer. The rewrite compiles the wheels in a first stage. The runtime installs them through a bind mount from that stage, so the wheels are no layer, and keeps only `python3` and `libpython3.10`. The Kafka connector gets its mode in the same `ADD`. |

**Checks of `lastleg/spark:3.5.9` (2026-10-24),** with the master, the worker and the history
server recreated on it:

- **Imports:** Great Expectations 0.18.19 (`SparkDFDataset`), Sedona 1.9.0, h3 4.5.0 and
  delta-spark 3.3.3 import. The 7 jars are in `/opt/spark/jars` with mode 644.
- **`smoke_lakehouse.py`:** it read 23,603,484 pings of 2024-06-01 over S3A, counted 80,800,000
  `oms` orders over JDBC, and wrote and registered a Delta table.
- **`validate.py --suite dp1 --ds 2024-11-27`:** 73 of 73 expectations passed. One earlier
  attempt failed before it registered with the master. That attempt started during the removal of
  a killed application. The cause of the failure was not captured, and the rerun passed.
- **`check_h3.py`** (Sedona against h3) passed.

**Checks of `lastleg/airflow:2.10.5` (2026-10-24),** with the webserver and the scheduler
recreated on it:

- **Providers:** `airflow providers list` shows amazon 9.2.0, postgres 6.0.0, apache-spark 5.0.0
  and the DataHub plugin 1.5.0.17.
- **DAGs:** Airflow reports no import errors and lists the 3 DAGs.
- **A task through the new client:** `airflow tasks test dp1_ingest_validate validate_bronze`
  for 2024-11-27 ran spark-submit with the drivers' Python and passed 73 of 73.
- **Lineage:** the DataHub listener started with `enabled=True` and the GMS address.

**Checks of `lastleg/flink:1.20.5` (2026-10-24),** with the jobmanager and the taskmanager
recreated on it. The dedup job ran twice in a row on one TaskManager, on the 84,712 events in
`parcel-scans-replay`. That is the second-job case that the pemja fix covers.

- Both jobs finished.
- `KEYED PROCESS` received 84,712 records and sent 83,460.
- The final windows numbered 32,124, with 83,460 scans, 27,577 delivered and 4,738
  late-delivered. These counts equal the truth in [streaming.md](streaming.md).

**Pitfall: the DataHub plugin needs the OpenLineage provider.** The first slim build had no
`apache-airflow-providers-openlineage`. The scheduler then logged `Failed to import plugin
acryl-datahub-airflow-plugin: No module named 'openlineage'`. The tasks still passed, but with that
error Airflow could not send lineage. The image now pins openlineage 2.0.0 and its libraries at
1.27.0, as in the full image.

## Idle memory

Each image that can start without the rest of the platform ran with its default configuration on
a throwaway network. The values come from `docker stats --no-stream`, read 90 s after start. At
that time, the Spark worker and the Flink taskmanager were registered, and the Trino and
OpenSearch logs showed that they started. For Airflow, a second reading 6 min after start gives
the range in its row.

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

The services below were not measured here, because they need other services configured first:

- **Hive Metastore** needs a JDBC URL. On first start, it creates its schema in the `metastore`
  database. Its memory, measured with the lakehouse services, is in
  [lakehouse.md](lakehouse.md#services-profile-batch). The image sets no heap, so the JVM default
  applies: a maximum heap of 25% of the memory it can see.
- **DataHub** GMS, frontend and upgrade need OpenSearch, Kafka and Postgres configured. Their
  memory, measured with the governance services, is in [datahub.md](datahub.md#services-profile-gov).
  GMS ships with an empty `JAVA_OPTS`, so it also gets the 25% default, about 3.9 GiB in this VM.

Idle memory is only the floor of the memory budget. Everything above adds up to about 5 GiB. But
under load, each JVM grows toward its default limit. No container has a memory limit, so several
of those default limits are sized from the whole 16 GB VM:

- Trino's 80% would let it take about 12.5 GiB.
- GMS and the Metastore would each take up to 25%.

So every service gets an explicit heap or `mem_limit` in the commit that adds it. At that point,
`docker stats` measures the memory of each set of profiles that must run together.

## PyFlink on arm64

PyFlink was the one real arm64 risk. PyPI has no Linux arm64 wheel for `apache-flink`, so
installing PyFlink in the Flink image builds it from source. The test below ran on 2026-10-23 as a
throwaway two-stage build. Its Dockerfile is not in the repo. The Flink image now follows the same
recipe in `infra/flink/Dockerfile`.

| Package | arm64 wheel on PyPI? | What `pip wheel` does |
|---|---|---|
| `apache-flink` 1.20.5 | no, source only | compiles 7 Cython modules (`coder_impl_fast`, `beam_operations_fast`, ...) into `apache_flink-1.20.5-cp310-cp310-linux_aarch64.whl` (5.9 MB) |
| `apache-flink-libraries` 1.20.5 | no, source only (the Flink jars) | packs them into a pure-Python wheel (232 MB) |
| `pemja` 0.5.7 | yes (`manylinux_2_17_aarch64`) | downloads it |
| `apache-beam` 2.48.0, `pyarrow` 11.0.0, `numpy` 1.24.4, `pandas` 2.2.3, `grpcio` 1.84.1 | yes | downloads them |

The recipe:

1. **Build stage:** it starts from `flink:1.20.5-scala_2.12-java17` and adds `python3 python3-dev python3-pip
   build-essential` from apt. Then it runs
   `pip wheel --wheel-dir /wheels "apache-flink==1.20.5" "setuptools<82"`, which makes 41 wheels
   in 88 s.
2. **Runtime stage:** it starts from the same base and adds `python3 python3-pip libpython3.10`.
   Then it runs `pip install --no-index --find-links /wheels "apache-flink==1.20.5"`.

The check ran a Python `map` function (`[1, 2, 3, 4]` to `[1, 4, 9, 16]`) on a local Flink
mini-cluster. It ran on `aarch64` with Python 3.10.12, in both Python execution modes:

| `python.execution-mode` | Without `libpython3.10` | With `libpython3.10` |
|---|---|---|
| `process` (the default: Python runs in a separate Beam worker process) | works | works |
| `thread` (Python runs inside the JVM through pemja) | fails | works, on Java 17 and on Java 11 |

Without `libpython3.10`, thread mode fails differently on each Java version:

- **Java 17:** `NoSuchFieldException: loadedLibraryNames`.
- **Java 11:** `UnsatisfiedLinkError: Expecting an absolute path of the library: None`.

The Java 17 message points at a JDK internal field. But the real cause is the same on both
versions: pemja loads the shared Python library, and Ubuntu's `python3` package does not install
it.

Decisions:

- **PyFlink 1.20.5, the same version as the Flink image.** The earlier plan said 1.20.3. That
  version pins `pemja` 0.5.5, which has no arm64 wheel. So the build stage would also need a JDK to
  compile pemja's JNI code. Version 1.20.5 pins `pemja` 0.5.7, which has an arm64 wheel.
- **Keep the Java 17 Flink image** and install `libpython3.10` in the runtime stage.
- **No `platform: linux/amd64` fallback is needed** for Flink.
- **In the session cluster, `flink-python` goes in `lib/` and pemja is parent-first.** The test
  above ran one job per JVM. On a TaskManager, every job loads pemja through its own classloader.
  But the JVM lets a native library load in only one classloader. The second thread-mode job then
  failed with the same `NoSuchFieldException: loadedLibraryNames`, because pemja's fallback for
  that case reads a field that Java 17 removed. With the jar copied to `lib/` and
  `classloader.parent-first-patterns.additional: pemja.`, three jobs ran in a row
  ([streaming.md](streaming.md)).

## Start, stop and unplug safely

All Docker data is inside `Docker.raw` on the T9. If you pull the drive while Docker writes, you
can corrupt that file, and with it every database, bucket and topic.

**Start:**

1. Plug in the T9. Wait until it shows in Finder (`ls /Volumes` lists `T9`).
2. Start Docker Desktop.
3. Run `docker compose --profile core up -d --wait`, with the other profiles that you need.

**Stop and unplug,** every time. The script `scripts/safe-shutdown.sh` runs the five steps below in
order. If any step fails, the script stops before the eject and leaves the drive mounted.

```bash
scripts/safe-shutdown.sh          # asks first whether every job is finished
scripts/safe-shutdown.sh --yes    # no question
```

By hand:

1. Make sure that no generator, Spark, Airflow or Flink job still writes data.
2. From the repo, run `docker compose --profile "*" stop -t 60`.
3. Quit Docker Desktop.
4. Run `lsof 2>/dev/null | grep /Volumes/T9 || echo "T9 not in use"`. Make sure that it prints
   `T9 not in use`.
5. Eject the physical disk, then unplug the drive. The command `diskutil eject /Volumes/T9` ejects
   only the APFS volume, which sits on a synthesized disk. Find the physical disk with
   `diskutil info /Volumes/T9 | grep "APFS Physical Store"` (for example `disk4s2`). Then run
   `diskutil eject disk4`. The disk number can change each time you plug in the drive. The script
   looks up the number when it runs.

Stop the containers with Compose (step 2) before you quit Docker Desktop (step 3). Each service
then gets its own grace period. The command `docker compose --profile "*" ps -a` also shows every
exit code before the VM shuts down.

**If the drive was unplugged by accident:** do not use Docker Desktop's "Reset to factory
defaults", which deletes the disk image. Plug the drive back in. Then start Docker Desktop. Check
the output of `docker compose --profile core ps -a`. The generator is deterministic, so it can
regenerate the raw data, and the lakehouse is then rebuilt from that data.

## Disk safety for the 100 GB runs

The full-scale data and the Spark jobs that read it share the T9 with everything else. Two limits
apply. The first is the free space on the SSD, which `Docker.raw` grows into. The second is the
Docker VM's own disk, which stops at the 800 GB limit. On 2026-10-24 before the full generation,
the SSD had 889 GB free and the VM had 705 GB, so the VM is the tighter limit.

**The watcher.** The script `scripts/disk-watch.sh` checks both limits every 30 s and takes the
smaller number. When that number falls below the threshold (default 50 GB), the watcher does three
things:

1. It kills every running Spark application through the master UI
   (`POST /app/kill/?id=<app>&terminate=true`).
2. It stops the generator with `pkill -f`. The pattern `-m generator.(oms_reference|oms_orders|app_exports)`
   matches the `uv` wrapper, its Python process and a shell that runs them.
3. It exits with status 2.

```bash
scripts/disk-watch.sh &                      # beside a 100 GB run
scripts/disk-watch.sh --once                 # one check
GENERATOR_PATTERN= scripts/disk-watch.sh ... # leave the generator alone
```

**Check (2026-10-24).** A test application (`disk-watch-test`, two tasks that sleep for 10
minutes) ran while the full generation loaded `oms`. The watcher ran with
`--once --threshold-gb 100000` and `GENERATOR_PATTERN=`, so the threshold was certain to trip. It
printed `killed Spark application app-20261024083026-0109` and exited 2. The master listed the
application as `KILLED`, and the generator kept running.

**Spark cleanup settings.**

- **Worker** (`SPARK_WORKER_OPTS` in compose): with `spark.worker.cleanup.enabled=true`, the worker
  checks every 30 minutes (`interval=1800`). It deletes the work folders of applications that ended
  over an hour ago (`appDataTtl=3600`).
- **Jobs** (`infra/spark/conf/spark-defaults.conf`): the setting is
  `spark.cleaner.periodicGC.interval 15min`. With it, a long job deletes the shuffle files of data
  that it no longer references. It does this every 15 minutes instead of every 30.
- **Shuffle files of a finished application:** without an external shuffle service, the standalone
  worker deletes the local folders of an application when the application ends. That is Spark's
  behaviour. It was not measured here.

## Decisions and pitfalls

- **`pkill` on macOS has no `-E`.** BSD `pgrep` and `pkill` already read the pattern as an extended
  regular expression, and `-E` is an illegal option. Inside `if`, that failure would only mean
  "no process stopped". The watcher's first pattern also missed the generator. The command `uv run`
  starts `.venv/bin/python3 -m generator...`, which the pattern `python -m` does not match. With
  the final pattern, `pgrep -fl` lists the `uv` wrapper, its Python process and the shell that
  runs them.
- **SeaweedFS needs about 26 s to stop.** It handles SIGTERM and shuts down its volume server,
  filer and master in order. That took 26 s and 28 s in two measured stops. Docker's default grace
  period is 10 s, after which Docker sends SIGKILL (exit 137), so the first shutdown on 2026-10-22
  killed SeaweedFS mid-way. The `seaweedfs` service now has `stop_grace_period: 60s`. A plain
  `docker compose stop seaweedfs` then exits 0, and an object written before the stop reads back
  with the same SHA-256 after the restart.
- **Kafka exits 143 on stop.** That is the normal exit code of a JVM that received SIGTERM and ran
  its shutdown hooks. It does not mean that Kafka failed.
- **`docker compose config` prints secrets.** Without `-q`, it prints the file with the values from
  `.env` filled in. To validate the file, use `docker compose config -q`. On a running container,
  `docker inspect` shows the container's environment too, but `docker image inspect` is safe.
