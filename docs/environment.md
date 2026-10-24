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
| Spark (built) | `lastleg/spark:3.5.9-naive` from `infra/spark/Dockerfile.naive` | linux/arm64 | 2.57 GB | the base plus Delta 3.3.3, S3A and JDBC jars, Great Expectations ([lakehouse.md](lakehouse.md)) | lakehouse |
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

## Decisions and pitfalls

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
