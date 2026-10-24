# Core services: the source systems and the event broker

The `core` Compose profile starts the three services that every later part depends on:

- Postgres holds the order system (`oms`) and the platform's own databases.
- SeaweedFS is the S3 object store for the courier-app exports and the lakehouse.
- Kafka carries the live parcel-scan events.

## Services

| Service | Image | Role | Port on the Mac | Data volume |
|---|---|---|---|---|
| `postgres` | `postgres:16.15` | One Postgres server, five databases (below) | `127.0.0.1:15432` | `postgres_data` |
| `seaweedfs` | `chrislusf/seaweedfs:4.48` | S3 API, plus a filer and master with web UIs | `127.0.0.1:8333` (S3), `:8888` (filer UI), `:9333` (master UI) | `seaweedfs_data` |
| `seaweedfs-init` | same image | One-shot job: creates the buckets, then exits | — | — |
| `kafka` | `apache/kafka:3.9.2` | Single broker in KRaft mode (no ZooKeeper) | `127.0.0.1:9092` | `kafka_data` |
| `kafka-init` | same image | One-shot job: creates the topics, then exits | — | — |

Every port binds to `127.0.0.1`, so the services are reachable only from this Mac. The volumes live
inside Docker Desktop's disk image, which is on the T9 SSD.

## Start and stop

```bash
cp .env.example .env                       # first time only, then change every change-me value
docker compose --profile core up -d --wait # start and wait for the health checks
docker compose --profile core ps -a        # postgres, seaweedfs, kafka healthy; both init jobs exit 0
docker compose --profile core stop         # stop and keep the data
```

`docker compose --profile core down -v` also deletes the volumes, and with them every database,
bucket and topic. Before unplugging the SSD, follow
[Start, stop and unplug safely](environment.md#start-stop-and-unplug-safely).

## What gets created

**Postgres databases** (`infra/postgres/init/01-create-databases.sql`):

| Database | Used by |
|---|---|
| `oms` | the order management system, a source system that stands in for the Operations department |
| `dwh` | the warehouse (star schema and `obt_` tables), loaded by DP2 |
| `airflow` | Airflow's metadata |
| `metastore` | the Hive Metastore catalog of lakehouse tables |
| `datahub` | DataHub's metadata |

Postgres also creates `postgres` and a database named after `POSTGRES_USER` (`platform`) by
default. The init script runs only on the first start of an empty volume.

**SeaweedFS buckets:**

| Bucket | Holds |
|---|---|
| `app-exports` | the courier-app team's daily Parquet exports (GPS pings, parcel scans), a source system |
| `lakehouse` | the platform's Delta tables: bronze, silver and gold |

**Kafka topics:**

| Topic | Partitions | Used by |
|---|---:|---|
| `parcel-scans` | 3 | live parcel-scan events |
| `parcel-scans-replay` | 1 | exact replays for Flink checks (one partition keeps the watermark simple) |
| `hub-delivery-5m` | 3 | Flink output: scans, delivered and late-delivered counts per hub and 5-minute window |
| `scans-late` | 1 | Flink side output: scans that arrive after the lateness limit |

## Verification (2026-10-22)

```text
$ docker compose --profile core ps -a
kafka            Up (healthy)     127.0.0.1:9092->9092/tcp
kafka-init       Exited (0)
postgres         Up (healthy)     127.0.0.1:15432->5432/tcp
seaweedfs        Up (healthy)     127.0.0.1:8333->8333/tcp, 127.0.0.1:8888->8888/tcp, 127.0.0.1:9333->9333/tcp
seaweedfs-init   Exited (0)

$ psql ... -c "SELECT datname FROM pg_database WHERE NOT datistemplate"
airflow  datahub  dwh  metastore  oms  platform  postgres

seaweedfs-init:  created bucket app-exports / created bucket lakehouse
kafka-init:      hub-delivery-5m  parcel-scans  parcel-scans-replay  scans-late

S3 from the Mac (boto3 with the .env keys): buckets ['app-exports', 'lakehouse']; put and delete ok
```

Further checks:

- **Restart:** after `docker compose restart kafka`, all four topics are still there.
- **Second `up`:** both init jobs run again and exit 0.
- **Memory at idle:** postgres 109 MiB, seaweedfs 90 MiB, kafka 319 MiB (about 0.5 GB in total).

## Decisions and pitfalls

- **Port 15432 for Postgres.** A Postgres already running on the Mac uses 5432, so the container
  publishes 15432. Containers still reach it as `postgres:5432`.
- **No credentials file for SeaweedFS.** SeaweedFS 4.48 creates its admin S3 identity from
  `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, which Compose fills from `.env`. Anonymous
  requests and wrong keys get HTTP 403. The public repo holds no keys.
- **`-volume.max=0`.** By default SeaweedFS stops at 8 volumes of 30 GB, which is 240 GB. That is
  less than the exports plus the bronze, silver and gold copies. With `0`, it sizes the limit from
  the free disk.
- **`KAFKA_LOG_DIRS=/var/lib/kafka/data`.** The image writes to `/tmp/kraft-combined-logs` by
  default. That folder is not on the volume, so a container rebuild would lose every topic. The
  reference project mounted the volume without setting this.
- **Fixed `CLUSTER_ID`.** KRaft stamps the cluster ID into the data folder. The ID is generated
  once with `kafka-storage.sh random-uuid` and kept in the compose file, so the volume stays valid.
- **`stop_grace_period: 60s` for SeaweedFS.** A clean stop takes about 26 s, so Docker's default
  10 s killed it (exit 137). Measurements are in [environment.md](environment.md#decisions-and-pitfalls).
- **Topics are explicit.** `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, so a typo in a topic name
  fails loudly instead of creating a new topic.
- **No time-based retention on the four stream topics.** Their records carry the 2024 event time
  as the record timestamp, and the 7-day default deletes a segment whose timestamps are older, a
  few minutes after it is written. `kafka-init` sets `retention.ms=-1` on them (an alter, so it is
  safe to rerun). The producer and the Flink runs empty them before each run
  ([streaming.md](streaming.md)).
