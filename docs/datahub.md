# DataHub: the platform's catalog, lineage and data quality

DataHub v1.5.0.6 holds the schema of every table in every zone. Lineage from the Airflow
pipelines and the Great Expectations results follow in the next steps.

## Services (profile `gov`)

| Service | Image | Role | Port on the Mac | Memory |
|---|---|---|---|---|
| `opensearch` | `opensearchproject/opensearch:2.19.7` | search and graph index (security plugin off: local only) | | heap 512 MB, 1.09 GiB in use |
| `datahub-system-update` | `acryldata/datahub-upgrade:v1.5.0.6` | one-shot: SQL schema in the `datahub` database, search indices, Kafka topics | | heap 512 MB |
| `datahub-gms` | `acryldata/datahub-gms:v1.5.0.6` | metadata service: REST and GraphQL API, internal schema registry, Kafka consumers | `127.0.0.1:8083` | heap 1 GB, 958 MiB in use |
| `datahub-frontend` | `acryldata/datahub-frontend-react:v1.5.0.6` | the UI | `127.0.0.1:9002` (user `datahub`, password `DATAHUB_ADMIN_PASSWORD` in `.env`) | heap 512 MB, 464 MiB in use |

`gov` runs on top of `core`: the metadata is in the `datahub` database of the shared Postgres,
and DataHub's event topics are in the core Kafka.

```bash
docker compose --profile core --profile gov up -d --wait
```

With `core`, `batch`, `query`, `orch` and `gov` all running, the containers use 9.1 GiB of the
16 GB VM.

**Settings that matter**, taken from the reference project and DataHub's quickstart:

- **Schema-registry flags.** `SCHEMA_REGISTRY_SYSTEM_UPDATE` and the auto-register flags are set
  on the system-update job only. Set on GMS too, its Kafka consumers stop and nothing gets indexed.
- **`ES_BULK_REFRESH_POLICY: NONE`.** Indexing does not wait for a refresh on every bulk write.
- **Topics.** `DATAHUB_PRECREATE_TOPICS: "true"`, because the broker does not auto-create topics.
- **Login.** The frontend writes `user.props` from `DATAHUB_ADMIN_PASSWORD` when it starts, so no
  password file is in the repo.
- **Authentication.** `METADATA_SERVICE_AUTH_ENABLED: "false"`: GMS listens only on `127.0.0.1`
  and the compose network.
- **Heaps.** They are set on every JVM. GMS ships without one and would otherwise take 25% of the
  VM ([environment.md](environment.md)).

## Schema ingestion (`datahub/recipes/`)

The recipes run from the Mac with the DataHub CLI. Credentials come from the environment, here
`.env`, so no recipe holds a secret:

```bash
set -a; . ./.env; set +a
uvx --python 3.11 --from 'acryl-datahub[trino,postgres,kafka]==1.5.0.17' datahub ingest -c datahub/recipes/trino_lakehouse.yml
uvx --python 3.11 --from 'acryl-datahub[trino,postgres,kafka]==1.5.0.17' datahub ingest -c datahub/recipes/postgres.yml
uvx --python 3.11 --from 'acryl-datahub[trino,postgres,kafka]==1.5.0.17' datahub ingest -c datahub/recipes/kafka.yml
```

| Recipe | Reads | Datasets |
|---|---|---|
| `trino_lakehouse.yml` | Trino catalog `delta`, schemas `bronze`, `silver`, `gold` (the bookkeeping table `dp3_progress` excluded) | 7 bronze, 6 silver, 10 gold |
| `postgres.yml` | databases `oms` (the source) and `dwh` (the warehouse), schema `public` | 5 `oms`, 7 `dwh` |
| `kafka.yml` | the platform's 4 topics; DataHub's own event topics on the same broker are excluded | 4 |

**URNs.** A lakehouse table's dataset URN is
`urn:li:dataset:(urn:li:dataPlatform:trino,delta.<zone>.<table>,PROD)`, for example
`delta.bronze.raw_orders`. The Airflow lineage uses the same URNs, so the lineage lands on the
datasets ingested here.

### Checks (2026-10-23)

- The system-update job completed 36 of 36 steps ("Upgrade SystemUpdate completed with result
  SUCCEEDED").
- GMS and the frontend were healthy 54 s after `up`.
- Every recipe finished without failures:
  - Trino: 24 tables scanned, 117 events;
  - Postgres: 12 tables, 70 events;
  - Kafka: 4 topics, 22 records. Its warning is that the topics have no registered schema.

A GraphQL search on GMS (`searchAcrossEntities`) lists the datasets by platform and zone:

| Platform | Zone | Datasets |
|---|---|---:|
| postgres | `oms` (source) | 5 |
| kafka | topics | 4 |
| trino | bronze | 7 |
| trino | silver | 6 |
| trino | gold | 10 |
| postgres | `dwh` | 7 |

## Pitfalls

- **Recipe patterns match from the start of the name.** A deny pattern of `\.dp3_progress$` let
  the table through. `.*\.dp3_progress$` excludes it. The entry that had been ingested was
  soft-deleted with `datahub delete --soft`.
- **Port 8080 on the Mac is the Spark master UI,** so GMS is on 8083.
