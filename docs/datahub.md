# DataHub: the platform's catalog, lineage and data quality

DataHub v1.5.0.6 holds the schema of every table in every zone and the lineage of the Airflow
pipelines DP1 to DP3. The Great Expectations results follow in the next step.

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

## Lineage from Airflow

The DataHub Airflow plugin (`acryl-datahub-airflow-plugin` 1.5.0.17, in the Airflow image) sends
each pipeline and task to GMS when a task runs:

- a DAG becomes a DataFlow, and a task becomes a DataJob of that flow;
- a task's `inlets` and `outlets` become the DataJob's inputs and outputs.

Spark jobs have no lineage that the plugin can read, so every `SparkSubmitOperator` declares its
datasets. The helpers in `airflow/dags/lakehouse_tasks.py` build them with the URNs that the
recipes ingested:

| Helper | Dataset URN name | Example |
|---|---|---|
| `lake(zone, *tables)` | `trino`, `delta.<zone>.<table>` | `delta.silver.stg_orders` |
| `postgres(db, *tables)` | `postgres`, `<db>.public.<table>` | `oms.public.orders`, `dwh.public.fact_delivery` |
| `exports(*datasets)` | `s3`, `app-exports/<dataset>` | `app-exports/courier_pings` |

| DAG | Task | Inputs | Outputs |
|---|---|---|---|
| `dp1_ingest_validate` | `ingest_bronze` | 5 `oms` tables, 2 app exports | 7 bronze tables |
| | `validate_bronze` | 7 bronze tables | |
| `dp2_transform_validate` | `silver` | 7 bronze tables | 6 silver tables |
| | `gold_dims` | `stg_hubs`, `stg_customers`, `stg_courier_changes` | 5 dimensions |
| | `gold_facts` | `stg_orders`, `stg_courier_pings`, `dim_courier`, `dim_hub` | 2 facts |
| | `warehouse` | 5 dimensions, `fact_delivery` | 7 `dwh` tables |
| | `validate_suite`, `validate_scd2` | the tables they check | |
| `dp3_features_validate` | `features` | 2 facts, 4 dimensions | 3 `feat_` tables |
| | `validate_suite`, `validate_incremental` | the tables they check | |

The validate tasks have inputs only: they read and write nothing back. The Airflow connection
`datahub_rest_default` points the plugin at `http://datahub-gms:8080`. To run Airflow without the
`gov` profile, set `DATAHUB_ENABLED=false`.

### Checks (2026-10-23)

The chain ran for 2024-11-27 with the plugin on: DP1 in 67 s, DP2 at 346 s, DP3 at 496 s, all
green. Then a GraphQL query on GMS returned:

- 3 DataFlows and 13 DataJobs. Each Spark task has the inputs and outputs in the table above; the
  two trigger tasks have none.
- An upstream lineage search from `dwh.public.fact_delivery` that reaches every zone back to
  the sources: 6 gold, 5 silver, 7 bronze, the 5 `oms` tables and the 2 app exports. The silver
  table that does not appear is `stg_parcel_scans`, because no gold job reads it.

## Pitfalls

- **Recipe patterns match from the start of the name.** A deny pattern of `\.dp3_progress$` let
  the table through. `.*\.dp3_progress$` excludes it. The entry that had been ingested was
  soft-deleted with `datahub delete --soft`.
- **Port 8080 on the Mac is the Spark master UI,** so GMS is on 8083.
