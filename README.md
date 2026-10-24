# Late-delivery prediction: the LastLeg Logistics data platform

The EDAI-1 coursework data platform for **LastLeg Logistics**, a fictional last-mile parcel carrier
in the Los Angeles area. The platform generates the company's source data, moves it through
bronze, silver and gold lakehouse zones, and builds the feature tables that a model will use to
predict late deliveries. The model itself is trained in EDAI-2.

All data in this repository is synthetic, and the company is fictional.

## Table of contents

1. [Business domain](#business-domain)
2. [Architecture](#architecture)
3. [Repo structure](#repo-structure)
4. [Requirements](#requirements)
5. [Documentation](#documentation)

## Business domain

LastLeg Logistics delivers parcels from about 40 hubs across Los Angeles.

- **Prediction moment:** the `picked_up` scan, when a courier leaves the hub with the parcel. At
  that moment the courier, vehicle, traffic, weather and hub load are known, and the dispatcher can
  still act.
- **Label:** a delivery is late when `delivered_ts > promised_ts`.
- **Promised time:** `promised_ts = order_ts + SLA`. The SLA is set when the customer orders, from
  the area type plus 3 minutes per km of distance from the hub to the drop point.
- **Seed data:** the [Amazon Delivery Dataset](https://www.kaggle.com/datasets/sujalsuthar/amazon-delivery-dataset)
  on Kaggle (MIT license, 43,739 rows). It only sets the shapes of the generated data, such as how
  traffic and weather change delivery time. Every row in the platform is generated.

EDAI-1 builds the data platform and the feature tables. It does not train a model.

## Architecture

The full deployment diagram is added once all the services exist. The deployable units built so
far are in the `core` Compose profile ([details](docs/core_services.md)) and the `batch` and `query` profiles:

| Unit | Role |
|---|---|
| Postgres 16 | source database `oms` (order system), plus the `dwh`, `airflow`, `metastore` and `datahub` databases |
| SeaweedFS | S3 object store: bucket `app-exports` (courier-app exports, a source) and bucket `lakehouse` |
| Kafka (KRaft) | live parcel-scan events and Flink output topics |
| Hive Metastore | catalog of the lakehouse tables (profiles `batch` and `query`, [details](docs/lakehouse.md)) |
| Spark master, worker, history server | builds the Delta tables in bucket `lakehouse` (profile `batch`) |
| Trino | SQL over the Delta tables for DBeaver and DataHub (profile `query`) |

Start them with `cp .env.example .env` (then change the passwords) and
`docker compose --profile core --profile batch up -d --wait`.

## Repo structure

```text
.
├── README.md            this file: domain, architecture, structure, links to docs
├── docker-compose.yaml  every service, grouped by Compose profile (core so far)
├── generator/           host-Python data generator: seed model, LA config, oms tables, app exports, checks
│   └── sql/             DDL of the oms tables, applied by the generator
├── seed/                the Kaggle seed file (gzipped) and its MIT license
├── tests/               pytest checks, run with: uv run pytest
├── jobs/                Spark jobs (DP1 ingest and validation, DP2 silver, gold and the warehouse, DP3 features), run inside spark-master with spark-submit
│   └── sql/             DDL of the dwh star schema
├── infra/
│   ├── postgres/init/   SQL run on the first start of Postgres (creates the databases)
│   ├── spark/           the Spark image (Dockerfile.naive) and conf/spark-defaults.conf
│   └── trino/catalog/   Trino catalog delta (Delta tables through the Hive Metastore)
├── scripts/
│   └── safe-shutdown.sh stop everything, quit Docker Desktop and eject the SSD
├── docs/                one document per major part, linked from docs/README.md
├── pyproject.toml       Python project settings, dependencies and the ruff lint rules
├── uv.lock              exact versions of every Python dependency
├── .env.example     template for the local .env file (the real .env is never committed)
└── .gitignore       files and folders that git must never track
```

## Requirements

- An Apple Silicon Mac. Every image runs as linux/arm64 ([image inventory](docs/environment.md#image-inventory)).
- Docker Desktop with 16 GB memory, 8 CPUs, 2 GB swap and an 800 GB disk limit. Its disk image is
  on an external SSD; run `scripts/safe-shutdown.sh` before unplugging it
  ([procedure](docs/environment.md#start-stop-and-unplug-safely)).
- [uv](https://docs.astral.sh/uv/) for Python. `uv sync` installs Python 3.10 and the
  dependencies, `uv run pytest` runs the tests, and `uvx ruff check .` runs the linter.

## Documentation

All detailed documents live in [`docs/`](docs/README.md). This README only summarizes them.
