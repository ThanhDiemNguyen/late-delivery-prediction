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
far are all in the `core` Compose profile ([details](docs/core_services.md)):

| Unit | Role |
|---|---|
| Postgres 16 | source database `oms` (order system), plus the `dwh`, `airflow`, `metastore` and `datahub` databases |
| SeaweedFS | S3 object store: bucket `app-exports` (courier-app exports, a source) and bucket `lakehouse` |
| Kafka (KRaft) | live parcel-scan events and Flink output topics |

Start them with `cp .env.example .env` (then change the passwords) and
`docker compose --profile core up -d --wait`.

## Repo structure

```text
.
├── README.md            this file: domain, architecture, structure, links to docs
├── docker-compose.yaml  every service, grouped by Compose profile (core so far)
├── infra/
│   └── postgres/init/   SQL run on the first start of Postgres (creates the databases)
├── docs/                one document per major part, linked from docs/README.md
├── pyproject.toml       Python project settings and the ruff lint rules
├── .env.example     template for the local .env file (the real .env is never committed)
└── .gitignore       files and folders that git must never track
```

## Requirements

- An Apple Silicon Mac. Every image runs as linux/arm64.
- Docker Desktop with 16 GB memory, 8 CPUs, 2 GB swap and an 800 GB disk limit.
- [uv](https://docs.astral.sh/uv/) for Python tools. The linter runs with `uvx ruff check .`.

## Documentation

All detailed documents live in [`docs/`](docs/README.md). This README only summarizes them.
