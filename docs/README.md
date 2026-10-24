# Documentation index

One document per major part of the platform. Each document explains what was built, how it is
set up, and the evidence (screenshots, measurements) that proves it works. Screenshots sit next to
the text that explains them.

Documents are added here as each part is built.

| Document | Covers |
|---|---|
| [environment.md](environment.md) | The machine, every pinned image (arm64, size, idle memory), PyFlink on arm64, and the safe stop and unplug procedure |
| [lakehouse.md](lakehouse.md) | Hive Metastore, Spark standalone, Delta Lake and Trino: services, the Spark image, the smoke test, DBeaver settings, the Delta/Spark pairing |
| [core_services.md](core_services.md) | Postgres, SeaweedFS and Kafka: what they hold, how to start them, verification, pitfalls |
| [data_generator.md](data_generator.md) | The seed, the delivery-time model, the LA config and SLA rule, the OMS reference data (hubs, couriers and their audit, customers) and orders with their dirty values, the courier-app ping and scan exports (app v2 schema change, re-uploaded duplicates), and their checks |
| [data_characteristics.md](data_characteristics.md) | The generated data: volume, skewed and high-cardinality columns, schema before and after app v2, duplicate and dirty-value rates (measured by check_data) |
| [pipelines.md](pipelines.md) | DP1 to DP3 Spark jobs: DP1 incremental ingest into bronze (JDBC by change time, exports by export date with mergeSchema) , its Great Expectations suite, and DP2 silver (cleaning, dedup, v1/v2 unification) |
