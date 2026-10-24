# Documentation index

One document per major part of the platform. Each document explains what was built, how it is
set up, and the evidence (screenshots, measurements) that proves it works. Screenshots sit next to
the text that explains them.

Documents are added here as each part is built.

| Document | Covers |
|---|---|
| [environment.md](environment.md) | The machine, every pinned image (arm64, size, idle memory), PyFlink on arm64, and the safe stop and unplug procedure |
| [core_services.md](core_services.md) | Postgres, SeaweedFS and Kafka: what they hold, how to start them, verification, pitfalls |
| [data_generator.md](data_generator.md) | The seed, the delivery-time model, the LA config and SLA rule, the OMS reference data (hubs, couriers and their audit, customers) and orders with their dirty values, the courier-app ping and scan exports, and their checks |
