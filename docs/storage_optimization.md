# Storage optimization: the warehouse and the lakehouse

Each part shows the code of the optimization, the queries it serves, and the measured difference
against the same data without it.

- [Warehouse: indexes on `dwh.obt_delivery`](#warehouse-indexes-on-dwhobt_delivery)
- [Lakehouse: compaction and Z-order on the pings](#lakehouse-compaction-and-z-order-on-the-pings)

## Warehouse: indexes on `dwh.obt_delivery`

`obt_delivery` is the one big table for analysts: one wide row per order of the last 30 days. On
the dev slice it has 168,000 rows in 4,420 pages of 8 kB. Without an index, every query reads all
of it.
Three indexes serve the three kinds of question the hub managers ask.

```sql
-- jobs/sql/dwh_schema.sql, applied by every warehouse load (IF NOT EXISTS)
CREATE INDEX IF NOT EXISTS obt_delivery_hub_promised ON obt_delivery (hub_id, promised_ts);
CREATE INDEX IF NOT EXISTS obt_delivery_late_hub_delivered ON obt_delivery (hub_id, delivered_ts) WHERE is_late;
CREATE INDEX IF NOT EXISTS obt_delivery_order_ts_brin ON obt_delivery USING brin (order_ts);
```

```python
# jobs/dp2_warehouse.py: the swap fills obt_delivery in order_ts order
PHYSICAL_ORDER = {"obt_delivery": "order_ts"}
...
order = f" ORDER BY {PHYSICAL_ORDER[name]}" if name in PHYSICAL_ORDER else ""
swap.append(f"INSERT INTO public.{name} ({columns}) SELECT {columns} FROM staging.{name}{order}")
```

- **B-tree on `(hub_id, promised_ts)`:** one hub's deliveries promised in a time window. The
  equality column comes first, so the range on the second column reads one contiguous part of the
  index.
- **Partial index on `(hub_id, delivered_ts) WHERE is_late`:** a hub's latest late deliveries.
  It holds only the late rows, 17.6% of the table, so it is 1.4 MB against 8.3 MB for the full
  B-tree. It already returns the rows in `delivered_ts` order, so `ORDER BY ... LIMIT 20` stops
  after 20 entries.
- **BRIN on `order_ts`:** time-window scans. A BRIN index keeps only the minimum and maximum of
  each range of 128 pages, so it is 24 kB. It works only when the rows sit on disk in that
  column's order. Spark writes staging in four unordered partitions: the correlation of `order_ts`
  with the physical order is −0.007. So the load now inserts `obt_delivery` ordered by `order_ts`,
  which brings the correlation to 1.000.

**Measured (2026-10-24)** with `uv run python scripts/dwh_indexes.py`. Each query runs `EXPLAIN
(ANALYZE, BUFFERS)` three times and the warm third run counts. The "Without" run drops the three
indexes inside a transaction and then rolls it back, so the warehouse does not change.

| Query | Without the index | With the index | Pages read | Time |
|---|---|---|---:|---:|
| one hub's deliveries promised on one day | Seq Scan: 4,420 pages, 18.42 ms | Bitmap Index Scan on `obt_delivery_hub_promised`: 89 pages, 0.24 ms | 50× fewer | 77× faster |
| a hub's 20 latest late deliveries | Seq Scan: 4,534 pages, 13.34 ms | Index Scan on `obt_delivery_late_hub_delivered`: 22 pages, 0.02 ms | 206× fewer | 670× faster |
| orders per hub in a two-hour window | Seq Scan: 4,532 pages, 14.17 ms | Bitmap Index Scan on `obt_delivery_order_ts_brin`: 326 pages, 2.05 ms | 14× fewer | 7× faster |

| Index | Size |
|---|---:|
| `obt_delivery_pkey` (`order_id`) | 3,704 kB |
| `obt_delivery_hub_promised` | 8,296 kB |
| `obt_delivery_late_hub_delivered` | 1,384 kB |
| `obt_delivery_order_ts_brin` | 24 kB |

- **The cost of the indexes** is the extra work on each load. The warehouse load, including
  the three indexes, took 72 s on the slice.
- **BRIN reads more pages than a B-tree would.** It returns whole 128-page ranges, which the
  bitmap heap scan then filters. In exchange it is 345 times smaller than the B-tree.

A diagram in [schema_design.md](schema_design.md#warehouse-dwh-postgres-written-by-dp2) shows the
relationships of the warehouse tables (the declared primary and foreign keys).

## Lakehouse: compaction and Z-order on the pings

`silver.stg_courier_pings` is partitioned by `ping_date`. Partitioning already lets a query of one
day read only that day's folder. But partitioning does not fix how many files a day holds, or how
the rows are spread over them. Each task of a job writes one file into every date that it holds
rows for. After DP2's dedup window, which uses 200 shuffle partitions, the slice's silver table had
9,150 files for 1.68 GB. That is 50 files of about 180 kB in each date.

**The benchmark.** `jobs/lakehouse_layout_benchmark.py` writes one full-scale week of pings,
166,023,384 rows, the way a 200-task job writes it. Then it rewrites the table twice:

```python
pings.repartition(200).write.format("delta").partitionBy("ping_date").save(TABLE)   # 200 files a date
table = DeltaTable.forPath(spark, TABLE)
table.optimize().executeCompaction()          # OPTIMIZE: files of up to 128 MB in each date
table.optimize().executeZOrderBy("courier_id")  # OPTIMIZE ZORDER BY (courier_id)
table.vacuum(0)                               # VACUUM: delete the files the rewrites replaced
```

- **`OPTIMIZE` (compaction)** rewrites each date's small files into a few large ones. A query then
  opens fewer files and reads fewer footers, and the large files compress better.
- **`ZORDER BY (courier_id)`** sorts the rows by courier as it rewrites them. Each file then holds
  a narrow range of couriers, and Delta records each file's minimum and maximum `courier_id`. A
  query for one courier skips every file whose range cannot hold it.
- **`VACUUM`** deletes the files the two rewrites replaced. Until then they stay in storage, so
  time travel can read the old versions.

Each of the two queries runs twice after each step, and the second run counts. Their files and bytes read
come from the Spark SQL metrics of the `Scan parquet` node:

```sql
SELECT count(*), round(avg(speed_kmh), 2) FROM pings WHERE ping_date = '2024-06-03' AND courier_id = 15162  -- one courier, one day
SELECT ping_date, count(*) FROM pings WHERE hub_id = 17 GROUP BY ping_date                                  -- one hub, one week
```

**Measured on the full-scale week (2026-10-24):**

| Step (time it took) | Files in the table | One courier, one day | One hub, one week |
|---|---:|---|---|
| after the write (338.8 s) | 1,400 (4.83 GB) | 200 files, 657.3 MiB, 23,685,732 rows scanned, **2.83 s** | 1,400 files, 4.5 GiB, **23.52 s** |
| after `OPTIMIZE` (181.1 s) | 42 (4.58 GB) | 6 files, 623.5 MiB, 23,682,660 rows, **1.34 s** | 42 files, 4.3 GiB, **2.44 s** |
| after `ZORDER BY (courier_id)` (262.0 s) | 28 (4.27 GB) | **1 file, 148.6 MiB, 3,600,000 rows, 0.62 s** | 28 files, 4.0 GiB, 0.75 s |
| after `VACUUM` (23.8 s) | 28 (4.27 GB) | 1 file, 148.6 MiB, 0.39 s | 28 files, 4.0 GiB, 0.89 s |

- **Compaction fixes the small files.** The week query opens 42 files instead of 1,400, and runs
  in 2.44 s instead of 23.52 s, 9.6 times faster. It reads almost the same bytes (4.3 GiB against
  4.5 GiB), so the time went into the number of files, not into the data.
- **Z-order lets the courier query skip files.** Within the date it reads 1 file, where it read 6
  after compaction alone, and 3.6 million rows instead of 23.7 million. That is 0.62 s against
  1.34 s, and 2.83 s at the start: 4.6 times faster overall.
- **Z-order on `courier_id` does not skip files for the hub query.** It still reads every file of
  the week. Its smaller bytes come from the better compression of sorted data.
- **The rewrites are not free.** `OPTIMIZE` and Z-order together took 7.4 minutes for a week. The
  table shrank by 12% (4.83 GB to 4.27 GB), because sorted columns compress better.

**In the pipeline.** DP2 now compacts each date of `stg_courier_pings` immediately after it writes
that date, and orders it by courier (`jobs/dp2_silver.py`):

```python
save(pings, "stg_courier_pings", partition_by="ping_date")
listed = ", ".join(f"DATE'{d}'" for d in dates)
spark.sql(f"OPTIMIZE silver.stg_courier_pings WHERE ping_date IN ({listed}) ZORDER BY (courier_id)")
```

The pipeline schedules no `VACUUM`, so the files that each `OPTIMIZE` replaces stay in storage for
time travel. The sizes in the table below are those of the current version.

On the slice's own silver table (`lakehouse_layout_benchmark.py --silver`, then a DP2 silver
rebuild that took 6.4 minutes and ran the new step):

| `silver.stg_courier_pings` (slice) | Files | Size | One courier, one day | One hub, one week |
|---|---:|---:|---|---|
| before | 9,150 | 1.68 GB | 50 files, 7.5 MiB, 0.76 s | 350 files, 52.6 MiB, 1.42 s |
| after `OPTIMIZE ZORDER BY (courier_id)` | 183 | 1.17 GB | 1 file, 5.2 MiB, 0.25 s | 7 files, 36.1 MiB, 0.53 s |

A slice day fits in one file, so on the slice the gain comes from the compaction. It gives 50 times
fewer files, a table 30% smaller, and both queries about 3 times faster. The skipping inside a day
shows at full scale, in the benchmark above.

**Pitfall: an executor killed by the VM.** One benchmark run lost its executor during the Z-order,
with exit code 137 (SIGKILL), not Java's 52. The kernel killed it, because every profile was
running and the Docker VM ran short of memory. With Flink and Trino stopped (`docker compose stop`), the
rerun finished.
