# Storage optimization: the warehouse and the lakehouse

Each part shows the code of the optimization, the queries it serves, and the measured difference
against the same data without it.

## Warehouse: indexes on `dwh.obt_delivery`

`obt_delivery` is the one big table for analysts: one wide row per order of the last 30 days
(168,000 rows on the dev slice, 4,420 pages of 8 kB). Without an index, every query reads all of it.
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
  column's order. Spark writes staging in four unordered partitions (correlation of `order_ts` with
  the physical order: −0.007), so the load now inserts `obt_delivery` ordered by `order_ts`. The
  correlation is then 1.000.

**Measured (2026-10-24)** with `uv run python scripts/dwh_indexes.py`. Each query runs `EXPLAIN
(ANALYZE, BUFFERS)` three times and the warm third run counts. "Without" drops the three indexes
in a transaction that is rolled back, so the warehouse is not changed.

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

The relationships of the warehouse tables (declared primary and foreign keys) are drawn in
[schema_design.md](schema_design.md#warehouse-dwh-postgres-written-by-dp2).
