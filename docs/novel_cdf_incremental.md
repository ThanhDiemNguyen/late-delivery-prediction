# Novel idea 2: incremental features from Delta's Change Data Feed

DP3 normally recomputes every feature row from the full gold facts. With Delta Lake's Change Data
Feed (CDF), a technique the EDAI course does not teach, DP3 instead reads just the fact rows that
changed since its last run, finds the couriers, hubs and zones they touch, and recomputes and
merges only those feature rows. For changes in the facts, a full recompute proves the result is
identical; the limits below say which changes the feed does not see.

## What the Change Data Feed is

When a Delta table has `delta.enableChangeDataFeed = true`, every write also records which rows it
inserted, updated or deleted. The change feed of versions *a* to *b* lists those rows with three
extra columns: `_change_type`, `_commit_version` and `_commit_timestamp`. The change types are
`insert`, `update_preimage`, `update_postimage` and `delete`. A consumer reads only those rows,
never the whole table.

## What changed in the pipelines

| Part | Before | Now |
|---|---|---|
| `gold.fact_delivery` (DP2) | rewritten from silver on every run | `MERGE` by `order_id`. An existing order is updated only when one of its values really differs (a NULL-safe comparison of 13 columns), so the CDF lists real changes only. CDF is on |
| `gold.fact_courier_activity_hourly` (DP2) | new ping dates added | the same, with CDF on |
| DP3 (`jobs/dp3_features.py`) | full recompute | `--incremental` reads the CDF of both facts since the versions in `gold.dp3_progress` |
| proof | | `jobs/check_cdf.py` compares every feature table with a full recompute |

CDF is turned on through the table's path (`ALTER TABLE delta.\`s3a://…\` SET TBLPROPERTIES`). So
only the Delta log changes, and the Hive Metastore entry, which Hive Metastore 3.1 cannot alter for
Delta tables, is never touched.

### How an incremental DP3 run works

1. **Find the new versions.** Read the versions the last run processed from `gold.dp3_progress`,
   and the current version of each fact. If nothing changed, stop.
2. **Read the changes.** Read the CDF of both facts between those versions. Pre-images and deletes
   count too: an order that moved from one courier to another changes both couriers' features.
3. **Find what they touch.** For each feature table, collect the entities the changed rows name
   (courier, hub, H3 zone) and the earliest date or hour they changed.
4. **Recompute.** Recompute those entities' feature rows from that date on. The builders read only
   the history the rolling windows need: 29 days before the date for the 30-day features, and 167
   hours before it for the 7-day hourly feature.
5. **Merge and record.** `MERGE` the rows into the feature table by its key, and record the new
   fact versions.

A rerun with no new fact version does nothing.

## Proof that it worked (2026-10-23, dev slice)

Two scenarios, each run the same way:

1. Rebuild the facts as of a local date.
2. Run DP3 in full.
3. Merge the rest of the period into the facts.
4. Run DP3 with `--incremental`, then once more.
5. Prove the result with `jobs/check_cdf.py`.

| Scenario | Fact changes read from the CDF | Feature rows rewritten | Full DP3 | Incremental DP3 | Differences from a full recompute |
|---|---|---|---:|---:|---:|
| one month: facts as of 2024-10-31, then November | 168,917 `fact_delivery` rows (1 version), 59,620 activity rows (2 versions) | every courier, hub and 1,493 zones from November on | 67 s | 63 s | 0 |
| one day: facts as of 2024-11-29, then 2024-11-30 | 14,593 `fact_delivery` rows, 2,000 activity rows | 400 of 33,836 courier rows (1.2%), 1,240 of 175,760 hub rows (0.7%), 2,466 of 275,448 zone rows (0.9%) | 73 s | 67 s | 0 |

The one-day run, as printed:

```text
CDF rows read: fact_delivery 14,593 (versions 16-16), fact_courier_activity_hourly 2,000 (versions 19-20)
gold.feat_courier_daily: 200 entities recomputed from 2024-11-30; 400 feature rows rewritten of 33,836; merged in 18.7 s
gold.feat_hub_hourly: 40 entities recomputed from 2024-11-30 02:00:00; 1,240 feature rows rewritten of 175,760; merged in 11.0 s
gold.feat_zone_daily: 1,233 entities recomputed from 2024-11-30; 2,466 feature rows rewritten of 275,448; merged in 6.4 s
$ dp3_features.py --incremental        # run again
no fact changed since versions {'fact_courier_activity_hourly': 20, 'fact_delivery': 16}
$ check_cdf.py
PASS  gold.feat_courier_daily: 33,836 rows; only in the stored table 0, only in the full recompute 0
PASS  gold.feat_hub_hourly: 175,760 rows; only in the stored table 0, only in the full recompute 0
PASS  gold.feat_zone_daily: 275,448 rows; only in the stored table 0, only in the full recompute 0
0 differences
```

**What the proof shows:**

- **Identical results.** After each incremental run, every feature table equals a full recompute
  row for row: `exceptAll` in both directions over every column except `created`, 0 rows.
- **Little rewritten.** A daily run rewrote about 1% of the feature rows, and read 14,593 changed
  fact rows instead of 808,000 deliveries and 336,000 activity rows.
- **Nothing to do on a rerun.** A rerun with no new fact version does nothing.
- **Trimming helps.** Trimming the history to the rolling windows cut the courier recompute from
  23.6 s to 17.1 s, and the hub recompute from 19.1 s to 10.3 s, in the one-month scenario.

On the 1% slice, the wall-clock gain is small (about 6 s), because most of each run is Spark's fixed
cost. The work avoided grows with the data: at full scale a day's changes are still about 1/183
of the facts, while a full recompute reads all of them.

## Limits

- **The slice is small.** Spark's fixed cost (starting the session, planning, a few
  small jobs) is most of each run, so the wall-clock saving is small. The saving that grows with
  the data is the work avoided: rows read and feature rows rewritten.
- **The grain of recomputation is coarse.** A changed delivery recomputes its courier's rows from
  its date to the end of the period, not just the 30 days its windows reach. That is simpler, and
  it is the same rows in a daily run, where the change is near the end anyway.
- **A rebuild resets the feed.** Rewriting a fact (`--rebuild`) records every row as changed, so
  the next incremental run recomputes everything. That is correct, just not cheaper.
- **A new dimension key waits for a fact.** The incremental run follows only the facts' Change
  Data Feed, but the courier and zone grids also come from `dim_courier` and `dim_zone`. A key
  that arrives without fact rows is not added until a fact names it. Then its rows from that
  fact's date on are merged, and its earlier rows are never written. Two cases: a courier hired
  on a day with no orders or pings for that courier loses the row of the hire day, and a new drop
  zone loses its rows before its first delivery. `jobs/check_cdf.py` would report these rows as
  only in the full recompute. The proofs above rebuilt only the facts, so they did not test this
  case.
