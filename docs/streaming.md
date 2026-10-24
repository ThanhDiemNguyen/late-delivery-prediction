# Streaming: live parcel-scan events in Kafka

The handhelds at the hubs and in the vans send every parcel scan to Kafka as it happens. The stream
has two problems on purpose:

- **late arrivals:** weak signal holds some scans back for minutes;
- **duplicates:** a handheld that gets no acknowledgement sends the same scan again.

Flink counts the scans in 5-minute windows per hub. That part follows in the next steps.

## Topics

The topics are created by `kafka-init` ([core_services.md](core_services.md)).

| Topic | Partitions | Written by | Holds |
|---|---:|---|---|
| `parcel-scans` | 3 | `generator/scan_producer.py` | the live stream, keyed by `order_id`, so the scans of one parcel stay in order on one partition |
| `parcel-scans-replay` | 1 | `generator/scan_producer.py` | the same events on one partition, for the Flink runs (see below) |
| `hub-delivery-5m` | 3 | Flink | delivered and late-delivered scans per hub and 5-minute window |
| `scans-late` | 1 | Flink | the side output: scans that arrived after their window closed |

**Why a 1-partition replay topic.** A Flink source's watermark is the minimum over its
partitions, so a partition that lags or sits idle holds every window open. With one partition,
the watermark follows the arrival order exactly, and a run is repeatable.

## The event

One JSON record per scan send. The key is `order_id`. The **Kafka record timestamp is
`scan_ts`**, the event time (`CreateTime`), so Flink can take event time from the record.

| Field | Type | Meaning |
|---|---|---|
| `scan_id` | bigint | the scan; a retry carries the same id |
| `order_id` | bigint | the parcel's order |
| `hub_id` | int | the hub of the order |
| `courier_id` | int or null | the courier; null for the `at_hub` scan by hub staff |
| `scan_type` | string | `at_hub`, `picked_up` or `delivered` |
| `scan_ts` | epoch ms (UTC) | when the scan happened (event time) |
| `promised_ts` | epoch ms (UTC) | the promised delivery time, so a `delivered` scan can be counted as late-delivered |
| `sent_ts` | epoch ms (UTC) | when the handheld sent it (arrival) |

## How the producer builds the events

```bash
uv run python -m generator.scan_producer --start 2024-11-04 --days 7 --topic parcel-scans-replay
uv run python -m generator.check_stream  --start 2024-11-04 --days 7 --topic parcel-scans-replay
```

1. **Read the scans** of export dates `--start` to `--start + days - 1` from
   `s3://app-exports/parcel_scans/` (each date must have its `_SUCCESS` marker). The same Parquet
   files are the truth that the Flink windows are checked against.
2. **Read each order's `promised_ts` and drop `area`** from `oms.orders` (the area trimmed).
3. **Delay every scan.** A scan is sent 0 to 3 s after it happens. Exactly 8% of the scans arrive
   2 to 20 minutes late instead (uniform). The late ones are chosen per area:
   - 25% of the scans of Semi-Urban and Other drops, where the signal is weak;
   - in Metropolitan and Urban, the share that makes 8% overall (6.1% for the default week).
4. **Retry 1.50% of the scans.** Each is sent a second time, 1 to 10 s after its first send,
   with the same payload.
5. **Send in arrival order** (`sent_ts`), keyed by `order_id`, with `timestamp=scan_ts`. The
   producer is idempotent (`enable.idempotence`), so Kafka's own retries add no duplicates. Only
   the 1.50% are duplicates.

The counts are exact (`round(share × scans)` rows drawn without replacement), and one seed fixes
every choice, so a rerun sends the same events.

- **Replacing the events.** Before sending, the producer empties the topic: `delete_records` up
  to the end offsets, which keep counting. `--keep` appends instead.
- **Pacing.** `--rate N` limits the send to N events per second. Without it, the week goes out
  in under a second. Event time comes from the records, so the send speed does not change any
  window.

The default slice is the week of 2024-11-04 to 2024-11-10. It is outside both peak weeks, so its
volume is ordinary: 83,460 scans, from the 28,000 orders of that week plus the ones that span its
midnights. 9.9% of the week's orders drop in Semi-Urban or Other.

## Measured (2026-10-24)

`check_stream` reads every record back from the topic and compares each rate with its target.
It passed 11 of 11 on `parcel-scans-replay`, and again on `parcel-scans` (3 partitions):

| Check | Measured | Target |
|---|---:|---|
| record timestamp is `scan_ts` | 100% | 100% |
| record timestamps of type `CreateTime` | 100% | 100% |
| late scans (2 to 20 min) | 8.000% | 8.00% ± 0.05 |
| late scans in Semi-Urban and Other | 25.006% | 25.00% ± 0.05 |
| late delays outside 2 to 20 min | 0 | 0 |
| on-time delays outside 0 to 3 s | 0 | 0 |
| scans sent twice | 1.500% | 1.50% ± 0.05 |
| scans sent more than twice | 0 | 0 |
| retry gaps outside 1 to 10 s | 0 | 0 |
| retries with a different payload | 0 | 0 |
| scans missing or extra against the export | 0 | 0 |

**Facts:**

- **Records:** 84,712: 83,460 distinct scans and 1,252 retries. Event time runs from 2024-11-04
  08:00 UTC (00:00 in LA) to 2024-11-11 07:59 UTC.
- **Late share by area:** Metropolitan 6.00%, Urban 6.34%, Semi-Urban 25.14%, Other 24.70%.
- **Late delays, in minutes:** 1,086 scans at 2 to 5, 1,894 at 5 to 10, 1,851 at 10 to 15, and
  1,846 at 15 to 20. That is uniform; the first bucket is 3 minutes wide.
- **On-time delay:** median 1.5 s, p99 2.97 s, maximum 3.00 s. **Retry gap:** 1.01 to 9.99 s.
- **Out of order:** 14.16% of the records arrive behind the newest event time already seen.
  7.99% arrive more than 10 s behind it, which is the bound of the Flink baseline's watermark.
  On `parcel-scans`, with the order taken within each of its 3 partitions, they are 10.29% and
  7.97%.
- **Speed:** 84,712 events in 0.6 s as fast as possible, and 16.9 s at `--rate 5000`.

`tests/test_scan_producer.py` checks the same rules on synthetic scans, without Kafka: the exact
shares, the delay ranges, the retries, arrival order, the same events from the same seed, and a
missing order refused. It also checks that every `check_stream` check passes on the built events.

## Pitfalls

- **The slice must exist in the exports.** The producer stops if a date lacks `_SUCCESS`. After
  the full-scale generation, the same command sends 100 times as many events (about 1.2 million a
  day).
- **Without emptying, the topic holds every replay,** and a reader from the earliest offset would
  count each scan once per replay. That is why the producer empties the topic by default.
