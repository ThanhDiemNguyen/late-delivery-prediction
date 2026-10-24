"""Measure the scan stream in a Kafka topic: event time, late arrivals and duplicates.

    uv run python -m generator.check_stream --topic parcel-scans-replay --start 2024-11-04 --days 7

Reads every record of the topic and compares each injected rate with its target, as check_data does:
the record timestamp is the scan's event time; 8.00% of the scans arrive 2 to 20 minutes late (25%
in Semi-Urban and Other drops), the rest within 3 s; 1.50% are sent twice, 1 to 10 s apart, with the
same payload. The distinct scans must equal the export Parquet of the same dates, so nothing is
lost. The command exits with status 1 if any check fails. The printed facts (delay histogram,
out-of-order share) are the numbers in docs/streaming.md.
"""

from __future__ import annotations

import argparse
import json
import sys

import numpy as np
import pandas as pd
from confluent_kafka import TIMESTAMP_CREATE_TIME, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from generator import db
from generator.check_data import Check
from generator.scan_producer import (
    BOOTSTRAP, DUPLICATE_SHARE, LATE_DELAY_MS, LATE_SHARE, LATE_SHARE_WEAK_SIGNAL, ON_TIME_DELAY_MS, RETRY_GAP_MS,
    WEAK_SIGNAL_AREAS, read_orders, read_scans,
)
from generator.storage import s3_client

WATERMARK_BOUND_MS = 10_000  # the Flink baseline's out-of-orderness bound


def read_topic(topic: str, bootstrap: str) -> pd.DataFrame:
    """Return every record of the topic: partition, offset, timestamp type and value, and the payload."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "check-stream", "enable.auto.commit": False})
    marks = {
        p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        for p in admin.list_topics(topic, timeout=10).topics[topic].partitions
    }
    remaining = {p: high - low for p, (low, high) in marks.items() if high > low}
    consumer.assign([TopicPartition(topic, p, low) for p, (low, _) in marks.items()])
    rows = []
    while remaining:
        for msg in consumer.consume(10_000, timeout=5):
            if msg.error():
                raise RuntimeError(msg.error())
            ts_type, ts = msg.timestamp()
            rows.append({"partition": msg.partition(), "offset": msg.offset(), "ts_type": ts_type, "record_ts": ts,
                         **json.loads(msg.value())})
            if msg.offset() == marks[msg.partition()][1] - 1:
                remaining.pop(msg.partition(), None)
    consumer.close()
    return pd.DataFrame(rows)


def measure(records: pd.DataFrame, areas: pd.DataFrame, exported: pd.DataFrame | None) -> tuple[list[Check], dict]:
    """Return the checks and facts of the stream."""
    records = records.sort_values(["partition", "offset"], ignore_index=True)
    first = records.sort_values(["sent_ts", "offset"]).drop_duplicates("scan_id").merge(areas, on="order_id", how="left")
    delay = first.sent_ts - first.scan_ts
    late = delay >= LATE_DELAY_MS[0]
    weak = first.area.isin(WEAK_SIGNAL_AREAS)
    payload = ["scan_id", "order_id", "hub_id", "courier_id", "scan_type", "scan_ts", "promised_ts"]
    retries = records[records.duplicated("scan_id", keep=False)]
    sends = retries.groupby("scan_id").agg(sends=("sent_ts", "size"), gap=("sent_ts", lambda s: s.max() - s.min()))
    differing = retries.drop_duplicates(payload).duplicated("scan_id").sum()
    newest = records.groupby("partition").record_ts.cummax()
    checks = [
        Check("record timestamp is scan_ts %", 100 * (records.record_ts == records.scan_ts).mean(), 100, 0),
        Check("record timestamps of type CreateTime %", 100 * (records.ts_type == TIMESTAMP_CREATE_TIME).mean(), 100, 0),
        Check("late scans (2 to 20 min) %", 100 * late.mean(), 100 * LATE_SHARE, 0.05),
        Check("late scans in Semi-Urban and Other %", 100 * late[weak].mean(), 100 * LATE_SHARE_WEAK_SIGNAL, 0.05),
        Check("late delays outside 2 to 20 min", int((delay[late] > LATE_DELAY_MS[1]).sum()), 0, 0),
        Check("on-time delays outside 0 to 3 s", int(((delay < ON_TIME_DELAY_MS[0]) | (~late & (delay > ON_TIME_DELAY_MS[1]))).sum()), 0, 0),
        Check("scans sent twice %", 100 * len(sends) / len(first), 100 * DUPLICATE_SHARE, 0.05),
        Check("scans sent more than twice", int((sends.sends > 2).sum()), 0, 0),
        Check("retry gaps outside 1 to 10 s", int(((sends.gap < RETRY_GAP_MS[0]) | (sends.gap > RETRY_GAP_MS[1])).sum()), 0, 0),
        Check("retries with a different payload", int(differing), 0, 0),
    ]
    if exported is not None:
        missing = len(set(exported.scan_id) - set(first.scan_id))
        extra = len(set(first.scan_id) - set(exported.scan_id))
        checks.append(Check("scans missing or extra against the export", missing + extra, 0, 0))
    edges = [2, 5, 10, 15, 20]
    histogram = pd.cut(delay[late] / 60_000, edges, include_lowest=True).value_counts(sort=False)
    facts = {
        "records": f"{len(records):,} in {records.partition.nunique()} partition(s); {len(first):,} distinct scans, "
                   f"{len(records) - len(first):,} retries",
        "event time span": f"{pd.to_datetime(first.scan_ts.min(), unit='ms', utc=True)} to "
                           f"{pd.to_datetime(first.scan_ts.max(), unit='ms', utc=True)}",
        "late share by area %": (100 * late.groupby(first.area).mean()).round(2).to_dict(),
        "late delay histogram (minutes: scans)": {str(k): int(v) for k, v in histogram.items()},
        "on-time delay ms (p50, p99, max)": [int(np.percentile(delay[~late], q)) for q in (50, 99, 100)],
        "retry gap ms (min, max)": [int(sends.gap.min()), int(sends.gap.max())],
        "records behind the newest event time %": round(100 * (records.record_ts < newest).mean(), 2),
        f"records more than {WATERMARK_BOUND_MS // 1000} s behind it %":
            round(100 * (records.record_ts < newest - WATERMARK_BOUND_MS).mean(), 2),
    }
    return checks, facts


def main() -> None:
    """Read the topic, measure it and print the checks and facts."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--topic", default="parcel-scans")
    parser.add_argument("--start", default="2024-11-04", help="first export date the producer sent")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--bootstrap", default=BOOTSTRAP)
    args = parser.parse_args()
    records = read_topic(args.topic, args.bootstrap)
    if records.empty:
        raise SystemExit(f"{args.topic} has no records; run generator.scan_producer first")
    with db.connect("oms") as conn, conn.cursor() as cur:
        areas = read_orders(cur, records.order_id.unique())[["order_id", "area"]]
    checks, facts = measure(records, areas, read_scans(s3_client(), args.start, args.days))
    width = max(len(c.name) for c in checks)
    for c in checks:
        print(f"{'PASS' if c.passed else 'FAIL'}  {c.name:<{width}}  {c.measured:>10,.3f}  target {c.target:,.3f} ± {c.tolerance:g}")
    print()
    for key, value in facts.items():
        print(f"{key}: {value}")
    failed = [c.name for c in checks if not c.passed]
    print(f"\n{len(checks) - len(failed)} of {len(checks)} checks passed" + (f"; failed: {failed}" if failed else ""))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
