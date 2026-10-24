"""Live scan events: replay the parcel scans of a slice of days into Kafka, some late, some sent twice.

    uv run python -m generator.scan_producer --start 2024-11-04 --days 7 --topic parcel-scans-replay

The scans come from the export Parquet in s3://app-exports/parcel_scans/, the truth that the Flink
windows are checked against, and each order's area and promised_ts from oms.orders. A handheld sends
a scan 0 to 3 s after it happens. Exactly 8% of the scans arrive 2 to 20 minutes late instead: 25% of
the scans of Semi-Urban and Other drops, where the signal is weak, and the share of the other areas
that makes 8% overall. Exactly 1.50% of the scans are sent a second time, 1 to 10 s after the first
send and with the same scan_id. The events go out in arrival order (sent_ts), keyed by order_id, with
the event time scan_ts as the Kafka record timestamp. One seed fixes every choice, so a rerun sends
the same events.
"""

from __future__ import annotations

import argparse
import io
import json
import time

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from generator import db
from generator.app_exports import BUCKET, SCANS
from generator.oms_reference import DEFAULT_SEED
from generator.storage import s3_client

BOOTSTRAP = "127.0.0.1:9092"
LATE_SHARE = 0.08
LATE_SHARE_WEAK_SIGNAL = 0.25
WEAK_SIGNAL_AREAS = ("Semi-Urban", "Other")
ON_TIME_DELAY_MS = (0, 3_000)
LATE_DELAY_MS = (2 * 60_000, 20 * 60_000)
DUPLICATE_SHARE = 0.015
RETRY_GAP_MS = (1_000, 10_000)
EVENT_COLUMNS = ["scan_id", "order_id", "hub_id", "courier_id", "scan_type", "scan_ts", "promised_ts", "sent_ts"]


def read_scans(s3, start: str, days: int) -> pd.DataFrame:
    """Return the exported scans of the export dates start to start + days - 1, scan_ts in epoch ms."""
    frames = []
    for date in pd.date_range(start, periods=days).date:
        prefix = f"{SCANS}/export_date={date}/"
        keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])]
        parquet = [k for k in keys if k.endswith(".parquet")]
        if f"{prefix}_SUCCESS" not in keys or not parquet:
            raise SystemExit(f"export date {date} is not complete in s3://{BUCKET}/{prefix}")
        frames += [pq.read_table(io.BytesIO(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())).to_pandas() for k in parquet]
    scans = pd.concat(frames, ignore_index=True)
    scans["scan_ts"] = scans.scan_ts.dt.as_unit("ms").astype("int64")
    return scans.drop(columns="uploaded_ts")


def read_orders(cur, order_ids: np.ndarray) -> pd.DataFrame:
    """Return order_id, promised_ts (epoch ms) and the trimmed drop area of the given orders."""
    cur.execute(
        "SELECT order_id, (extract(epoch FROM promised_ts) * 1000)::bigint, btrim(area) FROM orders WHERE order_id = ANY(%s)",
        (order_ids.tolist(),),
    )
    return pd.DataFrame(cur.fetchall(), columns=["order_id", "promised_ts", "area"])


def build_events(scans: pd.DataFrame, orders: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """Return the events as the handhelds send them: delays, late arrivals and retries, in arrival order."""
    events = scans.merge(orders, on="order_id", how="left", validate="many_to_one")
    if events.promised_ts.isna().any():
        raise ValueError(f"{events.promised_ts.isna().sum()} scans have no order in oms")
    n = len(events)
    weak = events.area.isin(WEAK_SIGNAL_AREAS).to_numpy()
    weak_idx, other_idx = np.flatnonzero(weak), np.flatnonzero(~weak)
    n_weak_late = round(LATE_SHARE_WEAK_SIGNAL * len(weak_idx))
    n_other_late = round(LATE_SHARE * n) - n_weak_late
    if not 0 <= n_other_late <= len(other_idx):
        raise ValueError(f"no share of the other areas makes {LATE_SHARE:.0%} late overall")
    late = np.zeros(n, dtype=bool)
    late[rng.choice(weak_idx, n_weak_late, replace=False)] = True
    late[rng.choice(other_idx, n_other_late, replace=False)] = True
    delay = np.where(
        late,
        rng.integers(LATE_DELAY_MS[0], LATE_DELAY_MS[1] + 1, n),
        rng.integers(ON_TIME_DELAY_MS[0], ON_TIME_DELAY_MS[1] + 1, n),
    )
    events["sent_ts"] = events.scan_ts.to_numpy() + delay
    retried = events.iloc[np.sort(rng.choice(n, round(DUPLICATE_SHARE * n), replace=False))].copy()
    retried["sent_ts"] += rng.integers(RETRY_GAP_MS[0], RETRY_GAP_MS[1] + 1, len(retried))
    events = pd.concat([events, retried], ignore_index=True)
    events["promised_ts"] = events.promised_ts.astype("int64")
    return events.sort_values(["sent_ts", "scan_id"], kind="stable", ignore_index=True)[EVENT_COLUMNS]


def truncate(topic: str, bootstrap: str) -> int:
    """Delete every record of the topic (its offsets keep counting) and return how many there were."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "scan-producer", "enable.auto.commit": False})
    marks = {
        p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        for p in admin.list_topics(topic, timeout=10).topics[topic].partitions
    }
    consumer.close()
    removed = sum(high - low for low, high in marks.values())
    if removed:
        for future in admin.delete_records([TopicPartition(topic, p, high) for p, (_, high) in marks.items()]).values():
            future.result()
    return removed


def to_json(event) -> str:
    """Return one event as JSON: integers, the scan type, and null for the hub scan's courier."""
    return json.dumps({k: None if pd.isna(v) else v if isinstance(v, str) else int(v) for k, v in event._asdict().items()})


def send(events: pd.DataFrame, topic: str, bootstrap: str, rate: float) -> float:
    """Send the events in order, at most `rate` per second (0: as fast as possible); return the seconds taken."""
    producer = Producer({
        "bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all",
        "linger.ms": 20, "compression.type": "lz4",
    })
    failed = []

    def delivered(err, _msg) -> None:
        """Keep the error of an event that Kafka did not deliver."""
        if err is not None:
            failed.append(err)

    started = time.perf_counter()
    for i, event in enumerate(events.itertuples(index=False)):
        while True:
            try:
                producer.produce(topic, key=str(event.order_id), value=to_json(event), timestamp=int(event.scan_ts),
                                 on_delivery=delivered)
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
        if rate and i % 100 == 99:
            ahead = (i + 1) / rate - (time.perf_counter() - started)
            if ahead > 0:
                time.sleep(ahead)
    producer.flush(60)
    if failed:
        raise RuntimeError(f"{len(failed)} events were not delivered, first error: {failed[0]}")
    return time.perf_counter() - started


def main() -> None:
    """Build the events of a slice of days and send them to a Kafka topic."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", default="2024-11-04", help="first export date of the slice")
    parser.add_argument("--days", type=int, default=7, help="number of export dates")
    parser.add_argument("--topic", default="parcel-scans", help="parcel-scans (3 partitions) or parcel-scans-replay (1)")
    parser.add_argument("--rate", type=float, default=0, help="events per second; 0 sends as fast as possible")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--bootstrap", default=BOOTSTRAP)
    parser.add_argument("--keep", action="store_true", help="append to the topic instead of emptying it first")
    args = parser.parse_args()
    scans = read_scans(s3_client(), args.start, args.days)
    with db.connect("oms") as conn, conn.cursor() as cur:
        orders = read_orders(cur, scans.order_id.unique())
    events = build_events(scans, orders, np.random.default_rng([args.seed, 30]))
    if not args.keep:
        removed = truncate(args.topic, args.bootstrap)
        print(f"emptied {args.topic}: {removed:,} old records deleted")
    seconds = send(events, args.topic, args.bootstrap, args.rate)
    print(f"sent {len(events):,} events ({len(scans):,} scans, {len(events) - len(scans):,} retries) "
          f"from export dates {args.start} + {args.days} days to {args.topic} in {seconds:.1f} s")


if __name__ == "__main__":
    main()
